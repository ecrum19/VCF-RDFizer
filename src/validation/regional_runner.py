#!/usr/bin/env python3
"""Indexed regional access: SPARQL against a coordinate-indexed VCF.

The whole-file retrieval comparison in ``validation_runner`` pits a SPARQL
engine against cyvcf2 *scanning the file*, because none of Q1-Q13 is
coordinate-restricted. That is internally consistent, but it measures the graph
against the one VCF access mode nobody uses for a selective question. Real VCF
work seeks: bgzip + tabix, then ``bcftools -r`` or cyvcf2's region iterator,
which reads a handful of BGZF blocks instead of the file.

This runner adds that arm. It asks the same five bioinformatic questions,
restricted to a coordinate window, of every access path:

    cyvcf2-scan       whole-file iteration with a position filter (the status
                      quo, kept so the indexed arms have a baseline)
    cyvcf2-indexed    VCF(path)(region) over the bgzipped, indexed file
    bcftools-indexed  bcftools query -r
    comunica|hdt|cottas|qlever   the same question as SPARQL

Results are compared for exact equality before any timing is reported, exactly
as the whole-file suite does. A speed number from arms that disagree is not a
result, it is a bug.

Run it through ``benchmarks/14_regional_access.sh``, which mounts the inputs
and pins the image; the raw entry point is documented in ``--help``.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import validation_runner as V  # noqa: E402

# ---------------------------------------------------------------------------
# What a window means, on both sides
# ---------------------------------------------------------------------------
REGION_SEMANTICS = """\
A window is POS-based, 1-based and inclusive: a record belongs to it when
START <= POS <= END on the named contig.

This has to be said explicitly because htslib does not mean that. A tabix or
CSI seek returns every record whose *span* overlaps the region, so a deletion
beginning before START and reaching into the window comes back from
`bcftools -r` and from cyvcf2's region iterator, while a POS filter in SPARQL
excludes it. Left alone, that difference would make the arms disagree on any
window whose left edge cuts an indel -- and it would look like a graph defect
rather than a coordinate convention.

So both indexed arms keep the index seek (the thing being measured) and then
drop records whose POS falls outside the window. The seek still does the work;
the post-filter only reconciles the convention, costs one integer comparison
per returned record, and is applied by every VCF arm alike.
"""

#: The regional questions, in report order.
REGIONAL_QUERIES = (
    "r01_region_record_count",
    "r02_region_variant_shape_counts",
    "r03_region_titv",
    "r04_region_filter_distribution",
    "r05_region_sample_genotype_counts",
)

#: Same shape as ``validation_runner.QUERY_SCHEMAS``: fields, integer fields,
#: sort fields. Reused by the SPARQL normalizer so a regional answer is
#: canonicalised the same way a whole-file answer is.
REGIONAL_QUERY_SCHEMAS = {
    "r01_region_record_count": (("recordCount",), {"recordCount"}, ()),
    "r02_region_variant_shape_counts": (("variantClass", "recordCount"), {"recordCount"}, ("variantClass",)),
    "r03_region_titv": (
        ("biallelicSnvCount", "transitionCount", "transversionCount"),
        {"biallelicSnvCount", "transitionCount", "transversionCount"},
        (),
    ),
    "r04_region_filter_distribution": (("filterStatus", "filterLexical", "recordCount"), {"recordCount"}, ("filterStatus", "filterLexical")),
    "r05_region_sample_genotype_counts": (("sampleId", "genotypeClass", "callCount"), {"callCount"}, ("sampleId", "genotypeClass")),
}

#: Aggregates over an empty window still return one row, so these are shaped as
#: a single dict rather than a list.
REGIONAL_SINGLE_ROW = frozenset({"r01_region_record_count", "r03_region_titv"})

#: Queries that need the sample layer. On a single-sample file this is noise;
#: on a cohort it is most of the work, and it is the reason an arm's ranking can
#: differ between R1-R4 and R5.
REGIONAL_SAMPLE_LEVEL_QUERIES = frozenset({"r05_region_sample_genotype_counts"})

VCF_ARMS = ("cyvcf2-scan", "cyvcf2-indexed", "bcftools-indexed")
DEFAULT_WINDOW_SIZES = (1_000, 100_000, 1_000_000, 10_000_000)
DEFAULT_WINDOWS_PER_SIZE = 20
DEFAULT_SEED = 20260923


# ---------------------------------------------------------------------------
# Window selection
# ---------------------------------------------------------------------------
def contig_record_positions(vcf_path: Path) -> dict[str, list[int]]:
    """One pass to learn where the records actually are.

    Windows are anchored on real record positions rather than drawn uniformly
    across the contig. A GIAB benchmark VCF covers a few percent of the genome,
    so uniform 1 kb windows would be almost all empty and the experiment would
    measure the cost of returning nothing. Anchoring guarantees every window
    holds at least one record, which is what makes the four window sizes a
    selectivity ladder instead of a sparsity ladder.

    This is setup, not measurement: it runs once, before any timing.
    """
    if V.VCF is None:
        raise RuntimeError("cyvcf2 is unavailable; run this inside the VCF-RDFizer image")
    positions: dict[str, list[int]] = {}
    reader = V.VCF(str(vcf_path))
    try:
        for variant in reader:
            positions.setdefault(str(variant.CHROM), []).append(int(variant.POS))
    finally:
        reader.close()
    return positions


def draw_windows(
    positions: dict[str, list[int]],
    sizes: tuple[int, ...],
    per_size: int,
    seed: int,
) -> list[dict[str, Any]]:
    """Draw the same windows for every arm, reproducibly.

    Each window is anchored at a randomly chosen record and extended rightwards,
    so a window of any size contains that record. The draw is seeded, and the
    resulting list is written to the results directory, so a rerun measures the
    identical regions and a reader can check which ones they were.
    """
    if not positions:
        raise RuntimeError("the VCF has no records, so there is nothing to window")
    rng = random.Random(seed)
    contigs = sorted(positions)
    windows: list[dict[str, Any]] = []
    for size in sizes:
        for index in range(per_size):
            contig = rng.choice(contigs)
            anchor = rng.choice(positions[contig])
            # Anchor at the record, extend right. Starting at the anchor rather
            # than centring on it keeps START >= 1 without a clamp that would
            # quietly shrink the smallest windows.
            start = anchor
            end = start + size - 1
            windows.append({
                "window_id": f"w{size}_{index:02d}",
                "chrom": contig,
                "start": start,
                "end": end,
                "size": size,
                "anchor_pos": anchor,
            })
    return windows


def windows_expected_counts(
    positions: dict[str, list[int]], windows: list[dict[str, Any]]
) -> None:
    """Record how many records each window holds, for the report's x-axis.

    Selectivity is what the experiment varies, and window *size* is only a
    proxy for it -- a 1 Mb window in a dense region can hold more records than
    a 10 Mb window in a sparse one. Reporting the realised record count lets the
    result be plotted against actual selectivity.
    """
    for window in windows:
        contig_positions = positions.get(window["chrom"], [])
        start, end = window["start"], window["end"]
        window["records_in_window"] = sum(1 for pos in contig_positions if start <= pos <= end)


# ---------------------------------------------------------------------------
# Index preparation (one-time cost, timed and reported separately)
# ---------------------------------------------------------------------------
def is_bgzf(path: Path) -> bool:
    """True when the file is BGZF, which is what an index needs.

    A plain gzip .vcf.gz is not seekable, and tabix refuses it. Detecting this
    up front turns a confusing downstream failure into one clear message.
    """
    try:
        with path.open("rb") as handle:
            header = handle.read(18)
    except OSError:
        return False
    if len(header) < 18 or header[:3] != b"\x1f\x8b\x08":
        return False
    # BGZF marks itself with an BC extra subfield in the gzip header.
    return header[12:14] == b"BC"


def prepare_indexed_vcf(
    vcf_path: Path, workdir: Path, *, index_kind: str = "auto"
) -> dict[str, Any]:
    """Produce a bgzipped, coordinate-indexed copy, timing each step.

    Returns the paths and the one-time seconds. This is the VCF side's
    equivalent of building an HDT or a QLever index, and it is reported the same
    way: separately from query time, so neither side's setup is smuggled into a
    per-question number.

    `index_kind`: "tbi" is the familiar tabix index but cannot address a contig
    longer than 2^29-1 bp; "csi" has no such limit. "auto" picks csi when any
    contig needs it, which is the only safe default for a whole-genome file.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"source": str(vcf_path), "indexKind": index_kind}

    target = workdir / (vcf_path.name if vcf_path.name.endswith(".gz") else vcf_path.name + ".gz")
    compress_seconds = 0.0
    if is_bgzf(vcf_path):
        # Already seekable: copy rather than recompress, and say so, because a
        # recompression would inflate the one-time cost with work a real user
        # would not repeat.
        started = time.monotonic()
        shutil.copy2(vcf_path, target)
        compress_seconds = time.monotonic() - started
        report["compression"] = "already-bgzf (copied)"
    else:
        started = time.monotonic()
        _bgzip(vcf_path, target)
        compress_seconds = time.monotonic() - started
        report["compression"] = "bgzip"
    report["bgzipSeconds"] = compress_seconds
    report["bgzipBytes"] = target.stat().st_size

    if index_kind == "auto":
        index_kind = "csi" if _needs_csi(target) else "tbi"
        report["indexKind"] = index_kind
        report["indexKindReason"] = (
            "a contig exceeds the 2^29-1 bp that a .tbi can address"
            if index_kind == "csi" else "every contig fits a .tbi"
        )

    report["sortSeconds"] = 0.0
    started = time.monotonic()
    try:
        index_path = _index(target, index_kind)
    except subprocess.CalledProcessError as error:
        if not _is_unsorted_error(error):
            raise
        # An index needs coordinate order, and a VCF is not required to be in
        # it. Sorting is what a user would have to do before seeking, so it is
        # done here and charged to the VCF side's one-time cost -- reported as
        # its own line rather than folded into the index time.
        sorted_target = target.with_name(target.name.replace(".vcf.gz", "") + ".sorted.vcf.gz")
        sort_started = time.monotonic()
        _sort(vcf_path, sorted_target, workdir)
        report["sortSeconds"] = time.monotonic() - sort_started
        report["sorted"] = "input was not coordinate-sorted; sorted with bcftools sort"
        target.unlink(missing_ok=True)
        target = sorted_target
        report["bgzipBytes"] = target.stat().st_size
        started = time.monotonic()
        index_path = _index(target, index_kind)
    report["indexSeconds"] = time.monotonic() - started
    report["indexedVcf"] = str(target)
    report["indexPath"] = str(index_path)
    report["indexBytes"] = index_path.stat().st_size
    report["totalSetupSeconds"] = (
        report["bgzipSeconds"] + report["sortSeconds"] + report["indexSeconds"]
    )
    return report


#: What tabix and bcftools print when records are not in coordinate order:
#: "Chromosome blocks not continuous" when a contig recurs after another one,
#: "unsorted positions" when POS goes backwards within a contig.
UNSORTED_MARKERS = ("not continuous", "unsorted")


def _is_unsorted_error(error: subprocess.CalledProcessError) -> bool:
    stderr = error.stderr or b""
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", "replace")
    return any(marker in stderr.lower() for marker in UNSORTED_MARKERS)


def _sort(source: Path, target: Path, workdir: Path) -> None:
    """Coordinate-sort into a BGZF file, the only way an unsorted VCF can be indexed."""
    if not shutil.which("bcftools"):
        raise RuntimeError(
            "the VCF is not coordinate-sorted, so it cannot be indexed, and bcftools "
            "is not available to sort it"
        )
    sort_tmp = workdir / "bcftools-sort-tmp"
    sort_tmp.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["bcftools", "sort", "-Oz", "-T", str(sort_tmp), "-o", str(target), str(source)],
        check=True, capture_output=True,
    )
    shutil.rmtree(sort_tmp, ignore_errors=True)


def _bgzip(source: Path, target: Path) -> None:
    """Compress with bgzip, falling back to bcftools when bgzip is absent.

    The image installs `tabix`, which provides bgzip. The bcftools fallback
    keeps the runner usable on an image built before that was added, and
    produces the same BGZF container.
    """
    if shutil.which("bgzip"):
        with target.open("wb") as handle:
            if _is_gzip(source):
                # A plain-gzip VCF has to be decompressed first. `bgzip -c` on
                # it compresses the gzip bytes a second time, and tabix then
                # fails to parse the result ("was wrong -p [type] used?"). The
                # benchmark's derived slices are plain gzip, so this is the
                # common case, not an edge one.
                with gzip.open(source, "rb") as src:
                    proc = subprocess.Popen(["bgzip", "-c"], stdin=subprocess.PIPE,
                                            stdout=handle)
                    assert proc.stdin is not None
                    try:
                        shutil.copyfileobj(src, proc.stdin, length=1024 * 1024)
                    finally:
                        proc.stdin.close()
                    if proc.wait() != 0:
                        raise subprocess.CalledProcessError(proc.returncode, ["bgzip", "-c"])
            else:
                subprocess.run(["bgzip", "-c", str(source)], stdout=handle, check=True)
        return
    if shutil.which("bcftools"):
        subprocess.run(
            ["bcftools", "view", "--no-version", "-Oz", "-o", str(target), str(source)],
            check=True, capture_output=True,
        )
        return
    raise RuntimeError("neither bgzip nor bcftools is available to produce a BGZF file")


def _is_gzip(path: Path) -> bool:
    """True for any gzip stream, BGZF or not (BGZF is checked by is_bgzf)."""
    try:
        with path.open("rb") as handle:
            return handle.read(2) == b"\x1f\x8b"
    except OSError:
        return False


def _needs_csi(bgzf_path: Path) -> bool:
    """True when any contig is longer than a .tbi can address."""
    limit = 2 ** 29 - 1
    try:
        header = V.read_vcf_header_text(bgzf_path)
    except (OSError, ValueError):
        return False
    for line in header.splitlines():
        if not line.startswith("##contig="):
            continue
        for field in line[len("##contig=<"):].rstrip(">").split(","):
            key, _, value = field.partition("=")
            if key.strip() == "length":
                try:
                    if int(value) > limit:
                        return True
                except ValueError:
                    continue
    return False


def _index(bgzf_path: Path, index_kind: str) -> Path:
    suffix = ".csi" if index_kind == "csi" else ".tbi"
    expected = Path(str(bgzf_path) + suffix)
    if shutil.which("tabix"):
        args = ["tabix", "-f", "-p", "vcf"]
        if index_kind == "csi":
            args.append("-C")
        subprocess.run([*args, str(bgzf_path)], check=True, capture_output=True)
    elif shutil.which("bcftools"):
        flag = "-c" if index_kind == "csi" else "-t"
        subprocess.run(["bcftools", "index", flag, "-f", str(bgzf_path)],
                       check=True, capture_output=True)
    else:
        raise RuntimeError("neither tabix nor bcftools is available to build an index")
    if not expected.is_file():
        raise RuntimeError(f"index was not produced at {expected}")
    return expected


# ---------------------------------------------------------------------------
# The VCF arms
# ---------------------------------------------------------------------------
# Every arm returns the same canonical structure for a given question, so the
# comparator is a plain equality test rather than a per-arm special case. The
# classification helpers are imported from validation_runner, not reimplemented,
# so a regional answer is classified character-for-character as the whole-file
# oracle classifies it.

def _empty_answer(query_id: str) -> Any:
    if query_id == "r01_region_record_count":
        return {"recordCount": 0}
    if query_id == "r03_region_titv":
        return {"biallelicSnvCount": 0, "transitionCount": 0, "transversionCount": 0}
    return []


class _Accumulator:
    """Per-question counters for one window."""

    def __init__(self, query_id: str, samples: list[str]):
        self.query_id = query_id
        self.samples = samples
        self.records = 0
        self.shapes: Counter[str] = Counter()
        self.filters: Counter[tuple[str, str]] = Counter()
        self.genotypes: Counter[tuple[str, str]] = Counter()
        self.biallelic_snvs = 0
        self.transitions = 0
        self.transversions = 0

    def add_variant(self, variant: Any) -> None:
        query_id = self.query_id
        if query_id == "r01_region_record_count":
            self.records += 1
            return

        alt = V.alt_lexical(variant)
        ref = str(variant.REF)

        if query_id == "r02_region_variant_shape_counts":
            self.shapes[V.classify_variant_shape(ref, alt)] += 1
            return

        if query_id == "r03_region_titv":
            ref_upper, alt_upper = ref.upper(), alt.upper()
            if (len(ref_upper) == 1 and len(alt_upper) == 1
                    and ref_upper in "ACGT" and alt_upper in "ACGT"
                    and ref_upper != alt_upper):
                self.biallelic_snvs += 1
                if {ref_upper, alt_upper} in ({"A", "G"}, {"C", "T"}):
                    self.transitions += 1
                else:
                    self.transversions += 1
            return

        if query_id == "r04_region_filter_distribution":
            raw = _filter_lexical(variant)
            self.filters[(V.filter_status(raw), raw)] += 1
            return

        if query_id == "r05_region_sample_genotype_counts":
            self.add_genotypes(variant)

    def add_genotypes(self, variant: Any) -> None:
        has_gt = "GT" in (variant.FORMAT or [])
        alleles: list[Any] = [None] * len(self.samples)
        if self.samples and has_gt:
            raw = list(variant.genotypes)
            if len(raw) != len(self.samples):
                raise ValueError(
                    f"sample/genotype length mismatch at {variant.CHROM}:{variant.POS}"
                )
            alleles = [V.genotype_alleles(entry) for entry in raw]
        for sample, call in zip(self.samples, alleles):
            self.genotypes[(sample, V.classify_genotype(call, has_gt=has_gt))] += 1

    def answer(self) -> Any:
        query_id = self.query_id
        if query_id == "r01_region_record_count":
            return {"recordCount": self.records}
        if query_id == "r02_region_variant_shape_counts":
            return [{"variantClass": name, "recordCount": int(count)}
                    for name, count in sorted(self.shapes.items())]
        if query_id == "r03_region_titv":
            return {"biallelicSnvCount": self.biallelic_snvs,
                    "transitionCount": self.transitions,
                    "transversionCount": self.transversions}
        if query_id == "r04_region_filter_distribution":
            return [{"filterStatus": status, "filterLexical": lexical,
                     "recordCount": int(count)}
                    for (status, lexical), count in sorted(self.filters.items())]
        return [{"sampleId": sample, "genotypeClass": genotype_class,
                 "callCount": int(count)}
                for (sample, genotype_class), count in sorted(self.genotypes.items())]


def _filter_lexical(variant: Any) -> str:
    """The FILTER column exactly as the file spells it.

    cyvcf2 reports PASS as None, so the mapping back to the lexical form has to
    be explicit; `.` means no filter was applied and is a different state again.
    """
    raw = variant.FILTER
    if raw is None:
        return "PASS"
    return str(raw)


def vcf_sample_names(vcf_path: Path) -> list[str]:
    header = V.read_vcf_header_text(vcf_path)
    for line in header.splitlines():
        if line.startswith("#CHROM"):
            fields = line.rstrip("\r\n").split("\t")
            return fields[9:] if len(fields) > 9 else []
    return []


def answer_cyvcf2(
    vcf_path: Path,
    query_id: str,
    windows: list[dict[str, Any]],
    samples: list[str],
    *,
    indexed: bool,
) -> dict[str, Any]:
    """Answer one question for one or more windows with cyvcf2.

    ``indexed=True`` uses the region iterator, which seeks through the index;
    ``indexed=False`` iterates the whole file and filters on POS, which is the
    status quo. Both apply the POS filter, for the reason in REGION_SEMANTICS.
    """
    if V.VCF is None:
        raise RuntimeError("cyvcf2 is unavailable; run this inside the VCF-RDFizer image")
    accumulators = {w["window_id"]: _Accumulator(query_id, samples) for w in windows}

    if indexed:
        reader = V.VCF(str(vcf_path))
        try:
            for window in windows:
                region = f"{window['chrom']}:{window['start']}-{window['end']}"
                accumulator = accumulators[window["window_id"]]
                for variant in reader(region):
                    # htslib returns overlap; the window means POS.
                    if window["start"] <= int(variant.POS) <= window["end"]:
                        accumulator.add_variant(variant)
        finally:
            reader.close()
    else:
        by_contig: dict[str, list[dict[str, Any]]] = {}
        for window in windows:
            by_contig.setdefault(window["chrom"], []).append(window)
        reader = V.VCF(str(vcf_path))
        try:
            for variant in reader:
                contig_windows = by_contig.get(str(variant.CHROM))
                if not contig_windows:
                    continue
                pos = int(variant.POS)
                for window in contig_windows:
                    if window["start"] <= pos <= window["end"]:
                        accumulators[window["window_id"]].add_variant(variant)
        finally:
            reader.close()

    return {window_id: acc.answer() for window_id, acc in accumulators.items()}


#: One bcftools format string per question: ask for the columns that question
#: needs and nothing else, so the arm is not penalised for fetching fields it
#: will discard.
#:
#: CHROM is in every one of them even though `-r` already restricts the contig.
#: Without it the folder would be trusting the region argument to have done its
#: job, and a record at the same coordinate on another contig would be counted
#: silently. One extra column makes the arm check itself.
BCFTOOLS_FORMATS = {
    "r01_region_record_count": "%CHROM\t%POS\n",
    "r02_region_variant_shape_counts": "%CHROM\t%POS\t%REF\t%ALT\n",
    "r03_region_titv": "%CHROM\t%POS\t%REF\t%ALT\n",
    "r04_region_filter_distribution": "%CHROM\t%POS\t%FILTER\n",
    "r05_region_sample_genotype_counts": "%CHROM\t%POS[\t%GT]\n",
}


def answer_bcftools(
    vcf_path: Path, query_id: str, window: dict[str, Any], samples: list[str]
) -> Any:
    """Answer one question for one window with `bcftools query -r`.

    This is the arm a bioinformatician would actually reach for, and the only
    one that reads the FILTER column's exact text without a Python parser in
    the way.
    """
    region = f"{window['chrom']}:{window['start']}-{window['end']}"
    completed = subprocess.run(
        ["bcftools", "query", "-r", region, "-f", BCFTOOLS_FORMATS[query_id], str(vcf_path)],
        check=True, capture_output=True, text=True,
    )
    return _fold_bcftools_output(completed.stdout, query_id, window, samples)


def _fold_bcftools_output(
    output: str, query_id: str, window: dict[str, Any], samples: list[str]
) -> Any:
    records = 0
    shapes: Counter[str] = Counter()
    filters: Counter[tuple[str, str]] = Counter()
    genotypes: Counter[tuple[str, str]] = Counter()
    biallelic = transitions = transversions = 0

    for line in output.splitlines():
        if not line:
            continue
        fields = line.split("\t")
        chrom, pos = fields[0], int(fields[1])
        # The contig is checked rather than assumed, and POS is reconciled the
        # same way the cyvcf2 arms reconcile it (see REGION_SEMANTICS).
        if chrom != str(window["chrom"]):
            continue
        if not (window["start"] <= pos <= window["end"]):
            continue

        if query_id == "r01_region_record_count":
            records += 1
        elif query_id == "r02_region_variant_shape_counts":
            shapes[V.classify_variant_shape(fields[2], fields[3])] += 1
        elif query_id == "r03_region_titv":
            ref, alt = fields[2].upper(), fields[3].upper()
            if (len(ref) == 1 and len(alt) == 1 and ref in "ACGT"
                    and alt in "ACGT" and ref != alt):
                biallelic += 1
                if {ref, alt} in ({"A", "G"}, {"C", "T"}):
                    transitions += 1
                else:
                    transversions += 1
        elif query_id == "r04_region_filter_distribution":
            raw = fields[2]
            filters[(V.filter_status(raw), raw)] += 1
        elif query_id == "r05_region_sample_genotype_counts":
            calls = fields[2:]
            for sample, raw_gt in zip(samples, calls):
                genotypes[(sample, _classify_bcftools_gt(raw_gt))] += 1

    if query_id == "r01_region_record_count":
        return {"recordCount": records}
    if query_id == "r02_region_variant_shape_counts":
        return [{"variantClass": name, "recordCount": int(count)}
                for name, count in sorted(shapes.items())]
    if query_id == "r03_region_titv":
        return {"biallelicSnvCount": biallelic, "transitionCount": transitions,
                "transversionCount": transversions}
    if query_id == "r04_region_filter_distribution":
        return [{"filterStatus": status, "filterLexical": lexical, "recordCount": int(count)}
                for (status, lexical), count in sorted(filters.items())]
    return [{"sampleId": sample, "genotypeClass": genotype_class, "callCount": int(count)}
            for (sample, genotype_class), count in sorted(genotypes.items())]


def _classify_bcftools_gt(raw_gt: str) -> str:
    """Map a `%GT` string onto the same classes cyvcf2's oracle produces.

    `bcftools query` hands back the genotype as text, so this reproduces
    `classify_genotype` from the lexical form rather than from allele indices.
    A record with no GT field prints `.` here, which is indistinguishable from
    a missing call -- so a file whose records lack GT entirely would make this
    arm disagree with the cyvcf2 arms, and the comparator would catch it.
    """
    normalized = raw_gt.replace("|", "/")
    if normalized in ("", "."):
        return "MISSING"
    if "." in normalized:
        return "MISSING"
    parts = normalized.split("/")
    if len(parts) == 1:
        return "HAPLOID_REF" if parts[0] == "0" else "HAPLOID_ALT"
    if len(parts) == 2:
        if parts[0] == parts[1]:
            return "HOM_REF" if parts[0] == "0" else "HOM_ALT"
        return "HET"
    return "OTHER_PLOIDY"


# ---------------------------------------------------------------------------
# The SPARQL arm
# ---------------------------------------------------------------------------
REGIONAL_QUERY_ROOT = SCRIPT_DIR / "queries" / "regional"


def regional_query_path(representation: str, query_id: str) -> Path:
    """Regional templates live beside the whole-file ones, same lookup order."""
    candidate = REGIONAL_QUERY_ROOT / representation / f"{query_id}.rq"
    if candidate.is_file():
        return candidate
    return REGIONAL_QUERY_ROOT / "common" / f"{query_id}.rq"


def render_query(template_path: Path, window: dict[str, Any]) -> str:
    """Substitute one window into a template.

    The contig name is substituted into a SPARQL string literal, so a name
    containing a quote or a backslash would change the query's meaning. VCF
    contig names cannot contain whitespace and in practice are alphanumeric,
    but this refuses rather than silently emitting a broken -- or differently
    scoped -- query.
    """
    chrom = str(window["chrom"])
    if any(character in chrom for character in '"\\\n\r\t'):
        raise ValueError(f"contig name is not safe to substitute into SPARQL: {chrom!r}")
    return (template_path.read_text(encoding="utf-8")
            .replace("{{CHROM}}", chrom)
            .replace("{{START}}", str(int(window["start"])))
            .replace("{{END}}", str(int(window["end"]))))


def normalize_regional(query_id: str, raw_path: Path) -> Any:
    """Canonicalise SPARQL Results JSON into the arms' shared shape.

    One deliberate canonicalisation: for the single-row aggregates, a field the
    engine leaves unbound is read as 0. An empty window makes `SUM` unbound on
    some engines and 0 on others, which is an engine difference rather than a
    disagreement about the data -- and without this, every empty window would
    be reported as a mismatch between engines that in fact agree.
    """
    fields, integer_fields, sort_fields = REGIONAL_QUERY_SCHEMAS[query_id]
    rows: list[dict[str, Any]] = []
    for number, binding in enumerate(V.bindings(raw_path), start=1):
        row: dict[str, Any] = {}
        for field in fields:
            entry = binding.get(field)
            if entry is None:
                if field in integer_fields and query_id in REGIONAL_SINGLE_ROW:
                    row[field] = 0
                    continue
                raise ValueError(f"row {number} of {query_id} has no {field!r}")
            lexical = entry["value"]
            row[field] = int(lexical) if field in integer_fields else str(lexical)
        rows.append(row)

    if query_id in REGIONAL_SINGLE_ROW:
        if not rows:
            return _empty_answer(query_id)
        if len(rows) != 1:
            raise ValueError(f"{query_id} must return one row, got {len(rows)}")
        return rows[0]

    if sort_fields:
        rows.sort(key=lambda row: tuple(row[field] for field in sort_fields))
    return rows


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------
def canonical(answer: Any) -> str:
    """A stable string for exact comparison between arms."""
    return json.dumps(answer, sort_keys=True, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def timed_vcf_arm(
    arm: str,
    vcf_path: Path,
    indexed_vcf: Path,
    query_id: str,
    window: dict[str, Any],
    samples: list[str],
) -> tuple[Any, float]:
    """Run one question, one window, one arm, and time exactly that."""
    started = time.monotonic()
    if arm == "bcftools-indexed":
        answer = answer_bcftools(indexed_vcf, query_id, window, samples)
    elif arm == "cyvcf2-indexed":
        answer = answer_cyvcf2(indexed_vcf, query_id, [window], samples, indexed=True)[window["window_id"]]
    elif arm == "cyvcf2-scan":
        answer = answer_cyvcf2(vcf_path, query_id, [window], samples, indexed=False)[window["window_id"]]
    else:
        raise ValueError(f"not a VCF arm: {arm}")
    return answer, time.monotonic() - started


def run(args: argparse.Namespace) -> int:
    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = results_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    arms = [arm.strip() for arm in args.arms.split(",") if arm.strip()]
    unknown = [a for a in arms if a not in VCF_ARMS and a not in V.SPARQL_ENGINES]
    if unknown:
        raise SystemExit(f"unknown arm(s): {', '.join(unknown)}")
    thin_arms = {arm.strip() for arm in args.thin_arms.split(",") if arm.strip()}
    unknown_thin = sorted(a for a in thin_arms if a not in VCF_ARMS and a not in V.SPARQL_ENGINES)
    if unknown_thin:
        raise SystemExit(f"unknown arm(s) in --thin-arms: {', '.join(unknown_thin)}")
    queries = [q.strip() for q in args.queries.split(",") if q.strip()]
    unknown_queries = [q for q in queries if q not in REGIONAL_QUERIES]
    if unknown_queries:
        raise SystemExit(f"unknown question(s): {', '.join(unknown_queries)}")

    samples = vcf_sample_names(args.vcf)
    sizes = tuple(int(s) for s in args.window_sizes.split(","))

    print(f"[setup] scanning {args.vcf.name} to place windows on real records")
    setup_started = time.monotonic()
    positions = contig_record_positions(args.vcf)
    position_scan_seconds = time.monotonic() - setup_started
    total_records = sum(len(v) for v in positions.values())
    print(f"[setup] {total_records:,} records across {len(positions)} contigs "
          f"in {position_scan_seconds:.1f}s")

    windows = draw_windows(positions, sizes, args.windows_per_size, args.seed)
    windows_expected_counts(positions, windows)
    V.write_json(results_dir / "windows.json", {
        "seed": args.seed,
        "sizes": list(sizes),
        "windowsPerSize": args.windows_per_size,
        "anchoring": "each window starts at a randomly chosen record position",
        "regionSemantics": REGION_SEMANTICS,
        "positionScanSeconds": position_scan_seconds,
        "totalRecords": total_records,
        "windows": windows,
    })

    setup: dict[str, Any] = {"positionScanSeconds": position_scan_seconds}

    needs_index = any(a in ("cyvcf2-indexed", "bcftools-indexed") for a in arms)
    indexed_vcf = args.vcf
    if needs_index:
        print("[setup] building the bgzip + index (one-time VCF-side cost)")
        index_report = prepare_indexed_vcf(
            args.vcf, args.scratch_dir / "indexed", index_kind=args.index_kind
        )
        indexed_vcf = Path(index_report["indexedVcf"])
        setup["vcfIndex"] = index_report
        print(f"[setup] bgzip {index_report['bgzipSeconds']:.1f}s + "
              f"{index_report['indexKind']} index {index_report['indexSeconds']:.1f}s")

    # Reference answers: one whole-file pass per question covering EVERY window
    # at once. This is the equality reference, and it is deliberately not timed
    # -- a pass that fills 80 windows is not what a one-question user pays.
    print("[reference] computing expected answers for every window")
    reference: dict[str, dict[str, Any]] = {}
    for query_id in queries:
        reference[query_id] = answer_cyvcf2(args.vcf, query_id, windows, samples, indexed=False)

    rows: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []

    def record(arm: str, query_id: str, window: dict[str, Any], replicate: int,
               answer: Any, seconds: float, status: str = "OK",
               error: str | None = None) -> None:
        expected = reference[query_id][window["window_id"]]
        agrees = canonical(answer) == canonical(expected) if status == "OK" else None
        if agrees is False:
            mismatches.append({
                "arm": arm, "query_id": query_id, "window_id": window["window_id"],
                "expected": expected, "observed": answer,
            })
        rows.append({
            "arm": arm,
            "query_id": query_id,
            "window_id": window["window_id"],
            "chrom": window["chrom"],
            "start": window["start"],
            "end": window["end"],
            "window_size": window["size"],
            "records_in_window": window["records_in_window"],
            "replicate": replicate,
            "wall_seconds": seconds,
            "status": status,
            "agrees_with_reference": agrees,
            "error": error,
        })

    # The scan arm's cost does not depend on the window -- it reads the whole
    # file whichever region is asked for. Timing it on all 80 windows would
    # measure one number 80 times. It is sampled instead, and the sample size is
    # reported so the thinness is visible. The same applies to any arm named in
    # --thin-arms: Comunica, HDT and COTTAS cost tens of seconds a question, so
    # timing them on every window would take a day and a half per scale.
    thin_windows = _sample_scan_windows(windows, args.scan_windows_per_size)

    for arm in [a for a in arms if a in VCF_ARMS]:
        targets = thin_windows if arm in thin_arms else windows
        print(f"[arm] {arm}: {len(targets)} windows x {len(queries)} questions "
              f"x {args.replicates} replicates")
        for query_id in queries:
            for window in targets:
                for replicate in range(1, args.replicates + 1):
                    try:
                        answer, seconds = timed_vcf_arm(
                            arm, args.vcf, indexed_vcf, query_id, window, samples)
                        record(arm, query_id, window, replicate, answer, seconds)
                    except (subprocess.CalledProcessError, OSError, ValueError) as error:
                        record(arm, query_id, window, replicate, None, float("nan"),
                               status="FAILED", error=str(error))

    engine_arms = [a for a in arms if a in V.SPARQL_ENGINES]
    if engine_arms and args.rdf is None:
        raise SystemExit("--rdf is required when a SPARQL engine is among --arms")

    with tempfile.TemporaryDirectory(prefix="regional-rdf-", dir=str(args.scratch_dir)) as rdf_scratch:
        ntriples = None
        if engine_arms:
            # The engines take N-Triples, exactly as in the validation stage: a
            # packaged artifact (.nt.gz, .nt.br, .hdt, .cottas) is materialized
            # once, here, and the engines are handed the plain file. Passing the
            # package straight through made qlever-index, Comunica and the COTTAS
            # builder all fail on an .nt.gz, which is the artifact the harness
            # reuses from 13_query_cost. The decode is one-time setup and is
            # reported as such.
            print(f"[setup] materializing {args.rdf_format} as N-Triples for the SPARQL arms")
            started = time.monotonic()
            ntriples, materialization = V.materialize_ntriples(
                args.rdf, args.rdf_format, Path(rdf_scratch),
                log_dir=raw_dir / "materialization",
            )
            materialization["wallSeconds"] = time.monotonic() - started
            materialization["ntriplesPath"] = str(ntriples)
            setup["rdfMaterialization"] = materialization

        for arm in engine_arms:
            targets = thin_windows if arm in thin_arms else windows
            _run_engine_arm(arm, args, ntriples, raw_dir, setup, targets, queries, record)

    _write_outputs(results_dir, args, rows, mismatches, setup, windows, queries, arms)

    failures = sum(1 for row in rows if row["status"] != "OK")
    disagreements = sum(1 for row in rows if row["agrees_with_reference"] is False)
    print(f"\n{len(rows)} timed executions, {failures} failed, "
          f"{disagreements} disagreed with the reference")
    if disagreements:
        print("Arms disagree, so no speed comparison from this run is reportable.")
        print(f"See {results_dir / 'mismatches.json'}")
        return 1
    return 0


def engine_options(args: argparse.Namespace) -> dict[str, Any]:
    """The option names the validation engines read, not the runner's own flags.

    The engines are validation_runner's, and they look up ``memory_gb``,
    ``port``, ``startup_timeout``, ``query_timeout`` and -- for HDT and COTTAS --
    ``artifact_path`` / ``artifact_format``, which lets them query the supplied
    artifact natively instead of rebuilding it from N-Triples. Passing the
    runner's flag names instead made every one of those settings a silent no-op.
    """
    return {
        "query_timeout": args.query_timeout,
        "memory_gb": args.qlever_memory_gb,
        "port": args.qlever_port,
        "startup_timeout": args.qlever_startup_timeout,
        "artifact_path": str(args.rdf),
        "artifact_format": args.rdf_format,
    }


def _run_engine_arm(
    arm: str,
    args: argparse.Namespace,
    ntriples: Path,
    raw_dir: Path,
    setup: dict[str, Any],
    windows: list[dict[str, Any]],
    queries: list[str],
    record,
) -> None:
    """Time one SPARQL engine on every window; record, never raise, its failures."""
    print(f"[arm] {arm}: preparing engine")
    engine_raw = raw_dir / arm
    engine_raw.mkdir(parents=True, exist_ok=True)
    try:
        engine = V.build_engine(arm, ntriples, raw_dir=engine_raw,
                                scratch=args.scratch_dir, options=engine_options(args))
    except (ValueError, RuntimeError) as error:
        setup.setdefault("engineErrors", {})[arm] = str(error)
        print(f"[arm] {arm}: unavailable ({error})")
        return

    try:
        with engine:
            setup.setdefault("engineSetupSeconds", {})[arm] = engine.setup_seconds
            print(f"[arm] {arm}: setup {engine.setup_seconds:.2f}s; "
                  f"{len(windows)} windows x {len(queries)} questions "
                  f"x {args.replicates} replicates")
            with tempfile.TemporaryDirectory(dir=str(args.scratch_dir)) as rendered_dir:
                rendered_root = Path(rendered_dir)
                for query_id in queries:
                    template = regional_query_path(args.representation, query_id)
                    for window in windows:
                        rendered = rendered_root / f"{query_id}__{window['window_id']}.rq"
                        rendered.write_text(render_query(template, window), encoding="utf-8")
                        for replicate in range(1, args.replicates + 1):
                            envelope = engine.execute(
                                f"{query_id}__{window['window_id']}__r{replicate}", rendered)
                            if envelope["status"] != "PASS":
                                record(arm, query_id, window, replicate, None,
                                       envelope["wallSeconds"], status="FAILED",
                                       error=envelope.get("error") or "engine execution failed")
                                continue
                            try:
                                answer = normalize_regional(
                                    query_id, Path(envelope["rawResult"]))
                            except (ValueError, KeyError, OSError) as error:
                                record(arm, query_id, window, replicate, None,
                                       envelope["wallSeconds"], status="UNREADABLE",
                                       error=str(error))
                                continue
                            record(arm, query_id, window, replicate, answer,
                                   envelope["wallSeconds"])
    except Exception as error:  # noqa: BLE001 - one engine must not end the run
        # Start-up is where engines fail, and they fail in their own ways: the
        # COTTAS builder raised KeyError on an .nt.gz, for instance. Whatever
        # the type, it is this engine's failure -- recorded against it, while
        # the other arms' results stand.
        setup.setdefault("engineErrors", {})[arm] = f"{type(error).__name__}: {error}"
        print(f"[arm] {arm}: failed ({type(error).__name__}: {error})")


def _sample_scan_windows(
    windows: list[dict[str, Any]], per_size: int
) -> list[dict[str, Any]]:
    """The first N windows of each size, in the order they were drawn."""
    seen: Counter[int] = Counter()
    sampled = []
    for window in windows:
        if seen[window["size"]] < per_size:
            sampled.append(window)
            seen[window["size"]] += 1
    return sampled


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
REGIONAL_CSV_HEADER = [
    "arm", "query_id", "window_id", "chrom", "start", "end",
    "window_size", "records_in_window", "replicate",
    "wall_seconds", "status", "agrees_with_reference", "error",
]


def _write_outputs(
    results_dir: Path,
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    mismatches: list[dict[str, Any]],
    setup: dict[str, Any],
    windows: list[dict[str, Any]],
    queries: list[str],
    arms: list[str],
) -> None:
    csv_path = results_dir / "regional.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REGIONAL_CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in REGIONAL_CSV_HEADER})

    # Medians per (arm, question, window size): replicate noise collapsed, but
    # the raw rows stay in the CSV so the collapse can be checked.
    summary: dict[str, Any] = {}
    grouped: dict[tuple[str, str, int], list[float]] = {}
    for row in rows:
        if row["status"] != "OK":
            continue
        grouped.setdefault(
            (row["arm"], row["query_id"], row["window_size"]), []
        ).append(row["wall_seconds"])
    for (arm, query_id, size), seconds in sorted(grouped.items()):
        summary.setdefault(arm, {}).setdefault(query_id, {})[str(size)] = {
            "medianSeconds": statistics.median(seconds),
            "minSeconds": min(seconds),
            "maxSeconds": max(seconds),
            "executions": len(seconds),
        }

    V.write_json(results_dir / "regional.json", {
        "datasetId": args.dataset_id,
        "vcf": str(args.vcf),
        "vcfSha256": V.sha256_file(args.vcf),
        "rdf": str(args.rdf) if args.rdf else None,
        "representation": args.representation,
        "arms": arms,
        "queries": queries,
        "replicates": args.replicates,
        "scanWindowsPerSize": args.scan_windows_per_size,
        "thinArms": [a for a in arms if a in {
            t.strip() for t in args.thin_arms.split(",") if t.strip()}],
        "windowCount": len(windows),
        "setup": setup,
        "regionSemantics": REGION_SEMANTICS,
        "summary": summary,
        "executions": len(rows),
        "failures": sum(1 for row in rows if row["status"] != "OK"),
        "disagreements": sum(1 for row in rows if row["agrees_with_reference"] is False),
    })

    V.write_json(results_dir / "mismatches.json", {
        "count": len(mismatches),
        "note": "Any entry here invalidates the speed comparison for that arm.",
        "mismatches": mismatches[:200],
    })

    print(f"\nwrote {csv_path}")
    print(f"wrote {results_dir / 'regional.json'}")
    print(f"wrote {results_dir / 'windows.json'}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--vcf", type=Path, required=True,
                        help="source VCF; the scan arm and the reference read this")
    parser.add_argument("--rdf", type=Path, default=None,
                        help="RDF artifact for the SPARQL arms (.nt/.nt.gz/.hdt/.cottas)")
    parser.add_argument("--rdf-format", default="nt",
                        help="format of --rdf, as validation_runner names it")
    parser.add_argument("--representation", choices=("expanded", "condensed"),
                        default="expanded")
    parser.add_argument("--arms", default=",".join(VCF_ARMS) + ",qlever",
                        help="comma-separated: " + ", ".join(VCF_ARMS + V.SPARQL_ENGINES))
    parser.add_argument("--queries", default=",".join(REGIONAL_QUERIES))
    parser.add_argument("--window-sizes",
                        default=",".join(str(s) for s in DEFAULT_WINDOW_SIZES))
    parser.add_argument("--windows-per-size", type=int, default=DEFAULT_WINDOWS_PER_SIZE)
    parser.add_argument("--scan-windows-per-size", type=int, default=3,
                        help="windows timed for each arm in --thin-arms "
                             "(default: 3)")
    parser.add_argument("--thin-arms", default="cyvcf2-scan",
                        help="arms timed on only --scan-windows-per-size windows "
                             "of each size; cyvcf2-scan by default, whose cost "
                             "does not depend on the window")
    parser.add_argument("--replicates", type=int, default=3)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--index-kind", choices=("auto", "tbi", "csi"), default="auto")
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--scratch-dir", type=Path, default=Path("/work"))
    parser.add_argument("--query-timeout", type=int, default=V.DEFAULT_QUERY_TIMEOUT)
    parser.add_argument("--qlever-memory-gb", type=int, default=4)
    parser.add_argument("--qlever-port", type=int, default=7019)
    parser.add_argument("--qlever-startup-timeout", type=int, default=900)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.scratch_dir.mkdir(parents=True, exist_ok=True)
    try:
        return run(args)
    except subprocess.CalledProcessError as error:
        # A tool the runner shells out to failed: say which one and what it
        # printed, instead of ending in a traceback.
        detail = error.stderr.decode("utf-8", "replace").strip() if isinstance(
            error.stderr, bytes) else (error.stderr or "").strip()
        print(f"error: {' '.join(map(str, error.cmd))} exited {error.returncode}"
              + (f": {detail.splitlines()[-1]}" if detail else ""), file=sys.stderr)
        return 2
    except (RuntimeError, ValueError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
