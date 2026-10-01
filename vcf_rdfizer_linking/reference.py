"""Digest-pinned reference acquisition: GFF3 interval lookup and sequence maps."""

from bisect import bisect_left
from collections import defaultdict
import gzip
import hashlib
from pathlib import Path
import re
import tempfile
from urllib.parse import unquote, urlsplit
from urllib.request import build_opener, Request, url2pathname

from .session import NoRedirects


#: Where linker reference downloads and cached HTTP responses live. It sits in
#: this module rather than in ``runner`` because the argument parser needs it as
#: a default, and ``runner`` imports rdflib -- which would make merely printing
#: ``--help`` require the data-linking dependency.
DEFAULT_CACHE = Path.home() / ".cache" / "vcf-rdfizer" / "linkers"


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check_assembly(reference, declared, override=None):
    """An explicit assembly can fill missing metadata, never override a mismatch."""
    aliases = {"hg19": "GRCh37", "hg38": "GRCh38"}

    def identify(value):
        value = value.strip().strip('"')
        if value == declared:
            return declared
        names = set(re.findall(r"(?<![A-Za-z0-9])(GRCh37|GRCh38|hg19|hg38)(?![A-Za-z0-9])", value))
        names = {aliases.get(n, n) for n in names}
        if len(names) > 1:
            raise ValueError(f"Ambiguous reference assembly: {value}")
        return next(iter(names), None)

    expected = aliases.get(declared, declared)
    observed = identify(reference)
    explicit = identify(override) if override else None
    if observed and aliases.get(observed, observed) != expected:
        raise ValueError(f"Assembly mismatch: input is {observed}; reference bundle is {declared}")
    if override and explicit != expected:
        raise ValueError(f"Assembly mismatch: --assembly {override}; reference bundle is {declared}")
    if not observed and not explicit:
        raise ValueError(f"Cannot establish assembly from {reference!r}; supply --assembly {declared} after checking the input")


def acquire_reference(reference, cache_dir, *, offline=False, dry_run=False, stats=None):
    """Hash the stored bytes (compressed bytes for .gz), including on cache hits."""
    parts = urlsplit(reference.url)
    target = cache_dir / "references" / reference.sha256
    if target.is_file():
        if digest_file(target) != reference.sha256:
            raise ValueError(f"Reference cache digest mismatch: {target}")
        if stats is not None:
            stats["cache_hits"] += 1
        return target
    if parts.scheme == "file":
        if parts.netloc not in {"", "localhost"}:
            raise ValueError("Reference file URL must be local")
        source = Path(url2pathname(parts.path))
        if digest_file(source) != reference.sha256:
            raise ValueError(f"Reference digest mismatch: {source}")
        # Local bundles already are offline references. No copy on dry-run.
        if dry_run:
            return source
    elif parts.scheme != "https":
        raise ValueError("References must use file: or https: URLs")
    elif offline or dry_run:
        raise ValueError(f"Reference cache miss (network disabled): {reference.sha256}")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            if parts.scheme == "file":
                input_handle = source.open("rb")
            else:
                if stats is not None:
                    stats["requests"] += 1
                input_handle = build_opener(NoRedirects()).open(Request(reference.url, headers={"User-Agent": "VCF-RDFizer/reference-fetch"}), timeout=60)
                if stats is not None:
                    stats["final_service_status"] = input_handle.code
            with input_handle:
                for chunk in iter(lambda: input_handle.read(1024 * 1024), b""):
                    output.write(chunk)
                    if parts.scheme != "file" and stats is not None:
                        stats["bytes_transferred"] += len(chunk)
            output.close()
            if digest_file(temporary) != reference.sha256:
                raise ValueError(f"Reference digest mismatch: {reference.url}")
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
    return target


class IntervalIndex:
    """Sorted starts plus prefix-max ends: handles nested/overlapping features.

    With `aliases` (a SequenceMap), GFF3 seqids and record contigs are both
    resolved to the sequence accession, so `17` and `chr17` meet; a name the
    map does not list is kept as it is.
    """

    def __init__(self, path, reference, aliases=None):
        self.canonical = aliases.canonical if aliases else (lambda name: name)
        features = defaultdict(list)
        with path.open("rb") as raw:
            compressed = raw.read(2) == b"\x1f\x8b"
        opener = gzip.open if compressed else open
        with opener(path, "rt", encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                if line.startswith("##FASTA"):
                    break
                if line.startswith("#") or not line.strip():
                    continue
                columns = line.rstrip("\r\n").split("\t")
                if len(columns) != 9:
                    raise ValueError(f"GFF3 line {number}: expected nine columns")
                if columns[2] != reference.feature_type:
                    continue
                try:
                    start, end = int(columns[3]), int(columns[4])
                except ValueError as exc:
                    raise ValueError(f"GFF3 line {number}: invalid coordinates") from exc
                if start < 1 or end < start:
                    raise ValueError(f"GFF3 line {number}: invalid interval")
                attributes = dict(item.split("=", 1) for item in columns[8].split(";") if "=" in item)
                identifier = unquote(attributes.get(reference.id_attribute, ""))
                if not identifier:
                    raise ValueError(f"GFF3 line {number}: missing {reference.id_attribute}")
                features[self.canonical(columns[0])].append((start, end, identifier))
        self.chromosomes = {}
        for chrom, rows in features.items():
            rows.sort()
            maximum, ends = 0, []
            for _, end, _ in rows:
                maximum = max(maximum, end)
                ends.append(maximum)
            self.chromosomes[chrom] = (rows, [r[0] for r in rows], ends)
        if not features:
            raise ValueError(f"GFF3 contains no {reference.feature_type!r} features")

    def overlaps(self, key):
        rows, starts, max_ends = self.chromosomes.get(self.canonical(key.chrom), ([], [], []))
        index = bisect_left(starts, key.end + 1) - 1
        while index >= 0 and max_ends[index] >= key.start:
            start, end, identifier = rows[index]
            if end >= key.start:
                yield identifier
            index -= 1


class SequenceMap:
    """Contig name -> (sequence accession, length), from a digest-pinned TSV.

    One row per sequence: ``accession<TAB>length<TAB>name[,name...]``, with
    ``#`` comments. Every name a VCF may use for the sequence is listed, so
    ``chr17`` and ``17`` resolve to the same accession and nothing is inferred
    from a naming convention.
    """

    def __init__(self, path):
        self.sequences = {}
        with path.open(encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                if line.startswith("#") or not line.strip():
                    continue
                columns = line.rstrip("\r\n").split("\t")
                if len(columns) != 3 or not columns[1].isdigit():
                    raise ValueError(f"Sequence map line {number}: expected accession, length, names")
                accession, length = columns[0], int(columns[1])
                if not re.fullmatch(r"[A-Za-z0-9_]+\.[0-9]+", accession):
                    raise ValueError(f"Sequence map line {number}: {accession!r} is not a versioned accession")
                for name in columns[2].split(","):
                    if not name or name in self.sequences:
                        raise ValueError(f"Sequence map line {number}: empty or repeated name {name!r}")
                    self.sequences[name] = (accession, length)
        if not self.sequences:
            raise ValueError("Sequence map lists no sequences")

    def resolve(self, chrom):
        """(accession, length) for a contig name, or None when it is not mapped."""
        return self.sequences.get(chrom)

    def canonical(self, chrom):
        """The accession a contig name denotes, or the name itself when unmapped."""
        return self.sequences.get(chrom, (chrom,))[0]


def load_reference_index(path, reference, aliases=None):
    """The lookup a reference bundle's format calls for; `aliases` is a SequenceMap or None."""
    return SequenceMap(path) if reference.format == "sequence-map" else IntervalIndex(path, reference, aliases)
