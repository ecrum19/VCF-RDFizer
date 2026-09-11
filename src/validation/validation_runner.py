#!/usr/bin/env python3
"""Validate a VCF-RDFizer RDF graph against its source VCF.

The validator computes six deterministic summaries from the VCF with cyvcf2
(and bcftools for exact FILTER strings), runs the equivalent SPARQL queries
against the graph, and compares canonical integer results exactly.

Two axes are independent, and both are recorded in every report:

**Artifact format** - ``.nt``, ``.nt.gz``, ``.nt.br``, ``.hdt``, ``.cottas``,
``.cottas.gz``, ``.cottas.br``. Anything that is not already plain N-Triples is
decoded into the container scratch directory first, so validating an ``.hdt``
proves it decodes to a graph that still satisfies every semantic check - a
strictly stronger statement than the triple-count round-trip that
``validate_compression.py`` performs during compression. A plain ``.nt`` source
is read in place and never copied. Nothing decoded is written beneath ``--out``.

**SPARQL engine** - ``comunica`` (default) queries the file with no setup but
holds the graph in memory; ``qlever`` builds an on-disk index and answers over
a container-local HTTP server, which is what makes cohort-scale graphs
queryable. Both answer identical queries, so the choice is never semantic.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

# cyvcf2 only exists inside the image. Importing it lazily keeps the pure
# normalization/comparison layer below importable on the host, which is what
# the mutation tests in test/test_validation_logic_unit.py exercise.
try:  # pragma: no cover - present in the container, absent on the host
    import cyvcf2
    from cyvcf2 import VCF
except ImportError:  # pragma: no cover
    cyvcf2 = None
    VCF = None


def eprint(*args: Any) -> None:
    """Print to stderr.

    This runner executes standalone inside the container, so it cannot borrow
    the identically named helper from ``vcf_rdfizer.py`` on the host. Both the
    oversized-graph advisory and the query-timeout abandon message call this;
    before it existed here, either one raised ``NameError`` and killed the run
    at the exact moment it was trying to warn the operator.
    """
    print(*args, file=sys.stderr, flush=True)


SCRIPT_DIR = Path(__file__).resolve().parent
QUERY_ROOT = SCRIPT_DIR / "queries"
CORE_QUERIES = (
    "q01_record_density_1mb",
    "q02_variant_shape_counts",
    "q03_titv",
    "q04_filter_distribution",
    "q05_sample_genotype_counts",
    "q06_ac_an_distribution",
    "q07_file_metadata",
    "q08_header_line_census",
    "q09_predicate_census",
    "q10_class_census",
    "q11_record_digest",
    "q12_info_value_digest",
    "q13_format_value_digest",
)
#: Which oracle phases a query would actually have paid for, had it been the
#: only question asked. The oracle is one pass that fills every query's
#: counters together, so there is no measured per-query slice of it; this is the
#: explicit statement of what a one-question cyvcf2 script would have cost.
#:
#: Every query pays to open the file, scan the records, and assemble its answer.
#: Only the sample-level queries pay for the per-sample genotype/AC-AN block,
#: which is the part whose cost scales with cohort size. Measured on the
#: shipped fixtures, the block is 15.5% of the scan at one sample and 73.5% at
#: 2,504 -- which is the whole reason per-query oracle numbers are worth having.
#:
#: Known coarseness: `assemblySeconds` is charged to every query equally, and on
#: a cohort file it is not small (149 s against a 181 s scan on the 2,504-sample
#: fixture) because it sorts the per-sample counters. Most of that work belongs
#: to the sample-level queries, so this attribution OVER-charges record-level
#: queries on multi-sample inputs -- i.e. it understates the gap it is measuring.
#: Splitting it would mean restructuring the post-loop assembly into per-query
#: builders; until then the bias is in the conservative direction and is stated
#: rather than hidden.
ORACLE_SAMPLE_LEVEL_QUERIES = frozenset({
    "q05_sample_genotype_counts",
    "q06_ac_an_distribution",
    "q13_format_value_digest",
})

#: Queries that must return exactly one row, normalized to a dict rather than
#: a list so the comparison reads as a field-by-field check.
SINGLE_ROW_QUERIES = frozenset({"q03_titv", "q07_file_metadata"})
PREFLIGHT_QUERIES = (
    "preflight_record_cardinality",
    "preflight_position_datatype",
    "preflight_missing_token_conformance",
    "preflight_representation_profile",
    "preflight_sample_gt_inventory",
    "preflight_blank_nodes",
    "preflight_empty_values",
    "preflight_distinct_triple_count",
)
#: Anomaly-style preflights return at most 100 example rows so a broken graph
#: cannot produce an unbounded report. That makes the sample useless as a
#: severity measure, so each one is paired with an aggregate that returns the
#: exact count. Both are reported: the count says how bad, the sample says how.
ANOMALY_PREFLIGHT_QUERIES = (
    "preflight_record_cardinality",
    "preflight_position_datatype",
    "preflight_missing_token_conformance",
    "preflight_representation_profile",
    "preflight_blank_nodes",
    "preflight_empty_values",
)
PREFLIGHT_COUNT_QUERIES = tuple(f"{name}_count" for name in ANOMALY_PREFLIGHT_QUERIES)
ANOMALY_SAMPLE_LIMIT = 100
TRANSITIONS = {("A", "G"), ("G", "A"), ("C", "T"), ("T", "C")}

QUERY_SPECS = {
    "q01_record_density_1mb": (("chrom", "windowIndex"), ("recordCount",)),
    "q02_variant_shape_counts": (("variantClass",), ("recordCount",)),
    "q03_titv": ((), ("biallelicSnvCount", "transitionCount", "transversionCount")),
    "q04_filter_distribution": (("filterStatus", "filterLexical"), ("recordCount",)),
    "q05_sample_genotype_counts": (("sampleId", "genotypeClass"), ("callCount",)),
    "q06_ac_an_distribution": (("an", "ac"), ("siteCount",)),
    "q07_file_metadata": ((), ("fileFormat", "referenceGenome", "sourceSoftware")),
    "q08_header_line_census": (("headerKey",), ("lineCount",)),
    "q09_predicate_census": (("predicate",), ("tripleCount",)),
    "q10_class_census": (("class",), ("resourceCount",)),
    "q11_record_digest": (("bucket",), ("recordCount",)),
    "q12_info_value_digest": (("bucket",), ("valueCount",)),
    "q13_format_value_digest": (("bucket",), ("valueCount",)),
}
QUERY_SCHEMAS = {
    "q01_record_density_1mb": (("chrom", "windowIndex", "recordCount"), {"windowIndex", "recordCount"}, ("chrom", "windowIndex")),
    "q02_variant_shape_counts": (("variantClass", "recordCount"), {"recordCount"}, ("variantClass",)),
    "q03_titv": (("biallelicSnvCount", "transitionCount", "transversionCount"), {"biallelicSnvCount", "transitionCount", "transversionCount"}, ()),
    "q04_filter_distribution": (("filterStatus", "filterLexical", "recordCount"), {"recordCount"}, ("filterStatus", "filterLexical")),
    "q05_sample_genotype_counts": (("sampleId", "genotypeClass", "callCount"), {"callCount"}, ("sampleId", "genotypeClass")),
    "q06_ac_an_distribution": (("an", "ac", "siteCount"), {"an", "ac", "siteCount"}, ("an", "ac")),
    "q07_file_metadata": (("fileFormat", "referenceGenome", "sourceSoftware"), set(), ()),
    "q08_header_line_census": (("headerKey", "lineCount"), {"lineCount"}, ("headerKey",)),
    "q09_predicate_census": (("predicate", "tripleCount"), {"tripleCount"}, ("predicate",)),
    "q10_class_census": (("class", "resourceCount"), {"resourceCount"}, ("class",)),
    "q11_record_digest": (("bucket", "recordCount"), {"recordCount"}, ("bucket",)),
    "q12_info_value_digest": (("bucket", "valueCount"), {"valueCount"}, ("bucket",)),
    "q13_format_value_digest": (("bucket", "valueCount"), {"valueCount"}, ("bucket",)),
}
#: Checks that are only meaningful for the shipped RML mapping: they assume its
#: predicate inventory and its IRI templates. A custom mapping changes both by
#: design, so the wrapper switches these to report-only in that case.
DEFAULT_MAPPING_QUERIES = (
    "q09_predicate_census",
    "q10_class_census",
    "q11_record_digest",
    "q12_info_value_digest",
    "q13_format_value_digest",
)
MAPPING_POLICIES = ("strict", "report-only")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class ValidationProgress:
    """Best-effort JSONL progress writer shared with the host progress UI.

    Validation can spend most of its time inside a single SPARQL query. A
    small event before and after each query gives the existing host-side
    ``ProgressSession`` a useful total without retaining query output or RDF
    data in memory. Failures to write the optional sidecar are deliberately
    ignored so observability can never change validation semantics.
    """

    def __init__(self, path: Path | None, total: int):
        self.path = path
        self.total = total
        if path is not None:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.unlink(missing_ok=True)
            except OSError:
                pass

    def emit(
        self,
        phase: str,
        *,
        completed: int | None = None,
        query_id: str | None = None,
        detail: str | None = None,
    ) -> None:
        if self.path is None:
            return
        payload: dict[str, Any] = {
            "stage": "validation",
            "phase": phase,
            "total": self.total,
            "unit": "queries",
        }
        if completed is not None:
            payload["completed"] = completed
        if query_id is not None:
            payload["query"] = query_id
        if detail is not None:
            payload["detail"] = detail
        try:
            with self.path.open("a", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"))
                handle.write("\n")
        except OSError:
            pass


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def tool_version(command: list[str], *, table_label: str | None = None) -> str | None:
    try:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            timeout=20,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    output = result.stdout.strip()
    if table_label:
        for line in output.splitlines():
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) >= 2 and cells[0] == table_label:
                return cells[1]
    return output.splitlines()[0] if output else None


def alt_lexical(variant: Any) -> str:
    return ",".join("." if item is None else str(item) for item in (variant.ALT or [None]))


def classify_variant_shape(ref_value: str, alt_value: str) -> str:
    ref, alt = str(ref_value).upper(), str(alt_value).upper()
    if alt == ".":
        return "NO_ALT"
    if "," in alt:
        return "MULTIALLELIC"
    if alt == "*" or "[" in alt or "]" in alt or (alt.startswith("<") and alt.endswith(">")):
        return "SYMBOLIC_OR_BREAKEND"
    if not re.fullmatch(r"[ACGTN]+", ref) or not re.fullmatch(r"[ACGTN]+", alt):
        return "OTHER"
    if len(ref) == len(alt) == 1:
        return "SNV"
    if len(ref) == len(alt):
        return "MNV_OR_EQUAL_LENGTH_SUBSTITUTION"
    return "INSERTION_SHAPE" if len(ref) < len(alt) else "DELETION_SHAPE"


def filter_status(value: str) -> str:
    return "PASS" if value == "PASS" else "NOT_APPLIED" if value == "." else "FAILED"


def exact_filter_lexical(variant: Any) -> str:
    values = list(variant.FILTERS or [])
    if values:
        return ";".join(str(value) for value in values)
    fields = str(variant).rstrip("\r\n").split("\t", 8)
    if len(fields) < 7:
        raise ValueError("Could not recover FILTER from serialized VCF record")
    return fields[6]


def filters_with_bcftools(vcf_path: Path) -> Counter[tuple[str, str]]:
    process = subprocess.run(
        ["bcftools", "query", "-f", "%FILTER\\n", str(vcf_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if process.returncode:
        raise RuntimeError(f"bcftools FILTER extraction failed: {process.stderr.strip()}")
    values = [line.rstrip("\r") for line in process.stdout.splitlines()]
    if any(value == "" for value in values):
        raise ValueError("bcftools returned an empty FILTER value")
    return Counter((filter_status(value), value) for value in values)


def genotype_alleles(raw_gt: Any) -> tuple[int | None, ...] | None:
    if raw_gt is None:
        return None
    return tuple(None if value is None or int(value) < 0 else int(value) for value in raw_gt[:-1])


def classify_genotype(alleles: tuple[int | None, ...] | None, *, has_gt: bool) -> str:
    if not has_gt:
        return "NO_GT_FIELD"
    if alleles is None or not alleles or any(value is None for value in alleles):
        return "MISSING"
    complete = tuple(int(value) for value in alleles if value is not None)
    if len(complete) == 1:
        return "HAPLOID_REF" if complete[0] == 0 else "HAPLOID_ALT"
    if len(complete) == 2:
        if complete[0] == complete[1]:
            return "HOM_REF" if complete[0] == 0 else "HOM_ALT"
        return "HET"
    return "OTHER_PLOIDY"


#: VCF meta-information keys the mapping lifts onto the VCFFile resource, in
#: the case-insensitive form `vcf_as_tsv.sh` matches them.
FILE_METADATA_KEYS = {
    "fileformat": "fileFormat",
    "reference": "referenceGenome",
    "source": "sourceSoftware",
    "filedate": "fileDate",
}
METADATA_ABSENT = "(absent)"


# ---------------------------------------------------------------------------
# Graph census expectations
# ---------------------------------------------------------------------------
# The census compares the graph's predicate and class inventory against counts
# derived from the VCF. It is the completeness check: a predicate missing, one
# with the wrong cardinality, and one that should not exist at all are all the
# same comparison.
#
# Expectations are derived from the VCF and the emitters' documented shapes -
# never from default_rules.ttl. Deriving them from the mapping would make the
# mapping test itself, which is exactly how QUAL stayed invisible for so long.
# A custom mapping therefore invalidates them, and the wrapper switches the
# policy to report-only in that case.

# The vocabulary terms, the VCF-version model and the lexical parsers are
# shared with the wrapper rather than mirrored here. They used to be duplicated,
# with unit tests asserting the copies stayed identical; one module removes the
# drift the tests were policing. `Dockerfile` copies it next to this runner, so
# it imports the same way inside the container and on the host.
try:
    import vcf_rdfizer_vocab as vocab
except ImportError:
    # In the container the module sits next to this file; in a source checkout
    # it is at the repository root. Try both before giving up, so the runner
    # works as a script in either layout.
    for candidate in (SCRIPT_DIR, SCRIPT_DIR.parent.parent):
        if (candidate / "vcf_rdfizer_vocab.py").is_file():
            sys.path.insert(0, str(candidate))
            break
    import vcf_rdfizer_vocab as vocab

VCFC = vocab.VCFC_NAMESPACE
RDF_TYPE = vocab.RDF_TYPE_URI


def _nonzero(counts: dict[str, int]) -> dict[str, int]:
    return {key: value for key, value in counts.items() if value}


rml_uri_component = vocab.rml_uri_component


#: Field separator for the record digest. U+001F cannot appear in a VCF field,
#: so no shift of a field boundary can make two different records collide. The
#: query in q11_record_digest.rq writes it as a SPARQL \\u001F escape.


DIGEST_SEPARATOR = "\u001f"
#: Hash prefix length in hex characters. Two gives 256 buckets: enough to make
#: an accidental collision of a *changed* record with its own bucket unlikely
#: to hide anything, while keeping the result fixed-size for any graph.
DIGEST_BUCKET_CHARS = 2


def record_digest_bucket(fields: list[str]) -> str:
    """Bucket one record exactly as q11_record_digest.rq does."""
    joined = DIGEST_SEPARATOR.join(fields)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:DIGEST_BUCKET_CHARS]


# ---------------------------------------------------------------------------
# Emitted-shape counters
# ---------------------------------------------------------------------------
#
# The census asserts an exact inventory: these predicates and classes, with
# these counts, and nothing else. So every resource family the emitters produce
# has to be counted here, from the VCF, without reading the graph or the
# mapping. These two functions are the single derivation: parse_vcf calls them
# with the columns it read, and the test fixture calls them with the columns it
# declares, so the container oracle and the fixture oracle cannot disagree.
#
# They mirror append_header_representation_rdf and append_record_detail_rdf.
# Where a decision depends on a shared table (which '##' keys are structured,
# which INFO keys carry an SV carrier, which Number tokens are positional), the
# table comes from vcf_rdfizer_vocab rather than being restated.


def _count_field_definition(counts: Counter, *, number: str, has_type: bool,
                            has_description: bool) -> None:
    """Count the ID/Number/Type/Description quartet of one field definition."""
    counts["fieldId"] += 1
    counts["fieldNumber"] += 1
    if vocab.arity_individual(number) is not None:
        counts["fieldArity"] += 1
    if vocab.number_as_integer(number) is not None:
        counts["fieldNumberInteger"] += 1
    if has_type:
        counts["fieldType"] += 1
    if has_description:
        counts["fieldDescription"] += 1


def emitted_header_counters(header_lines: list[tuple[str, str]]) -> dict[str, Any]:
    """Counters for everything append_header_representation_rdf emits.

    ``header_lines`` is the ordered ``(key, value)`` list of '##' lines, without
    the leading '##' and split on the first '='.
    """
    classes: Counter[str] = Counter()
    predicates: Counter[str] = Counter()
    contigs = 0
    declarations: set[str] = set()

    for key, value in header_lines:
        key_lower = key.lower()
        structured = vocab.is_structured_header_value(value)
        line_class = HEADER_LINE_CLASSES.get(key_lower)
        if line_class is None:
            # An unrecognized key is still one of the two VCF forms. An
            # unstructured line is only typed when it has a value, because
            # vcfc:UnstructuredHeaderLineShape requires one.
            if structured:
                line_class = "StructuredHeaderLine"
            elif value:
                line_class = "UnstructuredHeaderLine"
            else:
                continue
        classes[line_class] += 1

        if not structured:
            if key_lower == "assembly" and value:
                predicates["assemblyUrl"] += 1
            elif key_lower == "pedigreedb" and value:
                predicates["pedigreeDbUrl"] += 1
            continue

        attributes = parse_structured_header_attributes(value)
        classes["HeaderAttribute"] += len(attributes)
        for name in ("hasAttribute", "attributeKey", "attributeValue", "attributeIndex"):
            predicates[name] += len(attributes)

        fields = dict(attributes)
        identifier = (fields.get("ID") or "").strip()

        if key_lower in {"info", "format"}:
            if not identifier:
                continue
            # An absent Description is synthesized, so the property is always
            # present on an INFO or FORMAT declaration.
            _count_field_definition(
                predicates, number=fields.get("Number") or ".",
                has_type=True, has_description=True,
            )
            if key_lower == "info":
                for attribute, predicate in vocab.INFO_EXTRA_ATTRIBUTES.items():
                    if fields.get(attribute):
                        predicates[predicate] += 1
        elif key_lower == "filter":
            if identifier:
                predicates["filterId"] += 1
                predicates["fieldDescription"] += 1
        elif key_lower == "alt":
            if identifier:
                predicates["altId"] += 1
                predicates["fieldDescription"] += 1
        elif key_lower == "contig":
            if not identifier:
                continue
            contigs += 1
            predicates["contigId"] += 1
            for attribute, (predicate, _datatype) in vocab.CONTIG_ATTRIBUTES.items():
                if fields.get(attribute):
                    predicates[predicate] += 1
        elif key_lower == "meta":
            if not identifier:
                continue
            _count_field_definition(
                predicates, number=fields.get("Number") or ".",
                has_type=True, has_description=fields.get("Description") is not None,
            )
            predicates["metaAllowedValue"] += len(_meta_values(fields.get("Values")))
        elif key_lower == "sample":
            if identifier:
                predicates["declaresSample"] += 1
                if identifier not in declarations:
                    declarations.add(identifier)
                    classes["SampleDeclaration"] += 1
        elif key_lower == "pedigree":
            for attribute_key, attribute_value in attributes:
                if attribute_key == "ID" or not attribute_value:
                    continue
                predicates[
                    vocab.PEDIGREE_ROLE_PROPERTIES.get(attribute_key, "pedigreeAncestor")
                ] += 1
                predicates["ancestorRole"] += 1
                if attribute_value not in declarations:
                    declarations.add(attribute_value)
                    classes["SampleDeclaration"] += 1

    if contigs:
        predicates["contigCount"] += 1
    return {
        "emittedHeaderClasses": dict(classes),
        "emittedHeaderPredicates": dict(predicates),
    }


def _meta_values(raw: str | None) -> list[str]:
    """Split a ##META ``Values=[a, b, c]`` list, as the emitter does."""
    if not raw:
        return []
    inner = raw.strip()
    if inner.startswith("[") and inner.endswith("]"):
        inner = inner[1:-1]
    return [item.strip() for item in inner.split(",") if item.strip()]


def emitted_record_counters(
    rows: list[list[str]],
    samples: list[str],
    *,
    version: Any,
    contig_ids: set[str],
    alt_declaration_ids: set[str],
    has_assembly_line: bool = False,
    info_numbers: dict[str, str],
    format_numbers: dict[str, str],
) -> dict[str, Any]:
    """Counters for the allele, INFO, value-item, SV and genotype families.

    ``rows`` are the raw tab-split VCF data columns. Everything is derived from
    those and from the version, exactly as the emitters derive it.
    """
    classes: Counter[str] = Counter()
    predicates: Counter[str] = Counter()

    assembly_contig_ids: set[str] = set()
    reference_alleles = alt_alleles = 0
    value_items = value_item_alleles = tuple_items = 0
    genotypes = genotype_calls = called_alleles = 0

    for row in rows:
        chrom = row[0] if len(row) > 0 else ""
        ref = row[3] if len(row) > 3 else ""
        alt = row[4] if len(row) > 4 else ""
        info = row[7] if len(row) > 7 else ""

        # A bracketed CHROM names a breakpoint-assembly contig instead of a
        # declared reference sequence; the two are mutually exclusive.
        assembly_id = vocab.parse_bracketed_chrom(chrom)
        if assembly_id is not None:
            predicates["chromAssemblyContig"] += 1
            if assembly_id not in assembly_contig_ids:
                assembly_contig_ids.add(assembly_id)
                classes["AssemblyContig"] += 1
                predicates["assemblyContigId"] += 1
                if has_assembly_line:
                    predicates["declaredInAssembly"] += 1
        elif chrom in contig_ids:
            predicates["chromosome"] += 1

        alleles = vocab.parse_alt_alleles(ref, alt)
        allele_uris = {allele.index for allele in alleles}
        alt_count = sum(1 for allele in alleles if allele.index >= 1)
        for allele in alleles:
            if allele.index == 0:
                reference_alleles += 1
            else:
                alt_alleles += 1
            if allele.symbolic_id and allele.symbolic_id in alt_declaration_ids:
                predicates["declaredByAlt"] += 1
            if allele.symbolic_type:
                predicates["svType"] += 1
            if allele.breakend is not None:
                classes["Breakend"] += 1
                if allele.breakend.orientation is not None:
                    predicates["breakendOrientation"] += 1
                if allele.breakend.replacement:
                    predicates["breakendReplacementString"] += 1
                if allele.breakend.is_single:
                    predicates["isSingleBreakend"] += 1

        entries = parse_info_entries(info)
        for key, value in entries:
            if value is None:
                continue
            number = info_numbers.get(key, ".")
            if not (allele_uris and version.is_positional(key, number)):
                continue
            items = vocab.split_value_items(value)
            value_items += len(items)
            arity = version.tuple_arity(key)
            if arity is not None:
                tuple_items += len(items)
            for index in range(len(items)):
                link = version.value_item_link(key, number, index)
                if link.allele_index is not None and link.allele_index in allele_uris:
                    value_item_alleles += 1
                elif link.genotype_index is not None:
                    predicates["forGenotypeIndex"] += 1
                elif link.gt_allele_index is not None:
                    predicates["forGTAlleleIndex"] += 1

        if samples:
            format_keys = (row[8].split(":") if len(row) > 8 and row[8] else [])
            payloads = row[9 : 9 + len(samples)]
            if "GT" in format_keys:
                gt_index = format_keys.index("GT")
                for payload in payloads:
                    fields = payload.split(":") if payload else []
                    raw_gt = fields[gt_index] if gt_index < len(fields) else ""
                    parsed = vocab.parse_genotype(raw_gt)
                    if parsed is None:
                        continue
                    genotypes += 1
                    genotype_calls += len(parsed.calls)
                    called_alleles += sum(
                        1 for call in parsed.calls
                        if call.allele_index is not None and call.allele_index <= alt_count
                    )

    if reference_alleles:
        classes["ReferenceAllele"] = reference_alleles
        predicates["hasReferenceAllele"] = reference_alleles
    if alt_alleles:
        classes["AltAllele"] = alt_alleles
        predicates["hasAltAllele"] = alt_alleles
    total_alleles = reference_alleles + alt_alleles
    for name in ("alleleIndex", "alleleValue", "alleleKind"):
        predicates[name] += total_alleles

    if value_items:
        classes["FieldValueItem"] += value_items
        for name in ("hasValueItem", "valueIndex", "itemValue"):
            predicates[name] += value_items
        predicates["forAllele"] += value_item_alleles
        predicates["tupleArity"] += tuple_items

    # The parsed genotype layer is expanded-only, so it is reported separately
    # and merged by expected_census for that profile alone. Keeping parse_vcf
    # representation-independent is what lets one parse serve both censuses.
    genotype_classes: Counter[str] = Counter()
    genotype_predicates: Counter[str] = Counter()
    if genotypes:
        genotype_classes["Genotype"] += genotypes
        genotype_classes["GenotypeAlleleCall"] += genotype_calls
        for name in ("hasGenotype", "genotypeString", "ploidy", "phasingStatus"):
            genotype_predicates[name] += genotypes
        for name in ("hasAlleleCall", "callIndex", "isNoCall"):
            genotype_predicates[name] += genotype_calls
        genotype_predicates["calledAllele"] += called_alleles

    return {
        "emittedRecordClasses": dict(classes),
        "emittedRecordPredicates": dict(predicates),
        "emittedGenotypeClasses": dict(genotype_classes),
        "emittedGenotypePredicates": dict(genotype_predicates),
        "alleleCount": total_alleles,
        "valueItemCount": value_items,
        "genotypeCount": genotypes,
        "genotypeAlleleCallCount": genotype_calls,
    }


def synthesized_definition_numbers(
    rows: list[list[str]], *, declared_info: set[str], declared_format: set[str]
) -> dict[str, list[str]]:
    """The Number token of every definition the value emitters have to invent.

    A key declared by a '##INFO' or '##FORMAT' line is owned by the header
    emitter, which emits its ID/Number/Type/Description at the header line's
    own IRI. Only an undeclared key gets a definition invented at value time,
    with Number "0" for a Flag and "." otherwise.
    """
    info: list[str] = []
    format_keys: list[str] = []
    seen_info: set[str] = set()
    seen_format: set[str] = set()
    for row in rows:
        for key, value in parse_info_entries(row[7] if len(row) > 7 else ""):
            if key in declared_info or key in seen_info:
                continue
            seen_info.add(key)
            info.append("0" if value is None else ".")
        declared_keys = row[8].split(":") if len(row) > 8 and row[8] else []
        payloads = [p.split(":") if p else [] for p in row[9:]]
        width = max([len(declared_keys), *(len(p) for p in payloads)], default=0)
        for index in range(width):
            key = (declared_keys[index]
                   if index < len(declared_keys) and declared_keys[index]
                   else f"FIELD_{index + 1}")
            if key in declared_format or key in seen_format:
                continue
            seen_format.add(key)
            format_keys.append(".")
    return {"synthesizedInfoNumbers": info, "synthesizedFormatNumbers": format_keys}


def _count_definitions(predicates: dict[str, int], numbers: list[str]) -> None:
    """Count the field* triples of each synthesized field definition.

    A synthesized definition always carries ID, Number, Type and Description,
    plus the arity individual or the fixed count its Number token implies.
    """
    for number in numbers:
        for name in ("fieldId", "fieldNumber", "fieldType", "fieldDescription"):
            predicates[f"{VCFC}{name}"] = predicates.get(f"{VCFC}{name}", 0) + 1
        if vocab.arity_individual(number) is not None:
            predicates[f"{VCFC}fieldArity"] = predicates.get(f"{VCFC}fieldArity", 0) + 1
        if vocab.number_as_integer(number) is not None:
            predicates[f"{VCFC}fieldNumberInteger"] = (
                predicates.get(f"{VCFC}fieldNumberInteger", 0) + 1
            )


def expected_census(
    parser: dict[str, Any], representation: str, *, info_representation: str = "structured",
    header_representation: str = "structured",
) -> dict[str, list[dict[str, Any]]]:
    """Predicate and class counts the graph must contain, and nothing else."""
    records = parser["totalRecords"]
    samples = parser["sampleCount"]
    header_lines = parser["headerLineCount"]

    classes: dict[str, int] = {
        f"{VCFC}VCFFile": 1,
        f"{VCFC}VCFHeader": 1,
        f"{VCFC}ColumnHeaderLine": 1,
        f"{VCFC}HeaderLine": header_lines,
        f"{VCFC}VCFRecord": records,
        f"{VCFC}VariantCall": records,
    }
    # The mapping's version sentinel resolves to the subclass for the version
    # the file declares, or back to vcfc:VCFFile when the version has no
    # conformance overlay -- in which case it adds no new class.
    version = vocab.parse_vcf_version(parser.get("fileFormat"))
    if version is not None:
        classes[f"{VCFC}{version.file_class}"] = 1
    predicates: dict[str, int] = {
        f"{VCFC}hasHeader": 1,
        f"{VCFC}hasColumnHeader": 1,
        f"{VCFC}hasHeaderLine": header_lines,
        f"{VCFC}headerKey": header_lines,
        f"{VCFC}headerValue": parser["headerValueCount"],
        f"{VCFC}lineIndex": header_lines,
        f"{VCFC}hasRecord": records,
        f"{VCFC}chrom": records,
        f"{VCFC}pos": records,
        f"{VCFC}ref": records,
        f"{VCFC}recordIndex": records,
        f"{VCFC}hasCall": records,
        # ID, ALT, QUAL, FILTER and INFO moved out of the mapping and into the
        # wrapper's emitter, because each may be the VCF missing token and RML
        # cannot type an object per row. They are still one per record.
        f"{VCFC}recordId": records,
        f"{VCFC}alt": records,
        f"{VCFC}qual": records,
        f"{VCFC}filter": records,
        f"{VCFC}infoRaw": records,
        f"{VCFC}formatRaw": parser["recordsWithFormatColumn"],
    }
    # RMLStreamer emits nothing for an absent value, so an undeclared
    # meta-information line contributes no triple rather than an empty one.
    for field, predicate in (
        ("fileFormat", f"{VCFC}fileFormat"),
        ("referenceGenome", f"{VCFC}referenceGenome"),
        ("sourceSoftware", f"{VCFC}sourceSoftware"),
    ):
        predicates[predicate] = 0 if parser[field] == METADATA_ABSENT else 1

    # A sites-only VCF still declares a profile: vcfc:RepresentationProfileShape
    # requires exactly one on every file, and a file with no genotype columns
    # has no genotype data to condense.
    predicates[f"{VCFC}representationProfile"] = 1
    if samples:
        definitions = parser["distinctFormatKeyCount"]
        # The reusable sample set is file-scoped, not profile-scoped: both
        # profiles emit it, and the #CHROM line links to its members.
        classes[f"{VCFC}SampleSet"] = 1
        classes[f"{VCFC}VCFSample"] = samples
        classes[f"{VCFC}FormatFieldDefinition"] = definitions
        predicates[f"{VCFC}hasSampleSet"] = 1
        predicates[f"{VCFC}hasSample"] = samples
        predicates[f"{VCFC}sampleName"] = samples
        predicates[f"{VCFC}sampleIndex"] = samples
        predicates[f"{VCFC}hasGenotypeColumns"] = samples
        # A FORMAT key declared by a '##FORMAT' line gets its
        # ID/Number/Type/Description from the header emitter, at the same IRI.
        # Only a key the header never declared needs an invented definition
        # here, so only those are counted.
        _count_definitions(predicates, parser.get("synthesizedFormatNumbers", []))

        if representation == "expanded":
            for class_name, count in parser.get("emittedGenotypeClasses", {}).items():
                classes[f"{VCFC}{class_name}"] = classes.get(f"{VCFC}{class_name}", 0) + count
            for name, count in parser.get("emittedGenotypePredicates", {}).items():
                predicates[f"{VCFC}{name}"] = predicates.get(f"{VCFC}{name}", 0) + count
            classes[f"{VCFC}SampleCall"] = records * samples
            classes[f"{VCFC}FormatFieldValue"] = parser["formatValueSlots"]
            predicates[f"{VCFC}hasSampleCall"] = records * samples
            predicates[f"{VCFC}sampleId"] = records * samples
            predicates[f"{VCFC}forSample"] = records * samples
            predicates[f"{VCFC}hasFormatValue"] = parser["formatValueSlots"]
            predicates[f"{VCFC}fieldValue"] = parser["nonEmptyFormatValues"]
            # Every expanded FORMAT value now cites its declaration.
            predicates[f"{VCFC}declaredBy"] = (
                predicates.get(f"{VCFC}declaredBy", 0) + parser["formatValueSlots"]
            )
        else:
            classes[f"{VCFC}CohortCallMatrix"] = parser["recordsWithFormatKeys"]
            classes[f"{VCFC}FormatValueVector"] = parser["formatKeyOccurrences"]
            predicates[f"{VCFC}hasCallMatrix"] = parser["recordsWithFormatKeys"]
            predicates[f"{VCFC}appliesToSampleSet"] = parser["recordsWithFormatKeys"]
            predicates[f"{VCFC}hasFormatValueVector"] = parser["formatKeyOccurrences"]
            predicates[f"{VCFC}valueEncoding"] = parser["formatKeyOccurrences"]
            predicates[f"{VCFC}encodedValues"] = parser["formatKeyOccurrences"]
            predicates[f"{VCFC}declaredBy"] = (
                predicates.get(f"{VCFC}declaredBy", 0) + parser["formatKeyOccurrences"]
            )

    predicates[f"{VCFC}fileDate"] = 0 if parser["fileDate"] == METADATA_ABSENT else 1

    if header_representation == "structured":
        # Every structured-header family is counted once, by
        # emitted_header_counters, from the same '##' lines the emitter reads.
        # vcfc:FilterDefinition and vcfc:AltDefinition are no longer asserted:
        # vcfc:FILTERHeaderLine and vcfc:ALTHeaderLine are subclasses of them in
        # the vocabulary, so RDFS supplies the type and an explicit triple would
        # be redundant.
        for class_name, count in parser["emittedHeaderClasses"].items():
            classes[f"{VCFC}{class_name}"] = classes.get(f"{VCFC}{class_name}", 0) + count
        for name, count in parser["emittedHeaderPredicates"].items():
            predicates[f"{VCFC}{name}"] = predicates.get(f"{VCFC}{name}", 0) + count

    if info_representation == "structured":
        values = parser["infoValueCount"]
        definitions = parser["infoDefinitionCount"]
        classes[f"{VCFC}InfoFieldValue"] = values
        classes[f"{VCFC}InfoFieldDefinition"] = definitions
        predicates[f"{VCFC}hasInfoValue"] = values
        predicates[f"{VCFC}fieldValueBoolean"] = parser["infoFlagCount"]
        predicates[f"{VCFC}fieldValueInteger"] = parser["infoTypedIntegerCount"]
        predicates[f"{VCFC}fieldValueDecimal"] = parser["infoTypedDecimalCount"]
        # declaredBy and the field* descriptors are shared with the FORMAT
        # definitions, so accumulate rather than overwrite.
        predicates[f"{VCFC}declaredBy"] = predicates.get(f"{VCFC}declaredBy", 0) + values
        # As above: only an INFO key with no '##INFO' declaration needs its
        # definition invented, and only those contribute field* triples here.
        _count_definitions(predicates, parser.get("synthesizedInfoNumbers", []))
        predicates[f"{VCFC}fieldValue"] = (
            predicates.get(f"{VCFC}fieldValue", 0) + values - parser["infoFlagCount"]
        )

        # The allele layer, the value items, the SV carriers and the parsed
        # genotype layer travel with the structured INFO representation.
        for class_name, count in parser["emittedRecordClasses"].items():
            classes[f"{VCFC}{class_name}"] = classes.get(f"{VCFC}{class_name}", 0) + count
        for name, count in parser["emittedRecordPredicates"].items():
            predicates[f"{VCFC}{name}"] = predicates.get(f"{VCFC}{name}", 0) + count

    classes = _nonzero(classes)
    predicates = _nonzero(predicates)
    # Every typed resource contributes exactly one rdf:type triple here.
    predicates[RDF_TYPE] = sum(classes.values())

    return {
        "q09_predicate_census": [
            {"predicate": iri, "tripleCount": count}
            for iri, count in sorted(predicates.items())
        ],
        "q10_class_census": [
            {"class": iri, "resourceCount": count} for iri, count in sorted(classes.items())
        ],
    }


def sample_uri_ids(sample_ids: list[str]) -> list[str]:
    """Derive each sample's IRI component the way the emitters do.

    Mirrors ``_sample_id_to_uri_id`` in ``vcf_rdfizer.py``, including the
    occurrence suffix that keeps two samples whose ids sanitize to the same
    string distinguishable.
    """
    counts: dict[str, int] = {}
    out: list[str] = []
    for index, sample_id in enumerate(sample_ids, start=1):
        base = re.sub(r"[^A-Za-z0-9._~-]+", "_", sample_id).strip("_") or f"sample_{index}"
        counts[base] = counts.get(base, 0) + 1
        out.append(f"{base}_{counts[base]}" if counts[base] > 1 else base)
    return out


# Shared with the wrapper through vcf_rdfizer_vocab, so the oracle and the
# emitter cannot disagree about what a '##' line means or how it parses.
HEADER_LINE_CLASSES = vocab.HEADER_LINE_CLASSES
STRUCTURED_HEADER_KEYS = vocab.STRUCTURED_HEADER_KEYS
parse_structured_header_fields = vocab.parse_structured_header_fields
parse_structured_header_attributes = vocab.parse_structured_header_attributes
parse_info_entries = vocab.parse_info_entries


def info_declared_types(raw_header: str) -> dict[str, str]:
    """Map each declared INFO key to its VCF Type, for typed-value counting."""
    types: dict[str, str] = {}
    for line in raw_header.splitlines():
        if not line.startswith("##INFO="):
            continue
        body = line[len("##INFO=") :].strip()
        if body.startswith("<") and body.endswith(">"):
            body = body[1:-1]
        fields: dict[str, str] = {}
        for match in re.finditer(r'(\w+)=("(?:[^"\\]|\\.)*"|[^,]*)', body):
            value = match.group(2)
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            fields[match.group(1)] = value
        key = fields.get("ID", "").strip()
        if key:
            types.setdefault(key, fields.get("Type") or "String")
    return types


def read_vcf_header_text(vcf_path: Path) -> str:
    """Read the header block straight from the VCF file.

    Returns the leading ``#`` lines verbatim - the ``##`` meta-information
    lines and the ``#CHROM`` column line - which is the same span cyvcf2's
    ``raw_header`` covers, so callers of either see the same shape.

    Deliberately not cyvcf2's ``raw_header``: htslib normalises the header it
    exposes, injecting declarations the file does not contain - notably
    ``##FILTER=<ID=PASS,Description="All filters passed">``. The conversion
    reads the file's own text, so the oracle must too, or it expects header
    resources the graph could never contain.
    """
    opener = gzip.open if vcf_path.name.endswith(".gz") else open
    lines: list[str] = []
    with opener(vcf_path, "rt", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.startswith("#"):
                break
            lines.append(line.rstrip("\n"))
    return "\n".join(lines)


def parse_header_metadata(raw_header: str) -> dict[str, Any]:
    """Summarise the '##' meta-information block of a VCF.

    The `#CHROM` line is deliberately excluded: it is not a meta-information
    line and the mapping emits no HeaderLine resource for it, so counting it
    here would guarantee a spurious mismatch.
    """
    metadata: dict[str, Any] = {value: METADATA_ABSENT for value in FILE_METADATA_KEYS.values()}
    keys: Counter[str] = Counter()
    for line in raw_header.splitlines():
        if not line.startswith("##"):
            continue
        body = line[2:]
        key, _, value = body.partition("=")
        keys[key] += 1
        field = FILE_METADATA_KEYS.get(key.lower())
        # A repeated declaration keeps the first, matching the awk parser.
        if field is not None and metadata[field] == METADATA_ABSENT:
            metadata[field] = value
    # Header-representation counters, derived by the one shared implementation
    # the emitter's shape is described by.
    header_lines: list[tuple[str, str]] = []
    contig_ids: set[str] = set()
    alt_ids: set[str] = set()
    info_numbers: dict[str, str] = {}
    format_numbers: dict[str, str] = {}
    for line in raw_header.splitlines():
        if not line.startswith("##"):
            continue
        key, _, value = line[2:].partition("=")
        header_lines.append((key, value))
        lowered = key.lower()
        if lowered not in {"contig", "alt", "info", "format"}:
            continue
        fields = parse_structured_header_fields(value)
        identifier = (fields.get("ID") or "").strip()
        if not identifier:
            continue
        if lowered == "contig":
            contig_ids.add(identifier)
        elif lowered == "alt":
            alt_ids.add(identifier)
        elif lowered == "info":
            info_numbers.setdefault(identifier, fields.get("Number") or ".")
        else:
            format_numbers.setdefault(identifier, fields.get("Number") or ".")

    metadata.update(emitted_header_counters(header_lines))
    metadata["headerLines"] = header_lines
    metadata["contigIds"] = sorted(contig_ids)
    metadata["hasAssemblyLine"] = any(
        key.lower() == "assembly" and value for key, value in header_lines
    )
    metadata["altDeclarationIds"] = sorted(alt_ids)
    metadata["infoKeyNumbers"] = info_numbers
    metadata["formatKeyNumbers"] = format_numbers
    metadata["contigCount"] = len(contig_ids)
    metadata["headerLineCount"] = sum(keys.values())
    metadata["headerValueCount"] = sum(
        1 for line in raw_header.splitlines()
        if line.startswith("##") and line[2:].partition("=")[2] != ""
    )
    metadata["q08_header_line_census"] = [
        {"headerKey": key, "lineCount": int(count)} for key, count in sorted(keys.items())
    ]
    return metadata


def attach_census_expectations(
    parser: dict[str, Any], representation: str, *, info_representation: str = "structured",
    header_representation: str = "structured",
) -> dict[str, Any]:
    """Add the expected predicate/class inventory to a parser summary.

    Kept separate from ``parse_vcf`` because the expectation depends on the
    representation, which is a validation-run choice rather than a property of
    the VCF.
    """
    parser.update(expected_census(
        parser, representation, info_representation=info_representation,
        header_representation=header_representation,
    ))
    key = (
        "_expandedFormatValueDigest" if representation == "expanded"
        else "_condensedFormatValueDigest"
    )
    parser["q13_format_value_digest"] = parser.get(key, [])
    return parser


def parse_vcf(
    vcf_path: Path, *, filter_oracle: str, timing: dict[str, float] | None = None
) -> dict[str, Any]:
    """Compute every query's expected value in ONE pass over the VCF.

    ``timing``, when supplied, is filled with the phase breakdown below. It is
    opt-in because the sample-block measurement costs two clock reads per
    record, which is negligible next to a real parse but pointless when nobody
    is going to read it.

        readerOpenSeconds     open the file and parse the header
        scanSeconds           the record loop, in total
        sampleBlockSeconds    the per-sample genotype/AC-AN portion INSIDE the
                              scan -- the only part of the pass whose cost
                              depends on which queries you want
        assemblySeconds       building the per-query expected structures after
                              the loop

    Why phases and not per-query totals: this is a single pass that accumulates
    every query's counters together, so there is no per-query slice of it to
    measure. Attributing the shared scan to individual queries is a decision
    about what a one-question script would have paid, not a measurement, so it
    is made explicitly in the analysis (see ORACLE_QUERY_PHASES) rather than
    silently here.

    Gating the pass per query was considered and rejected: the accumulators in
    the sample block feed the expected values, so a gate left on in a
    correctness run would corrupt the oracle every equality claim rests on.
    """
    if VCF is None:
        raise RuntimeError(
            "cyvcf2 is unavailable; the validator must run inside the "
            "VCF-RDFizer container image"
        )
    use_bcftools = filter_oracle == "bcftools" or (
        filter_oracle == "auto" and shutil.which("bcftools") is not None
    )
    if filter_oracle == "bcftools" and not shutil.which("bcftools"):
        raise RuntimeError("--filter-oracle=bcftools requested but bcftools is unavailable")
    filters = filters_with_bcftools(vcf_path) if use_bcftools else Counter()
    _open_started = time.monotonic()
    reader = VCF(str(vcf_path), strict_gt=True)
    samples = list(reader.samples)
    header_metadata = parse_header_metadata(read_vcf_header_text(vcf_path))
    if timing is not None:
        timing["readerOpenSeconds"] = time.monotonic() - _open_started
    # Hoisted out of the loop: a per-record attribute lookup on `timing` would
    # be measuring the measurement.
    _time_samples = timing is not None
    _sample_block_seconds = 0.0
    density: Counter[tuple[str, int]] = Counter()
    shapes: Counter[str] = Counter()
    genotypes: Counter[tuple[str, str]] = Counter()
    ac_an: Counter[tuple[int, int]] = Counter()
    total_records = gt_records = single_alt_records = q06_eligible = 0
    transition_count = transversion_count = biallelic_snv_count = 0
    # Census inputs. These mirror SampleRecordStream._parse_row, which widens
    # FORMAT to the longest sample payload when a record drops trailing fields.
    digest_buckets: Counter[str] = Counter()
    info_value_digest: Counter[str] = Counter()
    expanded_format_digest: Counter[str] = Counter()
    condensed_format_digest: Counter[str] = Counter()
    sample_components = [rml_uri_component(uri_id) for uri_id in sample_uri_ids(samples)]
    declared_info_types = info_declared_types(read_vcf_header_text(vcf_path))
    info_definitions: set[str] = set()
    info_values = info_flags = info_typed_integers = info_typed_decimals = 0
    source_component = rml_uri_component(vcf_path.name)
    record_rows: list[list[str]] = []
    records_with_format_column = records_with_format_keys = 0
    format_key_occurrences = format_value_slots = non_empty_format_values = 0
    distinct_format_keys: set[str] = set()
    _scan_started = time.monotonic()
    try:
        for variant in reader:
            total_records += 1
            density[(str(variant.CHROM), (int(variant.POS) - 1) // 1_000_000)] += 1
            alt = alt_lexical(variant)
            shapes[classify_variant_shape(variant.REF, alt)] += 1
            ref_upper, alt_upper = str(variant.REF).upper(), alt.upper()
            if re.fullmatch(r"[ACGT]", ref_upper) and re.fullmatch(r"[ACGT]", alt_upper) and ref_upper != alt_upper:
                biallelic_snv_count += 1
                if (ref_upper, alt_upper) in TRANSITIONS:
                    transition_count += 1
                else:
                    transversion_count += 1
            if not use_bcftools:
                raw_filter = exact_filter_lexical(variant)
                filters[(filter_status(raw_filter), raw_filter)] += 1
            raw_format = variant.FORMAT
            if not raw_format:
                format_keys: list[str] = []
            elif isinstance(raw_format, str):
                format_keys = raw_format.split(":")
            else:
                format_keys = [str(item) for item in raw_format]
            has_gt = "GT" in format_keys
            if has_gt:
                gt_records += 1

            # `vcfc:formatRaw` comes from the FORMAT column itself, so it is
            # present whenever that column is non-empty - samples or not.
            if format_keys:
                records_with_format_column += 1

            columns = str(variant).rstrip("\r\n").split("\t")
            record_rows.append(columns)
            # The record digest is computed from the raw line so it matches the
            # lexical values the mapping puts in the graph, character for
            # character, with no round-trip through cyvcf2's typed accessors.
            record_iri = (
                f"file://{source_component}#record/"
                f"{rml_uri_component(str(total_records))}"
            )
            digest_buckets[record_digest_bucket([record_iri, *columns[:8]])] += 1

            # Structured INFO counts. The typed-value rules mirror
            # `_typed_info_object` in vcf_rdfizer.py: only a single-valued
            # Integer/Float that actually parses gains a typed predicate.
            row_component = rml_uri_component(str(total_records))
            for key, value in parse_info_entries(columns[7] if len(columns) > 7 else ""):
                info_values += 1
                info_definitions.add(key)
                if value is None:
                    info_flags += 1
                    continue
                info_iri = (
                    f"file://{source_component}#call/{row_component}"
                    f"/info/{rml_uri_component(key)}"
                )
                info_value_digest[record_digest_bucket([info_iri, value])] += 1
                declared = declared_info_types.get(key, "String")
                if "," in value or value == ".":
                    continue
                try:
                    if declared == "Integer":
                        int(value)
                        info_typed_integers += 1
                    elif declared == "Float":
                        float(value)
                        info_typed_decimals += 1
                except ValueError:
                    pass
            payload_fields = [
                payload.split(":") if payload else [] for payload in columns[9:]
            ]
            # SampleRecordStream widens FORMAT to the longest sample payload and
            # names any surplus field FIELD_<n>, so the emitted key set can be
            # wider than the declared one. Mirror that exactly.
            width = max([len(format_keys), *(len(fields) for fields in payload_fields)], default=0)
            widened_keys = [
                format_keys[index] if index < len(format_keys) and format_keys[index]
                else f"FIELD_{index + 1}"
                for index in range(width)
            ]
            if samples and widened_keys:
                records_with_format_keys += 1
                distinct_format_keys.update(widened_keys)
                format_key_occurrences += width
                format_value_slots += width * len(samples)
                non_empty_format_values += sum(
                    1
                    for fields in payload_fields
                    for index in range(width)
                    if index < len(fields) and fields[index] != ""
                )
                # Both representations are accumulated: only one is compared,
                # chosen by the run's representation, but computing both keeps
                # this loop single-pass.
                call_iri = f"file://{source_component}#call/{row_component}"
                for key_index, key in enumerate(widened_keys):
                    key_component = rml_uri_component(key)
                    for sample_index, fields in enumerate(payload_fields):
                        cell = fields[key_index] if key_index < len(fields) else ""
                        if not cell:
                            continue
                        value_iri = (
                            f"file://{source_component}#sample/{row_component}"
                            f"/{sample_components[sample_index]}/fmt/{key_component}"
                        )
                        expanded_format_digest[
                            record_digest_bucket([value_iri, cell])
                        ] += 1
                    encoded = "\t".join(
                        (fields[key_index] if key_index < len(fields) and fields[key_index]
                         else ".")
                        for fields in payload_fields
                    )
                    vector_iri = f"{call_iri}/matrix/fmt/{key_component}"
                    condensed_format_digest[
                        record_digest_bucket([vector_iri, encoded])
                    ] += 1
            _sample_started = time.monotonic() if _time_samples else 0.0
            alleles = [None] * len(samples)
            if samples and has_gt:
                raw_genotypes = list(variant.genotypes)
                if len(raw_genotypes) != len(samples):
                    raise ValueError(f"Sample/genotype length mismatch at {variant.CHROM}:{variant.POS}")
                alleles = [genotype_alleles(raw) for raw in raw_genotypes]
            for sample, call in zip(samples, alleles, strict=True):
                genotypes[(sample, classify_genotype(call, has_gt=has_gt))] += 1
            if alt != "." and "," not in alt:
                single_alt_records += 1
                if has_gt:
                    an = ac = 0
                    for call in alleles:
                        if call is None or any(value is None for value in call):
                            continue
                        complete = tuple(int(value) for value in call if value is not None)
                        if len(complete) not in (1, 2) or any(value not in (0, 1) for value in complete):
                            continue
                        an += len(complete)
                        ac += sum(complete)
                    if an:
                        ac_an[(an, ac)] += 1
                        q06_eligible += 1
            if _time_samples:
                _sample_block_seconds += time.monotonic() - _sample_started
    finally:
        reader.close()
    if timing is not None:
        timing["scanSeconds"] = time.monotonic() - _scan_started
        timing["sampleBlockSeconds"] = _sample_block_seconds
    _assembly_started = time.monotonic()
    if sum(filters.values()) != total_records:
        raise ValueError("FILTER oracle record count differs from the VCF record count")
    q05 = [
        {"sampleId": sample, "genotypeClass": genotype_class, "callCount": int(count)}
        for (sample, genotype_class), count in sorted(genotypes.items())
    ]
    result = {
        "source": str(vcf_path),
        "sourceSha256": sha256_file(vcf_path),
        "filterOracle": "bcftools" if use_bcftools else "cyvcf2-serialization",
        "headerLineCount": header_metadata["headerLineCount"],
        "fileDate": header_metadata["fileDate"],
        "emittedHeaderClasses": header_metadata["emittedHeaderClasses"],
        "emittedHeaderPredicates": header_metadata["emittedHeaderPredicates"],
        "contigCount": header_metadata["contigCount"],
        **emitted_record_counters(
            record_rows,
            samples,
            version=vocab.resolve_vcf_version(header_metadata["fileFormat"])[0],
            contig_ids=set(header_metadata["contigIds"]),
            alt_declaration_ids=set(header_metadata["altDeclarationIds"]),
            has_assembly_line=header_metadata["hasAssemblyLine"],
            info_numbers=header_metadata["infoKeyNumbers"],
            format_numbers=header_metadata["formatKeyNumbers"],
        ),
        **synthesized_definition_numbers(
            record_rows,
            declared_info=set(header_metadata["infoKeyNumbers"]),
            declared_format=set(header_metadata["formatKeyNumbers"]),
        ),
        "headerValueCount": header_metadata["headerValueCount"],
        "recordsWithFormatColumn": records_with_format_column,
        "recordsWithFormatKeys": records_with_format_keys,
        "formatKeyOccurrences": format_key_occurrences,
        "formatValueSlots": format_value_slots,
        "nonEmptyFormatValues": non_empty_format_values,
        "distinctFormatKeyCount": len(distinct_format_keys),
        "q12_info_value_digest": [
            {"bucket": bucket, "valueCount": int(count)}
            for bucket, count in sorted(info_value_digest.items())
        ],
        "_expandedFormatValueDigest": [
            {"bucket": bucket, "valueCount": int(count)}
            for bucket, count in sorted(expanded_format_digest.items())
        ],
        "_condensedFormatValueDigest": [
            {"bucket": bucket, "valueCount": int(count)}
            for bucket, count in sorted(condensed_format_digest.items())
        ],
        "infoValueCount": info_values,
        "infoDefinitionCount": len(info_definitions),
        "infoFlagCount": info_flags,
        "infoTypedIntegerCount": info_typed_integers,
        "infoTypedDecimalCount": info_typed_decimals,
        "q11_record_digest": [
            {"bucket": bucket, "recordCount": int(count)}
            for bucket, count in sorted(digest_buckets.items())
        ],
        "fileFormat": header_metadata["fileFormat"],
        "referenceGenome": header_metadata["referenceGenome"],
        "sourceSoftware": header_metadata["sourceSoftware"],
        "q07_file_metadata": {
            "fileFormat": header_metadata["fileFormat"],
            "referenceGenome": header_metadata["referenceGenome"],
            "sourceSoftware": header_metadata["sourceSoftware"],
        },
        "q08_header_line_census": header_metadata["q08_header_line_census"],
        "sampleCount": len(samples),
        "samples": samples,
        "totalRecords": total_records,
        "gtRecordCount": gt_records,
        "singleAltRecordCount": single_alt_records,
        "q06EligibleSiteCount": q06_eligible,
        "q01_record_density_1mb": [
            {"chrom": chrom, "windowIndex": window, "recordCount": int(count)}
            for (chrom, window), count in sorted(density.items())
        ],
        "q02_variant_shape_counts": [
            {"variantClass": kind, "recordCount": int(count)} for kind, count in sorted(shapes.items())
        ],
        "q03_titv": {
            "biallelicSnvCount": biallelic_snv_count,
            "transitionCount": transition_count,
            "transversionCount": transversion_count,
            "tiTvRatio": transition_count / transversion_count if transversion_count else None,
        },
        "q04_filter_distribution": [
            {"filterStatus": status, "filterLexical": lexical, "recordCount": int(count)}
            for (status, lexical), count in sorted(filters.items())
        ],
        "q05_sample_genotype_counts": q05,
        "q06_ac_an_distribution": [
            {"an": an, "ac": ac, "siteCount": int(count), "af": ac / an}
            for (an, ac), count in sorted(ac_an.items())
        ],
    }
    if timing is not None:
        timing["assemblySeconds"] = time.monotonic() - _assembly_started
    return result


# ---------------------------------------------------------------------------
# Source artifact materialization
# ---------------------------------------------------------------------------
# Semantic validation always runs against N-Triples. A compressed or indexed
# artifact is therefore decoded into the container-local scratch directory
# first, which is what makes "validate the HDT" mean "prove the HDT decodes to
# a graph that still satisfies every check", rather than only that it decodes
# to the right number of triples (which validate_compression.py already does).
#
# Decoding needs scratch space of roughly the uncompressed graph size. The
# scratch directory is removed before the process exits in all cases.

RDF_FORMATS = ("nt", "nt.gz", "nt.br", "hdt", "cottas", "cottas.gz", "cottas.br")
#: Formats that are read in place, with nothing written to scratch.
DIRECT_FORMATS = frozenset({"nt"})
FORMAT_SUFFIXES = (
    (".nt.gz", "nt.gz"),
    (".nt.br", "nt.br"),
    (".nt", "nt"),
    (".cottas.gz", "cottas.gz"),
    (".cottas.br", "cottas.br"),
    (".cottas", "cottas"),
    (".hdt", "hdt"),
)
COTTAS_TOOL = Path("/opt/vcf-rdfizer/cottas_tool.py")


def detect_rdf_format(path: Path) -> str | None:
    """Infer the artifact format from a filename, or None when unrecognised."""
    name = path.name
    for suffix, fmt in FORMAT_SUFFIXES:
        if name.endswith(suffix):
            return fmt
    return None


def _resolve_binary(env_var: str, *candidates: str) -> str:
    """Find a container binary the same way the compression stages do."""
    override = os.environ.get(env_var, "").strip()
    if override and (Path(override).is_file() or shutil.which(override)):
        return override
    for candidate in candidates:
        found = shutil.which(candidate) or (candidate if Path(candidate).is_file() else None)
        if found:
            return found
    raise RuntimeError(f"Required binary not found in container: {candidates[0]}")


#: Substrings that mean a decode step failed even though it exited 0. hdt-cpp's
#: ``hdt2rdf`` is the reason this list exists: its raptor file serializer emits
#: ``error: :0:0: write error`` once per triple and still returns success.
STEP_ERROR_MARKERS = ("write error", "error:", "ERROR:")


def _run_step(
    command: list[str],
    *,
    label: str,
    log_dir: Path,
    env: dict[str, str] | None = None,
    stdout_path: Path | None = None,
    error_markers: tuple[str, ...] = (),
) -> None:
    """Run one decode step, preserving its output for diagnosis on failure.

    ``stdout_path`` redirects the command's stdout to that file and keeps only
    stderr in the log, which is how a tool that can only serialize correctly to
    stdout is driven.

    ``error_markers`` are matched against the log after a *successful* exit. An
    exit code is only as trustworthy as the tool that returns it, and a decode
    step that reports success while writing garbage is worse than one that
    fails: the garbage flows downstream and the run dies somewhere unrelated,
    hours later.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{label}.log"
    if stdout_path is not None:
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        with stdout_path.open("wb") as out, log_path.open("wb") as log:
            result = subprocess.run(command, check=False, stdout=out, stderr=log, env=env)
    else:
        with log_path.open("wb") as log:
            result = subprocess.run(
                command, check=False, stdout=log, stderr=subprocess.STDOUT, env=env
            )

    tail = ""
    try:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
    except OSError:
        pass

    if result.returncode != 0:
        raise RuntimeError(
            f"{label} failed with exit code {result.returncode}. Output tail: {tail}"
        )

    if error_markers:
        try:
            log_text = log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            log_text = tail
        hit = next((m for m in error_markers if m in log_text), None)
        if hit is not None:
            raise RuntimeError(
                f"{label} exited 0 but reported errors on stderr "
                f"(matched {hit!r}); treating the step as failed because its "
                f"output cannot be trusted. Output tail: {tail}"
            )


def _verify_ntriples(path: Path, *, label: str) -> int:
    """Reject a materialized N-Triples file that is structurally impossible.

    Cheap enough to run on any size (it reads a prefix and a suffix, never the
    whole file) and it catches the failure mode that cost this project days: a
    4.4 GiB "N-Triples" file containing no newline at all, whose every line was
    a truncated subject IRI. Raptor accepts that as one enormous line and
    degrades to quadratic behaviour, so the run does not fail -- it crawls, at
    roughly 7 KiB/s, and looks like a hang rather than a bug.

    Returns the size in bytes. Raises RuntimeError with a diagnosis otherwise.
    """
    try:
        size = path.stat().st_size
    except OSError as error:
        raise RuntimeError(f"{label} produced no readable output at {path}: {error}") from error
    if size == 0:
        raise RuntimeError(f"{label} produced an empty N-Triples file at {path}")

    probe = 1024 * 1024
    with path.open("rb") as handle:
        head = handle.read(probe)
        if b"\n" not in head:
            raise RuntimeError(
                f"{label} produced {size} bytes with no newline in the first "
                f"{min(probe, size)}; this is not N-Triples. First 200 bytes: "
                f"{head[:200]!r}"
            )
        first_line = head.split(b"\n", 1)[0].strip()
        if not (first_line.startswith(b"<") or first_line.startswith(b"_:")):
            raise RuntimeError(
                f"{label} produced a first line that does not begin a triple: {first_line[:200]!r}"
            )
        if not first_line.endswith(b"."):
            raise RuntimeError(
                f"{label} produced a first line with no statement terminator: "
                f"{first_line[:200]!r}"
            )
        handle.seek(max(0, size - 4096))
        if not handle.read().endswith(b"\n"):
            raise RuntimeError(
                f"{label} produced a file not terminated by a newline; it is "
                f"most likely truncated ({size} bytes)"
            )
    return size


def materialize_ntriples(
    source: Path,
    rdf_format: str,
    scratch: Path,
    *,
    log_dir: Path,
) -> tuple[Path, dict[str, Any]]:
    """Return ``(ntriples_path, provenance)`` for any supported artifact.

    A plain ``.nt`` source is used in place and never copied.
    """
    provenance: dict[str, Any] = {
        "sourceFormat": rdf_format,
        "materialized": rdf_format not in DIRECT_FORMATS,
        "steps": [],
    }
    if rdf_format == "nt":
        return source, provenance

    target = scratch / "input.nt"
    if rdf_format == "nt.gz":
        with gzip.open(source, "rb") as handle, target.open("wb") as out:
            shutil.copyfileobj(handle, out, length=1024 * 1024)
        _verify_ntriples(target, label="gzip-decompress")
        provenance["steps"].append({"tool": "python-gzip", "output": str(target)})
        return target, provenance

    if rdf_format == "nt.br":
        brotli = _resolve_binary("BROTLI_BIN", "brotli")
        _run_step([brotli, "-d", "-c", "-o", str(target), str(source)],
                  label="brotli-decompress", log_dir=log_dir)
        _verify_ntriples(target, label="brotli-decompress")
        provenance["steps"].append({"tool": brotli, "output": str(target)})
        return target, provenance

    if rdf_format == "hdt":
        hdt2rdf = _resolve_binary("HDT2RDF_BIN", "hdt2rdf", "/usr/local/bin/hdt2rdf")
        # hdt-cpp's file-output path is broken: for every output format its
        # raptor iostream serializer emits "error: :0:0: write error" per
        # triple, writes a truncated subject IRI with no terminator and no
        # newline, and still exits 0. Its stdout serializer takes a different
        # code path and is correct, verified at 200k triples with empty
        # stderr, so the dump is taken from stdout and redirected here.
        _run_step(
            [hdt2rdf, "-f", "ntriples", str(source), "-"],
            label="hdt2rdf",
            log_dir=log_dir,
            stdout_path=target,
            error_markers=STEP_ERROR_MARKERS,
        )
        _verify_ntriples(target, label="hdt2rdf")
        provenance["steps"].append({
            "tool": hdt2rdf,
            "output": str(target),
            "mode": "stdout-redirect",
        })
        return target, provenance

    if rdf_format in {"cottas", "cottas.gz", "cottas.br"}:
        cottas_input = source
        if rdf_format != "cottas":
            # pycottas needs a seekable Parquet file, so a packaged artifact is
            # unwrapped into scratch before it is decoded.
            cottas_input = scratch / "input.cottas"
            if rdf_format == "cottas.gz":
                with gzip.open(source, "rb") as handle, cottas_input.open("wb") as out:
                    shutil.copyfileobj(handle, out, length=1024 * 1024)
                provenance["steps"].append({"tool": "python-gzip", "output": str(cottas_input)})
            else:
                brotli = _resolve_binary("BROTLI_BIN", "brotli")
                _run_step([brotli, "-d", "-c", "-o", str(cottas_input), str(source)],
                          label="brotli-unwrap", log_dir=log_dir)
                provenance["steps"].append({"tool": brotli, "output": str(cottas_input)})
        python_bin = os.environ.get("COTTAS_PYTHON_BIN") or sys.executable
        _run_step(
            [python_bin, str(COTTAS_TOOL), "decompress", str(cottas_input), str(target)],
            label="cottas-decompress",
            log_dir=log_dir,
        )
        _verify_ntriples(target, label="cottas-decompress")
        provenance["steps"].append({"tool": f"{python_bin} cottas_tool.py decompress",
                                    "output": str(target)})
        if cottas_input != source:
            cottas_input.unlink(missing_ok=True)
        return target, provenance

    raise ValueError(f"Unsupported RDF artifact format: {rdf_format}")


#: Violations retained in the report. A badly broken graph can produce one per
#: record, so the full text is never inlined into a JSON report.
SHACL_SAMPLE_LIMIT = 50


def validate_shacl(source: Path, shapes: Path, results_dir: Path) -> dict[str, Any]:
    """Validate the graph against SHACL shapes, if pyshacl is available.

    This is an independent structural layer: it checks the shapes the
    vocabulary publishes, rather than comparing counts against the VCF, so it
    catches a different class of defect from everything else here.

    pyshacl loads the graph into memory, so this is opt-in and unsuitable for a
    cohort-scale aggregate. It is reported as EXECUTION_FAILED rather than a
    conformance failure when the tool is missing, so an absent optional
    dependency can never look like a bad graph.
    """
    report_path = results_dir / "shacl.json"
    try:
        from pyshacl import validate as pyshacl_validate
    except ImportError as error:
        result = {
            "status": "EXECUTION_FAILED",
            "error": f"pyshacl is not installed in this image: {error}",
            "shapes": str(shapes),
        }
        write_json(report_path, result)
        return result

    started = time.monotonic()
    try:
        conforms, _graph, text = pyshacl_validate(
            str(source),
            shacl_graph=str(shapes),
            data_graph_format="nt",
            shacl_graph_format="turtle",
            inference="none",
            advanced=False,
        )
    except Exception as error:  # noqa: BLE001 - reported, never fatal here
        result = {
            "status": "EXECUTION_FAILED",
            "error": f"SHACL validation could not run: {error}",
            "shapes": str(shapes),
        }
        write_json(report_path, result)
        return result

    violations = [
        line.strip() for line in text.splitlines()
        if line.strip().startswith("Constraint Violation")
    ]
    paths = sorted({
        line.split("Result Path:", 1)[1].strip()
        for line in text.splitlines() if "Result Path:" in line
    })
    log_path = results_dir / "shacl-report.txt"
    log_path.write_text(text, encoding="utf-8")
    result = {
        "status": "PASS" if conforms else "FAIL",
        "conforms": bool(conforms),
        "shapes": str(shapes),
        "violationCount": len(violations),
        "violationPaths": paths,
        "report": str(log_path),
        "wallSeconds": time.monotonic() - started,
        "sampleLimitedTo": SHACL_SAMPLE_LIMIT,
        "sample": violations[:SHACL_SAMPLE_LIMIT],
    }
    write_json(report_path, result)
    return result


def validate_ntriples(source: Path, results_dir: Path) -> dict[str, Any]:
    rapper = shutil.which("rapper")
    if not rapper:
        return {"status": "EXECUTION_FAILED", "error": "rapper is not installed"}
    result = subprocess.run(
        [rapper, "-i", "ntriples", "-c", str(source)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    log = results_dir / "rdf-validation" / "rapper.txt"
    log.parent.mkdir(parents=True, exist_ok=True)
    output = result.stdout + result.stderr
    log.write_text(output, encoding="utf-8")
    # Rapper reports the parsed triple count; keeping it makes a decoded
    # HDT/COTTAS artifact directly comparable against its source graph.
    match = re.search(r"Parsing returned (\d+) triples", output)
    return {
        "status": "PASS" if result.returncode == 0 else "FAIL",
        "log": str(log),
        "exitCode": result.returncode,
        "tripleCount": int(match.group(1)) if match else None,
    }


# ---------------------------------------------------------------------------
# SPARQL engines
# ---------------------------------------------------------------------------
# Every engine answers the same queries and writes SPARQL Results JSON to the
# same place, so the comparison layer is engine-agnostic and a run's engine
# choice is only ever a performance/scale decision, never a semantic one.
#
# comunica: zero setup, loads the whole graph into the Node heap. Fine for the
#           scale most single-sample runs reach, and the default.
# qlever:   builds an on-disk index, then answers over HTTP. Slower to start,
#           but the only option once a graph stops fitting in memory.

#: Every backend that can answer the validation queries. `comunica` and
#: `qlever` read the materialized N-Triples; `hdt` and `cottas` query the
#: compressed representation *directly*, without decoding it first, which is
#: the property those formats exist for. Several may be requested in one run:
#: they all answer the same queries, so the comparison is meaningful and the
#: recorded timings are directly comparable.
SPARQL_ENGINES = ("comunica", "qlever", "hdt", "cottas")
#: Engines that query a compressed artifact natively, and the artifact format
#: each one needs. When the run's source is a different format, the artifact is
#: built in scratch first and the build is timed separately from the queries,
#: so an index build is never mistaken for query cost.
NATIVE_ENGINE_FORMATS = {"hdt": "hdt", "cottas": "cottas"}
# QLever's binaries are copied out of the upstream image, which is built on a
# different Ubuntu release, so their Boost/ICU/jemalloc sonames come with them
# in a private directory. Pointing only QLever's own processes at it keeps
# those libraries from shadowing anything the rest of the image uses.
QLEVER_LIB_DIR = "/opt/qlever/lib"
QLEVER_BIN_DIR = "/opt/qlever/bin"
DEFAULT_QLEVER_PORT = 7019
DEFAULT_QLEVER_MEMORY_GB = 4
DEFAULT_QLEVER_STARTUP_TIMEOUT = 900
DEFAULT_QUERY_TIMEOUT = 3600
DEFAULT_COMUNICA_PORT = 7020
#: Seconds to wait for comunica-sparql-file-http to bind its port. The graph is
#: not read yet at that point, so this is process startup only and stays short.
DEFAULT_COMUNICA_BIND_TIMEOUT = 120
#: Seconds allowed for the warm-up query that proves the endpoint can read the
#: source before the suite commits to it. Budgeted separately from a normal
#: query because on a large graph even a LIMIT 1 has to start streaming it.
DEFAULT_COMUNICA_WARMUP_TIMEOUT = 3600
#: The HDT endpoint runs alongside the N-Triples one within a single run, so
#: they must not contend for a port.
DEFAULT_HDT_ENDPOINT_PORT = 7021


QLEVER_STATUS_FILE = Path("/opt/vcf-rdfizer/qlever-status.txt")


def qlever_build_status() -> str:
    """Read the image's build-time note about whether QLever links here."""
    try:
        status = QLEVER_STATUS_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return "QLever build status unknown (marker file absent)."
    return "QLever binaries linked cleanly at image build time." if status == "ok" else status


def _expand_template(template: str, **fields: str) -> list[str]:
    """Split a shell-style override and substitute ``{name}`` placeholders."""
    return [part.format(**fields) for part in shlex.split(template)]


class QueryEngine:
    """Common lifecycle and result envelope for a SPARQL backend."""

    name = "abstract"

    def __init__(self, source: Path, *, raw_dir: Path, scratch: Path, options: dict[str, Any]):
        self.source = source
        self.raw_dir = raw_dir
        self.scratch = scratch
        self.options = options
        self.query_timeout = int(options.get("query_timeout") or DEFAULT_QUERY_TIMEOUT)
        #: Seconds spent preparing the backend (index build, format conversion,
        #: server startup) before any query ran. Reported separately from query
        #: time so a benchmark can attribute cost correctly.
        self.setup_seconds: float | None = None

    def prepare(self) -> None:
        """Time ``start`` so setup cost is recorded for every engine alike."""
        started = time.monotonic()
        self.start()
        self.setup_seconds = time.monotonic() - started

    def __enter__(self) -> "QueryEngine":
        self.prepare()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.stop()
        return False

    def start(self) -> None:
        """Prepare the engine. Raise RuntimeError when it cannot be used."""

    def stop(self) -> None:
        """Release anything ``start`` acquired. Must be safe to call twice."""

    def describe(self) -> dict[str, Any]:
        return {"engine": self.name, "setupSeconds": self.setup_seconds}

    def execute(self, query_id: str, query_path: Path) -> dict[str, Any]:
        raise NotImplementedError

    def _envelope(
        self,
        query_id: str,
        query_path: Path,
        *,
        returncode: int,
        started: float,
        raw_path: Path,
        stderr_path: Path,
        time_path: Path | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        envelope = {
            "status": "PASS" if returncode == 0 else "EXECUTION_FAILED",
            "engine": self.name,
            "exitCode": returncode,
            "wallSeconds": time.monotonic() - started,
            "query": str(query_path),
            "rawResult": str(raw_path),
            "stderr": str(stderr_path),
            "resourceMetrics": str(time_path) if time_path and time_path.exists() else None,
        }
        if error is not None:
            envelope["error"] = error
        return envelope


#: Above this decoded-graph size, an engine with no persistent index spends
#: more time parsing than querying. It is not a hard limit -- host memory and
#: query shape decide the real ceiling -- but it is the point at which the
#: per-query parse starts to dominate, and where a 27-query run stops being a
#: reasonable thing to start without knowing that.
UNINDEXED_ENGINE_ADVICE_BYTES = 4 * 1024 * 1024 * 1024


def engine_advice(engine_name: str, source: Path, query_count: int, timeout: int) -> str | None:
    """Warn before a run commits to an engine that cannot finish it.

    Comunica still has no index. The endpoint removes per-query engine startup
    but streams the source for every query, so a full-scan query re-reads the
    whole graph each time. On a large graph that is the dominant cost and no
    amount of tuning fixes it -- only an engine that indexes will.
    """
    if engine_name != "comunica":
        return None
    try:
        size = source.stat().st_size
    except OSError:
        return None
    if size < UNINDEXED_ENGINE_ADVICE_BYTES:
        return None
    return (
        f"comunica has no index and streams the source for every query, so "
        f"each of the {query_count} full-scan queries re-reads all "
        f"{size / (1024 ** 3):.1f} GiB. Prefer --engine qlever, or validate an "
        f"indexed artifact (--validation-targets hdt)."
    )


def run_query_process(
    command: list[str], *, stdout_path: Path, stderr_path: Path, timeout: int
) -> tuple[int, str | None]:
    """Run one query process, killing its whole process tree on timeout.

    ``subprocess.run(timeout=...)`` signals only the process it started. Every
    query here is wrapped in ``/usr/bin/time``, which forks the real engine, so
    killing the wrapper leaves the engine running -- holding the entire graph in
    memory, because that is what an in-memory SPARQL engine does. A handful of
    those orphans is enough to push a host into swap and make the rest of the
    run appear to hang rather than fail.

    Starting a new session and signalling the group kills the wrapper and the
    engine together. SIGTERM first so the engine can unmap its store, SIGKILL if
    it does not go.
    """
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            command, stdout=stdout, stderr=stderr, start_new_session=True
        )
        try:
            return process.wait(timeout=timeout), None
        except subprocess.TimeoutExpired:
            _terminate_process_group(process)
            return 124, f"query exceeded {timeout}s"


def _terminate_process_group(process: subprocess.Popen) -> None:
    """Signal a process group, escalating to SIGKILL, and reap the leader."""
    for signal_number, grace in ((signal.SIGTERM, 10), (signal.SIGKILL, 5)):
        try:
            os.killpg(os.getpgid(process.pid), signal_number)
        except (ProcessLookupError, PermissionError):
            break
        try:
            process.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            continue
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


class ComunicaHttpEndpointMixin:
    """One long-lived Comunica endpoint, shared by every query in the suite.

    Comunica's one-shot CLIs (``comunica-sparql-file``, ``comunica-sparql-hdt``)
    were spawned once per query, so each of the 27 queries paid Node startup
    and engine initialisation again. The matching ``*-http`` binaries pay that
    once and answer every query from the running process.

    Measured in this image on 1.5M N-Triples, per ``SELECT (COUNT(*))``:

    ==========================  ========
    one-shot ``comunica-sparql-file``  26.0s
    this endpoint                      10.3s
    ==========================  ========

    What it does **not** do is materialize the graph. ``LIMIT 1`` answers in
    0.59s while ``COUNT(*)`` takes 10s on every repeat, which is the signature
    of a source streamed per query rather than an in-memory store. Loading it
    into one instead was measured and rejected: an ``N3.Store`` of the same
    1.5M triples cost 2.45 GiB and made ``COUNT`` *slower* (15.0s), which
    extrapolates to roughly 145 GiB for an 88M-triple cohort graph. For a graph
    that large the answer is an engine that indexes -- QLever, or the HDT
    artifact -- not a bigger heap.

    So the saving here is per-query engine startup, which is real and repeated
    27 times, and the scan cost is unchanged. Per-query worker recycling
    (``--freshWorker``) would give the startup cost straight back and is never
    passed. The worker count is pinned to 1: workers do not share state, so
    each extra one only adds another process reading the same file.
    """

    #: Endpoint binary, e.g. "comunica-sparql-file-http".
    endpoint_binary = ""
    #: Key in ``options`` holding this endpoint's port.
    endpoint_port_option = "comunica_port"
    default_endpoint_port = DEFAULT_COMUNICA_PORT
    #: Guidance appended when the binary is missing from the image.
    endpoint_missing_hint = "rebuild the image or select --engine qlever"

    def _init_endpoint(self, options: dict[str, Any]) -> None:
        self.port = int(
            options.get(self.endpoint_port_option) or self.default_endpoint_port
        )
        self.bind_timeout = int(
            options.get("comunica_bind_timeout") or DEFAULT_COMUNICA_BIND_TIMEOUT
        )
        self.warmup_timeout = int(
            options.get("comunica_warmup_timeout")
            or max(DEFAULT_COMUNICA_WARMUP_TIMEOUT, self.query_timeout)
        )
        self.server: subprocess.Popen | None = None
        self.executable: str | None = None
        self.endpoint: str | None = None
        self.warmup_seconds: float | None = None
        self.command: list[str] = []

    def _endpoint_source_argument(self) -> str:
        """The source argv entry, in whatever form this engine's binary needs."""
        raise NotImplementedError

    def _candidate_endpoints(self) -> list[str]:
        return [
            f"http://127.0.0.1:{self.port}/sparql",
            f"http://127.0.0.1:{self.port}/",
        ]

    def _require_endpoint_binary(self) -> str:
        """Fail before any expensive setup when the binary is missing.

        An engine that cannot possibly run should say so before an artifact is
        built for it, not after.
        """
        self.executable = shutil.which(self.endpoint_binary)
        if not self.executable:
            raise RuntimeError(
                f"{self.endpoint_binary} is not installed in this image; "
                f"{self.endpoint_missing_hint}"
            )
        return self.executable

    def _start_endpoint(self) -> None:
        if not self.executable:
            self._require_endpoint_binary()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.command = [
            self.executable,
            self._endpoint_source_argument(),
            "-p", str(self.port),
            # One worker: workers share no state, so extra ones would only add
            # more processes reading the same source.
            "-w", "1",
            # Comunica's own per-query ceiling, aligned with the client side so
            # neither silently pre-empts the other.
            "-t", str(self.query_timeout),
        ]
        server_log = self.log_dir / f"{self.name}-server.log"
        self.server = subprocess.Popen(
            self.command,
            stdout=server_log.open("wb"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self._await_bind(server_log)
        self._await_warm(server_log)

    def _await_bind(self, server_log: Path) -> None:
        """Wait for the port to answer at all. The graph is not read yet."""
        deadline = time.monotonic() + self.bind_timeout
        last_error = "endpoint never bound"
        while time.monotonic() < deadline:
            self._assert_alive(server_log)
            for endpoint in self._candidate_endpoints():
                try:
                    self._post(endpoint, "ASK { }", timeout=10)
                    self.endpoint = endpoint
                    return
                except Exception as error:  # noqa: BLE001 - readiness probe
                    last_error = str(error)
            time.sleep(0.5)
        raise RuntimeError(
            f"comunica endpoint did not bind port {self.port} within "
            f"{self.bind_timeout}s: {last_error}"
        )

    def _await_warm(self, server_log: Path) -> None:
        """Prove the endpoint can actually read the source, before the suite runs.

        Charged to setup rather than to the first query, so one query is not
        billed for readiness that every later query got for free.
        """
        assert self.endpoint is not None
        started = time.monotonic()
        try:
            self._post(
                self.endpoint, "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
                timeout=self.warmup_timeout,
            )
        except Exception as error:  # noqa: BLE001 - surfaced as engine failure
            self._assert_alive(server_log)
            raise RuntimeError(
                f"{self.name} could not read {self._endpoint_source_argument()} "
                f"within {self.warmup_timeout}s (raise --comunica-warmup-timeout, "
                f"or use --engine qlever): {error}"
            ) from error
        self.warmup_seconds = time.monotonic() - started

    def _assert_alive(self, server_log: Path) -> None:
        if self.server is not None and self.server.poll() is not None:
            tail = ""
            try:
                tail = server_log.read_text(encoding="utf-8", errors="replace")[-2000:]
            except OSError:
                pass
            raise RuntimeError(
                f"{self.name} endpoint exited with code {self.server.returncode}. "
                f"Log tail: {tail}"
            )

    def _post(self, endpoint: str, query: str, *, timeout: int) -> bytes:
        import urllib.request

        request = urllib.request.Request(
            endpoint,
            data=urllib.parse.urlencode({"query": query}).encode("utf-8"),
            headers={
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read()

    def endpoint_describe(self) -> dict[str, Any]:
        """Endpoint-specific report fields, merged by each engine's describe."""
        return {
            "version": tool_version(
                [self.endpoint_binary, "--version"], table_label="Comunica Engine"
            ),
            "mode": "one long-lived endpoint, source streamed per query",
            "endpoint": self.endpoint,
            "port": self.port,
            # Readiness, not a graph load: the source is not materialized.
            "warmupSeconds": self.warmup_seconds,
            "command": list(self.command),
        }

    def execute(self, query_id: str, query_path: Path) -> dict[str, Any]:
        raw_path = self.raw_dir / f"{query_id}.sparql.json"
        stderr_path = self.raw_dir / f"{query_id}.stderr.txt"
        started = time.monotonic()
        if self.endpoint is None:
            message = f"{self.name} endpoint is not running"
            stderr_path.write_text(message, encoding="utf-8")
            raw_path.write_bytes(b"")
            return self._envelope(
                query_id, query_path, returncode=1, started=started,
                raw_path=raw_path, stderr_path=stderr_path, error=message,
            )
        try:
            payload = self._post(
                self.endpoint, query_path.read_text(encoding="utf-8"),
                timeout=self.query_timeout,
            )
        except Exception as error:  # noqa: BLE001 - reported as EXECUTION_FAILED
            stderr_path.write_text(str(error), encoding="utf-8")
            raw_path.write_bytes(b"")
            return self._envelope(
                query_id, query_path, returncode=1, started=started,
                raw_path=raw_path, stderr_path=stderr_path, error=str(error),
            )
        raw_path.write_bytes(payload)
        stderr_path.write_bytes(b"")
        return self._envelope(
            query_id, query_path, returncode=0, started=started,
            raw_path=raw_path, stderr_path=stderr_path,
        )

    def stop(self) -> None:
        if self.server is not None and self.server.poll() is None:
            # The endpoint holds the whole graph in memory; a survivor would
            # starve everything that runs after it, so the group is signalled
            # rather than just the leader.
            _terminate_process_group(self.server)
        self.server = None
        self.endpoint = None


class ComunicaEngine(ComunicaHttpEndpointMixin, QueryEngine):
    """Serve the N-Triples file from one load via comunica-sparql-file-http."""

    name = "comunica"
    endpoint_binary = "comunica-sparql-file-http"
    endpoint_port_option = "comunica_port"
    default_endpoint_port = DEFAULT_COMUNICA_PORT

    def __init__(self, source: Path, *, raw_dir: Path, scratch: Path, options: dict[str, Any]):
        super().__init__(source, raw_dir=raw_dir, scratch=scratch, options=options)
        self.log_dir = raw_dir / "engine"
        self._init_endpoint(options)

    def _endpoint_source_argument(self) -> str:
        return str(self.source)

    def start(self) -> None:
        self._start_endpoint()

    def describe(self) -> dict[str, Any]:
        return {
            "engine": self.name,
            "setupSeconds": self.setup_seconds,
            **self.endpoint_describe(),
        }


class QleverEngine(QueryEngine):
    """Build a QLever index over the graph, then answer queries over HTTP.

    QLever has no one-shot "query this file" mode, so the index build and the
    short-lived server are both owned by this object and torn down in ``stop``.
    The index lives in the container scratch directory and never reaches the
    host.
    """

    name = "qlever"

    def __init__(self, source: Path, *, raw_dir: Path, scratch: Path, options: dict[str, Any]):
        super().__init__(source, raw_dir=raw_dir, scratch=scratch, options=options)
        self.port = int(options.get("port") or DEFAULT_QLEVER_PORT)
        self.memory_gb = int(options.get("memory_gb") or DEFAULT_QLEVER_MEMORY_GB)
        self.startup_timeout = int(
            options.get("startup_timeout") or DEFAULT_QLEVER_STARTUP_TIMEOUT
        )
        self.index_dir = self.scratch / "qlever-index"
        self.index_base = self.index_dir / "vcf-rdfizer"
        self.log_dir = raw_dir / "engine"
        self.server: subprocess.Popen | None = None
        self.index_seconds: float | None = None
        self.commands: dict[str, list[str]] = {}
        self.server_binary: str | None = None
        self.extra_index_args = list(options.get("extra_index_args") or []) + shlex.split(
            os.environ.get("QLEVER_EXTRA_INDEX_ARGS", "")
        )
        self.extra_server_args = list(options.get("extra_server_args") or []) + shlex.split(
            os.environ.get("QLEVER_EXTRA_SERVER_ARGS", "")
        )

    def _binary(self, env_var: str, name: str) -> str:
        try:
            return _resolve_binary(
                env_var, f"{QLEVER_BIN_DIR}/{name}", name, f"/usr/local/bin/{name}"
            )
        except RuntimeError as error:
            raise RuntimeError(f"{error}. {qlever_build_status()}") from error

    def _environment(self) -> dict[str, str]:
        """Prepend QLever's private library directory for its processes only."""
        environment = dict(os.environ)
        existing = environment.get("LD_LIBRARY_PATH", "")
        environment["LD_LIBRARY_PATH"] = (
            f"{QLEVER_LIB_DIR}:{existing}" if existing else QLEVER_LIB_DIR
        )
        return environment

    def index_command(self, index_builder: str) -> list[str]:
        """QLever's CLI has changed across releases, so the default argv is a
        starting point rather than a contract: ``--qlever-index-arg`` (or
        ``QLEVER_EXTRA_INDEX_ARGS``) appends to it, and
        ``QLEVER_INDEX_COMMAND`` replaces it entirely. ``{index}``, ``{input}``
        and ``{memory}`` are substituted in a replacement template.
        """
        template = os.environ.get("QLEVER_INDEX_COMMAND", "").strip()
        if template:
            return _expand_template(
                template, index=str(self.index_base), input=str(self.source),
                memory=f"{self.memory_gb}G",
            )
        return [
            index_builder,
            "-i", str(self.index_base),
            "-f", str(self.source),
            "-F", "nt",
            "-m", f"{self.memory_gb}G",
            *self.extra_index_args,
        ]

    def server_command(self, server_main: str) -> list[str]:
        """See :meth:`index_command`; ``QLEVER_SERVER_COMMAND`` replaces this."""
        template = os.environ.get("QLEVER_SERVER_COMMAND", "").strip()
        if template:
            return _expand_template(
                template, index=str(self.index_base), port=str(self.port),
                memory=f"{self.memory_gb}G",
            )
        return [
            server_main,
            "-i", str(self.index_base),
            "-p", str(self.port),
            "-m", f"{self.memory_gb}G",
            # qlever-server's own default query timeout is 30s, which the
            # cohort-scale aggregate queries here routinely exceed. Keep the
            # server-side limit aligned with the client-side one.
            "-s", f"{self.query_timeout}s",
            *self.extra_server_args,
        ]

    def start(self) -> None:
        index_builder = self._binary("QLEVER_INDEX_BUILDER_BIN", "qlever-index")
        server_main = self._binary("QLEVER_SERVER_BIN", "qlever-server")
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.server_binary = server_main
        self.commands["index"] = self.index_command(index_builder)
        self.commands["server"] = self.server_command(server_main)

        environment = self._environment()
        started = time.monotonic()
        _run_step(
            self.commands["index"], label="qlever-index",
            log_dir=self.log_dir, env=environment,
        )
        self.index_seconds = time.monotonic() - started

        server_log = self.log_dir / "qlever-server.log"
        self.server = subprocess.Popen(
            self.commands["server"],
            stdout=server_log.open("wb"),
            stderr=subprocess.STDOUT,
            env=environment,
        )
        self._await_ready(server_log)

    def _await_ready(self, server_log: Path) -> None:
        """Poll the server until it answers a trivial query, or give up."""
        deadline = time.monotonic() + self.startup_timeout
        last_error = "server did not become ready"
        while time.monotonic() < deadline:
            if self.server is not None and self.server.poll() is not None:
                tail = ""
                try:
                    tail = server_log.read_text(encoding="utf-8", errors="replace")[-2000:]
                except OSError:
                    pass
                raise RuntimeError(
                    f"QLever server exited with code {self.server.returncode}. Log tail: {tail}"
                )
            try:
                self._post("SELECT * WHERE { ?s ?p ?o } LIMIT 1", timeout=10)
                return
            except Exception as error:  # noqa: BLE001 - readiness probe
                last_error = str(error)
            time.sleep(0.5)
        raise RuntimeError(
            f"QLever server was not ready within {self.startup_timeout}s: {last_error}"
        )

    def _post(self, query: str, *, timeout: int) -> bytes:
        import urllib.error
        import urllib.request

        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/",
            data=urllib.parse.urlencode({"query": query}).encode("utf-8"),
            headers={
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read()

    def describe(self) -> dict[str, Any]:
        return {
            "engine": self.name,
            "setupSeconds": self.setup_seconds,
            "version": tool_version([self.server_binary, "--help"]) if self.server_binary else None,
            "mode": "on-disk index served over HTTP",
            "indexDirectory": str(self.index_dir),
            "indexBuildSeconds": self.index_seconds,
            "memoryLimitGb": self.memory_gb,
            "port": self.port,
            # The exact argv is recorded because QLever's CLI is overridable and
            # varies by release; a report should say what actually ran.
            "commands": {name: list(argv) for name, argv in self.commands.items()},
            "buildStatus": qlever_build_status(),
        }

    def execute(self, query_id: str, query_path: Path) -> dict[str, Any]:
        raw_path = self.raw_dir / f"{query_id}.sparql.json"
        stderr_path = self.raw_dir / f"{query_id}.stderr.txt"
        started = time.monotonic()
        try:
            payload = self._post(
                query_path.read_text(encoding="utf-8"), timeout=self.query_timeout
            )
        except Exception as error:  # noqa: BLE001 - reported as EXECUTION_FAILED
            stderr_path.write_text(str(error), encoding="utf-8")
            raw_path.write_bytes(b"")
            return self._envelope(
                query_id, query_path, returncode=1, started=started,
                raw_path=raw_path, stderr_path=stderr_path, error=str(error),
            )
        raw_path.write_bytes(payload)
        stderr_path.write_bytes(b"")
        return self._envelope(
            query_id, query_path, returncode=0, started=started,
            raw_path=raw_path, stderr_path=stderr_path,
        )

    def stop(self) -> None:
        if self.server is not None and self.server.poll() is None:
            self.server.terminate()
            try:
                self.server.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=30)
        self.server = None
        # The index is large and container-local; remove it as soon as the
        # queries are done rather than waiting for the scratch teardown.
        shutil.rmtree(self.index_dir, ignore_errors=True)


class NativeArtifactEngine(QueryEngine):
    """Base for engines that query a compressed artifact without decoding it.

    The point of HDT and COTTAS is that they are queryable in place. Validating
    them by decoding to N-Triples first proves the decode is faithful but says
    nothing about querying them, and measures the wrong thing entirely for a
    performance comparison.

    When the run's own artifact is already in this engine's format it is used
    directly, which is the honest measurement. Otherwise one is built in scratch
    from the materialized N-Triples, and that build is timed as setup rather
    than as query cost.
    """

    #: Artifact format this engine reads, e.g. "hdt".
    artifact_format = ""

    def __init__(self, source: Path, *, raw_dir: Path, scratch: Path, options: dict[str, Any]):
        super().__init__(source, raw_dir=raw_dir, scratch=scratch, options=options)
        self.artifact: Path | None = None
        self.artifact_origin = "unknown"
        self.log_dir = raw_dir / "engine"

    def _resolve_artifact(self) -> Path:
        """Use the run's own artifact when it matches, else build one."""
        supplied = self.options.get("artifact_path")
        if supplied is not None and self.options.get("artifact_format") == self.artifact_format:
            self.artifact_origin = "run artifact"
            return Path(supplied)
        self.artifact_origin = "built from N-Triples for this engine"
        target = self.scratch / f"{self.name}-engine.{self.artifact_format}"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.build_artifact(target)
        return target

    def build_artifact(self, target: Path) -> None:
        raise NotImplementedError

    def start(self) -> None:
        self.artifact = self._resolve_artifact()

    def describe(self) -> dict[str, Any]:
        return {
            "engine": self.name,
            "setupSeconds": self.setup_seconds,
            "mode": f"native {self.artifact_format} query, no decode",
            "artifact": str(self.artifact) if self.artifact else None,
            "artifactOrigin": self.artifact_origin,
            "artifactSizeBytes": (
                self.artifact.stat().st_size
                if self.artifact and self.artifact.is_file() else None
            ),
        }


class HdtEngine(ComunicaHttpEndpointMixin, NativeArtifactEngine):
    """Query a .hdt artifact in place, from one load, over Comunica's endpoint.

    The one-shot ``comunica-sparql-hdt`` CLI was previously spawned per query,
    so every query re-opened the artifact and re-initialised the engine. HDT is
    memory-mapped rather than parsed, so the per-query cost is far smaller than
    Comunica's N-Triples reload -- but it is still paid 27 times for nothing,
    and it makes the measured query time include engine startup, which is
    exactly what a benchmark must not do.
    """

    name = "hdt"
    artifact_format = "hdt"
    endpoint_binary = "comunica-sparql-hdt-http"
    endpoint_port_option = "hdt_port"
    default_endpoint_port = DEFAULT_HDT_ENDPOINT_PORT
    endpoint_missing_hint = (
        "so HDT cannot be queried natively. Rebuild the image, or validate the "
        "HDT artifact by decoding it (--rdf file.hdt --engine comunica)"
    )

    def __init__(self, source: Path, *, raw_dir: Path, scratch: Path, options: dict[str, Any]):
        super().__init__(source, raw_dir=raw_dir, scratch=scratch, options=options)
        self._init_endpoint(options)

    def build_artifact(self, target: Path) -> None:
        rdf2hdt = _resolve_binary("RDF2HDT_BIN", "rdf2hdt", "/usr/local/bin/rdf2hdt")
        _run_step(
            [rdf2hdt, str(self.source), str(target)],
            label="hdt-engine-build", log_dir=self.log_dir,
        )

    def _endpoint_source_argument(self) -> str:
        # Comunica needs the source type declared: a bare path is treated as a
        # link to dereference and fails with "could not dereference".
        return f"hdt@{self.artifact}"

    def start(self) -> None:
        # Check the binary before resolving the artifact: building an HDT for
        # an engine that cannot run wastes the most expensive step in the run.
        self._require_endpoint_binary()
        # Then resolve (or build) the artifact; the endpoint needs its path.
        NativeArtifactEngine.start(self)
        self._start_endpoint()

    def describe(self) -> dict[str, Any]:
        return {
            **NativeArtifactEngine.describe(self),
            **self.endpoint_describe(),
            "mode": "native hdt query, no decode, single load served over HTTP",
        }


class CottasEngine(NativeArtifactEngine):
    """Query a .cottas artifact in place through pycottas's rdflib store.

    pycottas exposes ``COTTASStore``, an rdflib Store backed by the Parquet
    artifact, so this runs in-process rather than shelling out. The validator
    already runs under the interpreter that owns pycottas.
    """

    name = "cottas"
    artifact_format = "cottas"

    def build_artifact(self, target: Path) -> None:
        import pycottas

        pycottas.rdf2cottas(str(self.source), str(target))

    def start(self) -> None:
        try:
            import pycottas  # noqa: F401
            import rdflib  # noqa: F401
        except ImportError as error:
            raise RuntimeError(
                f"pycottas and rdflib are required to query COTTAS natively: {error}"
            ) from error
        super().start()

        import pycottas
        import rdflib

        self.graph = rdflib.Graph(store=pycottas.COTTASStore(str(self.artifact)))

    def execute(self, query_id: str, query_path: Path) -> dict[str, Any]:
        raw_path = self.raw_dir / f"{query_id}.sparql.json"
        stderr_path = self.raw_dir / f"{query_id}.stderr.txt"
        started = time.monotonic()
        try:
            result = self.graph.query(query_path.read_text(encoding="utf-8"))
            raw_path.write_bytes(result.serialize(format="json"))
            stderr_path.write_bytes(b"")
            returncode, error = 0, None
        except Exception as failure:  # noqa: BLE001 - reported as EXECUTION_FAILED
            raw_path.write_text("{}", encoding="utf-8")
            stderr_path.write_text(str(failure), encoding="utf-8")
            returncode, error = 1, str(failure)
        return self._envelope(
            query_id, query_path, returncode=returncode, started=started,
            raw_path=raw_path, stderr_path=stderr_path, error=error,
        )

    def stop(self) -> None:
        graph = getattr(self, "graph", None)
        if graph is not None:
            try:
                graph.close()
            except Exception:  # noqa: BLE001 - teardown must not mask a result
                pass
            self.graph = None


ENGINE_CLASSES = {
    "comunica": ComunicaEngine,
    "qlever": QleverEngine,
    "hdt": HdtEngine,
    "cottas": CottasEngine,
}


def build_engine(
    name: str, source: Path, *, raw_dir: Path, scratch: Path, options: dict[str, Any]
) -> QueryEngine:
    try:
        engine_class = ENGINE_CLASSES[name]
    except KeyError as error:
        raise ValueError(
            f"Unknown SPARQL engine {name!r}; choose one of {', '.join(SPARQL_ENGINES)}"
        ) from error
    return engine_class(source, raw_dir=raw_dir, scratch=scratch, options=options)


def bindings(path: Path) -> list[dict[str, Any]]:
    try:
        value = read_json(path)["results"]["bindings"]
    except (KeyError, TypeError) as error:
        raise ValueError(f"Invalid SPARQL Results JSON: {path}") from error
    if not isinstance(value, list):
        raise ValueError(f"SPARQL bindings are not a list: {path}")
    return value


def binding_int(binding: dict[str, Any], field: str) -> int:
    return int(binding[field]["value"])


def normalize(query_id: str, path: Path) -> Any:
    fields, integer_fields, sort_fields = QUERY_SCHEMAS[query_id]
    rows: list[dict[str, Any]] = []
    for number, binding in enumerate(bindings(path), start=1):
        row: dict[str, Any] = {}
        for field in fields:
            try:
                lexical = binding[field]["value"]
            except (KeyError, TypeError) as error:
                raise ValueError(f"Row {number} has no {field!r}") from error
            if field in integer_fields:
                try:
                    numeric = Decimal(lexical)
                except InvalidOperation as error:
                    raise ValueError(f"{field} is not numeric: {lexical!r}") from error
                if not numeric.is_finite() or numeric != numeric.to_integral_value():
                    raise ValueError(f"{field} is not an integer: {lexical!r}")
                row[field] = int(numeric)
            else:
                row[field] = str(lexical)
        rows.append(row)
    if sort_fields:
        rows.sort(key=lambda row: tuple(row[field] for field in sort_fields))
        keys = [tuple(row[field] for field in sort_fields) for row in rows]
        if len(keys) != len(set(keys)):
            raise ValueError(f"{query_id} returned duplicate canonical keys")
    if query_id in SINGLE_ROW_QUERIES:
        if len(rows) != 1:
            raise ValueError(f"{query_id} must return exactly one row, got {len(rows)}")
        row = rows[0]
        if query_id == "q03_titv":
            row["tiTvRatio"] = (
                row["transitionCount"] / row["transversionCount"]
                if row["transversionCount"] else None
            )
        return row
    if query_id == "q06_ac_an_distribution":
        for row in rows:
            row["af"] = row["ac"] / row["an"]
    return rows


def anomaly_count(executions: dict[str, dict[str, Any]], query_id: str) -> Any:
    """Exact anomaly total from the companion aggregate, or None if unavailable.

    Returns ``None`` rather than the sample size, so a report never presents a
    truncated count as if it were exact.
    """
    execution = executions.get(f"{query_id}_count")
    if execution is None or execution.get("status") != "PASS":
        return None
    try:
        rows = bindings(Path(execution["rawResult"]))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    if len(rows) != 1:
        return None
    try:
        return binding_int(rows[0], "anomalyCount")
    except (KeyError, TypeError, ValueError):
        return None


def duplicate_triple_report(
    executions: dict[str, dict[str, Any]], parsed_triple_count: int | None
) -> dict[str, Any]:
    """Compare statements parsed against distinct triples stored.

    A SPARQL store deduplicates on load, so no query can see a repeated line.
    The parser counted every statement it read; the store counted the distinct
    ones. Their difference is exactly the number of redundant statements.

    Reports NOT_EVALUATED rather than PASS when either number is unavailable, so
    a missing input can never be mistaken for a clean result.
    """
    execution = executions.get("preflight_distinct_triple_count")
    if parsed_triple_count is None:
        return {
            "status": "NOT_EVALUATED",
            "reason": "the parser did not report a statement count",
        }
    if execution is None or execution.get("status") != "PASS":
        return {
            "status": "NOT_EVALUATED",
            "reason": "the distinct-triple query did not run",
        }
    try:
        rows = bindings(Path(execution["rawResult"]))
        distinct = binding_int(rows[0], "distinctTripleCount")
    except (OSError, ValueError, KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
        return {"status": "NOT_EVALUATED", "reason": f"unreadable result: {error}"}

    duplicates = int(parsed_triple_count) - distinct
    return {
        "status": "PASS" if duplicates == 0 else "FAIL",
        "parsedTripleCount": int(parsed_triple_count),
        "distinctTripleCount": distinct,
        "duplicateTripleCount": duplicates,
    }


def preflight(
    executions: dict[str, dict[str, Any]],
    parser: dict[str, Any],
    representation: str,
    *,
    parsed_triple_count: int | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {}
    report["preflight_duplicate_triples"] = duplicate_triple_report(
        executions, parsed_triple_count
    )
    for query_id in PREFLIGHT_QUERIES:
        execution = executions[query_id]
        if execution["status"] != "PASS":
            report[query_id] = {"status": "EXECUTION_FAILED", "execution": execution}
            continue
        returned = bindings(Path(execution["rawResult"]))
        exact = anomaly_count(executions, query_id)
        if query_id == "preflight_distinct_triple_count":
            # Consumed by duplicate_triple_report above; nothing to judge alone.
            report[query_id] = {"status": "PASS", "rows": len(returned)}
        elif query_id in {
            "preflight_record_cardinality",
            "preflight_position_datatype",
            "preflight_blank_nodes",
            "preflight_empty_values",
        }:
            report[query_id] = {
                "status": "PASS" if not returned else "FAIL",
                "anomalyCount": exact,
                "anomalyCountReturned": len(returned),
                "limitedTo": ANOMALY_SAMPLE_LIMIT,
                "sampleTruncated": len(returned) >= ANOMALY_SAMPLE_LIMIT,
            }
        elif query_id == "preflight_representation_profile":
            # VCF-RDFizer intentionally omits a sample representation profile
            # when a VCF has no sample columns, because no sample graph is
            # emitted. That is not a representation mismatch.
            profile_missing_is_expected = parser["sampleCount"] == 0
            report[query_id] = {
                "status": "PASS" if not returned or profile_missing_is_expected else "FAIL",
                "anomalyCount": exact,
                "anomalyCountReturned": len(returned),
                "limitedTo": ANOMALY_SAMPLE_LIMIT,
                "sampleTruncated": len(returned) >= ANOMALY_SAMPLE_LIMIT,
                "note": "No sample representation is emitted for a sample-free VCF." if returned and profile_missing_is_expected else None,
            }
        elif query_id == "preflight_missing_token_conformance":
            report[query_id] = {
                "status": "PASS" if not returned else "EXPECTED_CONFORMANCE_FAILURE",
                "anomalyCount": exact,
                "plainDotCountReturned": len(returned),
                "limitedTo": ANOMALY_SAMPLE_LIMIT,
                "sampleTruncated": len(returned) >= ANOMALY_SAMPLE_LIMIT,
            }
        elif len(returned) != 1:
            report[query_id] = {"status": "FAIL", "error": f"Expected one aggregate row, got {len(returned)}"}
        elif representation == "expanded":
            actual = {field: binding_int(returned[0], field) for field in ("sampleCallCount", "sampleIdCount", "gtValueNodeCount")}
            expected = {
                "sampleCallCount": parser["sampleCount"] * parser["totalRecords"],
                "sampleIdCount": parser["sampleCount"],
                "gtValueNodeCount": parser["sampleCount"] * parser["gtRecordCount"],
            }
            report[query_id] = {"status": "PASS" if actual == expected else "FAIL", "expected": expected, "actual": actual}
        else:
            actual = {field: binding_int(returned[0], field) for field in ("sampleCount", "sampleIdCount", "gtVectorCount")}
            expected = {"sampleCount": parser["sampleCount"], "sampleIdCount": parser["sampleCount"], "gtVectorCount": parser["gtRecordCount"]}
            report[query_id] = {"status": "PASS" if actual == expected else "FAIL", "expected": expected, "actual": actual}
    return report


def compare_rows(query_id: str, expected: Any, actual: Any) -> dict[str, Any]:
    keys, values = QUERY_SPECS[query_id]
    selected = keys + values
    if not keys:
        expected_value = {field: expected[field] for field in values}
        actual_value = {field: actual[field] for field in values}
        return {
            "status": "PASS" if expected_value == actual_value else "MISMATCH",
            "expected": expected_value,
            "actual": actual_value,
        }
    def index(rows: list[dict[str, Any]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        indexed: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            key = tuple(row[field] for field in keys)
            if key in indexed:
                raise ValueError(f"Duplicate {query_id} key: {key!r}")
            indexed[key] = {field: row[field] for field in selected}
        return indexed
    expected_index, actual_index = index(expected), index(actual)
    missing_keys, extra_keys = set(expected_index) - set(actual_index), set(actual_index) - set(expected_index)
    differing = [
        {"expected": expected_index[key], "actual": actual_index[key]}
        for key in sorted(set(expected_index) & set(actual_index))
        if any(expected_index[key][field] != actual_index[key][field] for field in values)
    ]
    return {
        "status": "PASS" if not missing_keys and not extra_keys and not differing else "MISMATCH",
        "missingRows": [expected_index[key] for key in sorted(missing_keys)],
        "extraRows": [actual_index[key] for key in sorted(extra_keys)],
        "differingRows": differing,
    }


def invariant_checks(payload: dict[str, Any], parser: dict[str, Any], *, check_q06_exact: bool) -> list[dict[str, Any]]:
    checks: list[tuple[str, bool, str]] = []
    total = parser["totalRecords"]
    for query_id, field in (("q01_record_density_1mb", "recordCount"), ("q02_variant_shape_counts", "recordCount"), ("q04_filter_distribution", "recordCount")):
        count = sum(int(row[field]) for row in payload[query_id])
        checks.append((f"{query_id}_total", count == total, f"{count} == {total}"))
    q3 = payload["q03_titv"]
    checks.append(("q03_partition", q3["transitionCount"] + q3["transversionCount"] == q3["biallelicSnvCount"], "transition + transversion == biallelic SNV"))
    if parser["sampleCount"] and parser["gtRecordCount"]:
        per_sample: Counter[str] = Counter()
        for row in payload["q05_sample_genotype_counts"]:
            per_sample[row["sampleId"]] += int(row["callCount"])
        for sample, count in sorted(per_sample.items()):
            checks.append((f"q05_total_{sample}", count == total, f"{count} == {total}"))
        q06_total = sum(int(row["siteCount"]) for row in payload["q06_ac_an_distribution"])
        checks.append(("q06_subset", q06_total <= parser["singleAltRecordCount"], f"{q06_total} <= {parser['singleAltRecordCount']}"))
        if check_q06_exact:
            checks.append(("q06_eligible_total", q06_total == parser["q06EligibleSiteCount"], f"{q06_total} == {parser['q06EligibleSiteCount']}"))
    for row in payload["q06_ac_an_distribution"]:
        checks.append((f"q06_bounds_{row['an']}_{row['ac']}", row["an"] > 0 and 0 <= row["ac"] <= row["an"] and row["siteCount"] > 0, "AN/AC bounds"))
    return [{"name": name, "status": "PASS" if passed else "FAIL", "detail": detail} for name, passed, detail in checks]


def compare(
    parser: dict[str, Any], sparql: dict[str, Any], *, mapping_policy: str = "strict"
) -> dict[str, Any]:
    required = bool(parser["sampleCount"] and parser["gtRecordCount"])
    query_results = {query_id: compare_rows(query_id, parser[query_id], sparql[query_id]) for query_id in CORE_QUERIES}
    if not required:
        for query_id in ("q05_sample_genotype_counts", "q06_ac_an_distribution"):
            query_results[query_id] = {"status": "NOT_APPLICABLE_VERIFIED_NO_SAMPLES_OR_GT", "diagnosticComparison": query_results[query_id]}
    if mapping_policy == "report-only":
        # A custom mapping emits a different inventory and different IRIs by
        # design, so these are reported for inspection but cannot fail a run.
        for query_id in DEFAULT_MAPPING_QUERIES:
            query_results[query_id] = {
                "status": "NOT_APPLICABLE_CUSTOM_MAPPING",
                "diagnosticComparison": query_results[query_id],
            }
    parser_invariants = invariant_checks(parser, parser, check_q06_exact=True)
    sparql_invariants = invariant_checks(sparql, parser, check_q06_exact=False)
    allowed = {
        "PASS",
        "NOT_APPLICABLE_VERIFIED_NO_SAMPLES_OR_GT",
        "NOT_APPLICABLE_CUSTOM_MAPPING",
    }
    passed = all(value["status"] in allowed for value in query_results.values()) and all(item["status"] == "PASS" for item in parser_invariants + sparql_invariants)
    return {"status": "PASS" if passed else "MISMATCH", "queries": query_results, "invariants": {"parser": parser_invariants, "sparql": sparql_invariants}}


# Queries whose failure means the graph's core structure is wrong, so the
# aggregate comparison below them cannot be interpreted.
BLOCKING_PREFLIGHT_QUERIES = (
    "preflight_record_cardinality",
    "preflight_position_datatype",
    "preflight_representation_profile",
    # Graph-integrity checks. A blank node breaks the IRI-template identity the
    # digests rely on; an empty term means a value was lost rather than marked
    # missing; a duplicated statement means the conversion emitted the same
    # data twice. None of these leave a comparison worth interpreting.
    "preflight_blank_nodes",
    "preflight_empty_values",
    "preflight_duplicate_triples",
)


def evaluate_validation(
    executions: dict[str, dict[str, Any]],
    parser: dict[str, Any],
    representation: str,
    *,
    strict_conformance: bool = False,
    mapping_policy: str = "strict",
    parsed_triple_count: int | None = None,
) -> dict[str, Any]:
    """Turn raw query executions into a validation verdict.

    This is the single place the PASS / MISMATCH / BLOCKED_BY_PREFLIGHT
    decision is made. ``run_validation`` uses it for real runs and the mutation
    harness uses it directly, so the harness measures the shipped decision
    logic rather than a reimplementation of it.
    """
    report = preflight(
        executions, parser, representation, parsed_triple_count=parsed_triple_count
    )
    sparql: dict[str, Any] = {}
    failures: dict[str, str] = {}
    for query_id in CORE_QUERIES:
        try:
            sparql[query_id] = normalize(query_id, Path(executions[query_id]["rawResult"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            failures[query_id] = str(error)
    if failures:
        return {
            "status": "EXECUTION_FAILED",
            "preflight": report,
            "normalizationFailures": failures,
            "sparql": sparql,
            "comparison": None,
        }

    comparison = compare(parser, sparql, mapping_policy=mapping_policy)
    # NOT_EVALUATED is not a failure: it means an input the check needs was
    # unavailable, which must not be reported as a bad graph.
    blocking = any(
        report[name]["status"] not in {"PASS", "NOT_EVALUATED"}
        for name in BLOCKING_PREFLIGHT_QUERIES
    )
    inventory_failed = report["preflight_sample_gt_inventory"]["status"] != "PASS"
    conformance_failed = strict_conformance and (
        report["preflight_missing_token_conformance"]["status"] != "PASS"
    )
    if blocking:
        status = "BLOCKED_BY_PREFLIGHT"
    elif inventory_failed or conformance_failed or comparison["status"] != "PASS":
        status = "MISMATCH"
    else:
        status = "PASS"
    return {
        "status": status,
        "preflight": report,
        "normalizationFailures": {},
        "sparql": sparql,
        "comparison": comparison,
        "strictConformanceApplied": bool(strict_conformance),
        "mappingPolicy": mapping_policy,
    }


# ---------------------------------------------------------------------------
# Benchmarking
# ---------------------------------------------------------------------------
# A validation run already measures everything a performance comparison needs:
# it computes the answers twice, once from the VCF with a conventional parser
# and once from RDF with a SPARQL engine, over the same data and to the same
# result. Recording those timings turns each run into a directly comparable
# oracle-versus-SPARQL measurement, and a multi-engine run into a comparison
# between the engines as well.

BENCHMARK_CSV_HEADER = [
    "engine",
    "query_id",
    "status",
    "wall_seconds",
    # The parser's cost for THIS query, comparable row-wise against
    # `wall_seconds`. Empty unless the oracle recorded its phase breakdown.
    "oracle_query_seconds",
    # The parser's total for ALL queries, repeated on every row so the file
    # needs no join. NOT comparable row-wise against `wall_seconds`.
    "oracle_wall_seconds",
    "engine_setup_seconds",
    "artifact_origin",
]


def oracle_query_seconds(
    phases: dict[str, float] | None, query_ids: tuple[str, ...]
) -> dict[str, float]:
    """Attribute the oracle's measured phases to individual queries.

    Not a measurement of each query in isolation -- see
    :data:`ORACLE_SAMPLE_LEVEL_QUERIES` for why that does not exist -- but the
    cost a one-question script would have paid, composed from phases that were
    each measured directly.
    """
    if not phases:
        return {}
    reader_open = phases.get("readerOpenSeconds") or 0.0
    scan = phases.get("scanSeconds") or 0.0
    sample_block = phases.get("sampleBlockSeconds") or 0.0
    assembly = phases.get("assemblySeconds") or 0.0
    record_level_scan = max(scan - sample_block, 0.0)

    out: dict[str, float] = {}
    for query_id in query_ids:
        pays_sample_block = query_id in ORACLE_SAMPLE_LEVEL_QUERIES
        out[query_id] = (
            reader_open
            + (scan if pays_sample_block else record_level_scan)
            + assembly
        )
    return out


def build_benchmark(
    per_engine: dict[str, dict[str, Any]],
    engine_descriptions: dict[str, Any],
    *,
    oracle_seconds: dict[str, float] | None,
    materialization_seconds: float | None,
    shacl_seconds: float | None,
    query_ids: tuple[str, ...],
) -> dict[str, Any]:
    """Assemble per-query and per-engine timings into one report."""
    oracle_seconds = oracle_seconds or {}
    engines: dict[str, Any] = {}
    for name, verdict in per_engine.items():
        executions = verdict.get("executions") or verdict.get("queryExecutions") or {}
        queries = {
            query_id: {
                "status": execution.get("status"),
                "wallSeconds": execution.get("wallSeconds"),
            }
            for query_id, execution in executions.items()
        }
        timed = [
            entry["wallSeconds"] for entry in queries.values()
            if isinstance(entry["wallSeconds"], (int, float))
        ]
        description = engine_descriptions.get(name, {})
        engines[name] = {
            "status": verdict.get("status"),
            "setupSeconds": description.get("setupSeconds"),
            "artifactOrigin": description.get("artifactOrigin"),
            "artifactSizeBytes": description.get("artifactSizeBytes"),
            "querySeconds": sum(timed) if timed else None,
            "slowestQuery": (
                max(queries.items(), key=lambda item: item[1]["wallSeconds"] or 0)[0]
                if timed else None
            ),
            "queries": queries,
        }

    oracle_phases = oracle_seconds.get("phases") or {}
    per_query_oracle = oracle_query_seconds(oracle_phases, query_ids)
    return {
        "oracle": {
            # The parser side of the comparison: what it costs to compute the
            # same answers from the VCF directly.
            "totalSeconds": oracle_seconds.get("total"),
            "vcfParseSeconds": oracle_seconds.get("parse"),
            "censusSeconds": oracle_seconds.get("census"),
            # Directly measured phases of the single pass.
            "phases": oracle_phases or None,
            # Per-query cost a one-question script would have paid, composed
            # from those phases. `sampleLevelQueries` names the ones that pay
            # for the per-sample block, so the attribution is auditable rather
            # than implicit.
            "perQuerySeconds": per_query_oracle or None,
            "sampleLevelQueries": sorted(
                q for q in query_ids if q in ORACLE_SAMPLE_LEVEL_QUERIES
            ) or None,
        },
        "preparation": {
            "materializationSeconds": materialization_seconds,
            "shaclSeconds": shacl_seconds,
        },
        "engines": engines,
        "totals": {
            "oracleSeconds": oracle_seconds.get("total"),
            "engineQuerySeconds": {
                name: value["querySeconds"] for name, value in engines.items()
            },
            "engineSetupSeconds": {
                name: value["setupSeconds"] for name, value in engines.items()
            },
        },
        "queryIds": list(query_ids),
    }


def write_benchmark_csv(path: Path, benchmark: dict[str, Any]) -> Path:
    """Write one row per engine and query, for direct analysis."""
    path.parent.mkdir(parents=True, exist_ok=True)
    oracle_total = benchmark["oracle"]["totalSeconds"]
    per_query = benchmark["oracle"].get("perQuerySeconds") or {}
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=BENCHMARK_CSV_HEADER)
        writer.writeheader()
        for name, engine in benchmark["engines"].items():
            for query_id, entry in engine["queries"].items():
                writer.writerow({
                    "engine": name,
                    "query_id": query_id,
                    "status": entry["status"],
                    "wall_seconds": entry["wallSeconds"],
                    "oracle_query_seconds": per_query.get(query_id),
                    # Repeated on each row so the CSV is usable without a join.
                    "oracle_wall_seconds": oracle_total,
                    "engine_setup_seconds": engine["setupSeconds"],
                    "artifact_origin": engine["artifactOrigin"] or "",
                })
    return path


def compare_engines(per_engine: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Check that every engine that ran produced the same normalized results.

    Engines disagreeing is a finding in its own right: the graph may be correct
    and one engine wrong, as when QLever's literal canonicalisation made a
    datatype preflight fail on it alone. A multi-engine run is the cheapest
    place to notice that, so it is checked rather than assumed.
    """
    usable = {
        name: verdict["sparql"]
        for name, verdict in per_engine.items()
        if verdict.get("status") != "EXECUTION_FAILED" and verdict.get("sparql")
    }
    if len(usable) < 2:
        return {
            "agree": True,
            "comparedEngines": sorted(usable),
            "note": "fewer than two engines produced results, so there is nothing to compare",
            "differences": {},
        }
    reference_name = sorted(usable)[0]
    reference = usable[reference_name]
    differences: dict[str, Any] = {}
    for name in sorted(usable):
        if name == reference_name:
            continue
        differing = [
            query_id for query_id in reference
            if usable[name].get(query_id) != reference[query_id]
        ]
        if differing:
            differences[name] = differing
    return {
        "agree": not differences,
        "comparedEngines": sorted(usable),
        "referenceEngine": reference_name,
        "differences": differences,
    }


def build_manifest(
    args: argparse.Namespace,
    query_dir: Path,
    parser: dict[str, Any],
    *,
    engine_description: dict[str, Any],
    materialization: dict[str, Any],
) -> dict[str, Any]:
    query_paths = sorted({query_path(query_dir, query_id) for query_id in PREFLIGHT_QUERIES + CORE_QUERIES})
    source_rdf_entry = {
        "path": str(args.rdf),
        "sha256": sha256_file(args.rdf),
        "format": args.rdf_format,
    }
    manifest = {
        "datasetId": args.dataset_id,
        "representation": args.representation,
        "commandLine": sys.argv,
        "sourceVcf": {"path": str(args.vcf), "sha256": parser["sourceSha256"]},
        "sourceRdf": source_rdf_entry,
        "engine": engine_description,
        "materialization": materialization,
        "temporaryRdf": {
            "decompressedInsideContainer": bool(materialization.get("materialized")),
            "persisted": False,
            "cleanupConfirmed": True,
        },
        "tools": {
            "python": platform.python_version(),
            "cyvcf2": getattr(cyvcf2, "__version__", None),
            "bcftools": tool_version(["bcftools", "--version"]), "node": tool_version(["node", "--version"]),
            "comunicaQuerySparqlFile": tool_version(["comunica-sparql-file-http", "--version"], table_label="Comunica Engine"),
            "rapper": tool_version(["rapper", "--version"]),
        },
        "queries": {path.stem: {"path": str(path), "sha256": sha256_file(path)} for path in query_paths},
    }
    # Preserve the original key for consumers of standalone gzip-validation
    # manifests while exposing the format-neutral ``sourceRdf`` entry.
    if args.rdf_format == "nt.gz":
        manifest["sourceRdfGzip"] = dict(source_rdf_entry)
    return manifest


def query_path(representation_dir: Path, query_id: str) -> Path:
    """Resolve a representation-specific query, falling back to common RDF queries."""
    candidate = representation_dir / f"{query_id}.rq"
    return candidate if candidate.is_file() else QUERY_ROOT / "common" / f"{query_id}.rq"


def run_validation(args: argparse.Namespace) -> int:
    results_dir = args.results_dir.resolve()
    raw_dir, normalized_dir = results_dir / "raw", results_dir / "normalized"
    raw_dir.mkdir(parents=True, exist_ok=True)
    normalized_dir.mkdir(parents=True, exist_ok=True)
    query_dir = QUERY_ROOT / args.representation
    query_ids = PREFLIGHT_QUERIES + PREFLIGHT_COUNT_QUERIES + CORE_QUERIES
    missing = [name for name in query_ids if not query_path(query_dir, name).is_file()]
    if missing:
        raise RuntimeError(f"Missing {args.representation} validation query files: {', '.join(missing)}")
    progress = ValidationProgress(getattr(args, "progress_path", None), len(query_ids))
    quiet = bool(getattr(args, "quiet", False))
    progress.emit(
        "started",
        completed=0,
        detail=f"{args.representation} validation started",
    )
    summary: dict[str, Any] | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="vcf-rdfizer-validation-", dir=args.scratch_dir) as scratch:
            scratch_path = Path(scratch)
            progress.emit(
                "progress",
                completed=0,
                detail=f"materializing {args.rdf_format} artifact inside container",
            )
            materialize_started = time.monotonic()
            decoded, materialization = materialize_ntriples(
                args.rdf,
                args.rdf_format,
                scratch_path,
                log_dir=raw_dir / "materialization",
            )
            materialization["wallSeconds"] = time.monotonic() - materialize_started
            materialization["ntriplesPath"] = str(decoded)
            progress.emit(
                "progress",
                completed=0,
                detail="parsing source VCF",
            )
            oracle_started = time.monotonic()
            oracle_phases: dict[str, float] = {}
            parser = parse_vcf(
                args.vcf, filter_oracle=args.filter_oracle, timing=oracle_phases
            )
            parse_seconds = time.monotonic() - oracle_started
            census_started = time.monotonic()
            parser = attach_census_expectations(parser, args.representation)
            oracle_phases["censusSeconds"] = time.monotonic() - census_started
            oracle_seconds = {
                "parse": parse_seconds,
                "census": oracle_phases["censusSeconds"],
                "total": time.monotonic() - oracle_started,
                # Phase breakdown, so per-query oracle cost can be attributed
                # from measured parts rather than guessed.
                "phases": dict(oracle_phases),
            }
            parser["oracleSeconds"] = oracle_seconds
            write_json(results_dir / "parser.json", parser)
            progress.emit(
                "progress",
                completed=0,
                detail="validating RDF syntax and cardinality",
            )
            rdf_validation = validate_ntriples(decoded, results_dir)
            shacl_result = None
            if args.shacl_shapes is not None:
                progress.emit("progress", completed=0, detail="validating SHACL shapes")
                shacl_result = validate_shacl(decoded, args.shacl_shapes, results_dir)
            write_json(results_dir / "rdf-validation.json", rdf_validation)
            materialization["decodedTripleCount"] = rdf_validation.get("tripleCount")
            write_json(results_dir / "materialization.json", materialization)
            engine_options = {
                "port": args.qlever_port,
                "memory_gb": args.qlever_memory_gb,
                "startup_timeout": args.qlever_startup_timeout,
                "query_timeout": args.query_timeout,
                "extra_index_args": list(args.qlever_index_arg),
                "extra_server_args": list(args.qlever_server_arg),
                "comunica_port": args.comunica_port,
                "hdt_port": args.hdt_port,
                "comunica_bind_timeout": args.comunica_bind_timeout,
                "comunica_warmup_timeout": args.comunica_warmup_timeout,
            }
            engine_options["artifact_path"] = str(args.rdf)
            engine_options["artifact_format"] = args.rdf_format
            manifest = build_manifest(
                args, query_dir, parser,
                engine_description={"engines": list(args.engines), "options": engine_options},
                materialization=materialization,
            )
            write_json(results_dir / "manifest.json", manifest)
            if rdf_validation["status"] != "PASS":
                summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "BLOCKED_BY_PREFLIGHT", "rdfValidation": rdf_validation}
                return 1
            if shacl_result is not None and shacl_result["status"] == "FAIL":
                # A shape violation means the graph is structurally wrong, so
                # the aggregate comparisons below it cannot be interpreted.
                summary = {
                    "datasetId": args.dataset_id, "representation": args.representation,
                    "status": "BLOCKED_BY_PREFLIGHT", "shacl": shacl_result,
                }
                return 1

            # Each requested engine answers the whole query set independently.
            # They are compared against the same oracle and against each other,
            # and every engine's timings are recorded, which is what makes a
            # multi-engine run usable as a benchmark.
            per_engine: dict[str, dict[str, Any]] = {}
            engine_descriptions: dict[str, Any] = {}
            engine_warnings: dict[str, list[str]] = {}
            for engine_name in args.engines:
                engine_raw_dir = raw_dir / engine_name
                engine_raw_dir.mkdir(parents=True, exist_ok=True)
                engine = build_engine(
                    engine_name, decoded, raw_dir=engine_raw_dir, scratch=scratch_path,
                    options=engine_options,
                )
                progress.emit("progress", completed=0, detail=f"preparing {engine_name} engine")
                if not quiet:
                    print(f"[{args.dataset_id}] preparing {engine_name} engine", flush=True)
                advice = engine_advice(
                    engine_name, decoded, len(query_ids), args.query_timeout
                )
                if advice:
                    engine_warnings.setdefault(engine_name, []).append(advice)
                    eprint(f"[{args.dataset_id}] warning: {advice}")
                try:
                    engine.prepare()
                except (RuntimeError, OSError) as error:
                    per_engine[engine_name] = {
                        "status": "EXECUTION_FAILED",
                        "error": f"{engine_name} engine could not be prepared: {error}",
                    }
                    engine_descriptions[engine_name] = {"engine": engine_name, "error": str(error)}
                    continue

                executions: dict[str, dict[str, Any]] = {}
                abandoned: str | None = None
                engine_started = time.monotonic()
                try:
                    engine_descriptions[engine_name] = engine.describe()
                    for completed, query_id in enumerate(query_ids, start=1):
                        progress.emit(
                            "progress", completed=completed - 1, query_id=query_id,
                            detail=f"{engine_name}: {args.representation}/{query_id}",
                        )
                        if not quiet:
                            print(
                                f"[{args.dataset_id}] running {args.representation}/{query_id}"
                                f" ({engine_name})",
                                flush=True,
                            )
                        executions[query_id] = engine.execute(
                            query_id, query_path(query_dir, query_id)
                        )
                        progress.emit(
                            "progress", completed=completed, query_id=query_id,
                            detail=f"{engine_name}: completed {query_id}",
                        )

                        # A query that fails, times out, or returns the wrong
                        # answer never stops the suite: its verdict is recorded
                        # and the next query runs. Coverage is the point --
                        # "26 of 27 passed, q11 disagreed" is a result, while
                        # "stopped at q01" is not.
                        #
                        # --stop-after-query-timeout opts back into abandoning
                        # the rest, which caps the worst case at one timeout
                        # per engine rather than one per query. It is off by
                        # default now that each engine loads the graph once
                        # instead of once per query, so the remaining queries
                        # no longer each re-pay that load.
                        if (
                            args.stop_after_query_timeout
                            and executions[query_id].get("exitCode") == 124
                        ):
                            abandoned = (
                                f"{query_id} exceeded the {engine.query_timeout}s "
                                f"per-query timeout; skipped "
                                f"{len(query_ids) - completed} further queries "
                                f"rather than spending that timeout on each. "
                                f"Raise --query-timeout, or use an indexed "
                                f"engine or artifact for a graph this size."
                            )
                            break

                        budget = args.validation_time_budget
                        if budget and time.monotonic() - engine_started > budget:
                            abandoned = (
                                f"engine exceeded the {budget}s validation time "
                                f"budget after {completed} of {len(query_ids)} "
                                f"queries. Raise or clear "
                                f"--validation-time-budget to allow more."
                            )
                            break
                finally:
                    engine.stop()

                if abandoned is not None:
                    if not quiet:
                        eprint(f"[{args.dataset_id}] {engine_name}: {abandoned}")
                    progress.emit(
                        "progress", completed=len(query_ids),
                        detail=f"{engine_name}: abandoned",
                    )

                engine_dir = results_dir / "engines" / engine_name
                engine_dir.mkdir(parents=True, exist_ok=True)
                write_json(engine_dir / "query-executions.json", executions)
                if abandoned is not None or any(
                    item["status"] != "PASS" for item in executions.values()
                ):
                    per_engine[engine_name] = {
                        "status": "EXECUTION_FAILED", "queryExecutions": executions,
                    }
                    if abandoned is not None:
                        per_engine[engine_name]["abandoned"] = abandoned
                        per_engine[engine_name]["queriesRun"] = len(executions)
                        per_engine[engine_name]["queriesPlanned"] = len(query_ids)
                    continue

                verdict = evaluate_validation(
                    executions, parser, args.representation,
                    strict_conformance=args.strict_conformance,
                    mapping_policy=args.mapping_policy,
                    parsed_triple_count=rdf_validation.get("tripleCount"),
                )
                write_json(engine_dir / "preflight.json", verdict["preflight"])
                if verdict["status"] != "EXECUTION_FAILED":
                    write_json(engine_dir / "sparql.json", verdict["sparql"])
                    write_json(engine_dir / "comparison.json", verdict["comparison"])
                verdict["executions"] = executions
                per_engine[engine_name] = verdict

            for engine_name, warnings in engine_warnings.items():
                if engine_name in per_engine:
                    per_engine[engine_name]["warnings"] = warnings

            benchmark = build_benchmark(
                per_engine, engine_descriptions,
                oracle_seconds=oracle_seconds,
                materialization_seconds=materialization.get("wallSeconds"),
                shacl_seconds=(shacl_result or {}).get("wallSeconds"),
                query_ids=query_ids,
            )
            write_json(results_dir / "benchmark.json", benchmark)
            write_benchmark_csv(results_dir / "benchmark.csv", benchmark)

            agreement = compare_engines(per_engine)
            write_json(results_dir / "engine-agreement.json", agreement)

            # The primary engine's artifacts keep their historical locations so
            # existing single-engine consumers are unaffected.
            primary = args.engines[0]
            primary_verdict = per_engine[primary]
            report = primary_verdict.get("preflight", {})
            write_json(results_dir / "preflight.json", report)
            if primary_verdict["status"] == "EXECUTION_FAILED":
                summary = {
                    "datasetId": args.dataset_id, "representation": args.representation,
                    "status": "EXECUTION_FAILED",
                    "engine": primary, "engines": list(args.engines),
                    "normalizationFailures": primary_verdict.get("normalizationFailures", {}),
                    "queryExecutions": primary_verdict.get("queryExecutions"),
                    "error": primary_verdict.get("error"),
                    "preflight": report, "benchmark": benchmark["totals"],
                }
                return 1
            sparql = primary_verdict["sparql"]
            for query_id, rows in sparql.items():
                write_json(normalized_dir / f"{query_id}.json", rows)
            write_json(results_dir / "sparql.json", sparql)
            comparison = primary_verdict["comparison"]
            write_json(results_dir / "comparison.json", comparison)

            statuses = {name: value["status"] for name, value in per_engine.items()}
            failed_engines = [name for name, value in statuses.items() if value != "PASS"]
            status = statuses[primary]
            if not failed_engines and not agreement["agree"]:
                # Every engine validated, but they disagree with each other. The
                # graph may be fine and an engine wrong; either way the run
                # cannot be called a pass.
                status = "ENGINE_DISAGREEMENT"
            elif failed_engines:
                status = statuses[primary] if statuses[primary] != "PASS" else "MISMATCH"
            summary = {
                "datasetId": args.dataset_id, "representation": args.representation, "status": status,
                "engine": primary, "engines": list(args.engines),
                "engineStatuses": statuses, "engineAgreement": agreement["agree"],
                "sourceFormat": args.rdf_format,
                "strictConformance": bool(args.strict_conformance),
                "shacl": shacl_result,
                "mappingPolicy": args.mapping_policy,
                "recordCount": parser["totalRecords"], "sampleCount": parser["sampleCount"], "gtRecordCount": parser["gtRecordCount"],
                "preflight": report, "comparisonStatus": comparison["status"],
                "benchmark": benchmark["totals"],
                "results": {"manifest": str(results_dir / "manifest.json"), "parser": str(results_dir / "parser.json"), "sparql": str(results_dir / "sparql.json"), "comparison": str(results_dir / "comparison.json"), "benchmark": str(results_dir / "benchmark.csv")},
            }
            return 0 if status == "PASS" else 1
    except Exception as error:
        summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "EXECUTION_FAILED", "error": str(error)}
        return 1
    finally:
        if summary is None:
            summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "EXECUTION_FAILED", "error": "validation ended without a result"}
        summary.setdefault("engine", args.engines[0])
        summary.setdefault("engines", list(args.engines))
        summary.setdefault("sourceFormat", args.rdf_format)
        summary["temporaryRdf"] = {
            "decompressedInsideContainer": args.rdf_format not in DIRECT_FORMATS,
            "persisted": False,
            "cleanupConfirmed": True,
        }
        write_json(results_dir / "summary.json", summary)
        status = str(summary.get("status", "EXECUTION_FAILED"))
        if status == "EXECUTION_FAILED":
            progress.emit("failed", detail="validation execution failed")
        else:
            progress.emit(
                "complete",
                completed=progress.total if status in {"PASS", "MISMATCH"} else 0,
                detail=f"validation {status.lower()}",
            )
        if not quiet:
            print(json.dumps(summary, indent=2), flush=True)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vcf", type=Path, required=True)
    rdf_group = parser.add_mutually_exclusive_group(required=True)
    rdf_group.add_argument(
        "--rdf",
        type=Path,
        help=(
            "RDF artifact to validate: .nt, .nt.gz, .nt.br, .hdt, .cottas, "
            ".cottas.gz, or .cottas.br"
        ),
    )
    # Retained so existing callers and reports keep working.
    rdf_group.add_argument("--rdf-gz", type=Path, help="Deprecated alias for --rdf (.nt.gz)")
    rdf_group.add_argument("--rdf-nt", type=Path, help="Deprecated alias for --rdf (.nt)")
    parser.add_argument(
        "--rdf-format",
        choices=("auto", *RDF_FORMATS),
        default="auto",
        help="Override artifact format detection (default: infer from the filename)",
    )
    parser.add_argument("--representation", choices=("expanded", "condensed"), required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--filter-oracle", choices=("auto", "bcftools", "cyvcf2"), default="auto")
    parser.add_argument("--scratch-dir", type=Path, default=Path("/work"))
    parser.add_argument(
        "--engine",
        default="comunica",
        help=(
            "SPARQL engine(s), comma-separated, or 'all'. comunica queries the "
            "N-Triples in memory; qlever builds an on-disk index and serves it; "
            "hdt and cottas query those compressed artifacts natively, without "
            "decoding them. Several may be given: each answers the whole query "
            "set, results are compared across engines, and every engine's "
            "timings are recorded for benchmarking. The first is the primary, "
            "whose reports keep the single-engine layout "
            f"(choices: {', '.join(SPARQL_ENGINES)}; default: comunica)"
        ),
    )
    parser.add_argument(
        "--validation-time-budget",
        type=int,
        default=0,
        help=(
            "Wall-clock ceiling in seconds for one engine's whole query set "
            "(default: 0, no ceiling). A backstop for queries that are slow but "
            "never individually time out; the per-query timeout and "
            "--stop-after-query-timeout handle the common case."
        ),
    )
    parser.add_argument(
        "--stop-after-query-timeout",
        dest="stop_after_query_timeout",
        action="store_true",
        default=False,
        help=(
            "Abandon an engine's remaining queries once one exceeds the "
            "per-query timeout. Off by default: a validation suite reports "
            "coverage, so a run is worth far more when every query has a "
            "recorded verdict than when it stops at the first slow one. Turn "
            "this on to cap the worst case at one timeout per engine instead "
            "of one per query"
        ),
    )
    parser.add_argument(
        "--continue-after-query-timeout",
        dest="stop_after_query_timeout",
        action="store_false",
        help=(
            "Run every query even after one times out. This is now the "
            "default; the flag is kept so existing callers keep working"
        ),
    )
    parser.add_argument(
        "--query-timeout",
        type=int,
        default=DEFAULT_QUERY_TIMEOUT,
        help=f"Per-query timeout in seconds (default: {DEFAULT_QUERY_TIMEOUT})",
    )
    parser.add_argument(
        "--qlever-memory-gb",
        type=int,
        default=DEFAULT_QLEVER_MEMORY_GB,
        help=f"QLever index/server memory budget in GiB (default: {DEFAULT_QLEVER_MEMORY_GB})",
    )
    parser.add_argument(
        "--qlever-port",
        type=int,
        default=DEFAULT_QLEVER_PORT,
        help=f"Container-local QLever port (default: {DEFAULT_QLEVER_PORT})",
    )
    parser.add_argument(
        "--comunica-port",
        type=int,
        default=DEFAULT_COMUNICA_PORT,
        help=f"Container-local Comunica endpoint port (default: {DEFAULT_COMUNICA_PORT})",
    )
    parser.add_argument(
        "--hdt-port",
        type=int,
        default=DEFAULT_HDT_ENDPOINT_PORT,
        help=(
            "Container-local port for the native HDT endpoint "
            f"(default: {DEFAULT_HDT_ENDPOINT_PORT})"
        ),
    )
    parser.add_argument(
        "--comunica-bind-timeout",
        type=int,
        default=DEFAULT_COMUNICA_BIND_TIMEOUT,
        help=(
            "Seconds to wait for the Comunica endpoint to bind its port, before "
            f"the graph is read (default: {DEFAULT_COMUNICA_BIND_TIMEOUT})"
        ),
    )
    parser.add_argument(
        "--comunica-warmup-timeout",
        type=int,
        default=DEFAULT_COMUNICA_WARMUP_TIMEOUT,
        help=(
            "Seconds allowed for the Comunica endpoint's warm-up query, which "
            "proves it can read the source before the suite starts "
            f"(default: {DEFAULT_COMUNICA_WARMUP_TIMEOUT})"
        ),
    )
    parser.add_argument(
        "--qlever-startup-timeout",
        type=int,
        default=DEFAULT_QLEVER_STARTUP_TIMEOUT,
        help=(
            "Seconds to wait for the QLever server to answer after indexing "
            f"(default: {DEFAULT_QLEVER_STARTUP_TIMEOUT})"
        ),
    )
    parser.add_argument(
        "--shacl-shapes",
        type=Path,
        default=None,
        help=(
            "Validate the graph against a SHACL shapes file as an independent "
            "structural layer. Off by default: pyshacl loads the whole graph "
            "into memory, so it does not scale to a cohort-sized aggregate"
        ),
    )
    parser.add_argument(
        "--mapping-policy",
        choices=MAPPING_POLICIES,
        default="strict",
        help=(
            "How to treat checks that assume the shipped RML mapping (the "
            "predicate/class census and the record-identity digest): strict "
            "requires an exact match; report-only records them without failing, "
            "which is what a custom mapping needs (default: strict)"
        ),
    )
    parser.add_argument(
        "--strict-conformance",
        action="store_true",
        help=(
            "Treat a missing-token conformance failure (a plain '.' literal not "
            "typed as vcfc:Null) as a validation failure instead of a report-only "
            "observation"
        ),
    )
    parser.add_argument(
        "--qlever-index-arg",
        action="append",
        default=[],
        metavar="ARG",
        help=(
            "Extra argument for QLever's IndexBuilderMain (repeatable). "
            "QLEVER_INDEX_COMMAND replaces the whole command line instead."
        ),
    )
    parser.add_argument(
        "--qlever-server-arg",
        action="append",
        default=[],
        metavar="ARG",
        help=(
            "Extra argument for QLever's ServerMain (repeatable). "
            "QLEVER_SERVER_COMMAND replaces the whole command line instead."
        ),
    )
    parser.add_argument(
        "--progress-path",
        type=Path,
        help="optional JSONL sidecar consumed by the host progress display",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="suppress per-query and summary output on stdout",
    )
    return parser


def parse_engine_list(raw: str) -> list[str]:
    """Parse a comma-separated engine list, preserving order and de-duplicating.

    Order matters: the first engine is the primary, whose reports keep the
    single-engine layout that existing consumers read.
    """
    value = (raw or "").strip()
    if value == "all":
        return list(SPARQL_ENGINES)
    engines: list[str] = []
    for token in value.split(","):
        engine = token.strip()
        if not engine:
            continue
        if engine not in SPARQL_ENGINES:
            raise ValueError(
                f"unknown SPARQL engine '{engine}'; choose from "
                f"{', '.join(SPARQL_ENGINES)}, or 'all'"
            )
        if engine not in engines:
            engines.append(engine)
    if not engines:
        raise ValueError("--engine requires at least one engine")
    return engines


def resolve_args(parser: argparse.ArgumentParser, argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and normalise arguments, collapsing the RDF aliases into --rdf."""
    args = parser.parse_args(argv)
    try:
        args.engines = parse_engine_list(args.engine)
    except ValueError as error:
        parser.error(str(error))
    # Retained as the primary engine's name for reports and callers that read it.
    args.engine = args.engines[0]
    args.vcf = args.vcf.resolve()

    supplied = args.rdf or args.rdf_gz or args.rdf_nt
    args.rdf = supplied.resolve()
    if args.rdf_gz is not None and args.rdf_format == "auto":
        args.rdf_format = "nt.gz"
    if args.rdf_nt is not None and args.rdf_format == "auto":
        args.rdf_format = "nt"
    if args.rdf_format == "auto":
        detected = detect_rdf_format(args.rdf)
        if detected is None:
            parser.error(
                f"Could not infer an RDF format from {args.rdf.name!r}; pass "
                f"--rdf-format with one of: {', '.join(RDF_FORMATS)}"
            )
        args.rdf_format = detected

    if not args.vcf.is_file():
        parser.error(f"VCF does not exist: {args.vcf}")
    if not args.rdf.is_file():
        parser.error(f"RDF artifact does not exist: {args.rdf}")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", args.dataset_id):
        parser.error("--dataset-id may contain only letters, digits, dot, underscore, and hyphen")
    if not args.scratch_dir.is_dir():
        parser.error(f"Scratch directory does not exist: {args.scratch_dir}")
    for name, value in (
        ("--query-timeout", args.query_timeout),
        ("--qlever-memory-gb", args.qlever_memory_gb),
        ("--qlever-startup-timeout", args.qlever_startup_timeout),
        ("--comunica-bind-timeout", args.comunica_bind_timeout),
        ("--comunica-warmup-timeout", args.comunica_warmup_timeout),
    ):
        if value <= 0:
            parser.error(f"{name} must be a positive integer")
    # 0 is meaningful here: no ceiling.
    if args.validation_time_budget < 0:
        parser.error("--validation-time-budget must be zero or a positive integer")
    if not 1 <= args.qlever_port <= 65535:
        parser.error("--qlever-port must be between 1 and 65535")
    if not 1 <= args.comunica_port <= 65535:
        parser.error("--comunica-port must be between 1 and 65535")
    if not 1 <= args.hdt_port <= 65535:
        parser.error("--hdt-port must be between 1 and 65535")
    if len({args.comunica_port, args.qlever_port, args.hdt_port}) != 3:
        parser.error("--comunica-port, --qlever-port and --hdt-port must all differ")
    if args.shacl_shapes is not None:
        args.shacl_shapes = args.shacl_shapes.resolve()
        if not args.shacl_shapes.is_file():
            parser.error(f"SHACL shapes file does not exist: {args.shacl_shapes}")
    if args.progress_path is not None:
        args.progress_path = args.progress_path.resolve()
    return args


def parse_args() -> argparse.Namespace:
    return resolve_args(build_arg_parser())


if __name__ == "__main__":
    raise SystemExit(run_validation(parse_args()))
