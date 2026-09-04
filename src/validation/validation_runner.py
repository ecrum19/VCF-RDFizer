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
import gzip
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
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

VCFR = "https://w3id.org/vcf-rdfizer/vocab#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _nonzero(counts: dict[str, int]) -> dict[str, int]:
    return {key: value for key, value in counts.items() if value}


#: Field separator for the record digest. U+001F cannot appear in a VCF field,
#: so no shift of a field boundary can make two different records collide. The
#: query in q11_record_digest.rq writes it as a SPARQL \\u001F escape.
def rml_uri_component(value: str) -> str:
    """Encode a template value the way RMLStreamer 2.5.0 does.

    Mirrors ``_rml_uri_component`` in ``vcf_rdfizer.py``; that module is not on
    the container's import path, and ``test_validation_logic_unit.py`` asserts
    the two agree so they cannot drift.
    """
    encoded = quote_plus(value, safe="*-._", encoding="utf-8", errors="strict")
    # urllib leaves '~' unescaped; java.net.URLEncoder does not.
    return encoded.replace("+", "%20").replace("~", "%7E")


DIGEST_SEPARATOR = "\u001f"
#: Hash prefix length in hex characters. Two gives 256 buckets: enough to make
#: an accidental collision of a *changed* record with its own bucket unlikely
#: to hide anything, while keeping the result fixed-size for any graph.
DIGEST_BUCKET_CHARS = 2


def record_digest_bucket(fields: list[str]) -> str:
    """Bucket one record exactly as q11_record_digest.rq does."""
    joined = DIGEST_SEPARATOR.join(fields)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:DIGEST_BUCKET_CHARS]


def expected_census(
    parser: dict[str, Any], representation: str, *, info_representation: str = "structured",
    header_representation: str = "structured",
) -> dict[str, list[dict[str, Any]]]:
    """Predicate and class counts the graph must contain, and nothing else."""
    records = parser["totalRecords"]
    samples = parser["sampleCount"]
    header_lines = parser["headerLineCount"]

    classes: dict[str, int] = {
        f"{VCFR}VCFFile": 1,
        f"{VCFR}VCFHeader": 1,
        f"{VCFR}HeaderLine": header_lines,
        f"{VCFR}VCFRecord": records,
        f"{VCFR}VariantCall": records,
    }
    predicates: dict[str, int] = {
        f"{VCFR}hasHeader": 1,
        f"{VCFR}hasHeaderLine": header_lines,
        f"{VCFR}headerKey": header_lines,
        f"{VCFR}headerValue": parser["headerValueCount"],
        f"{VCFR}hasRecord": records,
        f"{VCFR}chrom": records,
        f"{VCFR}pos": records,
        f"{VCFR}recordId": records,
        f"{VCFR}ref": records,
        f"{VCFR}alt": records,
        f"{VCFR}hasCall": records,
        f"{VCFR}qual": records,
        f"{VCFR}filter": records,
        f"{VCFR}infoRaw": records,
        f"{VCFR}formatRaw": parser["recordsWithFormatColumn"],
    }
    # RMLStreamer emits nothing for an absent value, so an undeclared
    # meta-information line contributes no triple rather than an empty one.
    for field, predicate in (
        ("fileFormat", f"{VCFR}fileFormat"),
        ("referenceGenome", f"{VCFR}referenceGenome"),
        ("sourceSoftware", f"{VCFR}sourceSoftware"),
    ):
        predicates[predicate] = 0 if parser[field] == METADATA_ABSENT else 1

    if samples:
        predicates[f"{VCFR}representationProfile"] = 1
        if representation == "expanded":
            classes[f"{VCFR}SampleCall"] = records * samples
            classes[f"{VCFR}FormatFieldValue"] = parser["formatValueSlots"]
            predicates[f"{VCFR}hasSampleCall"] = records * samples
            predicates[f"{VCFR}sampleId"] = records * samples
            predicates[f"{VCFR}hasFormatValue"] = parser["formatValueSlots"]
            predicates[f"{VCFR}fieldValue"] = parser["nonEmptyFormatValues"]
        else:
            definitions = parser["distinctFormatKeyCount"]
            classes[f"{VCFR}SampleSet"] = 1
            classes[f"{VCFR}VCFSample"] = samples
            classes[f"{VCFR}CohortCallMatrix"] = parser["recordsWithFormatKeys"]
            classes[f"{VCFR}FormatValueVector"] = parser["formatKeyOccurrences"]
            classes[f"{VCFR}FormatFieldDefinition"] = definitions
            predicates[f"{VCFR}hasSampleSet"] = 1
            predicates[f"{VCFR}hasSample"] = samples
            predicates[f"{VCFR}sampleName"] = samples
            predicates[f"{VCFR}sampleIndex"] = samples
            predicates[f"{VCFR}hasCallMatrix"] = parser["recordsWithFormatKeys"]
            predicates[f"{VCFR}appliesToSampleSet"] = parser["recordsWithFormatKeys"]
            predicates[f"{VCFR}hasFormatValueVector"] = parser["formatKeyOccurrences"]
            predicates[f"{VCFR}declaredBy"] = parser["formatKeyOccurrences"]
            predicates[f"{VCFR}valueEncoding"] = parser["formatKeyOccurrences"]
            predicates[f"{VCFR}encodedValues"] = parser["formatKeyOccurrences"]
            predicates[f"{VCFR}fieldId"] = definitions
            predicates[f"{VCFR}fieldNumber"] = definitions
            predicates[f"{VCFR}fieldDescription"] = definitions

    predicates[f"{VCFR}fileDate"] = 0 if parser["fileDate"] == METADATA_ABSENT else 1

    if header_representation == "structured":
        for class_name, count in parser["headerLineClassCounts"].items():
            classes[f"{VCFR}{class_name}"] = classes.get(f"{VCFR}{class_name}", 0) + count
        # A FILTER/ALT line carries both its header-line subclass and its
        # definition class, so it contributes two rdf:type triples.
        classes[f"{VCFR}FilterDefinition"] = parser["filterDefinitionCount"]
        classes[f"{VCFR}AltDefinition"] = parser["altDefinitionCount"]
        predicates[f"{VCFR}filterId"] = parser["filterDefinitionCount"]
        predicates[f"{VCFR}altId"] = parser["altDefinitionCount"]
        predicates[f"{VCFR}contigId"] = parser["contigCount"]
        predicates[f"{VCFR}contigCount"] = 1 if parser["contigCount"] else 0
        for attribute, predicate in (
            ("length", "contigLength"), ("md5", "contigMd5"), ("assembly", "contigAssembly"),
        ):
            predicates[f"{VCFR}{predicate}"] = parser["contigAttributeCounts"].get(attribute, 0)
        predicates[f"{VCFR}fieldDescription"] = (
            predicates.get(f"{VCFR}fieldDescription", 0)
            + parser["describedDefinitionCount"]
        )

    if info_representation == "structured":
        values = parser["infoValueCount"]
        definitions = parser["infoDefinitionCount"]
        classes[f"{VCFR}InfoFieldValue"] = values
        classes[f"{VCFR}InfoFieldDefinition"] = definitions
        predicates[f"{VCFR}hasInfoValue"] = values
        predicates[f"{VCFR}fieldValueBoolean"] = parser["infoFlagCount"]
        predicates[f"{VCFR}fieldValueInteger"] = parser["infoTypedIntegerCount"]
        predicates[f"{VCFR}fieldValueDecimal"] = parser["infoTypedDecimalCount"]
        predicates[f"{VCFR}fieldType"] = definitions
        # declaredBy and the field* descriptors are shared with the condensed
        # FORMAT definitions, so accumulate rather than overwrite.
        for predicate, count in (
            (f"{VCFR}declaredBy", values),
            (f"{VCFR}fieldId", definitions),
            (f"{VCFR}fieldNumber", definitions),
            (f"{VCFR}fieldDescription", definitions),
        ):
            predicates[predicate] = predicates.get(predicate, 0) + count
        predicates[f"{VCFR}fieldValue"] = (
            predicates.get(f"{VCFR}fieldValue", 0) + values - parser["infoFlagCount"]
        )

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


#: '##' key (lower-cased) -> vocabulary subclass. Mirrors HEADER_LINE_CLASSES
#: in vcf_rdfizer.py; the unit tests assert the two stay identical.
HEADER_LINE_CLASSES = {
    "fileformat": "FileFormatHeaderLine",
    "filedate": "FileDateHeaderLine",
    "source": "SourceHeaderLine",
    "reference": "ReferenceHeaderLine",
    "info": "INFOHeaderLine",
    "format": "FORMATHeaderLine",
    "filter": "FILTERHeaderLine",
    "alt": "ALTHeaderLine",
    "contig": "ContigHeaderLine",
}


def parse_structured_header_fields(value: str) -> dict[str, str]:
    """Parse '<ID=..,Description="..">' attributes, respecting quoting.

    Mirrors ``_parse_structured_header_fields`` in ``vcf_rdfizer.py``; the unit
    tests assert the two agree.
    """
    inner = value.strip()
    if inner.startswith("<") and inner.endswith(">"):
        inner = inner[1:-1]
    tokens: list[str] = []
    token: list[str] = []
    in_quotes = escaped = False
    for character in inner:
        if escaped:
            token.append(character)
            escaped = False
        elif character == "\\" and in_quotes:
            token.append(character)
            escaped = True
        elif character == '"':
            token.append(character)
            in_quotes = not in_quotes
        elif character == "," and not in_quotes:
            tokens.append("".join(token))
            token = []
        else:
            token.append(character)
    tokens.append("".join(token))
    fields: dict[str, str] = {}
    for item in tokens:
        if "=" not in item:
            continue
        key, raw_value = item.split("=", 1)
        parsed = raw_value.strip()
        if len(parsed) >= 2 and parsed[0] == parsed[-1] == '"':
            parsed = parsed[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        fields[key.strip()] = parsed
    return fields


def parse_info_entries(info: str) -> list[tuple[str, str | None]]:
    """Split an INFO column into ``(key, value)`` pairs; a bare key is a Flag.

    Mirrors ``parse_info_entries`` in ``vcf_rdfizer.py``. The two are asserted
    to agree in ``test_validation_logic_unit.py``.
    """
    if info in ("", "."):
        return []
    entries: list[tuple[str, str | None]] = []
    for item in info.split(";"):
        if not item:
            continue
        key, separator, value = item.partition("=")
        entries.append((key, value if separator else None))
    return entries


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
    # Header-representation counters. The class map mirrors HEADER_LINE_CLASSES
    # in vcf_rdfizer.py; the two are asserted to agree in the unit tests.
    header_classes: Counter[str] = Counter()
    filter_definitions = alt_definitions = contigs = described = 0
    contig_attributes: Counter[str] = Counter()
    for line in raw_header.splitlines():
        if not line.startswith("##"):
            continue
        key, _, value = line[2:].partition("=")
        class_name = HEADER_LINE_CLASSES.get(key.lower())
        if class_name is None:
            continue
        header_classes[class_name] += 1
        if key.lower() not in {"filter", "alt", "contig"}:
            continue
        fields = parse_structured_header_fields(value)
        if not (fields.get("ID") or "").strip():
            continue
        if key.lower() == "contig":
            contigs += 1
            for attribute in ("length", "md5", "assembly"):
                if fields.get(attribute):
                    contig_attributes[attribute] += 1
            continue
        if key.lower() == "filter":
            filter_definitions += 1
        else:
            alt_definitions += 1
        if fields.get("Description"):
            described += 1
    metadata["headerLineClassCounts"] = dict(header_classes)
    metadata["filterDefinitionCount"] = filter_definitions
    metadata["altDefinitionCount"] = alt_definitions
    metadata["contigCount"] = contigs
    metadata["contigAttributeCounts"] = dict(contig_attributes)
    metadata["describedDefinitionCount"] = described
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


def parse_vcf(vcf_path: Path, *, filter_oracle: str) -> dict[str, Any]:
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
    reader = VCF(str(vcf_path), strict_gt=True)
    samples = list(reader.samples)
    header_metadata = parse_header_metadata(reader.raw_header)
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
    declared_info_types = info_declared_types(reader.raw_header)
    info_definitions: set[str] = set()
    info_values = info_flags = info_typed_integers = info_typed_decimals = 0
    source_component = rml_uri_component(vcf_path.name)
    records_with_format_column = records_with_format_keys = 0
    format_key_occurrences = format_value_slots = non_empty_format_values = 0
    distinct_format_keys: set[str] = set()
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

            # `vcfr:formatRaw` comes from the FORMAT column itself, so it is
            # present whenever that column is non-empty - samples or not.
            if format_keys:
                records_with_format_column += 1

            columns = str(variant).rstrip("\r\n").split("\t")
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
    finally:
        reader.close()
    if sum(filters.values()) != total_records:
        raise ValueError("FILTER oracle record count differs from the VCF record count")
    q05 = [
        {"sampleId": sample, "genotypeClass": genotype_class, "callCount": int(count)}
        for (sample, genotype_class), count in sorted(genotypes.items())
    ]
    return {
        "source": str(vcf_path),
        "sourceSha256": sha256_file(vcf_path),
        "filterOracle": "bcftools" if use_bcftools else "cyvcf2-serialization",
        "headerLineCount": header_metadata["headerLineCount"],
        "fileDate": header_metadata["fileDate"],
        "headerLineClassCounts": header_metadata["headerLineClassCounts"],
        "filterDefinitionCount": header_metadata["filterDefinitionCount"],
        "altDefinitionCount": header_metadata["altDefinitionCount"],
        "contigCount": header_metadata["contigCount"],
        "contigAttributeCounts": header_metadata["contigAttributeCounts"],
        "describedDefinitionCount": header_metadata["describedDefinitionCount"],
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


def _run_step(
    command: list[str], *, label: str, log_dir: Path, env: dict[str, str] | None = None
) -> None:
    """Run one decode step, preserving its output for diagnosis on failure."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{label}.log"
    with log_path.open("wb") as log:
        result = subprocess.run(
            command, check=False, stdout=log, stderr=subprocess.STDOUT, env=env
        )
    if result.returncode != 0:
        tail = ""
        try:
            tail = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
        except OSError:
            pass
        raise RuntimeError(
            f"{label} failed with exit code {result.returncode}. Output tail: {tail}"
        )


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
        provenance["steps"].append({"tool": "python-gzip", "output": str(target)})
        return target, provenance

    if rdf_format == "nt.br":
        brotli = _resolve_binary("BROTLI_BIN", "brotli")
        _run_step([brotli, "-d", "-c", "-o", str(target), str(source)],
                  label="brotli-decompress", log_dir=log_dir)
        provenance["steps"].append({"tool": brotli, "output": str(target)})
        return target, provenance

    if rdf_format == "hdt":
        hdt2rdf = _resolve_binary("HDT2RDF_BIN", "hdt2rdf", "/usr/local/bin/hdt2rdf")
        _run_step([hdt2rdf, str(source), str(target)], label="hdt2rdf", log_dir=log_dir)
        provenance["steps"].append({"tool": hdt2rdf, "output": str(target)})
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

SPARQL_ENGINES = ("comunica", "qlever")
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

    def __enter__(self) -> "QueryEngine":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.stop()
        return False

    def start(self) -> None:
        """Prepare the engine. Raise RuntimeError when it cannot be used."""

    def stop(self) -> None:
        """Release anything ``start`` acquired. Must be safe to call twice."""

    def describe(self) -> dict[str, Any]:
        return {"engine": self.name}

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


class ComunicaEngine(QueryEngine):
    """Query the N-Triples file directly with comunica-sparql-file."""

    name = "comunica"

    def start(self) -> None:
        self.executable = shutil.which("comunica-sparql-file")
        if not self.executable:
            raise RuntimeError(
                "comunica-sparql-file is not installed in this image; rebuild it or "
                "select --engine qlever"
            )

    def describe(self) -> dict[str, Any]:
        return {
            "engine": self.name,
            "version": tool_version(
                ["comunica-sparql-file", "--version"], table_label="Comunica Engine"
            ),
            "mode": "in-memory file query",
        }

    def execute(self, query_id: str, query_path: Path) -> dict[str, Any]:
        raw_path = self.raw_dir / f"{query_id}.sparql.json"
        stderr_path = self.raw_dir / f"{query_id}.stderr.txt"
        time_path = self.raw_dir / f"{query_id}.time.txt"
        command = [
            self.executable,
            str(self.source),
            "-f",
            str(query_path),
            "-t",
            "application/sparql-results+json",
        ]
        timed = (
            ["/usr/bin/time", "-v", "-o", str(time_path), *command]
            if tool_version(["/usr/bin/time", "--version"])
            else command
        )
        started = time.monotonic()
        try:
            with raw_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                result = subprocess.run(
                    timed, check=False, stdout=stdout, stderr=stderr,
                    timeout=self.query_timeout,
                )
            returncode = result.returncode
            error = None
        except subprocess.TimeoutExpired:
            returncode = 124
            error = f"query exceeded {self.query_timeout}s"
        return self._envelope(
            query_id, query_path, returncode=returncode, started=started,
            raw_path=raw_path, stderr_path=stderr_path, time_path=time_path, error=error,
        )


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


ENGINE_CLASSES = {"comunica": ComunicaEngine, "qlever": QleverEngine}


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
            "comunicaQuerySparqlFile": tool_version(["comunica-sparql-file", "--version"], table_label="Comunica Engine"),
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
            decoded, materialization = materialize_ntriples(
                args.rdf,
                args.rdf_format,
                scratch_path,
                log_dir=raw_dir / "materialization",
            )
            materialization["ntriplesPath"] = str(decoded)
            progress.emit(
                "progress",
                completed=0,
                detail="parsing source VCF",
            )
            parser = attach_census_expectations(
                parse_vcf(args.vcf, filter_oracle=args.filter_oracle), args.representation
            )
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
            }
            engine = build_engine(
                args.engine, decoded, raw_dir=raw_dir, scratch=scratch_path,
                options=engine_options,
            )
            manifest = build_manifest(
                args, query_dir, parser,
                engine_description={"engine": args.engine, "options": engine_options},
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
            executions: dict[str, dict[str, Any]] = {}
            progress.emit("progress", completed=0, detail=f"preparing {args.engine} engine")
            if not quiet:
                print(f"[{args.dataset_id}] preparing {args.engine} engine", flush=True)
            try:
                engine.start()
            except (RuntimeError, OSError) as error:
                summary = {
                    "datasetId": args.dataset_id, "representation": args.representation,
                    "status": "EXECUTION_FAILED",
                    "error": f"{args.engine} engine could not be prepared: {error}",
                }
                return 1
            try:
                engine_description = engine.describe()
                manifest["engine"] = engine_description
                write_json(results_dir / "manifest.json", manifest)
                for completed, query_id in enumerate(query_ids, start=1):
                    progress.emit(
                        "progress",
                        completed=completed - 1,
                        query_id=query_id,
                        detail=f"running {args.representation}/{query_id}",
                    )
                    if not quiet:
                        print(
                            f"[{args.dataset_id}] running {args.representation}/{query_id}"
                            f" ({args.engine})",
                            flush=True,
                        )
                    executions[query_id] = engine.execute(
                        query_id, query_path(query_dir, query_id)
                    )
                    progress.emit(
                        "progress",
                        completed=completed,
                        query_id=query_id,
                        detail=f"completed {args.representation}/{query_id}",
                    )
            finally:
                engine.stop()
            write_json(results_dir / "query-executions.json", executions)
            if any(item["status"] != "PASS" for item in executions.values()):
                summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "EXECUTION_FAILED", "queryExecutions": executions}
                return 1
            verdict = evaluate_validation(
                executions, parser, args.representation,
                strict_conformance=args.strict_conformance,
                mapping_policy=args.mapping_policy,
                parsed_triple_count=rdf_validation.get("tripleCount"),
            )
            report = verdict["preflight"]
            write_json(results_dir / "preflight.json", report)
            if verdict["status"] == "EXECUTION_FAILED":
                summary = {
                    "datasetId": args.dataset_id, "representation": args.representation,
                    "status": "EXECUTION_FAILED",
                    "normalizationFailures": verdict["normalizationFailures"],
                    "preflight": report,
                }
                return 1
            sparql = verdict["sparql"]
            for query_id, rows in sparql.items():
                write_json(normalized_dir / f"{query_id}.json", rows)
            write_json(results_dir / "sparql.json", sparql)
            comparison = verdict["comparison"]
            write_json(results_dir / "comparison.json", comparison)
            status = verdict["status"]
            summary = {
                "datasetId": args.dataset_id, "representation": args.representation, "status": status,
                "engine": args.engine, "sourceFormat": args.rdf_format,
                "strictConformance": bool(args.strict_conformance),
                "shacl": shacl_result,
                "mappingPolicy": args.mapping_policy,
                "recordCount": parser["totalRecords"], "sampleCount": parser["sampleCount"], "gtRecordCount": parser["gtRecordCount"],
                "preflight": report, "comparisonStatus": comparison["status"],
                "results": {"manifest": str(results_dir / "manifest.json"), "parser": str(results_dir / "parser.json"), "sparql": str(results_dir / "sparql.json"), "comparison": str(results_dir / "comparison.json")},
            }
            return 0 if status == "PASS" else 1
    except Exception as error:
        summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "EXECUTION_FAILED", "error": str(error)}
        return 1
    finally:
        if summary is None:
            summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "EXECUTION_FAILED", "error": "validation ended without a result"}
        summary.setdefault("engine", args.engine)
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
        choices=SPARQL_ENGINES,
        default="comunica",
        help=(
            "SPARQL engine: comunica queries the file in memory; qlever builds "
            "an on-disk index and serves it (default: comunica)"
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
            "typed as vcfr:Null) as a validation failure instead of a report-only "
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


def resolve_args(parser: argparse.ArgumentParser, argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and normalise arguments, collapsing the RDF aliases into --rdf."""
    args = parser.parse_args(argv)
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
    ):
        if value <= 0:
            parser.error(f"{name} must be a positive integer")
    if not 1 <= args.qlever_port <= 65535:
        parser.error("--qlever-port must be between 1 and 65535")
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
