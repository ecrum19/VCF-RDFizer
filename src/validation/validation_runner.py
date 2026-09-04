#!/usr/bin/env python3
"""Validate a VCF-RDFizer N-Triples graph against its source VCF.

When the input is compressed, it is expanded only to a container-local
temporary file. It is parsed with Raptor, queried with Comunica, and removed in
a finally-safe temporary directory before this process exits. Plain ``.nt``
inputs are read directly and are never copied by the validator.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from cyvcf2 import VCF
import cyvcf2


SCRIPT_DIR = Path(__file__).resolve().parent
QUERY_ROOT = SCRIPT_DIR / "queries"
CORE_QUERIES = (
    "q01_record_density_1mb",
    "q02_variant_shape_counts",
    "q03_titv",
    "q04_filter_distribution",
    "q05_sample_genotype_counts",
    "q06_ac_an_distribution",
)
PREFLIGHT_QUERIES = (
    "preflight_record_cardinality",
    "preflight_position_datatype",
    "preflight_missing_token_conformance",
    "preflight_representation_profile",
    "preflight_sample_gt_inventory",
)
TRANSITIONS = {("A", "G"), ("G", "A"), ("C", "T"), ("T", "C")}

QUERY_SPECS = {
    "q01_record_density_1mb": (("chrom", "windowIndex"), ("recordCount",)),
    "q02_variant_shape_counts": (("variantClass",), ("recordCount",)),
    "q03_titv": ((), ("biallelicSnvCount", "transitionCount", "transversionCount")),
    "q04_filter_distribution": (("filterStatus", "filterLexical"), ("recordCount",)),
    "q05_sample_genotype_counts": (("sampleId", "genotypeClass"), ("callCount",)),
    "q06_ac_an_distribution": (("an", "ac"), ("siteCount",)),
}
QUERY_SCHEMAS = {
    "q01_record_density_1mb": (("chrom", "windowIndex", "recordCount"), {"windowIndex", "recordCount"}, ("chrom", "windowIndex")),
    "q02_variant_shape_counts": (("variantClass", "recordCount"), {"recordCount"}, ("variantClass",)),
    "q03_titv": (("biallelicSnvCount", "transitionCount", "transversionCount"), {"biallelicSnvCount", "transitionCount", "transversionCount"}, ()),
    "q04_filter_distribution": (("filterStatus", "filterLexical", "recordCount"), {"recordCount"}, ("filterStatus", "filterLexical")),
    "q05_sample_genotype_counts": (("sampleId", "genotypeClass", "callCount"), {"callCount"}, ("sampleId", "genotypeClass")),
    "q06_ac_an_distribution": (("an", "ac", "siteCount"), {"an", "ac", "siteCount"}, ("an", "ac")),
}


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


def parse_vcf(vcf_path: Path, *, filter_oracle: str) -> dict[str, Any]:
    use_bcftools = filter_oracle == "bcftools" or (
        filter_oracle == "auto" and shutil.which("bcftools") is not None
    )
    if filter_oracle == "bcftools" and not shutil.which("bcftools"):
        raise RuntimeError("--filter-oracle=bcftools requested but bcftools is unavailable")
    filters = filters_with_bcftools(vcf_path) if use_bcftools else Counter()
    reader = VCF(str(vcf_path), strict_gt=True)
    samples = list(reader.samples)
    density: Counter[tuple[str, int]] = Counter()
    shapes: Counter[str] = Counter()
    genotypes: Counter[tuple[str, str]] = Counter()
    ac_an: Counter[tuple[int, int]] = Counter()
    total_records = gt_records = single_alt_records = q06_eligible = 0
    transition_count = transversion_count = biallelic_snv_count = 0
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
    log.write_text(result.stdout + result.stderr, encoding="utf-8")
    return {"status": "PASS" if result.returncode == 0 else "FAIL", "log": str(log), "exitCode": result.returncode}


def execute_query(query_id: str, source: Path, query_path: Path, raw_dir: Path) -> dict[str, Any]:
    executable = shutil.which("comunica-sparql-file")
    if not executable:
        return {"status": "EXECUTION_FAILED", "error": "comunica-sparql-file is not installed"}
    raw_path = raw_dir / f"{query_id}.sparql.json"
    stderr_path = raw_dir / f"{query_id}.stderr.txt"
    time_path = raw_dir / f"{query_id}.time.txt"
    command = [executable, str(source), "-f", str(query_path), "-t", "application/sparql-results+json"]
    timed = ["/usr/bin/time", "-v", "-o", str(time_path), *command] if tool_version(["/usr/bin/time", "--version"]) else command
    started = time.monotonic()
    with raw_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        result = subprocess.run(timed, check=False, stdout=stdout, stderr=stderr)
    return {
        "status": "PASS" if result.returncode == 0 else "EXECUTION_FAILED",
        "exitCode": result.returncode,
        "wallSeconds": time.monotonic() - started,
        "query": str(query_path),
        "rawResult": str(raw_path),
        "stderr": str(stderr_path),
        "resourceMetrics": str(time_path) if time_path.exists() else None,
    }


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
    if query_id == "q03_titv":
        if len(rows) != 1:
            raise ValueError(f"q03_titv must return exactly one row, got {len(rows)}")
        row = rows[0]
        row["tiTvRatio"] = row["transitionCount"] / row["transversionCount"] if row["transversionCount"] else None
        return row
    if query_id == "q06_ac_an_distribution":
        for row in rows:
            row["af"] = row["ac"] / row["an"]
    return rows


def preflight(executions: dict[str, dict[str, Any]], parser: dict[str, Any], representation: str) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for query_id in PREFLIGHT_QUERIES:
        execution = executions[query_id]
        if execution["status"] != "PASS":
            report[query_id] = {"status": "EXECUTION_FAILED", "execution": execution}
            continue
        returned = bindings(Path(execution["rawResult"]))
        if query_id in {"preflight_record_cardinality", "preflight_position_datatype"}:
            report[query_id] = {"status": "PASS" if not returned else "FAIL", "anomalyCountReturned": len(returned), "limitedTo": 100}
        elif query_id == "preflight_representation_profile":
            # VCF-RDFizer intentionally omits a sample representation profile
            # when a VCF has no sample columns, because no sample graph is
            # emitted. That is not a representation mismatch.
            profile_missing_is_expected = parser["sampleCount"] == 0
            report[query_id] = {
                "status": "PASS" if not returned or profile_missing_is_expected else "FAIL",
                "anomalyCountReturned": len(returned),
                "limitedTo": 100,
                "note": "No sample representation is emitted for a sample-free VCF." if returned and profile_missing_is_expected else None,
            }
        elif query_id == "preflight_missing_token_conformance":
            report[query_id] = {
                "status": "PASS" if not returned else "EXPECTED_CONFORMANCE_FAILURE",
                "plainDotCountReturned": len(returned),
                "limitedTo": 100,
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


def compare(parser: dict[str, Any], sparql: dict[str, Any]) -> dict[str, Any]:
    required = bool(parser["sampleCount"] and parser["gtRecordCount"])
    query_results = {query_id: compare_rows(query_id, parser[query_id], sparql[query_id]) for query_id in CORE_QUERIES}
    if not required:
        for query_id in ("q05_sample_genotype_counts", "q06_ac_an_distribution"):
            query_results[query_id] = {"status": "NOT_APPLICABLE_VERIFIED_NO_SAMPLES_OR_GT", "diagnosticComparison": query_results[query_id]}
    parser_invariants = invariant_checks(parser, parser, check_q06_exact=True)
    sparql_invariants = invariant_checks(sparql, parser, check_q06_exact=False)
    allowed = {"PASS", "NOT_APPLICABLE_VERIFIED_NO_SAMPLES_OR_GT"}
    passed = all(value["status"] in allowed for value in query_results.values()) and all(item["status"] == "PASS" for item in parser_invariants + sparql_invariants)
    return {"status": "PASS" if passed else "MISMATCH", "queries": query_results, "invariants": {"parser": parser_invariants, "sparql": sparql_invariants}}


def build_manifest(args: argparse.Namespace, query_dir: Path, parser: dict[str, Any]) -> dict[str, Any]:
    query_paths = sorted({query_path(query_dir, query_id) for query_id in PREFLIGHT_QUERIES + CORE_QUERIES})
    source_rdf = args.rdf_gz or args.rdf_nt
    source_rdf_format = "nt.gz" if args.rdf_gz else "nt"
    source_rdf_entry = {
        "path": str(source_rdf),
        "sha256": sha256_file(source_rdf),
        "format": source_rdf_format,
    }
    manifest = {
        "datasetId": args.dataset_id,
        "representation": args.representation,
        "commandLine": sys.argv,
        "sourceVcf": {"path": str(args.vcf), "sha256": parser["sourceSha256"]},
        "sourceRdf": source_rdf_entry,
        "temporaryRdf": {
            "decompressedInsideContainer": bool(args.rdf_gz),
            "persisted": False,
            "cleanupConfirmed": True,
        },
        "tools": {
            "python": platform.python_version(), "cyvcf2": cyvcf2.__version__,
            "bcftools": tool_version(["bcftools", "--version"]), "node": tool_version(["node", "--version"]),
            "comunicaQuerySparqlFile": tool_version(["comunica-sparql-file", "--version"], table_label="Comunica Engine"),
            "rapper": tool_version(["rapper", "--version"]),
        },
        "queries": {path.stem: {"path": str(path), "sha256": sha256_file(path)} for path in query_paths},
    }
    # Preserve the original key for consumers of standalone gzip-validation
    # manifests while exposing the format-neutral ``sourceRdf`` entry.
    if args.rdf_gz:
        manifest["sourceRdfGzip"] = {
            "path": str(args.rdf_gz),
            "sha256": sha256_file(args.rdf_gz),
        }
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
    query_ids = PREFLIGHT_QUERIES + CORE_QUERIES
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
            temporary_rdf = bool(args.rdf_gz)
            if temporary_rdf:
                decoded = Path(scratch) / "input.nt"
                progress.emit(
                    "progress",
                    completed=0,
                    detail="decompressing RDF inside container",
                )
                with gzip.open(args.rdf_gz, "rb") as source, decoded.open("wb") as target:
                    shutil.copyfileobj(source, target, length=1024 * 1024)
            else:
                decoded = args.rdf_nt
            progress.emit(
                "progress",
                completed=0,
                detail="parsing source VCF",
            )
            parser = parse_vcf(args.vcf, filter_oracle=args.filter_oracle)
            write_json(results_dir / "parser.json", parser)
            progress.emit(
                "progress",
                completed=0,
                detail="validating RDF syntax and cardinality",
            )
            rdf_validation = validate_ntriples(decoded, results_dir)
            write_json(results_dir / "rdf-validation.json", rdf_validation)
            manifest = build_manifest(args, query_dir, parser)
            write_json(results_dir / "manifest.json", manifest)
            if rdf_validation["status"] != "PASS":
                summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "BLOCKED_BY_PREFLIGHT", "rdfValidation": rdf_validation}
                return 1
            executions: dict[str, dict[str, Any]] = {}
            for completed, query_id in enumerate(query_ids, start=1):
                progress.emit(
                    "progress",
                    completed=completed - 1,
                    query_id=query_id,
                    detail=f"running {args.representation}/{query_id}",
                )
                if not quiet:
                    print(
                        f"[{args.dataset_id}] running {args.representation}/{query_id}",
                        flush=True,
                    )
                executions[query_id] = execute_query(
                    query_id, decoded, query_path(query_dir, query_id), raw_dir
                )
                progress.emit(
                    "progress",
                    completed=completed,
                    query_id=query_id,
                    detail=f"completed {args.representation}/{query_id}",
                )
            write_json(results_dir / "query-executions.json", executions)
            if any(item["status"] != "PASS" for item in executions.values()):
                summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "EXECUTION_FAILED", "queryExecutions": executions}
                return 1
            report = preflight(executions, parser, args.representation)
            write_json(results_dir / "preflight.json", report)
            sparql: dict[str, Any] = {}
            failures: dict[str, str] = {}
            for query_id in CORE_QUERIES:
                try:
                    sparql[query_id] = normalize(query_id, Path(executions[query_id]["rawResult"]))
                    write_json(normalized_dir / f"{query_id}.json", sparql[query_id])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                    failures[query_id] = str(error)
            if failures:
                summary = {"datasetId": args.dataset_id, "representation": args.representation, "status": "EXECUTION_FAILED", "normalizationFailures": failures, "preflight": report}
                return 1
            write_json(results_dir / "sparql.json", sparql)
            comparison = compare(parser, sparql)
            write_json(results_dir / "comparison.json", comparison)
            blocking = any(report[name]["status"] != "PASS" for name in ("preflight_record_cardinality", "preflight_position_datatype", "preflight_representation_profile"))
            inventory_failed = report["preflight_sample_gt_inventory"]["status"] != "PASS"
            status = "BLOCKED_BY_PREFLIGHT" if blocking else "MISMATCH" if inventory_failed or comparison["status"] != "PASS" else "PASS"
            summary = {
                "datasetId": args.dataset_id, "representation": args.representation, "status": status,
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
        summary["temporaryRdf"] = {
            "decompressedInsideContainer": bool(args.rdf_gz),
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vcf", type=Path, required=True)
    rdf_group = parser.add_mutually_exclusive_group(required=True)
    rdf_group.add_argument("--rdf-gz", type=Path, help="N-Triples gzip input (.nt.gz)")
    rdf_group.add_argument("--rdf-nt", type=Path, help="Uncompressed N-Triples input (.nt)")
    parser.add_argument("--representation", choices=("expanded", "condensed"), required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--filter-oracle", choices=("auto", "bcftools", "cyvcf2"), default="auto")
    parser.add_argument("--scratch-dir", type=Path, default=Path("/work"))
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
    args = parser.parse_args()
    args.vcf = args.vcf.resolve()
    if args.rdf_gz is not None:
        args.rdf_gz = args.rdf_gz.resolve()
    if args.rdf_nt is not None:
        args.rdf_nt = args.rdf_nt.resolve()
    if not args.vcf.is_file():
        parser.error(f"VCF does not exist: {args.vcf}")
    if args.rdf_gz is not None and (not args.rdf_gz.is_file() or not args.rdf_gz.name.endswith(".nt.gz")):
        parser.error("--rdf-gz must be an existing .nt.gz file")
    if args.rdf_nt is not None and (not args.rdf_nt.is_file() or not args.rdf_nt.name.endswith(".nt")):
        parser.error("--rdf-nt must be an existing .nt file")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", args.dataset_id):
        parser.error("--dataset-id may contain only letters, digits, dot, underscore, and hyphen")
    if not args.scratch_dir.is_dir():
        parser.error(f"Scratch directory does not exist: {args.scratch_dir}")
    if args.progress_path is not None:
        args.progress_path = args.progress_path.resolve()
    return args


if __name__ == "__main__":
    raise SystemExit(run_validation(parse_args()))
