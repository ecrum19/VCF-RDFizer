#!/usr/bin/env python3
"""Authoring helper for custom VCF-RDFizer RML mappings.

VCF-RDFizer accepts any RML mapping through ``--rules``, but a custom mapping
has to honour a small contract with the wrapper. This tool makes that contract
discoverable and checkable instead of something you find out on the next
multi-hour run:

``vcf-rdfizer-rules columns``
    Show the TSV sources the pipeline generates and every column each one has,
    so you know what is available to reference.

``vcf-rdfizer-rules init -o my_rules.ttl``
    Copy the shipped default mapping as an annotated starting point.

``vcf-rdfizer-rules check my_rules.ttl``
    Validate a mapping against the contract before running the pipeline: the
    logical-source paths the wrapper can rewrite, the column names it can
    supply, and which ``--sample-representation`` values remain usable.

The contract itself
-------------------
1. Logical sources must use the five canonical container paths verbatim
   (``/data/tsv/records.tsv`` and friends). Full mode processes one VCF at a
   time and rewrites exactly those five strings to the per-input file names, so
   a mapping that hard-codes anything else silently reads the wrong file.
2. Referenced columns must exist in the TSV that ``vcf_as_tsv.sh`` writes.
3. Consuming ``sample_calls.tsv`` or ``sample_format_values.tsv`` beyond the
   four built-in sample maps forces the wrapper to materialize those helper
   tables, which costs one row per variant x sample (x FORMAT key), and is
   incompatible with ``--sample-representation condensed``.

The checks are deliberately lexical rather than a full Turtle parse: the tool
stays dependency-free, and the mistakes worth catching before a long run are
wrong paths and misspelled column names.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

try:
    from vcf_rdfizer import (
        SAMPLE_CALLS_HEADER,
        SAMPLE_FORMAT_HEADER,
        resolve_default_rules_path,
        resolve_sample_workflow,
        sample_support_strategy,
    )
except ImportError as exc:  # pragma: no cover - installed together
    raise SystemExit(f"vcf-rdfizer-rules requires the vcf_rdfizer module: {exc}")


@dataclass(frozen=True)
class TsvSource:
    """One TSV table the pipeline generates for RMLStreamer to read."""

    container_path: str
    description: str
    columns: tuple[str, ...]
    #: A trailing column whose *name* varies per input (the VCF sample header).
    dynamic_tail_column: str | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)


# The first three column lists mirror the headers written by `src/vcf_as_tsv.sh`;
# `test_rules_helper_unit.py` runs that script and asserts they still match, so
# the two cannot drift apart silently. The last two reuse the wrapper's own
# constants directly.
TSV_SOURCES: dict[str, TsvSource] = {
    "records": TsvSource(
        container_path="/data/tsv/records.tsv",
        description="One row per VCF data line.",
        columns=(
            "SOURCE_FILE",
            "ROW_ID",
            "CHROM",
            "POS",
            "ID",
            "REF",
            "ALT",
            "QUAL",
            "FILTER",
            "INFO",
            "FORMAT",
        ),
        dynamic_tail_column="SAMPLES",
        notes=(
            "ROW_ID is a 1-based counter over data lines, stable within one file.",
            "The final column holds every sample's payload separated by spaces. Its"
            " header is the whitespace-joined sample ids from #CHROM, or SAMPLES when"
            " the VCF declares none, so it cannot be referenced by a fixed name.",
            "Genotypes are emitted by the wrapper from this table directly; see"
            " --sample-representation.",
        ),
    ),
    "header_lines": TsvSource(
        container_path="/data/tsv/header_lines.tsv",
        description="One row per '##' meta-information header line.",
        columns=(
            "SOURCE_FILE",
            "HEADER_INDEX",
            "HEADER_KEY",
            "HEADER_VALUE",
            "RAW_LINE",
        ),
        notes=(
            "HEADER_KEY/HEADER_VALUE split on the first '='; RAW_LINE keeps the"
            " original text without the leading '##'.",
        ),
    ),
    "file_metadata": TsvSource(
        container_path="/data/tsv/file_metadata.tsv",
        description="Exactly one row summarising the source VCF.",
        columns=(
            "SOURCE_FILE",
            "FILE_FORMAT",
            "FILE_DATE",
            "SOURCE_SOFTWARE",
            "REFERENCE_GENOME",
            "HEADER_COUNT",
            "RECORD_COUNT",
        ),
    ),
    "sample_calls": TsvSource(
        container_path="/data/tsv/sample_calls.tsv",
        description="Helper table: one row per variant x sample.",
        columns=tuple(SAMPLE_CALLS_HEADER),
        notes=(
            "Header-only for the four built-in sample maps. Referencing it from a"
            " custom map makes the wrapper materialize every row.",
        ),
    ),
    "sample_format_values": TsvSource(
        container_path="/data/tsv/sample_format_values.tsv",
        description="Helper table: one row per variant x sample x FORMAT key.",
        columns=tuple(SAMPLE_FORMAT_HEADER),
        notes=(
            "Header-only for the four built-in sample maps. Referencing it from a"
            " custom map makes the wrapper materialize every row, which is the"
            " largest table the pipeline can produce.",
        ),
    ),
}

CANONICAL_SOURCE_PATHS = {source.container_path for source in TSV_SOURCES.values()}

CSVW_URL_RE = re.compile(r'csvw:url\s+"([^"]*)"')
REFERENCE_RE = re.compile(r'rml:reference\s+"([^"]*)"')
TEMPLATE_RE = re.compile(r'rr:template\s+"((?:[^"\\]|\\.)*)"')
TEMPLATE_VAR_RE = re.compile(r"(?<!\\)\{([^{}]*)\}")
TRIPLES_MAP_RE = re.compile(r"rr:subjectMap|rml:logicalSource")

INIT_HEADER = """\
# Custom VCF-RDFizer RML mapping.
#
# Started from the shipped default mapping. Two rules the wrapper relies on:
#
#   1. Keep the `csvw:url` values exactly as they are. Full mode rewrites those
#      five literal strings to the per-input file names (`<sample>.records.tsv`
#      and so on). A different path is not rewritten and will not be found.
#
#   2. Only reference columns the pipeline actually writes. Run
#      `vcf-rdfizer-rules columns` to see them all.
#
# Validate this file before a long run:
#
#   vcf-rdfizer-rules check <this file>
#
# Then use it with:
#
#   vcf-rdfizer --mode full --rules <this file> ...
#
"""


def all_known_columns() -> dict[str, list[str]]:
    """Map each column name to the sources that provide it."""
    owners: dict[str, list[str]] = {}
    for name, source in TSV_SOURCES.items():
        for column in source.columns:
            owners.setdefault(column, []).append(name)
        if source.dynamic_tail_column:
            owners.setdefault(source.dynamic_tail_column, []).append(name)
    return owners


def referenced_columns(text: str) -> set[str]:
    """Collect every column name the mapping reads, from references and templates."""
    names = set(REFERENCE_RE.findall(text))
    for template in TEMPLATE_RE.findall(text):
        names.update(
            variable.strip() for variable in TEMPLATE_VAR_RE.findall(template) if variable.strip()
        )
    return names


def check_rules(rules_path: Path) -> dict:
    """Validate one mapping against the wrapper contract.

    Returns a report with ``errors`` (the run will not work), ``warnings``
    (it will work but at a cost worth knowing about), and ``info``.
    """
    report: dict = {
        "rules_path": str(rules_path),
        "errors": [],
        "warnings": [],
        "info": [],
        "sources": [],
        "columns": [],
        "sample_representations": {},
    }
    text = rules_path.read_text(encoding="utf-8")

    if not TRIPLES_MAP_RE.search(text):
        report["errors"].append(
            "No rml:logicalSource or rr:subjectMap found; this does not look like an "
            "RML mapping."
        )

    urls = CSVW_URL_RE.findall(text)
    report["sources"] = sorted(set(urls))
    if not urls:
        report["errors"].append(
            'No csvw:url found. Each logical source needs one of: '
            + ", ".join(sorted(CANONICAL_SOURCE_PATHS))
        )
    for url in sorted(set(urls)):
        if url not in CANONICAL_SOURCE_PATHS:
            report["errors"].append(
                f"Logical source '{url}' is not one of the five paths the wrapper "
                "rewrites per input, so it will not point at the generated TSV. Use "
                "one of: " + ", ".join(sorted(CANONICAL_SOURCE_PATHS))
            )

    owners = all_known_columns()
    used = referenced_columns(text)
    report["columns"] = sorted(used)
    for column in sorted(used):
        if column not in owners:
            report["errors"].append(
                f"Column '{column}' is not written by any pipeline TSV. Run "
                "'vcf-rdfizer-rules columns' for the full list."
            )

    unused_sources = CANONICAL_SOURCE_PATHS - set(urls)
    if unused_sources:
        report["info"].append(
            "Sources not used by this mapping: " + ", ".join(sorted(unused_sources))
        )

    strategy = sample_support_strategy(rules_path)
    if strategy == "none":
        report["info"].append(
            "No sample helper tables are consumed; the wrapper emits genotype RDF "
            "directly and no helper rows are materialized."
        )
    elif strategy == "stream":
        report["info"].append(
            "The four built-in sample maps are present unchanged, so the helper "
            "tables stay header-only and genotype RDF is streamed directly."
        )
    else:
        report["warnings"].append(
            "This mapping consumes sample_calls.tsv or sample_format_values.tsv in a "
            "way the wrapper cannot stream, so those helper tables will be "
            "materialized: one row per variant x sample, and one per variant x "
            "sample x FORMAT key. On a cohort VCF that is the largest intermediate "
            "the pipeline can produce."
        )

    for representation in ("expanded", "condensed"):
        try:
            workflow = resolve_sample_workflow(representation, rules_path)
        except ValueError as exc:
            report["sample_representations"][representation] = {
                "usable": False,
                "reason": str(exc),
            }
            continue
        report["sample_representations"][representation] = {
            "usable": True,
            "helper_strategy": workflow.helper_strategy,
            "emitter": workflow.emitter or "none (RML emits genotypes)",
        }

    report["ok"] = not report["errors"]
    return report


def print_report(report: dict) -> None:
    print(f"Checked: {report['rules_path']}")
    print()
    print("Logical sources:")
    for url in report["sources"]:
        mark = "ok " if url in CANONICAL_SOURCE_PATHS else "BAD"
        print(f"  [{mark}] {url}")
    print()
    print(f"Referenced columns ({len(report['columns'])}): {', '.join(report['columns']) or '(none)'}")
    print()
    print("Sample representation compatibility:")
    for representation, detail in report["sample_representations"].items():
        if detail["usable"]:
            print(
                f"  --sample-representation {representation}: usable "
                f"(helper tables: {detail['helper_strategy']}, emitter: {detail['emitter']})"
            )
        else:
            print(f"  --sample-representation {representation}: NOT usable")
            print(f"      {detail['reason']}")

    for label, items in (("Errors", report["errors"]), ("Warnings", report["warnings"]), ("Notes", report["info"])):
        if not items:
            continue
        print()
        print(f"{label}:")
        for item in items:
            print(f"  - {item}")

    print()
    print("Result: OK" if report["ok"] else "Result: FAILED")


def command_columns(args: argparse.Namespace) -> int:
    selected = TSV_SOURCES if args.source is None else {args.source: TSV_SOURCES[args.source]}
    if args.json:
        payload = {
            name: {
                "container_path": source.container_path,
                "description": source.description,
                "columns": list(source.columns),
                "dynamic_tail_column": source.dynamic_tail_column,
                "notes": list(source.notes),
            }
            for name, source in selected.items()
        }
        print(json.dumps(payload, indent=2))
        return 0

    for name, source in selected.items():
        print(f"{name}  ({source.container_path})")
        print(f"  {source.description}")
        print("  Columns:")
        for column in source.columns:
            print(f"    - {column}")
        if source.dynamic_tail_column:
            print(f"    - <sample ids> or {source.dynamic_tail_column} (name varies per input)")
        for note in source.notes:
            print(f"  Note: {note}")
        print()
    return 0


def command_init(args: argparse.Namespace) -> int:
    output = Path(args.output).expanduser()
    if output.exists() and not args.force:
        print(f"error: {output} already exists (use --force to overwrite)", file=sys.stderr)
        return 2
    try:
        default_rules = resolve_default_rules_path(Path(__file__).resolve().parent)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        INIT_HEADER + default_rules.read_text(encoding="utf-8"), encoding="utf-8"
    )
    print(f"Wrote {output} (from {default_rules})")
    print("Next: edit it, then run")
    print(f"  vcf-rdfizer-rules check {output}")
    return 0


def command_check(args: argparse.Namespace) -> int:
    rules_path = Path(args.rules).expanduser()
    if not rules_path.is_file():
        print(f"error: rules file not found: {rules_path}", file=sys.stderr)
        return 2
    try:
        report = check_rules(rules_path)
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print_report(report)
    return 0 if report["ok"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vcf-rdfizer-rules",
        description=(
            "Inspect, scaffold, and validate custom RML mappings for VCF-RDFizer."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  See what columns a mapping can reference:\n"
            "    vcf-rdfizer-rules columns\n"
            "  Start a custom mapping from the shipped default:\n"
            "    vcf-rdfizer-rules init -o my_rules.ttl\n"
            "  Validate it before a long run:\n"
            "    vcf-rdfizer-rules check my_rules.ttl\n"
            "  Then use it:\n"
            "    vcf-rdfizer --mode full -i cohort.vcf.gz --rules my_rules.ttl \\\n"
            "      --rdf-storage-mode plain -o ./results\n"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    columns = subparsers.add_parser(
        "columns", help="List the TSV sources and columns a mapping can reference"
    )
    columns.add_argument(
        "--source", choices=sorted(TSV_SOURCES), default=None, help="Show one source only"
    )
    columns.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    columns.set_defaults(func=command_columns)

    init = subparsers.add_parser(
        "init", help="Write an annotated copy of the default mapping to start from"
    )
    init.add_argument(
        "-o", "--output", default="custom_rules.ttl", help="Destination path (default: custom_rules.ttl)"
    )
    init.add_argument("--force", action="store_true", help="Overwrite an existing file")
    init.set_defaults(func=command_init)

    check = subparsers.add_parser(
        "check", help="Validate a mapping against the wrapper contract"
    )
    check.add_argument("rules", help="Path to the .ttl mapping to check")
    check.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    check.set_defaults(func=command_check)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
