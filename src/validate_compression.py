#!/usr/bin/env python3
"""Validate a generated HDT or COTTAS artifact without writing RDF output.

The validator deliberately compares decoded triple counts with the source
count.  Decoding to ``/dev/stdout`` exercises the reader and keeps the full
N-Triples representation in a pipe rather than creating another large file.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import subprocess
import sys
from pathlib import Path


def is_triple_line(line: bytes) -> bool:
    """Return whether a serialized line represents an N-Triples statement."""
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith(b"#") and stripped.endswith(b".")


def count_nt(path: Path) -> int:
    """Count N-Triples records while transparently reading ``.nt.gz``."""
    opener = gzip.open if path.name.endswith(".gz") else Path.open
    with opener(path, "rb") as handle:
        return sum(1 for line in handle if is_triple_line(line))


def count_decoded(command: list[str]) -> int:
    """Run an RDF exporter and count its streamed N-Triples output."""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    assert process.stdout is not None
    count = sum(1 for line in process.stdout if is_triple_line(line))
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"decoder exited with status {return_code}: {' '.join(command)}")
    return count


def resolve_hdt2rdf() -> str:
    candidates = (
        os.environ.get("HDT2RDF_BIN", ""),
        "/usr/local/bin/hdt2rdf",
        "/opt/hdt-cpp/bin/hdt2rdf",
    )
    for candidate in candidates:
        if candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError("Missing hdt2rdf binary in container")


def validate(args: argparse.Namespace) -> dict:
    source = Path(args.source)
    artifact = Path(args.artifact)
    if not source.is_file():
        raise FileNotFoundError(f"source RDF file not found: {source}")
    if not artifact.is_file() or artifact.stat().st_size == 0:
        raise FileNotFoundError(f"compression artifact is missing or empty: {artifact}")

    source_triples = (
        args.source_triples if args.source_triples is not None else count_nt(source)
    )
    if args.expected_triples is not None and source_triples != args.expected_triples:
        return {
            "valid": False,
            "source_triples": source_triples,
            "decoded_triples": None,
            "expected_triples": args.expected_triples,
            "count_match": False,
            "error": (
                "source triple count does not match the upstream conversion count: "
                f"source={source_triples}, expected={args.expected_triples}"
            ),
        }

    if args.format == "hdt":
        # Loading through the bundled Java launcher also verifies that the HDT
        # structure is readable and eagerly creates the query index.
        if not args.skip_index_check:
            index_helper = Path("/opt/vcf-rdfizer/ensure_hdt_index.sh")
            if not index_helper.is_file():
                raise RuntimeError(f"Missing HDT index helper: {index_helper}")
            index_check = subprocess.run(
                [str(index_helper), str(artifact)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if index_check.returncode != 0:
                raise RuntimeError(
                    "HDT index/readability check failed with status "
                    f"{index_check.returncode}"
                )
        # hdt2rdf uses "-" (not /dev/stdout) as its stdout sentinel. This
        # keeps the decoded RDF in the pipe and avoids another large file.
        decoded_triples = count_decoded([resolve_hdt2rdf(), str(artifact), "-"])
        validator = "hdt2rdf"
    else:
        try:
            import pycottas
        except ImportError as exc:
            raise RuntimeError(f"COTTAS dependency is unavailable: {exc}") from exc

        # pycottas exposes cottas2rdf; its output path can be /dev/stdout, so
        # the COTTAS reader is exercised without materializing decoded RDF.
        decoded_triples = count_decoded(
            [
                sys.executable,
                "-c",
                (
                    "import pycottas, sys; "
                    "pycottas.cottas2rdf(sys.argv[1], '/dev/stdout')"
                ),
                str(artifact),
            ]
        )
        validator = "pycottas.cottas2rdf"

    return {
        "valid": True,
        "source_triples": source_triples,
        "decoded_triples": decoded_triples,
        "expected_triples": args.expected_triples,
        "count_match": source_triples == decoded_triples,
        "validator": validator,
    } | ({
        "error": (
            "decoded triple count does not match the source: "
            f"source={source_triples}, decoded={decoded_triples}"
        )
    } if source_triples != decoded_triples else {})


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate an HDT or COTTAS artifact")
    parser.add_argument("--source", required=True, help="plain or gzip-compressed N-Triples")
    parser.add_argument("--artifact", required=True, help="HDT or COTTAS artifact")
    parser.add_argument("--format", required=True, choices=("hdt", "cottas"))
    parser.add_argument("--expected-triples", type=int)
    parser.add_argument(
        "--source-triples",
        type=int,
        help="source count already collected by an upstream streaming pass",
    )
    parser.add_argument(
        "--skip-index-check",
        action="store_true",
        help="skip HDT index initialization when the caller already performed it",
    )
    parser.add_argument("--result-path", required=True)
    args = parser.parse_args()

    result_path = Path(args.result_path)
    try:
        result = validate(args)
        exit_code = 0 if result.get("valid") and result.get("count_match") else 1
    except Exception as exc:
        result = {
            "valid": False,
            "source_triples": None,
            "decoded_triples": None,
            "expected_triples": args.expected_triples,
            "count_match": False,
            "error": str(exc),
        }
        exit_code = 1

    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if exit_code:
        print(result.get("error", "compression validation failed"), file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
