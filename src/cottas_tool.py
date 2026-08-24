#!/usr/bin/env python3
"""Small Docker-side adapter for the pycottas conversion and merge API."""

import argparse
import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def cottas_scratch_workspace():
    """Run one pycottas operation with an isolated DuckDB working directory."""
    scratch_root = Path(os.environ.get("COTTAS_SCRATCH_DIR", "/work")).resolve()
    scratch_root.mkdir(parents=True, exist_ok=True)
    original_working_directory = Path.cwd()
    # pycottas defaults to ``pycottas.duckdb`` in the current directory.
    # A fresh directory prevents one chunk from reusing another chunk's
    # database, whose ``quads`` table already exists.
    with tempfile.TemporaryDirectory(prefix="vcf-rdfizer-cottas-", dir=scratch_root) as directory:
        try:
            os.chdir(directory)
            yield
        finally:
            # Leave the directory before TemporaryDirectory removes it.
            os.chdir(original_working_directory)


def main() -> int:
    parser = argparse.ArgumentParser(description="VCF-RDFizer COTTAS adapter")
    subparsers = parser.add_subparsers(dest="command", required=True)

    convert = subparsers.add_parser("convert", help="convert one RDF file to COTTAS")
    convert.add_argument("rdf_path")
    convert.add_argument("cottas_path")
    convert.add_argument("index", nargs="?", default="spo")

    merge = subparsers.add_parser("merge", help="merge two COTTAS files")
    merge.add_argument("left_path")
    merge.add_argument("right_path")
    merge.add_argument("cottas_path")
    merge.add_argument("index", nargs="?", default="spo")

    args = parser.parse_args()
    try:
        import pycottas
    except ImportError as exc:
        print(f"COTTAS dependency is unavailable: {exc}", file=sys.stderr)
        return 127

    if args.command == "convert":
        rdf_path = str(Path(args.rdf_path).resolve())
        cottas_path = str(Path(args.cottas_path).resolve())
        # disk=True keeps parser/index construction from requiring the whole
        # RDF chunk in Python memory.
        with cottas_scratch_workspace():
            pycottas.rdf2cottas(
                rdf_path,
                cottas_path,
                index=args.index,
                disk=True,
            )
        return 0

    left_path = str(Path(args.left_path).resolve())
    right_path = str(Path(args.right_path).resolve())
    cottas_path = str(Path(args.cottas_path).resolve())
    with cottas_scratch_workspace():
        pycottas.cat(
            [left_path, right_path],
            cottas_path,
            index=args.index,
            remove_input_files=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
