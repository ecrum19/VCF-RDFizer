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

    merge_many = subparsers.add_parser(
        "merge-many",
        help="merge multiple COTTAS files in one indexed pass (explicit use)",
    )
    merge_many.add_argument(
        "--input-cottas-files",
        nargs="+",
        required=True,
        help="COTTAS inputs to merge",
    )
    merge_many.add_argument("--output-cottas-file", required=True)
    merge_many.add_argument("--index", default="spo")

    reindex = subparsers.add_parser(
        "reindex",
        help="rebuild the embedded COTTAS query index in place",
    )
    reindex.add_argument("cottas_path")
    reindex.add_argument("index", nargs="?", default="spo")

    decompress = subparsers.add_parser("decompress", help="convert COTTAS to RDF")
    decompress.add_argument("cottas_path")
    decompress.add_argument("rdf_path")

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

    if args.command == "decompress":
        cottas_path = str(Path(args.cottas_path).resolve())
        rdf_path = str(Path(args.rdf_path).resolve())
        # Keep DuckDB scratch state in the container-local workspace while
        # pycottas writes the decoded RDF directly to the mounted output.
        with cottas_scratch_workspace():
            pycottas.cottas2rdf(cottas_path, rdf_path)
        return 0

    if args.command == "reindex":
        cottas_path = Path(args.cottas_path).resolve()
        if not cottas_path.is_file():
            print(f"COTTAS file not found: {cottas_path}", file=sys.stderr)
            return 2

        # COTTAS indexes are part of the Parquet artifact rather than sibling
        # files. Rebuild into a temporary file in the same directory, then
        # replace the original only after pycottas has completed successfully.
        temporary_path = None
        try:
            file_descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{cottas_path.name}.reindex-",
                suffix=".cottas",
                dir=str(cottas_path.parent),
            )
            os.close(file_descriptor)
            temporary_path = Path(temporary_name)
            temporary_path.unlink()
            with cottas_scratch_workspace():
                pycottas.cat(
                    [str(cottas_path)],
                    str(temporary_path),
                    index=args.index,
                    remove_input_files=False,
                )
            if not temporary_path.is_file() or temporary_path.stat().st_size == 0:
                raise RuntimeError("pycottas did not create a non-empty reindexed file")
            os.replace(temporary_path, cottas_path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        return 0

    if args.command == "merge-many":
        input_paths = [str(Path(path).resolve()) for path in args.input_cottas_files]
        cottas_path = str(Path(args.output_cottas_file).resolve())
        if len(input_paths) < 2:
            print("merge-many requires at least two input COTTAS files", file=sys.stderr)
            return 2
        with cottas_scratch_workspace():
            # pycottas.cat accepts a list of inputs and computes the requested
            # index once.  This adapter is retained for explicit callers; the
            # production partitioned workflow uses the two-input ``merge``
            # command because a very large input list can exceed memory.
            pycottas.cat(
                input_paths,
                cottas_path,
                index=args.index,
                remove_input_files=True,
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
