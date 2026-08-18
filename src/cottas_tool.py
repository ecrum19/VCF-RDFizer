#!/usr/bin/env python3
"""Small Docker-side adapter for the pycottas conversion and merge API."""

import argparse
import sys


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
        # disk=True keeps parser/index construction from requiring the whole
        # RDF chunk in Python memory.
        pycottas.rdf2cottas(
            args.rdf_path,
            args.cottas_path,
            index=args.index,
            disk=True,
        )
        return 0

    pycottas.cat(
        [args.left_path, args.right_path],
        args.cottas_path,
        index=args.index,
        remove_input_files=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
