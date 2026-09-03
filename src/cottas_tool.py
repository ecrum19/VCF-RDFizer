#!/usr/bin/env python3
"""Docker-side adapter for disk-backed COTTAS conversion and merge operations."""

import argparse
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path


DEFAULT_COTTAS_MERGE_MEMORY_LIMIT = "512M"
DEFAULT_COTTAS_MERGE_THREADS = 1
MEMORY_LIMIT_PATTERN = re.compile(
    r"^\d+(?:\.\d+)?\s*(?:B|K|M|G|T|KB|MB|GB|TB|KIB|MIB|GIB|TIB)$",
    re.IGNORECASE,
)


@contextmanager
def cottas_scratch_workspace():
    """Run one COTTAS operation with an isolated DuckDB working directory."""
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


def sql_literal(value: str | Path) -> str:
    """Quote one filesystem value for the DuckDB SQL emitted by this adapter."""
    return "'" + str(value).replace("'", "''") + "'"


def cottas_merge_memory_limit() -> str:
    """Return a validated DuckDB budget for COTTAS merge/reindex operations."""
    memory_limit = os.environ.get(
        "COTTAS_MERGE_MEMORY_LIMIT", DEFAULT_COTTAS_MERGE_MEMORY_LIMIT
    ).strip()
    if not MEMORY_LIMIT_PATTERN.fullmatch(memory_limit):
        raise ValueError(
            "COTTAS_MERGE_MEMORY_LIMIT must be a positive DuckDB byte value "
            "such as 512M or 1G"
        )
    return memory_limit


def cottas_merge_threads() -> int:
    """Return a bounded DuckDB worker count for deterministic merge memory use."""
    raw_threads = os.environ.get(
        "COTTAS_MERGE_THREADS", str(DEFAULT_COTTAS_MERGE_THREADS)
    ).strip()
    try:
        threads = int(raw_threads)
    except ValueError as exc:
        raise ValueError("COTTAS_MERGE_THREADS must be a positive integer") from exc
    if threads <= 0:
        raise ValueError("COTTAS_MERGE_THREADS must be a positive integer")
    return threads


def disk_backed_cottas_merge(
    input_paths: list[str],
    output_path: str,
    *,
    index: str,
    remove_input_files: bool,
) -> None:
    """Merge COTTAS Parquet inputs with DuckDB spill files rather than pycottas.cat.

    ``pycottas.cat`` uses DuckDB's process-global in-memory connection. Its
    global ``DISTINCT`` plus ``ORDER BY`` can therefore be SIGKILLed on a
    large condensed VCF even when every disk-backed chunk conversion succeeds.
    This adapter opens a dedicated on-disk database, caps its memory, restricts
    merge parallelism, and directs external sort/hash spill files to the
    disposable COTTAS scratch directory. The query retains COTTAS's global RDF
    set semantics and the requested Parquet sort/index order.
    """
    if not input_paths:
        raise ValueError("at least one COTTAS input is required for a merge")
    if not index or set(index.lower()) != {"s", "p", "o"} or len(index) != 3:
        raise ValueError("COTTAS merge index must be a permutation of spo")

    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(f"DuckDB dependency is unavailable: {exc}") from exc

    scratch_dir = Path.cwd()
    temporary_directory = scratch_dir / "duckdb-merge-tmp"
    temporary_directory.mkdir(parents=True, exist_ok=True)
    database_path = scratch_dir / "pycottas-merge.duckdb"
    memory_limit = cottas_merge_memory_limit()
    threads = cottas_merge_threads()
    quoted_inputs = ", ".join(sql_literal(path) for path in input_paths)
    parquet_scan = f"PARQUET_SCAN([{quoted_inputs}], union_by_name = true)"

    connection = None
    try:
        connection = duckdb.connect(str(database_path))
        # Apply limits before DuckDB plans the DISTINCT/ORDER BY operation.
        # One worker makes the memory budget predictable on hosts with many
        # CPUs and still permits DuckDB's external sort/hash operators to
        # spill to the named Docker volume.
        connection.execute("SET preserve_insertion_order = false")
        connection.execute("SET enable_progress_bar = false")
        connection.execute(f"SET temp_directory = {sql_literal(temporary_directory)}")
        connection.execute(f"SET memory_limit = {sql_literal(memory_limit)}")
        connection.execute(f"SET threads = {threads}")

        columns = {
            str(row[0])
            for row in connection.execute(
                f"DESCRIBE SELECT * FROM {parquet_scan} LIMIT 1"
            ).fetchall()
        }
        if not {"s", "p", "o"}.issubset(columns):
            raise RuntimeError("COTTAS inputs do not contain the required s, p, o columns")
        selected_columns = ["s", "p", "o"]
        if "g" in columns:
            selected_columns.append("g")
        selected_columns_sql = ", ".join(selected_columns)
        order_columns_sql = ", ".join(index.lower())
        copy_query = (
            f"COPY (SELECT DISTINCT {selected_columns_sql} FROM {parquet_scan} "
            f"ORDER BY {order_columns_sql}) TO {sql_literal(output_path)} "
            "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22, "
            "PARQUET_VERSION v2, "
            f"KV_METADATA {{index: {sql_literal(index.lower())}}})"
        )
        connection.execute(copy_query)
    except Exception as exc:
        # This context is intentionally included in stderr. The host wrapper
        # records it in the result JSON and surfaces it in the final error,
        # so storage, permissions, Parquet, and DuckDB SQL errors are not
        # collapsed into an unhelpful generic non-zero exit code.
        duckdb_version = getattr(duckdb, "__version__", "unknown")
        raise RuntimeError(
            "disk-backed COTTAS merge failed "
            f"(duckdb={duckdb_version}; inputs={len(input_paths)}; "
            f"memory_limit={memory_limit}; threads={threads}; "
            f"scratch={scratch_dir}; temp_directory={temporary_directory}; "
            f"output={output_path}): {exc}"
        ) from exc
    finally:
        if connection is not None:
            connection.close()

    if remove_input_files:
        for input_path in input_paths:
            Path(input_path).unlink(missing_ok=True)


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
        # replace the original only after the disk-backed DuckDB rewrite
        # completes successfully.
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
                disk_backed_cottas_merge(
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
            disk_backed_cottas_merge(
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
        disk_backed_cottas_merge(
            [left_path, right_path],
            cottas_path,
            index=args.index,
            remove_input_files=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
