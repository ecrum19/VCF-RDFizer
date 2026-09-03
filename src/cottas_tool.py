#!/usr/bin/env python3
"""Docker-side adapter for bounded-memory COTTAS conversion and merging."""

import argparse
import heapq
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path


DEFAULT_COTTAS_MERGE_BATCH_ROWS = 2048
COTTAS_OUTPUT_BATCH_ROWS = 16 * 1024
COTTAS_MERGE_PROGRESS_ROWS = 250_000


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


def cottas_merge_batch_rows() -> int:
    """Return a small, bounded Parquet batch size for the streaming merge."""
    raw_rows = os.environ.get(
        "COTTAS_MERGE_BATCH_ROWS", str(DEFAULT_COTTAS_MERGE_BATCH_ROWS)
    ).strip()
    try:
        batch_rows = int(raw_rows)
    except ValueError as exc:
        raise ValueError("COTTAS_MERGE_BATCH_ROWS must be a positive integer") from exc
    if batch_rows <= 0:
        raise ValueError("COTTAS_MERGE_BATCH_ROWS must be a positive integer")
    return batch_rows


def emit_merge_progress(
    progress_path: Path | None,
    phase: str,
    *,
    completed: int,
    total: int,
    detail: str,
) -> None:
    """Append a best-effort COTTAS merge heartbeat for the host progress UI."""
    if progress_path is None:
        return
    payload = {
        "stage": "cottas-merge",
        "phase": phase,
        "completed": completed,
        "total": total,
        "unit": "triples",
        "detail": detail,
    }
    try:
        with progress_path.open("a", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
            handle.write("\n")
    except OSError:
        # A missing/unwritable optional sidecar must not invalidate an index.
        pass


def cottas_file_index(parquet_file) -> str | None:
    """Read COTTAS's embedded Parquet sort-index metadata when available."""
    metadata = getattr(parquet_file.metadata, "metadata", None) or {}
    value = metadata.get(b"index") or metadata.get("index")
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return str(value).lower()


class CottasTripleStream:
    """Read one sorted COTTAS Parquet file in a bounded number of rows."""

    def __init__(self, parquet_module, path: Path, index: str, batch_rows: int):
        self.path = path
        self.index = index
        self.parquet_file = parquet_module.ParquetFile(path)
        self.schema = self.parquet_file.schema_arrow
        missing_columns = {"s", "p", "o"} - set(self.schema.names)
        if missing_columns:
            raise RuntimeError(
                f"COTTAS input {path.name} is missing columns: {', '.join(sorted(missing_columns))}"
            )
        source_index = cottas_file_index(self.parquet_file)
        if source_index != index:
            found = source_index or "missing"
            raise RuntimeError(
                f"COTTAS input {path.name} is indexed as {found!r}, not {index!r}; "
                "a streaming merge requires every input to use the requested index"
            )
        self.fields = tuple(self.schema.field(name) for name in ("s", "p", "o"))
        positions = {"s": 0, "p": 1, "o": 2}
        self._sort_positions = tuple(positions[column] for column in index)
        self._batches = self.parquet_file.iter_batches(
            batch_size=batch_rows,
            columns=["s", "p", "o"],
            use_threads=False,
        )
        self._values: tuple[list, list, list] | None = None
        self._row = 0
        self.current: tuple | None = None
        self.sort_key: tuple | None = None
        self._previous_sort_key: tuple | None = None
        self.exhausted = False
        self.advance()

    @property
    def row_count(self) -> int:
        return int(self.parquet_file.metadata.num_rows)

    def advance(self) -> None:
        """Move to one next triple, validating the COTTAS sort-order contract."""
        while self._values is None or self._row >= len(self._values[0]):
            try:
                batch = next(self._batches)
            except StopIteration:
                self.current = None
                self.sort_key = None
                self.exhausted = True
                return
            values = tuple(column.to_pylist() for column in batch.columns)
            if not values[0]:
                continue
            self._values = values
            self._row = 0

        triple = (self._values[0][self._row], self._values[1][self._row], self._values[2][self._row])
        self._row += 1
        if any(value is None for value in triple):
            raise RuntimeError(f"COTTAS input {self.path.name} contains a null RDF term")
        sort_key = tuple(triple[position] for position in self._sort_positions)
        if self._previous_sort_key is not None and sort_key < self._previous_sort_key:
            raise RuntimeError(
                f"COTTAS input {self.path.name} is not sorted by its declared {self.index!r} index"
            )
        self._previous_sort_key = sort_key
        self.current = triple
        self.sort_key = sort_key

    def close(self) -> None:
        """Release a Parquet file handle before the surrounding workspace exits."""
        close = getattr(self.parquet_file, "close", None)
        if callable(close):
            close()


def streaming_cottas_merge(
    input_paths: list[str],
    output_path: str,
    *,
    index: str,
    remove_input_files: bool,
    progress_path: Path | None = None,
) -> None:
    """Merge already-indexed COTTAS files without a global sort or spill area.

    Chunk conversion writes every COTTAS input in the requested lexical index
    order. A k-way heap therefore needs only one small Parquet batch from each
    input; equal triples meet at the heap head and are written once. This
    preserves the RDF set and COTTAS index semantics while avoiding DuckDB's
    full-data external sort, whose temporary files can exceed the original RDF
    size for large multi-sample VCFs.
    """
    if not input_paths:
        raise ValueError("at least one COTTAS input is required for a merge")
    if not index or set(index.lower()) != {"s", "p", "o"} or len(index) != 3:
        raise ValueError("COTTAS merge index must be a permutation of spo")

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "PyArrow is required for bounded-memory COTTAS merging: "
            f"{exc}"
        ) from exc

    normalized_index = index.lower()
    batch_rows = cottas_merge_batch_rows()
    streams: list[CottasTripleStream] = []
    temporary_path: Path | None = None
    writer = None
    try:
        streams = [
            CottasTripleStream(pq, Path(path), normalized_index, batch_rows)
            for path in input_paths
        ]
        fields = streams[0].fields
        expected_types = tuple(field.type for field in fields)
        for stream in streams[1:]:
            if tuple(field.type for field in stream.fields) != expected_types:
                raise RuntimeError(
                    "COTTAS inputs do not share the same RDF term column types"
                )

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{output.name}.merge-",
            suffix=".cottas",
            dir=str(output.parent),
        )
        os.close(descriptor)
        temporary_path = Path(temporary_name)
        temporary_path.unlink()
        output_schema = pa.schema(
            list(fields), metadata={b"index": normalized_index.encode("utf-8")}
        )
        writer = pq.ParquetWriter(
            temporary_path,
            output_schema,
            compression="zstd",
            compression_level=22,
            version="2.6",
        )
        total_rows = sum(stream.row_count for stream in streams)
        emit_merge_progress(
            progress_path,
            "started",
            completed=0,
            total=total_rows,
            detail=f"{len(streams):,} sorted COTTAS chunks",
        )
        heap = [
            (stream.sort_key, stream_number, stream.current)
            for stream_number, stream in enumerate(streams)
            if not stream.exhausted
        ]
        heapq.heapify(heap)
        output_rows: list[tuple] = []
        previous_triple = None
        processed_rows = 0
        written_rows = 0
        last_progress_rows = 0
        while heap:
            _, stream_number, triple = heapq.heappop(heap)
            processed_rows += 1
            if triple != previous_triple:
                output_rows.append(triple)
                previous_triple = triple
            # Keep Parquet row groups substantially larger than the per-input
            # read batch. That avoids creating tens of thousands of tiny row
            # groups for a cohort-sized graph without changing input memory.
            if len(output_rows) >= COTTAS_OUTPUT_BATCH_ROWS:
                arrays = [
                    pa.array([row[column] for row in output_rows], type=fields[column].type)
                    for column in range(3)
                ]
                writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=output_schema))
                written_rows += len(output_rows)
                output_rows.clear()

            stream = streams[stream_number]
            stream.advance()
            if not stream.exhausted:
                heapq.heappush(heap, (stream.sort_key, stream_number, stream.current))
            if processed_rows - last_progress_rows >= COTTAS_MERGE_PROGRESS_ROWS:
                emit_merge_progress(
                    progress_path,
                    "merging",
                    completed=processed_rows,
                    total=total_rows,
                    detail=f"{written_rows + len(output_rows):,} distinct triples written",
                )
                last_progress_rows = processed_rows

        if output_rows:
            arrays = [
                pa.array([row[column] for row in output_rows], type=fields[column].type)
                for column in range(3)
            ]
            writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=output_schema))
            written_rows += len(output_rows)
        writer.close()
        writer = None
        os.replace(temporary_path, output)
        temporary_path = None
        emit_merge_progress(
            progress_path,
            "complete",
            completed=processed_rows,
            total=total_rows,
            detail=f"{written_rows:,} distinct triples written",
        )
    except Exception as exc:
        # This context is surfaced by the host wrapper after its ephemeral
        # Docker volume is removed, so a malformed or unexpectedly indexed
        # input does not become a generic non-zero exit code.
        pyarrow_version = getattr(pa, "__version__", "unknown")
        raise RuntimeError(
            "streaming COTTAS merge failed "
            f"(pyarrow={pyarrow_version}; inputs={len(input_paths)}; "
            f"index={normalized_index}; batch_rows={batch_rows}; "
            f"output={output_path}): {exc}"
        ) from exc
    finally:
        if writer is not None:
            writer.close()
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        for stream in streams:
            stream.close()

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
    merge_many.add_argument(
        "--progress-path",
        help="optional JSONL sidecar for bounded streaming-merge progress",
    )

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
        # replace the original only after the streaming Parquet rewrite
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
                streaming_cottas_merge(
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
            streaming_cottas_merge(
                input_paths,
                cottas_path,
                index=args.index,
                remove_input_files=True,
                progress_path=(Path(args.progress_path) if args.progress_path else None),
            )
        return 0

    left_path = str(Path(args.left_path).resolve())
    right_path = str(Path(args.right_path).resolve())
    cottas_path = str(Path(args.cottas_path).resolve())
    with cottas_scratch_workspace():
        streaming_cottas_merge(
            [left_path, right_path],
            cottas_path,
            index=args.index,
            remove_input_files=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
