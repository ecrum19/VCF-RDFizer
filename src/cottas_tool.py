#!/usr/bin/env python3
"""Docker-side adapter for bounded-memory COTTAS conversion and merging."""

import argparse
import heapq
import json
import os
import sys
import tempfile
from contextlib import contextmanager, nullcontext
from pathlib import Path

from vcf_rdfizer_cottas import (
    COTTAS_ALL_INDEXES, cottas_index_paths, parse_cottas_indexes, require_dataset_indexes,
)


DEFAULT_COTTAS_MERGE_BATCH_ROWS = 2048
COTTAS_OUTPUT_BATCH_ROWS = 16 * 1024
COTTAS_MERGE_PROGRESS_ROWS = 250_000
COTTAS_REINDEX_FAN_IN = 128


def parse_nquads_chunk(source: str, output: Path) -> None:
    """Stream N-Quads without pycottas 1.1.0's per-chunk blank-node renaming."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyoxigraph
    import rdflib
    from rdflib.store import Store

    schema = pa.schema([(name, pa.string()) for name in "spog"])
    rows = []

    class BlankNodeLabels(dict):
        def get(self, key, default=None):
            return key

    class QuadSink(Store):
        context_aware = graph_aware = True

        def add_graph(self, graph):
            pass

        def remove_graph(self, graph):
            pass

        def add(self, triple, context, quoted=False):
            terms = []
            for term in triple:
                if isinstance(term, rdflib.Literal):
                    datatype = pyoxigraph.NamedNode(str(term.datatype)) if term.datatype else None
                    terms.append(str(pyoxigraph.Literal(str(term), language=term.language, datatype=datatype)))
                else:
                    terms.append(term.n3())
            graph = context.identifier
            rows.append((*terms, None if graph == dataset.default_context.identifier else graph.n3()))
            if len(rows) >= COTTAS_OUTPUT_BATCH_ROWS:
                flush()

    def flush():
        writer.write_table(pa.table({name: [row[i] for row in rows] for i, name in enumerate("spog")}, schema=schema))
        rows.clear()

    dataset = rdflib.Dataset(store=QuadSink())
    dataset.default_context = rdflib.Graph(store=dataset.store, identifier=rdflib.BNode())
    normalize = rdflib.NORMALIZE_LITERALS
    try:
        # Preserve lexical forms such as "01"^^xsd:integer.
        rdflib.NORMALIZE_LITERALS = False
        with pq.ParquetWriter(output, schema, compression="zstd") as writer:
            dataset.parse(source, format="nquads", bnode_context=BlankNodeLabels())
            if rows:
                flush()
    finally:
        rdflib.NORMALIZE_LITERALS = normalize


def sort_cottas_chunk(source: Path, outputs: dict[str, Path], *, deduplicate: bool = False) -> None:
    """Build extra orders from one parsed chunk, with disk-backed sorting."""
    import duckdb
    import pyarrow.parquet as pq

    has_graph = "g" in pq.read_schema(source).names
    if has_graph:
        require_dataset_indexes(tuple(outputs))

    with duckdb.connect("index-sort.duckdb") as connection:
        connection.execute("SET preserve_insertion_order = false")
        for index, output in outputs.items():
            if index not in COTTAS_ALL_INDEXES:
                raise ValueError("COTTAS index must be a permutation of spo or spog")
            columns = "s, p, o"
            if "g" in index:
                # pycottas 1.1.0 emits DEFAULT for the N-Quads default graph.
                columns += ", NULLIF(g, 'DEFAULT') AS g" if has_graph else ", NULL::VARCHAR AS g"
            connection.execute(
                f"COPY (SELECT {'DISTINCT ' if deduplicate else ''}{columns} FROM read_parquet($source) ORDER BY {', '.join(column + ' NULLS LAST' for column in index)}) "
                f"TO $target (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22, "
                f"PARQUET_VERSION v2, KV_METADATA {{index: '{index}'}})",
                {"source": str(source), "target": str(output)},
            )
            if deduplicate:
                source = output
                deduplicate = False


def normalize_graph_column(table):
    """Represent default graphs consistently as NULL, including legacy files."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if "g" not in table.column_names:
        return table.append_column("g", pa.nulls(table.num_rows, type=pa.string()))
    graph = table["g"]
    return table.set_column(table.column_names.index("g"), "g", pc.if_else(pc.equal(graph, "DEFAULT"), None, graph))


def decompress_cottas(source: str, output: str) -> None:
    """Stream triples or quads, omitting the graph term for the default graph."""
    import pyarrow.parquet as pq
    import pycottas

    with pq.ParquetFile(source) as parquet:
        if "g" not in parquet.schema_arrow.names:
            pycottas.cottas2rdf(source, output)
            return
        with (nullcontext(sys.stdout) if output == "/dev/stdout" else open(output, "w", encoding="utf-8")) as handle:
            for batch in parquet.iter_batches(batch_size=COTTAS_OUTPUT_BATCH_ROWS, columns=list("spog")):
                for s, p, o, g in zip(*(column.to_pylist() for column in batch.columns)):
                    if any(term is None for term in (s, p, o)):
                        raise RuntimeError("COTTAS input contains a null RDF term")
                    if g not in (None, "DEFAULT") and Path(output).suffix == ".nt":
                        raise ValueError("Named graphs require N-Quads output; choose --decompress-out ending in .nq")
                    handle.write(f"{s} {p} {o}" + (f" {g}" if g not in (None, "DEFAULT") else "") + " .\n")


def reindex_cottas(source: Path, outputs: dict[str, Path]) -> None:
    """Reorder bounded Parquet batches, then merge each requested index."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    with pq.ParquetFile(source) as parquet:
        if "g" in parquet.schema_arrow.names:
            require_dataset_indexes(tuple(outputs))
        if len(outputs) == 1 and "g" not in next(iter(outputs)) and cottas_file_index(parquet) == next(iter(outputs)):
            index, output = next(iter(outputs.items()))
            streaming_cottas_merge([str(source)], str(output), index=index, remove_input_files=False)
            return
        # Sorting batches keeps reindexing bounded even for a cohort-sized file.
        with tempfile.TemporaryDirectory(prefix="reindex-runs-", dir=Path.cwd()) as directory:
            runs = {index: [] for index in outputs}
            columns = list("spog" if "g" in parquet.schema_arrow.names else "spo")
            for number, batch in enumerate(parquet.iter_batches(batch_size=COTTAS_OUTPUT_BATCH_ROWS, columns=columns)):
                table = pa.Table.from_batches([batch])
                for index in outputs:
                    path = Path(directory) / f"{number}.{index}.cottas"
                    indexed = normalize_graph_column(table) if "g" in index else table
                    ordered = indexed.sort_by([(column, "ascending") for column in index])
                    pq.write_table(ordered.replace_schema_metadata({b"index": index.encode()}), path, compression="zstd")
                    runs[index].append(str(path))
            for index, output in outputs.items():
                if runs[index]:
                    paths = runs[index]
                    round_number = 0
                    while len(paths) > COTTAS_REINDEX_FAN_IN:
                        merged = []
                        for start in range(0, len(paths), COTTAS_REINDEX_FAN_IN):
                            path = Path(directory) / f"merge-{index}-{round_number}-{start}.cottas"
                            streaming_cottas_merge(paths[start:start + COTTAS_REINDEX_FAN_IN], str(path), index=index, remove_input_files=True)
                            merged.append(str(path))
                        paths = merged
                        round_number += 1
                    streaming_cottas_merge(paths, str(output), index=index, remove_input_files=True)
                else:
                    schema = pa.schema([parquet.schema_arrow.field(name) for name in ("s", "p", "o")], metadata={b"index": index.encode()})
                    table = pa.Table.from_batches([], schema=schema)
                    if "g" in index:
                        table = normalize_graph_column(table)
                    pq.write_table(table, output, compression="zstd")


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
        try:
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
            import pyarrow as pa
            if "g" in self.schema.names:
                require_dataset_indexes((index,))
            self.columns = "spog" if "g" in index else "spo"
            self.fields = tuple(
                self.schema.field(name) if name in self.schema.names else pa.field(name, pa.string())
                for name in self.columns
            )
            positions = {name: position for position, name in enumerate(self.columns)}
            self._sort_positions = tuple(positions[column] for column in index)
            self._batches = self.parquet_file.iter_batches(
                batch_size=batch_rows,
                columns=[name for name in self.columns if name in self.schema.names],
                use_threads=False,
            )
            self._values: tuple[list, ...] | None = None
            self._row = 0
            self.current: tuple | None = None
            self.sort_key: tuple | None = None
            self._previous_sort_key: tuple | None = None
            self.exhausted = False
            self.advance()
        except Exception:
            self.close()
            raise

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
            if len(self.columns) == 4 and len(values) == 3:
                values += ([None] * len(values[0]),)
            self._values = values
            self._row = 0

        triple = tuple(values[self._row] for values in self._values)
        if len(triple) == 4 and triple[3] == "DEFAULT":
            triple = triple[:3] + (None,)
        self._row += 1
        if any(value is None for value in triple[:3]):
            raise RuntimeError(f"COTTAS input {self.path.name} contains a null RDF term")
        sort_key = tuple(
            (triple[position] is None, triple[position] or "") if position == 3 else triple[position]
            for position in self._sort_positions
        )
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
    if index.lower() not in COTTAS_ALL_INDEXES:
        raise ValueError("COTTAS merge index must be a permutation of spo or spog")

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
        for path in input_paths:
            streams.append(CottasTripleStream(pq, Path(path), normalized_index, batch_rows))
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
                    for column in range(len(fields))
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
                for column in range(len(fields))
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
    convert.add_argument("index", nargs="?", default="spo", type=parse_cottas_indexes)

    merge = subparsers.add_parser("merge", help="merge two COTTAS files")
    merge.add_argument("left_path")
    merge.add_argument("right_path")
    merge.add_argument("cottas_path")
    merge.add_argument("index", nargs="?", default="spo", type=str.lower, choices=COTTAS_ALL_INDEXES)

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
    merge_many.add_argument("--index", default="spo", type=str.lower, choices=COTTAS_ALL_INDEXES)
    merge_many.add_argument(
        "--progress-path",
        help="optional JSONL sidecar for bounded streaming-merge progress",
    )

    reindex = subparsers.add_parser(
        "reindex",
        help="rebuild the embedded COTTAS query index in place",
    )
    reindex.add_argument("cottas_path")
    reindex.add_argument("index", nargs="?", default="spo", type=parse_cottas_indexes)

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
        if Path(rdf_path).suffix.lower() in {".nq", ".trig"}:
            require_dataset_indexes(args.index)
        # Keep one RDF parse/DISTINCT per chunk. Quad orders are sorted after
        # normalizing pycottas's DEFAULT sentinel, so NULL ordering is uniform.
        with cottas_scratch_workspace():
            primary_is_quad = "g" in args.index[0]
            parsed = Path.cwd() / "parsed.cottas" if primary_is_quad else Path(cottas_path)
            if Path(rdf_path).suffix.lower() == ".nq":
                parse_nquads_chunk(rdf_path, parsed)
            else:
                pycottas.rdf2cottas(
                    rdf_path, str(parsed), index="" if primary_is_quad else args.index[0], disk=True,
                )
            outputs = cottas_index_paths(Path(cottas_path), args.index)
            if not primary_is_quad:
                outputs.pop(args.index[0])
            if outputs:
                sort_cottas_chunk(parsed, outputs, deduplicate=Path(rdf_path).suffix.lower() == ".nq")
        return 0

    if args.command == "decompress":
        cottas_path = str(Path(args.cottas_path).resolve())
        rdf_path = str(Path(args.rdf_path).absolute())
        # Keep DuckDB scratch state in the container-local workspace while
        # pycottas writes the decoded RDF directly to the mounted output.
        with cottas_scratch_workspace():
            decompress_cottas(cottas_path, rdf_path)
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
        outputs = cottas_index_paths(cottas_path, args.index)
        for output in list(outputs.values())[1:]:
            if output.exists():
                raise FileExistsError(f"COTTAS index output already exists: {output}")
        temporary_paths = {}
        try:
            for index, output in outputs.items():
                file_descriptor, temporary_name = tempfile.mkstemp(
                    prefix=f".{output.name}.reindex-", suffix=".cottas", dir=str(output.parent),
                )
                os.close(file_descriptor)
                temporary_paths[index] = Path(temporary_name)
                temporary_paths[index].unlink()
            with cottas_scratch_workspace():
                reindex_cottas(cottas_path, temporary_paths)
            for temporary_path in temporary_paths.values():
                if not temporary_path.is_file() or temporary_path.stat().st_size == 0:
                    raise RuntimeError("pycottas did not create a non-empty reindexed file")
            # Publish the primary last so failed builds preserve the original.
            for index in reversed(args.index):
                os.replace(temporary_paths[index], outputs[index])
        finally:
            for temporary_path in temporary_paths.values():
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
