# Compression Pipeline Change Log

This document records the storage and chunked-compression changes introduced
after the original partitioned HDT implementation. The design is intended for
large VCF conversions where retaining all RMLStreamer output parts and a full
uncompressed aggregate at the same time is impractical.

## Storage Modes

Full mode now supports two mutually exclusive aggregate storage modes:

| Option | Aggregate representation | Intended use |
| --- | --- | --- |
| `--rdf-storage-mode plain` | One uncompressed `<sample>.nt` | Debugging, inspection, and tools that require a normal file |
| `--rdf-storage-mode space-optimized` | One concatenated-gzip `<sample>.nt.gz` | Constrained disks and streamed chunk conversion |

RMLStreamer still writes its normal line-oriented N-Triples parts. The wrapper
then consumes those parts sequentially. In `plain` mode, each part is appended
to the aggregate and deleted. In `space-optimized` mode, each part is first
annotated and compressed with `gzip -c`, appended as a gzip member, and deleted
before the next part is processed. Concatenated gzip members are a valid gzip
stream, so standard gzip readers expose the same logical N-Triples sequence.

Exact duplicate part payloads are skipped in both modes. This protects against
duplicate Spark output without requiring a second full aggregate comparison.

## Public Compression Plan

The CLI separates staging, primary representation, and packaging:

| Option | Values | Purpose |
| --- | --- | --- |
| `--rdf-storage-mode` | `plain`, `space-optimized` | Controls the temporary RDF aggregate used by the chunk planner |
| `--rdf-compression` | `gzip`, `brotli`, `none` | Creates final raw RDF artifacts |
| `--representations` | `hdt`, `cottas`, `none` | Selects queryable indexed outputs |
| `--artifact-compression` | `gzip`, `brotli`, `none` | Packages each selected HDT/COTTAS representation |

For example, `--representations hdt --artifact-compression gzip` produces both
the queryable `sample.hdt` and the packaged `sample.hdt.gz`. The same plan can
package COTTAS as `sample.cottas.gz` or `sample.cottas.br`. Packaged artifacts
must be decompressed before querying; the unwrapped indexed artifact should be
retained for direct queries.

All three selectors accept comma-separated values. `none` is a standalone
no-op value, and artifact packaging is rejected unless a base representation is
selected.

The gzip stream created by `--rdf-storage-mode space-optimized` is an
intermediate aggregate. It is not a final raw RDF artifact unless
`--rdf-compression gzip` is selected or the aggregate is retained explicitly.

The storage mode is passed to `src/run_conversion.sh` through
`RDF_STORAGE_MODE`. The conversion metrics JSON records the selected mode,
serialization, compressed state, logical output path, and triple count.

## Record-Safe Chunk Planning

HDT and COTTAS use the same chunk planner. The public controls are:

```text
--chunk-target-bytes N
--chunk-min-bytes N
--chunk-max-bytes N
```

The planner reads the aggregate source once, using binary lines for exact byte
accounting. A gzip source is decompressed incrementally; it is never expanded
into a second full `.nt` file. A chunk boundary is emitted only after a
complete newline-terminated N-Triples record. The planner therefore cannot
split a triple, even when a target or maximum falls in the middle of a line.

During this same pass it builds an in-workspace guide containing source paths,
record ranges, byte ranges, payload sizes, and chunk counts. This avoids a
second decompression pass. The guide is returned in the raw
`__partitioned_compression__` metrics artifact for diagnostics, but is not
retained as a host-side `<sample>.chunks.json` file.

## Shared HDT/COTTAS Pipeline

When multiple partitioned representations are selected, the temporary `.nt`
chunks are shared inside one ephemeral Docker-managed workspace:

```text
RMLStreamer parts
        |
        v
plain .nt or streamed .nt.gz aggregate
        |
        v
read-only source mount + Docker volume workspace
        |
        v
record-safe temporary .nt chunks + in-volume guide
        |                         |
        v                         v
     rdf2hdt                 pycottas.rdf2cottas
        |                         |
        v                         v
  HDT intermediates          COTTAS intermediates
        |                         |
        v                         v
 balanced HDTCat merge       balanced pycottas.cat merge
        |                         |
        v                         v
 final HDT + generated       final COTTAS + rebuilt indexes
 .hdt.index.v1-1             + optional gzip/Brotli packaging
```

The aggregate is always mounted read-only; final artifacts are written
directly to the output mount. Each selected
converter consumes a chunk before that chunk is deleted, preventing the HDT
conversion from forcing a second RDF materialization for COTTAS. The same
container owns DuckDB scratch data, converter outputs, merge intermediates,
and packaging stages, so the host never accumulates the intermediate files.
The volume is initialized for the mapped host user and removed in a `finally`
cleanup path after success, failure, or interruption.

### HDT

Each chunk is converted with `rdf2hdt`. Intermediate HDTs are merged pairwise
in a balanced tree using HDTCat. Unpaired files are carried into the next
round. After the final HDT is produced, the bundled HDT Java `hdtSearch.sh`
launcher is fed only `exit`. This invokes `mapIndexedHDT()`, eagerly creating
the final sibling `.hdt.index.v1-1` sidecar without executing a potentially
large query. This is HDT Java 3.0.10's v1-1 index format. The helper accepts
the versioned `.hdt.index.*` filename and records the actual sidecar path in
metrics.
Intermediate indexes are intentionally not copied: the final dictionary and
triple layout are the authoritative query representation.

The same operation is available independently as:

```text
vcf-rdfizer --mode index --hdt path/to/sample.hdt --out results
```

The standalone mode records its result in
`run_metrics/<RUN_ID>/hdt_index_metrics.json`.

### COTTAS

Each chunk is converted with `pycottas.rdf2cottas(..., disk=True)`. Intermediate
COTTAS files are merged pairwise with `pycottas.cat(...,
remove_input_files=True)`. COTTAS rebuilds the selected `spo` indexes for each
merged output, so queries target the final merged file rather than stale
chunk-local indexes.

The Docker image installs `pycottas` in `/opt/pycottas-venv` and invokes the
small adapter at `/opt/vcf-rdfizer/cottas_tool.py`. COTTAS is therefore part of
the Docker workflow, not a dependency of the lightweight pip/conda wrapper.

The adapter runs every `pycottas.rdf2cottas` and `pycottas.cat` operation in a
new temporary subdirectory of the container-local `/work` filesystem. pycottas
uses a default `pycottas.duckdb` database in its current working directory;
without isolation, a later chunk can reopen the prior database and fail while
creating its existing `quads` table. Input and output paths are resolved before
entering the temporary directory, so COTTAS artifacts remain in the designated
workspace/output mount while the DuckDB database and related scratch files are
removed immediately after each operation. `COTTAS_SCRATCH_DIR` can override
`/work` for image-internal deployments, but the wrapper does not require users
to set it.

## Metrics and Cleanup

Every chunk conversion, merge, index-generation, and final representation
packaging stage is timed separately inside the container. The sample-level
result sums sequential wall, user, and system times and records the maximum
resident set size. Raw metrics also include a
`__partitioned_compression__` artifact with the chunk plan, stage timings,
merge details, and workspace cleanup status.

Raw RDF is removed only after all selected methods report success. The exception
is the space-optimized `.nt.gz` source when `gzip` is selected: that file is
already the requested gzip artifact and is retained. Cleanup is enabled by
default; `--remove-rdf-storage-output` makes aggregate removal explicit, while
`--keep-rmlstreamer-rdf-output` retains the aggregate produced by RMLStreamer.
The two flags are mutually exclusive.

### Representation Validation

Every generated base HDT or COTTAS representation is validated before any
packaging stage and before raw RDF cleanup. The validation has two parts:

1. The artifact is opened and decoded with its native reader. HDT is loaded
   through `ensure_hdt_index.sh`/HDT Java and streamed through `hdt2rdf`;
   COTTAS is streamed through `pycottas.cottas2rdf`.
2. The decoded N-Triples stream is counted and compared with the source count.

Full mode passes the `output_triples` count recorded by RMLStreamer whenever
it is available. The partition planner already reads the source once, so its
record count is reused for validation. Compression-only mode has no upstream
conversion metric; in that case the validator counts plain or gzip-compressed
N-Triples directly as a fallback. No decoded RDF file is written during this
check.

A missing decoder, unreadable artifact, or count mismatch makes the
compression stage fail and prevents RDF cleanup. Reports are stored in the
method details of the raw compression JSON and expose `source_triples`,
`decoded_triples`, `count_match`, and `valid`. The aggregate `metrics.csv`
also records the source count, decoded count, and validation status for HDT
and COTTAS.

## Compatibility and Limits

Full mode requires `--rdf-storage-mode plain` or
`--rdf-storage-mode space-optimized`; both produce one logical RDF aggregate.
HDT-specific chunk option names were removed; chunk sizing is deliberately
shared by HDT and COTTAS. `--hdt-strategy single` is not allowed for a gzip
aggregate because it would require materializing the full uncompressed RDF
file; use the partitioned strategy instead.

The planner assumes line-oriented N-Triples input. Conversion and merge stages
are sequential to constrain peak disk and memory usage. Temporary chunks and
intermediate compressed files still require Docker storage space, although the
space-optimized path avoids the largest duplicate uncompressed aggregate and
the host output filesystem does not carry those intermediates.

## Relevant Components

- `vcf_rdfizer.py`: CLI, storage-mode dispatch, chunk planning, conversion, merge, and metrics
- `src/run_conversion.sh`: RMLStreamer output aggregation and streamed gzip storage
- `src/cottas_tool.py`: Docker-side pycottas conversion/merge adapter
- `src/partitioned_compression.py`: one-container chunk, merge, index, packaging, and stage-metrics runner
- `Dockerfile`: runtime dependencies and pycottas environment
- `README.md`: user-facing CLI and workflow documentation
