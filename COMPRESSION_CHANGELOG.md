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

During this same pass it writes `chunks.json`, containing source paths, record
ranges, byte ranges, payload sizes, and chunk counts. This is both an audit
trail and a reusable guide for diagnosing an unsuccessful conversion. The
guide is generated during the read rather than by a second decompression pass.
For a successful aggregate partitioned run, it is retained beside the final
artifacts as `<sample>.chunks.json`; temporary chunk files are removed after
all selected conversions complete.

## Shared HDT/COTTAS Pipeline

When multiple partitioned representations are selected, the temporary `.nt` chunks are
shared:

```text
RMLStreamer parts
        |
        v
plain .nt or streamed .nt.gz aggregate
        |
        v
record-safe temporary .nt chunks + chunks.json
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
 .hdt.index                  + optional gzip/Brotli packaging
```

Each selected converter consumes a chunk before that chunk is deleted. This
prevents the HDT conversion from forcing a second RDF materialization for
COTTAS. If a conversion or merge fails, temporary files and raw RDF are not
silently treated as successfully compressed output.

### HDT

Each chunk is converted with `rdf2hdt`. Intermediate HDTs are merged pairwise
in a balanced tree using HDTCat. Unpaired files are carried into the next
round. After the final HDT is produced, the bundled HDT Java `hdtSearch.sh`
launcher is fed only `exit`. This invokes `mapIndexedHDT()`, eagerly creating
the final sibling `.hdt.index` without executing a potentially large query.
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

## Metrics and Cleanup

Every chunk conversion, merge, index-generation, and final HDT post-compression
stage is timed separately. The sample-level result sums sequential wall, user,
and system times and records the maximum resident set size. Raw metrics also
include a `__partitioned_compression__` artifact with the chunk plan and merge
details.

Raw RDF is removed only after all selected methods report success. The exception
is the space-optimized `.nt.gz` source when `gzip` is selected: that file is
already the requested gzip artifact and is retained. Cleanup is enabled by
default; `--remove-rdf-storage-output` makes aggregate removal explicit, while
`--keep-rmlstreamer-rdf-output` retains the aggregate produced by RMLStreamer.
The two flags are mutually exclusive.

## Compatibility and Limits

Full mode requires `--rdf-storage-mode plain` or
`--rdf-storage-mode space-optimized`; both produce one logical RDF aggregate.
HDT-specific chunk option names were removed; chunk sizing is deliberately
shared by HDT and COTTAS. `--hdt-strategy single` is not allowed for a gzip
aggregate because it would require materializing the full uncompressed RDF
file; use the partitioned strategy instead.

The planner assumes line-oriented N-Triples input. Conversion and merge stages
are sequential to constrain peak disk and memory usage. Temporary chunks and
intermediate compressed files still require working space, although the
space-optimized path avoids the largest duplicate uncompressed aggregate.

## Relevant Components

- `vcf_rdfizer.py`: CLI, storage-mode dispatch, chunk planning, conversion, merge, and metrics
- `src/run_conversion.sh`: RMLStreamer output aggregation and streamed gzip storage
- `src/cottas_tool.py`: Docker-side pycottas conversion/merge adapter
- `Dockerfile`: runtime dependencies and pycottas environment
- `README.md`: user-facing CLI and workflow documentation
