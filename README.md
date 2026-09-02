[![Unit Tests](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/tests.yml/badge.svg)](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/tests.yml)
[![Publish Python](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/publish-python.yml/badge.svg)](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/publish-python.yml)
[![Publish Docker](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/publish-docker.yml/badge.svg)](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/publish-docker.yml)
[![Codecov](https://codecov.io/gh/ecrum19/VCF-RDFizer/graph/badge.svg)](https://codecov.io/gh/ecrum19/VCF-RDFizer)
[![PyPI version](https://img.shields.io/pypi/v/vcf-rdfizer)](https://pypi.org/project/vcf-rdfizer/)
[![Python versions](https://img.shields.io/pypi/pyversions/vcf-rdfizer)](https://pypi.org/project/vcf-rdfizer/)
[![Docker Pulls](https://img.shields.io/docker/pulls/ecrum19/vcf-rdfizer)](https://hub.docker.com/r/ecrum19/vcf-rdfizer)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/vcf-rdfizer)](https://anaconda.org/conda-forge/vcf-rdfizer)
[![License](https://img.shields.io/github/license/ecrum19/VCF-RDFizer)](https://github.com/ecrum19/VCF-RDFizer/blob/main/LICENSE)

<p align="center">
  <img src="assets/logo/logo.png" alt="VCF-RDFizer logo" width="220" />
</p>

VCF-RDFizer is a Docker-first CLI wrapper for:
1. VCF -> RDF (N-Triples) with RMLStreamer
2. Optional RDF compression/decompression

The VCF-RDFizer vocabulary is available at [https://w3id.org/vcf-rdfizer/vocab#](https://w3id.org/vcf-rdfizer/vocab#).

## Requirements

- Python 3.10+
- Docker (installed and running)

Install options:

```bash
pip install vcf-rdfizer
```

or

```bash
pipx install vcf-rdfizer
```

or

```bash
conda install -c conda-forge vcf-rdfizer
```

or pull the prebuilt Docker image directly:

```bash
docker pull ecrum19/vcf-rdfizer:latest
```

Release maintainers: see [`RELEASING.md`](RELEASING.md) for the PyPI,
Docker Hub, and conda-forge release procedure.

## Important CLI Rule

`--out` is required for all modes.

This is the run output root directory. VCF-RDFizer places:
- final RDF/compression outputs
- run metrics/logs
- hidden intermediates

inside this directory.

## Modes

- `full`: VCF -> TSV -> RDF -> compression
- `tsv`: VCF -> TSV only (benchmarking)
- `compress`: compress an existing `.nt` or `.nt.gz`
- `decompress`: decompress `.nt.gz`, `.nt.br`, `.hdt`, `.cottas`, `.cottas.gz`, or `.cottas.br`
- `index`: only generate or regenerate the query index for an existing `.hdt` or `.cottas`

In `full` mode with multiple VCF inputs, failures are isolated per input:
- the run continues with remaining files
- failed inputs are summarized in `run_metrics/<RUN_ID>/failed_inputs.csv`

## Main Flags (Most Used)

- `-m, --mode {full,compress,decompress,tsv,index}`
- `-o, --out` required output root directory
- `--rdf-compression` final raw RDF codecs: `gzip`, `brotli`, or `none`
- `--representations` queryable RDF outputs: `hdt`, `cottas`, or `none`
- `--artifact-compression` packaging codecs for selected representations: `gzip`, `brotli`, or `none`
- `--hdt-strategy {auto,partitioned,single}` HDT generation policy
- `--chunk-target-bytes`, `--chunk-min-bytes`, `--chunk-max-bytes` shared record-safe chunk sizing
- `--sample-representation {dense,condensed}` genotype graph shape (`dense` by default)
- `-I, --image` Docker image repo (default `ecrum19/vcf-rdfizer`)
- `-v, --image-version` Docker tag/version
- `-b, --build` force Docker build
- `-B, --no-build` fail if image not found
- `-h, --help` show full usage

## Compression Plan

Compression is configured as three independent decisions:

1. **RDF staging**: `--rdf-storage-mode` controls how RMLStreamer output is
   assembled before chunking. `plain` creates one `.nt` aggregate;
   `space-optimized` streams the parts into one `.nt.gz` aggregate and removes
   each source part immediately.
2. **Raw RDF artifacts**: `--rdf-compression gzip,brotli` creates compressed
   copies of the RDF aggregate. Use `--rdf-compression none` when RDF is only a
   temporary input to HDT/COTTAS.
3. **Queryable representations and packaging**: `--representations hdt,cottas`
   creates the selected indexed formats. `--artifact-compression gzip,brotli`
   packages each selected representation, producing `.hdt.gz`, `.hdt.br`,
   `.cottas.gz`, or `.cottas.br` in addition to the queryable base artifact.

Each selector accepts comma-separated values. Use `none` by itself to disable
that stage; do not combine `none` with another value. `--artifact-compression`
requires at least one selected value in `--representations`.

The gzip used by `--rdf-storage-mode space-optimized` is staging storage; it is
not automatically a final raw RDF artifact. The default plan preserves the
historical outputs: `--rdf-compression gzip,brotli` and
`--representations hdt`. For the smallest final output, select
`--rdf-compression none`, one representation, and
`--remove-rdf-storage-output`.

Packaged `.hdt.gz`, `.hdt.br`, `.cottas.gz`, and `.cottas.br` files are archives,
not directly queryable indexed files. Keep the unwrapped `.hdt`/`.cottas` file
when queries must run without a decompression step.

Use `--mode decompress` to decode either base representation. COTTAS packages
are unpacked inside the Docker container before `pycottas` writes the decoded
N-Triples output, so the temporary unwrapped COTTAS file is not added to the
host filesystem.

## Full Mode Flags

- `-i, --input` required VCF file or directory
- `-r, --rules` mapping rules file (`.ttl`)
  - default: `rules/default_rules.ttl`
- `--sample-representation {dense,condensed}` sample genotype representation
  - `dense` (default): one `SampleCall` per record/sample and one `FormatFieldValue` per FORMAT key
  - `condensed`: reusable file-level samples plus one ordered value vector per record/FORMAT key
- `--rdf-storage-mode {plain,space-optimized}` required full-mode aggregate storage policy
  - `plain`: merge RMLStreamer parts into one uncompressed `.nt`
  - `space-optimized`: gzip each part into one `.nt.gz` aggregate and delete the source part immediately
- `--rdf-compression {gzip,brotli,none}` raw RDF artifacts to retain
- `--representations {hdt,cottas,none}` queryable primary representations
- `--artifact-compression {gzip,brotli,none}` optional packaging applied to each selected representation
- `--hdt-strategy {auto,partitioned,single}`
  - `auto`: in full mode, build smaller HDT chunks and merge them with native `hdtc`
  - `partitioned`: always use chunked HDT generation for HDT-based methods
  - `single`: always use one `rdf2hdt` run per RDF input
  - with `space-optimized`, use `auto` or `partitioned`; `single` cannot consume the gzip stream without expanding it
- `--chunk-target-bytes` target uncompressed bytes per HDT/COTTAS chunk
- `--chunk-min-bytes` minimum uncompressed bytes before flushing a chunk group
- `--chunk-max-bytes` maximum uncompressed bytes in a chunk; boundaries remain on complete NT lines
- `-P, --spark-partitions` optional Spark partition hint (positive integer)
  - low-cost way to tune RMLStreamer parallelism while the wrapper still produces one aggregate RDF output
- `-k, --keep-tsv` keep hidden TSV intermediates
- `-R, --keep-rmlstreamer-rdf-output` keep the aggregate RDF output produced by RMLStreamer
- `--remove-rdf-storage-output` explicitly remove the aggregate `.nt`/`.nt.gz` after successful compression
- `-e, --estimate-size` preflight size estimate

## Sample Representation Modes

Full mode has exactly two explicit sample workflows. There is no automatic
sample-count threshold, so the same command always produces the same graph
shape and downstream consumers can select the contract they support.

### Dense (default)

Use `--sample-representation dense` for single-sample and low-sample VCFs. It
preserves the original vocabulary model:

- every record/sample pair is a `vcfr:SampleCall`;
- every represented FORMAT slot is a `vcfr:FormatFieldValue`;
- the VCF file declares `vcfr:representationProfile vcfr:DenseRepresentation`.

With the default rules, these triples are appended directly from `records.tsv`;
the large expanded helper TSVs are not materialized. The final graph is still
dense and grows approximately with `variants × samples × FORMAT fields`.

```bash
vcf-rdfizer --mode full \
  --input ./small.vcf \
  --sample-representation dense \
  --rdf-storage-mode plain \
  --out ./results
```

### Condensed

Use `--sample-representation condensed` for large multi-sample cohorts. It
uses the vocabulary introduced in VCF-RDFizer Vocabulary 1.1.0:

- sample columns are declared once as an ordered `vcfr:SampleSet` of reusable
  `vcfr:VCFSample` resources;
- each genotype-bearing call has one `vcfr:CohortCallMatrix`;
- each FORMAT key has one `vcfr:FormatValueVector`, rather than one RDF value
  resource per sample;
- the VCF file declares
  `vcfr:representationProfile vcfr:CondensedRepresentation`.

`vcfr:encodedValues` uses `vcfr:VCFTextVector`: one tab-separated lexical item
per sample in `vcfr:sampleIndex` order. Commas inside a FORMAT value remain part
of that item, and absent values are emitted as `.` so all vectors stay aligned.
Consumers reconstruct sample `i`'s value for a FORMAT key by selecting position
`i` from its vector. This changes genotype graph growth to approximately
`samples + variants × FORMAT fields`; the literal payload still contains all
source values, but they no longer cause per-value RDF structural triples.

```bash
vcf-rdfizer --mode full \
  --input ./large-cohort.vcf.gz \
  --sample-representation condensed \
  --rdf-storage-mode space-optimized \
  --representations hdt \
  --out ./results
```

The workflow resolver runs only one sample emitter. Condensed mode rejects
custom mappings that consume expanded `sample_calls.tsv` or
`sample_format_values.tsv`, because running those dense maps alongside the
condensed emitter would create both representations and restore the semantic
inflation this mode is designed to avoid. Remove those helper-table consumers
or select dense mode. Custom rules with no helper-table consumers remain
compatible with condensed emission.

## TSV Mode Flags

- `-i, --input` required VCF file or directory
- Outputs per-run benchmark summary in `run_metrics/<RUN_ID>/tsv_metrics.csv`
- Raw TSV timing + artifact JSON per input in `run_metrics/<RUN_ID>/raw_metrics/tsv_*`

## Compression Mode Flags

- `--rdf` required input `.nt` or `.nt.gz` file

## Decompression Mode Flags

- `-C, --compressed-input` required `.nt.gz`, `.nt.br`, `.hdt`, `.cottas`, `.cottas.gz`, or `.cottas.br`
- `-d, --decompress-out` optional explicit output `.nt` path (must be inside `--out`)

## Index-Only Mode Flags

- `-H, --hdt` existing `.hdt` file; creates or regenerates its sibling sidecar
- `--cottas` existing `.cottas` file; rebuilds its embedded query index in place
- Exactly one of `--hdt` or `--cottas` is required.
- HDT indexing is Java-free: the image uses `hdtc` 1.1.0 to generate the
  canonical v1-1 sidecar named `<file>.hdt.index.v1-1`.
- COTTAS indexes are stored inside the Parquet-based `.cottas` file, so the
  file is rewritten atomically; no separate COTTAS index file is expected.
- Existing indexes are intentionally replaced. Use this mode when an HDT
  sidecar is missing/stale or when a COTTAS file needs its query ordering and
  zone-map metadata rebuilt. No VCF conversion, RDF conversion, packaging, or
  decompression output is produced.
- The operation is also run automatically after each partitioned HDT merge.

## Quick Start

Show help:

```bash
vcf-rdfizer --help
```

Full pipeline (plain aggregate RDF):

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-storage-mode plain \
  --rdf-compression none \
  --representations none \
  --out ./results
```

Full pipeline (plain aggregate, chunked HDT + native `hdtc` merge):

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-storage-mode plain \
  --rdf-compression none \
  --representations hdt \
  --hdt-strategy partitioned \
  --chunk-target-bytes 536870912 \
  --chunk-min-bytes 134217728 \
  --chunk-max-bytes 1073741824 \
  --out ./results
```

Full pipeline (space-optimized aggregate with shared HDT and COTTAS chunks):

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-storage-mode space-optimized \
  --rdf-compression none \
  --representations hdt,cottas \
  --chunk-target-bytes 536870912 \
  --chunk-min-bytes 134217728 \
  --chunk-max-bytes 1073741824 \
  --out ./results
```

Full pipeline with a Spark partition hint:

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-storage-mode space-optimized \
  --spark-partitions 8 \
  --rdf-compression none \
  --representations hdt \
  --out ./results
```

Full pipeline with custom rules + keep RMLStreamer RDF output:

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rules ./rules/my_rules.ttl \
  --rdf-storage-mode plain \
  --rdf-compression brotli \
  --representations hdt \
  --keep-rmlstreamer-rdf-output \
  --out ./results
```

Ultra-small full pipeline:

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-storage-mode space-optimized \
  --rdf-compression none \
  --representations hdt \
  --hdt-strategy partitioned \
  --remove-rdf-storage-output \
  --out ./results
```

Queryable HDT and COTTAS plus gzip/Brotli packages:

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-storage-mode space-optimized \
  --rdf-compression none \
  --representations hdt,cottas \
  --artifact-compression gzip,brotli \
  --out ./results
```

TSV-only benchmark:

```bash
vcf-rdfizer \
  --mode tsv \
  --input ./vcf_files \
  --out ./results
```

Compression-only:

```bash
vcf-rdfizer \
  --mode compress \
  --rdf ./results/sample/sample.nt \
  --rdf-compression none \
  --representations hdt \
  --artifact-compression gzip \
  --out ./results
```

Compression-only from a space-optimized aggregate:

```bash
vcf-rdfizer \
  --mode compress \
  --rdf ./results/sample/sample.nt.gz \
  --rdf-compression none \
  --representations hdt,cottas \
  --chunk-target-bytes 536870912 \
  --out ./results
```

Decompression-only:

```bash
vcf-rdfizer \
  --mode decompress \
  --compressed-input ./results/sample/sample.hdt \
  --out ./results
```

COTTAS decompression, including an externally packaged COTTAS file:

```bash
vcf-rdfizer \
  --mode decompress \
  --compressed-input ./results/sample/sample.cottas.gz \
  --out ./results
```

Initialize an index for an existing HDT:

```bash
vcf-rdfizer \
  --mode index \
  --hdt ./results/sample/sample.hdt \
  --out ./results
```

Regenerate the embedded index for an existing COTTAS file:

```bash
vcf-rdfizer \
  --mode index \
  --cottas ./results/sample/sample.cottas \
  --out ./results
```

`--mode index` is deliberately an in-place maintenance operation. It mounts
only the directory containing the selected artifact and writes metrics under
`<out>/run_metrics/<RUN_ID>/index_metrics.json`. HDT indexing creates a
versioned sidecar beside the input. COTTAS indexing rewrites the existing
`.cottas` file through `pycottas.cat` with one input file, keeping the data in
the same artifact while rebuilding its embedded index. If the operation fails,
the original COTTAS file is left in place; HDT's previous sidecars are restored.

HDT merging and indexing do not start a Java HDT tool. They use native `hdtc`:
`hdtc create` merges partitioned HDTs and `hdtc index` streams an HDT through
disk-backed external sorters. Both default to a 512 MiB soft memory budget.
Override index creation for any wrapper mode with an environment variable such
as:

```bash
HDT_INDEX_MEMORY_LIMIT=2G vcf-rdfizer \
  --mode index \
  --hdt ./results/sample/sample.hdt \
  --out ./results
```

Use `HDT_MERGE_MEMORY_LIMIT` in the same way to tune a partitioned merge.
Accepted values use an `M` or `G` suffix. Lower values reduce in-memory sort
buffers and may increase temporary I/O; higher values can improve performance
when memory is available. Temporary files live in the container's `/work` area
and are removed after the attempt.

In `full` mode, an HDT sidecar-index failure is non-fatal when the HDT data
itself remains readable; the run continues and the HDT can be repaired later
with the standalone command above. If COTTAS generation/indexing cannot
produce a usable artifact, COTTAS-specific outputs are skipped while the rest
of the full pipeline continues. These warnings are printed in the run output
and written to `run_metrics/<RUN_ID>/index_warnings.json`. The raw RDF is
retained when a representation-dependent output was unavailable so the
standalone index command or a later rerun has a recoverable source.

## Output Layout

Given `--out ./results`:

- final outputs:
  - `./results/<sample>/...`
- per-run metrics/logs:
  - `./results/run_metrics/<RUN_ID>/...`
- hidden intermediates:
  - `./results/.intermediate/tsv/`

For an existing RDF input named `test-larger.nt` or `test-larger.nt.gz`,
`--mode compress --out ./results` always uses one directory:

```text
./results/test-larger/test-larger.hdt
./results/test-larger/test-larger.hdt.index.v1-1
./results/test-larger/test-larger.cottas
./results/test-larger/test-larger.nt.gz
```

The same basename rule applies in full mode. VCF-RDFizer performs an output
collision check before Docker or conversion starts and never overwrites a
planned pipeline artifact. The exception is the deliberate `--mode index`
maintenance operation, which regenerates the selected artifact's index in
place. Choose a new `--out` directory, or rename/remove a conflicting pipeline
artifact before rerunning.

Intermediates are hidden by default.
Raw RDF files are removed after successful compression by default. Use
`--remove-rdf-storage-output` to make that cleanup explicit, or use
`--keep-rmlstreamer-rdf-output` to retain the aggregate RDF output instead.
The space-optimized mode retains the `.nt.gz` aggregate when `gzip` is selected
because that file is the gzip artifact itself.

## Metrics

For each run, VCF-RDFizer writes:

- `run_metrics/<RUN_ID>/metrics.csv`
- `run_metrics/<RUN_ID>/wrapper_execution_times.csv`
- `run_metrics/<RUN_ID>/progress.log`
- `run_metrics/<RUN_ID>/index_warnings.json` when full-run HDT/COTTAS index
  generation was unsuccessful but the pipeline continued
- `run_metrics/<RUN_ID>/index_metrics.json` for standalone HDT/COTTAS index mode
- `run_metrics/<RUN_ID>/<format>_index_metrics.json` is also written for the
  selected format (`hdt` or `cottas`) for compatibility/discovery

Compression metrics now include per-method:

- `wall_seconds_*`
- `user_seconds_*`
- `sys_seconds_*`
- `max_rss_kb_*`

When HDT or COTTAS is selected, compression also validates the final base
artifact before packaging or RDF cleanup. The validator reads the source
triple count, streams the artifact back through the native decoder, and
requires equal counts. HDT validation also initializes the versioned `.hdt.index.*`
sidecar. Validation results and `source_triples`/`decoded_triples` are stored
in the per-run compression JSON and in the HDT/COTTAS columns of `metrics.csv`.
Compression fails closed if the artifact cannot be decoded or the counts do
not match. In compression-only mode, the source count is obtained by a
streaming fallback when no upstream conversion metrics are available.

For full runs, a readable HDT whose sidecar index could not be created is
validated with the index check skipped, marked with `index_status: "failed"`,
and reported in `index_warnings.json`; this allows packaging and later stages
to continue. COTTAS failures are reported the same way, but dependent COTTAS
artifacts are marked as not generated because the COTTAS file itself is not
usable. Explicit standalone `--mode index` runs remain strict and return a
failure status when regeneration fails.

For partitioned HDT/COTTAS runs, the final method metric reports one
sample-level result, while raw metrics also include a sample-scoped
`__partitioned_compression__` artifact describing chunk conversion, merge
rounds, and the generated chunk guide.

Metrics may use internal stage names such as `hdt_gzip` and `cottas_brotli`.
These correspond to the public combination of `--representations` and
`--artifact-compression`; users do not need to pass those compound names.

## Chunked Compression

Full mode always uses one of the two aggregate storage modes. Both modes create
one logical N-Triples aggregate. The space-optimized mode streams each
RMLStreamer part through gzip and deletes that part before processing the next
one, so it avoids retaining both the part files and a full uncompressed
aggregate.

When HDT or COTTAS is selected, the aggregate is read sequentially and split
into complete N-Triples records. Only one uncompressed chunk is present at a
time: it is consumed by both converters and removed before the next chunk is
read. This is especially important for `space-optimized` `.nt.gz` aggregates,
which must not be expanded into a second full raw-RDF copy. HDT chunks are
merged with the Java-free `hdtc create` command, which accepts existing HDT
inputs; the final HDT index is generated after merging with `hdtc index`.
COTTAS chunks are merged with
`pycottas.cat`, which rebuilds the query indexes for the merged representation.

After each final HDT/COTTAS base artifact is produced, VCF-RDFizer performs a
streaming decode/count check. This verifies both readability and that the
decoded artifact contains exactly the number of source triples. The check is
performed before `.hdt.gz`, `.hdt.br`, `.cottas.gz`, or `.cottas.br` packaging,
and before raw RDF cleanup.

Partitioned HDT/COTTAS compression runs in an ephemeral Docker-managed
workspace. Temporary RDF chunks, COTTAS/DuckDB scratch data, intermediate
representations, and merge files are not written to the output directory.
After a successful or failed run, the temporary workspace is removed; only
the selected final artifacts and normal run metrics remain on the host.
Each COTTAS conversion and merge also receives a fresh container-local DuckDB
workspace, which is removed as soon as that operation completes. This prevents
state from one chunk being reused by another and requires no user configuration.

HDT merging and index generation use the pinned Rust `hdtc` 1.1.0 executable,
not `hdtCat`, `hdtSearch.sh`, or another Java HDT process. `hdtc create`
merges the chunk HDTs with disk-backed external sorts, and `hdtc index` reads
BitmapTriples as a stream to build the object/predicate orderings. This avoids
the JVM heap path that can fail with `java.lang.OutOfMemoryError` while
producing the same canonical HDT v1-1 sidecar,
`<file>.hdt.index.v1-1`, used by hdt-java and hdt-cpp.

For standalone index mode, existing versioned sidecars are moved aside while
regeneration runs and restored if indexing fails. Incomplete replacements are
removed before restoration, so a failed or interrupted attempt does not leave
a partial index. Sort runs use `/work` and are removed when the command exits.
The image defaults both `HDT_INDEX_MEMORY_LIMIT` and
`HDT_MERGE_MEMORY_LIMIT` to `512M`. The wrapper forwards an explicitly set
host value of either variable into the relevant Docker command.
`HDT_INDEX_WORK_ROOT` can override the scratch root when invoking
`/opt/vcf-rdfizer/ensure_hdt_index.sh` directly inside the container.

COTTAS does not expose a separate index sidecar. Its index is part of the
Parquet artifact and is selected when the artifact is written. Standalone
COTTAS index mode uses `pycottas.cat` to write a new temporary COTTAS file from
the existing one with the default `spo` index, then atomically replaces the
original. This is still index-only from the pipeline's point of view: it does
not rerun VCF-to-RDF conversion or create an RDF output, but it may require
temporary disk space and time comparable to rewriting the COTTAS file.

The record-safe chunk plan and per-stage timings are retained in the raw
partitioned-compression metrics JSON for diagnostics. The temporary chunk
files and guide are not retained as host files.

For the default mapping, multi-sample VCF columns remain compact in
`records.tsv`. In dense mode, canonical `SampleCall` and `FormatFieldValue`
triples are streamed directly into the final `.nt` or `.nt.gz` aggregate rather
than first writing expanded helper rows. In condensed mode, the same input pass
emits shared samples, call matrices, and FORMAT vectors, avoiding both the
helper-table multiplier and the per-sample RDF structural multiplier.

The implementation keeps COTTAS conversion scratch state inside the Docker
container and removes temporary unpacked package files when decompression
finishes.

## Rules

- default rules file: `rules/default_rules.ttl`
- rules guide: `rules/README.md`

## Troubleshooting

If Docker permission issues occur, rerun with a Docker-allowed user (or configure Docker group/sudo access on your system).

If HDT compression fails on very large RDF files, use
`--rdf-storage-mode space-optimized` or `--rdf-storage-mode plain` with
`--hdt-strategy partitioned`, then lower `--chunk-target-bytes` and
`--chunk-max-bytes` to reduce each converter's working set. Both final HDT
merge and index creation are disk-backed, so ensure the Docker data volume has
enough temporary space for their external sorts. To reduce their bounded
in-memory buffers further, set `HDT_MERGE_MEMORY_LIMIT` and/or
`HDT_INDEX_MEMORY_LIMIT` (for example, `512M`); lower limits can require more
temporary I/O. Free space in the output filesystem alone does not increase the
Docker volume capacity.

Safe termination:

- Press `Ctrl+C` to interrupt a run.
- The wrapper exits with code `130`, writes progress to `run_metrics/<RUN_ID>/progress.log`, and performs best-effort cleanup of tracked intermediates.
- Raw RDF cleanup on interrupt follows `--keep-rmlstreamer-rdf-output`:
  - with `--keep-rmlstreamer-rdf-output`, raw RDF files are preserved
  - without it, tracked raw RDF files are removed during interrupt cleanup

## Citation

If you use VCF-RDFizer in a publication, please cite:

VCF-RDFizer maintainers. (2026). *VCF-RDFizer* (Version 2.1.0) [Computer software]. GitHub. https://github.com/ecrum19/VCF-RDFizer

BibTeX:

```bibtex
@software{vcf_rdfizer_2026,
  author  = {{VCF-RDFizer maintainers}},
  title   = {VCF-RDFizer},
  year    = {2026},
  version = {2.1.0},
  url     = {https://github.com/ecrum19/VCF-RDFizer},
  note    = {Computer software}
}
```

You can also use the machine-readable citation file: `CITATION.cff`.

## Contributing

Contributions are welcome. If you want to improve VCF-RDFizer:

- Open an issue first for bug reports, feature requests, or design changes.
- Fork the repo and create a feature branch from `main`.
- Keep changes focused and include/update tests for behavior changes.
- Run the unit tests locally before opening a PR:

```bash
python3 -m unittest discover -s test -p "test_*_unit.py" -q
```

- In your PR, include what changed, why it changed, and how you validated it.
- Use clear commit messages (for Docker publish control, include `[publish-docker]` only when intended).

## Licensing

- Project license: `LICENSE` (MIT)
- Third-party runtime notices: `THIRD_PARTY_NOTICES.md`
