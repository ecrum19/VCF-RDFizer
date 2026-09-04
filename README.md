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
3. Semantic validation of a compressed RDF graph against its source VCF

The VCF-RDFizer vocabulary is available at [https://w3id.org/vcf-rdfizer/vocab#](https://w3id.org/vcf-rdfizer/vocab#).

## Requirements

- Python 3.10+
- Docker (installed and running)

When VCF-RDFizer is connected to an interactive terminal, it shows a
lightweight Rich spinner/progress display. With redirected output or without
Rich installed, it instead prints compact status lines; CI output remains
quiet. Validation uses the same display and reports each preflight/SPARQL
query as it starts and completes. Use `--quiet` to suppress these terminal
progress displays (and validation's per-query/summary chatter) while keeping
the progress sidecar, command log, and metrics collection active. Use
`--no-progress` when the sidecar and terminal progress should both be disabled.
RMLStreamer progress reports the bytes and output parts already written;
partitioned HDT/COTTAS runs report source triples, chunks, and the currently
active merge/index stage. These updates are best-effort and do not scan RDF
content a second time or retain progress history in memory.

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

Release maintainers: see [`scripts/RELEASING.md`](scripts/RELEASING.md) for the PyPI,
Docker Hub, and conda-forge release procedure.

## Important CLI Rule

`--out` is required for all modes.

This is the run output root directory. VCF-RDFizer places:
- final RDF/compression outputs
- run metrics/logs
- hidden intermediates

inside this directory.

## Modes

- `full`: VCF -> TSV -> RDF -> compression (and optional semantic validation with `--validate`)
- `tsv`: VCF -> TSV only (benchmarking)
- `compress`: compress an existing `.nt` or `.nt.gz`
- `decompress`: decompress `.nt.gz`, `.nt.br`, `.hdt`, `.cottas`, `.cottas.gz`, or `.cottas.br`
- `validation`: compare a source VCF with its `.nt` or `.nt.gz` RDF using six semantic SPARQL queries
- `index`: only generate or regenerate the query index for an existing `.hdt` or `.cottas`

In `full` mode with multiple VCF inputs, failures are isolated per input:
- the run continues with remaining files
- failed inputs are summarized in `run_metrics/<INPUT_LABEL>__<RUN_ID>/reports/failed_inputs.csv`

## Main Flags (Most Used)

- `-m, --mode {full,compress,decompress,tsv,validation,index}`
- `-o, --out` required output root directory
- `--rdf-compression` final raw RDF codecs: `gzip`, `brotli`, or `none`
- `--representations` queryable RDF outputs: `hdt`, `cottas`, or `none`
- `--artifact-compression` packaging codecs for selected representations: `gzip`, `brotli`, or `none`
- `--hdt-strategy {auto,partitioned,single}` HDT generation policy
- `--chunk-target-bytes`, `--chunk-min-bytes`, `--chunk-max-bytes` shared record-safe chunk sizing
- `--sample-representation {expanded,condensed}` genotype graph shape (`expanded` by default)
- `--validate` (or `--run-validation`) run semantic VCF/RDF validation for every input in a full run
- `--filter-oracle {auto,bcftools,cyvcf2}` FILTER oracle used by validation (`auto` by default)
- `--quiet` suppress terminal progress displays while retaining sidecar/log/metrics tracking
- `--no-progress` disable terminal progress and progress sidecar creation
- `-I, --image` Docker image repo (default `ecrum19/vcf-rdfizer`)
- `-v, --image-version` Docker tag/version
- `-b, --build` force Docker build
- `-B, --no-build` fail if image not found
- `-h, --help` show full usage

## Validation Mode

Validate one source VCF against the `.nt` or `.nt.gz` aggregate from the same
conversion. Gzip input is decompressed, parsed, and queried only inside the
Docker container; any temporary raw N-Triples are removed before the container
exits. To run
the same checks as part of a full conversion, add `--validate`; validation then
runs once per input after RDF/compression and accepts either the generated
`.nt` or `.nt.gz` aggregate.

```bash
vcf-rdfizer --mode validation \
  --input ./cohort.vcf.gz \
  --rdf ./results/cohort/cohort.nt.gz \
  --sample-representation condensed \
  --out ./validation-results
```

Use `expanded` for the default graph shape and `condensed` for the vector-based
cohort graph. Reports from standalone and full-run validation use the canonical
metrics tree:
`run_metrics/<input-label>__<run-id>/reports/validation/<dataset-id>/`, with a
stage summary at `stages/validation/<dataset-id>.json`. See [Semantic VCF/RDF
validation](docs/validation.md) for query definitions, preflight checks, result
statuses, and cleanup evidence.

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
- `--sample-representation {expanded,condensed}` sample genotype representation
  - `expanded` (default): one `SampleCall` per record/sample and one `FormatFieldValue` per FORMAT key
  - `condensed`: reusable file-level samples plus one ordered value vector per record/FORMAT key
- `--validate` run semantic VCF/RDF validation once per input after RDF/compression;
  detailed results are stored beneath `run_metrics/.../reports/validation/`
- `--filter-oracle {auto,bcftools,cyvcf2}` FILTER oracle for `--validate`
- `--quiet` suppress terminal progress and validation query chatter while retaining logs/metrics
- `--no-progress` disable progress sidecars and terminal progress displays
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

See [Expanded and Condensed Knowledge Representations](docs/sample-representation-guide.md)
for a detailed, worked explanation of the graph shapes, scaling behavior, and
selection trade-offs.

### Expanded (default)

Use `--sample-representation expanded` for single-sample and low-sample VCFs. It
preserves the original vocabulary model:

- every record/sample pair is a `vcfr:SampleCall`;
- every represented FORMAT slot is a `vcfr:FormatFieldValue`;
- the VCF file declares `vcfr:representationProfile vcfr:ExpandedRepresentation`.

With the default rules, these triples are appended directly from `records.tsv`;
the large materialized helper TSVs are not created. The final graph is still
expanded and grows approximately with `variants × samples × FORMAT fields`.

```bash
vcf-rdfizer --mode full \
  --input ./small.vcf \
  --sample-representation expanded \
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
custom mappings that consume materialized `sample_calls.tsv` or
`sample_format_values.tsv`, because running those helper-table mappings alongside
the condensed emitter would create both representations and restore the semantic
inflation this mode is designed to avoid. Remove those helper-table consumers
or select expanded mode. Custom rules with no helper-table consumers remain
compatible with condensed emission.

## TSV Mode Flags

- `-i, --input` required VCF file or directory
- Outputs per-run benchmark summary in `run_metrics/<INPUT_LABEL>__<RUN_ID>/tsv_metrics.csv`
- Writes container timing and structured TSV metrics under `timings/tsv/` and
  `stages/tsv/` in that run directory

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
`<out>/run_metrics/<INPUT_LABEL>__<RUN_ID>/stages/index/`. HDT indexing creates a
versioned sidecar beside the input. COTTAS indexing rewrites the existing
`.cottas` file through a bounded streaming Parquet rewrite, keeping the data
in the same artifact while rebuilding its embedded index. If the operation
fails, the original COTTAS file is left in place; HDT's previous sidecars are
restored.

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

COTTAS avoids both a global in-memory `DISTINCT` and a global external sort.
Each chunk is already written in `spo` order, so the final stage performs a
bounded k-way Parquet merge: it holds one small batch from each chunk, writes
one copy of each adjacent equal triple, and preserves the `spo` index. Its
memory use is controlled by `COTTAS_MERGE_BATCH_ROWS` (default `2048`), not by
the total RDF graph size or a temporary DuckDB sort area. Override it only to
tune the memory/throughput tradeoff, for example:

```bash
COTTAS_MERGE_BATCH_ROWS=4096 vcf-rdfizer \
  --mode compress \
  --rdf ./results/cohort/cohort.nt.gz \
  --rdf-compression none \
  --representations cottas \
  --out ./results
```

In `full` mode, an HDT sidecar-index failure is non-fatal when the HDT data
itself remains readable; the run continues and the HDT can be repaired later
with the standalone command above. If COTTAS generation/indexing cannot
produce a usable artifact, COTTAS-specific outputs are skipped while the rest
of the full pipeline continues. These warnings are printed in the run output
and written to `run_metrics/<INPUT_LABEL>__<RUN_ID>/reports/index_warnings.json`. The raw RDF is
retained when a representation-dependent output was unavailable so the
standalone index command or a later rerun has a recoverable source.

## Output Layout

Given `--out ./results`:

- final outputs:
  - `./results/<sample>/...`
- per-run metrics/logs:
  - `./results/run_metrics/<INPUT_LABEL>__<RUN_ID>/...`
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

Each invocation receives a descriptive metrics directory:

```text
run_metrics/<INPUT_LABEL>__<RUN_ID>/
```

`<INPUT_LABEL>` is the source filename without its recognized VCF/RDF or
representation suffix (for example, `1000G_phase3_chr20`). A multi-file input
directory uses a batch label such as `batch-vcf_data-4-inputs`. This makes a
metrics directory recognizable without opening a timestamp-named folder.

Within each run directory, VCF-RDFizer writes:

- `run.json`: source identity, resolved input paths, requested workflow
  configuration, and image selection
- `summary.json`: final status, wrapper wall time, summary table rows, and an
  index of every stage report and log
- `metrics.csv`, `tsv_metrics.csv`, and `wrapper_execution_times.csv`: compact
  analysis-ready tables when applicable
- `logs/wrapper.log` and `logs/progress.log`
- `timings/<stage>/...`: raw GNU `time -v` output from inside the relevant
  Docker container
- `stages/tsv/`, `stages/conversion/`, `stages/compression/`,
  `stages/compression_operations/`, `stages/decompression/`, and
  `stages/index/`, and `stages/validation/`: structured stage results.
  `compression_operations/`
  preserves the underlying per-RDF operation and validation reports, while
  `compression/` provides the final output-level summary.
- `stages/partitioned/`: the full result handoff from the temporary
  partitioned-compression container, including every chunk build, merge,
  validation, workspace free-space sample, exit code, CPU time, and peak RSS
- `reports/index_warnings.json` and `reports/failed_inputs.csv` when applicable
- `reports/validation/<dataset-id>/`: detailed semantic-validation reports
  (`summary.json`, query results, preflight checks, and cleanup evidence) when
  validation is requested

`input_vcf_size_bytes` in `stages/conversion/*.json` and `metrics.csv` is the
*uncompressed* size of the source VCF, so that ratios are comparable between
plain and compressed inputs. The accompanying `input_vcf_size_method` records
how it was obtained:

| Method | Meaning |
| --- | --- |
| `stat` | Input was not compressed; the on-disk size is the answer |
| `bgzf` | Exact, summed from a `bgzip`/BGZF file's block headers with no decompression |
| `gzip-sample` | Exact; the whole single-member stream fitted in the sampling budget |
| `gzip-trailer` | Exact; the 32-bit `ISIZE` trailer resolved against the file's measured compression ratio |
| `inflate` / `inflate-shell` | Fallback full decompression pass, used when the file's structure cannot settle the answer (for example concatenated non-BGZF members) |

Only the fallback costs a full pass over the input. Because indexed `.vcf.gz`
files from `bcftools`/`tabix`/`htslib` are BGZF, the usual case is measured in
milliseconds rather than minutes. The same machinery makes `--estimate-size`
report a real uncompressed input size instead of an assumed expansion factor;
it says so when it had to fall back to the assumption.

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
sample-level result while `stages/partitioned/<sample>.json` retains the full
container-stage history. It includes chunk conversion, merge strategy and
rounds, validation, generated chunk plan, workspace free-space samples, CPU
time, peak RSS, exit codes, and bounded stderr diagnostics. This report is
preserved even when the temporary Docker volume is deleted after a failure.

Metrics may use internal stage names such as `hdt_gzip` and `cottas_brotli`.
These correspond to the public combination of `--representations` and
`--artifact-compression`; users do not need to pass those compound names.

When `--validate` is used in full mode, the same `metrics.csv` row also carries
`validation_status`, `validation_exit_code`, validation wall/CPU/RSS timings,
the detailed report path, and the RDF path that was validated. The validation
stage JSON retains the full status and temporary-RDF cleanup metadata.

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
COTTAS chunk conversion uses `pycottas.rdf2cottas(..., disk=True)`. The final
COTTAS merge deliberately does **not** call `pycottas.cat`: version 1.1.0 runs
its global `DISTINCT` plus `ORDER BY` through an unbounded in-memory DuckDB
connection, which can be killed on large condensed graphs. VCF-RDFizer instead
uses a PyArrow k-way merge of the already `spo`-sorted Parquet chunks. It keeps
only a configurable batch from each input, drops adjacent duplicate triples,
and writes the final COTTAS file incrementally—no graph-wide DuckDB hash table
or external-sort spill directory is created. The merge emits processed-source
and distinct-written triple counts in the terminal progress display. If the
stage fails in full mode, the warning contains the failing exit code and, when
available, a `stderr_tail`, maximum resident set size, and Docker-workspace
free-space samples; the raw RDF remains available for a retry.

After each final HDT/COTTAS base artifact is produced, VCF-RDFizer performs a
streaming decode/count check. This verifies both readability and that the
decoded artifact contains exactly the number of source triples. The check is
performed before `.hdt.gz`, `.hdt.br`, `.cottas.gz`, or `.cottas.br` packaging,
and before raw RDF cleanup.

Partitioned HDT/COTTAS compression runs in an ephemeral Docker-managed
workspace. Temporary RDF chunks, COTTAS conversion scratch data, intermediate
representations, and merge files are not written to the output directory.
After a successful or failed run, the temporary workspace is removed; only
the selected final artifacts and normal run metrics remain on the host.
Each COTTAS conversion receives a fresh container-local DuckDB workspace,
which is removed as soon as that operation completes. The final streaming
merge does not need a DuckDB workspace or a full-data temporary sort file.

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
COTTAS index mode writes a new temporary COTTAS file through the same
bounded streaming rewrite with the default `spo` index, then atomically
replaces the original. This is still index-only from the pipeline's point of
view: it does not rerun VCF-to-RDF conversion or create an RDF output, but it
rewrites the artifact once.

The record-safe chunk plan and per-stage timings are retained in the raw
partitioned-compression metrics JSON for diagnostics. The temporary chunk
files and guide are not retained as host files.

For the default mapping, multi-sample VCF columns remain compact in
`records.tsv`. In expanded mode, canonical `SampleCall` and `FormatFieldValue`
triples are streamed directly into the final `.nt` or `.nt.gz` aggregate rather
than first writing materialized helper rows. In condensed mode, the same input pass
emits shared samples, call matrices, and FORMAT vectors, avoiding both the
helper-table multiplier and the per-sample RDF structural multiplier.

The implementation keeps COTTAS conversion scratch state inside the Docker
container and removes temporary unpacked package files when decompression
finishes.

## Rules

- default rules file: `rules/default_rules.ttl`
- rules guide: `rules/README.md`

### Custom RML Mappings

`--rules` accepts any RML mapping, so you can change what RDF the pipeline
produces without touching the wrapper. A custom mapping has to honour a small
contract, and `vcf-rdfizer-rules` (installed alongside `vcf-rdfizer`) makes it
discoverable and checkable:

```bash
vcf-rdfizer-rules columns
```

Lists the five TSV sources the pipeline generates and every column each one
provides, so you know what a mapping can reference.

```bash
vcf-rdfizer-rules init -o my_rules.ttl
```

Writes an annotated copy of the shipped default mapping to start from.

```bash
vcf-rdfizer-rules check my_rules.ttl
```

Validates the mapping *before* you spend hours on a run. It reports:

- logical-source paths the wrapper cannot rewrite per input,
- referenced columns no generated TSV provides (typos such as `CHROMOSOME`),
- which `--sample-representation` values remain usable,
- whether the mapping forces the large sample helper tables to be materialized.

Exit code is `0` when the mapping is usable and `1` when it is not; add
`--json` for scripted use. Then run it:

```bash
vcf-rdfizer --mode full -i ./cohort.vcf.gz --rules my_rules.ttl --rdf-storage-mode plain -o ./results
```

#### The contract

1. **Keep the five `csvw:url` values exactly as they are.** Full mode processes
   one VCF at a time and rewrites those literal strings to the per-input file
   names (`/data/tsv/records.tsv` becomes `/data/tsv/<sample>.records.tsv`, and
   so on). Any other path is left untouched and will not resolve.

   | Logical source | Contents |
   | --- | --- |
   | `/data/tsv/records.tsv` | One row per VCF data line |
   | `/data/tsv/header_lines.tsv` | One row per `##` header line |
   | `/data/tsv/file_metadata.tsv` | One row summarising the source VCF |
   | `/data/tsv/sample_calls.tsv` | Helper: one row per variant x sample |
   | `/data/tsv/sample_format_values.tsv` | Helper: one row per variant x sample x FORMAT key |

2. **Only reference columns the pipeline writes.** `vcf-rdfizer-rules columns`
   is authoritative; a unit test pins those lists to what `src/vcf_as_tsv.sh`
   actually emits, so they cannot drift.

3. **Think before consuming the two helper tables.** The four built-in sample
   maps are recognised by the wrapper, which then keeps those tables
   header-only and streams the genotype RDF itself. A mapping that consumes
   them in any other way forces them to be materialized in full - the largest
   intermediate the pipeline can produce - and is rejected in
   `--sample-representation condensed`, which would otherwise emit both
   genotype representations at once. `check` warns about this explicitly.

The last column of `records.tsv` is the whitespace-joined sample ids from the
`#CHROM` line (or `SAMPLES` when the VCF declares none), so its *name* varies
per input and it cannot be referenced by a fixed name. Genotype RDF is emitted
by the wrapper from that column instead; see
[`docs/sample-representation-guide.md`](docs/sample-representation-guide.md).

## Repository Layout

VCF-RDFizer is deliberately split into a thin host-side CLI and a set of
container-side stages. Nothing on the host walks the RDF itself; it plans work,
launches Docker, and reads back the JSON/CSV reports each stage writes.

| Path | Role |
| --- | --- |
| `vcf_rdfizer.py` | Host CLI: argument validation, output-collision planning, Docker orchestration, metrics assembly, mode dispatch. Also emits the multi-sample genotype RDF (see below). |
| `vcf_rdfizer_rules.py` | `vcf-rdfizer-rules` CLI: scaffold, document, and validate custom RML mappings. |
| `vcf_rdfizer_gzip.py` | Uncompressed size of a gzip/BGZF VCF without decompressing it. Used by the host preflight estimate and, inside the image, by `run_conversion.sh`. |
| `src/vcf_as_tsv.sh` | VCF -> per-input `records`/`header_lines`/`file_metadata` TSV, in one `awk` pass. |
| `src/run_conversion.sh` | Runs RMLStreamer, normalizes Spark part files, merges them into one `.nt`/`.nt.gz` aggregate, records conversion metrics. |
| `src/partitioned_compression.py` | Record-safe RDF chunking plus chunked HDT/COTTAS generation and pairwise merge, inside an ephemeral Docker volume. |
| `src/cottas_tool.py` | COTTAS `convert` / `merge` / `reindex` / `decompress` adapter over `pycottas`, with a bounded-memory streaming merge. |
| `src/ensure_hdt_index.sh` | Java-free canonical `.hdt.index.v1-1` sidecar generation via `hdtc`, with restore-on-failure. |
| `src/validate_compression.py` | Round-trip check: decode a `.hdt`/`.cottas` artifact and compare its triple count against the source. |
| `src/validation/` | Semantic VCF-vs-RDF validation: `cyvcf2`/`bcftools` oracle, SPARQL queries per representation, comparison report. |
| `rules/default_rules.ttl` | Default RML mapping (also shipped as package data in `vcf_rdfizer_data/`). |
| `test/` | `unittest` suite; the shell/pipeline tests stub `java`, `docker`, and friends so no real external tool is needed. |
| `scripts/release.py` | Version bump + release metadata automation (see `scripts/RELEASING.md`). |

Genotype RDF is the one deliberate exception to "all data processing happens in
the container": `append_expanded_sample_rdf` and `append_condensed_sample_rdf`
in `vcf_rdfizer.py` append it directly to the aggregate, because the equivalent
RML maps would first have to materialize variants x samples (x FORMAT keys)
helper TSV rows. See [`docs/sample-representation-guide.md`](docs/sample-representation-guide.md).

Further reading:

- [`docs/validation.md`](docs/validation.md) - semantic validation design and query set
- [`docs/sample-representation-guide.md`](docs/sample-representation-guide.md) - emitted genotype shapes
- [`changelog.md`](changelog.md) - dated change history
- [`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md) - funding and attribution

## Troubleshooting

If Docker permission issues occur, rerun with a Docker-allowed user (or configure Docker group/sudo access on your system).

If COTTAS indexing/merging fails on a very large RDF file, first inspect the
`cottas-merge-stream` stage in the raw partitioned-compression metrics JSON and the
run wrapper log. An `exit_code=-9` means the child was killed by `SIGKILL`,
which is normally the kernel/Docker OOM killer; a shell wrapper may report the
same event as `137`. The warning's `stderr_tail`, `max_rss_kb`, and workspace
samples distinguish COTTAS data/schema problems from Docker disk-space errors.
The streaming merge has no graph-wide DuckDB spill area: it reads at most
`COTTAS_MERGE_BATCH_ROWS` rows per input at a time (default `2048`) and writes
the result incrementally. If the host is especially memory-constrained, lower
that value; if the merge is CPU-bound and RAM is available, raise it gradually.
The final COTTAS artifact still needs ordinary output disk space. Rebuild the
image after upgrading so the streaming merge workflow is installed.

If COTTAS is optional for the experiment, rerun with
`--representations hdt`; the HDT path is independent and can remain the
queryable artifact even when COTTAS cannot fit the available memory. If COTTAS
is required and the streaming merge still receives `-9`, lower
`COTTAS_MERGE_BATCH_ROWS`, verify that Docker has enough RAM for the selected
batch size, and
check the host kernel log for an external kill; this is a resource limit, not a
vocabulary or RDF-validity problem.

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
- The wrapper exits with code `130`, writes progress to `run_metrics/<INPUT_LABEL>__<RUN_ID>/logs/progress.log`, and performs best-effort cleanup of tracked intermediates.
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
