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
- `decompress`: decompress `.nt.gz`, `.nt.br`, or `.hdt`

In `full` mode with multiple VCF inputs, failures are isolated per input:
- the run continues with remaining files
- failed inputs are summarized in `run_metrics/<RUN_ID>/failed_inputs.csv`

## Main Flags (Most Used)

- `-m, --mode {full,compress,decompress,tsv}`
- `-o, --out` required output root directory
- `-c, --compression` methods: `gzip,brotli,hdt,hdt_gzip,hdt_brotli,cottas,none`
- `--hdt-strategy {auto,partitioned,single}` HDT generation policy
- `--chunk-target-bytes`, `--chunk-min-bytes`, `--chunk-max-bytes` shared record-safe chunk sizing
- `-I, --image` Docker image repo (default `ecrum19/vcf-rdfizer`)
- `-v, --image-version` Docker tag/version
- `-b, --build` force Docker build
- `-B, --no-build` fail if image not found
- `-h, --help` show full usage

## Full Mode Flags

- `-i, --input` required VCF file or directory
- `-r, --rules` mapping rules file (`.ttl`)
  - default: `rules/default_rules.ttl`
- `-l, --rdf-layout {aggregate,batch}` legacy full-mode RDF layout
- `--rdf-storage-mode {plain,space-optimized}` full-mode aggregate storage policy
  - `plain`: merge RMLStreamer parts into one uncompressed `.nt`
  - `space-optimized`: gzip each part into one `.nt.gz` aggregate and delete the source part immediately
- `--hdt-strategy {auto,partitioned,single}`
  - `auto`: in batch RDF layout or a storage mode, build smaller HDT chunks and merge them with `HDTCat`
  - `partitioned`: always use chunked HDT generation for HDT-based methods
  - `single`: always use one `rdf2hdt` run per RDF input
  - with `space-optimized`, use `auto` or `partitioned`; `single` cannot consume the gzip stream without expanding it
- `--chunk-target-bytes` target uncompressed bytes per HDT/COTTAS chunk
- `--chunk-min-bytes` minimum uncompressed bytes before flushing a chunk group
- `--chunk-max-bytes` maximum uncompressed bytes in a chunk; boundaries remain on complete NT lines
- `-P, --spark-partitions` optional Spark partition hint (positive integer)
  - low-cost way to reduce output part count by setting `spark.default.parallelism` and `spark.sql.shuffle.partitions`
- `-k, --keep-tsv` keep hidden TSV intermediates
- `-R, --keep-rdf` keep raw `.nt` after compression
- `-e, --estimate-size` preflight size estimate

## TSV Mode Flags

- `-i, --input` required VCF file or directory
- Outputs per-run benchmark summary in `run_metrics/<RUN_ID>/tsv_metrics.csv`
- Raw TSV timing + artifact JSON per input in `run_metrics/<RUN_ID>/raw_metrics/tsv_*`

## Compression Mode Flags

- `-q, --rdf, --nt` required input `.nt` file

## Decompression Mode Flags

- `-C, --compressed-input` required `.nt.gz`, `.nt.br`, or `.hdt`
- `-d, --decompress-out` optional explicit output `.nt` path (must be inside `--out`)

## Quick Start

Show help:

```bash
vcf-rdfizer --help
```

Full pipeline (aggregate RDF):

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-layout aggregate \
  --out ./results
```

Full pipeline (batch RDF parts):

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-layout batch \
  --compression hdt \
  --out ./results
```

Full pipeline (batch RDF parts, chunked HDT + HDTCat merge):

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-layout batch \
  --compression hdt \
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
  --compression hdt,cottas \
  --chunk-target-bytes 536870912 \
  --chunk-min-bytes 134217728 \
  --chunk-max-bytes 1073741824 \
  --out ./results
```

Full pipeline with low-cost partition cap (helps avoid too many tiny batch files):

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rdf-layout batch \
  --spark-partitions 8 \
  --compression hdt \
  --out ./results
```

Full pipeline with custom rules + keep RDF:

```bash
vcf-rdfizer \
  --mode full \
  --input ./vcf_files \
  --rules ./rules/my_rules.ttl \
  --rdf-layout aggregate \
  --compression hdt,brotli \
  --keep-rdf \
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
  --compression hdt_gzip \
  --out ./results
```

Compression-only from a space-optimized aggregate:

```bash
vcf-rdfizer \
  --mode compress \
  --rdf ./results/sample/sample.nt.gz \
  --compression hdt,cottas \
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

## Output Layout

Given `--out ./results`:

- final outputs:
  - `./results/<sample>/...`
- per-run metrics/logs:
  - `./results/run_metrics/<RUN_ID>/...`
- hidden intermediates:
  - `./results/.intermediate/tsv/`

Intermediates are hidden by default.
Raw RDF files are removed after compression unless `--keep-rdf` is provided.
The space-optimized mode retains the `.nt.gz` aggregate when `gzip` is selected
because that file is the gzip artifact itself.

## Metrics

For each run, VCF-RDFizer writes:

- `run_metrics/<RUN_ID>/metrics.csv`
- `run_metrics/<RUN_ID>/wrapper_execution_times.csv`
- `run_metrics/<RUN_ID>/progress.log`

Compression metrics now include per-method:

- `wall_seconds_*`
- `user_seconds_*`
- `sys_seconds_*`
- `max_rss_kb_*`

For partitioned HDT/COTTAS runs, the final method metric reports one
sample-level result, while raw metrics also include a sample-scoped
`__partitioned_compression__` artifact describing chunk conversion, merge
rounds, and the generated chunk guide.

## Chunked Compression

The new storage modes are mutually exclusive with the legacy `--rdf-layout`
option. Both modes create one logical N-Triples aggregate. The space-optimized
mode streams each RMLStreamer part through gzip and deletes that part before
processing the next one, so it avoids retaining both the part files and a full
uncompressed aggregate.

When HDT or COTTAS is selected, the aggregate is read sequentially and split
into complete N-Triples records. The chunk guide is generated during that same
pass, and the same temporary chunks are consumed by both converters before
cleanup. HDT chunks are merged with `HDTCat`, the final HDT index is generated
after merging, and COTTAS chunks are merged with `pycottas.cat`, which rebuilds
the query indexes for the merged representation.

Successful aggregate partitioned runs retain `<sample>.chunks.json` beside the
final compression artifacts. It records the record-safe chunk boundaries and
uncompressed byte ranges; temporary RDF chunks and merge intermediates are
removed after successful conversion.

See [`COMPRESSION_CHANGELOG.md`](COMPRESSION_CHANGELOG.md) for the detailed
implementation approach and operational constraints.

## Rules

- default rules file: `rules/default_rules.ttl`
- rules guide: `rules/README.md`

## Troubleshooting

If Docker permission issues occur, rerun with a Docker-allowed user (or configure Docker group/sudo access on your system).

If HDT compression fails on very large RDF files, use
`--rdf-storage-mode space-optimized` or `--rdf-storage-mode plain` with
`--hdt-strategy partitioned`, then lower `--chunk-target-bytes` and
`--chunk-max-bytes` to reduce each converter's working set.

Safe termination:

- Press `Ctrl+C` to interrupt a run.
- The wrapper exits with code `130`, writes progress to `run_metrics/<RUN_ID>/progress.log`, and performs best-effort cleanup of tracked intermediates.
- Raw RDF cleanup on interrupt follows `--keep-rdf`:
  - with `--keep-rdf`, raw `.nt` files are preserved
  - without `--keep-rdf`, tracked raw `.nt` files are removed during interrupt cleanup

## Citation

If you use VCF-RDFizer in a publication, please cite:

VCF-RDFizer maintainers. (2026). *VCF-RDFizer* (Version 1.2.3) [Computer software]. GitHub. https://github.com/ecrum19/VCF-RDFizer

BibTeX:

```bibtex
@software{vcf_rdfizer_2026,
  author  = {{VCF-RDFizer maintainers}},
  title   = {VCF-RDFizer},
  year    = {2026},
  version = {1.2.3},
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
