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
- `compress`: compress an existing `.nt`
- `decompress`: decompress `.nt.gz`, `.nt.br`, or `.hdt`

In `full` mode with multiple VCF inputs, failures are isolated per input:
- the run continues with remaining files
- failed inputs are summarized in `run_metrics/<RUN_ID>/failed_inputs.csv`

## Main Flags (Most Used)

- `-m, --mode {full,compress,decompress,tsv}`
- `-o, --out` required output root directory
- `-c, --compression` methods: `gzip,brotli,hdt,hdt_gzip,hdt_brotli,none`
- `--hdt-strategy {auto,partitioned,single}` HDT generation policy
- `-I, --image` Docker image repo (default `ecrum19/vcf-rdfizer`)
- `-v, --image-version` Docker tag/version
- `-b, --build` force Docker build
- `-B, --no-build` fail if image not found
- `-h, --help` show full usage

## Full Mode Flags

- `-i, --input` required VCF file or directory
- `-r, --rules` mapping rules file (`.ttl`)
  - default: `rules/default_rules.ttl`
- `-l, --rdf-layout {aggregate,batch}` required in full mode
- `--hdt-strategy {auto,partitioned,single}`
  - `auto`: in batch RDF layout, build smaller HDT chunks and merge them with `HDTCat`
  - `partitioned`: always use chunked HDT generation for HDT-based methods
  - `single`: always use one `rdf2hdt` run per RDF input
- `--hdt-target-chunk-bytes` target chunk size for partitioned HDT generation
- `--hdt-min-chunk-bytes` minimum size to accumulate before flushing a chunk group
- `--hdt-max-chunk-bytes` maximum size allowed before line-preserving re-splitting
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
  --hdt-target-chunk-bytes 536870912 \
  --hdt-min-chunk-bytes 134217728 \
  --hdt-max-chunk-bytes 1073741824 \
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
Raw `.nt` files are removed after compression unless `--keep-rdf` is provided.

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

For partitioned HDT runs, the final HDT metric still reports one end-to-end
HDT conversion result per sample/output, while raw metrics also include a
sample-scoped `__partitioned_hdt__` artifact describing the merged HDT build.

## HDT Optimization

When HDT-based compression is selected in `full` mode with `--rdf-layout batch`,
the default `--hdt-strategy auto` uses the existing RDF part files as split
boundaries, builds smaller HDT chunks, merges them with `HDTCat`, and
pre-generates the final `.hdt.index` file for query-ready output.

This is usually faster and more robust than converting one very large merged
`.nt` file in a single `rdf2hdt` run.

## Rules

- default rules file: `rules/default_rules.ttl`
- rules guide: `rules/README.md`

## Troubleshooting

If Docker permission issues occur, rerun with a Docker-allowed user (or configure Docker group/sudo access on your system).

If HDT compression fails on very large `.nt` files, switch to `--rdf-layout batch`
and keep `--hdt-strategy auto` or set `--hdt-strategy partitioned` explicitly.

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
