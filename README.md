## VCF-RDFizer

[![Unit Tests](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/tests.yml/badge.svg)](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/tests.yml)
[![Publish Python](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/publish-python.yml/badge.svg)](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/publish-python.yml)
[![Publish Docker](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/publish-docker.yml/badge.svg)](https://github.com/ecrum19/VCF-RDFizer/actions/workflows/publish-docker.yml)
[![Codecov](https://codecov.io/gh/ecrum19/VCF-RDFizer/graph/badge.svg)](https://codecov.io/gh/ecrum19/VCF-RDFizer)
[![PyPI version](https://img.shields.io/pypi/v/vcf-rdfizer)](https://pypi.org/project/vcf-rdfizer/)
[![Python versions](https://img.shields.io/pypi/pyversions/vcf-rdfizer)](https://pypi.org/project/vcf-rdfizer/)
[![Docker Pulls](https://img.shields.io/docker/pulls/ecrum19/vcf-rdfizer)](https://hub.docker.com/r/ecrum19/vcf-rdfizer)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/vcf-rdfizer)](https://anaconda.org/conda-forge/vcf-rdfizer)
[![License](https://img.shields.io/github/license/ecrum19/VCF-RDFizer)](https://github.com/ecrum19/VCF-RDFizer/blob/main/LICENSE)

VCF-RDFizer generates [RDF](https://www.w3.org/2001/sw/wiki/RDF) serializations 
of [VCF files](https://samtools.github.io/hts-specs/VCFv4.2.pdf) as [N-Triples](https://www.w3.org/TR/n-triples/)
using [RML](http://rml.io/) rules and the [RMLStreamer](https://github.com/RMLio/RMLStreamer) application. 

## Overview

Pipeline steps:
1. Convert one VCF to TSV (`src/vcf_as_tsv.sh`)
2. Convert TSV to RDF with RMLStreamer (`src/run_conversion.sh`)
3. Compress RDF (wrapper uses Dockerized compression commands; `src/compression.sh` remains available for standalone script use)
4. Repeat per input VCF (for directory/multi-file inputs)

Wrapper modes:
- `full`: run the entire VCF -> TSV -> RDF -> compression pipeline
- `compress`: compress a designated RDF file (`.nt` or `.nq`) only
- `decompress`: decompress a designated `.gz`, `.br`, or `.hdt` RDF file

`src/vcf_as_tsv.sh` now writes:
- Per-VCF intermediate TSVs:
  - `<sample>.records.tsv`
  - `<sample>.header_lines.tsv`
  - `<sample>.file_metadata.tsv`

The default mapping emits triples (no graph term).

Full mode requires `--rdf-layout` to choose how post-RML RDF parts are handled:
- `aggregate`: concatenate RMLStreamer output parts into a single `<sample>.nt`, then compress that file.
- `batch`: keep RMLStreamer output parts as separate `.nt` files and compress each file individually.

Tradeoffs:
- `aggregate` advantages: easier downstream consumption (single file per sample), simpler transfer/indexing.
- `aggregate` disadvantages: very large intermediate files can cause disk/memory pressure.
- `batch` advantages: lower peak file size per artifact, can improve resilience on constrained disks.
- `batch` disadvantages: downstream consumers must handle multiple files per sample.

By default, raw RDF `.nt/.nq` files are removed after compression to save disk space. Use `--keep-rdf` to keep them.

Vocabulary references:
- Ontology: [vcf-rdfizer-vocabulary.ttl](https://github.com/ecrum19/VCF-RDFizer-vocabulary/blob/main/ontology/vcf-rdfizer-vocabulary.ttl)
- SHACL: [vcf-rdfizer-vocabulary.shacl.ttl](https://github.com/ecrum19/VCF-RDFizer-vocabulary/blob/main/shacl/vcf-rdfizer-vocabulary.shacl.ttl)

## Quick Start (Docker + Python)

Prereqs:
- Docker (running)
- Python 3.9+

If you need help installing Docker see [here](https://docs.docker.com/engine/install/).

Once Docker is installed activate it via the CLI command:
```
sudo systemctl enable --now docker
```


Example usage:

1. Show CLI help (all modes and options):
```bash
python3 vcf_rdfizer.py -h
```

2. Full pipeline with default mapping (`rules/default_rules.ttl`):
```bash
python3 vcf_rdfizer.py --mode full --input ./vcf_files --rdf-layout aggregate
```

3. Full pipeline with a custom mapping and size pre-check:
```bash
python3 vcf_rdfizer.py --mode full --input ./vcf_files --rdf-layout aggregate --rules ./rules/my_rules.ttl --estimate-size
```

3b. Full pipeline in batch mode (compress each RMLStreamer output file separately):
```bash
python3 vcf_rdfizer.py --mode full --input ./vcf_files --rdf-layout batch --compression hdt
```

3c. Full pipeline while keeping raw `.nt/.nq` files:
```bash
python3 vcf_rdfizer.py --mode full --input ./vcf_files --rdf-layout aggregate --keep-rdf
```

4. Compression-only mode (compress one `.nt` into selected formats):
```bash
python3 vcf_rdfizer.py --mode compress --nq ./out/sample/sample.nt --compression gzip,brotli
```

4b. Compression-only mode using compound HDT-first methods:
```bash
python3 vcf_rdfizer.py --mode compress --nq ./out/sample/sample.nt --compression hdt_gzip,hdt_brotli
```

5. Decompression-only mode (auto output path under `./out/`):
```bash
python3 vcf_rdfizer.py --mode decompress --compressed-input ./out/sample/sample.nt.gz
```

6. Decompression-only mode with explicit output file:
```bash
python3 vcf_rdfizer.py --mode decompress --compressed-input ./out/sample/sample.hdt --decompress-out ./out/sample_from_hdt.nt
```

## Official Installation Paths

Recommended user-facing install commands once releases are published:

1. `pipx` (best for CLI tools):
```bash
pipx install vcf-rdfizer
vcf-rdfizer --help
```

2. `pip`:
```bash
pip install vcf-rdfizer
vcf-rdfizer --help
```

3. `conda` (after conda-forge recipe is merged):
```bash
conda install -c conda-forge vcf-rdfizer
vcf-rdfizer --help
```

Note: these installs provide the Python wrapper CLI. Docker is still required at runtime.

Outputs:
- `./tsv` for TSV intermediates
- `./out` for RDF output
  - conversion outputs per sample in `./out/<sample>/`
  - `--rdf-layout aggregate`:
    - merged N-Triples file: `./out/<sample>/<sample>.nt`
    - compressed outputs: `./out/<sample>/<sample>.nt.gz`, `.br`, `.hdt`, `.hdt.gz`, `.hdt.br`
  - `--rdf-layout batch`:
    - raw RMLStreamer part files stay separate (for example `part-00000.nt`, `part-00001.nt`, ...)
    - each part is compressed individually (for example `part-00000.nt.gz`, `part-00000.hdt`, ...)
  - decompressed outputs (decompression mode default):
    - `./out/<sample>/<sample>.nt`
- `./run_metrics` for logs and metrics
  - each wrapper invocation creates a run-specific subdirectory: `run_metrics/<RUN_ID>/`
    - example: `run_metrics/20260225T120434/`
  - `run_metrics/<RUN_ID>/metrics.csv` includes both conversion and compression metrics for that run
    - in `--rdf-layout batch`, compression metrics are aggregated across all part files so CSV stays one row per sample
    - compound-compression fields are explicit and separate from raw-RDF compression:
      - `gzip_on_hdt_*` (gzip applied to `.hdt`)
      - `brotli_on_hdt_*` (brotli applied to `.hdt`)
      - `hdt_source` (`generated` vs `existing` when reused)
  - conversion step artifacts:
    - `run_metrics/<RUN_ID>/conversion_time/<output_name>/<RUN_ID>.txt`
    - `run_metrics/<RUN_ID>/conversion_metrics/<output_name>/<RUN_ID>.json`
  - compression step artifacts:
    - `run_metrics/<RUN_ID>/compression_time/<method>/<output_name>/<RUN_ID>.txt`
    - `run_metrics/<RUN_ID>/compression_metrics/<output_name>/<RUN_ID>.json`
  - wrapper runtime artifacts:
    - `run_metrics/<RUN_ID>/wrapper_execution_times.csv` (one row for that run with mode, elapsed time, status, and full-mode triple totals when available)
  - `run_metrics/<RUN_ID>/wrapper_logs/<RUN_ID>.log` stores detailed Docker/stdout/stderr command output

Small VCF fixtures for RDF size/inflation test runs:
- `test/test_vcf_files/test-100.vcf` (100 total lines)
- `test/test_vcf_files/test-1k.vcf` (1000 total lines)
- `test/test_vcf_files/test-10k.vcf` (10000 total lines)

Example inflation check:
```bash
python3 vcf_rdfizer.py --mode full --input test/test_vcf_files/test-1k.vcf --rdf-layout aggregate --compression none --keep-tsv --keep-rdf
wc -l out/test-1k/test-1k.nt
```

## How Dependencies Are Handled

The Docker image bundles:
- Java 11 runtime
- HDT-cpp (`rdf2hdt`, `hdt2rdf`)
- Brotli and Node.js
- RMLStreamer standalone jar (downloaded at build time)
- The conversion scripts from `src/`

## Wrapper Checks

The wrapper validates:
- Docker is installed and running
- Mode-specific required inputs are provided
- Full mode input path exists and contains `.vcf` or `.vcf.gz`
- Full mode rules file exists
- Full mode requires `--rdf-layout` (`aggregate` or `batch`)
- Full mode converts only the VCF file(s) selected at pipeline start (ignores unrelated preexisting TSV intermediates)
- Full mode runs TSV -> RDF -> compression sequentially per selected VCF to reduce peak disk usage
- Compression mode input is an RDF file (`.nt` or `.nq`)
- Decompression mode input is `.gz`, `.br`, or `.hdt`
- In compression mode, a warning is shown before HDT compression if input `.nt` is larger than 5 GB
- HDT mode uses `rdf2hdt` / `hdt2rdf` (HDT-cpp) in the container
- Compound methods `hdt_gzip` and `hdt_brotli` first ensure `<sample>.hdt` exists, then compress that HDT artifact
- For compound methods, if `<sample>.hdt` already exists in the output directory, the wrapper reuses it (no extra HDT regeneration)
- Docker image exists or is built (if `--image-version` is set, it will attempt to pull that version and fail if missing)
- Docker commands are attempted without `sudo` first, then automatically retried with `sudo` if needed
- Docker runs as the host UID/GID by default to prevent root-owned output files on mounted volumes
- If mounted output/metrics paths are not writable (e.g., stale root-owned files), the wrapper automatically attempts a one-time in-container permission repair before running
- Raw command output is written to a hidden wrapper log file instead of printed directly to the terminal
- A concise elapsed-time summary is printed at the end of each mode run and written to `run_metrics/<RUN_ID>/wrapper_execution_times.csv`
- Full mode prints triples produced per input (and total) when conversion metrics are available
- Optional preflight storage estimate (`--estimate-size`) with a disk-space warning if the upper-bound estimate exceeds free space

## Size Estimation Logic

Use `--estimate-size` to print a rough preflight estimate before conversion starts.

Current heuristic per input file:
- If input is `.vcf`: use on-disk size as the expanded VCF size
- If input is `.vcf.gz`: estimate expanded VCF as `compressed_size * 5.0`
- Estimate TSV intermediates as `expanded_vcf * 1.10`
- Estimate RDF N-Triples as a range: `expanded_vcf * 42.0` to `expanded_vcf * 67.0`

Current RDF inflation calibration points:
- test1: `5.9 KB -> 248 KB` (~42x)
- test2: `61 KB -> 2.6 MB` (~44x)
- test3: `612 KB -> 26.6 MB` (~43x)
- real1: `386 MB -> ~25 GB` (~66x)

Accuracy statement:
- This is a coarse planning estimate, not a guarantee.
- Real output size depends heavily on record count, INFO/FORMAT richness, and mapping complexity.
- Treat it as a risk indicator for disk exhaustion, especially the upper bound.

## Configuration

CLI usage:
```
python3 vcf_rdfizer.py --mode <full|compress|decompress> [mode-specific options] [global options]
```

Options:
- `-m, --mode` (default `full`): execution mode (`full`, `compress`, `decompress`)
- `-i, --input`: full mode input path (`.vcf` / `.vcf.gz` file or directory)
- `-r, --rules`: full mode RML mapping `.ttl` (default `rules/default_rules.ttl`)
- `-l, --rdf-layout` (required in full mode): RDF post-processing strategy (`aggregate` or `batch`)
- `-q, --nq, --nt, --rdf`: compression mode input RDF file (`.nt` or `.nq`)
- `-C, --compressed-input`: decompression mode input (`.gz`, `.br`, or `.hdt`)
- `-d, --decompress-out`: decompression mode output RDF file path (default `.nt`)
- `-o, --out` (default `./out`): RDF output directory (and compression/decompression output root)
- `-t, --tsv` (default `./tsv`): TSV output directory (full mode)
- `-I, --image` (default `ecrum19/vcf-rdfizer`): Docker image repo (no tag) or full image reference
- `-v, --image-version` (default `latest` effective tag when omitted): image tag/version to use when `--image` has no tag
- `-b, --build`: force docker build
- `-B, --no-build`: fail if image missing
- `-n, --out-name` (default `rdf`): fallback output basename in full mode
- `-M, --metrics` (default `./run_metrics`): metrics root directory (a `<RUN_ID>/` subdirectory is created per run)
- `-c, --compression` (default `gzip,brotli,hdt`): compression methods (`gzip,brotli,hdt,hdt_gzip,hdt_brotli,none`)
- `-k, --keep-tsv`: keep TSV intermediates (full mode)
- `-R, --keep-rdf`: keep raw `.nt/.nq` RDF outputs after compression (full mode; default is delete)
- `-e, --estimate-size`: print rough input/TSV/RDF size estimates and free disk before running (full mode)
- `-h, --help`: show usage guide and exit

Environment override:
- `VCF_RDFIZER_DOCKER_AS_USER=0`: disable host UID/GID mapping for Docker runs (not recommended; can reintroduce root-owned output files)

## Packaging And Publishing (Step-by-Step)

This repo now includes:
- Python packaging metadata: `pyproject.toml`
- Packaged fallback default rules data for installs outside a source checkout: `vcf_rdfizer_data/rules/default_rules.ttl`
- CI package smoke test: `.github/workflows/tests.yml`
- PyPI publish workflow (tag-based): `.github/workflows/publish-python.yml`
- Docker Hub publish workflow (tag-based): `.github/workflows/publish-docker.yml`
- Conda recipe starter: `conda-recipe/meta.yaml`

### 1) Local packaging check

Run this before every release:
```bash
python -m pip install --upgrade pip build twine
python -m build
python -m twine check dist/*
python -m pip install dist/*.whl
vcf-rdfizer --help
```

### 2) Publish to PyPI (official release)

1. Bump version in `pyproject.toml`.
2. Commit and push.
3. Create and push a release tag that matches version:
```bash
git tag v1.0.0
git push origin v1.0.0
```
4. GitHub Actions workflow `publish-python.yml` builds and publishes automatically.

### 3) Configure PyPI trusted publishing (one-time)

In PyPI project settings:
1. Add a trusted publisher.
2. Repository: `ecrum19/VCF-RDFizer`
3. Workflow: `publish-python.yml`
4. Environment: `pypi`

Without this, the publish job will fail with an authentication/trust error.

### 4) Publish to conda-forge

1. Update `conda-recipe/meta.yaml`:
  - set `version`
  - set `sha256` for the GitHub release tarball
2. Submit recipe PR to `conda-forge/staged-recipes`.
3. After feedstock creation, update version/sha in feedstock PRs for each release.

Detailed conda notes are in `conda-recipe/README.md`.

### 5) Publish Docker image to Docker Hub

1. Add GitHub repository secrets:
  - `DOCKERHUB_USERNAME`
  - `DOCKERHUB_TOKEN` (Docker Hub access token)
2. Push a release tag:
```bash
git tag v1.0.0
git push origin v1.0.0
```
3. GitHub Actions workflow `publish-docker.yml` publishes:
  - `ecrum19/vcf-rdfizer:1.0.0`
  - `ecrum19/vcf-rdfizer:1.0`
  - `ecrum19/vcf-rdfizer:1`
  - `ecrum19/vcf-rdfizer:latest`

Then users can run:
```bash
docker pull ecrum19/vcf-rdfizer
```

### 6) Keep versions aligned

For each release, align:
- `pyproject.toml` version
- Git tag (`vX.Y.Z`)
- Docker image tag (`ecrum19/vcf-rdfizer:X.Y.Z`)
- `conda-recipe/meta.yaml` version

This keeps install methods consistent and avoids user confusion.

## Packaging Troubleshooting

1. `No module named vcf_rdfizer_data` after install:
  - reinstall from fresh wheel: `pip install --force-reinstall dist/*.whl`
2. PyPI publish job fails authentication:
  - verify trusted publisher config and environment name (`pypi`)
3. Conda recipe fails source hash:
  - recompute sha256 from exact release tarball URL and update `meta.yaml`
4. Installed CLI cannot find default rules:
  - run `vcf-rdfizer --rules /absolute/path/to/default_rules.ttl ...` and report issue
5. CLI installs but conversion fails:
  - Docker not running or unavailable to current user
6. Docker publish workflow fails:
  - verify `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets

## Rules Directory

- `rules/default_rules.ttl`: active default mapping aligned to `https://w3id.org/vcf-rdfizer/vocab#`
- `rules/README.md`: guide for extending `default_rules.ttl` with custom triples maps

## Notes On Mappings

The wrapper runs RMLStreamer with working directory `/data/rules`.
`rules/default_rules.ttl` is a template. During execution, the wrapper creates per-input rules files that point to each VCF's per-file TSV triplet:
- `/data/tsv/file_metadata.tsv`
- `/data/tsv/header_lines.tsv`
- `/data/tsv/records.tsv`

If you create custom rules, keep these template TSV paths in your mapping so the wrapper can rewrite them per input file.

## Testing

Run unit tests:
```
python3 -m unittest discover -s test -p "test_*_unit.py" -v
```

Test suite notes:
- Tests live in `test/`.
- External tools are mocked (Docker, Java/RMLStreamer, gzip, brotli, rdf2hdt).
- GitHub Actions runs this suite on each push and pull request (`.github/workflows/tests.yml`).
- A successful run prints `[PASS]` markers for each test and ends with `OK`.
- See `test/README.md` for an example of successful output.


## Manual Setup (Legacy)

If you prefer to run the steps without Docker, the original manual commands are below.

### VCF Commands To Execute
Download the RMLStreamer STANDALONE jar file:
```
wget --content-disposition --trust-server-names \
  https://github.com/RMLio/RMLStreamer/releases/download/v2.5.0/RMLStreamer-v2.5.0-standalone.jar
```

Install Brotli:
```
sudo apt install brotli
```

Install HDT-cpp tools:
```
git clone https://github.com/rdfhdt/hdt-cpp.git
cd hdt-cpp

sudo apt install autoconf automake build-essential libtool pkg-config zlib1g-dev libserd-dev
./autogen.sh
./configure
make -j"$(nproc)"
sudo make install
# binaries are installed to /usr/local/bin (rdf2hdt, hdt2rdf)
```

Generate tsv representations of vcf files (for all VCFs to be converted):
```
bash src/vcf_as_tsv.sh vcf_files/ tsv/
```

Run VCF Conversion:
```
bash src/run_conversion.sh
```

## TODO:
Develop a custom conversion implementation for directly converting VCF files (without TSV conversion)
Make the run script automatically sense the vcf (converted to tsv) files for conversion...



### Quick start (standalone)

* Download `RMLStreamer-<version>-standalone.jar` from the [latest release](https://github.com/RMLio/RMLStreamer/releases/latest).
* Run it as
```
$ java -jar RMLStreamer-<version>-standalone.jar <commands and options>
```

See [Basic commands](#basic-commands) (where you replace `$FLINK_BIN run <path to RMLStreamer jar>` with `java -jar RMLStreamer-<version>-standalone.jar`)
and [Complete RMLStreamer usage](#complete-rmlstreamer-usage) for
examples, possible commands and options.

### Quick start (Docker - the fast way to test)

This runs the stand-alone version of RMLStreamer in a Docker container.
This is a good way to quickly test things or run RMLStreamer on a single machine, 
but you don't have the features of a Flink cluster set-up (distributed, failover, checkpointing). 
If you need those features, see [docker/README.md](docker/README.md). 
   
#### Example usage:

```
$ docker run -v $PWD:/data --rm rmlio/rmlstreamer toFile -m /data/mapping.ttl -o /data/output
```

#### Build your own image:

This option builds RMLStreamer from source and puts that build into a Docker container ready to run.
The main purpose is to have a one-time job image.

```
$ ./buildDocker.sh
```

If the build succeeds, you can invoke it as follows.
If you go to the directory where your data and mappings are,
you can run something like (change tag to appropriate version):

```
$ docker run -v $PWD:/data --rm rmlstreamer:v2.5.1-SNAPSHOT toFile -m /data/mapping.ttl -o /data/output.ttl 
```

There are more options for the script, if you want to use specific tags or push to Docker Hub:
```
$ ./buildDocker.sh -h

Build and push Docker images for RMLStreamer

buildDocker.sh [-h]
buildDocker.sh [-a][-n][-p][-u <username>][-v <version>]
options:
-a   Build for platforms linux/arm64 and linux/amd64. Default: perform a standard 'docker build'
-h   Print this help and exit.
-n   Do NOT (re)build RMLStreamer before building the Docker image. This is risky because the Docker build needs a stand-alone version of RMLStreamer.
-u <username>  Add an username name to the tag name as on Docker Hub, like <username>/rmlstreamer:<version>.
-p   Push to Docker Hub repo. You must be logged in for this to succeed.
-v <version>       Override the version in the tag name, like <username>/rmlstreamer:<version>. If not given, use the current version found in pom.xml.
```

### Moderately quick start (Docker - the recommended way)

If you want to get RMLStreamer up and running within 5 minutes using Docker, check out [docker/README.md](docker/README.md)
