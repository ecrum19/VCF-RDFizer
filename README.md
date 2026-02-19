## VCF-RDFizer

VCF-RDFizer generates [RDF](https://www.w3.org/2001/sw/wiki/RDF) serializations 
of [VCF files](https://samtools.github.io/hts-specs/VCFv4.2.pdf) as [N-Triples](https://www.w3.org/TR/n-triples/)
using [RML](http://rml.io/) rules and the [RMLStreamer](https://github.com/RMLio/RMLStreamer) application. 

## Overview

Pipeline steps:
1. Convert VCF to TSV (`src/vcf_as_tsv.sh`)
2. Convert TSV to RDF with RMLStreamer (`src/run_conversion.sh`)
3. Compress resultant RDF (`src/compression.sh`)

Wrapper modes:
- `full`: run the entire VCF -> TSV -> RDF -> compression pipeline
- `compress`: compress a designated RDF file (`.nt` or `.nq`) only
- `decompress`: decompress a designated `.gz`, `.br`, or `.hdt` RDF file

`src/vcf_as_tsv.sh` now writes:
- Per-VCF intermediate TSVs:
  - `<sample>.records.tsv`
  - `<sample>.header_lines.tsv`
  - `<sample>.file_metadata.tsv`

The default mapping emits triples (no graph term). The conversion step compiles all RMLStreamer output parts into one N-Triples file named after the TSV basename (for example `sample.nt` in `out/sample/`).

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
python3 vcf_rdfizer.py --mode full --input ./vcf_files
```

3. Full pipeline with a custom mapping and size pre-check:
```bash
python3 vcf_rdfizer.py --mode full --input ./vcf_files --rules ./rules/my_rules.ttl --estimate-size
```

4. Compression-only mode (compress one `.nt` into selected formats):
```bash
python3 vcf_rdfizer.py --mode compress --nq ./out/sample/sample.nt --compression gzip,brotli
```

5. Decompression-only mode (auto output path under `./out/decompressed/`):
```bash
python3 vcf_rdfizer.py --mode decompress --compressed-input ./out/gzip/sample.nt.gz
```

6. Decompression-only mode with explicit output file:
```bash
python3 vcf_rdfizer.py --mode decompress --compressed-input ./out/hdt/sample.hdt --decompress-out ./out/decompressed/sample_from_hdt.nt
```

Outputs:
- `./tsv` for TSV intermediates
- `./out` for RDF output
  - conversion outputs per TSV basename in `./out/<sample>/`
  - each conversion output directory contains one merged N-Triples file:
    - `./out/<sample>/<sample>.nt`
  - compressed outputs preserve source RDF extension (for example `sample.nt.gz`, `sample.nt.br`)
  - compressed outputs:
    - `./out/gzip/*.gz`
    - `./out/brotli/*.br`
    - `./out/hdt/*.hdt`
  - decompressed outputs (decompression mode default):
    - `./out/decompressed/*.nt`
- `./run_metrics` for logs and metrics
  - `run_metrics/metrics.csv` includes both conversion and compression metrics per run
  - conversion step artifacts:
    - `run_metrics/conversion-time-<output_name>-<run_id>.txt`
    - `run_metrics/conversion-metrics-<output_name>-<run_id>.json`
  - compression step artifacts:
    - `run_metrics/compression-time-<method>-<output_name>-<run_id>.txt`
    - `run_metrics/compression-metrics-<output_name>-<run_id>.json`
  - `run_metrics/.wrapper_logs/wrapper-<timestamp>.log` stores detailed Docker/stdout/stderr command output

Small VCF fixtures for RDF size/inflation test runs:
- `test_vcf_files/infl100.vcf` (100 total lines)
- `test_vcf_files/infl1k.vcf` (1000 total lines)
- `test_vcf_files/infl10k.vcf` (10000 total lines)

Example inflation check:
```bash
python3 vcf_rdfizer.py --mode full --input test_vcf_files/infl1k.vcf --compression none --keep-tsv
wc -l out/infl1k/infl1k.nt
```

## How Dependencies Are Handled

The Docker image bundles:
- Java 11 runtime
- HDT Java libraries
- Brotli and Node.js
- RMLStreamer standalone jar (downloaded at build time)
- The conversion scripts from `src/`

## Wrapper Checks

The wrapper validates:
- Docker is installed and running
- Mode-specific required inputs are provided
- Full mode input path exists and contains `.vcf` or `.vcf.gz`
- Full mode rules file exists
- Full mode converts only the VCF file(s) selected at pipeline start (ignores unrelated preexisting TSV intermediates)
- Compression mode input is an RDF file (`.nt` or `.nq`)
- Decompression mode input is `.gz`, `.br`, or `.hdt`
- HDT mode pre-checks that `rdf2hdt.sh` is executable and that Java is available
- Docker image exists or is built (if `--image-version` is set, it will attempt to pull that version and fail if missing)
- Raw command output is written to a hidden wrapper log file instead of printed directly to the terminal
- Optional preflight storage estimate (`--estimate-size`) with a disk-space warning if the upper-bound estimate exceeds free space

## Size Estimation Logic

Use `--estimate-size` to print a rough preflight estimate before conversion starts.

Current heuristic per input file:
- If input is `.vcf`: use on-disk size as the expanded VCF size
- If input is `.vcf.gz`: estimate expanded VCF as `compressed_size * 5.0`
- Estimate TSV intermediates as `expanded_vcf * 1.10`
- Estimate RDF N-Triples as a range: `expanded_vcf * 4.0` to `expanded_vcf * 12.0`

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
- `-q, --nq, --rdf`: compression mode input RDF file (`.nt` or `.nq`)
- `-C, --compressed-input`: decompression mode input (`.gz`, `.br`, or `.hdt`)
- `-d, --decompress-out`: decompression mode output RDF file path (default `.nt`)
- `-o, --out` (default `./out`): RDF output directory (and compression/decompression output root)
- `-t, --tsv` (default `./tsv`): TSV output directory (full mode)
- `-I, --image` (default `ecrum19/vcf-rdfizer`): Docker image repo (no tag) or full image reference
- `-v, --image-version` (default `1.0.0` effective tag when omitted): image tag/version to use when `--image` has no tag
- `-b, --build`: force docker build
- `-B, --no-build`: fail if image missing
- `-n, --out-name` (default `rdf`): fallback output basename in full mode
- `-M, --metrics` (default `./run_metrics`): metrics/log directory
- `-c, --compression` (default `gzip,brotli,hdt`): compression methods (`gzip,brotli,hdt,none`)
- `-k, --keep-tsv`: keep TSV intermediates (full mode)
- `-e, --estimate-size`: print rough input/TSV/RDF size estimates and free disk before running (full mode)
- `-h, --help`: show usage guide and exit

## Rules Directory

- `rules/default_rules.ttl`: active default mapping aligned to `https://w3id.org/vcf-rdfizer/vocab#`
- `rules/rules.ttl`: preserved legacy mapping (for comparison/migration)
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

Install HDT library:
```
git clone git@github.com:rdfhdt/hdt-java.git
cd hdt-java

sudo apt install openjdk-11-jdk
sudo update-alternatives --config java  # and choose jdk-11 as default 

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
export PATH="$JAVA_HOME/bin:$PATH"

mvn clean install -DskipTests

cd hdt-java-cli
mvn clean install -DskipTests
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
