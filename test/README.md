# Test Suite Coverage Rationale

This repository uses `unittest` (Python standard library) to isolate orchestration logic and shell script behavior without calling real external tools (Docker daemon, RMLStreamer, Brotli, HDT).

## What is covered

- `test/test_linking_unit.py`
  - Exercises all three shipped linkers with known-answer token and interval
    cases, the actual Ensembl resolver over fake HTTP, offline cache replay,
    budgets/retries/host pacing, digest and assembly refusal, and atomic output.
  - Covers unordered/plain/gzip RDF, existing subject identity, full/post-hoc
    wiring, discovery, scaffolding and previews. No Docker or public API calls.
  - These checks do not change the core semantic mutation score; plug-in
    SPARQL/mutation auto-discovery remains planned. See `docs/datalinking.md`.

- `test/test_vcf_rdfizer_unit.py`
  - Verifies wrapper control flow for the 5-step pipeline.
  - Verifies image/version resolution behavior and error handling.
  - Verifies the separate RDF-compression, representation, and artifact-packaging plan.

- `test/test_vcf_rdfizer_cross_platform_unit.py`
  - Runs a Windows/macOS/Linux-safe subset of wrapper tests.
  - Focuses on CLI parsing, image resolution, compression method parsing, and mocked compress mode execution.

- `test/test_vcf_as_tsv_unit.py`
  - Verifies `.vcf` and `.vcf.gz` input handling.
  - Verifies header extraction and per-VCF TSV generation (`<sample>.records.tsv`, `<sample>.header_lines.tsv`, `<sample>.file_metadata.tsv`).
  - Verifies header normalization (`#CHROM` -> `CHROM`) and data row retention.
  - Verifies error path for empty input directories.

- `test/test_run_conversion_unit.py`
  - Replaces `java` with a fake executable to avoid real RMLStreamer.
  - Verifies output normalization to `.nt`.
  - Verifies unified metrics CSV row creation and schema consistency.

- `test/test_partitioned_compression_unit.py` and `test/test_cottas_tool.py`
  - Exercise the container-side chunking, merge, and COTTAS adapter logic.

- `test/test_rules_helper_unit.py`
  - Verifies the `vcf-rdfizer-rules` contract checks (source paths, column
    references, sample-representation compatibility, helper-table warnings).
  - Pins the documented TSV column lists to the headers `src/vcf_as_tsv.sh`
    actually writes, so the two cannot drift apart.

- `test/test_validation_mutation_unit.py` (+ `validation_fixtures.py`,
  `validation_mutations.py`)
  - Mutation testing for the semantic validation suite: corrupts a correct
    graph in 42 named ways and asserts which corruptions the validator
    detects, producing a reproducible mutation score (currently 76/78).
  - Requires `rdflib` (now a runtime dependency for linking); the tests still
    skip cleanly without it in source-only environments. See
    `docs/validation-methodology.md`.
  - The fixture derives the VCF, the RDF graph and the parser oracle from one
    declarative spec, and builds its graph with the project's own emitters, so
    the two halves cannot drift apart.

- `test/cross_engine_agreement.py`
  - Not a unittest module: run inside the image to assert every validation
    query returns identical values under all four engines (Comunica, QLever,
    native HDT, native COTTAS), across both representations.
  - Also runs the shipped validation decision under each engine, so an engine
    must agree with the Python oracle and not merely with the other engines.
  - Pass a comma-separated subset as the first argument to narrow it.

- `test/test_validation_logic_unit.py`
  - Mutation tests over the validator's pure comparison layer, run on the host
    without cyvcf2 or Docker.
  - Records both what a validation `PASS` detects and the coverage gaps it does
    not, so closing a gap fails a test rather than passing unnoticed.

- `test/test_validation_engines_unit.py`
  - Verifies artifact format detection and decode paths (`.nt`, `.nt.gz`,
    `.nt.br`, `.hdt`, `.cottas[.gz|.br]`) with the container tools faked.
  - Verifies Comunica and QLever engine construction, QLever's
    index/serve/teardown lifecycle and overridable command lines, and the
    wrapper's validation-target resolution.

- `test/test_validation_benchmark_unit.py`
  - Verifies multi-engine selection (`--validation-engine a,b` and `all`) in
    both the host wrapper and the container runner, and that the two layers
    cannot drift apart on which engines exist.
  - Verifies the native HDT/COTTAS engines: reusing the run's own artifact
    versus building one, Comunica's `hdt@<path>` typed-source prefix, and that
    a failing query is reported rather than raised.
  - Verifies the benchmark report and its long-format CSV, and the
    cross-engine agreement comparison.

- `test/test_gzip_size_unit.py`
  - Verifies uncompressed-size measurement for BGZF, single-member gzip, and
    concatenated members, each against a full-inflate ground truth.
  - Verifies that an unresolvable file falls back rather than reporting a wrong
    size, including the 32-bit `ISIZE` wrap and multi-member trailers.

## VCF fixtures (`test/test_vcf_files/`)

`test/test_vcf_files/*` is gitignored with an explicit `!` exception per file,
so a new fixture is invisible to git until `.gitignore` names it.

| File | Records | Samples | Uncompressed | Purpose |
| --- | --- | --- | --- | --- |
| `test-100.vcf` | 100 | 1 | 6 KB | Fast unit-test input; mapping smoke tests |
| `test-1k.vcf` | 1,000 | 1 | 60 KB | Small end-to-end runs |
| `test-10k.vcf` | 10,000 | 1 | 612 KB | Larger end-to-end runs still fast enough for CI |
| `test-larger.vcf.gz` | 1,155,741 | 1 | 296 MB | **Many records**, one sample: the record-scaling axis |
| `test-larger-multisample.vcf.gz` | 10,000 | 2,504 | 97 MB | **Many samples**, few records: the sample-scaling axis |

The two `test-larger*` fixtures are deliberately complementary. Conversion cost
is driven by *record count x sample count*, and each isolates one factor:

- `test-larger.vcf.gz` — 1.16M records x 1 sample = **1.16M sample calls**
- `test-larger-multisample.vcf.gz` — 10k records x 2,504 samples = **25.0M sample calls**

A single-sample fixture cannot exercise the per-sample fan-out at all, which in
the expanded representation is where the triple count actually comes from. Use
both, and never generalise a scaling result from one to the other.

### Provenance of `test-larger-multisample.vcf.gz`

Derived from the 1000 Genomes Project phase 3 chromosome 20 call set (b37,
2,504 samples, 1,812,841 records) by taking **20 contiguous blocks of 500
records at even intervals** across the source, keeping all 2,504 sample columns
and the original header unmodified. Blocks rather than a random sample, so
neighbouring-variant structure survives; spread across the whole chromosome, so
the allele-frequency spectrum and variant mix are representative rather than
whatever happens to sit at the start of the file.

The result spans chr20 positions 60,343-60,416,754 and contains 9,575 SNPs, 428
indels, 5 structural variants and 58 multi-allelic sites. It is
`##fileformat=VCFv4.1` (the source's own version — `test-larger.vcf.gz` is
VCFv4.2, so the pair also covers both header versions), POS-ascending,
single-contig, and verified to parse cleanly with both `bcftools stats` and
`cyvcf2` — the two readers the validation oracle itself uses. Three `##` lines
record its derivation inside the file.

1000 Genomes is open-consent, freely redistributable data, so the fixture
carries no access restrictions.

## CI matrix behavior

- Windows runners execute:
  - `test/test_vcf_rdfizer_cross_platform_unit.py`
  - package smoke test (`pip install` + `vcf-rdfizer --help`)
- macOS/Linux runners execute the full suite, including shell-script unit tests.

## Why this coverage is useful

- It tests the highest-risk logic in this codebase: orchestration, branching, path wiring, and metrics consistency.
- It catches regressions in command construction without requiring heavyweight dependencies.
- It validates that metrics remain comparable across runs by enforcing one shared CSV schema.

## What a successful run looks like

Run:

```bash
python -m unittest discover -s test -p "test_*_unit.py" -v
```

Success indicators:

- Each test prints a clear marker:
  - `[TEST] <description>`
  - `[PASS] <description>`
- `unittest` prints `ok` next to each test.
- Final summary ends with:
  - `Ran <N> tests ...`
  - `OK`

Example (truncated):

```text
[TEST] Wrapper runs all pipeline steps and forwards compression arguments.
[PASS] Wrapper runs all pipeline steps and forwards compression arguments.
...
Ran 10 tests in 0.90s
OK
```
