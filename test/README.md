# Test Suite Coverage Rationale

This repository uses `unittest` (Python standard library) to isolate orchestration logic and shell script behavior without calling real external tools (Docker daemon, RMLStreamer, Brotli, HDT).

## What is covered

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
    graph in ~25 named ways and asserts which corruptions the validator
    detects, producing a reproducible mutation score.
  - Requires `rdflib` (test-only, in the `dev` extra); the tests skip cleanly
    without it. See `docs/validation-methodology.md`.
  - The fixture derives the VCF, the RDF graph and the parser oracle from one
    declarative spec, and builds its graph with the project's own emitters, so
    the two halves cannot drift apart.

- `test/cross_engine_agreement.py`
  - Not a unittest module: run inside the image to assert every validation
    query returns identical values under Comunica and QLever.

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

- `test/test_gzip_size_unit.py`
  - Verifies uncompressed-size measurement for BGZF, single-member gzip, and
    concatenated members, each against a full-inflate ground truth.
  - Verifies that an unresolvable file falls back rather than reporting a wrong
    size, including the 32-bit `ISIZE` wrap and multi-member trailers.

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
