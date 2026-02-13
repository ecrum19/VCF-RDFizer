# Test Suite Coverage Rationale

This repository uses `unittest` (Python standard library) to isolate orchestration logic and shell script behavior without calling real external tools (Docker daemon, RMLStreamer, Brotli, HDT).

## What is covered

- `test/test_vcf_rdfizer_unit.py`
  - Verifies wrapper control flow for the 5-step pipeline.
  - Verifies image/version resolution behavior and error handling.
  - Verifies CLI compression option propagation into `compression.sh`.

- `test/test_vcf_as_tsv_unit.py`
  - Verifies `.vcf` and `.vcf.gz` input handling.
  - Verifies header normalization (`#CHROM` -> `CHROM`) and data row retention.
  - Verifies error path for empty input directories.

- `test/test_run_conversion_unit.py`
  - Replaces `java` with a fake executable to avoid real RMLStreamer.
  - Verifies output normalization to `.nq`.
  - Verifies unified metrics CSV row creation and schema consistency.

- `test/test_compression_unit.py`
  - Replaces `gzip`, `brotli`, and `rdf2hdt` with fake executables.
  - Verifies compression artifact generation and metrics row update.
  - Verifies `-m none` behavior (no compression outputs, metrics still updated).

## Why this coverage is useful

- It tests the highest-risk logic in this codebase: orchestration, branching, path wiring, and metrics consistency.
- It catches regressions in command construction without requiring heavyweight dependencies.
- It validates that metrics remain comparable across runs by enforcing one shared CSV schema.
