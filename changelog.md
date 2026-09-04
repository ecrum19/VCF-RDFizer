# Changelog

## 2026-09-04 — Custom-mapping tooling, cheap gzip sizing, dead-script removal

### Added

- `vcf-rdfizer-rules`, a second console script (`vcf_rdfizer_rules.py`) for
  authoring custom RML mappings. It replaces the `update_rules.sh` sed script
  with something that serves the actual need:
  - `columns` documents the five generated TSV sources and every column each
    one provides, including why `records.tsv`'s final column has no fixed name.
  - `init` writes an annotated copy of the default mapping to start from.
  - `check` validates a mapping against the wrapper contract *before* a long
    run: logical-source paths the wrapper cannot rewrite per input, referenced
    columns no TSV provides, which `--sample-representation` values remain
    usable, and whether the mapping forces the large sample helper tables to be
    materialized. Exits 1 on a contract violation; `--json` for scripting.
  The old script rewrote a rules file *in place* to point at fixed TSV paths,
  which the pipeline has not needed since `render_rules_for_triplet` began
  rendering a per-input copy - and which silently broke the per-input rewrite
  it was supposed to help with.
- `vcf_rdfizer_gzip.py`: uncompressed size of a gzip/BGZF file without
  decompressing it. Three tiers, and the method used is recorded next to the
  size so the metric stays auditable:
  - `bgzf` - sums the per-block `ISIZE` trailers of a `bgzip` file by walking
    block headers. Exact, no inflate, and immune to the 32-bit wrap. This is
    what `bcftools`/`tabix`/`htslib` produce, so it covers the usual case.
  - `gzip-sample` / `gzip-trailer` - single-member gzip: measured outright when
    it fits the sampling budget, otherwise the 32-bit `ISIZE` trailer resolved
    against a compression ratio sampled from the file's own prefix.
  - `inflate` - full pass, used whenever the structure cannot settle the
    answer. Concatenated non-BGZF members are detected and routed here rather
    than trusting a trailer that describes only the final member.

### Changed

- `run_conversion.sh` uses that helper for `input_vcf_size_bytes`, which
  previously always cost a full `gzip -dc | wc -c` pass over the source VCF.
  On the repository's 52 MB test fixture this is 1.115s -> 0.066s (17x); on a
  634 MB gzip whose uncompressed size exceeds 4 GiB, 13.6s -> 0.029s (470x),
  where a naive trailer read would have been 93% wrong. The shell keeps its
  `gzip -dc` fallback if the helper is unavailable.
- `stages/conversion/*.json` gains `input_vcf_size_method` recording how the
  size was obtained.
- `--estimate-size` now uses the real uncompressed input size when the file's
  structure can supply it, instead of always assuming a 5x expansion, and says
  so when it had to fall back to the assumption.

### Removed

- `src/compression.sh` and `test/test_compression_unit.py`. The wrapper issues
  its own gzip/brotli/rdf2hdt commands and delegates chunked work to
  `partitioned_compression.py`; nothing had called this script for some time,
  and it wrote a stale flat `metrics.csv` schema.
- `src/update_rules.sh` and `test/test_update_rules_unit.py`, superseded by
  `vcf-rdfizer-rules` above.

### Documentation

- README: new "Custom RML Mappings" section with the three-point contract, the
  logical-source table, and the `vcf-rdfizer-rules` workflow; a metrics table
  explaining each `input_vcf_size_method`; repository-layout and test-suite
  entries for the two new modules.
- `rules/README.md` points at the new CLI as the way to author a mapping.

### Tests

- `test/test_rules_helper_unit.py` (12 tests), including a drift guard that
  runs `src/vcf_as_tsv.sh` and asserts the documented column lists still match
  the headers it writes.
- `test/test_gzip_size_unit.py` (13 tests), each checking a measured size
  against a full-inflate ground truth across BGZF, plain, concatenated, and
  optional-header-field gzip layouts.

## 2026-09-04 — Code review: cleanup, hot-path performance, and bug fixes

Behaviour-preserving unless noted. Emitted RDF was verified byte-identical
(SHA-256) against the previous implementation for both sample representations
and for both `run_conversion.sh` storage modes.

### Fixed

- `run_compression_methods_for_rdf`: an existing `.hdt` that failed validation
  during a full run recorded a **COTTAS** index warning (wrong format, wrong
  artifact path, wrong message) via a copy-pasted block whose assignment also
  never propagated, because `cottas_failure_warning` was not `nonlocal` there.
  The block is removed; `validate_container_artifact` already downgrades a
  recoverable HDT index problem to a warning, so reaching that point means the
  artifact is genuinely unusable.
- `--mode validation` raised an uncaught `ValueError` traceback when the
  results directory already existed and was non-empty, because that check sits
  after the argument-validation `try/except`. It now prints `Error: ...` and
  exits 2 like every other input error. Covered by a new regression test.
- `count_triples_in_nt_files` (the host-side fallback triple count) used a
  slightly different rule than the container-side counters in
  `validate_compression.py` / `partitioned_compression.py`, so a fallback count
  could disagree with the authoritative one. The predicate is now shared as
  `is_triple_line` with identical semantics.

### Performance

- Sample RDF emission (`append_expanded_sample_rdf`,
  `append_condensed_sample_rdf`) no longer percent-encodes the same values on
  every record: sample-column URI components and `sampleId` literals are
  computed once per file, and FORMAT-key components are memoized. Previously
  these ran `variants x samples` and `variants x samples x FORMAT keys` times.
  ~1.6x faster at 50 samples, with the gain growing with cohort width.
- `_rml_uri_component` and `_ntriples_string_literal` short-circuit values that
  need no encoding/escaping (~2.7x and ~1.3x on typical VCF tokens).
- `SampleRecordStream` caches the derived FORMAT-key tuple and its
  duplicate-key check per distinct `FORMAT` string instead of rebuilding both
  for every record.
- `count_triples_in_nt_files` scans bytes instead of decoding to `str`, and
  reads gzip aggregates through a `BufferedReader` rather than `GzipFile`
  line-by-line.
- `run()` discards subprocess output at the file-descriptor level instead of
  buffering a whole container's output in memory only to drop it.
- `run_conversion.sh`: the `"."` -> `"."^^vcfr:Null` rewrite is now applied to
  each RMLStreamer part while it is streamed into the aggregate, instead of as
  a separate pass over the finished aggregate. This removes one full read and
  one full write of the complete RDF output, and removes the transient
  full-size temporary copy (which previously required 2x the aggregate size in
  free disk at the end of a plain-storage run).
- `run_conversion.sh`: triple counting uses one `LC_ALL=C grep -c` instead of
  `awk` / `grep | wc -l`. On a 228 MB gzip aggregate this is ~5x faster
  (6.5s -> 1.2s); it is the last unavoidable full pass over the RDF output.
- `run_conversion.sh`: the `stat` dialect is detected once instead of probed on
  every call, and the per-second progress heartbeat does one directory walk
  with no `basename` subprocess per part.

### Changed

- Space-optimized aggregates are ~14 bytes per RMLStreamer part smaller,
  because gzip members are now produced from a pipe and therefore no longer
  embed the temporary part filename and mtime. Decompressed content is
  unchanged and the output is more reproducible.

### Removed

- Dead host-side code: `resolve_input` (superseded by
  `resolve_input_snapshot`), and the host chunk planners
  `plan_record_safe_rdf_chunks`, `plan_partitioned_hdt_chunks`,
  `split_nt_file_for_hdt`, `write_nt_chunk`, `iter_rdf_binary_lines`. Chunking
  has been the container runner's job (`src/partitioned_compression.py`) since
  partitioned compression moved into an ephemeral Docker volume; keeping a
  second host-side implementation was exactly what
  `run_partitioned_representation_methods_for_rdf_files` documents against.
  Their two tests were removed with them.
- An unreachable `else:` arm in `run_full_mode`'s output summary (it iterated a
  list the guard had just proved empty), the unused `input_metrics_target`
  parameter of `run_full_mode`, an always-true `is not None` guard, and two
  unused local assignments.

### Documentation

- `vcf_rdfizer.py` gained a module-level division-of-labour note and section
  map, plus section banners for the previously unmarked regions (triple
  counting, artifact naming, destructive filesystem operations, preflight
  estimation, genotype representations, compression-plan parsing, metrics
  layout).
- `README.md`: new "Repository Layout" section explaining the host/container
  split and what each file does; fixed the broken `RELEASING.md` link
  (`scripts/RELEASING.md`); added links to `docs/`, `changelog.md`, and
  `ACKNOWLEDGEMENTS.md`.
- `src/compression.sh` and `src/update_rules.sh` headers now state that they
  are standalone utilities the pipeline does not call, and what supersedes
  each. `compression.sh` previously claimed the Python wrapper was its primary
  caller, which has not been true since compression moved inline.
- Added `ACKNOWLEDGEMENTS.md` with funding/attribution and a paper
  acknowledgement template.

## 2026-09-04 — Validation progress and quiet mode

- Integrated semantic validation with the existing JSONL progress sidecar and
  host `ProgressSession`; validation now reports its preflight/core query
  progress using the same terminal UI as conversion and compression.
- Added `--quiet` to suppress terminal progress displays and validation query
  chatter while retaining progress bookkeeping, command logs, and metrics.
- Kept `--no-progress` as the stronger opt-out that disables sidecar creation
  and progress rendering entirely.

## 2026-09-04 — Full-run semantic validation

- Added `--validate`/`--run-validation` to run the semantic VCF/RDF validator
  once per input as the final stage of a `--mode full` run.
- Full-mode validation accepts the generated plain `.nt` or gzip `.nt.gz`
  aggregate, and retains the validator's container-local temporary-file
  guarantees.
- Validation timing, status, exit code, input RDF, and report paths are now
  included in the run's `metrics.csv`, compression JSON, stage reports, and
  recursive `summary.json` report index.

## 2026-09-04 — Expanded sample representation naming

- Renamed the default per-sample genotype graph strategy to `expanded`, while
  retaining `condensed` as the alternative strategy.
- Updated the CLI default and accepted values, internal workflow/emitter names,
  tests, RML comments, and user documentation.
- The emitted profile IRI is now
  `vcfr:representationProfile vcfr:ExpandedRepresentation`.

## 2026-09-04 — Input-labelled, container-complete metrics layout

- Replaced timestamp-only run-metrics directories with
  `run_metrics/<input-label>__<run-id>/`. A single VCF/RDF/representation input
  uses its source stem (for example, `1000G_phase3_chr20`); a directory input
  with multiple VCFs receives a deterministic batch label.
- Added `run.json` and `summary.json` as the stable entry points for every
  mode. The manifest records inputs, requested configuration, output root, and
  resolved image; the summary records final status, wrapper runtime, tabular
  rows, and an index of stage reports and logs.
- Organized metrics by purpose: `logs/`, `timings/`, `stages/`, and `reports/`.
  TSV, RML conversion, compression, decompression, indexing, and warnings now
  have predictable locations instead of unrelated `raw_metrics`,
  `*_metrics`, and timestamp-nested directories.
- Compression-only, decompression, and standalone index modes now persist
  container wall/CPU/system time, peak RSS, exit code, input/output sizes, and
  structured stage reports. Full and TSV modes retain the same detail.
- Preserved the complete partitioned-compression runner handoff under
  `stages/partitioned/`, including every chunk build, merge, validation,
  temporary-workspace free-space sample, timing, peak RSS, and bounded stderr
  diagnostic. The report is retained on both successful and failed runs before
  the temporary Docker volume is removed.

## 2026-09-03 — Streaming COTTAS merge for condensed cohorts

- Replaced the final COTTAS merge/reindex implementation with a PyArrow k-way
  merge of the already `spo`-sorted Parquet chunks. It keeps one configurable
  batch per input (`COTTAS_MERGE_BATCH_ROWS`, default `2048`), deduplicates
  adjacent equal triples, and writes the final COTTAS artifact incrementally.
  It preserves RDF set semantics and the embedded COTTAS index without a
  graph-wide DuckDB hash table or external-sort spill area.
- This supersedes the prior DuckDB merge variants. A 52-chunk, 36-million-triple
  cohort exhausted 41.4 GiB of Docker workspace while DuckDB attempted an
  external sort; the streaming merge has no proportional temporary-sort file.
- Added PyArrow 22.0.0 to the image and third-party notices for deterministic
  Parquet reader/writer support.
- COTTAS merge progress now reports source triples processed and distinct
  triples written. Rich interactive displays remain available, and redirected
  terminals receive compact line-based progress updates.
- Retained the bounded stderr tail in failures so malformed COTTAS input,
  incompatible index metadata, output disk errors, and other code-1 failures
  remain actionable after the ephemeral workspace is removed.

### Rerun guidance

Build an image from this revision before retrying. The preserved raw `.nt.gz`
can be retried with `--mode compress`, avoiding another RDF run. The merge
requires normal space for the COTTAS chunks and final artifact, but no longer
requires a large DuckDB spill directory.

## 2026-09-03 — COTTAS SIGKILL/OOM handling

- Classified `exit_code=-9` in partitioned index warnings as a child process
  killed by `SIGKILL` (normally the Linux kernel/Docker OOM killer). Shell
  wrappers can surface the same condition as exit code `137`.
- Initially changed the production COTTAS merge to a bounded pairwise tree.
  This reduced input fan-in but did not bound the final graph-wide
  `DISTINCT`/`ORDER BY`; the disk-backed replacement above supersedes it.
- Added the failing stage's `max_rss_kb` to `index_warnings.json`, alongside
  exit code, stderr tail, and workspace free-space samples, so OOM diagnosis
  can be compared directly with the container memory limit.
- The `merge-many` adapter remains available for explicit experiments, but is
  no longer selected by the default large-file workflow.

### Rerun guidance

Rebuild the image and rerun the full conversion (or use `--mode compress` with
the retained raw `.nt.gz`). The current streaming k-way merge supersedes this
earlier pairwise guidance.

When COTTAS is optional, `--representations hdt` avoids the COTTAS merge
resource requirement while retaining a queryable representation.

## 2026-09-02 — COTTAS large-input merge reliability

- Diagnosed the previous generic `COTTAS merge/index creation failed` warning:
  the warning is emitted only after per-chunk COTTAS conversion succeeds and
  the final indexed merge returns a non-zero status. HDT generation/indexing
  is independent and is not implicated by that warning.
- Added a multi-input `pycottas.cat` adapter for explicit use. The production
  workflow now uses the bounded pairwise strategy documented in the 2026-09-03
  entry because the merge API can require substantial in-memory buffers.
- Added cleanup of all COTTAS chunks and merge outputs after a failed COTTAS
  attempt so stale intermediates cannot consume the Docker workspace.
- Preserved a bounded stderr tail and exit code for every partitioned stage.
  Full-run `index_warnings.json` entries now identify resource/OOM signals,
  include before/after Docker-workspace free-space samples, and retain the
  underlying COTTAS/DuckDB diagnostic when available.
- Pinned the Docker image to `pycottas==1.1.0` for reproducible behavior.
- Added regression tests for the multi-input merge command, stage diagnostics,
  and isolated COTTAS scratch cleanup.

### Rerun guidance

Rebuild the image and rerun the same full command. If the COTTAS stage still
fails, inspect `stages/partitioned/<sample>.json` and the wrapper log for the
failing `cottas-merge-r*` stage and its
`stderr_tail`. For a resource error, lower `--chunk-target-bytes` and
`--chunk-max-bytes`, or enlarge the Docker data-volume allocation. The raw
`.nt.gz` retained by the failed run is a valid recovery source.
