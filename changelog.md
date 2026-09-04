# Changelog

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
