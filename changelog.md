# Changelog

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
fails, inspect `raw_metrics/compression_metrics/.../__partitioned_compression__`
and the run wrapper log for the failing `cottas-merge-r*` stage and its
`stderr_tail`. For a resource error, lower `--chunk-target-bytes` and
`--chunk-max-bytes`, or enlarge the Docker data-volume allocation. The raw
`.nt.gz` retained by the failed run is a valid recovery source.
