# Changelog

## 2026-09-03 — Disk-backed COTTAS merge for condensed cohorts

- Pinned the image to DuckDB 1.5.5 so COTTAS merge behavior cannot vary with
  Docker build-cache state or the newest dependency accepted by pycottas.
- Included the bounded DuckDB stderr tail in code-1 merge failures. The error
  now identifies the real cause (such as unavailable spill storage, permission
  problems, a malformed chunk, or a DuckDB exception) instead of reporting
  only `exit_code=1`.
- Replaced the final merge's hash-backed `SELECT DISTINCT` with an external
  lexical sort and `LAG`-based adjacent-row deduplication. This produces the
  same RDF triple set and requested COTTAS ordering without requiring a global
  in-memory hash table for every distinct triple.

- Replaced the final COTTAS merge/reindex implementation with a dedicated
  disk-backed DuckDB connection. `pycottas.cat` in version 1.1.0 performs its
  global `DISTINCT` and `ORDER BY` through the process-global in-memory
  connection, so both all-input and final pairwise merges could receive
  `SIGKILL` on the 36-million-triple condensed cohort.
- The new merge preserves the same global RDF set semantics and `spo` Parquet
  ordering, but configures DuckDB with a dedicated `.duckdb` database,
  `COTTAS_MERGE_MEMORY_LIMIT=512M`, `COTTAS_MERGE_THREADS=1`, and a disposable
  `/work` spill directory. The final merge can therefore externalize sort and
  distinct state instead of exhausting container memory.
- Forwarded explicit host-side `COTTAS_MERGE_MEMORY_LIMIT` and
  `COTTAS_MERGE_THREADS` values into partitioned full/compress runs and
  standalone COTTAS index mode.
- Updated diagnostics, README troubleshooting, and regression tests to target
  `cottas-merge-disk` rather than the obsolete pairwise merge path.

### Rerun guidance

Build an image from this revision before retrying. Start with the default
512 MiB/one-worker merge budget; if the Docker cgroup cap is below that,
lower it (for example `COTTAS_MERGE_MEMORY_LIMIT=256M`). Ensure Docker has
substantial free disk space for temporary DuckDB spill files. The preserved
raw `.nt.gz` can be retried with `--mode compress`, avoiding another RDF run.

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
the retained raw `.nt.gz`). The disk-backed merge workflow supersedes this
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
