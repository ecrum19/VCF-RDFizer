# Changelog

## 2026-09-03 — COTTAS SIGKILL/OOM handling

- Classified `exit_code=-9` in partitioned index warnings as a child process
  killed by `SIGKILL` (normally the Linux kernel/Docker OOM killer). Shell
  wrappers can surface the same condition as exit code `137`.
- Changed the production COTTAS merge back to a bounded pairwise merge tree:
  each `pycottas.cat` invocation receives only two COTTAS inputs. This keeps
  peak Parquet/index memory bounded; the previous one-shot multi-input merge
  could exceed the container memory limit even though all per-chunk converts
  succeeded.
- Added the failing stage's `max_rss_kb` to `index_warnings.json`, alongside
  exit code, stderr tail, and workspace free-space samples, so OOM diagnosis
  can be compared directly with the container memory limit.
- The `merge-many` adapter remains available for explicit experiments, but is
  no longer selected by the default large-file workflow.

### Rerun guidance

Rebuild the image and rerun the full conversion. If a pairwise COTTAS merge is
still killed, lower `--chunk-target-bytes` and `--chunk-max-bytes` (for example,
to 256 MiB or 128 MiB) and/or increase the Docker/container memory limit. The
raw `.nt.gz` retained by the failed run is a valid recovery source; no
RMLStreamer reconversion is required.

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
