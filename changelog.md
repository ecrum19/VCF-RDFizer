# Changelog

## 2026-09-02 — COTTAS large-input merge reliability

- Diagnosed the previous generic `COTTAS merge/index creation failed` warning:
  the warning is emitted only after per-chunk COTTAS conversion succeeds and
  the final indexed merge returns a non-zero status. HDT generation/indexing
  is independent and is not implicated by that warning.
- Replaced the partitioned COTTAS pairwise merge tree with one multi-input
  `pycottas.cat` pass. The query index is now built once, avoiding repeated
  Parquet rewrites and reducing peak temporary-file use for large VCF-derived
  graphs.
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
and the run wrapper log for `cottas-merge-all` and its `stderr_tail`. For a
resource error, lower `--chunk-target-bytes` and `--chunk-max-bytes`, or enlarge
the Docker data-volume allocation. The raw `.nt.gz` retained by the failed run
is a valid recovery source.
