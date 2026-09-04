# Output layout, metrics, and run reports

Every VCF-RDFizer run writes three kinds of thing: the artifacts you asked for,
a complete record of how they were produced, and evidence about what was
cleaned up. This document is the map.

---

## 1. Output layout

`--out` is required for every mode. It is the run output **root**, and
everything lives beneath it:

```text
<out>/
  <sample>/                                  final artifacts
    <sample>.nt | <sample>.nt.gz | .nt.br
    <sample>.hdt  +  <sample>.hdt.index.v1-1
    <sample>.cottas
    <sample>.hdt.gz | .cottas.br | ...
  run_metrics/<INPUT_LABEL>__<RUN_ID>/       per-run reports and logs
  .intermediate/tsv/                         hidden intermediates
  decompressed/                              --mode decompress default target
```

The directory name and every artifact basename come from the source filename
with its recognized VCF/RDF/representation suffix removed. `--mode compress` on
`test-larger.nt` and on `test-larger.nt.gz` therefore both write into
`<out>/test-larger/`.

**Collision policy.** Before Docker starts, the wrapper computes every artifact
the run intends to write and fails if any already exists. It never overwrites a
planned pipeline artifact. Choose a new `--out`, or move the conflicting file.
`--mode index` is the single deliberate exception, because regenerating an index
in place is its entire purpose.

**Intermediate cleanup.** TSV intermediates are hidden and removed unless
`--keep-tsv`. Raw RDF is removed after successful compression by default;
`--remove-rdf-storage-output` makes that explicit and
`--keep-rmlstreamer-rdf-output` prevents it. Under `space-optimized`, the
`.nt.gz` aggregate is retained when `gzip` is a selected RDF codec, because that
file *is* the gzip artifact.

When `--validate` fails for an input, raw RDF cleanup is **skipped** for that
input and the aggregate is kept with the note `retained after validation
failure`, so the graph that failed is still available to inspect.

## 2. The run metrics directory

```text
run_metrics/<INPUT_LABEL>__<RUN_ID>/
```

`<INPUT_LABEL>` is the source filename without its suffix (for example
`1000G_phase3_chr20`); a multi-file directory input uses a batch label such as
`batch-vcf_data-4-inputs`. The point is that a metrics directory is
recognisable without opening a timestamp-named folder.

| Path | Contents |
| --- | --- |
| `run.json` | Source identity, resolved input paths, requested configuration, image selection |
| `summary.json` | Final status, wrapper wall time, summary rows, and an index of every stage report and log |
| `metrics.csv` | Analysis-ready per-output table |
| `tsv_metrics.csv` | TSV-mode benchmark table |
| `wrapper_execution_times.csv` | Host-side stage timings |
| `logs/wrapper.log`, `logs/progress.log` | Command log and progress history |
| `timings/<stage>/…` | Raw GNU `time -v` output from inside the relevant container |
| `stages/tsv/`, `stages/conversion/`, `stages/compression/`, `stages/compression_operations/`, `stages/decompression/`, `stages/index/`, `stages/validation/` | Structured per-stage results |
| `stages/partitioned/<sample>.json` | Full handoff from the partitioned-compression container |
| `reports/index_warnings.json` | Degraded-index events |
| `reports/failed_inputs.csv` | Inputs abandoned during a multi-file run |
| `reports/validation/<validation-id>/` | Detailed semantic-validation reports |

`compression_operations/` preserves the per-RDF operation and round-trip
validation reports; `compression/` is the final output-level summary over them.

`stages/partitioned/<sample>.json` is the one to open when a large run fails.
It retains every chunk build, the merge strategy and round count, validation
results, the generated chunk plan, workspace free-space samples, CPU time, peak
RSS, exit codes, and bounded `stderr` diagnostics — **and it survives the
deletion of the temporary Docker volume**, which is what makes a failed
cohort-scale run diagnosable at all.

## 3. Input size accounting

`input_vcf_size_bytes` in `stages/conversion/*.json` and `metrics.csv` is the
**uncompressed** size of the source VCF, so ratios are comparable between plain
and compressed inputs. `input_vcf_size_method` records how it was obtained:

| Method | Meaning |
| --- | --- |
| `stat` | Input was not compressed; the on-disk size is the answer |
| `bgzf` | Exact, summed from BGZF block headers with no decompression |
| `gzip-sample` | Exact; the whole single-member stream fitted in the sampling budget |
| `gzip-trailer` | Exact; the 32-bit `ISIZE` trailer resolved against the measured compression ratio |
| `inflate` / `inflate-shell` | Fallback full decompression pass |

Only the fallback costs a full pass. Indexed `.vcf.gz` files from
`bcftools`/`tabix`/`htslib` are BGZF, so the usual case is measured in
milliseconds. The same machinery makes `--estimate-size` report a real
uncompressed size rather than an assumed expansion factor — and it says so when
it had to fall back to the assumption.

## 4. Compression metrics

Per method, `metrics.csv` carries `wall_seconds_*`, `user_seconds_*`,
`sys_seconds_*`, and `max_rss_kb_*`, plus `source_triples` / `decoded_triples`
from the round-trip check described in
[`representations.md`](representations.md#6-round-trip-verification).

Metrics may use internal stage names such as `hdt_gzip` and `cottas_brotli`.
These correspond to the public combination of `--representations` and
`--artifact-compression`; nothing on the command line uses those compound names.

For partitioned runs, the method-level metric reports one sample-level result
while `stages/partitioned/` retains the full history.

## 5. Validation reports

Detailed results live at:

```text
<out>/run_metrics/<input-label>__<run-id>/reports/validation/<validation-id>/
```

with a stage-level summary at `stages/validation/<validation-id>.json`. When
several artifacts are validated in one run, the aggregate keeps
`<validation-id>/` and the representations use `<validation-id>__hdt/` and
`<validation-id>__cottas/`.

Key files: `summary.json`, `manifest.json`, `parser.json`,
`rdf-validation.json`, `materialization.json`, `preflight.json`, `sparql.json`,
`comparison.json`. Raw SPARQL Results JSON, stderr and query resource logs are
in `raw/`; normalized results in `normalized/`.

`manifest.json` records the engine that ran (including QLever's exact argv), the
source artifact's format and checksum, and every decode step.
`materialization.json` records how a compressed artifact was turned into
N-Triples and how many triples that yielded. Both the stage report and
`summary.json` record whether a gzip aggregate was decompressed inside the
container and that no validation scratch RDF was retained on the host.

Statuses: `PASS`, `MISMATCH` (both paths ran and differ),
`BLOCKED_BY_PREFLIGHT` (syntax or core graph structure failed),
`EXECUTION_FAILED` (a parser or engine could not complete).

Full treatment in [`validation.md`](validation.md).

## 6. Progress

Containers write newline-delimited JSON to a small sidecar under the run's
temporary `.progress/` area; the host polls it while the container runs and
renders it with Rich when attached to an interactive terminal, or as compact
status lines otherwise. Command logs and binary subprocess output stay separate
from the terminal UI.

| Flag | Effect |
| --- | --- |
| *(none)* | Sidecar written, terminal progress rendered |
| `--quiet` | Sidecar, command log and metrics retained; **all** terminal rendering and the validator's per-query chatter suppressed |
| `--no-progress` | Sidecar creation disabled as well |

Progress is best-effort: RMLStreamer reports bytes and output parts already
written, partitioned runs report source triples, chunks, and the active
merge/index stage. Nothing rescans RDF content a second time to produce a
number, and no progress history is retained in memory.

## 7. Interrupts and exit codes

`Ctrl+C` exits with **130**, writes progress to `logs/progress.log`, and
performs best-effort cleanup of tracked intermediates. Raw RDF cleanup on
interrupt follows `--keep-rmlstreamer-rdf-output`: with it, raw RDF is
preserved; without it, tracked raw RDF files are removed.

Otherwise the wrapper exits `0` on success and non-zero on failure. In full mode
with `--validate`, a validation failure sets a non-zero exit even when the
conversion itself succeeded, so a run can be gated in CI on semantic
correctness rather than on Docker having returned.

---

## See also

- [Architecture](architecture.md) — which process writes which report
- [Representations](representations.md) — what the compression metrics describe
- [Validation](validation.md) — reading a validation report
- [CLI reference](cli-reference.md) — the flags referenced above
