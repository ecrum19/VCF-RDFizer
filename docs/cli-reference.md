# CLI reference

Every flag `vcf-rdfizer` accepts, grouped by where it applies, with defaults and
the constraints that are enforced. `vcf-rdfizer --help` is the authority; this
page adds the *why* and the interactions.

Two companion CLIs are installed alongside: `vcf-rdfizer-rules` (see
[`rml-mappings.md`](rml-mappings.md)) and, once implemented,
`vcf-rdfizer-link` (see [`datalinking-design.md`](datalinking-design.md)).

---

## The one universal rule

`-o, --out` is **required in every mode**. It is the run output root: final
artifacts, run metrics and logs, and hidden intermediates all live beneath it.

## Modes

| `-m, --mode` | Purpose | Required input |
| --- | --- | --- |
| `full` *(default)* | VCF → TSV → RDF → compression, optionally validated | `-i/--input` |
| `tsv` | VCF → TSV only, for benchmarking | `-i/--input` |
| `compress` | Compress an existing `.nt` / `.nt.gz` | `--rdf` |
| `decompress` | Decode a compressed or indexed artifact back to N-Triples | `-C/--compressed-input` |
| `validation` | Compare a source VCF against its RDF | `-i/--input` **and** `--rdf` |
| `index` | Regenerate an artifact's query index in place | exactly one of `-H/--hdt` or `--cottas` |

## Inputs and outputs

| Flag | Meaning |
| --- | --- |
| `-i, --input` | VCF file or directory. Only `*.vcf` and `*.vcf.gz` are recognised; a directory is enumerated one level deep and snapshotted at run start |
| `--rdf` | RDF input for `compress`, or the artifact to check in `validation` |
| `-C, --compressed-input` | `.nt.gz`, `.nt.br`, `.hdt`, `.cottas`, `.cottas.gz`, `.cottas.br` |
| `-H, --hdt` / `--cottas` | Existing artifact for `--mode index` |
| `-d, --decompress-out` | Explicit output `.nt` path; must be inside `--out` |
| `-o, --out` | **Required.** Run output root |
| `-n, --out-name` | Fallback basename when one cannot be inferred (default `rdf`) |

## Graph shape

| Flag | Values | Default | Effect |
| --- | --- | --- | --- |
| `-r, --rules` | path to `.ttl` | shipped `default_rules.ttl` | RML mapping; see the contract in [`rml-mappings.md`](rml-mappings.md) |
| `--sample-representation` | `expanded`, `condensed` | `expanded` | Genotype graph shape |
| `--info-representation` | `structured`, `raw` | `structured` | `structured` adds typed `InfoFieldValue` nodes alongside `infoRaw` |
| `--header-representation` | `structured`, `basic` | `structured` | `structured` types each `##` line and lifts its attributes |

There is **no automatic sample-count threshold**: the same command always
produces the same graph shape, so a downstream consumer can rely on the contract
it selected. QUAL is always emitted regardless of these options.

`condensed` rejects a custom mapping that consumes the materialized sample
helper tables, because that would emit both genotype representations at once.

## Compression plan

| Flag | Values | Default |
| --- | --- | --- |
| `--rdf-storage-mode` | `plain`, `space-optimized` | **required in full mode** |
| `--rdf-compression` | `gzip`, `brotli`, `none` | `gzip,brotli` |
| `--representations` | `hdt`, `cottas`, `none` | `hdt` |
| `--artifact-compression` | `gzip`, `brotli`, `none` | `none` |
| `--hdt-strategy` | `auto`, `partitioned`, `single` | `auto` |
| `--chunk-target-bytes` | bytes | 512 MiB |
| `--chunk-min-bytes` | bytes | 128 MiB |
| `--chunk-max-bytes` | bytes | 1 GiB |

Constraints that are enforced rather than documented-and-hoped:

- Each selector takes a comma-separated list; `none` must appear alone.
- `--artifact-compression` requires at least one selected representation.
- `--hdt-strategy single` cannot consume a `space-optimized` gzip stream.

`-c, --compression` is a hidden legacy alias retained for backward
compatibility. Use the three explicit selectors.

## Intermediates and cleanup

| Flag | Effect |
| --- | --- |
| `-k, --keep-tsv` | Keep the hidden TSV intermediates |
| `-R, --keep-rmlstreamer-rdf-output` | Keep the RDF aggregate after compression |
| `--remove-rdf-storage-output` | Explicitly remove the aggregate after successful compression |
| `-e, --estimate-size` | Preflight size estimate; no conversion |

## Validation

| Flag | Values | Default | Meaning |
| --- | --- | --- | --- |
| `--validate` / `--run-validation` | — | off | Run validation once per input in full mode |
| `--validate-artifacts` | `aggregate`, `hdt`, `cottas`, `all` | `aggregate` | Which produced artifacts to check, each in its own report directory |
| `--validation-id` | name | source basename | Report directory name; existing directories are never overwritten |
| `--validation-engine` | `comunica`, `qlever`, `hdt`, `cottas`, `all`, or a comma-separated list | `comunica` | SPARQL backend(s); a scale and performance decision, never a semantic one. `hdt`/`cottas` query the compressed artifact in place. Several engines answer the whole query set, are cross-checked against each other, and are timed in `benchmark.csv` |
| `--filter-oracle` | `auto`, `bcftools`, `cyvcf2` | `auto` | FILTER-field oracle |
| `--shacl-shapes` | path | off | Independent structural layer via `pyshacl`; in-memory, so not for cohort scale |
| `--strict-conformance` | — | off | Promote a missing-token conformance anomaly from report to failure |
| `--validation-query-timeout` | seconds | 3600 | Per-query timeout, every engine |
| `--qlever-memory-gb` | N | 4 | QLever index and server memory budget |
| `--qlever-port` | N | 7019 | Container-local only; never published |
| `--qlever-startup-timeout` | seconds | 900 | Wait for the server after indexing |
| `--qlever-index-arg` / `--qlever-server-arg` | string, repeatable | — | Extra arguments for the QLever binaries |

A validation failure in full mode marks that input as failed, retains its raw
RDF for inspection, and makes the run exit non-zero — so a pipeline can be gated
on semantic correctness rather than on Docker having returned.

`--mapping-policy` exists in `validation_runner.py` but is **not exposed by the
wrapper**; see the known gap in
[`rml-mappings.md`](rml-mappings.md#4-what-a-custom-mapping-costs-in-validation).

## Docker

| Flag | Default | Meaning |
| --- | --- | --- |
| `-I, --image` | `ecrum19/vcf-rdfizer` | Image repository |
| `-v, --image-version` | latest resolved | Image tag |
| `-b, --build` | off | Force a local build |
| `-B, --no-build` | off | Fail if the image is not present rather than building |

## Output control

| Flag | Effect |
| --- | --- |
| `--quiet` | Suppress terminal progress and the validator's per-query chatter; sidecar, command log and metrics are still written |
| `--no-progress` | Also disable progress-sidecar creation |
| `-P, --spark-partitions` | RMLStreamer parallelism hint; does not change the output |

## Environment variables

These are read by the container and, where noted, forwarded by the wrapper when
set on the host.

| Variable | Default | Effect |
| --- | --- | --- |
| `HDT_MERGE_MEMORY_LIMIT` | `512M` | Soft memory budget for `hdtc create` |
| `HDT_INDEX_MEMORY_LIMIT` | `512M` | Soft memory budget for `hdtc index` |
| `COTTAS_MERGE_BATCH_ROWS` | `2048` | Rows held per input in the streaming COTTAS merge |
| `HDT_INDEX_WORK_ROOT` | `/work` | Scratch root when calling `ensure_hdt_index.sh` directly |
| `QLEVER_INDEX_COMMAND` | built-in | Full argv template with `{index}` `{input}` `{memory}` placeholders |
| `QLEVER_SERVER_COMMAND` | built-in | Full argv template with `{index}` `{port}` `{memory}` placeholders |

Memory limits accept an `M` or `G` suffix. Lower values reduce in-memory sort
buffers and increase temporary I/O. Their scratch lives on the **Docker data
volume**, not the output filesystem.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success (possibly with recorded index warnings) |
| `1` | One or more inputs failed, including a semantic validation failure |
| `130` | Interrupted with `Ctrl+C`; progress written and tracked intermediates cleaned up |

---

## See also

- [Representations](representations.md) — what the compression flags build
- [Validation](validation.md) — what the validation flags check
- [Output and metrics](output-and-metrics.md) — where the results land
