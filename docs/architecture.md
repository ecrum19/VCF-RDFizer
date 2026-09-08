# Architecture

How VCF-RDFizer is put together, why it is split the way it is, and where the
split leaks. Read this before [`conversion.md`](conversion.md) or
[`representations.md`](representations.md), which go stage by stage.

---

## 1. The shape of the tool

VCF-RDFizer is a **thin host-side Python CLI wrapping a fat Docker image**. The
host process never installs Java, Flink, HDT, QLever, `bcftools` or `cyvcf2`; it
plans work, launches containers, and reads back the JSON and CSV each stage
writes.

```text
  host                                    container (ecrum19/vcf-rdfizer)
  ─────────────────────────────────       ──────────────────────────────────
  vcf_rdfizer.py
    ├─ parse + validate arguments
    ├─ snapshot inputs
    ├─ plan output paths
    ├─ collision check
    ├─ docker run ────────────────────▶   src/vcf_as_tsv.sh      (awk)
    │                                      src/run_conversion.sh  (RMLStreamer/Flink)
    ├─ append direct RDF (see §4)
    ├─ optional host-side linking ─────▶  <sample>.links.nt (separate side-graph)
    ├─ docker run ────────────────────▶   src/partitioned_compression.py
    │                                      src/cottas_tool.py
    │                                      src/ensure_hdt_index.sh
    │                                      src/validate_compression.py
    ├─ docker run ────────────────────▶   src/validation/validation_runner.py
    └─ assemble run_metrics/
```

The reason for this shape is reproducibility. The toolchain is a pinned set of
awkward dependencies — RMLStreamer 2.5.0 on Flink, a Rust `hdtc` 1.1.0 build,
`pycottas`, Comunica 5.3.0, QLever, `pyshacl`, `cyvcf2`, `bcftools`. Asking a
user to assemble that on their own machine is asking for irreproducible results.
Pinning it in one image means a run on a laptop and a run on a cluster execute
the same binaries. The price is that **Docker is a hard requirement for
conversion and representation operations** and the image is large; see
[`limitations.md`](limitations.md). Post-hoc linking and the authoring CLIs
run entirely on the host.

## 2. Host responsibilities

| Responsibility | Why on the host |
| --- | --- |
| Argument validation and mode dispatch | Fail before the first container starts |
| Input snapshotting | A directory input is enumerated once, so files appearing mid-run cannot change the work set |
| Output path planning and collision check | The pipeline never overwrites a planned artifact; the check runs before Docker |
| Uncompressed input sizing | `vcf_rdfizer_gzip.py` reads BGZF block headers / the gzip trailer rather than decompressing |
| Docker orchestration | Mounts, user mapping, environment forwarding, permission auto-fix |
| Progress rendering | Containers write a JSONL sidecar; the host polls and renders it |
| Metrics assembly | Every stage's JSON is collected into one `run_metrics/` tree |
| Interrupt handling | `Ctrl+C` triggers best-effort cleanup of tracked intermediates |
| Optional data linking | Python plug-ins, reference/response caches and an atomic side-graph share one host runner for full and post-hoc mode |

## 3. Container responsibilities

| Component | Role |
| --- | --- |
| `src/vcf_as_tsv.sh` | One `awk` pass per VCF producing `records`, `header_lines`, `file_metadata` TSVs |
| `src/run_conversion.sh` | Runs RMLStreamer, normalizes Spark/Flink part files, merges them into one aggregate, records conversion metrics |
| `src/partitioned_compression.py` | Record-safe RDF chunking, chunked HDT/COTTAS generation, merge, all inside an ephemeral Docker volume |
| `src/cottas_tool.py` | `convert` / `merge` / `reindex` / `decompress` over `pycottas`, with a bounded-memory streaming merge |
| `src/ensure_hdt_index.sh` | Java-free `.hdt.index.v1-1` sidecar generation via `hdtc`, restoring the previous sidecar on failure |
| `src/validate_compression.py` | Round-trip check: decode an artifact, compare its triple count against the source |
| `src/validation/validation_runner.py` | Semantic VCF-vs-RDF validation: parser oracle, SPARQL queries, comparison report |

## 4. Where the split leaks, and why

The conversion design intent is "all data processing happens in the container".
There are **three direct-emission exceptions**, all in `vcf_rdfizer.py`, appending directly
to the RDF aggregate between the conversion container and the compression
container.

One rule decides what goes where:

> **The RML mapping carries every field whose RDF datatype is the same for every
> row. The wrapper carries everything else.**

Two kinds of field fall on the wrapper's side. A value that may be the VCF
missing token `.` needs `"."^^vcfc:Null` on some rows and a typed literal on
others, and RML cannot switch an object's datatype per row. A value that has to
be decomposed into several resources would need a materialized helper table
whose size is the product of its dimensions.

| Emitter | Emits | Why not RML |
| --- | --- | --- |
| `emit_sample_representation` | Expanded: `SampleCall`/`FormatFieldValue`/`Genotype`/`PhaseSet`/`LocalAlleleSet`/`BaseModification`. Condensed: `SampleSet`/`CohortCallMatrix`/`FormatValueVector`. `SampleSet` and `VCFSample` in both. | RML would first have to materialize a helper table of variants × samples (× FORMAT keys) — the largest intermediate the pipeline can produce |
| `append_header_representation_rdf` | Header-line subclasses, the ordered `HeaderAttribute` resources every structured line needs, and the INFO/FORMAT/FILTER/ALT/contig/META/SAMPLE/PEDIGREE declaration properties | RML cannot choose a class per row, nor decompose one angle-bracketed cell into an ordered set of resources |
| `emit_record_detail` | `ID`, `ALT`, `QUAL`, `FILTER`, `infoRaw` (each may be the missing token); the allele layer (`ReferenceAllele`/`AltAllele`, breakends, symbolic types); structured `InfoFieldValue` with its `FieldValueItem` decomposition; and the SV carriers | RML cannot type the missing token per row, cannot switch a literal's datatype on a declared `Type`, and the allele and value-item layers would need variants × alleles and variants × INFO-keys helper tables |

The exception count did not change with the move to the VCF Core vocabulary, but
what each emitter covers grew considerably: the vocabulary models the VCF 4.5
logical model in full, and almost all of the additions are decompositions of a
single source cell, which is exactly the category RML cannot express.

This is worth stating plainly because it has consequences:

- A **custom `--rules` mapping does not control these triples.** They are emitted
  by the wrapper regardless, unless the corresponding
  `--sample-representation` / `--header-representation` / `--info-representation`
  option turns them off. A mapping author who expects `--rules` to be the single
  source of truth for the graph will be surprised.
- The host process **does** read `records.tsv` and write N-Triples, so the claim
  that nothing on the host touches the data is not literally true. What is true
  is that no *third-party* toolchain runs on the host.
- These emitters are streaming and append-only, guarded by
  `_append_rdf_atomically`, so an interrupted append does not leave a
  half-written aggregate.

Optional data linking is a separate host-side stage in `vcf_rdfizer_linking/`.
It reads the completed aggregate, stages only join fields/keys in SQLite,
resolves the selected plug-ins, and atomically publishes a side-graph. That
extra graph scan preserves actual subject identities and makes full and
post-hoc linking share exactly one implementation. The reference interval
index lives in memory; HTTP and reference bytes are cached on disk. See
[`datalinking.md`](datalinking.md) for this extension's limits.

## 5. Data flow in full mode

```text
  input.vcf(.gz)
        │  src/vcf_as_tsv.sh (awk, one pass)
        ▼
  <sample>.records.tsv, .header_lines.tsv, .file_metadata.tsv
        │  RMLStreamer + rules/default_rules.ttl
        ▼
  Flink part files ──▶ merged aggregate  <sample>.nt  or  <sample>.nt.gz
        │  host-side direct emitters (§4) append in place
        ▼
  complete aggregate
        │  optional: --link <ids> (host-side; aggregate unchanged)
        ├──────────────▶ <sample>.links.nt
        │  optional: --rdf-compression gzip,brotli
        ├──────────────▶ <sample>.nt.gz / <sample>.nt.br
        │  optional: --representations hdt,cottas  (chunked)
        ├──────────────▶ <sample>.hdt + .hdt.index.v1-1
        ├──────────────▶ <sample>.cottas
        │  round-trip triple-count check on each base artifact
        │  optional: --artifact-compression gzip,brotli
        ├──────────────▶ <sample>.hdt.gz / .cottas.br / ...
        │  optional: --validate
        └──────────────▶ run_metrics/.../reports/validation/...
```

Multiple VCF inputs are processed **one at a time**, and a failure is isolated
to that input: the run continues and the failure is recorded in
`reports/failed_inputs.csv`.

## 6. Failure policy

The tool distinguishes three kinds of failure, and treats them differently:

| Kind | Example | Behaviour |
| --- | --- | --- |
| **Fail closed** | An HDT decodes to the wrong triple count | The stage fails; no artifact is published |
| **Degrade and record** | An HDT is readable but its sidecar index could not be built | The run continues, the artifact is marked `index_status: "failed"`, and the raw RDF is retained so it can be repaired later; recorded in `reports/index_warnings.json` |
| **Isolate** | One VCF out of twelve fails conversion | That input is abandoned, the rest proceed, and it appears in `reports/failed_inputs.csv` |

A COTTAS failure degrades differently from HDT: because the `.cottas` file
itself is unusable, every dependent COTTAS artifact is marked not generated
rather than published with a warning.

Standalone `--mode index` is deliberately **strict** — it is a maintenance
operation whose whole purpose is to produce a valid index, so a partial result
is a failure, and the previous sidecar is restored.

## 7. Isolation and cleanup guarantees

These are properties the tool tries hard to hold, and states in its reports so
they can be checked rather than trusted:

- Partitioned compression runs in an **ephemeral Docker-managed volume**.
  Chunks, COTTAS scratch, and merge files never touch the output directory, and
  the volume is removed on success *and* on failure.
- Validation mounts the source artifact **read-only** and decodes compressed
  input under the container's `/work`. Nothing decoded is written beneath
  `--out`; the stage report records that it was cleaned up.
- Each COTTAS conversion gets a fresh container-local DuckDB workspace, removed
  when that operation completes.
- Output collisions are checked *before* Docker starts, and no planned artifact
  is ever overwritten. `--mode index` is the single deliberate exception.

## 8. Source map

| Path | Role |
| --- | --- |
| [`vcf_rdfizer.py`](../vcf_rdfizer.py) | Host CLI: validation, planning, Docker orchestration, metrics, and the three direct emitters of §4 |
| [`vcf_rdfizer_rules.py`](../vcf_rdfizer_rules.py) | `vcf-rdfizer-rules`: scaffold, document, and check custom RML mappings |
| [`vcf_rdfizer_gzip.py`](../vcf_rdfizer_gzip.py) | Uncompressed size of a gzip/BGZF VCF without decompressing it |
| [`src/`](../src) | Container-side stages (table in §3) |
| [`rules/default_rules.ttl`](../rules/default_rules.ttl) | Default RML mapping, also shipped as package data in `vcf_rdfizer_data/` |
| [`test/`](../test) | `unittest` suite; pipeline tests stub `java`, `docker` and friends so no external tool is required |
| [`scripts/release.py`](../scripts/release.py) | Version bump and release metadata automation |

## 9. Pinned toolchain

Set as build arguments in the [`Dockerfile`](../Dockerfile):

| Component | Version | Used for |
| --- | --- | --- |
| RMLStreamer | 2.5.0 | RML mapping execution on Flink |
| `hdtc` | 1.1.0 (Rust) | HDT create/merge/index, Java-free |
| Comunica | 5.3.0 | Default validation SPARQL engine |
| QLever | from `adfreiburg/qlever` (verified against build `bfd5741`) | Scale validation SPARQL engine |
| `pycottas` | image-pinned | COTTAS conversion and decode |

QLever is copied from a differently based upstream image, so its
release-specific Boost/ICU/jemalloc/io_uring libraries are copied alongside into
`/opt/qlever/lib` and only QLever's own processes are pointed there. A
build-time `ldd` check records whether they resolve; Comunica remains the
default, so an image whose QLever binaries do not link still validates.

---

## See also

- [Conversion](conversion.md) — VCF to RDF, stage by stage
- [Representations](representations.md) — compression and queryable artifacts
- [Output and metrics](output-and-metrics.md) — what a run writes and where
- [Limitations](limitations.md) — what this architecture cannot do
