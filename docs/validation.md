# Semantic VCF/RDF validation

*Part of the [VCF-RDFizer documentation](README.md). How coverage is measured:
[`validation-methodology.md`](validation-methodology.md). Element-by-element
results: [`vcf-coverage.md`](vcf-coverage.md).*

`vcf-rdfizer --mode validation` checks that a converted RDF graph reproduces
six deterministic VCF summaries. The same validator can be added to a full run
with `--validate`; in that case it runs once for each input after RDF creation
and compression. It computes one result from the source VCF
using `cyvcf2` (and `bcftools` for exact FILTER strings when available), runs
the equivalent SPARQL queries against the graph, then compares canonical
integer results exactly. Two axes are configurable and independent: which
artifact is validated (N-Triples, HDT, or COTTAS) and which SPARQL engine runs
the queries (Comunica or QLever). Read "What is *not* tested" below before
treating a `PASS` as a correctness proof.

The source artifact is mounted read-only. Anything that is not already plain
N-Triples is decoded under `/work` **inside the Docker container**; a plain
`.nt` input is read in place. The validator uses the resulting stream for
Raptor syntax validation and for the SPARQL queries, and removes any temporary
decode before the container exits. No decoded RDF is written beneath `--out`;
only reports are retained.

## Run it

Use the representation selected when the RDF was created.

```bash
# Expanded (the default VCF-RDFizer graph shape)
vcf-rdfizer --mode validation \
  --input ./vcf_data/HG004_GRCh38.vcf.gz \
  --rdf ./results/HG004_GRCh38/HG004_GRCh38.nt.gz \
  --sample-representation expanded \
  --out ./validation-results

# Condensed multi-sample graph
vcf-rdfizer --mode validation \
  --input ./vcf_data/1000G_phase3_chr20.vcf.gz \
  --rdf ./results/1000G_phase3_chr20/1000G_phase3_chr20.nt.gz \
  --sample-representation condensed \
  --out ./validation-results
```

To include validation in the conversion itself, use the full mode's opt-in
stage. It runs after the aggregate and selected compression artifacts are
available, once per VCF input:

```bash
vcf-rdfizer --mode full \
  --input ./cohort.vcf.gz \
  --sample-representation condensed \
  --rdf-storage-mode space-optimized \
  --rdf-compression none \
  --representations hdt \
  --validate \
  --out ./results
```

For standalone validation, the input VCF and the RDF artifact must originate
from the same conversion.

## Which artifact is validated

`--rdf` accepts any artifact the pipeline produces:

| Extension | How it is read |
|---|---|
| `.nt` | Read in place; nothing is copied |
| `.nt.gz` | Expanded into the container scratch directory |
| `.nt.br` | Expanded with `brotli -d` |
| `.hdt` | Decoded with `hdt2rdf` |
| `.cottas` | Decoded with `cottas_tool.py decompress` (pycottas) |
| `.cottas.gz`, `.cottas.br` | Unwrapped, then decoded as above |

Format is inferred from the filename; `--rdf-format` overrides that for an
artifact with an unusual name.

Validating an `.hdt` or `.cottas` decodes it back to N-Triples and then runs the
full semantic suite over the result. That is a strictly stronger statement than
the triple-count round-trip `validate_compression.py` performs during
compression: it proves the artifact decodes to a graph that still reproduces
every VCF summary, not merely to the right number of triples. The decode needs
scratch space of roughly the uncompressed graph size under the container's
`/work`, and nothing decoded is ever written beneath `--out`.

```bash
# Validate a compressed representation directly
vcf-rdfizer --mode validation \
  --input ./cohort.vcf.gz \
  --rdf ./results/cohort/cohort.hdt \
  --sample-representation condensed \
  --out ./validation-results
```

In full mode, `--validate-artifacts` chooses which produced artifacts to check.
Each one is validated independently and gets its own report directory, so a run
can prove the aggregate, the HDT, and the COTTAS file all agree with the VCF:

```bash
vcf-rdfizer --mode full -i ./cohort.vcf.gz \
  --rdf-storage-mode space-optimized \
  --representations hdt,cottas \
  --validate --validate-artifacts all \
  --out ./results
```

Accepted values are `aggregate` (the default), `hdt`, `cottas`, `all`, or a
comma-separated subset. A representation that was not selected, or whose
artifact is missing after a recoverable index warning, is skipped rather than
reported as a failure - the run already records why it is absent.

## Which SPARQL engine runs the queries

`--validation-engine` selects the backend. Both answer identical queries and
feed the same comparison layer, so the choice is a scale decision, never a
semantic one; every report records which engine produced it.

| Engine | Behaviour | Use it when |
|---|---|---|
| `comunica` (default) | Queries the N-Triples file with no setup, holding the graph in the Node heap | The graph fits comfortably in RAM |
| `qlever` | Builds an on-disk [QLever](https://github.com/ad-freiburg/qlever) index in container scratch, serves it on a container-local port, then tears both down | The graph no longer fits in memory, or the aggregate queries are too slow |

```bash
vcf-rdfizer --mode validation \
  --input ./cohort.vcf.gz --rdf ./results/cohort/cohort.nt.gz \
  --validation-engine qlever --qlever-memory-gb 32 \
  --out ./validation-results
```

QLever tuning, all optional:

| Option | Meaning |
|---|---|
| `--qlever-memory-gb N` | Index and server memory budget (default 4) |
| `--qlever-port N` | Container-local port (default 7019; never published) |
| `--qlever-startup-timeout N` | Seconds to wait for the server after indexing (default 900) |
| `--validation-query-timeout N` | Per-query timeout, both engines (default 3600) |
| `--qlever-index-arg ARG` | Extra argument for the index builder (repeatable) |
| `--qlever-server-arg ARG` | Extra argument for the server (repeatable) |

QLever's command-line interface has changed across releases. If a future
QLever image disagrees with the defaults, the whole command line can be
replaced without changing code, using `{index}`, `{input}`, `{memory}`, and
`{port}` placeholders:

```bash
docker run -e QLEVER_INDEX_COMMAND='qlever-index -i {index} -f {input} -F nt -m {memory}' ...
docker run -e QLEVER_SERVER_COMMAND='qlever-server -i {index} -p {port} -m {memory}' ...
```

The exact argv that ran is recorded in each report's `manifest.json` under
`engine.commands`. The QLever index lives only in container scratch and is
removed as soon as the queries finish.

QLever is copied into the image from the upstream `adfreiburg/qlever` image at
build time (`qlever-index` and `qlever-server`, verified against build
`bfd5741`); pin a version with
`--build-arg QLEVER_IMAGE=adfreiburg/qlever:<tag>`. That image is built on a
different Ubuntu release, so its release-specific Boost, ICU, jemalloc and
io_uring libraries are copied alongside the binaries into `/opt/qlever/lib`,
and only QLever's own processes are pointed there - they cannot shadow
anything the rest of the image links against. A build-time `ldd` check records
whether they resolve, and the validator reports that note if the engine cannot
start. Comunica remains the default, so an image whose QLever binaries do not
link still validates normally.

### Engine equivalence, and one place they differed

Both engines are held to producing identical results. That is verified, not
assumed: all eleven queries were run under both engines against the same
expanded and condensed graphs, and every normalized result matched.

One difference had to be fixed to make that true. QLever canonicalises numeric
literals at index time, so `"100"^^xsd:integer` is reported by `DATATYPE()` as
`xsd:int`. `preflight_position_datatype` originally required exactly
`xsd:integer`, which meant it flagged every record on QLever while passing on
Comunica - every run would have been `BLOCKED_BY_PREFLIGHT`. The query now
accepts the XSD integer family, which is what the check actually means. It
still reports a plain-string or `xsd:decimal` POS as an anomaly on both
engines.

If you add a query, prefer datatype-family checks and lexical comparisons over
assertions about a store's internal numeric representation.

`--validation-id NAME` changes the report directory name. The default is the
source VCF basename without `.vcf` or `.vcf.gz`. Existing result directories
are never overwritten. `--filter-oracle {auto,bcftools,cyvcf2}` controls the
FILTER-field oracle; `auto` uses `bcftools` when it is available in the image.

Validation progress uses the same JSONL sidecar protocol as conversion and
partitioned compression. It emits a `validation` task covering every
preflight, exact-count and core query, then records each query start and completion under the
run's temporary `.progress/` area while the host displays it through the
normal Rich/plain progress session. `--quiet` suppresses that terminal display
and the validator's per-query/summary stdout, but still writes command logs,
stage reports, and metrics. `--no-progress` disables sidecar creation as well.

## What is tested

The common record-level queries are used for both graph shapes:

| Query | Exact comparison |
|---|---|
| Q1 | record counts per source contig and zero-based 1 Mb window |
| Q2 | record-level ALT/REF shape classes |
| Q3 | biallelic A/C/G/T SNV transition and transversion counts |
| Q4 | FILTER broad status and exact lexical value |
| Q5 | per-sample genotype class counts |
| Q6 | genotype-derived single-ALT `(AN, AC, siteCount)` distribution |

| Q7 | file-level `##fileformat`, `##reference` and `##source` declarations |
| Q8 | how many `##` meta-information lines carry each header key |
| Q9 | every predicate in the graph and its triple count |
| Q10 | every asserted class and its resource count |
| Q11 | per-record identity digest, bucketed |
| Q12 | per-INFO-value identity digest, bucketed |
| Q13 | per-FORMAT-value identity digest, bucketed |

Q9 and Q10 are the completeness check: comparing the graph's inventory against
what the VCF implies catches a predicate that is missing, one with the wrong
cardinality, and one that should not be there at all. Q11-Q13 close the
permutation gap - see
[`validation-methodology.md`](validation-methodology.md#identity-digests-and-why-they-are-histograms).

Q9-Q13 assume the shipped RML mapping's predicate inventory and IRI templates.
A custom `--rules` changes both by design. `validation_runner.py` has a
`--mapping-policy report-only` setting for exactly that case, which records
those five queries without failing on them and lets the aggregate comparisons
carry the run.

> **Known gap.** The `vcf-rdfizer` wrapper does not currently forward
> `--mapping-policy`, so the runner always runs `strict` and a *correct* custom
> mapping is reported as `MISMATCH` on Q9-Q13. See
> [`rml-mappings.md`](rml-mappings.md#4-what-a-custom-mapping-costs-in-validation)
> and [`roadmap.md`](roadmap.md#3-validation-mapping-policy-is-not-forwarded).

Q5/Q6 are only required when the VCF has both samples and a `GT` FORMAT field.
The suite additionally checks N-Triples syntax, record cardinality, POS
datatype, representation profile, and representation-specific sample/GT
inventory before interpreting the result sets.

### Graph integrity

Three checks run before any comparison, because a graph that fails one is not
worth comparing:

| Check | What it catches |
|---|---|
| `preflight_blank_nodes` | Any blank node. Every class in the vocabulary declares an IRI template, so a blank node means a term map produced no IRI - and the record and value digests could not address such a node. |
| `preflight_empty_values` | Empty or whitespace-only literals and IRIs. An empty literal means a value was lost rather than marked missing; an empty IRI means a template substitution collapsed. |
| `preflight_duplicate_triples` | The same statement emitted more than once. |

Duplicates need explaining, because no SPARQL query can see them: a store holds
a **set**, so a repeated line is collapsed on load and every other check here
counts it once. The only way to detect it is to compare the statements the
parser read against the distinct triples the store holds. Raptor reports the
first, `preflight_distinct_triple_count` the second, and the difference is
exactly the number of redundant statements. This is worth having because
duplicated RDF parts are a known failure mode of the conversion -
`run_conversion.sh` already carries a defensive dedupe for it - and a duplicated
aggregate costs twice the storage for no added information.

All three are blocking. When an input a check needs is unavailable it reports
`NOT_EVALUATED` rather than `PASS`, so a check that could not run is never
mistaken for a clean result, and never fails the run either.

Each structural anomaly preflight runs twice: a `LIMIT 100` query returning
example rows for diagnosis, and a companion aggregate returning the **exact**
anomaly count. Reports carry both, plus `sampleTruncated`, so a graph with ten
million anomalies is never confused with one that has a hundred.

`--strict-conformance` promotes a missing-token conformance failure (a plain
`"."` literal not typed as `vcfr:Null`) from a report-only observation to a
validation failure.

### SHACL

`--shacl-shapes PATH` adds an independent structural layer: the graph is checked
against a SHACL shapes file (for example the vocabulary's published
`shacl/vcf-rdfizer-vocabulary.shacl.ttl`) with `pyshacl`, inside the container.
A violation blocks the run, because a structurally wrong graph makes the
aggregate comparisons uninterpretable.

It is **off by default**: `pyshacl` loads the whole graph into memory, so it is
suitable for a single-sample graph or a sample of a cohort, not for a
cohort-scale aggregate. The report records the conformance verdict, the exact
violation count, the distinct property paths involved, and the first 50
violations; the full text is written alongside it.

```bash
vcf-rdfizer --mode validation -i ./sample.vcf.gz --rdf ./results/sample/sample.nt.gz \
  --shacl-shapes ./vocabulary/shacl/vcf-rdfizer-vocabulary.shacl.ttl \
  -o ./validation-results
```

For the expanded representation, Q5/Q6 traverse `SampleCall` and
`FormatFieldValue` resources. For the condensed representation, their matching
queries traverse `SampleSet`, `CohortCallMatrix`, and `FormatValueVector`, then
extract each tab-delimited GT value by its `sampleIndex`. This makes the
condensed tests semantically equivalent without inflating the persisted RDF
back into per-sample value resources.

## What is *not* tested

The suite is deliberately a set of exact aggregate comparisons. That makes it
cheap, engine-independent, and free of a second RDF parser to disagree with -
but it bounds what it can detect, and those bounds are worth stating plainly.
`test/test_validation_logic_unit.py` encodes both the detections and the gaps
below, so a change that closes one will fail a test rather than pass silently.

**No per-record identity.** Every core query is a `GROUP BY` count. A graph that
swapped `POS` between two records on the same contig and in the same 1 Mb
window - or `REF`/`ALT` between two records of the same shape class - produces
byte-identical results and passes. The suite proves the *distributions* match,
not that record *i* carries record *i*'s values. Detecting a permutation needs a
per-record identity check, which the current query set does not have.

**Whole VCF columns are unchecked.** Nothing compares `ID`, `QUAL`, or `INFO`,
and no `FORMAT` field other than `GT` is validated. A conversion bug that
mangled every `DP` value, dropped every rsID, or corrupted `INFO` would pass.
The `header_lines` and `file_metadata` triples maps - the `HeaderLine`
resources and the `VCFFile` attributes - are likewise never queried.

**No completeness bound.** Nothing asserts a total triple count or the absence
of extraneous triples. `preflight_record_cardinality` catches duplicated or
missing per-record properties, but only for `VCFRecord` subjects.

**Header line values are compared only through their structured form.** The
attributes that matter - `filterId`, `altId`, `contigId`, contig length/md5/
assembly, and the INFO/FORMAT declarations - are lifted into their own
properties and counted. The raw `vcfr:headerValue` literal itself is counted but
never read, and `vcfr:contigCount` is likewise checked for presence rather than
value.

**The census assumes the shipped mapping.** Under a custom `--rules`, Q9-Q13
are no longer meaningful and the run should rest on the aggregate comparisons
alone - which is what `--mapping-policy report-only` is for, and which the
wrapper does not yet enable (see the note above).

In short: a `PASS` is strong evidence that the conversion preserved the VCF's
*summary statistics* over the elements it covers, and it reliably catches
dropped records, misclassified variants, flipped genotypes, corrupted FILTER
strings, allele-count errors, missing header lines, and altered file metadata.
It is not proof of a faithful record-by-record round-trip. Treat it as a
regression gate, not a correctness proof.

These limits are measured rather than asserted. Every claim above corresponds
to a named mutation in the catalogue described in
[`validation-methodology.md`](validation-methodology.md); the current score and
the full element-by-element breakdown are in
[`vcf-coverage.md`](vcf-coverage.md).

## Results and cleanup evidence

Results are written to the run's canonical metrics tree:

```text
<out>/run_metrics/<input-label>__<run-id>/reports/validation/<validation-id>/
```

Detailed results live beneath `reports/validation/`, so they are indexed by
the same `summary.json` used for conversion and compression metrics.

Important files include `summary.json`, `manifest.json`, `parser.json`,
`rdf-validation.json`, `materialization.json`, `preflight.json`, `sparql.json`,
and `comparison.json`. Raw SPARQL Results JSON, stderr, and query resource logs
are in `raw/`; normalized results are in `normalized/`. `manifest.json` records
the engine that ran (including QLever's exact argv), the source artifact format
and checksum, and every decode step; `materialization.json` records how a
compressed or indexed artifact was turned into N-Triples and how many triples
that yielded.

When several artifacts are validated in one full run, each gets its own
directory: the aggregate keeps `<validation-id>/`, and the representations use
`<validation-id>__hdt/` and `<validation-id>__cottas/`.

The parent VCF-RDFizer run metrics include
`run_metrics/<input-label>__<run-id>/stages/validation/<validation-id>.json`.
That stage report and the detailed `reports/validation/.../summary.json` record
whether a gzip aggregate was decompressed inside the container and that no
validation scratch RDF was retained on the host.

`PASS` means every required query and invariant matched. `MISMATCH` means that
both paths ran but differ. `BLOCKED_BY_PREFLIGHT` means RDF syntax or core graph
structure failed. `EXECUTION_FAILED` means a parser or query engine could not
complete.


---

## See also

- [Validation methodology](validation-methodology.md) — how this suite's coverage is measured
- [VCF coverage matrix](vcf-coverage.md) — element by element, with the mutation that proves each row
- [Representations](representations.md) — the round-trip check that runs during compression
- [Output and metrics](output-and-metrics.md) — where reports land in the run tree
- [Limitations](limitations.md) — consolidated, across the whole tool
