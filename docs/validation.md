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
the queries (Comunica, QLever, native HDT, native COTTAS - or several at once,
which cross-checks them and benchmarks them against each other and against the
parser). Read "What is *not* tested" below before treating a `PASS` as a
correctness proof.

The source artifact is mounted read-only. Anything that is not already plain
N-Triples is decoded under `/work` **inside the Docker container**; a plain
`.nt` input is read in place. The validator uses the resulting stream for
Raptor syntax validation and for the SPARQL queries, and removes any temporary
decode before the container exits. No decoded RDF is written beneath `--out`;
only reports are retained.

## Choosing an engine, and what happens when it cannot keep up

**Every engine is now set up once per target**, and all 27 queries reuse that
setup. Setup is timed separately as `setupSeconds` so a benchmark can attribute
it correctly instead of smearing it across 27 query timings.

| Engine | What is paid once |
| --- | --- |
| `comunica` | one `comunica-sparql-file-http` process: Node startup and engine init |
| `hdt` | one `comunica-sparql-hdt-http` process, holding the memory-mapped `.hdt` |
| `qlever` | an on-disk index build, then its own HTTP server |
| `cottas` | one `COTTASStore`-backed rdflib graph |

Earlier releases spawned the one-shot `comunica-sparql-file` and
`comunica-sparql-hdt` CLIs **once per query**, paying startup 27 times.
Measured in this image on 1.5M N-Triples, per `SELECT (COUNT(*))`:

| | per query |
| --- | --- |
| one-shot `comunica-sparql-file` | 26.0s |
| `comunica-sparql-file-http` endpoint | 10.3s |

**Comunica still has no index, and the endpoint does not change that.** It
streams the source for every query: `LIMIT 1` answers in 0.59s while `COUNT(*)`
takes 10s on every repeat. So a full-scan query still re-reads the whole graph
each time — the endpoint removes per-query *startup*, not per-query *scanning*.

Materializing the graph into an in-memory store instead was measured and
rejected: an `N3.Store` of the same 1.5M triples cost 2.45 GiB and made `COUNT`
*slower* (15.0s), which extrapolates to roughly 145 GiB for an 88M-triple
cohort graph. Above fixture scale the answer is an engine that indexes, not a
bigger heap.

| Graph size | Recommended |
| --- | --- |
| Fixtures, small VCFs | `--validation-engine comunica` (default; no index build) |
| Anything above a few GiB of N-Triples | `--validation-engine qlever`, or validate the indexed artifact with `--validate-artifacts hdt` |

Above 4 GiB the runner warns before it starts and names the alternatives.

### If validation seems to hang

First check `setupSeconds` in the engine report. Slow *setup* is a scale
problem (an index build, or a source the endpoint struggles to read); a slow
*query* is a `--validation-query-timeout` problem (default 3600s).

**Every query runs, always.** A query that fails, times out, or returns the
wrong answer is recorded and the suite moves on — a validation run is a
coverage report, and "26 of 27 passed, q11 disagreed" is a result while
"stopped at q01" is not. `--validation-stop-after-query-timeout` opts into
abandoning an engine's remaining queries after the first timeout, which caps
the worst case at one timeout per engine rather than one per query:

```json
{"status": "EXECUTION_FAILED",
 "abandoned": "q01_record_density_1mb exceeded the 3600s per-query timeout; skipped 26 further queries …",
 "queriesRun": 1, "queriesPlanned": 27}
```

Options for a graph that is genuinely too large:

```bash
# Build an index once and query from it.
vcf-rdfizer --mode full -i cohort.vcf.gz --validate --validation-engine qlever -o ./out

# Or validate the HDT artifact, which is already indexed.
vcf-rdfizer --mode full -i cohort.vcf.gz --validate --validate-artifacts hdt -o ./out

# Or allow longer per query, with a ceiling on the whole engine.
vcf-rdfizer --mode full -i cohort.vcf.gz --validate \
  --validation-query-timeout 14400 --validation-time-budget 86400 -o ./out
```

### A decode that reports success and writes garbage

Every decode into N-Triples is now checked structurally before anything reads
it: non-empty, newline-terminated, with a parseable first statement. A step
that exits 0 but writes errors to stderr is treated as failed.

This exists because hdt-cpp's `hdt2rdf` **file** serializer is broken: it emits
`error: :0:0: write error` once per triple, writes truncated subject IRIs with
no terminators and no newlines at all, and still exits 0. Its stdout serializer
is correct, so the decode is taken from stdout and redirected. Left unchecked,
the malformed dump reached `rapper`, which saw one multi-gigabyte "line",
degraded to quadratic parsing, and spent 38 hours reaching 23% of a 4.4 GiB
file — a hang, in every way that matters, produced by a tool that reported
success.

Queries run in their own process session, so a timeout kills the engine along
with its `/usr/bin/time` wrapper. Before that fix the engine survived the
timeout still holding the whole graph in memory, and enough of those pushed the
host into swap — which is what made a slow step look like a stuck one.

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

`--validation-engine` selects the backend. Every engine answers the same
queries and feeds the same comparison layer, so the choice is a scale and
performance decision, never a semantic one; every report records which engine
produced it.

| Engine | Queries | Setup | Use it when |
|---|---|---|---|
| `comunica` (default) | The N-Triples file directly | none | The graph fits comfortably in RAM |
| `qlever` | An on-disk [QLever](https://github.com/ad-freiburg/qlever) index, served on a container-local port | index build | The graph no longer fits in memory, or the aggregate queries are too slow |
| `hdt` | A `.hdt` artifact **in place**, through Comunica's HDT engine | reuses the run's HDT, or builds one | Checking that the compressed artifact is queryable, not just decodable |
| `cottas` | A `.cottas` artifact **in place**, through `pycottas`'s rdflib store | reuses the run's COTTAS, or builds one | Same, for COTTAS |

### Validating a compressed artifact without decoding it

`--validation-target hdt,cottas` validates those artifacts by decoding them
back to N-Triples first. That proves the decode is faithful; it says nothing
about querying them, and it measures the wrong thing entirely in a performance
comparison.

`--validation-engine hdt` and `--validation-engine cottas` instead query the
artifact where it lies. When the run's own artifact is already in the engine's
format it is used directly - the honest measurement. Otherwise the engine
builds one in container scratch from the materialized N-Triples, and that build
is timed as **setup**, kept out of the query total. Each report records which
of the two happened, in `artifactOrigin`.

```bash
vcf-rdfizer --mode validation \
  --input ./cohort.vcf.gz --rdf ./results/cohort/cohort.hdt \
  --validation-engine hdt \
  --out ./validation-results
```

HDT is queried with `comunica-sparql-hdt`, installed in the image alongside
Comunica; the source is addressed as `hdt@<path>`, because a bare path would be
treated as a link to dereference. COTTAS is queried in-process through
`pycottas.COTTASStore`, an rdflib `Store` over the Parquet artifact, under the
same interpreter that already owns pycottas.

### Several engines in one run

`--validation-engine` accepts a comma-separated list, or `all`:

```bash
vcf-rdfizer --mode validation \
  --input ./cohort.vcf.gz --rdf ./results/cohort/cohort.nt.gz \
  --validation-engine all \
  --out ./validation-results
```

Every requested engine answers the whole query set. Two things follow:

- **Cross-checking.** The normalized results of every engine are compared
  against each other and written to `engine-agreement.json`. Engines
  disagreeing is a finding in its own right - see below.
- **Benchmarking.** Every engine is timed identically, against the same graph,
  in the same container, in one run.

The first engine named is the **primary**. Its reports keep the single-engine
layout at the top of the results directory, so existing consumers are
unaffected; each engine additionally gets `engines/<name>/` with its own
`preflight.json`, `sparql.json`, `comparison.json` and `query-executions.json`.
The run's `status` is `PASS` only if every engine passed, and `summary.json`
carries `engineStatuses` with the per-engine verdict.

### Timings, and comparing SPARQL against the parser

Every run writes `benchmark.json` and a long-format `benchmark.csv` - one row
per engine and query, ready to plot without reshaping:

```
engine,query_id,status,wall_seconds,oracle_wall_seconds,engine_setup_seconds,artifact_origin
```

`benchmark.json` adds the breakdown: per-engine setup and total query time, the
slowest query per engine, N-Triples materialization and SHACL time, and the
**oracle** - what it costs to compute the same answers directly from the VCF
with cyvcf2, split into parse and census. That last figure is the point of
comparison: the validation suite computes every expected value twice, once by
parsing and once by querying, so a run measures a SPARQL engine against a
purpose-built parser on identical work.

An engine that produced no timed query reports `null` rather than `0`, because
zero seconds reads as "instant" rather than "never ran".

The parent run's `metrics.csv` carries the summary columns
`validation_engines`, `validation_oracle_seconds`,
`validation_engine_query_seconds` and `validation_engine_setup_seconds`, the
last two as `engine=seconds` pairs.

An indicative single-container run over a small fixture (312 triples, expanded)
gives the shape of the difference rather than a benchmark result:

| Engine | Setup | 27 queries | Notes |
|---|---|---|---|
| oracle (cyvcf2) | - | 0.003 s | the parser computing the same answers |
| qlever | 0.17 s | 0.31 s | index built once, then served |
| cottas | 0.28 s | 0.97 s | in-process, no subprocess per query |
| hdt | 0.003 s | 23.9 s | one Comunica process per query |
| comunica | - | 25.1 s | one Comunica process per query |

Comunica's cost here is almost entirely per-process startup, which a
fixture-sized graph cannot amortize; the ordering says nothing about how these
engines behave on a cohort-sized graph.

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
| `--validation-query-timeout N` | Per-query timeout, every engine (default 3600) |
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
build time (`qlever-index` and `qlever-server`). The image is pinned by its
multi-architecture index digest `sha256:f8aa7704` rather than by tag, because
`:latest` is a rolling build of upstream's main branch and would change which
engine a released image contains. The pinned digest is build `bfd5741` - the
one QLever's documented behaviour here was verified against.
Override with `--build-arg QLEVER_IMAGE=adfreiburg/qlever@sha256:<index digest>`.
That image is built on a
different Ubuntu release, so its release-specific Boost, ICU, jemalloc and
io_uring libraries are copied alongside the binaries into `/opt/qlever/lib`,
and only QLever's own processes are pointed there - they cannot shadow
anything the rest of the image links against. A build-time `ldd` check records
whether they resolve, and the validator reports that note if the engine cannot
start. Comunica remains the default, so an image whose QLever binaries do not
link still validates normally.

### Engine equivalence, and one place they differed

Every engine is held to producing identical results. That is verified, not
assumed: [`test/cross_engine_agreement.py`](../test/cross_engine_agreement.py)
runs the full query set under all four engines, against the same expanded and
condensed graphs, and additionally runs the shipped validation decision under
each - so an engine must agree both with the other engines and with the Python
oracle. Engines agreeing with each other and all being wrong is a real failure
mode; only the oracle rules it out.

A multi-engine production run performs the first half of that check on the
real graph and records it in `engine-agreement.json`.

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
`"."` literal not typed as `vcfc:Null`) from a report-only observation to a
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

The suite is three independent layers - exact aggregate comparisons, a
predicate/class census with per-record and per-value identity digests, and
graph-integrity checks - plus optional SHACL. That is a broad net, but it is
still a net, and its holes are worth stating plainly. Every claim below is
backed by a named mutation in
[`test/validation_mutations.py`](../test/validation_mutations.py), so a change
that closes a gap fails a test rather than passing silently.

**`vcfc:contigCount` is counted, not read.** The census asserts the predicate is
present the expected number of times; nothing compares its *value*. A derived
contig total that is simply wrong passes. This is the one mutation in the
catalogue that is still undetected.

**Header line values are compared only through their structured form.** The
attributes that matter - `filterId`, `altId`, `contigId`, contig length/md5/
assembly, and the INFO/FORMAT declarations - are lifted into their own
properties and compared. The raw `vcfc:headerValue` literal itself is counted
but never read.

**Multi-valued INFO fields keep only their lexical value.** A `Number=1` INFO
value additionally carries a typed `fieldValueInteger`/`fieldValueDecimal`; a
`Number=A/R/G/.` field does not, because the vocabulary's IRI template gives one
node per key. The full lexical value is still digested, so corruption is caught
- but no per-element typing is checked.

**The census assumes the shipped mapping.** A custom `--rules` changes the
predicate inventory and the IRI templates by design, so `q09`-`q13` fall back to
report-only and the run rests on the aggregate comparisons alone. That is
correct behaviour, not a bug, but it means a custom mapping is validated more
weakly than the default one.

**SHACL conformance is checked by hand, not by this suite.** Both
representation profiles are fully ontology-backed as of published VCF Core
2.0.0, and the converter's output validates against the complete profile set —
portable, SPARQL, consistency and all five version overlays — with zero
violations. That check is currently a manual step rather than a test; wiring it
in is tracked in
[`validation-migration-notes.md`](validation-migration-notes.md).

**A high mutation score is a lower bound on blindness, not a proof.** It says
"almost every corruption we thought to write down is caught". Corruptions nobody
wrote down are, by construction, not measured.

In short: a `PASS` means the graph contains exactly the predicates and classes
the VCF implies, that every record, INFO value and FORMAT value hashes to the
same bucket as its counterpart in the source, that the summary statistics agree
exactly, and that the graph carries no blank nodes, empty terms or duplicated
statements. It reliably catches dropped records, misclassified variants, flipped
genotypes, corrupted FILTER strings, allele-count errors, permuted record
fields, missing header lines, and altered file metadata. It is still not proof
of a faithful record-by-record round-trip. Treat it as a regression gate.

These limits are measured rather than asserted. The current mutation score and
the full element-by-element breakdown are in
[`vcf-coverage.md`](vcf-coverage.md); the method is described in
[`validation-methodology.md`](validation-methodology.md).

## Results and cleanup evidence

Results are written to the run's canonical metrics tree:

```text
<out>/run_metrics/<input-label>__<run-id>/reports/validation/<validation-id>/
```

### The tree

```text
<out>/run_metrics/<input-label>__<run-id>/
├── summary.json                      ← start here: a `validation` array indexes every target
├── stages/validation/<id>.json       ← one compact stage report per target
└── reports/validation/<id>/          ← the detail for that target
    ├── summary.json                    verdict, engines, pointers
    ├── manifest.json                   provenance
    ├── parser.json                     what the oracle read from the VCF
    ├── sparql.json                     what the queries got from the graph
    ├── comparison.json                 the two, compared per query
    ├── preflight.json                  graph-integrity gates
    ├── rdf-validation.json             syntax check and parsed triple count
    ├── materialization.json            how a compressed artifact was decoded
    ├── benchmark.json / benchmark.csv  timings
    ├── shacl.json / shacl-report.txt   only with --shacl-shapes
    ├── engine-agreement.json           only with several engines
    ├── engines/<name>/                 only with several engines
    ├── raw/<engine>/                   raw engine output
    └── normalized/                     per-query rows the comparison used
```

### What each file answers

| File | Open it when you want to know |
| --- | --- |
| `summary.json` | The verdict, which engines ran and what each returned, and where the rest is. **Start here.** |
| `comparison.json` | *Why* a run is `MISMATCH`: per query, the rows that were missing, extra, or differing |
| `parser.json` | What the oracle independently computed **from the VCF** — the side of the comparison that does not involve RDF |
| `sparql.json` | What the queries returned **from the graph**, normalized — the other side |
| `preflight.json` | Which integrity gate failed on a `BLOCKED_BY_PREFLIGHT`: blank nodes, empty terms, duplicate statements, record cardinality, missing-token conformance, representation profile |
| `rdf-validation.json` | Whether the N-Triples parsed at all (`rapper -c`), and how many statements it holds |
| `materialization.json` | How an `.hdt`/`.cottas`/`.gz` artifact was turned into N-Triples, and how many triples that yielded — the first place to look when a compressed artifact validates differently from the aggregate |
| `manifest.json` | Provenance for a result you want to reproduce or cite: the engine and its exact argv (including QLever's), the source artifact's format and checksum, every decode step |
| `benchmark.json`, `benchmark.csv` | Timings. The CSV is long-format — one row per engine per query — and is the file to load for analysis |
| `shacl.json`, `shacl-report.txt` | Shape conformance, when `--shacl-shapes` was given. Independent of the VCF: it checks the graph against the vocabulary, not against the source |
| `engine-agreement.json` | Whether every engine returned identical normalized results, and which queries differed if not |
| `engines/<name>/` | The same `preflight`/`sparql`/`comparison` for one engine, plus `query-executions.json` with each query's exit code, wall time and error. The top-level reports are the **primary** (first-named) engine's |
| `raw/<engine>/` | Raw SPARQL Results JSON, stderr, and `/usr/bin/time` resource logs, per query — for debugging an engine rather than the data |
| `normalized/` | The per-query rows the comparison actually used, after normalization |

### Reading order when something fails

Take the `status` from `summary.json` and go straight to the file that explains it:

| Status | Meaning | Go to |
| --- | --- | --- |
| `PASS` | Every required query and invariant matched | — |
| `MISMATCH` | Both sides ran and disagree | `comparison.json` |
| `BLOCKED_BY_PREFLIGHT` | RDF syntax or core graph structure failed, so the comparison never ran | `preflight.json`, then `rdf-validation.json` |
| `EXECUTION_FAILED` | A parser or an engine could not complete | `engines/<name>/query-executions.json` |

An engine that exceeded `--validation-query-timeout` reports `EXECUTION_FAILED`
with an `abandoned` field naming the query that tripped it and how many were
skipped. See [If validation seems to hang](#if-validation-seems-to-hang).

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
