# Data linking: implementations and examples

VCF-RDFizer can now connect its graph to external resources through three tiers
of linker plug-in. The output is a separate N-Triples linkset: enabling linking
does not change the aggregate, its compression inputs, or the core validation
queries. This is an initial implementation of
[`datalinking-design.md`](datalinking-design.md), with the remaining work listed
in §8 below.

The examples and the runner are installed with the Python package. Linking runs
on the host; only conversion still needs Docker. RDFLib is now a runtime
dependency for parsing actual Turtle manifests and unordered N-Triples, rather
than approximating either syntax with regular expressions.

---

## 1. Five runnable examples

| Tier | Installed ID | Join and result | Network |
| --- | --- | --- | --- |
| 1: declarative | `rsid-dbsnp` | `ID` rsID tokens → dbSNP identifier IRIs | None |
| 2: reference bundle | `gene-demo` | Explicit allele REF spans → synthetic GFF3 gene intervals | None with the shipped local bundle |
| 2: reference bundle | `ensembl-genes-grch38` | Explicit allele REF spans → overlapping Ensembl 116 genes (a real annotation, fetched once by digest) | One download, then offline |
| 2: reference bundle | `spdi` | Normalised alleles → NCBI SPDI IRIs, the same in every file whatever it calls the chromosome | None |
| 3: live resolver | `rsid-ensembl` | Deduplicated rsID batches → service-confirmed Ensembl variation links | Ensembl HTTPS API, or cached responses |
| 3: live resolver | `rsid-myvariant` | rsID batches of up to 1,000 → dbSNP IRIs, linked only when MyVariant.info's dbSNP record carries the rsID | MyVariant.info HTTPS API, or cached responses |

The canonical example directories are under
[`vcf_rdfizer_data/linkers/`](../vcf_rdfizer_data/linkers/). Each has its own
README and `linker.ttl`; only Tier 3 has `resolver.py`.

`gene-demo` is deliberately **synthetic**. Its three genes use `example.org`
identifiers and invented intervals. Its `GRCh38` declaration exercises the
assembly guard; it does not turn the fixture into a biological annotation.
Use a real, digest-pinned reference before interpreting overlaps biologically.

From a checkout:

```bash
python -m pip install -e .
vcf-rdfizer-link list
vcf-rdfizer-link keys

# Two local tiers, no Docker or network needed.
vcf-rdfizer-link run -i examples/linking/example.vcf \
  --link rsid-dbsnp,gene-demo --offline \
  --links-cache ./example-cache -o ./example.links.nt
```

This writes **seven links**: three dbSNP links and four gene overlaps, plus
provenance triples. Record 2 overlaps both demo genes A and B. Record 3 has no
rsID and falls beyond the gene interval; it produces no link. The JSON report
is `example.links.json`. Existing graph or report paths are refused.

The companion `run -i` command derives subjects with the converter's default
IRI templates; it does not create or verify a base graph. To link an actual
conversion, use `--rdf` or one of the two entry points below.

## 2. Conversion and post-hoc linking

Full mode inserts the stage after the direct RDF emitters and before
compression:

```bash
vcf-rdfizer --mode full -i examples/linking/example.vcf \
  --rdf-storage-mode plain --representations none --rdf-compression none \
  --link rsid-dbsnp,gene-demo --offline -o ./results
```

The aggregate is `results/example/example.nt`; the side-graph is
`results/example/example.links.nt`. Space-optimized `.nt.gz` aggregates work as
well. If linking fails, that input fails at `data-linking` and compression is
not started for it. The completed base aggregate remains available.

To enrich an existing aggregate without conversion or Docker:

```bash
vcf-rdfizer --mode link --rdf ./results/example/example.nt \
  --link rsid-dbsnp,gene-demo --offline -o ./relinked
```

The result is `relinked/example.links.nt`, with a normal `run_metrics/` tree.
`vcf-rdfizer-link run --rdf <path> --link <ids> -o <file.links.nt>` uses the
same runner but writes a single companion JSON report instead.

Both RDF entry points read the graph's `hasRecord` and `hasCall` edges. They
retain the actual subjects, including custom IRI templates, and reject a call
whose subject is absent. They require the VCF-RDFizer predicates for those
edges and the selected join fields. Arbitrary custom vocabularies cannot be
inferred. N-Triples may be unordered.

The graph is read with SPARQL (`vcf_rdfizer_linking/inputs.py`): one query for
the records, and a set of declared queries for the assumptions the reader rests
on (one call per record, one value per join field, and so on), each returning a
row when the graph breaks it. A file is loaded into a temporary on-disk
Oxigraph store first, keeping only the join fields (genotype triples are parsed
and dropped). A graph already served by an endpoint needs no copy:
`vcf-rdfizer-link run --endpoint <url> --link <ids> -o <file.links.nt>`.

## 3. Tier 1: a manifest is the implementation

[`rsid-dbsnp/linker.ttl`](../vcf_rdfizer_data/linkers/rsid-dbsnp/linker.ttl)
implements the proposal's token example unchanged in substance:

- `vcfl:TokenJoin` reads `ID`, or a named INFO value using `INFO/<key>`.
- `vcfl:splitOn` splits literal delimiters, defaulting to `;` for ID and `,`
  for INFO. Flags, missing keys, empty tokens, and `.` do not create keys.
- `vcfl:accept` is a case-sensitive Python regular expression matched against
  the **whole token**. Duplicate tokens do not duplicate a triple.
- `vcfl:objectTemplate` contains exactly one `{TOKEN}` placeholder. The runner
  percent-encodes the value before substitution, then validates the IRI.
- `vcfl:subject` selects `vcfl:VariantCall` or `vcfl:VCFRecord`; the manifest
  declares the predicate. The runner owns subject identity and serialization.

This example asserts a weak identifier connection with `vcfl:sameVariantAs`.
It does not check whether dbSNP still recognises the rsID, resolve retired IDs,
or assert `owl:sameAs`.

## 4. Tier 2: a reference, a digest, and an assembly

[`gene-demo/linker.ttl`](../vcf_rdfizer_data/linkers/gene-demo/linker.ttl)
declares `vcfl:IntervalJoin`, a GFF3 reference, its exact SHA-256, and its
assembly. The interval index is the implementation; no plug-in Python is needed.

The join uses **1-based closed intervals**:
`[POS, POS + len(REF) - 1]`. A position at either boundary overlaps. Nested
and overlapping features are retained. `vcfl:featureType` defaults to `gene`;
`vcfl:idAttribute` defaults to `ID`. GFF3 percent escapes are decoded and the
selected identifier feeds the `{ID}` object template.

Chromosome names must match exactly (`1` and `chr1` are different) unless the
manifest declares `vcfl:contigAliases`: a digest-pinned sequence map (§4, allele
identity) through which both the GFF3 seqids and the records' contigs resolve to
an accession. `ensembl-genes-grch38` uses one, so Ensembl's `17` meets `chr17`.
There is no liftover. A record keys its REF span whatever its ALT: a
spanning-deletion `*` (skipped up to v3.3.0), a symbolic allele or a breakend
(skipped up to v3.3.1) is linked by the REF it writes. Only a record whose REF
is not DNA bases is skipped and counted in `skipped_records`.
The runner does not interpret INFO/END, breakend mates or confidence ranges.
It uses explicit DNA REF spans, including their anchor base, rather than an
inferred biological affected region, so a structural variant links to the genes
its anchor lies in, not to every gene it affects.

References may be HTTPS URLs, local `file:` URLs, or relative IRI references
such as `<genes.gff3>` resolved beside `linker.ttl`. An absolute URL string is
also accepted, as in the original proposal. Fetched/local bundles are cached
under `references/<sha256>`; the digest is checked on every cache use. For
gzip references the digest covers the **compressed bytes**. The GFF3 index is
rebuilt in memory each invocation; the bundle bytes are cached, not the index.

Before resolution, the runner compares the input reference against the bundle.
It recognises `GRCh37`, `GRCh38`, and the `hg19`/`hg38` aliases within reference
paths, or an exact declared assembly string. Missing/unrecognised metadata
requires `--assembly` after the user checks the input. That option cannot
override a recognised mismatch. No reference is fetched until the assembly
checks for all selected linkers have passed.

To adapt the example:

```bash
vcf-rdfizer-link init --example gene-demo -o my-gene-linker
# Edit id, reference URL, digest, assembly, attribute, and object template.
vcf-rdfizer-link check my-gene-linker --links-cache ./reference-cache
vcf-rdfizer-link dry-run my-gene-linker -i your-small.vcf --limit 100
```

`check` parses the manifest, verifies/acquires the reference and parses its
features. Without an input VCF it cannot certify input assembly compatibility.

### Allele identity: `vcfl:AlleleJoin` and the `spdi` linker

A file's record IRIs (`file://x.vcf#record/12`) are scoped to that file, so two
files holding the same variant share no term. An **allele join** gives them
one. For each ALT with explicit bases, the runner computes an SPDI expression
(`sequence:position:deleted:inserted`, 0-based), and emits it through the
`{SPDI}` object template:

```bash
vcf-rdfizer --mode link --rdf genome.nt.gz --link spdi --offline -o linked/
vcf-rdfizer --mode link --rdf clinvar.nt.gz --link spdi --offline -o linked/
# genome and ClinVar calls for the same variant now share one sameVariantAs object
```

The reference is a **sequence map** (`vcfl:format vcfl:SequenceMap`), a TSV of
`accession  length  names`, where `names` lists every contig name that denotes
the sequence. That is the contig aliasing the interval join does not do: `chr17`
and `17` both resolve to `NC_000017.11`, because the map says so, not because
of a naming rule. A contig the map does not list is not linked. A position
beyond its sequence's length fails the run. The shipped
[`spdi`](../vcf_rdfizer_data/linkers/spdi/README.md) map covers the 25 GRCh38
primary-assembly sequences.

The expression is SPDI's *trimmed* form: the shared suffix is removed first,
then the shared prefix, so a left-aligned indel stays left-aligned. NCBI's
*canonical* form shifts an indel across its repeat, and that needs the reference
sequence, which the runner does not read. Identifiers therefore agree across
files that were **normalised the same way** (`bcftools norm -f <ref> -m -any`).
The linkset records this as its assertion basis (`allele-expression`), and the
report does not count these links as verified. On real ClinVar variants,
identifiers outside repeats equal NCBI's SPDI exactly. Inside a repeat they
differ from NCBI's contextual form but denote the same allele. That is checked
against recorded NCBI answers in
[vcf-rdfizer-testing `plugin-tests/spdi/`](https://github.com/ecrum19/vcf-rdfizer-testing/tree/main/plugin-tests). Symbolic alleles, breakends,
`*` and `.` are skipped and counted: they have no allele sequence to express.

## 5. Tier 3: a narrow Python resolver

The shipped resolver calls Ensembl's
[batch variation endpoint](https://rest.ensembl.org/documentation/info/variation_post).
It submits one POST per batch (up to 100 rsIDs in this example) and emits links
only for returned variation objects with a name. Missing IDs produce no link;
malformed response objects abort the linker. No clinical assertions or
frequencies are inferred from the response.

```python
from vcf_rdfizer_linking import Link, LinkKey, LinkerContext

def resolve(batch, ctx):
    response = ctx.session.post(
        "https://rest.ensembl.org/variation/homo_sapiens",
        json={"ids": [key.token for key in batch]},
    )
    response.raise_for_status()
    # Inspect response.json(), then yield Link(key, absolute_object_iri).
```

`LinkKey` is immutable (`token`, `chrom`, `start`, `end`). `Link` contains only
`key` and `object`. A resolver cannot supply a subject or change the manifest's
predicate through this protocol, and returning an undispatched key is an error.
The session offers `get(url, params=...)` and `post(url, json=...)`; responses
have `status_code`, `headers`, `content`, `json()`, and `raise_for_status()`.

Preview first, then supply your actual contact address for a live run:

```bash
vcf-rdfizer-link init --example rsid-ensembl -o ensembl-example
vcf-rdfizer-link dry-run ensembl-example -i examples/linking/example.vcf

vcf-rdfizer-link run -i examples/linking/example.vcf --link rsid-ensembl \
  --links-contact-email you@your-institution.org \
  --links-cache ./ensembl-cache -o ./ensembl.links.nt

# Reuse identical request batches without contacting the service.
vcf-rdfizer-link run -i examples/linking/example.vcf --link rsid-ensembl \
  --offline --links-cache ./ensembl-cache -o ./ensembl-replay.links.nt
```

The built-in manifest's placeholder contact blocks a cache-miss live request;
an offline replay can still use its cache. Set `vcfl:contactEmail` in a copied
manifest or use `--links-contact-email`. Selecting a live linker sends its join
keys to the declared service; no sample/genotype payload is passed to this
example's resolver.

The session implements:

- A declared HTTPS endpoint origin and path. Redirects are refused, including
  reference-download redirects, so hidden requests cannot change hosts.
- A mandatory disk cache under `responses/<id>/<version>/<request-hash>.json`.
  The hash includes method, URL and canonical JSON body; stored bodies have
  digests. Only successful responses are cached. Corrupt entries fail closed.
- Full key deduplication **per input aggregate**, before any request. Sorted
  keys make batching deterministic. SQLite stores key-to-subject associations
  and resolved triples so those do not need to fit in memory.
- A per-host token bucket with capacity one, shared by selected linkers, using
  the strictest declared rate. At most one request is in flight per run.
  Per-linker request ceilings are shared across inputs in a directory run.
- Up to four HTTP attempts for 429/5xx, with exponential delay and numeric or
  HTTP-date `Retry-After` honoured. Retries count toward the run ceiling. This
  follows Ensembl's documented
  [rate-limit protocol](https://github.com/Ensembl/ensembl-rest/wiki/Rate-Limits).
- A request timeout (30 s unless the manifest's `vcfl:requestTimeout` says
  otherwise, 1–600 s: a slow service needs a longer one, not retries) and a
  16 MiB response limit. Other HTTP failures,
  connection failures, and exhausted budgets abort the linker.
- `--offline` / `--links-cache-only`: no network, with clear cache-miss errors.
  Verified local GFF3 references remain usable on a cold cache.

The limiter is shared within one invocation, not across separate processes.
Directory inputs are deduplicated individually; identical request batches in
later inputs use the response cache. Different batch membership produces a
different cache key even if some identifiers overlap. Cache entries have no
automatic expiry: use a fresh cache directory or a new plug-in version to
refresh service answers deliberately.

**Python plug-ins are trusted code.** The session constrains traffic made
through its methods. Importing a third-party resolver is not a security sandbox
and cannot prevent that code from opening its own socket. The shipped resolver
uses only `ctx.session`. `list`, `check`, and `dry-run` do not import resolver
modules; Python entry-point discovery does execute the installed provider.

Tier 3 remains suitable for filtered variant sets. Genome-wide linking should
use a suitable local reference bundle. The network budget is a hard ceiling,
not a promise that a large run will finish.

## 6. Discovery, authoring, and previews

`vcf-rdfizer-link init -o my-linker` copies the annotated Tier 1 example;
`--example` selects any of the three tiers. It refuses an existing destination.
Give copies a new `vcfl:id` before adding them to a search path. Discovery reads
packaged examples, repeatable `--linker-path` directories, the platform-separated
`VCF_RDFIZER_LINKER_PATH`, and `vcf_rdfizer.linkers` Python entry points.

A path may be a linker directory or a parent containing linker directories.
Duplicate IDs at different locations are rejected rather than shadowed.
Entry points export a zero-argument callable returning an installed directory:

```toml
[project.entry-points."vcf_rdfizer.linkers"]
my-linker = "my_package:linker_directory"
```

```python
from pathlib import Path

def linker_directory():
    return Path(__file__).parent / "my-linker"
```

Package that directory's manifest, resolver and optional local data. `list`
prints versions, tiers, references, licensing and terms; `list --json` provides
the parsed fields. `check --json` is available for automation. IDs and versions
must be safe path components. Unsupported joins, template variables, unknown
`vcfl:` properties, invalid regexes, bad digests and invalid budgets are errors.

`dry-run <directory> -i <vcf> --limit N` reads the first N records (default 100),
writes no persistent graph/report/cache, and never makes a network request. It
shows up to 20 concrete triples for local tiers, total local link counts, and
Tier 3 unique-key/batch counts and the declared request ceiling. A remote
Tier 2 bundle must already be cached. For Tier 3 it reports a **plan**, not
imagined API results; a general resolver's response-dependent request count
cannot be predicted without executing it. The shipped resolver makes one
request per batch before retries.

## 7. Output integrity and accounting

The runner stages all resolutions before serializing into a temporary file
with the wrapper's `_append_rdf_atomically` contract. It publishes the completed
side-graph atomically and refuses existing outputs. Failures preserve the base
aggregate and publish no partial linkset. Successful response/reference caches
from a failed run are retained for retry.

Each source/linker pair has a `vcfl:Linkset` node, even with zero matches. It
records producer ID/version, source, link count, generation time, and manifest
digest; Tier 2 adds reference digest and assembly; Tier 3 adds resolver and used
response digests. `linkCount` counts distinct links emitted by that linker for
that source and excludes provenance. If two linkers emit the same triple, it
appears once in the side-graph but counts in each linker's own provenance.

Full and post-hoc wrapper runs record linking results in `run.json` and
`stages/linking/<name>.links.json`, with `RunTracker` logging and the normal
progress sidecar protocol (`--quiet`/`--no-progress` apply). Reports include
unique keys, skipped records, links, requests, cache hits, transferred response
bytes, elapsed time, final HTTP status and cached-response timestamps/digests.
Failure reports carry the error and per-linker status. Main-aggregate triple
counts continue to exclude side-graph triples. `--mode link` summary counts are
the new side-graph's triples, including provenance.

An offline replay reproduces links from the same cached requests. Its
`generatedAtTime` and run timings are new; the entire side-graph is therefore
not promised byte-identical.

## 8. What remains from the proposal

The implemented tiers are examples of the extension contract, not completion
of every feature in the proposal:

- **An allele join, but no normalisation.** `vcfl:AlleleJoin` expresses each
  record's alleles as trimmed SPDI and does not left-align or check them
  against the reference sequence; normalise inputs first. Canonical SPDI and
  GA4GH VRS identifiers, which need the sequence, are future work. No
  ClinVar/gnomAD/CADD annotation is implied: matching against ClinVar means
  linking ClinVar too.
- **No production gene bundle is shipped.** The synthetic fixture demonstrates
  digest checking, indexing and assembly refusal. Users supply real references.
- **No `--merge-links`.** Keeping side-graphs separate leaves core validation
  intact; the wrapper's custom-mapping report-only forwarding gap is still open.
- **No plug-in SPARQL/mutation auto-discovery.** Optional `queries/` and
  `mutations.py` from the proposal are not executed. The new unit tests cover
  linker invariants and failures but do not extend the published core mutation
  score. No standalone linkset validator is claimed.
- **No HDT/COTTAS input reader for linking.** Supply `.nt` or `.nt.gz`.
- **No persisted interval index or global cross-process quota.** Reference
  features are indexed in memory; temporary SQLite storage still requires disk
  proportional to join fields and associations. This implementation has not
  been benchmarked on genome-scale cohorts.
- **The manifest vocabulary is provisional.** Third-party authors should pin
  the tool version while the remaining joins and validation contract settle.

## 9. Verification

[`test/test_linking_unit.py`](../test/test_linking_unit.py) exercises known-answer
token links, INFO splitting/escaping, interval boundaries and nested features,
SPDI trimming, contig aliasing and the length guard,
assembly refusal, digest corruption, unordered and gzip RDF, custom subjects,
empty inputs, full-mode integration, discovery, authoring commands, API batching,
cache replay, host pacing, retries, ceilings and failure atomicity.

The shipped Ensembl resolver runs in these tests against a fake HTTP transport;
tests never contact Ensembl or require Docker. That verifies its response
handling and the session policy, not the current availability or completeness
of a public service.

```bash
python -m unittest test.test_linking_unit -v
python -m unittest discover -s test -p 'test_*_unit.py'
```

---

## See also

- [Original design](datalinking-design.md) — rationale and remaining ambitions
- [Custom mappings](rml-mappings.md) — subject and vocabulary compatibility
- [Output and metrics](output-and-metrics.md) — the shared run report layout
- [Validation methodology](validation-methodology.md) — the separate core suite
