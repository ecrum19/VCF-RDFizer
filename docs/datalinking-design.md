# Data linking: a plug-in architecture

*Status: **design proposal**. Nothing described here is implemented yet. This
document exists to fix the extension contract before code is written, because
third-party linkers are the point and a contract is much harder to change once
people depend on it.*

VCF-RDFizer converts a VCF into RDF that is faithful to the VCF and to nothing
else. Every IRI it mints is derived from the source file, so the graph is
self-contained, reproducible, and — deliberately — an island. It says
`vcfr:recordId "rs334"` and stops there.

Data linking is the step that connects that island to the rest of the
linked-data web: the rsID becomes a dbSNP resource, the coordinate becomes an
Ensembl gene, the allele becomes a ClinVar assertion. The requirement is that
users other than the maintainers can add those connections for their own
domain without patching the tool.

---

## 1. The observation the design rests on

Linking a variant to an external resource is always a **join**, and in this
domain there are only three join keys:

| Strategy | Key | Typical targets |
| --- | --- | --- |
| `token` | a value in `ID`, or the value of a named `INFO` key | dbSNP/Ensembl rsIDs, COSMIC identifiers, `GENEINFO` gene symbols |
| `interval` | `CHROM` + `POS` (+ `REF` length) overlap | genes, transcripts, exons, regulatory regions, cytobands, panel membership |
| `allele` | normalized `CHROM`-`POS`-`REF`-`ALT` | ClinVar assertions, gnomAD frequencies, CADD scores, variant-annotation APIs |

Everything on the near-term wish list — rsID linking, medically relevant gene
linking, clinical significance, population frequency, drug-target association —
is one of those three.

This matters because it determines where the seam goes. If the framework owns
the three join strategies, a plugin author never writes VCF parsing, never
mints an IRI, never escapes an N-Triples literal, and never thinks about
streaming. They declare *which key* and *what to emit on a match*. If the
framework instead offered a generic "here is a record, do what you like" hook,
every plugin would reimplement the same four things, each slightly wrong.

---

## 2. Three tiers of plugin

A linker is a directory. It is resolved both from a plugin search path and from
Python entry points (`vcf_rdfizer.linkers`), so a linker can be `pip install`ed
*or* dropped into a directory and mounted into the container.

```text
linkers/rsid-dbsnp/
  linker.ttl        # manifest: identity, join, source, emission, budget
  resolver.py       # OPTIONAL - only when resolution is not templatable
  queries/          # OPTIONAL - .rq validation queries for the produced links
  mutations.py      # OPTIONAL - mutation catalogue entries for those links
```

### Tier 1 — declarative, no code

The rsID case needs no Python and no network. It is a token join feeding an IRI
template:

```turtle
@prefix vcfl: <https://w3id.org/vcf-rdfizer/linking#> .

<#rsIDdbSNP> a vcfl:Linker ;
  vcfl:id        "rsid-dbsnp" ;
  vcfl:version   "1.0.0" ;
  vcfl:title     "rsID to dbSNP" ;
  vcfl:join [ a vcfl:TokenJoin ;
              vcfl:field   "ID" ;
              vcfl:splitOn ";" ;
              vcfl:accept  "^rs[0-9]+$" ] ;
  vcfl:emit [ vcfl:subject        vcfl:VariantCall ;
              vcfl:predicate      vcfl:sameVariantAs ;
              vcfl:objectTemplate "https://identifiers.org/dbsnp:{TOKEN}" ] .
```

Roughly two thirds of genuinely useful linkers are expressible at this tier.
Making Tier 1 real is the highest-leverage decision in the whole design: it
turns "write a plugin" from a Python project into a fifteen-line file, which is
the difference between an extension point that gets used and one that does not.

### Tier 2 — local reference bundle

Gene linking is an interval join against a reference the plugin *declares*
rather than ships:

```turtle
  vcfl:reference [ vcfl:url      "https://ftp.ensembl.org/.../Homo_sapiens.GRCh38.113.gff3.gz" ;
                   vcfl:sha256   "e3b0c442..." ;
                   vcfl:assembly "GRCh38" ;
                   vcfl:format   vcfl:GFF3 ] ;
```

The runner fetches once, verifies the digest, caches under
`~/.cache/vcf-rdfizer/linkers/`, and builds the interval index. Because the
assembly is declared, the runner can compare it against the VCF's `##reference`
and **refuse to run on a mismatch**. That strictness is deliberate: a
GRCh37-coordinate variant linked against GRCh38 gene intervals produces
confident, plausible, wrong annotations, and nothing downstream would catch it.

### Tier 3 — live service

A `resolver.py` implementing one narrow protocol:

```python
def resolve(batch: Sequence[LinkKey], ctx: LinkerContext) -> Iterable[Link]:
    """Map a batch of join keys to links. Called with deduplicated keys."""
```

Batch in, links out. The plugin never opens a socket itself — it calls
`ctx.session.get(...)`. That indirection is what makes the safeguards in §4
enforceable rather than advisory.

---

## 3. Where linking happens in the pipeline

Two entry points, sharing one implementation:

**In-run.** `--link rsid-dbsnp,gene-ensembl` adds a linking stage to full mode.
It runs beside the existing direct emitters (`emit_record_detail`,
`emit_sample_representation` in `vcf_rdfizer.py`), reusing the
`_append_rdf_atomically(rdf_path, stats, producer)` contract so links stream
with the same atomicity, the same stats shape, and the same progress sidecar
events as everything else.

**Post-hoc.** `--mode link --rdf <existing>.nt.gz --link <ids>` enriches a graph
from a conversion that has already happened. This is what makes the feature
adoptable by anyone with existing outputs, and it is also how a linkset is
re-generated when a reference bundle is updated.

---

## 4. Network safeguards

Live APIs and local bundles are both supported, but every request a Tier 3
plugin makes goes through `ctx.session`, so the runner — not the plugin —
enforces policy:

- **Declared, enforced budget.** `vcfl:maxRequestsPerSecond`,
  `vcfl:maxRequestsPerRun`, `vcfl:batchSize`, and `vcfl:contactEmail` are
  manifest fields. Exceeding the per-run ceiling aborts that linker with a clear
  error rather than degrading into an unattended crawl.
- **Per-host token bucket, shared across plugins.** Two linkers that both target
  `rest.ensembl.org` share one rate budget; a service sees one client, not two.
- **Mandatory on-disk response cache**, keyed by
  `(linker id, linker version, request hash)`. On a cohort VCF the hit rate is
  the entire performance story.
- **Deduplication before dispatch.** The full run's key set is deduplicated
  before a single request is issued. Five million variants routinely reduce to a
  few hundred thousand distinct rsIDs.
- **Backoff that respects the service.** `Retry-After` honoured, exponential
  backoff on 429/5xx, a hard concurrency cap, and a `User-Agent` carrying the
  tool version and the declared contact address.
- **`--offline` and `--links-cache-only`.** Reproducing a published run must not
  require the service to still be up, and must not silently pick up an answer
  that has since changed.
- **Full accounting in the run manifest.** Requests issued, cache hits, bytes
  transferred, wall time, and final service status, per linker — using the
  existing `write_run_manifest` and `RunTracker` plumbing.

**An honest scale caveat.** Tier 3 linking is a different order of operation
from the rest of the pipeline: the conversion is bounded by local I/O, a live
API is bounded by someone else's rate limit. Tier 3 is appropriate for filtered
or prioritized variant sets. Anything genome-wide should use a Tier 2 bundle,
and the documentation should say so rather than let users discover it after
eight hours.

---

## 5. Output and provenance

Links are written to a **side-graph**, `<sample>.links.nt`, next to the main
aggregate — not merged into it by default. Three reasons:

1. The validation suite's predicate census, class census and identity digests
   (`q09`–`q13`) are defined against what the VCF implies. Injecting external
   triples into the aggregate would make every one of them report a false
   positive.
2. Users can adopt links without changing the graph they already query.
3. A linkset can be regenerated, versioned, or discarded independently of an
   expensive conversion.

Each linkset carries its own provenance node:

```turtle
<file://cohort.vcf#linkset/rsid-dbsnp>
    a vcfl:Linkset ;
    vcfl:producedBy      <https://w3id.org/vcf-rdfizer/linker/rsid-dbsnp/1.0.0> ;
    vcfl:referenceDigest "sha256:e3b0c442..." ;
    vcfl:assembly        "GRCh38" ;
    vcfl:linkCount       41233 ;
    prov:generatedAtTime "2026-09-04T11:04:22Z"^^xsd:dateTime .
```

`--merge-links` folds the side-graph into the aggregate for consumers who want
one file, at which point the mapping-policy fallback already used for custom
`--rules` applies and `q09`–`q13` become report-only.

**On predicate choice.** `owl:sameAs` between a VCF record and a dbSNP resource
is a much stronger claim than most users intend — it licenses inferring every
dbSNP property onto the VCF record and vice versa. The default vocabulary should
therefore be explicit and weak: `vcfl:sameVariantAs`, `vcfl:overlapsGene`,
`vcfl:hasAnnotation`. A manifest can override it, deliberately.

---

## 6. Authoring tooling

`vcf-rdfizer-link`, mirroring the existing `vcf-rdfizer-rules` CLI:

| Command | Purpose |
| --- | --- |
| `vcf-rdfizer-link list` | Installed linkers, versions, tiers, and declared references |
| `vcf-rdfizer-link keys` | The join keys a manifest may reference — the analogue of `rules columns` |
| `vcf-rdfizer-link init -o my-linker/` | Scaffold a manifest with annotated examples |
| `vcf-rdfizer-link check my-linker/` | Validate the manifest, reference digest, assembly, and budget before a long run |
| `vcf-rdfizer-link dry-run my-linker/ -i sample.vcf` | Run over the first N records, write nothing, report the links it would emit and the requests it would issue |

`dry-run` is the one that earns its keep: it is what stops an author from
discovering a two-million-request mistake at hour three of a run.

---

## 7. Validation of linksets

A linker that ships `queries/*.rq` and `mutations.py` is picked up by the same
discovery already used for `src/validation/queries/`, so its links are
mutation-scored exactly like the core graph — see
[`validation-methodology.md`](validation-methodology.md).

Linkset-specific checks worth making standard:

- every link subject resolves to a subject that exists in the base graph;
- every link object is a syntactically valid absolute IRI;
- no blank nodes and no empty terms (the existing preflights apply unchanged);
- the declared `vcfl:linkCount` matches the emitted triple count;
- the reference digest recorded in the linkset matches the bundle actually used.

Holding third-party linkers to the same mutation-testing bar as the core
conversion is a genuinely defensible claim, and it is only available because
the validation harness was built to be extended.

---

## 8. Build order

| Step | Delivers | Unlocks |
| --- | --- | --- |
| 1 | `vcfl:` vocabulary, manifest parsing, `token` join, Tier 1 templates | rsID linking with no network and no plugin code |
| 2 | Discovery (path + entry points), `vcf-rdfizer-link` CLI, side-graph + provenance, manifest metrics | The third-party extension contract |
| 3 | `interval` join, reference fetch/cache/digest, assembly guard | Gene and region linking |
| 4 | `allele` join and allele normalization | ClinVar, gnomAD, CADD from bundles |
| 5 | `ctx.session` with the full safeguard stack | Tier 3 live services |

Steps 1–2 are the contract; everything after is capability. The manifest
vocabulary should be frozen and versioned at the end of step 2.

---

## 9. Known hard problems

Stated up front rather than discovered later.

**Allele normalization is the real correctness risk.** Left-alignment,
trimming, and multi-allelic splitting must match whatever the reference bundle
did, or the join silently under-matches. Under-matching looks like "this variant
has no ClinVar entry", which is indistinguishable from a true negative. Step 4
should not ship without a normalization test set with known-answer cases.

**"Medically relevant" is a curation claim, not a data fact.** A gene-relevance
linker should link to a *named, versioned panel* — and say which — rather than
asserting clinical relevance in VCF-RDFizer's own voice. The tool is not a
clinical authority and its output should not imply that it is.

**Assembly mismatch is silent.** Hence the declared assembly and the hard
refusal in §2. There is no way to detect it after the fact from the links alone.

**External identifiers drift.** rsIDs are merged and retired; ClinVar
significance is revised. A link is true as of the reference version recorded in
the linkset, which is exactly why the provenance node in §5 is mandatory rather
than optional.

**Licensing is the plugin author's problem, and must be visible.** Some
reference resources are not redistributable and some APIs forbid bulk use. The
manifest should carry a `vcfl:license` and `vcfl:termsOfUse` that
`vcf-rdfizer-link list` prints, so a user can see what they are agreeing to
before a run rather than after a publication.

---

## See also

- [Architecture](architecture.md) — where a linking stage sits in the pipeline
- [Custom RML mappings](rml-mappings.md) — the existing extension point this design is modelled on
- [Validation methodology](validation-methodology.md) — the harness linksets would plug into
- [Roadmap](roadmap.md) — how this relates to the other planned work
