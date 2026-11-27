# Roadmap and future development

What is planned, what is merely known-to-be-wrong, and what has been assessed
and deliberately rejected. Every item links to the document where it is
described in full.

This is a working document, not a commitment. Items are ordered by how much they
block something else, not by effort.

---

## Resolved by the move to the VCF Core vocabulary

Both items that previously blocked publication are closed. They are kept here
briefly because the reasoning is referenced elsewhere.

### ~~1. Vocabulary coverage for the condensed representation~~ — **done**

The 17 condensed-representation terms the conversion emitted without an
ontology behind them — `CohortCallMatrix`, `FormatValueVector`, `SampleSet`,
`VCFSample`, `VCFTextVector`, `representationProfile`, `sampleIndex` and the
rest — are all defined in VCF Core, with SHACL shapes. Condensed graphs are
ontology-backed and citable as linked data.

### ~~2. The SHACL / missing-value contradiction~~ — **done**

`vcfc:VCFRecordShape` now accepts `xsd:string` or `vcfc:Null` for `vcfc:alt`, so
a record with `ALT=.` satisfies both the shape and `vcfc:missingValuePolicy`.
`vcfc:VariantCallShape` likewise accepts the full VCF Float lexical space for
`qual`.

## Blocking publication

### ~~1. The validation suite has not been migrated~~ — **done**

The suite passes: 707 tests, mutation score 96/113 (85%) across 60 mutations.
The runner and the wrapper now share one vocabulary module instead of mirroring
each other, and the census derives every new resource family from the VCF. Ten
of the eighteen new mutations are recorded gaps, each with what would close it.
What changed and what remains:
[`validation-migration-notes.md`](validation-migration-notes.md).

### 2. SHACL conformance is not a test

Checking the output against the vocabulary's own SHACL profiles found four real
modelling bugs during the migration, all of the same kind: a resource typed into
a class whose shape requires a property the record did not supply. That is a
cheap, strong invariant and it should not be a manual step. It needs `pyshacl`
and `rdflib` as dev dependencies and access to the vocabulary's `ontology/` and
`shacl/` directories.

### ~~3. Ordinal datatype inconsistency in the vocabulary~~ — **done**

VCF Core now supplies `vcfc:IntegerLiteralShape`, which accepts `xsd:integer`
and every integer-derived XSD datatype with the bound applied numerically. The
ontology range and the shapes no longer disagree.

## Known defects

### 3. Validation mapping policy is not forwarded

`validation_runner.py` supports `--mapping-policy report-only`, which is exactly
what a custom `--rules` mapping needs: it records the mapping-dependent queries
(`q09`–`q13`) without failing on them. The `vcf-rdfizer` wrapper never passes
it, so the runner always executes `strict` and a correct custom-mapping
conversion is reported as `MISMATCH`.

Fix: expose a `--mapping-policy` flag on the wrapper, and default it to
`report-only` automatically whenever `--rules` is not the shipped default.
Small change; it makes the custom-mapping extension point actually usable.
[Detail](rml-mappings.md#4-what-a-custom-mapping-costs-in-validation).

### 4. Validation coverage gaps with named fixes

From [`vcf-coverage.md`](vcf-coverage.md#remaining-gaps):

- **`vcfc:contigCount` is counted, not read.** No source-comparison query
  compares the derived contig total. Since v3.3.3 the `full` shape profile
  catches a wrong one (`vcfr:ContigCountAgreementShape`), but the default
  profile and the queries still do not.
- **Header line values are not compared.** `q08` compares how many lines carry
  each key and `q10` their types; `vcfc:headerValue` itself is only counted. The
  structured attributes that matter are already covered.

Both are query-set additions with matching mutation-catalogue entries, so
closing either one will *fail* the mutation harness until
[`vcf-coverage.md`](vcf-coverage.md) is updated — by design.

### 9. Fixes held until after the paper's submission

Found while validating the v3.3.3 results. Each was left alone so that what the
paper reports stays reproducible, and each is small. The first changes
conversion, so it needs its own release note.

- **Haploid genotypes get the wrong phasing.** `parse_genotype` in
  `vcf_rdfizer_vocab.py` gives a bare haploid call such as `0` the indicator `/`
  and `vcfc:Unphased`. VCF 4.4 (p. 11) defines an omitted first indicator as `/`
  only if another indicator is `/`, and `|` otherwise, and its BCF section calls
  a haploid `0` implicitly phased. So the call should get `|` and
  `vcfc:Phased`. Today the expanded fault-injection control fails one `full`
  constraint, `vcfc:GenotypeAlleleAgreementShape`, on record 6. The fix:
  - **Converter:** change the default indicator and the haploid branch of
    `is_phased`.
  - **Agreement constraint:** change `vcfr:PhasingStatusAgreementShape`
    (`vcf-rdfizer-agreement.shacl.ttl`) in the same commit. It mirrors today's
    rule and expects `vcfc:Unphased` when the GT holds no separator, so a fixed
    converter would fail it.
  - **Tests:** update the fixture's expected values and the mutation harness's
    `flip_phasing_status` case.
  - **Result:** the change alters the RDF of every haploid call, for example
    chrX and chrY in male samples and mitochondrial calls. The release note
    should say so, and that the complete HG005 VCF (autosomes only) is
    unaffected.
- **The COTTAS engine breaks with Comunica 5.5.0.**
  `@elias.crum/query-sparql-cottas@0.1.0` depends on Comunica's packages by
  semver range. A fresh image build picked up Comunica 5.5.0 (2026-11-24), with
  which a pattern inside an OPTIONAL with a FILTER gets no source ("requires an
  operation with source annotation"). The expanded `preflight_sample_gt_inventory`
  and `q05` queries then fail on COTTAS. The image now installs the engine from
  `docker/comunica-cottas`, which pins every `@comunica` package at the 5.4.0
  tree of the v3.3.2 image. The real fix belongs in `comunica-feature-cottas`:
  - **Upstream:** release a version that works with 5.5.0, or pins its range,
    then drop the pin here.
  - **The other engines:** the file and HDT engines are still installed
    globally with ranged dependencies, so the same thing can happen to them. A
    lockfile for those as well would make the image build reproducible.
- **The QLever version in validation reports is an error message.**
  `tool_version` runs the QLever binary without its library path, so the
  report's `version` field holds a libjemalloc load error instead of a version.
  The SHACL report already leaves the field out. Fix `tool_version` for QLever,
  for example by running it with `LD_LIBRARY_PATH=/opt/qlever/lib` or reading
  the version from the image's build metadata.
- **The published shapes need three changes upstream** (the
  `vcf-rdfizer-vocabulary` repository, then re-vendored with new digests):
  - **Prefixes:** declare `sh:prefixes`. The SPARQL constraints use `vcfc:`
    without declaring it, which SHACL requires. pyshacl fills it in, Rudof does
    not, and the QLever path injects it.
  - **Equivalent queries:** publish the four that VCF-RDFizer substitutes by
    digest (record order, contig contiguity, duplicate IDs, field cardinality),
    so every engine gets the scalable form.
  - **Agreement constraints:** move the four `vcfr:` constraints into a
    vocabulary release, since they check VCF Core properties.
- **Report the three Rudof bugs** listed under item 8.
- **The `full` profile's node-local shapes were not run on the complete HG005
  VCF.** The SPARQL constraints were, on QLever, with no violations. pyshacl's
  record batches need a decoded copy of the graph on disk, which vcf-bench-3
  could not hold for 657.4M triples. Writing the batches compressed, or
  streaming them, would remove that limit.

## New capability

### 5. Data linking as a plug-in system

An initial implementation now connects the graph through declarative,
distributable linker plug-ins, with examples of all three tiers: dbSNP token
links, GFF3 interval bundles (synthetic, and Ensembl's gene annotation), SPDI
allele identity, and two live API resolvers (Ensembl, MyVariant.info).

Full design, including the three join strategies, the three plugin tiers, the
network safeguards, the provenance model and the build order, is in
[`datalinking-design.md`](datalinking-design.md). The implemented contract and
commands are in [`datalinking.md`](datalinking.md). The allele join exists
(`spdi`, over pre-normalised inputs). Normalisation inside the join (canonical
SPDI, GA4GH VRS), link merging, and plug-in validation/mutation auto-discovery
remain planned;
the manifest vocabulary is provisional.

The hardest part is not the plugin system — it is allele normalization
(§9 of that document), because an under-matching join looks exactly like a true
negative.

### 6. Query-time decoding for condensed graphs

Condensed mode stores each FORMAT key as one tab-separated vector, which is what
makes cohort scale tractable. Reconstructing sample *i*'s value at query time
needs a split operation SPARQL does not portably provide.

The options have been assessed in detail in
[`sample-representation-guide.md`](sample-representation-guide.md#7-assessment-of-geosparql-and-graphdb-sparql-extensions):

| Option | Verdict |
| --- | --- |
| GeoSPARQL geometry functions | **No.** Genotype vectors are not geometries; the analogy does not survive contact |
| GraphDB `spif:split` | Useful as a prototype tokenizer and validation aid, not as a production join |
| `spif:split` + `spif:for` | Building blocks, but no ordinal pairing mechanism, so no trustworthy sample↔value join |
| A custom vector-extraction function, or an application-side decoder | **The promising direction** |

The recommended shape is a function that takes a vector and a sample index (or a
`VCFSample` IRI) and returns both the position and the value, with the six
safeguards listed in that section. Crucially, this belongs in a **separate
decoding/projection layer** — the condensed model itself should not change.

There is also a scale caveat that any implementation has to respect: expanding
every vector in a cohort-wide query produces millions of bindings before
filtering. A targeted single-position extractor is the right primitive; a
whole-graph expansion is not.

### 7. Granular privacy policies over the graph

Today the tool has exactly one disclosure setting: convert everything. There is
no way to release a cohort graph with some participants withheld, some regions
degraded, or some fields suppressed, and no machine-readable record of what a
given artifact was permitted to contain.

The proposal is an ODRL profile whose assets are **graph selectors** (by class,
predicate, sample, genomic region, declared field or pattern), compiled to a
release plan and enforced at the cheapest available point in the existing
pipeline — TSV pre-filtering, emitter-time filtering, or a post-hoc pass.
Full design in [`privacy-policy-design.md`](privacy-policy-design.md). The first
slice, now v0.2.0 and evaluated on single-sample real genomes up to a whole
genome, is specified in [`policy-demonstrator.md`](policy-demonstrator.md); its
§10 maps later versions onto the full design's build order.

Two findings from that design are worth surfacing here because they affect work
outside it:

- **`ParsedSampleRecord` does not carry `CHROM` or `POS`.** `_parse_row` reads
  columns 0, 1, 7, 9, 10 and −1. Any region-scoped feature evaluated at emission
  time needs both, and adding them is two fields and two index reads.
- **Condensed mode is not per-sample enforceable by triple filtering.** One
  `FormatValueVector` literal holds every participant's value for a FORMAT key,
  so the unit of protection is finer than the unit of storage. Redaction has to
  rewrite the literal, and a single masked position is itself disclosive.

The honest framing, which the design keeps throughout: this is *governed
release*, not anonymization. Genotypes identify people, and no access-control
layer changes that.

### 8. A native SHACL engine (Rudof)

pyshacl validates the node-local shapes a batch of records at a time, and the
`full` profile's SPARQL constraints run once each over a QLever index. pyshacl
stays because every published result was produced with it. Rudof 0.3.39, a Rust
engine, was evaluated as its replacement. Given RDFS typing and declared
prefixes, it reaches pyshacl's verdict on every fault-injection graph with the
core profile, and on 113 of 115 with the full one, where the two differences are
explained. It is about 24 times faster on the core profile, with the same
memory per triple.

Porting means running Rudof on the record batches, keeping the SPARQL
constraints on QLever, and shipping its binary in the image. Three Rudof issues
need reporting upstream first: right-associative `a - b + c`, a blank-node
`$this` pre-bound with `VALUES`, and `xsd:int` not compared with `xsd:integer`.
The evaluation also found issues in the published shapes and a converter bug,
both listed under item 9. Measurements, issues and the porting steps:
[`shacl-engines.md`](shacl-engines.md).

## Deliberately not planned

Stated so the absence reads as a decision rather than an oversight.

| Not planned | Why |
| --- | --- |
| A Docker-free installation path | The pinned image is what makes results reproducible across machines |
| Variant normalization inside the conversion | The graph is a faithful transcription of the file; normalization is a linking-time concern |
| Distributed or multi-node execution | Out of scope; chunking already bounds memory on one machine |
| Incremental graph update | Would require identity and provenance machinery the current model does not have |
| Clinical interpretation or pathogenicity assertion | The tool transcribes; it is not a clinical authority |
| Differential privacy on aggregate queries | Formal guarantees need a persistent per-recipient budget ledger; without one it is decoration. See [`privacy-policy-design.md` §9](privacy-policy-design.md#9-beyond-access-control-disclosure-limitation) |
| Deciding whether a release is legally compliant | A tool can implement and evidence a policy; it cannot make a data-protection determination |
| `.bcf` input | Would pull `htslib` into the parsing path; convert with `bcftools` first |

---

## See also

- [Limitations](limitations.md) — the current state, honestly
- [Data linking design](datalinking-design.md) — the largest planned addition
- [Privacy policy design](privacy-policy-design.md) — governed release over parts of the graph
- [Validation methodology](validation-methodology.md) — how coverage is measured, so gaps stay falsifiable
- [Releases](https://github.com/ecrum19/VCF-RDFizer/releases) — what has actually shipped
