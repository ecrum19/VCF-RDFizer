# Roadmap and future development

What is planned, what is merely known-to-be-wrong, and what has been assessed
and deliberately rejected. Every item links to the document where it is
described in full.

This is a working document, not a commitment. Items are ordered by how much they
block something else, not by effort.

---

## Blocking publication

### 1. Vocabulary coverage for the condensed representation — **done, pending release**

The conversion emitted 17 terms that `https://w3id.org/vcf-rdfizer/vocab#` did
not define — `CohortCallMatrix`, `FormatValueVector`, `SampleSet`, `VCFSample`,
`VCFTextVector`, `representationProfile`, `sampleIndex` and the rest — so
condensed graphs were not ontology-backed.

All 17 are now defined in the vocabulary repository (v1.1.0), with two
superclasses giving the enumerations a range, SHACL shapes for the condensed
profile, and a worked example. What remains is publication: the terms only
dereference for a third-party consumer once v1.1.0 is deployed to the w3id
namespace. Nothing further is required in this repository.
Detail: [`vcf-coverage.md`](vcf-coverage.md#vocabulary-alignment).

### 2. The SHACL / missing-value contradiction — **resolved**

`vcfr:missingValuePolicy` required `"."^^vcfr:Null` for a missing token while
`VCFRecordShape` constrained `vcfr:alt` to `sh:datatype xsd:string`, so a record
with `ALT=.` could satisfy neither and the tool could not be conformant.

Vocabulary v1.1.0 resolves it by stating where VCF actually permits the token:
`alt` and `recordId` now accept `xsd:string` or `vcfr:Null`, as `qual` already
did, while `chrom`, `pos` and `ref` keep an exact datatype because they are
required and have no missing form. The conversion already followed the policy,
so no change was needed here.
Detail: [`vcf-coverage.md`](vcf-coverage.md#vocabulary-alignment).

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

- **`vcfr:contigCount` is counted, not read.** A wrong derived contig total is
  undetected. Needs the value in a comparison, not just the predicate in the
  census.
- **Header line values are not compared.** `q08` compares how many lines carry
  each key and `q10` their types; `vcfr:headerValue` itself is only counted. The
  structured attributes that matter are already covered.

Both are query-set additions with matching mutation-catalogue entries, so
closing either one will *fail* the mutation harness until
[`vcf-coverage.md`](vcf-coverage.md) is updated — by design.

## New capability

### 5. Data linking as a plug-in system

The largest planned addition: let users connect the graph to external resources
(rsIDs, genes, clinical assertions, frequencies) through declarative,
distributable linker plugins rather than by patching the tool.

Full design, including the three join strategies, the three plugin tiers, the
network safeguards, the provenance model and the build order, is in
[`datalinking-design.md`](datalinking-design.md). Nothing is implemented yet;
the design exists to fix the extension contract before code depends on it.

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
Full design in [`privacy-policy-design.md`](privacy-policy-design.md).

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
- [`changelog.md`](../changelog.md) — what has actually shipped
