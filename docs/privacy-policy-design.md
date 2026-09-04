# Granular privacy policies over a VCF graph

*Status: **design proposal**. Nothing described here is implemented yet. Like
[`datalinking-design.md`](datalinking-design.md), this document exists to fix a
contract before code depends on it — and, in this case, to be explicit about
what a policy layer can and cannot achieve, because the failure mode of a
privacy feature is a false sense of safety.*

VCF-RDFizer currently makes one decision about disclosure: it converts
everything. Every sample, every genotype, every header line, every free-text
`Description` goes into the graph, and every artifact is all-or-nothing. The
only granularity available today is "convert a different VCF".

What is wanted is the ability to say, in a machine-readable and auditable way:
*these samples, under this purpose, may see these regions at this resolution* —
and to have the tool produce artifacts that actually honour it.

---

## 1. The uncomfortable premise

**Genotypes are identifiers.** A few dozen independent common variants are
enough to pick one individual out of a population (Lin, Owen & Altman,
*Science* 2004), aggregate allele frequencies alone can reveal whether a known
individual was in a cohort (Homer et al., *PLoS Genetics* 2008), and Y-chromosome
haplotypes have been used to recover surnames from public genealogy databases
(Gymrek et al., *Science* 2013).

Three consequences follow, and they shape everything below:

1. **Removing `vcfr:sampleId` does not anonymize anything.** The genotype vector
   that remains is a stronger identifier than the label you deleted.
2. **Access control is not anonymization.** A policy layer governs *who is
   permitted to receive what*, and creates an audit trail. It does not make the
   released data non-identifying, and nothing in this design should be described
   as if it did.
3. **The artifact is the enforcement boundary.** Once someone holds a `.hdt`
   file, no policy engine is in the loop. Query-time enforcement only exists
   while you control the endpoint.

This document therefore proposes a system for **governed release**, not for
anonymity. Where a mechanism reduces re-identification risk (thresholding,
generalization), it says by how much and under what assumption.

---

## 2. What "a portion of the VCF graph" means

The graph has a small number of natural cut planes, and they map cleanly onto
the shapes the conversion already emits (see
[`conversion.md`](conversion.md#6-iri-templates)):

| Cut | Addresses | Example |
| --- | --- | --- |
| **By class** | all resources of a type | every `vcfr:SampleCall` |
| **By predicate** | all triples with a predicate | every `vcfr:sampleName` |
| **By sample** | one participant's contribution | everything hanging off `#samples/NA12878` |
| **By region** | a genomic interval | `chr19:44,905,791-44,909,393` (*APOE*) |
| **By declared field** | one INFO or FORMAT key | `DP` yes, `GT` no |
| **By record predicate** | an arbitrary graph pattern | records where `FILTER != PASS` |
| **By granularity** | not a subgraph at all | counts permitted, individual calls not |

The last row is the one that access control alone cannot express, and it is the
one that matters most for the attacks in §1. It is handled in §9.

**A note on scope.** Two families of triple leak more than people expect and are
easy to forget when thinking in terms of genotypes:

- **IRIs themselves.** `file://cohort.vcf#sample/1/NA12878` contains the sample
  name. `#record/{ROW_ID}` is a monotonic counter, so row identifiers disclose
  the source ordering — and therefore approximate genomic position — even if
  `vcfr:pos` is dropped. And `{SOURCE_FILE}` is the VCF's basename, which in
  practice is often `patient_12345.vcf`. Filtering triples while leaving IRIs
  intact leaks membership. See §7.
- **Header lines.** `vcfr:headerValue` and the raw line text carry `##source`
  (pipeline and centre identifiers), `##SAMPLE` and `##PEDIGREE` (family
  structure, by design), free-text `Description` fields, and `##fileDate`.
  A policy that covers genotypes and ignores the header section has not covered
  the graph.

---

## 3. Is ODRL the right choice?

Yes for the policy layer, no as the whole answer. Being precise about the gap is
what makes the rest of the design tractable.

**What ODRL gives you, that is genuinely hard to get elsewhere:**

- A W3C Recommendation, so a policy is a citable, interoperable artifact rather
  than a bespoke config file.
- RDF-native: the policy lives in the same graph model as the data, can be
  published, dereferenced, versioned and signed.
- The vocabulary the problem actually needs — `odrl:permission` /
  `odrl:prohibition` / `odrl:duty`, `odrl:assignee`, `odrl:constraint` with
  `odrl:purpose` and `odrl:recipient` left operands.
- A **profile mechanism** designed for exactly this situation: extend the core
  with domain terms without forking it.

**What ODRL does not give you, and must be supplied:**

| Gap | Consequence |
| --- | --- |
| `odrl:target` points at an *Asset* (an IRI). There is no notion of "these triples". | A selector vocabulary is required. This is the core technical work. |
| Evaluation semantics in the ODRL Information Model are **informative**, not normative. | Conflict resolution and rule ordering must be pinned by the profile, or two implementations will disagree. |
| Rules permit or prohibit. They cannot say *how* to degrade. | An effect/transform vocabulary is required (drop, generalize, threshold, pseudonymize). |
| No enforcement engine exists. | Enforcement is ours to build; ODRL is the declaration, not the mechanism. |

**Alternatives considered:**

| Option | Verdict |
| --- | --- |
| SHACL shapes as policy | Excellent at *describing* a permitted shape, and worth reusing to **verify** a release (§11) — but it is a validation language with no notion of party, purpose or duty. Not a policy language. |
| Solid WAC / ACP | Resource-granular. A VCF graph is one resource; the whole problem is sub-resource granularity. |
| XACML | Mature and battle-tested, but not RDF-native, heavyweight, and it would put the policy outside the data ecosystem the rest of the tool lives in. |
| Bare SPARQL views | Simplest possible enforcement, and in fact what the compiler emits — but as a *policy* it records no party, purpose, obligation or provenance, so it cannot be audited or presented to a data access committee. |

**Recommended shape:** ODRL as the front end, a VCF-RDFizer profile supplying
selectors and effects, compiled down to a plain internal **release plan**. Keep
the enforcement layer independent of ODRL so a second front end (a DUO-only
consent file, say) can be added without touching the engine.

---

## 4. The profile

Namespace `vcfp:` = `https://w3id.org/vcf-rdfizer/policy#`, declared through
`odrl:profile`.

### 4.1 Assets are graph selections

```turtle
@prefix odrl: <http://www.w3.org/ns/odrl/2/> .
@prefix vcfp: <https://w3id.org/vcf-rdfizer/policy#> .
@prefix vcfr: <https://w3id.org/vcf-rdfizer/vocab#> .
@prefix duo:  <http://purl.obolibrary.org/obo/> .

<#apoe-locus> a odrl:Asset , vcfp:GraphSelection ;
  vcfp:selector [ a vcfp:RegionSelector ;
                  vcfp:assembly "GRCh38" ;
                  vcfp:chrom    "chr19" ;
                  vcfp:start    44905791 ;
                  vcfp:end      44909393 ] .

<#direct-identifiers> a odrl:Asset , vcfp:GraphSelection ;
  vcfp:selector [ a vcfp:PredicateSelector ;
                  vcfp:predicate vcfr:sampleName , vcfr:sampleId ] .

<#withdrawn-participants> a odrl:Asset , vcfp:GraphSelection ;
  vcfp:selector [ a vcfp:SampleSelector ;
                  vcfp:sampleId "NA12878" , "NA12891" ] .
```

| Selector | Keys | Notes |
| --- | --- | --- |
| `vcfp:ClassSelector` | `vcfp:class` | Every resource asserting that type, and its outbound triples |
| `vcfp:PredicateSelector` | `vcfp:predicate` | Triple-level; the cheapest to enforce |
| `vcfp:SampleSelector` | `vcfp:sampleId`, `vcfp:sampleIndex` | Resolves through `SampleSet` membership |
| `vcfp:RegionSelector` | `vcfp:assembly`, `vcfp:chrom`, `vcfp:start`, `vcfp:end` | **`vcfp:assembly` is mandatory** and checked against `##reference`; a mismatch aborts, for the same reason it does in the linking design |
| `vcfp:FieldSelector` | `vcfp:formatKey`, `vcfp:infoKey` | By declared field id |
| `vcfp:HeaderSelector` | `vcfp:headerKey` | `##SAMPLE`, `##PEDIGREE`, `##source`, … |
| `vcfp:PatternSelector` | `vcfp:ask` | A SPARQL graph pattern. The escape hatch; expensive, and excluded from the cheap enforcement tiers (§5) |

Selectors compose with `vcfp:allOf` / `vcfp:anyOf` / `vcfp:not`. ODRL's own
`odrl:AssetCollection` with `odrl:refinement` is deliberately **not** reused
here: its refinement semantics are about collection membership, not about
sub-graph extents, and overloading it would produce policies that look standard
while meaning something non-standard.

### 4.2 Effects: what a rule actually does

A prohibition that can only mean "drop everything" is too blunt for genomic
data, where the useful answer is usually *less resolution*, not *nothing*. The
profile therefore refines `odrl:duty` with a transform:

| `vcfp:transform` | Effect |
| --- | --- |
| `vcfp:drop` | Omit matched triples entirely (default for a prohibition) |
| `vcfp:pseudonymize` | Replace a term with a keyed, per-release token (§7) |
| `vcfp:generalize` | Reduce resolution: `POS` → 1 Mb window, `GT` → carrier / non-carrier, `DP` → banded |
| `vcfp:threshold` | Suppress a value below a count, e.g. allele count < 5 |
| `vcfp:aggregateOnly` | The subgraph is reachable only through a counting query (§9) |
| `vcfp:maskVectorPositions` | Condensed mode only: rewrite the tab-separated literal, replacing masked sample positions with `.` (§8) |

```turtle
<#cohort-release-1.2> a odrl:Set ;
  odrl:uid      <https://example.org/policy/cohort-release/1.2> ;
  odrl:profile  <https://w3id.org/vcf-rdfizer/policy> ;
  odrl:conflict odrl:prohibit ;

  # Hard exclusion: withdrawn consent. No purpose overrides this.
  odrl:prohibition [
      odrl:target   <#withdrawn-participants> ;
      odrl:action   odrl:read ;
      odrl:assignee odrl:All ] ;

  # Degrade rather than deny: APOE is readable only as carrier status.
  odrl:permission [
      odrl:target <#apoe-locus> ;
      odrl:action odrl:read ;
      odrl:duty [ odrl:action    odrl:anonymize ;
                  vcfp:transform vcfp:generalize ;
                  vcfp:genotypeResolution vcfp:carrierStatus ] ] ;

  # Purpose-bound permission, expressed with GA4GH DUO codes.
  odrl:permission [
      odrl:target    <#genotypes> ;
      odrl:action    odrl:read ;
      odrl:assignee  <https://example.org/party/consortium-b> ;
      odrl:constraint [ odrl:leftOperand  odrl:purpose ;
                        odrl:operator     odrl:isAnyOf ;
                        odrl:rightOperand duo:DUO_0000007 ] ;
      odrl:duty [ odrl:action odrl:inform ;
                  vcfp:auditSink <https://example.org/audit> ] ] .
```

### 4.3 Consent codes as purposes

`odrl:purpose` right operands should be **GA4GH Data Use Ontology** terms rather
than free strings. DUO is what data access committees and repositories already
speak, so this makes a policy reviewable by the people who actually grant
access, and it means the tool is not inventing a consent vocabulary.

Pin the DUO release in the policy (`vcfp:duoVersion`) and resolve term IRIs
against that release — DUO evolves, and a policy that silently re-interprets a
consent code is worse than one that fails to load.

### 4.4 Conflict resolution, pinned

Because ODRL leaves this informative, the profile **normatively** fixes it:

1. `odrl:conflict odrl:prohibit` is the default and the only value recommended
   for a release policy. Deny wins.
2. A rule with no applicable transform defaults to `vcfp:drop`.
3. Effects on the same selection compose most-restrictive-first
   (`drop` > `aggregateOnly` > `threshold` > `generalize` > `pseudonymize`).
4. `odrl:conflict odrl:invalid` is supported for strict deployments: any
   conflict voids the policy and the run aborts rather than guessing.
5. **Anything not matched by a permission is denied.** Default-deny is the only
   defensible posture; a default-allow policy language for genomic release is a
   trap.

---

## 5. Where enforcement happens

Three tiers, in increasing cost, falling directly out of the existing pipeline
([`architecture.md`](architecture.md#5-data-flow-in-full-mode)). The compiler
assigns each rule to the cheapest tier that can express it.

### Tier 1 — pre-RML, on the TSV

Row and column removal applied to `<sample>.records.tsv` and
`<sample>.header_lines.tsv` **before RMLStreamer runs**.

This is by far the cheapest option and the only one that covers RML-produced
triples as well as wrapper-produced ones, because nothing downstream ever sees
the excluded data. It handles `SampleSelector` (drop a sample column),
`RegionSelector` (drop rows — CHROM and POS are columns 3 and 4, right there),
and `HeaderSelector` (drop header rows).

Its limit is granularity: it removes whole rows and columns, so it cannot
express "keep this record but drop its `DP`".

### Tier 2 — emission time, in the wrapper's emitters

The three direct emitters
([`architecture.md`](architecture.md#4-where-the-split-leaks-and-why)) hold the
full parsed record while they emit, so a predicate-, field- or value-level rule
costs one branch per emitted triple and needs no second pass.

Concrete hook points in [`vcf_rdfizer.py`](../vcf_rdfizer.py):

| Where | Change |
| --- | --- |
| `_append_rdf_atomically(rdf_path, stats, producer)` | Wrap `emit` in a policy filter; the atomic-append and rollback behaviour is unchanged |
| `append_expanded_sample_rdf`, `append_condensed_sample_rdf` | Sample- and field-level rules; the condensed one also needs §8 |
| `append_record_detail_rdf` | QUAL and INFO field rules |
| `append_header_representation_rdf` | Header rules |

**One small prerequisite.** `ParsedSampleRecord` currently carries
`source_file`, `row_id`, `qual`, `info`, `format_keys`, `sample_payloads` and
`sample_values` — `_parse_row` reads columns 0, 1, 7, 9, 10 and −1, and **skips
CHROM and POS** (columns 2 and 3). A `RegionSelector` evaluated at emission time
needs both. That is two extra fields on the dataclass and two extra index reads
in `_parse_row`, with no measurable cost, and it should be the first commit in
this work.

### Tier 3 — post-hoc, over an existing artifact

For applying a policy to a graph that has already been converted
(`--mode redact --rdf <existing>.nt.gz --policy <p>.ttl`).

Triple- and predicate-level rules are a single streaming pass. **Region rules
are not**, because an N-Triples stream has no ordering guarantee: by the time
you see `<…#sample/4711/NA12878> vcfr:fieldValue "0/1"`, the triple that told
you record 4711 is at `chr19:44906000` may be long gone. This needs two passes:
pass one builds a `row_id → (chrom, pos)` map restricted to records that fall in
a policy region, pass two filters. Memory scales with the number of **in-scope**
records, not the graph, which keeps it bounded in practice — but it is
materially more expensive than doing the same work in Tier 1 or 2, and the
documentation should say so rather than let users discover it.

### Query time, and why it is secondary

A policy can also be compiled to SPARQL rewriting at an endpoint. It is worth
building, but it is **not** the primary enforcement point for this tool, for a
reason worth stating plainly: VCF-RDFizer's principal output is *files that
people take away*. An HDT or COTTAS artifact handed to a collaborator is outside
any policy engine forever. Query-time enforcement protects an endpoint you
operate; materialization protects a file you ship. Most users of this tool need
the second.

Where an endpoint *is* operated, note also that rewriting is easy to get subtly
wrong: a prohibition enforced with `FILTER NOT EXISTS` still lets a caller infer
the excluded set from counts and negative results. Query-time enforcement should
be paired with the aggregate controls in §9, not treated as sufficient alone.

---

## 6. The compile step

```text
  ODRL policy (.ttl)
        │  parse, validate against the profile, resolve DUO version
        ▼
  rule set  ──▶  conflict resolution (§4.4)  ──▶  release plan
        │
        ├── Tier 1 ops:  TSV row/column predicates
        ├── Tier 2 ops:  per-triple predicates, keyed by emitter
        ├── Tier 3 ops:  post-hoc stream filters (+ any two-pass requirements)
        └── residual:    rules no tier can enforce  ──▶  ABORT
```

The **residual set must abort the run, never warn**. A privacy rule that was
parsed, reported, and then not applied is the single worst outcome this design
can produce: the operator believes the release is governed and it is not. If the
compiler cannot enforce a rule, it must refuse to produce an artifact.

The release plan is a plain data structure with no ODRL dependency, which keeps
the engine testable in isolation and leaves room for a second front end.

---

## 7. Pseudonymization and IRI re-minting

Because IRIs embed sample names, the source filename and a positional row
counter (§2), filtering triples is not enough. Under `vcfp:pseudonymize` the
release view must re-mint IRIs.

**Construction.** `token = base32(HMAC-SHA256(release_key, namespace ‖ value))`,
truncated to a documented length, where `namespace` distinguishes sample names
from row ids from filenames so the same string in two roles does not produce the
same token.

**Properties this gives, and the ones it does not:**

- Stable *within* a release, so joins inside the released graph still work.
- Unlinkable *across* releases, because `release_key` is fresh per release —
  unless linkage is deliberately wanted, in which case a named, reused key is an
  explicit policy choice recorded in the manifest.
- The key is **never** written into the artifact or the manifest; only its
  identifier and algorithm are. Re-identification remains possible for whoever
  holds the key, which is the point — this is pseudonymization, not
  anonymization, and the manifest should use that word.
- It does **not** defeat genotype-based re-identification (§1). Nothing here
  does.

Row-id re-minting deserves specific attention: replacing `#record/4711` with a
token removes the ordering leak, but only if the tokens are emitted in an order
that does not reconstruct it. Sort the released graph by token, not by source
order.

---

## 8. The condensed-representation problem

This one is specific to VCF-RDFizer and easy to miss until it produces a leak.

In **expanded** mode, one participant's value is its own resource:

```text
<…#sample/4711/NA12878/fmt/GT>  vcfr:fieldValue  "0/1" .
```

Excluding a sample is triple filtering. Straightforward.

In **condensed** mode, all participants' values for one FORMAT key live in
**one literal**:

```text
<…#call/4711/matrix/fmt/GT>  vcfr:encodedValues  "0/1\t0/0\t1/1\t./."^^vcfr:VCFTextVector .
```

There is no triple to remove for one sample. Graph-pattern access control cannot
express "sample 2 only" over this shape at all — the unit of protection is
finer than the unit of storage.

Two consequences:

1. **Redaction must rewrite the literal.** `vcfp:maskVectorPositions` replaces
   the masked participant's token with `.`, which is the VCF missing marker and
   keeps the vector aligned with `vcfr:sampleIndex` — alignment the format
   depends on. Dropping a position instead would silently shift every downstream
   sample's value, which is a data-corruption bug wearing a privacy feature's
   clothing.
2. **Masking is itself visible.** A `.` where neighbours have values discloses
   that a value existed and was withheld, and combined with `SampleSet`
   membership it discloses *whose*. Where that matters, the policy must mask the
   position across **all** samples for that record, or drop the vector entirely.
   The compiler should detect a single-position mask and warn — or, under a
   strict profile setting, refuse.

A blunter option is worth offering: `vcfp:requireRepresentation expanded`, which
makes a policy refuse to run against a condensed graph. For policies with
per-sample rules that is often the honest answer, and it trades storage
efficiency for enforceability deliberately rather than by accident.

---

## 9. Beyond access control: disclosure limitation

Access control does not address §1's attacks, all of which operate on data the
recipient was *permitted* to see. Three mechanisms, in increasing ambition:

**Count thresholds (`vcfp:threshold`).** Suppress a variant whose allele count
falls below *k*. Cheap, well understood, and directly targets the
rare-variant-as-fingerprint problem, since rare alleles carry most of the
identifying signal. Implementable in Tier 2 with a counting pre-pass.
Recommended default for any cohort release: suppress `AC < 5`, and say so in the
manifest.

**Aggregate-only exposure (`vcfp:aggregateOnly`).** The subgraph is not
materialized; only counting queries over it are answerable. This is the Beacon
model, and it inherits the Beacon model's known weakness — repeated membership
queries leak — so it must be paired with query budgeting and audit, not offered
as a safe default.

**Differential privacy.** Noise on aggregate counts is the only mechanism here
with a formal guarantee, and it is also the easiest to implement incorrectly:
a per-query epsilon with no global budget provides no protection at all against
a patient adversary. **Not proposed for implementation.** If it is added later
it needs a persistent budget ledger per recipient, and the design should say
plainly that without one it is decoration.

---

## 10. The release manifest

Every policy-derived artifact carries provenance, in the same spirit as the
linkset node in [`datalinking-design.md`](datalinking-design.md#5-output-and-provenance):

```turtle
<file://cohort-release.nt#release>
    a vcfp:ReleaseView ;
    vcfp:derivedFrom      <file://cohort.vcf> ;
    vcfp:policy           <https://example.org/policy/cohort-release/1.2> ;
    vcfp:policyDigest     "sha256:9f2c…" ;
    vcfp:duoVersion       "2024-11-03" ;
    vcfp:pseudonymKeyId   "release-2026-09-key-3" ;
    vcfp:representation   vcfr:ExpandedRepresentation ;
    vcfp:triplesWithheld  1840221 ;
    vcfp:samplesWithheld  2 ;
    vcfp:thresholdApplied 5 ;
    prov:generatedAtTime  "2026-09-04T11:04:22Z"^^xsd:dateTime .
```

Three requirements that are easy to get wrong:

- **The policy is referenced by digest, not just by IRI.** A policy IRI whose
  content changed later cannot explain an artifact produced last year.
- **A release view gets its own IRI namespace**, so it can never be silently
  mistaken for, or merged with, the full graph.
- **Counts of what was withheld are published.** They are not themselves
  sensitive at this granularity, and they are what makes a release auditable
  rather than merely asserted.

---

## 11. Verifying a release

A privacy claim that is not checked is a privacy claim that is wrong. This is
where the existing validation harness earns its keep a second time.

**Policy conformance checks.** Compile every prohibition into a SPARQL `ASK` (or
a `SELECT COUNT`) that must return zero over the released artifact, and run them
as preflights, exactly as `preflight_blank_nodes` and friends already work. A
release that fails one is not published. This turns "the APOE region was
excluded" from a claim into a measurement — the distinction
[`validation-methodology.md`](validation-methodology.md) is built around.

**SHACL for shape-level guarantees.** "No `vcfr:sampleName` appears anywhere" is
naturally a shape constraint, and the `--shacl-shapes` layer already exists.

**Mutation testing the redactor.** Add a mutation class to
[`test/validation_mutations.py`](../test/validation_mutations.py) that
deliberately breaks the redaction — reinstate a withheld sample, un-mask one
vector position, restore a dropped predicate — and assert the conformance checks
catch it. A redactor nobody has tried to defeat is an assumption, not evidence,
which is precisely the argument the validation methodology already makes about
the validator itself.

**Adversarial checks worth writing early**, because they catch the leaks that
correct-looking implementations produce anyway:

- Does any IRI in the released graph contain a withheld sample id?
- Do row identifiers reconstruct the source ordering?
- Is any masked vector position identifiable as masked-for-one-sample (§8)?
- Does the header section still name a withheld participant?

---

## 12. Authoring tooling

`vcf-rdfizer-policy`, mirroring `vcf-rdfizer-rules` and the proposed
`vcf-rdfizer-link`:

| Command | Purpose |
| --- | --- |
| `policy init -o p.ttl` | Scaffold with annotated examples and a default-deny skeleton |
| `policy check p.ttl` | Validate against the profile: unknown selectors, unresolvable DUO terms, assembly declared, conflicts, and **any rule no tier can enforce** |
| `policy explain p.ttl` | Render the resolved release plan in English: what is dropped, degraded, thresholded, and what remains |
| `policy dry-run p.ttl -i sample.vcf` | Report counts that *would* be withheld, per rule, writing nothing |
| `policy diff p1.ttl p2.ttl` | What changes between two policy versions — the question a data access committee actually asks |

`explain` is the one that earns its keep. A policy nobody can read is a policy
nobody can approve, and the failure mode is approval-by-exhaustion.

---

## 13. Build order

| Step | Delivers | Unlocks |
| --- | --- | --- |
| 1 | `vcfp:` profile, policy parsing, `Predicate`/`Class`/`Sample`/`Header` selectors, `vcfp:drop`, default-deny, conflict rules | Coarse governed release with no new machinery |
| 2 | Tier 1 (TSV) + Tier 2 (emitter) enforcement, `CHROM`/`POS` on `ParsedSampleRecord`, `RegionSelector` | Region and field granularity at no runtime cost |
| 3 | Release manifest, policy conformance preflights, mutation entries | Releases become auditable and verified rather than asserted |
| 4 | `vcf-rdfizer-policy` CLI, `explain`, `diff` | Policies become reviewable by non-implementers |
| 5 | `vcfp:pseudonymize` with IRI re-minting, `maskVectorPositions`, `threshold` | The leaks in §7 and §8 closed |
| 6 | `--mode redact` (Tier 3), then optional query-time rewriting | Existing artifacts, and endpoint deployments |

Steps 1–3 are the contract and the evidence. Note that step 3 comes *before* the
sophisticated transforms in step 5 — deliberately. A crude redaction that is
verified is worth more than a sophisticated one that is not.

---

## 14. Known hard problems

Stated up front, as in the linking design.

**This is not anonymization, and the vocabulary must not drift.** Every user-
facing string, manifest field and log line should say *pseudonymized*,
*withheld*, or *governed release*. The moment the tool says "anonymized", it is
making a claim it cannot support (§1), and someone will rely on it.

**Compliance is not a feature.** GDPR special-category processing, national
genomic-data law, and the scope of a specific consent form are determinations
for a data protection officer and a data access committee. The tool can
*implement* a policy and *evidence* what it did. It cannot decide what the
policy should be, and the documentation must not imply otherwise.

**Structural leakage survives redaction.** A withheld region still shows as a
gap; a withheld sample still shows as a missing `SampleSet` member unless the
set itself is rewritten; a thresholded variant still shows as absent. Whether
those gaps matter is a policy question, but the tool should surface them —
`policy explain` is the right place.

**Composition across releases is unmanaged.** Two separately compliant releases
to the same recipient can jointly disclose more than either alone: different
regions, different thresholds, or a fresh pseudonym key that is nonetheless
linkable through the genotypes. Nothing in this design tracks cumulative
disclosure, and the manifest is the only thread that would make it possible
later. Say so rather than implying releases are independent.

**Policy drift versus artifact.** A policy IRI resolves to whatever it resolves
to *today*; the artifact was produced under what it said *then*. The digest in
§10 is what makes that recoverable, and it is why it is mandatory rather than
nice to have.

**The escape hatch is a footgun.** `vcfp:PatternSelector` accepts arbitrary
SPARQL, which cannot be pushed into Tier 1 or 2 and whose cost is unbounded. It
should be permitted, reported prominently by `explain`, and excluded from any
profile intended for unattended use.

---

## See also

- [Data linking design](datalinking-design.md) — the sibling proposal; same plugin, provenance and verification patterns
- [Conversion](conversion.md) — the IRI templates and graph shapes a policy addresses
- [Sample representations](sample-representation-guide.md) — why condensed mode changes the enforceability of per-sample rules
- [Validation methodology](validation-methodology.md) — the harness that makes a privacy claim measurable
- [Limitations](limitations.md) — what the tool does not do today
- [Roadmap](roadmap.md) — how this relates to the other planned work
