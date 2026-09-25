# Policy attachment — v0.1.0

*Part of the [VCF-RDFizer documentation](README.md). Status: **implemented
(v0.1.0)**; walkthrough in [`examples/policy/`](../examples/policy/README.md);
tests in [vcf-rdfizer-testing `plugin-tests/policy/`](https://github.com/ecrum19/vcf-rdfizer-testing/tree/main/plugin-tests).
This is the first slice of [`privacy-policy-design.md`](privacy-policy-design.md):
it uses that document's vocabulary and rules, implements a subset of them, and
states plainly what it leaves out.*

`vcf-rdfizer-policy` attaches ODRL policies to an RDF graph and produces one
release view per request. It is built from three generic steps, and each is
configured in Turtle, not code:

1. **Select.** A rule's target is either one resource IRI or a *selection*. A
   selection is computed by a selector type, declared as a SPARQL `SELECT`.
2. **Partition.** A *profile* says what a selected resource owns, so that
   withholding a record also withholds its call, its alleles and its genotypes.
3. **Decide.** ODRL semantics: a binding permission must own a resource, no
   binding prohibition may, and deny wins.

The bundled **VCF Core profile** makes this work on graphs written by
VCF-RDFizer, with region and variant selectors. Anyone can add a selector, such
as FILTER state, QUAL, an INFO key or genes from a linkset, by declaring it.
The same engine partitions any other RDF graph with a different profile.

This is **governed release, not anonymization**, and every string the tool
emits says so. See [`privacy-policy-design.md` §1](privacy-policy-design.md#1-the-uncomfortable-premise).

---

## 1. Why

Consent for genomic data is recorded per dataset. A repository record carries
one data-use code, and VCF itself has no place to state a policy at all. Real
policies are finer than that:

- *this participant* consented to health research but not to clinical use;
- *this region* holds actionable incidental findings, readable only for
  clinical care;
- *this variant* is restricted to disease-specific research;
- *this participant* withdrew, and that overrides everything else.

An RDF graph can hold every one of those statements next to the data it
governs, in standard vocabularies: ODRL for the rules, GA4GH DUO for the
purposes, PROV for what was released. It can also answer questions that mix
the two (§6). A VCF-based workflow would need an external spreadsheet, and its
enforcement would live in someone's scripts.

---

## 2. How it works

```text
policy.ttl ──► rules ─────────────────────────────┐
                 │ target                          │ kind, assignee, purpose constraints
                 ▼                                 ▼
           ┌───────────┐   resources   ┌───────────┐   owned sets   ┌──────────┐
graph ────►│  select   │──────────────►│ partition │───────────────►│  decide  │──► view, decisions,
           └───────────┘               └───────────┘                └──────────┘    manifest
             selector types              ownership rule                ODRL, deny wins
             (SPARQL, profile)           (profile)                     (engine.py)
```

| Layer | What it is | Where |
| --- | --- | --- |
| Engine | Select, partition, decide; knows only ODRL, selector declarations, ownership rules and a purpose hierarchy. It never mentions VCF | `vcf_rdfizer_policies/engine.py` |
| Profile | Selector types, the ownership rule, and the reporting-unit query, in Turtle | `vcf_rdfizer_data/policy/vcf-core-profile.ttl` (bundled), or `--profile` |
| Purposes | An RDFS or SKOS hierarchy | `vcf_rdfizer_data/policy/duo-subset.ttl` (bundled), or `--purposes` |
| VCF oracle | An independent check of a view against the source VCF text (§7) | `vcf_rdfizer_policies/vcf_oracle.py` |

### 2.1 Selector types

A selector type is a SPARQL `SELECT` that projects `?resource`, plus the
parameters a policy supplies. Each `vcfp:parameter` is a property the
selector node must carry, and its value is bound to the query variable named
after the property's local name: `vcfp:start` binds `?start`. An optional
`vcfp:violations` query lists reasons the selector cannot apply to a graph.
Any row it returns stops evaluation; the VCF Core selectors use it to require
that every file declares the policy's assembly.

This is the shipped region selector, in full:

```turtle
vcfp:RegionSelector a vcfp:SelectorType ;
    vcfp:parameter vcfp:assembly , vcfp:chrom , vcfp:start , vcfp:end ;
    vcfp:query """
        PREFIX vcfc: <https://w3id.org/vcf-core/vocab#>
        SELECT ?resource WHERE {
            ?resource a vcfc:VCFRecord ; vcfc:chrom ?c ; vcfc:pos ?pos .
            FILTER(STR(?c) = STR(?chrom) && ?pos >= ?start && ?pos <= ?end) }""" ;
    vcfp:violations """ … files whose vcfc:referenceGenome is not ?assembly … """ .
```

A policy uses it like this:

```turtle
ex:brca1 a odrl:Asset , vcfp:GraphSelection ;
    vcfp:selector [ a vcfp:RegionSelector ; vcfp:assembly "GRCh38" ;
                    vcfp:chrom "chr17" ; vcfp:start 43044295 ; vcfp:end 43125483 ] .
```

Selector types are read from the profile files, and also from the policy file
itself, so a policy can bring its own (§8).

### 2.2 Partitioning

A withheld resource takes with it everything it **owns**:

- the resources its `vcfp:ownershipPath` (a SPARQL property path) reaches;
- when `vcfp:iriSubtree` is true, every IRI beneath any of those, after a `#`
  or `/`.

The VCF Core profile sets the path to `vcfc:hasCall/vcfc:hasSampleCall?` and
turns the subtree rule on, which matches the converter's IRIs
([conversion §6](conversion.md#6-iri-templates)):

- a file owns everything under `file://NAME#`: its header, sample set and
  records;
- a record `…#record/9` owns its alleles (`…#record/9/allele/…`), its call
  `…#call/9` with that call's INFO values and condensed matrix, and, through
  the call, its expanded per-sample calls `…#sample/9/P001`.

A triple is released when its subject is released, and its object too if the
object is a node of the graph. So no view ever points at something it doesn't
contain.

### 2.3 Reporting units

`vcfp:unitQuery` names what the per-unit report counts. It must project
`?resource` and `?group`, and any other variables become report columns. The
VCF Core profile reports records grouped by file, with chrom, pos, ref and alt.

---

## 3. Policies

Namespace `vcfp:` = `https://w3id.org/vcf-rdfizer/policy#`.

| Construct | Supported | Notes |
| --- | --- | --- |
| `odrl:Set` / `Policy` / `Offer` / `Agreement` | yes | |
| `odrl:permission`, `odrl:prohibition` | yes | |
| `odrl:action odrl:read` | yes | The only action evaluated |
| `odrl:target` = a resource IRI | yes | The resource and everything it owns, e.g. `<file://P003.vcf>` |
| `odrl:target` = a `vcfp:GraphSelection` | yes | One `vcfp:selector`, whose type the profile or policy declares |
| `odrl:assignee` | yes | An IRI, or `odrl:All` |
| `odrl:constraint` on `odrl:purpose`, `isAnyOf` / `isNoneOf` | yes | Terms of the purpose vocabulary (§3.1) |
| `odrl:duty` | recorded | Copied into the manifest as obligations. **Not enforced** |
| `odrl:conflict odrl:prohibit` | required | Deny wins; any other value is refused |
| `vcfp:transform vcfp:drop` | yes | The only effect |
| anything else on a rule (`odrl:refinement`, `odrl:remedy`, …) | **refused** | It could change what the rule means |
| `generalize`, `pseudonymize`, `threshold`, `maskVectorPositions` | **refused** | Later versions (§10) |

**What the engine cannot evaluate, it refuses.** A missing parameter, an
undeclared selector type, a purpose outside the vocabulary, a selector whose
`violations` query returns rows: each stops the run with an error, never a
warning. A tool that silently skipped a rule would demonstrate the opposite of
its purpose.

### 3.1 Purposes

A requester's purpose satisfies a term when it **is that term or a narrower
one**, following `rdfs:subClassOf` or `skos:broader`. So any RDFS or SKOS
vocabulary works, passed with `--purposes`. Names resolve as full IRIs or
through the vocabulary's own prefixes.

The default is four GA4GH DUO terms (`duo-subset.ttl`), copied from DUO release
2021-02-23 together with their subclass links:

| Code | IRI | In the example |
| --- | --- | --- |
| GRU, general research use | `DUO:0000042` | P001, P002 consent; the general-research requester |
| HMB, health/medical/biomedical research | `DUO:0000006` | P003 consent |
| DS, disease-specific research | `DUO:0000007` | P005 consent; the Alzheimer's study |
| CC, clinical care use | `DUO:0000043` | P001, P002 consent; the clinical requester |

DS ⊑ HMB ⊑ GRU, so a disease-specific study falls within a general-research
consent, but not the reverse. DUO files CC as a data use *modifier*, not a
permission. Here it is a purpose like the others, and matches only itself.

---

## 4. Decision rules (normative)

A **request** is `(assignee a, purpose p)`. A rule *binds* a request when its
assignee is `odrl:All` or `a`, and each constraint holds:

- `isAnyOf S` holds when `p ⊑ s` for some `s` in `S`;
- `isNoneOf S` holds when `p ⊑ s` for none.

For each binding rule, the engine selects its target's resources (§2.1) and
takes everything they own (§2.2).

**A resource is released if and only if:**

1. some binding **permission** owns it — the default-deny rule: no
   permission, no release; **and**
2. no binding **prohibition** owns it — deny wins.

A permission on a file and a prohibition on a region inside it combine as
"everything in the file except the region". No "most-specific target wins"
rule is needed, which is fortunate, since ODRL does not define one.

---

## 5. The example: one cohort, three requesters

`examples/policy/` holds five synthetic participants, one small single-sample
VCF each: 23–34 records, 139 in all. The positions are GRCh38 and fall in real
loci: *BRCA1* (chr17:43,044,295–43,125,483), *APOE* (including the real
rs429358 T>C and rs7412 C>T), and background sites on chr1 and chr20.

Every other allele, and every genotype, is synthetic, drawn from a fixed seed.
The design's first premise is that genotypes identify people, so a privacy
example should not be built on real ones.

The generator also plants boundary cases:
- records one base inside and one base outside each end of the *BRCA1* window;
- a decoy T>G at rs429358's position, to test that the variant rule matches
  alleles and not just position.

Single-sample files make each participant's consent a policy on their own
file, and a withdrawal the withholding of one file. They also mean the
condensed profile's per-sample vectors have one entry each, so nothing ever
has to be masked *inside* a literal.

`policy.ttl` holds:
- five consents, one per file (P004's includes the withdrawal, a prohibition);
- a cohort policy with two prohibitions, *BRCA1* unless the purpose is CC and
  rs429358 unless the purpose is DS.

| Requester | Purpose | P001 | P002 | P003 | P004 | P005 | *BRCA1* | rs429358 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| General-research consortium | GRU | 21 | 16 | — | — | — | withheld | withheld |
| Alzheimer's consortium | DS | 22 | 16 | 18 | — | 20 | withheld | **released** |
| Clinical genetics lab | CC | 33 | 23 | — | — | — | **released** | withheld |

The numbers are records released from each file. Every cell follows from §4:
- P003's HMB consent admits DS but not GRU;
- P004's withdrawal is a prohibition, so it wins everywhere;
- each cohort rule is lifted for exactly one purpose.

Released records total 37, 76 and 56 of 139, with 7,869, 4,649 and 6,539
triples withheld in the expanded profile. The condensed profile releases the
same records.

---

## 6. What it writes

### 6.1 `attach`: policies in the graph

`attach` merges the policies into the data. It links each directly targeted
resource, and each resource a selection selects, to its policy:

```turtle
<file://P003.vcf>           odrl:hasPolicy ex:consent-P003 .
ex:brca1                    vcfp:selects   <file://P001.vcf#record/10> , … .
<file://P001.vcf#record/10> odrl:hasPolicy ex:cohort .
```

Policy and data are then one graph, and one query asks which records a
general-research collaborator may not receive, and under which rule. It needs
no knowledge of the selectors:

```sparql
PREFIX odrl: <http://www.w3.org/ns/odrl/2/>
PREFIX vcfc: <https://w3id.org/vcf-core/vocab#>
SELECT ?record ?chrom ?pos ?rule WHERE {
  ?record a vcfc:VCFRecord ; vcfc:chrom ?chrom ; vcfc:pos ?pos ; odrl:hasPolicy ?policy .
  ?policy odrl:prohibition ?rule . }
```

### 6.2 `evaluate`: one request's release

| File | Contents |
| --- | --- |
| `view.nt` | The released triples, sorted |
| `decisions.csv` | One row per reporting unit: the unit query's columns, released or not, and the rule that decided it |
| `summary.json` | Counts per group and per deciding reason |
| `manifest.ttl` | The `vcfp:ReleaseView`, below |

```turtle
<#release> a vcfp:ReleaseView ;
  vcfp:derivedFrom     <file://P001.vcf> , … , <file://P005.vcf> ;
  vcfp:policy          ex:cohort , ex:consent-P001 , … ;
  vcfp:policyDigest    "sha256:…" ;
  vcfp:request         [ odrl:assignee <https://example.org/party/alz-consortium> ;
                         odrl:purpose  obo:DUO_0000007 ] ;
  vcfp:recordsReleased 76 ; vcfp:recordsWithheld 63 ;
  vcfp:groupsWithheld  1 ;  vcfp:triplesWithheld 4649 ;
  vcfp:obligation      [ odrl:action odrl:attribute ] ;
  vcfp:disclosureModel "governed release; not anonymization" ;
  prov:wasGeneratedBy  <urn:vcf-rdfizer-policy:0.1.0> ;
  prov:generatedAtTime "…"^^xsd:dateTime .
```

The policy is recorded by digest as well as by IRI, since an IRI's content can
change after the fact. Views keep the source IRIs, as there is no
pseudonymization yet (§10), and `vcfp:disclosureModel` says so.

---

## 7. Checking a view

`vcf-rdfizer-policy check --view DIR --rdf SOURCE --policy P [--vcf …]` fails
on each of the following.

**Structural checks, for any graph:**
1. The manifest's policy digest doesn't match the policy.
2. Anything a binding prohibition owns appears in the view, as subject or
   object.
3. A subject in the view is owned by no binding permission (default-deny).
4. A triple points at a node of the source that the view does not contain.

**The VCF oracle (`--vcf`):** it reads the source VCFs as text and builds a
minimal VCF Core graph of the fixed columns except INFO: CHROM, POS, ID, REF,
ALT, QUAL and FILTER. It then evaluates the same policy on that graph. The
view's records must equal the records released there **exactly**: an extra
record is a leak, and a missing one is over-withholding.

The structural checks reuse the engine's selectors, so they confirm that a
view honours the policy, but they cannot catch a wrong selector. The oracle
can, because its input never went through the converter. A selector that
reads INFO, FORMAT or the header is outside what the oracle models, so don't
pass `--vcf` for such a policy.

**The checks earned their place early.** In development, a withdrawn file's
header and sample set stayed in every view even though every record decision
was right, so nothing looked wrong from the counts. The check named the leaked
IRI. That failure is why the partition is defined by ownership (§2.2) rather
than by a list of record subtrees.

**The tests** are in
[vcf-rdfizer-testing `plugin-tests/policy/`](https://github.com/ecrum19/vcf-rdfizer-testing/tree/main/plugin-tests),
kept apart from this code and run by hand. They cover:
- the §5 grid cell by cell, in both profiles;
- the refusals;
- the generality cases: a selector declared in a policy, a non-VCF graph with
  a property-path partition, a SKOS vocabulary;
- mutation tests, where each planted fault must fail `check`: a reinstated
  prohibited record, a restored triple of the withdrawn file, a deleted
  record, a dangling reference, a swapped view, and a changed policy.

---

## 8. Extending it without code

**A new selector.** Declare a `vcfp:SelectorType`, either in the policy file
or in a file passed with `--profile`.
[`examples/policy/custom-selector.ttl`](../examples/policy/custom-selector.ttl)
adds "records whose QUAL is below a threshold" as one short declaration, and
withholds 38 of the example's 139 records at a threshold of 60. Selectors for
FILTER state, an INFO value, a sample (for multi-sample files), or genes from a
linkset all follow the same pattern.

**Another kind of graph.** Pass a profile with a different ownership rule and
unit query. For a graph of documents that own their sections through
`ex:hasSection`, with opaque IRIs:

```turtle
ex:Docs a vcfp:Profile ;
    vcfp:ownershipPath "<https://example.org/hasSection>*" ;
    vcfp:iriSubtree false ;
    vcfp:unitQuery "SELECT ?resource ?group WHERE { ?group <https://example.org/hasDoc> ?resource }" .
```

**Other purposes.** Pass `--purposes` a vocabulary: full DUO, or a local SKOS
scheme.

---

## 9. The command

```bash
vcf-rdfizer-policy explain  --policy policy.ttl
vcf-rdfizer-policy attach   --rdf P00*.nt.gz --policy policy.ttl -o annotated.nt
vcf-rdfizer-policy evaluate --rdf P00*.nt.gz --policy policy.ttl \
                            --assignee https://example.org/party/alz-consortium --purpose DUO:0000007 -o views/alz
vcf-rdfizer-policy check    --view views/alz --rdf P00*.nt.gz --policy policy.ttl --vcf P00*.vcf
```

Every subcommand takes:
- `--profile`, repeatable; a file or `vcf-core` (the default);
- `--purposes`, the purpose vocabulary.

The command runs on the host and needs `rdflib`, not Docker. It evaluates in
memory, and refuses graphs over 5M triples.

| Exit code | Meaning |
| --- | --- |
| 0 | Success; for `check`, every check passed |
| 1 | `check` found a failure |
| 2 | The policy, profile, graph or request cannot be evaluated |

```text
vcf_rdfizer_policy.py         the command
vcf_rdfizer_policies/
  engine.py                   select, partition, decide -- generic
  policy.py                   ODRL -> rules; refuses what it cannot evaluate
  profile.py                  selector types and the ownership rule, from Turtle
  vocabulary.py               purpose hierarchies (RDFS / SKOS)
  release.py                  evaluate, attach, manifest
  check.py                    the structural checks -- generic
  vcf_oracle.py               the VCF-text oracle
  graphs.py                   loading, IRI hierarchy
vcf_rdfizer_data/policy/
  vcf-core-profile.ttl        the VCF Core profile: region and variant selectors, ownership, units
  duo-subset.ttl              the default purpose vocabulary
  vcfp-0.1.ttl                the profile terms
examples/policy/              the cohort, policy.ttl, custom-selector.ttl, run_demo.sh
```

---

## 10. Beyond v0.1.0

Mapped onto the full design's build order
([`privacy-policy-design.md` §13](privacy-policy-design.md#13-build-order)).
Selectors are now declarations, so the full design's sample, field, header and
class selectors are Turtle, not engine work. What remains needs code:

| Version | Adds | Design § |
| --- | --- | --- |
| **v0.2** | Multi-sample files: a sample selector (a declaration) for expanded graphs, and `maskVectorPositions` for condensed ones, which rewrites a literal and so needs code; the `generalize` effect (genotype → carrier status) | §4.2, §8 |
| **v0.3** | Enforcement during conversion (TSV and emitter tiers), and a streaming evaluator, lifting the size limit | §5 |
| **v0.4** | Pseudonymization: IRI re-minting with per-release keys | §7 |
| **v0.5** | Full DUO with release pinning and MONDO qualifiers; `policy diff`; enforced duties with an audit sink | §4.3, §12 |
| later | `threshold`; query-time rewriting for an operated endpoint | §5, §9 |

Two rules carry forward unchanged:

- **What cannot be enforced stops the run.** A version may widen what can be
  enforced, but never turn that error into a warning.
- **Every version ships with its checks and mutation tests.** "A crude
  redaction that is verified is worth more than a sophisticated one that is
  not."

---

## See also

- [Privacy policy design](privacy-policy-design.md) — the full design this is a slice of
- [Data linking](datalinking.md) — the sibling plug-in; the same manifest and provenance pattern
- [Conversion §6](conversion.md#6-iri-templates) — the IRIs the VCF Core ownership rule follows
- [Validation methodology](validation-methodology.md) — why a policy claim needs an oracle
