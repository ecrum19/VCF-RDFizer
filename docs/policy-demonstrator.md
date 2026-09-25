# Policy attachment demonstrator — v0.1.0

*Part of the [VCF-RDFizer documentation](README.md). Status: **design, not yet
implemented.** This is the first, deliberately small slice of
[`privacy-policy-design.md`](privacy-policy-design.md): it uses that document's
vocabulary and rules, implements a subset of them, and states plainly what it
leaves out.*

v0.1.0 shows three things on a small cohort of synthetic single-sample VCFs:

1. **Attach.** ODRL policies are attached to files, genomic regions and
   individual variants, and they sit in the same graph as the data, so a
   single SPARQL query can ask about both.
2. **Evaluate.** One set of policies gives each requester a different release
   view, according to who they are and the purpose they state.
3. **Verify.** Each view is checked against an independent oracle computed
   from the source VCFs. It must withhold exactly what the policy says: no
   more and no less.

The rest of this document specifies the fixture, the subset of the profile,
the decision semantics, the command, the tests and what goes into the paper,
and it ends with the plan for later versions.

It is **governed release, not anonymization**, and every string the tool emits
must say so. See [`privacy-policy-design.md` §1](privacy-policy-design.md#1-the-uncomfortable-premise).

---

## 1. Why this is worth demonstrating

Today, consent for genomic data is attached at the level of a whole dataset. A
repository record carries one data-use code, and VCF itself has no place to
state a policy at all. Real policies are finer than that:

- *this participant* consented to health research but not to clinical use;
- *this region* holds actionable incidental findings and may only be read for
  clinical care;
- *this variant* is sensitive enough to be restricted to disease-specific
  research;
- *this participant* withdrew, and that overrides everything else.

An RDF graph can hold every one of those statements next to the data it
governs, in standard vocabularies (ODRL for the rules, GA4GH DUO for the
purposes, PROV for what was released), and it can answer questions that mix
the two. A VCF-based workflow would need an external spreadsheet, and its
enforcement would live in someone's scripts. The demonstrator makes that
difference concrete, and it measures it rather than just asserting it.

---

## 2. The fixture: a cohort of single-sample files

Five synthetic participants, **one small VCF each**:

| File | Participant | Consent (file-level policy) |
| --- | --- | --- |
| `P001.vcf` | P001 | General research use **and** clinical care use |
| `P002.vcf` | P002 | General research use **and** clinical care use |
| `P003.vcf` | P003 | Health/medical/biomedical research only |
| `P004.vcf` | P004 | **Withdrawn**: prohibited for every requester |
| `P005.vcf` | P005 | Disease-specific research only |

Each file is `VCFv4.3` with `##reference=GRCh38`, about 40 records, and one
sample column. The records sit at real GRCh38 positions in three groups:

| Group | Where | Why |
| --- | --- | --- |
| *BRCA1* | chr17:43,044,295–43,125,483 | Actionable incidental findings; the region rule applies here |
| *APOE* | chr19, including rs429358 (44,908,684 T>C) and rs7412 (44,908,822 C>T) | rs429358 defines the ε4 allele; the variant rule applies to it alone, and rs7412 is its unrestricted neighbour |
| Background | a few loci on chr1 and chr20 | Governed only by the file-level consent |

**The genotypes are synthetic and generated from a fixed seed.** The design
doc's first premise is that genotypes identify people, and a privacy
demonstrator should not be built on real individuals. Positions and alleles
are real, so the region and variant selectors are tested against real
coordinates. Everything else is invented.

The generator is `examples/policy/make_fixture.py`. It is deterministic, and
it writes the five VCFs together with a `fixture.json` that records the seed,
the loci and each file's intended consent.

**Why single-sample files help:**

- **Consent is per participant, so it becomes a file-level policy.** The
  policy travels with the participant's file, which is how consent actually
  works.
- **Withdrawing a participant means withholding one file.** No per-sample
  surgery inside a shared graph is needed.
- **Both sample profiles work.** In the condensed profile, a record's sample
  vector has exactly one entry, so withholding a record removes whole
  literals and never has to rewrite one. The masking problem in
  [`privacy-policy-design.md` §8](privacy-policy-design.md#8-the-condensed-representation-problem)
  does not arise. Multi-sample files, where it does, are v0.2 (§10).
- **The cohort is a union of graphs.** Each file's IRIs start with
  `file://P00n.vcf`, so the five graphs merge without collisions (see
  [conversion §6](conversion.md#6-iri-templates)). Region and variant rules
  are then cohort-wide rules that cut across files.

---

## 3. The profile subset

Namespace `vcfp:` = `https://w3id.org/vcf-rdfizer/policy#`, as in the full
design. v0.1.0 implements exactly this:

| Construct | v0.1.0 | Notes |
| --- | --- | --- |
| `odrl:Set` / `odrl:Policy` | yes | One policy per participant, plus one cohort policy |
| `odrl:permission`, `odrl:prohibition` | yes | |
| `odrl:target` = a file IRI | yes | The file resource *is* the asset; no selector is needed |
| `odrl:target` = `vcfp:GraphSelection` with `vcfp:RegionSelector` | yes | `vcfp:assembly` is required and checked against `vcfc:referenceGenome` |
| `odrl:target` = `vcfp:GraphSelection` with `vcfp:VariantSelector` | yes, **new** | chrom, pos, ref, alt and assembly. The full design has no mutation-level selector; this adds one |
| `odrl:assignee` | yes | An IRI, or `odrl:All` |
| `odrl:constraint` on `odrl:purpose` with `odrl:isAnyOf` / `odrl:isNoneOf` | yes | Right-hand sides are DUO terms, matched through the DUO hierarchy (§3.1) |
| `odrl:action odrl:read` | yes | The only action evaluated |
| `odrl:duty` (`odrl:attribute`, `odrl:inform`) | recorded | Copied into the release manifest as obligations the requester accepts. **Not enforced** |
| `odrl:conflict odrl:prohibit` | yes, required | Deny wins; any other value is rejected |
| `vcfp:transform vcfp:drop` | yes | The only effect |
| `vcfp:SampleSelector`, `FieldSelector`, `HeaderSelector`, `ClassSelector`, `PredicateSelector`, `PatternSelector` | **no** | v0.2 and later (§10). A policy that uses one is rejected, not ignored |
| `generalize`, `pseudonymize`, `threshold`, `aggregateOnly`, `maskVectorPositions` | **no** | Later versions. Rejected if present |

**Anything v0.1.0 cannot evaluate stops the run with an error.** That applies
to unknown selectors, unsupported effects, missing assemblies, and assemblies
that don't match the data. This is the design doc's "the residual set must
abort" rule (§6), and it matters most in the smallest version: a demonstrator
that silently skipped a rule would demonstrate the opposite of its purpose.

### 3.1 Purposes and the DUO hierarchy

Purposes are GA4GH Data Use Ontology terms:

| Code | IRI | Role in the fixture |
| --- | --- | --- |
| GRU, general research use | `obo:DUO_0000042` | P001, P002 consent; the general-research requester |
| HMB, health/medical/biomedical research | `obo:DUO_0000006` | P003 consent |
| DS, disease-specific research | `obo:DUO_0000007` | P005 consent; the Alzheimer's consortium's purpose |
| CC, clinical care use | `obo:DUO_0000043` | P001, P002 consent; the clinical requester |

Matching uses subsumption. **A requester's purpose satisfies a consent when
the purpose is the consented term or a narrower one**, so a disease-specific
study falls within a general-research consent, but not the other way round.
v0.1.0 bundles the four terms and their `rdfs:subClassOf` links as
`vcf_rdfizer_data/policy/duo-subset.ttl`, with the DUO release they were taken
from recorded in the file. Full DUO, with disease qualifiers via MONDO and
release pinning in the policy (`vcfp:duoVersion`), is later (§10).

---

## 4. Decision semantics (normative for v0.1.0)

A **request** is `(assignee a, purpose p)`. The unit of decision is the
**record**. File-level resources (the header, the file, the sample set) are
decided with their file.

A rule *applies* to a request when its assignee is `odrl:All` or equals `a`,
and every constraint holds for `p`:

- `purpose isAnyOf S` holds when `p ⊑ s` for some `s` in `S`;
- `purpose isNoneOf S` holds when `p ⊑ s` for no `s` in `S`.

A rule *covers* a record when its target selects the record: the record's
file, a region containing the record's `vcfc:pos` on its `vcfc:chrom`, or a
variant matching its chrom, pos, ref and alt.

**Record `r` in file `f` is released to request `q` if and only if:**

1. some **permission** that applies to `q` covers `f` — the default-deny
   rule: no permission, no release; **and**
2. no **prohibition** that applies to `q` covers `r` or `f` — deny wins.

A file's own resources are released when (1) and (2) hold for the file
itself. A released triple must have both its subject and any IRI object
released, so no triple in a view points at something the view withholds.

The fixture's cohort policy has two rules, and both are prohibitions with an
exception. That keeps the semantics above complete without needing
"most-specific target wins", which ODRL doesn't define:

```turtle
@prefix odrl: <http://www.w3.org/ns/odrl/2/> .
@prefix vcfp: <https://w3id.org/vcf-rdfizer/policy#> .
@prefix obo:  <http://purl.obolibrary.org/obo/> .

<#brca1> a odrl:Asset , vcfp:GraphSelection ;
  vcfp:selector [ a vcfp:RegionSelector ; vcfp:assembly "GRCh38" ;
                  vcfp:chrom "chr17" ; vcfp:start 43044295 ; vcfp:end 43125483 ] .

<#apoe-e4> a odrl:Asset , vcfp:GraphSelection ;
  vcfp:selector [ a vcfp:VariantSelector ; vcfp:assembly "GRCh38" ;
                  vcfp:chrom "chr19" ; vcfp:pos 44908684 ; vcfp:ref "T" ; vcfp:alt "C" ] .

<#cohort-policy> a odrl:Set ;
  odrl:uid      <https://example.org/policy/demo-cohort/0.1> ;
  odrl:profile  <https://w3id.org/vcf-rdfizer/policy> ;
  odrl:conflict odrl:prohibit ;
  # Incidental findings: readable only for clinical care.
  odrl:prohibition [ odrl:target <#brca1> ; odrl:action odrl:read ; odrl:assignee odrl:All ;
      odrl:constraint [ odrl:leftOperand odrl:purpose ; odrl:operator odrl:isNoneOf ;
                        odrl:rightOperand obo:DUO_0000043 ] ] ;
  # APOE e4: readable only for disease-specific research.
  odrl:prohibition [ odrl:target <#apoe-e4> ; odrl:action odrl:read ; odrl:assignee odrl:All ;
      odrl:constraint [ odrl:leftOperand odrl:purpose ; odrl:operator odrl:isNoneOf ;
                        odrl:rightOperand obo:DUO_0000007 ] ] .

# A participant's consent, attached to their file.
<#consent-P003> a odrl:Set ;
  odrl:uid      <https://example.org/policy/demo-cohort/P003> ;
  odrl:profile  <https://w3id.org/vcf-rdfizer/policy> ;
  odrl:conflict odrl:prohibit ;
  odrl:permission [ odrl:target <file://P003.vcf> ; odrl:action odrl:read ;
      odrl:assignee odrl:All ;
      odrl:constraint [ odrl:leftOperand odrl:purpose ; odrl:operator odrl:isAnyOf ;
                        odrl:rightOperand obo:DUO_0000006 ] ;
      odrl:duty [ odrl:action odrl:attribute ] ] .
```

### 4.1 What the three requesters get

| Requester | Purpose | P001 | P002 | P003 | P004 | P005 | *BRCA1* | rs429358 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| General-research consortium | GRU | ✓ | ✓ | — | — | — | withheld | withheld |
| Alzheimer's consortium | DS | ✓ | ✓ | ✓ | — | ✓ | withheld | **released** |
| Clinical genetics lab | CC | ✓ | ✓ | — | — | — | **released** | withheld |

Every cell follows from §4 alone:

- P003 is out for general research, because GRU is broader than P003's
  health-research consent.
- P003 and P005 are in for the Alzheimer's consortium, because DS falls
  within both HMB and DS.
- P004 is out everywhere: the withdrawal is a prohibition, and prohibitions
  win.
- *BRCA1* and rs429358 are withheld unless the purpose is the one
  exception each rule allows.

This grid is the demonstrator's headline result, and §7 makes it the oracle.

---

## 5. What `attach` adds to the graph

`attach` merges the policy graph into the data. It then materializes what each
selection selects, so policies can be queried alongside data without
re-implementing the selectors in SPARQL:

```turtle
<file://P003.vcf>          odrl:hasPolicy <#consent-P003> .
<#brca1>                   vcfp:selects   <file://P001.vcf#record/12> , … .
<file://P001.vcf#record/12> odrl:hasPolicy <#cohort-policy> .
```

It also writes an attachment manifest: the policy's digest, and the selection
counts per asset.

The query that makes the argument: *which records could I not share with a
general-research collaborator, and why?*

```sparql
PREFIX odrl: <http://www.w3.org/ns/odrl/2/>
PREFIX vcfc: <https://w3id.org/vcf-core/vocab#>
PREFIX obo:  <http://purl.obolibrary.org/obo/>
SELECT ?record ?chrom ?pos ?rule WHERE {
  ?record a vcfc:VCFRecord ; vcfc:chrom ?chrom ; vcfc:pos ?pos ;
          odrl:hasPolicy ?policy .
  ?policy odrl:prohibition ?rule .
  ?rule odrl:constraint [ odrl:operator odrl:isNoneOf ; odrl:rightOperand ?exempt ] .
  FILTER(?exempt != obo:DUO_0000042)
}
```

That query runs over the annotated graph unchanged, under any of the four
engines the validation stage already uses. No VCF tool can answer it.

---

## 6. What `evaluate` writes

For one request it writes a directory:

| File | Contents |
| --- | --- |
| `view.nt` | The release view: the released triples, sorted |
| `manifest.ttl` | A `vcfp:ReleaseView` (§6.1) |
| `decisions.csv` | One row per record: file, chrom, pos, ref, alt, released or withheld, and the rule that decided it |
| `summary.json` | Counts per file and per rule, for the paper figure |

### 6.1 The manifest

This follows [`privacy-policy-design.md` §10](privacy-policy-design.md#10-the-release-manifest),
with the fields v0.1.0 can fill:

```turtle
<#release> a vcfp:ReleaseView ;
  vcfp:derivedFrom     <file://P001.vcf> , <file://P002.vcf> , <file://P003.vcf> , <file://P004.vcf> , <file://P005.vcf> ;
  vcfp:policy          <https://example.org/policy/demo-cohort/0.1> ;
  vcfp:policyDigest    "sha256:…" ;
  vcfp:request         [ odrl:assignee <https://example.org/party/alz-consortium> ;
                         odrl:purpose  obo:DUO_0000007 ] ;
  vcfp:recordsReleased 142 ; vcfp:recordsWithheld 58 ;
  vcfp:filesWithheld   1 ;   vcfp:triplesWithheld 9412 ;
  vcfp:obligation      [ odrl:action odrl:attribute ] ;
  vcfp:disclosureModel "governed release; not anonymization" ;
  prov:wasGeneratedBy  [ prov:used <urn:vcf-rdfizer-policy:0.1.0> ] ;
  prov:generatedAtTime "…"^^xsd:dateTime .
```

The numbers in the example are placeholders. The policy is referenced by
digest as well as by IRI, and the view keeps the original IRIs, since
pseudonymization is v0.4 (§10). The manifest says so in
`vcfp:disclosureModel`.

### 6.2 Implementation

The evaluator loads the union graph with `rdflib`, which is already a runtime
dependency, so no image change is needed. It then does three things:

- **Resolves selectors to records** with one SPARQL query per selector, over
  `vcfc:chrom`, `vcfc:pos`, `vcfc:ref` and `vcfc:alt`, with the assembly
  checked against each file's `vcfc:referenceGenome`.
- **Decides each record** by §4.
- **Expands each withheld record to its subtree by IRI prefix**, using the
  hierarchy in [conversion §6](conversion.md#6-iri-templates):
  - `#record/{ROW}` and everything under it (alleles, events);
  - `#call/{ROW}` and everything under it (INFO values; the condensed matrix
    and vectors);
  - `#sample/{ROW}/` and everything under it (expanded sample calls,
    genotypes and FORMAT values).

  A withheld file removes everything under `file://{FILE}`. A final pass
  drops any triple whose IRI object falls in a withheld subtree.

This is in memory by design. A size guard refuses graphs above 5M triples,
with a message that names the demonstrator's scope. The streaming version is
v0.3 (§10).

---

## 7. How `check` verifies a view

The checks follow the validation methodology: a claim is tested against an
independent computation, not taken on trust.

1. **Oracle agreement.** `check` reparses the source VCFs directly, without
   the graph or any SPARQL, and computes which records each request should
   see from `fixture.json` and §4. The view's released records must equal
   that set **exactly**:
   - an extra record is a leak;
   - a missing record is over-withholding.

   Both are failures, because a redactor that withheld everything would
   otherwise pass.
2. **Prohibition ASKs.** Each prohibition that applies is compiled into a
   SPARQL `ASK` that must be false on the view: for example, any record in
   the *BRCA1* window, or any subject under `file://P004.vcf`.
3. **No dangling references.** No IRI object in the view points into a
   withheld subtree.
4. **Header and IRI leakage.** No IRI or header literal in the view names a
   withheld participant. In v0.1.0 that can only occur through a
   whole-file withdrawal, which the prefix rule removes, so the check
   guards against regressions in that rule.

**Mutation tests** (`test/test_policy_unit.py`) break a correct view on
purpose and assert that `check` catches each break:

- reinsert one withheld *BRCA1* triple;
- restore one triple of P004;
- delete one record that should have been released;
- leave one dangling allele reference;
- swap two requesters' views.

A check that has never caught a planted leak is not evidence of anything.

---

## 8. The command

`vcf-rdfizer-policy`, a separate console script in the pattern of
`vcf-rdfizer-link`. It runs on the host and needs no Docker.

```bash
vcf-rdfizer-policy attach   --rdf P00*.nt.gz --policy policy.ttl -o cohort-annotated.nt
vcf-rdfizer-policy evaluate --rdf P00*.nt.gz --policy policy.ttl \
                            --assignee https://example.org/party/alz-consortium \
                            --purpose obo:DUO_0000007 -o views/alz
vcf-rdfizer-policy check    --view views/alz --policy policy.ttl --vcf P00*.vcf
vcf-rdfizer-policy explain  --policy policy.ttl       # the §4.1 grid, from the policy alone
```

| Exit code | Meaning |
| --- | --- |
| 0 | Success; for `check`, every check passed |
| 1 | `check` found a leak, over-withholding or a dangling reference |
| 2 | The policy uses something v0.1.0 cannot evaluate, the assembly doesn't match, or the graph is over the size guard |

**Layout:**

```text
vcf_rdfizer_policy_cli.py        console entry point (pyproject: vcf-rdfizer-policy)
vcf_rdfizer_policy/
  __init__.py                    version 0.1.0
  profile.py                     parse + validate a policy against the v0.1.0 subset
  purposes.py                    DUO subsumption over the bundled subset
  selectors.py                   file, region and variant selectors -> record IRIs
  decide.py                      the §4 semantics; the one place they live
  attach.py  evaluate.py  check.py  manifest.py
vcf_rdfizer_data/policy/
  vcfp-0.1.ttl                   the profile terms v0.1.0 defines
  duo-subset.ttl                 four DUO terms and their hierarchy, release recorded
examples/policy/
  make_fixture.py  fixture.json  P001.vcf … P005.vcf
  policy.ttl  requests/{gru,alz,clinical}.ttl
  converted/                     the five converted graphs, committed so the demo runs without Docker
  run_demo.sh  README.md
test/test_policy_unit.py         runs in CI (matches test_*_unit.py)
```

`decide.py` holds the semantics, and **both `evaluate` and `check`'s oracle
call it**, so the §4 rules exist in exactly one place. The oracle's
independence comes from its input, the VCF text rather than the graph, not
from a second implementation of the rules.

---

## 9. What goes into the paper

The capabilities table's *Provenance/policy annotations* row currently reads
"Not implemented / Not evaluated". It becomes:

> **Demonstrator (v0.1.0)** · **Exercised on a fixture** · ODRL policies
> attached to files, regions and variants; per-request release views
> verified against a VCF-side oracle; not anonymization, and not enforced at
> conversion time.

Plus one figure and one short passage (Discussion, or a Results subsection
next to linking):

**Figure: policies attached at three granularities, and what they do.**

- **(a)** Where the policies attach. A schematic, in the style of Figure 2: a
  participant's consent on `VCFFile`, the *BRCA1* rule on a region selection,
  the ε4 rule on one variant, and each linked to its policy by
  `odrl:hasPolicy`.
- **(b)** What each requester receives. The §4.1 grid, with participants and
  the two cohort rules as rows and the three requesters as columns, cells
  marked released or withheld, and triples withheld per requester along the
  bottom. The data come from `summary.json`, drawn by `make_figures.py`.
- **(c)** A policy and the question it enables. The 10-line ODRL rule for
  rs429358 beside the §5 query. This can be a listing instead, if the figure
  gets crowded.

**The passage states the advantages, each with what supports it:**

- Granularity finer than a dataset-level consent code: file, region and
  variant, each governed separately (panel b).
- Policies that are queryable alongside the data they govern (the §5 query).
- Standards rather than bespoke configuration: ODRL, DUO and PROV.
- Releases that are auditable: a digest-pinned manifest per view.
- A claim that is measured: every view matches an independent VCF-side
  oracle, and the mutation tests show the check catches planted leaks.

**And it states the limits,** in the same paragraph:

- the fixture is synthetic, and small;
- there is no pseudonymization, so a view keeps its original IRIs;
- enforcement happens after conversion, not during it;
- duties are recorded but not enforced;
- it is governed release, not anonymization.

---

## 10. Beyond v0.1.0

Each version maps onto the full design's build order
([`privacy-policy-design.md` §13](privacy-policy-design.md#13-build-order)).

| Version | Adds | Design § |
| --- | --- | --- |
| **v0.2** | Multi-sample files: `vcfp:SampleSelector` (expanded first), then `vcfp:maskVectorPositions` for condensed, with the single-position-mask warning; `FieldSelector` and `HeaderSelector`; the `generalize` effect (genotype → carrier status, which suits *APOE*) | §4.1, §4.2, §8 |
| **v0.3** | Enforcement during conversion: TSV-level (Tier 1) and emitter-level (Tier 2), including `CHROM`/`POS` on `ParsedSampleRecord`; a streaming post-hoc `--mode redact` (Tier 3) with the two-pass region map; the size guard lifted | §5 |
| **v0.4** | Pseudonymization with IRI re-minting and per-release keys, and sorting views by token so row order cannot be reconstructed | §7 |
| **v0.5** | Full DUO with release pinning and MONDO disease qualifiers; `policy diff` and a fuller `policy check` (lint); enforced duties, with an audit sink for `odrl:inform` | §4.3, §12 |
| later | `threshold`; query-time rewriting for an operated endpoint, paired with aggregate controls | §5, §9 |

Two rules carry forward unchanged from v0.1.0:

- **Anything that can't be enforced stops the run.** A version may widen what
  can be enforced, but never replace that error with a warning.
- **Every version ships its oracle checks and mutation tests with it.** The
  full design says it directly: "A crude redaction that is verified is worth
  more than a sophisticated one that is not."

---

## 11. Work plan

| Step | Delivers | Estimate |
| --- | --- | --- |
| 1 | `make_fixture.py`, the five VCFs, `fixture.json`; convert and commit the graphs | 0.5 day |
| 2 | `profile.py` (subset validation, rejection of everything else), `purposes.py`, `vcfp-0.1.ttl`, `duo-subset.ttl` | 0.5–1 day |
| 3 | `selectors.py`, `decide.py`, `evaluate.py`: views, manifest, `decisions.csv`, `summary.json` | 1–1.5 days |
| 4 | `attach.py`, and the §5 query checked under the validation engines | 0.5 day |
| 5 | `check.py` (oracle, ASKs, dangling, leakage) and `test/test_policy_unit.py`, including the mutation tests | 1 day |
| 6 | `vcf_rdfizer_policy_cli.py`, `explain`, `run_demo.sh`, `examples/policy/README.md`, links from `limitations.md` and `roadmap.md` | 0.5 day |
| 7 | Paper: the capabilities row, the passage, the figure (panel b from `summary.json`) | 0.5 day |
| | **Total** | **about 4–5 days** |

**Done means all of the following:**

- `run_demo.sh` produces three views from a clean checkout without Docker.
- `check` passes on all three views.
- Every mutation test fails the check it targets.
- The CI suite passes.
- The paper's figure is regenerated from the demo's own output.

---

## See also

- [Privacy policy design](privacy-policy-design.md) — the full design this is a slice of
- [Data linking](datalinking.md) — the sibling plug-in; the same manifest and provenance pattern
- [Conversion §6](conversion.md#6-iri-templates) — the IRI hierarchy the evaluator relies on
- [Validation methodology](validation-methodology.md) — why a policy claim needs an oracle
