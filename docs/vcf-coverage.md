# VCF coverage matrix

*Part of the [VCF-RDFizer documentation](README.md). What the conversion emits:
[`conversion.md`](conversion.md). How this table is measured:
[`validation-methodology.md`](validation-methodology.md).*

What of a VCF file VCF-RDFizer represents in RDF, and what the semantic
validation suite actually verifies. One row per VCF element. This is the
tracking artifact for the coverage work and the source table for publication.

Two independent questions per row, deliberately kept apart:

- **Represented** — does the conversion emit RDF for this element at all?
- **Validated** — would a corruption of it be *detected*? Backed by a named
  mutation in [`test/validation_mutations.py`](../test/validation_mutations.py),
  not by inspection.

Last measured mutation score: **96/113 (85%)** across 60 distinct mutations,
measured against the VCF Core vocabulary. Regenerate with:

```bash
VCF_RDFIZER_MUTATION_REPORT=mutation-score.json \
  python -m unittest test.test_validation_mutation_unit
```

The score fell from 97% because the denominator grew, not because anything
regressed: the migration added 18 mutations covering the layers VCF Core
introduced, and 10 of those are recorded gaps. That is the methodology working
as intended — a new capability that nothing checks shows up as a lower score
rather than as silent confidence. Every gap is listed under
[Remaining gaps](#remaining-gaps) with what would close it.

---

## Fixed fields (the eight mandatory columns)

| VCF element | `records.tsv` | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- | --- |
| CHROM | `CHROM` | `vcfc:chrom` | yes | `q01`, `q11` | `corrupt_chrom` |
| POS | `POS` | `vcfc:pos` (`xsd:integer`) | yes | `q01`, `q11`, `preflight_position_datatype` | `corrupt_pos`, `drop_pos`, `retype_pos_as_string` |
| POS ↔ record binding | — | — | yes | `q11_record_digest` | `permute_pos` |
| ID | `ID` | `vcfc:recordId` | yes | `q11_record_digest` | (covered by digest) |
| REF | `REF` | `vcfc:ref` | yes | `q02`, `q03`, `q11` | `corrupt_alt` |
| ALT | `ALT` | `vcfc:alt` | yes | `q02`, `q03`, `q11` | `corrupt_alt` |
| REF/ALT ↔ record binding | — | — | yes | `q11_record_digest` | `permute_ref_alt` |
| QUAL | `QUAL` | `vcfc:qual` (`xsd:decimal` / `vcfc:Null`) | yes | `q09`, `q11` | `drop_qual`, `drop_all_qual`, `corrupt_qual` |
| FILTER | `FILTER` | `vcfc:filter` | yes | `q04` (exact lexical), `q11` | `drop_filter`, `corrupt_filter_lexical` |
| INFO (raw) | `INFO` | `vcfc:infoRaw` | yes | `q11_record_digest` | `corrupt_info_raw` |
| INFO (structured) | `INFO` | `vcfc:hasInfoValue` → `vcfc:InfoFieldValue` → `vcfc:declaredBy` | yes | `q09`, `q12_info_value_digest` | `drop_info_value`, `corrupt_info_value`, `retype_info_value` |

INFO values carry `vcfc:fieldValue` plus a typed `fieldValueInteger` /
`fieldValueDecimal` when the declaration says `Number=1` and the value parses;
a Flag entry carries `vcfc:fieldValueBoolean true`. A multi-valued field is no
longer lexical-only: `Number=A`/`R`/`LA`/`LR`/`G`/`LG`/`P` values are decomposed
into ordered `vcfc:FieldValueItem` resources joined to their allele with
`vcfc:forAllele`, or carrying a genotype/GT ordinal.

### Allele layer

| VCF element | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- |
| REF as an ordered allele | `vcfc:hasReferenceAllele` → `vcfc:ReferenceAllele` (`alleleIndex` 0) | yes | — | — |
| Each ALT item, in source order | `vcfc:hasAltAllele` → `vcfc:AltAllele` (`alleleIndex` 1..n) | yes | — | — |
| ALT syntactic category | `vcfc:alleleKind` | yes | — | — |
| Symbolic ALT reserved type | `vcfc:svType` → `vcfc:SymbolicAlleleType` | yes | — | — |
| Symbolic ALT ↔ `##ALT` declaration | `vcfc:declaredByAlt` | yes | — | — |
| Breakend structure | `vcfc:Breakend`, `breakendOrientation`, `breakendReplacementString`, `isSingleBreakend` | yes | — | — |
| CHROM ↔ contig declaration | `vcfc:chromosome` | yes | — | — |
| Indexed field values | `vcfc:hasValueItem` → `vcfc:FieldValueItem` (`valueIndex`, `itemValue`, `forAllele`, `tupleArity`) | yes | — | — |

### Structural variation

| VCF element | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- |
| `SVLEN` | `vcfc:svLength` on the allele | yes | — | — |
| `SVCLAIM` | `vcfc:svClaim` → `vcfc:SVClaim` | yes | — | — |
| `IMPRECISE`, `NOVEL` | `vcfc:isImprecise`, `vcfc:isNovel` | yes | — | — |
| `EVENT` + `EVENTTYPE` | `vcfc:inEvent` → `vcfc:VariantEvent` with `vcfc:eventType` | yes (both keys required) | — | — |
| `CIPOS`, `CIEND` | `vcfc:posConfidenceInterval` / `endConfidenceInterval` → FALDO `InRangePosition` | yes | — | — |
| `CILEN`, `CICN` | `vcfc:ConfidenceInterval` with `ciLower` / `ciUpper` | yes | — | — |
| `RN` + `RUS`/`RUL`/`RUC`/`RB` | `vcfc:TandemRepeatAllele` → `vcfc:RepeatSequence` | yes (`RN` required) | — | — |
| gVCF `<*>` with `END` | `vcfc:ReferenceBlock`, `endPosition`, `referenceBlockLength` | yes (`END` required) | — | — |

## Genotype fields

| VCF element | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- |
| FORMAT declaration | `vcfc:formatRaw` | yes | `q09_predicate_census` | — |
| Sample identity | `vcfc:sampleId` / `vcfc:sampleName` | yes | `preflight_sample_gt_inventory`, `q09` | `drop_sample_call` |
| GT values (lexical) | `vcfc:hasFormatValue` → `vcfc:fieldValue` | yes | `q05`, `q06`, `q13` | `flip_genotype` |
| Non-GT FORMAT (DP, GQ, AD, PL…) | `vcfc:FormatFieldValue` / `vcfc:FormatValueVector` | yes | `q13_format_value_digest` | `drop_format_value_dp`, `corrupt_format_value_dp`, `corrupt_format_vector` |
| Reusable sample identity | `vcfc:SampleSet` → `vcfc:VCFSample` (`sampleName`, `sampleIndex`); `vcfc:forSample` from a `SampleCall` | yes, both profiles | — | — |
| `#CHROM` sample columns | `vcfc:ColumnHeaderLine` → `vcfc:hasGenotypeColumns` | yes | — | — |
| FORMAT value ↔ declaration | `vcfc:declaredBy` → `vcfc:FormatFieldDefinition` | yes | — | — |

**Parsed genotype layer — expanded profile only.** The condensed profile keeps
these values inside `vcfc:encodedValues`, which is the point of the profile.

| VCF element | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- |
| `GT` parsed | `vcfc:hasGenotype` → `vcfc:Genotype` (`genotypeString`, `ploidy`) | expanded only | — | — |
| Phasing (`\|` vs `/`) | `vcfc:phasingStatus` → `vcfc:Phased` / `vcfc:Unphased` | expanded only | — | — |
| Each GT position | `vcfc:hasAlleleCall` → `vcfc:GenotypeAlleleCall` (`callIndex`, `isNoCall`, `calledAllele`) | expanded only | — | — |
| `FT` | `vcfc:sampleFilter` | expanded only | — | — |
| `PS`/`PSL`/`PSO`/`PSQ` | `vcfc:inPhaseSet` → `vcfc:PhaseSet` | expanded only | — | — |
| `LAA` | `vcfc:hasLocalAlleleSet` → `vcfc:LocalAlleleSet` | expanded only | — | — |
| `CN`/`CNQ`/`CNL`/`CNP` | `vcfc:copyNumber` and friends | expanded only | — | — |
| `HAP`/`AHAP` | `vcfc:haplotypeId`, `vcfc:ancestralHaplotypeId` | expanded only | — | — |
| `M*`/`DPM*`/`ADM*` | `vcfc:BaseModification` with a ChEBI `modifiedResidue` | expanded only | — | — |

## Header section

| VCF element | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- |
| `##` line count and keys | `vcfc:HeaderLine` + `vcfc:headerKey` | yes | `q08_header_line_census` | `drop_header_line` |
| Line type | `FileFormatHeaderLine`, `INFOHeaderLine`, `ContigHeaderLine`, … | yes | `q10_class_census` | `untype_header_line` |
| `##fileformat` | `vcfc:fileFormat` | yes | `q07_file_metadata` | `corrupt_file_metadata` |
| `##reference` | `vcfc:referenceGenome` | yes | `q07_file_metadata` | `drop_reference_genome` |
| `##source` | `vcfc:sourceSoftware` | yes | `q07_file_metadata` | — |
| `##fileDate` | `vcfc:fileDate` (`xsd:date` when the form allows) | yes | `q09_predicate_census` | `drop_file_date` |
| `##FILTER` | `vcfc:FilterDefinition` + `vcfc:filterId` | yes | `q09`, `q10` | `drop_filter_definition` |
| `##ALT` | `vcfc:AltDefinition` + `vcfc:altId` | yes | `q09`, `q10` | `drop_alt_definition` |
| `##contig` | `vcfc:ContigHeaderLine` + `contigId`/`contigLength` (`xsd:integer`)/`contigMd5`/`contigAssembly`/`contigUrl` (`xsd:anyURI`) | yes | `q09`, `q10` | `drop_contig_attribute` |
| `##INFO` / `##FORMAT` definitions | `vcfc:InfoFieldDefinition` / `vcfc:FormatFieldDefinition` with `fieldId`/`fieldNumber`/`fieldType`/`fieldDescription` | yes | `q09`, `q10` | `drop_info_definition` |
| `Number` arity and fixed count | `vcfc:fieldArity` → `vcfc:VCFNumberArity`, `vcfc:fieldNumberInteger` | yes | — | — |
| `##INFO` `Source` / `Version` | `vcfc:fieldSource`, `vcfc:fieldVersion` | yes | — | — |
| Structured-line attributes (all of them) | `vcfc:hasAttribute` → `vcfc:HeaderAttribute` (`attributeKey`, `attributeValue`, `attributeIndex`) | yes | — | — |
| `#CHROM` column-header line | `vcfc:ColumnHeaderLine` + `vcfc:hasColumnHeader` | yes | — | — |
| Header line order | `vcfc:lineIndex` (`xsd:integer`) | yes | — | — |
| `##assembly` | `vcfc:AssemblyHeaderLine` + `vcfc:assemblyUrl` | yes | — | — |
| `##pedigreeDB` | `vcfc:PedigreeDBHeaderLine` + `vcfc:pedigreeDbUrl` | yes | — | — |
| `##META` | `vcfc:MetaHeaderLine` / `vcfc:MetaDefinition` + `vcfc:metaAllowedValue` | yes | — | — |
| `##SAMPLE` | `vcfc:SampleHeaderLine` → `vcfc:declaresSample` → `vcfc:SampleDeclaration` | yes | — | — |
| `##PEDIGREE` | `vcfc:PedigreeHeaderLine` + `pedigreeMother`/`pedigreeFather`/`pedigreeOriginal`/`pedigreeAncestor` + `ancestorRole` | yes | — | — |
| Header line *values* | `vcfc:headerValue` | yes | `q09` (count only) | — |
| `vcfc:contigCount` | derived scalar | yes | **count only, not value** | `corrupt_contig_count` |

An unrecognized `##` key is typed as `vcfc:StructuredHeaderLine` or
`vcfc:UnstructuredHeaderLine` according to the form of its value, which keeps it
queryable without inventing a subclass the vocabulary does not define.

Every structured line carries its `vcfc:HeaderAttribute` resources whether or
not it also has dedicated properties. `vcfc:StructuredHeaderLineShape` requires
at least one, and they are the only way an implementation-defined attribute
stays queryable. This is why `--header-representation basic` does not produce a
conformant graph.

## Graph-level properties

| Property | Validated by | Mutation |
| --- | --- | --- |
| N-Triples syntax | Raptor (`rapper -c`) | — |
| SHACL shape conformance | `--shacl-shapes` (opt-in, independent layer) | — |
| Per-record property cardinality | `preflight_record_cardinality` + exact count | `drop_pos` |
| Missing-token policy | `preflight_missing_token_conformance`, fatal under `--strict-conformance` | `plain_dot_literal` |
| Representation profile | `preflight_representation_profile` | `wrong_representation_profile` |
| Record count | q01/q02/q04 totals | `drop_record`, `duplicate_record` |
| Record order (`vcfc:recordIndex`) | SPARQL SHACL profile: uniqueness, nondecreasing POS within CHROM, contiguous CHROM blocks | — |
| Expanded/condensed exclusivity | SPARQL SHACL profile | `wrong_representation_profile` |
| **No extraneous triples** | `q09_predicate_census` (extra rows) | `spurious_predicate` |
| **No blank nodes** | `preflight_blank_nodes` | `introduce_blank_node`, `blank_node_object` |
| **No empty or whitespace-only terms** | `preflight_empty_values` | `empty_literal`, `whitespace_only_literal` |
| **No duplicate statements** | `preflight_duplicate_triples` (parsed vs distinct) | `duplicate_triple`, `duplicate_whole_graph` |
| Class inventory | `q10_class_census` | `untype_header_line` |
| Per-record identity | `q11_record_digest` | `permute_pos`, `permute_ref_alt` |

## Remaining gaps

1. **`vcfc:contigCount` is counted, not read.** A wrong derived contig total is
   not detected. Closing it needs the value in a comparison, not just the
   predicate in the census.
2. **Header line values are not compared.** `q08` compares how many lines carry
   each key and `q10` their types, but `vcfc:headerValue` itself is only
   counted. The structured attributes that matter (`filterId`, `contigId`, …)
   *are* covered.
3. **The census assumes the shipped mapping.** A custom `--rules` changes the
   inventory and IRI templates by design, so `q09`–`q13` fall back to
   report-only. That is correct, but it means a custom mapping is validated
   only by the aggregate comparisons.
4. **The new layers are counted but their values are not compared.** The
   allele layer, header attributes, value items and the parsed genotype layer
   all appear in the predicate and class censuses, so *dropping* one is caught.
   Corrupting a value inside one is not: `corrupt_allele_value`,
   `corrupt_allele_kind`, `corrupt_attribute_value`, `corrupt_value_item_allele`,
   `corrupt_called_allele`, `flip_phasing_status` and `wrong_declaration_owner`
   are all recorded as undetected. Closing them needs digests over those
   resources, the way `q11` already covers the record fields — or leaning on the
   vocabulary's own consistency SHACL profile, which checks several of these
   agreements directly.
5. **Ordering is checked by shapes, not by queries.** `vcfc:lineIndex` and
   `vcfc:recordIndex` are counted, but `corrupt_record_index` is undetected by
   the query layer. The SPARQL SHACL profile enforces uniqueness, nondecreasing
   POS within a CHROM block, and contiguous CHROM blocks, so the coverage exists
   — in a different layer.
6. **Version overlays have no mutation.** The converter detects each input's VCF
   version and follows it, and `drop_version_class` / `wrong_version_class`
   cover the file class. The version-dependent *behaviour* — per-ALT versus
   per-record `CIPOS` linkage, the families a version does not define — is
   covered by unit tests and by the version overlays themselves, not by the
   mutation harness, because the fixture declares a single version.
7. **SHACL conformance is not yet a test.** It was checked by hand and found
   seven real modelling bugs; wiring it in would make that a standing invariant.
   It needs `pyshacl` and `rdflib` as dev dependencies.

## Vocabulary alignment

The conversion targets the **VCF Core vocabulary**,
`https://w3id.org/vcf-core/vocab#` (prefix `vcfc:`), **published at 2.0.0** and
covering VCF 4.1 through 4.5. It replaces the retired
`https://w3id.org/vcf-rdfizer/vocab#`, which is **not** redirected: it serves a
deprecation document in which every former term is present, deprecated, and
linked to its successor. Only one term was renamed rather than re-namespaced —
`vcfr:DenseRepresentation` became `vcfc:ExpandedRepresentation`.

The converter references the namespace, not a version IRI. The vocabulary's
release cadence is independent of this converter's, and pinning a version here
would go stale without adding anything: the namespace is what the emitted IRIs
actually contain.



### VCF version coverage

VCF Core supplies a conformance overlay per VCF version, and the converter
follows each input's own `##fileformat` line automatically. Coverage:

| Version | Overlay | Detected | Notes |
| --- | --- | --- | --- |
| 4.0 | none claimed by the vocabulary | reported as unrecognized | converts with 4.5 rules and **no** `vcfc:VCF4xFile` class |
| 4.1 | `vcf-4.1.shacl.ttl` | ✓ `vcfc:VCF41File` | no `Number=R`; `CIPOS` is one pair per record |
| 4.2 | `vcf-4.2.shacl.ttl` | ✓ `vcfc:VCF42File` | `Number=R` added |
| 4.3 | `vcf-4.3.shacl.ttl` | ✓ `vcfc:VCF43File` | |
| 4.4 | `vcf-4.4.shacl.ttl` | ✓ `vcfc:VCF44File` | `Number=P`; tuples become per-ALT; `CILEN`/`CICN`/`EVENTTYPE` added |
| 4.5 | `vcf-4.5.shacl.ttl` | ✓ `vcfc:VCF45File` | `Number=LA/LR/LG/M`; local alleles and base modifications |

See [`conversion.md`](conversion.md#4a-vcf-versions) for what each difference
changes in the emitted graph.

### What the converter emits, versus what VCF Core can express

VCF Core 2.0.0 defines 338 terms. The converter emits from most of them, and its
output is conformant, but the vocabulary can express more than the converter
currently derives. Ten families are entirely unemitted:

| Family | Terms | What it would add |
| --- | --- | --- |
| Parsed FILTER | `filterStatus`, `FiltersPassed/Failed/NotApplied`, `FilterCode`, `hasFilterCode`, `declaredByFilter` | FILTER as resources joined to their `##FILTER` declarations, instead of only the raw string |
| Padding semantics | `PaddingInterpretation`, `paddingRule`, `paddingSide`, `paddingAnchorPosition`, `paddingBaseCount` and their individuals | Which base of REF/ALT is the VCF padding base, and why |
| Parsed FORMAT keys | `FormatKey`, `hasFormatKey`, `fieldIndex`, `keyPattern` | The FORMAT key list as ordered resources, not only `formatRaw` |
| Parsed record IDs | `RecordIdentifier`, `hasIdentifier`, `identifierValue`, `aliasOf` | The semicolon-separated ID column split into individual identifiers |
| Repeat units | `RepeatUnit`, `hasRepeatUnit`, `repeatUnitBases`, `rucConfidenceInterval`, `rbConfidenceInterval` | `RUB`-level detail below a repeat sequence |
| Breakend mates | `mateBreakend`, `partnerBreakend`, `insertedSequence`, `isTelomereBreakend` | Resolving a breakend to the record describing its mate |
| Local-allele membership | `LocalAlleleMembership`, `localAllele`, `localIndex` | LAA membership with its own index, beside the current `LocalAlleleSet` |
| Mixed phasing | `MixedPhasing`, `phaseIndicator`, `allelePhaseSet` | Per-position phase indicators rather than one status per genotype |
| Reference blocks | `hasReferenceBlock`, `blockAllele` | A record-level link to its gVCF block |
| Raw/decoded values | `rawValue`, `decodedValue`, `percentEncodingPolicy`, `sampleFieldsRaw` | Percent-decoded companions beside the lossless source values |

**None of these is required for conformance.** Across 19 graphs — every example
VCF the vocabulary ships, plus a stress matrix for 4.0 through 4.5 in both
representation profiles — the complete profile set reports zero violations
without them. They are enrichment the vocabulary supports and the converter does
not yet derive.

This matters for benchmarking in one direction only: a benchmark of conversion
throughput, graph size or compression is measuring the converter's actual output
and is unaffected. A benchmark of *query* coverage against the vocabulary would
be measuring a subset, and should say so.

### Conformance

The default mapping's output validates against the **complete** published
profile set — the portable and SPARQL profiles, the consistency profile, and all
five version overlays — with **zero violations**, in both representation
profiles, checked with pySHACL using the bundled ontology plus every
`ontology/versions/*.ttl` and RDFS inference, which is the configuration the
vocabulary's own `tests/validate_shacl.py` uses. See
[`validation-migration-notes.md`](validation-migration-notes.md#how-this-was-verified)
for exactly what was covered.

The only results reported are `sh:Warning`s where an input VCF omits the
`Source` and `Version` attributes that VCF 4.5 recommends on `##INFO`
declarations. Those are properties of the input, and the converter deliberately
does not fabricate them.

---

## See also

- [Validation methodology](validation-methodology.md) — how the coverage in this table is measured
- [Validation](validation.md) — running the validator
- [Conversion](conversion.md) — how each represented element is emitted
- [Roadmap](roadmap.md) — the vocabulary and coverage gaps above, with their planned fixes
