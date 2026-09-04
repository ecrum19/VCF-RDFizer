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

Last measured mutation score: **76/78 (97%)** across 42 distinct mutations.
Regenerate with:

```bash
VCF_RDFIZER_MUTATION_REPORT=mutation-score.json \
  python -m unittest test.test_validation_mutation_unit
```

---

## Fixed fields (the eight mandatory columns)

| VCF element | `records.tsv` | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- | --- |
| CHROM | `CHROM` | `vcfr:chrom` | yes | `q01`, `q11` | `corrupt_chrom` |
| POS | `POS` | `vcfr:pos` (`xsd:integer`) | yes | `q01`, `q11`, `preflight_position_datatype` | `corrupt_pos`, `drop_pos`, `retype_pos_as_string` |
| POS ↔ record binding | — | — | yes | `q11_record_digest` | `permute_pos` |
| ID | `ID` | `vcfr:recordId` | yes | `q11_record_digest` | (covered by digest) |
| REF | `REF` | `vcfr:ref` | yes | `q02`, `q03`, `q11` | `corrupt_alt` |
| ALT | `ALT` | `vcfr:alt` | yes | `q02`, `q03`, `q11` | `corrupt_alt` |
| REF/ALT ↔ record binding | — | — | yes | `q11_record_digest` | `permute_ref_alt` |
| QUAL | `QUAL` | `vcfr:qual` (`xsd:decimal` / `vcfr:Null`) | yes | `q09`, `q11` | `drop_qual`, `drop_all_qual`, `corrupt_qual` |
| FILTER | `FILTER` | `vcfr:filter` | yes | `q04` (exact lexical), `q11` | `drop_filter`, `corrupt_filter_lexical` |
| INFO (raw) | `INFO` | `vcfr:infoRaw` | yes | `q11_record_digest` | `corrupt_info_raw` |
| INFO (structured) | `INFO` | `vcfr:hasInfoValue` → `vcfr:InfoFieldValue` → `vcfr:declaredBy` | yes | `q09`, `q12_info_value_digest` | `drop_info_value`, `corrupt_info_value`, `retype_info_value` |

INFO values carry `vcfr:fieldValue` plus a typed `fieldValueInteger` /
`fieldValueDecimal` when the declaration says `Number=1` and the value parses;
a Flag entry carries `vcfr:fieldValueBoolean true`. A multi-valued field
(`Number=A/R/G/.`) keeps only the lexical value, because the vocabulary's IRI
template gives one node per key.

## Genotype fields

| VCF element | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- |
| FORMAT declaration | `vcfr:formatRaw` | yes | `q09_predicate_census` | — |
| Sample identity | `vcfr:sampleId` / `vcfr:sampleName` | yes | `preflight_sample_gt_inventory`, `q09` | `drop_sample_call` |
| GT values | `vcfr:hasFormatValue` → `vcfr:fieldValue` | yes | `q05`, `q06`, `q13` | `flip_genotype` |
| Non-GT FORMAT (DP, GQ, AD, PL…) | `vcfr:FormatFieldValue` / `vcfr:FormatValueVector` | yes | `q13_format_value_digest` | `drop_format_value_dp`, `corrupt_format_value_dp`, `corrupt_format_vector` |

## Header section

| VCF element | RDF term | Represented | Validated by | Mutation |
| --- | --- | --- | --- | --- |
| `##` line count and keys | `vcfr:HeaderLine` + `vcfr:headerKey` | yes | `q08_header_line_census` | `drop_header_line` |
| Line type | `FileFormatHeaderLine`, `INFOHeaderLine`, `ContigHeaderLine`, … | yes | `q10_class_census` | `untype_header_line` |
| `##fileformat` | `vcfr:fileFormat` | yes | `q07_file_metadata` | `corrupt_file_metadata` |
| `##reference` | `vcfr:referenceGenome` | yes | `q07_file_metadata` | `drop_reference_genome` |
| `##source` | `vcfr:sourceSoftware` | yes | `q07_file_metadata` | — |
| `##fileDate` | `vcfr:fileDate` (`xsd:date` when the form allows) | yes | `q09_predicate_census` | `drop_file_date` |
| `##FILTER` | `vcfr:FilterDefinition` + `vcfr:filterId` | yes | `q09`, `q10` | `drop_filter_definition` |
| `##ALT` | `vcfr:AltDefinition` + `vcfr:altId` | yes | `q09`, `q10` | `drop_alt_definition` |
| `##contig` | `vcfr:ContigHeaderLine` + `contigId`/`contigLength`/`contigMd5`/`contigAssembly` | yes | `q09`, `q10` | `drop_contig_attribute` |
| `##INFO` / `##FORMAT` definitions | `vcfr:InfoFieldDefinition` / `vcfr:FormatFieldDefinition` with `fieldId`/`fieldNumber`/`fieldType`/`fieldDescription` | yes | `q09`, `q10` | `drop_info_definition` |
| Header line *values* | `vcfr:headerValue` | yes | `q09` (count only) | — |
| `vcfr:contigCount` | derived scalar | yes | **count only, not value** | `corrupt_contig_count` |

An unrecognized `##` key keeps only the base `vcfr:HeaderLine` type: inventing
a subclass would put a term in the graph that the vocabulary does not define.

## Graph-level properties

| Property | Validated by | Mutation |
| --- | --- | --- |
| N-Triples syntax | Raptor (`rapper -c`) | — |
| SHACL shape conformance | `--shacl-shapes` (opt-in, independent layer) | — |
| Per-record property cardinality | `preflight_record_cardinality` + exact count | `drop_pos` |
| Missing-token policy | `preflight_missing_token_conformance`, fatal under `--strict-conformance` | `plain_dot_literal` |
| Representation profile | `preflight_representation_profile` | `wrong_representation_profile` |
| Record count | q01/q02/q04 totals | `drop_record`, `duplicate_record` |
| **No extraneous triples** | `q09_predicate_census` (extra rows) | `spurious_predicate` |
| **No blank nodes** | `preflight_blank_nodes` | `introduce_blank_node`, `blank_node_object` |
| **No empty or whitespace-only terms** | `preflight_empty_values` | `empty_literal`, `whitespace_only_literal` |
| **No duplicate statements** | `preflight_duplicate_triples` (parsed vs distinct) | `duplicate_triple`, `duplicate_whole_graph` |
| Class inventory | `q10_class_census` | `untype_header_line` |
| Per-record identity | `q11_record_digest` | `permute_pos`, `permute_ref_alt` |

## Remaining gaps

1. **`vcfr:contigCount` is counted, not read.** A wrong derived contig total is
   not detected. Closing it needs the value in a comparison, not just the
   predicate in the census.
2. **Header line values are not compared.** `q08` compares how many lines carry
   each key and `q10` their types, but `vcfr:headerValue` itself is only
   counted. The structured attributes that matter (`filterId`, `contigId`, …)
   *are* covered.
3. **The census assumes the shipped mapping.** A custom `--rules` changes the
   inventory and IRI templates by design, so `q09`–`q13` fall back to
   report-only. That is correct, but it means a custom mapping is validated
   only by the aggregate comparisons.

## Vocabulary alignment

The tool emits 17 terms that `https://w3id.org/vcf-rdfizer/vocab#` does **not**
define — all belonging to the condensed representation:

`CohortCallMatrix`, `CondensedRepresentation`, `ExpandedRepresentation`,
`FormatValueVector`, `SampleSet`, `VCFSample`, `VCFTextVector`,
`appliesToSampleSet`, `encodedValues`, `hasCallMatrix`, `hasFormatValueVector`,
`hasSample`, `hasSampleSet`, `representationProfile`, `sampleIndex`,
`sampleName`, `valueEncoding`

Dereferencing any of these returns nothing, so **condensed graphs are not
ontology-backed**. This is the one remaining publication blocker and is work in
the vocabulary repository, not here.

### An open conflict inside the vocabulary

Running SHACL surfaced a contradiction between two parts of the published
vocabulary that the tool cannot satisfy simultaneously:

- `vcfr:missingValuePolicy` says a missing token SHOULD be `"."^^vcfr:Null`.
- `VCFRecordShape` constrains `vcfr:alt` to `sh:datatype xsd:string`.

A record with `ALT=.` therefore violates the shape no matter which rule the
conversion follows. The tool currently follows the missing-value policy. This
needs a decision in the vocabulary: either relax the `alt`/`ref`/`chrom` shapes
to `sh:or([xsd:string] [vcfr:Null])`, as the `qual` shape already does, or drop
the missing-value policy for those fields.

Two shape violations found the same way have already been fixed here: QUAL is
now `xsd:decimal`/`vcfr:Null` rather than a plain literal, and `##fileDate` is
typed `xsd:date` when its form allows.

---

## See also

- [Validation methodology](validation-methodology.md) — how the coverage in this table is measured
- [Validation](validation.md) — running the validator
- [Conversion](conversion.md) — how each represented element is emitted
- [Roadmap](roadmap.md) — the vocabulary and coverage gaps above, with their planned fixes
