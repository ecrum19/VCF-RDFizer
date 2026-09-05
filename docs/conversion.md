# Conversion: VCF to RDF

What the conversion actually does to a VCF, in order, and what it decides on
your behalf. The companion documents are
[`architecture.md`](architecture.md) (how the pieces fit),
[`rml-mappings.md`](rml-mappings.md) (how to change the mapping), and
[`vcf-coverage.md`](vcf-coverage.md) (which VCF element ends up where).

---

## 1. Input acceptance

`--input` takes a single file or a directory.

- A file must be named `*.vcf` or `*.vcf.gz`. **Nothing else is accepted** — not
  `.bcf`, not `.vcf.bgz`, not a bare `.gz`. Extension, not content, decides.
- A directory is enumerated **one level deep**, sorted, at the moment the run
  starts. Files that appear later are not picked up. Subdirectories are ignored.
- Each input is processed independently and end to end before the next one
  begins. A failure is isolated to its input.

The output basename is the filename with `.vcf` / `.vcf.gz` removed. Two inputs
that reduce to the same basename would collide, and the pre-flight collision
check rejects the run before Docker starts.

## 2. Stage one — VCF to TSV

[`src/vcf_as_tsv.sh`](../src/vcf_as_tsv.sh) reads the VCF (through `gzip -dc`
when compressed) in **one `awk` pass** and writes three tab-separated tables.

| Output | One row per | Columns |
| --- | --- | --- |
| `<sample>.file_metadata.tsv` | file (exactly one row) | `SOURCE_FILE`, `FILE_FORMAT`, `FILE_DATE`, `SOURCE_SOFTWARE`, `REFERENCE_GENOME`, `HEADER_COUNT`, `RECORD_COUNT` |
| `<sample>.header_lines.tsv` | `##` meta-information line | `SOURCE_FILE`, `HEADER_INDEX`, `HEADER_KEY`, `HEADER_VALUE`, `RAW_LINE` |
| `<sample>.records.tsv` | VCF data line | `SOURCE_FILE`, `ROW_ID`, `CHROM`, `POS`, `ID`, `REF`, `ALT`, `QUAL`, `FILTER`, `INFO`, `FORMAT`, *(sample payload)* |

Two more paths exist for compatibility, `sample_calls.tsv` and
`sample_format_values.tsv`. Under the shipped mapping they are written
**header-only** — see §5.

What this stage does, precisely:

- `##` lines are split on the **first** `=`. Everything after it is
  `HEADER_VALUE`; `RAW_LINE` keeps the original text minus the leading `##`.
  No structural parsing happens here.
- Four keys are lifted into `file_metadata.tsv` by case-insensitive match:
  `fileformat`, `filedate`, `source`, `reference`.
- `HEADER_INDEX` counts `##` lines *and* the `#CHROM` line, 1-based.
- `ROW_ID` is a 1-based counter over data lines, stable within one file. It is
  the join key for every record-level IRI.
- Trailing carriage returns are stripped from every field.
- All sample columns are joined into **one** trailing field separated by single
  spaces, and runs of whitespace are collapsed. The column's *name* is the
  whitespace-joined sample IDs from `#CHROM`, or the literal `SAMPLES` when the
  VCF declares none — which is why a mapping cannot reference it by a fixed name.

**Limitations of this stage, stated plainly.** There is no `htslib` here. The
parser is regex-and-field-index `awk`, which means:

- The `#CHROM` line is recognised only when it is **tab-delimited**. A
  space-delimited header line is matched by no rule, so sample column names are
  lost and the records header falls back to `SAMPLES` — silently.
- Nothing validates VCF spec conformance. A malformed file produces a malformed
  graph rather than an error, and the first thing that notices is the validation
  suite.
- A data line with fewer than 8 columns yields empty strings for the missing
  fields rather than an error.
- Because sample fields are whitespace-normalized, a value containing a literal
  space would be corrupted. The VCF specification forbids spaces in data fields,
  so this is only reachable with an already-invalid file — but it is not detected.

## 3. Stage two — TSV to RDF with RMLStreamer

[`src/run_conversion.sh`](../src/run_conversion.sh) runs RMLStreamer 2.5.0 over
the TSVs using the mapping at `--rules` (default
[`rules/default_rules.ttl`](../rules/default_rules.ttl)), then normalizes the
Flink part files and merges them into one aggregate.

The mapping's five `csvw:url` values are **literal container paths**
(`/data/tsv/records.tsv` and friends) that the wrapper rewrites per input to
`/data/tsv/<sample>.records.tsv`. That rewrite is why the contract in
[`rml-mappings.md`](rml-mappings.md) insists those strings stay verbatim.

`--rdf-storage-mode` decides how the aggregate is assembled:

| Mode | Behaviour |
| --- | --- |
| `plain` | Parts are merged into one uncompressed `<sample>.nt` |
| `space-optimized` | Each part is streamed through gzip into one `<sample>.nt.gz` and the source part is deleted immediately, so a full uncompressed copy never exists |

The output is **N-Triples**. No named graphs are produced, and no blank nodes:
every class in the vocabulary declares an IRI template, and
`preflight_blank_nodes` treats a blank node as a validation failure precisely
because it means a term map produced no IRI.

`--spark-partitions` is a parallelism hint passed through to RMLStreamer. It
does not change the output, only how many parts are produced before the merge.

## 4. Stage three — the wrapper's own emitters

Three classes of triple cannot come from RML, and are appended to the aggregate
by the host process before compression. See
[`architecture.md`](architecture.md#4-where-the-split-leaks-and-why) for why.

### `emit_record_detail` — QUAL and structured INFO

`QUAL` is **always** emitted. The published SHACL shape is
`sh:or([sh:datatype xsd:decimal] [sh:datatype vcfr:Null])`, and RML cannot pick
a datatype per row:

| QUAL value | Emitted as |
| --- | --- |
| a number | `"<lexical form>"^^xsd:decimal` — the source lexical form, so no precision is gained or lost |
| `.` or empty | `"."^^vcfr:Null` |
| anything else | a plain string literal, deliberately kept rather than dropped, and reported by the SHACL layer |

`--info-representation structured` (the default) additionally emits one
`vcfr:InfoFieldValue` per record and key at
`…#call/{ROW_ID}/info/{KEY}`, linked to the `##INFO` declaration through
`vcfr:declaredBy`. A single-valued field (`Number=1`) whose declared `Type` is
`Integer` or `Float` also gets a typed `fieldValueInteger` / `fieldValueDecimal`;
a `Flag` gets `vcfr:fieldValueBoolean true`. A multi-valued field
(`Number=A/R/G/.`) keeps only the lexical value, because the vocabulary's IRI
template gives one node per key, not per value — a real modelling gap, recorded
in [`limitations.md`](limitations.md).

`--info-representation raw` emits only the opaque `vcfr:infoRaw` string.

### `append_header_representation_rdf` — structured headers

`--header-representation structured` (the default) types each `##` line with its
vocabulary subclass (`INFOHeaderLine`, `ContigHeaderLine`, `FilterDefinition`,
`AltDefinition`, …) and lifts the `<ID=…,Number=…,Type=…,Description=…>`
attributes into their own properties. The attribute parser respects quoting and
backslash escapes, so a `Description` containing a comma is handled correctly.

An **unrecognised** `##` key keeps only the base `vcfr:HeaderLine` type.
Inventing a subclass would put a term in the graph that the vocabulary does not
define.

`##fileDate` is emitted as `xsd:date` when its form is recognisable, and
verbatim as a plain literal otherwise — again preserved rather than dropped, and
reported by SHACL.

`--header-representation basic` keeps only the base header-line triples.

### `emit_sample_representation` — genotypes

Exactly one sample emitter runs per conversion, chosen by
`--sample-representation`. Full treatment in
[`sample-representation-guide.md`](sample-representation-guide.md); the short
version:

| Mode | Shape | Growth |
| --- | --- | --- |
| `expanded` (default) | one `vcfr:SampleCall` per record × sample, one `vcfr:FormatFieldValue` per FORMAT key | ≈ variants × samples × FORMAT fields |
| `condensed` | one reusable `vcfr:SampleSet`, one `vcfr:CohortCallMatrix` per call, one `vcfr:FormatValueVector` per FORMAT key holding a tab-separated `vcfr:VCFTextVector` | ≈ samples + variants × FORMAT fields |

The file declares which shape it carries via `vcfr:representationProfile`, so a
consumer can branch on it before querying.

## 5. The two helper tables

`sample_calls.tsv` and `sample_format_values.tsv` exist so that a mapping *can*
express genotypes in RML if it wants to. Under the shipped mapping they stay
header-only and the wrapper streams genotypes directly, because materializing
them means one row per variant × sample (× FORMAT key) — the largest
intermediate the pipeline can produce.

A custom mapping that consumes them in any other way forces full
materialization, and is **rejected** in `--sample-representation condensed`,
which would otherwise emit both genotype representations into one graph.

## 6. IRI templates

Every IRI is derived from the source filename and the row counter, so a
conversion is deterministic and re-running it produces byte-comparable subjects.

| Resource | Template |
| --- | --- |
| `VCFFile` | `file://{SOURCE_FILE}` |
| `VCFHeader` | `file://{SOURCE_FILE}#header` |
| `HeaderLine` | `file://{SOURCE_FILE}#header/line/{HEADER_INDEX}` |
| `VCFRecord` | `file://{SOURCE_FILE}#record/{ROW_ID}` |
| `VariantCall` | `file://{SOURCE_FILE}#call/{ROW_ID}` |
| `InfoFieldValue` | `file://{SOURCE_FILE}#call/{ROW_ID}/info/{KEY}` |
| `SampleCall` (expanded) | `file://{SOURCE_FILE}#sample/{ROW_ID}/{SAMPLE}` |
| `FormatFieldValue` (expanded) | `file://{SOURCE_FILE}#sample/{ROW_ID}/{SAMPLE}/fmt/{KEY}` |
| `SampleSet` (condensed) | `file://{SOURCE_FILE}#samples` |
| `VCFSample` (condensed) | `file://{SOURCE_FILE}#samples/{SAMPLE}` |
| `CohortCallMatrix` (condensed) | `file://{SOURCE_FILE}#call/{ROW_ID}/matrix` |
| `FormatValueVector` (condensed) | `file://{SOURCE_FILE}#call/{ROW_ID}/matrix/fmt/{KEY}` |

`{SOURCE_FILE}` is the **basename**, not a path, so a graph does not encode
where the VCF happened to live. The consequence is that two different VCFs with
the same filename mint the same IRIs; if you convert `chr1/data.vcf` and
`chr2/data.vcf`, their graphs will collide when merged. Rename before
converting, or keep the graphs separate.

## 7. Missing values

The vocabulary's `vcfr:missingValuePolicy` says a missing token should be
`"."^^vcfr:Null`, and the conversion follows it. `preflight_missing_token_conformance`
reports a plain `"."` literal as an anomaly, and `--strict-conformance` promotes
that from a report to a failure.

This policy **conflicted with the published SHACL shapes** for `alt`, which
constrained it to `sh:datatype xsd:string` so that a record with `ALT=.` could
satisfy neither. Vocabulary v1.1.0 resolves it by drawing the boundary the
policy was missing: ALT and ID admit the missing token in VCF 4.5 and their
shapes now accept `xsd:string` or `vcfr:Null`, while CHROM, POS and REF are
required, have no missing form, and keep an exact datatype. The conversion
already followed the policy and is unchanged. See
[`vcf-coverage.md`](vcf-coverage.md#vocabulary-alignment).

## 8. What conversion does *not* do

- **No normalization.** No left-alignment, no trimming, no multi-allelic
  splitting. `ALT=A,T` stays one record with one `alt` literal. This is
  deliberate — the graph is a faithful transcription of the file — but it means
  the graph is not directly joinable with normalized external resources. This is
  the central problem the [data-linking design](datalinking-design.md) has to
  solve.
- **No reference checking.** `REF` is not verified against any genome.
- **No structural-variant modelling.** Symbolic ALTs (`<DEL>`), breakends
  (`N[chr2:321[`) and `*` are carried as literals. The validation suite
  classifies them (`SYMBOLIC_OR_BREAKEND`) but the vocabulary gives them no
  structure, and `INFO` keys such as `END` and `SVTYPE` get no special meaning.
- **No genotype interpretation.** A `GT` value is a lexical token. Phasing
  (`|` versus `/`) is preserved in the literal but not modelled.
- **No cross-file merging.** Each VCF produces its own graph.

---

## See also

- [VCF coverage matrix](vcf-coverage.md) — element-by-element, with the validation check that covers each
- [Sample representations](sample-representation-guide.md) — the genotype shapes in depth
- [Custom RML mappings](rml-mappings.md) — changing what is emitted
- [Limitations](limitations.md) — consolidated
