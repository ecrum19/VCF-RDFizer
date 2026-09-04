# Expanded and Condensed Knowledge Representations

*Part of the [VCF-RDFizer documentation](README.md). How these triples are emitted:
[`conversion.md`](conversion.md). Where section 7's assessment leads:
[`roadmap.md`](roadmap.md#6-query-time-decoding-for-condensed-graphs).*

This guide explains the two ways VCF-RDFizer can represent sample genotype
information in RDF:

- **expanded**, the default representation;
- **condensed**, selected with `--sample-representation condensed`.

The word *representation* here means the shape of the RDF graph. It does not
mean that the input VCF is changed or that genotype values are discarded.

## 1. Why sample representation matters

A VCF is naturally a wide table. A small part of a multi-sample VCF might look
like this:

```text
#CHROM  POS  REF  ALT  FORMAT       SAMPLE_A          SAMPLE_B       SAMPLE_C
20      100  A    G    GT:DP:AD     0/1:42:30,12      0/0:18:18,0   1/1:9:9,0
```

There is one variant row, followed by one sample column for every individual.
The `FORMAT` column says that each sample value has three fields:

- `GT`: genotype;
- `DP`: read depth;
- `AD`: allele depths.

For a real cohort, the numbers can be much larger. For example, a file with
10,000 variants, 2,500 samples, and three FORMAT fields contains:

```text
10,000 × 2,500 × 3 = 75,000,000 individual FORMAT values
```

RDF can describe each of those values separately, but doing so creates a very
large number of RDF resources and relationships. The two modes let users
choose between a graph that is easy to query one value at a time and a graph
that is much more economical for large cohorts.

## 2. Expanded representation: one RDF object per sample value

Expanded mode preserves the original, explicit vocabulary model. For every
variant/sample pair, VCF-RDFizer creates a `vcfr:SampleCall`. For every FORMAT
field in that pair, it creates a `vcfr:FormatFieldValue`.

For the example above, the conceptual graph includes resources like these
(the full output has one line per RDF statement):

```text
<file://cohort.vcf#call/1>
    vcfr:hasSampleCall <file://cohort.vcf#sample/1/SAMPLE_A> .

<file://cohort.vcf#sample/1/SAMPLE_A>
    a vcfr:SampleCall ;
    vcfr:sampleId "SAMPLE_A" ;
    vcfr:hasFormatValue <file://cohort.vcf#sample/1/SAMPLE_A/fmt/GT> .

<file://cohort.vcf#sample/1/SAMPLE_A/fmt/GT>
    a vcfr:FormatFieldValue ;
    vcfr:fieldValue "0/1" .
```

The same structure is repeated for `DP` and `AD`, and then repeated again for
`SAMPLE_B` and `SAMPLE_C`.

### What expanded mode provides

Expanded mode makes each value easy to address directly:

- a consumer can find one sample call without decoding a vector;
- a consumer can ask for one FORMAT field as one RDF resource;
- existing consumers that expect `SampleCall` and `FormatFieldValue` can use
  the graph directly;
- the sample identifier is attached to every sample-call resource.

The VCF file is marked with
`vcfr:representationProfile vcfr:ExpandedRepresentation`, so a consumer can
identify the graph shape explicitly.

### Expanded growth

Let:

- `V` = number of variant records;
- `S` = number of samples;
- `F` = average number of FORMAT fields per record.

Expanded mode creates approximately:

```text
V × S       SampleCall resources
V × S × F   FormatFieldValue resources
```

In the simplified case where each sample call has three structural statements
(link, type, and sample ID), and each FORMAT value has three statements (link,
type, and value), the sample portion of the graph contains approximately:

```text
3 × (V × S) + 3 × (V × S × F)
```

statements, before counting the rest of the VCF graph.

For the worked example (`V=1`, `S=3`, `F=3`):

| Expanded item | Count |
|---|---:|
| `SampleCall` resources | 3 |
| `FormatFieldValue` resources | 9 |
| Approximate sample-related statements | 36 |
| Representation-profile statement | 1 |

The exact number can vary when values are missing or when custom mappings add
properties, but the important pattern is the multiplication by both samples
and FORMAT fields.

## 3. Condensed representation: shared samples plus ordered vectors

Condensed mode avoids creating a separate RDF resource for every scalar sample
value. It describes the sample columns once, then stores the values for each
variant and FORMAT key in one ordered vector.

For the same example, the graph first declares a reusable sample set:

```text
<file://cohort.vcf#samples>
    a vcfr:SampleSet ;
    vcfr:hasSample <file://cohort.vcf#samples/SAMPLE_A> ;
    vcfr:hasSample <file://cohort.vcf#samples/SAMPLE_B> ;
    vcfr:hasSample <file://cohort.vcf#samples/SAMPLE_C> .

<file://cohort.vcf#samples/SAMPLE_A>
    a vcfr:VCFSample ;
    vcfr:sampleName "SAMPLE_A" ;
    vcfr:sampleIndex "1" .
```

The variant then has one cohort matrix. The matrix points back to the sample
set and has one vector for each FORMAT key:

```text
<file://cohort.vcf#call/1/matrix>
    a vcfr:CohortCallMatrix ;
    vcfr:appliesToSampleSet <file://cohort.vcf#samples> ;
    vcfr:hasFormatValueVector <file://cohort.vcf#call/1/matrix/fmt/GT> .

<file://cohort.vcf#call/1/matrix/fmt/GT>
    a vcfr:FormatValueVector ;
    vcfr:valueEncoding vcfr:VCFTextVector ;
    vcfr:encodedValues "0/1\t0/0\t1/1" .
```

The three tab-separated items in `encodedValues` are in `sampleIndex` order:

| `sampleIndex` | Sample | `GT` value in the vector |
|---:|---|---|
| 1 | `SAMPLE_A` | `0/1` |
| 2 | `SAMPLE_B` | `0/0` |
| 3 | `SAMPLE_C` | `1/1` |

The `DP` vector is `"42\t18\t9"`, and the `AD` vector is
`"30,12\t18,0\t9,0"`. A comma inside one VCF value remains inside that item;
it is not treated as a sample separator. If a value is absent, the vector
uses `.` to keep every sample position aligned.

The file is marked with
`vcfr:representationProfile vcfr:CondensedRepresentation`.

### What condensed mode provides

- Sample resources are declared once per file, not once per variant.
- Each variant has one `CohortCallMatrix`, rather than one sample-call
  resource for every sample.
- Each variant/FORMAT-key pair has one `FormatValueVector`, rather than one
  `FormatFieldValue` resource per sample.
- The original sample order and all lexical values remain available.
- A vector-aware consumer can reconstruct sample `i` by looking up the sample
  with `sampleIndex=i` and selecting item `i` from the corresponding vector.

Condensed mode therefore moves repetition from RDF structure into the literal
payload of a vector. It is a storage and graph-shape optimization, not a loss
of genotype information.

### Condensed growth

Using the same variables, condensed mode creates approximately:

```text
S       reusable sample descriptions
V       cohort matrices
V × F   format-value vectors
F       shared FORMAT definitions (per file/header)
```

The RDF structure grows roughly with `S + V + (V × F)`, while each vector
literal still contains the `S` values for that variant and key.

For `V=1`, `S=3`, and `F=3`, the graph has approximately:

| Condensed item | Count |
|---|---:|
| Reusable `VCFSample` resources | 3 |
| `CohortCallMatrix` resources | 1 |
| `FormatValueVector` resources | 3 |
| Shared FORMAT definitions | 3 |
| Vector literals containing sample values | 3 |

Because a condensed graph has fixed metadata for the sample set, matrix, and
FORMAT definitions, it can be similar in size to—or even slightly larger than—
an expanded graph for a tiny file. Its advantage appears when many variants reuse
the same sample columns.

## 4. The scaling difference in numbers

Assume a cohort has:

```text
V = 1,000 variants
S = 2,500 samples
F = 3 FORMAT fields
```

### Expanded counts

```text
SampleCall resources:       1,000 × 2,500 = 2,500,000
FormatFieldValue resources: 1,000 × 2,500 × 3 = 7,500,000
Total per-value resources:  10,000,000
```

Using the simplified three-statements-per-resource estimate, this is about
30,000,000 sample-related RDF statements.

### Condensed counts

```text
Reusable samples:       2,500
Cohort matrices:        1,000
Format-value vectors:   1,000 × 3 = 3,000
FORMAT definitions:     3
```

That is roughly 6,500 resources in the sample representation, plus vector
literals containing the same 7,500,000 scalar values. Counting the explicit
statements emitted by the built-in condensed writer gives about 28,000
sample-related statements for this simplified case (the exact total depends on
header metadata and other record properties).

So the RDF *structure* is reduced by roughly:

```text
30,000,000 ÷ 28,000 ≈ 1,070×
```

This does not mean the compressed file will always be 1,070 times smaller. The
75 million values still exist inside vector literals, and their text length,
escaping, dictionary compression, and the rest of the VCF graph affect the
final `.nt`, `.hdt`, or `.cottas` size. The large saving is the removal of
millions of repeated RDF nodes, predicates, and type statements.

## 5. Why VCF-RDFizer keeps both strategies

The modes serve different downstream needs. Combining them into one universal
graph would either make small files unnecessarily complicated or make large
cohorts unnecessarily expensive.

### Expanded is useful when direct RDF querying is the priority

Expanded mode is a good fit when:

- the VCF has one sample or a small number of samples;
- downstream software already expects `SampleCall` and
  `FormatFieldValue` resources;
- queries need to match one sample value directly, without splitting a vector;
- maximum vocabulary-level transparency is more important than graph size.

For example, a consumer can navigate directly from a call to the sample whose
`sampleId` is `SAMPLE_A`, then to its `GT` `FormatFieldValue`. Every value is a
normal RDF resource and literal.

### Condensed is useful when cohort scale is the priority

Condensed mode is a good fit when:

- there are hundreds or thousands of samples;
- the same sample columns repeat across many variant rows;
- materializing one RDF object per sample/variant/FORMAT combination causes
  excessive memory, disk, or indexing work;
- downstream software can use `sampleIndex` and decode tab-separated vectors.

For a 2,500-sample cohort, expanded mode creates 2,500 sample-call resources for
*every* variant. Condensed mode creates the 2,500 sample descriptions once and
reuses them for all variants.

### The semantic trade-off

| Question | Expanded | Condensed |
|---|---|---|
| Where is each sample value? | Its own RDF resource | An item in an ordered vector literal |
| How often are samples declared? | Repeated per variant/sample call | Once per file/sample set |
| Simple one-value RDF query | Easier | Requires vector lookup/decoding |
| Large-cohort graph size | Grows with `V × S × F` | Structure grows roughly with `S + V × F` |
| All source values retained? | Yes | Yes |
| Vocabulary profile | `ExpandedRepresentation` | `CondensedRepresentation` |

Neither mode is universally “more correct.” They expose the same underlying
VCF information through different graph contracts.

## 6. How this is implemented in VCF-RDFizer

The command-line choice is explicit:

```bash
# Expanded is the default
vcf-rdfizer --mode full --input cohort.vcf.gz \
  --sample-representation expanded \
  --rdf-storage-mode plain --out results

# Condensed is intended for large multi-sample cohorts
vcf-rdfizer --mode full --input cohort.vcf.gz \
  --sample-representation condensed \
  --rdf-storage-mode space-optimized \
  --representations hdt --out results
```

There is no automatic “switch at N samples” threshold. This is deliberate:
the same command should produce the same graph contract every time, and a
downstream consumer should not unexpectedly receive a different RDF shape
just because a file happened to contain more samples.

For the built-in rules, both modes read the wide sample payload in
`records.tsv` and stream the selected RDF representation directly. The current
implementation does **not** first materialize the enormous helper
tables for the built-in maps. That implementation optimization is separate
from the semantic choice:

- expanded still emits the expanded `SampleCall`/`FormatFieldValue` graph;
- condensed still emits the condensed `SampleSet`/`CohortCallMatrix`/
  `FormatValueVector` graph.

Custom rules that consume `sample_calls.tsv` or
`sample_format_values.tsv` retain the materialized helper-table behavior. Such
custom helper-table mappings are rejected in condensed mode, because running them
alongside the condensed emitter would produce both graph shapes and recreate
the expansion that condensed mode is intended to avoid.

## 7. Assessment of GeoSPARQL and GraphDB SPARQL extensions

GraphDB's [SPARQL extensions reference](https://graphdb.ontotext.com/documentation/11.5/sparql-ext-functions-reference.html)
contains several different kinds of functionality. They should not all be
treated as ways to read a condensed VCF vector.

### 7.1 GeoSPARQL geometry functions are not a direct match

The standard `geof:` functions in the GraphDB reference operate on a
`geomLiteral`, such as a literal with the datatype `geo:wktLiteral` or
`geo:gmlLiteral`. They perform geometry operations such as distance, buffer,
intersection, union, envelope, and spatial relationship tests. GraphDB's
additional `geoext:` functions provide operations such as area, geometry
validity, simplification, and Hausdorff distance. These functions are intended
for points, lines, polygons, and other spatial objects, not arbitrary ordered
text values. See the [GeoSPARQL function table](https://graphdb.ontotext.com/documentation/11.5/sparql-ext-functions-reference.html#geosparql-functions)
and [GraphDB GeoSPARQL extensions](https://graphdb.ontotext.com/documentation/11.5/sparql-ext-functions-reference.html#geosparql-extension-functions).

Our condensed value is instead a normal RDF string associated with
`vcfr:VCFTextVector`, for example:

```text
"0/1\t0/0\t1/1"
```

It is not a WKT geometry.

The analogy with WKT remains useful at the design level: both pack a sequence
into one literal. The GeoSPARQL operations themselves, however, are not
reusable for genotype-vector extraction.

### 7.2 The useful GraphDB feature is string splitting

The reference also documents the GraphDB/SPIN magic predicate `spif:split`.
It takes a string and a regular expression and produces one result row for
each split item. The implementation is described as using Java's
`String.split()` method. A prototype query for all tokens in condensed vectors
could look like this:

```sparql
PREFIX vcfr: <https://w3id.org/vcf-rdfizer/vocab#>
PREFIX spif: <http://spinrdf.org/spif#>

SELECT ?vector ?token WHERE {
  ?vector a vcfr:FormatValueVector ;
          vcfr:valueEncoding vcfr:VCFTextVector ;
          vcfr:encodedValues ?encoded .
  ?token spif:split (STR(?encoded) "\t") .
}
```

For a `GT` vector, this would expose `0/1`, `0/0`, and `1/1` as separate query
bindings. It is useful for experimentation, validation, or exporting values
to an application.

There is an important limitation: splitting gives the values, but the query
also needs the ordinal position of each value in order to identify the sample.
The condensed model says that the first token belongs to `sampleIndex=1`, the
second to `sampleIndex=2`, and so on. The documented `spif:split` pattern does
not itself return that ordinal. GraphDB also documents `spif:for`, which can
generate a sequence of integers, but the reference does not provide a direct
“zip these generated indexes with the split results” operation. This means
that `spif:split` alone is not a complete, reliable sample lookup mechanism.

The page also lists RDF-list functions such as `list:index` and
`list:length`. Those operate on RDF Collections. `vcfr:encodedValues` is a
single string literal, not an RDF Collection, so these functions do not apply
unless the data model is changed to materialize every vector item as list
structure. That would reintroduce much of the per-item RDF overhead that
condensed mode was designed to avoid. GraphDB's `helper:tuple` and
`helper:iterate` functions similarly operate on internal query-time lists;
they do not automatically parse a persisted VCF vector literal.

### 7.3 What is useful now and what is not

| GraphDB feature | Usefulness for `VCFTextVector` | Assessment |
|---|---|---|
| `geof:*` GeoSPARQL functions | Geometry calculations | Not applicable to genotype text vectors |
| `geoext:*` geometry extensions | Geometry validity and transformations | Not applicable unless a future dataset contains real sample geometries |
| `spif:split` | Split tab-separated vector text | Useful prototype, but loses the token ordinal |
| `spif:for` | Generate expected sample positions | Helpful support function, but does not pair positions with split tokens |
| `list:index` / `list:length` | Index RDF Collections | Not applicable to the current string encoding |
| `helper:tuple` / `helper:iterate` | Work with internal query lists | Not a persisted-vector parser |
| A custom `vcfr:` vector function | Return value at a sample index | Most direct future SPARQL integration |
| Application-side parsing | Decode one selected vector after retrieval | Best portable near-term approach |

GraphDB explicitly labels these as extensions beyond the W3C SPARQL
specification, so a query using `spif:*` or a custom function would be tied to
GraphDB (or to another engine that implements the same extension). That can be
acceptable for a GraphDB deployment, but it should not be presented as a
portable SPARQL solution. These are query-time GraphDB operations; using them
would not change VCF-RDFizer's native HDT creation or indexing path, but their
memory and latency behavior would still need to be benchmarked inside the
GraphDB server.

### 7.4 Recommended future extraction design

If query-time sample lookup becomes an important use case, the most useful
addition would be an extractor that understands the VCF-RDFizer metadata and
returns both the position and the value. Conceptually, it could have a
function or magic-predicate contract like:

```text
vcfr:vectorValue(?vector, ?sampleIndex) → ?value
```

or:

```text
?vector vcfr:valueAt (?sampleIndex ?value)
```

For a sample-aware form, the function could accept the `vcfr:VCFSample` IRI
instead of an integer, resolve that resource's `sampleIndex`, and then extract
the matching token. Returning the index as well as the value would make it
possible to validate that a vector has the expected number of positions.

A production implementation should also consider these safeguards:

1. Verify that the vector belongs to the expected `SampleSet` through its
   `CohortCallMatrix`.
2. Check that the requested index is within the sample-set size.
3. Preserve `.` as the VCF missing-value marker rather than silently turning
   it into an unbound result.
4. Treat tab as the vector separator and keep commas inside a value such as
   `AD=30,12`.
5. Validate that the vector contains exactly one item per declared sample.
6. Expose the function through a GraphDB plugin only when GraphDB-specific
   deployment is acceptable; otherwise provide a small application-side
   decoder or a materialized per-value projection.

There is also a performance question. With 1,000 variants, 2,500 samples, and
three FORMAT keys, condensed mode stores 3,000 vectors but each vector contains
2,500 values. Expanding every vector in a SPARQL query would produce up to

```text
1,000 × 3 × 2,500 = 7,500,000 query result rows
```

before filtering to a particular sample. For a targeted lookup, a function that
extracts one position is preferable to `spif:split` over the entire vector. For
large cohort-wide analyses, an external decoder or a precomputed analytical
table may be more appropriate than forcing a graph database to emit millions
of token bindings.

### 7.5 Overall conclusion

The GraphDB work is useful as a source of implementation ideas, but not as a
drop-in GeoSPARQL solution:

- **GeoSPARQL geometry functions:** no, they should not be used for genotype
  vectors.
- **`spif:split`:** yes, as a prototype tokenizer and validation aid.
- **`spif:split` plus `spif:for`:** potentially useful building blocks, but not
  sufficient for a trustworthy sample/value join without an ordinal pairing
  mechanism.
- **Custom vector extraction function or application decoder:** yes, this is
  the most promising future direction.

The condensed model should therefore remain as it is for storage efficiency,
while future query support should be added as a separate decoding/projection
layer rather than changing genotype vectors into geometries or RDF Lists.

## 8. Practical choice

Use this short rule of thumb:

```text
Small or single-sample VCF + simple RDF queries  → expanded
Large multi-sample cohort + storage/scale focus  → condensed
```

If a consumer is unsure which graph it received, inspect the file’s
`vcfr:representationProfile` value before querying. That profile is the
explicit signal that tells the consumer whether sample values are represented
as individual resources or as ordered vectors.

## Glossary

- **Variant record**: one row describing a genomic position and its alleles.
- **Sample**: one individual or biological sample represented by a VCF column.
- **FORMAT key**: a field name such as `GT`, `DP`, or `AD` describing one part
  of a sample’s value.
- **RDF resource**: a named graph object that can have properties, such as one
  `SampleCall` or one `FormatFieldValue`.
- **Vector**: an ordered list of values. In condensed mode, values are stored
  as one tab-separated `VCFTextVector` literal in `sampleIndex` order.
- **Graph shape**: which resources and relationships are present, independent
  of the underlying biological values.


---

## See also

- [Conversion](conversion.md) — how these triples are emitted, and why not through RML
- [VCF coverage matrix](vcf-coverage.md) — which genotype elements are validated
- [Representations](representations.md) — why condensed matters at cohort scale
- [Roadmap](roadmap.md#6-query-time-decoding-for-condensed-graphs) — where section 7 leads
