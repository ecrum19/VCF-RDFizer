# Rules Directory

This directory contains RML mappings used by the conversion pipeline.

Writing your own mapping? Use the `vcf-rdfizer-rules` CLI, which documents the
available TSV columns, scaffolds a starting point, and validates a mapping
against the wrapper's contract before you spend a run on it:

```bash
vcf-rdfizer-rules columns          # what a mapping can reference
vcf-rdfizer-rules init -o my.ttl   # annotated copy of default_rules.ttl
vcf-rdfizer-rules check my.ttl     # validate before running the pipeline
```

See "Custom RML Mappings" in the top-level `README.md` for the full contract.

## Files

- `default_rules.ttl`
  - Active default mapping for this repository.
  - Targets the VCF Core vocabulary (`https://w3id.org/vcf-core/vocab#`,
    prefix `vcfc:`), which replaces the retired
    `https://w3id.org/vcf-rdfizer/vocab#`. No version IRI is pinned.
  - Uses template TSV paths:
    - `/data/tsv/file_metadata.tsv`
    - `/data/tsv/header_lines.tsv`
    - `/data/tsv/records.tsv`
    - `/data/tsv/sample_calls.tsv`
    - `/data/tsv/sample_format_values.tsv`
  - For the built-in sample maps, `sample_calls.tsv` and
    `sample_format_values.tsv` are header-only compatibility sources. In
    `--sample-representation expanded`, the Python wrapper streams their equivalent
    `SampleCall` and `FormatFieldValue` triples directly from `records.tsv`. In
    `--sample-representation condensed`, it instead streams `SampleSet`,
    `CohortCallMatrix`, and `FormatValueVector` resources. Only one emitter runs.
  - Custom mappings with additional consumers of either helper source retain
    materialized TSV generation in expanded mode. They are rejected in condensed mode
    to prevent simultaneous expanded and condensed output.
  - The Python wrapper rewrites these template paths per input VCF to:
    - `/data/tsv/<sample>.file_metadata.tsv`
    - `/data/tsv/<sample>.header_lines.tsv`
    - `/data/tsv/<sample>.records.tsv`
    - `/data/tsv/<sample>.sample_calls.tsv`
    - `/data/tsv/<sample>.sample_format_values.tsv`

## How To Create A Custom Mapping

1. Copy `default_rules.ttl` to a new file (for example `my_rules.ttl`).
2. Keep TSV source expectations aligned with `src/vcf_as_tsv.sh` unless you also customize that script.
3. Add new `rr:TriplesMap` blocks for additional properties/classes.
4. Preserve stable subjects (`file://{SOURCE_FILE}`, `file://{SOURCE_FILE}#record/{ROW_ID}`) if you want joins to keep working.
5. Run the wrapper with your custom mapping:

```bash
python3 vcf_rdfizer.py --input <vcf-or-dir> --rules rules/my_rules.ttl \
  --rdf-storage-mode plain --out ./results
```

Custom rules that do not consume either sample helper table can be used with
`--sample-representation condensed`; the wrapper adds the condensed sample graph
after RMLStreamer emits the custom record-level graph.

## SHACL Notes

The related SHACL constraints are maintained in the vocabulary repository, in
two profiles:

- `shacl/vcf-core-vocabulary.shacl.ttl` — portable: structural links, VCF 4.5
  lexical patterns, field type and arity constraints.
- `shacl/vcf-core-vocabulary-sparql.shacl.ttl` — cross-resource: header and
  record ordering, sample uniqueness, and the rule that a graph must not mix
  expanded sample calls with condensed call matrices.

Both need the bundled ontology as an ontology graph and RDFS inference enabled.

This mapping emits:

- `vcfc:VCFFile` + `vcfc:hasHeader`, `vcfc:fileFormat`, `vcfc:sourceSoftware`,
  `vcfc:referenceGenome`
- `vcfc:VCFHeader` + `vcfc:hasHeaderLine`, and `vcfc:hasColumnHeader` to the
  `#CHROM` `vcfc:ColumnHeaderLine`
- `vcfc:HeaderLine` with `headerKey`, `headerValue` and `lineIndex`
- `vcfc:VCFRecord` with `chrom`, `pos`, `ref`, `recordIndex` and `hasCall`
- `vcfc:VariantCall` with `formatRaw`
- the expanded `vcfc:SampleCall` / `vcfc:FormatFieldValue` compatibility maps

`ID`, `ALT`, `QUAL`, `FILTER` and `infoRaw` are **not** here. Each may be the
VCF missing token, which the vocabulary requires as `"."^^vcfc:Null`, and RML
cannot switch an object's datatype per row — so the wrapper emits them, along
with the header attributes, the allele layer, structured INFO, the SV carriers
and the genotype layer. The rule is stated at the top of `default_rules.ttl`:
**RML carries every field whose datatype is the same for every row; the wrapper
carries everything else.**
