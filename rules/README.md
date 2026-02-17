# Rules Directory

This directory contains RML mappings used by the conversion pipeline.

## Files

- `default_rules.ttl`
  - Active default mapping for this repository.
  - Targets the VCF-RDFizer vocabulary (`https://w3id.org/vcf-rdfizer/vocab#`).
  - Uses template TSV paths:
    - `/data/tsv/file_metadata.tsv`
    - `/data/tsv/header_lines.tsv`
    - `/data/tsv/records.tsv`
  - The Python wrapper rewrites these template paths per input VCF to:
    - `/data/tsv/<sample>.file_metadata.tsv`
    - `/data/tsv/<sample>.header_lines.tsv`
    - `/data/tsv/<sample>.records.tsv`

- `rules.ttl`
  - Preserved legacy mapping from the previous repository layout.
  - Kept for comparison and migration reference.

## How To Create A Custom Mapping

1. Copy `default_rules.ttl` to a new file (for example `my_rules.ttl`).
2. Keep TSV source expectations aligned with `src/vcf_as_tsv.sh` unless you also customize that script.
3. Add new `rr:TriplesMap` blocks for additional properties/classes.
4. Preserve stable subjects (`vcf-file/{SOURCE_FILE}`, `record/{SOURCE_FILE}/{ROW_ID}`) if you want joins to keep working.
5. Run the wrapper with your custom mapping:

```bash
python3 vcf_rdfizer.py --input <vcf-or-dir> --rules rules/my_rules.ttl
```

## SHACL Notes

The related SHACL constraints are maintained in the vocabulary repository:

- [vcf-rdfizer-vocabulary.shacl.ttl](https://github.com/ecrum19/VCF-RDFizer-vocabulary/blob/main/shacl/vcf-rdfizer-vocabulary.shacl.ttl)

The default mapping is structured to align with those classes/properties, especially:

- `vcfr:VCFFile` + `vcfr:hasHeader`
- `vcfr:VCFHeader` + `vcfr:hasHeaderLine`
- `vcfr:VCFRecord` core fields (`chrom`, `pos`, `ref`, `alt`)
- `vcfr:VariantCall` with raw call attributes
