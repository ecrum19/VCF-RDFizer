# Tier 2 — synthetic gene intervals

This plug-in exercises an interval join without Python. `linker.ttl` declares
the local `genes.gff3`, its SHA-256 and an assembly guard. The genes and their
coordinates are **synthetic**, licensed with the example under MIT. The GRCh38
label exists to test assembly matching; these are not biological annotations.

```bash
vcf-rdfizer-link init --example gene-demo -o my-gene-linker
vcf-rdfizer-link check my-gene-linker --offline
vcf-rdfizer-link dry-run my-gene-linker -i sample.vcf --limit 100
```

Intervals are 1-based closed; keys cover POS through POS + len(REF) - 1. The
example contains overlapping genes A/B and a gene C on chromosome 2. Exons are
ignored. Chromosome names match exactly. Symbolic alleles are skipped.

For a real reference, change the plug-in ID, reference URL, actual SHA-256,
assembly and object template. Point `idAttribute` at the GFF3 attribute your
template needs; do not assume every publisher's `ID` is an Ensembl gene ID.
HTTPS and local file URLs are supported. Gzip digests cover compressed bytes.
Keep the reference's licensing and terms visible in the manifest.

The source repository's `docs/datalinking.md` explains caching and assembly
refusal, including the limited role of `--assembly`.
