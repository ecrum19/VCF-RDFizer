# Tier 2: overlapping Ensembl genes (GRCh38)

Links each record to every Ensembl release 116 gene its REF span overlaps
(`vcfl:overlapsGene <https://identifiers.org/ensembl:ENSG…>`). The GFF3 is
fetched once from Ensembl over HTTPS (108 MB), checked against its SHA-256, and
cached; later runs are offline.

Contig names are resolved through the GRCh38 sequence map
([`grch38-refseq.tsv`](grch38-refseq.tsv), shared with `spdi`), so the GFF3's
`17` and a VCF's `chr17` meet. Only `gene` features count (21,581 in release
116, including all protein-coding genes); `ncRNA_gene` and pseudogenes do not.

A record's span is its REF span whatever its ALT, `*` included. Symbolic
alleles and breakends, whose extent is END or a mate, are not linked; the
report counts them in `skipped_records`.

```bash
vcf-rdfizer-link run -i sample.vcf --link ensembl-genes-grch38 -o sample.links.nt
```
