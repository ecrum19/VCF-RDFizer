# Linking example input

`example.vcf` has four artificial records with a GRCh38 reference declaration.
Its coordinates are chosen to exercise the synthetic `gene-demo` GFF3 intervals;
the rsIDs demonstrate token parsing and are not claims about those coordinates.

```bash
vcf-rdfizer-link run -i examples/linking/example.vcf \
  --link rsid-dbsnp,gene-demo --offline \
  --links-cache ./example-cache -o ./example.links.nt
```

Expected links, excluding provenance:

| VCF row | dbSNP tokens | Demo genes |
| --- | --- | --- |
| 1 | rs334 | A |
| 2 | rs699, rs334 | A, B |
| 3 | none | none |
| 4 | none | C |

Seven links in total. Live resolver usage, caching and authoring are documented
in [`docs/datalinking.md`](../../docs/datalinking.md).
