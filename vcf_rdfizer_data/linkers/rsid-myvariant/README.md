# Tier 3 — MyVariant.info

`resolver.py` sends one POST per deduplicated batch of up to 1,000 rsIDs to
MyVariant.info's [batch query](https://docs.myvariant.info/en/latest/doc/variant_query_service.html),
scoped to `dbsnp.rsid` on hg38. An rsID is linked to its dbSNP identifier
(`identifiers.org/dbsnp:`, the IRI `rsid-dbsnp` writes without checking) only
when a returned dbSNP record carries it. It adds no alleles, clinical
significance or frequencies.

```bash
vcf-rdfizer-link dry-run rsid-myvariant -i sample.vcf
vcf-rdfizer-link run -i sample.vcf --link rsid-myvariant \
  --links-contact-email you@your-institution.org \
  --links-cache ./api-cache -o sample.myvariant.links.nt
```

The shipped budget is 1 request/second and 30 HTTP attempts per invocation
(about 30,000 rsIDs), with a 120 s read timeout. An unknown ID is skipped;
malformed replies, service failures and exhausted budgets abort the linker.
The code and manifest are MIT; the service's terms, and the licences of the
sources it aggregates, are linked in the manifest and in each reply.
