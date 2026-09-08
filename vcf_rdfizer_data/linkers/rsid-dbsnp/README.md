# Tier 1 — rsID to dbSNP

This entire plug-in is `linker.ttl`: split VCF ID on semicolons, accept complete
`rs` + digits tokens, and emit a `vcfl:sameVariantAs` link to the dbSNP IRI.
There is no resolver, reference or network. This constructs identifier links;
it does not check whether an identifier is current or retired.

```bash
vcf-rdfizer-link init --example rsid-dbsnp -o my-rsid-linker
vcf-rdfizer-link check my-rsid-linker
vcf-rdfizer-link dry-run my-rsid-linker -i sample.vcf --limit 100
```

To read INFO tokens, change `field` to `INFO/<key>` and select the delimiter.
To change the target, edit `objectTemplate` while retaining `{TOKEN}`. Change
`id` before installing a copy beside this example. The runner escapes the
token, owns the subject and removes duplicate triples.

The source repository's `docs/datalinking.md` documents the full contract.
