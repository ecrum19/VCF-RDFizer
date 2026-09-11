# Tier 3 — Ensembl variation service

`resolver.py` sends one POST per deduplicated batch to Ensembl's
[variation endpoint](https://rest.ensembl.org/documentation/info/variation_post).
It links returned identifiers to Ensembl variation pages; it does not add
clinical significance, frequencies, or allele-level equivalence assertions.

```bash
vcf-rdfizer-link init --example rsid-ensembl -o my-api-linker
vcf-rdfizer-link check my-api-linker
vcf-rdfizer-link dry-run my-api-linker -i sample.vcf --limit 100

vcf-rdfizer-link run -i sample.vcf --link rsid-ensembl \
  --links-contact-email you@your-institution.org \
  --links-cache ./api-cache -o sample.ensembl.links.nt
```

Set the contact address before a live cache miss. The shipped budget is 2
requests/second, 100 total HTTP attempts per invocation, 100 rsIDs per batch;
retries consume the ceiling. Use filtered inputs. The runner's session owns
HTTPS access, caching, host pacing and retries; the resolver imports no HTTP
client. The session is a policy API, not a sandbox for untrusted Python.

`dry-run` reports batches without calling the service or executing the resolver.
An offline replay needs an existing matching cache. A missing ID is skipped;
malformed responses, service failures and exhausted budgets abort the linker.
The example's code/manifest are MIT; service terms are linked in the manifest.

The source repository's `docs/datalinking.md` documents the protocol and limits.
