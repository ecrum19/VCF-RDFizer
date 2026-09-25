# Policy attachment example (v0.1.0)

One synthetic cohort, one set of ODRL policies, three requesters, and three
different release views, each checked against the source VCFs. How the engine
works (select → partition → decide, each configured in Turtle) is in
[`docs/policy-demonstrator.md`](../../docs/policy-demonstrator.md).

This is **governed release, not anonymization.** Views keep the original IRIs,
and a released genotype still identifies the person it came from.

## Run it

```bash
examples/policy/run_demo.sh /tmp/policy-demo     # needs rdflib; no Docker
PROFILE=condensed examples/policy/run_demo.sh /tmp/policy-demo-condensed
```

It explains the policy, attaches it to the graph, and then for each requester
evaluates a view and checks it. Finally it prints the decision grid:

```text
requester P001.vcf  P002.vcf  P003.vcf  P004.vcf  P005.vcf  triples withheld
alz       22        16        18        withheld  20        4649
clinical  33        23        withheld  withheld  withheld  6539
gru       21        16        withheld  withheld  withheld  7869
```

Each number is the records released from that file. Where P001 and P002 differ
between requesters, that's the cohort rules at work. *BRCA1* records are
released only for clinical care, and the APOE ε4 variant (rs429358) only for
disease-specific research. P004 withdrew.

## What is here

| File | What it is |
| --- | --- |
| `make_fixture.py` | Deterministic generator for the five VCFs and `fixture.json` |
| `P001.vcf` … `P005.vcf` | One synthetic participant each, GRCh38, 23–34 records |
| `fixture.json` | Seed, loci, each participant's consent, and the three requesters |
| `policy.ttl` | Five consents (one per file) and the cohort policy (*BRCA1*, ε4) |
| `custom-selector.ttl` | A selector type declared in the policy itself (below) |
| `converted/` | The VCFs converted once per sample profile, so nothing needs Docker; see `PROVENANCE.json` |
| `run_demo.sh` | The walkthrough above |

The positions are real GRCh38 coordinates in real loci, and rs429358 and
rs7412 are the real variants. Every other allele, and every genotype, is
synthetic. To regenerate the fixture, run
`python3 examples/policy/make_fixture.py`.

## The pieces, one at a time

```bash
vcf-rdfizer-policy explain  --policy policy.ttl
vcf-rdfizer-policy attach   --rdf converted/expanded/P00*.nt.gz --policy policy.ttl -o annotated.nt
vcf-rdfizer-policy evaluate --rdf converted/expanded/P00*.nt.gz --policy policy.ttl \
    --assignee https://example.org/party/alz-consortium --purpose DUO:0000007 -o views/alz
vcf-rdfizer-policy check    --view views/alz --rdf converted/expanded/P00*.nt.gz \
    --policy policy.ttl --vcf P00*.vcf
```

`evaluate` writes the following into its output directory:
- `view.nt`, the released triples;
- `decisions.csv`, one row per record with the rule that decided it;
- `summary.json`, the counts;
- `manifest.ttl`, the policy's digest, the request, what was withheld and the
  obligations accepted.

`check` runs the structural checks against the source graph. With `--vcf`, it
also re-derives the expected records straight from the VCF text and fails on
any difference.

## Write your own selector

Selector types are SPARQL, declared in Turtle, and there is nothing to code.
`custom-selector.ttl` declares one inside the policy that uses it:

```turtle
ex:QualityBelow a vcfp:SelectorType ;
    vcfp:parameter ex:threshold ;                      # bound to ?threshold
    vcfp:query """
        PREFIX vcfc: <https://w3id.org/vcf-core/vocab#>
        SELECT ?resource WHERE {
            ?resource a vcfc:VCFRecord ; vcfc:hasCall ?call .
            ?call vcfc:qual ?qual .
            FILTER(?qual < ?threshold) }""" .

ex:low-quality a odrl:Asset , vcfp:GraphSelection ;
    vcfp:selector [ a ex:QualityBelow ; ex:threshold 60 ] .
```

With every file released for general research except that selection, 38 of
the 139 records are withheld:

```bash
vcf-rdfizer-policy evaluate --rdf converted/expanded/P00*.nt.gz --policy custom-selector.ttl \
    --assignee https://example.org/party/anyone --purpose DUO:0000042 -o views/quality
```

The query must project `?resource`. Each `vcfp:parameter` names a property of
the selector node, bound to the variable of the same local name. A selector
type can also carry a `vcfp:violations` query, and any row it returns stops
evaluation; the shipped region and variant selectors use one to check the
assembly.

To share a selector across policies, put it in a file and pass
`--profile vcf-core --profile my-selectors.ttl`.
