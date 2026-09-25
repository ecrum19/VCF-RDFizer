# Policy demonstrator (v0.1.0)

One synthetic cohort, one set of ODRL policies, three requesters, and three
different release views, each checked against the source VCFs. The design, and
what v0.1.0 deliberately leaves out, is
[`docs/policy-demonstrator.md`](../../docs/policy-demonstrator.md).

This is **governed release, not anonymization.** The views keep the original
IRIs, and a released genotype still identifies the person it came from.

## Run it

```bash
examples/policy/run_demo.sh /tmp/policy-demo     # needs rdflib; no Docker
PROFILE=condensed examples/policy/run_demo.sh /tmp/policy-demo-condensed
```

It explains the policy, attaches it to the graph, and then for each requester
evaluates a view, checks it, and finally prints the decision grid:

```text
requester P001.vcf  P002.vcf  P003.vcf  P004.vcf  P005.vcf  triples withheld
alz       22        16        18        withheld  20        4649
clinical  33        23        withheld  withheld  withheld  6539
gru       21        16        withheld  withheld  withheld  7869
```

Each number is the records released from that file. Where P001 and P002 differ
between requesters, that's the cohort rules at work. *BRCA1* records are
released only for clinical care, and the APOE ε4 variant (rs429358) only for
disease-specific research.

## What is here

| File | What it is |
| --- | --- |
| `make_fixture.py` | Deterministic generator for the five VCFs and `fixture.json` |
| `P001.vcf` … `P005.vcf` | One synthetic participant each, GRCh38, 23–34 records |
| `fixture.json` | Seed, loci, each participant's consent, and the three requesters |
| `policy.ttl` | Five consent policies (one per file) and the cohort policy (*BRCA1*, ε4) |
| `converted/` | The VCFs converted once per sample profile, so nothing needs Docker; see `PROVENANCE.json` |
| `run_demo.sh` | The walkthrough above |

Positions are real GRCh38 coordinates in real loci. rs429358 and rs7412 are
the real variants. Every other allele, and every genotype, is synthetic. To
regenerate the fixture, run `python3 examples/policy/make_fixture.py`. A test
checks that the committed files are byte-identical to what it writes.

## The pieces, one at a time

```bash
vcf-rdfizer-policy explain  --policy policy.ttl
vcf-rdfizer-policy attach   --rdf converted/expanded/P00*.nt.gz --policy policy.ttl -o annotated.nt
vcf-rdfizer-policy evaluate --rdf converted/expanded/P00*.nt.gz --policy policy.ttl \
    --assignee https://example.org/party/alz-consortium --purpose DUO:0000007 -o views/alz
vcf-rdfizer-policy check    --view views/alz --policy policy.ttl --vcf P00*.vcf
```

`evaluate` writes the following into its output directory:
- `view.nt`, the released triples;
- `decisions.csv`, one row per record with the rule that decided it;
- `summary.json`, the counts;
- `manifest.ttl`, the policy's digest, the request, what was withheld, the
  obligations the requester accepted, and `"governed release; not
  anonymization"`.

`check` re-derives the expected release straight from the VCF text and fails
on any difference. An extra record counts as a leak, and a missing one as
over-withholding.
