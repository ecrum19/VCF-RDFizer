# Tier 2: normalised alleles to NCBI SPDI

This plug-in gives the same variant the same IRI in every file. Each record
with explicit alleles gets one `vcfl:sameVariantAs` link per ALT, to its
[SPDI](https://www.ncbi.nlm.nih.gov/variation/notation/) expression under NCBI
Variation Services:

```text
chr17  43045712  G  A     ->  https://api.ncbi.nlm.nih.gov/variation/v0/spdi/NC_000017.11:43045711:G:A
17     43045712  G  A     ->  (the same IRI)
```

It runs offline with no code. `linker.ttl` declares `vcfl:AlleleJoin` and a
digest-pinned sequence map, [`grch38-refseq.tsv`](grch38-refseq.tsv). The map
lists the 25 GRCh38 primary-assembly sequences (chr1–22, X, Y, M) by RefSeq
accession and length, with every contig name that denotes each one (`chr17`
and `17`). The accessions and lengths were checked against NCBI nuccore
(GRCh38.p14).

```bash
vcf-rdfizer --mode link --rdf sample.nt.gz --link spdi --offline -o linked/
vcf-rdfizer-link init --example spdi -o my-spdi      # to adapt it: another assembly or template
vcf-rdfizer-link dry-run my-spdi -i sample.vcf --limit 20
```

## What an identifier means here

- **Trimmed, not canonical.** The expression is the record's REF/ALT with the
  shared suffix and then the shared prefix removed, and a 0-based position.
  NCBI's *canonical* SPDI shifts an indel across its whole repeat, and that
  needs the reference sequence, which this plug-in does not read. Two files
  get the same IRI when they write the variant the same way, so **normalise
  every input identically first**:

  ```bash
  bcftools norm -f GRCh38.fa -m -any in.vcf.gz -Oz -o normalised.vcf.gz
  ```

  Left-aligned inputs give left-aligned identifiers. Suffix-first trimming
  never moves a left-aligned indel to the right. On seven expert-panel ClinVar
  BRCA1 variants, one or two per variant class, the SNV, insertion and two
  delins identifiers are NCBI's exactly. The deletion, duplication and
  microsatellite, all in repeats, differ from NCBI's contextual strings but
  denote the same allele. The known-answer suite is in
  [vcf-rdfizer-testing `plugin-tests/spdi/`](https://github.com/ecrum19/vcf-rdfizer-testing/tree/main/plugin-tests).
- **REF is not checked** against the reference sequence. The linkset records
  this as its assertion basis, and the link is not counted as verified. A
  position beyond the sequence's length fails the run, since it means the input
  is not on this assembly.
- **Multi-allelic records** get one link per ALT, all from the same call.
  Split them first (`-m -any` above) for one identifier per call.
- **Not linked:** symbolic alleles (`<DEL>`), breakends, `*`, missing `.`,
  alleles with bases other than `ACGTN`, and records on contigs the map does
  not list (alts, patches, decoys). Such records are counted in the report
  rather than dropped silently.

The input assembly is checked before anything is linked. A file whose
`##reference` names GRCh37 or hg19 is refused, and one that names no assembly
needs `--assembly GRCh38` after you have checked it.

The IRI dereferences to NCBI's description of the expression. The runner
itself never contacts NCBI: it builds the IRI from the pinned table.
