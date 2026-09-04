# Semantic VCF/RDF validation

`vcf-rdfizer --mode validation` checks that a converted RDF graph reproduces
six deterministic VCF summaries. It computes one result from the source VCF
using `cyvcf2` (and `bcftools` for exact FILTER strings when available), runs
the equivalent SPARQL queries with Comunica, then compares canonical integer
results exactly.

The mode consumes a single N-Triples gzip aggregate (`.nt.gz`). It mounts that
file read-only, expands it under `/work` **inside the Docker container**, uses
the temporary `.nt` for Raptor syntax validation and Comunica queries, then
removes it before the container exits. No decompressed RDF is written beneath
`--out`; only reports are retained.

## Run it

Use the representation selected when the RDF was created.

```bash
# Expanded (the default VCF-RDFizer graph shape)
vcf-rdfizer --mode validation \
  --input ./vcf_data/HG004_GRCh38.vcf.gz \
  --rdf ./results/HG004_GRCh38/HG004_GRCh38.nt.gz \
  --sample-representation expanded \
  --out ./validation-results

# Condensed multi-sample graph
vcf-rdfizer --mode validation \
  --input ./vcf_data/1000G_phase3_chr20.vcf.gz \
  --rdf ./results/1000G_phase3_chr20/1000G_phase3_chr20.nt.gz \
  --sample-representation condensed \
  --out ./validation-results
```

The input VCF and `.nt.gz` must originate from the same conversion. `--rdf`
must name an existing `.nt.gz` file; validation deliberately does not accept an
uncompressed `.nt`, HDT, or COTTAS artifact. Use full mode with
`--rdf-storage-mode space-optimized` to produce the required gzip aggregate.

`--validation-id NAME` changes the report directory name. The default is the
source VCF basename without `.vcf` or `.vcf.gz`. Existing result directories
are never overwritten. `--filter-oracle {auto,bcftools,cyvcf2}` controls the
FILTER-field oracle; `auto` uses `bcftools` when it is available in the image.

## What is tested

The common record-level queries are used for both graph shapes:

| Query | Exact comparison |
|---|---|
| Q1 | record counts per source contig and zero-based 1 Mb window |
| Q2 | record-level ALT/REF shape classes |
| Q3 | biallelic A/C/G/T SNV transition and transversion counts |
| Q4 | FILTER broad status and exact lexical value |
| Q5 | per-sample genotype class counts |
| Q6 | genotype-derived single-ALT `(AN, AC, siteCount)` distribution |

Q5/Q6 are only required when the VCF has both samples and a `GT` FORMAT field.
The suite additionally checks N-Triples syntax, record cardinality, POS
datatype, representation profile, and representation-specific sample/GT
inventory before interpreting the six result sets.

For the expanded representation, Q5/Q6 traverse `SampleCall` and
`FormatFieldValue` resources. For the condensed representation, their matching
queries traverse `SampleSet`, `CohortCallMatrix`, and `FormatValueVector`, then
extract each tab-delimited GT value by its `sampleIndex`. This makes the
condensed tests semantically equivalent without inflating the persisted RDF
back into per-sample value resources.

## Results and cleanup evidence

Results are written to:

```text
<out>/validation/<validation-id>/
```

Important files include `summary.json`, `manifest.json`, `parser.json`,
`rdf-validation.json`, `preflight.json`, `sparql.json`, and `comparison.json`.
Raw Comunica JSON, stderr, and query resource logs are in `raw/`; normalized
results are in `normalized/`.

The parent VCF-RDFizer run metrics include
`run_metrics/<input-label>__<run-id>/stages/validation/<validation-id>.json`.
Both that report and `summary.json` record that the `.nt.gz` was decompressed
inside the container and that no raw RDF was retained on the host.

`PASS` means every required query and invariant matched. `MISMATCH` means that
both paths ran but differ. `BLOCKED_BY_PREFLIGHT` means RDF syntax or core graph
structure failed. `EXECUTION_FAILED` means a parser or query engine could not
complete.
