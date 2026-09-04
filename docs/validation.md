# Semantic VCF/RDF validation

`vcf-rdfizer --mode validation` checks that a converted RDF graph reproduces
six deterministic VCF summaries. The same validator can be added to a full run
with `--validate`; in that case it runs once for each input after RDF creation
and compression. It computes one result from the source VCF
using `cyvcf2` (and `bcftools` for exact FILTER strings when available), runs
the equivalent SPARQL queries with Comunica, then compares canonical integer
results exactly.

Standalone validation consumes a single N-Triples aggregate (`.nt` or
`.nt.gz`). It mounts the source read-only; gzip input is expanded under
`/work` **inside the Docker container**, while plain `.nt` input is read in
place. The validator uses the resulting stream for Raptor syntax validation
and Comunica queries, and removes any temporary expansion before the container
exits. No decompressed RDF is written beneath `--out`; only reports are
retained.

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

To include validation in the conversion itself, use the full mode's opt-in
stage. It runs after the aggregate and selected compression artifacts are
available, once per VCF input:

```bash
vcf-rdfizer --mode full \
  --input ./cohort.vcf.gz \
  --sample-representation condensed \
  --rdf-storage-mode space-optimized \
  --rdf-compression none \
  --representations hdt \
  --validate \
  --out ./results
```

For standalone validation, the input VCF and `.nt`/`.nt.gz` must originate from
the same conversion. `--rdf` must name an existing `.nt` or `.nt.gz` file;
standalone validation does not accept HDT or COTTAS artifacts. Use full mode
with `--rdf-storage-mode space-optimized` for a gzip aggregate, or `plain` for
an uncompressed aggregate.

`--validation-id NAME` changes the report directory name. The default is the
source VCF basename without `.vcf` or `.vcf.gz`. Existing result directories
are never overwritten. `--filter-oracle {auto,bcftools,cyvcf2}` controls the
FILTER-field oracle; `auto` uses `bcftools` when it is available in the image.

Validation progress uses the same JSONL sidecar protocol as conversion and
partitioned compression. It emits a `validation` task with a total of eleven
preflight/core queries, then records each query start and completion under the
run's temporary `.progress/` area while the host displays it through the
normal Rich/plain progress session. `--quiet` suppresses that terminal display
and the validator's per-query/summary stdout, but still writes command logs,
stage reports, and metrics. `--no-progress` disables sidecar creation as well.

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

Results are written to the run's canonical metrics tree:

```text
<out>/run_metrics/<input-label>__<run-id>/reports/validation/<validation-id>/
```

Standalone validation consumes either `.nt` or `.nt.gz`. Full-mode validation
uses the aggregate produced by that run and can read either `.nt` (plain
storage) or `.nt.gz` (space-optimized storage). In both cases the detailed
results live beneath `reports/validation/`, so they are indexed by the same
`summary.json` used for conversion and compression metrics.

Important files include `summary.json`, `manifest.json`, `parser.json`,
`rdf-validation.json`, `preflight.json`, `sparql.json`, and `comparison.json`.
Raw Comunica JSON, stderr, and query resource logs are in `raw/`; normalized
results are in `normalized/`.

The parent VCF-RDFizer run metrics include
`run_metrics/<input-label>__<run-id>/stages/validation/<validation-id>.json`.
That stage report and the detailed `reports/validation/.../summary.json` record
whether a gzip aggregate was decompressed inside the container and that no
validation scratch RDF was retained on the host.

`PASS` means every required query and invariant matched. `MISMATCH` means that
both paths ran but differ. `BLOCKED_BY_PREFLIGHT` means RDF syntax or core graph
structure failed. `EXECUTION_FAILED` means a parser or query engine could not
complete.
