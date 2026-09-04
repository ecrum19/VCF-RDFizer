# Limitations

A single place that says what VCF-RDFizer cannot do, does badly, or does in a
way that will surprise you. Nothing here is hidden elsewhere in the
documentation — this page collects it so a prospective user can decide against
the tool without reading everything first.

Each item says what it is, why it is that way, and whether it is fixable.

---

## 1. Operational

**Docker is mandatory.** There is no pure-Python path. The toolchain — Flink,
RMLStreamer, a Rust `hdtc`, `pycottas`, Comunica, QLever, `pyshacl`, `cyvcf2`,
`bcftools` — is pinned in one image, which is what makes results reproducible
across machines. The cost is a large image, a Docker daemon requirement, and no
usable story on a cluster that only offers Singularity/Apptainer.
*Fixable in principle; not planned.*

**The Docker data volume, not `--out`, is the binding disk constraint.**
Partitioned merges and HDT indexing perform disk-backed external sorts under the
container's `/work`. Free space in the output filesystem does not help, and the
resulting failure surfaces as `exit_code=-9` / `137` — an OOM kill — rather than
as a disk-space message. This is the single most common cause of a failed
cohort-scale run.

**Single machine, single process per input.** Inputs are processed one at a
time. `--spark-partitions` tunes RMLStreamer's internal parallelism but there is
no distributed execution and no work queue.

**No incremental update.** Adding variants to a converted dataset means
reconverting the VCF and rebuilding every representation from scratch.

**Interrupt cleanup is best-effort.** `Ctrl+C` exits 130 and removes tracked
intermediates, but a `SIGKILL` or a host crash can leave a partial output
directory that the collision check will then refuse to write into.

## 2. Input handling

**Only `*.vcf` and `*.vcf.gz`.** Extension decides, not content. `.bcf` is not
supported; neither is `.vcf.bgz` or a differently named gzip stream. A
directory input is enumerated one level deep, sorted, once, at run start.

**The VCF parser is `awk`, not `htslib`.** [`src/vcf_as_tsv.sh`](../src/vcf_as_tsv.sh)
makes one pass and splits on tabs. Consequences:

- The `#CHROM` line is only recognised when tab-delimited. A space-delimited
  header line matches no rule, so sample column names are lost and the records
  header silently falls back to `SAMPLES`.
- Nothing validates VCF spec conformance. A malformed file yields a malformed
  graph rather than an error; the validation suite is the first thing that
  notices.
- A data line with fewer than eight columns produces empty fields, not an error.
- Sample fields are whitespace-normalized, so a value containing a literal space
  would be corrupted. The specification forbids that, so it is only reachable
  with an already-invalid file — but it is undetected.

**IRIs are minted from the filename, not the path.** Two different VCFs named
`data.vcf` produce identical subject IRIs and their graphs collide when merged.
Rename before converting, or keep the graphs apart.

## 3. What the RDF does and does not model

**No variant normalization.** No left-alignment, no trimming, no multi-allelic
splitting. `ALT=A,T` stays one record with one `alt` literal. This is deliberate
— the graph is a faithful transcription — but it means the graph is not directly
joinable with normalized external resources, which is the central problem the
[data-linking design](datalinking-design.md) has to solve.

**No reference checking.** `REF` is never verified against a genome, and the
declared assembly is recorded but not used for anything.

**Structural variants are literals.** Symbolic ALTs (`<DEL>`), breakends and `*`
are carried as strings. The validation suite classifies them
(`SYMBOLIC_OR_BREAKEND`) but the vocabulary gives them no structure, and `END` /
`SVTYPE` carry no special meaning.

**Multi-valued INFO fields keep only their lexical value.** A field declared
`Number=A/R/G/.` gets no typed per-value nodes, because the vocabulary's IRI
template gives one node per key rather than per value. This is a modelling gap
in the vocabulary, not a bug in the conversion.

**Genotypes are lexical.** Phasing is preserved inside the literal but not
modelled; ploidy is not interpreted.

**Triples only.** No named graphs anywhere in the pipeline, and no blank nodes —
a blank node is treated as a validation failure, because every class in the
vocabulary declares an IRI template.

**No cross-file merging.** Each VCF produces its own graph.

## 4. The custom-mapping extension point

**`--rules` does not control the whole graph.** Three families of triple —
genotypes, structured headers, and QUAL/structured INFO — are emitted by the
wrapper rather than by RML, because RML cannot choose a datatype or class per
row and the alternatives require materializing enormous helper tables. See
[`architecture.md`](architecture.md#4-where-the-split-leaks-and-why). They can be
narrowed with `--sample-representation` / `--header-representation` /
`--info-representation`, but not replaced by a mapping.

**A custom mapping is validated less thoroughly.** Queries `q09`–`q13` assume
the shipped mapping's predicate inventory and IRI templates.

**And, currently, it is validated incorrectly.** `validation_runner.py` supports
`--mapping-policy report-only` for exactly this case, but the wrapper never
forwards it, so a custom mapping run through `vcf-rdfizer --validate` reports
`MISMATCH` on those five queries even when the conversion is correct.
*Fixable; tracked in [`roadmap.md`](roadmap.md).*

**`vcf-rdfizer-rules check` is lexical, not semantic.** It catches wrong logical
-source paths and misspelled columns — the two mistakes that waste the most time
— and nothing subtler. A mapping that passes `check` can still be wrong.

## 5. Compression and representations

**Packaged artifacts are not queryable.** `.hdt.gz`, `.hdt.br`, `.cottas.gz` and
`.cottas.br` are archives. This is easy to forget when
`--remove-rdf-storage-output` has already removed the alternative.

**COTTAS is the more fragile path.** Its upstream `cat` cannot handle large
condensed graphs, which is why VCF-RDFizer implements its own bounded k-way
merge. Even so, a memory-constrained host may need a reduced
`COTTAS_MERGE_BATCH_ROWS`, and `--representations hdt` remains the independent
fallback.

**The round-trip check counts, it does not compare.** Matching triple counts
prove an artifact decodes and holds the right *number* of statements — not that
they are the right statements. The stronger claim requires
`--validate-artifacts hdt,cottas`.

**A degraded HDT index is a success, not a failure.** In full mode an HDT whose
data is readable but whose sidecar could not be built is published with
`index_status: "failed"` and a warning. That is intentional, but it means a
successful run can leave a non-indexed artifact.

## 6. Validation

The validation suite has its own detailed limits in
[`validation.md`](validation.md#what-is-not-tested) and
[`vcf-coverage.md`](vcf-coverage.md#remaining-gaps). The headline items:

**A `PASS` is a regression gate, not a correctness proof.** It reliably catches
dropped records, misclassified variants, flipped genotypes, corrupted FILTER
strings, allele-count errors, missing header lines and altered file metadata. It
is not proof of a faithful record-by-record round-trip.

**Coverage is relative to the mutation catalogue.** The score says "almost every
corruption we thought to write down is caught". It is a lower bound on
blindness, not a measure of correctness, and a score that rises without the
catalogue growing means nothing.

**`vcfr:contigCount` is counted, not read**, so a wrong derived contig total is
undetected. **Header line values are not compared** — only how many lines carry
each key, their types, and the structured attributes lifted out of them.

**SHACL is opt-in and does not scale.** `pyshacl` loads the whole graph into
memory, so it is for a single-sample graph or a sample of a cohort.

**QLever's argv is a moving target.** Its CLI has changed across releases; the
`QLEVER_*_COMMAND` environment overrides exist because of that, and the exact
argv is recorded in every report so a future divergence is diagnosable.

## 7. Vocabulary

**Condensed graphs are not ontology-backed.** The tool emits 17 terms that
`https://w3id.org/vcf-rdfizer/vocab#` does not define, all belonging to the
condensed representation. Dereferencing any of them returns nothing. This is the
one remaining publication blocker, and the work is in the vocabulary repository.

**The published SHACL shapes contradict the missing-value policy.**
`vcfr:missingValuePolicy` says a missing token should be `"."^^vcfr:Null`, while
`VCFRecordShape` constrains `vcfr:alt` to `sh:datatype xsd:string`. A record with
`ALT=.` cannot satisfy both. The conversion follows the missing-value policy.
The vocabulary needs a decision; see
[`vcf-coverage.md`](vcf-coverage.md#an-open-conflict-inside-the-vocabulary).

**Condensed mode has no query-time decoder.** Reconstructing sample *i*'s value
means splitting a tab-separated literal, which SPARQL cannot do portably. The
options are assessed in
[`sample-representation-guide.md`](sample-representation-guide.md#7-assessment-of-geosparql-and-graphdb-sparql-extensions);
none is currently implemented.

## 8. Scope

**No data linking yet.** The graph is self-contained and connects to nothing
external. This is by design so far, and the plan to change it is
[`datalinking-design.md`](datalinking-design.md).

**No disclosure control.** Conversion is all-or-nothing: every sample, every
genotype, every header line and every free-text `Description` goes into the
graph, and there is no way to withhold a participant, degrade a region, or
record what an artifact was permitted to contain. Three consequences today:

- **IRIs carry identifiers.** `file://cohort.vcf#sample/1/NA12878` embeds the
  sample name, `{SOURCE_FILE}` embeds the VCF's basename, and `#record/{ROW_ID}`
  is a monotonic counter that discloses source ordering.
- **Header lines are a leak surface.** `##source`, `##SAMPLE`, `##PEDIGREE` and
  free-text `Description` fields are transcribed verbatim into
  `vcfr:headerValue`.
- **Even if that were fixed, genotypes identify people.** A few dozen
  independent common variants are enough to single out an individual, so no
  amount of label removal makes a released genotype graph non-identifying.

The plan is [`privacy-policy-design.md`](privacy-policy-design.md), which is
explicit that what it offers is *governed release*, not anonymization.

**No clinical claims.** The tool transcribes a VCF. It does not interpret,
annotate, prioritize, or assess pathogenicity, and its output should not be
presented as if it did.

---

## See also

- [Roadmap](roadmap.md) — which of these are being addressed
- [Privacy policy design](privacy-policy-design.md) — the disclosure-control gap, and the proposal to close it
- [VCF coverage matrix](vcf-coverage.md) — the element-by-element measurement
- [Validation](validation.md) — the detailed "what is not tested"
