# Limitations

A single place that says what VCF-RDFizer cannot do, does badly, or does in a
way that will surprise you. Nothing here is hidden elsewhere in the
documentation — this page collects it so a prospective user can decide against
the tool without reading everything first.

Each item says what it is, why it is that way, and whether it is fixable.

---

## 1. Operational

**Docker is mandatory for conversion and representation operations.** There
is no pure-Python conversion path. Post-hoc data linking and the authoring CLIs
run on the host without Docker. The conversion toolchain — Flink,
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

**Structural variants and genotypes are no longer lexical-only.** Moving the
target to the VCF Core vocabulary closed three gaps that used to be listed here:
symbolic ALTs, breakends and `*` are now classified and parsed; `Number=A/R/G/P`
values are decomposed into per-allele `vcfc:FieldValueItem` resources; and `GT`
is parsed into ordered allele calls with an explicit phasing status. See
[`conversion.md`](conversion.md#4-stage-three--the-wrappers-own-emitters).

What remains true:

**Genotype interpretation is expanded-only.** The condensed profile keeps
per-sample values inside `vcfc:encodedValues` vectors, so there is no
`vcfc:Genotype`, no phase set and no base-modification resource in a condensed
graph. That is the point of the profile — the values stay recoverable by
decoding a vector against its FORMAT definition and the matrix `SampleSet` — but
a SPARQL engine cannot filter inside a vector payload without a decoder. Choose
`expanded` if you need to query genotypes directly.

**Symbolic ALT types are only recognised for the reserved codes.** A
caller-specific `<MY_EVENT>` gets `vcfc:SymbolicAllele` and a link to its
`##ALT` declaration, but no `vcfc:svType`, because the vocabulary enumerates
only the codes VCF 4.5 reserves.

**Some SV carriers need more than one INFO key to be emitted.** A
`vcfc:VariantEvent` needs `EVENT` *and* `EVENTTYPE`; a gVCF reference block
needs `END` *and* `POS`; a tandem repeat needs `RN` before `RUS`/`RUL`/`RUC`/`RB`
have a grouping. Where the record supplies only part of the set, the carrier is
omitted and the values remain as ordinary INFO values. Emitting a partial
carrier would produce a resource that fails its SHACL shape, which is worse than
not producing it.

**VCF 4.0 has no conformance overlay.** VCF Core supplies version overlays for
4.1 through 4.5 only. A 4.0 file converts with the newest supported rules, so
nothing representable is dropped, but it gets no `vcfc:VCF4xFile` class and its
graph is only checked by the version-neutral profiles. The same applies to a
file with a missing or malformed `##fileformat` line. All three cases are
reported in the run log rather than assumed silently.

**Version detection trusts the file's own declaration.** `##fileformat` is
required to be the first line of a conforming VCF, and that is what the
converter reads. A file whose declared version does not match its actual content
converts against the declared one; `--vcf-version` is the override. The
converter does not sniff content to second-guess the declaration, because a
wrong guess would silently produce a graph that validates against rules the file
was never written to.

**Parsing stays lenient across versions.** A GT with a leading phase indicator
in a VCF 4.1 file is still parsed into a `vcfc:Genotype`, and a `Number=R`
declaration in a 4.1 file is still recorded. The version overlays report both as
non-conformant. This is deliberate: the converter transcribes, the validator
judges, and a converter that refused to represent a slightly non-conforming file
would be less useful than one that represents it faithfully and lets SHACL say
so.

**`--header-representation basic` is not SHACL-conformant.** It emits no
header-line subclass and no `vcfc:HeaderAttribute` resources, and
`vcfc:StructuredHeaderLineShape` requires at least one attribute on every
structured line. It remains available as the smallest, fastest header form.

**Triples only.** No named graphs anywhere in the pipeline, and no blank nodes —
a blank node is treated as a validation failure, because every class in the
vocabulary declares an IRI template.

**No cross-file merging.** Each VCF produces its own graph.

## 4. The custom-mapping extension point

**`--rules` does not control the whole graph.** Three families of triple —
genotypes, structured headers, and the record detail (the fixed fields that may
be missing, the allele layer, structured INFO and the SV carriers) — are emitted
by the wrapper rather than by RML, because RML cannot choose a datatype or class
per row and the alternatives require materializing enormous helper tables. See
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

**`vcfc:contigCount` is counted, not read**, so a wrong derived contig total is
undetected. **Header line values are not compared** — only how many lines carry
each key, their types, and the structured attributes lifted out of them.

**SHACL is opt-in and does not scale.** `pyshacl` loads the whole graph into
memory, so it is for a single-sample graph or a sample of a cohort.

**QLever's argv is a moving target.** Its CLI has changed across releases; the
`QLEVER_*_COMMAND` environment overrides exist because of that, and the exact
argv is recorded in every report so a future divergence is diagnosable.

## 7. Vocabulary

**VCF Core is published at 2.0.0** (`https://w3id.org/vcf-core/vocab#`) and
covers VCF 4.1 through 4.5. The three constraints listed here previously — the
condensed terms being undefined, the `ALT=.` shape contradiction, and the
ordinal datatype disagreement — are all resolved in that release. What remains:

**VCF 4.0 has no conformance overlay.** The vocabulary claims 4.1–4.5. A 4.0
file converts with the newest supported rules, so nothing representable is
dropped, but it gets no `vcfc:VCF4xFile` class and only the version-neutral
profiles check it. Reported in the run log, never assumed silently.

**The converter's version table is a copy.** VCF Core publishes
`ontology/versions/registry.json` as the source of truth for version-scoped
behaviour; the converter keeps its own dependency-free copy so it runs without
the vocabulary checked out.
`test_version_model_matches_the_published_registry` cross-checks the two when a
vocabulary checkout is available (`VCF_CORE_VOCABULARY_DIR`), and skips
otherwise — so drift is caught in development but not in a minimal CI.

**Condensed mode has no query-time decoder.** Reconstructing sample *i*'s value
means splitting a tab-separated literal, which SPARQL cannot do portably. The
options are assessed in
[`sample-representation-guide.md`](sample-representation-guide.md#7-assessment-of-geosparql-and-graphdb-sparql-extensions);
none is currently implemented.

## 8. Scope

**Data linking is an initial implementation.** Optional plug-ins can produce
separate linksets through token joins, GFF3 intervals or a guarded live session.
The gene example is synthetic; allele normalization and automatic plug-in
validation are not implemented. See [`datalinking.md`](datalinking.md) for the
implemented contract and [`datalinking-design.md`](datalinking-design.md) for
the remaining proposal. The base graph remains a faithful, separate artifact.

**No disclosure control.** Conversion is all-or-nothing: every sample, every
genotype, every header line and every free-text `Description` goes into the
graph, and there is no way to withhold a participant, degrade a region, or
record what an artifact was permitted to contain. Three consequences today:

- **IRIs carry identifiers.** `file://cohort.vcf#sample/1/NA12878` embeds the
  sample name, `{SOURCE_FILE}` embeds the VCF's basename, and `#record/{ROW_ID}`
  is a monotonic counter that discloses source ordering.
- **Header lines are a leak surface.** `##source`, `##SAMPLE`, `##PEDIGREE` and
  free-text `Description` fields are transcribed verbatim into
  `vcfc:headerValue`.
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
