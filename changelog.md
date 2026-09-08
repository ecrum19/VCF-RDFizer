# Changelog

## 2026-09-08 — Verify against the published VCF Core 2.0.0

VCF Core is published at **2.0.0**, covering VCF 4.1 through 4.5. This checks
the converter against the released artifacts and closes the gap that made the
version model a private assumption.

### Fixed

- **A bracketed CHROM now links to its assembly contig.** VCF 4.5 lets CHROM
  name a breakpoint-assembly contig in angle brackets (`<asm1>`) instead of a
  declared reference sequence. The converter emitted only the raw `vcfc:chrom`
  literal, which the published consistency profile rejects: a bracketed CHROM
  must resolve to a `vcfc:AssemblyContig` whose `vcfc:assemblyContigId` matches
  the bracketed text, and must *not* also name a declared `##contig`. The
  record now carries `vcfc:chromAssemblyContig`, and the contig resource cites
  the `##assembly` header line with `vcfc:declaredInAssembly`.

  Found by validating against the vocabulary's new `example-vcf45-boundaries`
  fixture; the emitted IRIs and properties match its reference graph exactly.

### Added

- **The version model is cross-checked against the vocabulary's own registry.**
  VCF Core publishes `ontology/versions/registry.json` as the single source of
  truth for its version-scoped artifacts — the SHACL overlays, the reserved-key
  snapshots and the `VCF4xFile` classes are generated from it. The converter
  keeps a dependency-free copy of the same facts so it runs without the
  vocabulary checked out;
  `test_version_model_matches_the_published_registry` now pins the two together,
  field by field: supported versions, `code`, `className`,
  `leadingPhaseIndicator`, `svTupleScope`, the INFO/FORMAT `numberCodes`, and
  the flattened-tuple keys and widths. It skips when no vocabulary checkout is
  present; `VCF_CORE_VOCABULARY_DIR` points it at one.

  The two agree exactly as published — no drift to fix — but a new VCF version
  or a corrected arity can no longer land on one side alone.

- **`vcf_version` and `vcf_version_source` in the conversion metrics**, in both
  `metrics.csv` and `stages/conversion/*.json`. The source is `declared`,
  `forced` or `fallback`. Benchmarks need these to group runs: the version
  selects the SHACL overlay, the tuple semantics and which FORMAT families
  exist, so cost and graph size are not comparable across versions without
  them. `metrics_layout_version` is now `3`.

### Verified

- Every VCF version resolves end to end: detection from `##fileformat`, the
  correct `vcfc:VCF4xFile` on the file subject, the sentinel never left
  unresolved, TSV paths rewritten, and `--vcf-version` overriding cleanly.
  VCF 4.0, a missing declaration and a malformed one all fall back to the
  newest supported rules with no version class, and say so.
- `--vcf-version` accepts exactly the versions the registry declares.
- The vocabulary's own example VCFs now include `example-vcf45-boundaries`, and
  `example-condensed-cohort` declares VCFv4.1; both convert correctly.

### Documentation

- **`vcf-coverage.md` records the term-coverage delta.** VCF Core 2.0.0 defines
  338 terms; ten families are entirely unemitted — parsed FILTER, padding
  semantics, parsed FORMAT keys, parsed record IDs, repeat units, breakend
  mates, local-allele membership, mixed phasing, reference-block links, and
  raw/decoded value pairs. None is required for conformance, and the table says
  which kinds of benchmark the gap does and does not affect.
- `limitations.md` no longer lists the condensed terms, the `ALT=.` shape
  contradiction or the ordinal datatype disagreement: all three are resolved in
  the published release. What remains is VCF 4.0 having no overlay, and the
  version table being a checked copy rather than the original.

## 2026-09-07 — Stop a slow validation engine from looking like a hang

A full run with `--validate` on a real cohort VCF spent 62 hours on its first
validation step. The cause was not a deadlock: it was three defects that
compound into one.

### Fixed

- **A timed-out query no longer costs one timeout for every remaining query.**
  The per-query timeout defaults to 3600s and there are 27 queries per engine
  per target, so one engine that cannot answer the graph burned **27 hours per
  target** — the loop ran every query and only checked for failures afterwards.
  It now stops at the first timeout, records which query tripped it and how many
  were skipped, and moves on to the next engine. `--validation-continue-after-`
  `query-timeout` restores the old behaviour.
- **A timed-out query no longer orphans its engine process.** Queries are
  wrapped in `/usr/bin/time`, which forks the real engine.
  `subprocess.run(timeout=)` signals only the process it started, so killing the
  wrapper left the engine running — holding the entire graph in memory, because
  that is what an in-memory SPARQL engine does. Twenty-seven of those is enough
  to push a host into swap, which is what turned a slow step into an apparent
  hang. Queries now run in their own session and the whole process group is
  signalled, SIGTERM then SIGKILL.
- **The validation results mount is resolved.** Docker reads a relative bind
  source as a *named volume*, so an unresolved path would have sent the reports
  into a volume instead of onto the host. Every other mount in that command was
  already resolved.

### Added

- **`--validation-time-budget`** (seconds, 0 = no ceiling, the default): a
  wall-clock ceiling for one engine's whole query set. A backstop for queries
  that are slow but never individually time out.
- **An up-front warning when an unindexed engine meets a large graph.** Comunica
  has no persistent index: every query re-parses the whole source into memory,
  so the cost is paid 27 times. Above 4 GiB the run now says so before
  committing, states the worst-case hours at the current timeout, and points at
  `--engine qlever` or `--validation-targets hdt`. This is the asymmetry behind
  "QLever passed, Comunica did not": QLever builds an index once and answers
  from it.
- **`summary.json` now carries a `validation` index.** Each target's stage
  report, results directory, summary, benchmark JSON/CSV and the engines that
  ran are named explicitly, with status, engine and wall time, instead of being
  left to find among the report globs.

## 2026-09-07 — Migrate the validation suite to VCF Core

The conversion moved to VCF Core and became version-aware; the validation suite
still described the previous graph, so 31 of its tests failed. It now passes,
with its design intact: one declarative fixture deriving three artifacts,
coverage measured by mutation rather than asserted, and expectations derived
from the VCF rather than from the mapping.

**415 tests pass.** Mutation score: **96/113 (85%)** across 60 mutations, up
from 42.

### Fixed

- **Every declared INFO and FORMAT definition was emitted twice.** The header
  emitter produced a declaration's `fieldId`/`fieldNumber`/`fieldType`/
  `fieldDescription` from the `##` line, and the value emitters produced the
  same four triples at the same header-line IRI when the key was first used.
  The header emitter now owns any declaration derived from a `##` line; the
  value emitters cite it with `vcfc:declaredBy` and invent a definition only for
  a key no header declared.

  This is worth noting for what caught it: `preflight_duplicate_triples`, which
  compares the parsed triple count against the distinct count. No SHACL shape
  can express that — a duplicate triple is not a distinct RDF statement, so the
  graph validated cleanly with the duplicates present.

### Changed

- **The runner and the wrapper share one vocabulary module.**
  `validation_runner.py` carried its own copies of `HEADER_LINE_CLASSES`,
  `rml_uri_component`, `parse_structured_header_fields` and
  `parse_info_entries`, with unit tests asserting the copies stayed identical.
  Both now import `vcf_rdfizer_vocab`, which the `Dockerfile` copies next to the
  runner. The drift the tests policed is removed rather than policed.
- **The census derives every new resource family from the VCF.** Two shared
  functions, `emitted_header_counters` and `emitted_record_counters`, are called
  by the container's `parse_vcf` and by the fixture's `parser_summary`, so the
  two oracles cannot disagree. They cover the header attributes, the allele
  layer, the `vcfc:FieldValueItem` decomposition, the breakend components and
  the parsed genotype layer. `parse_vcf` stays representation-independent; the
  genotype counters are reported separately and merged for the expanded profile
  only.
- **`base_triples()` tracks the new mapping**: the version class, the
  `#CHROM` column-header line, `vcfc:lineIndex` and `vcfc:recordIndex` in, and
  `ID`/`ALT`/`FILTER`/`infoRaw` out — those moved to the wrapper's emitter
  because each may be the VCF missing token.
- **The fixture derives its expected version class** from its own declared
  `FILE_FORMAT`, so changing that changes the expected graph with it.

### Added

- **18 mutations** for the layers VCF Core introduced: the version class, header
  attributes, the allele layer, indexed values, reusable sample identity, the
  parsed genotype layer and field-declaration ownership. Ten are recorded as
  undetected, each naming what would close it — the score fell from 97% to 85%
  because the denominator grew, which is the methodology working rather than a
  regression.
- **`corrupt_sample_index` is split by profile**, because the same corruption is
  detected in the condensed profile and not the expanded one. In the condensed
  profile the ordinal associates a vector position with a sample, so corrupting
  it moves genotypes between samples; in the expanded profile each `SampleCall`
  carries its own `vcfc:sampleId` and nothing reads the ordinal. A real
  difference in what the two profiles depend on, now recorded as such.
- Census tests asserting that every new family is in the inventory, and that the
  genotype layer appears in the expanded census and *not* in the condensed one.

## 2026-09-07 — Version-aware conversion for VCF 4.1 through 4.5

VCF Core gained a SHACL overlay per VCF version, each scoped by the file's
`vcfc:fileFormat`, plus optional `vcfc:VCF41File`…`vcfc:VCF45File` classes that
activate those gates. Three things the converter emits genuinely differ between
versions, so the conversion now follows the version of each input instead of
assuming 4.5.

### Added

- **Automatic version detection, per input.** `##fileformat` is required to be
  the first line of a conforming VCF and `src/vcf_as_tsv.sh` already lifts it
  into `file_metadata.tsv`, so detection costs one small read and needs nothing
  from the user. It is per input rather than per run, so a directory of mixed
  versions converts correctly. The run log states the version and how it was
  chosen.
- **`--vcf-version {auto,4.1,…,4.5}`**, defaulting to `auto`. An explicit
  version is an override for a file whose declaration is missing or wrong, and
  the log says when the file's own declaration was overridden.
- **A version sentinel in the RML mapping.** `<#VCFFileMap>`'s subject map
  carries `rr:class vcfc:VCFVersionFile`, which `render_rules_for_triplet`
  rewrites per input — the same mechanism that already rewrites the five
  `csvw:url` paths — to the `vcfc:VCF4xFile` subclass for that input's version.
  One mapping stays the single source of truth for all five versions instead of
  five near-identical copies that would drift apart. `vcf-rdfizer-rules check`
  reports whether a custom mapping has the sentinel.
- **A `VCFVersion` model** in `vcf_rdfizer_vocab.py` carrying, per version, the
  permitted Number codes, the flattened-tuple keys and their widths, whether
  tuples repeat per ALT allele, and whether the local-allele, base-modification,
  EVENTTYPE and leading-phase-indicator features exist.

### Changed

- **`CIPOS`/`CIEND` tuple semantics follow the version.** Before VCF 4.4 the
  list is one pair describing the record, so its `vcfc:FieldValueItem`s carry an
  index and a `vcfc:tupleArity` but no `vcfc:forAllele`, and every ALT allele of
  a multi-allelic record shares the interval. From 4.4 the list repeats per ALT
  and each item joins its own allele. `CILEN` and `CICN` are only tuple keys
  from 4.4.
- **`EVENT` follows the version.** It is `Number=1` before 4.4 — one event for
  the record — and `Number=A` from 4.4, one per ALT allele, so the `vcfc:inEvent`
  link moved onto the allele there. `EVENTTYPE` does not exist before 4.4, and
  `vcfc:VariantEventShape` requires an event type, so a pre-4.4 EVENT stays an
  ordinary INFO value.
- **Families a version does not define are no longer invented.** A `LAA` column
  in a 4.4 file, or an `M27551C` column in a 4.3 file, keeps its raw
  `vcfc:fieldValue` and gets no `vcfc:LocalAlleleSet` or `vcfc:BaseModification`
  resource.
- **VCF 4.0 is reported as unrecognized.** VCF Core claims no 4.0 overlay, so a
  4.0 file converts with the newest supported rules — nothing representable is
  dropped — but emits no version class, so the graph never claims a gate that
  cannot be checked. A missing or malformed `##fileformat` line behaves the
  same. All three are reported, never assumed silently.

Parsing leniency is deliberately unchanged: a GT with a leading phase indicator
in a 4.1 file is still parsed, and the version overlay reports it. The converter
transcribes; the validator judges.

### Fixed

Two defects found by validating against the new complete profile set:

- **Flattened tuple keys were never decomposed.** `CIPOS` and friends are
  declared `Number=.` in 4.4/4.5, which on its own says "no positional
  meaning", so no `vcfc:FieldValueItem`s were emitted — but the version overlays
  count those items against the ALT count. Tuple keys are now positional by
  virtue of being tuple keys, whatever their declared Number.
- **Confidence-interval bounds were plain strings.** `vcfc:ciLower`/`ciUpper`
  and the FALDO `begin`/`end` go through `vcfc:NumericLiteralShape`, which
  requires an integer- or decimal-derived datatype.
- **A sites-only VCF got no representation profile.** Both sample emitters
  returned early when the file declared no sample columns, so a VCF with only
  the eight fixed columns violated `vcfc:RepresentationProfileShape`, which
  requires exactly one profile on every `vcfc:VCFFile`. The profile is now
  emitted regardless; the `vcfc:SampleSet` is still omitted, because
  `vcfc:SampleSetShape` requires at least one member.

### Verified

Zero violations across 19 graphs against the **complete** profile set — the
portable, SPARQL and consistency profiles plus all five version overlays — run
with pySHACL using the bundled ontology plus every `ontology/versions/*.ttl`
and RDFS inference, which is the configuration the vocabulary's own
`tests/validate_shacl.py` uses. Inputs were all ten of the vocabulary
repository's example VCFs (4.1 through 4.5, including the breakend, gVCF, repeat
and condensed-cohort fixtures) and a hand-written stress file rendered for every
version plus a 4.0 variant, in both representation profiles.

### Note on the vocabulary's own version number

The vocabulary is now `owl:versionInfo "2.0.0"` with
`owl:versionIRI <https://w3id.org/vcf-core/vocab/2.0.0>`, which matches the
v2.0.0 originally requested — it read 4.0.0 when the migration started. This
converter pins no version IRI, referencing only the namespace, so nothing here
needed changing and nothing goes stale.

## 2026-09-07 — Retarget the conversion to the VCF Core vocabulary

The conversion produced RDF in `https://w3id.org/vcf-rdfizer/vocab#`. That
namespace is retired: the vocabulary was renamed **VCF Core** and moved to
`https://w3id.org/vcf-core/vocab#` (prefix `vcfc:`), because it is a semantic
target any conversion system can adopt and its name should not carry the name of
one converter. The old namespace is **not** redirected — it serves a deprecation
document linking every former term to its successor — so this is a breaking
change to the emitted graph.

VCF Core also models considerably more of VCF 4.5 than its predecessor. The
mapping rules and the wrapper's emitters were rewritten to cover it rather than
only re-prefixed.

### Changed

- **Namespace.** Every emitted IRI moves from `vcfr:` to `vcfc:`. One term was
  renamed rather than re-namespaced: `vcfr:DenseRepresentation` is now
  `vcfc:ExpandedRepresentation`. No version IRI is pinned anywhere — the
  converter targets the namespace, and the vocabulary's release line is
  independent of this one's.
- **The RML/wrapper split is now one rule**, stated in the mapping's header and
  in [`architecture.md`](docs/architecture.md#4-where-the-split-leaks-and-why):
  *RML carries every field whose RDF datatype is the same for every row; the
  wrapper carries everything else.* Accordingly `ID`, `ALT`, `FILTER` and
  `infoRaw` moved out of the mapping and into `emit_record_detail`, joining
  `QUAL` and `fileDate`. Each may be the VCF missing token, which the vocabulary
  requires as `"."^^vcfc:Null`, and RML cannot switch an object's datatype per
  row. `CHROM`, `POS` and `REF` stay in the mapping precisely because VCF 4.5
  forbids them from being missing.
- **Ordinals are serialized `xsd:integer`, not `xsd:positiveInteger`.** The
  SHACL profiles constrain every ordinal with `sh:datatype xsd:integer` and that
  constraint compares the datatype IRI exactly, so an `xsd:positiveInteger`
  literal fails the shape while an `xsd:integer` literal satisfies both the shape
  and the ontology's declared range. This affects `vcfc:sampleIndex`, which the
  condensed emitter previously typed `xsd:positiveInteger`.
- **`--header-representation basic` no longer produces a conformant graph.**
  `vcfc:StructuredHeaderLineShape` requires at least one `vcfc:hasAttribute` on
  every structured header line, and `basic` emits no subclass and no attributes.
  It remains available as the smallest, fastest header form.
- **QUAL accepts the full VCF Float lexical space.** `INF`, `INFINITY` and `NAN`
  in any case are emitted as `vcfc:VCFFloat` rather than coerced or flattened to
  a plain literal; a finite value still takes `xsd:decimal` in its source lexical
  form.

### Added

- **`vcf_rdfizer_vocab.py`** — the vocabulary terms and the VCF 4.5 parsers, as
  pure functions so they are testable without producing RDF: allele
  classification, breakend parsing, GT parsing, `Number` arity resolution, value-
  item indexing, and base-modification key recognition.
- **Header layer.** Ordered `vcfc:HeaderAttribute` resources on every structured
  line; `##assembly`, `##pedigreeDB`, `##META`, `##SAMPLE` and `##PEDIGREE` typed
  and decomposed; `vcfc:ColumnHeaderLine` for `#CHROM` with
  `vcfc:hasGenotypeColumns`; `vcfc:lineIndex` and `vcfc:recordIndex` for the
  ordering constraints the SPARQL SHACL profile checks; `vcfc:fieldArity` and
  `vcfc:fieldNumberInteger`; `fieldSource`/`fieldVersion`; typed contig `length`
  and `URL`. An unrecognised `##` key is now typed as structured or unstructured
  by the form of its value rather than left bare.
- **Allele layer.** `REF` and each `ALT` item become ordered
  `vcfc:ReferenceAllele`/`vcfc:AltAllele` resources with `alleleIndex`,
  `alleleValue` and `alleleKind`; symbolic alleles are linked to their `##ALT`
  declaration and given a `vcfc:svType` where the ID is a reserved code;
  breakends are parsed into orientation, replacement string and mate position;
  records are linked to their contig declaration with `vcfc:chromosome`.
- **Value items.** `Number=A/R/LA/LR/G/LG/P` values are decomposed into ordered
  `vcfc:FieldValueItem` resources joined to their allele with `vcfc:forAllele`,
  with `vcfc:tupleArity` for the keys that flatten fixed-width tuples. This
  closes the multi-valued-INFO gap previously recorded in `limitations.md`.
- **Structural variation.** `SVLEN`, `SVCLAIM`, `IMPRECISE`, `NOVEL`, `END`,
  `EVENT`+`EVENTTYPE`, the `CI*` confidence intervals (FALDO `InRangePosition`
  for `CIPOS`/`CIEND`), the `RN`/`RUS`/`RUL`/`RUC`/`RB` tandem-repeat structure,
  and gVCF `<*>` reference blocks.
- **Genotype layer, expanded profile only.** `GT` parsed into a `vcfc:Genotype`
  with `genotypeString`, `ploidy`, an explicit `vcfc:phasingStatus` and one
  `vcfc:GenotypeAlleleCall` per position; plus `FT`, `PS`/`PSL`/`PSO`/`PSQ`,
  `LAA`, the copy-number and haplotype keys, and the `M`/`DPM`/`ADM`
  base-modification families with their ChEBI residue. The condensed profile
  deliberately keeps these inside its vectors — deriving them per sample is the
  materialization that profile exists to avoid.
- **Reusable sample identity in both profiles.** `vcfc:SampleSet` and its ordered
  `vcfc:VCFSample` members are emitted in expanded mode too, and a `SampleCall`
  links to its column with `vcfc:forSample`. It costs one resource per sample
  column for the whole file.
- `docs/validation-migration-notes.md` — what the migration leaves to do in the
  validation suite, and how the converter's SHACL conformance was verified.

### Fixed

- The condensed emitter omitted `vcfc:fieldType` on its `FormatFieldDefinition`
  resources, which `vcfc:FormatFieldDefinitionShape` requires. Condensed graphs
  were therefore non-conformant.
- Four modelling bugs found by running the vocabulary's SHACL profiles against
  the output, all the same shape: a resource typed into a class whose shape
  requires a property the record did not supply. `<*>` was typed
  `vcfc:ReferenceBlock` without `referenceBlockLength`; `<CNV:TR>` was typed
  `vcfc:TandemRepeatAllele` without `repeatSequenceCount`; `EVENT` minted a
  `vcfc:VariantEvent` without `eventType`; and emitting `vcfc:endPosition` on a
  symbolic SV allele made RDFS infer `vcfc:ReferenceBlock` for it. Each carrier
  is now emitted only when the record supplies everything its shape needs.

### Verified

The output validates against both published SHACL profiles —
`vcf-core-vocabulary.shacl.ttl` and `vcf-core-vocabulary-sparql.shacl.ttl` —
with **zero violations**, in both representation profiles, run with pySHACL
using the bundled ontology and RDFS inference. Inputs were the vocabulary's own
`examples/example.vcf` and a VCF 4.5 stress file covering structured headers,
symbolic and breakend ALTs, tandem repeats, a gVCF reference block,
`Number=A/R/G` fields, phased GT with a phase set, `LAA`, `FT`, `CN` and a base
modification.

### Known broken

The validation suite was migrated separately; see the entry above it. At the
time of this change 31 of its tests failed because they asserted against the
retired namespace and the previous emitted shape.

## 2026-09-06 — Runnable examples of all three data-linking tiers

### Added

- `rsid-dbsnp` declarative token links, `gene-demo` with a digest-pinned
  synthetic GFF3 bundle, and a batched `rsid-ensembl` live resolver.
- A shared host-side runner with Turtle manifests, directory/entry-point
  discovery, disk-backed key deduplication, interval joins, assembly refusal,
  atomic side-graphs, provenance and per-linker run metrics.
- `--link` in full mode; Docker-free `--mode link --rdf`; and
  `vcf-rdfizer-link list|keys|init|check|dry-run|run` authoring commands.
- Cached HTTPS sessions with per-host pacing, per-run request ceilings,
  retries respecting `Retry-After`, offline replay and contact headers.
- Known-answer and failure tests for the three examples and their shared
  runtime. The live resolver is tested using fake HTTP, not a public API.

### Changed

- RDFLib is now a runtime dependency for Turtle/N-Triples parsing.
- The [implementation guide](docs/datalinking.md) documents commands and
  limitations; the original proposal is marked partially implemented. Allele
  normalization, link merging and plug-in validation discovery remain planned.
## 2026-09-05 — Fix a unit test that inverted under CI

`shell+pipeline-unit` failed on all three OS matrix legs of the `dev` -> `main`
merge (ubuntu-latest, ubuntu-22.04, macos-latest) while passing on developer
machines. One test, three red checks.

### Fixed

- `test_validation_forwards_progress_sidecar_and_quiet_flag` asserted that
  `run_validation_mode` forwards `--progress-path` to the container. It does —
  but `progress_events_enabled()` returns `False` whenever `CI` is set, and
  GitHub Actions sets `CI=true`, so the sidecar is deliberately not requested
  there. The test pinned `_PROGRESS_ALLOWED`, `_PROGRESS_EVENTS_ALLOWED` and
  `_QUIET` but not the environment, so it tested a different code path in CI
  than the one it was written for. Reproducible locally with
  `CI=true python -m unittest discover -s test -p "test_*_unit.py"`.

  The tool's behaviour is correct and unchanged: the sidecar exists only to
  drive the terminal display and `ProgressSession.__exit__` unlinks it, so
  suppressing it where nothing renders costs nothing.

### Changed

- `stable_progress_env()` replaces the `{"VCF_RDFIZER_NO_PROGRESS": "", "CI": ""}`
  dict that two sibling progress tests already carried inline. The third test
  omitted it, which is how this happened; a named helper with the reason in its
  docstring makes the requirement visible to the next test author.
- The validation-command setup is factored into
  `_run_validation_mode_capturing_commands()`, shared by both sides of the
  contract.

### Added

- `test_validation_omits_progress_sidecar_under_ci` pins the CI behaviour that
  was previously only implicit: no `--progress-path` is requested, and `--quiet`
  still is. Verified as a real guard by removing the `CI` check from
  `progress_events_enabled()` and confirming the test fails.

## 2026-09-05 — Vocabulary alignment for the condensed representation (upstream)

The two open items in [`docs/roadmap.md`](docs/roadmap.md) under "Blocking
publication" are closed. Both were work in the
[vocabulary repository](https://github.com/ecrum19/VCF-RDFizer-vocabulary),
released there as **v1.1.0**; **no change was needed in this repository**, and
this entry records what the conversion may now rely on.

### Condensed graphs are ontology-backed

The conversion emitted 17 terms the vocabulary did not define — `SampleSet`,
`VCFSample`, `CohortCallMatrix`, `FormatValueVector`, `representationProfile`,
`sampleIndex`, `encodedValues` and the rest — so a condensed graph could not be
dereferenced or meaningfully SHACL-checked. All 17 are now defined, with two
superclasses (`RepresentationProfile`, `VectorEncoding`) giving the enumerations
a range, plus SHACL shapes for the profile and a worked example that is the
expanded example's own record in the other profile.

The condensed shapes constrain what positional decoding depends on: exactly one
`sampleIndex` per sample, a sample set on every matrix, and an encoding and
FORMAT declaration on every vector. Ten targeted corruptions of a condensed
graph are each reported as a violation, so the shapes are load-bearing.

`--shacl-shapes` therefore now covers `--sample-representation condensed`, which
it previously could not.

### The missing-value contradiction is resolved

`vcfr:missingValuePolicy` required `"."^^vcfr:Null` for a missing token while
`VCFRecordShape` constrained `vcfr:alt` to `sh:datatype xsd:string`, so a record
with `ALT=.` could satisfy neither and the tool could not be conformant on any
VCF containing one.

The fix draws the boundary the policy was missing rather than relaxing every
field: ALT and ID admit the missing token in VCF 4.5, so their shapes now accept
`xsd:string` or `vcfr:Null` as `qual` already did; CHROM, POS and REF are
required and have no missing form, so their shapes stay exact — a `.` there is
malformed input and should still be reported. The policy also now states that a
missing value inside a `FormatValueVector` stays a `.` character at its sample's
position, rather than becoming a typed literal that would break alignment.

The conversion already followed the policy, so it needs no change. Reproduced
against the 1.0.1 shapes before the fix and confirmed gone after.

### Caveat

The terms only dereference for a third-party consumer once vocabulary v1.1.0 is
published to `https://w3id.org/vcf-rdfizer/vocab#`. Until then the definitions
exist but are not yet live, which is why the docs below say "pending release"
rather than "done".

### Documentation

- [`docs/vcf-coverage.md`](docs/vcf-coverage.md) — the "Vocabulary alignment"
  section now records both gaps as closed, with what was verified.
- [`docs/roadmap.md`](docs/roadmap.md) — both publication blockers marked done.
- [`docs/limitations.md`](docs/limitations.md),
  [`docs/conversion.md`](docs/conversion.md),
  [`docs/validation.md`](docs/validation.md) updated to match.

## 2026-09-04 — Multi-engine validation, native HDT/COTTAS querying, and benchmarking

Validation was one engine against one N-Triples file. It is now up to four
engines against the artifact each of them reads best, in a single run, timed
and cross-checked.

### Added

- **Several SPARQL engines in one run.** `--validation-engine` accepts a
  comma-separated list or `all`. Every requested engine answers the whole query
  set; the first is the primary and keeps the existing single-engine report
  layout, while each engine additionally gets `engines/<name>/` with its own
  preflight, SPARQL, comparison and execution reports. The run's `status` is
  `PASS` only if every engine passed, and `summary.json` carries
  `engineStatuses`.
- **Cross-engine agreement on the real graph.** Every engine's normalized
  results are compared against each other and written to
  `engine-agreement.json`, naming the queries that differ. This is not
  theoretical: QLever's literal canonicalisation had already been caught this
  way in the test harness.
- **Native HDT and COTTAS querying** (`--validation-engine hdt` / `cottas`).
  `--validation-target hdt,cottas` validates those artifacts by *decoding* them
  first, which proves the decode is faithful and says nothing about querying
  them — and measures the wrong thing entirely in a performance comparison. The
  new engines query the artifact in place: HDT through `comunica-sparql-hdt`,
  COTTAS in-process through `pycottas.COTTASStore`, an rdflib `Store` over the
  Parquet artifact. When the run's own artifact is already in the engine's
  format it is used directly; otherwise one is built in container scratch and
  that build is timed as **setup**, kept out of the query total. Each report
  records which happened, in `artifactOrigin`.
- **`@comunica/query-sparql-hdt` in the image**, installed alongside
  `build-essential` (needed for its native bindings) which is purged in the same
  layer.
- **Timings for everything, and a comparison against the parser.** Every run
  writes `benchmark.json` and a long-format `benchmark.csv` — one row per engine
  and query, ready to plot without reshaping. `benchmark.json` adds per-engine
  setup and query totals, the slowest query per engine, materialization and
  SHACL time, and the **oracle**: what it costs to compute the same answers
  directly from the VCF with cyvcf2, split into parse and census. The suite
  already computes every expected value twice, once by parsing and once by
  querying, so those two costs are for identical work on identical input — the
  performance comparison the reports were missing.
- **Benchmark columns in the run's `metrics.csv`**: `validation_engines`,
  `validation_oracle_seconds`, `validation_engine_query_seconds` and
  `validation_engine_setup_seconds`, the last two as `engine=seconds` pairs.

### Fixed

- **Comunica could not query an HDT file by path.** A bare path is treated as a
  link to dereference and failed with "Could not dereference". The source is now
  addressed as `hdt@<path>`. Found by the first real four-engine run, not by
  inspection.
- **The parser oracle was reading htslib's header, not the file's.** cyvcf2's
  `raw_header` is normalised: htslib injects
  `##FILTER=<ID=PASS,Description="All filters passed">` into files that never
  declared it. The oracle therefore expected a `FilterDefinition` resource the
  graph could not contain, and every header check failed. `read_vcf_header_text`
  now reads the header block from the file itself — the same text the conversion
  reads — for both plain and gzipped VCFs.

### Verified

- A four-engine run over the fixture: all four `PASS`, all four agree, and the
  benchmark records per-query timings for each.
- `test/cross_engine_agreement.py` extended from two engines to all four,
  across both representations, with per-run scratch so the native engines cannot
  read each other's artifacts. All queries agree and every engine agrees with
  the Python oracle.
- 347 host tests pass (24 new); mutation score unchanged at **76/78**.

### Documentation

- [`docs/validation.md`](docs/validation.md) — rewritten engine section covering
  all four engines, multi-engine runs, native artifact querying, and the
  benchmark report. Its "What is *not* tested" section was stale — it predated
  the census, the identity digests, and the QUAL/INFO coverage work, and
  contradicted [`docs/vcf-coverage.md`](docs/vcf-coverage.md) — and now states
  the gaps that actually remain.
- [`docs/validation-methodology.md`](docs/validation-methodology.md) — "Two
  engines" became "Four engines", with the two new cross-engine findings and a
  section on why a multi-engine run is a controlled benchmark rather than an
  incidental one.
- [`README.md`](README.md), [`docs/cli-reference.md`](docs/cli-reference.md),
  [`test/README.md`](test/README.md) updated; the README's stale mutation score
  (36 mutations, 64/66) corrected to 42 and 76/78.

## 2026-09-04 — Design proposal: granular privacy policies over the VCF graph

[`docs/privacy-policy-design.md`](docs/privacy-policy-design.md) — **design
proposal, not implemented.** A system for governed, partial release of a VCF
graph: which participants, which regions, which fields, at what resolution, to
whom, for what purpose — declared in ODRL and enforced by the pipeline.

### Added

- **An ODRL profile whose assets are graph selectors.** ODRL's `odrl:target`
  addresses an Asset IRI and has no notion of "these triples", so the profile
  supplies `ClassSelector`, `PredicateSelector`, `SampleSelector`,
  `RegionSelector`, `FieldSelector`, `HeaderSelector` and a SPARQL
  `PatternSelector` escape hatch. It also pins the conflict semantics ODRL
  leaves informative: deny-overrides, default-deny, most-restrictive-first
  composition.
- **Effects beyond permit/prohibit**, because the useful answer for genomic data
  is usually less resolution rather than nothing: `drop`, `pseudonymize`,
  `generalize`, `threshold`, `aggregateOnly`, and `maskVectorPositions`.
- **Three enforcement tiers derived from the existing pipeline** — TSV
  pre-filtering before RMLStreamer (cheapest, covers RML-produced triples),
  emitter-time filtering inside `_append_rdf_atomically` (full record context,
  no second pass), and a post-hoc `--mode redact` (needs two passes for region
  rules, since an N-Triples stream has no ordering guarantee). The compiler
  assigns each rule to the cheapest tier that can express it, and **aborts** on
  any rule no tier can enforce rather than warning.
- **GA4GH DUO consent codes as `odrl:purpose` right operands**, so a policy is
  reviewable by the data access committees that already speak DUO.
- **Release manifests and policy conformance verification**: prohibitions
  compile to `ASK` preflights that must return zero, with mutation-catalogue
  entries that break the redactor and assert the checks catch it — the same
  "measured, not asserted" standard as
  [`validation-methodology.md`](docs/validation-methodology.md).

### Findings that affect work outside the proposal

- **`ParsedSampleRecord` carries no `CHROM` or `POS`.** `_parse_row` reads
  columns 0, 1, 7, 9, 10 and −1, skipping 2 and 3. Any region-scoped feature
  evaluated at emission time needs both; adding them is two dataclass fields and
  two index reads.
- **Condensed mode cannot express per-sample protection by triple filtering.**
  One `FormatValueVector` literal holds every participant's value for a FORMAT
  key, so the unit of protection is finer than the unit of storage. Redaction
  must rewrite the literal in place (replacing a position with `.` to preserve
  `sampleIndex` alignment), and a single masked position is itself disclosive.
- **IRIs and header lines leak independently of genotypes.** Sample names, the
  source filename and a monotonic row counter are all embedded in IRIs, and
  `##source` / `##SAMPLE` / `##PEDIGREE` / free-text `Description` fields are
  transcribed verbatim. Recorded in
  [`docs/limitations.md`](docs/limitations.md).

### Framing kept throughout

The document is explicit that this is **governed release, not anonymization**.
Genotypes are identifiers — a few dozen independent common variants single out
an individual — so an access-control layer governs who receives what and creates
an audit trail; it does not make released data non-identifying. Differential
privacy is discussed and explicitly **not** proposed, because without a
persistent per-recipient budget ledger it provides no protection.

## 2026-09-04 — Documentation set, and a data-linking design proposal

`docs/` becomes an in-depth explanation of the whole tool rather than four
validation-focused documents. Every part of the pipeline is now described, with
its limitations stated next to its capabilities rather than in a footnote.

### Added

- [`docs/README.md`](docs/README.md) — index, reading paths per audience, and
  the conventions the documentation set follows.
- [`docs/architecture.md`](docs/architecture.md) — the host/container split,
  what runs where, the **three places that split deliberately leaks** (genotype,
  header and QUAL/INFO emission happen on the host because RML cannot choose a
  datatype or class per row), the three-way failure policy, and the pinned
  toolchain versions.
- [`docs/conversion.md`](docs/conversion.md) — VCF -> TSV -> RDF stage by stage,
  including what the single-`awk`-pass parser can and cannot see, the complete
  IRI template table, and the datatype and missing-value decisions.
- [`docs/representations.md`](docs/representations.md) — the compression plan's
  three independent decisions, record-safe chunking, the `hdtc` and PyArrow
  rationales, and what the round-trip check does *not* prove.
- [`docs/output-and-metrics.md`](docs/output-and-metrics.md) — output layout,
  the `run_metrics/` tree, input size accounting, progress, interrupts, exit
  codes.
- [`docs/rml-mappings.md`](docs/rml-mappings.md) — the `--rules` contract in
  full, plus what a custom mapping does *not* control and what it costs in
  validation.
- [`docs/cli-reference.md`](docs/cli-reference.md) — every flag, grouped, with
  enforced constraints, environment variables, and exit codes.
- [`docs/limitations.md`](docs/limitations.md) — one consolidated, honest
  account: operational, input handling, modelling, extension points,
  compression, validation, vocabulary, and scope.
- [`docs/roadmap.md`](docs/roadmap.md) — planned work, known defects, and
  options that were assessed and deliberately rejected.
- [`docs/datalinking-design.md`](docs/datalinking-design.md) — **design
  proposal, not implemented.** A plug-in architecture for linking the graph to
  external resources, built on the observation that rsID, gene and clinical
  linking are all one of three joins (`token`, `interval`, `allele`). Three
  plugin tiers (declarative template, local reference bundle, live service),
  enforced network safeguards, side-graph output with provenance, and a build
  order that freezes the extension contract before capability is added.

### Fixed

- **Documented behaviour that did not match the code.** `README.md` and
  `docs/validation.md` both stated that the wrapper switches `q09`-`q13` to
  report-only under a custom `--rules`. It does not: `--mapping-policy` exists
  in `validation_runner.py` but `vcf_rdfizer.py` never forwards it, so the
  runner always executes `strict` and a *correct* custom-mapping conversion is
  reported as `MISMATCH`. The claim is corrected in both places, recorded in
  [`docs/limitations.md`](docs/limitations.md), and tracked as a defect in
  [`docs/roadmap.md`](docs/roadmap.md). No code change yet.

### Changed

- Existing documents gained consistent navigation headers and "See also"
  footers, and `README.md` now points at `docs/README.md` as the documentation
  entry point rather than listing two files.

## 2026-09-04 — Graph integrity: blank nodes, empty terms, duplicate statements

Three checks that verify the N-Triples graph itself, independently of what the
VCF contains. Mutation score **64/66 -> 76/78**, catalogue 36 -> 42 mutations.

### Added

- `preflight_blank_nodes` — any blank node, in subject or object position. Every
  class in the vocabulary declares an `vcfc:iriTemplate`, so a blank node means
  a term map produced no IRI; the record and value digests could not address
  such a node, and neither could anything that later merges the graph. A
  predicate cannot be blank in RDF, so it is not examined.
- `preflight_empty_values` — empty or whitespace-only literals and IRIs, each
  labelled `EMPTY_LITERAL` or `EMPTY_IRI` in the sample. An empty literal means
  a value was lost rather than marked missing (the pipeline writes
  `"."^^vcfc:Null` for a genuine missing token); an empty IRI means a template
  substitution collapsed. Whitespace-only counts as empty: it carries no more
  information and is just as certainly a defect.
- `preflight_duplicate_triples` — the same statement emitted more than once.

Both anomaly checks have exact-count companions, so severity is measured rather
than saturating at the 100-row diagnostic sample.

### How duplicates are detected, and why it could not be a query

A SPARQL store holds a **set**. A repeated line is collapsed on load, so it is
invisible to every other check here — `COUNT(*)` over `?s ?p ?o` returns the
distinct total on Comunica, QLever and rdflib alike. Verified before building
anything: a three-line file with one repeat reports 2.

The detector compares the statements the parser read against the distinct
triples the store holds. Raptor supplies the first (`rapper -c` counts parsed
statements including repeats — confirmed in the image: it reports 3 where the
store reports 2), `preflight_distinct_triple_count` the second, and the
difference is exactly the number of redundant statements. Duplicating the whole
fixture graph reports `parsed=624, distinct=312, duplicates=312`.

This is worth having because duplicated RDF parts are a known failure mode of
the conversion — `run_conversion.sh` already carries a defensive dedupe for it —
and a duplicated aggregate costs twice the storage for no added information.

### Behaviour

- All three are blocking: a graph that fails one is not worth comparing.
- When an input a check needs is unavailable, it reports `NOT_EVALUATED` rather
  than `PASS`, and `NOT_EVALUATED` never fails a run. A check that could not run
  must not be mistaken for a clean result in either direction.

### Verified

- All 22 queries (13 core, 9 preflight/count) return identical values under
  Comunica and QLever for both representations, and both engines produce `PASS`
  against the Python oracle — including the new `ISBLANK`, `ISIRI` and
  `REGEX`-based checks.
- 317 tests pass. Six new mutations (`introduce_blank_node`,
  `blank_node_object`, `empty_literal`, `whitespace_only_literal`,
  `duplicate_triple`, `duplicate_whole_graph`) are all detected.

### Documentation

- `docs/validation.md` gained a "Graph integrity" section explaining why
  duplicates cannot be found by a query.
- `docs/vcf-coverage.md` lists the three under graph-level properties.
- `docs/validation-methodology.md` now describes four independent layers rather
  than three.

## 2026-09-04 — Full VCF coverage: census, identity digests, INFO, headers, SHACL

Phases 2-6 of the validation-coverage plan. Mutation score **29/45 -> 64/66**
(64% -> 97%), across a catalogue that grew from 21 to 36 distinct mutations.

### Fixed — silent data loss

- **QUAL was extracted from every VCF and never mapped into RDF.** It is now
  emitted, typed `xsd:decimal` or `"."^^vcfc:Null` as the published SHACL shape
  requires. RML cannot choose a datatype per row, so it comes from a new
  record-detail emitter rather than `default_rules.ttl`.
- **`##fileDate` was never mapped.** Now emitted as `vcfc:fileDate`, typed
  `xsd:date` when the value's form allows and lexically otherwise.

### Added — completeness (Phase 2)

- `q09_predicate_census` and `q10_class_census` compare the graph's predicate
  and class inventory against counts derived from the VCF. One comparison
  catches three things: a predicate missing, one with the wrong cardinality, and
  one that should not be in the graph at all. Expectations are derived from the
  VCF and the emitters' documented shapes, never from `default_rules.ttl` -
  deriving them from the mapping is how QUAL stayed invisible.
- `--mapping-policy {strict,report-only}`: a custom `--rules` changes the
  inventory and IRI templates by design, so these checks become report-only.

### Added — record and value identity (Phases 3-4)

- `q11_record_digest`, `q12_info_value_digest`, `q13_format_value_digest` hash
  each record, INFO value and FORMAT value **together with its own IRI** and
  bucket the result. Binding identity into the hash is what catches a
  permutation; bucketing keeps the result at most 256 rows for any graph size
  and needs no ordering guarantee, which `GROUP_CONCAT` could not have given
  portably. Fields are separated by U+001F, which cannot occur in a VCF field.
- These close the two largest gaps: values permuted between records, and non-GT
  FORMAT values (a mangled DP, GQ or AD previously passed).

### Added — structured INFO (Phase 4)

- `--info-representation {structured,raw}`, default `structured`: one
  `vcfc:InfoFieldValue` per record and key at the vocabulary's declared IRI
  template, linked by `vcfc:declaredBy` to an `InfoFieldDefinition` carrying
  `fieldId`/`fieldNumber`/`fieldType`/`fieldDescription`. Single-valued
  Integer/Float fields also get `fieldValueInteger`/`fieldValueDecimal`; a Flag
  gets `fieldValueBoolean true`. `vcfc:infoRaw` is retained.
- The model was already fully defined in the vocabulary and simply unused.

### Added — header section (Phase 5)

- `--header-representation {structured,basic}`, default `structured`: each `##`
  line is typed with its vocabulary subclass, and FILTER, ALT and contig
  declarations get `filterId`, `altId`, `contigId`, `contigLength`,
  `contigMd5`, `contigAssembly` and `contigCount`. An unrecognized key keeps
  only the base `HeaderLine` type rather than inventing an undefined term.

### Added — SHACL (Phase 6)

- `--shacl-shapes PATH` validates the graph against a SHACL shapes file with
  `pyshacl`, inside the container, as a layer independent of the VCF entirely.
  Off by default because `pyshacl` loads the graph into memory. A violation
  blocks the run; a missing `pyshacl` is reported as `EXECUTION_FAILED`, never
  as a conformance failure.
- It found three violations the other layers could not see. Two are fixed above.
  The third is a **contradiction inside the published vocabulary**:
  `vcfc:missingValuePolicy` says a missing token SHOULD be `"."^^vcfc:Null`,
  while `VCFRecordShape` constrains `vcfc:alt` to `xsd:string`, so a record with
  `ALT=.` cannot satisfy both. Recorded in `docs/vcf-coverage.md` for a decision
  in the vocabulary repository.

### Changed

- `evaluate_validation()` is the single place the PASS/MISMATCH/BLOCKED decision
  is made, used by real runs and the mutation harness alike.
- `test_validation_logic_unit.py` now shares the canonical fixture instead of
  maintaining a second description of the same data.

### Verified

- All 13 core queries plus 4 exact-count preflights return **identical values
  under Comunica and QLever**, for both representations, and both engines
  produce `PASS` against the Python oracle end to end. The agreement script now
  checks that last part too: engines agreeing with each other is not enough when
  the digests are compared against values Python computes.
- 297 tests pass; the suite still passes without `rdflib` (those tests skip).

### Documentation

- `docs/vcf-coverage.md` rewritten: every VCF element with separate
  "represented" and "validated" columns, each validated claim backed by a named
  mutation, the remaining gaps, and the vocabulary alignment problem.
- `docs/validation-methodology.md`: the three independent layers, why identity
  digests are histograms, and how to reproduce the score.
- README gained a "VCF Coverage" section for the three representation options.

## 2026-09-04 — Validation mutation harness, and the first coverage gaps closed

### Added — measurement (Phase 0)

- A mutation-testing harness for the semantic validation suite. A correct graph
  is corrupted in ~25 named ways and the validator is asked for a verdict; the
  proportion detected is a reproducible **mutation score**. Before this, the
  suite's coverage was prose, and that prose was wrong: `QUAL` is extracted from
  every VCF and then never mapped into RDF, and no check could have noticed.
  - `test/validation_fixtures.py` derives the VCF, the RDF graph and the parser
    oracle from one declarative specification, so they cannot drift apart. The
    genotype triples come from the project's own emitters.
  - `test/validation_mutations.py` is the catalogue: each entry names the VCF
    element it targets, the check expected to catch it, and — for a recorded
    gap — why it is not caught.
  - `test/test_validation_mutation_unit.py` evaluates queries in-process with
    rdflib (test-only dependency, tests skip without it) and routes every
    verdict through the shipped `evaluate_validation()`, so the harness measures
    real code rather than a reimplementation.
  - **Known gaps are assertions.** A mutation marked `known_undetected` is
    asserted to still be undetected, so closing a gap fails a test and forces
    the catalogue and `docs/vcf-coverage.md` to be updated.
- `test/cross_engine_agreement.py` plus a CI job asserting every validation
  query returns identical values under Comunica and QLever. Verified: all 34
  executions across both representations agree.
- `.github/workflows/validation-mutation.yml` publishes `mutation-score.json`
  as a build artifact.

### Added — coverage (Phase 1)

- **Exact anomaly counts.** Each structural anomaly preflight now has a
  companion aggregate returning the true count alongside the `LIMIT 100`
  diagnostic sample. Reports carry `anomalyCount`, `anomalyCountReturned` and
  `sampleTruncated`, so ten million anomalies is no longer indistinguishable
  from a hundred. Verified against a graph with 250 anomalies: exact count 250,
  sample 100, truncated true.
- **`--strict-conformance`** (runner and wrapper) promotes a missing-token
  conformance failure from a report-only observation to a validation failure.
- **File metadata and header coverage.** New `q07_file_metadata` compares the
  `##fileformat` / `##reference` / `##source` declarations, and
  `q08_header_line_census` compares how many `##` lines carry each header key.
  `parse_vcf` gained `parse_header_metadata` to compute the oracle side. These
  close the two entirely uncovered triples maps.

Mutation score: **23/45 → 29/45 (51% → 64%)**.

### Changed

- The PASS / MISMATCH / BLOCKED_BY_PREFLIGHT decision is extracted into
  `evaluate_validation()`, the single place real runs and the mutation harness
  both use.
- `normalize()` generalises its single-row handling beyond `q03_titv` via
  `SINGLE_ROW_QUERIES`.
- `rdflib>=7.0.0` added to the `dev` extra. It is not a runtime dependency and
  the test suite passes without it.

### Documentation

- **`docs/vcf-coverage.md`** — the coverage matrix: one row per VCF element,
  with separate "represented" and "validated" columns, each validated claim
  backed by a named mutation. Includes the vocabulary alignment gap (17 emitted
  terms the vocabulary does not define, all condensed-representation) and the
  open gaps in priority order. This is the table intended for publication.
- **`docs/validation-methodology.md`** — the mutation-testing method, why
  self-reported coverage failed, the two-engine two-layer design, and how to
  reproduce the score.
- `docs/validation.md` — Q7/Q8 documented, the exact-count behaviour and
  `--strict-conformance` described, and the "What is *not* tested" section
  narrowed to what is still true.

## 2026-09-04 — QLever integration verified against a live container

Everything below came out of actually running the integration, and each item
was wrong or missing beforehand.

### Fixed

- **`preflight_position_datatype` would have failed every QLever run.** QLever
  canonicalises numeric literals at index time, so `"100"^^xsd:integer` is
  reported by `DATATYPE()` as `xsd:int`. The query required exactly
  `xsd:integer`, so on QLever it flagged every record and the run ended as
  `BLOCKED_BY_PREFLIGHT`, while passing on Comunica. It now accepts the XSD
  integer family, which is what the check means; a plain-string or
  `xsd:decimal` POS is still reported as an anomaly on both engines (verified).
- **QLever's binaries are not called `IndexBuilderMain`/`ServerMain`, and are
  not in `/usr/bin`.** They are `/qlever/qlever-index` and
  `/qlever/qlever-server`. The Dockerfile COPY paths and the default argv were
  both wrong and would have failed the build.
- **Copying the binaries alone does not work.** The upstream image is Ubuntu
  24.04 and this base is Ubuntu 26.04; the binaries need Boost 1.83, ICU 74,
  jemalloc and io_uring sonames that do not exist here and cannot be
  apt-installed (the sonames are release-pinned). Those nine libraries are now
  copied into `/opt/qlever/lib`, and `LD_LIBRARY_PATH` is set for QLever's own
  processes only, so they cannot shadow anything else in the image. glibc is
  deliberately not copied - it is backward compatible and this base is newer.
- **`qlever-server` defaults to a 30-second query timeout**, far below what the
  aggregate queries need. The server is now started with `-s` matching
  `--validation-query-timeout`.
- Index/server flags corrected against the real CLI: `-m` is `--stxxl-memory`
  for the indexer and `--memory-max-size` for the server (`--stxxl-memory` is
  not a server flag).

### Verified

- Both binaries link and run in the target image (`qlever-index --version`
  reports build `bfd5741`).
- A real index build, server start, readiness poll, HTTP query, SPARQL Results
  JSON parse and `normalize()` round-trip, driven through the shipped
  `QleverEngine` code rather than a reimplementation.
- **Engine equivalence**: all eleven validation queries run under both Comunica
  and QLever against the same expanded and condensed graphs — graphs built with
  the project's own sample emitters — produce identical normalized results, and
  the values are independently correct.

### Tests

- `QleverEnvironmentTests`: the private library path is prepended for QLever
  only, and the POS datatype preflight accepts the integer family while still
  rejecting string/decimal/double.
- The QLever lifecycle test now asserts the verified binary names and flags,
  including the explicit server-side query timeout.

### Still unverified

- The HDT and COTTAS decode paths, and a full `docker build .` of the real
  image. Those need hdt-cpp and the pycottas venv; the QLever work above was
  done against a minimal probe image that isolates the integration.

## 2026-09-04 — Validation: QLever engine, HDT/COTTAS artifacts, coverage audit

### Added

- `--validation-engine {comunica,qlever}`. Comunica remains the default and
  queries the graph in memory; [QLever](https://github.com/ad-freiburg/qlever)
  builds an on-disk index inside the container, serves it on a container-local
  port, answers the queries over HTTP, and tears both down. Both engines answer
  identical queries and feed the same comparison layer, so the choice is a scale
  decision, never a semantic one, and every report records which engine ran.
  Tunable with `--qlever-memory-gb`, `--qlever-port`,
  `--qlever-startup-timeout`, and `--validation-query-timeout`.
- QLever binaries are copied into the image from the upstream
  `adfreiburg/qlever` image; pin a version with
  `--build-arg QLEVER_IMAGE=adfreiburg/qlever:<tag>`. A build-time linkage
  check records whether they resolve against this base, and the validator
  surfaces that note if the engine cannot start.
- QLever's CLI has changed across releases, so both command lines are
  overridable without code changes: repeatable `--qlever-index-arg` /
  `--qlever-server-arg`, or full replacement via `QLEVER_INDEX_COMMAND` /
  `QLEVER_SERVER_COMMAND` with `{index}`, `{input}`, `{memory}`, `{port}`
  placeholders. The exact argv that ran is recorded in `manifest.json`.
- Validation of compressed and indexed artifacts. `--rdf` now accepts `.nt`,
  `.nt.gz`, `.nt.br`, `.hdt`, `.cottas`, `.cottas.gz`, and `.cottas.br`;
  `--rdf-format` overrides filename detection. Anything that is not already
  plain N-Triples is decoded in container scratch (`hdt2rdf`,
  `cottas_tool.py decompress`, gzip/brotli) and then put through the full
  semantic suite. That is strictly stronger than the triple-count round-trip
  `validate_compression.py` performs during compression: it proves the artifact
  decodes to a graph that still reproduces every VCF summary. Nothing decoded
  is written beneath `--out`.
- `--validate-artifacts {aggregate,hdt,cottas,all}` for full mode. Each
  requested artifact is validated independently with its own report directory
  (`<id>`, `<id>__hdt`, `<id>__cottas`), so one run can prove the aggregate and
  both representations agree with the VCF. A representation that was not
  selected, or whose artifact is missing after a recoverable index warning, is
  skipped rather than reported as a failure. Default is `aggregate`, which
  preserves the previous behaviour.
- `materialization.json` per validation run, recording how the artifact was
  decoded and how many triples that yielded; `manifest.json` gained `engine`
  and a format-tagged `sourceRdf`. Rapper's parsed triple count is now captured
  rather than discarded.

### Changed

- The validator's cyvcf2 import is lazy, so the pure normalization/comparison
  layer is importable and testable on the host without the container.
- `--mode validation` no longer requires `.nt.gz`; any supported artifact works.
  `--rdf-nt` / `--rdf-gz` remain accepted as aliases.

### Tests

- `test/test_validation_logic_unit.py` (16 tests): mutation tests that drive the
  comparison layer directly and record what it detects — dropped records,
  misclassified shapes, flipped genotypes, corrupted FILTER lexicals, Ti/Tv
  swaps, allele-count errors, spurious records, impossible AC/AN — **and** what
  it does not: record permutation, and the entirely unvalidated `ID`, `QUAL`,
  `INFO`, non-`GT` `FORMAT`, and header/metadata columns. The blind-spot tests
  are deliberate, so closing a gap fails a test instead of passing unnoticed.
- `test/test_validation_engines_unit.py` (25 tests): artifact format detection
  and host/container agreement, each decode path, decode-failure reporting,
  engine registry, Comunica command construction, QLever index/serve/teardown,
  crashed-server diagnostics, command-line overrides, CLI argument validation,
  and the wrapper's target resolution and docker argv.

### Documentation

- `docs/validation.md`: new "Which artifact is validated", "Which SPARQL engine
  runs the queries", and — importantly — "What is *not* tested", which states
  the coverage limits plainly: aggregate-only comparison with no per-record
  identity, unvalidated VCF columns, no completeness bound, saturating preflight
  anomaly counts.
- README: validation artifacts/engines subsection with a short note on what a
  `PASS` does and does not mean.

## 2026-09-04 — Custom-mapping tooling, cheap gzip sizing, dead-script removal

### Added

- `vcf-rdfizer-rules`, a second console script (`vcf_rdfizer_rules.py`) for
  authoring custom RML mappings. It replaces the `update_rules.sh` sed script
  with something that serves the actual need:
  - `columns` documents the five generated TSV sources and every column each
    one provides, including why `records.tsv`'s final column has no fixed name.
  - `init` writes an annotated copy of the default mapping to start from.
  - `check` validates a mapping against the wrapper contract *before* a long
    run: logical-source paths the wrapper cannot rewrite per input, referenced
    columns no TSV provides, which `--sample-representation` values remain
    usable, and whether the mapping forces the large sample helper tables to be
    materialized. Exits 1 on a contract violation; `--json` for scripting.
  The old script rewrote a rules file *in place* to point at fixed TSV paths,
  which the pipeline has not needed since `render_rules_for_triplet` began
  rendering a per-input copy - and which silently broke the per-input rewrite
  it was supposed to help with.
- `vcf_rdfizer_gzip.py`: uncompressed size of a gzip/BGZF file without
  decompressing it. Three tiers, and the method used is recorded next to the
  size so the metric stays auditable:
  - `bgzf` - sums the per-block `ISIZE` trailers of a `bgzip` file by walking
    block headers. Exact, no inflate, and immune to the 32-bit wrap. This is
    what `bcftools`/`tabix`/`htslib` produce, so it covers the usual case.
  - `gzip-sample` / `gzip-trailer` - single-member gzip: measured outright when
    it fits the sampling budget, otherwise the 32-bit `ISIZE` trailer resolved
    against a compression ratio sampled from the file's own prefix.
  - `inflate` - full pass, used whenever the structure cannot settle the
    answer. Concatenated non-BGZF members are detected and routed here rather
    than trusting a trailer that describes only the final member.

### Changed

- `run_conversion.sh` uses that helper for `input_vcf_size_bytes`, which
  previously always cost a full `gzip -dc | wc -c` pass over the source VCF.
  On the repository's 52 MB test fixture this is 1.115s -> 0.066s (17x); on a
  634 MB gzip whose uncompressed size exceeds 4 GiB, 13.6s -> 0.029s (470x),
  where a naive trailer read would have been 93% wrong. The shell keeps its
  `gzip -dc` fallback if the helper is unavailable.
- `stages/conversion/*.json` gains `input_vcf_size_method` recording how the
  size was obtained.
- `--estimate-size` now uses the real uncompressed input size when the file's
  structure can supply it, instead of always assuming a 5x expansion, and says
  so when it had to fall back to the assumption.

### Removed

- `src/compression.sh` and `test/test_compression_unit.py`. The wrapper issues
  its own gzip/brotli/rdf2hdt commands and delegates chunked work to
  `partitioned_compression.py`; nothing had called this script for some time,
  and it wrote a stale flat `metrics.csv` schema.
- `src/update_rules.sh` and `test/test_update_rules_unit.py`, superseded by
  `vcf-rdfizer-rules` above.

### Documentation

- README: new "Custom RML Mappings" section with the three-point contract, the
  logical-source table, and the `vcf-rdfizer-rules` workflow; a metrics table
  explaining each `input_vcf_size_method`; repository-layout and test-suite
  entries for the two new modules.
- `rules/README.md` points at the new CLI as the way to author a mapping.

### Tests

- `test/test_rules_helper_unit.py` (12 tests), including a drift guard that
  runs `src/vcf_as_tsv.sh` and asserts the documented column lists still match
  the headers it writes.
- `test/test_gzip_size_unit.py` (13 tests), each checking a measured size
  against a full-inflate ground truth across BGZF, plain, concatenated, and
  optional-header-field gzip layouts.

## 2026-09-04 — Code review: cleanup, hot-path performance, and bug fixes

Behaviour-preserving unless noted. Emitted RDF was verified byte-identical
(SHA-256) against the previous implementation for both sample representations
and for both `run_conversion.sh` storage modes.

### Fixed

- `run_compression_methods_for_rdf`: an existing `.hdt` that failed validation
  during a full run recorded a **COTTAS** index warning (wrong format, wrong
  artifact path, wrong message) via a copy-pasted block whose assignment also
  never propagated, because `cottas_failure_warning` was not `nonlocal` there.
  The block is removed; `validate_container_artifact` already downgrades a
  recoverable HDT index problem to a warning, so reaching that point means the
  artifact is genuinely unusable.
- `--mode validation` raised an uncaught `ValueError` traceback when the
  results directory already existed and was non-empty, because that check sits
  after the argument-validation `try/except`. It now prints `Error: ...` and
  exits 2 like every other input error. Covered by a new regression test.
- `count_triples_in_nt_files` (the host-side fallback triple count) used a
  slightly different rule than the container-side counters in
  `validate_compression.py` / `partitioned_compression.py`, so a fallback count
  could disagree with the authoritative one. The predicate is now shared as
  `is_triple_line` with identical semantics.

### Performance

- Sample RDF emission (`append_expanded_sample_rdf`,
  `append_condensed_sample_rdf`) no longer percent-encodes the same values on
  every record: sample-column URI components and `sampleId` literals are
  computed once per file, and FORMAT-key components are memoized. Previously
  these ran `variants x samples` and `variants x samples x FORMAT keys` times.
  ~1.6x faster at 50 samples, with the gain growing with cohort width.
- `_rml_uri_component` and `_ntriples_string_literal` short-circuit values that
  need no encoding/escaping (~2.7x and ~1.3x on typical VCF tokens).
- `SampleRecordStream` caches the derived FORMAT-key tuple and its
  duplicate-key check per distinct `FORMAT` string instead of rebuilding both
  for every record.
- `count_triples_in_nt_files` scans bytes instead of decoding to `str`, and
  reads gzip aggregates through a `BufferedReader` rather than `GzipFile`
  line-by-line.
- `run()` discards subprocess output at the file-descriptor level instead of
  buffering a whole container's output in memory only to drop it.
- `run_conversion.sh`: the `"."` -> `"."^^vcfc:Null` rewrite is now applied to
  each RMLStreamer part while it is streamed into the aggregate, instead of as
  a separate pass over the finished aggregate. This removes one full read and
  one full write of the complete RDF output, and removes the transient
  full-size temporary copy (which previously required 2x the aggregate size in
  free disk at the end of a plain-storage run).
- `run_conversion.sh`: triple counting uses one `LC_ALL=C grep -c` instead of
  `awk` / `grep | wc -l`. On a 228 MB gzip aggregate this is ~5x faster
  (6.5s -> 1.2s); it is the last unavoidable full pass over the RDF output.
- `run_conversion.sh`: the `stat` dialect is detected once instead of probed on
  every call, and the per-second progress heartbeat does one directory walk
  with no `basename` subprocess per part.

### Changed

- Space-optimized aggregates are ~14 bytes per RMLStreamer part smaller,
  because gzip members are now produced from a pipe and therefore no longer
  embed the temporary part filename and mtime. Decompressed content is
  unchanged and the output is more reproducible.

### Removed

- Dead host-side code: `resolve_input` (superseded by
  `resolve_input_snapshot`), and the host chunk planners
  `plan_record_safe_rdf_chunks`, `plan_partitioned_hdt_chunks`,
  `split_nt_file_for_hdt`, `write_nt_chunk`, `iter_rdf_binary_lines`. Chunking
  has been the container runner's job (`src/partitioned_compression.py`) since
  partitioned compression moved into an ephemeral Docker volume; keeping a
  second host-side implementation was exactly what
  `run_partitioned_representation_methods_for_rdf_files` documents against.
  Their two tests were removed with them.
- An unreachable `else:` arm in `run_full_mode`'s output summary (it iterated a
  list the guard had just proved empty), the unused `input_metrics_target`
  parameter of `run_full_mode`, an always-true `is not None` guard, and two
  unused local assignments.

### Documentation

- `vcf_rdfizer.py` gained a module-level division-of-labour note and section
  map, plus section banners for the previously unmarked regions (triple
  counting, artifact naming, destructive filesystem operations, preflight
  estimation, genotype representations, compression-plan parsing, metrics
  layout).
- `README.md`: new "Repository Layout" section explaining the host/container
  split and what each file does; fixed the broken `RELEASING.md` link
  (`scripts/RELEASING.md`); added links to `docs/`, `changelog.md`, and
  `ACKNOWLEDGEMENTS.md`.
- `src/compression.sh` and `src/update_rules.sh` headers now state that they
  are standalone utilities the pipeline does not call, and what supersedes
  each. `compression.sh` previously claimed the Python wrapper was its primary
  caller, which has not been true since compression moved inline.
- Added `ACKNOWLEDGEMENTS.md` with funding/attribution and a paper
  acknowledgement template.

## 2026-09-04 — Validation progress and quiet mode

- Integrated semantic validation with the existing JSONL progress sidecar and
  host `ProgressSession`; validation now reports its preflight/core query
  progress using the same terminal UI as conversion and compression.
- Added `--quiet` to suppress terminal progress displays and validation query
  chatter while retaining progress bookkeeping, command logs, and metrics.
- Kept `--no-progress` as the stronger opt-out that disables sidecar creation
  and progress rendering entirely.

## 2026-09-04 — Full-run semantic validation

- Added `--validate`/`--run-validation` to run the semantic VCF/RDF validator
  once per input as the final stage of a `--mode full` run.
- Full-mode validation accepts the generated plain `.nt` or gzip `.nt.gz`
  aggregate, and retains the validator's container-local temporary-file
  guarantees.
- Validation timing, status, exit code, input RDF, and report paths are now
  included in the run's `metrics.csv`, compression JSON, stage reports, and
  recursive `summary.json` report index.

## 2026-09-04 — Expanded sample representation naming

- Renamed the default per-sample genotype graph strategy to `expanded`, while
  retaining `condensed` as the alternative strategy.
- Updated the CLI default and accepted values, internal workflow/emitter names,
  tests, RML comments, and user documentation.
- The emitted profile IRI is now
  `vcfc:representationProfile vcfc:ExpandedRepresentation`.

## 2026-09-04 — Input-labelled, container-complete metrics layout

- Replaced timestamp-only run-metrics directories with
  `run_metrics/<input-label>__<run-id>/`. A single VCF/RDF/representation input
  uses its source stem (for example, `1000G_phase3_chr20`); a directory input
  with multiple VCFs receives a deterministic batch label.
- Added `run.json` and `summary.json` as the stable entry points for every
  mode. The manifest records inputs, requested configuration, output root, and
  resolved image; the summary records final status, wrapper runtime, tabular
  rows, and an index of stage reports and logs.
- Organized metrics by purpose: `logs/`, `timings/`, `stages/`, and `reports/`.
  TSV, RML conversion, compression, decompression, indexing, and warnings now
  have predictable locations instead of unrelated `raw_metrics`,
  `*_metrics`, and timestamp-nested directories.
- Compression-only, decompression, and standalone index modes now persist
  container wall/CPU/system time, peak RSS, exit code, input/output sizes, and
  structured stage reports. Full and TSV modes retain the same detail.
- Preserved the complete partitioned-compression runner handoff under
  `stages/partitioned/`, including every chunk build, merge, validation,
  temporary-workspace free-space sample, timing, peak RSS, and bounded stderr
  diagnostic. The report is retained on both successful and failed runs before
  the temporary Docker volume is removed.

## 2026-09-03 — Streaming COTTAS merge for condensed cohorts

- Replaced the final COTTAS merge/reindex implementation with a PyArrow k-way
  merge of the already `spo`-sorted Parquet chunks. It keeps one configurable
  batch per input (`COTTAS_MERGE_BATCH_ROWS`, default `2048`), deduplicates
  adjacent equal triples, and writes the final COTTAS artifact incrementally.
  It preserves RDF set semantics and the embedded COTTAS index without a
  graph-wide DuckDB hash table or external-sort spill area.
- This supersedes the prior DuckDB merge variants. A 52-chunk, 36-million-triple
  cohort exhausted 41.4 GiB of Docker workspace while DuckDB attempted an
  external sort; the streaming merge has no proportional temporary-sort file.
- Added PyArrow 22.0.0 to the image and third-party notices for deterministic
  Parquet reader/writer support.
- COTTAS merge progress now reports source triples processed and distinct
  triples written. Rich interactive displays remain available, and redirected
  terminals receive compact line-based progress updates.
- Retained the bounded stderr tail in failures so malformed COTTAS input,
  incompatible index metadata, output disk errors, and other code-1 failures
  remain actionable after the ephemeral workspace is removed.

### Rerun guidance

Build an image from this revision before retrying. The preserved raw `.nt.gz`
can be retried with `--mode compress`, avoiding another RDF run. The merge
requires normal space for the COTTAS chunks and final artifact, but no longer
requires a large DuckDB spill directory.

## 2026-09-03 — COTTAS SIGKILL/OOM handling

- Classified `exit_code=-9` in partitioned index warnings as a child process
  killed by `SIGKILL` (normally the Linux kernel/Docker OOM killer). Shell
  wrappers can surface the same condition as exit code `137`.
- Initially changed the production COTTAS merge to a bounded pairwise tree.
  This reduced input fan-in but did not bound the final graph-wide
  `DISTINCT`/`ORDER BY`; the disk-backed replacement above supersedes it.
- Added the failing stage's `max_rss_kb` to `index_warnings.json`, alongside
  exit code, stderr tail, and workspace free-space samples, so OOM diagnosis
  can be compared directly with the container memory limit.
- The `merge-many` adapter remains available for explicit experiments, but is
  no longer selected by the default large-file workflow.

### Rerun guidance

Rebuild the image and rerun the full conversion (or use `--mode compress` with
the retained raw `.nt.gz`). The current streaming k-way merge supersedes this
earlier pairwise guidance.

When COTTAS is optional, `--representations hdt` avoids the COTTAS merge
resource requirement while retaining a queryable representation.

## 2026-09-02 — COTTAS large-input merge reliability

- Diagnosed the previous generic `COTTAS merge/index creation failed` warning:
  the warning is emitted only after per-chunk COTTAS conversion succeeds and
  the final indexed merge returns a non-zero status. HDT generation/indexing
  is independent and is not implicated by that warning.
- Added a multi-input `pycottas.cat` adapter for explicit use. The production
  workflow now uses the bounded pairwise strategy documented in the 2026-09-03
  entry because the merge API can require substantial in-memory buffers.
- Added cleanup of all COTTAS chunks and merge outputs after a failed COTTAS
  attempt so stale intermediates cannot consume the Docker workspace.
- Preserved a bounded stderr tail and exit code for every partitioned stage.
  Full-run `index_warnings.json` entries now identify resource/OOM signals,
  include before/after Docker-workspace free-space samples, and retain the
  underlying COTTAS/DuckDB diagnostic when available.
- Pinned the Docker image to `pycottas==1.1.0` for reproducible behavior.
- Added regression tests for the multi-input merge command, stage diagnostics,
  and isolated COTTAS scratch cleanup.

### Rerun guidance

Rebuild the image and rerun the same full command. If the COTTAS stage still
fails, inspect `stages/partitioned/<sample>.json` and the wrapper log for the
failing `cottas-merge-r*` stage and its
`stderr_tail`. For a resource error, lower `--chunk-target-bytes` and
`--chunk-max-bytes`, or enlarge the Docker data-volume allocation. The raw
`.nt.gz` retained by the failed run is a valid recovery source.
