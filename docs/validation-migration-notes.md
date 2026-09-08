# Validation and the VCF Core migration

The conversion targets the **VCF Core vocabulary**
(`https://w3id.org/vcf-core/vocab#`, prefix `vcfc:`), replacing the retired
VCF-RDFizer vocabulary (`https://w3id.org/vcf-rdfizer/vocab#`, prefix `vcfr:`),
and follows each input's declared VCF version. The validation suite has been
migrated with it. This file records what changed, what it found, and what is
still open.

**Status: the suite passes.** 415 tests, no failures. The mutation score is
96/113 (85%) across 60 mutations, each listed with its expected detector.

---

## What the migration preserved

The suite's design is what made this tractable, so it was kept intact:

- **One declarative fixture, three derived artifacts.**
  `test/validation_fixtures.py` still declares one VCF and derives the graph and
  the oracle from it, so a fixture change cannot make the two disagree.
- **Coverage measured, not asserted.** A mutation the suite cannot catch is
  recorded with `known_undetected`, and the harness asserts it is *still* not
  caught — so closing a gap fails a test and forces the catalogue and the
  coverage documentation to be updated together.
- **Expectations derived from the VCF, never from the mapping.** Deriving them
  from `default_rules.ttl` would make the mapping test itself, which is how QUAL
  once stayed invisible. `base_triples()` still hand-writes what the mapping
  produces, so a mapping change the fixture does not follow shows up as a
  mismatch rather than being absorbed.

## What changed

### 1. One vocabulary module, not two mirrored copies

`src/validation/validation_runner.py` carried its own copies of
`HEADER_LINE_CLASSES`, `rml_uri_component`, `parse_structured_header_fields` and
`parse_info_entries`, with unit tests asserting the copies stayed identical to
the wrapper's. They are gone: both import `vcf_rdfizer_vocab`, and the
`Dockerfile` copies that module next to the runner so the import resolves the
same way inside the container and on the host.

That removes the drift the tests were policing rather than continuing to police
it. `test_header_class_map_matches_the_wrapper` now compares a module against
itself; it is kept as a guard against someone reintroducing a second copy.

### 2. The census learned the new layers

`expected_census` asserts an exact inventory — *these* predicates and classes
with *these* counts and nothing else — so every family the VCF Core emitters
added had to become derivable from the VCF. Two shared functions do that:

- `emitted_header_counters(header_lines)` — header-line subclasses,
  `vcfc:HeaderAttribute` resources, the INFO/FORMAT/FILTER/ALT/contig/META/
  SAMPLE/PEDIGREE declaration properties, `assemblyUrl`, `pedigreeDbUrl`.
- `emitted_record_counters(rows, samples, …)` — the allele layer, the
  `vcfc:FieldValueItem` decomposition, the breakend components, and — reported
  separately, because it is expanded-only — the parsed genotype layer.

Both are called by `parse_vcf` inside the container and by the fixture's
`parser_summary` on the host, so the two oracles cannot disagree about what the
graph should contain. `parse_vcf` stays representation-independent, which is
what lets one parse serve both censuses.

### 3. `vcfc:VCF4xFile` is part of the expectation

The mapping's version sentinel resolves to the subclass for the version the
input declares, so the census expects that class. The fixture derives it from
its own `FILE_FORMAT` rather than hard-coding it, so changing the fixture's
declared version changes the expected graph with it.

### 4. Fields that moved from RML to the wrapper

`ID`, `ALT`, `FILTER` and `infoRaw` left `base_triples()`: each may be the VCF
missing token, which the vocabulary requires as `"."^^vcfc:Null`, and RML cannot
switch an object's datatype per row. The counts are unchanged — one per record —
so only the fixture's account of *which half emits them* moved.

`preflight_missing_token_conformance` is consequently expected to return zero
rows for any input now. Before the migration, a record with `ID=.` produced a
plain `"."` literal and the check reported it.

### 5. Ordinals are `xsd:integer`

The fixture's `XSD_POSITIVE_INTEGER` is gone. VCF Core's
`vcfc:IntegerLiteralShape` accepts `xsd:integer` and every integer-derived
datatype with the bound applied numerically, so the ontology range and the
shapes no longer disagree.

## What the suite found

Running it against the migrated emitters surfaced a real conversion defect that
neither the unit tests nor SHACL had caught:

> **Every declared INFO and FORMAT definition was emitted twice.** The header
> emitter produced a declaration's `fieldId`/`fieldNumber`/`fieldType`/
> `fieldDescription` from the `##` line, and the value emitters produced the
> same four triples at the same header-line IRI when the key was first
> referenced — twenty duplicate triples in the fixture graph alone.

`preflight_duplicate_triples` caught it: a check that compares the parsed triple
count against the distinct count. No shape can express that, because a duplicate
triple is not a distinct RDF statement at all — SHACL validated the graph
happily, since the duplicate simply vanishes on parse. It is a good argument for
keeping the graph-integrity preflights alongside the shape layer rather than
treating SHACL as a replacement for them.

The fix gives the header emitter sole ownership of any declaration derived from
a `##` line. The value emitters cite it with `vcfc:declaredBy` and invent a
definition only for a key no header declared.
`test_declared_field_definitions_are_emitted_exactly_once` pins it.

## Recorded gaps

Ten of the 18 new mutations are undetected, each with what would close it. They
fall into three groups.

**Values counted but not compared.** `corrupt_allele_value`,
`corrupt_allele_kind`, `corrupt_attribute_value`, `corrupt_value_item_allele`,
`corrupt_called_allele`, `flip_phasing_status`, `wrong_declaration_owner`.
Dropping any of these resources is caught by the census; corrupting one inside
is not. Closing them means digests over those resources, the way `q11` covers
the record fields. Several are already checked by the vocabulary's own
`vcf-core-consistency.shacl.ttl`, which verifies raw/parsed agreement — so the
cheapest route is probably to run that profile rather than to reimplement it in
SPARQL.

**Ordering, covered in a different layer.** `corrupt_record_index` is undetected
by the queries. The SPARQL SHACL profile enforces index uniqueness, nondecreasing
POS within a CHROM block, and contiguous CHROM blocks.

**One profile-specific asymmetry, worth stating.** `corrupt_sample_index` is
detected in the condensed profile and not in the expanded one, and the catalogue
carries it as two entries for that reason. In the condensed profile the ordinal
is what associates a vector position with a sample, so corrupting it moves
genotypes between samples and `q05`/`q06` shift. In the expanded profile each
`SampleCall` carries its own `vcfc:sampleId`, so nothing reads the ordinal. That
is a real difference in what the two profiles depend on, not a testing
oversight.

## Still open

**SHACL conformance is not a test.** Validating the output against the
vocabulary's profiles found seven modelling bugs during this work and is a
stronger, cheaper invariant than a hand-derived oracle. It needs `pyshacl` and
`rdflib` as dev dependencies plus a path to the vocabulary's `ontology/`,
`ontology/versions/` and `shacl/` directories, and it is slow — roughly ten
seconds per graph with the full profile set — so it belongs in a marked slow
suite rather than the default unit run.

**The fixture declares one VCF version.** Version-dependent behaviour is covered
by unit tests and by the version overlays, not by the mutation harness. The
vocabulary repository ships one example VCF per version under
`examples/vcf-versions/`, a ready-made corpus for a per-version fixture matrix
if that coverage is wanted.

**The SV carriers have no mutation.** They are represented and conformant, and
the census counts the families the fixture exercises, but the fixture contains
no structural variants, so `emitted_record_counters` is untested against
breakends, tandem repeats and reference blocks. Adding one SV record to the
fixture would exercise them through the existing machinery.

## How conformance was verified

Separately from the suite, the converter's output was validated against the
**complete** published profile set — the portable, SPARQL and consistency
profiles plus all five version overlays — with pySHACL, the bundled ontology
plus every `ontology/versions/*.ttl`, and RDFS inference. That is the
configuration the vocabulary's own `tests/validate_shacl.py` uses.

Inputs: all ten example VCFs from the vocabulary repository (4.1 through 4.5,
including the breakend, gVCF, repeat and condensed-cohort fixtures), and a
hand-written stress file rendered for every version plus a 4.0 variant, in both
representation profiles.

Result: **zero violations** across 19 graphs. The only results are `sh:Warning`s
that VCF 4.5 recommends `Source` and `Version` on `##INFO` declarations — a
property of the input files, which the converter correctly does not fabricate.
