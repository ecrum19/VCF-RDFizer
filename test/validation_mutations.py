"""A catalogue of graph mutations, and what the validator should make of each.

Every entry names a specific way a conversion could be wrong, and declares
whether the validation suite catches it. Entries with ``known_undetected`` set
are recorded gaps: the harness asserts they are still *not* detected, so
closing a gap makes a test fail and forces both the catalogue and the coverage
documentation to be updated. That is what turns "coverage" into a number
instead of an opinion.

Mutations operate on N-Triples text so they are engine-independent and can be
replayed under rdflib on the host or Comunica/QLever in the container.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

VCFC = "https://w3id.org/vcf-core/vocab#"
FILE = "file://fixture.vcf"


# ---------------------------------------------------------------------------
# N-Triples editing helpers
# ---------------------------------------------------------------------------
def _lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.strip()]


def _join(lines: list[str]) -> str:
    return "\n".join(lines) + "\n"


def drop_matching(text: str, *, subject: str | None = None, predicate: str | None = None,
                  limit: int | None = 1) -> str:
    """Remove triples matching a subject and/or predicate."""
    out, removed = [], 0
    for line in _lines(text):
        matches = (
            (subject is None or line.startswith(f"<{subject}> "))
            and (predicate is None or f"<{predicate}>" in line)
        )
        if matches and (limit is None or removed < limit):
            removed += 1
            continue
        out.append(line)
    if removed == 0:
        raise AssertionError(f"mutation matched nothing (subject={subject}, predicate={predicate})")
    return _join(out)


def replace_object(text: str, *, subject: str, predicate: str, new_object: str) -> str:
    """Rewrite the object of the first triple matching subject+predicate."""
    out, done = [], False
    for line in _lines(text):
        if not done and line.startswith(f"<{subject}> ") and f"<{predicate}>" in line:
            out.append(f"<{subject}> <{predicate}> {new_object} .")
            done = True
            continue
        out.append(line)
    if not done:
        raise AssertionError(f"mutation matched nothing ({subject} {predicate})")
    return _join(out)


def swap_objects(text: str, *, subject_a: str, subject_b: str, predicate: str) -> str:
    """Exchange the objects of the same predicate between two subjects."""
    objects: dict[str, str] = {}
    for line in _lines(text):
        for subject in (subject_a, subject_b):
            if line.startswith(f"<{subject}> ") and f"<{predicate}>" in line:
                objects[subject] = line.split(f"<{predicate}>", 1)[1].rsplit(" .", 1)[0].strip()
    if len(objects) != 2:
        raise AssertionError(f"swap needs both subjects to have {predicate}")
    text = replace_object(text, subject=subject_a, predicate=predicate,
                          new_object=objects[subject_b])
    return replace_object(text, subject=subject_b, predicate=predicate,
                          new_object=objects[subject_a])


def drop_line_containing(text: str, needle: str) -> str:
    """Remove every line containing ``needle`` (an object IRI, typically)."""
    return _join([line for line in _lines(text) if needle not in line])


def append_lines(text: str, *new: str) -> str:
    return _join(_lines(text) + list(new))


def duplicate_subject(text: str, *, subject: str, new_subject: str) -> str:
    """Copy every triple of a subject under a new IRI (a spurious extra record)."""
    copies = [
        line.replace(f"<{subject}>", f"<{new_subject}>", 1)
        for line in _lines(text)
        if line.startswith(f"<{subject}> ")
    ]
    if not copies:
        raise AssertionError(f"nothing to duplicate for {subject}")
    return append_lines(text, *copies)


# ---------------------------------------------------------------------------
# Catalogue
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Mutation:
    """One way a converted graph can be wrong."""

    id: str
    description: str
    #: Which part of the VCF this corrupts, for the coverage matrix.
    vcf_element: str
    apply: Callable[[str], str]
    #: The check expected to catch it, for documentation.
    expected_detected_by: str
    #: Set when the suite provably cannot catch this yet, with the reason.
    known_undetected: str | None = None
    #: Representations this mutation is meaningful for.
    representations: tuple[str, ...] = ("expanded", "condensed")
    #: Fixture options the mutation needs present in the graph. A mutation that
    #: targets a triple the shipped mapping does not yet emit still measures
    #: something real - "if we emitted this, would we notice it breaking?" - so
    #: the fixture is asked to include it rather than the mutation being skipped.
    graph_options: tuple[tuple[str, bool], ...] = ()
    #: Census policy to validate under. A graph carrying triples the census does
    #: not model yet would otherwise fail for that reason alone, masking whether
    #: the mutation itself is detectable.
    mapping_policy: str = "strict"


MUTATIONS: tuple[Mutation, ...] = (
    Mutation(
        id="drop_record",
        description="Remove every triple of one VCFRecord.",
        vcf_element="record",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#record/1", limit=None),
        expected_detected_by="q01/q02 record totals",
    ),
    Mutation(
        id="drop_pos",
        description="Remove one record's POS triple.",
        vcf_element="POS",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#record/1", predicate=f"{VCFC}pos"),
        expected_detected_by="preflight_record_cardinality",
    ),
    Mutation(
        id="corrupt_pos",
        description="Move a record into a different 1 Mb window.",
        vcf_element="POS",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFC}pos",
            new_object='"9100100"^^<http://www.w3.org/2001/XMLSchema#integer>'),
        expected_detected_by="q01_record_density_1mb",
    ),
    Mutation(
        id="permute_pos",
        description="Swap POS between two records in the same contig and 1 Mb window.",
        vcf_element="record identity",
        apply=lambda t: swap_objects(
            t, subject_a=f"{FILE}#record/1", subject_b=f"{FILE}#record/2",
            predicate=f"{VCFC}pos"),
        expected_detected_by="q11_record_digest",
    ),
    Mutation(
        id="permute_ref_alt",
        description="Swap REF and ALT between two transition SNVs in the same window.",
        vcf_element="record identity",
        apply=lambda t: swap_objects(
            swap_objects(t, subject_a=f"{FILE}#record/1", subject_b=f"{FILE}#record/6",
                         predicate=f"{VCFC}ref"),
            subject_a=f"{FILE}#record/1", subject_b=f"{FILE}#record/6",
            predicate=f"{VCFC}alt"),
        expected_detected_by="q11_record_digest",
    ),
    Mutation(
        id="corrupt_chrom",
        description="Change one record's CHROM.",
        vcf_element="CHROM",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFC}chrom", new_object='"X"'),
        expected_detected_by="q01_record_density_1mb",
    ),
    Mutation(
        id="corrupt_alt",
        description="Change one record's ALT so its shape class changes.",
        vcf_element="ALT",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFC}alt", new_object='"GGG"'),
        expected_detected_by="q02_variant_shape_counts",
    ),
    Mutation(
        id="retype_pos_as_string",
        description="Emit POS as a plain string instead of an integer.",
        vcf_element="POS datatype",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFC}pos", new_object='"100"'),
        expected_detected_by="preflight_position_datatype",
    ),
    Mutation(
        id="duplicate_record",
        description="Emit a second copy of a record under a new IRI.",
        vcf_element="record",
        apply=lambda t: duplicate_subject(
            t, subject=f"{FILE}#record/1", new_subject=f"{FILE}#record/1-copy"),
        expected_detected_by="q01/q02 record totals",
    ),
    Mutation(
        id="introduce_blank_node",
        description="Emit a record as a blank node instead of an IRI.",
        vcf_element="graph integrity",
        apply=lambda t: append_lines(
            t, f'_:orphan <{VCFC}chrom> "20" .'),
        expected_detected_by="preflight_blank_nodes",
    ),
    Mutation(
        id="blank_node_object",
        description="Point a record at a blank node instead of its call resource.",
        vcf_element="graph integrity",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFC}hasCall",
            new_object="_:call1"),
        expected_detected_by="preflight_blank_nodes",
    ),
    Mutation(
        id="empty_literal",
        description="Emit an empty literal where a value was expected.",
        vcf_element="graph integrity",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFC}chrom", new_object='""'),
        expected_detected_by="preflight_empty_values",
    ),
    Mutation(
        id="whitespace_only_literal",
        description="Emit a literal containing only whitespace.",
        vcf_element="graph integrity",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/2", predicate=f"{VCFC}chrom", new_object='"   "'),
        expected_detected_by="preflight_empty_values",
    ),
    Mutation(
        id="duplicate_triple",
        description="Emit the same statement twice, as a duplicated RDF part would.",
        vcf_element="graph integrity",
        apply=lambda t: append_lines(
            t, f'<{FILE}#record/1> <{VCFC}chrom> "20" .'),
        expected_detected_by="preflight_duplicate_triples",
    ),
    Mutation(
        id="duplicate_whole_graph",
        description="Concatenate the graph with itself, as a duplicated part file would.",
        vcf_element="graph integrity",
        apply=lambda t: t + t,
        expected_detected_by="preflight_duplicate_triples",
    ),
    Mutation(
        id="spurious_predicate",
        description="Add a triple using a predicate the vocabulary does not define.",
        vcf_element="graph completeness",
        apply=lambda t: append_lines(
            t, f'<{FILE}#record/1> <{VCFC}notARealProperty> "x" .'),
        expected_detected_by="q09_predicate_census (extra row)",
    ),
    Mutation(
        id="drop_filter",
        description="Remove one FILTER triple.",
        vcf_element="FILTER",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#call/1", predicate=f"{VCFC}filter"),
        expected_detected_by="q04_filter_distribution",
    ),
    Mutation(
        id="corrupt_filter_lexical",
        description="Change a FILTER value while keeping its broad status class.",
        vcf_element="FILTER",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/3", predicate=f"{VCFC}filter", new_object='"q20"'),
        expected_detected_by="q04_filter_distribution",
    ),
    Mutation(
        id="plain_dot_literal",
        description="Emit a missing token as a plain '.' instead of '.'^^vcfc:Null.",
        vcf_element="missing-value policy",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/2", predicate=f"{VCFC}recordId", new_object='"."'),
        expected_detected_by="preflight_missing_token_conformance (--strict-conformance)",
    ),
    Mutation(
        id="flip_genotype",
        description="Change one sample's GT value.",
        vcf_element="FORMAT/GT",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#sample/1/HG001/fmt/GT", predicate=f"{VCFC}fieldValue",
            new_object='"1/1"'),
        expected_detected_by="q05_sample_genotype_counts",
        representations=("expanded",),
    ),
    Mutation(
        id="drop_sample_call",
        description="Remove one SampleCall entirely.",
        vcf_element="sample call",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#sample/1/HG001", limit=None),
        expected_detected_by="q05 per-sample totals",
        representations=("expanded",),
    ),
    Mutation(
        id="drop_format_value_dp",
        description="Remove a non-GT FORMAT value node (DP).",
        vcf_element="FORMAT/DP",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#sample/1/HG001/fmt/DP", limit=None),
        expected_detected_by="q09_predicate_census",
        representations=("expanded",),
    ),
    Mutation(
        id="corrupt_format_value_dp",
        description="Change a DP value.",
        vcf_element="FORMAT/DP",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#sample/1/HG001/fmt/DP", predicate=f"{VCFC}fieldValue",
            new_object='"999"'),
        expected_detected_by="q13_format_value_digest",
        representations=("expanded",),
    ),
    Mutation(
        id="drop_qual",
        description="Remove a QUAL triple.",
        vcf_element="QUAL",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#call/1", predicate=f"{VCFC}qual"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_all_qual",
        description="Emit no QUAL at all, as the mapping did before it was fixed.",
        vcf_element="QUAL",
        apply=lambda t: drop_matching(t, predicate=f"{VCFC}qual", limit=None),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_qual",
        description="Change a QUAL value.",
        vcf_element="QUAL",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/1", predicate=f"{VCFC}qual", new_object='"0"'),
        expected_detected_by="q11_record_digest",
    ),
    Mutation(
        id="drop_info_value",
        description="Remove a structured INFO value node.",
        vcf_element="INFO",
        apply=lambda t: drop_matching(t, predicate=f"{VCFC}hasInfoValue"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_info_value",
        description="Change a structured INFO value.",
        vcf_element="INFO",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/1/info/AC", predicate=f"{VCFC}fieldValue",
            new_object='"99"'),
        expected_detected_by="q12_info_value_digest",
    ),
    Mutation(
        id="drop_info_definition",
        description="Remove an INFO field declaration resource.",
        vcf_element="INFO declaration",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#header/line/6", predicate=f"{VCFC}fieldType"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="retype_info_value",
        description="Drop the typed integer form of an INFO value.",
        vcf_element="INFO typing",
        apply=lambda t: drop_matching(t, predicate=f"{VCFC}fieldValueInteger"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_info_raw",
        description="Change a record's raw INFO string.",
        vcf_element="INFO",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/1", predicate=f"{VCFC}infoRaw", new_object='"AC=99"'),
        expected_detected_by="q11_record_digest",
    ),
    Mutation(
        id="corrupt_format_vector",
        description="Change one sample's value inside a condensed FORMAT vector.",
        vcf_element="FORMAT/DP",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/1/matrix/fmt/DP", predicate=f"{VCFC}encodedValues",
            new_object='"999\t28"'),
        expected_detected_by="q13_format_value_digest",
        representations=("condensed",),
    ),
    Mutation(
        id="drop_header_line",
        description="Remove one HeaderLine resource.",
        vcf_element="header line",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#header/line/5", limit=None),
        expected_detected_by="q08_header_line_census (Phase 1c)",
    ),
    Mutation(
        id="untype_header_line",
        description="Strip a header line's vocabulary subclass.",
        vcf_element="header line typing",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#header/line/10",
            predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        expected_detected_by="q10_class_census",
    ),
    Mutation(
        id="drop_contig_attribute",
        description="Remove a contig's declared length.",
        vcf_element="contig declaration",
        apply=lambda t: drop_matching(t, predicate=f"{VCFC}contigLength"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_filter_definition",
        description="Remove a FILTER declaration's id.",
        vcf_element="FILTER declaration",
        apply=lambda t: drop_matching(t, predicate=f"{VCFC}filterId"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_alt_definition",
        description="Remove a symbolic ALT declaration's id.",
        vcf_element="ALT declaration",
        apply=lambda t: drop_matching(t, predicate=f"{VCFC}altId"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_file_date",
        description="Remove the declared file date.",
        vcf_element="file metadata",
        apply=lambda t: drop_matching(t, subject=FILE, predicate=f"{VCFC}fileDate"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_contig_count",
        description="Declare the wrong number of contigs.",
        vcf_element="contig declaration",
        apply=lambda t: replace_object(
            t, subject=FILE, predicate=f"{VCFC}contigCount",
            new_object='"99"^^<http://www.w3.org/2001/XMLSchema#integer>'),
        expected_detected_by="header census",
        known_undetected=(
            "contigCount is a derived scalar: the census counts that one triple "
            "exists but never reads its value."
        ),
    ),
    Mutation(
        id="corrupt_file_metadata",
        description="Change the declared fileformat.",
        vcf_element="file metadata",
        apply=lambda t: replace_object(
            t, subject=FILE, predicate=f"{VCFC}fileFormat", new_object='"VCFv9.9"'),
        expected_detected_by="q07_file_metadata (Phase 1c)",
    ),
    Mutation(
        id="drop_reference_genome",
        description="Remove the declared reference genome.",
        vcf_element="file metadata",
        apply=lambda t: drop_matching(t, subject=FILE, predicate=f"{VCFC}referenceGenome"),
        expected_detected_by="q07_file_metadata (Phase 1c)",
    ),
    # ------------------------------------------------------------------
    # The layers the VCF Core vocabulary added. Each names one way the new
    # emitters could regress; whether the suite catches it is measured, not
    # asserted, so a `known_undetected` entry here is a recorded gap.
    # ------------------------------------------------------------------
    Mutation(
        id="drop_version_class",
        description="Remove the vcfc:VCF4xFile class the version sentinel resolves to.",
        vcf_element="VCF version",
        apply=lambda t: drop_line_containing(t, f"<{VCFC}VCF42File>"),
        expected_detected_by="q10_class_census",
    ),
    Mutation(
        id="wrong_version_class",
        description="Type the file with a VCF version it does not declare.",
        vcf_element="VCF version",
        apply=lambda t: t.replace(f"<{VCFC}VCF42File>", f"<{VCFC}VCF45File>"),
        expected_detected_by="q10_class_census",
    ),
    Mutation(
        id="drop_line_index",
        description="Remove one header line's ordering index.",
        vcf_element="header ordering",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#header/line/1", predicate=f"{VCFC}lineIndex"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_record_index",
        description="Renumber one record, breaking the file's record order.",
        vcf_element="record ordering",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/2", predicate=f"{VCFC}recordIndex",
            new_object='"99"^^<http://www.w3.org/2001/XMLSchema#integer>'),
        expected_detected_by="nothing yet",
        known_undetected=(
            "recordIndex is counted by q09 but its values are not compared. "
            "The SPARQL SHACL profile checks uniqueness and ordering; the "
            "aggregate queries do not."
        ),
    ),
    Mutation(
        id="drop_header_attribute",
        description="Remove one vcfc:HeaderAttribute from a structured header line.",
        vcf_element="header attributes",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#header/line/6/attribute/1", limit=None),
        expected_detected_by="q09_predicate_census, q10_class_census",
    ),
    Mutation(
        id="corrupt_attribute_value",
        description="Change one header attribute's value.",
        vcf_element="header attributes",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#header/line/6/attribute/1",
            predicate=f"{VCFC}attributeValue", new_object='"WRONG"'),
        expected_detected_by="nothing yet",
        known_undetected=(
            "Attribute values are counted but not compared. Closing this needs "
            "a digest over the structured header attributes, the way q11 covers "
            "record fields."
        ),
    ),
    Mutation(
        id="drop_alt_allele",
        description="Remove one ALT allele resource from a multi-allelic record.",
        vcf_element="allele layer",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#record/4/allele/2", limit=None),
        expected_detected_by="q09_predicate_census, q10_class_census",
    ),
    Mutation(
        id="corrupt_allele_value",
        description="Change one parsed ALT allele's lexical value.",
        vcf_element="allele layer",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1/allele/1", predicate=f"{VCFC}alleleValue",
            new_object='"TTT"'),
        expected_detected_by="nothing yet",
        known_undetected=(
            "Allele values are counted but not compared against the raw ALT "
            "column. A digest joining vcfc:alleleValue to vcfc:alt would close "
            "it; the vocabulary's own consistency SHACL profile already checks "
            "this agreement."
        ),
    ),
    Mutation(
        id="corrupt_allele_kind",
        description="Misclassify an SNV allele as a symbolic one.",
        vcf_element="allele layer",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1/allele/1", predicate=f"{VCFC}alleleKind",
            new_object=f"<{VCFC}SymbolicAllele>"),
        expected_detected_by="nothing yet",
        known_undetected=(
            "alleleKind is counted but its value is not compared. q02 already "
            "classifies variant shape from REF/ALT, so cross-checking the two "
            "would close this without a new oracle."
        ),
    ),
    Mutation(
        id="drop_contig_link",
        description="Unlink one record from the contig its CHROM names.",
        vcf_element="allele layer",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFC}chromosome"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_value_item",
        description="Remove one parsed item of a Number=A INFO value.",
        vcf_element="indexed values",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#call/4/info/AC/value/1", limit=None),
        expected_detected_by="q09_predicate_census, q10_class_census",
    ),
    Mutation(
        id="corrupt_value_item_allele",
        description="Point a Number=A value item at the wrong ALT allele.",
        vcf_element="indexed values",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/4/info/AC/value/0", predicate=f"{VCFC}forAllele",
            new_object=f"<{FILE}#record/4/allele/2>"),
        expected_detected_by="nothing yet",
        known_undetected=(
            "forAllele is counted but the join is not checked. The vocabulary's "
            "consistency profile checks item/raw agreement; an equivalent "
            "aggregate here would need a per-item digest."
        ),
    ),
    Mutation(
        id="drop_sample_set",
        description="Remove the file's reusable sample set.",
        vcf_element="sample identity",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#samples", limit=None),
        expected_detected_by="q09_predicate_census, q10_class_census",
    ),
    # The same corruption is detected in one profile and not the other, which
    # is worth recording rather than smoothing over: in the condensed profile
    # the ordinal is what associates a vector position with a sample, so
    # breaking it moves genotypes between samples and the aggregates shift. In
    # the expanded profile each SampleCall carries its own vcfc:sampleId, so the
    # set ordinal is decorative and nothing downstream reads it.
    Mutation(
        id="corrupt_sample_index",
        description="Give two samples the same ordinal in the sample set.",
        vcf_element="sample identity",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#samples/HG002", predicate=f"{VCFC}sampleIndex",
            new_object='"1"^^<http://www.w3.org/2001/XMLSchema#integer>'),
        expected_detected_by="q05_sample_genotype_counts, q06_ac_an_distribution",
        representations=("condensed",),
    ),
    Mutation(
        id="corrupt_sample_index_expanded",
        description="The same ordinal corruption, where nothing decodes by position.",
        vcf_element="sample identity",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#samples/HG002", predicate=f"{VCFC}sampleIndex",
            new_object='"1"^^<http://www.w3.org/2001/XMLSchema#integer>'),
        expected_detected_by="nothing yet",
        known_undetected=(
            "In the expanded profile the sample set is a convenience: each "
            "SampleCall carries vcfc:sampleId and vcfc:forSample, so no query "
            "reads the ordinal. The SPARQL SHACL profile rejects duplicate "
            "sampleIndex values within a file, so the shape layer covers it."
        ),
        representations=("expanded",),
    ),
    Mutation(
        id="drop_genotype",
        description="Remove one parsed genotype resource.",
        vcf_element="genotype layer",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#sample/1/HG001/genotype", limit=None),
        expected_detected_by="q09_predicate_census, q10_class_census",
        representations=("expanded",),
    ),
    Mutation(
        id="flip_phasing_status",
        description="Report a phased genotype as unphased.",
        vcf_element="genotype layer",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#sample/1/HG001/genotype",
            predicate=f"{VCFC}phasingStatus", new_object=f"<{VCFC}Unphased>"),
        expected_detected_by="nothing yet",
        known_undetected=(
            "Phasing is counted but not compared. q05 classifies genotypes from "
            "the raw GT string and normalizes '|' to '/', so it cannot see the "
            "difference; cross-checking vcfc:phasingStatus against the raw "
            "genotypeString would close it."
        ),
        representations=("expanded",),
    ),
    Mutation(
        id="corrupt_called_allele",
        description="Point a genotype allele call at the wrong allele.",
        vcf_element="genotype layer",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#sample/1/HG001/genotype/call/0",
            predicate=f"{VCFC}calledAllele",
            new_object=f"<{FILE}#record/1/allele/1>"),
        expected_detected_by="nothing yet",
        known_undetected=(
            "calledAllele is counted but the join is not checked. q05 reads the "
            "raw GT, so a parsed call pointing at the wrong allele is invisible "
            "to it."
        ),
        representations=("expanded",),
    ),
    Mutation(
        id="duplicate_field_definition",
        description="Emit a declared field definition's ID twice.",
        vcf_element="field declarations",
        apply=lambda t: append_lines(
            t, f'<{FILE}#header/line/6> <{VCFC}fieldId> "AC" .'),
        expected_detected_by="preflight_duplicate_triples",
    ),
    Mutation(
        id="wrong_declaration_owner",
        description="Move a declared definition's ID onto the wrong header line.",
        vcf_element="field declarations",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#header/line/6", predicate=f"{VCFC}fieldId",
            new_object='"DB"'),
        expected_detected_by="nothing yet",
        known_undetected=(
            "Field IDs are counted but not tied to their header line. q08 "
            "counts lines per '##' key and q09 counts fieldId triples, so "
            "swapping which line carries which ID changes neither."
        ),
    ),
    Mutation(
        id="wrong_representation_profile",
        description="Declare the wrong sample representation profile.",
        vcf_element="representation profile",
        apply=lambda t: replace_object(
            t, subject=FILE, predicate=f"{VCFC}representationProfile",
            new_object=f"<{VCFC}ExpandedRepresentation>"),
        expected_detected_by="preflight_representation_profile",
        representations=("condensed",),
    ),
)


def for_representation(representation: str) -> tuple[Mutation, ...]:
    return tuple(m for m in MUTATIONS if representation in m.representations)
