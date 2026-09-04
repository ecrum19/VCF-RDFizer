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

VCFR = "https://w3id.org/vcf-rdfizer/vocab#"
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
        apply=lambda t: drop_matching(t, subject=f"{FILE}#record/1", predicate=f"{VCFR}pos"),
        expected_detected_by="preflight_record_cardinality",
    ),
    Mutation(
        id="corrupt_pos",
        description="Move a record into a different 1 Mb window.",
        vcf_element="POS",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFR}pos",
            new_object='"9100100"^^<http://www.w3.org/2001/XMLSchema#integer>'),
        expected_detected_by="q01_record_density_1mb",
    ),
    Mutation(
        id="permute_pos",
        description="Swap POS between two records in the same contig and 1 Mb window.",
        vcf_element="record identity",
        apply=lambda t: swap_objects(
            t, subject_a=f"{FILE}#record/1", subject_b=f"{FILE}#record/2",
            predicate=f"{VCFR}pos"),
        expected_detected_by="q11_record_digest",
    ),
    Mutation(
        id="permute_ref_alt",
        description="Swap REF and ALT between two transition SNVs in the same window.",
        vcf_element="record identity",
        apply=lambda t: swap_objects(
            swap_objects(t, subject_a=f"{FILE}#record/1", subject_b=f"{FILE}#record/6",
                         predicate=f"{VCFR}ref"),
            subject_a=f"{FILE}#record/1", subject_b=f"{FILE}#record/6",
            predicate=f"{VCFR}alt"),
        expected_detected_by="q11_record_digest",
    ),
    Mutation(
        id="corrupt_chrom",
        description="Change one record's CHROM.",
        vcf_element="CHROM",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFR}chrom", new_object='"X"'),
        expected_detected_by="q01_record_density_1mb",
    ),
    Mutation(
        id="corrupt_alt",
        description="Change one record's ALT so its shape class changes.",
        vcf_element="ALT",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFR}alt", new_object='"GGG"'),
        expected_detected_by="q02_variant_shape_counts",
    ),
    Mutation(
        id="retype_pos_as_string",
        description="Emit POS as a plain string instead of an integer.",
        vcf_element="POS datatype",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFR}pos", new_object='"100"'),
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
            t, f'_:orphan <{VCFR}chrom> "20" .'),
        expected_detected_by="preflight_blank_nodes",
    ),
    Mutation(
        id="blank_node_object",
        description="Point a record at a blank node instead of its call resource.",
        vcf_element="graph integrity",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFR}hasCall",
            new_object="_:call1"),
        expected_detected_by="preflight_blank_nodes",
    ),
    Mutation(
        id="empty_literal",
        description="Emit an empty literal where a value was expected.",
        vcf_element="graph integrity",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/1", predicate=f"{VCFR}chrom", new_object='""'),
        expected_detected_by="preflight_empty_values",
    ),
    Mutation(
        id="whitespace_only_literal",
        description="Emit a literal containing only whitespace.",
        vcf_element="graph integrity",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/2", predicate=f"{VCFR}chrom", new_object='"   "'),
        expected_detected_by="preflight_empty_values",
    ),
    Mutation(
        id="duplicate_triple",
        description="Emit the same statement twice, as a duplicated RDF part would.",
        vcf_element="graph integrity",
        apply=lambda t: append_lines(
            t, f'<{FILE}#record/1> <{VCFR}chrom> "20" .'),
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
            t, f'<{FILE}#record/1> <{VCFR}notARealProperty> "x" .'),
        expected_detected_by="q09_predicate_census (extra row)",
    ),
    Mutation(
        id="drop_filter",
        description="Remove one FILTER triple.",
        vcf_element="FILTER",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#call/1", predicate=f"{VCFR}filter"),
        expected_detected_by="q04_filter_distribution",
    ),
    Mutation(
        id="corrupt_filter_lexical",
        description="Change a FILTER value while keeping its broad status class.",
        vcf_element="FILTER",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/3", predicate=f"{VCFR}filter", new_object='"q20"'),
        expected_detected_by="q04_filter_distribution",
    ),
    Mutation(
        id="plain_dot_literal",
        description="Emit a missing token as a plain '.' instead of '.'^^vcfr:Null.",
        vcf_element="missing-value policy",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#record/2", predicate=f"{VCFR}recordId", new_object='"."'),
        expected_detected_by="preflight_missing_token_conformance (--strict-conformance)",
    ),
    Mutation(
        id="flip_genotype",
        description="Change one sample's GT value.",
        vcf_element="FORMAT/GT",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#sample/1/HG001/fmt/GT", predicate=f"{VCFR}fieldValue",
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
            t, subject=f"{FILE}#sample/1/HG001/fmt/DP", predicate=f"{VCFR}fieldValue",
            new_object='"999"'),
        expected_detected_by="q13_format_value_digest",
        representations=("expanded",),
    ),
    Mutation(
        id="drop_qual",
        description="Remove a QUAL triple.",
        vcf_element="QUAL",
        apply=lambda t: drop_matching(t, subject=f"{FILE}#call/1", predicate=f"{VCFR}qual"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_all_qual",
        description="Emit no QUAL at all, as the mapping did before it was fixed.",
        vcf_element="QUAL",
        apply=lambda t: drop_matching(t, predicate=f"{VCFR}qual", limit=None),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_qual",
        description="Change a QUAL value.",
        vcf_element="QUAL",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/1", predicate=f"{VCFR}qual", new_object='"0"'),
        expected_detected_by="q11_record_digest",
    ),
    Mutation(
        id="drop_info_value",
        description="Remove a structured INFO value node.",
        vcf_element="INFO",
        apply=lambda t: drop_matching(t, predicate=f"{VCFR}hasInfoValue"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_info_value",
        description="Change a structured INFO value.",
        vcf_element="INFO",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/1/info/AC", predicate=f"{VCFR}fieldValue",
            new_object='"99"'),
        expected_detected_by="q12_info_value_digest",
    ),
    Mutation(
        id="drop_info_definition",
        description="Remove an INFO field declaration resource.",
        vcf_element="INFO declaration",
        apply=lambda t: drop_matching(
            t, subject=f"{FILE}#header/line/6", predicate=f"{VCFR}fieldType"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="retype_info_value",
        description="Drop the typed integer form of an INFO value.",
        vcf_element="INFO typing",
        apply=lambda t: drop_matching(t, predicate=f"{VCFR}fieldValueInteger"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_info_raw",
        description="Change a record's raw INFO string.",
        vcf_element="INFO",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/1", predicate=f"{VCFR}infoRaw", new_object='"AC=99"'),
        expected_detected_by="q11_record_digest",
    ),
    Mutation(
        id="corrupt_format_vector",
        description="Change one sample's value inside a condensed FORMAT vector.",
        vcf_element="FORMAT/DP",
        apply=lambda t: replace_object(
            t, subject=f"{FILE}#call/1/matrix/fmt/DP", predicate=f"{VCFR}encodedValues",
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
        apply=lambda t: drop_matching(t, predicate=f"{VCFR}contigLength"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_filter_definition",
        description="Remove a FILTER declaration's id.",
        vcf_element="FILTER declaration",
        apply=lambda t: drop_matching(t, predicate=f"{VCFR}filterId"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_alt_definition",
        description="Remove a symbolic ALT declaration's id.",
        vcf_element="ALT declaration",
        apply=lambda t: drop_matching(t, predicate=f"{VCFR}altId"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="drop_file_date",
        description="Remove the declared file date.",
        vcf_element="file metadata",
        apply=lambda t: drop_matching(t, subject=FILE, predicate=f"{VCFR}fileDate"),
        expected_detected_by="q09_predicate_census",
    ),
    Mutation(
        id="corrupt_contig_count",
        description="Declare the wrong number of contigs.",
        vcf_element="contig declaration",
        apply=lambda t: replace_object(
            t, subject=FILE, predicate=f"{VCFR}contigCount",
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
            t, subject=FILE, predicate=f"{VCFR}fileFormat", new_object='"VCFv9.9"'),
        expected_detected_by="q07_file_metadata (Phase 1c)",
    ),
    Mutation(
        id="drop_reference_genome",
        description="Remove the declared reference genome.",
        vcf_element="file metadata",
        apply=lambda t: drop_matching(t, subject=FILE, predicate=f"{VCFR}referenceGenome"),
        expected_detected_by="q07_file_metadata (Phase 1c)",
    ),
    Mutation(
        id="wrong_representation_profile",
        description="Declare the wrong sample representation profile.",
        vcf_element="representation profile",
        apply=lambda t: replace_object(
            t, subject=FILE, predicate=f"{VCFR}representationProfile",
            new_object=f"<{VCFR}ExpandedRepresentation>"),
        expected_detected_by="preflight_representation_profile",
        representations=("condensed",),
    ),
)


def for_representation(representation: str) -> tuple[Mutation, ...]:
    return tuple(m for m in MUTATIONS if representation in m.representations)
