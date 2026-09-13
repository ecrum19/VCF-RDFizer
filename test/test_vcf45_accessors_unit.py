"""Unit tests for the VCF 4.5 accessors added alongside the local-allele fix.

Each test states the specification rule it pins, because several of these are
places where a plausible-looking wrong answer is the failure mode: a depth bound
to the wrong allele, or a modification value bound to the wrong base, comes back
well-formed and with full provenance attached.
"""
import unittest

import vcf_rdfizer as R
import vcf_rdfizer_vocab as V
from test.helpers import VerboseTestCase


class LocalAlleleResolutionTests(VerboseTestCase):
    """Number=LA/LR index the sample's own allele subset, not the ALT column."""

    def test_LR_item_zero_is_the_reference(self):
        """A Number=LR list runs REF first, then the local ALT alleles."""
        link = V.value_item_link("LR", 0, tuple_arity=None, local_alleles=[2, 4])
        self.assertEqual(link.allele_index, 0)

    def test_LR_items_resolve_through_LAA_not_by_position(self):
        """With LAA=2,4 the list is REF, ALT2, ALT4 -- not REF, ALT1, ALT2."""
        got = [
            V.value_item_link("LR", i, tuple_arity=None, local_alleles=[2, 4]).allele_index
            for i in range(3)
        ]
        self.assertEqual(got, [0, 2, 4])

    def test_LA_items_skip_the_reference(self):
        """Number=LA is one value per local ALT allele, so item 0 is LAA[0]."""
        got = [
            V.value_item_link("LA", i, tuple_arity=None, local_alleles=[2, 4]).allele_index
            for i in range(2)
        ]
        self.assertEqual(got, [2, 4])

    def test_without_LAA_a_local_item_gets_no_allele(self):
        """A wrong allele link is worse than none, so absence wins."""
        link = V.value_item_link("LR", 1, tuple_arity=None, local_alleles=None)
        self.assertIsNone(link.allele_index)
        self.assertTrue(link.is_empty)

    def test_an_item_past_the_end_of_LAA_gets_no_allele(self):
        """A payload longer than the local set resolves no further."""
        link = V.value_item_link("LR", 5, tuple_arity=None, local_alleles=[2])
        self.assertIsNone(link.allele_index)

    def test_global_codes_are_untouched_by_a_local_allele_set(self):
        """Number=R still indexes the ALT column even when LAA is present."""
        link = V.value_item_link("R", 1, tuple_arity=None, local_alleles=[2, 4])
        self.assertEqual(link.allele_index, 1)


class BaseModificationKeyTests(VerboseTestCase):
    """VCF 4.5 reserves both a ChEBI-numeric spelling and a named alias."""

    def test_the_numeric_form_parses(self):
        parsed = V.parse_base_modification_key("M27551C")
        self.assertEqual((parsed.family, parsed.chebi_id, parsed.base), ("M", "27551", "C"))
        self.assertFalse(parsed.is_alias)

    def test_an_alias_resolves_to_the_same_residue(self):
        """M5mC is documented as an alias for M27551C."""
        alias = V.parse_base_modification_key("M5mC")
        numeric = V.parse_base_modification_key("M27551C")
        self.assertEqual(alias.residue_uri, numeric.residue_uri)
        self.assertTrue(alias.is_alias)

    def test_an_alias_keeps_its_written_spelling(self):
        """The IRI must trace back to the column as the file wrote it."""
        parsed = V.parse_base_modification_key("M5mC")
        self.assertEqual(parsed.key, "M5mC")
        self.assertEqual(parsed.canonical_key, "M27551C")

    def test_all_three_families_share_one_modification_identity(self):
        """M, DPM and ADM describe one modification, so they must name one."""
        ids = {
            V.parse_base_modification_key(k).modification_id
            for k in ("M5mC", "DPM5mC", "ADM5mC")
        }
        self.assertEqual(ids, {"27551C"})

    def test_each_family_carries_its_own_value_property(self):
        self.assertEqual(
            [V.parse_base_modification_key(k).value_property
             for k in ("M5mC", "DPM5mC", "ADM5mC")],
            ["modificationFraction", "modificationDepth", "modificationAlleleDepth"],
        )

    def test_a_key_that_is_neither_spelling_is_rejected(self):
        for key in ("NOTAKEY", "M5mCX", "GT", "M", ""):
            with self.subTest(key=key):
                self.assertIsNone(V.parse_base_modification_key(key))


class BaseModificationPositionTests(VerboseTestCase):
    """One value per base, either strand, that could carry the modification."""

    def test_the_specification_worked_example(self):
        """VCF 4.5: an allele of CGA has two M5mC values.

        The forward-strand C at the first base, and the reverse-strand C at the
        second, whose forward base is G.
        """
        self.assertEqual(
            V.base_modification_positions([(0, "CGA")], "C"),
            [(0, 0, "+"), (0, 1, "-")],
        )

    def test_bases_that_cannot_carry_it_yield_nothing(self):
        self.assertEqual(V.base_modification_positions([(0, "AAA")], "C"), [])

    def test_N_yields_both_strands_negative_after_positive(self):
        """The specification calls N out: every base could carry it."""
        self.assertEqual(
            V.base_modification_positions([(0, "AC")], "N"),
            [(0, 0, "+"), (0, 0, "-"), (0, 1, "+"), (0, 1, "-")],
        )

    def test_positions_run_across_alleles_in_genotype_order(self):
        """Values are encoded over the concatenated genotype allele bases."""
        self.assertEqual(
            V.base_modification_positions([(0, "C"), (2, "GC")], "C"),
            [(0, 0, "+"), (2, 0, "-"), (2, 1, "+")],
        )

    def test_an_empty_allele_list_yields_nothing(self):
        self.assertEqual(V.base_modification_positions([], "C"), [])


class GenotypeAlleleSequenceTests(VerboseTestCase):
    """Number=M is encoded over the called alleles, in GT order."""

    def test_alleles_come_back_in_genotype_order(self):
        got = R._gt_allele_sequences(("2/0",), ("GT",), "G", "A,C,T")
        self.assertEqual(got, [(2, "C"), (0, "G")])

    def test_symbolic_alleles_contribute_nothing(self):
        """The specification: symbolic alleles encode no modification values."""
        got = R._gt_allele_sequences(("0/3",), ("GT",), "G", "A,C,<*>")
        self.assertEqual(got, [(0, "G")])

    def test_no_call_positions_contribute_nothing(self):
        got = R._gt_allele_sequences(("./1",), ("GT",), "G", "A")
        self.assertEqual(got, [(1, "A")])

    def test_a_record_without_GT_yields_nothing(self):
        self.assertEqual(R._gt_allele_sequences(("12",), ("DP",), "G", "A"), [])


class PhaseIndicatorTests(VerboseTestCase):
    """Each allele call carries the / or | that precedes it."""

    def test_an_unphased_genotype_gives_every_call_a_slash(self):
        parsed = V.parse_genotype("0/1")
        self.assertEqual([c.phase_indicator for c in parsed.calls], ["/", "/"])

    def test_a_phased_genotype_gives_every_call_a_bar(self):
        parsed = V.parse_genotype("0|1")
        self.assertEqual([c.phase_indicator for c in parsed.calls], ["|", "|"])

    def test_mixed_indicators_are_kept_per_call(self):
        """A single Phased/Unphased verdict would misdescribe this genotype.

        The first call's indicator is omitted in the source; it takes "/" here
        because the genotype is not wholly phased.
        """
        parsed = V.parse_genotype("0|1/2")
        self.assertEqual([c.phase_indicator for c in parsed.calls], ["/", "|", "/"])

    def test_a_bare_haploid_call_is_not_called_phased(self):
        """There is no separator, so there is nothing to phase against.

        Defaulting to "|" here would contradict the genotype's own
        phasingStatus, which is Unphased for exactly that reason.
        """
        parsed = V.parse_genotype("0")
        self.assertEqual([c.phase_indicator for c in parsed.calls], ["/"])
        self.assertFalse(parsed.is_phased)

    def test_an_explicit_leading_indicator_is_kept(self):
        """VCF 4.4 added the leading indicator; it is not invented or dropped."""
        parsed = V.parse_genotype("|0/1")
        self.assertEqual(parsed.calls[0].phase_indicator, "|")


if __name__ == "__main__":
    unittest.main()


def _emit_to_list():
    """Collect emitted N-Triples lines, with the vocab prefix folded away."""
    out = []
    def emit(line):
        out.append(line.strip().replace("https://w3id.org/vcf-core/vocab#", "vcfc:"))
    return emit, out


def _objects(lines, predicate):
    return [l.split("> ", 2)[-1].rstrip(" .").strip('"<>')
            for l in lines if f"<vcfc:{predicate}>" in l]


class RecordIdentifierEmissionTests(VerboseTestCase):
    """The ID column is a semicolon-separated list, kept in source order."""

    def test_each_component_becomes_an_ordered_identifier(self):
        emit, out = _emit_to_list()
        stats = {"record_identifiers": 0}
        R._emit_record_identifiers(emit, record_uri="F#record/1", value="idA;idB", stats=stats)
        self.assertEqual(stats["record_identifiers"], 2)
        self.assertEqual(_objects(out, "identifierValue"), ["idA", "idB"])
        self.assertEqual([o.split("^^")[0].strip('"') for o in _objects(out, "componentIndex")],
                         ["1", "2"])

    def test_the_missing_token_is_a_status_not_an_identifier(self):
        emit, out = _emit_to_list()
        stats = {"record_identifiers": 0}
        R._emit_record_identifiers(emit, record_uri="F#record/1", value=".", stats=stats)
        self.assertEqual(out, [])
        self.assertEqual(stats["record_identifiers"], 0)


class FilterCodeEmissionTests(VerboseTestCase):
    """PASS and the missing token are FILTER statuses, not failure codes."""

    def _run(self, value, definitions=None):
        emit, out = _emit_to_list()
        stats = {"filter_codes": 0}
        R._emit_filter_codes(emit, subject_uri="F#call/1", value=value,
                             filter_definitions=definitions or {}, stats=stats)
        return out, stats

    def test_pass_is_a_status_with_no_codes(self):
        out, stats = self._run("PASS")
        self.assertIn("vcfc:FiltersPassed", " ".join(out))
        self.assertEqual(stats["filter_codes"], 0)

    def test_the_missing_token_means_filters_were_not_applied(self):
        out, stats = self._run(".")
        self.assertIn("vcfc:FiltersNotApplied", " ".join(out))
        self.assertEqual(stats["filter_codes"], 0)

    def test_failure_codes_are_ordered_and_typed(self):
        out, stats = self._run("q10;s50")
        self.assertIn("vcfc:FiltersFailed", " ".join(out))
        self.assertEqual(stats["filter_codes"], 2)
        self.assertEqual(_objects(out, "filterCodeValue"), ["q10", "s50"])

    def test_a_declared_code_cites_its_header_line(self):
        """So a consumer can read the description without re-reading the header."""
        out, _ = self._run("q10", {"q10": "F#header/line/5"})
        self.assertEqual(_objects(out, "declaredByFilter"), ["F#header/line/5"])

    def test_an_undeclared_code_is_still_emitted_without_a_link(self):
        out, stats = self._run("q10", {})
        self.assertEqual(stats["filter_codes"], 1)
        self.assertEqual(_objects(out, "declaredByFilter"), [])


class BaseModificationEmissionTests(VerboseTestCase):
    """One carrier per modification; the families contribute their values to it."""

    def test_the_three_families_share_one_carrier(self):
        emit, out = _emit_to_list()
        stats = {"base_modifications": 0}
        seen = set()
        uris = set()
        for key, value in (("M5mC", "0.9"), ("DPM5mC", "20"), ("ADM5mC", "18")):
            uris.add(R._emit_base_modification(
                emit, sample_uri="F#sample/1/S1",
                modification=V.parse_base_modification_key(key),
                value=value, stats=stats, emitted_modifications=seen))
        self.assertEqual(len(uris), 1, "M, DPM and ADM must name one modification")
        self.assertEqual(stats["base_modifications"], 1)
        joined = " ".join(out)
        for prop in ("modificationFraction", "modificationDepth", "modificationAlleleDepth"):
            self.assertIn(f"vcfc:{prop}", joined)

    def test_a_missing_value_emits_no_carrier(self):
        emit, out = _emit_to_list()
        stats = {"base_modifications": 0}
        uri = R._emit_base_modification(
            emit, sample_uri="F#sample/1/S1",
            modification=V.parse_base_modification_key("M5mC"),
            value=".", stats=stats, emitted_modifications=set())
        self.assertIsNone(uri)
        self.assertEqual(out, [])


class ModificationItemTests(VerboseTestCase):
    """A Number=M payload decomposes over the bases that can carry it."""

    def _run(self, value, sequences, base="C"):
        emit, out = _emit_to_list()
        stats = {"value_items": 0, "modification_arity_mismatches": 0,
                 "modification_reverse_strand": 0}
        R._emit_modification_items(
            emit, parent_uri="F#fmt/M5mC", modification_uri="F#basemod/27551C",
            modification=V.parse_base_modification_key("M5mC" if base == "C" else "MXaoN"),
            value=value, record_uri="F#record/1", allele_sequences=sequences, stats=stats)
        return out, stats

    def test_each_item_names_its_allele_offset_and_modification(self):
        out, stats = self._run("0.9", [(0, "C")])
        self.assertEqual(stats["value_items"], 1)
        self.assertEqual(_objects(out, "forAllele"), ["F#record/1/allele/0"])
        self.assertEqual(_objects(out, "forBaseModification"), ["F#basemod/27551C"])
        self.assertEqual([o.split("^^")[0].strip('"') for o in _objects(out, "modifiedBaseOffset")],
                         ["0"])

    def test_a_reverse_strand_base_is_counted(self):
        """CGA: the G at offset 1 carries the modification on the reverse strand."""
        out, stats = self._run("0.1,0.2", [(0, "CGA")])
        self.assertEqual(stats["value_items"], 2)
        self.assertEqual(stats["modification_reverse_strand"], 1)

    def test_a_payload_that_disagrees_with_the_sequence_emits_nothing(self):
        """A value on the wrong base is worse than a whole list on fieldValue."""
        out, stats = self._run("0.1,0.2,0.3", [(0, "C")])
        self.assertEqual(out, [])
        self.assertEqual(stats["value_items"], 0)
        self.assertEqual(stats["modification_arity_mismatches"], 1)

    def test_a_missing_payload_emits_nothing(self):
        out, stats = self._run(".", [(0, "C")])
        self.assertEqual(out, [])
        self.assertEqual(stats["modification_arity_mismatches"], 0)


class PhasingStatusEmissionTests(VerboseTestCase):
    """A genotype whose indicators disagree is neither Phased nor Unphased."""

    def _status(self, gt):
        emit, out = _emit_to_list()
        stats = {"genotypes": 0, "genotype_calls": 0}
        R._emit_genotype(emit, sample_uri="F#sample/1/S1", record_uri="F#record/1",
                         value=gt, alt_count=3, stats=stats)
        return [o for o in _objects(out, "phasingStatus")]

    def test_mixed_indicators_emit_MixedPhasing(self):
        """vcfc:MixedPhasing exists for exactly this; the per-call indicators
        carry the precise semantics."""
        self.assertEqual(self._status("0|1/2"), ["vcfc:MixedPhasing"])

    def test_a_wholly_phased_genotype_is_Phased(self):
        self.assertEqual(self._status("0|1"), ["vcfc:Phased"])

    def test_a_wholly_unphased_genotype_is_Unphased(self):
        self.assertEqual(self._status("0/1"), ["vcfc:Unphased"])

    def test_a_malformed_genotype_produces_no_resource(self):
        """The value survives on fieldValue; inventing a Genotype would fail
        vcfc:GenotypeShape."""
        self.assertEqual(self._status("not-a-genotype"), [])


class MalformedGenotypeSequenceTests(VerboseTestCase):
    def test_a_genotype_outside_the_lexical_space_yields_no_sequences(self):
        """Number=M cannot be decomposed without a parsable GT."""
        self.assertEqual(R._gt_allele_sequences(("x/y",), ("GT",), "G", "A"), [])
