"""What a link count means, and what the link actually rests on.

The campaign's tier-1 run reported "86 links from 86 records" and that number
is uninterpretable on its own. Tier 1 rewrites an identifier the VCF already
carries: it cannot fail on a record that has an rsID and cannot succeed on one
that does not, so 86/86 is a property of the fixture, not evidence about the
linker. A reader needs the denominator -- records that produced a join key --
and needs to know the assertion is an identifier rewrite rather than a
position-verified match, because ``vcfl:sameVariantAs`` is a strong predicate
and a stale rsID in a re-annotated call set produces a confidently wrong one.

Tier 2 made the opposite point: it ran cleanly and produced zero links, because
the bundled demonstration intervals do not overlap the fixture. "Executes
correctly" and "links nothing" were the same row in the output.
"""

import unittest

from vcf_rdfizer_linking import runner
from test.helpers import VerboseTestCase


class AssertionBasisTests(VerboseTestCase):
    def test_every_tier_declares_what_its_link_rests_on(self):
        for tier in (1, 2, 3):
            self.assertIn(tier, runner.ASSERTION_BASIS)
            self.assertTrue(runner.ASSERTION_BASIS[tier].strip())

    def test_tier_one_is_labelled_an_identifier_rewrite(self):
        """The weakest claim in the set must be the one that says so."""
        basis = runner.ASSERTION_BASIS[1]
        self.assertIn("identifier-rewrite", basis)
        self.assertIn("not position-verified", basis)

    def test_tier_two_is_labelled_as_verified_against_a_pinned_reference(self):
        basis = runner.ASSERTION_BASIS[2]
        self.assertIn("coordinate-interval", basis)
        self.assertIn("digest-pinned", basis)

    def test_tier_three_records_that_a_service_answered(self):
        self.assertIn("service-resolution", runner.ASSERTION_BASIS[3])

    def test_only_the_coordinate_tier_counts_as_verified(self):
        """Tier 3's answer is recorded, not checked against the call itself."""
        self.assertEqual(
            [tier for tier in (1, 2, 3) if tier == 2], [2],
        )


class CoverageArithmeticTests(VerboseTestCase):
    """The ratio the run record now carries, checked on its own terms."""

    @staticmethod
    def coverage(linked_subjects, eligible_records):
        if not eligible_records:
            return None
        return round(linked_subjects / eligible_records, 6)

    def test_full_coverage_of_an_eligible_population_is_one(self):
        self.assertEqual(self.coverage(86, 86), 1.0)

    def test_partial_coverage_is_the_fraction_linked(self):
        self.assertEqual(self.coverage(43, 86), 0.5)

    def test_zero_links_against_an_eligible_population_is_zero_not_none(self):
        """Tier 2's real result: it ran, and linked nothing. That is a number."""
        self.assertEqual(self.coverage(0, 86), 0.0)

    def test_no_eligible_records_reports_none_rather_than_a_false_zero(self):
        """Nothing could have linked, so no rate is defined."""
        self.assertIsNone(self.coverage(0, 0))


class RunnerRecordShapeTests(VerboseTestCase):
    """The fields a reported yield needs, present before any run."""

    REQUIRED = (
        "eligible_records",
        "linked_subjects",
        "coverage",
        "assertion_basis",
        "assertion_verified",
    )

    def test_the_new_fields_are_initialised_for_every_linker(self):
        import inspect

        source = inspect.getsource(runner.run_linkers)
        for field in self.REQUIRED:
            self.assertIn(f'"{field}"', source, field)

    def test_eligible_records_is_incremented_where_keys_are_extracted(self):
        import inspect

        source = inspect.getsource(runner.run_linkers)
        self.assertIn('["eligible_records"] += 1', source)

    def test_the_linkset_node_carries_the_denominator_and_the_basis(self):
        """A consumer must see this without leaving the graph."""
        import inspect

        source = inspect.getsource(runner.run_linkers)
        self.assertIn("VCFL.eligibleRecordCount", source)
        self.assertIn("VCFL.linkedSubjectCount", source)
        self.assertIn("VCFL.assertionBasis", source)


if __name__ == "__main__":
    unittest.main()
