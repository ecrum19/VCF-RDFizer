"""The one combination that does not terminate, refused rather than started.

Reproduced twice and archived under benchmarks_outputs__stalled/. Native
pycottas answered the fourteen preflight queries and Q1-Q4 against the
condensed encoding of a 10,000-record fixture, then failed to complete
``q05_sample_genotype_counts``: 41 hours in the first attempt, about 10 in the
second, both at roughly 190% CPU. QLever answered all thirteen questions on
that same cell, and the sibling expanded encoding finished every engine
including pycottas in 185-194 minutes.

So the defect is narrow -- native pycottas x condensed encoding x a
genotype-level query -- and until it is diagnosed the honest thing is to refuse
the combination. 51 hours of unattended runtime were spent finding out the hard
way, and the per-query timeout could not bound it at the time.
"""

import unittest

import vcf_rdfizer
from test.helpers import VerboseTestCase


class RejectionTests(VerboseTestCase):
    def test_naming_cottas_against_condensed_is_refused(self):
        self.assertIsNotNone(
            vcf_rdfizer.cottas_condensed_engine_rejection(
                engines=["cottas"], sample_representation="condensed", allow=False
            )
        )

    def test_the_refusal_names_both_ways_out(self):
        """A refusal that does not say what works instead is just a failure."""
        message = vcf_rdfizer.cottas_condensed_engine_rejection(
            engines=["cottas"], sample_representation="condensed", allow=False
        )
        self.assertIn("qlever", message)
        self.assertIn("expanded", message)
        self.assertIn("--allow-cottas-condensed", message)

    def test_the_refusal_cites_the_measurement_rather_than_asserting(self):
        message = vcf_rdfizer.cottas_condensed_engine_rejection(
            engines=["cottas"], sample_representation="condensed", allow=False
        )
        self.assertIn("41 h", message)
        self.assertIn("18 of 27", message)

    def test_cottas_against_expanded_is_allowed(self):
        """Expanded completed every engine in 185-194 min; nothing to refuse."""
        self.assertIsNone(
            vcf_rdfizer.cottas_condensed_engine_rejection(
                engines=["cottas"], sample_representation="expanded", allow=False
            )
        )

    def test_other_engines_against_condensed_are_allowed(self):
        for engine in ("qlever", "comunica", "hdt"):
            self.assertIsNone(
                vcf_rdfizer.cottas_condensed_engine_rejection(
                    engines=[engine], sample_representation="condensed", allow=False
                ),
                engine,
            )

    def test_the_override_lets_it_through(self):
        self.assertIsNone(
            vcf_rdfizer.cottas_condensed_engine_rejection(
                engines=["cottas"], sample_representation="condensed", allow=True
            )
        )


class ResolutionTests(VerboseTestCase):
    """Naming cottas is a request; asking for 'all' is a request for breadth."""

    def test_an_explicit_request_is_refused_not_silently_dropped(self):
        engines, warning, refusal = vcf_rdfizer.cottas_condensed_resolution(
            engines=["qlever", "cottas"],
            sample_representation="condensed",
            requested_all=False,
            allow=False,
        )
        self.assertIsNotNone(refusal)
        self.assertIsNone(warning)
        self.assertEqual(engines, ["qlever", "cottas"])

    def test_engine_all_drops_cottas_and_runs_the_rest(self):
        """06_equivalence used 'all' and hung twice; this completes instead."""
        engines, warning, refusal = vcf_rdfizer.cottas_condensed_resolution(
            engines=list(vcf_rdfizer.VALIDATION_ENGINE_CHOICES),
            sample_representation="condensed",
            requested_all=True,
            allow=False,
        )
        self.assertIsNone(refusal)
        self.assertIsNotNone(warning)
        self.assertNotIn("cottas", engines)
        self.assertEqual(engines, ["comunica", "qlever", "hdt"])

    def test_the_drop_warning_says_what_still_ran_and_how_to_override(self):
        _engines, warning, _refusal = vcf_rdfizer.cottas_condensed_resolution(
            engines=list(vcf_rdfizer.VALIDATION_ENGINE_CHOICES),
            sample_representation="condensed",
            requested_all=True,
            allow=False,
        )
        self.assertIn("comunica", warning)
        self.assertIn("qlever", warning)
        self.assertIn("--allow-cottas-condensed", warning)

    def test_engine_all_against_expanded_keeps_every_engine(self):
        engines, warning, refusal = vcf_rdfizer.cottas_condensed_resolution(
            engines=list(vcf_rdfizer.VALIDATION_ENGINE_CHOICES),
            sample_representation="expanded",
            requested_all=True,
            allow=False,
        )
        self.assertIsNone(refusal)
        self.assertIsNone(warning)
        self.assertEqual(engines, list(vcf_rdfizer.VALIDATION_ENGINE_CHOICES))

    def test_the_override_keeps_cottas_under_all(self):
        engines, warning, refusal = vcf_rdfizer.cottas_condensed_resolution(
            engines=list(vcf_rdfizer.VALIDATION_ENGINE_CHOICES),
            sample_representation="condensed",
            requested_all=True,
            allow=True,
        )
        self.assertIsNone(refusal)
        self.assertIsNone(warning)
        self.assertIn("cottas", engines)


if __name__ == "__main__":
    unittest.main()
