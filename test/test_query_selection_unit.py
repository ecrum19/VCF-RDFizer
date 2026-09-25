"""A subset run must be a measurement, never a validation verdict.

``--queries`` exists so retrieval cost can be measured one question at a time:
the thirteen core queries are what Figure 6 of the manuscript reports, while
the preflight set costs an order of magnitude more and appears nowhere in it.
Paying for the whole set to time thirteen queries is the thing this avoids.

The risk it introduces is that someone reads a subset run as a passed
validation. ``evaluate_validation`` decides PASS/MISMATCH from preflight
gating, the sample/GT inventory and invariants computed across every query, so
a subset cannot reach that decision honestly. These tests pin the two
properties that keep the two apart: selection resolves to canonical ids in
canonical order, and the subset path reports per query without aggregating a
verdict.
"""

import importlib.util
import unittest
from pathlib import Path

RUNNER = Path(__file__).resolve().parent.parent / "src" / "validation" / "validation_runner.py"
_spec = importlib.util.spec_from_file_location("validation_runner_for_tests", RUNNER)
vr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vr)


class QuerySelectionTests(unittest.TestCase):
    def test_groups_expand(self):
        self.assertEqual(vr.parse_query_selection("core"), vr.CORE_QUERIES)
        self.assertEqual(
            vr.parse_query_selection("all"),
            vr.PREFLIGHT_QUERIES + vr.PREFLIGHT_COUNT_QUERIES + vr.CORE_QUERIES,
        )

    def test_single_query(self):
        self.assertEqual(vr.parse_query_selection("q03_titv"), ("q03_titv",))

    def test_result_is_in_canonical_order_not_caller_order(self):
        # Execution order matters: an anomaly preflight's ``_count`` companion
        # is derived from the sample that must already have run, so honouring
        # the caller's order would break that derivation.
        self.assertEqual(
            vr.parse_query_selection("q13_format_value_digest,q01_record_density_1mb"),
            ("q01_record_density_1mb", "q13_format_value_digest"),
        )

    def test_count_query_pulls_in_its_sample(self):
        # Asking for the count alone would otherwise re-scan the graph for a
        # number the sample query already carries.
        self.assertEqual(
            vr.parse_query_selection("preflight_blank_nodes_count"),
            ("preflight_blank_nodes", "preflight_blank_nodes_count"),
        )

    def test_mixed_groups_and_ids_deduplicate(self):
        self.assertEqual(vr.parse_query_selection("core,q03_titv"), vr.CORE_QUERIES)

    def test_unknown_query_is_rejected(self):
        with self.assertRaises(ValueError) as caught:
            vr.parse_query_selection("q99_nonexistent")
        self.assertIn("q99_nonexistent", str(caught.exception))

    def test_empty_selection_is_rejected(self):
        for raw in ("", ",", "  ,  "):
            with self.assertRaises(ValueError):
                vr.parse_query_selection(raw)

    def test_whitespace_is_tolerated(self):
        self.assertEqual(
            vr.parse_query_selection(" q03_titv , q01_record_density_1mb "),
            ("q01_record_density_1mb", "q03_titv"),
        )


class TimingOnlyVerdictTests(unittest.TestCase):
    """The subset path reports per query and refuses to aggregate."""

    def _parser(self, **overrides):
        parser = {
            "sampleCount": 1,
            "gtRecordCount": 10,
            "q03_titv": {
                "biallelicSnvCount": 4, "transitionCount": 3, "transversionCount": 1,
            },
        }
        parser.update(overrides)
        return parser

    def _executions(self, tmp, payload):
        import json
        raw = Path(tmp) / "q03.json"
        raw.write_text(json.dumps(payload))
        return {"q03_titv": {"status": "PASS", "rawResult": str(raw)}}

    def test_agreeing_subset_reports_timing_only_and_no_verdict(self):
        import tempfile
        rows = [{
            "biallelicSnvCount": {"value": "4"},
            "transitionCount": {"value": "3"},
            "transversionCount": {"value": "1"},
        }]
        with tempfile.TemporaryDirectory() as tmp:
            result = vr.evaluate_timing_only(
                self._executions(tmp, {"results": {"bindings": rows}}),
                self._parser(), "expanded", selected=("q03_titv",),
            )
        self.assertEqual(result["status"], "TIMING_ONLY")
        self.assertTrue(result["answersAgree"])
        self.assertEqual(result["selectedQueries"], ["q03_titv"])
        # The keys a validation consumer looks for must be absent, so reading a
        # subset as a pass fails loudly rather than silently.
        self.assertNotIn("comparison", result)
        self.assertNotIn("preflight", result)

    def test_a_disagreement_is_still_a_failure(self):
        # A fast wrong answer is not a result: the equality check survives even
        # though the verdict does not.
        import tempfile
        rows = [{
            "biallelicSnvCount": {"value": "999"},
            "transitionCount": {"value": "3"},
            "transversionCount": {"value": "1"},
        }]
        with tempfile.TemporaryDirectory() as tmp:
            result = vr.evaluate_timing_only(
                self._executions(tmp, {"results": {"bindings": rows}}),
                self._parser(), "expanded", selected=("q03_titv",),
            )
        self.assertFalse(result["answersAgree"])


if __name__ == "__main__":
    unittest.main()
