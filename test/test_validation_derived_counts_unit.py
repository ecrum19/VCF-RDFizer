"""Deriving an anomaly total instead of re-scanning the graph for it.

Every anomaly preflight is a ``SELECT ... LIMIT 100`` paired with a ``*_count``
aggregate. Both scan the whole graph -- the LIMIT bounds what comes back, not
what is examined -- so on the 17.1M-triple benchmark graph the pair cost 97.0 s
and 94.6 s to report the same zero. Three such scans were 294.1 s of the 298.1 s
that all fourteen preflights cost, against 17.1 s for the thirteen semantic
queries they exist to protect.

When the sample returns fewer rows than its limit it enumerated every match, so
the exact total is the number of rows it returned. Deriving it is not an
approximation, and these tests pin that distinction: derive below the limit,
execute at or above it.
"""

import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_derived", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()


def write_sample(directory: Path, rows: int, query_id: str = "preflight_empty_values") -> dict:
    """A PASSing sample execution whose raw result holds ``rows`` bindings."""
    raw = directory / f"{query_id}.json"
    raw.write_text(
        json.dumps(
            {
                "head": {"vars": ["s", "p", "o", "issue"]},
                "results": {
                    "bindings": [
                        {
                            "s": {"type": "uri", "value": f"urn:s{index}"},
                            "p": {"type": "uri", "value": "urn:p"},
                            "o": {"type": "literal", "value": " "},
                            "issue": {"type": "literal", "value": "EMPTY_LITERAL"},
                        }
                        for index in range(rows)
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    return {
        "status": "PASS",
        "engine": "qlever",
        "exitCode": 0,
        "wallSeconds": 97.01,
        "query": f"/queries/{query_id}.rq",
        "rawResult": str(raw),
        "stderr": None,
        "resourceMetrics": None,
    }


class DeriveAnomalyCountTests(VerboseTestCase):
    def test_a_clean_sample_derives_a_zero_count_without_a_second_scan(self):
        """Zero anomalies is the common case and must not cost a full scan."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            sample = write_sample(directory, rows=0)
            derived = V.derive_anomaly_count_execution(
                sample, "preflight_empty_values", directory
            )
            self.assertIsNotNone(derived)
            self.assertTrue(derived["derived"])
            self.assertEqual(derived["derivedFrom"], "preflight_empty_values")
            self.assertEqual(derived["status"], "PASS")
            self.assertEqual(derived["wallSeconds"], 0.0)
            self.assertEqual(
                V.anomaly_count({"preflight_empty_values_count": derived},
                                "preflight_empty_values"),
                0,
            )

    def test_a_partial_sample_derives_exactly_the_rows_it_returned(self):
        """Under the limit, the sample IS the population -- not an estimate."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            sample = write_sample(directory, rows=20)
            derived = V.derive_anomaly_count_execution(
                sample, "preflight_empty_values", directory
            )
            self.assertIsNotNone(derived)
            self.assertEqual(
                V.anomaly_count({"preflight_empty_values_count": derived},
                                "preflight_empty_values"),
                20,
            )

    def test_a_truncated_sample_refuses_to_derive_and_demands_the_aggregate(self):
        """At the limit the true count is unknown; guessing it would be a lie."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            sample = write_sample(directory, rows=V.ANOMALY_SAMPLE_LIMIT)
            self.assertIsNone(
                V.derive_anomaly_count_execution(
                    sample, "preflight_empty_values", directory
                )
            )

    def test_a_failed_sample_refuses_to_derive(self):
        """A sample that did not run says nothing about how many matches exist."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            sample = write_sample(directory, rows=0)
            sample["status"] = "EXECUTION_FAILED"
            self.assertIsNone(
                V.derive_anomaly_count_execution(
                    sample, "preflight_empty_values", directory
                )
            )

    def test_an_unreadable_sample_refuses_to_derive(self):
        """A corrupt result must not silently become a confident zero."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            sample = write_sample(directory, rows=0)
            Path(sample["rawResult"]).write_text("not json", encoding="utf-8")
            self.assertIsNone(
                V.derive_anomaly_count_execution(
                    sample, "preflight_empty_values", directory
                )
            )

    def test_every_anomaly_preflight_has_a_count_companion_that_can_be_derived(self):
        """The optimisation applies to the whole family, not just empty_values."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for query_id in V.ANOMALY_PREFLIGHT_QUERIES:
                self.assertIn(f"{query_id}_count", V.PREFLIGHT_COUNT_QUERIES)
                sample = write_sample(directory, rows=0, query_id=query_id)
                derived = V.derive_anomaly_count_execution(sample, query_id, directory)
                self.assertIsNotNone(derived, query_id)
                self.assertEqual(
                    V.anomaly_count({f"{query_id}_count": derived}, query_id), 0, query_id
                )

    def test_the_derived_record_names_why_it_was_not_executed(self):
        """A 0.0 s row must carry its own explanation, not need one."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            derived = V.derive_anomaly_count_execution(
                write_sample(directory, rows=3), "preflight_empty_values", directory
            )
            self.assertIn("enumerated every match", derived["derivedReason"])
            self.assertIn("preflight_empty_values", derived["derivedReason"])


class WhichQueriesTheLoopSkipsTests(VerboseTestCase):
    """The decision the query loop makes, isolated from any engine."""

    def test_a_semantic_query_is_always_executed(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.assertIsNone(
                V.derived_count_for("q05_sample_genotype_counts", {}, directory)
            )

    def test_a_sample_preflight_is_always_executed(self):
        """The sample is the thing the count is derived FROM; it must run."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.assertIsNone(
                V.derived_count_for("preflight_empty_values", {}, directory)
            )

    def test_a_count_with_no_sample_recorded_yet_is_executed(self):
        """Ordering safety: never derive from a result that is not there."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.assertIsNone(
                V.derived_count_for("preflight_empty_values_count", {}, directory)
            )

    def test_the_non_anomaly_distinct_triple_count_is_executed(self):
        """It has a _count suffix but no sample pair, so it is not derivable."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.assertNotIn(
                "preflight_distinct_triple_count", V.ANOMALY_PREFLIGHT_QUERIES
            )
            self.assertIsNone(
                V.derived_count_for("preflight_distinct_triple_count", {}, directory)
            )

    def test_a_count_whose_sample_came_back_clean_is_derived(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            executions = {"preflight_empty_values": write_sample(directory, rows=0)}
            derived = V.derived_count_for(
                "preflight_empty_values_count", executions, directory
            )
            self.assertIsNotNone(derived)
            self.assertTrue(derived["derived"])

    def test_the_sample_always_precedes_its_count_in_the_execution_order(self):
        """Derivation depends on it; a reordering must fail here, not in a run."""
        order = V.PREFLIGHT_QUERIES + V.PREFLIGHT_COUNT_QUERIES
        for query_id in V.ANOMALY_PREFLIGHT_QUERIES:
            self.assertLess(
                order.index(query_id),
                order.index(f"{query_id}_count"),
                query_id,
            )


class DerivedRowsAreMarkedInTheBenchmarkTests(VerboseTestCase):
    def test_benchmark_csv_carries_a_derived_column(self):
        """Analysis must be able to exclude derived rows before summing cost."""
        self.assertIn("derived", V.BENCHMARK_CSV_HEADER)

    def test_a_derived_row_is_flagged_and_an_executed_row_is_not(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "benchmark.csv"
            V.write_benchmark_csv(
                path,
                {
                    "oracle": {"totalSeconds": 12.41, "perQuerySeconds": {}},
                    "engines": {
                        "qlever": {
                            "setupSeconds": 22.5,
                            "artifactOrigin": "run artifact",
                            "queries": {
                                "preflight_empty_values": {
                                    "status": "PASS",
                                    "wallSeconds": 97.01,
                                    "derived": False,
                                },
                                "preflight_empty_values_count": {
                                    "status": "PASS",
                                    "wallSeconds": 0.0,
                                    "derived": True,
                                },
                            },
                        }
                    },
                },
            )
            rows = {
                row["query_id"]: row
                for row in csv.DictReader(path.open(encoding="utf-8"))
            }
            self.assertEqual(rows["preflight_empty_values"]["derived"], "0")
            self.assertEqual(rows["preflight_empty_values_count"]["derived"], "1")


if __name__ == "__main__":
    unittest.main()
