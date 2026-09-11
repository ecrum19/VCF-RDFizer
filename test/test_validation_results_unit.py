"""Reading engine output, and refusing to trust it when it is unreadable.

Between a SPARQL engine writing its result and the comparison layer declaring a
verdict sits a thin reading layer: parse SPARQL Results JSON, recover an exact
anomaly total, reconcile parsed statements against distinct triples, and reject a
materialized N-Triples file that cannot be what it claims to be.

Every failure here has the same shape, and it is the one that matters most in a
validator: a result that could not be read must never be reported as clean. The
tests below drive each of those paths directly, with no engine, decoder, or
Docker involved - the container-only binaries are stubbed, because they are
absent in CI and a skipped test measures nothing.
"""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test.helpers import VerboseTestCase

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_results", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()


class BindingsTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def write(self, payload) -> Path:
        path = self.root / "result.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_well_formed_results_are_returned_as_rows(self):
        """The happy path is the SPARQL Results JSON bindings array."""
        path = self.write({"results": {"bindings": [{"n": {"value": "1"}}]}})
        self.assertEqual(V.bindings(path), [{"n": {"value": "1"}}])

    def test_an_empty_result_set_is_valid(self):
        """No rows is an answer, not a malformed document."""
        self.assertEqual(V.bindings(self.write({"results": {"bindings": []}})), [])

    def test_a_document_without_bindings_is_rejected(self):
        """A response shaped like an error must not read as zero rows."""
        with self.assertRaises(ValueError) as raised:
            V.bindings(self.write({"results": {}}))
        self.assertIn("Invalid SPARQL Results JSON", str(raised.exception))

    def test_a_non_list_bindings_value_is_rejected(self):
        """An object where an array belongs is refused rather than iterated."""
        with self.assertRaises(ValueError) as raised:
            V.bindings(self.write({"results": {"bindings": {}}}))
        self.assertIn("not a list", str(raised.exception))

    def test_a_completely_wrong_document_is_rejected(self):
        """A bare list or string is not a results document."""
        with self.assertRaises(ValueError):
            V.bindings(self.write([1, 2, 3]))

    def test_an_integer_binding_is_read_from_its_value(self):
        """SPARQL wraps every term in an object; the lexical form is what counts."""
        self.assertEqual(V.binding_int({"n": {"value": "42"}}, "n"), 42)


class AnomalyCountTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def execution(self, payload, status="PASS") -> dict:
        path = self.root / "count.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return {"preflight_blank_nodes_count": {"status": status, "rawResult": str(path)}}

    def test_the_exact_total_is_read_from_the_companion_aggregate(self):
        """The sampled query is truncated; the count query is not."""
        executions = self.execution(
            {"results": {"bindings": [{"anomalyCount": {"value": "17"}}]}})
        self.assertEqual(V.anomaly_count(executions, "preflight_blank_nodes"), 17)

    def test_an_absent_companion_reports_none_not_zero(self):
        """A missing exact count must not be presented as 'no anomalies'."""
        self.assertIsNone(V.anomaly_count({}, "preflight_blank_nodes"))

    def test_a_failed_companion_reports_none(self):
        """A query that did not pass cannot supply a trustworthy total."""
        executions = self.execution(
            {"results": {"bindings": [{"anomalyCount": {"value": "17"}}]}}, status="FAIL")
        self.assertIsNone(V.anomaly_count(executions, "preflight_blank_nodes"))

    def test_an_unreadable_companion_result_reports_none(self):
        """Corrupt output degrades to 'unknown' rather than raising."""
        path = self.root / "count.json"
        path.write_text("{not json", encoding="utf-8")
        executions = {"preflight_blank_nodes_count": {"status": "PASS", "rawResult": str(path)}}
        self.assertIsNone(V.anomaly_count(executions, "preflight_blank_nodes"))

    def test_a_missing_result_file_reports_none(self):
        """A result path that does not exist is unknown, not zero."""
        executions = {"preflight_blank_nodes_count": {
            "status": "PASS", "rawResult": str(self.root / "absent.json")}}
        self.assertIsNone(V.anomaly_count(executions, "preflight_blank_nodes"))

    def test_an_aggregate_returning_several_rows_reports_none(self):
        """A count query must return exactly one row to be an exact total."""
        executions = self.execution({"results": {"bindings": [
            {"anomalyCount": {"value": "1"}}, {"anomalyCount": {"value": "2"}},
        ]}})
        self.assertIsNone(V.anomaly_count(executions, "preflight_blank_nodes"))

    def test_a_row_without_the_count_variable_reports_none(self):
        """A projection change must not be read as a valid total."""
        executions = self.execution({"results": {"bindings": [{"other": {"value": "1"}}]}})
        self.assertIsNone(V.anomaly_count(executions, "preflight_blank_nodes"))

    def test_a_non_numeric_count_reports_none(self):
        """A malformed literal is unknown rather than an exception."""
        executions = self.execution(
            {"results": {"bindings": [{"anomalyCount": {"value": "many"}}]}})
        self.assertIsNone(V.anomaly_count(executions, "preflight_blank_nodes"))


class DuplicateTripleReportTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def executions(self, distinct, status="PASS") -> dict:
        path = self.root / "distinct.json"
        path.write_text(json.dumps(
            {"results": {"bindings": [{"distinctTripleCount": {"value": str(distinct)}}]}}),
            encoding="utf-8")
        return {"preflight_distinct_triple_count": {"status": status, "rawResult": str(path)}}

    def test_equal_counts_pass_with_no_duplicates(self):
        """A store that deduplicated nothing means the source had no repeats."""
        report = V.duplicate_triple_report(self.executions(100), 100)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["duplicateTripleCount"], 0)

    def test_the_difference_is_the_duplicate_count(self):
        """Parsed minus distinct is exactly the number of redundant statements."""
        report = V.duplicate_triple_report(self.executions(90), 100)
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["parsedTripleCount"], 100)
        self.assertEqual(report["distinctTripleCount"], 90)
        self.assertEqual(report["duplicateTripleCount"], 10)

    def test_a_missing_parsed_count_is_not_evaluated(self):
        """Without the parser's total there is nothing to compare against."""
        report = V.duplicate_triple_report(self.executions(90), None)
        self.assertEqual(report["status"], "NOT_EVALUATED")
        self.assertIn("statement count", report["reason"])

    def test_a_query_that_did_not_run_is_not_evaluated(self):
        """An absent or failed query cannot establish a clean result."""
        self.assertEqual(
            V.duplicate_triple_report({}, 100)["status"], "NOT_EVALUATED")
        self.assertEqual(
            V.duplicate_triple_report(self.executions(90, status="FAIL"), 100)["status"],
            "NOT_EVALUATED")

    def test_an_unreadable_result_is_not_evaluated_and_says_why(self):
        """A corrupt result is reported as unknown with its cause attached."""
        path = self.root / "distinct.json"
        path.write_text("{not json", encoding="utf-8")
        report = V.duplicate_triple_report(
            {"preflight_distinct_triple_count": {"status": "PASS", "rawResult": str(path)}}, 100)
        self.assertEqual(report["status"], "NOT_EVALUATED")
        self.assertIn("unreadable result", report["reason"])

    def test_an_empty_result_set_is_not_evaluated(self):
        """No row means no distinct count, which is not the same as zero."""
        path = self.root / "distinct.json"
        path.write_text(json.dumps({"results": {"bindings": []}}), encoding="utf-8")
        report = V.duplicate_triple_report(
            {"preflight_distinct_triple_count": {"status": "PASS", "rawResult": str(path)}}, 100)
        self.assertEqual(report["status"], "NOT_EVALUATED")


class MaterializedGraphVerificationTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def verify(self, data: bytes) -> int:
        path = self.root / "decoded.nt"
        path.write_bytes(data)
        return V._verify_ntriples(path, label="hdt2rdf")

    def test_a_well_formed_decode_returns_its_size(self):
        """The size is reused for reporting, so it is returned rather than discarded."""
        payload = b"<urn:a> <urn:b> <urn:c> .\n"
        self.assertEqual(self.verify(payload), len(payload))

    def test_a_leading_blank_node_is_accepted(self):
        """A graph may legitimately begin with a blank-node subject."""
        self.assertGreater(self.verify(b"_:b1 <urn:b> <urn:c> .\n"), 0)

    def test_an_unreadable_path_is_diagnosed(self):
        """A decoder that wrote nothing at all must be named as the cause."""
        with self.assertRaises(RuntimeError) as raised:
            V._verify_ntriples(self.root / "absent.nt", label="hdt2rdf")
        self.assertIn("produced no readable output", str(raised.exception))

    def test_an_empty_file_is_rejected(self):
        """A zero-byte decode is a failure even when the exit code was zero."""
        with self.assertRaises(RuntimeError) as raised:
            self.verify(b"")
        self.assertIn("empty N-Triples file", str(raised.exception))

    def test_a_file_with_no_newline_is_rejected_immediately(self):
        """This is the failure that looked like a hang: one enormous line."""
        with self.assertRaises(RuntimeError) as raised:
            self.verify(b"<urn:a> <urn:b> <urn:c> .")
        self.assertIn("no newline", str(raised.exception))

    def test_a_first_line_that_does_not_begin_a_triple_is_rejected(self):
        """A subject must be an IRI or a blank node, not arbitrary text."""
        with self.assertRaises(RuntimeError) as raised:
            self.verify(b"oops <urn:b> <urn:c> .\n")
        self.assertIn("does not begin a triple", str(raised.exception))

    def test_a_first_line_without_a_terminator_is_rejected(self):
        """A missing '.' means the line was truncated mid-statement."""
        with self.assertRaises(RuntimeError) as raised:
            self.verify(b"<urn:a> <urn:b> <urn:c>\n")
        self.assertIn("no statement terminator", str(raised.exception))

    def test_a_file_not_ending_in_a_newline_is_reported_as_truncated(self):
        """The tail check catches a decode that stopped partway through."""
        with self.assertRaises(RuntimeError) as raised:
            self.verify(b"<urn:a> <urn:b> <urn:c> .\n<urn:d> <urn:e> <urn:f> .")
        self.assertIn("most likely truncated", str(raised.exception))

    def test_the_diagnosis_quotes_the_offending_bytes(self):
        """An operator needs to see what was produced, not only that it was wrong."""
        with self.assertRaises(RuntimeError) as raised:
            self.verify(b"garbage line here\n")
        self.assertIn("garbage line here", str(raised.exception))


class BinaryResolutionTests(VerboseTestCase):
    def test_an_environment_override_wins_when_it_resolves(self):
        """The image pins decoder builds through environment variables."""
        with mock.patch.dict(V.os.environ, {"HDT2RDF_BIN": "/bin/sh"}, clear=False):
            self.assertEqual(V._resolve_binary("HDT2RDF_BIN", "hdt2rdf"), "/bin/sh")

    def test_an_unresolvable_override_falls_through_to_the_candidates(self):
        """A stale override must not mask a working binary on PATH."""
        with mock.patch.dict(V.os.environ, {"HDT2RDF_BIN": "not-a-real-binary-xyz"}, clear=False):
            self.assertEqual(
                V._resolve_binary("HDT2RDF_BIN", "sh"), V.shutil.which("sh"))

    def test_a_blank_override_is_ignored(self):
        """An empty variable is 'unset', not a path to the empty string."""
        with mock.patch.dict(V.os.environ, {"HDT2RDF_BIN": "   "}, clear=False):
            self.assertEqual(V._resolve_binary("HDT2RDF_BIN", "sh"), V.shutil.which("sh"))

    def test_a_missing_binary_names_the_primary_candidate(self):
        """The error must say which tool the image is missing."""
        with mock.patch.dict(V.os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as raised:
                V._resolve_binary("HDT2RDF_BIN", "hdt2rdf-absent-xyz", "also-absent-xyz")
        self.assertIn("hdt2rdf-absent-xyz", str(raised.exception))


class EngineAdviceTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.source = self.root / "graph.nt"
        self.source.write_bytes(b"x" * 4096)

    def test_an_indexing_engine_needs_no_warning(self):
        """The advisory is specifically about comunica's lack of an index."""
        self.assertIsNone(V.engine_advice("qlever", self.source, 13, 600))

    def test_a_small_graph_needs_no_warning(self):
        """Below the threshold, streaming per query is perfectly reasonable."""
        self.assertIsNone(V.engine_advice("comunica", self.source, 13, 600))

    def test_a_large_graph_warns_and_names_the_alternatives(self):
        """The warning must be actionable, not merely alarming."""
        with mock.patch.object(V, "UNINDEXED_ENGINE_ADVICE_BYTES", 1024):
            advice = V.engine_advice("comunica", self.source, 13, 600)
        self.assertIsNotNone(advice)
        self.assertIn("13", advice)
        self.assertIn("--engine qlever", advice)

    def test_an_unreadable_source_produces_no_advice(self):
        """A missing file is a different error, reported elsewhere."""
        self.assertIsNone(V.engine_advice("comunica", self.root / "absent.nt", 13, 600))


class NtriplesSyntaxCheckTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.source = self.root / "graph.nt"
        self.source.write_bytes(b"<urn:a> <urn:b> <urn:c> .\n")

    def test_an_absent_rapper_is_an_execution_failure_not_a_conformance_failure(self):
        """A missing tool must never be reported as an invalid graph."""
        with mock.patch.object(V.shutil, "which", return_value=None):
            result = V.validate_ntriples(self.source, self.root)
        self.assertEqual(result["status"], "EXECUTION_FAILED")
        self.assertIn("rapper", result["error"])

    def test_a_clean_parse_passes_and_records_the_triple_count(self):
        """The count makes a decoded artifact comparable against its source."""
        completed = mock.Mock(
            returncode=0, stdout="", stderr="rapper: Parsing returned 5 triples\n")
        with mock.patch.object(V.shutil, "which", return_value="/usr/bin/rapper"), \
                mock.patch.object(V.subprocess, "run", return_value=completed):
            result = V.validate_ntriples(self.source, self.root)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["tripleCount"], 5)
        self.assertEqual(result["exitCode"], 0)

    def test_a_non_zero_exit_fails(self):
        """A syntax error in the graph is a conformance failure."""
        completed = mock.Mock(returncode=1, stdout="", stderr="rapper: Error - syntax\n")
        with mock.patch.object(V.shutil, "which", return_value="/usr/bin/rapper"), \
                mock.patch.object(V.subprocess, "run", return_value=completed):
            result = V.validate_ntriples(self.source, self.root)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["exitCode"], 1)

    def test_an_unparsable_banner_leaves_the_count_unknown(self):
        """A missing count is None rather than a fabricated zero."""
        completed = mock.Mock(returncode=0, stdout="", stderr="done\n")
        with mock.patch.object(V.shutil, "which", return_value="/usr/bin/rapper"), \
                mock.patch.object(V.subprocess, "run", return_value=completed):
            result = V.validate_ntriples(self.source, self.root)
        self.assertIsNone(result["tripleCount"])

    def test_the_tool_output_is_written_beside_the_report(self):
        """The log is the evidence for the verdict and must survive the run."""
        completed = mock.Mock(returncode=0, stdout="out\n", stderr="err\n")
        with mock.patch.object(V.shutil, "which", return_value="/usr/bin/rapper"), \
                mock.patch.object(V.subprocess, "run", return_value=completed):
            result = V.validate_ntriples(self.source, self.root)
        log = Path(result["log"])
        self.assertTrue(log.is_file())
        self.assertIn("out", log.read_text(encoding="utf-8"))
        self.assertIn("err", log.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
