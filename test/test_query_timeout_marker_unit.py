"""A query that times out must never be recorded as passing.

When a Comunica endpoint's own per-query timeout fires, it has already sent
HTTP 200 and the start of the result, so it ends the response with
``!TIMEDOUT!`` and kills its worker. The validator read the 200 as success:
in the vcf-bench-3 scale run (HG005, 1,000,000 records, HDT), q05 ran for
3600.06s, returned the body below, and was recorded as ``PASS``. The worker's
replacement was not yet listening, so q06-q13 were then refused in under a
millisecond each.

These tests pin both halves: the marker is a timeout (exit 124, not a pass),
any exit-0 result that is not SPARQL Results JSON fails, and the endpoint is
restarted before the next query so one timeout costs one query, not the rest
of the suite.

A crash is handled the same way. In the same run the COTTAS endpoint's worker
died during q06 (a V8 fatal error during garbage collection, SIGTRAP): q06 got
``IncompleteRead(70 bytes read)``, q07 a reset connection, and q08-q13 were
refused. The crashed query is a failure; the endpoint is restarted and the
suite resumes with the next query.
"""

import argparse
import copy
import http.client
import importlib.util
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from test import validation_fixtures as fixtures
from test.helpers import VerboseTestCase

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_timeout_marker", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()

#: The q05 response recorded on vcf-bench-3, byte for byte (raw/hdt/
#: q05_sample_genotype_counts.sparql.json in the v3.3.1 provenance rerun).
RECORDED_TIMEOUT_BODY = (
    b'{"head": {"vars":["sampleId","genotypeClass","callCount"]},\n'
    b'"results": { "bindings": [\n'
    b"!TIMEDOUT!"
)
EMPTY_RESULT = b'{"results": {"bindings": []}}'


class FakeComunica:
    """A Comunica endpoint whose worker dies when its timeout fires.

    ``Popen`` starts it, a query in ``timeouts`` gets the recorded truncated
    body, and from then on every connection is refused until it is started
    again -- what the real master does while it forks a replacement worker.
    """

    def __init__(self, *, timeouts=(), crashes=(), answers=None, restartable=True):
        self.timeouts = set(timeouts)
        self.crashes = set(crashes)
        self.answers = answers or {}
        self.restartable = restartable
        self.alive = False
        self.starts = 0
        self.queries = []
        self.log = None

    def popen(self, *_args, **kwargs):
        self.starts += 1
        self.alive = self.starts == 1 or self.restartable
        self.log = kwargs.get("stdout")
        if self.log is not None:
            self.log.write(f"Server running (start {self.starts})\n".encode("utf-8"))
            self.log.flush()
        process = mock.MagicMock()
        process.poll.return_value = None
        return process

    def post(self, _engine, _endpoint, query, *, timeout):
        if not self.alive:
            raise urllib.error.URLError(ConnectionRefusedError(111, "Connection refused"))
        query_id = query.splitlines()[0].lstrip("# ").strip()
        if query_id not in set(self.answers) | self.timeouts | self.crashes:
            return EMPTY_RESULT  # bind and warm-up probes
        self.queries.append(query_id)
        if query_id in self.crashes:
            # What the real worker left: a stack trace in the log, and a
            # response cut off before its declared length.
            self.alive = False
            self.log.write(b"# Fatal error in , line 0\nWorker 69 died with SIGTRAP.\n")
            self.log.flush()
            raise http.client.IncompleteRead(b"x" * 70)
        if query_id in self.timeouts:
            self.alive = False
            return RECORDED_TIMEOUT_BODY
        return json.dumps(self.answers[query_id]).encode("utf-8")

    def patches(self):
        return [
            mock.patch.object(V.shutil, "which", return_value="/usr/bin/comunica-sparql-file-http"),
            mock.patch.object(V, "tool_version", return_value=None),
            mock.patch.object(V.subprocess, "Popen", side_effect=self.popen),
            mock.patch.object(V, "_terminate_process_group"),
            mock.patch.object(V.time, "sleep"),
            mock.patch.object(V.ComunicaHttpEndpointMixin, "_post", self.post_method()),
        ]

    def post_method(self):
        fake = self

        def _post(engine, endpoint, query, *, timeout):
            return fake.post(engine, endpoint, query, timeout=timeout)

        return _post


def query_file(directory, query_id):
    path = Path(directory) / f"{query_id}.rq"
    path.write_text(f"# {query_id}\nSELECT * WHERE {{ ?s ?p ?o }}\n", encoding="utf-8")
    return path


class EngineTimeoutMarkerTests(VerboseTestCase):
    def _run(self, fake, query_ids, *, options=None):
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.nt"
            source.write_bytes(b"<s> <p> <o> .\n")
            engine = V.build_engine(
                "comunica", source, raw_dir=tmp_path, scratch=tmp_path,
                options={"query_timeout": 3600, **(options or {})},
            )
            patches = fake.patches()
            for patch in patches:
                patch.start()
            try:
                engine.start()
                warmup = engine.warmup_seconds
                results = [engine.execute(qid, query_file(tmp_path, qid)) for qid in query_ids]
                raws = [Path(r["rawResult"]).read_bytes() for r in results]
                engine.stop()
            finally:
                for patch in reversed(patches):
                    patch.stop()
        return engine, warmup, results, raws

    def test_the_recorded_timeout_response_is_a_timeout_not_a_pass(self):
        fake = FakeComunica(timeouts={"q05"})
        _engine, _warmup, (result,), (raw,) = self._run(fake, ["q05"])
        self.assertEqual(result["status"], "EXECUTION_FAILED")
        # 124 is the timeout convention --stop-after-query-timeout keys on.
        self.assertEqual(result["exitCode"], 124)
        self.assertIn("exceeded 3600s", result["error"])
        self.assertIn("!TIMEDOUT!", result["error"])
        # What the endpoint sent is kept as the evidence, not blanked.
        self.assertEqual(raw, RECORDED_TIMEOUT_BODY)

    def test_the_query_after_a_timeout_runs_on_a_restarted_endpoint(self):
        answer = {"results": {"bindings": [{"n": {"value": "1"}}]}}
        fake = FakeComunica(timeouts={"q05"}, answers={"q06": answer, "q07": answer})
        engine, warmup, results, raws = self._run(fake, ["q05", "q06", "q07"])
        timed_out, after, later = results

        self.assertEqual(timed_out["exitCode"], 124)
        # Before the fix q06 and q07 were refused: the worker was gone.
        self.assertEqual(after["status"], "PASS")
        self.assertEqual(later["status"], "PASS")
        self.assertEqual(json.loads(raws[1]), answer)
        self.assertEqual(fake.queries, ["q05", "q06", "q07"])
        # Restarted exactly once, by the query that followed the timeout, and
        # the restart is reported on that query rather than hidden.
        self.assertEqual(fake.starts, 2)
        self.assertIn("endpointRestartSeconds", after)
        self.assertNotIn("endpointRestartError", after)
        self.assertNotIn("endpointRestartSeconds", later)
        self.assertNotIn("endpointRestartSeconds", timed_out)
        # The restart's warm-up is not passed off as the original setup's.
        self.assertEqual(engine.warmup_seconds, warmup)

    def test_a_client_side_timeout_also_restarts_the_endpoint(self):
        """If urlopen gives up first, the worker is still busy with the old query."""
        import socket

        fake = FakeComunica(answers={"q06": {"results": {"bindings": []}}})
        original = fake.post

        def post(engine, endpoint, query, *, timeout):
            if query.startswith("# q05"):
                raise urllib.error.URLError(socket.timeout("timed out"))
            return original(engine, endpoint, query, timeout=timeout)

        fake.post = post
        _engine, _warmup, (timed_out, after), _raws = self._run(fake, ["q05", "q06"])
        self.assertEqual(timed_out["exitCode"], 124)
        self.assertEqual(after["status"], "PASS")
        self.assertEqual(fake.starts, 2)

    def test_an_ordinary_failure_does_not_restart_the_endpoint(self):
        """A query the endpoint answers with a rejection leaves the endpoint fine."""
        fake = FakeComunica(answers={"q06": {"results": {"bindings": []}}})
        original = fake.post

        def post(engine, endpoint, query, *, timeout):
            if query.startswith("# q05"):
                raise urllib.error.URLError("query rejected")
            return original(engine, endpoint, query, timeout=timeout)

        fake.post = post
        _engine, _warmup, (failed, after), _raws = self._run(fake, ["q05", "q06"])
        self.assertEqual(failed["exitCode"], 1)
        self.assertEqual(after["status"], "PASS")
        self.assertEqual(fake.starts, 1)

    def test_an_endpoint_that_will_not_restart_fails_the_rest_quickly(self):
        fake = FakeComunica(timeouts={"q05"}, answers={"q06": {}, "q07": {}}, restartable=False)
        _engine, _warmup, results, _raws = self._run(
            fake, ["q05", "q06", "q07"], options={"comunica_bind_timeout": 1},
        )
        _timed_out, after, later = results
        self.assertEqual(after["status"], "EXECUTION_FAILED")
        self.assertIn("did not bind", after["endpointRestartError"])
        # One restart attempt, not one per remaining query.
        self.assertEqual(fake.starts, 2)
        self.assertEqual(later["status"], "EXECUTION_FAILED")
        self.assertIn("not running", later["error"])
        self.assertNotIn("endpointRestartSeconds", later)

    def test_a_crash_fails_that_query_and_the_suite_resumes_after_it(self):
        """vcf-bench-3, COTTAS: one crash in q06 cost q06-q13; now it costs q06."""
        answer = {"results": {"bindings": [{"n": {"value": "1"}}]}}
        fake = FakeComunica(
            crashes={"q06"}, answers={q: answer for q in ("q05", "q07", "q08")}
        )
        _engine, _warmup, results, _raws = self._run(fake, ["q05", "q06", "q07", "q08"])
        before, crashed, after, later = results

        self.assertEqual(before["status"], "PASS")
        self.assertEqual(crashed["status"], "EXECUTION_FAILED")
        # A crash is not a timeout: exit 1, so --stop-after-query-timeout
        # does not treat it as one.
        self.assertEqual(crashed["exitCode"], 1)
        self.assertIn("endpoint was lost", crashed["error"])
        self.assertIn("IncompleteRead", crashed["error"])
        self.assertEqual(after["status"], "PASS")
        self.assertEqual(later["status"], "PASS")
        # The crashed query is not retried, and every later query runs.
        self.assertEqual(fake.queries, ["q05", "q06", "q07", "q08"])
        self.assertEqual(fake.starts, 2)
        self.assertIn("endpointRestartSeconds", after)
        self.assertNotIn("endpointRestartSeconds", later)

    def test_a_refused_or_reset_connection_also_restarts_the_endpoint(self):
        """What the queries after the crash saw before the restart existed."""
        answer = {"results": {"bindings": []}}
        for error in (
            urllib.error.URLError(ConnectionRefusedError(111, "Connection refused")),
            ConnectionResetError(104, "Connection reset by peer"),
        ):
            with self.subTest(error=type(error).__name__):
                fake = FakeComunica(answers={"q08": answer})
                original = fake.post

                def post(engine, endpoint, query, *, timeout, error=error):
                    if query.startswith("# q07"):
                        raise error
                    return original(engine, endpoint, query, timeout=timeout)

                fake.post = post
                _engine, _warmup, (lost, after), _raws = self._run(fake, ["q07", "q08"])
                self.assertEqual(lost["exitCode"], 1)
                self.assertIn("endpoint was lost", lost["error"])
                self.assertEqual(after["status"], "PASS")
                self.assertEqual(fake.starts, 2)

    def test_the_restart_keeps_the_crash_in_the_server_log(self):
        """The stack trace is the only record of why the endpoint died."""
        answer = {"results": {"bindings": []}}
        fake = FakeComunica(crashes={"q06"}, answers={"q07": answer})
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.nt"
            source.write_bytes(b"<s> <p> <o> .\n")
            engine = V.build_engine(
                "comunica", source, raw_dir=tmp_path, scratch=tmp_path,
                options={"query_timeout": 3600},
            )
            patches = fake.patches()
            for patch in patches:
                patch.start()
            try:
                engine.start()
                for qid in ("q06", "q07"):
                    engine.execute(qid, query_file(tmp_path, qid))
                engine.stop()
            finally:
                for patch in reversed(patches):
                    patch.stop()
            log = (engine.log_dir / "comunica-server.log").read_text(encoding="utf-8")
        self.assertIn("Server running (start 1)", log)
        self.assertIn("Worker 69 died with SIGTRAP", log)
        self.assertIn("=== endpoint restarted after q06 ===", log)
        self.assertIn("Server running (start 2)", log)
        self.assertLess(log.index("SIGTRAP"), log.index("restarted after q06"))

    def test_only_a_lost_connection_counts_as_a_lost_endpoint(self):
        self.assertTrue(V.is_endpoint_lost_error(http.client.IncompleteRead(b"", 10)))
        self.assertTrue(V.is_endpoint_lost_error(ConnectionResetError()))
        self.assertTrue(V.is_endpoint_lost_error(http.client.RemoteDisconnected()))
        self.assertTrue(V.is_endpoint_lost_error(
            urllib.error.URLError(ConnectionRefusedError(111, "Connection refused"))
        ))
        # The endpoint answered: its verdict on the query is the result.
        self.assertFalse(V.is_endpoint_lost_error(
            urllib.error.HTTPError("http://x", 500, "error", {}, None)
        ))
        self.assertFalse(V.is_endpoint_lost_error(urllib.error.URLError("query rejected")))
        self.assertFalse(V.is_endpoint_lost_error(ValueError("bad JSON")))

    def test_a_marker_mid_stream_is_still_a_timeout(self):
        """Comunica appends the marker to whatever it had already streamed."""
        self.assertTrue(V.ends_with_timeout_marker(b'{"results":{"bindings":[{"a":1},!TIMEDOUT!\n'))
        self.assertTrue(V.ends_with_timeout_marker(b"!TIMEDOUT!"))
        self.assertFalse(V.ends_with_timeout_marker(EMPTY_RESULT))
        # A literal that happens to contain the word is not a timeout.
        self.assertFalse(V.ends_with_timeout_marker(
            b'{"results":{"bindings":[{"v":{"value":"!TIMEDOUT!"}}]}}'
        ))


class ResultProblemTests(VerboseTestCase):
    """Exit 0 is not a pass unless the engine produced a SPARQL result."""

    def _problem(self, payload):
        with tempfile.TemporaryDirectory() as td:
            raw = Path(td) / "q.sparql.json"
            if payload is not None:
                raw.write_bytes(payload)
            return V.result_problem(raw)

    def test_well_formed_results_pass(self):
        self.assertIsNone(self._problem(EMPTY_RESULT))
        self.assertIsNone(self._problem(b'{"head":{},"results":{"bindings":[{"x":{"value":"1"}}]}}'))
        self.assertIsNone(self._problem(b'{"head":{},"boolean":false}'))

    def test_the_recorded_timeout_body_is_named_as_a_timeout(self):
        self.assertIn("timeout marker", self._problem(RECORDED_TIMEOUT_BODY))

    def test_malformed_results_are_problems(self):
        for payload in (
            b"",
            b'{"results": {"bindings": [',
            b"<html>502 Bad Gateway</html>",
            b"[]",
            b'{"results": {}}',
            b'{"results": {"bindings": {}}}',
            b'{"boolean": "true"}',
            b'{"error": "out of memory"}',
        ):
            with self.subTest(payload=payload):
                self.assertIsNotNone(self._problem(payload))

    def test_a_missing_result_file_is_a_problem(self):
        self.assertIn("unreadable", self._problem(None))

    def test_every_engine_envelope_applies_the_check(self):
        """QLever and the file engines share _envelope, so none can pass garbage."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            engine = V.build_engine(
                "qlever", tmp_path / "a.nt", raw_dir=tmp_path, scratch=tmp_path, options={},
            )
            raw = tmp_path / "q.sparql.json"
            raw.write_bytes(b'{"results": {"bindings": [')
            envelope = engine._envelope(
                "q", tmp_path / "q.rq", returncode=0, started=V.time.monotonic(),
                raw_path=raw, stderr_path=tmp_path / "q.stderr.txt",
            )
            self.assertEqual(envelope["status"], "EXECUTION_FAILED")
            self.assertEqual(envelope["exitCode"], 0)
            self.assertIn("not JSON", envelope["error"])

            raw.write_bytes(EMPTY_RESULT)
            envelope = engine._envelope(
                "q", tmp_path / "q.rq", returncode=0, started=V.time.monotonic(),
                raw_path=raw, stderr_path=tmp_path / "q.stderr.txt",
            )
            self.assertEqual(envelope["status"], "PASS")
            self.assertNotIn("error", envelope)


def bindings_for(value):
    def cell(item):
        return {key: {"value": str(val)} for key, val in item.items()}

    if isinstance(value, dict):
        return {"results": {"bindings": [cell(value)]}}
    return {"results": {"bindings": [cell(row) for row in value]}}


class RunValidationRecordsTheTimeoutTests(VerboseTestCase):
    """The incident end to end: run_validation, a real engine, a fake Comunica."""

    QUERIES = ("q04_filter_distribution", "q05_sample_genotype_counts", "q06_ac_an_distribution")

    def _args(self, tmp_path):
        return argparse.Namespace(
            results_dir=tmp_path / "results",
            representation="expanded",
            info_representation="structured",
            header_representation="structured",
            progress_path=tmp_path / ".progress" / "validation.jsonl",
            quiet=True,
            scratch_dir=tmp_path / "scratch",
            rdf=tmp_path / "sample.nt",
            rdf_format="nt",
            engine="comunica",
            engines=["comunica"],
            mapping_policy="strict",
            strict_conformance=False,
            shacl_shapes=None,
            query_timeout=3600,
            validation_time_budget=0,
            stop_after_query_timeout=False,
            qlever_memory_gb=4,
            qlever_port=7019,
            comunica_port=7020,
            hdt_port=7021,
            comunica_bind_timeout=60,
            comunica_warmup_timeout=60,
            qlever_startup_timeout=60,
            qlever_index_arg=[],
            qlever_server_arg=[],
            vcf=tmp_path / "sample.vcf",
            filter_oracle="cyvcf2",
            dataset_id="sample",
            queries=self.QUERIES,
            shacl_max_triples=V.DEFAULT_SHACL_MAX_TRIPLES,
            node_heap_mb=V.DEFAULT_NODE_HEAP_MB,
        )

    def test_a_timed_out_query_is_not_a_pass_and_the_next_query_still_runs(self):
        parser_summary = copy.deepcopy(fixtures.parser_summary("expanded"))
        parser_summary.setdefault("sourceSha256", "0" * 64)
        answers = {qid: bindings_for(parser_summary[qid]) for qid in self.QUERIES}
        fake = FakeComunica(timeouts={"q05_sample_genotype_counts"}, answers=answers)

        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            (tmp_path / "scratch").mkdir()
            (tmp_path / "sample.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            (tmp_path / "sample.nt").write_text("<s> <p> <o> .\n")
            queries = tmp_path / "queries"
            queries.mkdir()
            args = self._args(tmp_path)
            patches = fake.patches() + [
                mock.patch.object(V, "query_path",
                                  side_effect=lambda _dir, qid: query_file(queries, qid)),
                mock.patch.object(V, "parse_vcf", return_value=parser_summary),
                mock.patch.object(V, "attach_census_expectations",
                                  side_effect=lambda p, *a, **k: p),
                mock.patch.object(V, "validate_ntriples",
                                  return_value={"status": "PASS", "tripleCount": 1}),
                mock.patch.object(V, "materialize_ntriples",
                                  return_value=(tmp_path / "sample.nt", {})),
            ]
            for patch in patches:
                patch.start()
            try:
                rc = V.run_validation(args)
            finally:
                for patch in reversed(patches):
                    patch.stop()
            benchmark = json.loads((args.results_dir / "benchmark.json").read_text())
            executions = json.loads(
                (args.results_dir / "engines" / "comunica" / "query-executions.json").read_text()
            )

        per_query = benchmark["engines"]["comunica"]["queries"]
        self.assertEqual(per_query["q05_sample_genotype_counts"]["status"], "EXECUTION_FAILED")
        self.assertEqual(executions["q05_sample_genotype_counts"]["exitCode"], 124)
        self.assertEqual(per_query["q04_filter_distribution"]["status"], "PASS")
        self.assertEqual(per_query["q06_ac_an_distribution"]["status"], "PASS")
        self.assertIn("endpointRestartSeconds", executions["q06_ac_an_distribution"])
        self.assertEqual(fake.queries, list(self.QUERIES))
        self.assertNotEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
