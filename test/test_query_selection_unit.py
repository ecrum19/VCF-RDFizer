"""A subset run must be a measurement, never a validation verdict.

``--queries`` exists so retrieval cost can be measured one question at a time:
the thirteen core queries are what Figure 6 of the manuscript reports, while
the preflight set costs an order of magnitude more and appears nowhere in it.
Paying for the whole set to time thirteen queries is the thing this avoids.

The risk it introduces is that someone reads a subset run as a passed
validation. ``evaluate_validation`` decides PASS/MISMATCH from preflight
gating, the sample/GT inventory and invariants computed across every query, so
a subset cannot reach that decision honestly.

Three properties keep the two apart, and each is pinned below:

1. Selection resolves to canonical ids in canonical execution order.
2. The subset path compares each query EXACTLY as the verdict path does --
   `--queries` changes which queries are checked, never how one is checked.
3. The subset path reports per query and emits none of the keys a validation
   consumer reads, so misreading a subset as a pass fails loudly.

And one more, because a correct runner behind a wrapper that never passes the
option is worth nothing: the selection reaches the container.
"""

import argparse
import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from unittest import mock

import vcf_rdfizer
from test import validation_fixtures as fixtures
from test.helpers import VerboseTestCase

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = REPO_ROOT / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_selection", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()


def executions_from(rows_by_query, tmp_path):
    """Write each query's bindings to disk the way an engine would."""
    executions = {}
    for query_id, payload in rows_by_query.items():
        raw = Path(tmp_path) / f"{query_id}.json"
        raw.write_text(json.dumps(payload))
        executions[query_id] = {"status": "PASS", "rawResult": str(raw)}
    return executions


def bindings_for(value):
    """Turn a normalized oracle answer back into SPARQL JSON bindings.

    The runner's normalize() is the inverse of this, so round-tripping the
    oracle through it produces the graph result a correct conversion would
    have produced -- which is what lets these tests compare the two evaluation
    paths on the same realistic input rather than on hand-written rows.
    """
    def cell(item):
        return {key: {"value": str(val)} for key, val in item.items()}

    if isinstance(value, dict):
        return {"results": {"bindings": [cell(value)]}}
    return {"results": {"bindings": [cell(row) for row in value]}}


class QuerySelectionTests(VerboseTestCase):
    def test_groups_expand(self):
        self.assertEqual(V.parse_query_selection("core"), V.CORE_QUERIES)
        self.assertEqual(
            V.parse_query_selection("all"),
            V.PREFLIGHT_QUERIES + V.PREFLIGHT_COUNT_QUERIES + V.CORE_QUERIES,
        )
        self.assertEqual(
            V.parse_query_selection("preflight"),
            V.PREFLIGHT_QUERIES + V.PREFLIGHT_COUNT_QUERIES,
        )

    def test_single_query(self):
        self.assertEqual(V.parse_query_selection("q03_titv"), ("q03_titv",))

    def test_result_is_in_canonical_order_not_caller_order(self):
        # Execution order matters: an anomaly preflight's ``_count`` companion
        # is derived from the sample that must already have run, so honouring
        # the caller's order would break that derivation.
        self.assertEqual(
            V.parse_query_selection("q13_format_value_digest,q01_record_density_1mb"),
            ("q01_record_density_1mb", "q13_format_value_digest"),
        )

    def test_count_query_pulls_in_its_sample(self):
        # Asking for the count alone would otherwise re-scan the graph for a
        # number the sample query already carries.
        self.assertEqual(
            V.parse_query_selection("preflight_blank_nodes_count"),
            ("preflight_blank_nodes", "preflight_blank_nodes_count"),
        )

    def test_every_count_query_pulls_in_its_sample(self):
        for count_query in V.PREFLIGHT_COUNT_QUERIES:
            with self.subTest(count_query=count_query):
                selected = V.parse_query_selection(count_query)
                self.assertIn(count_query[: -len("_count")], selected)

    def test_mixed_groups_and_ids_deduplicate(self):
        self.assertEqual(V.parse_query_selection("core,q03_titv"), V.CORE_QUERIES)

    def test_unknown_query_is_rejected(self):
        with self.assertRaises(ValueError) as caught:
            V.parse_query_selection("q99_nonexistent")
        self.assertIn("q99_nonexistent", str(caught.exception))

    def test_empty_selection_is_rejected(self):
        for raw in ("", ",", "  ,  "):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    V.parse_query_selection(raw)

    def test_whitespace_is_tolerated(self):
        self.assertEqual(
            V.parse_query_selection(" q03_titv , q01_record_density_1mb "),
            ("q01_record_density_1mb", "q03_titv"),
        )

    def test_every_selectable_id_resolves_to_itself(self):
        """No id in the catalogue is unreachable through the parser."""
        for query_id in V.QUERY_GROUPS["all"]:
            with self.subTest(query_id=query_id):
                self.assertIn(query_id, V.parse_query_selection(query_id))


class TimingOnlyMatchesTheVerdictPathTests(VerboseTestCase):
    """The subset path must compare exactly as the full path does.

    This is the property the design rests on. ``--queries`` is only defensible
    if selecting a subset changes WHICH queries are checked and nothing about
    HOW each one is checked -- otherwise a timing run could report agreement
    the verdict path would have called a mismatch, and the manuscript's
    "equality established before any timing was compared" would not hold for
    these runs.
    """

    def _agreeing(self, tmp_path, queries, parser=None):
        parser = parser or copy.deepcopy(fixtures.parser_summary("expanded"))
        payloads = {q: bindings_for(parser[q]) for q in queries}
        return parser, executions_from(payloads, tmp_path)

    def test_full_selection_reproduces_compare_query_for_query(self):
        with tempfile.TemporaryDirectory() as td:
            parser, executions = self._agreeing(td, V.CORE_QUERIES)
            timing = V.evaluate_timing_only(
                executions, parser, "expanded", selected=V.CORE_QUERIES
            )
            sparql = {q: V.normalize(q, Path(executions[q]["rawResult"]))
                      for q in V.CORE_QUERIES}
            verdict = V.compare(copy.deepcopy(parser), sparql)

        self.assertEqual(sorted(timing["comparedQueries"]), sorted(verdict["queries"]))
        for query_id in V.CORE_QUERIES:
            with self.subTest(query_id=query_id):
                self.assertEqual(
                    timing["comparedQueries"][query_id]["status"],
                    verdict["queries"][query_id]["status"],
                    f"{query_id} was judged differently by the two paths",
                )

    def test_a_mismatch_is_judged_the_same_way_by_both_paths(self):
        with tempfile.TemporaryDirectory() as td:
            parser = copy.deepcopy(fixtures.parser_summary("expanded"))
            broken = copy.deepcopy(parser)
            broken["q03_titv"] = dict(broken["q03_titv"])
            broken["q03_titv"]["transitionCount"] += 1
            executions = executions_from(
                {q: bindings_for(broken[q]) for q in V.CORE_QUERIES}, td)

            timing = V.evaluate_timing_only(
                executions, parser, "expanded", selected=V.CORE_QUERIES)
            sparql = {q: V.normalize(q, Path(executions[q]["rawResult"]))
                      for q in V.CORE_QUERIES}
            verdict = V.compare(copy.deepcopy(parser), sparql)

        self.assertFalse(timing["answersAgree"])
        self.assertNotEqual(verdict["status"], "PASS")
        self.assertEqual(
            timing["comparedQueries"]["q03_titv"]["status"],
            verdict["queries"]["q03_titv"]["status"],
        )

    def test_selecting_one_query_judges_it_as_the_full_run_would(self):
        """Isolation must not change a verdict on the query in isolation."""
        for query_id in ("q01_record_density_1mb", "q11_record_digest",
                         "q12_info_value_digest"):
            with self.subTest(query_id=query_id), tempfile.TemporaryDirectory() as td:
                parser, executions = self._agreeing(td, (query_id,))
                alone = V.evaluate_timing_only(
                    executions, parser, "expanded", selected=(query_id,))
                self.assertEqual(
                    alone["comparedQueries"][query_id]["status"], "PASS")
                self.assertTrue(alone["answersAgree"])


class TimingOnlyEdgeCaseTests(VerboseTestCase):
    def test_a_normalization_failure_is_recorded_and_fails_the_run(self):
        """An unreadable result is not an absent one; it must not read as agreement."""
        with tempfile.TemporaryDirectory() as td:
            raw = Path(td) / "broken.json"
            raw.write_text("{ this is not json")
            executions = {"q03_titv": {"status": "PASS", "rawResult": str(raw)}}
            result = V.evaluate_timing_only(
                executions, copy.deepcopy(fixtures.parser_summary("expanded")),
                "expanded", selected=("q03_titv",))
        self.assertIn("q03_titv", result["normalizationFailures"])
        self.assertFalse(result["answersAgree"])
        self.assertEqual(result["comparedQueries"], {})

    def test_a_missing_execution_is_a_failure_not_a_silent_skip(self):
        """A query that never ran must not quietly count as agreement."""
        result = V.evaluate_timing_only(
            {}, copy.deepcopy(fixtures.parser_summary("expanded")),
            "expanded", selected=("q03_titv",))
        self.assertIn("q03_titv", result["normalizationFailures"])
        self.assertFalse(result["answersAgree"])

    def test_preflight_only_selection_carries_no_compared_queries(self):
        """Selecting preflights alone is legal and compares nothing.

        Preflights are judged against the graph, not against the parser, so
        there is no oracle comparison to make. The run still reports its
        executions so a timing stays attributable, and ``answersAgree`` stays
        true because nothing disagreed -- there was nothing to disagree.
        """
        result = V.evaluate_timing_only(
            {"preflight_blank_nodes": {"status": "PASS", "rawResult": "unused"}},
            copy.deepcopy(fixtures.parser_summary("expanded")),
            "expanded", selected=("preflight_blank_nodes",))
        self.assertEqual(result["comparedQueries"], {})
        self.assertTrue(result["answersAgree"])
        self.assertEqual(
            result["preflightExecutions"],
            {"preflight_blank_nodes": {"status": "PASS"}})

    def test_sample_queries_are_not_applicable_without_samples_or_gt(self):
        """The carve-out evaluate_validation applies, applied here too.

        Without samples or a GT column these two have nothing to compare, and
        that is a verified not-applicable rather than a mismatch. If the subset
        path missed this, a sample-free input would look like a failure.
        """
        parser = copy.deepcopy(fixtures.parser_summary("expanded"))
        parser["sampleCount"] = 0
        parser["gtRecordCount"] = 0
        wanted = ("q05_sample_genotype_counts", "q06_ac_an_distribution")
        with tempfile.TemporaryDirectory() as td:
            executions = executions_from(
                {q: bindings_for(parser[q]) for q in wanted}, td)
            result = V.evaluate_timing_only(executions, parser, "expanded",
                                            selected=wanted)
        for query_id in wanted:
            with self.subTest(query_id=query_id):
                self.assertEqual(result["comparedQueries"][query_id]["status"],
                                 "NOT_APPLICABLE_VERIFIED_NO_SAMPLES_OR_GT")
        self.assertTrue(result["answersAgree"])

    def test_the_verdict_keys_are_absent_so_a_subset_cannot_read_as_a_pass(self):
        """The safety property, stated as a test rather than only as a comment."""
        with tempfile.TemporaryDirectory() as td:
            parser = copy.deepcopy(fixtures.parser_summary("expanded"))
            executions = executions_from(
                {"q03_titv": bindings_for(parser["q03_titv"])}, td)
            result = V.evaluate_timing_only(executions, parser, "expanded",
                                            selected=("q03_titv",))
        for key in ("comparison", "preflight", "comparisonStatus"):
            with self.subTest(key=key):
                self.assertNotIn(key, result)
        self.assertEqual(result["status"], "TIMING_ONLY")
        self.assertIn("not-evaluated", result["verdict"])


class ManifestRecordsOnlyWhatRanTests(VerboseTestCase):
    """A subset run's manifest must not claim evidence the run never produced."""

    def _manifest_for(self, query_ids):
        with tempfile.TemporaryDirectory() as td:
            rdf = Path(td) / "graph.nt"
            rdf.write_bytes(b"<s> <p> <o> .\n")
            args = argparse.Namespace(
                dataset_id="d", representation="expanded",
                vcf=Path(td) / "x.vcf", rdf=rdf, rdf_format="nt")
            return V.build_manifest(
                args, V.QUERY_ROOT / "expanded", {"sourceSha256": "abc"},
                engine_description={}, materialization={}, query_ids=query_ids)

    def test_only_the_selected_queries_are_digested(self):
        manifest = self._manifest_for(("q03_titv",))
        self.assertEqual(list(manifest["queries"]), ["q03_titv"])

    def test_omitting_the_argument_digests_the_whole_catalogue(self):
        """Default behaviour is unchanged for a full run."""
        manifest = self._manifest_for(None)
        self.assertGreater(len(manifest["queries"]), len(V.CORE_QUERIES))


class WrapperForwardsTheSelectionTests(VerboseTestCase):
    """The flag is useless if it stops at the container boundary.

    A wiring gap of exactly this shape -- runner correct, wrapper never passing
    the option -- once made every raw-INFO conversion validate against a
    structured oracle. That was caught by a benchmark campaign rather than by a
    test, which is why this one exists.
    """

    def _command_for(self, engine_options):
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            vcf_path = tmp_path / "cohort.vcf"
            vcf_path.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n", encoding="utf-8")
            rdf_path = tmp_path / "cohort.nt"
            rdf_path.write_bytes(b"<s> <p> <o> .\n")
            commands = []
            with mock.patch.object(
                vcf_rdfizer, "run",
                side_effect=lambda cmd, **kw: commands.append(cmd) or 0,
            ):
                vcf_rdfizer.run_validation_mode(
                    vcf_path=vcf_path, rdf_path=rdf_path,
                    representation="expanded",
                    info_representation="structured",
                    header_representation="structured",
                    validation_id="cohort",
                    results_dir=tmp_path / "results",
                    metrics_dir=tmp_path / "metrics",
                    run_id="RID", timestamp="TS",
                    image_ref="example/vcf-rdfizer:latest",
                    filter_oracle="auto", engine="qlever",
                    engine_options=engine_options,
                    wrapper_log_path=tmp_path / "wrapper.log")
            return commands[0]

    def test_the_selection_reaches_the_runner(self):
        command = self._command_for({"queries": "core"})
        self.assertIn("--queries", command)
        self.assertEqual(command[command.index("--queries") + 1], "core")

    def test_an_explicit_list_is_forwarded_verbatim(self):
        command = self._command_for({"queries": "q03_titv,q11_record_digest"})
        self.assertEqual(command[command.index("--queries") + 1],
                         "q03_titv,q11_record_digest")

    def test_no_selection_means_no_flag(self):
        """Absent must mean absent: a full run must not become a subset run."""
        self.assertNotIn("--queries", self._command_for({}))


class WrapperCliSurfaceTests(VerboseTestCase):
    def test_validation_queries_is_a_registered_option(self):
        """The parser is built inside main(), so ask the CLI itself."""
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "vcf_rdfizer.py"), "--help"],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0)
        self.assertIn("--validation-queries", result.stdout)


if __name__ == "__main__":
    unittest.main()


class RunValidationHonoursTheSelectionTests(VerboseTestCase):
    """Drive run_validation() itself, with the engine and oracle faked.

    The unit tests above prove the pieces. This proves the wiring inside
    run_validation: that the selection actually shortens the executed query
    list, that a subset lands on the TIMING_ONLY summary rather than the
    verdict summary, and that the exit code still reflects agreement.
    """

    def _args(self, tmp_path, queries):
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
            query_timeout=60,
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
            queries=queries,
        )

    def _run(self, queries, *, corrupt=None):
        """Run the validator against a graph that agrees with the oracle."""
        parser_summary = copy.deepcopy(fixtures.parser_summary("expanded"))
        # build_manifest reads this off the oracle; the shared fixture is a
        # census rather than a full parse result, so it does not carry one.
        parser_summary.setdefault("sourceSha256", "0" * 64)
        answers = copy.deepcopy(parser_summary)
        if corrupt:
            answers[corrupt] = dict(answers[corrupt])
            key = next(k for k, v in answers[corrupt].items() if isinstance(v, int))
            answers[corrupt][key] += 1

        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            (tmp_path / "scratch").mkdir()
            (tmp_path / "sample.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            (tmp_path / "sample.nt").write_text("<s> <p> <o> .\n")
            raw_dir = tmp_path / "raw"
            raw_dir.mkdir()
            query_file = tmp_path / "query.rq"
            query_file.write_text("SELECT * WHERE { ?s ?p ?o }\n")

            executed = []

            def execute(query_id, _path):
                executed.append(query_id)
                raw = raw_dir / f"{query_id}.json"
                if query_id in answers:
                    raw.write_text(json.dumps(bindings_for(answers[query_id])))
                else:
                    # Preflights return no anomalies on a healthy graph.
                    raw.write_text(json.dumps({"results": {"bindings": []}}))
                return {"status": "PASS", "rawResult": str(raw), "wallSeconds": 0.01}

            engine = mock.MagicMock()
            engine.describe.return_value = {"engine": "comunica", "setupSeconds": 0.1}
            engine.execute.side_effect = execute
            engine.query_timeout = 60

            args = self._args(tmp_path, queries)
            with mock.patch.object(V, "query_path", return_value=query_file), \
                    mock.patch.object(V, "parse_vcf", return_value=parser_summary), \
                    mock.patch.object(V, "attach_census_expectations",
                                      side_effect=lambda p, *a, **k: p), \
                    mock.patch.object(V, "validate_ntriples",
                                      return_value={"status": "PASS", "tripleCount": 1}), \
                    mock.patch.object(V, "materialize_ntriples",
                                      return_value=(tmp_path / "sample.nt", {})), \
                    mock.patch.object(V, "build_engine", return_value=engine):
                rc = V.run_validation(args)
            summary = json.loads((args.results_dir / "summary.json").read_text())
        return rc, executed, summary

    def test_a_subset_runs_only_the_selected_queries(self):
        _rc, executed, _summary = self._run(("q03_titv",))
        self.assertEqual(executed, ["q03_titv"])

    def test_a_subset_reports_timing_only_and_not_a_verdict(self):
        rc, _executed, summary = self._run(("q03_titv",))
        self.assertEqual(summary["status"], "TIMING_ONLY")
        self.assertEqual(summary["selectedQueries"], ["q03_titv"])
        self.assertTrue(summary["answersAgree"])
        self.assertEqual(rc, 0)
        # The keys a validation consumer reads must not be here, so reading a
        # subset run as a pass fails rather than succeeding wrongly.
        self.assertNotIn("comparisonStatus", summary)
        self.assertNotIn("preflight", summary)

    def test_a_disagreeing_subset_exits_non_zero(self):
        """A fast wrong answer is not a result, verdict or no verdict."""
        rc, _executed, summary = self._run(("q03_titv",), corrupt="q03_titv")
        self.assertEqual(rc, 1)
        self.assertFalse(summary["answersAgree"])
        self.assertIn("q03_titv", summary["disagreeingQueries"])

    def test_several_selected_queries_run_in_canonical_order(self):
        selected = V.parse_query_selection("q11_record_digest,q01_record_density_1mb")
        _rc, executed, _summary = self._run(selected)
        self.assertEqual(executed, ["q01_record_density_1mb", "q11_record_digest"])

    def test_no_selection_still_produces_a_full_verdict(self):
        """The default path must be untouched by this feature."""
        _rc, executed, summary = self._run(None)
        self.assertNotEqual(summary["status"], "TIMING_ONLY")
        self.assertGreater(len(executed), len(V.CORE_QUERIES))
        self.assertIn("comparisonStatus", summary)


class WrapperCliWiringTests(VerboseTestCase):
    """The CLI flag must reach engine_options, not just exist.

    This is the gap the other tests leave. ``run_validation_mode`` is called
    directly above with engine_options already built, which proves the
    container boundary but says nothing about whether ``--validation-queries``
    ever populates it. That wiring lives in main(), and a flag that parses but
    is never read is exactly the failure this project has seen before: the
    runner honoured --info-representation, the wrapper never sent it, and every
    raw-INFO conversion silently validated against a structured oracle.
    """

    def _engine_options_for(self, extra_argv):
        """Run main() far enough to build engine_options, then capture them."""
        captured = {}

        def capture(**kwargs):
            captured.update(kwargs)
            return 0

        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            vcf_path = tmp_path / "sample.vcf"
            vcf_path.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            rdf_path = tmp_path / "sample.nt"
            rdf_path.write_text("<s> <p> <o> .\n")
            argv = [
                "vcf_rdfizer.py", "--mode", "validation",
                "--input", str(vcf_path), "--rdf", str(rdf_path),
                "--out", str(tmp_path / "out"),
                # --no-build so the wrapper does not try to build an image on
                # its way to the call this test is actually about.
                "--image", "example/vcf-rdfizer:test", "--no-build",
                *extra_argv,
            ]
            # check_docker gates the mode before the call we care about; the
            # container itself is irrelevant to whether the flag was wired.
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(vcf_rdfizer, "check_docker",
                                      return_value=True), \
                    mock.patch.object(vcf_rdfizer, "docker_image_exists",
                                      return_value=True), \
                    mock.patch.object(vcf_rdfizer, "run_validation_mode",
                                      side_effect=capture):
                vcf_rdfizer.main()
        return captured.get("engine_options", {})

    def test_the_flag_populates_engine_options(self):
        options = self._engine_options_for(["--validation-queries", "core"])
        self.assertEqual(options.get("queries"), "core")

    def test_an_explicit_list_survives_the_wiring(self):
        options = self._engine_options_for(
            ["--validation-queries", "q03_titv,q11_record_digest"])
        self.assertEqual(options.get("queries"), "q03_titv,q11_record_digest")

    def test_without_the_flag_no_selection_is_recorded(self):
        """Absent must stay absent all the way through, so the run stays full."""
        self.assertNotIn("queries", self._engine_options_for([]))


class RunnerArgParsingTests(VerboseTestCase):
    """parse_args resolves the selection, and rejects a bad one cleanly.

    A typo in a query name is a user error, so it must produce argparse's usual
    exit-2 message rather than a traceback from inside the run.
    """

    def _parse(self, extra_argv):
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            vcf_path = tmp_path / "sample.vcf"
            vcf_path.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            rdf_path = tmp_path / "sample.nt"
            rdf_path.write_text("<s> <p> <o> .\n")
            argv = [
                "validation_runner.py",
                "--vcf", str(vcf_path),
                "--rdf", str(rdf_path),
                "--representation", "expanded",
                "--results-dir", str(tmp_path / "results"),
                "--dataset-id", "sample",
                # Defaults to the container's /work, which does not exist here.
                # Without this the parser exits 2 for the wrong reason, which
                # would make the rejection test below pass without testing
                # anything.
                "--scratch-dir", str(tmp_path),
                *extra_argv,
            ]
            with mock.patch.object(sys, "argv", argv):
                return V.parse_args()

    def test_a_group_is_resolved_to_canonical_ids(self):
        args = self._parse(["--queries", "core"])
        self.assertEqual(args.queries, V.CORE_QUERIES)

    def test_no_flag_leaves_the_selection_unset(self):
        """None is what run_validation reads as 'run everything'."""
        self.assertIsNone(self._parse([]).queries)

    def test_an_unknown_query_exits_two_rather_than_raising(self):
        stderr = StringIO()
        with self.assertRaises(SystemExit) as caught, redirect_stderr(stderr):
            self._parse(["--queries", "q99_nonexistent"])
        self.assertEqual(caught.exception.code, 2)
        # Assert on the message too: an exit 2 from some unrelated argument
        # would otherwise make this test pass without exercising the rejection.
        self.assertIn("q99_nonexistent", stderr.getvalue())
