"""Multi-engine selection, native artifact querying, and the benchmark report.

One validation run may answer the whole query set under several engines. These
tests cover the three things that makes possible: parsing the engine list, the
two native-artifact engines that query HDT and COTTAS without decoding them,
and the benchmark that turns the resulting timings into a comparison.
"""

import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import vcf_rdfizer
from test.helpers import VerboseTestCase

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_benchmark", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()


class EngineListTests(VerboseTestCase):
    def test_a_single_engine_still_parses_to_a_one_element_list(self):
        """The single-engine case is the multi-engine case with one entry."""
        self.assertEqual(V.parse_engine_list("comunica"), ["comunica"])
        self.assertEqual(vcf_rdfizer.parse_validation_engines("comunica"), ["comunica"])

    def test_several_engines_keep_the_order_they_were_requested_in(self):
        """The first engine is the primary, so order is not incidental."""
        self.assertEqual(
            V.parse_engine_list("qlever,comunica"), ["qlever", "comunica"]
        )
        self.assertEqual(
            vcf_rdfizer.parse_validation_engines("qlever,comunica"),
            ["qlever", "comunica"],
        )

    def test_repeats_and_surrounding_whitespace_are_absorbed(self):
        """'comunica, qlever, comunica' asks for two engines, not three."""
        self.assertEqual(
            V.parse_engine_list(" comunica , qlever , comunica "),
            ["comunica", "qlever"],
        )
        self.assertEqual(
            vcf_rdfizer.parse_validation_engines(" comunica , qlever , comunica "),
            ["comunica", "qlever"],
        )

    def test_all_expands_to_every_supported_engine(self):
        """The benchmarking shorthand must cover the full set, in both layers."""
        self.assertEqual(V.parse_engine_list("all"), list(V.SPARQL_ENGINES))
        self.assertEqual(
            vcf_rdfizer.parse_validation_engines("all"),
            list(vcf_rdfizer.VALIDATION_ENGINE_CHOICES),
        )

    def test_the_host_and_container_agree_on_the_engine_set(self):
        """A wrapper that offers an engine the runner rejects is unusable."""
        self.assertEqual(
            tuple(vcf_rdfizer.VALIDATION_ENGINE_CHOICES), tuple(V.SPARQL_ENGINES)
        )
        self.assertEqual(set(V.ENGINE_CLASSES), set(V.SPARQL_ENGINES))

    def test_an_unknown_or_empty_engine_is_rejected_by_both_layers(self):
        """Failing at parse time beats failing after materialization."""
        for raw in ("virtuoso", "comunica,virtuoso", "", "  ", ","):
            with self.assertRaises(ValueError, msg=raw):
                V.parse_engine_list(raw)
            with self.assertRaises(ValueError, msg=raw):
                vcf_rdfizer.parse_validation_engines(raw)

    def test_the_cli_resolves_engines_and_keeps_engine_as_the_primary(self):
        """Existing consumers read args.engine; it stays the first requested."""
        parser = V.build_arg_parser()
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            (tmp_path / "in.vcf").write_text("##fileformat=VCFv4.2\n", encoding="utf-8")
            (tmp_path / "in.nt").write_text("", encoding="utf-8")
            args = V.resolve_args(parser, [
                "--vcf", str(tmp_path / "in.vcf"),
                "--rdf", str(tmp_path / "in.nt"),
                "--results-dir", str(tmp_path / "out"),
                "--representation", "expanded",
                "--dataset-id", "fixture",
                "--scratch-dir", str(tmp_path),
                "--engine", "qlever,cottas",
            ])
        self.assertEqual(args.engines, ["qlever", "cottas"])
        self.assertEqual(args.engine, "qlever")


class NativeArtifactEngineTests(VerboseTestCase):
    """HDT and COTTAS are queried in place, not decoded to N-Triples first."""

    def _engine(self, name, tmp_path, options=None):
        return V.build_engine(
            name,
            tmp_path / "cohort.nt",
            raw_dir=tmp_path / "raw",
            scratch=tmp_path / "scratch",
            options=options or {},
        )

    def test_the_native_engines_declare_no_decode_in_their_description(self):
        """The mode string is what a reader of the report sees; it must be true."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            for name, artifact_format in (("hdt", "hdt"), ("cottas", "cottas")):
                engine = self._engine(name, tmp_path)
                self.assertIsInstance(engine, V.NativeArtifactEngine)
                self.assertEqual(engine.artifact_format, artifact_format)
                self.assertIn("no decode", engine.describe()["mode"])

    def test_a_matching_run_artifact_is_queried_directly(self):
        """Building a second copy would measure the build, not the artifact."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            supplied = tmp_path / "cohort.hdt"
            supplied.write_bytes(b"fake-hdt")
            engine = self._engine("hdt", tmp_path, options={
                "artifact_path": supplied, "artifact_format": "hdt",
            })
            with mock.patch.object(V.HdtEngine, "build_artifact") as build:
                resolved = engine._resolve_artifact()
            build.assert_not_called()
            self.assertEqual(resolved, supplied)
            self.assertEqual(engine.artifact_origin, "run artifact")

    def test_a_mismatched_run_artifact_makes_the_engine_build_its_own(self):
        """Validating a .nt aggregate under --engine hdt still needs an HDT."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            engine = self._engine("hdt", tmp_path, options={
                "artifact_path": tmp_path / "cohort.cottas", "artifact_format": "cottas",
            })
            with mock.patch.object(V.HdtEngine, "build_artifact") as build:
                resolved = engine._resolve_artifact()
            build.assert_called_once()
            self.assertEqual(resolved.suffix, ".hdt")
            self.assertEqual(resolved.parent, tmp_path / "scratch")
            self.assertEqual(engine.artifact_origin, "built from N-Triples for this engine")

    def test_hdt_queries_are_addressed_with_comunicas_typed_source_prefix(self):
        """A bare path is treated as a link to dereference and fails.

        Verified against comunica-sparql-hdt 5.0.1: '/tmp/g.hdt' reports
        "Could not dereference", while 'hdt@/tmp/g.hdt' answers the query.
        """
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            raw_dir = tmp_path / "raw"
            raw_dir.mkdir()
            query = tmp_path / "q.rq"
            query.write_text("SELECT * WHERE { ?s ?p ?o }", encoding="utf-8")
            engine = self._engine("hdt", tmp_path)
            engine.executable = "/usr/bin/comunica-sparql-hdt"
            engine.artifact = tmp_path / "cohort.hdt"
            recorded = {}

            def fake_run(command, **kwargs):
                recorded["command"] = command
                return mock.Mock(returncode=0)

            with mock.patch.object(V.subprocess, "run", side_effect=fake_run):
                envelope = engine.execute("q01", query)

        self.assertEqual(recorded["command"][1], f"hdt@{tmp_path / 'cohort.hdt'}")
        self.assertIn("application/sparql-results+json", recorded["command"])
        self.assertEqual(envelope["status"], "PASS")
        self.assertEqual(envelope["engine"], "hdt")

    def test_hdt_without_the_native_binary_explains_the_decode_alternative(self):
        """An unusable engine must say what the user can do instead."""
        with tempfile.TemporaryDirectory() as td:
            engine = self._engine("hdt", Path(td))
            with mock.patch.object(V.shutil, "which", return_value=None):
                with self.assertRaises(RuntimeError) as raised:
                    engine.start()
        self.assertIn("--engine comunica", str(raised.exception))

    def test_a_failing_cottas_query_is_reported_not_raised(self):
        """One bad query must not abort the other engines in a benchmark run."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            raw_dir = tmp_path / "raw"
            raw_dir.mkdir()
            query = tmp_path / "q.rq"
            query.write_text("SELECT * WHERE { ?s ?p ?o }", encoding="utf-8")
            engine = self._engine("cottas", tmp_path)
            engine.graph = mock.Mock()
            engine.graph.query.side_effect = RuntimeError("parquet is unreadable")
            envelope = engine.execute("q01", query)

            self.assertEqual(envelope["status"], "EXECUTION_FAILED")
            self.assertIn("parquet is unreadable", envelope["error"])
            self.assertEqual(json.loads(Path(envelope["rawResult"]).read_text()), {})
            self.assertIn(
                "parquet is unreadable",
                Path(envelope["stderr"]).read_text(encoding="utf-8"),
            )

    def test_an_unknown_engine_name_names_the_supported_ones(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(ValueError) as raised:
                self._engine("virtuoso", Path(td))
        for name in V.SPARQL_ENGINES:
            self.assertIn(name, str(raised.exception))


def _verdict(status, wall_seconds, sparql=None):
    return {
        "status": status,
        "executions": {
            query_id: {"status": "PASS", "wallSeconds": seconds}
            for query_id, seconds in wall_seconds.items()
        },
        "sparql": sparql if sparql is not None else {"q01": [{"n": 1}]},
    }


class BenchmarkReportTests(VerboseTestCase):
    def _benchmark(self):
        return V.build_benchmark(
            {
                "comunica": _verdict("PASS", {"q01": 1.0, "q02": 3.0}),
                "qlever": _verdict("PASS", {"q01": 0.01, "q02": 0.02}),
            },
            {
                "comunica": {"setupSeconds": 0.001, "artifactOrigin": None,
                             "artifactSizeBytes": None},
                "qlever": {"setupSeconds": 2.5, "artifactOrigin": None,
                           "artifactSizeBytes": None},
            },
            oracle_seconds={"total": 0.4, "parse": 0.3, "census": 0.1},
            materialization_seconds=0.7,
            shacl_seconds=None,
            query_ids=("q01", "q02"),
        )

    def test_query_time_is_summed_per_engine_and_the_slowest_named(self):
        """The point of the report is comparing engines, so both must be right."""
        benchmark = self._benchmark()
        self.assertAlmostEqual(benchmark["engines"]["comunica"]["querySeconds"], 4.0)
        self.assertAlmostEqual(benchmark["engines"]["qlever"]["querySeconds"], 0.03)
        self.assertEqual(benchmark["engines"]["comunica"]["slowestQuery"], "q02")

    def test_the_oracle_is_timed_so_sparql_can_be_compared_against_it(self):
        """The comparison the user asked for is engine versus parser."""
        benchmark = self._benchmark()
        self.assertEqual(benchmark["oracle"]["totalSeconds"], 0.4)
        self.assertEqual(benchmark["oracle"]["vcfParseSeconds"], 0.3)
        self.assertEqual(benchmark["oracle"]["censusSeconds"], 0.1)
        self.assertEqual(benchmark["totals"]["oracleSeconds"], 0.4)
        self.assertEqual(benchmark["preparation"]["materializationSeconds"], 0.7)

    def test_an_engine_with_no_timed_query_reports_none_not_zero(self):
        """Zero seconds would read as 'instant' rather than 'never ran'."""
        benchmark = V.build_benchmark(
            {"hdt": {"status": "EXECUTION_FAILED", "executions": {}, "sparql": {}}},
            {"hdt": {"setupSeconds": 0.1}},
            oracle_seconds=None,
            materialization_seconds=None,
            shacl_seconds=None,
            query_ids=("q01",),
        )
        self.assertIsNone(benchmark["engines"]["hdt"]["querySeconds"])
        self.assertIsNone(benchmark["engines"]["hdt"]["slowestQuery"])
        self.assertIsNone(benchmark["oracle"]["totalSeconds"])

    def test_the_csv_carries_one_row_per_engine_and_query(self):
        """A long-format CSV is directly usable in a plot without reshaping."""
        benchmark = self._benchmark()
        with tempfile.TemporaryDirectory() as td:
            path = V.write_benchmark_csv(Path(td) / "benchmark.csv", benchmark)
            rows = list(csv.DictReader(path.open(encoding="utf-8")))
        self.assertEqual(len(rows), 4)
        self.assertEqual(list(rows[0]), V.BENCHMARK_CSV_HEADER)
        self.assertEqual(
            {(row["engine"], row["query_id"]) for row in rows},
            {("comunica", "q01"), ("comunica", "q02"),
             ("qlever", "q01"), ("qlever", "q02")},
        )
        # Repeated on every row so the file needs no join to be usable.
        self.assertEqual({row["oracle_wall_seconds"] for row in rows}, {"0.4"})


class EngineAgreementTests(VerboseTestCase):
    def test_engines_returning_the_same_rows_agree(self):
        result = V.compare_engines({
            "comunica": _verdict("PASS", {"q01": 1.0}, sparql={"q01": [{"n": 2}]}),
            "qlever": _verdict("PASS", {"q01": 0.1}, sparql={"q01": [{"n": 2}]}),
        })
        self.assertTrue(result["agree"])
        self.assertEqual(result["differences"], {})
        self.assertEqual(result["comparedEngines"], ["comunica", "qlever"])

    def test_a_disagreement_names_the_engine_and_the_queries(self):
        """This is how QLever's literal canonicalisation would have been caught."""
        result = V.compare_engines({
            "comunica": _verdict("PASS", {"q01": 1.0}, sparql={"q01": [{"n": 2}], "q02": []}),
            "qlever": _verdict("PASS", {"q01": 0.1}, sparql={"q01": [{"n": 3}], "q02": []}),
        })
        self.assertFalse(result["agree"])
        self.assertEqual(result["differences"], {"qlever": ["q01"]})
        self.assertEqual(result["referenceEngine"], "comunica")

    def test_an_engine_that_failed_to_execute_is_left_out_of_the_comparison(self):
        """Comparing against an empty result set would be a false disagreement."""
        result = V.compare_engines({
            "comunica": _verdict("PASS", {"q01": 1.0}, sparql={"q01": [{"n": 2}]}),
            "hdt": {"status": "EXECUTION_FAILED", "executions": {}, "sparql": {}},
        })
        self.assertEqual(result["comparedEngines"], ["comunica"])
        self.assertTrue(result["agree"])
        self.assertIn("fewer than two", result["note"])


if __name__ == "__main__":
    unittest.main()
