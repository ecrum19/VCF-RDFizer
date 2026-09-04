"""Engine selection and artifact materialization for semantic validation.

The container tools (hdt2rdf, pycottas, comunica, QLever) are replaced with
fakes, so these tests verify the command lines and control flow the validator
builds rather than the third-party tools themselves.
"""

import gzip
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import vcf_rdfizer
from test.helpers import VerboseTestCase

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_engines", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()
TRIPLES = b"<s1> <p> <o1> .\n<s2> <p> <o2> .\n"


class ArtifactFormatTests(VerboseTestCase):
    def test_every_pipeline_artifact_suffix_is_recognised(self):
        """Format detection covers each artifact the pipeline can produce."""
        expected = {
            "cohort.nt": "nt",
            "cohort.nt.gz": "nt.gz",
            "cohort.nt.br": "nt.br",
            "cohort.hdt": "hdt",
            "cohort.cottas": "cottas",
            "cohort.cottas.gz": "cottas.gz",
            "cohort.cottas.br": "cottas.br",
        }
        for name, fmt in expected.items():
            self.assertEqual(V.detect_rdf_format(Path(name)), fmt, name)
        self.assertIsNone(V.detect_rdf_format(Path("cohort.ttl")))

    def test_wrapper_and_runner_agree_on_formats(self):
        """The host and container must not disagree about what is supported."""
        wrapper = {fmt for _suffix, fmt in vcf_rdfizer.VALIDATION_RDF_SUFFIXES}
        self.assertEqual(wrapper, set(V.RDF_FORMATS))
        for name in ("a.nt", "a.nt.gz", "a.nt.br", "a.hdt", "a.cottas", "a.cottas.gz", "a.cottas.br"):
            self.assertEqual(
                vcf_rdfizer.detect_validation_rdf_format(Path(name)),
                V.detect_rdf_format(Path(name)),
                name,
            )

    def test_plain_ntriples_is_read_in_place(self):
        """A .nt source is never copied into scratch."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.nt"
            source.write_bytes(TRIPLES)
            scratch = tmp_path / "scratch"
            scratch.mkdir()
            decoded, provenance = V.materialize_ntriples(
                source, "nt", scratch, log_dir=tmp_path / "log"
            )
            self.assertEqual(decoded, source)
            self.assertFalse(provenance["materialized"])
            self.assertEqual(list(scratch.iterdir()), [])

    def test_gzip_is_expanded_into_scratch(self):
        """A .nt.gz source is decoded to scratch and reports its provenance."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.nt.gz"
            with gzip.open(source, "wb") as handle:
                handle.write(TRIPLES)
            scratch = tmp_path / "scratch"
            scratch.mkdir()
            decoded, provenance = V.materialize_ntriples(
                source, "nt.gz", scratch, log_dir=tmp_path / "log"
            )
            self.assertEqual(decoded.read_bytes(), TRIPLES)
            self.assertTrue(provenance["materialized"])
            self.assertEqual(provenance["sourceFormat"], "nt.gz")

    def test_hdt_is_decoded_with_hdt2rdf(self):
        """An .hdt source is decoded by the container's hdt2rdf binary."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.hdt"
            source.write_bytes(b"fake-hdt")
            scratch = tmp_path / "scratch"
            scratch.mkdir()
            calls = []

            def fake_run(command, **kwargs):
                calls.append(command)
                Path(command[2]).write_bytes(TRIPLES)
                return subprocess.CompletedProcess(command, 0)

            with mock.patch.object(V, "_resolve_binary", return_value="/usr/local/bin/hdt2rdf"), \
                    mock.patch.object(V.subprocess, "run", side_effect=fake_run):
                decoded, provenance = V.materialize_ntriples(
                    source, "hdt", scratch, log_dir=tmp_path / "log"
                )
            self.assertEqual(calls[0][0], "/usr/local/bin/hdt2rdf")
            self.assertEqual(calls[0][1], str(source))
            self.assertEqual(decoded.read_bytes(), TRIPLES)
            self.assertTrue(provenance["materialized"])

    def test_cottas_is_decoded_with_the_cottas_tool(self):
        """A .cottas source is decoded through cottas_tool.py decompress."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.cottas"
            source.write_bytes(b"fake-cottas")
            scratch = tmp_path / "scratch"
            scratch.mkdir()
            calls = []

            def fake_run(command, **kwargs):
                calls.append(command)
                Path(command[-1]).write_bytes(TRIPLES)
                return subprocess.CompletedProcess(command, 0)

            with mock.patch.dict(os.environ, {"COTTAS_PYTHON_BIN": "/opt/py/bin/python"}), \
                    mock.patch.object(V.subprocess, "run", side_effect=fake_run):
                decoded, _ = V.materialize_ntriples(
                    source, "cottas", scratch, log_dir=tmp_path / "log"
                )
            self.assertEqual(calls[0][0], "/opt/py/bin/python")
            self.assertIn("decompress", calls[0])
            self.assertEqual(decoded.read_bytes(), TRIPLES)

    def test_packaged_cottas_is_unwrapped_then_decoded(self):
        """A .cottas.gz is gunzipped before pycottas, and the unwrap is cleaned up."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.cottas.gz"
            with gzip.open(source, "wb") as handle:
                handle.write(b"fake-cottas")
            scratch = tmp_path / "scratch"
            scratch.mkdir()

            def fake_run(command, **kwargs):
                self.assertTrue(Path(command[-2]).is_file(), "unwrapped input must exist")
                Path(command[-1]).write_bytes(TRIPLES)
                return subprocess.CompletedProcess(command, 0)

            with mock.patch.object(V.subprocess, "run", side_effect=fake_run):
                decoded, provenance = V.materialize_ntriples(
                    source, "cottas.gz", scratch, log_dir=tmp_path / "log"
                )
            self.assertEqual(decoded.read_bytes(), TRIPLES)
            self.assertFalse((scratch / "input.cottas").exists())
            self.assertEqual(len(provenance["steps"]), 2)

    def test_a_failed_decode_reports_the_tool_output(self):
        """A decode failure raises with the tool's own output, not a bare code."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.hdt"
            source.write_bytes(b"broken")
            scratch = tmp_path / "scratch"
            scratch.mkdir()

            def fake_run(command, stdout=None, **kwargs):
                stdout.write(b"hdt2rdf: corrupt header\n")
                return subprocess.CompletedProcess(command, 3)

            with mock.patch.object(V, "_resolve_binary", return_value="hdt2rdf"), \
                    mock.patch.object(V.subprocess, "run", side_effect=fake_run):
                with self.assertRaises(RuntimeError) as caught:
                    V.materialize_ntriples(source, "hdt", scratch, log_dir=tmp_path / "log")
            self.assertIn("corrupt header", str(caught.exception))


class EngineTests(VerboseTestCase):
    def test_engine_registry_matches_the_wrapper_choices(self):
        """The host CLI cannot offer an engine the container does not implement."""
        self.assertEqual(
            set(vcf_rdfizer.VALIDATION_ENGINE_CHOICES), set(V.ENGINE_CLASSES)
        )
        self.assertEqual(set(V.SPARQL_ENGINES), set(V.ENGINE_CLASSES))

    def test_unknown_engine_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(ValueError):
                V.build_engine(
                    "sparqlmagic", Path(td) / "a.nt",
                    raw_dir=Path(td), scratch=Path(td), options={},
                )

    def test_comunica_engine_builds_the_expected_command(self):
        """The default engine still queries the file directly."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.nt"
            source.write_bytes(TRIPLES)
            query = tmp_path / "q.rq"
            query.write_text("SELECT * WHERE { ?s ?p ?o }\n", encoding="utf-8")
            engine = V.build_engine(
                "comunica", source, raw_dir=tmp_path, scratch=tmp_path, options={}
            )
            captured = []

            def fake_run(command, **kwargs):
                captured.append(command)
                return subprocess.CompletedProcess(command, 0)

            with mock.patch.object(V.shutil, "which", return_value="/usr/bin/comunica-sparql-file"), \
                    mock.patch.object(V, "tool_version", return_value=None), \
                    mock.patch.object(V.subprocess, "run", side_effect=fake_run):
                engine.start()
                result = engine.execute("q02_variant_shape_counts", query)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["engine"], "comunica")
            self.assertIn(str(source), captured[0])
            self.assertIn("application/sparql-results+json", captured[0])

    def test_comunica_reports_a_missing_binary_clearly(self):
        """An image without Comunica must say so, not fail obscurely."""
        with tempfile.TemporaryDirectory() as td:
            engine = V.build_engine(
                "comunica", Path(td) / "a.nt", raw_dir=Path(td), scratch=Path(td), options={}
            )
            with mock.patch.object(V.shutil, "which", return_value=None):
                with self.assertRaises(RuntimeError) as caught:
                    engine.start()
            self.assertIn("qlever", str(caught.exception))

    def test_qlever_builds_an_index_then_serves_it(self):
        """QLever indexes into scratch, waits for readiness, and answers over HTTP."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.nt"
            source.write_bytes(TRIPLES)
            query = tmp_path / "q.rq"
            query.write_text("SELECT * WHERE { ?s ?p ?o }\n", encoding="utf-8")
            engine = V.build_engine(
                "qlever", source, raw_dir=tmp_path, scratch=tmp_path,
                options={"memory_gb": 8, "port": 7031},
            )
            steps = []
            server = mock.MagicMock()
            server.poll.return_value = None

            with mock.patch.object(V, "_resolve_binary", side_effect=lambda env, *names: names[0]), \
                    mock.patch.object(V, "_run_step", side_effect=lambda cmd, **kw: steps.append(cmd)), \
                    mock.patch.object(V.subprocess, "Popen", return_value=server), \
                    mock.patch.object(V, "tool_version", return_value="QLever 1.0"), \
                    mock.patch.object(V.QleverEngine, "_post", return_value=b'{"results":{"bindings":[]}}'):
                engine.start()
                result = engine.execute("q03_titv", query)
                description = engine.describe()
                engine.stop()

            # Flags verified against qlever-index/qlever-server bfd5741.
            self.assertEqual(steps[0][0], f"{V.QLEVER_BIN_DIR}/qlever-index")
            self.assertIn("-f", steps[0])
            self.assertIn(str(source), steps[0])
            self.assertEqual(steps[0][steps[0].index("-F") + 1], "nt")
            self.assertEqual(steps[0][steps[0].index("-m") + 1], "8G")
            server_argv = engine.commands["server"]
            self.assertEqual(server_argv[0], f"{V.QLEVER_BIN_DIR}/qlever-server")
            # qlever-server defaults to a 30s query timeout, far below what the
            # aggregate queries need, so it must be set explicitly.
            self.assertIn("-s", server_argv)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["engine"], "qlever")
            self.assertEqual(json.loads(Path(result["rawResult"]).read_bytes())["results"]["bindings"], [])
            self.assertEqual(description["port"], 7031)
            self.assertIn("index", description["commands"])
            server.terminate.assert_called_once()
            self.assertFalse(engine.index_dir.exists(), "index must not outlive the engine")

    def test_qlever_surfaces_a_server_that_died_during_startup(self):
        """A crashed server is reported with its log tail, not a silent timeout."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "cohort.nt"
            source.write_bytes(TRIPLES)
            engine = V.build_engine(
                "qlever", source, raw_dir=tmp_path, scratch=tmp_path,
                options={"startup_timeout": 5},
            )
            server = mock.MagicMock()
            server.poll.return_value = 1
            server.returncode = 1

            def fake_popen(command, stdout=None, **kwargs):
                stdout.write(b"ServerMain: index not found\n")
                stdout.flush()
                return server

            with mock.patch.object(V, "_resolve_binary", side_effect=lambda env, *names: names[0]), \
                    mock.patch.object(V, "_run_step"), \
                    mock.patch.object(V.subprocess, "Popen", side_effect=fake_popen):
                with self.assertRaises(RuntimeError) as caught:
                    engine.start()
            self.assertIn("index not found", str(caught.exception))

    def test_qlever_command_lines_are_overridable(self):
        """QLever's CLI varies by release, so both argv are user-overridable."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            engine = V.build_engine(
                "qlever", tmp_path / "cohort.nt", raw_dir=tmp_path, scratch=tmp_path,
                options={"memory_gb": 2, "extra_index_args": ["--parser-buffer-size", "20"]},
            )
            self.assertIn("--parser-buffer-size", engine.index_command("qlever-index"))
            with mock.patch.dict(
                os.environ,
                {"QLEVER_INDEX_COMMAND": "qlever index -b {index} -i {input} -m {memory}"},
            ):
                argv = engine.index_command("qlever-index")
            self.assertEqual(argv[:3], ["qlever", "index", "-b"])
            self.assertIn(str(tmp_path / "cohort.nt"), argv)
            self.assertIn("2G", argv)


class RunnerArgumentTests(VerboseTestCase):
    def _resolve(self, argv):
        return V.resolve_args(V.build_arg_parser(), argv)

    def _base_args(self, tmp_path: Path, rdf_name: str) -> list[str]:
        vcf = tmp_path / "cohort.vcf"
        vcf.write_text("##fileformat=VCFv4.2\n", encoding="utf-8")
        rdf = tmp_path / rdf_name
        rdf.write_bytes(TRIPLES)
        return [
            "--vcf", str(vcf),
            "--rdf", str(rdf),
            "--representation", "expanded",
            "--results-dir", str(tmp_path / "results"),
            "--dataset-id", "cohort",
            "--scratch-dir", str(tmp_path),
        ]

    def test_format_is_inferred_from_the_filename(self):
        with tempfile.TemporaryDirectory() as td:
            args = self._resolve(self._base_args(Path(td), "cohort.hdt"))
            self.assertEqual(args.rdf_format, "hdt")
            self.assertEqual(args.engine, "comunica")

    def test_explicit_format_overrides_detection(self):
        with tempfile.TemporaryDirectory() as td:
            argv = self._base_args(Path(td), "cohort.bin") + ["--rdf-format", "nt"]
            self.assertEqual(self._resolve(argv).rdf_format, "nt")

    def test_unrecognised_extension_is_rejected_with_guidance(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(SystemExit):
                self._resolve(self._base_args(Path(td), "cohort.bin"))

    def test_deprecated_aliases_still_work(self):
        """Existing callers that pass --rdf-nt/--rdf-gz keep working."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            vcf = tmp_path / "cohort.vcf"
            vcf.write_text("##fileformat=VCFv4.2\n", encoding="utf-8")
            rdf = tmp_path / "cohort.nt.gz"
            with gzip.open(rdf, "wb") as handle:
                handle.write(TRIPLES)
            args = self._resolve([
                "--vcf", str(vcf),
                "--rdf-gz", str(rdf),
                "--representation", "condensed",
                "--results-dir", str(tmp_path / "results"),
                "--dataset-id", "cohort",
                "--scratch-dir", str(tmp_path),
            ])
            self.assertEqual(args.rdf, rdf.resolve())
            self.assertEqual(args.rdf_format, "nt.gz")

    def test_engine_tuning_is_validated(self):
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            argv = self._base_args(tmp_path, "cohort.nt") + [
                "--engine", "qlever", "--qlever-memory-gb", "16", "--qlever-port", "7040",
            ]
            args = self._resolve(argv)
            self.assertEqual((args.engine, args.qlever_memory_gb, args.qlever_port),
                             ("qlever", 16, 7040))
            with self.assertRaises(SystemExit):
                self._resolve(self._base_args(tmp_path, "cohort.nt") + ["--qlever-port", "99999"])
            with self.assertRaises(SystemExit):
                self._resolve(self._base_args(tmp_path, "cohort.nt") + ["--query-timeout", "0"])


if __name__ == "__main__":
    unittest.main()


class WrapperValidationTargetTests(VerboseTestCase):
    def test_targets_resolve_only_to_artifacts_that_exist(self):
        """A representation that was not produced is skipped, not failed."""
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "cohort"
            out_dir.mkdir()
            aggregate = out_dir / "cohort.nt.gz"
            with gzip.open(aggregate, "wb") as handle:
                handle.write(TRIPLES)
            (out_dir / "cohort.hdt").write_bytes(b"fake-hdt")

            targets = vcf_rdfizer.resolve_validation_targets(
                requested=["aggregate", "hdt", "cottas"],
                output_dir=out_dir,
                output_name="cohort",
                aggregate_path=aggregate,
                selected_methods=["hdt", "cottas"],
            )
            self.assertEqual([t["name"] for t in targets], ["aggregate", "hdt"])
            self.assertEqual(targets[0]["format"], "nt.gz")
            self.assertEqual(targets[1]["format"], "hdt")

    def test_targets_skip_representations_that_were_not_selected(self):
        """Asking for hdt without selecting it produces no target."""
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "cohort"
            out_dir.mkdir()
            aggregate = out_dir / "cohort.nt"
            aggregate.write_bytes(TRIPLES)
            (out_dir / "cohort.hdt").write_bytes(b"fake-hdt")
            targets = vcf_rdfizer.resolve_validation_targets(
                requested=["hdt"],
                output_dir=out_dir,
                output_name="cohort",
                aggregate_path=aggregate,
                selected_methods=["gzip"],
            )
            self.assertEqual(targets, [])

    def test_target_option_parsing(self):
        self.assertEqual(vcf_rdfizer.parse_validation_targets("all"), ["aggregate", "hdt", "cottas"])
        self.assertEqual(vcf_rdfizer.parse_validation_targets("hdt,cottas,hdt"), ["hdt", "cottas"])
        self.assertEqual(vcf_rdfizer.parse_validation_targets("none"), [])
        with self.assertRaises(ValueError):
            vcf_rdfizer.parse_validation_targets("hdt,bogus")

    def test_validation_command_carries_engine_and_format(self):
        """The wrapper passes the engine and artifact format to the container."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            vcf_path = tmp_path / "cohort.vcf"
            vcf_path.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n", encoding="utf-8")
            hdt_path = tmp_path / "cohort.hdt"
            hdt_path.write_bytes(b"fake-hdt")
            commands = []

            with mock.patch.object(vcf_rdfizer, "run", side_effect=lambda cmd, **kw: commands.append(cmd) or 0):
                vcf_rdfizer.run_validation_mode(
                    vcf_path=vcf_path,
                    rdf_path=hdt_path,
                    representation="condensed",
                    validation_id="cohort",
                    results_dir=tmp_path / "results",
                    metrics_dir=tmp_path / "metrics",
                    run_id="RID",
                    timestamp="TS",
                    image_ref="example/vcf-rdfizer:latest",
                    filter_oracle="auto",
                    engine="qlever",
                    engine_options={"qlever_memory_gb": 12, "qlever_index_args": ["--x", "1"]},
                    wrapper_log_path=tmp_path / "wrapper.log",
                )
            command = commands[0]
            self.assertIn("--rdf-format", command)
            self.assertEqual(command[command.index("--rdf-format") + 1], "hdt")
            self.assertEqual(command[command.index("--engine") + 1], "qlever")
            self.assertEqual(command[command.index("--qlever-memory-gb") + 1], "12")
            self.assertIn("--qlever-index-arg", command)

    def test_unsupported_artifact_is_rejected_by_the_wrapper(self):
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            vcf_path = tmp_path / "cohort.vcf"
            vcf_path.write_text("##fileformat=VCFv4.2\n", encoding="utf-8")
            bogus = tmp_path / "cohort.ttl"
            bogus.write_text("", encoding="utf-8")
            with self.assertRaises(ValueError):
                vcf_rdfizer.run_validation_mode(
                    vcf_path=vcf_path,
                    rdf_path=bogus,
                    representation="expanded",
                    validation_id="cohort",
                    results_dir=tmp_path / "results",
                    metrics_dir=tmp_path / "metrics",
                    run_id="RID",
                    timestamp="TS",
                    image_ref="example/vcf-rdfizer:latest",
                    filter_oracle="auto",
                    wrapper_log_path=tmp_path / "wrapper.log",
                )


class QleverEnvironmentTests(VerboseTestCase):
    def test_qlever_processes_get_their_private_library_path(self):
        """QLever's copied Boost/ICU/jemalloc must not shadow the rest of the image."""
        with tempfile.TemporaryDirectory() as td:
            engine = V.build_engine(
                "qlever", Path(td) / "a.nt", raw_dir=Path(td), scratch=Path(td), options={}
            )
            with mock.patch.dict(os.environ, {"LD_LIBRARY_PATH": "/usr/local/lib"}):
                environment = engine._environment()
            self.assertEqual(
                environment["LD_LIBRARY_PATH"], f"{V.QLEVER_LIB_DIR}:/usr/local/lib"
            )
            self.assertTrue(V.QLEVER_LIB_DIR.startswith("/opt/qlever"))

    def test_position_datatype_preflight_accepts_the_xsd_integer_family(self):
        """QLever canonicalises xsd:integer to xsd:int, so both must pass.

        Verified against qlever-index/qlever-server bfd5741: without the wider
        accepted set this preflight failed every run on QLever while passing on
        Comunica. xsd:string and xsd:decimal are still reported as anomalies.
        """
        text = (V.QUERY_ROOT / "common" / "preflight_position_datatype.rq").read_text(
            encoding="utf-8"
        )
        self.assertIn("NOT IN", text)
        accepted_list = text.split("NOT IN", 1)[1].split("))", 1)[0]
        for accepted in ("xsd:integer", "xsd:int", "xsd:long", "xsd:nonNegativeInteger"):
            self.assertIn(accepted, accepted_list)
        for rejected in ("xsd:string", "xsd:decimal", "xsd:double"):
            self.assertNotIn(rejected, accepted_list)
