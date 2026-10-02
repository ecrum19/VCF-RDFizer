"""Two memory guards, each fixed after it failed on a 170.9M-triple graph.

**The shape-layer gate measured the wrong thing.** pyshacl is in-memory, so the
wrapper size-gates it -- but on the PACKAGED artifact's bytes, while what
pyshacl pays for is the graph. One 170,935,101-triple graph therefore landed on
both sides of one 512 MiB gate purely by packaging:

    cottas      390,728,158 B   under -> shapes attempted -> OOM
    nt.gz       756,594,166 B   over  -> skipped
    hdt       1,182,206,289 B   over  -> skipped

The COTTAS run was SIGKILLed at 32.2 GB RSS on a 31 GB machine, after COTTAS
decoding and rapper had both succeeded on every triple. The better a format
compresses, the likelier it was to exhaust memory: the guard inverted. The fix
gates on the decoded triple count, which no packaging can change.

**Node chose a ceiling the machine did not.** On the same graph the HDT
endpoint aborted with "Reached heap limit Allocation failed - JavaScript heap
out of memory" while ~25 GB was free; the kernel OOM killer was never involved.
Node does not size its old-space from the host, so the endpoints now accept an
explicit ceiling.
"""

import argparse
import copy
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

import vcf_rdfizer
from test import validation_fixtures as fixtures
from test.helpers import VerboseTestCase
from test.test_query_selection_unit import bindings_for

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"
_spec = importlib.util.spec_from_file_location("validation_runner_guards", RUNNER_PATH)
V = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(V)

# The real numbers from the run that motivated the fix.
GRAPH_TRIPLES = 170_935_101
ARTIFACT_BYTES = {
    "cottas": 390_728_158,
    "nt.gz": 756_594_166,
    "hdt": 1_182_206_289,
}
WRAPPER_BYTE_GATE = 512 * 1024 * 1024


class ShaclGateTests(VerboseTestCase):
    def test_the_packaging_no_longer_decides(self):
        """The property the bug violated: same graph, same verdict.

        Every packaging of one graph must get one answer. This is the
        regression test for the inversion itself.
        """
        verdicts = {
            kind: V.shacl_exceeds_limit(GRAPH_TRIPLES, V.DEFAULT_SHACL_MAX_TRIPLES)
            for kind in ARTIFACT_BYTES
        }
        self.assertEqual(set(verdicts.values()), {True},
                         "the decoded graph is the same for every artifact, so "
                         "every artifact must reach the same decision")

    def test_the_old_byte_gate_did_disagree_across_packagings(self):
        """Pins why the fix was needed, so nobody reinstates the byte gate.

        Not a test of current behaviour -- a record of the defect, expressed in
        the numbers that produced it.
        """
        byte_verdicts = {k: b <= WRAPPER_BYTE_GATE for k, b in ARTIFACT_BYTES.items()}
        self.assertTrue(byte_verdicts["cottas"], "cottas slipped under the gate")
        self.assertFalse(byte_verdicts["nt.gz"])
        self.assertFalse(byte_verdicts["hdt"])
        self.assertEqual(len(set(byte_verdicts.values())), 2,
                         "the byte gate split one graph three ways; that was the bug")

    def test_the_graph_that_oomed_is_refused(self):
        self.assertTrue(V.shacl_exceeds_limit(GRAPH_TRIPLES, V.DEFAULT_SHACL_MAX_TRIPLES))

    def test_the_campaigns_largest_validated_graph_still_passes(self):
        """0.96M triples is the largest graph the campaign ran shapes on. The
        gate must not retroactively disable published behaviour."""
        self.assertFalse(V.shacl_exceeds_limit(958_919, V.DEFAULT_SHACL_MAX_TRIPLES))

    def test_the_real_genome_that_exhausted_memory_is_refused(self):
        """58.2M triples of NG131FQA1I hung a 31 GB host in one piece."""
        self.assertTrue(V.shacl_exceeds_limit(58_231_176, V.DEFAULT_SHACL_MAX_TRIPLES))

    def test_an_unknown_count_is_treated_as_too_large(self):
        """Skipping is recoverable and recorded; exhausting memory is not."""
        self.assertTrue(V.shacl_exceeds_limit(None, V.DEFAULT_SHACL_MAX_TRIPLES))

    def test_zero_disables_the_gate(self):
        for count in (0, 1, GRAPH_TRIPLES):
            with self.subTest(count=count):
                self.assertFalse(V.shacl_exceeds_limit(count, 0))
        self.assertFalse(V.shacl_exceeds_limit(None, 0))

    def test_the_boundary_is_inclusive(self):
        limit = V.DEFAULT_SHACL_MAX_TRIPLES
        self.assertFalse(V.shacl_exceeds_limit(limit, limit))
        self.assertTrue(V.shacl_exceeds_limit(limit + 1, limit))

    def test_the_default_sits_between_the_two_observations(self):
        """It must admit what worked and refuse what died, or it is arbitrary."""
        self.assertGreater(V.DEFAULT_SHACL_MAX_TRIPLES, 958_919)
        self.assertLess(V.DEFAULT_SHACL_MAX_TRIPLES, 58_231_176)


class NodeHeapEnvTests(VerboseTestCase):
    def test_unset_leaves_the_environment_alone(self):
        """Published results were produced under Node's default; keep it."""
        env = V.node_endpoint_env(None, {"PATH": "/usr/bin"})
        self.assertNotIn("NODE_OPTIONS", env)
        self.assertEqual(env["PATH"], "/usr/bin")

    def test_zero_is_also_unset(self):
        self.assertNotIn("NODE_OPTIONS", V.node_endpoint_env(0, {}))

    def test_a_ceiling_is_applied(self):
        env = V.node_endpoint_env(16384, {})
        self.assertEqual(env["NODE_OPTIONS"], "--max-old-space-size=16384")

    def test_an_existing_setting_is_kept_not_replaced(self):
        """A caller's own NODE_OPTIONS must survive; only the heap is added."""
        env = V.node_endpoint_env(8192, {"NODE_OPTIONS": "--enable-source-maps"})
        self.assertIn("--enable-source-maps", env["NODE_OPTIONS"])
        self.assertIn("--max-old-space-size=8192", env["NODE_OPTIONS"])

    def test_the_base_environment_is_not_mutated(self):
        base = {"NODE_OPTIONS": "--enable-source-maps"}
        V.node_endpoint_env(4096, base)
        self.assertEqual(base["NODE_OPTIONS"], "--enable-source-maps")

    def test_it_defaults_to_the_process_environment(self):
        env = V.node_endpoint_env(None)
        self.assertEqual(env.get("PATH"), os.environ.get("PATH"))

    def test_the_default_is_node_s_own(self):
        """Unset by default, so this change alters no existing measurement."""
        self.assertIsNone(V.DEFAULT_NODE_HEAP_MB)

    def test_the_heap_is_appended_after_the_callers_options(self):
        """Pinned exactly: one space, caller's options first, nothing dropped."""
        env = V.node_endpoint_env(
            8192, {"NODE_OPTIONS": "--enable-source-maps --trace-warnings"})
        self.assertEqual(
            env["NODE_OPTIONS"],
            "--enable-source-maps --trace-warnings --max-old-space-size=8192")

    def test_a_blank_existing_setting_is_not_kept_as_padding(self):
        env = V.node_endpoint_env(2048, {"NODE_OPTIONS": "   "})
        self.assertEqual(env["NODE_OPTIONS"], "--max-old-space-size=2048")

    def test_unset_returns_a_copy_not_the_process_environment(self):
        """Callers may mutate what they get back without touching os.environ."""
        env = V.node_endpoint_env(None)
        self.assertIsNot(env, os.environ)
        self.assertIsInstance(env, dict)


class RunValidationToleratesAMinimalNamespaceTests(VerboseTestCase):
    """A new option must not become a required attribute of run_validation.

    run_validation is driven by hand-built namespaces in several places -- this
    suite and the mutation harness among them -- so reading a new option with
    plain attribute access breaks those callers at runtime rather than at
    import. That is exactly how adding --shacl-max-triples and --node-heap-mb
    broke CI: five tests failed with "'Namespace' object has no attribute
    'node_heap_mb'", and the failure surfaced in a merged feature's tests
    rather than in the change that caused it.

    The convention the file already used for progress_path and quiet is
    getattr with the documented default. This pins it, so the next option
    added cannot reintroduce the same break silently.
    """

    def test_run_validation_accepts_a_namespace_without_the_new_options(self):
        """Behavioural, not textual: drive run_validation and look for the break.

        An earlier version of this test asserted on the source text and failed
        twice on formatting, which is the wrong instrument -- it constrains how
        the guard is written rather than that it works.
        """
        import argparse
        import json
        import tempfile
        from unittest import mock

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            (tmp / "scratch").mkdir()
            (tmp / "s.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            (tmp / "s.nt").write_text("<s> <p> <o> .\n")
            # Deliberately missing shacl_max_triples and node_heap_mb.
            args = argparse.Namespace(
                results_dir=tmp / "results", representation="expanded",
                info_representation="structured", header_representation="structured",
                progress_path=None, quiet=True, scratch_dir=tmp / "scratch",
                rdf=tmp / "s.nt", rdf_format="nt", engine="comunica",
                engines=["comunica"], mapping_policy="strict",
                strict_conformance=False, shacl_shapes=None,
                query_timeout=60, validation_time_budget=0,
                stop_after_query_timeout=False, qlever_memory_gb=4,
                qlever_port=7019, comunica_port=7020, hdt_port=7021,
                comunica_bind_timeout=60, comunica_warmup_timeout=60,
                qlever_startup_timeout=60, qlever_index_arg=[],
                qlever_server_arg=[], vcf=tmp / "s.vcf",
                filter_oracle="cyvcf2", dataset_id="sample", queries=None,
            )
            engine = mock.MagicMock()
            engine.describe.return_value = {"engine": "comunica"}
            engine.execute.return_value = {"status": "FAILED"}
            with mock.patch.object(V, "parse_vcf", return_value={
                        "totalRecords": 1, "sampleCount": 0, "gtRecordCount": 0,
                        "sourceSha256": "0" * 64}), \
                    mock.patch.object(V, "attach_census_expectations",
                                      side_effect=lambda p, *a, **k: p), \
                    mock.patch.object(V, "validate_ntriples",
                                      return_value={"status": "PASS", "tripleCount": 1}), \
                    mock.patch.object(V, "materialize_ntriples",
                                      return_value=(tmp / "s.nt", {})), \
                    mock.patch.object(V, "build_manifest", return_value={}), \
                    mock.patch.object(V, "build_engine", return_value=engine):
                V.run_validation(args)
            summary = json.loads((args.results_dir / "summary.json").read_text())

        # The run may fail for its own reasons -- the engine here is a stub --
        # but never because an option was read by attribute.
        self.assertNotIn(
            "has no attribute", str(summary.get("error") or ""),
            "run_validation read a new option by attribute; use getattr with "
            "its documented default so callers that build their own Namespace "
            "keep working",
        )

    def test_a_namespace_without_them_still_resolves(self):
        """The behaviour the getattr guard buys, checked rather than assumed."""
        import argparse

        args = argparse.Namespace()
        self.assertEqual(
            getattr(args, "shacl_max_triples", V.DEFAULT_SHACL_MAX_TRIPLES),
            V.DEFAULT_SHACL_MAX_TRIPLES,
        )
        self.assertIsNone(getattr(args, "node_heap_mb", V.DEFAULT_NODE_HEAP_MB))


#: Marks a field _runner_args must leave off the Namespace entirely.
_ABSENT = object()


def _runner_args(tmp: Path, **overrides) -> argparse.Namespace:
    """The runner's full argument surface, with any field overridable or absent."""
    fields = dict(
        results_dir=tmp / "results", representation="expanded",
        info_representation="structured", header_representation="structured",
        progress_path=None, quiet=True, scratch_dir=tmp / "scratch",
        rdf=tmp / "s.nt", rdf_format="nt", engine="comunica",
        engines=["comunica"], mapping_policy="strict",
        strict_conformance=False, shacl_shapes=None, shacl_ontology=None,
        query_timeout=60, validation_time_budget=0,
        stop_after_query_timeout=False, qlever_memory_gb=4,
        qlever_port=7019, comunica_port=7020, hdt_port=7021,
        comunica_bind_timeout=60, comunica_warmup_timeout=60,
        qlever_startup_timeout=60, qlever_index_arg=[],
        qlever_server_arg=[], vcf=tmp / "s.vcf",
        filter_oracle="cyvcf2", dataset_id="sample", queries=None,
        shacl_max_triples=V.DEFAULT_SHACL_MAX_TRIPLES,
        # Whole-graph validation, the path the size gate guards. The batched
        # path is exempt from it; RunValidationShaclGateTests pins both.
        shacl_batch_triples=0, shacl_workers=1,
        node_heap_mb=V.DEFAULT_NODE_HEAP_MB,
    )
    fields.update(overrides)
    return argparse.Namespace(
        **{k: v for k, v in fields.items() if v is not _ABSENT})


class RunValidationShaclGateTests(VerboseTestCase):
    """The gate where it actually runs: inside run_validation.

    The helper tests above prove shacl_exceeds_limit decides correctly; these
    prove run_validation acts on the decision -- that a refused graph never
    reaches pyshacl, that the skip is recorded rather than silent, and that an
    admitted graph still gets its shapes checked.
    """

    def _drive(self, *, rdf_validation=None, shapes=True, **overrides):
        """Run the validator with every expensive stage stubbed out.

        Returns what a caller can observe afterwards: the exit code, the
        summary, shacl.json (or None), the validate_shacl and build_engine
        mocks, and stderr.
        """
        if rdf_validation is None:
            rdf_validation = {"status": "PASS", "tripleCount": 1}
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            (tmp / "scratch").mkdir()
            (tmp / "s.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            (tmp / "s.nt").write_text("<s> <p> <o> .\n")
            shapes_path = tmp / "shapes.ttl"
            shapes_path.write_text("")
            if shapes:
                overrides.setdefault("shacl_shapes", [shapes_path])
            args = _runner_args(tmp, **overrides)
            # An engine that agrees with the oracle, so the run reaches the
            # full summary rather than stopping on a query failure.
            parser_summary = copy.deepcopy(fixtures.parser_summary("expanded"))
            parser_summary.setdefault("sourceSha256", "0" * 64)
            raw_dir = tmp / "raw"
            raw_dir.mkdir()
            query_file = tmp / "query.rq"
            query_file.write_text("SELECT * WHERE { ?s ?p ?o }\n")

            def execute(query_id, _path):
                raw = raw_dir / f"{query_id}.json"
                answer = parser_summary.get(query_id)
                raw.write_text(json.dumps(
                    bindings_for(answer) if answer is not None
                    else {"results": {"bindings": []}}))
                return {"status": "PASS", "rawResult": str(raw), "wallSeconds": 0.01}

            engine = mock.MagicMock()
            engine.describe.return_value = {"engine": "comunica", "setupSeconds": 0.1}
            engine.execute.side_effect = execute
            engine.query_timeout = 60
            stderr = StringIO()
            with mock.patch.object(V, "query_path", return_value=query_file), \
                    mock.patch.object(V, "parse_vcf", return_value=parser_summary), \
                    mock.patch.object(V, "attach_census_expectations",
                                      side_effect=lambda p, *a, **k: p), \
                    mock.patch.object(V, "validate_ntriples",
                                      return_value=dict(rdf_validation)), \
                    mock.patch.object(V, "materialize_ntriples",
                                      return_value=(tmp / "s.nt", {})), \
                    mock.patch.object(V, "validate_shacl", return_value={
                        "status": "PASS", "wallSeconds": 0.1}) as validate_shacl, \
                    mock.patch.object(V, "build_manifest", return_value={}), \
                    mock.patch.object(V, "build_engine",
                                      return_value=engine) as build_engine, \
                    redirect_stderr(stderr):
                rc = V.run_validation(args)
            results = args.results_dir
            summary = json.loads((results / "summary.json").read_text())
            shacl_path = results / "shacl.json"
            shacl = json.loads(shacl_path.read_text()) if shacl_path.exists() else None
        return {
            "rc": rc, "summary": summary, "shacl": shacl,
            "validate_shacl": validate_shacl, "build_engine": build_engine,
            "stderr": stderr.getvalue(), "decoded": tmp / "s.nt",
        }

    def assertRunDidNotError(self, run):
        self.assertIsNone(
            run["summary"].get("error"),
            f"run_validation raised instead of deciding: {run['summary'].get('error')}")

    def test_a_graph_above_the_limit_never_reaches_pyshacl(self):
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES})
        self.assertRunDidNotError(run)
        run["validate_shacl"].assert_not_called()

    def test_batched_shapes_are_not_gated(self):
        """A batch bounds pyshacl's memory, so a large graph still gets shapes."""
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES},
            shacl_batch_triples=V.DEFAULT_SHACL_BATCH_TRIPLES, shacl_workers=3)
        self.assertRunDidNotError(run)
        run["validate_shacl"].assert_called_once()
        self.assertTrue(run["validate_shacl"].call_args.kwargs["records_per_batch"])
        self.assertEqual(run["validate_shacl"].call_args.kwargs["workers"], 3)
        self.assertIsNone(run["shacl"], "nothing was skipped, so nothing is recorded")

    def test_sparql_shapes_are_still_gated_when_batching_is_on(self):
        """A SPARQL constraint compares records, so those shapes stay whole."""
        with mock.patch.object(V, "shapes_are_node_local", return_value=False):
            run = self._drive(
                rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES},
                shacl_batch_triples=V.DEFAULT_SHACL_BATCH_TRIPLES)
        run["validate_shacl"].assert_not_called()
        self.assertEqual(run["shacl"]["status"], "SKIPPED_TOO_LARGE")

    def test_the_skip_is_recorded_with_its_reason(self):
        """A skip nobody can see is the silent failure this replaces."""
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES})
        shacl = run["shacl"]
        self.assertIsNotNone(shacl, "the skip must be written to shacl.json")
        self.assertEqual(shacl["status"], "SKIPPED_TOO_LARGE")
        self.assertEqual(shacl["tripleCount"], GRAPH_TRIPLES)
        self.assertEqual(shacl["limitTriples"], V.DEFAULT_SHACL_MAX_TRIPLES)
        # Both numbers, human-formatted, and the flag that changes the outcome.
        self.assertIn(f"{GRAPH_TRIPLES:,}", shacl["reason"])
        self.assertIn(f"{V.DEFAULT_SHACL_MAX_TRIPLES:,}", shacl["reason"])
        self.assertIn("--shacl-max-triples", shacl["reason"])

    def test_the_skip_is_announced_on_stderr(self):
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES})
        self.assertIn("[sample] shapes skipped:", run["stderr"])

    def test_the_skip_is_what_the_summary_reports(self):
        """Downstream reads the summary, not shacl.json; it must agree."""
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES})
        self.assertEqual(run["summary"]["shacl"], run["shacl"])

    def test_a_skip_is_not_a_shape_failure(self):
        """SKIPPED must not be mistaken for FAIL and block the run on shapes."""
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES})
        self.assertNotEqual(run["summary"].get("status"), "BLOCKED_BY_PREFLIGHT")

    def test_a_graph_within_the_limit_still_gets_its_shapes_checked(self):
        """The campaign's largest shape-validated graph, at the default gate."""
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": 958_919})
        self.assertRunDidNotError(run)
        run["validate_shacl"].assert_called_once()
        self.assertEqual(run["validate_shacl"].call_args[0][0], run["decoded"])
        self.assertEqual(run["summary"]["shacl"]["status"], "PASS")
        self.assertNotIn("shapes skipped", run["stderr"])

    def test_the_limit_itself_is_admitted(self):
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": 1_000},
            shacl_max_triples=1_000)
        run["validate_shacl"].assert_called_once()

    def test_one_over_the_limit_is_refused(self):
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": 1_001},
            shacl_max_triples=1_000)
        run["validate_shacl"].assert_not_called()
        self.assertEqual(run["shacl"]["limitTriples"], 1_000)

    def test_zero_disables_the_gate_in_the_run(self):
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES},
            shacl_max_triples=0)
        self.assertRunDidNotError(run)
        run["validate_shacl"].assert_called_once()

    def test_an_unknown_count_is_skipped_and_recorded_not_crashed(self):
        """rapper passed but its count line did not parse, so tripleCount is None.

        The helper treats that as too large, so the run must record a skip --
        not die while formatting the reason.
        """
        run = self._drive(rdf_validation={"status": "PASS", "tripleCount": None})
        self.assertRunDidNotError(run)
        run["validate_shacl"].assert_not_called()
        self.assertEqual(run["shacl"]["status"], "SKIPPED_TOO_LARGE")
        self.assertIsNone(run["shacl"]["tripleCount"])
        self.assertIn("unknown", run["shacl"]["reason"])

    def test_a_missing_rapper_still_blocks_on_preflight_not_on_the_gate(self):
        """No tripleCount key at all: the preflight verdict must survive."""
        run = self._drive(rdf_validation={
            "status": "EXECUTION_FAILED", "error": "rapper is not installed"})
        self.assertRunDidNotError(run)
        self.assertEqual(run["summary"]["status"], "BLOCKED_BY_PREFLIGHT")
        run["validate_shacl"].assert_not_called()

    def test_without_shapes_there_is_nothing_to_gate(self):
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES},
            shapes=False)
        self.assertRunDidNotError(run)
        run["validate_shacl"].assert_not_called()
        self.assertIsNone(run["shacl"], "no shapes asked for, so no shacl.json")
        self.assertNotIn("shapes skipped", run["stderr"])

    def test_a_namespace_without_the_limit_gets_the_default_gate(self):
        """Absent means the documented default, not 'ungated'."""
        run = self._drive(
            rdf_validation={"status": "PASS", "tripleCount": GRAPH_TRIPLES},
            shacl_max_triples=_ABSENT)
        self.assertRunDidNotError(run)
        run["validate_shacl"].assert_not_called()
        self.assertEqual(run["shacl"]["limitTriples"], V.DEFAULT_SHACL_MAX_TRIPLES)

    def test_the_heap_ceiling_reaches_the_engine(self):
        run = self._drive(shapes=False, node_heap_mb=16384)
        options = run["build_engine"].call_args.kwargs["options"]
        self.assertEqual(options["node_heap_mb"], 16384)

    def test_a_namespace_without_the_heap_gets_node_s_default(self):
        run = self._drive(shapes=False, node_heap_mb=_ABSENT)
        options = run["build_engine"].call_args.kwargs["options"]
        self.assertIsNone(options["node_heap_mb"])


class EndpointHeapTests(VerboseTestCase):
    """The ceiling must reach the Node process, for all three endpoints.

    node_endpoint_env is only half the fix; the other half is that each
    Comunica-backed engine reads the option and hands the environment to the
    process it spawns. A ceiling that stops at engine construction is the same
    wiring gap as a flag the wrapper never forwards.
    """

    ENGINES = ("comunica", "hdt", "cottas")

    def _engine(self, name, tmp, options):
        return V.build_engine(
            name, tmp / "graph.nt", raw_dir=tmp, scratch=tmp, options=options)

    def _spawn_env(self, name, options, environ):
        """Start the endpoint with the process stubbed; return the env it got."""
        with tempfile.TemporaryDirectory() as td:
            engine = self._engine(name, Path(td), options)
            engine.executable = f"/usr/bin/{engine.endpoint_binary}"
            with mock.patch.dict(os.environ, environ, clear=True), \
                    mock.patch.object(type(engine), "_endpoint_source_argument",
                                      return_value="graph"), \
                    mock.patch.object(V.subprocess, "Popen") as popen, \
                    mock.patch.object(V.ComunicaHttpEndpointMixin, "_await_bind"), \
                    mock.patch.object(V.ComunicaHttpEndpointMixin, "_await_warm"):
                engine._start_endpoint()
                popen.return_value.stdout = None
            return popen.call_args.kwargs["env"]

    def test_every_endpoint_engine_uses_the_shared_mixin(self):
        """If one stops inheriting it, it silently stops honouring the ceiling."""
        for name in self.ENGINES:
            with self.subTest(engine=name):
                self.assertTrue(issubclass(
                    V.ENGINE_CLASSES[name], V.ComunicaHttpEndpointMixin))

    def test_each_engine_reads_the_ceiling(self):
        for name in self.ENGINES:
            with self.subTest(engine=name), tempfile.TemporaryDirectory() as td:
                engine = self._engine(name, Path(td), {"node_heap_mb": 12288})
                self.assertEqual(engine.node_heap_mb, 12288)

    def test_each_engine_defaults_to_node_s_own(self):
        for options in ({}, {"node_heap_mb": None}, {"node_heap_mb": 0}):
            for name in self.ENGINES:
                with self.subTest(engine=name, options=options), \
                        tempfile.TemporaryDirectory() as td:
                    engine = self._engine(name, Path(td), options)
                    self.assertIsNone(engine.node_heap_mb)

    def test_the_ceiling_reaches_the_spawned_process(self):
        for name in self.ENGINES:
            with self.subTest(engine=name):
                env = self._spawn_env(
                    name, {"node_heap_mb": 12288},
                    {"PATH": "/usr/bin", "NODE_OPTIONS": "--enable-source-maps"})
                self.assertEqual(
                    env["NODE_OPTIONS"],
                    "--enable-source-maps --max-old-space-size=12288")
                self.assertEqual(env["PATH"], "/usr/bin")

    def test_unset_spawns_with_the_environment_unchanged(self):
        for name in self.ENGINES:
            with self.subTest(engine=name):
                env = self._spawn_env(name, {}, {"PATH": "/usr/bin"})
                self.assertEqual(env, {"PATH": "/usr/bin"})


class RunnerArgParsingTests(VerboseTestCase):
    """Both options exist on the runner, with the documented defaults and types."""

    def _parse(self, extra_argv):
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            (tmp / "s.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            (tmp / "s.nt").write_text("<s> <p> <o> .\n")
            argv = [
                "validation_runner.py", "--vcf", str(tmp / "s.vcf"),
                "--rdf", str(tmp / "s.nt"), "--representation", "expanded",
                "--results-dir", str(tmp / "results"), "--dataset-id", "sample",
                # The default is the container's /work, which does not exist
                # here; without this the parse fails for the wrong reason.
                "--scratch-dir", str(tmp),
                *extra_argv,
            ]
            with mock.patch.object(sys, "argv", argv):
                return V.parse_args()

    def test_the_defaults(self):
        args = self._parse([])
        self.assertEqual(args.shacl_max_triples, V.DEFAULT_SHACL_MAX_TRIPLES)
        self.assertIsNone(args.node_heap_mb)

    def test_both_parse_as_integers(self):
        args = self._parse(["--shacl-max-triples", "0", "--node-heap-mb", "16384"])
        self.assertEqual(args.shacl_max_triples, 0)
        self.assertEqual(args.node_heap_mb, 16384)

    def test_a_non_integer_exits_two(self):
        for flag in ("--shacl-max-triples", "--node-heap-mb"):
            with self.subTest(flag=flag):
                stderr = StringIO()
                with self.assertRaises(SystemExit) as caught, redirect_stderr(stderr):
                    self._parse([flag, "lots"])
                self.assertEqual(caught.exception.code, 2)
                self.assertIn(flag, stderr.getvalue())


class WrapperCliTests(VerboseTestCase):
    """main() must validate both flags and put them into engine_options.

    A flag that parses but is never read is the wiring gap this project has
    been bitten by before, so absence and presence are both pinned.
    """

    def _main(self, extra_argv):
        """Run main() up to run_validation_mode; return (rc, engine_options, stderr)."""
        captured = {}

        def capture(**kwargs):
            captured.update(kwargs)
            return 0

        stderr = StringIO()
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            (tmp / "s.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            (tmp / "s.nt").write_text("<s> <p> <o> .\n")
            argv = [
                "vcf_rdfizer.py", "--mode", "validation",
                "--input", str(tmp / "s.vcf"), "--rdf", str(tmp / "s.nt"),
                "--out", str(tmp / "out"),
                "--image", "example/vcf-rdfizer:test", "--no-build",
                *extra_argv,
            ]
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(vcf_rdfizer, "check_docker", return_value=True), \
                    mock.patch.object(vcf_rdfizer, "docker_image_exists",
                                      return_value=True), \
                    mock.patch.object(vcf_rdfizer, "run_validation_mode",
                                      side_effect=capture), \
                    redirect_stderr(stderr), redirect_stdout(StringIO()):
                rc = vcf_rdfizer.main()
        return rc, captured.get("engine_options"), stderr.getvalue()

    def test_neither_flag_means_neither_option(self):
        """Absent stays absent, so the runner applies its own defaults."""
        _rc, options, _err = self._main([])
        self.assertNotIn("shacl_max_triples", options)
        self.assertNotIn("node_heap_mb", options)

    def test_the_triple_limit_is_forwarded_as_an_integer(self):
        _rc, options, _err = self._main(["--shacl-max-triples", "100000000"])
        self.assertEqual(options["shacl_max_triples"], 100_000_000)

    def test_zero_is_kept_because_it_disables_the_gate(self):
        _rc, options, _err = self._main(["--shacl-max-triples", "0"])
        self.assertEqual(options["shacl_max_triples"], 0)

    def test_a_bad_triple_limit_is_a_usage_error(self):
        for value, message in (
            ("lots", "--shacl-max-triples must be an integer"),
            ("1.5", "--shacl-max-triples must be an integer"),
            ("-1", "--shacl-max-triples must be zero or a positive integer"),
        ):
            with self.subTest(value=value):
                rc, options, err = self._main(["--shacl-max-triples", value])
                self.assertEqual(rc, 2)
                self.assertIsNone(options, "a bad value must stop before the run")
                self.assertIn(message, err)

    def test_the_batch_options_are_forwarded_as_integers(self):
        _rc, options, _err = self._main(
            ["--shacl-batch-triples", "0", "--shacl-workers", "2"])
        self.assertEqual(options["shacl_batch_triples"], 0)
        self.assertEqual(options["shacl_workers"], 2)

    def test_bad_batch_options_are_usage_errors(self):
        for argv, message in (
            (["--shacl-batch-triples", "-1"],
             "--shacl-batch-triples must be zero or a positive integer"),
            (["--shacl-workers", "0"], "--shacl-workers must be a positive integer"),
        ):
            with self.subTest(argv=argv):
                rc, options, err = self._main(argv)
                self.assertEqual(rc, 2)
                self.assertIsNone(options)
                self.assertIn(message, err)

    def test_the_heap_is_forwarded_as_an_integer(self):
        _rc, options, _err = self._main(["--node-heap-mb", "16384"])
        self.assertEqual(options["node_heap_mb"], 16384)

    def test_a_bad_heap_is_a_usage_error(self):
        """Unlike the triple limit, 0 is not meaningful here: unset is how you
        keep Node's default."""
        for value in ("0", "-512", "lots"):
            with self.subTest(value=value):
                rc, options, err = self._main(["--node-heap-mb", value])
                self.assertEqual(rc, 2)
                self.assertIsNone(options)
                self.assertIn("--node-heap-mb must be a positive integer", err)


class WrapperForwardsTheGuardsTests(VerboseTestCase):
    """Both options must cross the container boundary, and the runner must
    accept what arrives."""

    def _command_for(self, engine_options):
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            (tmp / "s.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n")
            (tmp / "s.nt").write_bytes(b"<s> <p> <o> .\n")
            commands = []
            with mock.patch.object(
                vcf_rdfizer, "run",
                side_effect=lambda cmd, **kw: commands.append(cmd) or 0,
            ), redirect_stdout(StringIO()):
                vcf_rdfizer.run_validation_mode(
                    vcf_path=tmp / "s.vcf", rdf_path=tmp / "s.nt",
                    representation="expanded",
                    info_representation="structured",
                    header_representation="structured",
                    validation_id="s", results_dir=tmp / "results",
                    metrics_dir=tmp / "metrics", run_id="RID", timestamp="TS",
                    image_ref="example/vcf-rdfizer:latest",
                    filter_oracle="auto", engine="hdt",
                    engine_options=engine_options,
                    wrapper_log_path=tmp / "wrapper.log")
            return commands[0]

    def _value(self, command, flag):
        return command[command.index(flag) + 1]

    def test_both_are_forwarded(self):
        command = self._command_for(
            {"shacl_max_triples": 100_000_000, "node_heap_mb": 16384})
        self.assertEqual(self._value(command, "--shacl-max-triples"), "100000000")
        self.assertEqual(self._value(command, "--node-heap-mb"), "16384")

    def test_a_zero_limit_is_forwarded_not_dropped_as_falsy(self):
        """Dropping 0 would silently turn 'no gate' back into the default gate."""
        command = self._command_for({"shacl_max_triples": 0})
        self.assertEqual(self._value(command, "--shacl-max-triples"), "0")

    def test_absent_options_send_no_flags(self):
        command = self._command_for({})
        self.assertNotIn("--shacl-max-triples", command)
        self.assertNotIn("--node-heap-mb", command)

    def test_the_runner_accepts_what_the_wrapper_sends(self):
        """The contract across the boundary: the flag names and value types
        the wrapper emits are ones the runner parses."""
        command = self._command_for(
            {"shacl_max_triples": 0, "node_heap_mb": 16384,
             "shacl_batch_triples": 0, "shacl_workers": 2})
        actions = V.build_arg_parser()._option_string_actions
        for flag, expected in (("--shacl-max-triples", 0), ("--node-heap-mb", 16384),
                               ("--shacl-batch-triples", 0), ("--shacl-workers", 2)):
            with self.subTest(flag=flag):
                self.assertIn(flag, actions)
                self.assertEqual(actions[flag].type(self._value(command, flag)), expected)


if __name__ == "__main__":
    unittest.main()
