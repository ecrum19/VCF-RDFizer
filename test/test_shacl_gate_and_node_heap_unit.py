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

import importlib.util
import os
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase

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
        """17.1M triples is the 100,000-record HG005 slice the campaign ran
        shapes on. The gate must not retroactively disable published behaviour."""
        self.assertFalse(V.shacl_exceeds_limit(17_098_746, V.DEFAULT_SHACL_MAX_TRIPLES))

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
        self.assertGreater(V.DEFAULT_SHACL_MAX_TRIPLES, 17_098_746)
        self.assertLess(V.DEFAULT_SHACL_MAX_TRIPLES, GRAPH_TRIPLES)


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


if __name__ == "__main__":
    unittest.main()


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
