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
