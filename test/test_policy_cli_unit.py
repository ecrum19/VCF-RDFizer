"""vcf-rdfizer-policy end to end: every command, both modes, and the exit-code contract.

The contract is 0 success, 1 a check failed, 2 the policy, graph or request
cannot be evaluated. The difference between 1 and 2 matters to a pipeline: a
1 means a view was produced and is wrong, a 2 means nothing was evaluated at
all. Each is exercised here, on the demo cohort, through main() exactly as the
command line reaches it.
"""

from contextlib import redirect_stderr, redirect_stdout
import gzip
from io import StringIO
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

from test import policy_fixtures as F
from test.helpers import VerboseTestCase

try:
    import rdflib
except ModuleNotFoundError:  # pragma: no cover - exercised only without rdflib
    rdflib = None

import vcf_rdfizer_policy

VCFC = "https://w3id.org/vcf-core/vocab#"


def run(*argv):
    """(exit code, stdout, stderr) of main(argv)."""
    out, err = StringIO(), StringIO()
    with redirect_stdout(out), redirect_stderr(err), \
            mock.patch("vcf_rdfizer_policies.engine.os.cpu_count", return_value=1):
        code = vcf_rdfizer_policy.main([str(a) for a in argv])
    return code, out.getvalue(), err.getvalue()


def common():
    return ["--policy", F.POLICY]


def rdf():
    return ["--rdf", *F.RDF]


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class CliTests(VerboseTestCase):
    def setUp(self):
        self.work = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.work, True)

    def evaluate(self, requester, out, *extra):
        spec = F.REQUESTERS[requester]
        return run("evaluate", *common(), *rdf(), "--assignee", spec["assignee"],
                   "--purpose", spec["purpose"], "-o", out, *extra)

    # --- explain ---

    def test_explain_describes_every_rule_and_the_conflict_strategy(self):
        code, out, _ = run("explain", *common())
        self.assertEqual(code, 0)
        lines = out.strip().splitlines()
        self.assertEqual(len(lines), 9, "eight rules, then the deny-wins summary")
        self.assertTrue(lines[-1].startswith("Deny wins; anything no permission covers is withheld."))
        self.assertTrue(any("(a RegionSelector)" in line for line in lines))
        self.assertTrue(any("duties: attribute" in line for line in lines))
        self.assertTrue(any("purpose isNoneOf DUO_0000043" in line for line in lines))

    # --- attach ---

    def test_attach_writes_an_annotated_graph(self):
        out_path = self.work / "annotated.nt"
        code, out, _ = run("attach", *common(), *rdf(), "-o", out_path)
        self.assertEqual(code, 0)
        self.assertIn("resource(s) linked", out)
        graph = rdflib.Graph().parse(str(out_path), format="nt")
        self.assertGreater(len(list(graph.triples((None, rdflib.URIRef(
            "http://www.w3.org/ns/odrl/2/hasPolicy"), None)))), 0)

    def test_attach_never_overwrites(self):
        out_path = self.work / "annotated.nt"
        out_path.write_text("keep me", encoding="utf-8")
        code, _, err = run("attach", *common(), *rdf(), "-o", out_path)
        self.assertEqual(code, 2)
        self.assertIn("never overwrites", err)
        self.assertEqual(out_path.read_text(encoding="utf-8"), "keep me")

    # --- evaluate and check, in memory ---

    def test_evaluate_then_check_passes(self):
        view = self.work / "gru"
        code, out, _ = self.evaluate("gru", view)
        self.assertEqual(code, 0)
        self.assertIn(f"released {len(F.expected_released('gru'))} record(s)", out)
        code, out, _ = run("check", "--view", view, *common(), *rdf(), "--vcf", *F.VCFS)
        self.assertEqual((code, out.strip()), (0, "PASS"))

    def test_a_tampered_view_fails_the_check_with_exit_one(self):
        """Exit 1, not 2: a view exists and it is wrong."""
        view = self.work / "gru"
        self.evaluate("gru", view)
        record = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        with (view / "view.nt").open("a", encoding="utf-8") as handle:
            handle.write(f'<{record}> <{VCFC}chrom> "chr17" .\n')
        code, out, _ = run("check", "--view", view, *common(), *rdf())
        self.assertEqual(code, 1)
        self.assertIn(f"FAIL prohibited content present", out)
        self.assertTrue(out.strip().endswith("failure(s)"))

    def test_an_unknown_purpose_cannot_be_evaluated_and_exits_two(self):
        code, _, err = run("evaluate", *common(), *rdf(), "--assignee", "urn:x",
                           "--purpose", "DUO:9999999", "-o", self.work / "v")
        self.assertEqual(code, 2)
        self.assertIn("not a term of the purpose vocabulary", err)
        self.assertFalse((self.work / "v").exists(), "nothing is written when nothing is evaluated")

    def test_a_missing_policy_file_exits_two(self):
        code, _, err = run("explain", "--policy", self.work / "absent.ttl")
        self.assertEqual(code, 2)
        self.assertTrue(err.startswith("error:"))

    # --- evaluate and check, streamed against endpoints ---

    def test_streamed_evaluate_then_check_passes_over_http(self):
        from vcf_rdfizer_policies.vcf_oracle import graph_from_vcfs

        view = self.work / "alz"
        with F.SparqlEndpoint(F.shared_graph()) as source:
            code, out, _ = self.evaluate("alz", view, "--endpoint", source.url)
            self.assertEqual(code, 0)
            self.assertTrue((view / "view.nt.gz").exists())
            self.assertIn(f"released {len(F.expected_released('alz'))} record(s)", out)

            text = gzip.open(view / "view.nt.gz", "rt", encoding="utf-8").read()
            with F.SparqlEndpoint(rdflib.Graph().parse(data=text, format="nt")) as served, \
                    F.SparqlEndpoint(graph_from_vcfs(F.VCFS)) as oracle:
                for oracle_args in (["--vcf", *F.VCFS], ["--oracle-endpoint", oracle.url], []):
                    with self.subTest(oracle=oracle_args[:1] or "none"):
                        code, out, _ = run("check", "--view", view, *common(), *rdf(),
                                           "--endpoint", source.url, "--view-endpoint", served.url, *oracle_args)
                        self.assertEqual((code, out.strip()), (0, "PASS"))

    def test_a_streamed_check_without_a_view_endpoint_exits_two(self):
        view = self.work / "gru"
        with F.SparqlEndpoint(F.shared_graph()) as source:
            self.evaluate("gru", view, "--endpoint", source.url)
            code, _, err = run("check", "--view", view, *common(), *rdf(), "--endpoint", source.url)
        self.assertEqual(code, 2)
        self.assertIn("--view-endpoint", err)

    # --- oracle ---

    def test_oracle_writes_plain_or_compressed_and_never_overwrites(self):
        for name in ("o.nt", "o.nt.gz"):
            with self.subTest(name=name):
                code, out, _ = run("oracle", "--vcf", *F.VCFS, "-o", self.work / name)
                self.assertEqual(code, 0)
                self.assertIn("triple(s)", out)
        self.assertEqual(gzip.open(self.work / "o.nt.gz").read(), (self.work / "o.nt").read_bytes())
        code, _, err = run("oracle", "--vcf", *F.VCFS, "-o", self.work / "o.nt")
        self.assertEqual(code, 2)
        self.assertIn("never overwrites", err)

    # --- the rest of the surface ---

    def test_without_rdflib_every_command_says_how_to_install_it(self):
        real_import = __import__

        def no_rdflib(name, *args, **kwargs):
            if name == "rdflib":
                raise ModuleNotFoundError("No module named 'rdflib'")
            return real_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=no_rdflib):
            code, _, err = run("explain", *common())
        self.assertEqual(code, 2)
        self.assertIn("pip install rdflib", err)

    def test_version(self):
        out = StringIO()
        with redirect_stdout(out), self.assertRaises(SystemExit) as caught:
            vcf_rdfizer_policy.main(["--version"])
        self.assertEqual(caught.exception.code, 0)
        from vcf_rdfizer_policies import VERSION
        self.assertIn(VERSION, out.getvalue())


if __name__ == "__main__":
    unittest.main()
