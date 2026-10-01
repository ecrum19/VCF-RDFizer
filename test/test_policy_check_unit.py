"""Verifying a release view, in memory and streamed, and against the VCF-text oracle.

A checker is only worth having if it fails on a bad view. So each test below
starts from a view the engine produced correctly, confirms it passes, then
breaks it in one specific way and asserts the specific failure:

  prohibited content    a withheld BRCA1 record put back into a view
  ungoverned subject    a record from a file no permission covers
  dangling reference    a released record pointing at a call that was removed
  wrong policy          the view checked against a policy it was not made under
  leak / over-withheld  the oracle, which reads the VCF text and never the graph

The streamed checker gets the same treatment, plus the guard that the view
endpoint really serves the view being checked.
"""

import gzip
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

from vcf_rdfizer_policies import PolicyError

VCFC = "https://w3id.org/vcf-core/vocab#"
BRCA1 = "https://example.org/policy/demo-cohort/brca1"


def serial():
    return mock.patch("vcf_rdfizer_policies.engine.os.cpu_count", return_value=1)


def triples_about(graph, subject):
    return [(s, p, o) for s, p, o in graph.triples((rdflib.URIRef(subject), None, None))]


def record_with_its_edge(record):
    """A record as a real leak would emit it: its own triples and its file's hasRecord edge.

    The oracle finds records through ?file vcfc:hasRecord ?record, so a leak of
    a record's content without that edge is invisible to it -- by design, since
    the structural check catches that case (see test_a_content_only_leak_...).
    """
    file_iri = rdflib.URIRef(record.split("#", 1)[0])
    return triples_about(F.shared_graph(), record) + [
        (file_iri, rdflib.URIRef(VCFC + "hasRecord"), rdflib.URIRef(record))]


def withhold(lines, record):
    """Drop `record` from N-Triples `lines` exactly as the engine would.

    That means everything it owns under the profile's ownership rule (its call
    and sample call, and every IRI beneath them), and every line that points at
    any of it -- so the result is structurally valid and only an oracle that
    knows which records should be there can tell anything is missing.
    """
    from vcf_rdfizer_policies.engine import Partition, _ends

    partition = Partition(F.shared_graph(), F.setup()[1])
    owned = partition.owned([record])
    kept = []
    for line in lines:
        subject, obj = _ends(line)
        if partition.contains(owned, subject) or (obj is not None and partition.contains(owned, obj)):
            continue
        kept.append(line)
    return kept


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class CheckViewTests(VerboseTestCase):
    """The in-memory checker on views written by evaluate."""

    @classmethod
    def setUpClass(cls):
        from vcf_rdfizer_policies.policy import policy_digest
        from vcf_rdfizer_policies.release import evaluate, write_release

        _, cls.profile, cls.vocabulary, rules = F.setup()
        cls.rules = list(rules)
        cls.work = Path(tempfile.mkdtemp())
        cls.views = {}
        for requester in ("gru", "alz", "clinical"):
            release = evaluate(F.shared_graph(), cls.rules, F.request(requester), cls.profile, cls.vocabulary)
            write_release(release, cls.work / requester, policies={r.policy for r in cls.rules},
                          digest=policy_digest(F.POLICY))
            cls.views[requester] = cls.work / requester

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.work, ignore_errors=True)

    def check(self, view, manifest, request, policy=F.POLICY):
        from vcf_rdfizer_policies.check import check_view

        return check_view(view, manifest, request, policy_path=policy, rules=self.rules, profile=self.profile,
                          vocabulary=self.vocabulary, source=F.shared_graph())

    def read(self, requester):
        from vcf_rdfizer_policies.check import read_view

        return read_view(self.views[requester])

    def test_every_correct_view_passes(self):
        for requester in self.views:
            with self.subTest(requester=requester):
                self.assertEqual(self.check(*self.read(requester)), [])

    def test_prohibited_content_put_back_is_named_with_its_rule(self):
        view, manifest, request = self.read("gru")
        record = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        for triple in triples_about(F.shared_graph(), record):
            view.add(triple)
        failures = self.check(view, manifest, request)
        self.assertIn(f"prohibited content present: prohibition on <{BRCA1}> owns <{record}>", failures)

    def test_a_subject_no_permission_covers_is_reported(self):
        """P003 consented to health research only; the general-research view must not hold it."""
        view, manifest, request = self.read("gru")
        record = "file://P003.vcf#record/1"
        for triple in triples_about(F.shared_graph(), record):
            view.add(triple)
        self.assertIn(f"no permission covers <{record}>", self.check(view, manifest, request))

    def test_a_reference_to_a_removed_node_is_dangling(self):
        view, manifest, request = self.read("gru")
        record = sorted(F.expected_released("gru"))[0]
        call = str(next(F.shared_graph().objects(rdflib.URIRef(record), rdflib.URIRef(VCFC + "hasCall"))))
        for triple in list(view.triples((rdflib.URIRef(call), None, None))):
            view.remove(triple)
        self.assertIn(f"dangling reference: <{record}> -> <{call}>", self.check(view, manifest, request))

    def test_a_view_checked_against_another_policy_is_rejected(self):
        """The manifest's digest binds a view to the exact bytes it was made under."""
        view, manifest, request = self.read("gru")
        edited = self.work / "edited.ttl"
        edited.write_text(F.POLICY.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
        failures = self.check(view, manifest, request, policy=edited)
        self.assertTrue(any(f.startswith("view was produced under a different policy") for f in failures))

    def test_failures_of_one_kind_are_capped(self):
        """A badly broken view would otherwise print thousands of lines."""
        from vcf_rdfizer_policies.check import LIMIT

        view, manifest, request = self.read("gru")
        for n in range(1, 28):                      # every P003 record: 27 ungoverned subjects
            for triple in triples_about(F.shared_graph(), f"file://P003.vcf#record/{n}"):
                view.add(triple)
        uncovered = [f for f in self.check(view, manifest, request) if f.startswith("no permission covers")]
        self.assertEqual(len(uncovered), LIMIT)

    def test_read_manifest_recovers_the_request(self):
        from vcf_rdfizer_policies.check import read_manifest

        _, request = read_manifest(self.views["alz"])
        self.assertEqual(request, F.request("alz"))

    # --- the VCF-text oracle ---

    def compare(self, view, request):
        from vcf_rdfizer_policies.vcf_oracle import compare

        return compare(view, F.VCFS, rules=self.rules, request=request, profile=self.profile,
                       vocabulary=self.vocabulary)

    def test_the_oracle_agrees_with_every_correct_view(self):
        for requester in self.views:
            with self.subTest(requester=requester):
                view, _, request = self.read(requester)
                self.assertEqual(self.compare(view, request), [])

    def test_the_oracle_names_a_leaked_record(self):
        view, _, request = self.read("gru")
        record = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        for triple in record_with_its_edge(record):
            view.add(triple)
        self.assertEqual(self.compare(view, request),
                         [f"leak: <{record}> is released but the policy withholds it"])

    def test_a_content_only_leak_escapes_the_oracle_but_not_the_structural_check(self):
        """Why the tool has both layers, shown on one tampered view.

        Put a withheld record's content back without its file's hasRecord edge.
        The oracle counts records it can reach as reporting units, so it sees
        nothing wrong. The structural check looks at every subject, so it does.
        Neither layer is redundant: drop the structural check and this leak
        ships.
        """
        view, manifest, request = self.read("gru")
        record = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        for triple in triples_about(F.shared_graph(), record):
            view.add(triple)
        self.assertEqual(self.compare(view, request), [], "the oracle alone misses it")
        self.assertIn(f"prohibited content present: prohibition on <{BRCA1}> owns <{record}>",
                      self.check(view, manifest, request), "the structural check catches it")

    def test_the_oracle_names_an_over_withheld_record(self):
        view, _, request = self.read("gru")
        record = sorted(F.expected_released("gru"))[0]
        for triple in list(view.triples((rdflib.URIRef(record), None, None))):
            view.remove(triple)
        self.assertEqual(self.compare(view, request),
                         [f"over-withheld: <{record}> should have been released"])


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class OracleGraphTests(VerboseTestCase):
    """The oracle graph is built from VCF text, so its records must match the fixture's reading."""

    def test_it_holds_one_record_per_data_row(self):
        from vcf_rdfizer_policies.vcf_oracle import graph_from_vcfs

        graph = graph_from_vcfs(F.VCFS)
        records = {str(s) for s in graph.subjects(rdflib.RDF.type, rdflib.URIRef(VCFC + "VCFRecord"))}
        self.assertEqual(records, {iri for p in F.VCFS for iri, *_ in F.vcf_records(p)})

    def test_expected_records_is_the_policy_evaluated_on_the_oracle(self):
        from vcf_rdfizer_policies.vcf_oracle import expected_records, graph_from_vcfs

        _, profile, vocabulary, rules = F.setup()
        oracle = graph_from_vcfs(F.VCFS)
        for requester in ("gru", "alz", "clinical"):
            with self.subTest(requester=requester):
                self.assertEqual(expected_records(oracle, list(rules), F.request(requester), profile, vocabulary),
                                 F.expected_released(requester))


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class CheckStreamTests(VerboseTestCase):
    """The streamed checker, with source, view and oracle each served over HTTP."""

    @classmethod
    def setUpClass(cls):
        from vcf_rdfizer_policies.policy import policy_digest
        from vcf_rdfizer_policies.release import evaluate_stream
        from vcf_rdfizer_policies.store import MemoryStore

        _, cls.profile, cls.vocabulary, rules = F.setup()
        cls.rules = list(rules)
        cls.work = Path(tempfile.mkdtemp())
        cls.views = {}
        with serial():
            for requester in ("gru", "nobody"):
                request = (F.request(requester) if requester != "nobody" else
                           __import__("vcf_rdfizer_policies.engine", fromlist=["Request"]).Request(
                               "urn:anyone", cls.vocabulary.resolve("DUO:0000001")))
                evaluate_stream(MemoryStore(F.shared_graph()), F.RDF, cls.rules, request, cls.profile,
                                cls.vocabulary, cls.work / requester, policies={r.policy for r in cls.rules},
                                digest=policy_digest(F.POLICY))
                cls.views[requester] = cls.work / requester

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.work, ignore_errors=True)

    def view_graph(self, view_dir):
        text = gzip.open(Path(view_dir) / "view.nt.gz", "rt", encoding="utf-8").read()
        return rdflib.Graph().parse(data=text, format="nt")

    def tampered(self, name, edit):
        """A copy of the gru view whose view.nt.gz lines have been passed through `edit`."""
        target = F.copy_view(self.views["gru"], self.work / name)
        lines = F.read_gzip_lines(target / "view.nt.gz")
        F.gzip_text(target / "view.nt.gz", "".join(edit(lines)))
        return target

    def check(self, view_dir, *, store=None, view_store="auto", oracle=None):
        from vcf_rdfizer_policies.check import check_stream
        from vcf_rdfizer_policies.store import MemoryStore

        if view_store == "auto":
            view_store = MemoryStore(self.view_graph(view_dir))
        return check_stream(view_dir, policy_path=F.POLICY, rules=self.rules, profile=self.profile,
                            vocabulary=self.vocabulary, store=store or MemoryStore(F.shared_graph()),
                            view_store=view_store, oracle=oracle)

    def oracle(self):
        from vcf_rdfizer_policies.store import MemoryStore
        from vcf_rdfizer_policies.vcf_oracle import graph_from_vcfs

        return MemoryStore(graph_from_vcfs(F.VCFS))

    def test_a_correct_streamed_view_passes_with_the_oracle(self):
        self.assertEqual(self.check(self.views["gru"], oracle=self.oracle()), [])

    def test_it_passes_end_to_end_over_http(self):
        """Source, view and oracle each behind their own endpoint, as the CLI runs it."""
        from vcf_rdfizer_policies.store import EndpointStore
        from vcf_rdfizer_policies.vcf_oracle import graph_from_vcfs

        with F.SparqlEndpoint(F.shared_graph()) as source, \
                F.SparqlEndpoint(self.view_graph(self.views["gru"])) as view, \
                F.SparqlEndpoint(graph_from_vcfs(F.VCFS)) as oracle:
            failures = self.check(self.views["gru"], store=EndpointStore(source.url),
                                  view_store=EndpointStore(view.url), oracle=EndpointStore(oracle.url))
            self.assertEqual(failures, [])
            self.assertGreater(view.queries, 0)
            self.assertGreater(oracle.queries, 0)

    def test_prohibited_content_in_the_stream_is_named(self):
        record = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        extra = [f"<{record}> <{VCFC}chrom> \"chr17\" .\n"]
        failures = self.check(self.tampered("prohibited", lambda lines: lines + extra))
        self.assertIn(f"prohibited content present: prohibition on <{BRCA1}> owns <{record}>", failures)

    def test_a_prohibited_object_is_caught_even_under_a_permitted_subject(self):
        """A released record may not point at a withheld one."""
        record = sorted(F.expected_released("gru"))[0]
        withheld = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        extra = [f"<{record}> <urn:seeAlso> <{withheld}> .\n"]
        failures = self.check(self.tampered("prohibited-object", lambda lines: lines + extra))
        self.assertIn(f"prohibited content present: prohibition on <{BRCA1}> owns <{withheld}>", failures)

    def test_an_ungoverned_subject_in_the_stream_is_reported(self):
        extra = ['<file://P003.vcf#record/1> <urn:p> "x" .\n']
        failures = self.check(self.tampered("ungoverned", lambda lines: lines + extra))
        self.assertIn("no permission covers <file://P003.vcf#record/1>", failures)

    def test_a_dangling_reference_in_the_stream_is_reported(self):
        record = sorted(F.expected_released("gru"))[0]
        call = str(next(F.shared_graph().objects(rdflib.URIRef(record), rdflib.URIRef(VCFC + "hasCall"))))
        failures = self.check(self.tampered("dangling", lambda lines: [
            line for line in lines if not line.startswith(f"<{call}>")]))
        self.assertIn(f"dangling reference to <{call}>", failures)

    def test_a_view_endpoint_serving_something_else_is_refused(self):
        """Checking against the wrong endpoint would check the wrong view, and pass it."""
        from vcf_rdfizer_policies.store import MemoryStore

        lines = len(F.read_gzip_lines(self.views["gru"] / "view.nt.gz"))
        failures = self.check(self.views["gru"], view_store=MemoryStore(rdflib.Graph()))
        self.assertEqual(failures[-1], f"the view endpoint serves 0 triples; view.nt.gz has {lines:,} lines")

    def test_blank_lines_are_neither_triples_nor_a_count_mismatch(self):
        """A view with blank lines serves the same triples, so it must still pass.

        Counting them would put the file's line count above the endpoint's
        triple count, and a correct view would fail the endpoint guard.
        """
        target = self.tampered("blank-lines", lambda lines: ["\n"] + lines + ["\n", "   \n"])
        self.assertEqual(self.check(target), [])

    def test_a_non_empty_view_needs_a_view_endpoint(self):
        with self.assertRaises(PolicyError) as caught:
            self.check(self.views["gru"], view_store=None)
        self.assertIn("--view-endpoint", str(caught.exception))

    def test_an_empty_view_needs_no_view_endpoint_and_passes(self):
        """Nothing is released, so nothing can dangle and no endpoint can index it."""
        self.assertEqual(F.read_gzip_lines(self.views["nobody"] / "view.nt.gz"), [])
        self.assertEqual(self.check(self.views["nobody"], view_store=None), [])

    def test_the_streamed_oracle_names_a_leak(self):
        record = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        lines = [f"{s.n3()} {p.n3()} {o.n3()} .\n" for s, p, o in record_with_its_edge(record)]
        failures = self.check(self.tampered("leak", lambda existing: existing + lines), oracle=self.oracle())
        self.assertIn(f"leak: <{record}> is released but the policy withholds it", failures)

    def test_the_streamed_oracle_names_an_over_withheld_record(self):
        record = sorted(F.expected_released("gru"))[0]
        target = self.tampered("over-withheld", lambda lines: withhold(lines, record))
        self.assertEqual(self.check(target), [], "structurally the view is still valid")
        self.assertEqual(self.check(target, oracle=self.oracle()),
                         [f"over-withheld: <{record}> should have been released"],
                         "only the oracle, which reads the VCF text, can see the record is missing")


if __name__ == "__main__":
    unittest.main()
