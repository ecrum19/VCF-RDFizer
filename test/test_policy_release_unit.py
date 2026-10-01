"""Evaluating a request, writing its release, and attaching policies, on the demo cohort.

Every expected outcome here comes from test/policy_fixtures.py, which derives
it from the VCF text and a hand-written reading of policy.ttl. None of it is
recorded from the engine's own output, so these tests check the engine rather
than restate it.

The streaming path is held to the in-memory one: `evaluate_stream` against an
endpoint must release exactly the triples, records, groups and duties that
`evaluate` does on the same graph.
"""

import csv
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from test import policy_fixtures as F
from test.helpers import VerboseTestCase

try:
    import rdflib
except ModuleNotFoundError:  # pragma: no cover - exercised only without rdflib
    rdflib = None

from vcf_rdfizer_policies import DISCLOSURE_MODEL, ODRL, VCFP, PolicyError

REQUESTERS = ("gru", "alz", "clinical")
ATTRIBUTE = ODRL + "attribute"


def serial():
    """stream_view forks one worker per input; keep it in-process beside a live server thread."""
    return mock.patch("vcf_rdfizer_policies.engine.os.cpu_count", return_value=1)


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class EvaluateTests(VerboseTestCase):
    def release(self, requester):
        from vcf_rdfizer_policies.release import evaluate

        _, profile, vocabulary, rules = F.setup()
        return evaluate(F.shared_graph(), list(rules), F.request(requester), profile, vocabulary)

    def test_each_request_releases_exactly_the_records_the_policy_allows(self):
        for requester in REQUESTERS:
            with self.subTest(requester=requester):
                release = self.release(requester)
                released = {u["resource"] for u, ok, _ in release.units if ok}
                self.assertEqual(released, F.expected_released(requester))

    def test_each_request_releases_exactly_the_files_the_consents_allow(self):
        for requester in REQUESTERS:
            with self.subTest(requester=requester):
                groups = {g.split("//", 1)[1] for g, (ok, _) in self.release(requester).groups.items() if ok}
                self.assertEqual(groups, F.RELEASED_FILES[requester])

    def test_every_unit_is_decided_and_every_triple_is_accounted_for(self):
        release = self.release("gru")
        self.assertEqual(len(release.units), 139)
        self.assertEqual(release.triples_released, len(release.view))
        self.assertEqual(release.triples_released + release.triples_withheld, len(F.shared_graph()))

    def test_duties_are_those_of_permissions_that_released_something(self):
        for requester in REQUESTERS:
            with self.subTest(requester=requester):
                self.assertEqual(self.release(requester).duties, (ATTRIBUTE,))

    def test_a_request_no_permission_covers_releases_nothing_and_owes_nothing(self):
        """DUO_0000001 is broader than every consent, so nothing is within it."""
        from vcf_rdfizer_policies.engine import Request
        from vcf_rdfizer_policies.release import evaluate

        _, profile, vocabulary, rules = F.setup()
        release = evaluate(F.shared_graph(), list(rules),
                           Request("urn:anyone", vocabulary.resolve("DUO:0000001")), profile, vocabulary)
        self.assertEqual(release.view, [])
        self.assertEqual(release.duties, ())
        self.assertFalse(any(ok for _, ok, _ in release.units))


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class SummaryTests(VerboseTestCase):
    def test_the_counts_add_up(self):
        from vcf_rdfizer_policies.release import evaluate, summary

        _, profile, vocabulary, rules = F.setup()
        counts = summary(evaluate(F.shared_graph(), list(rules), F.request("alz"), profile, vocabulary))
        self.assertEqual(counts["records_released"], len(F.expected_released("alz")))
        self.assertEqual(counts["records_released"] + counts["records_withheld"], 139)
        self.assertEqual(sum(counts["reasons"].values()), 139, "every unit has exactly one reason")
        self.assertEqual(sum(g["records_released"] + g["records_withheld"] for g in counts["groups"].values()), 139)
        self.assertEqual(counts["request"]["purpose"], "http://purl.obolibrary.org/obo/DUO_0000007")


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class WriteReleaseTests(VerboseTestCase):
    def setUp(self):
        from vcf_rdfizer_policies.policy import policy_digest
        from vcf_rdfizer_policies.release import evaluate

        _, profile, vocabulary, rules = F.setup()
        self.rules = list(rules)
        self.release = evaluate(F.shared_graph(), self.rules, F.request("gru"), profile, vocabulary)
        self.written = {"policies": {r.policy for r in self.rules}, "digest": policy_digest(F.POLICY)}
        self.work = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.work, ignore_errors=True))

    def write(self, name="view"):
        from vcf_rdfizer_policies.release import write_release

        write_release(self.release, self.work / name, **self.written)
        return self.work / name

    def test_it_writes_the_four_release_files(self):
        out = self.write()
        self.assertEqual(sorted(p.name for p in out.iterdir()),
                         ["decisions.csv", "manifest.ttl", "summary.json", "view.nt"])

    def test_the_view_round_trips_through_read_view(self):
        from vcf_rdfizer_policies.check import read_view

        view, _, request = read_view(self.write())
        self.assertEqual(set(view), set(self.release.view))
        self.assertEqual(request, F.request("gru"))

    def test_the_view_file_is_deterministic(self):
        """Sorted lines, so two writes of one release are byte-identical and diffable."""
        self.assertEqual((self.write("a") / "view.nt").read_bytes(), (self.write("b") / "view.nt").read_bytes())

    def test_decisions_csv_has_one_row_per_unit_with_its_reason(self):
        with (self.write() / "decisions.csv").open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 139)
        released = {r["resource"] for r in rows if r["released"] == "True"}
        self.assertEqual(released, F.expected_released("gru"))
        self.assertTrue(all(r["reason"].startswith(("released:", "withheld:")) for r in rows))

    def test_the_manifest_records_what_made_the_view(self):
        from vcf_rdfizer_policies.policy import policy_digest

        manifest = rdflib.Graph().parse(str(self.write() / "manifest.ttl"), format="turtle")
        vcfp = rdflib.Namespace(VCFP)
        (node,) = manifest.subjects(rdflib.RDF.type, vcfp.ReleaseView)
        self.assertEqual(str(manifest.value(node, vcfp.policyDigest)), policy_digest(F.POLICY))
        self.assertEqual(str(manifest.value(node, vcfp.disclosureModel)), DISCLOSURE_MODEL,
                         "every manifest says this is governed release, not anonymization")
        self.assertEqual(int(manifest.value(node, vcfp.recordsReleased)), len(F.expected_released("gru")))
        self.assertEqual(int(manifest.value(node, vcfp.groupsWithheld)), 3, "P003, P004 and P005")
        self.assertEqual({str(o) for o in manifest.objects(node, vcfp.derivedFrom)},
                         {f"file://{p.name}" for p in F.VCFS})
        (obligation,) = manifest.objects(node, vcfp.obligation)
        self.assertEqual(str(manifest.value(obligation, rdflib.URIRef(ODRL + "action"))), ATTRIBUTE)

    def test_a_release_is_never_written_over(self):
        self.write()
        with self.assertRaises(FileExistsError):
            self.write()

    def test_an_existing_empty_directory_is_used(self):
        (self.work / "empty").mkdir()
        self.write("empty")
        self.assertTrue((self.work / "empty" / "view.nt").exists())

    def test_reports_with_no_units_still_have_a_header(self):
        from vcf_rdfizer_policies.release import Release, write_reports

        out = self.work / "bare"
        out.mkdir()
        write_reports(Release(F.request("gru")), out, **self.written)
        self.assertEqual((out / "decisions.csv").read_text(encoding="utf-8").splitlines(),
                         ["resource,group,released,reason"])


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class EvaluateStreamTests(VerboseTestCase):
    """The streaming path, through a real HTTP endpoint, held to the in-memory one."""

    def stream(self, requester, store):
        from vcf_rdfizer_policies.policy import policy_digest
        from vcf_rdfizer_policies.release import evaluate_stream

        _, profile, vocabulary, rules = F.setup()
        out = Path(tempfile.mkdtemp()) / "view"
        self.addCleanup(lambda: __import__("shutil").rmtree(out.parent, ignore_errors=True))
        with serial():
            release = evaluate_stream(store, F.RDF, list(rules), F.request(requester), profile, vocabulary, out,
                                      policies={r.policy for r in rules}, digest=policy_digest(F.POLICY))
        return release, out

    def test_it_releases_what_the_in_memory_path_releases(self):
        from vcf_rdfizer_policies.release import evaluate
        from vcf_rdfizer_policies.store import EndpointStore

        _, profile, vocabulary, rules = F.setup()
        with F.SparqlEndpoint(F.shared_graph()) as endpoint:
            for requester in REQUESTERS:
                with self.subTest(requester=requester):
                    memory = evaluate(F.shared_graph(), list(rules), F.request(requester), profile, vocabulary)
                    streamed, out = self.stream(requester, EndpointStore(endpoint.url))
                    lines = rdflib.Graph().parse(
                        data=gzip.open(out / "view.nt.gz", "rt", encoding="utf-8").read(), format="nt")
                    self.assertEqual(set(lines), set(memory.view), "the same triples")
                    self.assertEqual(streamed.units, memory.units, "the same per-record decisions")
                    self.assertEqual(streamed.groups, memory.groups)
                    self.assertEqual(streamed.duties, memory.duties)
                    self.assertEqual((streamed.triples_released, streamed.triples_withheld),
                                     (len(memory.view), memory.triples_withheld))
            self.assertGreater(endpoint.queries, 0, "the decisions really came from the endpoint")

    def test_it_writes_view_nt_gz_and_the_reports(self):
        from vcf_rdfizer_policies.store import MemoryStore

        _, out = self.stream("gru", MemoryStore(F.shared_graph()))
        self.assertEqual(sorted(p.name for p in out.iterdir()),
                         ["decisions.csv", "manifest.ttl", "summary.json", "view.nt.gz"])
        summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["records_released"], len(F.expected_released("gru")))

    def test_a_streamed_release_is_never_written_over(self):
        from vcf_rdfizer_policies.store import MemoryStore

        _, out = self.stream("gru", MemoryStore(F.shared_graph()))
        from vcf_rdfizer_policies.policy import policy_digest
        from vcf_rdfizer_policies.release import evaluate_stream

        _, profile, vocabulary, rules = F.setup()
        with self.assertRaises(FileExistsError), serial():
            evaluate_stream(MemoryStore(F.shared_graph()), F.RDF, list(rules), F.request("gru"), profile,
                            vocabulary, out, policies=set(), digest=policy_digest(F.POLICY))


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class AttachTests(VerboseTestCase):
    def setUp(self):
        from vcf_rdfizer_policies.release import attach

        self.policy_graph, _, _, rules = F.setup()
        self.rules = list(rules)
        self.graph = F.cohort_graph()          # attach mutates its graph
        self.counts = attach(self.graph, self.policy_graph, self.rules)

    def test_counts_name_each_asset_and_how_many_resources_it_governs(self):
        brca1 = sum(F.in_brca1(*f) for p in F.VCFS for _, *f in F.vcf_records(p))
        apoe = sum(F.is_apoe_e4(*f) for p in F.VCFS for _, *f in F.vcf_records(p))
        expected = {f"file://{p.name}": 1 for p in F.VCFS}
        expected.update({"https://example.org/policy/demo-cohort/brca1": brca1,
                         "https://example.org/policy/demo-cohort/apoe-e4": apoe})
        self.assertEqual(self.counts, expected)

    def test_each_selected_record_points_at_its_policy(self):
        has_policy = rdflib.URIRef(ODRL + "hasPolicy")
        cohort = rdflib.URIRef("https://example.org/policy/demo-cohort/cohort")
        record = rdflib.URIRef(F.first_record(F.DEMO / "P001.vcf", F.in_brca1))
        self.assertIn((record, has_policy, cohort), self.graph)

    def test_a_selection_records_what_it_selects(self):
        """So a SPARQL query over the attached graph needs no knowledge of the selectors."""
        selects = rdflib.URIRef(VCFP + "selects")
        asset = rdflib.URIRef("https://example.org/policy/demo-cohort/apoe-e4")
        selected = {str(o) for o in self.graph.objects(asset, selects)}
        expected = {iri for p in F.VCFS for iri, *f in F.vcf_records(p) if F.is_apoe_e4(*f)}
        self.assertEqual(selected, expected)

    def test_the_policies_themselves_are_merged_into_the_graph(self):
        self.assertIn((rdflib.URIRef("https://example.org/policy/demo-cohort/cohort"), rdflib.RDF.type,
                       rdflib.URIRef(ODRL + "Set")), self.graph)

    def test_attach_runs_the_preconditions_before_changing_anything(self):
        """A graph the policy cannot govern must be refused, not half-annotated."""
        from vcf_rdfizer_policies.release import attach

        graph = F.cohort_graph()
        file_iri = rdflib.URIRef("file://P001.vcf")
        graph.set((file_iri, rdflib.URIRef("https://w3id.org/vcf-core/vocab#referenceGenome"),
                   rdflib.Literal("GRCh37")))
        before = len(graph)
        with self.assertRaises(PolicyError):
            attach(graph, self.policy_graph, self.rules)
        self.assertEqual(len(graph), before, "nothing was written before the refusal")


if __name__ == "__main__":
    unittest.main()
