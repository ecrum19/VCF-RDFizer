"""The v0.1.0 policy demonstrator: profile, decisions, release views and their check.

The decision tests pin docs/policy-demonstrator.md §4.1 cell by cell. The
release tests run the real committed fixture graphs in both sample profiles.
The mutation tests break a correct view in each way a redactor can go wrong
and require `check` to say so -- a check that has never caught a planted
leak is not evidence of anything.
"""

import contextlib
import filecmp
import importlib.util
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest

try:
    import rdflib
except ModuleNotFoundError:  # pragma: no cover - exercised only without rdflib
    rdflib = None

from test.helpers import VerboseTestCase

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / "examples" / "policy"
POLICY = EXAMPLE / "policy.ttl"
VCFS = sorted(EXAMPLE.glob("P00*.vcf"))
FIXTURE = json.loads((EXAMPLE / "fixture.json").read_text(encoding="utf-8"))
DUO = "http://purl.obolibrary.org/obo/DUO_"

PREFIXES = """@prefix odrl: <http://www.w3.org/ns/odrl/2/> .
@prefix vcfp: <https://w3id.org/vcf-rdfizer/policy#> .
@prefix obo:  <http://purl.obolibrary.org/obo/> .
@prefix ex:   <https://example.org/> .
"""


def request(key):
    from vcf_rdfizer_policies.decide import Request

    spec = FIXTURE["requesters"][key]
    return Request(spec["assignee"], DUO + spec["purpose"].split("_")[1])


def write_policy(directory, body, conflict="odrl:prohibit"):
    """A one-policy file around `body` (the policy's rules), for rejection tests."""
    path = Path(directory) / "policy.ttl"
    path.write_text(f"{PREFIXES}\nex:p a odrl:Set ; odrl:conflict {conflict} ;\n{body} .\n", encoding="utf-8")
    return path


FILE_PERMISSION = """odrl:permission [ odrl:target <file://P001.vcf> ; odrl:action odrl:read ]"""


@unittest.skipIf(rdflib is None, "rdflib is required")
class PurposeTests(VerboseTestCase):
    def test_every_accepted_spelling_names_the_same_term(self):
        from vcf_rdfizer_policies.purposes import purpose_iri

        for spelling in ("DUO:0000007", "DUO_0000007", "obo:DUO_0000007", DUO + "0000007"):
            self.assertEqual(purpose_iri(spelling), DUO + "0000007")

    def test_a_term_outside_the_bundled_subset_is_refused(self):
        from vcf_rdfizer_policies import PolicyError
        from vcf_rdfizer_policies.purposes import purpose_iri

        with self.assertRaises(PolicyError):
            purpose_iri("DUO:0000019")
        with self.assertRaises(PolicyError):
            purpose_iri("GRU")

    def test_narrower_purposes_fall_within_broader_consents_and_not_the_reverse(self):
        from vcf_rdfizer_policies.purposes import within

        gru, hmb, ds, cc = (DUO + n for n in ("0000042", "0000006", "0000007", "0000043"))
        self.assertTrue(within(ds, gru) and within(ds, hmb) and within(hmb, gru))
        self.assertFalse(within(gru, hmb) or within(hmb, ds))
        self.assertFalse(within(cc, gru) or within(gru, cc))


@unittest.skipIf(rdflib is None, "rdflib is required")
class ProfileTests(VerboseTestCase):
    def test_the_example_policy_loads_as_eight_rules(self):
        from vcf_rdfizer_policies.profile import FileTarget, RegionTarget, VariantTarget, load_policy

        _, rules = load_policy(POLICY)
        kinds = sorted((r.kind, type(r.target).__name__) for r in rules)
        self.assertEqual(kinds.count(("permission", "FileTarget")), 5)
        self.assertEqual(kinds.count(("prohibition", "FileTarget")), 1)      # P004's withdrawal
        self.assertIn(("prohibition", "RegionTarget"), kinds)
        self.assertIn(("prohibition", "VariantTarget"), kinds)
        region = next(r.target for r in rules if isinstance(r.target, RegionTarget))
        self.assertEqual((region.chrom, region.start, region.end), ("chr17", 43044295, 43125483))
        self.assertTrue(all(isinstance(r.target, (FileTarget, RegionTarget, VariantTarget)) for r in rules))

    def test_anything_outside_the_v010_subset_is_refused(self):
        from vcf_rdfizer_policies import PolicyError
        from vcf_rdfizer_policies.profile import load_policy

        region = ('ex:a a vcfp:GraphSelection ; vcfp:selector [ a vcfp:RegionSelector ; '
                  'vcfp:chrom "chr1" ; vcfp:start 1 ; vcfp:end 2 {extra} ] .\n')
        cases = {
            "deny-wins is required": (FILE_PERMISSION, "odrl:perm"),
            "only read": (FILE_PERMISSION.replace("odrl:read", "odrl:distribute"), "odrl:prohibit"),
            "unknown rule property": (FILE_PERMISSION[:-1] + " ; odrl:refinement [] ]", "odrl:prohibit"),
            "unsupported effect": (FILE_PERMISSION[:-1] + " ; odrl:duty [ odrl:action odrl:anonymize ; "
                                   "vcfp:transform vcfp:generalize ] ]", "odrl:prohibit"),
            "non-DUO purpose": (FILE_PERMISSION[:-1] + " ; odrl:constraint [ odrl:leftOperand odrl:purpose ; "
                                "odrl:operator odrl:isAnyOf ; odrl:rightOperand ex:x ] ]", "odrl:prohibit"),
            "target is not a file": (FILE_PERMISSION.replace("<file://P001.vcf>", "ex:thing"), "odrl:prohibit"),
        }
        for name, (body, conflict) in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as td:
                with self.assertRaises(PolicyError):
                    load_policy(write_policy(td, body, conflict))

        selector_cases = {
            "missing assembly": region.format(extra=""),
            "start after end": region.format(extra='; vcfp:assembly "GRCh38"').replace("vcfp:end 2", "vcfp:end 0"),
            "unsupported selector": region.format(extra='; vcfp:assembly "GRCh38"').replace(
                "RegionSelector", "SampleSelector"),
        }
        for name, asset in selector_cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as td:
                path = write_policy(td, "odrl:prohibition [ odrl:target ex:a ; odrl:action odrl:read ]")
                path.write_text(path.read_text() + asset, encoding="utf-8")
                with self.assertRaises(PolicyError):
                    load_policy(path)


@unittest.skipIf(rdflib is None, "rdflib is required")
class DecisionTests(VerboseTestCase):
    """The §4.1 grid, and the boundary cases the fixture plants."""

    @classmethod
    def setUpClass(cls):
        from vcf_rdfizer_policies.profile import load_policy

        _, cls.rules = load_policy(POLICY)

    def decide(self, subject, key):
        from vcf_rdfizer_policies.decide import decide

        return decide(subject, self.rules, request(key)).released

    def record(self, file="P001", chrom="chr1", pos=1, ref="A", alt="C"):
        from vcf_rdfizer_policies.graphs import Record

        return Record(f"file://{file}.vcf", 1, chrom, pos, ref, (alt,))

    def test_files_follow_each_participants_consent(self):
        grid = {"gru": "✓✓———", "alz": "✓✓✓—✓", "clinical": "✓✓———"}
        for key, expected in grid.items():
            got = "".join("✓" if self.decide(f"file://P00{n}.vcf", key) else "—" for n in range(1, 6))
            self.assertEqual(got, expected, key)

    def test_the_region_and_variant_rules_each_exempt_one_purpose(self):
        brca1 = self.record(chrom="chr17", pos=43100000)
        e4 = self.record(chrom="chr19", pos=44908684, ref="T", alt="C")
        self.assertEqual([self.decide(brca1, k) for k in ("gru", "alz", "clinical")], [False, False, True])
        self.assertEqual([self.decide(e4, k) for k in ("gru", "alz", "clinical")], [False, True, False])

    def test_region_bounds_are_inclusive(self):
        at = lambda pos: self.decide(self.record(chrom="chr17", pos=pos), "gru")  # noqa: E731
        self.assertEqual([at(43044294), at(43044295), at(43125483), at(43125484)],
                         [True, False, False, True])

    def test_the_variant_rule_matches_alleles_not_just_position(self):
        decoy = self.record(chrom="chr19", pos=44908684, ref="T", alt="G")
        self.assertTrue(self.decide(decoy, "gru"))

    def test_withdrawal_overrides_every_permission(self):
        for key in ("gru", "alz", "clinical"):
            self.assertFalse(self.decide(self.record(file="P004"), key), key)

    def test_a_rule_for_one_party_does_not_bind_another(self):
        from dataclasses import replace
        from vcf_rdfizer_policies.decide import applies

        withdrawal = next(r for r in self.rules if r.kind == "prohibition" and hasattr(r.target, "iri"))
        only_alz = replace(withdrawal, assignee=FIXTURE["requesters"]["alz"]["assignee"])
        self.assertTrue(applies(only_alz, request("alz")))
        self.assertFalse(applies(only_alz, request("gru")))

    def test_an_assembly_mismatch_stops_evaluation(self):
        from vcf_rdfizer_policies import PolicyError
        from vcf_rdfizer_policies.decide import check_assemblies

        check_assemblies(self.rules, {"file://P001.vcf": "GRCh38"})
        with self.assertRaises(PolicyError):
            check_assemblies(self.rules, {"file://P001.vcf": "GRCh37"})


class FixtureViews(VerboseTestCase):
    """Shared setup: every requester's view of both fixture profiles. No tests of its own."""

    @classmethod
    def setUpClass(cls):
        from vcf_rdfizer_policies.graphs import load
        from vcf_rdfizer_policies.profile import load_policy, policy_digest
        from vcf_rdfizer_policies.release import evaluate, write_release

        cls.policy_graph, cls.rules = load_policy(POLICY)
        cls.tmp = tempfile.TemporaryDirectory()
        cls.graphs, cls.views = {}, {}
        for profile in ("expanded", "condensed"):
            graph = load(sorted((EXAMPLE / "converted" / profile).glob("*.nt.gz")))
            cls.graphs[profile] = graph
            for key in FIXTURE["requesters"]:
                release = evaluate(graph, cls.rules, request(key))
                out = Path(cls.tmp.name) / profile / key
                write_release(release, out, policies={r.policy for r in cls.rules}, digest=policy_digest(POLICY))
                cls.views[profile, key] = (release, out)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()


@unittest.skipIf(rdflib is None, "rdflib is required")
class ReleaseTests(FixtureViews):
    """Evaluate the committed fixture graphs, in both profiles, and check every view."""

    def test_every_view_passes_its_check(self):
        from vcf_rdfizer_policies.check import check_view

        for (profile, key), (_, out) in self.views.items():
            with self.subTest(profile=profile, requester=key):
                self.assertEqual(check_view(out, POLICY, self.rules, VCFS), [])

    def test_a_view_partitions_the_graph(self):
        for (profile, key), (release, _) in self.views.items():
            self.assertEqual(len(release.view) + release.triples_withheld, len(self.graphs[profile]))

    def test_the_withdrawn_file_leaves_nothing_behind(self):
        for (_, key), (release, _) in self.views.items():
            self.assertFalse([t for t in release.view if "P004.vcf" in str(t[0]) + str(t[2])], key)

    def test_the_manifest_records_the_request_and_what_was_withheld(self):
        release, out = self.views["expanded", "alz"]
        manifest = (out / "manifest.ttl").read_text(encoding="utf-8")
        for expected in ("governed release; not anonymization", "DUO_0000007", "odrl:attribute",
                         f"vcfp:recordsWithheld {sum(not d.released for _, d in release.decisions)}"):
            self.assertIn(expected, manifest)
        self.assertEqual(json.loads((out / "summary.json").read_text())["files"]["file://P004.vcf"]["released"], False)

    def test_a_release_is_never_written_over_another(self):
        from vcf_rdfizer_policies.release import write_release

        release, out = self.views["expanded", "gru"]
        with self.assertRaises(FileExistsError):
            write_release(release, out, policies=set(), digest="")

    def test_attach_makes_policies_queryable_alongside_the_data(self):
        from vcf_rdfizer_policies.release import attach

        graph = rdflib.Graph()
        for triple in self.graphs["expanded"]:
            graph.add(triple)
        counts = attach(graph, self.policy_graph, self.rules)
        self.assertEqual(counts["https://example.org/policy/demo-cohort/apoe-e4"], 4)   # P001, P003-P005
        # docs/policy-demonstrator.md §5: records a general-research collaborator may not receive.
        rows = graph.query("""
            PREFIX odrl: <http://www.w3.org/ns/odrl/2/> PREFIX vcfc: <https://w3id.org/vcf-core/vocab#>
            SELECT DISTINCT ?chrom WHERE { ?r a vcfc:VCFRecord ; vcfc:chrom ?chrom ; odrl:hasPolicy ?p .
                                           ?p odrl:prohibition ?rule . }""")
        self.assertEqual(sorted(str(c) for (c,) in rows), ["chr17", "chr19"])


@unittest.skipIf(rdflib is None, "rdflib is required")
class MutationTests(FixtureViews):
    """Break a correct view each way a redactor can fail; `check` must catch every one."""

    def mutate(self, key, edit, *, policy=POLICY):
        """Copy the expanded `key` view, apply `edit` to its view.nt lines, and check it."""
        from vcf_rdfizer_policies.check import check_view

        _, out = self.views["expanded", key]
        with tempfile.TemporaryDirectory() as td:
            copy = Path(td) / "view"
            shutil.copytree(out, copy)
            lines = (copy / "view.nt").read_text(encoding="utf-8").splitlines()
            (copy / "view.nt").write_text("\n".join(edit(lines)) + "\n", encoding="utf-8")
            return "\n".join(check_view(copy, policy, self.rules, VCFS))

    def full_graph_lines(self, needle):
        return [line for line in self.graphs["expanded"].serialize(format="nt").splitlines() if needle in line]

    def test_a_reinstated_withheld_region_record_is_a_leak(self):
        brca1 = FIXTURE["loci"]["brca1"]
        record = next(r for r, _ in self.views["expanded", "gru"][0].decisions
                      if r.file.endswith("P001.vcf") and r.chrom == "chr17" and brca1["start"] < r.pos < brca1["end"])
        subject = f"<{record.file}#record/{record.row}>"
        report = self.mutate("gru", lambda lines: lines + self.full_graph_lines(subject + " "))
        self.assertIn("leak", report)
        self.assertIn("prohibited content present", report)

    def test_a_restored_triple_of_the_withdrawn_file_is_caught(self):
        report = self.mutate("alz", lambda lines: lines + self.full_graph_lines("<file://P004.vcf> ")[:1])
        self.assertIn("P004.vcf", report)

    def test_a_deleted_released_record_is_over_withholding(self):
        report = self.mutate("alz", lambda lines: [l for l in lines if not l.startswith("<file://P003.vcf#record/1>")])
        self.assertIn("over-withheld: file://P003.vcf#record/1", report)

    def test_a_reference_to_a_missing_resource_is_dangling(self):
        report = self.mutate("alz", lambda lines: [l for l in lines if not l.startswith("<file://P003.vcf#record/1/allele/0>")])
        self.assertIn("dangling reference", report)

    def test_one_requesters_view_does_not_pass_as_anothers(self):
        _, alz = self.views["expanded", "alz"]
        alz_lines = (alz / "view.nt").read_text(encoding="utf-8").splitlines()
        self.assertIn("leak", self.mutate("gru", lambda _: alz_lines))

    def test_a_view_is_checked_against_the_policy_it_was_made_under(self):
        with tempfile.TemporaryDirectory() as td:
            changed = Path(td) / "policy.ttl"
            changed.write_text(POLICY.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
            self.assertIn("different policy", self.mutate("gru", lambda lines: lines, policy=changed))


@unittest.skipIf(rdflib is None, "rdflib is required")
class CommandTests(VerboseTestCase):
    def run_cli(self, *argv):
        from vcf_rdfizer_policy import main

        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main([str(a) for a in argv])
        return code, stdout.getvalue() + stderr.getvalue()

    def test_evaluate_then_check_round_trip(self):
        rdf = sorted((EXAMPLE / "converted" / "condensed").glob("*.nt.gz"))
        spec = FIXTURE["requesters"]["clinical"]
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "clinical"
            code, text = self.run_cli("evaluate", "--rdf", *rdf, "--policy", POLICY,
                                      "--assignee", spec["assignee"], "--purpose", "DUO:0000043", "-o", out)
            self.assertEqual(code, 0, text)
            code, text = self.run_cli("check", "--view", out, "--policy", POLICY, "--vcf", *VCFS)
            self.assertEqual((code, text.strip().splitlines()[-1]), (0, "PASS"))
            code, text = self.run_cli("evaluate", "--rdf", *rdf, "--policy", POLICY,
                                      "--assignee", spec["assignee"], "--purpose", "DUO:0000043", "-o", out)
            self.assertEqual(code, 2)                       # never overwrites

    def test_an_unsupported_policy_exits_2_with_the_reason(self):
        with tempfile.TemporaryDirectory() as td:
            path = write_policy(td, FILE_PERMISSION, conflict="odrl:perm")
            code, text = self.run_cli("explain", "--policy", path)
        self.assertEqual(code, 2)
        self.assertIn("deny wins", text)

    def test_explain_and_attach(self):
        code, text = self.run_cli("explain", "--policy", POLICY)
        self.assertEqual(code, 0)
        self.assertIn("prohibition on <https://example.org/policy/demo-cohort/brca1>", text)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "annotated.nt"
            code, _ = self.run_cli("attach", "--rdf", EXAMPLE / "converted" / "expanded" / "P001.nt.gz",
                                   "--policy", POLICY, "-o", out)
            self.assertEqual(code, 0)
            self.assertIn("http://www.w3.org/ns/odrl/2/hasPolicy", out.read_text(encoding="utf-8"))


class FixtureTests(VerboseTestCase):
    def test_the_committed_fixture_is_what_the_generator_writes(self):
        spec = importlib.util.spec_from_file_location("make_fixture", EXAMPLE / "make_fixture.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as td, contextlib.redirect_stdout(io.StringIO()):
            module.main(Path(td))
            names = [p.name for p in VCFS] + ["fixture.json"]
            match, mismatch, errors = filecmp.cmpfiles(EXAMPLE, td, names, shallow=False)
        self.assertEqual((mismatch, errors), ([], []))
        self.assertEqual(len(match), 6)


if __name__ == "__main__":
    unittest.main()
