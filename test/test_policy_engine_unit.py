"""The engine's selection, ownership, decision and view rules.

Properties pinned here, each of which a plausible edit could break silently:

* **Fail closed on preconditions.** A region selector whose coordinates are on
  another assembly, or a linked selector evaluated without its link graph,
  selects nothing -- and a prohibition that selects nothing releases what it
  was meant to protect. Both must stop evaluation instead.
* **Ownership batching changes nothing.** Roots are sent to the store in
  batches of BATCH; the owned set must not depend on where the batches fall.
* **Deny wins, and default is deny**, with the reason naming the rule.
* **A view never points at something it does not contain.**
"""

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

from vcf_rdfizer_policies import PolicyError
from vcf_rdfizer_policies import engine
from vcf_rdfizer_policies.engine import Request, _ends, applies, units

VCFC = "https://w3id.org/vcf-core/vocab#"
LINK = "https://w3id.org/vcf-rdfizer/linking#overlapsGene"


def rules_from(text):
    from vcf_rdfizer_policies.policy import load_rules
    from vcf_rdfizer_policies.profile import load_profile

    _, _, vocabulary, _ = F.setup()
    graph = rdflib.Graph().parse(data=text, format="turtle")
    profile = load_profile([], extra_graph=graph)
    return load_rules(graph, profile, vocabulary), profile, vocabulary


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class AppliesTests(VerboseTestCase):
    """Whether a rule binds a request: assignee, then every purpose constraint."""

    def rule(self, assignee=None, constraints=()):
        from vcf_rdfizer_policies.policy import Direct, Rule

        return Rule("permission", "urn:p", Direct("urn:t"), assignee, tuple(constraints))

    def constraint(self, operator, *terms):
        from vcf_rdfizer_policies.policy import Constraint

        return Constraint(operator, frozenset(f"http://purl.obolibrary.org/obo/{t}" for t in terms))

    def setUp(self):
        _, _, self.vocabulary, _ = F.setup()

    def ask(self, rule, purpose, assignee="urn:me"):
        return applies(rule, Request(assignee, f"http://purl.obolibrary.org/obo/{purpose}"), self.vocabulary)

    def test_a_rule_for_someone_else_does_not_bind(self):
        self.assertFalse(self.ask(self.rule(assignee="urn:them"), "DUO_0000042"))
        self.assertTrue(self.ask(self.rule(assignee="urn:me"), "DUO_0000042"))

    def test_is_any_of_holds_for_the_term_and_anything_narrower(self):
        rule = self.rule(constraints=[self.constraint("isAnyOf", "DUO_0000042")])
        self.assertTrue(self.ask(rule, "DUO_0000042"))
        self.assertTrue(self.ask(rule, "DUO_0000007"), "disease-specific is within general research")
        self.assertFalse(self.ask(rule, "DUO_0000043"), "clinical care is on another branch")

    def test_is_none_of_is_the_complement(self):
        rule = self.rule(constraints=[self.constraint("isNoneOf", "DUO_0000007")])
        self.assertFalse(self.ask(rule, "DUO_0000007"))
        self.assertTrue(self.ask(rule, "DUO_0000042"), "broader than the term, so not within it")

    def test_every_constraint_must_hold(self):
        rule = self.rule(constraints=[self.constraint("isAnyOf", "DUO_0000042"),
                                      self.constraint("isNoneOf", "DUO_0000007")])
        self.assertTrue(self.ask(rule, "DUO_0000006"))
        self.assertFalse(self.ask(rule, "DUO_0000007"))


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class PreconditionTests(VerboseTestCase):
    REGION = """
ex:r a vcfp:GraphSelection ; vcfp:selector [ a vcfp:RegionSelector ;
    vcfp:assembly "{assembly}" ; vcfp:chrom "chr17" ; vcfp:start 1 ; vcfp:end 99999999 ] .
ex:set a odrl:Set ; odrl:conflict odrl:prohibit ;
    odrl:prohibition [ odrl:target ex:r ; odrl:action odrl:read ] .
"""
    PANEL = """
ex:panel a vcfp:GraphSelection ; vcfp:selector [ a vcfp:LinkedSelector ;
    vcfp:predicate vcfl:overlapsGene ; vcfp:entities ( <https://example.org/gene/G1> ) ] .
ex:set a odrl:Set ; odrl:conflict odrl:prohibit ;
    odrl:prohibition [ odrl:target ex:panel ; odrl:action odrl:read ] .
"""

    def prefixed(self, body):
        from test.test_policy_rules_unit import PREFIXES
        return PREFIXES + body

    def test_a_region_on_another_assembly_stops_evaluation(self):
        """GRCh37 coordinates on GRCh38 files would select the wrong records, or none."""
        rules, _, _ = rules_from(self.prefixed(self.REGION.format(assembly="GRCh37")))
        with self.assertRaises(PolicyError) as caught:
            engine.check_preconditions(F.shared_graph(), rules)
        message = str(caught.exception)
        self.assertIn("cannot be applied to this graph", message)
        self.assertIn("GRCh38", message, "the reason shows what the file actually declares")

    def test_the_matching_assembly_passes(self):
        rules, _, _ = rules_from(self.prefixed(self.REGION.format(assembly="GRCh38")))
        engine.check_preconditions(F.shared_graph(), rules)      # no exception

    def test_a_linked_selector_without_its_link_graph_stops_evaluation(self):
        """The fail-closed case the profile's comment describes.

        Without link triples the selector would select nothing, and a panel
        prohibition would release every record in the panel.
        """
        rules, _, _ = rules_from(self.prefixed(self.PANEL))
        with self.assertRaises(PolicyError) as caught:
            engine.check_preconditions(F.shared_graph(), rules)
        self.assertIn(LINK, str(caught.exception))

    def test_with_its_link_graph_it_passes_and_selects_the_linked_records(self):
        rules, _, _ = rules_from(self.prefixed(self.PANEL))
        graph = rdflib.Graph()
        graph += F.shared_graph()
        call = rdflib.URIRef("file://P001.vcf#call/3")
        graph.add((call, rdflib.URIRef(LINK), rdflib.URIRef("https://example.org/gene/G1")))
        graph.add((rdflib.URIRef("file://P001.vcf#call/4"), rdflib.URIRef(LINK),
                   rdflib.URIRef("https://example.org/gene/OTHER")))
        engine.check_preconditions(graph, rules)
        self.assertEqual(engine.select(graph, rules[0].target), {"file://P001.vcf#record/3"},
                         "only the record whose call links to a panel member")

    def test_a_direct_target_has_no_precondition_and_selects_itself(self):
        from vcf_rdfizer_policies.policy import Direct

        _, _, _, rules = F.setup()
        direct = [r for r in rules if isinstance(r.target, Direct)]
        engine.check_preconditions(F.shared_graph(), direct)
        self.assertEqual(engine.select(F.shared_graph(), direct[0].target), {direct[0].target.iri})


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class PartitionTests(VerboseTestCase):
    def setUp(self):
        _, self.profile, _, _ = F.setup()
        self.partition = engine.Partition(F.shared_graph(), self.profile)
        self.roots = sorted(F.expected_released("alz"))

    def test_ownership_follows_the_profile_path_from_record_to_call(self):
        owned = self.partition.owned(["file://P001.vcf#record/1"])
        self.assertIn("file://P001.vcf#record/1", owned)
        self.assertIn("file://P001.vcf#call/1", owned, "a record owns its call via vcfc:hasCall")

    def test_the_owned_set_does_not_depend_on_the_batch_size(self):
        """76 roots in one batch, in batches of 7, and one at a time, must agree."""
        whole = self.partition.owned(self.roots)
        for size in (7, 1):
            with self.subTest(batch=size), mock.patch.object(engine, "BATCH", size):
                self.assertEqual(self.partition.owned(self.roots), whole)

    def test_with_iri_subtree_a_resource_owns_the_iris_beneath_it(self):
        owned = frozenset({"file://P001.vcf#record/1"})
        self.assertTrue(self.partition.contains(owned, "file://P001.vcf#record/1/allele/0"))
        self.assertFalse(self.partition.contains(owned, "file://P001.vcf#record/10"),
                         "record/10 is a sibling, not a descendant, of record/1")

    def test_without_iri_subtree_only_exact_membership_counts(self):
        flat = engine.Partition(F.shared_graph(), _Profile(iri_subtree=False))
        owned = frozenset({"file://P001.vcf#record/1"})
        self.assertTrue(flat.contains(owned, "file://P001.vcf#record/1"))
        self.assertFalse(flat.contains(owned, "file://P001.vcf#record/1/allele/0"))

    def test_without_an_ownership_path_roots_own_only_themselves(self):
        flat = engine.Partition(F.shared_graph(), _Profile(iri_subtree=False))
        self.assertEqual(flat.owned(["file://P001.vcf#record/1"]), frozenset({"file://P001.vcf#record/1"}))


class _Profile:
    """The fields Partition reads, for profiles the bundled one cannot express."""

    def __init__(self, iri_subtree, ownership_path=None, unit_query=None, node_space=()):
        self.iri_subtree, self.ownership_path = iri_subtree, ownership_path
        self.unit_query, self.node_space = unit_query, node_space


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class DecisionTests(VerboseTestCase):
    def decided(self, requester):
        _, profile, vocabulary, rules = F.setup()
        return engine.evaluation(F.shared_graph(), list(rules), F.request(requester), profile, vocabulary)

    def test_a_withdrawal_overrides_its_own_permission(self):
        """P004 consented to general research, then withdrew: deny wins."""
        released, reason = self.decided("gru").decide("file://P004.vcf")
        self.assertFalse(released)
        self.assertEqual(reason, "withheld: prohibition on <file://P004.vcf>")

    def test_a_region_prohibition_withholds_a_record_inside_a_permitted_file(self):
        record = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        released, reason = self.decided("gru").decide(record)
        self.assertFalse(released)
        self.assertIn("brca1", reason)

    def test_the_same_record_is_released_for_the_purpose_the_prohibition_exempts(self):
        record = F.first_record(F.DEMO / "P001.vcf", F.in_brca1)
        self.assertEqual(self.decided("clinical").decide(record), (True, "released: permission on <file://P001.vcf>"))

    def test_with_no_permission_the_default_is_deny(self):
        released, reason = self.decided("gru").decide("file://P003.vcf")
        self.assertFalse(released)
        self.assertEqual(reason, "withheld: no permission covers it for this purpose")

    def test_released_agrees_with_decide_on_every_record_of_the_cohort(self):
        """The streaming path's fast lookup must never disagree with the reasoned one."""
        for requester in ("gru", "alz", "clinical"):
            decided = self.decided(requester)
            for path in F.VCFS:
                for iri, *_ in F.vcf_records(path):
                    with self.subTest(requester=requester, record=iri):
                        self.assertEqual(decided.released(iri), decided.decide(iri)[0])


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class ViewTests(VerboseTestCase):
    def test_a_view_never_points_at_a_node_it_does_not_contain(self):
        _, profile, vocabulary, rules = F.setup()
        graph = F.shared_graph()
        nodes = {str(s) for s in graph.subjects()}
        for requester in ("gru", "alz", "clinical"):
            decided = engine.evaluation(graph, list(rules), F.request(requester), profile, vocabulary)
            kept, withheld = engine.view(graph, decided)
            present = {str(s) for s, _, _ in kept}
            with self.subTest(requester=requester):
                dangling = {str(o) for _, _, o in kept if str(o) in nodes and str(o) not in present}
                self.assertEqual(dangling, set())
                self.assertEqual(len(kept) + withheld, len(graph), "every triple is either kept or counted")


class EndsTests(VerboseTestCase):
    """Reading one N-Triples line without a parser, for the streaming path."""

    def test_an_iri_object(self):
        self.assertEqual(_ends("<urn:s> <urn:p> <urn:o> .\n"), ("urn:s", "urn:o"))

    def test_a_literal_object_is_none(self):
        self.assertEqual(_ends('<urn:s> <urn:p> "a <b> c" .\n'), ("urn:s", None))

    def test_a_typed_literal_is_still_a_literal(self):
        self.assertEqual(_ends('<urn:s> <urn:p> "1"^^<urn:int> .\n'), ("urn:s", None))

    def test_a_non_iri_subject_is_refused(self):
        with self.assertRaises(PolicyError):
            _ends('_:b0 <urn:p> "x" .\n')

    def test_a_blank_node_object_is_refused(self):
        """A blank node has no IRI to decide on, so a stream cannot govern it."""
        with self.assertRaises(PolicyError):
            _ends("<urn:s> <urn:p> _:b0 .\n")


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class StreamGuardTests(VerboseTestCase):
    def test_streaming_refuses_a_profile_without_a_node_space(self):
        """Without it a stream cannot tell a node of the graph from an outside IRI."""
        with tempfile.TemporaryDirectory() as work:
            with self.assertRaises(PolicyError):
                engine.stream_view([F.RDF[0]], object(), (), Path(work) / "v.nt.gz")

    def test_a_profile_without_a_unit_query_has_no_units(self):
        self.assertEqual(units(F.shared_graph(), _Profile(iri_subtree=False)), [])

    def test_units_are_sorted_by_group_then_resource(self):
        _, profile, _, _ = F.setup()
        found = units(F.shared_graph(), profile)
        self.assertEqual(len(found), 139)
        self.assertEqual(found, sorted(found, key=lambda u: (u["group"], u["resource"])))


if __name__ == "__main__":
    unittest.main()
