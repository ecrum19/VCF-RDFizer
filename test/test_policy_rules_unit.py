"""Reading an ODRL policy into rules, and refusing what the engine cannot evaluate.

policy.py's contract is that a rule is either evaluated exactly as written or
the policy is refused. A rule that is parsed and then quietly not applied would
leave the operator believing a release is governed when it is not, so every
refusal below is a safety property: each one names a construct that changes
what a rule means, and checks that the loader stops rather than dropping it.
"""

import unittest

from test import policy_fixtures as F
from test.helpers import VerboseTestCase

try:
    import rdflib
except ModuleNotFoundError:  # pragma: no cover - exercised only without rdflib
    rdflib = None

from vcf_rdfizer_policies import ODRL, PolicyError

PREFIXES = """
@prefix odrl: <http://www.w3.org/ns/odrl/2/> .
@prefix vcfp: <https://w3id.org/vcf-rdfizer/policy#> .
@prefix vcfl: <https://w3id.org/vcf-rdfizer/linking#> .
@prefix obo:  <http://purl.obolibrary.org/obo/> .
@prefix ex:   <https://example.org/p/> .
"""

#: One valid permission; tests replace the rule body to make it invalid one way.
RULE = ("odrl:target <file://A.vcf> ; odrl:action odrl:read ; odrl:assignee odrl:All ; "
        "odrl:constraint [ odrl:leftOperand odrl:purpose ; odrl:operator odrl:isAnyOf ; "
        "odrl:rightOperand obo:DUO_0000042 ]")


def policy(rule=RULE, kind="permission", extra="", conflict="odrl:conflict odrl:prohibit ;", head=""):
    return (PREFIXES + extra +
            f"\nex:set a odrl:Set ; {conflict} {head} odrl:{kind} [ {rule} ] .\n")


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class LoadRulesTests(VerboseTestCase):
    def load(self, text):
        from vcf_rdfizer_policies.policy import load_rules

        _, profile, vocabulary, _ = F.setup()
        graph = rdflib.Graph().parse(data=text, format="turtle")
        # The profile must see the policy's own selector declarations, as the CLI arranges.
        from vcf_rdfizer_policies.profile import load_profile
        return load_rules(graph, load_profile([], extra_graph=graph), vocabulary)

    def refuses(self, text, fragment):
        with self.assertRaises(PolicyError) as caught:
            self.load(text)
        self.assertIn(fragment, str(caught.exception))

    # --- what a valid rule becomes ---

    def test_a_valid_permission_is_read_faithfully(self):
        (rule,) = self.load(policy())
        self.assertEqual(rule.kind, "permission")
        self.assertEqual(rule.policy, "https://example.org/p/set")
        self.assertEqual(rule.target.iri, "file://A.vcf")
        self.assertIsNone(rule.assignee, "odrl:All means anyone, which the engine models as None")
        (constraint,) = rule.constraints
        self.assertEqual(constraint.operator, "isAnyOf")
        self.assertEqual(constraint.purposes, frozenset({"http://purl.obolibrary.org/obo/DUO_0000042"}))
        self.assertEqual(rule.label, "permission on <file://A.vcf>")

    def test_a_named_assignee_is_kept(self):
        (rule,) = self.load(policy(RULE.replace("odrl:All", "<https://example.org/party/x>")))
        self.assertEqual(rule.assignee, "https://example.org/party/x")

    def test_duties_are_recorded_sorted(self):
        (rule,) = self.load(policy(RULE + " ; odrl:duty [ odrl:action odrl:inform ] , "
                                          "[ odrl:action odrl:attribute ]"))
        self.assertEqual(rule.duties, (ODRL + "attribute", ODRL + "inform"))

    def test_the_demo_policy_loads_every_rule(self):
        """Seven rules: five consents, one withdrawal, two cohort prohibitions -- minus none."""
        _, _, _, rules = F.setup()
        kinds = sorted(r.kind for r in rules)
        self.assertEqual(kinds, ["permission"] * 5 + ["prohibition"] * 3)

    # --- refusals: each construct would change what the rule means ---

    def test_a_graph_with_no_policy_is_refused(self):
        self.refuses(PREFIXES + "ex:x a odrl:Asset .", "no odrl:Policy")

    def test_deny_wins_must_be_declared(self):
        """The engine only implements deny-wins; a policy asking for anything else is refused."""
        self.refuses(policy(conflict=""), "odrl:conflict odrl:prohibit")
        self.refuses(policy(conflict="odrl:conflict odrl:perm ;"), "odrl:conflict odrl:prohibit")

    def test_an_obligation_is_refused(self):
        self.refuses(policy(head="odrl:obligation [ odrl:action odrl:delete ] ;"), "odrl:obligation")

    def test_an_unrecognised_rule_property_is_refused_not_dropped(self):
        """odrl:refinement narrows a rule. Dropping it would widen what is released."""
        self.refuses(policy(RULE + " ; odrl:refinement [ odrl:leftOperand odrl:count ]"),
                     "unsupported properties")

    def test_an_action_other_than_read_is_refused(self):
        self.refuses(policy(RULE.replace("odrl:read", "odrl:distribute")), "only supported action")

    def test_a_rule_without_a_target_is_refused(self):
        self.refuses(policy(RULE.replace("odrl:target <file://A.vcf> ; ", "")), "has no odrl:target")

    def test_a_blank_target_without_a_selector_is_refused(self):
        self.refuses(policy(RULE.replace("<file://A.vcf>", "[ a odrl:Asset ]")),
                     "must be an IRI or a vcfp:GraphSelection")

    def test_a_selection_needs_exactly_one_selector(self):
        two = ('ex:sel a vcfp:GraphSelection ; '
               'vcfp:selector [ a vcfp:RegionSelector ; vcfp:assembly "GRCh38" ; vcfp:chrom "1" ; '
               'vcfp:start 1 ; vcfp:end 2 ] , '
               '[ a vcfp:RegionSelector ; vcfp:assembly "GRCh38" ; vcfp:chrom "2" ; '
               'vcfp:start 1 ; vcfp:end 2 ] .\n')
        self.refuses(policy(RULE.replace("<file://A.vcf>", "ex:sel"), extra=two), "exactly one vcfp:selector")

    def test_an_undeclared_selector_type_is_refused(self):
        sel = 'ex:sel a vcfp:GraphSelection ; vcfp:selector [ a vcfp:MysterySelector ] .\n'
        self.refuses(policy(RULE.replace("<file://A.vcf>", "ex:sel"), extra=sel), "is not declared by the profile")

    def test_a_missing_selector_parameter_is_refused(self):
        """A region with no end would otherwise select open-ended: refused instead."""
        sel = ('ex:sel a vcfp:GraphSelection ; vcfp:selector [ a vcfp:RegionSelector ; '
               'vcfp:assembly "GRCh38" ; vcfp:chrom "1" ; vcfp:start 1 ] .\n')
        self.refuses(policy(RULE.replace("<file://A.vcf>", "ex:sel"), extra=sel), "needs <")

    def test_a_list_parameter_becomes_a_tuple_of_its_members(self):
        """vcfp:entities is an RDF list: a gene panel is a set of genes, not one gene."""
        panel = ('ex:panel a vcfp:GraphSelection ; vcfp:selector [ a vcfp:LinkedSelector ; '
                 'vcfp:predicate vcfl:overlapsGene ; '
                 'vcfp:entities ( <https://example.org/gene/G2> <https://example.org/gene/G1> ) ] .\n')
        (rule,) = self.load(policy(RULE.replace("<file://A.vcf>", "ex:panel"), extra=panel))
        bindings = dict(rule.target.bindings)
        self.assertEqual(tuple(str(v) for v in bindings["entities"]),
                         ("https://example.org/gene/G2", "https://example.org/gene/G1"),
                         "list order is preserved; the VALUES block carries every member")
        self.assertEqual(str(bindings["predicate"]), "https://w3id.org/vcf-rdfizer/linking#overlapsGene")

    def test_an_empty_list_parameter_is_refused(self):
        """An empty panel would select nothing, and a prohibition on nothing protects nothing."""
        panel = ('ex:panel a vcfp:GraphSelection ; vcfp:selector [ a vcfp:LinkedSelector ; '
                 'vcfp:predicate vcfl:overlapsGene ; vcfp:entities () ] .\n')
        self.refuses(policy(RULE.replace("<file://A.vcf>", "ex:panel"), extra=panel), "is an empty list")

    def test_a_non_purpose_constraint_is_refused(self):
        self.refuses(policy(RULE.replace("odrl:leftOperand odrl:purpose", "odrl:leftOperand odrl:spatial")),
                     "only odrl:purpose constraints")

    def test_an_unsupported_operator_is_refused(self):
        self.refuses(policy(RULE.replace("odrl:isAnyOf", "odrl:eq")), "only odrl:isAnyOf and odrl:isNoneOf")

    def test_a_constraint_with_no_right_operand_is_refused(self):
        self.refuses(policy(RULE.replace(" ; odrl:rightOperand obo:DUO_0000042", "")),
                     "no odrl:rightOperand")

    def test_a_purpose_outside_the_vocabulary_is_refused(self):
        self.refuses(policy(RULE.replace("obo:DUO_0000042", "obo:DUO_9999999")),
                     "not a term of the purpose vocabulary")

    def test_a_duty_with_a_transform_other_than_drop_is_refused(self):
        self.refuses(policy(RULE + " ; odrl:duty [ odrl:action odrl:anonymize ; vcfp:transform vcfp:hash ]"),
                     "only vcfp:drop")


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class PolicyDigestTests(VerboseTestCase):
    def test_the_digest_is_the_sha256_of_the_bytes_and_moves_with_one_byte(self):
        import hashlib
        import tempfile
        from pathlib import Path
        from vcf_rdfizer_policies.policy import policy_digest

        with tempfile.TemporaryDirectory() as work:
            path = Path(work) / "p.ttl"
            path.write_bytes(b"# a\n")
            self.assertEqual(policy_digest(path), "sha256:" + hashlib.sha256(b"# a\n").hexdigest())
            before = policy_digest(path)
            path.write_bytes(b"# b\n")
            self.assertNotEqual(policy_digest(path), before)


if __name__ == "__main__":
    unittest.main()
