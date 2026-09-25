"""Read an ODRL policy file into rules, refusing anything the engine cannot evaluate.

Supported (docs/policy-demonstrator.md §3): odrl:permission and odrl:prohibition
with action odrl:read; a target that is either a resource IRI or a
vcfp:GraphSelection whose selector type the profile declares; an assignee;
purpose constraints with odrl:isAnyOf / odrl:isNoneOf; duties, which are
recorded but not enforced; and odrl:conflict odrl:prohibit. Anything else --
an unknown selector type, a missing parameter, an effect other than drop, an
unrecognised property on a rule -- raises PolicyError. Silently ignoring a rule
would be worse than refusing the policy.
"""

from dataclasses import dataclass
import hashlib
from pathlib import Path

from . import ODRL, VCFP, PolicyError
from .profile import variable

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
POLICY_CLASSES = {ODRL + name for name in ("Policy", "Set", "Offer", "Agreement")}
#: Properties a rule may carry. Anything else (odrl:refinement, odrl:remedy, ...)
#: could change what the rule means, so it is refused rather than dropped.
RULE_PROPERTIES = {RDF_TYPE, ODRL + "target", ODRL + "action", ODRL + "assignee",
                   ODRL + "assigner", ODRL + "constraint", ODRL + "duty"}


@dataclass(frozen=True)
class Direct:
    """A target that is one resource, e.g. <file://P003.vcf>."""
    iri: str


@dataclass(frozen=True)
class Selection:
    """A target computed by a declared selector type with these parameter bindings."""
    asset: str
    selector: object        # profile.SelectorType
    bindings: tuple         # ((variable name, rdflib term), ...)


@dataclass(frozen=True)
class Constraint:
    operator: str           # "isAnyOf" or "isNoneOf"
    purposes: frozenset     # term IRIs


@dataclass(frozen=True)
class Rule:
    kind: str               # "permission" or "prohibition"
    policy: str             # the IRI of the policy the rule belongs to
    target: object          # Direct | Selection
    assignee: str = None    # None means odrl:All
    constraints: tuple = ()
    duties: tuple = ()      # ODRL action IRIs; recorded, not enforced

    @property
    def label(self) -> str:
        """Short, stable name for reports: kind and target."""
        return f"{self.kind} on <{getattr(self.target, 'asset', None) or self.target.iri}>"


def policy_digest(path: Path) -> str:
    """sha256 of the policy file's bytes, as recorded in every manifest."""
    return "sha256:" + hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_graph(path: Path):
    import rdflib

    return rdflib.Graph().parse(str(path), format="turtle")


def load_rules(graph, profile, vocabulary) -> list:
    """Every rule of every policy in `graph`, validated against the profile and vocabulary."""
    import rdflib

    policies = {s for s, o in graph.subject_objects(rdflib.RDF.type) if str(o) in POLICY_CLASSES}
    if not policies:
        raise PolicyError("no odrl:Policy, Set, Offer or Agreement found")
    rules = []
    for policy in sorted(policies, key=str):
        if str(graph.value(policy, rdflib.URIRef(ODRL + "conflict"))) != ODRL + "prohibit":
            raise PolicyError(f"<{policy}>: odrl:conflict odrl:prohibit (deny wins) is required")
        if graph.value(policy, rdflib.URIRef(ODRL + "obligation")) is not None:
            raise PolicyError(f"<{policy}>: odrl:obligation is not supported")
        for kind in ("permission", "prohibition"):
            for node in graph.objects(policy, rdflib.URIRef(ODRL + kind)):
                rules.append(_rule(graph, str(policy), kind, node, profile, vocabulary))
    return rules


def _rule(graph, policy, kind, node, profile, vocabulary):
    import rdflib

    where = f"{kind} in <{policy}>"
    unknown = {str(p) for p in graph.predicates(node)} - RULE_PROPERTIES
    if unknown:
        raise PolicyError(f"{where} uses unsupported properties: {sorted(unknown)}")
    if str(graph.value(node, rdflib.URIRef(ODRL + "action"))) != ODRL + "read":
        raise PolicyError(f"{where}: the only supported action is odrl:read")
    assignee = graph.value(node, rdflib.URIRef(ODRL + "assignee"))
    constraints = tuple(_constraint(graph, c, vocabulary)
                        for c in graph.objects(node, rdflib.URIRef(ODRL + "constraint")))
    return Rule(kind, policy, _target(graph, graph.value(node, rdflib.URIRef(ODRL + "target")), profile, where),
                None if assignee is None or str(assignee) == ODRL + "All" else str(assignee),
                constraints,
                tuple(sorted(_duty(graph, d) for d in graph.objects(node, rdflib.URIRef(ODRL + "duty")))))


def _target(graph, node, profile, where):
    import rdflib

    if node is None:
        raise PolicyError(f"{where} has no odrl:target")
    selectors = list(graph.objects(node, rdflib.URIRef(VCFP + "selector")))
    if not selectors:
        if not isinstance(node, rdflib.URIRef):
            raise PolicyError(f"{where}: a target must be an IRI or a vcfp:GraphSelection")
        return Direct(str(node))
    if len(selectors) != 1:
        raise PolicyError(f"<{node}>: a GraphSelection needs exactly one vcfp:selector")
    kind = str(graph.value(selectors[0], rdflib.RDF.type) or "")
    if kind not in profile.selectors:
        raise PolicyError(f"<{node}>: selector type <{kind or '(none)'}> is not declared by the profile")
    selector = profile.selectors[kind]
    bindings = []
    for prop in selector.parameters:
        value = graph.value(selectors[0], rdflib.URIRef(prop))
        if value is None:
            raise PolicyError(f"<{node}>: a {kind.rsplit('#', 1)[-1]} needs <{prop}>")
        bindings.append((variable(prop), value))
    return Selection(str(node), selector, tuple(bindings))


def _constraint(graph, node, vocabulary):
    import rdflib

    left = str(graph.value(node, rdflib.URIRef(ODRL + "leftOperand")))
    operator = str(graph.value(node, rdflib.URIRef(ODRL + "operator")))
    if left != ODRL + "purpose":
        raise PolicyError(f"only odrl:purpose constraints are supported, not <{left}>")
    if operator not in (ODRL + "isAnyOf", ODRL + "isNoneOf"):
        raise PolicyError(f"only odrl:isAnyOf and odrl:isNoneOf are supported, not <{operator}>")
    values = list(graph.objects(node, rdflib.URIRef(ODRL + "rightOperand")))
    if not values:
        raise PolicyError("a purpose constraint has no odrl:rightOperand")
    return Constraint(operator.rsplit("/", 1)[1], frozenset(vocabulary.resolve(str(v)) for v in values))


def _duty(graph, node):
    import rdflib

    transform = graph.value(node, rdflib.URIRef(VCFP + "transform"))
    if transform is not None and str(transform) != VCFP + "drop":
        raise PolicyError(f"effect <{transform}> is not supported; only vcfp:drop")
    return str(graph.value(node, rdflib.URIRef(ODRL + "action")))
