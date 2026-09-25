"""Read an ODRL policy file into rules, rejecting anything v0.1.0 cannot evaluate.

The supported subset is docs/policy-demonstrator.md §3. Everything outside it --
an unknown selector, an effect other than drop, a conflict strategy other than
deny-wins, an unrecognised property on a rule -- raises PolicyError. Silently
ignoring a rule would be worse than refusing the policy.
"""

from dataclasses import dataclass
import hashlib
from pathlib import Path

from . import ODRL, VCFP, PolicyError
from .purposes import purpose_iri

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
POLICY_CLASSES = {ODRL + name for name in ("Policy", "Set", "Offer", "Agreement")}
#: Properties a rule may carry. Anything else (odrl:refinement, odrl:remedy,
#: ...) could change what the rule means, so it is refused, not dropped.
RULE_PROPERTIES = {RDF_TYPE, ODRL + "target", ODRL + "action", ODRL + "assignee",
                   ODRL + "assigner", ODRL + "constraint", ODRL + "duty"}


@dataclass(frozen=True)
class FileTarget:
    """A whole converted file, named by its file IRI, e.g. <file://P003.vcf>."""
    iri: str


@dataclass(frozen=True)
class RegionTarget:
    """Records with POS in [start, end] on chrom, 1-based and inclusive."""
    asset: str
    assembly: str
    chrom: str
    start: int
    end: int


@dataclass(frozen=True)
class VariantTarget:
    """Records with exactly this chrom, pos, ref and alt."""
    asset: str
    assembly: str
    chrom: str
    pos: int
    ref: str
    alt: str


@dataclass(frozen=True)
class Constraint:
    """A purpose constraint: odrl:isAnyOf or odrl:isNoneOf a set of DUO terms."""
    operator: str
    purposes: frozenset


@dataclass(frozen=True)
class Rule:
    kind: str                 # "permission" or "prohibition"
    policy: str               # the policy IRI the rule belongs to
    target: object            # FileTarget | RegionTarget | VariantTarget
    assignee: str = None      # None means odrl:All
    constraints: tuple = ()
    duties: tuple = ()        # ODRL action IRIs; recorded, not enforced

    @property
    def label(self) -> str:
        """Short, stable name for reports: kind and target."""
        target = getattr(self.target, "asset", None) or self.target.iri
        return f"{self.kind} on <{target}>"


def policy_digest(path: Path) -> str:
    """sha256 of the policy file's bytes, as recorded in every manifest."""
    return "sha256:" + hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_policy(path: Path):
    """Parse and validate a policy file; returns (rdflib graph, [Rule, ...])."""
    import rdflib

    graph = rdflib.Graph().parse(str(path), format="turtle")
    policies = {s for s, o in graph.subject_objects(rdflib.RDF.type) if str(o) in POLICY_CLASSES}
    if not policies:
        raise PolicyError(f"{path}: no odrl:Policy, Set, Offer or Agreement found")
    rules = []
    for policy in sorted(policies, key=str):
        conflict = graph.value(policy, rdflib.URIRef(ODRL + "conflict"))
        if str(conflict) != ODRL + "prohibit":
            raise PolicyError(f"<{policy}>: v0.1.0 requires odrl:conflict odrl:prohibit (deny wins)")
        if graph.value(policy, rdflib.URIRef(ODRL + "obligation")) is not None:
            raise PolicyError(f"<{policy}>: odrl:obligation is not supported in v0.1.0")
        for kind in ("permission", "prohibition"):
            for node in graph.objects(policy, rdflib.URIRef(ODRL + kind)):
                rules.append(_rule(graph, str(policy), kind, node))
    return graph, rules


def _rule(graph, policy, kind, node):
    import rdflib

    unknown = {str(p) for p in graph.predicates(node)} - RULE_PROPERTIES
    if unknown:
        raise PolicyError(f"{kind} in <{policy}> uses unsupported properties: {sorted(unknown)}")
    if str(graph.value(node, rdflib.URIRef(ODRL + "action"))) != ODRL + "read":
        raise PolicyError(f"{kind} in <{policy}>: the only supported action is odrl:read")
    assignee = graph.value(node, rdflib.URIRef(ODRL + "assignee"))
    return Rule(
        kind=kind,
        policy=policy,
        target=_target(graph, graph.value(node, rdflib.URIRef(ODRL + "target"))),
        assignee=None if assignee is None or str(assignee) == ODRL + "All" else str(assignee),
        constraints=tuple(_constraint(graph, c) for c in graph.objects(node, rdflib.URIRef(ODRL + "constraint"))),
        duties=tuple(sorted(_duty(graph, d) for d in graph.objects(node, rdflib.URIRef(ODRL + "duty")))),
    )


def _target(graph, node):
    import rdflib

    if node is None:
        raise PolicyError("a rule has no odrl:target")
    selectors = list(graph.objects(node, rdflib.URIRef(VCFP + "selector")))
    if not selectors:
        iri = str(node)
        if isinstance(node, rdflib.URIRef) and iri.startswith("file://") and "#" not in iri:
            return FileTarget(iri)
        raise PolicyError(f"target <{node}> is neither a file IRI nor a vcfp:GraphSelection")
    if len(selectors) != 1:
        raise PolicyError(f"<{node}>: a GraphSelection needs exactly one vcfp:selector")
    selector = selectors[0]
    kind = str(graph.value(selector, rdflib.RDF.type) or "")

    def get(name, cast=str):
        value = graph.value(selector, rdflib.URIRef(VCFP + name))
        if value is None:
            raise PolicyError(f"<{node}>: selector is missing vcfp:{name}")
        return cast(value)

    if kind == VCFP + "RegionSelector":
        target = RegionTarget(str(node), get("assembly"), get("chrom"), get("start", int), get("end", int))
        if target.start > target.end:
            raise PolicyError(f"<{node}>: vcfp:start is after vcfp:end")
        return target
    if kind == VCFP + "VariantSelector":
        return VariantTarget(str(node), get("assembly"), get("chrom"), get("pos", int), get("ref"), get("alt"))
    raise PolicyError(f"<{node}>: selector type {kind or '(none)'} is not supported in v0.1.0")


def _constraint(graph, node):
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
    return Constraint(operator.rsplit("/", 1)[1], frozenset(purpose_iri(str(v)) for v in values))


def _duty(graph, node):
    import rdflib

    transform = graph.value(node, rdflib.URIRef(VCFP + "transform"))
    if transform is not None and str(transform) != VCFP + "drop":
        raise PolicyError(f"effect <{transform}> is not supported in v0.1.0; only vcfp:drop")
    return str(graph.value(node, rdflib.URIRef(ODRL + "action")))
