"""The engine: select, partition, decide (docs/policy-demonstrator.md §4).

Nothing here knows about VCF. A rule's target selects resources (a declared
SPARQL selector, or one IRI); the profile's ownership rule extends each
selection to everything it owns; and a resource is released when some binding
permission owns it and no binding prohibition does.
"""

from dataclasses import dataclass

from . import PolicyError
from .graphs import ancestors
from .policy import Direct


@dataclass(frozen=True)
class Request:
    assignee: str
    purpose: str       # a term IRI of the purpose vocabulary


def applies(rule, request, vocabulary) -> bool:
    """Does the rule bind this request: assignee matches, and every constraint holds?"""
    if rule.assignee is not None and rule.assignee != request.assignee:
        return False
    for constraint in rule.constraints:
        inside = any(vocabulary.within(request.purpose, term) for term in constraint.purposes)
        if inside != (constraint.operator == "isAnyOf"):
            return False
    return True


def select(graph, target) -> set:
    """The resources a target selects in `graph`."""
    import rdflib

    if isinstance(target, Direct):
        return {rdflib.URIRef(target.iri)}
    rows = graph.query(target.selector.query, initBindings=dict(target.bindings))
    return {row.resource for row in rows}


def check_preconditions(graph, rules) -> None:
    """Run each selection's declared violations query; any row stops evaluation."""
    for rule in rules:
        selector = getattr(rule.target, "selector", None)
        if selector is None or selector.violations is None:
            continue
        rows = list(graph.query(selector.violations, initBindings=dict(rule.target.bindings)))
        if rows:
            shown = "; ".join(" ".join(str(v) for v in row if v is not None) for row in rows[:3])
            raise PolicyError(f"{rule.label} cannot be applied to this graph: {shown}")


class Partition:
    """The profile's ownership rule, applied to one graph."""

    def __init__(self, graph, profile):
        self.graph, self.profile = graph, profile

    def owned(self, roots) -> frozenset:
        """The roots and everything their ownership path reaches (IRI subtrees are implicit)."""
        found = set(roots)
        if self.profile.ownership_path:
            query = (f"SELECT DISTINCT ?owned WHERE {{ ?root {self.profile.ownership_path} ?owned }}")
            for root in roots:
                found.update(row.owned for row in self.graph.query(query, initBindings={"root": root}))
        return frozenset(found)

    def contains(self, owned, term) -> bool:
        """Is `term` in `owned`, or (with iriSubtree) beneath an IRI that is?"""
        if term in owned:
            return True
        if not self.profile.iri_subtree or not hasattr(term, "startswith"):
            return False
        from rdflib import URIRef

        return any(URIRef(a) in owned for a in ancestors(str(term)))


@dataclass
class Evaluation:
    """Per-rule ownership, and the release decision for any term."""
    partition: Partition
    binding: list                 # (rule, owned) for every rule that binds the request

    def decide(self, term):
        """(released, reason) for one resource; deny wins, and default-deny."""
        for rule, owned in self.binding:
            if rule.kind == "prohibition" and self.partition.contains(owned, term):
                return False, f"withheld: {rule.label}"
        for rule, owned in self.binding:
            if rule.kind == "permission" and self.partition.contains(owned, term):
                return True, f"released: {rule.label}"
        return False, "withheld: no permission covers it for this purpose"


def evaluation(graph, rules, request, profile, vocabulary) -> Evaluation:
    check_preconditions(graph, rules)
    partition = Partition(graph, profile)
    binding = [(rule, partition.owned(select(graph, rule.target)))
               for rule in rules if applies(rule, request, vocabulary)]
    return Evaluation(partition, binding)


def view(graph, evaluation) -> tuple:
    """(released triples, number withheld). A triple is released when its subject is,
    and when its object, if it is a node of the graph, is released too -- so a view
    never points at something it does not contain."""
    import rdflib

    nodes = set(graph.subjects())
    cache = {}

    def released(term):
        if term not in cache:
            cache[term] = evaluation.decide(term)[0]
        return cache[term]

    kept, withheld = [], 0
    for s, p, o in graph:
        if released(s) and (not isinstance(o, (rdflib.URIRef, rdflib.BNode)) or o not in nodes or released(o)):
            kept.append((s, p, o))
        else:
            withheld += 1
    return kept, withheld


def units(graph, profile) -> list:
    """The profile's reporting units: dicts with 'resource', 'group' and any other columns."""
    if profile.unit_query is None:
        return []
    rows = graph.query(profile.unit_query)
    names = [str(v) for v in rows.vars]
    found = [{n: row[n] for n in names} for row in rows]
    return sorted(found, key=lambda u: (str(u["group"]), str(u["resource"])))
