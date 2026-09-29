"""The engine: select, partition, decide (docs/policy-demonstrator.md §4).

Nothing here knows about VCF. A rule's target selects resources (a declared
SPARQL selector, or one IRI); the profile's ownership rule extends each
selection to everything it owns; and a resource is released when some binding
permission owns it and no binding prohibition does.

Queries go to a store (store.py): an rdflib graph, or a SPARQL endpoint. Terms
are compared as plain strings, so both give the same decisions. `view` writes a
release from an in-memory graph; `stream_view` from N-Triples files, in one
pass whose memory is bounded by the selections rather than the graph.
"""

from dataclasses import dataclass, field
from functools import lru_cache
import gzip
import multiprocessing
import os
from pathlib import Path
import shutil
import tempfile

from . import PolicyError
from .graphs import ancestors
from .policy import Direct
from .store import as_store, iri, with_parameters

#: Roots per ownership query: enough to amortise a round trip, small enough for any endpoint.
BATCH = 500


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


def select(source, target) -> set:
    """The resource IRIs a target selects in `source` (a store or an rdflib graph)."""
    if isinstance(target, Direct):
        return {target.iri}
    query = with_parameters(target.selector.query, target.bindings)
    return {row["resource"] for row in as_store(source).rows(query)}


def check_preconditions(source, rules) -> None:
    """Run each selection's declared violations query; any row stops evaluation."""
    store = as_store(source)
    for rule in rules:
        selector = getattr(rule.target, "selector", None)
        if selector is None or selector.violations is None:
            continue
        rows = list(store.rows(with_parameters(selector.violations, rule.target.bindings)))
        if rows:
            shown = "; ".join(" ".join(v for v in row.values() if v is not None) for row in rows[:3])
            raise PolicyError(f"{rule.label} cannot be applied to this graph: {shown}")


class Partition:
    """The profile's ownership rule, applied to one store."""

    def __init__(self, source, profile):
        self.store, self.profile = as_store(source), profile

    def owned(self, roots) -> frozenset:
        """The roots and everything their ownership path reaches (IRI subtrees are implicit)."""
        found = set(roots)
        if self.profile.ownership_path:
            roots = sorted(roots)
            for start in range(0, len(roots), BATCH):
                values = " ".join(iri(r) for r in roots[start:start + BATCH])
                query = (f"SELECT DISTINCT ?owned WHERE {{ VALUES ?root {{ {values} }} "
                         f"?root {self.profile.ownership_path} ?owned }}")
                found.update(row["owned"] for row in self.store.rows(query))
        return frozenset(found)

    def contains(self, owned, term) -> bool:
        """Is `term` in `owned`, or (with iriSubtree) beneath an IRI that is?"""
        term = str(term)
        if term in owned:
            return True
        return self.profile.iri_subtree and any(a in owned for a in ancestors(term))


@dataclass
class Evaluation:
    """Per-rule ownership, and the release decision for any term."""
    partition: Partition
    binding: list                 # (rule, owned) for every rule that binds the request
    denied: frozenset = field(init=False)
    permitted: frozenset = field(init=False)

    def __post_init__(self):
        owned = lambda kind: frozenset().union(*(o for r, o in self.binding if r.kind == kind))
        self.denied, self.permitted = owned("prohibition"), owned("permission")

    def chain(self, term) -> tuple:
        """The IRIs whose ownership would cover `term`: itself, and with iriSubtree its ancestors."""
        return tuple(ancestors(term)) if self.partition.profile.iri_subtree else (str(term),)

    def released(self, term) -> bool:
        """decide(term)[0], from two set lookups per ancestor rather than a scan of every rule.

        The streaming path uses this; `view` keeps `decide`, so the two stay
        independent implementations that the equivalence tests compare.
        """
        chain = self.chain(term)
        return not any(a in self.denied for a in chain) and any(a in self.permitted for a in chain)

    def decide(self, term):
        """(released, reason) for one resource; deny wins, and default-deny.

        Rule by rule, so the reason names the rule; the term's ancestors are
        found once, not once per rule.
        """
        chain = self.chain(term)
        for rule, owned in self.binding:
            if rule.kind == "prohibition" and any(a in owned for a in chain):
                return False, f"withheld: {rule.label}"
        for rule, owned in self.binding:
            if rule.kind == "permission" and any(a in owned for a in chain):
                return True, f"released: {rule.label}"
        return False, "withheld: no permission covers it for this purpose"


def evaluation(source, rules, request, profile, vocabulary) -> Evaluation:
    check_preconditions(source, rules)
    partition = Partition(source, profile)
    binding = [(rule, partition.owned(select(partition.store, rule.target)))
               for rule in rules if applies(rule, request, vocabulary)]
    return Evaluation(partition, binding)


def view(graph, evaluation) -> tuple:
    """(released triples, number withheld) from an in-memory graph. A triple is released
    when its subject is, and when its object, if it is a node of the graph, is released
    too -- so a view never points at something it does not contain."""
    import rdflib

    nodes = {str(s) for s in graph.subjects()}
    released = lru_cache(maxsize=None)(lambda term: evaluation.decide(term)[0])
    kept, withheld = [], 0
    for s, p, o in graph:
        if released(str(s)) and (not isinstance(o, (rdflib.URIRef, rdflib.BNode))
                                 or str(o) not in nodes or released(str(o))):
            kept.append((s, p, o))
        else:
            withheld += 1
    return kept, withheld


def _ends(line: str):
    """(subject IRI, object IRI or None for a literal) of one N-Triples line."""
    if not line.startswith("<"):
        raise PolicyError(f"streaming needs IRI subjects; got {line[:60]!r}")
    subject_end = line.index(">")
    rest = line[line.index(">", subject_end + 1) + 1:].lstrip()   # after the predicate
    if rest.startswith("_:"):
        raise PolicyError(f"streaming does not support blank nodes: {line[:60]!r}")
    return line[1:subject_end], rest[1:rest.index(">")] if rest.startswith("<") else None


#: The evaluation a forked worker filters with; set before the workers start.
_EVALUATION = None


def _filter(path, node_space, part) -> tuple:
    """One input's released lines, gzipped to `part`: (kept, withheld)."""
    released = lru_cache(maxsize=1 << 20)(_EVALUATION.released)
    kept = withheld = 0
    last, last_released = None, False
    with (gzip.open(path, "rt", encoding="utf-8") if str(path).endswith(".gz")
          else open(path, encoding="utf-8")) as handle, \
            gzip.open(part, "wt", encoding="utf-8", compresslevel=1) as out:
        for line in handle:
            if not line.strip() or line.startswith("#"):
                continue
            subject, obj = _ends(line)
            if subject != last:             # a subject's lines are usually adjacent
                last, last_released = subject, released(subject)
            if last_released and (obj is None or not obj.startswith(node_space) or released(obj)):
                out.write(line if line.endswith("\n") else line + "\n")
                kept += 1
            else:
                withheld += 1
    return kept, withheld


def stream_view(paths, evaluation, node_space, out_path, *, workers=None) -> tuple:
    """Write the released lines of N-Triples `paths` to gzip file `out_path`: (kept, withheld).

    The same rule as `view`, except that "a node of the graph" is an IRI in the
    profile's declared node space: a stream cannot know every subject in advance.
    Lines are copied byte for byte, in input order. Inputs are filtered in
    parallel (forked workers, one gzip member each), then joined in order: a
    gzip file may hold several members.
    """
    global _EVALUATION
    if not node_space:
        raise PolicyError("streaming needs the profile to declare vcfp:nodeSpace")
    out_path = Path(out_path)
    workers = min(workers or os.cpu_count() or 1, len(paths))
    _EVALUATION = evaluation
    try:
        with tempfile.TemporaryDirectory(prefix=".view-parts-", dir=out_path.parent) as work:
            jobs = [(path, node_space, Path(work) / f"{n}.nt.gz") for n, path in enumerate(paths)]
            if workers > 1 and "fork" in multiprocessing.get_all_start_methods():
                with multiprocessing.get_context("fork").Pool(workers) as pool:
                    counts = pool.starmap(_filter, jobs)
            else:
                counts = [_filter(*job) for job in jobs]
            with open(out_path, "wb") as out:
                for _, _, part in jobs:
                    with open(part, "rb") as handle:
                        shutil.copyfileobj(handle, out)
    finally:
        _EVALUATION = None
    return sum(k for k, _ in counts), sum(w for _, w in counts)


def units(source, profile) -> list:
    """The profile's reporting units: dicts with 'resource', 'group' and any other columns."""
    if profile.unit_query is None:
        return []
    found = list(as_store(source).rows(profile.unit_query))
    return sorted(found, key=lambda u: (u["group"], u["resource"]))
