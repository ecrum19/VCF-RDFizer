"""Verify a release view: structural checks for any graph, plus an optional oracle.

Given the view, the policy, and the source graph the view was made from:

1. the view was produced under this policy (its manifest's digest matches);
2. nothing a binding prohibition owns appears in the view, as subject or object;
3. every subject in the view is owned by a binding permission (default-deny);
4. no triple points at a node of the source that the view does not contain.

Checks 2-4 reuse the engine's selectors and partition, so they confirm the view
honours the policy; they cannot catch a mistake in a selector itself. That is
what an oracle is for: `vcf_oracle.compare` re-derives the expected records
from the VCF text, an input the graph and its conversion never touched.
Each failure is returned as one human-readable line.
"""

from pathlib import Path

from . import ODRL, VCFP
from .engine import Partition, Request, applies, select
from .policy import policy_digest

#: Enough to diagnose; a broken view can otherwise fail thousands of times.
LIMIT = 20


def read_view(view_dir: Path):
    """(view graph, manifest graph, request) from a directory written by evaluate."""
    import rdflib

    view_dir = Path(view_dir)
    manifest = rdflib.Graph().parse(str(view_dir / "manifest.ttl"), format="turtle")
    request_node = next(manifest.objects(None, rdflib.URIRef(VCFP + "request")))
    request = Request(str(manifest.value(request_node, rdflib.URIRef(ODRL + "assignee"))),
                      str(manifest.value(request_node, rdflib.URIRef(ODRL + "purpose"))))
    return rdflib.Graph().parse(str(view_dir / "view.nt"), format="nt"), manifest, request


def check_view(view, manifest, request, *, policy_path, rules, profile, vocabulary, source) -> list:
    """Every way `view` departs from the policy, given the `source` it was made from."""
    import rdflib

    failures = []
    recorded = str(next(manifest.objects(None, rdflib.URIRef(VCFP + "policyDigest")), None))
    if recorded != policy_digest(policy_path):
        failures.append(f"view was produced under a different policy ({recorded})")

    partition = Partition(source, profile)
    binding = [(rule, partition.owned(select(source, rule.target)))
               for rule in rules if applies(rule, request, vocabulary)]
    terms = {t for triple in view for t in (triple[0], triple[2])
             if isinstance(t, (rdflib.URIRef, rdflib.BNode))}
    for rule, owned in binding:
        if rule.kind == "prohibition":
            present = sorted(str(t) for t in terms if partition.contains(owned, t))
            failures += [f"prohibited content present: {rule.label} owns <{t}>" for t in present[:LIMIT]]

    granted = [owned for rule, owned in binding if rule.kind == "permission"]
    ungoverned = sorted(str(s) for s in set(view.subjects())
                        if not any(partition.contains(owned, s) for owned in granted))
    failures += [f"no permission covers <{s}>" for s in ungoverned[:LIMIT]]

    nodes, present = set(source.subjects()), set(view.subjects())
    dangling = sorted((str(s), str(o)) for s, _, o in view if o in nodes and o not in present)
    failures += [f"dangling reference: <{s}> -> <{o}>" for s, o in dangling[:LIMIT]]
    return failures
