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

`check_view` checks an in-memory view against an rdflib source. `check_stream`
runs the same four checks, and the oracle, on a streamed view.nt.gz against
SPARQL endpoints for the source and for the view itself.
"""

from functools import lru_cache
import gzip
import json
from pathlib import Path

from . import ODRL, VCFP
from .engine import BATCH, Evaluation, Partition, Request, _ends, applies, select, units
from .policy import policy_digest
from .store import iri

#: Enough to diagnose; a broken view can otherwise fail thousands of times.
LIMIT = 20


def read_manifest(view_dir: Path):
    """(manifest graph, request) from a directory written by evaluate."""
    import rdflib

    manifest = rdflib.Graph().parse(str(Path(view_dir) / "manifest.ttl"), format="turtle")
    request_node = next(manifest.objects(None, rdflib.URIRef(VCFP + "request")))
    return manifest, Request(str(manifest.value(request_node, rdflib.URIRef(ODRL + "assignee"))),
                             str(manifest.value(request_node, rdflib.URIRef(ODRL + "purpose"))))


def read_view(view_dir: Path):
    """(view graph, manifest graph, request) from a directory written by evaluate."""
    import rdflib

    manifest, request = read_manifest(view_dir)
    return rdflib.Graph().parse(str(Path(view_dir) / "view.nt"), format="nt"), manifest, request


def _digest_failure(manifest, policy_path):
    import rdflib

    recorded = str(next(manifest.objects(None, rdflib.URIRef(VCFP + "policyDigest")), None))
    return [] if recorded == policy_digest(policy_path) else [
        f"view was produced under a different policy ({recorded})"]


def check_view(view, manifest, request, *, policy_path, rules, profile, vocabulary, source) -> list:
    """Every way `view` departs from the policy, given the `source` it was made from."""
    import rdflib

    failures = _digest_failure(manifest, policy_path)
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


def check_stream(view_dir, *, policy_path, rules, profile, vocabulary, store, view_store, oracle=None) -> list:
    """check_view's four checks, and the oracle when given, for a streamed view.

    `store` serves the source the view was made from, `view_store` the view
    alone, and `oracle` the graph `vcf-rdfizer-policy oracle` wrote from the VCF
    text (or is None). The view is read once, as a stream, for what each line
    can show by itself. Dangling references are a question about the whole
    view, so the view's endpoint answers it -- after confirming it serves as
    many triples as the file has lines.
    """
    manifest, request = read_manifest(view_dir)
    failures = _digest_failure(manifest, policy_path)
    partition = Partition(store, profile)
    binding = [(rule, partition.owned(select(store, rule.target)))
               for rule in rules if applies(rule, request, vocabulary)]
    prohibited = [(rule, owned) for rule, owned in binding if rule.kind == "prohibition"]
    sets = Evaluation(partition, binding)

    @lru_cache(maxsize=1 << 20)
    def owner(term):
        # The unions answer "is it prohibited"; the rules are scanned only to name one.
        if not any(a in sets.denied for a in sets.chain(term)):
            return None
        return next(rule.label for rule, owned in prohibited if partition.contains(owned, term))

    covered = lru_cache(maxsize=1 << 20)(lambda s: any(a in sets.permitted for a in sets.chain(s)))
    counts = {"prohibited": [], "uncovered": []}

    def note(kind, text):
        if len(counts[kind]) < LIMIT:
            counts[kind].append(text)

    unit_resources = {u["resource"] for u in units(store, profile)} if oracle is not None else set()
    present, lines, last = set(), 0, None
    with gzip.open(Path(view_dir) / "view.nt.gz", "rt", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            lines += 1
            subject, obj = _ends(line)
            if subject != last:             # a subject's lines are usually adjacent
                last = subject
                if owner(subject):
                    note("prohibited", f"prohibited content present: {owner(subject)} owns <{subject}>")
                if not covered(subject):
                    note("uncovered", f"no permission covers <{subject}>")
                if subject in unit_resources:
                    present.add(subject)
            if obj is not None and owner(obj):
                note("prohibited", f"prohibited content present: {owner(obj)} owns <{obj}>")
    failures += counts["prohibited"] + counts["uncovered"]

    served = int(next(iter(view_store.rows("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }")))["n"])
    if served != lines:
        return failures + [f"the view endpoint serves {served:,} triples; view.nt.gz has {lines:,} lines"]

    # Dangling: an object the view names but does not describe, which the source does describe.
    nodes = " || ".join(f"STRSTARTS(STR(?o), {json.dumps(prefix)})" for prefix in profile.node_space)
    missing = [row["o"] for row in view_store.rows(
        f"SELECT DISTINCT ?o WHERE {{ ?s ?p ?o FILTER(isIRI(?o) && ({nodes})) "
        "FILTER NOT EXISTS { ?o ?q ?x } } ORDER BY ?o")]
    dangling = []
    for start in range(0, len(missing), BATCH):
        values = " ".join(iri(o) for o in missing[start:start + BATCH])
        dangling += [row["o"] for row in store.rows(
            f"SELECT DISTINCT ?o WHERE {{ VALUES ?o {{ {values} }} ?o ?p ?x }}")]
    failures += [f"dangling reference to <{o}>" for o in sorted(dangling)[:LIMIT]]

    if oracle is not None:
        from .vcf_oracle import expected_records

        expected = expected_records(oracle, rules, request, profile, vocabulary)
        failures += [f"leak: <{r}> is released but the policy withholds it" for r in sorted(present - expected)]
        failures += [f"over-withheld: <{r}> should have been released" for r in sorted(expected - present)]
    return failures
