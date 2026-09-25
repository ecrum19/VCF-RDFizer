"""Evaluate a policy for one request, write the release, and attach policies to a graph.

`evaluate` produces the view (engine.view), a decision for every reporting unit
the profile names, and a decision for each unit group. `write_release` writes
them with a manifest. `attach` writes the policies into the data instead, so
they can be queried alongside it.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import csv
import json
from pathlib import Path

from . import DISCLOSURE_MODEL, ODRL, VCFP, VERSION
from .engine import check_preconditions, evaluation, select, units, view
from .policy import Direct


@dataclass
class Release:
    request: object
    view: list = field(default_factory=list)       # released (s, p, o)
    triples_withheld: int = 0
    units: list = field(default_factory=list)      # (unit dict, released, reason)
    groups: dict = field(default_factory=dict)     # group IRI -> (released, reason)
    duties: tuple = ()                             # of the permissions that released something


def evaluate(graph, rules, request, profile, vocabulary) -> Release:
    decided = evaluation(graph, rules, request, profile, vocabulary)
    release = Release(request)
    release.view, release.triples_withheld = view(graph, decided)
    for unit in units(graph, profile):
        release.units.append((unit, *decided.decide(unit["resource"])))
    for group in sorted({u["group"] for u, _, _ in release.units}, key=str):
        release.groups[str(group)] = decided.decide(group)
    released_subjects = {s for s, _, _ in release.view}
    release.duties = tuple(sorted({
        duty for rule, owned in decided.binding if rule.kind == "permission"
        and any(decided.partition.contains(owned, s) for s in released_subjects)
        for duty in rule.duties}))
    return release


def summary(release) -> dict:
    """Counts per group and per deciding reason, for the manifest and the paper figure."""
    counts = {g: {"released": ok, "reason": why, "records_released": 0, "records_withheld": 0}
              for g, (ok, why) in release.groups.items()}
    reasons = {}
    for unit, ok, why in release.units:
        counts[str(unit["group"])]["records_released" if ok else "records_withheld"] += 1
        reasons[why] = reasons.get(why, 0) + 1
    return {
        "request": {"assignee": release.request.assignee, "purpose": release.request.purpose},
        "groups": counts,
        "records_released": sum(ok for _, ok, _ in release.units),
        "records_withheld": sum(not ok for _, ok, _ in release.units),
        "triples_released": len(release.view),
        "triples_withheld": release.triples_withheld,
        "reasons": reasons,
    }


def write_release(release, out_dir: Path, *, policies, digest: str) -> None:
    """Write view.nt, decisions.csv, summary.json and manifest.ttl into a new directory."""
    import rdflib

    out_dir = Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"{out_dir} is not empty; a release is never overwritten")
    out_dir.mkdir(parents=True, exist_ok=True)

    graph = rdflib.Graph()
    for triple in release.view:
        graph.add(triple)
    lines = sorted(line for line in graph.serialize(format="nt").splitlines() if line.strip())
    (out_dir / "view.nt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    columns = list(release.units[0][0]) if release.units else ["resource", "group"]
    with (out_dir / "decisions.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns + ["released", "reason"])
        for unit, ok, why in release.units:
            writer.writerow([("" if unit[c] is None else str(unit[c])) for c in columns] + [ok, why])

    counts = summary(release)
    (out_dir / "summary.json").write_text(json.dumps(counts, indent=2) + "\n", encoding="utf-8")
    (out_dir / "manifest.ttl").write_text(
        manifest(counts, policies=policies, digest=digest, sources=release.groups, duties=release.duties),
        encoding="utf-8")


def manifest(counts, *, policies, digest, sources, duties) -> str:
    """The vcfp:ReleaseView description (docs/policy-demonstrator.md §6), as Turtle."""
    import rdflib
    from rdflib.namespace import PROV, XSD

    vcfp, odrl = rdflib.Namespace(VCFP), rdflib.Namespace(ODRL)
    g = rdflib.Graph()
    for prefix, namespace in (("vcfp", vcfp), ("odrl", odrl), ("prov", PROV)):
        g.bind(prefix, namespace)
    node, request = rdflib.URIRef("#release"), rdflib.BNode()
    g.add((node, rdflib.RDF.type, vcfp.ReleaseView))
    for source in sorted(sources):
        g.add((node, vcfp.derivedFrom, rdflib.URIRef(source)))
    for policy in sorted(policies):
        g.add((node, vcfp.policy, rdflib.URIRef(policy)))
    g.add((node, vcfp.policyDigest, rdflib.Literal(digest)))
    g.add((node, vcfp.request, request))
    g.add((request, odrl.assignee, rdflib.URIRef(counts["request"]["assignee"])))
    g.add((request, odrl.purpose, rdflib.URIRef(counts["request"]["purpose"])))
    for key, prop in (("records_released", "recordsReleased"), ("records_withheld", "recordsWithheld"),
                      ("triples_withheld", "triplesWithheld")):
        g.add((node, vcfp[prop], rdflib.Literal(counts[key])))
    g.add((node, vcfp.groupsWithheld, rdflib.Literal(sum(not c["released"] for c in counts["groups"].values()))))
    for duty in duties:
        obligation = rdflib.BNode()
        g.add((node, vcfp.obligation, obligation))
        g.add((obligation, odrl.action, rdflib.URIRef(duty)))
    g.add((node, vcfp.disclosureModel, rdflib.Literal(DISCLOSURE_MODEL)))
    g.add((node, PROV.wasGeneratedBy, rdflib.URIRef(f"urn:vcf-rdfizer-policy:{VERSION}")))
    g.add((node, PROV.generatedAtTime,
           rdflib.Literal(datetime.now(timezone.utc).replace(microsecond=0).isoformat(), datatype=XSD.dateTime)))
    return g.serialize(format="turtle")


def attach(graph, policy_graph, rules) -> dict:
    """Merge the policies into `graph` and link each governed resource to its policy.

    A direct target gets odrl:hasPolicy. A selection records what it selects
    (vcfp:selects), and each selected resource gets odrl:hasPolicy too, so a
    SPARQL query needs no knowledge of the selectors. Returns counts per asset.
    """
    import rdflib

    has_policy, selects = rdflib.URIRef(ODRL + "hasPolicy"), rdflib.URIRef(VCFP + "selects")
    check_preconditions(graph, rules)
    selections = {rule.target: select(graph, rule.target) for rule in rules}   # before merging
    graph += policy_graph
    counts = {}
    for rule in rules:
        policy = rdflib.URIRef(rule.policy)
        chosen = selections[rule.target]
        asset = None if isinstance(rule.target, Direct) else rdflib.URIRef(rule.target.asset)
        for resource in chosen:
            graph.add((resource, has_policy, policy))
            if asset is not None:
                graph.add((asset, selects, resource))
        counts[getattr(rule.target, "asset", None) or rule.target.iri] = len(chosen)
    return counts
