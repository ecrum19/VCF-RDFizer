"""Evaluate a policy for one request, and attach policies to a graph.

`evaluate` decides every file and record (decide.py), withholds the IRI subtrees
of whatever is refused, and drops any remaining triple that points into a
withheld subtree, so a view never references something it does not contain.
`attach` writes the policies into the data instead, so they can be queried
alongside it.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import csv
import json
from pathlib import Path

from . import DISCLOSURE_MODEL, ODRL, VCFP, VERSION
from .decide import applies, check_assemblies, covers, decide
from .graphs import assemblies, records, split
from .profile import FileTarget


@dataclass
class Release:
    request: object
    view: list = field(default_factory=list)          # (s, p, o) rdflib terms
    decisions: list = field(default_factory=list)     # (Record, Decision)
    files: dict = field(default_factory=dict)         # file IRI -> Decision
    duties: tuple = ()                                # of the permissions that released a file
    triples_withheld: int = 0


def evaluate(graph, rules, request) -> Release:
    import rdflib

    file_assemblies = assemblies(graph)
    check_assemblies(rules, file_assemblies)
    release = Release(request)
    release.files = {f: decide(f, rules, request) for f in sorted(file_assemblies)}
    withheld_rows = set()
    for record in records(graph):
        file_decision = release.files[record.file]
        decision = decide(record, rules, request) if file_decision.released else file_decision
        release.decisions.append((record, decision))
        if not decision.released:
            withheld_rows.add((record.file, record.row))
    withheld_files = {f for f, d in release.files.items() if not d.released}
    release.duties = tuple(sorted({
        duty for rule in rules if rule.kind == "permission" and applies(rule, request)
        and any(covers(rule.target, f) for f in release.files if f not in withheld_files)
        for duty in rule.duties}))

    def withheld(term) -> bool:
        if not isinstance(term, rdflib.URIRef):
            return False
        file_iri, row = split(str(term))
        return file_iri in withheld_files or (file_iri, row) in withheld_rows

    for triple in graph:
        if withheld(triple[0]) or withheld(triple[2]):
            release.triples_withheld += 1
        else:
            release.view.append(triple)
    return release


def summary(release) -> dict:
    """Counts per file and per deciding reason, for the manifest and the paper figure."""
    per_file = {}
    for record, decision in release.decisions:
        counts = per_file.setdefault(record.file, {"records_released": 0, "records_withheld": 0})
        counts["records_released" if decision.released else "records_withheld"] += 1
    reasons = {}
    for _, decision in release.decisions:
        reasons[decision.reason] = reasons.get(decision.reason, 0) + 1
    return {
        "request": {"assignee": release.request.assignee, "purpose": release.request.purpose},
        "files": {f: {"released": d.released, "reason": d.reason, **per_file.get(f, {})}
                  for f, d in release.files.items()},
        "records_released": sum(d.released for _, d in release.decisions),
        "records_withheld": sum(not d.released for _, d in release.decisions),
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

    view = rdflib.Graph()
    for triple in release.view:
        view.add(triple)
    lines = sorted(line for line in view.serialize(format="nt").splitlines() if line.strip())
    (out_dir / "view.nt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    with (out_dir / "decisions.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["file", "row", "chrom", "pos", "ref", "alts", "released", "reason"])
        for record, decision in release.decisions:
            writer.writerow([record.file, record.row, record.chrom, record.pos, record.ref,
                             ",".join(record.alts), decision.released, decision.reason])

    counts = summary(release)
    (out_dir / "summary.json").write_text(json.dumps(counts, indent=2) + "\n", encoding="utf-8")
    (out_dir / "manifest.ttl").write_text(
        manifest(counts, policies=policies, digest=digest, sources=release.files, duties=release.duties),
        encoding="utf-8")


def manifest(counts, *, policies, digest, sources, duties) -> str:
    """The vcfp:ReleaseView description (docs/policy-demonstrator.md §6.1), as Turtle."""
    import rdflib
    from rdflib.namespace import PROV, XSD

    vcfp, odrl = rdflib.Namespace(VCFP), rdflib.Namespace(ODRL)
    g = rdflib.Graph()
    g.bind("vcfp", vcfp), g.bind("odrl", odrl), g.bind("prov", PROV)
    view, request = rdflib.URIRef("#release"), rdflib.BNode()
    g.add((view, rdflib.RDF.type, vcfp.ReleaseView))
    for source in sorted(sources):
        g.add((view, vcfp.derivedFrom, rdflib.URIRef(source)))
    for policy in sorted(policies):
        g.add((view, vcfp.policy, rdflib.URIRef(policy)))
    g.add((view, vcfp.policyDigest, rdflib.Literal(digest)))
    g.add((view, vcfp.request, request))
    g.add((request, odrl.assignee, rdflib.URIRef(counts["request"]["assignee"])))
    g.add((request, odrl.purpose, rdflib.URIRef(counts["request"]["purpose"])))
    for name in ("records_released", "records_withheld", "triples_withheld"):
        g.add((view, vcfp[_camel(name)], rdflib.Literal(counts[name])))
    g.add((view, vcfp.filesWithheld, rdflib.Literal(sum(not f["released"] for f in counts["files"].values()))))
    for duty in sorted(duties):
        obligation = rdflib.BNode()
        g.add((view, vcfp.obligation, obligation))
        g.add((obligation, odrl.action, rdflib.URIRef(duty)))
    g.add((view, vcfp.disclosureModel, rdflib.Literal(DISCLOSURE_MODEL)))
    g.add((view, PROV.wasGeneratedBy, rdflib.URIRef(f"urn:vcf-rdfizer-policy:{VERSION}")))
    g.add((view, PROV.generatedAtTime,
           rdflib.Literal(datetime.now(timezone.utc).replace(microsecond=0).isoformat(), datatype=XSD.dateTime)))
    return g.serialize(format="turtle")


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part.title() for part in rest)


def attach(graph, policy_graph, rules) -> dict:
    """Merge the policies into `graph` and link each governed resource to its policy.

    Files get odrl:hasPolicy directly. A region or variant selection records what
    it selects (vcfp:selects), and each selected record gets odrl:hasPolicy too,
    so a SPARQL query needs no knowledge of the selectors. Returns counts per asset.
    """
    import rdflib

    has_policy, selects = rdflib.URIRef(ODRL + "hasPolicy"), rdflib.URIRef(VCFP + "selects")
    check_assemblies(rules, assemblies(graph))
    all_records = records(graph)
    counts = {}
    for triple in policy_graph:
        graph.add(triple)
    for rule in rules:
        policy = rdflib.URIRef(rule.policy)
        if isinstance(rule.target, FileTarget):
            graph.add((rdflib.URIRef(rule.target.iri), has_policy, policy))
            counts[rule.target.iri] = 1
            continue
        asset = rdflib.URIRef(rule.target.asset)
        selected = [r for r in all_records if covers(rule.target, r)]
        for record in selected:
            record_iri = rdflib.URIRef(f"{record.file}#record/{record.row}")
            graph.add((asset, selects, record_iri))
            graph.add((record_iri, has_policy, policy))
        counts[rule.target.asset] = len(selected)
    return counts
