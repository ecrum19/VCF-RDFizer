"""Verify a release view against an oracle computed from the source VCFs.

The oracle reads records straight from the VCF text -- no graph, no SPARQL --
and decides them with decide.py. A view passes only if its records equal the
oracle's released set exactly (an extra record is a leak; a missing one is
over-withholding) and it survives three structural checks: no triple inside
a prohibited target, no reference to anything withheld, and no mention of a
withheld file. Each failure is returned as one human-readable line.
"""

import gzip
from pathlib import Path
import re

from . import ODRL, VCFC, VCFP
from .decide import Request, applies, check_assemblies, decide
from .graphs import Record, records, split
from .profile import FileTarget, RegionTarget, policy_digest


def read_vcf(path: Path):
    """(file IRI, assembly, [Record]) from a VCF, numbering rows as the converter does."""
    path = Path(path)
    opener = gzip.open if path.name.endswith(".gz") else open
    name = re.sub(r"\.gz$", "", path.name)
    file_iri, assembly, found = f"file://{name}", None, []
    with opener(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("##reference="):
                assembly = line.split("=", 1)[1].strip()
            if line.startswith("#"):
                continue
            chrom, pos, _, ref, alt = line.rstrip("\n").split("\t")[:5]
            found.append(Record(file_iri, len(found) + 1, chrom, int(pos), ref,
                                tuple(sorted(a for a in alt.split(",") if a != "."))))
    return file_iri, assembly, found


def oracle(vcf_paths, rules, request):
    """The (file, row) pairs a correct view releases, and the files it withholds."""
    parsed = [read_vcf(p) for p in vcf_paths]
    check_assemblies(rules, {f: a for f, a, _ in parsed})
    released, withheld_files = set(), set()
    for file_iri, _, file_records in parsed:
        if not decide(file_iri, rules, request).released:
            withheld_files.add(file_iri)
            continue
        released |= {(r.file, r.row) for r in file_records if decide(r, rules, request).released}
    return released, withheld_files


def _only(graph, predicate):
    """The object of the one triple with this predicate (Graph.value needs a subject)."""
    import rdflib

    return next(graph.objects(None, rdflib.URIRef(predicate)), None)


def read_request(manifest_graph) -> Request:
    import rdflib

    request = _only(manifest_graph, VCFP + "request")
    return Request(str(manifest_graph.value(request, rdflib.URIRef(ODRL + "assignee"))),
                   str(manifest_graph.value(request, rdflib.URIRef(ODRL + "purpose"))))


def check_view(view_dir: Path, policy_path: Path, rules, vcf_paths) -> list:
    """Every way the view in `view_dir` departs from the policy; empty means it passes."""
    import rdflib

    view_dir = Path(view_dir)
    manifest = rdflib.Graph().parse(str(view_dir / "manifest.ttl"), format="turtle")
    view = rdflib.Graph().parse(str(view_dir / "view.nt"), format="nt")
    request = read_request(manifest)
    failures = []

    recorded = str(_only(manifest, VCFP + "policyDigest"))
    if recorded != policy_digest(policy_path):
        failures.append(f"view was produced under a different policy ({recorded})")

    expected, withheld_files = oracle(vcf_paths, rules, request)
    actual = {(r.file, r.row) for r in records(view)}
    failures += [f"leak: {f}#record/{row} is released but the policy withholds it"
                 for f, row in sorted(actual - expected)]
    failures += [f"over-withheld: {f}#record/{row} should have been released"
                 for f, row in sorted(expected - actual)]

    for rule in rules:
        if rule.kind == "prohibition" and applies(rule, request) and view.query(_ask(rule.target)).askAnswer:
            failures.append(f"prohibited content present: {rule.label}")

    subjects = {s for s in view.subjects() if isinstance(s, rdflib.URIRef)}
    for s, p, o in view:
        if isinstance(o, rdflib.URIRef) and split(str(o))[0] and o not in subjects:
            failures.append(f"dangling reference: <{s}> <{p}> <{o}>")

    names = {f.split("://", 1)[1] for f in withheld_files}
    for term in {t for triple in view for t in triple}:
        for name in names:
            if name in str(term):
                failures.append(f"withheld file {name} is named by {term.n3()}")
    return failures


def _ask(target) -> str:
    """A SPARQL ASK that is true when anything the target selects is in the graph."""
    if isinstance(target, FileTarget):
        return f'ASK {{ ?s ?p ?o FILTER(STRSTARTS(STR(?s), "{target.iri}#") || STR(?s) = "{target.iri}") }}'
    if isinstance(target, RegionTarget):
        where = f"FILTER(?pos >= {target.start} && ?pos <= {target.end})"
    else:
        where = f'FILTER(?pos = {target.pos}) ?r vcfc:ref "{target.ref}" ; vcfc:alt "{target.alt}" .'
    return (f'PREFIX vcfc: <{VCFC}> ASK {{ ?r a vcfc:VCFRecord ; vcfc:chrom "{target.chrom}" ; '
            f"vcfc:pos ?pos . {where} }}")
