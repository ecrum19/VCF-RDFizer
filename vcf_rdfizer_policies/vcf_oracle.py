"""The VCF-side oracle: which records a correct view of VCF-RDFizer graphs releases.

It reads the source VCFs as text -- no converted graph, no conversion -- and
builds a small VCF Core graph of the fixed columns except INFO: each file with
its reference genome; each record with CHROM, POS, ID, REF and ALT; and its
call with QUAL and FILTER, in the shapes and under the IRIs VCF-RDFizer uses
(file://NAME, #record/N, #call/N). A selector that reads INFO, FORMAT or the
header is outside what the oracle models; its views are covered by the
structural checks in check.py only, so do not pass --vcf for such a policy.
The same policy is then evaluated on that graph. The view's records must equal
the records released there exactly: an extra one is a leak, a missing one is
over-withholding. Its independence comes from its input, not from a second
copy of the rules.
"""

from decimal import Decimal
import gzip
from pathlib import Path
import re

from . import VCFC
from .engine import evaluation, units


def graph_from_vcfs(paths):
    """A minimal VCF Core graph built directly from VCF text."""
    import rdflib

    vcfc = rdflib.Namespace(VCFC)
    graph = rdflib.Graph()
    for path in map(Path, paths):
        name = re.sub(r"\.gz$", "", path.name)
        file_iri = rdflib.URIRef(f"file://{name}")
        graph.add((file_iri, rdflib.RDF.type, vcfc.VCFFile))
        row = 0
        opener = gzip.open if path.name.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("##reference="):
                    graph.add((file_iri, vcfc.referenceGenome, rdflib.Literal(line.split("=", 1)[1].strip())))
                if line.startswith("#"):
                    continue
                row += 1
                chrom, pos, ident, ref, alt, qual, filters = line.rstrip("\n").split("\t")[:7]
                record = rdflib.URIRef(f"{file_iri}#record/{row}")
                call = rdflib.URIRef(f"{file_iri}#call/{row}")
                graph.add((file_iri, vcfc.hasRecord, record))
                graph.add((record, rdflib.RDF.type, vcfc.VCFRecord))
                graph.add((record, vcfc.hasCall, call))
                graph.add((record, vcfc.chrom, rdflib.Literal(chrom)))
                graph.add((record, vcfc.pos, rdflib.Literal(int(pos))))
                graph.add((record, vcfc.ref, rdflib.Literal(ref)))
                for allele in alt.split(","):
                    if allele != ".":
                        graph.add((record, vcfc.alt, rdflib.Literal(allele)))
                for value in ident.split(";"):
                    if value != ".":
                        graph.add((record, vcfc.recordId, rdflib.Literal(value)))
                if qual != ".":
                    graph.add((call, vcfc.qual, rdflib.Literal(Decimal(qual))))
                if filters != ".":
                    graph.add((call, vcfc.filter, rdflib.Literal(filters)))
    return graph


def compare(view, vcf_paths, *, rules, request, profile, vocabulary) -> list:
    """Failures where the view's records differ from the oracle's released records."""
    oracle = graph_from_vcfs(vcf_paths)
    decided = evaluation(oracle, rules, request, profile, vocabulary)
    expected = {str(u["resource"]) for u in units(oracle, profile) if decided.decide(u["resource"])[0]}
    actual = {str(u["resource"]) for u in units(view, profile)}
    return ([f"leak: <{r}> is released but the policy withholds it" for r in sorted(actual - expected)]
            + [f"over-withheld: <{r}> should have been released" for r in sorted(expected - actual)])
