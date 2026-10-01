"""The VCF-side oracle: which records a correct view of VCF-RDFizer graphs releases.

It reads the source VCFs as text -- no converted graph, no conversion -- and
builds a small VCF Core graph of the fixed columns except INFO: each file with
its reference genome; each record with CHROM, POS, ID, REF and ALT; and its
call with QUAL and FILTER, in the shapes and under the IRIs VCF-RDFizer uses
(file://NAME, #record/N, #call/N). A selector that reads INFO, FORMAT or the
header is outside what the oracle models; its views are covered by the
structural checks in check.py only, so do not pass --vcf for such a policy. A
selector over link graphs (LinkedSelector) needs those link graphs served beside
the oracle: a linker computes them from the VCF text, so independence holds.
The same policy is then evaluated on that graph. The view's records must equal
the records released there exactly: an extra one is a leak, a missing one is
over-withholding. Its independence comes from its input, not from a second
copy of the rules.
"""

import gzip
from pathlib import Path
import re

from . import VCFC
from .engine import evaluation, units

_XSD = "http://www.w3.org/2001/XMLSchema#"
_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _literal(value: str, datatype: str = None) -> str:
    text = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"' + (f"^^<{_XSD}{datatype}>" if datatype else "")


def write_ntriples(paths, out) -> int:
    """Write the oracle graph for VCF `paths` to text stream `out`; return the triple count.

    No RDF library is involved, so this scales with the VCFs. `graph_from_vcfs`
    parses this same output, so the in-memory and endpoint oracles are one graph.
    """
    count = 0

    def emit(s, p, o):
        nonlocal count
        out.write(f"<{s}> <{p}> {o} .\n")
        count += 1

    for path in map(Path, paths):
        file_iri = f"file://{re.sub(r'[.]gz$', '', path.name)}"
        emit(file_iri, _RDF_TYPE, f"<{VCFC}VCFFile>")
        row = 0
        opener = gzip.open if path.name.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("##reference="):
                    emit(file_iri, VCFC + "referenceGenome", _literal(line.split("=", 1)[1].strip()))
                if line.startswith("#"):
                    continue
                row += 1
                chrom, pos, ident, ref, alt, qual, filters = line.rstrip("\n").split("\t")[:7]
                record, call = f"{file_iri}#record/{row}", f"{file_iri}#call/{row}"
                emit(file_iri, VCFC + "hasRecord", f"<{record}>")
                emit(record, _RDF_TYPE, f"<{VCFC}VCFRecord>")
                emit(record, VCFC + "hasCall", f"<{call}>")
                emit(record, VCFC + "chrom", _literal(chrom))
                emit(record, VCFC + "pos", _literal(str(int(pos)), "integer"))
                emit(record, VCFC + "ref", _literal(ref))
                for allele in alt.split(","):
                    if allele != ".":
                        emit(record, VCFC + "alt", _literal(allele))
                for value in ident.split(";"):
                    if value != ".":
                        emit(record, VCFC + "recordId", _literal(value))
                if qual != ".":
                    emit(call, VCFC + "qual", _literal(qual, "double" if "e" in qual.lower() else "decimal"))
                if filters != ".":
                    emit(call, VCFC + "filter", _literal(filters))
    return count


def graph_from_vcfs(paths):
    """The oracle graph in memory (rdflib), for small inputs."""
    import io
    import rdflib

    buffer = io.StringIO()
    write_ntriples(paths, buffer)
    return rdflib.Graph().parse(data=buffer.getvalue(), format="nt")


def expected_records(oracle, rules, request, profile, vocabulary) -> set:
    """The records a correct view releases: the policy evaluated on the oracle (store or graph)."""
    decided = evaluation(oracle, rules, request, profile, vocabulary)
    return {u["resource"] for u in units(oracle, profile) if decided.decide(u["resource"])[0]}


def compare(view, vcf_paths, *, rules, request, profile, vocabulary) -> list:
    """Failures where an in-memory view's records differ from the oracle's released records."""
    expected = expected_records(graph_from_vcfs(vcf_paths), rules, request, profile, vocabulary)
    actual = {u["resource"] for u in units(view, profile)}
    return ([f"leak: <{r}> is released but the policy withholds it" for r in sorted(actual - expected)]
            + [f"over-withheld: <{r}> should have been released" for r in sorted(expected - actual)])
