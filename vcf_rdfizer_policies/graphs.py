"""Load converted graphs and read the records the selectors act on.

A record is identified by its file IRI and row number, which is how the
converter mints its IRIs (docs/conversion.md §6). Everything that belongs to
row N lives under one of three IRI subtrees -- #record/N, #call/N and
#sample/N -- so withholding a record means withholding those subtrees.
"""

from dataclasses import dataclass
import gzip
from pathlib import Path
import re

from . import VCFC, PolicyError

#: v0.1.0 evaluates in memory. Above this the demonstrator is the wrong tool.
MAX_TRIPLES = 5_000_000

_ROW = re.compile(r"(?:record|call|sample)/(\d+)(?:/.*)?")


@dataclass(frozen=True)
class Record:
    file: str          # file IRI, e.g. file://P001.vcf
    row: int
    chrom: str
    pos: int
    ref: str
    alts: tuple


def load(paths) -> "rdflib.Graph":
    """Parse .nt / .nt.gz files into one graph, after a size check."""
    import rdflib

    total = 0
    for path in map(Path, paths):
        opener = gzip.open if path.name.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as handle:
            total += sum(1 for _ in handle)
    if total > MAX_TRIPLES:
        raise PolicyError(f"{total:,} triples exceeds the v0.1.0 in-memory limit of "
                          f"{MAX_TRIPLES:,}; the demonstrator is for fixtures, not cohorts")
    graph = rdflib.Graph()
    for path in map(Path, paths):
        opener = gzip.open if path.name.endswith(".gz") else open
        with opener(path, "rb") as handle:
            graph.parse(handle, format="nt")
    return graph


def records(graph) -> list:
    """Every vcfc:VCFRecord in the graph, with the fields selectors use."""
    rows = graph.query(f"""
        PREFIX vcfc: <{VCFC}>
        SELECT ?record ?chrom ?pos ?ref (GROUP_CONCAT(?alt; separator=",") AS ?alts)
        WHERE {{ ?record a vcfc:VCFRecord ; vcfc:chrom ?chrom ; vcfc:pos ?pos ; vcfc:ref ?ref .
                 OPTIONAL {{ ?record vcfc:alt ?alt }} }}
        GROUP BY ?record ?chrom ?pos ?ref""")
    found = []
    for record, chrom, pos, ref, alts in rows:
        file_iri, row = split(str(record))
        found.append(Record(file_iri, row, str(chrom), int(pos), str(ref),
                            tuple(sorted(str(alts).split(","))) if alts else ()))
    return sorted(found, key=lambda r: (r.file, r.row))


def assemblies(graph) -> dict:
    """Each file IRI mapped to its declared vcfc:referenceGenome."""
    rows = graph.query(f"""PREFIX vcfc: <{VCFC}>
        SELECT ?file ?assembly WHERE {{ ?file a vcfc:VCFFile . OPTIONAL {{ ?file vcfc:referenceGenome ?assembly }} }}""")
    return {str(f): (str(a) if a is not None else None) for f, a in rows}


def split(iri: str):
    """(file IRI, row or None) for any IRI the converter mints; (None, None) otherwise.

    Every IRI under file://X belongs to file X -- header lines and the sample set
    included, which is what lets a withheld file take its header with it. Only
    the #record/, #call/ and #sample/ subtrees also carry a row.
    """
    if not iri.startswith("file://"):
        return None, None
    file_iri, _, fragment = iri.partition("#")
    match = _ROW.fullmatch(fragment)
    return file_iri, int(match.group(1)) if match else None
