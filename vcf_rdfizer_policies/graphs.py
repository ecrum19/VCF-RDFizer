"""Load RDF inputs, and walk an IRI's hierarchy."""

import gzip
from pathlib import Path

from . import PolicyError

#: The engine evaluates in memory. Above this, the demonstrator is the wrong tool.
MAX_TRIPLES = 5_000_000


def _open(path: Path, mode: str):
    return gzip.open(path, mode) if path.name.endswith(".gz") else open(path, mode)


def load(paths) -> "rdflib.Graph":
    """Parse .nt / .nt.gz files into one graph, after a size check."""
    import rdflib

    paths = [Path(p) for p in paths]
    total = 0
    for path in paths:
        with _open(path, "rb") as handle:
            total += sum(1 for _ in handle)
    if total > MAX_TRIPLES:
        raise PolicyError(f"{total:,} triples exceeds the in-memory limit of {MAX_TRIPLES:,}; "
                          "the demonstrator is for fixtures, not cohorts")
    graph = rdflib.Graph()
    for path in paths:
        with _open(path, "rb") as handle:
            graph.parse(handle, format="nt")
    return graph


def ancestors(iri: str):
    """The IRI itself, then each prefix of it that ends just before a '#' or '/'.

    file://P1.vcf#record/9/allele/0 -> itself, file://P1.vcf#record/9/allele,
    file://P1.vcf#record/9, file://P1.vcf#record, file://P1.vcf. The scheme's own
    slashes are skipped, so nothing shorter than the authority is yielded.
    """
    yield iri
    start = iri.find("://") + 3 if "://" in iri else 0
    for index in range(len(iri) - 1, start, -1):
        if iri[index] in "#/":
            yield iri[:index]
