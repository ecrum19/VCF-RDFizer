"""DUO purposes, and the subsumption rule used to match them.

A requester's purpose satisfies a consented term when it is that term or a
narrower one: disease-specific research falls within a general-research consent,
not the reverse. The hierarchy is the bundled DUO subset
(vcf_rdfizer_data/policy/duo-subset.ttl), not a live ontology.
"""

from functools import lru_cache
from importlib.resources import files
import re

from . import OBO, PolicyError

_TERM = re.compile(r"(?:obo:|http://purl\.obolibrary\.org/obo/)?DUO[:_](\d{7})")


def purpose_iri(value: str) -> str:
    """Normalise obo:DUO_0000007, DUO:0000007, DUO_0000007 or a full IRI."""
    match = _TERM.fullmatch(value.strip())
    if not match:
        raise PolicyError(f"not a DUO term: {value!r}")
    iri = f"{OBO}DUO_{match.group(1)}"
    if iri not in _parents():
        raise PolicyError(f"{value} is not in the bundled DUO subset; v0.1.0 knows "
                          + ", ".join(sorted(t.rsplit("/", 1)[1] for t in _parents())))
    return iri


@lru_cache(maxsize=1)
def _parents() -> dict:
    """Each bundled term mapped to its one DUO parent."""
    import rdflib

    graph = rdflib.Graph().parse(files("vcf_rdfizer_data.policy") / "duo-subset.ttl")
    return {str(s): str(o) for s, o in graph.subject_objects(rdflib.RDFS.subClassOf)}


def within(purpose: str, term: str) -> bool:
    """True when `purpose` is `term` or a descendant of it."""
    parents = _parents()
    node = purpose
    while node is not None:
        if node == term:
            return True
        node = parents.get(node)
    return False
