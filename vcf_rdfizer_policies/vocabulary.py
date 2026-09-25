"""Purpose vocabularies: which purposes fall within which.

A purpose satisfies a term when it is that term or narrower, following
rdfs:subClassOf or skos:broader, so any RDFS or SKOS vocabulary works. The
bundled four-term DUO subset (vcf_rdfizer_data/policy/duo-subset.ttl) is only
the default; full DUO or a local vocabulary is passed with --purposes.
"""

from importlib.resources import files
from pathlib import Path

from . import PolicyError

BROADER = ("http://www.w3.org/2000/01/rdf-schema#subClassOf",
           "http://www.w3.org/2004/02/skos/core#broader")


class Vocabulary:
    def __init__(self, graph):
        import rdflib

        self.parents = {}
        for predicate in BROADER:
            for narrow, broad in graph.subject_objects(rdflib.URIRef(predicate)):
                self.parents.setdefault(str(narrow), set()).add(str(broad))
        self.terms = set(self.parents) | {b for broads in self.parents.values() for b in broads}
        self._namespaces = graph.namespace_manager

    @classmethod
    def load(cls, path=None) -> "Vocabulary":
        """The vocabulary at `path`, or the bundled DUO subset."""
        import rdflib

        source = Path(path) if path else files("vcf_rdfizer_data.policy") / "duo-subset.ttl"
        return cls(rdflib.Graph().parse(str(source), format="turtle"))

    def resolve(self, value: str) -> str:
        """A full IRI, or a prefixed name the vocabulary binds (DUO:0000007), as a known term."""
        iri = value.strip()
        if ":" in iri and "://" not in iri:
            try:
                iri = str(self._namespaces.expand_curie(iri))
            except ValueError:
                raise PolicyError(f"{value!r}: prefix is not bound by the purpose vocabulary") from None
        if iri not in self.terms:
            raise PolicyError(f"{value} is not a term of the purpose vocabulary")
        return iri

    def within(self, purpose: str, term: str) -> bool:
        """True when `purpose` is `term` or a descendant of it."""
        seen, frontier = set(), [purpose]
        while frontier:
            node = frontier.pop()
            if node == term:
                return True
            if node not in seen:
                seen.add(node)
                frontier.extend(self.parents.get(node, ()))
        return False
