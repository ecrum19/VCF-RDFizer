"""Profiles: the selector types and the partitioning rule, read from Turtle.

A profile is what makes the engine specific to a kind of graph without any
code. It declares the selector types a policy may use -- each a SPARQL SELECT
that projects ?resource -- and how a withheld resource takes others with it.
The bundled VCF Core profile (vcf_rdfizer_data/policy/vcf-core-profile.ttl)
covers graphs written by VCF-RDFizer; its comments document every term.
"""

from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path

from . import VCFP, PolicyError

#: The name `--profile vcf-core` refers to, and the default when none is given.
BUNDLED = {"vcf-core": "vcf-core-profile.ttl"}


@dataclass(frozen=True)
class SelectorType:
    iri: str
    query: str
    parameters: tuple          # property IRIs; each binds ?<local name>
    violations: str = None     # optional SELECT; any row means "cannot apply here"


@dataclass(frozen=True)
class Profile:
    iri: str
    ownership_path: str = None
    iri_subtree: bool = False
    unit_query: str = None
    selectors: dict = field(default_factory=dict)   # type IRI -> SelectorType


def variable(prop: str) -> str:
    """The query variable a parameter property binds: vcfp:start -> 'start'."""
    return prop.rstrip("/#").replace("#", "/").rsplit("/", 1)[1]


def load_profile(sources=(), extra_graph=None) -> Profile:
    """Merge profile files (paths or bundled names) and any declarations in `extra_graph`.

    `extra_graph` is the policy's own graph, so a policy can declare the selector
    types it uses. Exactly one vcfp:Profile must result.
    """
    import rdflib

    graph = rdflib.Graph()
    for source in sources or ("vcf-core",):
        path = files("vcf_rdfizer_data.policy") / BUNDLED[source] if source in BUNDLED else Path(source)
        graph.parse(str(path), format="turtle")
    if extra_graph is not None:
        graph += extra_graph

    def one(node, name, cast=str):
        value = graph.value(node, rdflib.URIRef(VCFP + name))
        return None if value is None else cast(value)

    profiles = list(graph.subjects(rdflib.RDF.type, rdflib.URIRef(VCFP + "Profile")))
    if len(profiles) != 1:
        raise PolicyError(f"expected exactly one vcfp:Profile, found {len(profiles)}")
    node = profiles[0]
    selectors = {}
    for kind in graph.subjects(rdflib.RDF.type, rdflib.URIRef(VCFP + "SelectorType")):
        selectors[str(kind)] = _selector(graph, kind, one)
    return Profile(
        iri=str(node),
        ownership_path=one(node, "ownershipPath"),
        iri_subtree=bool(one(node, "iriSubtree", lambda v: v.toPython())),
        unit_query=_checked(one(node, "unitQuery"), f"<{node}> vcfp:unitQuery", ("resource", "group")),
        selectors=selectors,
    )


def _selector(graph, kind, one) -> SelectorType:
    import rdflib

    query = one(kind, "query")
    if query is None:
        raise PolicyError(f"selector type <{kind}> has no vcfp:query")
    parameters = tuple(sorted(str(p) for p in graph.objects(kind, rdflib.URIRef(VCFP + "parameter"))))
    return SelectorType(str(kind), _checked(query, f"<{kind}> vcfp:query", ("resource",)),
                        parameters, _checked(one(kind, "violations"), f"<{kind}> vcfp:violations", ()))


def _checked(query, where, required):
    """Parse a declared query now, so a typo fails at load time, not mid-evaluation."""
    if query is None:
        return None
    from rdflib.plugins.sparql import prepareQuery

    try:
        prepared = prepareQuery(query)
    except Exception as error:  # rdflib raises several parser exception types
        raise PolicyError(f"{where} does not parse: {error}") from None
    projected = {str(v) for v in prepared.algebra.get("PV", [])}
    missing = [v for v in required if v not in projected]
    if missing:
        raise PolicyError(f"{where} must project {', '.join('?' + v for v in missing)}")
    return query
