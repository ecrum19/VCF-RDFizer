"""Policy demonstrator v0.1.0: attach ODRL policies to RDF graphs and release governed views.

Three generic steps, each configured in Turtle rather than code:

    select     a rule's target is an IRI, or a selection computed by a declared
               SPARQL selector type (profile.py)
    partition  the profile's ownership rule extends each selection to what it owns
    decide     ODRL: a binding permission must own a resource, and no binding
               prohibition may; deny wins (engine.py)

The bundled VCF Core profile makes it work on VCF-RDFizer graphs; vcf_oracle.py
adds an independent check against the source VCFs. This is governed release, not
anonymization. The specification is docs/policy-demonstrator.md.
"""

VERSION = "0.1.0"

ODRL = "http://www.w3.org/ns/odrl/2/"
VCFP = "https://w3id.org/vcf-rdfizer/policy#"
VCFC = "https://w3id.org/vcf-core/vocab#"
OBO = "http://purl.obolibrary.org/obo/"

#: Stated in every release manifest, so no artifact can be mistaken for more.
DISCLOSURE_MODEL = "governed release; not anonymization"


class PolicyError(ValueError):
    """A policy, graph or request that v0.1.0 cannot evaluate.

    Always fatal. A rule that is parsed and then not applied would leave the
    operator believing a release is governed when it is not, so the tool stops
    instead of guessing or skipping.
    """
