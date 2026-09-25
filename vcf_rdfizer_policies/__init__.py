"""Policy demonstrator v0.1.0: ODRL policies attached to VCF-RDFizer graphs.

Policies attach to files, genomic regions and single variants. One policy set
yields a different release view per request (who asks, and for what purpose),
and each view can be checked against an oracle computed from the source VCFs.

This is governed release, not anonymization. The specification, including what
v0.1.0 deliberately leaves out, is docs/policy-demonstrator.md.
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
