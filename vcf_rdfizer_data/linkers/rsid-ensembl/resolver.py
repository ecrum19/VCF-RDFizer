"""Ensembl variation POST example: one request per deduplicated batch.

API contract: https://rest.ensembl.org/documentation/info/variation_post
Only confirm returned identifiers. Do not infer clinical significance, allele
equivalence, frequencies, or current identifiers for merged/retired rsIDs.
"""

from collections.abc import Iterable, Sequence
from urllib.parse import quote

from vcf_rdfizer_linking import Link, LinkKey, LinkerContext


def resolve(batch: Sequence[LinkKey], ctx: LinkerContext) -> Iterable[Link]:
    response = ctx.session.post(
        "https://rest.ensembl.org/variation/homo_sapiens",
        json={"ids": [key.token for key in batch]},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Expected an Ensembl object keyed by requested identifiers")
    for key in batch:
        variant = payload.get(key.token)
        if variant is None:
            continue
        if not isinstance(variant, dict):
            raise ValueError(f"Invalid Ensembl variation object for {key.token}")
        if variant.get("error"):
            continue
        if not isinstance(variant.get("name"), str):
            raise ValueError(f"Ensembl response for {key.token} has no variant name")
        # Preserve the submitted identifier; the website handles redirects.
        yield Link(key, "https://www.ensembl.org/Homo_sapiens/Variation/Explore?v=" + quote(key.token, safe=""))
