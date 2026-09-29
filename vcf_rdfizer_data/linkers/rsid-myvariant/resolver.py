"""MyVariant.info batch query: one POST per deduplicated batch of up to 1,000 rsIDs.

API contract: https://docs.myvariant.info/en/latest/doc/variant_query_service.html
The reply lists one hit per allele (several for a multi-allelic rsID) and a
`notfound` entry for an unknown one. Only confirm an rsID that a hit's dbSNP
record carries; do not infer alleles, clinical significance or frequencies.
"""

from collections.abc import Iterable, Sequence
from urllib.parse import quote

from vcf_rdfizer_linking import Link, LinkKey, LinkerContext


def rsids(hit: dict) -> set:
    records = hit.get("dbsnp") or []
    return {r.get("rsid") for r in (records if isinstance(records, list) else [records]) if isinstance(r, dict)}


def resolve(batch: Sequence[LinkKey], ctx: LinkerContext) -> Iterable[Link]:
    response = ctx.session.post(
        "https://myvariant.info/v1/query",
        json={"q": [key.token for key in batch], "scopes": "dbsnp.rsid",
              "fields": "dbsnp.rsid", "assembly": "hg38"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Expected a MyVariant.info list of query results")
    keys, confirmed = {key.token: key for key in batch}, set()
    for hit in payload:
        if not isinstance(hit, dict) or hit.get("query") not in keys:
            raise ValueError(f"Invalid MyVariant.info result: {hit!r:.200}")
        if not hit.get("notfound") and hit["query"] in rsids(hit):
            confirmed.add(hit["query"])
    for token in sorted(confirmed):
        yield Link(keys[token], "https://identifiers.org/dbsnp:" + quote(token, safe=""))
