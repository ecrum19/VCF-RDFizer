#!/usr/bin/env python3
"""Generate the policy demonstrator's synthetic cohort.

    python3 examples/policy/make_fixture.py [OUT_DIR]  # writes P001.vcf ... P005.vcf, fixture.json

Five participants, one single-sample VCF each. Positions are GRCh38 and fall in
real loci -- BRCA1, APOE, and background sites on chr1 and chr20 -- so the region
and variant selectors are exercised against real coordinates. Two variants are
real and named: rs429358 (the APOE e4-defining SNP) and rs7412. Every other
allele, and every genotype, is synthetic and drawn from a fixed seed: the
genotypes identify no one, which is the point of using a fixture for a privacy
demonstration. REF alleles other than the two named variants are not checked
against the reference genome.

The site catalogue also plants the cases the tests need:
  * records one base outside each end of the BRCA1 window, so the region
    bounds are tested as inclusive;
  * a decoy at rs429358's position with a different ALT, so the variant
    selector is tested on alleles and not on position alone.

The output is deterministic: rerunning it rewrites byte-identical files.
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SEED = 20260925
ASSEMBLY = "GRCh38"
FILE_DATE = "20260925"

CONTIGS = {"chr1": 248956422, "chr17": 83257441, "chr19": 58617616, "chr20": 64444167}

BRCA1 = {"chrom": "chr17", "start": 43044295, "end": 43125483}
APOE_E4 = {"chrom": "chr19", "pos": 44908684, "ref": "T", "alt": "C", "id": "rs429358"}
APOE_E2 = {"chrom": "chr19", "pos": 44908822, "ref": "C", "alt": "T", "id": "rs7412"}

# Consent per participant, as DUO terms. P004 withdrew.
PARTICIPANTS = {
    "P001": {"consent": ["DUO_0000042", "DUO_0000043"]},
    "P002": {"consent": ["DUO_0000042", "DUO_0000043"]},
    "P003": {"consent": ["DUO_0000006"]},
    "P004": {"consent": ["DUO_0000042"], "withdrawn": True},
    "P005": {"consent": ["DUO_0000007"]},
}

REQUESTERS = {
    "gru": {"assignee": "https://example.org/party/research-consortium", "purpose": "DUO_0000042",
            "label": "General-research consortium"},
    "alz": {"assignee": "https://example.org/party/alz-consortium", "purpose": "DUO_0000007",
            "label": "Alzheimer's consortium"},
    "clinical": {"assignee": "https://example.org/party/clinical-genetics", "purpose": "DUO_0000043",
                 "label": "Clinical genetics lab"},
}

BASES = "ACGT"


def site_catalogue(rng: random.Random) -> list[dict]:
    """Every site any participant may carry, before sampling."""
    sites = []

    def snv(chrom, pos, tag, ident="."):
        ref = rng.choice(BASES)
        alt = rng.choice([b for b in BASES if b != ref])
        sites.append({"chrom": chrom, "pos": pos, "ref": ref, "alt": alt, "id": ident, "tag": tag})

    # BRCA1: twelve sites inside the window, and one just outside each end.
    for pos in sorted(rng.sample(range(BRCA1["start"] + 1, BRCA1["end"]), 12)):
        snv("chr17", pos, "brca1")
    snv("chr17", BRCA1["start"] - 1, "brca1-edge-outside")
    snv("chr17", BRCA1["end"] + 1, "brca1-edge-outside")
    snv("chr17", BRCA1["start"], "brca1-edge-inside")
    snv("chr17", BRCA1["end"], "brca1-edge-inside")

    # APOE: the two named variants, a decoy at the e4 position, and neighbours.
    sites.append({**APOE_E4, "tag": "apoe-e4"})
    sites.append({**APOE_E2, "tag": "apoe-e2"})
    sites.append({"chrom": "chr19", "pos": APOE_E4["pos"], "ref": "T", "alt": "G", "id": ".",
                  "tag": "apoe-e4-decoy"})
    for pos in sorted(rng.sample(range(44905791, 44909393), 4)):
        if pos not in (APOE_E4["pos"], APOE_E2["pos"]):
            snv("chr19", pos, "apoe-region")

    # Background, governed by consent alone.
    for chrom, lo, hi in (("chr1", 1_000_000, 240_000_000), ("chr20", 1_000_000, 60_000_000)):
        for pos in sorted(rng.sample(range(lo, hi), 10)):
            snv(chrom, pos, "background")
    return sites


def carried(rng: random.Random, participant: str, sites: list[dict]) -> list[dict]:
    """The sites one participant carries, with the scenario's cases forced in."""
    forced = {
        "apoe-e4": participant in ("P001", "P003", "P004", "P005"),
        "apoe-e4-decoy": participant == "P002",
        "brca1-edge-outside": participant == "P001",
        "brca1-edge-inside": participant == "P001",
    }
    chosen = []
    for site in sites:
        if site["tag"] in forced:
            keep = forced[site["tag"]]
        elif site["tag"] == "apoe-e2":
            keep = participant in ("P002", "P003", "P005")
        else:
            keep = rng.random() < 0.7
        if keep:
            chosen.append(site)
    return chosen


def write_vcf(path: Path, participant: str, records: list[dict], rng: random.Random) -> None:
    order = {c: i for i, c in enumerate(CONTIGS)}
    records = sorted(records, key=lambda s: (order[s["chrom"]], s["pos"], s["alt"]))
    lines = [
        "##fileformat=VCFv4.3",
        f"##fileDate={FILE_DATE}",
        "##source=vcf-rdfizer-policy-demonstrator-fixture",
        f"##reference={ASSEMBLY}",
    ]
    lines += [f"##contig=<ID={c},length={n},assembly={ASSEMBLY}>" for c, n in CONTIGS.items()]
    lines += [
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="Total read depth">',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
        '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">',
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + participant,
    ]
    for site in records:
        depth = rng.randint(20, 60)
        genotype = "1/1" if rng.random() < 0.3 else "0/1"
        qual = rng.randint(50, 99)
        lines.append("\t".join([site["chrom"], str(site["pos"]), site["id"], site["ref"], site["alt"],
                                str(qual), "PASS", f"DP={depth}", "GT:DP", f"{genotype}:{depth}"]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(out_dir: Path = HERE) -> None:
    out_dir = Path(out_dir)
    rng = random.Random(SEED)
    sites = site_catalogue(rng)
    files = {}
    for participant in PARTICIPANTS:
        records = carried(rng, participant, sites)
        write_vcf(out_dir / f"{participant}.vcf", participant, records, rng)
        files[f"{participant}.vcf"] = {"participant": participant, "records": len(records)}

    fixture = {
        "description": "Synthetic single-sample cohort for the v0.1.0 policy demonstrator. "
                       "Genotypes are synthetic; see make_fixture.py.",
        "seed": SEED,
        "assembly": ASSEMBLY,
        "participants": PARTICIPANTS,
        "files": files,
        "loci": {"brca1": BRCA1, "apoe_e4": APOE_E4, "apoe_e2": APOE_E2},
        "requesters": REQUESTERS,
    }
    (out_dir / "fixture.json").write_text(json.dumps(fixture, indent=2) + "\n", encoding="utf-8")
    for name, info in files.items():
        print(f"wrote {name}: {info['records']} records")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else HERE)
