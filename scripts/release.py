#!/usr/bin/env python3
"""Bump repository release metadata from a single semantic version.

Usage:
    python3 scripts/release.py 1.2.0

The script updates the project version markers, README citation text, the conda
recipe version, and a few release-oriented examples. It can also fetch the
GitHub source tarball SHA256 for the conda recipe when network access is
available.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def replace_once(text: str, pattern: str, replacement: str, *, file: Path) -> str:
    new_text, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise SystemExit(f"Expected exactly one match for {pattern!r} in {file}")
    return new_text


def sha256_for_url(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as response:
        digest = hashlib.sha256()
        for chunk in iter(lambda: response.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Bump VCF-RDFizer release metadata.")
    parser.add_argument("version", help="Target semantic version, for example 1.2.0")
    parser.add_argument(
        "--conda-sha256",
        default=None,
        help="Optional sha256 for the GitHub release tarball. If omitted, the script will try to fetch it.",
    )
    args = parser.parse_args()

    version = args.version.strip()
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise SystemExit("version must match MAJOR.MINOR.PATCH, for example 1.2.0")

    tag = f"v{version}"

    files = {
        ROOT / "pyproject.toml": [
            (r'^(version\s*=\s*)"\d+\.\d+\.\d+"$', rf'\g<1>"{version}"'),
        ],
        ROOT / "CITATION.cff": [
            (r'^(version:\s*)"\d+\.\d+\.\d+"$', rf'\g<1>"{version}"'),
        ],
        ROOT / "README.md": [
            (r'(Version\s+)\d+\.\d+\.\d+', rf"\g<1>{version}"),
            (r'(version\s*=\s*\{)\d+\.\d+\.\d+(\})', rf"\g<1>{version}\2"),
        ],
        ROOT / "conda-recipe" / "README.md": [
            (r'v\d+\.\d+\.\d+', tag),
        ],
        ROOT / "test" / "test_vcf_rdfizer_cross_platform_unit.py": [
            (r'("ecrum19/vcf-rdfizer",\s*")\d+\.\d+\.\d+(")', rf"\g<1>{version}\2"),
            (r'("ecrum19/vcf-rdfizer:)\d+\.\d+\.\d+(")', rf"\g<1>{version}\2"),
        ],
    }

    for path, replacements in files.items():
        text = read_text(path)
        if path == ROOT / "conda-recipe" / "README.md":
            text, count = re.subn(r"v\d+\.\d+\.\d+", tag, text)
            if count < 2:
                raise SystemExit(f"Expected to update at least two version references in {path}")
        else:
            for pattern, replacement in replacements:
                text = replace_once(text, pattern, replacement, file=path)
        write_text(path, text)

    meta_path = ROOT / "conda-recipe" / "meta.yaml"
    meta_text = read_text(meta_path)
    meta_text = replace_once(
        meta_text,
        r'^\{% set version = "\d+\.\d+\.\d+" %\}$',
        f'{{% set version = "{version}" %}}',
        file=meta_path,
    )

    sha256 = args.conda_sha256
    if sha256 is None:
        url = f"https://github.com/ecrum19/VCF-RDFizer/archive/refs/tags/{tag}.tar.gz"
        try:
            sha256 = sha256_for_url(url)
        except Exception as exc:
            sha256 = "REPLACE_WITH_GITHUB_TARBALL_SHA256"
            print(
                f"Warning: could not fetch {url} to compute sha256 automatically ({exc}).",
                file=sys.stderr,
            )
            print(
                "Use the tarball URL above to compute the sha256 before opening the conda-forge PR.",
                file=sys.stderr,
            )

    meta_text = replace_once(
        meta_text,
        r'^\s*sha256:\s*.*$',
        f"  sha256: {sha256}",
        file=meta_path,
    )
    write_text(meta_path, meta_text)

    print(f"Updated release metadata to {version}")
    print(f"Next steps:")
    print(f"  git add pyproject.toml CITATION.cff README.md conda-recipe/meta.yaml conda-recipe/README.md test/test_vcf_rdfizer_cross_platform_unit.py")
    print(f"  git commit -m \"Release {tag}\"")
    print(f"  git tag -a {tag} -m \"VCF-RDFizer {tag}\"")
    print("  git push origin main")
    print(f"  git push origin {tag}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
