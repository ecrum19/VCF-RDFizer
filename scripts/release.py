#!/usr/bin/env python3
"""Prepare or validate VCF-RDFizer release metadata.

Before creating a Git tag, update the committed release metadata:

    python3 scripts/release.py 1.2.4

After pushing ``v1.2.4``, fetch the immutable source-archive checksum needed
for a conda-forge feedstock update:

    python3 scripts/release.py 1.2.4 --fetch-conda-sha256

CI uses ``--check-tag``. It never rewrites metadata: a release tag must point
at source whose committed metadata already matches that tag.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION_PATTERN = r"\d+\.\d+\.\d+"
PLACEHOLDER_CONDA_SHA256 = "REPLACE_WITH_GITHUB_TARBALL_SHA256"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def replace_once(text: str, pattern: str, replacement: str, *, file: Path) -> str:
    new_text, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"Expected exactly one match for {pattern!r} in {file}")
    return new_text


def version_from_tag(tag: str) -> str:
    match = re.fullmatch(rf"v({VERSION_PATTERN})", tag.strip())
    if not match:
        raise ValueError("tag must match vMAJOR.MINOR.PATCH, for example v1.2.4")
    return match.group(1)


def validate_version(version: str) -> str:
    value = version.strip()
    if not re.fullmatch(VERSION_PATTERN, value):
        raise ValueError("version must match MAJOR.MINOR.PATCH, for example 1.2.4")
    return value


def validate_sha256(value: str) -> str:
    digest = value.strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("--conda-sha256 must be a 64-character lowercase hexadecimal SHA256")
    return digest


def source_archive_url(version: str) -> str:
    return f"https://github.com/ecrum19/VCF-RDFizer/archive/refs/tags/v{version}.tar.gz"


def sha256_for_url(url: str) -> str:
    """Download *url* and return its SHA256, with a system-curl TLS fallback."""
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            digest = hashlib.sha256()
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except urllib.error.URLError as url_error:
        curl = shutil.which("curl")
        if curl is None:
            raise url_error

        # Some macOS Python installations do not inherit the system CA bundle.
        # Curl uses the system trust store and keeps the release command usable.
        with tempfile.TemporaryDirectory(prefix="vcf-rdfizer-release-") as directory:
            archive = Path(directory) / "source.tar.gz"
            result = subprocess.run(
                [curl, "--fail", "--location", "--retry", "3", "--output", str(archive), url],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                details = result.stderr.strip() or result.stdout.strip() or "unknown curl error"
                raise ValueError(f"could not download {url}: {details}") from url_error
            return hashlib.sha256(archive.read_bytes()).hexdigest()


def release_metadata_errors(version: str) -> list[str]:
    """Return every committed release marker that disagrees with *version*."""
    expected_tag = f"v{version}"
    checks = (
        (ROOT / "pyproject.toml", rf'^version\s*=\s*"{re.escape(version)}"$'),
        (ROOT / "CITATION.cff", rf'^version:\s*"{re.escape(version)}"$'),
        (ROOT / "README.md", rf"Version\s+{re.escape(version)}"),
        (ROOT / "README.md", rf"version\s*=\s*\{{{re.escape(version)}\}}"),
        (
            ROOT / "conda-recipe" / "meta.yaml",
            rf'^\{{% set version = "{re.escape(version)}" %\}}$',
        ),
    )
    errors = []
    for path, pattern in checks:
        if not re.search(pattern, read_text(path), flags=re.MULTILINE):
            errors.append(f"{path.relative_to(ROOT)} does not declare version {version}")

    conda_readme = read_text(ROOT / "conda-recipe" / "README.md")
    if conda_readme.count(expected_tag) < 2:
        errors.append(
            f"conda-recipe/README.md must contain at least two {expected_tag} release examples"
        )
    return errors


def update_release_metadata(version: str, conda_sha256: str | None) -> None:
    """Synchronize local source metadata before a release tag is created."""
    files = {
        ROOT / "pyproject.toml": [
            (rf'^(version\s*=\s*)"{VERSION_PATTERN}"$', rf'\g<1>"{version}"'),
        ],
        ROOT / "CITATION.cff": [
            (rf'^(version:\s*)"{VERSION_PATTERN}"$', rf'\g<1>"{version}"'),
        ],
        ROOT / "README.md": [
            (rf'(Version\s+){VERSION_PATTERN}', rf"\g<1>{version}"),
            (rf'(version\s*=\s*\{{){VERSION_PATTERN}(\}})', rf"\g<1>{version}\2"),
        ],
    }
    for path, replacements in files.items():
        text = read_text(path)
        for pattern, replacement in replacements:
            text = replace_once(text, pattern, replacement, file=path)
        write_text(path, text)

    conda_readme_path = ROOT / "conda-recipe" / "README.md"
    conda_readme, count = re.subn(rf"v{VERSION_PATTERN}", f"v{version}", read_text(conda_readme_path))
    if count < 2:
        raise ValueError(f"Expected to update at least two version references in {conda_readme_path}")
    write_text(conda_readme_path, conda_readme)

    meta_path = ROOT / "conda-recipe" / "meta.yaml"
    meta_text = read_text(meta_path)
    previous_match = re.search(rf'^\{{% set version = "({VERSION_PATTERN})" %\}}$', meta_text, re.MULTILINE)
    if previous_match is None:
        raise ValueError(f"Could not find the conda recipe version in {meta_path}")
    previous_version = previous_match.group(1)
    meta_text = replace_once(
        meta_text,
        rf'^\{{% set version = "{VERSION_PATTERN}" %\}}$',
        f'{{% set version = "{version}" %}}',
        file=meta_path,
    )
    if conda_sha256 is not None:
        meta_text = replace_once(
            meta_text,
            r"^\s*sha256:\s*.*$",
            f"  sha256: {conda_sha256}",
            file=meta_path,
        )
    elif previous_version != version:
        # The archive for the new tag does not exist yet. Never retain a
        # checksum from the previous release, because it would be invalid.
        meta_text = replace_once(
            meta_text,
            r"^\s*sha256:\s*.*$",
            f"  sha256: {PLACEHOLDER_CONDA_SHA256}",
            file=meta_path,
        )
    write_text(meta_path, meta_text)


def print_next_steps(version: str, conda_sha256: str | None) -> None:
    tag = f"v{version}"
    print(f"Updated release metadata to {version}")
    if conda_sha256 is not None:
        print("The conda source-archive checksum is now recorded.")
        print("Next steps:")
        print("  git add conda-recipe/meta.yaml")
        print(f'  git commit -m "Record conda checksum for {tag}"')
        print("  git push origin main")
        print("  # Then update conda-forge/vcf-rdfizer-feedstock with conda-recipe/meta.yaml")
        return

    print("Next steps:")
    print("  git add pyproject.toml CITATION.cff README.md conda-recipe/meta.yaml conda-recipe/README.md")
    print(f'  git commit -m "Release {tag}"')
    print("  git push origin main")
    print(f'  git tag -a {tag} -m "VCF-RDFizer {tag}"')
    print(f"  git push origin {tag}")
    if conda_sha256 is None:
        print(f"  python3 scripts/release.py {version} --fetch-conda-sha256")
    print("  # Then update conda-forge/vcf-rdfizer-feedstock with conda-recipe/meta.yaml")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare or validate VCF-RDFizer release metadata.")
    parser.add_argument("version", nargs="?", help="Target semantic version, for example 1.2.4")
    parser.add_argument(
        "--check-tag",
        help="Validate that committed metadata matches vMAJOR.MINOR.PATCH without editing files.",
    )
    checksum_group = parser.add_mutually_exclusive_group()
    checksum_group.add_argument(
        "--conda-sha256",
        help="Set the SHA256 for the GitHub tag archive after that tag has been pushed.",
    )
    checksum_group.add_argument(
        "--fetch-conda-sha256",
        action="store_true",
        help="Fetch and set the SHA256 for the already-pushed GitHub tag archive.",
    )
    args = parser.parse_args(argv)

    try:
        if args.check_tag:
            if args.version or args.conda_sha256 or args.fetch_conda_sha256:
                raise ValueError("--check-tag cannot be combined with a version or conda checksum option")
            version = version_from_tag(args.check_tag)
            errors = release_metadata_errors(version)
            if errors:
                raise ValueError("Release metadata validation failed:\n- " + "\n- ".join(errors))
            print(f"Release metadata matches {args.check_tag.strip()}")
            return 0

        if not args.version:
            raise ValueError("provide a semantic version or --check-tag vMAJOR.MINOR.PATCH")
        version = validate_version(args.version)
        conda_sha256 = (
            validate_sha256(args.conda_sha256) if args.conda_sha256 is not None else None
        )
        if args.fetch_conda_sha256:
            conda_sha256 = sha256_for_url(source_archive_url(version))
            print(f"Fetched SHA256 for v{version}: {conda_sha256}")

        update_release_metadata(version, conda_sha256)
        errors = release_metadata_errors(version)
        if errors:
            raise ValueError("Updated metadata did not validate:\n- " + "\n- ".join(errors))
        print_next_steps(version, conda_sha256)
        return 0
    except (OSError, ValueError, urllib.error.URLError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
