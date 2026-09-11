"""Digest-pinned GFF3 acquisition and interval lookup."""

from bisect import bisect_left
from collections import defaultdict
import gzip
import hashlib
from pathlib import Path
import re
import tempfile
from urllib.parse import unquote, urlsplit
from urllib.request import build_opener, Request, url2pathname

from .session import NoRedirects


#: Where linker reference downloads and cached HTTP responses live. It sits in
#: this module rather than in ``runner`` because the argument parser needs it as
#: a default, and ``runner`` imports rdflib -- which would make merely printing
#: ``--help`` require the data-linking dependency.
DEFAULT_CACHE = Path.home() / ".cache" / "vcf-rdfizer" / "linkers"


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check_assembly(reference, declared, override=None):
    """An explicit assembly can fill missing metadata, never override a mismatch."""
    aliases = {"hg19": "GRCh37", "hg38": "GRCh38"}

    def identify(value):
        value = value.strip().strip('"')
        if value == declared:
            return declared
        names = set(re.findall(r"(?<![A-Za-z0-9])(GRCh37|GRCh38|hg19|hg38)(?![A-Za-z0-9])", value))
        names = {aliases.get(n, n) for n in names}
        if len(names) > 1:
            raise ValueError(f"Ambiguous reference assembly: {value}")
        return next(iter(names), None)

    expected = aliases.get(declared, declared)
    observed = identify(reference)
    explicit = identify(override) if override else None
    if observed and aliases.get(observed, observed) != expected:
        raise ValueError(f"Assembly mismatch: input is {observed}; reference bundle is {declared}")
    if override and explicit != expected:
        raise ValueError(f"Assembly mismatch: --assembly {override}; reference bundle is {declared}")
    if not observed and not explicit:
        raise ValueError(f"Cannot establish assembly from {reference!r}; supply --assembly {declared} after checking the input")


def acquire_reference(reference, cache_dir, *, offline=False, dry_run=False, stats=None):
    """Hash the stored bytes (compressed bytes for .gz), including on cache hits."""
    parts = urlsplit(reference.url)
    target = cache_dir / "references" / reference.sha256
    if target.is_file():
        if digest_file(target) != reference.sha256:
            raise ValueError(f"Reference cache digest mismatch: {target}")
        if stats is not None:
            stats["cache_hits"] += 1
        return target
    if parts.scheme == "file":
        if parts.netloc not in {"", "localhost"}:
            raise ValueError("Reference file URL must be local")
        source = Path(url2pathname(parts.path))
        if digest_file(source) != reference.sha256:
            raise ValueError(f"Reference digest mismatch: {source}")
        # Local bundles already are offline references. No copy on dry-run.
        if dry_run:
            return source
    elif parts.scheme != "https":
        raise ValueError("References must use file: or https: URLs")
    elif offline or dry_run:
        raise ValueError(f"Reference cache miss (network disabled): {reference.sha256}")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            if parts.scheme == "file":
                input_handle = source.open("rb")
            else:
                if stats is not None:
                    stats["requests"] += 1
                input_handle = build_opener(NoRedirects()).open(Request(reference.url, headers={"User-Agent": "VCF-RDFizer/reference-fetch"}), timeout=60)
                if stats is not None:
                    stats["final_service_status"] = input_handle.code
            with input_handle:
                for chunk in iter(lambda: input_handle.read(1024 * 1024), b""):
                    output.write(chunk)
                    if parts.scheme != "file" and stats is not None:
                        stats["bytes_transferred"] += len(chunk)
            output.close()
            if digest_file(temporary) != reference.sha256:
                raise ValueError(f"Reference digest mismatch: {reference.url}")
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
    return target


class IntervalIndex:
    """Sorted starts plus prefix-max ends: handles nested/overlapping features."""

    def __init__(self, path, reference):
        features = defaultdict(list)
        with path.open("rb") as raw:
            compressed = raw.read(2) == b"\x1f\x8b"
        opener = gzip.open if compressed else open
        with opener(path, "rt", encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                if line.startswith("##FASTA"):
                    break
                if line.startswith("#") or not line.strip():
                    continue
                columns = line.rstrip("\r\n").split("\t")
                if len(columns) != 9:
                    raise ValueError(f"GFF3 line {number}: expected nine columns")
                if columns[2] != reference.feature_type:
                    continue
                try:
                    start, end = int(columns[3]), int(columns[4])
                except ValueError as exc:
                    raise ValueError(f"GFF3 line {number}: invalid coordinates") from exc
                if start < 1 or end < start:
                    raise ValueError(f"GFF3 line {number}: invalid interval")
                attributes = dict(item.split("=", 1) for item in columns[8].split(";") if "=" in item)
                identifier = unquote(attributes.get(reference.id_attribute, ""))
                if not identifier:
                    raise ValueError(f"GFF3 line {number}: missing {reference.id_attribute}")
                features[columns[0]].append((start, end, identifier))
        self.chromosomes = {}
        for chrom, rows in features.items():
            rows.sort()
            maximum, ends = 0, []
            for _, end, _ in rows:
                maximum = max(maximum, end)
                ends.append(maximum)
            self.chromosomes[chrom] = (rows, [r[0] for r in rows], ends)
        if not features:
            raise ValueError(f"GFF3 contains no {reference.feature_type!r} features")

    def overlaps(self, key):
        rows, starts, max_ends = self.chromosomes.get(key.chrom, ([], [], []))
        index = bisect_left(starts, key.end + 1) - 1
        while index >= 0 and max_ends[index] >= key.start:
            start, end, identifier = rows[index]
            if end >= key.start:
                yield identifier
            index -= 1
