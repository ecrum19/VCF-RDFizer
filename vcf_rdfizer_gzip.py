#!/usr/bin/env python3
"""Uncompressed size of a gzip/BGZF file, without decompressing it when possible.

VCF-RDFizer records ``input_vcf_size_bytes`` as the *uncompressed* size of the
source VCF so that compression ratios are comparable across plain and
compressed inputs. Obtaining that number by inflating the whole file costs a
full pass over a cohort-scale VCF purely for a metric.

Three strategies, in order of preference. Every result carries the method that
produced it so a reported size is always auditable:

``bgzf``
    BGZF (``bgzip``) files - what ``bcftools``/``tabix``/``htslib`` produce and
    what indexed ``.vcf.gz`` files in practice are - are a sequence of gzip
    members, each holding at most 64 KiB and declaring its own compressed
    length in a ``BC`` extra subfield. Walking those declarations and summing
    each member's ``ISIZE`` trailer is exact, needs no inflate at all, and is
    immune to the 32-bit ``ISIZE`` wrap because every member is small.

``gzip-trailer``
    A plain single-member gzip stores the uncompressed size in its last four
    bytes, but only modulo 2**32, and a concatenated multi-member file exposes
    only its final member's value. Both hazards are resolved by inflating a
    small prefix to measure this file's actual compression ratio and requiring
    exactly one ``ISIZE + k * 2**32`` candidate to agree with it.

``inflate``
    Full streaming inflate. Always correct, always available, and the fallback
    whenever the checks above cannot settle the answer.

The module is dependency-free so it can run both on the host (for
``--estimate-size``) and inside the image (from ``run_conversion.sh``).
"""

from __future__ import annotations

import argparse
import struct
import sys
import zlib
from pathlib import Path

GZIP_MAGIC = b"\x1f\x8b"
GZIP_DEFLATE = 0x08
FEXTRA = 0x04
FNAME = 0x08
FCOMMENT = 0x10
FHCRC = 0x02

ISIZE_MODULUS = 1 << 32
# A BGZF member never holds more than 64 KiB, so its ISIZE can never wrap.
BGZF_MAX_BLOCK_ISIZE = 1 << 16

# DEFLATE's proven upper bound is 1032:1 (a 258-byte match encoded in ~2 bits).
# Nothing can decompress by more than this, so it bounds the candidate search.
MAX_DEFLATE_RATIO = 1032.0
# Inflate at most this much before extrapolating a ratio. A VCF body is highly
# homogeneous, so a prefix this size characterises the whole file closely.
RATIO_SAMPLE_BYTES = 16 * 1024 * 1024
# A candidate must land this close to the sampled projection to be accepted.
# Candidates are 4 GiB apart, so this is a wide margin for a correct answer and
# still rejects a multi-member file whose trailer describes one member only.
RATIO_TOLERANCE = 0.25

READ_CHUNK_BYTES = 1024 * 1024


class GzipStructureError(ValueError):
    """The file is not shaped the way the chosen fast path requires."""


def looks_like_gzip(path: Path) -> bool:
    """Return whether the file starts with the gzip magic bytes."""
    try:
        with path.open("rb") as handle:
            return handle.read(2) == GZIP_MAGIC
    except OSError:
        return False


def _read_exactly(handle, count: int) -> bytes:
    data = handle.read(count)
    if len(data) != count:
        raise GzipStructureError("unexpected end of gzip stream")
    return data


def _bgzf_block_size(header: bytes, handle) -> int:
    """Return the total on-disk length of one BGZF member, or raise."""
    if header[:2] != GZIP_MAGIC or header[2] != GZIP_DEFLATE:
        raise GzipStructureError("not a gzip member")
    flags = header[3]
    if not flags & FEXTRA:
        raise GzipStructureError("gzip member has no FEXTRA field")
    (xlen,) = struct.unpack("<H", header[10:12])
    extra = _read_exactly(handle, xlen)

    offset = 0
    while offset + 4 <= len(extra):
        subfield_id = extra[offset : offset + 2]
        (subfield_len,) = struct.unpack("<H", extra[offset + 2 : offset + 4])
        payload = extra[offset + 4 : offset + 4 + subfield_len]
        if subfield_id == b"BC":
            if subfield_len != 2:
                raise GzipStructureError("malformed BGZF BC subfield")
            # BSIZE is "total block size minus one".
            return struct.unpack("<H", payload)[0] + 1
        offset += 4 + subfield_len

    raise GzipStructureError("gzip member is not BGZF (no BC subfield)")


def bgzf_uncompressed_size(path: Path) -> int:
    """Sum every BGZF member's ISIZE by walking block headers only.

    Raises :class:`GzipStructureError` when the file is not valid BGZF, which
    includes an ordinary single-member gzip.
    """
    total = 0
    file_size = path.stat().st_size
    with path.open("rb") as handle:
        offset = 0
        while offset < file_size:
            handle.seek(offset)
            header = _read_exactly(handle, 12)
            block_size = _bgzf_block_size(header, handle)
            if block_size < 28 or offset + block_size > file_size:
                raise GzipStructureError("BGZF block size runs past end of file")
            handle.seek(offset + block_size - 4)
            (isize,) = struct.unpack("<I", _read_exactly(handle, 4))
            if isize >= BGZF_MAX_BLOCK_ISIZE:
                raise GzipStructureError("BGZF block declares an oversized payload")
            total += isize
            offset += block_size
    return total


def _skip_gzip_header(handle) -> None:
    """Advance past one gzip member header, leaving the deflate stream next."""
    header = _read_exactly(handle, 10)
    if header[:2] != GZIP_MAGIC or header[2] != GZIP_DEFLATE:
        raise GzipStructureError("not a gzip stream")
    flags = header[3]
    if flags & FEXTRA:
        (xlen,) = struct.unpack("<H", _read_exactly(handle, 2))
        _read_exactly(handle, xlen)
    for flag in (FNAME, FCOMMENT):
        if flags & flag:
            while _read_exactly(handle, 1) != b"\x00":
                pass
    if flags & FHCRC:
        _read_exactly(handle, 2)


class MemberSample:
    """What inflating a bounded prefix of the first gzip member revealed."""

    __slots__ = ("produced", "consumed", "member_complete", "trailing_bytes")

    def __init__(self, produced: int, consumed: int, member_complete: bool, trailing_bytes: int):
        self.produced = produced
        self.consumed = consumed
        #: The member ended within the sample budget.
        self.member_complete = member_complete
        #: Bytes after the member. Exactly 8 (its trailer) means single-member.
        self.trailing_bytes = trailing_bytes

    @property
    def ratio(self) -> float:
        return self.produced / self.consumed if self.consumed else 0.0

    @property
    def single_member(self) -> bool:
        return self.member_complete and self.trailing_bytes == 8


def sample_first_member(path: Path, sample_bytes: int = RATIO_SAMPLE_BYTES) -> MemberSample:
    """Inflate a bounded prefix of the first member to characterise the file.

    The cost is capped by ``sample_bytes`` regardless of how large the file is,
    which is what keeps the fast path constant-time.
    """
    decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
    produced = 0
    consumed = 0
    with path.open("rb") as handle:
        _skip_gzip_header(handle)
        header_bytes = handle.tell()
        while produced < sample_bytes:
            chunk = handle.read(READ_CHUNK_BYTES)
            if not chunk:
                break
            produced += len(decompressor.decompress(chunk))
            consumed += len(chunk)
            if decompressor.eof:
                # `unused_data` holds the member's 8-byte trailer plus anything
                # that follows it, so its length identifies a concatenation.
                trailing = len(decompressor.unused_data)
                deflate_bytes = consumed - trailing
                return MemberSample(
                    produced, header_bytes + deflate_bytes, True, trailing
                )
    if consumed <= 0 or produced <= 0:
        raise GzipStructureError("could not sample the deflate stream")
    return MemberSample(produced, header_bytes + consumed, False, 0)


def trailer_isize(path: Path) -> int:
    """Read the final member's ISIZE field (uncompressed size modulo 2**32)."""
    with path.open("rb") as handle:
        handle.seek(-4, 2)
        return struct.unpack("<I", _read_exactly(handle, 4))[0]


def resolve_trailer_size(path: Path) -> tuple[int, str]:
    """Resolve the 32-bit ISIZE trailer into a full uncompressed size.

    Raises :class:`GzipStructureError` unless the file is provably a single
    member measured in full, or exactly one ``ISIZE + k * 2**32`` candidate
    agrees with the ratio measured from the file's own prefix. That single
    requirement rules out both a >4 GiB wrap the trailer cannot express and a
    concatenated file whose trailer describes only its final member.
    """
    compressed_size = path.stat().st_size
    if compressed_size <= 18:
        raise GzipStructureError("file is too small to carry a gzip trailer")

    sample = sample_first_member(path)
    if sample.member_complete:
        if sample.single_member:
            # The whole file fitted in the sample budget: this is not an
            # estimate resolved against a trailer, it is the measured size.
            return sample.produced, "gzip-sample"
        raise GzipStructureError(
            "concatenated gzip members; the trailer covers only the last one"
        )

    projected = sample.ratio * compressed_size
    if projected <= 0:
        raise GzipStructureError("could not project an uncompressed size")

    isize = trailer_isize(path)
    upper_bound = compressed_size * MAX_DEFLATE_RATIO
    accepted = [
        candidate
        for candidate in range(isize, int(upper_bound) + ISIZE_MODULUS, ISIZE_MODULUS)
        if candidate <= upper_bound
        and abs(candidate - projected) <= RATIO_TOLERANCE * projected
    ]
    if len(accepted) != 1:
        raise GzipStructureError(
            f"gzip trailer is ambiguous ({len(accepted)} plausible sizes)"
        )
    return accepted[0], "gzip-trailer"


def inflate_uncompressed_size(path: Path) -> int:
    """Stream the whole file through zlib and count the output bytes.

    Raises :class:`zlib.error` on a truncated stream so a damaged input is
    reported rather than silently measured as short.
    """
    total = 0
    decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(READ_CHUNK_BYTES)
            if not chunk:
                break
            total += len(decompressor.decompress(chunk))
            while decompressor.eof and decompressor.unused_data:
                # Concatenated members: continue with the next one.
                tail = decompressor.unused_data
                decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                total += len(decompressor.decompress(tail))
        total += len(decompressor.flush())
    if not decompressor.eof:
        raise zlib.error(f"truncated or corrupt gzip stream: {path}")
    return total


def uncompressed_size(path: Path, *, exact: bool = False) -> tuple[int, str]:
    """Return ``(uncompressed_bytes, method)`` for a plain or gzip file.

    Set ``exact`` to skip the structural fast paths and always inflate.
    """
    path = Path(path)
    if not looks_like_gzip(path):
        return path.stat().st_size, "stat"

    if not exact:
        try:
            return bgzf_uncompressed_size(path), "bgzf"
        except (GzipStructureError, OSError, struct.error):
            pass
        try:
            return resolve_trailer_size(path)
        except (GzipStructureError, OSError, zlib.error, struct.error):
            pass

    return inflate_uncompressed_size(path), "inflate"


def uncompressed_size_without_inflating(path: Path) -> tuple[int | None, str]:
    """Structural size only: return ``(None, "unknown")`` rather than inflating.

    Callers that merely want a preflight estimate would gain nothing from a
    full decompression pass, so they use this instead of
    :func:`uncompressed_size`.
    """
    path = Path(path)
    if not looks_like_gzip(path):
        return path.stat().st_size, "stat"
    try:
        return bgzf_uncompressed_size(path), "bgzf"
    except (GzipStructureError, OSError, struct.error):
        pass
    try:
        return resolve_trailer_size(path)
    except (GzipStructureError, OSError, zlib.error, struct.error):
        pass
    return None, "unknown"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Print the uncompressed size in bytes of a gzip/BGZF file, "
            "avoiding a full decompression pass where the file's structure "
            "makes that possible."
        )
    )
    parser.add_argument("path", help="File to measure")
    parser.add_argument(
        "--exact",
        action="store_true",
        help="Always inflate instead of using a structural fast path",
    )
    parser.add_argument(
        "--print-method",
        action="store_true",
        help="Print '<bytes> <method>' instead of just the byte count",
    )
    args = parser.parse_args(argv)

    try:
        size, method = uncompressed_size(Path(args.path), exact=args.exact)
    except (OSError, zlib.error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"{size} {method}" if args.print_method else size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
