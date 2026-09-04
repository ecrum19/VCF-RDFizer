import gzip
import struct
import tempfile
import unittest
import zlib
from pathlib import Path

import vcf_rdfizer
import vcf_rdfizer_gzip
from test.helpers import VerboseTestCase

VCF_LINE = "20\t%d\trs%d\tA\tG\t50\tPASS\tAC=1;AN=2;DP=%d\tGT:AD:DP:GQ:PL\t0|1:35,0:35:99:0,99,1013\n"


def vcf_body(count: int, start: int = 0) -> bytes:
    return "".join(
        VCF_LINE % (i, i, i % 90) for i in range(start, start + count)
    ).encode("utf-8")


def write_bgzf(path: Path, payload: bytes, block_size: int = 65280) -> None:
    """Write a real BGZF (bgzip) file: gzip members carrying a BC extra subfield."""
    with path.open("wb") as out:
        for offset in range(0, len(payload), block_size):
            chunk = payload[offset : offset + block_size]
            compressor = zlib.compressobj(6, zlib.DEFLATED, -zlib.MAX_WBITS)
            body = compressor.compress(chunk) + compressor.flush()
            total = len(body) + 26
            header = (
                bytes([31, 139, 8, 4, 0, 0, 0, 0, 0, 255])
                + struct.pack("<H", 6)
                + b"BC"
                + struct.pack("<HH", 2, total - 1)
            )
            out.write(header)
            out.write(body)
            out.write(struct.pack("<II", zlib.crc32(chunk) & 0xFFFFFFFF, len(chunk)))
        # bgzip's canonical empty end-of-file marker block.
        out.write(
            bytes.fromhex(
                "1f8b08040000000000ff0600424302001b0003000000000000000000"
            )
        )


def inflate_truth(path: Path) -> int:
    total = 0
    with gzip.open(path, "rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                return total
            total += len(chunk)


class GzipSizeUnitTests(VerboseTestCase):
    def assert_matches_truth(self, path: Path, expected_method: str | None = None):
        size, method = vcf_rdfizer_gzip.uncompressed_size(path)
        self.assertEqual(size, inflate_truth(path), f"wrong size via {method}")
        if expected_method is not None:
            self.assertEqual(method, expected_method)
        return size, method

    def test_bgzf_size_is_exact_without_inflating(self):
        """A bgzip file is measured from its block headers alone."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "cohort.vcf.gz"
            payload = vcf_body(40_000)
            write_bgzf(path, payload)
            size, _ = self.assert_matches_truth(path, "bgzf")
            self.assertEqual(size, len(payload))

    def test_bgzf_single_block_and_eof_marker_only(self):
        """A one-block file and a bare EOF marker both measure correctly."""
        with tempfile.TemporaryDirectory() as td:
            small = Path(td) / "small.vcf.gz"
            write_bgzf(small, b"tiny\n")
            self.assert_matches_truth(small, "bgzf")

            empty = Path(td) / "empty.vcf.gz"
            write_bgzf(empty, b"")
            size, method = vcf_rdfizer_gzip.uncompressed_size(empty)
            self.assertEqual((size, method), (0, "bgzf"))

    def test_single_member_gzip_within_the_sample_budget_is_measured(self):
        """A small plain gzip is measured outright rather than extrapolated."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "small.vcf.gz"
            path.write_bytes(gzip.compress(vcf_body(2_000)))
            self.assert_matches_truth(path, "gzip-sample")

    def test_large_single_member_gzip_resolves_via_the_trailer(self):
        """Past the sample budget the ISIZE trailer is resolved by sampled ratio."""
        with tempfile.TemporaryDirectory() as td:
            # Comfortably past the sample budget so the member cannot finish
            # inside it, which is what forces the trailer-resolution path.
            payload = vcf_body(400_000)
            self.assertGreater(
                len(payload), 2 * vcf_rdfizer_gzip.RATIO_SAMPLE_BYTES
            )
            for level in (1, 6, 9):
                path = Path(td) / f"big-{level}.vcf.gz"
                path.write_bytes(gzip.compress(payload, level))
                self.assert_matches_truth(path, "gzip-trailer")

    def test_trailer_resolution_recovers_a_size_past_the_32_bit_wrap(self):
        """ISIZE is modulo 2**32; the resolved size adds back the missing wraps."""
        compressed_size = 634_493_967
        true_size = 4_600_694_700
        wrapped = true_size % vcf_rdfizer_gzip.ISIZE_MODULUS
        self.assertNotEqual(wrapped, true_size)

        # Reproduce the resolution step against a file whose true size cannot be
        # expressed by the trailer, without materialising 4.3 GiB on disk.
        ratio = true_size / compressed_size
        projected = ratio * compressed_size
        upper_bound = compressed_size * vcf_rdfizer_gzip.MAX_DEFLATE_RATIO
        accepted = [
            candidate
            for candidate in range(
                wrapped,
                int(upper_bound) + vcf_rdfizer_gzip.ISIZE_MODULUS,
                vcf_rdfizer_gzip.ISIZE_MODULUS,
            )
            if candidate <= upper_bound
            and abs(candidate - projected)
            <= vcf_rdfizer_gzip.RATIO_TOLERANCE * projected
        ]
        self.assertEqual(accepted, [true_size])

    def test_concatenated_members_fall_back_instead_of_trusting_the_trailer(self):
        """A trailer covering only the last member must never be used as-is."""
        with tempfile.TemporaryDirectory() as td:
            first = vcf_body(120_000)
            second = vcf_body(120_000, start=120_000)

            equal = Path(td) / "equal.vcf.gz"
            equal.write_bytes(gzip.compress(first) + gzip.compress(second))
            self.assert_matches_truth(equal, "inflate")

            lopsided = Path(td) / "lopsided.vcf.gz"
            lopsided.write_bytes(gzip.compress(first) + gzip.compress(b"tail\n"))
            self.assert_matches_truth(lopsided, "inflate")

            many = Path(td) / "many.vcf.gz"
            many.write_bytes(
                b"".join(
                    gzip.compress(first[i : i + 50_000])
                    for i in range(0, len(first), 50_000)
                )
            )
            self.assert_matches_truth(many, "inflate")

    def test_optional_gzip_header_fields_are_skipped(self):
        """FEXTRA, FNAME, and FCOMMENT headers do not confuse the sampler."""
        with tempfile.TemporaryDirectory() as td:
            payload = vcf_body(2_000)
            deflate_and_trailer = gzip.compress(payload)[10:]
            extra = struct.pack("<2sH", b"ZZ", 3) + b"abc"
            header = (
                bytes([31, 139, 8, 0x04 | 0x08 | 0x10, 0, 0, 0, 0, 0, 255])
                + struct.pack("<H", len(extra))
                + extra
                + b"name.vcf\x00"
                + b"a comment\x00"
            )
            path = Path(td) / "flagged.vcf.gz"
            path.write_bytes(header + deflate_and_trailer)
            self.assert_matches_truth(path)

    def test_plain_file_uses_its_on_disk_size(self):
        """An uncompressed VCF needs no gzip handling at all."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "plain.vcf"
            payload = vcf_body(500)
            path.write_bytes(payload)
            self.assertEqual(
                vcf_rdfizer_gzip.uncompressed_size(path), (len(payload), "stat")
            )

    def test_exact_flag_forces_a_full_inflate(self):
        """--exact bypasses the structural fast paths but agrees with them."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "cohort.vcf.gz"
            write_bgzf(path, vcf_body(20_000))
            fast, fast_method = vcf_rdfizer_gzip.uncompressed_size(path)
            exact, exact_method = vcf_rdfizer_gzip.uncompressed_size(path, exact=True)
            self.assertEqual(fast, exact)
            self.assertEqual(fast_method, "bgzf")
            self.assertEqual(exact_method, "inflate")

    def test_truncated_stream_is_reported_rather_than_measured_short(self):
        """A damaged input raises instead of silently reporting fewer bytes."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "truncated.vcf.gz"
            blob = gzip.compress(vcf_body(2_000))
            path.write_bytes(blob[: len(blob) // 2])
            with self.assertRaises(zlib.error):
                vcf_rdfizer_gzip.uncompressed_size(path, exact=True)

    def test_structural_accessor_declines_instead_of_inflating(self):
        """The preflight accessor returns None rather than paying for a full pass."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "concat.vcf.gz"
            body = vcf_body(120_000)
            path.write_bytes(gzip.compress(body) + gzip.compress(body))
            size, method = vcf_rdfizer_gzip.uncompressed_size_without_inflating(path)
            self.assertIsNone(size)
            self.assertEqual(method, "unknown")

    def test_preflight_estimate_uses_measured_sizes_when_available(self):
        """--estimate-size reads real uncompressed sizes for structured inputs."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            payload = vcf_body(20_000)
            bgzf_path = tmp_path / "cohort.vcf.gz"
            write_bgzf(bgzf_path, payload)

            estimate = vcf_rdfizer.estimate_pipeline_sizes([bgzf_path], tmp_path / "out")
            self.assertFalse(estimate["uncompressed_input_estimated"])
            self.assertEqual(estimate["input_size_methods"], ["bgzf"])
            self.assertEqual(
                estimate["tsv_bytes"],
                int(len(payload) * vcf_rdfizer.TSV_OVERHEAD_FACTOR),
            )

    def test_preflight_estimate_falls_back_to_the_expansion_factor(self):
        """An input whose structure cannot answer keeps the coarse assumption."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            body = vcf_body(120_000)
            path = tmp_path / "concat.vcf.gz"
            path.write_bytes(gzip.compress(body) + gzip.compress(body))

            estimate = vcf_rdfizer.estimate_pipeline_sizes([path], tmp_path / "out")
            self.assertTrue(estimate["uncompressed_input_estimated"])
            self.assertEqual(estimate["input_size_methods"], ["estimated"])


if __name__ == "__main__":
    unittest.main()
