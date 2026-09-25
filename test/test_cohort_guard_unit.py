"""Refusing an expanded run that cannot fit, before it starts filling the disk.

The expanded representation emits per sample per record, so its cost is
records x samples and is invisible in the input file size:
``1000G_phase3_chr20.vcf.gz`` is 327 MB gzipped and, at 1,812,841 records x
2,504 samples, needs roughly 2 TB. The benchmark harness guarded that case in
its own shell; the tool did not, so a user running the same file directly got
no warning and filled the volume.

The coefficients are fitted, not guessed -- see the constants -- and the guard
is deliberately one-sided: it only ever refuses ``expanded``, because condensed
grew 0.9% in total across the same 1-to-2504 sample ladder.
"""

import csv
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

import vcf_rdfizer
from test.helpers import VerboseTestCase
from test.test_vcf_rdfizer_unit import invoke_main, latest_metrics_run_dir


def write_records_tsv(path: Path, *, sample_ids: list[str], records: int) -> Path:
    """A records TSV with the column layout SampleRecordStream expects."""
    header = [
        "SOURCE_FILE", "ROW_ID", "CHROM", "POS", "ID", "REF", "ALT",
        "QUAL", "FILTER", "INFO", "FORMAT", " ".join(sample_ids) or "SAMPLES",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(header)
        for index in range(records):
            writer.writerow([
                "in.vcf", f"r{index}", "20", str(100 + index), ".", "A", "G",
                "50", "PASS", "DP=30", "GT:DP",
                " ".join("0/1:30" for _ in sample_ids),
            ])
    return path


class EstimateTests(VerboseTestCase):
    def test_the_estimate_is_the_product_of_records_samples_and_the_fits(self):
        self.assertEqual(
            vcf_rdfizer.estimate_expanded_workspace_bytes(10_000, 2_504),
            10_000
            * 2_504
            * vcf_rdfizer.EXPANDED_TRIPLES_PER_SAMPLE_CALL
            * vcf_rdfizer.EXPANDED_PEAK_BYTES_PER_TRIPLE,
        )

    def test_the_fitted_coefficient_reproduces_the_measured_ladder_rung(self):
        """s2504 x 10,000 records emitted 627,372,018 triples; the fit must be close."""
        measured_expanded_minus_condensed = 627_372_018 - 1_452_018
        predicted = (
            10_000 * 2_504 * vcf_rdfizer.EXPANDED_TRIPLES_PER_SAMPLE_CALL
        )
        relative_error = abs(predicted - measured_expanded_minus_condensed) / (
            measured_expanded_minus_condensed
        )
        self.assertLess(relative_error, 0.01, f"predicted {predicted:,}")

    def test_a_degenerate_shape_estimates_zero_rather_than_raising(self):
        self.assertEqual(vcf_rdfizer.estimate_expanded_workspace_bytes(0, 2_504), 0)
        self.assertEqual(vcf_rdfizer.estimate_expanded_workspace_bytes(-5, 2_504), 0)


class RecordsTsvReadingTests(VerboseTestCase):
    def test_sample_columns_are_read_from_the_tsv_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write_records_tsv(
                Path(tmp) / "r.tsv", sample_ids=["NA1", "NA2", "NA3"], records=2
            )
            self.assertEqual(vcf_rdfizer.read_records_tsv_sample_count(path), 3)

    def test_a_sample_free_tsv_reports_zero_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write_records_tsv(Path(tmp) / "r.tsv", sample_ids=[], records=2)
            self.assertEqual(vcf_rdfizer.read_records_tsv_sample_count(path), 0)

    def test_rows_are_counted_without_parsing_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write_records_tsv(
                Path(tmp) / "r.tsv", sample_ids=["NA1"], records=37
            )
            self.assertEqual(vcf_rdfizer.count_records_tsv_rows(path), 37)

    def test_an_unreadable_tsv_reports_zero_samples_instead_of_raising(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "missing.tsv"
            self.assertEqual(vcf_rdfizer.read_records_tsv_sample_count(path), 0)

    def test_a_non_utf8_tsv_raises_an_input_error_not_a_decode_error(self):
        """The guard's reader must not leak UnicodeDecodeError past the per-input handler."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "r.tsv"
            path.write_bytes(APPLEDOUBLE_BYTES)
            with self.assertRaises(vcf_rdfizer.InputEncodingError) as caught:
                vcf_rdfizer.read_records_tsv_sample_count(path)
            self.assertIn("not a UTF-8 text VCF", str(caught.exception))
            with self.assertRaises(vcf_rdfizer.InputEncodingError):
                vcf_rdfizer.check_records_tsv_is_utf8(path)

    def test_the_encoding_probe_accepts_utf8_cut_at_the_probe_boundary(self):
        """A multi-byte character split by the bounded read is not a bad byte."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "r.tsv"
            limit = vcf_rdfizer.RECORDS_TSV_ENCODING_PROBE_BYTES
            path.write_bytes(b"a" * (limit - 1) + "\u00e9".encode("utf-8"))
            vcf_rdfizer.check_records_tsv_is_utf8(path)


class GuardDecisionTests(VerboseTestCase):
    def _refusal(self, tmp, *, sample_ids, records, free_bytes, allow=False,
                 representation="expanded"):
        records_tsv = write_records_tsv(
            Path(tmp) / "r.tsv", sample_ids=sample_ids, records=records
        )
        usage = shutil.disk_usage(tmp)
        fake = type(usage)(usage.total, usage.total - free_bytes, free_bytes)
        with mock.patch.object(vcf_rdfizer.shutil, "disk_usage", return_value=fake):
            return vcf_rdfizer.cohort_scale_refusal(
                records_tsv=records_tsv,
                sample_representation=representation,
                out_dir=Path(tmp),
                allow=allow,
            )

    def test_a_cohort_expanded_run_that_cannot_fit_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            refusal = self._refusal(
                tmp,
                sample_ids=[f"NA{i}" for i in range(200)],
                records=50,
                free_bytes=1_000_000,
            )
            self.assertIsNotNone(refusal)
            self.assertIn("expanded", refusal)

    def test_the_refusal_names_the_condensed_alternative_and_the_override(self):
        """A refusal that does not say what to do instead is just a failure."""
        with tempfile.TemporaryDirectory() as tmp:
            refusal = self._refusal(
                tmp,
                sample_ids=[f"NA{i}" for i in range(200)],
                records=50,
                free_bytes=1_000_000,
            )
            self.assertIn("--sample-representation condensed", refusal)
            self.assertIn("--allow-cohort-expansion", refusal)
            self.assertIn("records x samples", refusal)

    def test_condensed_is_never_refused(self):
        """Condensed moved 0.9% over a 2504-fold change in samples."""
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(
                self._refusal(
                    tmp,
                    sample_ids=[f"NA{i}" for i in range(200)],
                    records=50,
                    free_bytes=1,
                    representation="condensed",
                )
            )

    def test_a_single_sample_file_is_never_refused(self):
        """Single-sample expanded is the ordinary case and must stay unguarded."""
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(
                self._refusal(tmp, sample_ids=["NA1"], records=100_000, free_bytes=1)
            )

    def test_a_run_that_fits_proceeds(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(
                self._refusal(
                    tmp,
                    sample_ids=[f"NA{i}" for i in range(4)],
                    records=10,
                    free_bytes=10 * 1024 ** 3,
                )
            )

    def test_the_override_lets_a_refused_run_proceed(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(
                self._refusal(
                    tmp,
                    sample_ids=[f"NA{i}" for i in range(200)],
                    records=50,
                    free_bytes=1_000_000,
                    allow=True,
                )
            )

    def test_the_guard_leaves_a_margin_rather_than_filling_the_volume(self):
        """Needing exactly the free space is refused; the share is below 1.0."""
        self.assertLess(vcf_rdfizer.COHORT_GUARD_FREE_SPACE_SHARE, 1.0)
        with tempfile.TemporaryDirectory() as tmp:
            samples, records = 200, 50
            needed = vcf_rdfizer.estimate_expanded_workspace_bytes(records, samples)
            self.assertIsNotNone(
                self._refusal(
                    tmp,
                    sample_ids=[f"NA{i}" for i in range(samples)],
                    records=records,
                    free_bytes=needed,
                )
            )


#: The head of a real macOS AppleDouble sidecar: magic, version, then binary
#: entries. 0xa3 is the byte the vcf-bench-1 run died on.
APPLEDOUBLE_BYTES = (
    b"\x00\x05\x16\x07\x00\x02\x00\x00Mac OS X        \x00\x02"
    + b"\x00" * 110
    + b"\xa3\x9f\xff\xfe" * 16
)


class NonUtf8InputTests(VerboseTestCase):
    """A binary input fails on its own; it does not take the directory down.

    vcf-bench-1, v3.1.0: a directory carrying macOS ``._P001.vcf`` sidecars
    crashed ``--sample-representation expanded`` with a UnicodeDecodeError out
    of the cohort guard, while condensed reported the same file as a failed
    input and converted the rest.
    """

    def _run_full(self, tmp_path: Path, vcf_names: dict[str, bytes], representation: str):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        tsv_dir = tmp_path / "tsv_fixture"
        tsv_dir.mkdir()
        triplets = []
        for name, payload in vcf_names.items():
            (input_dir / name).write_bytes(payload)
            prefix = vcf_rdfizer.vcf_output_prefix(Path(name))
            records = tsv_dir / f"{prefix}.records.tsv"
            if payload.startswith(b"##fileformat"):
                write_records_tsv(records, sample_ids=["NA1", "NA2"], records=3)
            else:
                # vcf_as_tsv.sh passes bytes through, so the binary survives
                # into the records TSV that the Python side then decodes.
                records.write_bytes(payload)
            (tsv_dir / f"{prefix}.header_lines.tsv").write_text(
                f"SOURCE_FILE\tLINE\n{name}\t##fileformat=VCFv4.2\n", encoding="utf-8"
            )
            (tsv_dir / f"{prefix}.file_metadata.tsv").write_text(
                "SOURCE_FILE\tKEY\tVALUE\n", encoding="utf-8"
            )
            triplets.append(
                {
                    "prefix": prefix,
                    "records": records,
                    "headers": tsv_dir / f"{prefix}.header_lines.tsv",
                    "metadata": tsv_dir / f"{prefix}.file_metadata.tsv",
                }
            )
        out_dir = tmp_path / "out"
        stdout, stderr = StringIO(), StringIO()
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with (
                mock.patch.object(vcf_rdfizer, "run", return_value=0),
                mock.patch.object(vcf_rdfizer, "check_docker", return_value=True),
                mock.patch.object(vcf_rdfizer, "docker_image_exists", return_value=True),
                mock.patch.object(vcf_rdfizer, "discover_tsv_triplets", return_value=triplets),
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                rc = invoke_main(
                    [
                        "--input", str(input_dir),
                        "--sample-representation", representation,
                        "--rdf-storage-mode", "plain",
                        "--compression", "none",
                        "--out", str(out_dir),
                    ]
                )
        finally:
            os.chdir(old_cwd)
        return rc, out_dir, stdout.getvalue(), stderr.getvalue()

    def test_a_directory_with_an_appledouble_sidecar_converts_the_vcf_and_reports_the_sidecar(self):
        with tempfile.TemporaryDirectory() as td:
            rc, out_dir, stdout, stderr = self._run_full(
                Path(td),
                {"P001.vcf": VALID_VCF, "._P001.vcf": APPLEDOUBLE_BYTES},
                "expanded",
            )
            self.assertNotIn("Traceback", stderr)
            self.assertNotIn("UnicodeDecodeError", stderr)
            self.assertEqual(rc, 0, stderr)
            self.assertIn("Skipping 1 macOS AppleDouble file(s)", stdout)
            self.assertIn("._P001.vcf", stdout)
            self.assertIn("Input 1/1: P001.vcf", stdout)
            self.assertTrue((out_dir / "P001" / "P001.nt").exists())

    def test_a_binary_vcf_fails_as_one_input_and_the_others_still_convert(self):
        """Not every binary is an AppleDouble sidecar; the encoding probe catches the rest."""
        for representation in ("expanded", "condensed"):
            with self.subTest(representation=representation), tempfile.TemporaryDirectory() as td:
                rc, out_dir, stdout, stderr = self._run_full(
                    Path(td),
                    {"P001.vcf": VALID_VCF, "x.vcf": APPLEDOUBLE_BYTES},
                    representation,
                )
                self.assertNotIn("Traceback", stderr)
                self.assertEqual(rc, 1, stderr)
                self.assertIn("(x.vcf) failed at input-encoding: not a UTF-8 text VCF", stderr)
                self.assertTrue((out_dir / "P001" / "P001.nt").exists())
                report = (
                    latest_metrics_run_dir(out_dir / "run_metrics")
                    / "reports" / "failed_inputs.csv"
                )
                with report.open(newline="", encoding="utf-8") as handle:
                    failures = list(csv.DictReader(handle))
                self.assertEqual(
                    [(f["expected_prefix"], f["stage"]) for f in failures],
                    [("x", "input-encoding")],
                )


VALID_VCF = b"##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t10\n"


class CliTests(VerboseTestCase):
    def test_the_override_flag_is_accepted_by_the_cli(self):
        """It must parse; without it the escape hatch is unreachable."""
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            with mock.patch.object(
                sys, "argv", ["vcf_rdfizer.py", "--allow-cohort-expansion", "--help"]
            ), self.assertRaises(SystemExit) as exc:
                vcf_rdfizer.main()
        self.assertEqual(exc.exception.code, 0)

    def test_the_help_text_explains_why_the_guard_exists(self):
        """A flag whose help says only what it toggles teaches nothing."""
        buffer = StringIO()
        with redirect_stdout(buffer), redirect_stderr(StringIO()):
            with mock.patch.object(
                sys, "argv", ["vcf_rdfizer.py", "--help"]
            ), self.assertRaises(SystemExit):
                vcf_rdfizer.main()
        help_text = buffer.getvalue()
        self.assertIn("--allow-cohort-expansion", help_text)
        self.assertIn("records x samples", help_text)


if __name__ == "__main__":
    unittest.main()
