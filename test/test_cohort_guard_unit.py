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
