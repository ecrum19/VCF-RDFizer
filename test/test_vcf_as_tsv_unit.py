import gzip
import subprocess
import tempfile
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "src" / "vcf_as_tsv.sh"


class VcfAsTsvUnitTests(VerboseTestCase):
    def test_vcf_as_tsv_errors_with_wrong_argument_count(self):
        """Wrong argument count: script exits non-zero and prints usage."""
        result = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Usage:", result.stdout)

    def test_vcf_as_tsv_errors_when_input_path_missing(self):
        """Missing input path: script exits non-zero with path-not-found error."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            output_dir = tmp_path / "tsv"
            result = subprocess.run(
                ["bash", str(SCRIPT), str(tmp_path / "nope"), str(output_dir)],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("not found", result.stdout)

    def test_vcf_as_tsv_errors_for_unsupported_input_file_extension(self):
        """Unsupported single-file extension: script rejects non-VCF inputs."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_file = tmp_path / "sample.txt"
            input_file.write_text("x")
            output_dir = tmp_path / "tsv"
            result = subprocess.run(
                ["bash", str(SCRIPT), str(input_file), str(output_dir)],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must end with .vcf or .vcf.gz", result.stdout)

    def test_vcf_as_tsv_directory_mode(self):
        """Directory input: writes per-file TSV plus per-VCF records/header/metadata TSVs."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir = tmp_path / "in"
            input_dir.mkdir()
            output_dir = tmp_path / "tsv"
            (input_dir / "sample.vcf").write_text(
                "##fileformat=VCFv4.2\n##source=test\n#CHROM\tPOS\tID\n1\t10\trs1\n"
            )

            result = subprocess.run(
                ["bash", str(SCRIPT), str(input_dir), str(output_dir)],
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            out_file = output_dir / "sample.tsv"
            self.assertTrue(out_file.exists())
            lines = out_file.read_text().splitlines()
            self.assertEqual(lines[0], "CHROM\tPOS\tID")
            self.assertEqual(lines[1], "1\t10\trs1")
            records_all = output_dir / "sample.records.tsv"
            headers_all = output_dir / "sample.header_lines.tsv"
            metadata_all = output_dir / "sample.file_metadata.tsv"
            self.assertTrue(records_all.exists())
            self.assertTrue(headers_all.exists())
            self.assertTrue(metadata_all.exists())
            self.assertIn("sample.vcf", records_all.read_text())
            self.assertIn("fileformat", headers_all.read_text().lower())
            self.assertIn("sample.vcf", metadata_all.read_text())

    def test_vcf_as_tsv_single_gz_file_mode(self):
        """Single .vcf.gz input: decompresses and writes per-VCF split TSV outputs."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_file = tmp_path / "sample.vcf.gz"
            output_dir = tmp_path / "tsv"
            with gzip.open(input_file, "wt") as f:
                f.write("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t20\n")

            result = subprocess.run(
                ["bash", str(SCRIPT), str(input_file), str(output_dir)],
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            out_file = output_dir / "sample.tsv"
            self.assertTrue(out_file.exists())
            self.assertIn("CHROM\tPOS", out_file.read_text())
            self.assertTrue((output_dir / "sample.records.tsv").exists())
            self.assertTrue((output_dir / "sample.header_lines.tsv").exists())
            self.assertTrue((output_dir / "sample.file_metadata.tsv").exists())

    def test_vcf_as_tsv_errors_when_no_vcf_files_found(self):
        """Empty directory input: exits non-zero with a clear no-files message."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir = tmp_path / "empty"
            input_dir.mkdir()
            output_dir = tmp_path / "tsv"

            result = subprocess.run(
                ["bash", str(SCRIPT), str(input_dir), str(output_dir)],
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("No .vcf or .vcf.gz files found", result.stdout)

    def test_vcf_as_tsv_processes_multiple_files_in_sorted_order(self):
        """Directory with multiple files: outputs one TSV per VCF in sorted filename order."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir = tmp_path / "in"
            input_dir.mkdir()
            output_dir = tmp_path / "tsv"
            (input_dir / "b.vcf").write_text("##x\n#CHROM\tPOS\n1\t2\n")
            (input_dir / "a.vcf").write_text("##x\n#CHROM\tPOS\n1\t1\n")

            result = subprocess.run(
                ["bash", str(SCRIPT), str(input_dir), str(output_dir)],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("a.tsv", result.stdout)
            self.assertIn("b.tsv", result.stdout)
            self.assertTrue((output_dir / "a.tsv").exists())
            self.assertTrue((output_dir / "b.tsv").exists())
            a_records = (output_dir / "a.records.tsv").read_text().splitlines()
            b_records = (output_dir / "b.records.tsv").read_text().splitlines()
            self.assertEqual(len(a_records), 2)
            self.assertEqual(len(b_records), 2)
            self.assertIn("a.vcf", a_records[1])
            self.assertIn("b.vcf", b_records[1])


if __name__ == "__main__":
    unittest.main()
