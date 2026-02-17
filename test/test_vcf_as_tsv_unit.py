import gzip
import subprocess
import tempfile
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "src" / "vcf_as_tsv.sh"


class VcfAsTsvUnitTests(VerboseTestCase):
    def test_vcf_as_tsv_directory_mode(self):
        """Directory input: converts VCF and normalizes #CHROM header to CHROM."""
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

    def test_vcf_as_tsv_single_gz_file_mode(self):
        """Single .vcf.gz input: decompresses and writes expected TSV output."""
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


if __name__ == "__main__":
    unittest.main()
