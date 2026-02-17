import subprocess
import tempfile
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "src" / "update_rules.sh"


class UpdateRulesUnitTests(VerboseTestCase):
    def test_update_rules_errors_with_wrong_argument_count(self):
        """No argument: script exits non-zero and prints usage."""
        with tempfile.TemporaryDirectory() as td:
            result = subprocess.run(["bash", str(SCRIPT)], cwd=td, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Usage:", result.stderr)

    def test_update_rules_errors_when_rules_file_missing(self):
        """Missing default rules file: script exits non-zero with a clear error."""
        with tempfile.TemporaryDirectory() as td:
            result = subprocess.run(
                ["bash", str(SCRIPT), "/data/tsv/records.tsv"],
                cwd=td,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("rules/default_rules.ttl not found", result.stderr)

    def test_update_rules_updates_csvw_url_and_creates_backup(self):
        """Valid input updates records/header/metadata csvw:url paths and writes backup."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            rules_dir = tmp_path / "rules"
            rules_dir.mkdir()
            rules = rules_dir / "default_rules.ttl"
            rules.write_text(
                'ex:a csvw:url "/data/tsv/records.tsv";\n'
                'ex:b csvw:url "/data/tsv/header_lines.tsv";\n'
                'ex:c csvw:url "/data/tsv/file_metadata.tsv";\n'
            )

            result = subprocess.run(
                ["bash", str(SCRIPT), "/tmp/new_records.tsv", "/tmp/new_headers.tsv", "/tmp/new_metadata.tsv"],
                cwd=tmp_path,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            content = rules.read_text()
            self.assertIn('csvw:url "/tmp/new_records.tsv";', content)
            self.assertIn('csvw:url "/tmp/new_headers.tsv";', content)
            self.assertIn('csvw:url "/tmp/new_metadata.tsv";', content)
            self.assertTrue((rules_dir / "default_rules.ttl.bak").exists())

    def test_update_rules_escapes_ampersand_in_filename(self):
        """Ampersands in replacement paths are preserved literally."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            rules_dir = tmp_path / "rules"
            rules_dir.mkdir()
            rules = rules_dir / "default_rules.ttl"
            rules.write_text('ex:a csvw:url "/data/tsv/records.tsv";\n')

            result = subprocess.run(
                ["bash", str(SCRIPT), "/tmp/A&B.tsv"],
                cwd=tmp_path,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn('csvw:url "/tmp/A&B.tsv";', rules.read_text())


if __name__ == "__main__":
    unittest.main()
