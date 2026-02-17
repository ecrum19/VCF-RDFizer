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
        """Missing rules.ttl: script exits non-zero with a clear error."""
        with tempfile.TemporaryDirectory() as td:
            result = subprocess.run(
                ["bash", str(SCRIPT), "sample.tsv"],
                cwd=td,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("rules.ttl not found", result.stderr)

    def test_update_rules_updates_csvw_url_and_creates_backup(self):
        """Valid input updates csvw:url and writes rules.ttl.bak backup."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            rules = tmp_path / "rules.ttl"
            rules.write_text('ex:source csvw:url "old.tsv";\n')

            result = subprocess.run(
                ["bash", str(SCRIPT), "new.tsv"],
                cwd=tmp_path,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn('csvw:url "new.tsv";', rules.read_text())
            self.assertTrue((tmp_path / "rules.ttl.bak").exists())

    def test_update_rules_escapes_ampersand_in_filename(self):
        """Ampersands in filename are preserved literally in replacement text."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            rules = tmp_path / "rules.ttl"
            rules.write_text('ex:source csvw:url "old.tsv";\n')

            result = subprocess.run(
                ["bash", str(SCRIPT), "A&B.tsv"],
                cwd=tmp_path,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn('csvw:url "A&B.tsv";', rules.read_text())


if __name__ == "__main__":
    unittest.main()
