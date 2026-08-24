"""Unit tests for the release metadata helper."""

from __future__ import annotations

import importlib.util
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
RELEASE_SCRIPT = REPO_ROOT / "scripts" / "release.py"


def load_release_module():
    spec = importlib.util.spec_from_file_location("release_script", RELEASE_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load release script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReleaseScriptTests(unittest.TestCase):
    def setUp(self):
        self.release = load_release_module()

    def test_current_metadata_matches_current_tag(self):
        pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        version_match = re.search(r'^version = "(\d+\.\d+\.\d+)"$', pyproject, re.MULTILINE)
        self.assertIsNotNone(version_match)
        version = version_match.group(1)
        self.assertEqual(self.release.release_metadata_errors(version), [])
        self.assertEqual(self.release.main(["--check-tag", f"v{version}"]), 0)

    def test_version_and_tag_validation_reject_invalid_values(self):
        with self.assertRaises(ValueError):
            self.release.validate_version("1.2")
        with self.assertRaises(ValueError):
            self.release.version_from_tag("1.2.3")
        with self.assertRaises(ValueError):
            self.release.validate_sha256("not-a-sha")

    def test_update_resets_stale_conda_checksum_until_tag_checksum_is_known(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "conda-recipe").mkdir()
            (root / "pyproject.toml").write_text('version = "1.2.3"\n', encoding="utf-8")
            (root / "CITATION.cff").write_text('version: "1.2.3"\n', encoding="utf-8")
            (root / "README.md").write_text(
                "Version 1.2.3\nversion = {1.2.3}\n", encoding="utf-8"
            )
            (root / "conda-recipe" / "README.md").write_text(
                "v1.2.3\nv1.2.3\n", encoding="utf-8"
            )
            (root / "conda-recipe" / "meta.yaml").write_text(
                '{% set version = "1.2.3" %}\nsource:\n  sha256: old-checksum\n',
                encoding="utf-8",
            )

            with mock.patch.object(self.release, "ROOT", root):
                self.release.update_release_metadata("1.2.4", None)
                meta_text = (root / "conda-recipe" / "meta.yaml").read_text(encoding="utf-8")
                self.assertIn('{% set version = "1.2.4" %}', meta_text)
                self.assertIn(self.release.PLACEHOLDER_CONDA_SHA256, meta_text)
                self.assertEqual(self.release.release_metadata_errors("1.2.4"), [])
