import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import vcf_rdfizer
from test.helpers import VerboseTestCase


def invoke_main(argv):
    with mock.patch.object(sys, "argv", ["vcf_rdfizer.py", *argv]):
        return vcf_rdfizer.main()


def prepare_inputs(base: Path):
    input_dir = base / "input"
    input_dir.mkdir()
    (input_dir / "sample.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t10\n")
    rules_path = base / "rules.ttl"
    rules_path.write_text("@prefix ex: <http://example.org/> .\n")
    return input_dir, rules_path


class WrapperUnitTests(VerboseTestCase):
    def test_main_rejects_build_and_no_build_together(self):
        """Wrapper rejects mutually exclusive --build and --no-build options."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            rc = invoke_main(
                [
                    "--input",
                    str(input_dir),
                    "--rules",
                    str(rules_path),
                    "--build",
                    "--no-build",
                ]
            )
            self.assertEqual(rc, 2)

    def test_main_happy_path_runs_pipeline_and_passes_compression(self):
        """Wrapper runs all pipeline steps and forwards compression arguments."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            commands = []

            def fake_run(cmd, cwd=None, env=None):
                commands.append(cmd)
                return 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_build_image", return_value=1
                ), mock.patch.object(
                    vcf_rdfizer, "docker_pull_image", return_value=1
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--compression",
                            "none",
                            "--keep-tsv",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 3)
            self.assertIn("/opt/vcf-rdfizer/vcf_as_tsv.sh", commands[0])
            self.assertIn("/opt/vcf-rdfizer/run_conversion.sh", commands[1])
            self.assertIn("/opt/vcf-rdfizer/compression.sh", commands[2])
            self.assertEqual(commands[2][-2:], ["-m", "none"])

    def test_main_removes_tsv_when_wrapper_created_it(self):
        """Wrapper removes TSV directory when it created it and --keep-tsv is not set."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            tsv_dir = tmp_path / "tsv-out"

            def fake_run(cmd, cwd=None, env=None):
                return 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--tsv",
                            str(tsv_dir),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertFalse(tsv_dir.exists())

    def test_main_keeps_preexisting_tsv_directory(self):
        """Wrapper preserves preexisting TSV directory to avoid deleting user-managed files."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            tsv_dir = tmp_path / "tsv-out"
            tsv_dir.mkdir()
            sentinel = tsv_dir / "keep.me"
            sentinel.write_text("x")

            def fake_run(cmd, cwd=None, env=None):
                return 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--tsv",
                            str(tsv_dir),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertTrue(sentinel.exists())

    def test_main_fails_when_tsv_step_fails(self):
        """Wrapper stops when TSV conversion command returns non-zero."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            calls = {"n": 0}

            def fake_run(cmd, cwd=None, env=None):
                calls["n"] += 1
                return 1 if calls["n"] == 1 else 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ):
                    rc = invoke_main(["--input", str(input_dir), "--rules", str(rules_path)])
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 1)

    def test_main_fails_when_conversion_step_fails(self):
        """Wrapper stops when conversion command returns non-zero."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            calls = {"n": 0}

            def fake_run(cmd, cwd=None, env=None):
                calls["n"] += 1
                return 1 if calls["n"] == 2 else 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ):
                    rc = invoke_main(["--input", str(input_dir), "--rules", str(rules_path)])
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 1)

    def test_main_fails_when_compression_step_fails(self):
        """Wrapper stops when compression command returns non-zero."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            calls = {"n": 0}

            def fake_run(cmd, cwd=None, env=None):
                calls["n"] += 1
                return 1 if calls["n"] == 3 else 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ):
                    rc = invoke_main(["--input", str(input_dir), "--rules", str(rules_path)])
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 1)

    def test_main_errors_when_versioned_image_does_not_exist(self):
        """Wrapper returns a user error when a requested image version cannot be pulled."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "check_docker", return_value=True), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=False
                ), mock.patch.object(
                    vcf_rdfizer, "docker_pull_image", return_value=1
                ), mock.patch.object(
                    vcf_rdfizer, "run", return_value=0
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--image",
                            "example/vcf-rdfizer",
                            "--image-version",
                            "9.9.9",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 2)

    def test_main_no_build_fails_if_local_image_missing(self):
        """Wrapper fails fast with --no-build when no local image is available."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "check_docker", return_value=True), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=False
                ), mock.patch.object(
                    vcf_rdfizer, "run", return_value=0
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--no-build",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 2)

    def test_main_errors_when_image_has_tag_and_image_version_is_set(self):
        """Wrapper rejects conflicting tag sources across --image and --image-version."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "check_docker", return_value=True):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--image",
                            "repo/image:latest",
                            "--image-version",
                            "1.2.3",
                        ]
                    )
            finally:
                os.chdir(old_cwd)
            self.assertEqual(rc, 2)

    def test_main_errors_when_rules_path_missing(self):
        """Wrapper fails when mapping rules path does not exist."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, _ = prepare_inputs(tmp_path)
            missing_rules = tmp_path / "missing.ttl"
            rc = invoke_main(["--input", str(input_dir), "--rules", str(missing_rules)])
            self.assertEqual(rc, 2)

    def test_main_errors_when_output_path_is_file(self):
        """Wrapper fails validation when --out points to a file instead of a directory."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_file = tmp_path / "out-file"
            out_file.write_text("x")
            rc = invoke_main(
                [
                    "--input",
                    str(input_dir),
                    "--rules",
                    str(rules_path),
                    "--out",
                    str(out_file),
                ]
            )
            self.assertEqual(rc, 2)

    def test_check_docker_false_when_binary_missing(self):
        """Docker check returns false if docker is absent from PATH."""
        with mock.patch("vcf_rdfizer.shutil.which", return_value=None):
            self.assertFalse(vcf_rdfizer.check_docker())

    def test_check_docker_false_when_daemon_unavailable(self):
        """Docker check returns false if docker version command fails."""
        with mock.patch("vcf_rdfizer.shutil.which", return_value="/usr/bin/docker"), mock.patch.object(
            vcf_rdfizer, "run", return_value=1
        ):
            self.assertFalse(vcf_rdfizer.check_docker())

    def test_resolve_image_ref_accepts_repo_plus_version(self):
        """Image repository and explicit version resolve to a tagged image reference."""
        ref, requested = vcf_rdfizer.resolve_image_ref("vcf-rdfizer", "1.2.3")
        self.assertEqual(ref, "vcf-rdfizer:1.2.3")
        self.assertTrue(requested)


if __name__ == "__main__":
    unittest.main()
