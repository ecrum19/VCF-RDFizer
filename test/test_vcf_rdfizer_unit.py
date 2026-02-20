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


def mocked_triplets():
    return [
        {
            "prefix": "sample",
            "records": Path("sample.records.tsv"),
            "headers": Path("sample.header_lines.tsv"),
            "metadata": Path("sample.file_metadata.tsv"),
        }
    ]


class WrapperUnitTests(VerboseTestCase):
    def test_help_flag_prints_usage_guide(self):
        """Help flag exits cleanly and prints mode usage examples."""
        out_buf = StringIO()
        with mock.patch.object(sys, "argv", ["vcf_rdfizer.py", "--help"]), redirect_stdout(out_buf):
            with self.assertRaises(SystemExit) as exc:
                vcf_rdfizer.main()

        self.assertEqual(exc.exception.code, 0)
        text = out_buf.getvalue()
        self.assertIn("Examples:", text)
        self.assertIn("-m {full,compress,decompress}", text)
        self.assertIn("-i INPUT", text)

    def test_estimate_pipeline_sizes_handles_plain_and_gz_inputs(self):
        """Size estimation scales gzipped inputs and reports free disk bytes."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            plain = tmp_path / "plain.vcf"
            gz = tmp_path / "compressed.vcf.gz"
            plain.write_bytes(b"a" * 100)
            gz.write_bytes(b"b" * 50)

            with mock.patch("vcf_rdfizer.shutil.disk_usage") as disk_usage:
                disk_usage.return_value = shutil._ntuple_diskusage(1_000_000, 100_000, 42_000)
                estimate = vcf_rdfizer.estimate_pipeline_sizes([plain, gz], tmp_path / "out")

            self.assertEqual(estimate["input_bytes"], 150)
            self.assertEqual(estimate["tsv_bytes"], 385)
            self.assertEqual(estimate["rdf_low_bytes"], 1400)
            self.assertEqual(estimate["rdf_high_bytes"], 4200)
            self.assertEqual(estimate["free_disk_bytes"], 42_000)

    def test_main_estimate_size_prints_summary_and_warning(self):
        """Estimate mode prints preflight ranges and warns when free disk is too low."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            commands = []

            def fake_run(cmd, cwd=None, env=None):
                commands.append(cmd)
                return 0

            fake_disk = shutil._ntuple_diskusage(10_000_000, 9_900_000, 64)
            out_buf = StringIO()
            err_buf = StringIO()

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ), mock.patch(
                    "vcf_rdfizer.shutil.disk_usage", return_value=fake_disk
                ), redirect_stdout(out_buf), redirect_stderr(err_buf):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--estimate-size",
                            "--keep-tsv",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertIn("Preflight size estimate (rough):", out_buf.getvalue())
            self.assertIn("Estimated RDF N-Triples output:", out_buf.getvalue())
            self.assertIn("Warning: Estimated upper-bound RDF size exceeds currently free disk.", err_buf.getvalue())
            self.assertEqual(len(commands), 3)

    def test_main_short_flags_work_for_full_mode(self):
        """Short aliases run full mode successfully."""
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
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ):
                    rc = invoke_main(
                        [
                            "-m",
                            "full",
                            "-i",
                            str(input_dir),
                            "-r",
                            str(rules_path),
                            "-c",
                            "none",
                            "-k",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 3)

    def test_main_compress_mode_runs_selected_methods(self):
        """Compression mode runs only requested methods for a designated .nq input."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            nq_path = tmp_path / "sample.nq"
            nq_path.write_text("<s> <p> <o> <g> .\n")
            out_dir = tmp_path / "out"
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
                ):
                    rc = invoke_main(
                        [
                            "--mode",
                            "compress",
                            "--nq",
                            str(nq_path),
                            "--compression",
                            "gzip,brotli",
                            "--out",
                            str(out_dir),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 2)
            self.assertIn("gzip -c", commands[0][-1])
            self.assertIn("/data/out/gzip/", commands[0][-1])
            self.assertIn("brotli -q 7 -c", commands[1][-1])
            self.assertIn("/data/out/brotli/", commands[1][-1])

    def test_main_compress_mode_accepts_nt_and_preserves_extension(self):
        """Compression mode accepts .nt input and emits extension-aware output names."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            nt_path = tmp_path / "sample.nt"
            nt_path.write_text("<s> <p> <o> .\n")
            out_dir = tmp_path / "out"
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
                ):
                    rc = invoke_main(
                        [
                            "--mode",
                            "compress",
                            "--nq",
                            str(nt_path),
                            "--compression",
                            "gzip",
                            "--out",
                            str(out_dir),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 1)
            self.assertIn("/data/out/gzip/sample.nt.gz", commands[0][-1])

    def test_main_compress_mode_requires_nq_argument(self):
        """Compression mode fails validation when --nq is missing."""
        rc = invoke_main(["--mode", "compress"])
        self.assertEqual(rc, 2)

    def test_main_compress_mode_rejects_non_rdf_input(self):
        """Compression mode rejects non-RDF input files."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            bad_input = tmp_path / "sample.txt"
            bad_input.write_text("x")
            rc = invoke_main(["--mode", "compress", "--nq", str(bad_input)])
            self.assertEqual(rc, 2)

    def test_main_full_mode_requires_input_argument(self):
        """Full mode fails validation when --input is not provided."""
        rc = invoke_main(["--mode", "full"])
        self.assertEqual(rc, 2)

    def test_main_compress_mode_none_skips_compression_commands(self):
        """Compression mode with method none performs no compression runs."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            nq_path = tmp_path / "sample.nq"
            nq_path.write_text("<s> <p> <o> <g> .\n")
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
                ):
                    rc = invoke_main(
                        [
                            "--mode",
                            "compress",
                            "--nq",
                            str(nq_path),
                            "--compression",
                            "none",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 0)

    def test_main_decompress_mode_gzip_uses_default_output_name(self):
        """Decompression mode inflates .gz RDF into default decompressed output path."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            compressed = tmp_path / "sample.nq.gz"
            compressed.write_bytes(b"fake-gzip-bytes")
            out_dir = tmp_path / "out"
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
                ):
                    rc = invoke_main(
                        [
                            "--mode",
                            "decompress",
                            "--compressed-input",
                            str(compressed),
                            "--out",
                            str(out_dir),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 1)
            self.assertIn("gzip -dc", commands[0][-1])
            self.assertIn("/data/out/sample.nq", commands[0][-1])

    def test_main_decompress_mode_hdt_uses_hdt2rdf(self):
        """Decompression mode maps .hdt input through hdt2rdf conversion."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            compressed = tmp_path / "sample.hdt"
            compressed.write_bytes(b"fake-hdt")
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
                ):
                    rc = invoke_main(
                        [
                            "--mode",
                            "decompress",
                            "--compressed-input",
                            str(compressed),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 1)
            self.assertIn("/opt/hdt-java/hdt-java-cli/bin/hdt2rdf.sh", commands[0][-1])
            self.assertIn("/data/out/sample.nt", commands[0][-1])

    def test_main_decompress_mode_rejects_unknown_extension(self):
        """Decompression mode rejects unsupported compressed RDF extensions."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            bad = tmp_path / "sample.zip"
            bad.write_bytes(b"x")
            rc = invoke_main(
                [
                    "--mode",
                    "decompress",
                    "--compressed-input",
                    str(bad),
                ]
            )
            self.assertEqual(rc, 2)

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
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
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

    def test_main_multiple_triplets_run_multiple_conversions_and_compress_all_outputs(self):
        """Multiple input triplets trigger per-sample conversion runs and all-output compression."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir = tmp_path / "input"
            input_dir.mkdir()
            (input_dir / "sample_a.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t10\n")
            (input_dir / "sample_b.vcf").write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t20\n")
            rules_path = tmp_path / "rules.ttl"
            rules_path.write_text("@prefix ex: <http://example.org/> .\n")
            commands = []

            def fake_run(cmd, cwd=None, env=None):
                commands.append(cmd)
                return 0

            multi_triplets = [
                {
                    "prefix": "sample_a",
                    "records": Path("sample_a.records.tsv"),
                    "headers": Path("sample_a.header_lines.tsv"),
                    "metadata": Path("sample_a.file_metadata.tsv"),
                },
                {
                    "prefix": "sample_b",
                    "records": Path("sample_b.records.tsv"),
                    "headers": Path("sample_b.header_lines.tsv"),
                    "metadata": Path("sample_b.file_metadata.tsv"),
                },
            ]

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=multi_triplets
                ):
                    rc = invoke_main(["--input", str(input_dir), "--rules", str(rules_path), "--keep-tsv"])
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 6)
            self.assertIn("/opt/vcf-rdfizer/vcf_as_tsv.sh", commands[0])
            self.assertIn("/data/in/sample_a.vcf", commands[0])
            self.assertIn("OUT_NAME=sample_a", commands[1])
            self.assertIn("OUT_NAME=sample_a", commands[2])
            self.assertIn("/opt/vcf-rdfizer/vcf_as_tsv.sh", commands[3])
            self.assertIn("/data/in/sample_b.vcf", commands[3])
            self.assertIn("OUT_NAME=sample_b", commands[4])
            self.assertIn("OUT_NAME=sample_b", commands[5])

    def test_main_full_mode_deletes_nt_after_compression_by_default(self):
        """Full mode removes merged .nt outputs after successful compression unless --keep-rdf is set."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    output_name = next(part.split("=", 1)[1] for part in cmd if part.startswith("OUT_NAME="))
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.nt").write_text("<s> <p> <o> .\n")
                if "/opt/vcf-rdfizer/compression.sh" in cmd:
                    output_name = next(part.split("=", 1)[1] for part in cmd if part.startswith("OUT_NAME="))
                    hdt_dir = out_dir / "hdt"
                    hdt_dir.mkdir(parents=True, exist_ok=True)
                    (hdt_dir / f"{output_name}.hdt").write_text("fake-hdt\n")
                return 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--out",
                            str(out_dir),
                            "--keep-tsv",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertFalse((out_dir / "sample" / "sample.nt").exists())
            self.assertTrue((out_dir / "hdt" / "sample.hdt").exists())

    def test_main_full_mode_keep_rdf_preserves_nt_after_compression(self):
        """Full mode keeps merged .nt outputs when --keep-rdf is provided."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    output_name = next(part.split("=", 1)[1] for part in cmd if part.startswith("OUT_NAME="))
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.nt").write_text("<s> <p> <o> .\n")
                if "/opt/vcf-rdfizer/compression.sh" in cmd:
                    output_name = next(part.split("=", 1)[1] for part in cmd if part.startswith("OUT_NAME="))
                    hdt_dir = out_dir / "hdt"
                    hdt_dir.mkdir(parents=True, exist_ok=True)
                    (hdt_dir / f"{output_name}.hdt").write_text("fake-hdt\n")
                return 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--out",
                            str(out_dir),
                            "--keep-tsv",
                            "--keep-rdf",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertTrue((out_dir / "sample" / "sample.nt").exists())
            self.assertTrue((out_dir / "hdt" / "sample.hdt").exists())

    def test_main_ignores_unrelated_existing_tsv_triplets(self):
        """Wrapper converts only triplets that match the CLI-selected VCF snapshot."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            commands = []

            def fake_run(cmd, cwd=None, env=None):
                commands.append(cmd)
                return 0

            triplets = [
                {
                    "prefix": "sample",
                    "records": Path("sample.records.tsv"),
                    "headers": Path("sample.header_lines.tsv"),
                    "metadata": Path("sample.file_metadata.tsv"),
                },
                {
                    "prefix": "stale",
                    "records": Path("stale.records.tsv"),
                    "headers": Path("stale.header_lines.tsv"),
                    "metadata": Path("stale.file_metadata.tsv"),
                },
            ]

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=triplets
                ):
                    rc = invoke_main(["--input", str(input_dir), "--rules", str(rules_path), "--keep-tsv"])
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 3)
            conversion_cmd = commands[1]
            self.assertIn("OUT_NAME=sample", conversion_cmd)
            self.assertNotIn("OUT_NAME=stale", conversion_cmd)

    def test_main_uses_default_rules_when_flag_is_omitted(self):
        """Wrapper uses repository default rules file when --rules is omitted."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, _ = prepare_inputs(tmp_path)

            def fake_run(cmd, cwd=None, env=None):
                return 0

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ):
                    rc = invoke_main(["--input", str(input_dir), "--keep-tsv"])
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)

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
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
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
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
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
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
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
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
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
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
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
