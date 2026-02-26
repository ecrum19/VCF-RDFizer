import csv
import json
import os
import re
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


def invoke_main(argv, *, auto_layout=True):
    args = list(argv)
    if auto_layout:
        mode = "full"
        for index, token in enumerate(args):
            if token in {"--mode", "-m"} and index + 1 < len(args):
                mode = args[index + 1]
                break
        if mode == "full" and "--rdf-layout" not in args and "-l" not in args:
            args.extend(["--rdf-layout", "aggregate"])

    with mock.patch.object(sys, "argv", ["vcf_rdfizer.py", *args]):
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


def output_name_from_command(cmd):
    for part in cmd:
        if isinstance(part, str) and part.startswith("OUT_NAME="):
            return part.split("=", 1)[1]
    if isinstance(cmd, list) and cmd and isinstance(cmd[-1], str):
        match = re.search(r"/data/out/(?:([^/]+)/)?([^/]+)\.hdt", cmd[-1])
        if match:
            return match.group(1) or match.group(2)
    return None


def latest_metrics_run_dir(metrics_root: Path) -> Path:
    """Return the single/latest per-run metrics directory."""
    run_dirs = sorted(
        (
            path
            for path in metrics_root.iterdir()
            if path.is_dir() and re.match(r"^\d{8}T\d{6}$", path.name)
        ),
        key=lambda path: path.name,
    )
    if not run_dirs:
        raise AssertionError(f"No per-run metrics directories found under {metrics_root}")
    return run_dirs[-1]


class WrapperUnitTests(VerboseTestCase):
    def test_print_summary_lists_all_selected_compression_sizes(self):
        """Summary printer includes one size line per requested compression method."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out" / "sample"
            out_root.mkdir(parents=True, exist_ok=True)
            nt_path = tmp_path / "sample.nt"
            nt_path.write_text("<s> <p> <o> .\n")
            (out_root / "sample.hdt").write_text("hdt\n")
            (out_root / "sample.nt.gz").write_text("gz\n")

            out_buf = StringIO()
            with redirect_stdout(out_buf):
                vcf_rdfizer.print_nt_hdt_summary(
                    output_root=out_root,
                    nt_path=nt_path,
                    hdt_path=out_root / "sample.hdt",
                    selected_methods=["hdt", "gzip"],
                    method_results={
                        "hdt": {"output_size_bytes": 4, "exit_code": 0},
                        "gzip": {"output_size_bytes": 3, "exit_code": 0},
                    },
                    indent="  ",
                )

            text = out_buf.getvalue()
            self.assertIn("- HDT (.hdt):", text)
            self.assertIn("- gzip (.nt.gz):", text)
            self.assertIn(str(out_root / "sample.hdt"), text)
            self.assertIn(str(out_root / "sample.nt.gz"), text)

    def test_update_metrics_csv_keeps_raw_and_hdt_compound_metrics_separate(self):
        """Metrics CSV keeps raw RDF gzip/brotli fields separate from gzip/brotli-on-HDT fields."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            metrics_csv = tmp_path / "metrics.csv"

            vcf_rdfizer.update_metrics_csv_with_compression(
                metrics_csv=metrics_csv,
                run_id="run-1",
                timestamp="2026-02-25T10:00:00",
                output_name="sample",
                output_dir=tmp_path / "out" / "sample",
                combined_size_bytes=100,
                selected_methods=["hdt_gzip"],
                method_results={
                    "hdt": {
                        "output_size_bytes": 40,
                        "exit_code": 0,
                        "wall_seconds": 1.25,
                        "source": "existing",
                    },
                    "hdt_gzip": {
                        "output_size_bytes": 12,
                        "exit_code": 0,
                        "wall_seconds": 0.50,
                    },
                },
            )

            with metrics_csv.open() as handle:
                row = next(csv.DictReader(handle))

            self.assertEqual(row["gzip_size_bytes"], "0")
            self.assertEqual(row["gzip_on_hdt_size_bytes"], "12")
            self.assertEqual(row["exit_code_gzip"], "0")
            self.assertEqual(row["exit_code_gzip_on_hdt"], "0")
            self.assertEqual(row["hdt_source"], "existing")

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
            self.assertEqual(estimate["rdf_low_bytes"], 14700)
            self.assertEqual(estimate["rdf_high_bytes"], 23450)
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
            self.assertIn("Estimated RDF N-Triples size:", out_buf.getvalue())
            self.assertIn("Warning: Estimated upper-bound RDF size exceeds currently free disk.", err_buf.getvalue())
            self.assertEqual(len(commands), 5)

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
            self.assertEqual(len(commands), 2)

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
            self.assertIn("/data/out/sample/sample.nq.gz", commands[0][-1])
            self.assertIn("brotli -q 7 -c", commands[1][-1])
            self.assertIn("/data/out/sample/sample.nq.br", commands[1][-1])

    def test_main_compress_mode_hdt_gzip_reuses_existing_hdt(self):
        """Compound method hdt_gzip reuses a preexisting HDT artifact instead of regenerating it."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            nt_path = tmp_path / "sample.nt"
            nt_path.write_text("<s> <p> <o> .\n")
            out_dir = tmp_path / "out"
            sample_out = out_dir / "sample"
            sample_out.mkdir(parents=True, exist_ok=True)
            (sample_out / "sample.hdt").write_text("prebuilt-hdt\n")
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
                            "hdt_gzip",
                            "--out",
                            str(out_dir),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 1)
            self.assertIn(
                "gzip -c /data/out/sample/sample.hdt > /data/out/sample/sample.hdt.gz",
                commands[0][-1],
            )
            self.assertNotIn("rdf2hdt", commands[0][-1])

    def test_main_compress_mode_hdt_brotli_generates_hdt_then_compresses_hdt(self):
        """Compound method hdt_brotli runs rdf2hdt first, then brotli on the generated HDT file."""
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
                            "hdt_brotli",
                            "--out",
                            str(out_dir),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(len(commands), 2)
            self.assertIn("rdf2hdt", commands[0][-1])
            self.assertIn("/data/out/sample/sample.hdt", commands[0][-1])
            self.assertIn(
                "brotli -q 7 -c /data/out/sample/sample.hdt > /data/out/sample/sample.hdt.br",
                commands[1][-1],
            )

    def test_main_compress_mode_logs_runtime_summary(self):
        """Compression mode writes runtime timing to metrics CSV and prints elapsed time."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            nq_path = tmp_path / "sample.nq"
            nq_path.write_text("<s> <p> <o> <g> .\n")
            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "metrics"

            def fake_run(cmd, cwd=None, env=None):
                return 0

            out_buf = StringIO()
            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), redirect_stdout(out_buf):
                    rc = invoke_main(
                        [
                            "--mode",
                            "compress",
                            "--nq",
                            str(nq_path),
                            "--compression",
                            "gzip",
                            "--out",
                            str(out_dir),
                            "--metrics",
                            str(metrics_dir),
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertIn("Run time (compress mode):", out_buf.getvalue())
            run_metrics_dir = latest_metrics_run_dir(metrics_dir)
            timings_csv = run_metrics_dir / "wrapper_execution_times.csv"
            self.assertTrue(timings_csv.exists())
            with timings_csv.open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[-1]["mode"], "compress")
            self.assertEqual(rows[-1]["status"], "success")

    def test_main_full_mode_prints_triplets_and_logs_total(self):
        """Full mode prints produced triples and records them in runtime timing log."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "metrics"

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    run_id = next(
                        (part.split("=", 1)[1] for part in cmd if isinstance(part, str) and part.startswith("RUN_ID=")),
                        "run",
                    )
                    out_name = next(
                        (part.split("=", 1)[1] for part in cmd if isinstance(part, str) and part.startswith("OUT_NAME=")),
                        "sample",
                    )
                    sample_dir = out_dir / out_name
                    sample_dir.mkdir(parents=True, exist_ok=True)
                    (sample_dir / f"{out_name}.nt").write_text("<s> <p> <o> .\n")
                    payload = {"artifacts": {"output_triples": {"TOTAL": 17}}}
                    run_metrics_dir = metrics_dir / run_id
                    run_metrics_dir.mkdir(parents=True, exist_ok=True)
                    conversion_metrics_dir = run_metrics_dir / "conversion_metrics" / out_name
                    conversion_metrics_dir.mkdir(parents=True, exist_ok=True)
                    (conversion_metrics_dir / run_id).write_text(
                        json.dumps(payload),
                        encoding="utf-8",
                    )
                return 0

            out_buf = StringIO()
            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ), redirect_stdout(out_buf):
                    rc = invoke_main(
                        [
                            "--mode",
                            "full",
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--rdf-layout",
                            "aggregate",
                            "--compression",
                            "none",
                            "--out",
                            str(out_dir),
                            "--metrics",
                            str(metrics_dir),
                            "--keep-tsv",
                            "--keep-rdf",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            output = out_buf.getvalue()
            self.assertIn("Triples produced: 17", output)
            self.assertIn("Total triples produced (full run): 17", output)
            self.assertIn("Run time (full mode):", output)

            run_metrics_dir = latest_metrics_run_dir(metrics_dir)
            timings_csv = run_metrics_dir / "wrapper_execution_times.csv"
            self.assertTrue(timings_csv.exists())
            with timings_csv.open() as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[-1]["mode"], "full")
            self.assertEqual(rows[-1]["status"], "success")
            self.assertEqual(rows[-1]["total_triples"], "17")

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
            self.assertIn("/data/out/sample/sample.nt.gz", commands[0][-1])

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

    def test_main_full_mode_requires_rdf_layout_argument(self):
        """Full mode fails validation when --rdf-layout is omitted."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            rc = invoke_main(
                ["--mode", "full", "--input", str(input_dir), "--rules", str(rules_path)],
                auto_layout=False,
            )
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
            self.assertTrue(any(arg.endswith("/out/sample:/data/out") for arg in commands[0]))
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
            self.assertIn("hdt2rdf", commands[0][-1])
            self.assertTrue(any(arg.endswith("/out/sample:/data/out") for arg in commands[0]))
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
            self.assertEqual(len(commands), 2)
            self.assertIn("/opt/vcf-rdfizer/vcf_as_tsv.sh", commands[0])
            self.assertIn("/opt/vcf-rdfizer/run_conversion.sh", commands[1])

    def test_main_full_mode_batch_layout_compresses_each_rml_part(self):
        """Batch layout compresses each part and prints one consolidated size summary."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"
            commands = []
            out_buf = StringIO()

            def fake_run(cmd, cwd=None, env=None):
                commands.append(cmd)
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    sample_dir = out_dir / "sample"
                    sample_dir.mkdir(parents=True, exist_ok=True)
                    (sample_dir / "part-00000.nt").write_text("<s1> <p> <o> .\n")
                    (sample_dir / "part-00001.nt").write_text("<s2> <p> <o> .\n")
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
                ), redirect_stdout(out_buf):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--rdf-layout",
                            "batch",
                            "--compression",
                            "gzip",
                            "--out",
                            str(out_dir),
                            "--keep-tsv",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertIn("AGGREGATE_RDF=0", commands[1])
            gzip_cmds = [cmd for cmd in commands if isinstance(cmd, list) and cmd and "gzip -c" in cmd[-1]]
            self.assertEqual(len(gzip_cmds), 2)
            self.assertIn("/data/in/part-00000.nt", gzip_cmds[0][-1])
            self.assertIn("/data/in/part-00001.nt", gzip_cmds[1][-1])
            self.assertEqual(out_buf.getvalue().count("* Output directory:"), 1)

    def test_main_full_mode_batch_metrics_upsert_is_sample_scoped(self):
        """Batch layout writes compression CSV metrics once per sample, not once per RDF part."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"
            seen_output_names = []

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    sample_dir = out_dir / "sample"
                    sample_dir.mkdir(parents=True, exist_ok=True)
                    (sample_dir / "part-00000.nt").write_text("<s1> <p> <o> .\n")
                    (sample_dir / "part-00001.nt").write_text("<s2> <p> <o> .\n")
                return 0

            def fake_update_metrics_csv_with_compression(**kwargs):
                seen_output_names.append(kwargs["output_name"])

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ), mock.patch.object(
                    vcf_rdfizer, "update_metrics_csv_with_compression", side_effect=fake_update_metrics_csv_with_compression
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--rdf-layout",
                            "batch",
                            "--compression",
                            "gzip",
                            "--out",
                            str(out_dir),
                            "--keep-tsv",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertEqual(seen_output_names, ["sample"])

    def test_main_full_mode_aggregate_layout_sets_merge_flag(self):
        """Aggregate layout passes AGGREGATE_RDF=1 to conversion step."""
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
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--rdf-layout",
                            "aggregate",
                            "--compression",
                            "none",
                            "--keep-tsv",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            self.assertIn("AGGREGATE_RDF=1", commands[1])

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
            self.assertEqual(len(commands), 10)
            self.assertIn("/opt/vcf-rdfizer/vcf_as_tsv.sh", commands[0])
            self.assertIn("/data/in/sample_a.vcf", commands[0])
            self.assertIn("OUT_NAME=sample_a", commands[1])
            self.assertIn("rdf2hdt", commands[4][-1])
            self.assertIn("/data/out/sample_a.hdt", commands[4][-1])
            self.assertIn("/opt/vcf-rdfizer/vcf_as_tsv.sh", commands[5])
            self.assertIn("/data/in/sample_b.vcf", commands[5])
            self.assertIn("OUT_NAME=sample_b", commands[6])
            self.assertIn("rdf2hdt", commands[9][-1])
            self.assertIn("/data/out/sample_b.hdt", commands[9][-1])

    def test_main_full_mode_deletes_nt_after_compression_by_default(self):
        """Full mode removes merged .nt outputs after successful compression unless --keep-rdf is set."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.nt").write_text("<s> <p> <o> .\n")
                if isinstance(cmd, list) and cmd and "rdf2hdt" in cmd[-1]:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.hdt").write_text("fake-hdt\n")
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
            self.assertTrue((out_dir / "sample" / "sample.hdt").exists())

    def test_main_full_mode_keep_rdf_preserves_nt_after_compression(self):
        """Full mode keeps merged .nt outputs when --keep-rdf is provided."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.nt").write_text("<s> <p> <o> .\n")
                if isinstance(cmd, list) and cmd and "rdf2hdt" in cmd[-1]:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.hdt").write_text("fake-hdt\n")
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
            self.assertTrue((out_dir / "sample" / "sample.hdt").exists())

    def test_main_full_mode_refuses_rdf_cleanup_until_all_methods_succeed(self):
        """Raw RDF is not deleted if any requested compression method is missing/failed."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.nt").write_text("<s> <p> <o> .\n")
                return 0

            # Simulate partial compression bookkeeping: gzip recorded, brotli missing.
            def fake_compress(*, rdf_path, out_dir, target_out_dir, image_ref, methods, wrapper_log_path, status_indent):
                return True, {
                    "gzip": {
                        "exit_code": 0,
                        "wall_seconds": 0.01,
                        "output_path": str((target_out_dir or out_dir) / f"{rdf_path.name}.gz"),
                        "output_size_bytes": 12,
                    }
                }

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ), mock.patch.object(
                    vcf_rdfizer, "run_compression_methods_for_rdf", side_effect=fake_compress
                ):
                    rc = invoke_main(
                        [
                            "--input",
                            str(input_dir),
                            "--rules",
                            str(rules_path),
                            "--out",
                            str(out_dir),
                            "--compression",
                            "gzip,brotli",
                            "--keep-tsv",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 1)
            self.assertTrue((out_dir / "sample" / "sample.nt").exists())

    def test_main_full_mode_writes_compression_metrics_artifacts(self):
        """Full mode writes compression metrics JSON/time artifacts and updates metrics.csv row."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "run_metrics"

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.nt").write_text("<s> <p> <o> .\n")
                if isinstance(cmd, list) and cmd and "rdf2hdt" in cmd[-1]:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.hdt").write_text("fake-hdt\n")
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
                            "--metrics",
                            str(metrics_dir),
                            "--compression",
                            "hdt",
                            "--keep-tsv",
                            "--keep-rdf",
                        ]
                    )
            finally:
                os.chdir(old_cwd)

            self.assertEqual(rc, 0)
            run_metrics_dir = latest_metrics_run_dir(metrics_dir)
            metrics_csv = run_metrics_dir / "metrics.csv"
            self.assertTrue(metrics_csv.exists())
            csv_text = metrics_csv.read_text()
            self.assertIn("compression_methods", csv_text)
            self.assertIn("sample", csv_text)
            self.assertIn("hdt", csv_text)

            json_file = run_metrics_dir / "compression_metrics" / "sample" / run_metrics_dir.name
            time_file = run_metrics_dir / "compression_time" / "hdt" / "sample" / run_metrics_dir.name
            self.assertTrue(json_file.exists())
            self.assertTrue(time_file.exists())

    def test_main_full_mode_deletes_nt_with_docker_fallback_on_permission_error(self):
        """Full mode falls back to Docker-based removal when .nt unlink raises PermissionError."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"
            target_nt = out_dir / "sample" / "sample.nt"
            target_nt_resolved = target_nt.resolve()
            commands = []
            original_unlink = Path.unlink

            def fake_run(cmd, cwd=None, env=None):
                commands.append(cmd)
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    target_nt.parent.mkdir(parents=True, exist_ok=True)
                    target_nt.write_text("<s> <p> <o> .\n")
                if isinstance(cmd, list) and cmd and "rdf2hdt" in cmd[-1]:
                    out_sample_dir = out_dir / "sample"
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / "sample.hdt").write_text("fake-hdt\n")
                if isinstance(cmd, list) and cmd[-1].startswith("rm -f ") and "/data/out/sample/sample.nt" in cmd[-1]:
                    if target_nt_resolved.exists():
                        original_unlink(target_nt_resolved)
                return 0

            def unlink_side_effect(path_obj, *args, **kwargs):
                if path_obj.resolve() == target_nt_resolved:
                    raise PermissionError(13, "Permission denied", str(path_obj))
                return original_unlink(path_obj, *args, **kwargs)

            old_cwd = os.getcwd()
            os.chdir(tmp_path)
            try:
                with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), mock.patch.object(
                    vcf_rdfizer, "check_docker", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "docker_image_exists", return_value=True
                ), mock.patch.object(
                    vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()
                ), mock.patch("pathlib.Path.unlink", autospec=True, side_effect=unlink_side_effect):
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
            self.assertFalse(target_nt.exists())
            self.assertTrue(
                any(
                    isinstance(cmd, list) and cmd[-1].startswith("rm -f ") and "/data/out/sample/sample.nt" in cmd[-1]
                    for cmd in commands
                )
            )

    def test_main_full_mode_deletes_raw_nq_and_keeps_compressed_output(self):
        """Full mode cleanup removes raw .nq (legacy) while preserving compressed outputs."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            input_dir, rules_path = prepare_inputs(tmp_path)
            out_dir = tmp_path / "out"

            def fake_run(cmd, cwd=None, env=None):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.nq").write_text("<s> <p> <o> <g> .\n")
                if isinstance(cmd, list) and cmd and "rdf2hdt" in cmd[-1]:
                    output_name = output_name_from_command(cmd) or "sample"
                    out_sample_dir = out_dir / output_name
                    out_sample_dir.mkdir(parents=True, exist_ok=True)
                    (out_sample_dir / f"{output_name}.hdt").write_text("fake-hdt\n")
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
            self.assertFalse((out_dir / "sample" / "sample.nq").exists())
            self.assertTrue((out_dir / "sample" / "sample.hdt").exists())

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
            self.assertEqual(len(commands), 5)
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

    def test_check_docker_falls_back_to_sudo_when_plain_docker_fails(self):
        """Docker check retries with sudo and flips command prefix mode when needed."""
        old_mode = vcf_rdfizer._DOCKER_USE_SUDO
        try:
            with mock.patch(
                "vcf_rdfizer.shutil.which",
                side_effect=["/usr/bin/docker", "/usr/bin/sudo"],
            ), mock.patch.object(vcf_rdfizer, "run", side_effect=[1, 0]) as mocked_run:
                self.assertTrue(vcf_rdfizer.check_docker())
                self.assertEqual(mocked_run.call_args_list[0].args[0], ["docker", "version"])
                self.assertEqual(mocked_run.call_args_list[1].args[0], ["sudo", "docker", "version"])
                self.assertEqual(vcf_rdfizer.docker_cmd_prefix(), ["sudo", "docker"])
        finally:
            vcf_rdfizer._DOCKER_USE_SUDO = old_mode

    def test_resolve_image_ref_accepts_repo_plus_version(self):
        """Image repository and explicit version resolve to a tagged image reference."""
        ref, requested = vcf_rdfizer.resolve_image_ref("vcf-rdfizer", "1.2.3")
        self.assertEqual(ref, "vcf-rdfizer:1.2.3")
        self.assertTrue(requested)


if __name__ == "__main__":
    unittest.main()
