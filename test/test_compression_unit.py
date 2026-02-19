import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase, env_with_path, make_executable, seed_conversion_metrics_row


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "src" / "compression.sh"


def prepare_fake_tools(bin_dir: Path, fail_gzip: bool = False, fail_brotli: bool = False, fail_hdt: bool = False):
    gzip_fail = "exit 9\n" if fail_gzip else ""
    brotli_fail = "exit 8\n" if fail_brotli else ""
    hdt_fail = "exit 7\n" if fail_hdt else ""
    make_executable(
        bin_dir / "gzip",
        """#!/usr/bin/env bash
set -euo pipefail
{gzip_fail}file=""
for arg in "$@"; do
  if [[ "$arg" != -* ]]; then
    file="$arg"
  fi
done
cat "$file"
""".format(gzip_fail=gzip_fail),
    )
    make_executable(
        bin_dir / "brotli",
        """#!/usr/bin/env bash
set -euo pipefail
{brotli_fail}file=""
for arg in "$@"; do
  if [[ "$arg" != -* ]]; then
    file="$arg"
  fi
done
cat "$file"
""".format(brotli_fail=brotli_fail),
    )
    hdt = bin_dir / "rdf2hdt.sh"
    make_executable(
        hdt,
        """#!/usr/bin/env bash
set -euo pipefail
{hdt_fail}cp "$1" "$2"
""".format(hdt_fail=hdt_fail),
    )
    return hdt


def read_metrics_row(metrics_csv: Path, run_id: str, output_name: str):
    with metrics_csv.open() as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        if row["run_id"] == run_id and row["output_name"] == output_name:
            return row
    raise AssertionError(f"Metrics row not found for run_id={run_id}, output_name={output_name}")


class CompressionUnitTests(VerboseTestCase):
    def test_compression_errors_for_invalid_cli_option(self):
        """Invalid CLI option: compression script exits non-zero with an option error."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            result = subprocess.run(
                ["bash", str(SCRIPT), "-z"],
                cwd=tmp_path,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("invalid option", result.stderr)

    def test_compression_errors_for_unsupported_method(self):
        """Unsupported compression method: script exits non-zero with clear guidance."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            (out_root / "rdf").mkdir(parents=True)
            env = {"OUT_ROOT_DIR": str(out_root), "LOGDIR": str(tmp_path / "metrics")}
            env.update({"PATH": os.environ["PATH"]})
            result = subprocess.run(
                ["bash", str(SCRIPT), "-m", "snappy"],
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("unsupported compression method", result.stderr)

    def test_compression_errors_when_hdt_requested_but_binary_missing(self):
        """HDT requested with missing binary: script exits non-zero before processing."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            (output / "rdf.nq").write_text("<s> <p> <o> .\n")
            env = {"OUT_ROOT_DIR": str(out_root), "OUT_NAME": "rdf", "LOGDIR": str(tmp_path / "metrics")}
            env.update({"PATH": os.environ["PATH"], "RDF2HDT": str(tmp_path / "missing.sh")})
            result = subprocess.run(
                ["bash", str(SCRIPT), "-m", "hdt"],
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("rdf2hdt script not found", result.stderr)

    def test_compression_updates_existing_metrics_row_with_mocked_tools(self):
        """Compression mode gzip|brotli|hdt updates existing metrics row and writes artifacts."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            (output / "rdf.nq").write_text("<s> <p> <o> .\n<s2> <p2> <o2> .\n")

            logdir = tmp_path / "metrics"
            run_id = "run-compress-1"
            timestamp = "2026-01-01T00:00:00"
            metrics_csv = logdir / "metrics.csv"
            seed_conversion_metrics_row(metrics_csv, run_id, timestamp, "rdf", output)

            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            hdt_path = prepare_fake_tools(fake_bin)

            env = env_with_path(fake_bin)
            env.update(
                {
                    "OUT_ROOT_DIR": str(out_root),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(logdir),
                    "RUN_ID": run_id,
                    "TIMESTAMP": timestamp,
                    "RDF2HDT": str(hdt_path),
                }
            )

            result = subprocess.run(
                ["bash", str(SCRIPT), "-m", "gzip,brotli,hdt"],
                env=env,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue((out_root / "gzip" / "rdf.nq.gz").exists())
            self.assertTrue((out_root / "brotli" / "rdf.nq.br").exists())
            self.assertTrue((out_root / "hdt" / "rdf.hdt").exists())
            self.assertTrue((logdir / "compression-time-gzip-rdf-run-compress-1.txt").exists())
            self.assertTrue((logdir / "compression-time-brotli-rdf-run-compress-1.txt").exists())
            self.assertTrue((logdir / "compression-time-hdt-rdf-run-compress-1.txt").exists())
            self.assertTrue((logdir / "compression-metrics-rdf-run-compress-1.json").exists())

            row = read_metrics_row(metrics_csv, run_id, "rdf")
            self.assertEqual(row["run_id"], run_id)
            self.assertEqual(row["output_name"], "rdf")
            self.assertEqual(row["exit_code_java"], "0")
            self.assertEqual(row["compression_methods"], "gzip|brotli|hdt")
            self.assertEqual(row["exit_code_gzip"], "0")
            self.assertEqual(row["exit_code_brotli"], "0")
            self.assertEqual(row["exit_code_hdt"], "0")
            self.assertGreater(int(row["combined_nq_size_bytes"]), 0)

    def test_compression_prefers_nt_when_present(self):
        """Compression mode uses primary .nt output when both .nt and .nq files are present."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            (output / "rdf.nt").write_text("<s_nt> <p> <o> .\n")
            (output / "rdf.nq").write_text("<s_nq> <p> <o> <g> .\n")

            logdir = tmp_path / "metrics"
            run_id = "run-prefers-nt"
            timestamp = "2026-01-01T00:00:00"
            metrics_csv = logdir / "metrics.csv"
            seed_conversion_metrics_row(metrics_csv, run_id, timestamp, "rdf", output)

            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            hdt_path = prepare_fake_tools(fake_bin)

            env = env_with_path(fake_bin)
            env.update(
                {
                    "OUT_ROOT_DIR": str(out_root),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(logdir),
                    "RUN_ID": run_id,
                    "TIMESTAMP": timestamp,
                    "RDF2HDT": str(hdt_path),
                }
            )

            result = subprocess.run(
                ["bash", str(SCRIPT), "-m", "gzip,brotli,hdt"],
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue((out_root / "gzip" / "rdf.nt.gz").exists())
            self.assertTrue((out_root / "brotli" / "rdf.nt.br").exists())
            self.assertTrue((out_root / "hdt" / "rdf.hdt").exists())

    def test_compression_none_updates_metrics_without_generating_outputs(self):
        """Compression mode none leaves no compressed artifacts and records zero sizes."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            (output / "rdf.nq").write_text("<s> <p> <o> .\n")

            logdir = tmp_path / "metrics"
            run_id = "run-compress-2"
            timestamp = "2026-01-01T00:00:00"
            metrics_csv = logdir / "metrics.csv"
            seed_conversion_metrics_row(metrics_csv, run_id, timestamp, "rdf", output)

            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            hdt_path = prepare_fake_tools(fake_bin)

            env = env_with_path(fake_bin)
            env.update(
                {
                    "OUT_ROOT_DIR": str(out_root),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(logdir),
                    "RUN_ID": run_id,
                    "TIMESTAMP": timestamp,
                    "RDF2HDT": str(hdt_path),
                }
            )

            result = subprocess.run(
                ["bash", str(SCRIPT), "-m", "none"],
                env=env,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertFalse((out_root / "gzip").exists())
            self.assertFalse((out_root / "brotli").exists())
            self.assertFalse((out_root / "hdt").exists())

            row = read_metrics_row(metrics_csv, run_id, "rdf")
            self.assertEqual(row["compression_methods"], "none")
            self.assertEqual(row["combined_nq_size_bytes"], "0")
            self.assertEqual(row["gzip_size_bytes"], "0")
            self.assertEqual(row["brotli_size_bytes"], "0")
            self.assertEqual(row["hdt_size_bytes"], "0")

    def test_compression_fails_when_no_nq_files_are_available_for_requested_methods(self):
        """Requested compression with no .nq files returns non-zero and records method failures."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            logdir = tmp_path / "metrics"
            run_id = "run-no-nq"
            timestamp = "2026-01-01T00:00:00"
            metrics_csv = logdir / "metrics.csv"
            seed_conversion_metrics_row(metrics_csv, run_id, timestamp, "rdf", output)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            hdt_path = prepare_fake_tools(fake_bin)
            env = env_with_path(fake_bin)
            env.update(
                {
                    "OUT_ROOT_DIR": str(out_root),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(logdir),
                    "RUN_ID": run_id,
                    "TIMESTAMP": timestamp,
                    "RDF2HDT": str(hdt_path),
                }
            )
            result = subprocess.run(
                ["bash", str(SCRIPT), "-m", "gzip,brotli,hdt"],
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            row = read_metrics_row(metrics_csv, run_id, "rdf")
            self.assertEqual(row["exit_code_gzip"], "1")
            self.assertEqual(row["exit_code_brotli"], "1")
            self.assertEqual(row["exit_code_hdt"], "1")

    def test_compression_backs_up_metrics_file_on_header_mismatch(self):
        """Header mismatch in metrics.csv creates a backup and rewrites a compatible header."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            (output / "rdf.nq").write_text("<s> <p> <o> .\n")
            logdir = tmp_path / "metrics"
            logdir.mkdir(parents=True, exist_ok=True)
            (logdir / "metrics.csv").write_text("bad,header\nx,y\n")
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            hdt_path = prepare_fake_tools(fake_bin)
            env = env_with_path(fake_bin)
            env.update(
                {
                    "OUT_ROOT_DIR": str(out_root),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(logdir),
                    "RUN_ID": "run-hdr",
                    "TIMESTAMP": "2026-01-01T00:00:00",
                    "RDF2HDT": str(hdt_path),
                }
            )
            result = subprocess.run(["bash", str(SCRIPT), "-m", "gzip"], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue((logdir / "metrics.csv.bak-run-hdr").exists())

    def test_compression_reports_failure_when_gzip_fails(self):
        """gzip failure path returns non-zero and records gzip exit code."""
        self._assert_method_failure(methods="gzip", fail_gzip=True, expected_field="exit_code_gzip")

    def test_compression_reports_failure_when_brotli_fails(self):
        """brotli failure path returns non-zero and records brotli exit code."""
        self._assert_method_failure(methods="brotli", fail_brotli=True, expected_field="exit_code_brotli")

    def test_compression_reports_failure_when_hdt_fails(self):
        """hdt failure path returns non-zero and records hdt exit code."""
        self._assert_method_failure(methods="hdt", fail_hdt=True, expected_field="exit_code_hdt")

    def _assert_method_failure(self, methods: str, expected_field: str, fail_gzip: bool = False, fail_brotli: bool = False, fail_hdt: bool = False):
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            (output / "rdf.nq").write_text("<s> <p> <o> .\n")
            logdir = tmp_path / "metrics"
            run_id = f"run-{methods}-fail"
            timestamp = "2026-01-01T00:00:00"
            metrics_csv = logdir / "metrics.csv"
            seed_conversion_metrics_row(metrics_csv, run_id, timestamp, "rdf", output)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            hdt_path = prepare_fake_tools(fake_bin, fail_gzip=fail_gzip, fail_brotli=fail_brotli, fail_hdt=fail_hdt)
            env = env_with_path(fake_bin)
            env.update(
                {
                    "OUT_ROOT_DIR": str(out_root),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(logdir),
                    "RUN_ID": run_id,
                    "TIMESTAMP": timestamp,
                    "RDF2HDT": str(hdt_path),
                }
            )
            result = subprocess.run(["bash", str(SCRIPT), "-m", methods], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            row = read_metrics_row(metrics_csv, run_id, "rdf")
            self.assertNotEqual(row[expected_field], "0")


if __name__ == "__main__":
    unittest.main()
