import csv
import subprocess
import tempfile
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase, env_with_path, make_executable, seed_conversion_metrics_row


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "src" / "compression.sh"


def prepare_fake_tools(bin_dir: Path):
    make_executable(
        bin_dir / "gzip",
        """#!/usr/bin/env bash
set -euo pipefail
file="${@: -1}"
cp "$file" "$file.gz"
""",
    )
    make_executable(
        bin_dir / "brotli",
        """#!/usr/bin/env bash
set -euo pipefail
file="${@: -1}"
cp "$file" "$file.br"
""",
    )
    hdt = bin_dir / "rdf2hdt.sh"
    make_executable(
        hdt,
        """#!/usr/bin/env bash
set -euo pipefail
cp "$1" "$2"
""",
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
    def test_compression_updates_existing_metrics_row_with_mocked_tools(self):
        """Compression mode gzip|brotli|hdt updates existing metrics row and writes artifacts."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            (output / "chunk-a.nq").write_text("<s> <p> <o> .\n")
            (output / "chunk-b.nq").write_text("<s2> <p2> <o2> .\n")

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
            self.assertTrue((output / "rdf.nq.gz").exists())
            self.assertTrue((output / "rdf.nq.br").exists())
            self.assertTrue((output / "rdf.hdt").exists())

            row = read_metrics_row(metrics_csv, run_id, "rdf")
            self.assertEqual(row["run_id"], run_id)
            self.assertEqual(row["output_name"], "rdf")
            self.assertEqual(row["exit_code_java"], "0")
            self.assertEqual(row["compression_methods"], "gzip|brotli|hdt")
            self.assertEqual(row["exit_code_gzip"], "0")
            self.assertEqual(row["exit_code_brotli"], "0")
            self.assertEqual(row["exit_code_hdt"], "0")
            self.assertGreater(int(row["combined_nq_size_bytes"]), 0)

    def test_compression_none_updates_metrics_without_generating_outputs(self):
        """Compression mode none leaves no compressed artifacts and records zero sizes."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            out_root = tmp_path / "out"
            output = out_root / "rdf"
            output.mkdir(parents=True)
            (output / "chunk-a.nq").write_text("<s> <p> <o> .\n")

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
            self.assertFalse((output / "rdf.nq.gz").exists())
            self.assertFalse((output / "rdf.nq.br").exists())
            self.assertFalse((output / "rdf.hdt").exists())

            row = read_metrics_row(metrics_csv, run_id, "rdf")
            self.assertEqual(row["compression_methods"], "none")
            self.assertEqual(row["combined_nq_size_bytes"], "0")
            self.assertEqual(row["gzip_size_bytes"], "0")
            self.assertEqual(row["brotli_size_bytes"], "0")
            self.assertEqual(row["hdt_size_bytes"], "0")


if __name__ == "__main__":
    unittest.main()
