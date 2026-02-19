import csv
import subprocess
import tempfile
import unittest
from pathlib import Path

from test.helpers import METRICS_HEADER, VerboseTestCase, env_with_path, make_executable


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "src" / "run_conversion.sh"


class RunConversionUnitTests(VerboseTestCase):
    def test_run_conversion_writes_nq_and_metrics_without_real_java(self):
        """Conversion script writes a single merged .nq output and unified metrics."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            make_executable(
                fake_bin / "java",
                """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-version" ]]; then
  echo 'openjdk version "11.0.0"' >&2
  exit 0
fi
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then
    out="$2"
    shift 2
    continue
  fi
  shift
done
mkdir -p "$out"
printf '<s> <p> <o> .\\n' > "$out/part-000"
""",
            )

            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "metrics"
            rules = tmp_path / "rules.ttl"
            rules.write_text("@prefix ex: <http://example.org/> .\n")
            vcf = tmp_path / "input.vcf"
            vcf.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t5\n")

            env = env_with_path(fake_bin)
            env.update(
                {
                    "JAR": "fake.jar",
                    "IN": str(rules),
                    "IN_VCF": str(vcf),
                    "OUT_DIR": str(out_dir),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(metrics_dir),
                    "RUN_ID": "run123",
                    "TIMESTAMP": "2026-01-01T00:00:00",
                }
            )

            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            merged_nq = out_dir / "rdf" / "rdf.nq"
            self.assertTrue(merged_nq.exists())
            self.assertIn("<s> <p> <o> .", merged_nq.read_text())
            self.assertTrue((metrics_dir / "conversion-time-rdf-run123.txt").exists())
            self.assertTrue((metrics_dir / "conversion-metrics-rdf-run123.json").exists())

            metrics_csv = metrics_dir / "metrics.csv"
            self.assertTrue(metrics_csv.exists())
            with metrics_csv.open() as f:
                rows = list(csv.DictReader(f))
            self.assertTrue(rows)
            row = rows[0]
            self.assertEqual(list(row.keys()), METRICS_HEADER)
            self.assertEqual(row["run_id"], "run123")
            self.assertEqual(row["output_name"], "rdf")
            self.assertEqual(row["exit_code_java"], "0")
            self.assertEqual(row["compression_methods"], "")

    def test_run_conversion_exits_non_zero_when_java_fails(self):
        """Conversion script returns non-zero and records exit_code_java when Java command fails."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            make_executable(
                fake_bin / "java",
                """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-version" ]]; then
  echo 'openjdk version "11.0.0"' >&2
  exit 0
fi
exit 42
""",
            )

            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "metrics"
            rules = tmp_path / "rules.ttl"
            rules.write_text("@prefix ex: <http://example.org/> .\n")
            vcf = tmp_path / "input.vcf"
            vcf.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t5\n")

            env = env_with_path(fake_bin)
            env.update(
                {
                    "JAR": "fake.jar",
                    "IN": str(rules),
                    "IN_VCF": str(vcf),
                    "OUT_DIR": str(out_dir),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(metrics_dir),
                    "RUN_ID": "run-fail",
                    "TIMESTAMP": "2026-01-01T00:00:00",
                }
            )

            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 42)

            metrics_csv = metrics_dir / "metrics.csv"
            with metrics_csv.open() as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(rows[0]["exit_code_java"], "42")

    def test_run_conversion_backs_up_metrics_file_on_header_mismatch(self):
        """Header mismatch in metrics.csv creates a backup and rewrites a compatible header."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            make_executable(
                fake_bin / "java",
                """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-version" ]]; then
  echo 'openjdk version "11.0.0"' >&2
  exit 0
fi
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then
    out="$2"
    shift 2
    continue
  fi
  shift
done
mkdir -p "$out"
printf '<s> <p> <o> .\\n' > "$out/part-000"
""",
            )

            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "metrics"
            metrics_dir.mkdir(parents=True, exist_ok=True)
            bad_metrics = metrics_dir / "metrics.csv"
            bad_metrics.write_text("bad,header\nx,y\n")
            rules = tmp_path / "rules.ttl"
            rules.write_text("@prefix ex: <http://example.org/> .\n")
            vcf = tmp_path / "input.vcf"
            vcf.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t5\n")

            env = env_with_path(fake_bin)
            env.update(
                {
                    "JAR": "fake.jar",
                    "IN": str(rules),
                    "IN_VCF": str(vcf),
                    "OUT_DIR": str(out_dir),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(metrics_dir),
                    "RUN_ID": "run-hdr",
                    "TIMESTAMP": "2026-01-01T00:00:00",
                }
            )

            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue((metrics_dir / "metrics.csv.bak-run-hdr").exists())

    def test_run_conversion_handles_comment_only_output_without_crashing(self):
        """Comment-only output does not crash triple counting and records zero triples."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            make_executable(
                fake_bin / "java",
                """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-version" ]]; then
  echo 'openjdk version "11.0.0"' >&2
  exit 0
fi
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then
    out="$2"
    shift 2
    continue
  fi
  shift
done
mkdir -p "$out"
printf '# only comments\\n' > "$out/comments-only.nq"
""",
            )

            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "metrics"
            rules = tmp_path / "rules.ttl"
            rules.write_text("@prefix ex: <http://example.org/> .\n")
            vcf = tmp_path / "input.vcf"
            vcf.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t5\n")

            env = env_with_path(fake_bin)
            env.update(
                {
                    "JAR": "fake.jar",
                    "IN": str(rules),
                    "IN_VCF": str(vcf),
                    "OUT_DIR": str(out_dir),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(metrics_dir),
                    "RUN_ID": "run-comment",
                    "TIMESTAMP": "2026-01-01T00:00:00",
                }
            )

            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            with (metrics_dir / "metrics.csv").open() as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(rows[0]["output_triples"], "0")

    def test_run_conversion_only_renames_non_nq_outputs(self):
        """Normalization + merge writes rdf.nq with combined converted content."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            fake_bin = tmp_path / "bin"
            fake_bin.mkdir()
            make_executable(
                fake_bin / "java",
                """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-version" ]]; then
  echo 'openjdk version "11.0.0"' >&2
  exit 0
fi
out=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then
    out="$2"
    shift 2
    continue
  fi
  shift
done
mkdir -p "$out"
printf '<s> <p> <o> .\\n' > "$out/no-ext"
printf '<s2> <p2> <o2> .\\n' > "$out/already.nq"
""",
            )

            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "metrics"
            rules = tmp_path / "rules.ttl"
            rules.write_text("@prefix ex: <http://example.org/> .\n")
            vcf = tmp_path / "input.vcf"
            vcf.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t5\n")

            env = env_with_path(fake_bin)
            env.update(
                {
                    "JAR": "fake.jar",
                    "IN": str(rules),
                    "IN_VCF": str(vcf),
                    "OUT_DIR": str(out_dir),
                    "OUT_NAME": "rdf",
                    "LOGDIR": str(metrics_dir),
                    "RUN_ID": "run-rename",
                    "TIMESTAMP": "2026-01-01T00:00:00",
                }
            )

            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            merged_nq = out_dir / "rdf" / "rdf.nq"
            self.assertTrue(merged_nq.exists())
            text = merged_nq.read_text()
            self.assertIn("<s> <p> <o> .", text)
            self.assertIn("<s2> <p2> <o2> .", text)


if __name__ == "__main__":
    unittest.main()
