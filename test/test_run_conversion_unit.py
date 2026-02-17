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
        """Conversion script writes .nq output and unified metrics using mocked Java."""
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
            self.assertTrue((out_dir / "rdf" / "part-000.nq").exists())

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


if __name__ == "__main__":
    unittest.main()
