import csv
import os
import unittest
from pathlib import Path


METRICS_HEADER = [
    "run_id",
    "timestamp",
    "output_name",
    "output_dir",
    "exit_code_java",
    "wall_seconds_java",
    "user_seconds_java",
    "sys_seconds_java",
    "max_rss_kb_java",
    "input_mapping_size_bytes",
    "input_vcf_size_bytes",
    "output_dir_size_bytes",
    "output_triples",
    "jar",
    "mapping_file",
    "output_path",
    "combined_nq_size_bytes",
    "gzip_size_bytes",
    "brotli_size_bytes",
    "hdt_size_bytes",
    "exit_code_gzip",
    "exit_code_brotli",
    "exit_code_hdt",
    "wall_seconds_gzip",
    "user_seconds_gzip",
    "sys_seconds_gzip",
    "max_rss_kb_gzip",
    "wall_seconds_brotli",
    "user_seconds_brotli",
    "sys_seconds_brotli",
    "max_rss_kb_brotli",
    "wall_seconds_hdt",
    "user_seconds_hdt",
    "sys_seconds_hdt",
    "max_rss_kb_hdt",
    "compression_methods",
]


def make_executable(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(path.stat().st_mode | 0o111)


def seed_conversion_metrics_row(
    metrics_csv: Path, run_id: str, timestamp: str, output_name: str, output_dir: Path
) -> None:
    metrics_csv.parent.mkdir(parents=True, exist_ok=True)
    with metrics_csv.open("w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(METRICS_HEADER)
        writer.writerow(
            [
                run_id,
                timestamp,
                output_name,
                str(output_dir),
                "0",
                "0.12",
                "0.10",
                "0.02",
                "10240",
                "100",
                "200",
                "300",
                "3",
                "fake.jar",
                "rules.ttl",
                str(output_dir),
            ]
            + [""] * 20
        )


def env_with_path(bin_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    return env


class VerboseTestCase(unittest.TestCase):
    """Print explicit test start/end markers to make outcomes easy to scan."""

    def run(self, result=None):
        label = self.shortDescription() or self.id().split(".")[-1]
        print(f"\n[TEST] {label}")

        if result is None:
            result = self.defaultTestResult()

        failures_before = len(result.failures)
        errors_before = len(result.errors)
        skips_before = len(result.skipped)
        unexpected_before = len(result.unexpectedSuccesses)

        super().run(result)

        failed = (len(result.failures) > failures_before) or (len(result.errors) > errors_before)
        failed = failed or (len(result.unexpectedSuccesses) > unexpected_before)
        skipped = len(result.skipped) > skips_before

        if skipped:
            print(f"[SKIP] {label}")
        elif failed:
            print(f"[FAIL] {label}")
        else:
            print(f"[PASS] {label}")

        return result
