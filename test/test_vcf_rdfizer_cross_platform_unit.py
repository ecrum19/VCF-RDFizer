import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

import vcf_rdfizer
from test.helpers import VerboseTestCase


def invoke_main(argv):
    args = []
    skip_next = False
    argv = list(argv)
    for index, token in enumerate(argv):
        if skip_next:
            skip_next = False
            continue
        if token == "--rdf":
            args.append("--rdf")
            continue
        if token in {"--metrics", "--tsv"} and index + 1 < len(argv):
            skip_next = True
            continue
        args.append(token)
    if "--out" not in args and "-o" not in args:
        args.extend(["--out", "./out"])

    with mock.patch.object(sys, "argv", ["vcf_rdfizer.py", *args]):
        return vcf_rdfizer.main()


class WrapperCrossPlatformUnitTests(VerboseTestCase):
    def test_help_flag_prints_usage(self):
        """CLI help succeeds and prints usage examples."""
        out_buf = StringIO()
        with mock.patch.object(sys, "argv", ["vcf_rdfizer.py", "--help"]), redirect_stdout(out_buf):
            with self.assertRaises(SystemExit) as exc:
                vcf_rdfizer.main()
        self.assertEqual(exc.exception.code, 0)
        self.assertIn("Examples:", out_buf.getvalue())

    def test_resolve_image_ref_with_version(self):
        """Image repository + explicit version resolves to a tagged image."""
        image, requested = vcf_rdfizer.resolve_image_ref("ecrum19/vcf-rdfizer", "1.0.0")
        self.assertEqual(image, "ecrum19/vcf-rdfizer:1.0.0")
        self.assertTrue(requested)

    def test_success_symbol_falls_back_for_cp1252_console(self):
        """Success marker uses ASCII fallback when stdout encoding cannot encode emoji."""
        fake_stdout = type("FakeStdout", (), {"encoding": "cp1252"})()
        with mock.patch.object(vcf_rdfizer.sys, "stdout", fake_stdout):
            self.assertEqual(vcf_rdfizer.success_symbol(), "[ok]")

    def test_parse_compression_methods_none(self):
        """Compression parser accepts 'none' as no-op selection."""
        self.assertEqual(vcf_rdfizer.parse_compression_methods("none"), [])

    def test_parse_compression_methods_rejects_unknown(self):
        """Compression parser rejects unsupported methods."""
        with self.assertRaises(ValueError):
            vcf_rdfizer.parse_compression_methods("gzip,unknown")

    def test_detect_compressed_format(self):
        """Compressed format detection works by extension."""
        self.assertEqual(vcf_rdfizer.detect_compressed_format(Path("sample.nt.gz")), "gzip")
        self.assertEqual(vcf_rdfizer.detect_compressed_format(Path("sample.nt.br")), "brotli")
        self.assertEqual(vcf_rdfizer.detect_compressed_format(Path("sample.hdt")), "hdt")

    def test_compress_mode_runs_with_mocks(self):
        """Compress mode succeeds with mocked Docker execution across OSes."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            nt_path = tmp_path / "sample.nt"
            nt_path.write_text("<s> <p> <o> .\n", encoding="utf-8")
            out_dir = tmp_path / "out"
            metrics_dir = tmp_path / "run_metrics"
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
                            "--rdf",
                            str(nt_path),
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
            self.assertGreaterEqual(len(commands), 1)


if __name__ == "__main__":
    unittest.main()
