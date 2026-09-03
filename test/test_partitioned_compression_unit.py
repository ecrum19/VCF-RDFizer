import gzip
import importlib.util
import tempfile
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase


ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "src" / "partitioned_compression.py"


def load_runner_module():
    spec = importlib.util.spec_from_file_location("partitioned_compression_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PartitionedCompressionUnitTests(VerboseTestCase):
    def test_hdtc_merge_command_uses_bounded_native_merge(self):
        """Partitioned HDT merges must not route through hdt-java's hdtCat."""
        runner = load_runner_module()
        with tempfile.TemporaryDirectory() as td:
            work_dir = Path(td) / "work"
            left = work_dir / "chunk-00000.hdt"
            right = work_dir / "chunk-00001.hdt"
            merged = work_dir / "hdt-merge-r01-00000.hdt"
            command = runner.hdtc_merge_command(
                "/usr/local/bin/hdtc",
                left,
                right,
                merged,
                work_dir=work_dir,
                memory_limit="512M",
            )

            self.assertEqual(
                command,
                [
                    "/usr/local/bin/hdtc",
                    "--quiet",
                    "create",
                    str(left),
                    str(right),
                    "--output",
                    str(merged),
                    "--memory-limit",
                    "512M",
                    "--temp-dir",
                    str(work_dir / ".hdt-merge-r01-00000.hdtc-work"),
                ],
            )
            self.assertNotIn("java", " ".join(command).lower())
            self.assertNotIn("hdtcat", " ".join(command).lower())

    def test_cottas_merge_many_command_uses_the_disk_backed_merge_adapter(self):
        """Production COTTAS merging scans chunks through one spill-capable stage."""
        runner = load_runner_module()
        command = runner.cottas_merge_many_command(
            "/opt/pycottas-venv/bin/python",
            [Path("/work/chunk-00000.cottas"), Path("/work/chunk-00001.cottas")],
            Path("/work/cottas-merge-final.cottas"),
        )
        self.assertEqual(
            command,
            [
                "/opt/pycottas-venv/bin/python",
                "/opt/vcf-rdfizer/cottas_tool.py",
                "merge-many",
                "--input-cottas-files",
                "/work/chunk-00000.cottas",
                "/work/chunk-00001.cottas",
                "--output-cottas-file",
                "/work/cottas-merge-final.cottas",
                "--index",
                "spo",
            ],
        )

    def test_cottas_merge_command_remains_compatible_for_two_inputs(self):
        """The adapter continues to expose a two-input disk-backed merge command."""
        runner = load_runner_module()
        command = runner.cottas_merge_command(
            "/opt/pycottas-venv/bin/python",
            Path("/work/chunk-00000.cottas"),
            Path("/work/chunk-00001.cottas"),
            Path("/work/cottas-merge-r01-00000.cottas"),
        )
        self.assertEqual(
            command,
            [
                "/opt/pycottas-venv/bin/python",
                "/opt/vcf-rdfizer/cottas_tool.py",
                "merge",
                "/work/chunk-00000.cottas",
                "/work/chunk-00001.cottas",
                "/work/cottas-merge-r01-00000.cottas",
                "spo",
            ],
        )

    def test_stage_runner_keeps_failed_stderr_tail(self):
        """Index warnings retain the subprocess diagnostic instead of hiding it."""
        runner_module = load_runner_module()
        with tempfile.TemporaryDirectory() as td:
            work_dir = Path(td) / "work"
            work_dir.mkdir()
            stage_runner = runner_module.StageRunner(work_dir)
            result = stage_runner.run(
                "cottas-merge-r01-00000",
                ["sh", "-c", "echo 'No space left on device' >&2; exit 1"],
            )
            self.assertEqual(result["exit_code"], 1)
            self.assertIn("No space left on device", result["stderr_tail"])
            self.assertFalse((work_dir / ".cottas-merge-r01-00000.stderr").exists())

    def test_stream_chunks_only_retains_the_chunk_being_consumed(self):
        """Gzip chunking does not stage a second full uncompressed aggregate."""
        runner = load_runner_module()
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "source.nt.gz"
            source_bytes = b"".join(
                f"<s{index}> <p> <o{index}> .\n".encode("utf-8")
                for index in range(12)
            )
            with gzip.open(source, "wb") as handle:
                handle.write(source_bytes)

            chunk_dir = tmp_path / "chunks"
            stream, plan = runner.stream_chunks(
                source,
                chunk_dir,
                target_bytes=40,
                min_bytes=20,
                max_bytes=60,
            )
            emitted = []
            for chunk, metadata in stream:
                self.assertEqual(list(chunk_dir.glob("*.nt")), [chunk])
                self.assertTrue(chunk.read_bytes().endswith(b"\n"))
                self.assertEqual(metadata["payload_bytes"], chunk.stat().st_size)
                emitted.append(chunk.read_bytes())
                chunk.unlink()

            self.assertEqual(b"".join(emitted), source_bytes)
            self.assertEqual(plan["record_count"], 12)
            self.assertEqual(plan["chunk_count"], len(plan["chunks"]))
            self.assertGreater(plan["chunk_count"], 1)
            self.assertEqual(list(chunk_dir.glob("*.nt")), [])


if __name__ == "__main__":
    unittest.main()
