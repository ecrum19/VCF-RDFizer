import importlib.util
import os
import re
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


COTTAS_TOOL_PATH = Path(__file__).resolve().parents[1] / "src" / "cottas_tool.py"


def load_cottas_tool():
    """Load the Docker-side adapter without requiring pycottas locally."""
    spec = importlib.util.spec_from_file_location("cottas_tool_for_test", COTTAS_TOOL_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _FakeDuckDBCursor:
    def __init__(self, rows=None):
        self.rows = rows or []

    def fetchall(self):
        return self.rows


class _RecordingDuckDB:
    """Minimal DuckDB stand-in for asserting the adapter's SQL contract."""

    def __init__(self):
        self.database_paths = []
        self.queries = []
        self.closed = False

    def connect(self, database_path):
        self.database_paths.append(Path(database_path))
        return self

    def execute(self, query):
        self.queries.append(query)
        if query.startswith("DESCRIBE"):
            return _FakeDuckDBCursor([("s",), ("p",), ("o",)])
        if query.startswith("COPY"):
            match = re.search(r"\bTO '((?:''|[^'])*)'", query)
            assert match is not None
            Path(match.group(1).replace("''", "'")).write_text("merged COTTAS output\n")
        return _FakeDuckDBCursor()

    def close(self):
        self.closed = True


class CottasToolTests(unittest.TestCase):
    def test_convert_uses_a_fresh_duckdb_workspace_for_each_invocation(self):
        """Sequential chunk builds cannot reuse pycottas.duckdb or its quads table."""
        module = load_cottas_tool()
        observed_workspaces = []

        def fake_rdf2cottas(rdf_path, cottas_path, *, index, disk):
            self.assertTrue(Path(rdf_path).is_absolute())
            self.assertTrue(Path(cottas_path).is_absolute())
            self.assertEqual(index, "spo")
            self.assertTrue(disk)
            database_path = Path.cwd() / "pycottas.duckdb"
            if database_path.exists():
                raise RuntimeError("Table with name quads already exists")
            database_path.write_text("temporary DuckDB state\n")
            Path(cottas_path).write_text("COTTAS output\n")
            observed_workspaces.append(Path.cwd())

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scratch_root = (root / "scratch").resolve()
            source = root / "input.nt"
            source.write_text("<s> <p> <o> .\n")
            original_working_directory = Path.cwd()

            with mock.patch.dict(sys.modules, {"pycottas": types.SimpleNamespace(rdf2cottas=fake_rdf2cottas)}), mock.patch.dict(
                os.environ, {"COTTAS_SCRATCH_DIR": str(scratch_root)}, clear=False
            ):
                for chunk_number in range(2):
                    output = root / f"chunk-{chunk_number}.cottas"
                    with mock.patch.object(
                        sys,
                        "argv",
                        ["cottas_tool.py", "convert", str(source), str(output)],
                    ):
                        self.assertEqual(module.main(), 0)
                    self.assertTrue(output.is_file())

            self.assertEqual(Path.cwd(), original_working_directory)
            self.assertEqual(len(observed_workspaces), 2)
            self.assertNotEqual(observed_workspaces[0], observed_workspaces[1])
            self.assertTrue(all(path.parent == scratch_root for path in observed_workspaces))
            self.assertFalse(any(scratch_root.iterdir()))

    def test_merge_uses_a_bounded_disk_backed_duckdb_connection(self):
        """Merge SQL is spill-capable instead of using pycottas.cat in memory."""
        module = load_cottas_tool()
        fake_duckdb = _RecordingDuckDB()

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scratch_root = (root / "scratch").resolve()
            left = root / "left.cottas"
            right = root / "right.cottas"
            output = root / "merged.cottas"
            left.write_text("left\n")
            right.write_text("right\n")

            with mock.patch.dict(
                sys.modules,
                {
                    "pycottas": types.SimpleNamespace(),
                    "duckdb": types.SimpleNamespace(connect=fake_duckdb.connect),
                },
            ), mock.patch.dict(
                os.environ, {"COTTAS_SCRATCH_DIR": str(scratch_root)}, clear=False
            ), mock.patch.object(
                sys,
                "argv",
                ["cottas_tool.py", "merge", str(left), str(right), str(output)],
            ):
                self.assertEqual(module.main(), 0)

            self.assertTrue(output.is_file())
            self.assertTrue(fake_duckdb.closed)
            self.assertEqual(len(fake_duckdb.database_paths), 1)
            self.assertEqual(fake_duckdb.database_paths[0].parent.parent, scratch_root)
            self.assertIn("SET memory_limit = '4G'", fake_duckdb.queries)
            self.assertIn("SET threads = 1", fake_duckdb.queries)
            copy_query = next(query for query in fake_duckdb.queries if query.startswith("COPY"))
            self.assertIn("LAG(s) OVER (ORDER BY s, p, o)", copy_query)
            self.assertIn("s IS DISTINCT FROM prior_s", copy_query)
            self.assertIn("ORDER BY s, p, o", copy_query)
            self.assertNotIn("SELECT DISTINCT", copy_query)
            self.assertFalse(left.exists())
            self.assertFalse(right.exists())
            self.assertFalse(any(scratch_root.iterdir()))

    def test_merge_many_passes_all_inputs_to_disk_backed_duckdb(self):
        """The production merge scans every chunk through one spill-capable query."""
        module = load_cottas_tool()
        fake_duckdb = _RecordingDuckDB()

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scratch_root = (root / "scratch").resolve()
            inputs = []
            for number in range(3):
                path = root / f"chunk-{number}.cottas"
                path.write_text(f"chunk {number}\n")
                inputs.append(path)
            output = root / "merged.cottas"

            with mock.patch.dict(
                sys.modules,
                {
                    "pycottas": types.SimpleNamespace(),
                    "duckdb": types.SimpleNamespace(connect=fake_duckdb.connect),
                },
            ), mock.patch.dict(
                os.environ, {"COTTAS_SCRATCH_DIR": str(scratch_root)}, clear=False
            ), mock.patch.object(
                sys,
                "argv",
                [
                    "cottas_tool.py",
                    "merge-many",
                    "--input-cottas-files",
                    *(str(path) for path in inputs),
                    "--output-cottas-file",
                    str(output),
                ],
            ):
                self.assertEqual(module.main(), 0)

            self.assertTrue(output.is_file())
            copy_query = next(query for query in fake_duckdb.queries if query.startswith("COPY"))
            for path in inputs:
                self.assertIn(str(path.resolve()), copy_query)
                self.assertFalse(path.exists())
            self.assertIn("SET memory_limit = '4G'", fake_duckdb.queries)
            self.assertIn("SET threads = 1", fake_duckdb.queries)
            self.assertTrue(fake_duckdb.closed)
            self.assertFalse(any(scratch_root.iterdir()))

    def test_decompress_uses_pycottas_and_isolated_scratch(self):
        """COTTAS decompression writes RDF while cleaning container-local state."""
        module = load_cottas_tool()
        observed_workspaces = []

        def fake_cottas2rdf(cottas_path, rdf_path):
            self.assertTrue(Path(cottas_path).is_absolute())
            self.assertTrue(Path(rdf_path).is_absolute())
            Path(rdf_path).write_text("<s> <p> <o> .\n")
            database_path = Path.cwd() / "pycottas.duckdb"
            database_path.write_text("temporary DuckDB state\n")
            observed_workspaces.append(Path.cwd())

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scratch_root = (root / "scratch").resolve()
            source = root / "input.cottas"
            output = root / "output.nt"
            source.write_text("COTTAS input\n")

            with mock.patch.dict(
                sys.modules,
                {"pycottas": types.SimpleNamespace(cottas2rdf=fake_cottas2rdf)},
            ), mock.patch.dict(
                os.environ, {"COTTAS_SCRATCH_DIR": str(scratch_root)}, clear=False
            ), mock.patch.object(
                sys,
                "argv",
                ["cottas_tool.py", "decompress", str(source), str(output)],
            ):
                self.assertEqual(module.main(), 0)

            self.assertTrue(output.is_file())
            self.assertEqual(output.read_text(), "<s> <p> <o> .\n")
            self.assertEqual(len(observed_workspaces), 1)
            self.assertFalse(any(scratch_root.iterdir()))

    def test_reindex_rewrites_atomically_without_removing_input(self):
        """Reindex uses a disk-backed rewrite and replaces only on success."""
        module = load_cottas_tool()
        calls = []

        def fake_disk_merge(paths, cottas_path, *, index, remove_input_files):
            calls.append((paths, cottas_path, index, remove_input_files))
            self.assertNotEqual(Path(cottas_path), source)
            self.assertTrue(Path(cottas_path).name.startswith(f".{source.name}.reindex-"))
            Path(cottas_path).write_text("reindexed COTTAS\n")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scratch_root = root / "scratch"
            source = root / "input.cottas"
            source.write_text("original COTTAS\n")

            with mock.patch.dict(
                sys.modules,
                {"pycottas": types.SimpleNamespace()},
            ), mock.patch.dict(
                os.environ, {"COTTAS_SCRATCH_DIR": str(scratch_root)}, clear=False
            ), mock.patch.object(
                module, "disk_backed_cottas_merge", side_effect=fake_disk_merge
            ), mock.patch.object(
                sys,
                "argv",
                ["cottas_tool.py", "reindex", str(source)],
            ):
                self.assertEqual(module.main(), 0)

            self.assertEqual(source.read_text(), "reindexed COTTAS\n")
            self.assertEqual(
                calls,
                [([str(source.resolve())], mock.ANY, "spo", False)],
            )
            self.assertEqual(list(root.glob(".input.cottas.reindex-*.cottas")), [])

    def test_reindex_keeps_original_when_disk_backed_merge_fails(self):
        """A failed COTTAS rebuild does not replace the existing artifact."""
        module = load_cottas_tool()

        def failing_disk_merge(*args, **kwargs):
            raise RuntimeError("simulated reindex failure")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scratch_root = root / "scratch"
            source = root / "input.cottas"
            source.write_text("original COTTAS\n")

            with mock.patch.dict(
                sys.modules,
                {"pycottas": types.SimpleNamespace()},
            ), mock.patch.dict(
                os.environ, {"COTTAS_SCRATCH_DIR": str(scratch_root)}, clear=False
            ), mock.patch.object(
                module, "disk_backed_cottas_merge", side_effect=failing_disk_merge
            ), mock.patch.object(
                sys,
                "argv",
                ["cottas_tool.py", "reindex", str(source)],
            ):
                with self.assertRaisesRegex(RuntimeError, "simulated reindex failure"):
                    module.main()

            self.assertEqual(source.read_text(), "original COTTAS\n")
            self.assertEqual(list(root.glob(".input.cottas.reindex-*.cottas")), [])
