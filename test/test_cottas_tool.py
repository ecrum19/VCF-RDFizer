import importlib.util
import os
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

    def test_merge_uses_the_same_isolated_scratch_policy(self):
        """Pairwise merges do not inherit a DuckDB database from chunk conversion."""
        module = load_cottas_tool()
        observed_workspaces = []

        def fake_cat(paths, cottas_path, *, index, remove_input_files):
            self.assertTrue(all(Path(path).is_absolute() for path in paths))
            self.assertTrue(Path(cottas_path).is_absolute())
            self.assertEqual(index, "spo")
            self.assertTrue(remove_input_files)
            database_path = Path.cwd() / "pycottas.duckdb"
            if database_path.exists():
                raise RuntimeError("Table with name quads already exists")
            database_path.write_text("temporary DuckDB state\n")
            Path(cottas_path).write_text("merged COTTAS output\n")
            observed_workspaces.append(Path.cwd())

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scratch_root = (root / "scratch").resolve()
            left = root / "left.cottas"
            right = root / "right.cottas"
            output = root / "merged.cottas"
            left.write_text("left\n")
            right.write_text("right\n")

            with mock.patch.dict(sys.modules, {"pycottas": types.SimpleNamespace(cat=fake_cat)}), mock.patch.dict(
                os.environ, {"COTTAS_SCRATCH_DIR": str(scratch_root)}, clear=False
            ), mock.patch.object(
                sys,
                "argv",
                ["cottas_tool.py", "merge", str(left), str(right), str(output)],
            ):
                self.assertEqual(module.main(), 0)

            self.assertTrue(output.is_file())
            self.assertEqual(len(observed_workspaces), 1)
            self.assertFalse(any(scratch_root.iterdir()))
