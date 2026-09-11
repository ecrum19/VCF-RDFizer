"""The compression validator's triple-count round trip.

``src/validate_compression.py`` is the cheap check that runs during conversion:
it counts N-Triples statements in the source and compares them against what the
generated HDT or COTTAS artifact decodes back to. It is weaker than the semantic
suite in ``src/validation/`` - a graph can survive this check and still be wrong -
so what matters here is that it never reports success it did not establish.

These tests run on the host: the decoder binaries and ``pycottas`` exist only
inside the image, so every test either stubs them or asserts the behaviour when
they are absent.
"""

from __future__ import annotations

import argparse
import gzip
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test.helpers import VerboseTestCase

MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "validate_compression.py"


def load_module():
    spec = importlib.util.spec_from_file_location("validate_compression_unit", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


M = load_module()


def emitter(*lines: str) -> list[str]:
    """A command that prints *lines*, standing in for a decoder binary."""
    payload = "".join(f"{line}\n" for line in lines)
    return [sys.executable, "-c", "import sys; sys.stdout.write(sys.argv[1])", payload]


class TripleLineTests(VerboseTestCase):
    def test_a_statement_terminated_by_a_period_counts(self):
        """The terminating '.' is what distinguishes a statement from a fragment."""
        self.assertTrue(M.is_triple_line(b"<urn:a> <urn:b> <urn:c> ."))

    def test_surrounding_whitespace_does_not_change_the_verdict(self):
        """Decoders differ in indentation; the count must not."""
        self.assertTrue(M.is_triple_line(b"  <urn:a> <urn:b> <urn:c> .  \n"))

    def test_blank_and_comment_lines_are_not_statements(self):
        """N-Triples comments and blank lines carry no triple."""
        for line in (b"", b"   ", b"\n", b"# a comment .", b"  # indented ."):
            with self.subTest(line=line):
                self.assertFalse(M.is_triple_line(line))

    def test_an_unterminated_line_is_not_a_statement(self):
        """A line without the terminator is a truncated write, not a triple."""
        self.assertFalse(M.is_triple_line(b"<urn:a> <urn:b> <urn:c>"))


class SourceCountTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def test_plain_ntriples_are_counted(self):
        """Only statement lines contribute to the source count."""
        path = self.root / "graph.nt"
        path.write_bytes(b"# header\n<urn:a> <urn:b> <urn:c> .\n\n<urn:d> <urn:e> <urn:f> .\n")
        self.assertEqual(M.count_nt(path), 2)

    def test_a_gzipped_source_counts_the_same_as_a_plain_one(self):
        """.nt.gz is read transparently, so compression is not a semantic choice."""
        plain = self.root / "graph.nt"
        plain.write_bytes(b"<urn:a> <urn:b> <urn:c> .\n<urn:d> <urn:e> <urn:f> .\n")
        gzipped = self.root / "graph.nt.gz"
        gzipped.write_bytes(gzip.compress(plain.read_bytes()))
        self.assertEqual(M.count_nt(gzipped), M.count_nt(plain))

    def test_an_empty_graph_counts_zero(self):
        """An empty source is a count, not an error, at this layer."""
        path = self.root / "empty.nt"
        path.write_bytes(b"")
        self.assertEqual(M.count_nt(path), 0)


class DecodedCountTests(VerboseTestCase):
    def test_streamed_statements_are_counted(self):
        """The decoder's stdout is counted without materializing a file."""
        self.assertEqual(M.count_decoded(emitter("<urn:a> <urn:b> <urn:c> .")), 1)

    def test_decoder_noise_is_excluded_from_the_count(self):
        """Comments and blank lines in the decode must not inflate the count."""
        command = emitter("# generated", "", "<urn:a> <urn:b> <urn:c> .")
        self.assertEqual(M.count_decoded(command), 1)

    def test_a_failing_decoder_raises_rather_than_returning_a_count(self):
        """A non-zero exit means the count is unusable, not merely low."""
        command = [sys.executable, "-c", "import sys; print('<urn:a> <urn:b> <urn:c> .'); sys.exit(3)"]
        with self.assertRaises(RuntimeError) as raised:
            M.count_decoded(command)
        self.assertIn("status 3", str(raised.exception))


class BinaryResolutionTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def make_executable(self, name: str) -> Path:
        path = self.root / name
        path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        path.chmod(0o755)
        return path

    def test_the_environment_override_wins(self):
        """HDT2RDF_BIN lets the image pin a build without editing the script."""
        binary = self.make_executable("hdt2rdf")
        with mock.patch.dict(M.os.environ, {"HDT2RDF_BIN": str(binary)}, clear=False):
            self.assertEqual(M.resolve_hdt2rdf(), str(binary))

    def test_a_non_executable_override_is_not_accepted(self):
        """A path that cannot be run is skipped rather than reported as found."""
        path = self.root / "not-executable"
        path.write_text("", encoding="utf-8")
        path.chmod(0o644)
        with mock.patch.dict(M.os.environ, {"HDT2RDF_BIN": str(path)}, clear=False):
            with self.assertRaises(RuntimeError):
                M.resolve_hdt2rdf()

    def test_an_absent_binary_is_refused_with_a_clear_message(self):
        """Missing hdt2rdf is a container build problem, and says so."""
        with mock.patch.dict(M.os.environ, {"HDT2RDF_BIN": str(self.root / "absent")}, clear=False):
            with mock.patch.object(M.Path, "is_file", return_value=False):
                with self.assertRaises(RuntimeError) as raised:
                    M.resolve_hdt2rdf()
        self.assertIn("Missing hdt2rdf binary", str(raised.exception))


class ValidateTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.source = self.root / "graph.nt"
        self.source.write_bytes(b"<urn:a> <urn:b> <urn:c> .\n<urn:d> <urn:e> <urn:f> .\n")
        self.artifact = self.root / "graph.hdt"
        self.artifact.write_bytes(b"not-really-hdt")

    def args(self, **overrides):
        values = {
            "source": str(self.source), "artifact": str(self.artifact), "format": "hdt",
            "expected_triples": None, "source_triples": None, "skip_index_check": True,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def decoding_to(self, count: int):
        """Stand in for hdt2rdf, which exists only inside the image."""
        return mock.patch.multiple(
            M, resolve_hdt2rdf=mock.Mock(return_value="/usr/local/bin/hdt2rdf"),
            count_decoded=mock.Mock(return_value=count),
        )

    def test_a_matching_round_trip_is_valid(self):
        """Equal source and decoded counts is the success case."""
        with self.decoding_to(2):
            result = M.validate(self.args())
        self.assertTrue(result["valid"])
        self.assertTrue(result["count_match"])
        self.assertEqual(result["validator"], "hdt2rdf")
        self.assertNotIn("error", result)

    def test_a_count_mismatch_reports_both_numbers(self):
        """A failure must name what was compared, not only that it failed."""
        with self.decoding_to(1):
            result = M.validate(self.args())
        self.assertFalse(result["count_match"])
        self.assertIn("source=2", result["error"])
        self.assertIn("decoded=1", result["error"])

    def test_a_precounted_source_is_trusted_instead_of_rereading(self):
        """The conversion already streamed the source, so it is not read twice."""
        with self.decoding_to(7):
            result = M.validate(self.args(source_triples=7))
        self.assertEqual(result["source_triples"], 7)
        self.assertTrue(result["count_match"])

    def test_a_source_disagreeing_with_the_conversion_stops_before_decoding(self):
        """If the source already lost triples, decoding it proves nothing."""
        decoder = mock.Mock()
        with mock.patch.object(M, "count_decoded", decoder):
            result = M.validate(self.args(expected_triples=99))
        self.assertFalse(result["valid"])
        self.assertIsNone(result["decoded_triples"])
        self.assertIn("does not match the upstream conversion count", result["error"])
        decoder.assert_not_called()

    def test_an_agreeing_expected_count_proceeds_to_the_decode(self):
        """The upstream count is a precondition, not a substitute for decoding."""
        with self.decoding_to(2):
            result = M.validate(self.args(expected_triples=2))
        self.assertTrue(result["valid"])
        self.assertTrue(result["count_match"])

    def test_a_missing_source_is_refused(self):
        """Validation cannot proceed without the graph it is validating."""
        with self.assertRaises(FileNotFoundError):
            M.validate(self.args(source=str(self.root / "absent.nt")))

    def test_a_missing_or_empty_artifact_is_refused(self):
        """A zero-byte artifact is a failed compression, not a valid empty graph."""
        empty = self.root / "empty.hdt"
        empty.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            M.validate(self.args(artifact=str(empty)))
        with self.assertRaises(FileNotFoundError):
            M.validate(self.args(artifact=str(self.root / "absent.hdt")))

    def test_the_index_check_is_required_unless_explicitly_skipped(self):
        """The helper lives in the image; its absence must fail loudly."""
        with self.assertRaises(RuntimeError) as raised:
            M.validate(self.args(skip_index_check=False))
        self.assertIn("HDT index helper", str(raised.exception))

    def test_a_failing_index_check_stops_the_validation(self):
        """An unreadable index is a failure even when the decode would succeed."""
        with mock.patch.object(M.Path, "is_file", return_value=True), \
                mock.patch.object(M.subprocess, "run", return_value=mock.Mock(returncode=4)):
            with self.assertRaises(RuntimeError) as raised:
                M.validate(self.args(skip_index_check=False))
        self.assertIn("status 4", str(raised.exception))

    def test_a_missing_cottas_dependency_is_reported_as_such(self):
        """pycottas is container-only; its absence must not look like a bad graph."""
        with mock.patch.dict(sys.modules, {"pycottas": None}):
            with self.assertRaises(RuntimeError) as raised:
                M.validate(self.args(format="cottas"))
        self.assertIn("COTTAS dependency is unavailable", str(raised.exception))

    def test_the_cottas_path_names_its_own_validator(self):
        """Reports record which decoder established the count."""
        with mock.patch.dict(sys.modules, {"pycottas": mock.Mock()}), \
                mock.patch.object(M, "count_decoded", return_value=2):
            result = M.validate(self.args(format="cottas"))
        self.assertEqual(result["validator"], "pycottas.cottas2rdf")
        self.assertTrue(result["count_match"])


class MainTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.source = self.root / "graph.nt"
        self.source.write_bytes(b"<urn:a> <urn:b> <urn:c> .\n")
        self.artifact = self.root / "graph.hdt"
        self.artifact.write_bytes(b"not-really-hdt")
        self.result_path = self.root / "reports" / "compression.json"

    def run_main(self, *extra):
        argv = [
            "validate_compression.py", "--source", str(self.source),
            "--artifact", str(self.artifact), "--format", "hdt",
            "--skip-index-check", "--result-path", str(self.result_path), *extra,
        ]
        with mock.patch.object(sys, "argv", argv):
            return M.main()

    def test_a_successful_validation_exits_zero_and_writes_its_result(self):
        """The JSON sidecar is the record; the exit code is only its summary."""
        with mock.patch.object(M, "resolve_hdt2rdf", return_value="/bin/true"), \
                mock.patch.object(M, "count_decoded", return_value=1):
            self.assertEqual(self.run_main(), 0)
        result = json.loads(self.result_path.read_text(encoding="utf-8"))
        self.assertTrue(result["valid"])
        self.assertTrue(result["count_match"])

    def test_the_result_directory_is_created_when_absent(self):
        """The caller should not have to prepare the reports tree first."""
        self.assertFalse(self.result_path.parent.exists())
        with mock.patch.object(M, "resolve_hdt2rdf", return_value="/bin/true"), \
                mock.patch.object(M, "count_decoded", return_value=1):
            self.run_main()
        self.assertTrue(self.result_path.is_file())

    def test_a_mismatch_exits_non_zero(self):
        """A decoded count that disagrees fails the run."""
        with mock.patch.object(M, "resolve_hdt2rdf", return_value="/bin/true"), \
                mock.patch.object(M, "count_decoded", return_value=0):
            self.assertEqual(self.run_main(), 1)
        self.assertFalse(json.loads(self.result_path.read_text())["count_match"])

    def test_an_exception_is_recorded_in_the_sidecar_rather_than_escaping(self):
        """A crashed decoder still leaves a machine-readable explanation behind."""
        with mock.patch.object(M, "resolve_hdt2rdf", side_effect=RuntimeError("no hdt2rdf here")):
            self.assertEqual(self.run_main(), 1)
        result = json.loads(self.result_path.read_text(encoding="utf-8"))
        self.assertFalse(result["valid"])
        self.assertIsNone(result["source_triples"])
        self.assertEqual(result["error"], "no hdt2rdf here")


if __name__ == "__main__":
    unittest.main()
