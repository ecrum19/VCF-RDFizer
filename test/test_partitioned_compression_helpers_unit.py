"""The partitioned compressor's planning, accounting, and reporting helpers.

``src/partitioned_compression.py`` splits an aggregate into record-safe chunks,
converts each one, and merges the results pairwise. The existing
``test_partitioned_compression_unit`` covers the merge command lines and the
streaming contract; this file covers the surrounding layer - chunk planning,
progress events, resource accounting, and the failure text the operator
actually sees.

That reporting layer matters more than its size suggests: once the container
exits, its ephemeral volume is gone, so anything ``failure_message`` drops is
lost for good. Every test here runs on the host with no hdtc, pycottas, or
Docker.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test.helpers import VerboseTestCase

MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "partitioned_compression.py"


def load_module():
    spec = importlib.util.spec_from_file_location("partitioned_compression_helpers", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


P = load_module()


def empty_totals() -> dict:
    return {
        "exit_code": 0, "wall_seconds": 0.0, "user_seconds": 0.0, "sys_seconds": 0.0,
        "max_rss_kb": 0, "has_user": False, "has_sys": False, "has_rss": False,
    }


class ProgressDescriptorTests(VerboseTestCase):
    def test_a_chunk_stage_carries_its_chunk_number(self):
        """Per-chunk stages drive a counted progress bar, so the index is parsed."""
        self.assertEqual(P.progress_descriptor("hdt-build-7"), ("hdt-chunks", "chunks", 7))
        self.assertEqual(P.progress_descriptor("cottas-build-12"), ("cottas-chunks", "chunks", 12))

    def test_a_malformed_chunk_number_degrades_to_the_plain_stage(self):
        """An unparsable suffix must not crash the run for a progress label."""
        self.assertEqual(P.progress_descriptor("hdt-build-x"), ("hdt", "stage", None))

    def test_merge_stages_are_reported_separately_from_builds(self):
        """Merging is the long tail of a partitioned run and gets its own task."""
        self.assertEqual(P.progress_descriptor("hdt-merge-3"), ("hdt-merge", "stage", None))
        self.assertEqual(P.progress_descriptor("cottas-merge-many"), ("cottas-merge", "stage", None))

    def test_remaining_stages_fall_back_to_their_representation(self):
        """Indexing and other stages still group under the right representation."""
        self.assertEqual(P.progress_descriptor("hdt-index"), ("hdt", "stage", None))
        self.assertEqual(P.progress_descriptor("cottas-index"), ("cottas", "stage", None))

    def test_an_unrecognised_stage_keeps_its_own_name(self):
        """A new stage name is reported verbatim rather than mislabelled."""
        self.assertEqual(P.progress_descriptor("verify"), ("verify", "stage", None))


class ProgressSidecarTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def test_the_sidecar_directory_is_created_on_demand(self):
        """The host mount may not exist yet when the container starts writing."""
        path = self.root / "nested" / "progress.jsonl"
        P.prepare_progress_path(path)
        self.assertTrue(path.parent.is_dir())

    def test_preparing_no_sidecar_is_a_no_op(self):
        """Progress is optional, so None must be accepted everywhere."""
        self.assertIsNone(P.prepare_progress_path(None))

    def test_an_unwritable_sidecar_location_is_swallowed(self):
        """Progress must never break a conversion that is otherwise fine."""
        with mock.patch.object(P.Path, "mkdir", side_effect=OSError("read-only")):
            P.prepare_progress_path(self.root / "nested" / "progress.jsonl")

    def test_each_event_is_one_json_object_per_line(self):
        """The host tails this file, so every line must parse on its own."""
        path = self.root / "progress.jsonl"
        P.emit_progress(path, "hdt", "chunks", completed=1, total=5, unit="chunk")
        P.emit_progress(path, "hdt", "stage")
        events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(events[0], {
            "stage": "hdt", "phase": "chunks", "completed": 1, "total": 5, "unit": "chunk",
        })
        self.assertEqual(events[1], {"stage": "hdt", "phase": "stage"})

    def test_absent_fields_are_omitted_rather_than_written_as_null(self):
        """A null total would render as a progress bar of unknown length."""
        path = self.root / "progress.jsonl"
        P.emit_progress(path, "cottas", "stage", detail="merging")
        event = json.loads(path.read_text(encoding="utf-8").strip())
        self.assertEqual(set(event), {"stage", "phase", "detail"})

    def test_emitting_without_a_sidecar_is_a_no_op(self):
        """The common case is no progress path at all."""
        self.assertIsNone(P.emit_progress(None, "hdt", "stage"))

    def test_a_failing_write_never_propagates(self):
        """A full or read-only mount must not abort the conversion."""
        with mock.patch.object(P.Path, "open", side_effect=OSError("no space")):
            P.emit_progress(self.root / "progress.jsonl", "hdt", "stage")


class ChunkPlanningTests(VerboseTestCase):
    RECORD = b"<urn:s%d> <urn:p> <urn:o> .\n"

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.source = self.root / "graph.nt"
        self.source.write_bytes(b"".join(self.RECORD % index for index in range(40)))

    def test_chunk_sizes_must_be_positive(self):
        """A zero or negative bound would produce an unbounded chunk loop."""
        with self.assertRaises(ValueError) as raised:
            P.stream_chunks(self.source, self.root / "c", target_bytes=0, min_bytes=1, max_bytes=2)
        self.assertIn("must be positive", str(raised.exception))

    def test_chunk_sizes_must_be_ordered(self):
        """min <= target <= max is what makes a record-safe chunk reachable."""
        for target, minimum, maximum in ((10, 20, 30), (10, 5, 8)):
            with self.subTest(target=target, minimum=minimum, maximum=maximum):
                with self.assertRaises(ValueError) as raised:
                    P.stream_chunks(
                        self.source, self.root / "c",
                        target_bytes=target, min_bytes=minimum, max_bytes=maximum,
                    )
                self.assertIn("min <= target <= max", str(raised.exception))

    def test_every_record_survives_the_split(self):
        """Chunking is a partition: no record may be lost, split, or duplicated."""
        paths, plan = P.plan_chunks(
            self.source, self.root / "chunks",
            target_bytes=200, min_bytes=100, max_bytes=400,
        )
        self.assertGreater(len(paths), 1)
        self.assertEqual(plan["record_count"], 40)
        rejoined = b"".join(path.read_bytes() for path in paths)
        self.assertEqual(rejoined, self.source.read_bytes())

    def test_the_plan_counts_what_it_wrote(self):
        """The plan is the run's record of the split and must agree with the files."""
        paths, plan = P.plan_chunks(
            self.source, self.root / "chunks",
            target_bytes=200, min_bytes=100, max_bytes=400,
        )
        self.assertEqual(plan["chunk_count"], len(paths))
        self.assertEqual(len(plan["chunks"]), len(paths))
        self.assertEqual(plan["chunk_input_bytes"], self.source.stat().st_size)

    def test_chunk_metadata_spans_the_source_without_gaps(self):
        """Record and byte ranges must be contiguous across consecutive chunks."""
        _paths, plan = P.plan_chunks(
            self.source, self.root / "chunks",
            target_bytes=200, min_bytes=100, max_bytes=400,
        )
        for previous, current in zip(plan["chunks"], plan["chunks"][1:]):
            self.assertEqual(previous["end_record"], current["start_record"])
            self.assertEqual(
                previous["end_uncompressed_byte"], current["start_uncompressed_byte"]
            )
        self.assertEqual(plan["chunks"][0]["start_record"], 0)
        self.assertEqual(plan["chunks"][-1]["end_record"], plan["record_count"])

    def test_a_source_smaller_than_the_target_yields_one_chunk(self):
        """Below the minimum, partitioning converges on the single-file case."""
        paths, plan = P.plan_chunks(
            self.source, self.root / "chunks-single",
            target_bytes=10_000_000, min_bytes=1_000_000, max_bytes=100_000_000,
        )
        self.assertEqual(len(paths), 1)
        self.assertEqual(plan["record_count"], 40)

    def test_a_gzip_source_partitions_identically_to_a_plain_one(self):
        """Aggregate compression is a storage choice, not a chunking one."""
        gzipped = self.root / "graph.nt.gz"
        gzipped.write_bytes(gzip.compress(self.source.read_bytes()))
        plain_paths, plain_plan = P.plan_chunks(
            self.source, self.root / "plain", target_bytes=200, min_bytes=100, max_bytes=400)
        gz_paths, gz_plan = P.plan_chunks(
            gzipped, self.root / "gz", target_bytes=200, min_bytes=100, max_bytes=400)
        self.assertEqual(len(gz_paths), len(plain_paths))
        self.assertEqual(gz_plan["record_count"], plain_plan["record_count"])
        self.assertEqual(
            [chunk["record_count"] for chunk in gz_plan["chunks"]],
            [chunk["record_count"] for chunk in plain_plan["chunks"]],
        )

    def test_comments_and_blank_lines_are_not_counted_as_records(self):
        """The record count feeds the round-trip check and must match the graph."""
        noisy = self.root / "noisy.nt"
        noisy.write_bytes(b"# generated\n\n<urn:a> <urn:b> <urn:c> .\n\n# trailing\n")
        _paths, plan = P.plan_chunks(
            noisy, self.root / "noisy-chunks",
            target_bytes=1000, min_bytes=100, max_bytes=10_000,
        )
        self.assertEqual(plan["record_count"], 1)


class BinaryAndSidecarResolutionTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def test_an_executable_on_the_path_is_resolved_to_its_absolute_location(self):
        """Stage commands are logged, so the resolved path must be concrete."""
        resolved = P.resolve_executable(("sh",), "shell")
        self.assertTrue(Path(resolved).is_absolute())
        self.assertTrue(Path(resolved).is_file())

    def test_the_first_available_candidate_wins(self):
        """Candidate order encodes preference between builds of the same tool."""
        self.assertEqual(
            P.resolve_executable(("definitely-absent-xyz", "sh"), "shell"),
            P.resolve_executable(("sh",), "shell"),
        )

    def test_a_missing_tool_names_what_is_missing(self):
        """A container build problem must be distinguishable from a data problem."""
        with self.assertRaises(RuntimeError) as raised:
            P.resolve_executable(("definitely-absent-xyz",), "hdtc")
        self.assertIn("Missing hdtc in container", str(raised.exception))

    def test_no_index_sidecar_reports_none(self):
        """An unindexed HDT is a state to handle, not an error to raise."""
        artifact = self.root / "graph.hdt"
        artifact.write_bytes(b"hdt")
        self.assertIsNone(P.find_hdt_index_sidecar(artifact))

    def test_an_empty_index_sidecar_is_not_accepted(self):
        """A zero-byte index is a failed index run, not a usable one."""
        artifact = self.root / "graph.hdt"
        artifact.write_bytes(b"hdt")
        (self.root / "graph.hdt.index.v1-1").write_bytes(b"")
        self.assertIsNone(P.find_hdt_index_sidecar(artifact))

    def test_a_populated_index_sidecar_is_found(self):
        """The canonical versioned sidecar is what makes the HDT queryable."""
        artifact = self.root / "graph.hdt"
        artifact.write_bytes(b"hdt")
        (self.root / "graph.hdt.index.v1-1").write_bytes(b"")
        (self.root / "graph.hdt.index.v1-2").write_bytes(b"index")
        found = P.find_hdt_index_sidecar(artifact)
        self.assertIsNotNone(found)
        self.assertEqual(found.name, "graph.hdt.index.v1-2")

    def test_each_merge_gets_an_isolated_workspace(self):
        """Concurrent merges must not share a spill directory."""
        left = P.hdtc_merge_temp_dir(self.root, self.root / "merge-a.hdt")
        right = P.hdtc_merge_temp_dir(self.root, self.root / "merge-b.hdt")
        self.assertNotEqual(left, right)
        self.assertEqual(left.parent, self.root)


class TimeLogTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def test_a_missing_log_reports_unknown_rather_than_zero(self):
        """Zero CPU seconds is a measurement; a missing log is not."""
        self.assertEqual(
            P.parse_time_log(self.root / "absent.txt"),
            {"user_seconds": None, "sys_seconds": None, "max_rss_kb": None},
        )

    def test_a_complete_log_yields_every_measurement(self):
        """The three fields feed the run's metrics row."""
        log = self.root / "time.txt"
        log.write_text(
            "\tUser time (seconds): 1.25\n"
            "\tSystem time (seconds): 0.50\n"
            "\tMaximum resident set size (kbytes): 20480\n",
            encoding="utf-8",
        )
        self.assertEqual(
            P.parse_time_log(log),
            {"user_seconds": 1.25, "sys_seconds": 0.5, "max_rss_kb": 20480},
        )

    def test_an_unrecognised_log_yields_no_measurements(self):
        """A truncated or foreign log must not be parsed into plausible numbers."""
        log = self.root / "time.txt"
        log.write_text("nothing useful here\n", encoding="utf-8")
        self.assertEqual(
            P.parse_time_log(log),
            {"user_seconds": None, "sys_seconds": None, "max_rss_kb": None},
        )

    def test_a_partial_log_keeps_what_it_can(self):
        """One missing field must not discard the others."""
        log = self.root / "time.txt"
        log.write_text("\tUser time (seconds): 2\n", encoding="utf-8")
        parsed = P.parse_time_log(log)
        self.assertEqual(parsed["user_seconds"], 2.0)
        self.assertIsNone(parsed["max_rss_kb"])

    def test_the_resident_set_size_is_an_integer(self):
        """The metrics column is integral kilobytes, not a float."""
        log = self.root / "time.txt"
        log.write_text("\tMaximum resident set size (kbytes): 1024\n", encoding="utf-8")
        self.assertIsInstance(P.parse_time_log(log)["max_rss_kb"], int)


class TotalsTests(VerboseTestCase):
    def test_no_stages_report_unknown_rather_than_zero(self):
        """A run with nothing measured must not claim zero CPU seconds."""
        self.assertEqual(P.finalize_totals(empty_totals()), {
            "exit_code": 0, "wall_seconds": 0.0,
            "user_seconds": None, "sys_seconds": None, "max_rss_kb": None,
        })

    def test_wall_and_cpu_time_accumulate_across_stages(self):
        """Stage times sum; the run's cost is the whole pipeline's."""
        totals = empty_totals()
        P.add_totals(totals, {"exit_code": 0, "wall_seconds": 1.5, "user_seconds": 0.5})
        P.add_totals(totals, {"exit_code": 0, "wall_seconds": 2.0, "user_seconds": 0.25})
        finalized = P.finalize_totals(totals)
        self.assertEqual(finalized["wall_seconds"], 3.5)
        self.assertEqual(finalized["user_seconds"], 0.75)

    def test_peak_memory_is_a_maximum_not_a_sum(self):
        """Stages run sequentially, so the peak is the largest, not the total."""
        totals = empty_totals()
        P.add_totals(totals, {"exit_code": 0, "wall_seconds": 0, "max_rss_kb": 100})
        P.add_totals(totals, {"exit_code": 0, "wall_seconds": 0, "max_rss_kb": 50})
        self.assertEqual(P.finalize_totals(totals)["max_rss_kb"], 100)

    def test_the_worst_exit_code_survives(self):
        """One failed stage must fail the run even when later stages succeed."""
        totals = empty_totals()
        P.add_totals(totals, {"exit_code": 2, "wall_seconds": 0})
        P.add_totals(totals, {"exit_code": 0, "wall_seconds": 0})
        self.assertEqual(P.finalize_totals(totals)["exit_code"], 2)

    def test_a_stage_missing_a_measurement_does_not_zero_the_total(self):
        """An unmeasured stage leaves the others' measurements intact."""
        totals = empty_totals()
        P.add_totals(totals, {"exit_code": 0, "wall_seconds": 1.0, "user_seconds": 0.5})
        P.add_totals(totals, {"exit_code": 0, "wall_seconds": 1.0})
        finalized = P.finalize_totals(totals)
        self.assertEqual(finalized["user_seconds"], 0.5)
        self.assertIsNone(finalized["sys_seconds"])

    def test_absent_numbers_are_treated_as_zero_contributions(self):
        """A stage result with null timings must not raise."""
        totals = empty_totals()
        P.add_totals(totals, {"exit_code": None, "wall_seconds": None})
        self.assertEqual(P.finalize_totals(totals)["wall_seconds"], 0.0)


class FailureMessageTests(VerboseTestCase):
    def test_no_stage_result_leaves_the_fallback_untouched(self):
        """A failure with no captured result still reports something useful."""
        self.assertEqual(P.failure_message(None, "hdt merge failed"), "hdt merge failed")
        self.assertEqual(P.failure_message({}, "hdt merge failed"), "hdt merge failed")

    def test_the_exit_code_is_always_reported(self):
        """The numeric code is the one thing every failed stage has."""
        self.assertIn("exit_code=1", P.failure_message({"exit_code": 1}, "failed"))

    def test_an_oom_kill_is_named_rather_than_left_as_a_number(self):
        """-9 and 137 both mean the kernel or Docker killed the process."""
        self.assertIn("SIGKILL", P.failure_message({"exit_code": -9}, "failed"))
        self.assertIn("OOM", P.failure_message({"exit_code": 137}, "failed"))

    def test_a_termination_is_distinguished_from_a_kill(self):
        """SIGTERM is an orderly stop and should not be reported as OOM."""
        message = P.failure_message({"exit_code": 143}, "failed")
        self.assertIn("SIGTERM", message)
        self.assertNotIn("OOM", message)

    def test_the_stderr_tail_is_preserved_and_whitespace_normalised(self):
        """The container's volume is gone by the time this is read."""
        message = P.failure_message(
            {"exit_code": 1, "stderr_tail": "  DuckDB error:\n   no space left  "}, "failed")
        self.assertIn("stderr=DuckDB error: no space left", message)

    def test_the_stderr_tail_is_bounded(self):
        """Top-level CLI output must stay readable however large the traceback."""
        message = P.failure_message(
            {"exit_code": 1, "stderr_tail": "x" * 5000}, "failed")
        self.assertLess(len(message), 2500)
        self.assertIn("x" * 100, message)

    def test_a_non_numeric_exit_code_is_reported_without_interpretation(self):
        """An unexpected payload must not raise while building an error message."""
        message = P.failure_message({"exit_code": "weird"}, "failed")
        self.assertIn("exit_code=weird", message)
        self.assertNotIn("SIGKILL", message)

    def test_an_empty_stderr_tail_adds_no_stderr_clause(self):
        """An empty tail should not render as 'stderr='."""
        self.assertNotIn("stderr=", P.failure_message(
            {"exit_code": 1, "stderr_tail": "   "}, "failed"))


class TripleLineTests(VerboseTestCase):
    def test_statements_are_recognised_regardless_of_whitespace(self):
        """Record counting must not depend on the writer's formatting."""
        self.assertTrue(P.is_triple_line(b"<urn:a> <urn:b> <urn:c> ."))
        self.assertTrue(P.is_triple_line(b"   <urn:a> <urn:b> <urn:c> .   \n"))

    def test_comments_blanks_and_fragments_are_not_statements(self):
        """Only terminated, non-comment lines count toward the record total."""
        for line in (b"", b"  \n", b"# comment .", b"<urn:a> <urn:b> <urn:c>"):
            with self.subTest(line=line):
                self.assertFalse(P.is_triple_line(line))


if __name__ == "__main__":
    unittest.main()
