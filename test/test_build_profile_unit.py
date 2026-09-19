"""Making the representation build's cost attributable instead of opaque.

The benchmark campaign could say that HDT and COTTAS construction was 90% of
end-to-end wall time and no more than that. Per-chunk and per-merge timings
were collected by StageRunner and then dropped at the wrapper boundary; the
shared chunk pass was never timed at all; and ``max_rss_kb`` was null for both
representations, so the stage taking 90% of the run reported no memory while
the mapping stage, at under 0.3%, reported 1.3-1.8 GB.

That gap is not only inconvenient. Reading the archived records, the identical
``chunk_count`` and ``chunk_input_bytes`` under both methods is equally
consistent with one shared chunk pass and with two -- and it is in fact one.
These tests pin the breakdown that makes the difference visible.
"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

from test.helpers import VerboseTestCase

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "partitioned_compression.py"
)


def load_module():
    spec = importlib.util.spec_from_file_location("partitioned_build_profile", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


P = load_module()


class FakeRunner:
    """A StageRunner stand-in carrying only what build_profile reads."""

    def __init__(self, stages):
        self.stages = stages

    def peak_workspace_bytes(self):
        return P.StageRunner.peak_workspace_bytes(self)


def stage(name, wall, *, rss=None, free_before=None, free_after=None, total=None):
    record = {"name": name, "wall_seconds": wall}
    if rss is not None:
        record["max_rss_kb"] = rss
    if free_before is not None:
        record["workspace_free_bytes_before"] = free_before
    if free_after is not None:
        record["workspace_free_bytes_after"] = free_after
    if total is not None:
        record["workspace_total_bytes"] = total
    return record


class ChunkStreamTimingTests(VerboseTestCase):
    def test_the_shared_chunk_pass_is_timed(self):
        """One decompress feeds every representation; its cost must be visible."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            source = directory / "graph.nt"
            source.write_bytes(b"".join(
                f"<urn:s{i}> <urn:p> <urn:o{i}> .\n".encode() for i in range(200)
            ))
            stream, plan = P.stream_chunks(
                source,
                directory / "chunks",
                target_bytes=256,
                min_bytes=64,
                max_bytes=4096,
            )
            for chunk_path, _metadata in stream:
                chunk_path.unlink(missing_ok=True)
            self.assertIn("chunk_stream_seconds", plan)
            self.assertIsInstance(plan["chunk_stream_seconds"], float)
            self.assertGreaterEqual(plan["chunk_stream_seconds"], 0.0)
            self.assertGreater(plan["chunk_count"], 1)

    def test_every_chunk_carries_its_own_write_time(self):
        """A slow chunk should be attributable, not averaged into a build total."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            source = directory / "graph.nt"
            source.write_bytes(b"".join(
                f"<urn:s{i}> <urn:p> <urn:o{i}> .\n".encode() for i in range(200)
            ))
            stream, plan = P.stream_chunks(
                source,
                directory / "chunks",
                target_bytes=256,
                min_bytes=64,
                max_bytes=4096,
            )
            for chunk_path, _metadata in stream:
                chunk_path.unlink(missing_ok=True)
            self.assertTrue(plan["chunks"])
            for chunk in plan["chunks"]:
                self.assertIn("write_seconds", chunk)
                self.assertIsNotNone(chunk["write_seconds"])

    def test_consumer_time_is_not_charged_to_the_stream(self):
        """The clock stops across the yield, or every build would look like I/O."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            source = directory / "graph.nt"
            source.write_bytes(b"".join(
                f"<urn:s{i}> <urn:p> <urn:o{i}> .\n".encode() for i in range(200)
            ))
            stream, plan = P.stream_chunks(
                source,
                directory / "chunks",
                target_bytes=256,
                min_bytes=64,
                max_bytes=4096,
            )
            import time as _time

            for chunk_path, _metadata in stream:
                _time.sleep(0.02)  # stand in for a chunk build
                chunk_path.unlink(missing_ok=True)
            # Several chunks x 20 ms of consumer time must not appear here.
            self.assertLess(plan["chunk_stream_seconds"], 0.02 * plan["chunk_count"])


class BuildProfileTests(VerboseTestCase):
    def test_chunk_builds_and_merges_are_bucketed_separately(self):
        runner = FakeRunner([
            stage("hdt-build-00000", 1.0),
            stage("hdt-build-00001", 2.0),
            stage("cottas-build-00000", 4.0),
            stage("hdt-merge-r01-00000", 0.5),
            stage("hdt-merge-r02-00000", 0.25),
        ])
        profile = P.build_profile(runner, {"chunk_stream_seconds": 9.0, "chunk_count": 2})
        buckets = profile["by_stage_kind"]
        self.assertEqual(buckets["hdt-chunk-build"]["stage_count"], 2)
        self.assertEqual(buckets["hdt-chunk-build"]["wall_seconds"], 3.0)
        self.assertEqual(buckets["cottas-chunk-build"]["wall_seconds"], 4.0)
        self.assertEqual(buckets["hdt-merge"]["stage_count"], 2)
        self.assertEqual(buckets["hdt-merge"]["wall_seconds"], 0.75)

    def test_merge_rounds_are_counted_per_representation(self):
        """log2(chunks) rounds of pairwise merging was previously unrecorded."""
        runner = FakeRunner([
            stage("hdt-merge-r01-00000", 1.0),
            stage("hdt-merge-r01-00001", 1.0),
            stage("hdt-merge-r02-00000", 1.0),
            stage("cottas-merge-r01-00000", 1.0),
        ])
        profile = P.build_profile(runner, {})
        self.assertEqual(profile["merge_rounds"], {"hdt": 2, "cottas": 1})

    def test_the_shared_chunk_cost_is_reported_beside_the_per_method_ones(self):
        runner = FakeRunner([stage("hdt-build-00000", 1.0)])
        profile = P.build_profile(
            runner, {"chunk_stream_seconds": 42.0, "chunk_count": 186,
                     "chunk_input_bytes": 99_325_167_164}
        )
        self.assertEqual(profile["chunk_stream_seconds"], 42.0)
        self.assertEqual(profile["chunk_count"], 186)
        self.assertEqual(profile["chunk_input_bytes"], 99_325_167_164)

    def test_peak_memory_is_the_maximum_across_stages(self):
        runner = FakeRunner([
            stage("hdt-build-00000", 1.0, rss=1_000),
            stage("cottas-build-00000", 1.0, rss=8_000),
            stage("hdt-merge-r01-00000", 1.0),
        ])
        self.assertEqual(P.build_profile(runner, {})["max_rss_kb"], 8_000)

    def test_a_build_with_no_memory_readings_reports_none_not_zero(self):
        """Absent is not zero; recording zero would be a false measurement."""
        runner = FakeRunner([stage("hdt-build-00000", 1.0)])
        self.assertIsNone(P.build_profile(runner, {})["max_rss_kb"])

    def test_a_missing_plan_does_not_break_the_failure_path(self):
        """build_profile also runs when the build raised before planning."""
        runner = FakeRunner([stage("hdt-build-00000", 1.0)])
        profile = P.build_profile(runner, None)
        self.assertIsNone(profile["chunk_stream_seconds"])
        self.assertIn("by_stage_kind", profile)


class VolumeWorkspaceTests(VerboseTestCase):
    def test_peak_volume_usage_is_derived_from_the_free_space_samples(self):
        """The host trace cannot see the Docker volume; this is the only source."""
        runner = FakeRunner([
            stage("hdt-build-00000", 1.0, free_before=90, free_after=70, total=100),
            stage("hdt-build-00001", 1.0, free_before=70, free_after=25, total=100),
        ])
        self.assertEqual(P.build_profile(runner, {})["peak_volume_workspace_bytes"], 75)

    def test_stages_without_workspace_samples_report_none(self):
        runner = FakeRunner([stage("hdt-build-00000", 1.0)])
        self.assertIsNone(P.build_profile(runner, {})["peak_volume_workspace_bytes"])


class ChildRssFallbackTests(VerboseTestCase):
    def test_the_fallback_returns_a_positive_reading_or_none(self):
        """GNU time is often absent in the image; this must fill that gap."""
        value = P.children_peak_rss_kb()
        if value is not None:
            self.assertGreater(value, 0)


if __name__ == "__main__":
    unittest.main()
