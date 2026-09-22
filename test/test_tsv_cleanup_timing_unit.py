"""When the TSV intermediates are freed, not merely that they are.

The wrapper always removed them; it removed them at the end of the per-input
iteration, which is after the representation build. That build is 90% of
end-to-end wall time, so on the whole-file HG005 cell a 2.63 GB TSV set that
RMLStreamer finished reading at 00:03:51 stayed on disk for the remaining
14.8 hours, and peak workspace carried it: 16.23 GB, held within 90% of peak
for 4.47 h.

Removing them at the point the last reader finishes is worth about 2.6 GB of
that peak. These tests pin the ordering, because "the files are gone when the
run ends" was already true and is not the property that matters.
"""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import vcf_rdfizer
from test.helpers import VerboseTestCase


def make_triplet(directory: Path) -> dict:
    triplet = {"prefix": "sample"}
    for key, name in (
        ("records", "sample.records.tsv"),
        ("headers", "sample.header_lines.tsv"),
        ("metadata", "sample.file_metadata.tsv"),
        ("sample_calls", "sample.sample_calls.tsv"),
        ("sample_format_values", "sample.sample_format_values.tsv"),
    ):
        path = directory / name
        path.write_text("x\n", encoding="utf-8")
        triplet[key] = path
    return triplet


class RemoveTsvTripletTests(VerboseTestCase):
    def test_every_member_of_the_triplet_is_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            triplet = make_triplet(directory)
            ok, failed = vcf_rdfizer.remove_tsv_triplet(
                triplet,
                tsv_dir=directory,
                image_ref="image",
                wrapper_log_path=directory / "log",
            )
            self.assertTrue(ok)
            self.assertIsNone(failed)
            for key in ("records", "headers", "metadata", "sample_calls",
                        "sample_format_values"):
                self.assertFalse(triplet[key].exists(), key)

    def test_calling_it_twice_is_a_no_op_not_a_failure(self):
        """It runs once before the build and again as an end-of-loop sweep."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            triplet = make_triplet(directory)
            kwargs = dict(
                tsv_dir=directory,
                image_ref="image",
                wrapper_log_path=directory / "log",
            )
            self.assertEqual(
                vcf_rdfizer.remove_tsv_triplet(triplet, **kwargs), (True, None)
            )
            self.assertEqual(
                vcf_rdfizer.remove_tsv_triplet(triplet, **kwargs), (True, None)
            )

    def test_a_triplet_missing_optional_helper_paths_is_handled(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            triplet = make_triplet(directory)
            del triplet["sample_calls"]
            triplet["sample_format_values"] = None
            ok, failed = vcf_rdfizer.remove_tsv_triplet(
                triplet,
                tsv_dir=directory,
                image_ref="image",
                wrapper_log_path=directory / "log",
            )
            self.assertTrue(ok)
            self.assertIsNone(failed)

    def test_a_removal_failure_names_the_path_that_could_not_be_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            triplet = make_triplet(directory)
            with mock.patch.object(
                vcf_rdfizer, "remove_file_with_docker_fallback", return_value=False
            ):
                ok, failed = vcf_rdfizer.remove_tsv_triplet(
                    triplet,
                    tsv_dir=directory,
                    image_ref="image",
                    wrapper_log_path=directory / "log",
                )
            self.assertFalse(ok)
            self.assertEqual(failed, triplet["records"])


class CleanupHappensBeforeTheRepresentationBuildTests(VerboseTestCase):
    """Source-order check: the cheap, exact way to pin a sequencing property.

    Driving a full pipeline run to observe the ordering would need a container;
    what actually has to hold is that the cleanup call precedes the compression
    stage in the loop body, and that is directly checkable.
    """

    def _source(self) -> str:
        return Path(vcf_rdfizer.__file__).read_text(encoding="utf-8")

    def test_the_tsvs_are_freed_before_the_representation_build_starts(self):
        source = self._source()
        cleanup_marker = "Every TSV reader has now run"
        build_marker = "method_results_by_file: dict[str, dict[str, dict]] = {}"
        self.assertIn(cleanup_marker, source)
        self.assertIn(build_marker, source)
        self.assertLess(
            source.index(cleanup_marker),
            source.index(build_marker),
            "TSV cleanup must precede the representation build, which is 90% "
            "of wall time and the whole reason the files lingered",
        )

    def test_the_last_tsv_reader_still_precedes_the_cleanup(self):
        """Freeing them one line too early would break the record-detail emitter."""
        source = self._source()
        self.assertLess(
            source.rindex("header_lines_tsv=triplet[\"headers\"]"),
            source.index("Every TSV reader has now run"),
            "a TSV reader must not run after the cleanup",
        )

    def test_keep_tsv_still_suppresses_the_early_cleanup(self):
        source = self._source()
        marker = source.index("Every TSV reader has now run")
        window = source[marker:marker + 700]
        self.assertIn("if not keep_tsv:", window)


if __name__ == "__main__":
    unittest.main()
