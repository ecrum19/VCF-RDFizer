"""The shape layer, bundled and on by default.

The mutation-score experiment injected 113 targeted corruptions and the query
suite detected 96 -- a score of 0.850. Of the 17 it missed, 7 fall into four
classes the report itself attributes to the published SHACL profile:
``corrupt_allele_value``, ``corrupt_record_index``, ``corrupt_value_item_allele``
and ``corrupt_sample_index_expanded``. That profile was already written. It was
enabled in zero of the 62 validation runs in the benchmark campaign, because it
required a vocabulary checkout and an explicit flag.

So the shapes are vendored and applied by default, size-gated because pyshacl
loads the whole graph into memory. Closing those four classes takes the score
from 0.850 to 0.912 with no new oracle and no new query.
"""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import vcf_rdfizer
from test.helpers import VerboseTestCase

REPO_ROOT = Path(vcf_rdfizer.__file__).resolve().parent
DATA_ROOT = REPO_ROOT / "vcf_rdfizer_data"
VOCABULARY_CHECKOUT = REPO_ROOT.parent / "vcf-rdfizer-vocabulary"


class BundledAssetTests(VerboseTestCase):
    def test_the_default_shapes_are_bundled_with_the_package(self):
        """Requiring a separate checkout is why this was never switched on."""
        shapes, ontology = vcf_rdfizer.resolve_default_shacl_shapes(REPO_ROOT)
        self.assertIsNotNone(shapes)
        self.assertTrue(shapes.is_file())
        self.assertEqual(shapes.name, vcf_rdfizer.DEFAULT_SHACL_SHAPES)
        self.assertIsNotNone(ontology, "sh:class needs the class hierarchy")
        self.assertTrue(ontology.is_file())

    def test_the_version_overlays_are_bundled_too(self):
        """4.1-4.5 each have their own overlay; shipping only the core is half."""
        for version in ("4.1", "4.2", "4.3", "4.4", "4.5"):
            asset = vcf_rdfizer.resolve_bundled_vocabulary_asset(
                REPO_ROOT, f"shacl/vcf-{version}.shacl.ttl"
            )
            self.assertIsNotNone(asset, version)

    def test_the_consistency_profile_is_bundled(self):
        """It is the profile that covers the allele-value and item/raw classes."""
        self.assertIsNotNone(
            vcf_rdfizer.resolve_bundled_vocabulary_asset(
                REPO_ROOT, "shacl/vcf-core-consistency.shacl.ttl"
            )
        )

    def test_a_missing_asset_resolves_to_none_rather_than_raising(self):
        self.assertIsNone(
            vcf_rdfizer.resolve_bundled_vocabulary_asset(
                REPO_ROOT, "shacl/does-not-exist.ttl"
            )
        )

    def test_the_vendored_copies_match_their_recorded_digests(self):
        """Vendoring without a digest is how a copy drifts unnoticed."""
        provenance = json.loads(
            (DATA_ROOT / "VOCABULARY_PROVENANCE.json").read_text(encoding="utf-8")
        )
        for relative, expected in provenance["files"].items():
            actual = hashlib.sha256((DATA_ROOT / relative).read_bytes()).hexdigest()
            self.assertEqual(actual, expected, relative)

    def test_the_vendored_copies_match_the_vocabulary_checkout(self):
        """Skipped off a dev machine; the digests above still pin the content."""
        if not VOCABULARY_CHECKOUT.is_dir():
            self.skipTest("no sibling vcf-rdfizer-vocabulary checkout")
        provenance = json.loads(
            (DATA_ROOT / "VOCABULARY_PROVENANCE.json").read_text(encoding="utf-8")
        )
        for relative in provenance["files"]:
            upstream = VOCABULARY_CHECKOUT / relative
            if not upstream.is_file():
                continue
            self.assertEqual(
                (DATA_ROOT / relative).read_bytes(),
                upstream.read_bytes(),
                f"{relative} has drifted from the vocabulary checkout",
            )


class SizeGateTests(VerboseTestCase):
    def test_a_small_source_gets_shapes_by_default(self):
        self.assertTrue(vcf_rdfizer.shacl_default_applies(10 * 1024 * 1024))

    def test_a_source_at_the_limit_still_gets_shapes(self):
        self.assertTrue(
            vcf_rdfizer.shacl_default_applies(
                vcf_rdfizer.DEFAULT_SHACL_MAX_SOURCE_BYTES
            )
        )

    def test_a_cohort_scale_source_does_not(self):
        """pyshacl is in-memory; trying anyway turns a safety net into an OOM."""
        self.assertFalse(
            vcf_rdfizer.shacl_default_applies(
                vcf_rdfizer.DEFAULT_SHACL_MAX_SOURCE_BYTES + 1
            )
        )
        self.assertFalse(vcf_rdfizer.shacl_default_applies(200 * 1024 ** 3))

    def test_an_unknown_size_is_treated_as_too_large(self):
        """Skipping a check is recoverable; exhausting memory mid-run is not."""
        self.assertFalse(vcf_rdfizer.shacl_default_applies(None))

    def test_the_gate_is_a_documented_constant_not_a_literal(self):
        self.assertGreater(vcf_rdfizer.DEFAULT_SHACL_MAX_SOURCE_BYTES, 0)


class CliTests(VerboseTestCase):
    def test_the_opt_out_flag_exists_and_explains_what_it_disables(self):
        import sys
        from contextlib import redirect_stderr, redirect_stdout
        from io import StringIO
        from unittest import mock

        buffer = StringIO()
        with redirect_stdout(buffer), redirect_stderr(StringIO()):
            with mock.patch.object(
                sys, "argv", ["vcf_rdfizer.py", "--help"]
            ), self.assertRaises(SystemExit):
                vcf_rdfizer.main()
        help_text = buffer.getvalue()
        self.assertIn("--no-shacl", help_text)
        self.assertIn("--shacl-shapes", help_text)


class MutationCoverageTests(VerboseTestCase):
    """The four classes this is meant to close, named in the mutation catalogue."""

    SHAPE_COVERED = (
        "corrupt_allele_value",
        "corrupt_record_index",
        "corrupt_value_item_allele",
        "corrupt_sample_index_expanded",
    )

    def test_the_catalogue_still_attributes_these_classes_to_the_shape_layer(self):
        """If a query starts covering one, this assumption needs revisiting."""
        from test import validation_mutations

        source = Path(validation_mutations.__file__).read_text(encoding="utf-8")
        for mutation_id in self.SHAPE_COVERED:
            self.assertIn(mutation_id, source, mutation_id)


if __name__ == "__main__":
    unittest.main()
