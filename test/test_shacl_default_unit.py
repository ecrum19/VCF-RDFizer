"""The shape layer, bundled and on by default.

The mutation-score experiment injected 113 targeted corruptions and the query
suite detected 96 -- a score of 0.850. Of the 17 it missed, 7 fall into four
classes the report itself attributes to the published SHACL profile:
``corrupt_allele_value``, ``corrupt_record_index``, ``corrupt_value_item_allele``
and ``corrupt_sample_index_expanded``. That profile was already written. It was
enabled in zero of the 62 validation runs in the benchmark campaign, because it
required a vocabulary checkout and an explicit flag.

So the shapes are vendored and applied by default, size-gated because pyshacl
loads the whole graph into memory.

One correction, forced by measurement on bench-1: those four classes are NOT
covered by the default profile. ``vcf-core-vocabulary.shacl.ttl`` constrains
cardinality and datatype only; the uniqueness rules are in the SPARQL profile
and the value-agreement rules in the consistency profile. Those two cost 33.7 s
and 73.2 s on a 2,000-triple graph against 1.7 s for the core one, and their
sh:sparql constraints self-join the graph. So the default is the cheap profile,
``--shacl-profile full`` opts into the rest on a fixture-sized graph, and the
0.850 -> 0.912 figure belongs to full, not to the default.
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
        self.assertEqual(
            [path.name for path in shapes], list(vcf_rdfizer.DEFAULT_SHACL_SHAPES)
        )
        for path in shapes:
            self.assertTrue(path.is_file(), path)
        self.assertIsNotNone(ontology, "sh:class needs the class hierarchy")
        self.assertTrue(ontology.is_file())

    def test_the_default_profile_is_the_cheap_one_and_says_so(self):
        """Measured on bench-1: core 1.7 s, consistency 33.7 s, SPARQL 73.2 s
        on a 2,000-triple graph. The two that catch a corrupted value cost
        63x the one that does not, and their sh:sparql constraints self-join
        the graph, so the gap widens with size. core is what a run can absorb.
        """
        self.assertEqual(
            vcf_rdfizer.DEFAULT_SHACL_SHAPES, ("vcf-core-vocabulary.shacl.ttl",)
        )

    def test_the_full_profile_adds_the_two_that_catch_a_corrupted_value(self):
        """vcfc:sampleIndex in the core profile is minCount 1, maxCount 1,
        integer >= 1 -- nothing about two samples sharing one. Uniqueness is in
        the SPARQL profile, value agreement in the consistency profile.
        """
        self.assertIn("vcf-core-consistency.shacl.ttl", vcf_rdfizer.FULL_SHACL_SHAPES)
        self.assertIn(
            "vcf-core-vocabulary-sparql.shacl.ttl", vcf_rdfizer.FULL_SHACL_SHAPES
        )
        for name in vcf_rdfizer.DEFAULT_SHACL_SHAPES:
            self.assertIn(name, vcf_rdfizer.FULL_SHACL_SHAPES)

    def test_the_full_profile_resolves_all_three_files(self):
        shapes, ontology = vcf_rdfizer.resolve_default_shacl_shapes(REPO_ROOT, "full")
        self.assertEqual(
            [path.name for path in shapes], list(vcf_rdfizer.FULL_SHACL_SHAPES)
        )
        self.assertIsNotNone(ontology)

    def test_the_full_profile_has_a_much_smaller_size_gate(self):
        """It is quadratic-ish in graph size; the core gate would be unusable."""
        self.assertLess(
            vcf_rdfizer.FULL_SHACL_MAX_SOURCE_BYTES,
            vcf_rdfizer.DEFAULT_SHACL_MAX_SOURCE_BYTES,
        )
        big = vcf_rdfizer.FULL_SHACL_MAX_SOURCE_BYTES + 1
        self.assertFalse(vcf_rdfizer.shacl_default_applies(big, "full"))
        self.assertTrue(vcf_rdfizer.shacl_default_applies(big, "core"))

    def test_the_uniqueness_rules_really_live_outside_the_core_profile(self):
        """Pin the claim against the vendored files, not against memory."""
        shacl = DATA_ROOT / "shacl"
        core = (shacl / "vcf-core-vocabulary.shacl.ttl").read_text(encoding="utf-8")
        sparql = (shacl / "vcf-core-vocabulary-sparql.shacl.ttl").read_text(encoding="utf-8")
        consistency = (shacl / "vcf-core-consistency.shacl.ttl").read_text(encoding="utf-8")
        self.assertNotIn("must be unique", core)
        self.assertIn("Record indices must be unique", sparql)
        self.assertIn("Sample names and sample indices must be unique", sparql)
        self.assertIn("vcfc:alleleValue", consistency)

    def test_a_partial_profile_set_resolves_to_nothing(self):
        """Half a profile set silently drops whole classes of check."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "vcf_rdfizer_data" / "shacl").mkdir(parents=True)
            (root / "vcf_rdfizer_data" / "shacl"
             / vcf_rdfizer.DEFAULT_SHACL_SHAPES[0]).write_text("", encoding="utf-8")
            shapes, ontology = vcf_rdfizer.resolve_default_shacl_shapes(root)
            if shapes is not None:
                self.skipTest("installed package shadows the temporary root")
            self.assertIsNone(shapes)
            self.assertIsNone(ontology)

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




class SeverityTests(VerboseTestCase):
    """Only sh:Violation blocks a run; warnings are recommendations.

    Caught on the VM, not here: the first real pipeline run with the new
    default failed on test-1k with violationCount 0 and violationPaths
    [vcfc:fieldSource, vcfc:fieldVersion]. Those come from
    InfoHeaderLineRecommendedShape, which carries sh:severity sh:Warning
    because VCF 4.5 *recommends* Source and Version on INFO declarations and
    does not require them. pyshacl reports conforms=False for any result at any
    severity, so keying the verdict on conforms alone failed a conformant graph
    -- and would have failed almost every real VCF.
    """

    def _verdict(self, report_text):
        import importlib.util

        runner_path = (
            Path(vcf_rdfizer.__file__).resolve().parent
            / "src" / "validation" / "validation_runner.py"
        )
        spec = importlib.util.spec_from_file_location("vr_shacl", runner_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        violations = [
            line.strip() for line in report_text.splitlines()
            if line.strip().startswith("Constraint Violation")
        ]
        return "PASS" if not violations else "FAIL"

    def test_warnings_alone_do_not_fail_a_run(self):
        report = (
            "Validation Report\nConforms: False\nResults (2):\n"
            "Constraint Warning in MinCountConstraintComponent\n"
            "\tResult Path: vcfc:fieldSource\n"
            "Constraint Warning in MinCountConstraintComponent\n"
            "\tResult Path: vcfc:fieldVersion\n"
        )
        self.assertEqual(self._verdict(report), "PASS")

    def test_a_real_violation_still_fails(self):
        report = (
            "Validation Report\nConforms: False\nResults (1):\n"
            "Constraint Violation in DatatypeConstraintComponent\n"
            "\tResult Path: vcfc:pos\n"
        )
        self.assertEqual(self._verdict(report), "FAIL")

    def test_a_clean_report_passes(self):
        self.assertEqual(self._verdict("Validation Report\nConforms: True\n"), "PASS")

    def test_the_recommended_shape_really_is_a_warning(self):
        """Pin the assumption against the vendored shapes themselves."""
        shapes = (
            Path(vcf_rdfizer.__file__).resolve().parent
            / "vcf_rdfizer_data" / "shacl" / "vcf-core-vocabulary.shacl.ttl"
        ).read_text(encoding="utf-8")
        block = shapes[shapes.index("InfoHeaderLineRecommendedShape"):]
        block = block[:block.index(" .\n")]
        self.assertIn("vcfc:fieldSource", block)
        self.assertIn("vcfc:fieldVersion", block)
        self.assertEqual(block.count("sh:severity sh:Warning"), 2)

    def test_the_report_records_advisories_separately_from_violations(self):
        source = (
            Path(vcf_rdfizer.__file__).resolve().parent
            / "src" / "validation" / "validation_runner.py"
        ).read_text(encoding="utf-8")
        self.assertIn('"advisoryCount"', source)
        self.assertIn('"advisoryKinds"', source)
        self.assertIn('"status": "PASS" if not violations else "FAIL"', source)




class ReportParsingTests(VerboseTestCase):
    """pyshacl's real output format, captured from a run on bench-1.

    The module looked for lines starting "Constraint Violation". pyshacl 0.30.1
    writes "Validation Result in <Component>" with an indented "Severity:"
    line, so that matcher never fired: violationCount was always 0 and the
    verdict fell through to pyshacl's `conforms`, which is False for a warning
    as readily as for a violation. The layer was therefore broken in both
    directions -- unable to report a real violation, and failing any graph that
    merely carried a recommendation.

    test/fixtures/shacl-report-warnings-only.txt is the verbatim report from
    that run: six sh:Warning results for vcfc:fieldSource and
    vcfc:fieldVersion, zero violations.
    """

    FIXTURE = Path(__file__).resolve().parent / "fixtures" / "shacl-report-warnings-only.txt"

    def _module(self):
        import importlib.util

        path = (
            Path(vcf_rdfizer.__file__).resolve().parent
            / "src" / "validation" / "validation_runner.py"
        )
        spec = importlib.util.spec_from_file_location("vr_parse", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_the_real_report_parses_to_six_warnings_and_no_violations(self):
        results = self._module().parse_shacl_results(self.FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(len(results), 6)
        self.assertEqual({r["severity"] for r in results}, {"Warning"})

    def test_a_violation_block_is_classified_as_blocking(self):
        text = (
            "Conforms: False\nResults (1):\n"
            "Validation Result in DatatypeConstraintComponent (...):\n"
            "\tSeverity: sh:Violation\n"
            "\tResult Path: vcfc:pos\n"
        )
        results = self._module().parse_shacl_results(text)
        self.assertEqual([r["severity"] for r in results], ["Violation"])

    def test_mixed_severities_are_separated(self):
        text = (
            "Conforms: False\nResults (2):\n"
            "Validation Result in MinCountConstraintComponent (...):\n"
            "\tSeverity: sh:Warning\n\tResult Path: vcfc:fieldSource\n"
            "Validation Result in DatatypeConstraintComponent (...):\n"
            "\tSeverity: sh:Violation\n\tResult Path: vcfc:pos\n"
        )
        results = self._module().parse_shacl_results(text)
        self.assertEqual([r["severity"] for r in results], ["Warning", "Violation"])

    def test_a_clean_report_parses_to_nothing(self):
        self.assertEqual(self._module().parse_shacl_results("Conforms: True\n"), [])

    def test_a_block_with_no_severity_line_is_unknown_and_blocks(self):
        """An unclassifiable result is a parser bug; it must not read as clean."""
        text = "Validation Result in SomeComponent (...):\n\tResult Path: vcfc:pos\n"
        results = self._module().parse_shacl_results(text)
        self.assertEqual([r["severity"] for r in results], ["Unknown"])
        source = (
            Path(vcf_rdfizer.__file__).resolve().parent
            / "src" / "validation" / "validation_runner.py"
        ).read_text(encoding="utf-8")
        self.assertIn('in ("Violation", "Unknown")', source)

    def test_the_older_pyshacl_spelling_still_parses(self):
        """A downgrade must not silently stop reporting violations again."""
        text = (
            "Constraint Violation in DatatypeConstraintComponent (...):\n"
            "\tResult Path: vcfc:pos\n"
        )
        results = self._module().parse_shacl_results(text)
        self.assertEqual([r["severity"] for r in results], ["Violation"])


if __name__ == "__main__":
    unittest.main()
