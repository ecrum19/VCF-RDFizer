"""Unit tests for the indexed regional-access arm.

The point of the regional experiment is a speed comparison between access
paths, and a speed comparison is only meaningful once the paths agree on the
answer. So most of what is worth testing here is agreement: that the bcftools
arm folds its text output into the same structure the cyvcf2 arm builds from
parsed records, that the SPARQL normalizer produces that structure too, and
that the window convention is applied identically everywhere.

The bcftools and index-building paths need binaries that are present in the
VCF-RDFizer image but not necessarily on a developer machine, so those tests
skip rather than fail when the binary is absent. The folding logic itself is a
pure function and is always tested.
"""

from __future__ import annotations

import json
import shutil
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "validation"))

from test.helpers import VerboseTestCase  # noqa: E402

try:
    import regional_runner as R
except ImportError:  # pragma: no cover - the module must import to test it
    R = None

FIXTURES = Path(__file__).resolve().parent / "test_vcf_files"
SMALL_VCF = FIXTURES / "test-1k.vcf"

cyvcf2_available = R is not None and R.V.VCF is not None


def _bcftools_text(vcf_path: Path, query_id: str, samples: list[str]) -> str:
    """Reproduce `bcftools query -f <format>` output from the VCF text.

    This is what makes the cross-check meaningful: the bcftools arm is fed the
    exact columns bcftools would have printed, without needing bcftools on the
    machine running the test.
    """
    lines = []
    for line in vcf_path.read_text().splitlines():
        if line.startswith("#"):
            continue
        fields = line.split("\t")
        chrom, pos, ref, alt, filt = fields[0], fields[1], fields[3], fields[4], fields[6]
        if query_id == "r01_region_record_count":
            lines.append(f"{chrom}\t{pos}")
        elif query_id in ("r02_region_variant_shape_counts", "r03_region_titv"):
            lines.append(f"{chrom}\t{pos}\t{ref}\t{alt}")
        elif query_id == "r04_region_filter_distribution":
            lines.append(f"{chrom}\t{pos}\t{filt}")
        elif query_id == "r05_region_sample_genotype_counts":
            gts = []
            format_keys = fields[8].split(":") if len(fields) > 8 else []
            gt_index = format_keys.index("GT") if "GT" in format_keys else None
            for payload in fields[9:9 + len(samples)]:
                parts = payload.split(":")
                gts.append(parts[gt_index] if gt_index is not None
                           and gt_index < len(parts) else ".")
            lines.append("\t".join([chrom, pos, *gts]))
    return "\n".join(lines) + "\n"


@unittest.skipUnless(cyvcf2_available, "cyvcf2 is required")
class ArmAgreementTests(VerboseTestCase):
    """The two VCF arms must produce byte-identical canonical answers."""

    @classmethod
    def setUpClass(cls):
        cls.positions = R.contig_record_positions(SMALL_VCF)
        cls.samples = R.vcf_sample_names(SMALL_VCF)
        cls.windows = R.draw_windows(cls.positions, (1_000, 100_000), 3, R.DEFAULT_SEED)
        R.windows_expected_counts(cls.positions, cls.windows)

    def test_bcftools_folding_agrees_with_the_cyvcf2_arm(self):
        for query_id in R.REGIONAL_QUERIES:
            text = _bcftools_text(SMALL_VCF, query_id, self.samples)
            expected = R.answer_cyvcf2(
                SMALL_VCF, query_id, self.windows, self.samples, indexed=False)
            for window in self.windows:
                with self.subTest(query=query_id, window=window["window_id"]):
                    folded = R._fold_bcftools_output(
                        text, query_id, window, self.samples)
                    self.assertEqual(
                        R.canonical(folded),
                        R.canonical(expected[window["window_id"]]),
                    )

    def test_every_window_holds_at_least_one_record(self):
        """Anchoring is what makes the sizes a selectivity ladder."""
        for window in self.windows:
            with self.subTest(window=window["window_id"]):
                self.assertGreater(window["records_in_window"], 0)

    def test_the_record_count_question_matches_the_windows_own_count(self):
        answers = R.answer_cyvcf2(
            SMALL_VCF, "r01_region_record_count", self.windows, self.samples, indexed=False)
        for window in self.windows:
            self.assertEqual(answers[window["window_id"]]["recordCount"],
                             window["records_in_window"])

    def test_windows_are_reproducible_from_the_seed(self):
        """Every arm has to see the same regions, across runs and machines."""
        again = R.draw_windows(self.positions, (1_000, 100_000), 3, R.DEFAULT_SEED)
        self.assertEqual([w["window_id"] for w in again],
                         [w["window_id"] for w in self.windows])
        self.assertEqual([(w["chrom"], w["start"]) for w in again],
                         [(w["chrom"], w["start"]) for w in self.windows])

    def test_a_different_seed_draws_different_windows(self):
        other = R.draw_windows(self.positions, (1_000,), 3, R.DEFAULT_SEED + 1)
        mine = [w for w in self.windows if w["size"] == 1_000]
        self.assertNotEqual([(w["chrom"], w["start"]) for w in other],
                            [(w["chrom"], w["start"]) for w in mine])


@unittest.skipUnless(R is not None, "regional_runner must import")
class RegionConventionTests(VerboseTestCase):
    """A window means POS, on every arm."""

    WINDOW = {"window_id": "w", "chrom": "1", "start": 100, "end": 200, "size": 101}

    def test_a_record_outside_the_window_is_dropped_by_the_folder(self):
        """htslib returns overlap; the post-filter reconciles it to POS."""
        text = "\n".join(f"1\t{pos}" for pos in (50, 100, 150, 200, 201)) + "\n"
        folded = R._fold_bcftools_output(text, "r01_region_record_count", self.WINDOW, [])
        self.assertEqual(folded, {"recordCount": 3})

    def test_the_window_is_inclusive_at_both_edges(self):
        text = "1\t100\n1\t200\n"
        folded = R._fold_bcftools_output(text, "r01_region_record_count", self.WINDOW, [])
        self.assertEqual(folded, {"recordCount": 2})

    def test_a_record_on_another_contig_is_not_counted(self):
        """Regression: the folder used to trust `-r` and filter on POS alone.

        A record at the same coordinate on a different contig was then counted,
        which `-r` happens to prevent -- so the arm was correct only by relying
        on an argument it never checked. Every format string now carries CHROM.
        """
        text = "\n".join(["1\t150", "2\t150", "X\t150"]) + "\n"
        folded = R._fold_bcftools_output(text, "r01_region_record_count", self.WINDOW, [])
        self.assertEqual(folded, {"recordCount": 1})

    def test_every_bcftools_format_asks_for_the_contig(self):
        for query_id, fmt in R.BCFTOOLS_FORMATS.items():
            with self.subTest(query=query_id):
                self.assertTrue(fmt.startswith("%CHROM\t%POS"), fmt)

    def test_region_semantics_are_documented_next_to_the_code(self):
        """The convention is the easiest thing here to get silently wrong."""
        self.assertIn("POS", R.REGION_SEMANTICS)
        self.assertIn("overlap", R.REGION_SEMANTICS.lower())


@unittest.skipUnless(R is not None, "regional_runner must import")
class GenotypeClassTests(VerboseTestCase):
    """`%GT` text must land in the same classes the allele-index oracle uses."""

    def test_the_lexical_classifier_matches_the_allele_classifier(self):
        cases = {
            "0/0": "HOM_REF", "0|0": "HOM_REF",
            "1/1": "HOM_ALT", "2|2": "HOM_ALT",
            "0/1": "HET", "1|2": "HET",
            "0": "HAPLOID_REF", "1": "HAPLOID_ALT",
            "./.": "MISSING", ".": "MISSING", "./1": "MISSING", "": "MISSING",
            "0/1/1": "OTHER_PLOIDY",
        }
        for raw, expected in cases.items():
            with self.subTest(gt=raw):
                self.assertEqual(R._classify_bcftools_gt(raw), expected)


@unittest.skipUnless(R is not None, "regional_runner must import")
class NormalizerTests(VerboseTestCase):
    """SPARQL Results JSON folds into the same shape the VCF arms produce."""

    def _raw(self, tmp: Path, bindings: list[dict]) -> Path:
        path = tmp / "raw.json"
        path.write_text(json.dumps({"results": {"bindings": bindings}}))
        return path

    def test_a_single_row_aggregate_becomes_a_dict(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            raw = self._raw(Path(td), [{"recordCount": {"value": "42"}}])
            self.assertEqual(
                R.normalize_regional("r01_region_record_count", raw),
                {"recordCount": 42},
            )

    def test_an_unbound_sum_on_an_empty_window_reads_as_zero(self):
        """Engines disagree on SUM over nothing; that is not a data mismatch."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            raw = self._raw(Path(td), [{"biallelicSnvCount": {"value": "0"}}])
            self.assertEqual(
                R.normalize_regional("r03_region_titv", raw),
                {"biallelicSnvCount": 0, "transitionCount": 0, "transversionCount": 0},
            )

    def test_no_rows_at_all_is_still_a_well_formed_empty_answer(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            raw = self._raw(Path(td), [])
            self.assertEqual(
                R.normalize_regional("r01_region_record_count", raw),
                {"recordCount": 0},
            )
            self.assertEqual(
                R.normalize_regional("r02_region_variant_shape_counts", raw), [])

    def test_grouped_rows_are_sorted_into_the_canonical_order(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            raw = self._raw(Path(td), [
                {"variantClass": {"value": "SNV"}, "recordCount": {"value": "2"}},
                {"variantClass": {"value": "DELETION_SHAPE"}, "recordCount": {"value": "1"}},
            ])
            self.assertEqual(
                R.normalize_regional("r02_region_variant_shape_counts", raw),
                [{"variantClass": "DELETION_SHAPE", "recordCount": 1},
                 {"variantClass": "SNV", "recordCount": 2}],
            )

    def test_a_missing_field_on_a_grouped_row_is_an_error_not_a_zero(self):
        """Only the single-row aggregates get the unbound-means-zero rule."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            raw = self._raw(Path(td), [{"variantClass": {"value": "SNV"}}])
            with self.assertRaises(ValueError):
                R.normalize_regional("r02_region_variant_shape_counts", raw)


@unittest.skipUnless(R is not None, "regional_runner must import")
class QueryTemplateTests(VerboseTestCase):
    def test_every_question_has_a_template(self):
        for query_id in R.REGIONAL_QUERIES:
            with self.subTest(query=query_id):
                self.assertTrue(R.regional_query_path("expanded", query_id).is_file())

    def test_rendering_substitutes_all_three_placeholders(self):
        window = {"chrom": "chr7", "start": 100, "end": 200}
        for query_id in R.REGIONAL_QUERIES:
            with self.subTest(query=query_id):
                text = R.render_query(R.regional_query_path("expanded", query_id), window)
                self.assertNotIn("{{", text)
                self.assertIn('"chr7"', text)
                self.assertIn("100", text)
                self.assertIn("200", text)

    def test_a_contig_name_that_would_break_out_of_the_literal_is_refused(self):
        """Substitution into a SPARQL string is the one injection risk here."""
        for bad in ('1" || (1=1) || "', "a\\b", "a\nb"):
            with self.subTest(chrom=bad):
                with self.assertRaises(ValueError):
                    R.render_query(
                        R.regional_query_path("expanded", "r01_region_record_count"),
                        {"chrom": bad, "start": 1, "end": 2},
                    )

    def test_every_template_restricts_on_both_contig_and_position(self):
        """A template missing a bound would silently answer a different question."""
        for query_id in R.REGIONAL_QUERIES:
            with self.subTest(query=query_id):
                text = R.regional_query_path("expanded", query_id).read_text()
                self.assertIn("{{CHROM}}", text)
                self.assertIn("{{START}}", text)
                self.assertIn("{{END}}", text)


@unittest.skipUnless(R is not None, "regional_runner must import")
class ScanSamplingTests(VerboseTestCase):
    def test_the_scan_arm_is_sampled_evenly_across_window_sizes(self):
        """Its cost does not vary with the window, so it is measured thinly."""
        windows = [{"window_id": f"w{size}_{i:02d}", "size": size}
                   for size in (1_000, 100_000) for i in range(20)]
        sampled = R._sample_scan_windows(windows, 3)
        self.assertEqual(len(sampled), 6)
        self.assertEqual(sum(1 for w in sampled if w["size"] == 1_000), 3)
        self.assertEqual(sum(1 for w in sampled if w["size"] == 100_000), 3)


@unittest.skipUnless(
    R is not None and (shutil.which("bgzip") or shutil.which("bcftools")),
    "bgzip or bcftools is required to build a BGZF file",
)
class IndexPreparationTests(VerboseTestCase):
    def test_building_the_index_reports_its_one_time_cost(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            report = R.prepare_indexed_vcf(SMALL_VCF, Path(td), index_kind="tbi")
            self.assertGreaterEqual(report["bgzipSeconds"], 0.0)
            self.assertGreaterEqual(report["indexSeconds"], 0.0)
            self.assertTrue(Path(report["indexPath"]).is_file())
            self.assertTrue(R.is_bgzf(Path(report["indexedVcf"])))

    def test_a_plain_gzip_file_is_not_mistaken_for_bgzf(self):
        """tabix cannot seek a plain gzip stream; detecting it early is kinder."""
        import gzip
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            plain = Path(td) / "plain.vcf.gz"
            plain.write_bytes(gzip.compress(SMALL_VCF.read_bytes()))
            self.assertFalse(R.is_bgzf(plain))


if __name__ == "__main__":
    unittest.main()
