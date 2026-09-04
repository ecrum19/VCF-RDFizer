"""What the semantic validator's comparison layer does and does not detect.

These tests drive the pure normalization/comparison functions directly, so they
run on the host without cyvcf2, Comunica, or Docker. Each one mutates a
graph-derived result and asserts the outcome, which turns the validator's
coverage into something recorded rather than assumed.

The ``test_blind_spot_*`` cases are deliberate: they document real gaps, so a
future change that closes one will fail here and prompt an update instead of
passing unnoticed.
"""

import copy
import gzip
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from test import validation_fixtures as fixtures
from test.helpers import VerboseTestCase

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_logic", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()


def parser_fixture() -> dict:
    """The canonical fixture's oracle.

    Shared with the graph-level mutation harness rather than hand-written here,
    so there is exactly one description of what the fixture VCF contains.
    """
    return copy.deepcopy(fixtures.parser_summary("expanded"))


def sparql_fixture() -> dict:
    """A graph-derived result that agrees with the parser exactly."""
    source = parser_fixture()
    return {key: copy.deepcopy(source[key]) for key in V.CORE_QUERIES}


class ValidationDetectionTests(VerboseTestCase):
    def test_matching_results_pass(self):
        """An agreeing graph and parser produce a PASS with no failed invariant."""
        report = V.compare(parser_fixture(), sparql_fixture())
        self.assertEqual(report["status"], "PASS")
        self.assertTrue(all(q["status"] == "PASS" for q in report["queries"].values()))

    def test_detects_a_dropped_record(self):
        """Losing one record breaks both the row comparison and the totals."""
        sparql = sparql_fixture()
        sparql["q01_record_density_1mb"][0]["recordCount"] -= 1
        report = V.compare(parser_fixture(), sparql)
        self.assertEqual(report["status"], "MISMATCH")
        self.assertEqual(report["queries"]["q01_record_density_1mb"]["status"], "MISMATCH")
        self.assertIn(
            "FAIL",
            [item["status"] for item in report["invariants"]["sparql"]],
        )

    def test_detects_a_misclassified_variant_shape(self):
        """Moving a record between shape classes is caught even though the total holds."""
        sparql = sparql_fixture()
        rows = sparql["q02_variant_shape_counts"]
        self.assertGreaterEqual(len(rows), 2, "fixture needs two shape classes")
        rows[0]["recordCount"] += 1
        rows[1]["recordCount"] -= 1
        report = V.compare(parser_fixture(), sparql)
        self.assertEqual(report["queries"]["q02_variant_shape_counts"]["status"], "MISMATCH")
        # The record total is unchanged, so only the distribution catches this.
        totals = {item["name"]: item["status"] for item in report["invariants"]["sparql"]}
        self.assertEqual(totals["q02_variant_shape_counts_total"], "PASS")

    def test_detects_a_flipped_genotype(self):
        """A genotype decoded into the wrong class changes the q05 distribution."""
        sparql = sparql_fixture()
        # A class the fixture does not already use, so this is a flip rather
        # than a collision with an existing row.
        sparql["q05_sample_genotype_counts"][0]["genotypeClass"] = "OTHER_PLOIDY"
        report = V.compare(parser_fixture(), sparql)
        self.assertEqual(report["queries"]["q05_sample_genotype_counts"]["status"], "MISMATCH")

    def test_detects_a_corrupted_filter_string(self):
        """FILTER is compared by exact lexical value, not only by status class."""
        sparql = sparql_fixture()
        sparql["q04_filter_distribution"][0]["filterLexical"] = "q20"
        report = V.compare(parser_fixture(), sparql)
        self.assertEqual(report["queries"]["q04_filter_distribution"]["status"], "MISMATCH")

    def test_detects_a_transition_transversion_swap(self):
        """Ti/Tv is compared field-by-field, so a swap is not hidden by the sum."""
        sparql = sparql_fixture()
        sparql["q03_titv"]["transitionCount"] = 1
        sparql["q03_titv"]["transversionCount"] = 2
        report = V.compare(parser_fixture(), sparql)
        self.assertEqual(report["queries"]["q03_titv"]["status"], "MISMATCH")

    def test_detects_an_allele_count_error(self):
        """A wrong AC/AN pairing shows up as missing and extra q06 rows."""
        sparql = sparql_fixture()
        sparql["q06_ac_an_distribution"][0]["ac"] = 2
        report = V.compare(parser_fixture(), sparql)
        self.assertEqual(report["queries"]["q06_ac_an_distribution"]["status"], "MISMATCH")

    def test_detects_an_extra_spurious_record(self):
        """A record the VCF does not contain appears as an extra row."""
        sparql = sparql_fixture()
        sparql["q01_record_density_1mb"].append(
            {"chrom": "chrUnplaced", "windowIndex": 0, "recordCount": 1}
        )
        report = V.compare(parser_fixture(), sparql)
        comparison = report["queries"]["q01_record_density_1mb"]
        self.assertEqual(comparison["status"], "MISMATCH")
        self.assertTrue(comparison["extraRows"])

    def test_impossible_allele_counts_fail_the_bounds_invariant(self):
        """AC > AN is rejected by an invariant even without a parser disagreement."""
        payload = parser_fixture()
        payload["q06_ac_an_distribution"] = [{"an": 2, "ac": 5, "siteCount": 1, "af": 2.5}]
        checks = V.invariant_checks(payload, parser_fixture(), check_q06_exact=False)
        self.assertIn("FAIL", [item["status"] for item in checks])

    # -- Recorded blind spots -------------------------------------------------

    def test_permuted_records_are_caught_only_by_the_digest(self):
        """The aggregates still cannot see a permutation; q11 is what catches it.

        Every other core query is a GROUP BY count, so moving a value between
        two records in the same bucket leaves them all identical. Removing the
        digest from the comparison must therefore restore a PASS.
        """
        parser = parser_fixture()
        sparql = sparql_fixture()
        # A permutation changes which digests exist, but no aggregate.
        sparql["q11_record_digest"] = [
            {"bucket": "00", "recordCount": parser["totalRecords"]}
        ]
        report = V.compare(parser, sparql)
        self.assertEqual(report["status"], "MISMATCH")
        self.assertEqual(report["queries"]["q11_record_digest"]["status"], "MISMATCH")
        aggregates_only = {
            name: result for name, result in report["queries"].items()
            if name != "q11_record_digest"
        }
        self.assertTrue(
            all(result["status"] != "MISMATCH" for result in aggregates_only.values()),
            "an aggregate query should not have noticed a permutation",
        )

    def test_core_query_set_is_pinned(self):
        """The query set is part of the contract; changing it changes coverage."""
        self.assertEqual(
            set(V.CORE_QUERIES),
            {
                "q01_record_density_1mb",
                "q02_variant_shape_counts",
                "q03_titv",
                "q04_filter_distribution",
                "q05_sample_genotype_counts",
                "q06_ac_an_distribution",
                "q07_file_metadata",
                "q08_header_line_census",
                "q09_predicate_census",
                "q10_class_census",
                "q11_record_digest",
                "q12_info_value_digest",
                "q13_format_value_digest",
            },
        )

    def test_digest_covers_every_fixed_field(self):
        """ID, QUAL and INFO are only covered through the record digest."""
        query = (V.QUERY_ROOT / "common" / "q11_record_digest.rq").read_text(encoding="utf-8")
        for term in ("vcfr:chrom", "vcfr:pos", "vcfr:recordId", "vcfr:ref",
                     "vcfr:alt", "vcfr:qual", "vcfr:filter", "vcfr:infoRaw"):
            self.assertIn(term, query, f"{term} missing from the record digest")

    def test_runner_and_wrapper_agree_on_template_encoding(self):
        """The digest rebuilds record IRIs, so both encoders must match."""
        import vcf_rdfizer

        for value in ("plain", "a b", "x~y", "\u00e9", "cohort.vcf.gz", "1"):
            self.assertEqual(
                V.rml_uri_component(value),
                vcf_rdfizer._rml_uri_component(value),
                value,
            )

    def test_anomaly_severity_is_measured_by_a_companion_aggregate(self):
        """The LIMIT 100 sample is for diagnosis; an aggregate gives exact severity."""
        for query_id in V.ANOMALY_PREFLIGHT_QUERIES:
            sample = V.query_path(V.QUERY_ROOT / "expanded", query_id).read_text(encoding="utf-8")
            self.assertIn("LIMIT 100", sample)
            exact = V.query_path(V.QUERY_ROOT / "expanded", f"{query_id}_count")
            self.assertTrue(exact.is_file(), f"missing exact-count companion for {query_id}")
            body = "\n".join(
                line for line in exact.read_text(encoding="utf-8").splitlines()
                if not line.lstrip().startswith("#")
            )
            self.assertIn("anomalyCount", body)
            # An exact count must not be capped, or it measures nothing.
            self.assertNotIn("LIMIT", body)


class NormalizationTests(VerboseTestCase):
    def _write_bindings(self, tmp_path: Path, rows: list[dict]) -> Path:
        path = tmp_path / "result.json"
        path.write_text(json.dumps({"results": {"bindings": rows}}), encoding="utf-8")
        return path

    def test_non_integer_counts_are_rejected(self):
        """A count that is not an integer fails rather than being coerced."""
        with tempfile.TemporaryDirectory() as td:
            path = self._write_bindings(
                Path(td),
                [{"variantClass": {"value": "SNV"}, "recordCount": {"value": "3.5"}}],
            )
            with self.assertRaises(ValueError):
                V.normalize("q02_variant_shape_counts", path)

    def test_duplicate_canonical_keys_are_rejected(self):
        """Two rows for the same key would silently hide one; that is an error."""
        with tempfile.TemporaryDirectory() as td:
            path = self._write_bindings(
                Path(td),
                [
                    {"variantClass": {"value": "SNV"}, "recordCount": {"value": "1"}},
                    {"variantClass": {"value": "SNV"}, "recordCount": {"value": "2"}},
                ],
            )
            with self.assertRaises(ValueError):
                V.normalize("q02_variant_shape_counts", path)

    def test_missing_projection_variable_is_rejected(self):
        """An engine that omits a selected variable fails loudly."""
        with tempfile.TemporaryDirectory() as td:
            path = self._write_bindings(Path(td), [{"variantClass": {"value": "SNV"}}])
            with self.assertRaises(ValueError):
                V.normalize("q02_variant_shape_counts", path)

    def test_engine_independent_normalization(self):
        """Both engines' SPARQL Results JSON normalizes to the same rows."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            comunica_style = self._write_bindings(
                tmp_path,
                [{"variantClass": {"type": "literal", "value": "SNV"},
                  "recordCount": {"type": "literal", "value": "3"}}],
            )
            rows = V.normalize("q02_variant_shape_counts", comunica_style)
            qlever_style = tmp_path / "qlever.json"
            qlever_style.write_text(
                json.dumps(
                    {
                        "head": {"vars": ["variantClass", "recordCount"]},
                        "results": {
                            "bindings": [
                                {
                                    "variantClass": {"type": "literal", "value": "SNV"},
                                    "recordCount": {
                                        "type": "literal",
                                        "value": "3",
                                        "datatype": "http://www.w3.org/2001/XMLSchema#int",
                                    },
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(rows, V.normalize("q02_variant_shape_counts", qlever_style))


if __name__ == "__main__":
    unittest.main()


class CensusAndDigestTests(VerboseTestCase):
    """The completeness and identity checks added in Phases 2-5."""

    def test_census_rejects_an_unexpected_predicate(self):
        """A predicate the VCF does not imply is an extra row, not a silent pass."""
        parser = parser_fixture()
        sparql = sparql_fixture()
        sparql["q09_predicate_census"].append(
            {"predicate": "https://example.org/invented", "tripleCount": 1}
        )
        report = V.compare(parser, sparql)
        self.assertEqual(report["queries"]["q09_predicate_census"]["status"], "MISMATCH")
        self.assertTrue(report["queries"]["q09_predicate_census"]["extraRows"])

    def test_census_is_report_only_for_a_custom_mapping(self):
        """A custom mapping changes the inventory by design, so it cannot fail."""
        parser = parser_fixture()
        sparql = sparql_fixture()
        sparql["q09_predicate_census"].append(
            {"predicate": "https://example.org/invented", "tripleCount": 1}
        )
        report = V.compare(parser, sparql, mapping_policy="report-only")
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(
            report["queries"]["q09_predicate_census"]["status"],
            "NOT_APPLICABLE_CUSTOM_MAPPING",
        )

    def test_census_covers_qual_and_structured_info(self):
        """The elements added in Phases 2, 4 and 5 are in the expected inventory."""
        predicates = {row["predicate"] for row in parser_fixture()["q09_predicate_census"]}
        for term in ("qual", "hasInfoValue", "fileDate", "contigId", "filterId", "altId"):
            self.assertIn(f"{V.VCFR}{term}", predicates, term)

    def test_digest_separator_prevents_field_boundary_collisions(self):
        """Shifting a field boundary must not be able to forge a matching digest."""
        self.assertEqual(V.DIGEST_SEPARATOR, chr(0x1F))
        joined_left = V.DIGEST_SEPARATOR.join(["ab", "c"])
        joined_right = V.DIGEST_SEPARATOR.join(["a", "bc"])
        self.assertNotEqual(joined_left, joined_right)

    def test_header_class_map_matches_the_wrapper(self):
        """The oracle and the emitter must agree on every '##' line's subclass."""
        import vcf_rdfizer

        self.assertEqual(V.HEADER_LINE_CLASSES, vcf_rdfizer.HEADER_LINE_CLASSES)

    def test_info_entry_parsers_agree(self):
        """Flag entries and '=' splitting must be identical on both sides."""
        import vcf_rdfizer

        for info in ("AC=1;DB", ".", "", "A=1;B=2,3;C", "KEY=a=b"):
            self.assertEqual(
                V.parse_info_entries(info), vcf_rdfizer.parse_info_entries(info), info
            )

    def test_qual_is_typed_as_the_published_shape_requires(self):
        """The shape demands xsd:decimal or vcfr:Null, never a plain literal."""
        import vcf_rdfizer

        self.assertIn("XMLSchema#decimal", vcf_rdfizer._qual_object("12.5"))
        self.assertIn("vocab#Null", vcf_rdfizer._qual_object("."))
        # A non-numeric QUAL is preserved rather than dropped, and the SHACL
        # layer reports it.
        self.assertEqual(vcf_rdfizer._qual_object("bogus"), '"bogus"')

    def test_file_date_is_typed_when_its_form_allows(self):
        """##fileDate has no mandated format, so typing is conditional."""
        import vcf_rdfizer

        self.assertIn("XMLSchema#date", vcf_rdfizer.file_date_object("20260101"))
        self.assertIn("2026-01-01", vcf_rdfizer.file_date_object("20260101"))
        self.assertEqual(vcf_rdfizer.file_date_object("Jan 2026"), '"Jan 2026"')
        self.assertIsNone(vcf_rdfizer.file_date_object(""))


class ShaclLayerTests(VerboseTestCase):
    """SHACL is an independent structural layer, and an optional one."""

    def test_missing_pyshacl_is_an_execution_failure_not_a_conformance_failure(self):
        """An absent optional dependency must never look like a bad graph."""
        import builtins
        import tempfile
        import unittest.mock

        real_import = builtins.__import__

        def blocked(name, *args, **kwargs):
            if name == "pyshacl":
                raise ImportError("blocked for test")
            return real_import(name, *args, **kwargs)

        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            source = tmp_path / "g.nt"
            source.write_text("<s> <p> <o> .\n", encoding="utf-8")
            shapes = tmp_path / "s.ttl"
            shapes.write_text("", encoding="utf-8")
            with unittest.mock.patch.object(builtins, "__import__", blocked):
                result = V.validate_shacl(source, shapes, tmp_path)
        self.assertEqual(result["status"], "EXECUTION_FAILED")
        self.assertIn("pyshacl", result["error"])
        self.assertNotIn("conforms", result)


class HeaderSourceTests(VerboseTestCase):
    """The oracle reads the VCF's own header text, not htslib's version of it."""

    def test_the_header_block_is_read_verbatim_and_stops_at_the_first_record(self):
        """Every '##' line in order, then '#CHROM', and nothing after it."""
        with tempfile.TemporaryDirectory() as td:
            vcf_path = fixtures.write_vcf(Path(td) / "fixture.vcf")
            lines = V.read_vcf_header_text(vcf_path).splitlines()
        meta = [f"##{key}={value}" for key, value in fixtures.HEADER_LINES]
        self.assertEqual(lines[:len(meta)], meta)
        self.assertEqual(len(lines), len(meta) + 1)
        self.assertTrue(lines[-1].startswith("#CHROM\t"))

    def test_htslibs_injected_declarations_are_not_treated_as_header_lines(self):
        """cyvcf2's raw_header is normalised; the conversion's input is not.

        htslib injects '##FILTER=<ID=PASS,Description="All filters passed">'
        into the header it exposes even when the file never declared it. Taking
        the oracle's header from there makes it expect a FilterDefinition the
        graph could not contain, and every header check fails. Reading the file
        keeps the oracle and the conversion looking at the same bytes.
        """
        injected = '##FILTER=<ID=PASS,Description="All filters passed">'
        with tempfile.TemporaryDirectory() as td:
            vcf_path = fixtures.write_vcf(Path(td) / "fixture.vcf")
            file_lines = {
                line for line in V.read_vcf_header_text(vcf_path).splitlines()
                if line.startswith("##")
            }
            source_lines = {
                line for line in vcf_path.read_text(encoding="utf-8").splitlines()
                if line.startswith("##")
            }
        self.assertEqual(file_lines, source_lines)
        self.assertNotIn(injected, file_lines)

    def test_a_gzipped_vcf_header_reads_the_same_as_a_plain_one(self):
        """Aggregates arrive compressed, so the oracle must handle both."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            vcf_path = fixtures.write_vcf(tmp_path / "fixture.vcf")
            gz_path = tmp_path / "fixture.vcf.gz"
            with gzip.open(gz_path, "wt", encoding="utf-8") as handle:
                handle.write(vcf_path.read_text(encoding="utf-8"))
            self.assertEqual(
                V.read_vcf_header_text(gz_path), V.read_vcf_header_text(vcf_path)
            )


class GraphIntegrityTests(VerboseTestCase):
    """Blank nodes, empty terms, and duplicated statements."""

    def _execution(self, tmp_path: Path, rows: list) -> dict:
        path = tmp_path / "distinct.json"
        path.write_text(json.dumps({"results": {"bindings": rows}}), encoding="utf-8")
        return {"preflight_distinct_triple_count": {"status": "PASS", "rawResult": str(path)}}

    def test_duplicates_are_the_gap_between_parsed_and_distinct(self):
        """A store deduplicates on load, so only the parser sees a repeat."""
        with tempfile.TemporaryDirectory() as td:
            executions = self._execution(
                Path(td), [{"distinctTripleCount": {"value": "100"}}]
            )
            clean = V.duplicate_triple_report(executions, 100)
            self.assertEqual(clean["status"], "PASS")
            self.assertEqual(clean["duplicateTripleCount"], 0)

            duplicated = V.duplicate_triple_report(executions, 137)
            self.assertEqual(duplicated["status"], "FAIL")
            self.assertEqual(duplicated["duplicateTripleCount"], 37)
            self.assertEqual(duplicated["parsedTripleCount"], 137)
            self.assertEqual(duplicated["distinctTripleCount"], 100)

    def test_a_missing_parsed_count_is_not_reported_as_clean(self):
        """An unavailable input must never look like an absence of duplicates."""
        with tempfile.TemporaryDirectory() as td:
            executions = self._execution(
                Path(td), [{"distinctTripleCount": {"value": "100"}}]
            )
            result = V.duplicate_triple_report(executions, None)
        self.assertEqual(result["status"], "NOT_EVALUATED")
        self.assertNotIn("duplicateTripleCount", result)

    def test_a_missing_distinct_query_is_not_reported_as_clean(self):
        result = V.duplicate_triple_report({}, 100)
        self.assertEqual(result["status"], "NOT_EVALUATED")

    def test_not_evaluated_does_not_block_a_run(self):
        """A check that could not run must not be treated as a failure."""
        self.assertIn("preflight_duplicate_triples", V.BLOCKING_PREFLIGHT_QUERIES)
        parser = parser_fixture()
        sparql = sparql_fixture()
        report = V.compare(parser, sparql)
        self.assertEqual(report["status"], "PASS")

    def test_integrity_checks_are_blocking(self):
        """Each of the three is treated as a structural defect, not a mismatch."""
        for name in ("preflight_blank_nodes", "preflight_empty_values",
                     "preflight_duplicate_triples"):
            self.assertIn(name, V.BLOCKING_PREFLIGHT_QUERIES, name)

    def test_blank_node_query_examines_subject_and_object_only(self):
        """A predicate cannot be a blank node in RDF, so it is not tested."""
        query = (V.QUERY_ROOT / "common" / "preflight_blank_nodes.rq").read_text(encoding="utf-8")
        self.assertIn("ISBLANK(?s)", query)
        self.assertIn("ISBLANK(?o)", query)
        self.assertNotIn("ISBLANK(?p)", query)

    def test_empty_value_query_covers_literals_and_iris(self):
        """Both a lost value and a collapsed template substitution are caught."""
        query = (V.QUERY_ROOT / "common" / "preflight_empty_values.rq").read_text(encoding="utf-8")
        self.assertIn("EMPTY_LITERAL", query)
        self.assertIn("EMPTY_IRI", query)
        # Whitespace-only carries no more information than empty.
        self.assertIn("REGEX", query)

    def test_every_anomaly_preflight_has_an_exact_count_companion(self):
        """Severity must be measurable for each new check too."""
        for query_id in V.ANOMALY_PREFLIGHT_QUERIES:
            companion = V.query_path(V.QUERY_ROOT / "expanded", f"{query_id}_count")
            self.assertTrue(companion.is_file(), f"missing companion for {query_id}")
        self.assertIn("preflight_blank_nodes", V.ANOMALY_PREFLIGHT_QUERIES)
        self.assertIn("preflight_empty_values", V.ANOMALY_PREFLIGHT_QUERIES)
