"""The validator's oracle layer: how a VCF is summarised before SPARQL runs.

These tests drive the pure classification and header-parsing functions that
produce the parser-side expectation every SPARQL result is compared against. A
defect here is invisible in the comparison tests, because both sides of the
comparison are derived from a fixture rather than from these functions - so the
oracle would agree with itself while disagreeing with the VCF.

Everything here runs on the host with no cyvcf2, bcftools, Comunica, or Docker:
each function is fed the plain values those tools would have produced. That is
deliberate - the container-only dependencies are absent in CI, and a test that
skips there measures nothing.

The ``test_known_gap_*`` cases record behaviour that is currently wrong, in the
style the mutation harness uses: the gap is an assertion, so closing it fails
here and prompts an update rather than passing unnoticed.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test.helpers import VerboseTestCase

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_oracle", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()


class FakeVariant:
    """The two attributes the lexical helpers read off a cyvcf2 variant."""

    def __init__(self, alt=None, filters=None, line=""):
        self.ALT = alt
        self.FILTERS = filters
        self._line = line

    def __str__(self) -> str:
        return self._line


class VariantShapeTests(VerboseTestCase):
    def test_every_shape_class_is_reachable(self):
        """Each q02 shape bucket is produced by a representative REF/ALT pair."""
        cases = {
            ("A", "."): "NO_ALT",
            ("A", "G,T"): "MULTIALLELIC",
            ("A", "*"): "SYMBOLIC_OR_BREAKEND",
            ("A", "<DEL>"): "SYMBOLIC_OR_BREAKEND",
            ("A", "A[2:321682["): "SYMBOLIC_OR_BREAKEND",
            ("A", "]2:321682]A"): "SYMBOLIC_OR_BREAKEND",
            ("A", "G"): "SNV",
            ("AT", "GC"): "MNV_OR_EQUAL_LENGTH_SUBSTITUTION",
            ("A", "AT"): "INSERTION_SHAPE",
            ("AT", "A"): "DELETION_SHAPE",
            ("A", "R"): "OTHER",
        }
        for (ref, alt), expected in cases.items():
            with self.subTest(ref=ref, alt=alt):
                self.assertEqual(V.classify_variant_shape(ref, alt), expected)

    def test_classification_is_case_insensitive(self):
        """Lower-case bases classify identically to upper-case ones."""
        self.assertEqual(V.classify_variant_shape("a", "g"), "SNV")
        self.assertEqual(V.classify_variant_shape("at", "a"), "DELETION_SHAPE")

    def test_no_alt_is_decided_before_the_base_alphabet(self):
        """'.' is NO_ALT rather than OTHER, whatever REF contains."""
        self.assertEqual(V.classify_variant_shape("N", "."), "NO_ALT")

    def test_multiallelic_wins_over_the_shape_of_its_members(self):
        """A comma makes the record MULTIALLELIC even when every allele is an SNV."""
        self.assertEqual(V.classify_variant_shape("A", "G,T"), "MULTIALLELIC")

    def test_a_non_iupac_base_is_other_not_a_length_comparison(self):
        """An unexpected alphabet is refused before insertion/deletion sizing."""
        self.assertEqual(V.classify_variant_shape("A", "GX"), "OTHER")
        self.assertEqual(V.classify_variant_shape("XY", "A"), "OTHER")

    def test_an_empty_alt_is_other(self):
        """An empty ALT matches no base pattern, so it is not sized as a deletion."""
        self.assertEqual(V.classify_variant_shape("AT", ""), "OTHER")


class AltLexicalTests(VerboseTestCase):
    def test_an_absent_alt_list_renders_as_the_missing_token(self):
        """No ALT and an empty ALT both render '.', which is what the graph carries."""
        self.assertEqual(V.alt_lexical(FakeVariant(alt=None)), ".")
        self.assertEqual(V.alt_lexical(FakeVariant(alt=[])), ".")

    def test_multiple_alleles_join_on_comma_in_source_order(self):
        """The lexical form is the VCF column, not a normalised set."""
        self.assertEqual(V.alt_lexical(FakeVariant(alt=["G", "T"])), "G,T")

    def test_a_none_member_becomes_the_missing_token(self):
        """A null allele inside the list renders '.' rather than 'None'."""
        self.assertEqual(V.alt_lexical(FakeVariant(alt=[None, "G"])), ".,G")


class FilterLexicalTests(VerboseTestCase):
    def test_filter_status_has_three_buckets(self):
        """PASS, the missing token, and everything else are distinguished."""
        self.assertEqual(V.filter_status("PASS"), "PASS")
        self.assertEqual(V.filter_status("."), "NOT_APPLIED")
        self.assertEqual(V.filter_status("q10"), "FAILED")

    def test_filter_status_is_case_sensitive(self):
        """Only the exact token 'PASS' is a pass; VCF does not define 'pass'."""
        self.assertEqual(V.filter_status("pass"), "FAILED")

    def test_an_empty_filter_is_not_treated_as_a_pass(self):
        """An empty string is a failure bucket, never silently a PASS."""
        self.assertEqual(V.filter_status(""), "FAILED")

    def test_the_filters_list_is_joined_with_semicolons(self):
        """Multiple filters reproduce the VCF column's own separator."""
        self.assertEqual(V.exact_filter_lexical(FakeVariant(filters=["q10", "s50"])), "q10;s50")

    def test_an_empty_filters_list_falls_back_to_the_serialized_record(self):
        """cyvcf2 reports no FILTERS for '.', so column 7 is read verbatim."""
        record = "1\t100\t.\tA\tG\t50\t.\tDP=3\tGT\t0/1\n"
        self.assertEqual(V.exact_filter_lexical(FakeVariant(filters=[], line=record)), ".")

    def test_the_fallback_recovers_a_real_filter_string(self):
        """The fallback reads the same value the FILTERS list would have given."""
        record = "1\t100\t.\tA\tG\t50\tq10;s50\tDP=3\n"
        self.assertEqual(V.exact_filter_lexical(FakeVariant(filters=None, line=record)), "q10;s50")

    def test_a_truncated_record_is_refused_rather_than_guessed(self):
        """Too few columns raises instead of silently reporting a wrong FILTER."""
        with self.assertRaises(ValueError):
            V.exact_filter_lexical(FakeVariant(filters=[], line="1\t100\t.\tA\tG\n"))


class GenotypeClassificationTests(VerboseTestCase):
    def test_the_phase_flag_is_dropped_from_the_allele_tuple(self):
        """cyvcf2 appends a phase boolean, which is not an allele."""
        self.assertEqual(V.genotype_alleles([0, 1, True]), (0, 1))
        self.assertEqual(V.genotype_alleles([0, False]), (0,))

    def test_a_negative_allele_index_becomes_none(self):
        """htslib encodes a missing allele as -1; the oracle records it as unknown."""
        self.assertEqual(V.genotype_alleles([0, -1, False]), (0, None))
        self.assertEqual(V.genotype_alleles([-1, -1, True]), (None, None))

    def test_no_genotype_array_stays_none(self):
        """A record with no GT array yields no allele tuple at all."""
        self.assertIsNone(V.genotype_alleles(None))

    def test_every_genotype_class_is_reachable(self):
        """Each q05 genotype bucket is produced by a representative allele tuple."""
        cases = {
            ((0,), True): "HAPLOID_REF",
            ((1,), True): "HAPLOID_ALT",
            ((0, 0), True): "HOM_REF",
            ((2, 2), True): "HOM_ALT",
            ((0, 1), True): "HET",
            ((0, 1, 1), True): "OTHER_PLOIDY",
        }
        for (alleles, has_gt), expected in cases.items():
            with self.subTest(alleles=alleles):
                self.assertEqual(V.classify_genotype(alleles, has_gt=has_gt), expected)

    def test_a_file_without_a_gt_field_is_distinguished_from_a_missing_call(self):
        """NO_GT_FIELD and MISSING are separate buckets, and must not collapse."""
        self.assertEqual(V.classify_genotype((0, 1), has_gt=False), "NO_GT_FIELD")
        self.assertEqual(V.classify_genotype(None, has_gt=True), "MISSING")

    def test_a_partially_missing_genotype_is_missing_not_haploid(self):
        """'./1' must not be counted as a haploid call by dropping the unknown half."""
        self.assertEqual(V.classify_genotype((None, 1), has_gt=True), "MISSING")

    def test_an_empty_allele_tuple_is_missing(self):
        """No alleles at all is a missing call rather than an unclassified record."""
        self.assertEqual(V.classify_genotype((), has_gt=True), "MISSING")


class InfoDeclaredTypeTests(VerboseTestCase):
    def test_each_declared_info_key_maps_to_its_type(self):
        """The INFO Type drives typed-value counting, so it is read per key."""
        header = "\n".join([
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">',
            '##INFO=<ID=AF,Number=A,Type=Float,Description="a">',
            '##INFO=<ID=SOMATIC,Number=0,Type=Flag,Description="s">',
        ])
        self.assertEqual(
            V.info_declared_types(header),
            {"DP": "Integer", "AF": "Float", "SOMATIC": "Flag"},
        )

    def test_a_description_containing_commas_does_not_split_the_declaration(self):
        """A quoted Description is one field, so Type is still recovered."""
        header = '##INFO=<ID=DP,Number=1,Type=Integer,Description="Total depth, all samples">'
        self.assertEqual(V.info_declared_types(header), {"DP": "Integer"})

    def test_a_declaration_without_a_type_defaults_to_string(self):
        """A malformed declaration is typed rather than dropped."""
        header = '##INFO=<ID=NOTYPE,Number=1,Description="no type">'
        self.assertEqual(V.info_declared_types(header), {"NOTYPE": "String"})

    def test_the_first_declaration_of_a_repeated_key_wins(self):
        """A duplicate INFO key keeps the first Type, matching the awk parser."""
        header = "\n".join([
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="first">',
            '##INFO=<ID=DP,Number=1,Type=String,Description="second">',
        ])
        self.assertEqual(V.info_declared_types(header), {"DP": "Integer"})

    def test_non_info_lines_are_ignored(self):
        """FORMAT and the column line contribute no INFO types."""
        header = "\n".join([
            "##fileformat=VCFv4.3",
            '##FORMAT=<ID=GT,Number=1,Type=String,Description="g">',
            "#CHROM\tPOS\tID",
        ])
        self.assertEqual(V.info_declared_types(header), {})


class HeaderMetadataTests(VerboseTestCase):
    HEADER = "\n".join([
        "##fileformat=VCFv4.3",
        "##fileDate=20260101",
        "##fileDate=20260202",
        "##reference=file:///ref/GRCh38.fa",
        "##source=myCaller",
        "##assembly=GRCh38",
        "##contig=<ID=chr1,length=248956422>",
        "##contig=<ID=chr2,length=242193529>",
        '##ALT=<ID=DEL,Description="Deletion">',
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="g">',
        '##FILTER=<ID=q10,Description="qual">',
        "#CHROM\tPOS\tID",
    ])

    def test_file_metadata_is_lifted_from_its_declarations(self):
        """The four file-level metadata fields are read off the header block."""
        metadata = V.parse_header_metadata(self.HEADER)
        self.assertEqual(metadata["fileFormat"], "VCFv4.3")
        self.assertEqual(metadata["referenceGenome"], "file:///ref/GRCh38.fa")
        self.assertEqual(metadata["sourceSoftware"], "myCaller")

    def test_a_repeated_metadata_declaration_keeps_the_first(self):
        """Two ##fileDate lines resolve to the first, matching the awk parser."""
        self.assertEqual(V.parse_header_metadata(self.HEADER)["fileDate"], "20260101")

    def test_an_undeclared_metadata_field_is_explicitly_absent(self):
        """A missing field carries the absent sentinel, never an empty string."""
        metadata = V.parse_header_metadata("##contig=<ID=c1>\n#CHROM\tPOS")
        self.assertEqual(metadata["fileFormat"], V.METADATA_ABSENT)
        self.assertEqual(metadata["fileDate"], V.METADATA_ABSENT)
        self.assertEqual(metadata["sourceSoftware"], V.METADATA_ABSENT)

    def test_the_column_line_is_excluded_from_every_count(self):
        """'#CHROM' is not a meta-information line and emits no HeaderLine."""
        metadata = V.parse_header_metadata(self.HEADER)
        self.assertEqual(metadata["headerLineCount"], 12)
        self.assertEqual(len(metadata["headerLines"]), 12)
        self.assertNotIn(
            "CHROM", {key for key, _ in metadata["headerLines"]}
        )

    def test_structured_declarations_are_indexed_by_id(self):
        """Contig, ALT, INFO and FORMAT identifiers are collected for the census."""
        metadata = V.parse_header_metadata(self.HEADER)
        self.assertEqual(metadata["contigIds"], ["chr1", "chr2"])
        self.assertEqual(metadata["contigCount"], 2)
        self.assertEqual(metadata["altDeclarationIds"], ["DEL"])
        self.assertEqual(metadata["infoKeyNumbers"], {"DP": "1"})
        self.assertEqual(metadata["formatKeyNumbers"], {"GT": "1"})

    def test_an_assembly_line_is_detected_only_when_it_carries_a_value(self):
        """A bare '##assembly' declares nothing to link a contig to."""
        self.assertTrue(V.parse_header_metadata(self.HEADER)["hasAssemblyLine"])
        self.assertFalse(V.parse_header_metadata("##assembly=\n#CHROM")["hasAssemblyLine"])

    def test_valueless_lines_are_counted_separately_from_all_lines(self):
        """A bare flag line is a header line but carries no value."""
        metadata = V.parse_header_metadata("##fileformat=VCFv4.3\n##bareflag\n#CHROM")
        self.assertEqual(metadata["headerLineCount"], 2)
        self.assertEqual(metadata["headerValueCount"], 1)

    def test_the_header_census_is_grouped_by_key_and_sorted(self):
        """q08 compares against a deterministic, key-sorted census."""
        census = V.parse_header_metadata(self.HEADER)["q08_header_line_census"]
        self.assertEqual(census, sorted(census, key=lambda row: row["headerKey"]))
        self.assertIn({"headerKey": "contig", "lineCount": 2}, census)
        self.assertIn({"headerKey": "fileDate", "lineCount": 2}, census)

    def test_a_structured_declaration_without_an_id_is_skipped(self):
        """An ID-less contig cannot be addressed by IRI, so it is not counted."""
        metadata = V.parse_header_metadata("##contig=<length=10>\n#CHROM")
        self.assertEqual(metadata["contigIds"], [])
        self.assertEqual(metadata["contigCount"], 0)


class EmittedHeaderCounterTests(VerboseTestCase):
    def test_a_sample_declared_twice_yields_one_declaration_resource(self):
        """SampleDeclaration is identity-scoped, so a repeat adds no new resource."""
        counters = V.emitted_header_counters([
            ("SAMPLE", '<ID=S1,Description="first">'),
            ("SAMPLE", '<ID=S1,Description="repeat">'),
        ])
        self.assertEqual(counters["emittedHeaderClasses"]["SampleDeclaration"], 1)
        self.assertEqual(counters["emittedHeaderPredicates"]["declaresSample"], 2)

    def test_a_pedigree_ancestor_declares_the_sample_it_names(self):
        """An ancestor mentioned only in a PEDIGREE line still needs a resource."""
        counters = V.emitted_header_counters([("PEDIGREE", "<ID=S1,Father=S2,Mother=S3>")])
        predicates = counters["emittedHeaderPredicates"]
        self.assertEqual(predicates["pedigreeFather"], 1)
        self.assertEqual(predicates["pedigreeMother"], 1)
        self.assertEqual(predicates["ancestorRole"], 2)
        self.assertEqual(counters["emittedHeaderClasses"]["SampleDeclaration"], 2)

    def test_the_pedigree_id_is_not_counted_as_its_own_ancestor(self):
        """ID names the subject of the pedigree line, not a parent of it."""
        counters = V.emitted_header_counters([("PEDIGREE", "<ID=S1,Father=S2>")])
        self.assertEqual(counters["emittedHeaderPredicates"]["ancestorRole"], 1)

    def test_the_contig_count_resource_is_emitted_once_for_any_number_of_contigs(self):
        """contigCount summarises the file, so it is one statement, not one per contig."""
        counters = V.emitted_header_counters([
            ("contig", "<ID=c1,length=10>"),
            ("contig", "<ID=c2,length=20>"),
        ])
        self.assertEqual(counters["emittedHeaderPredicates"]["contigId"], 2)
        self.assertEqual(counters["emittedHeaderPredicates"]["contigCount"], 1)

    def test_no_contigs_means_no_contig_count_statement(self):
        """An absent contig block emits no summary statement at all."""
        counters = V.emitted_header_counters([("fileformat", "VCFv4.3")])
        self.assertNotIn("contigCount", counters["emittedHeaderPredicates"])

    def test_a_meta_values_list_splits_into_its_members(self):
        """##META Values=[a, b, c] declares three allowed values."""
        self.assertEqual(V._meta_values("[WholeGenome, Exome, Panel]"),
                         ["WholeGenome", "Exome", "Panel"])

    def test_meta_values_tolerates_an_absent_or_unbracketed_list(self):
        """A missing list is empty, and a bare comma list still splits."""
        self.assertEqual(V._meta_values(None), [])
        self.assertEqual(V._meta_values(""), [])
        self.assertEqual(V._meta_values("a,b"), ["a", "b"])

    def test_every_member_of_a_multi_value_meta_list_is_counted(self):
        """A ##META ``Values=[a, b]`` list declares one allowed value per member.

        The commas inside the bracketed list separate members, not attributes.
        Splitting on them used to drop every member after the first - the
        fragments carry no '=', so they were discarded as non-attributes, and the
        surviving value kept its opening bracket.
        """
        counters = V.emitted_header_counters([
            ("META", "<ID=Assay,Type=String,Number=.,Values=[WholeGenome, Exome]>"),
        ])
        self.assertEqual(counters["emittedHeaderPredicates"]["metaAllowedValue"], 2)

    def test_a_single_value_meta_list_is_parsed_correctly(self):
        """The bracket stripping works whenever the list has no internal comma."""
        counters = V.emitted_header_counters([
            ("META", "<ID=Assay,Type=String,Number=.,Values=[WholeGenome]>"),
        ])
        self.assertEqual(counters["emittedHeaderPredicates"]["metaAllowedValue"], 1)

    def test_an_attribute_after_the_values_list_is_not_swallowed(self):
        """The list ends at its closing bracket; later attributes still parse."""
        counters = V.emitted_header_counters([
            ("META", '<ID=Assay,Type=String,Number=.,Values=[a, b],Description="an assay">'),
        ])
        predicates = counters["emittedHeaderPredicates"]
        self.assertEqual(predicates["metaAllowedValue"], 2)
        self.assertEqual(predicates["fieldDescription"], 1)

    def test_the_bracketed_list_does_not_change_the_attribute_count(self):
        """Values is one HeaderAttribute however many members it carries."""
        one = V.emitted_header_counters([
            ("META", "<ID=Assay,Type=String,Number=.,Values=[a]>")])
        many = V.emitted_header_counters([
            ("META", "<ID=Assay,Type=String,Number=.,Values=[a, b, c]>")])
        self.assertEqual(
            one["emittedHeaderClasses"]["HeaderAttribute"],
            many["emittedHeaderClasses"]["HeaderAttribute"],
        )


class ToolVersionTests(VerboseTestCase):
    def test_a_missing_binary_reports_none_rather_than_raising(self):
        """Manifest construction must not fail because an optional tool is absent."""
        self.assertIsNone(V.tool_version(["definitely-not-a-real-binary-xyz", "--version"]))

    def test_the_first_output_line_is_the_version(self):
        """Multi-line version banners are reduced to their first line."""
        completed = mock.Mock(stdout="tool 1.2.3\nbuilt from source\n")
        with mock.patch.object(V.subprocess, "run", return_value=completed):
            self.assertEqual(V.tool_version(["tool", "--version"]), "tool 1.2.3")

    def test_empty_output_reports_none(self):
        """A tool that prints nothing yields no version string."""
        with mock.patch.object(V.subprocess, "run", return_value=mock.Mock(stdout="  \n")):
            self.assertIsNone(V.tool_version(["tool", "--version"]))

    def test_a_labelled_table_row_is_read_by_its_label(self):
        """Comunica prints a markdown table; the version is the labelled cell."""
        banner = "| Package | Version |\n| Comunica Engine | 5.3.0 |\n"
        with mock.patch.object(V.subprocess, "run", return_value=mock.Mock(stdout=banner)):
            self.assertEqual(
                V.tool_version(["comunica", "--version"], table_label="Comunica Engine"),
                "5.3.0",
            )

    def test_a_label_that_never_appears_falls_back_to_the_first_line(self):
        """An unexpected banner shape still records something rather than nothing."""
        with mock.patch.object(V.subprocess, "run", return_value=mock.Mock(stdout="5.3.0\n")):
            self.assertEqual(
                V.tool_version(["comunica", "--version"], table_label="Comunica Engine"),
                "5.3.0",
            )

    def test_a_hanging_tool_is_abandoned_rather_than_blocking_the_run(self):
        """A timeout is absorbed, because a version banner is never load-bearing."""
        with mock.patch.object(
            V.subprocess, "run",
            side_effect=V.subprocess.TimeoutExpired(cmd="tool", timeout=20),
        ):
            self.assertIsNone(V.tool_version(["tool", "--version"]))


class ManifestTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.vcf = self.root / "cohort.vcf"
        self.vcf.write_text("##fileformat=VCFv4.3\n#CHROM\tPOS\n", encoding="utf-8")
        self.rdf = self.root / "cohort.nt"
        self.rdf.write_text("<urn:a> <urn:b> <urn:c> .\n", encoding="utf-8")

    def build(self, rdf_format="nt", materialized=False):
        args = argparse.Namespace(
            vcf=self.vcf, rdf=self.rdf, rdf_format=rdf_format,
            dataset_id="cohort-1", representation="expanded",
        )
        # Version probes shell out to container-only binaries; the manifest's
        # shape is what is under test, not which tools this host happens to have.
        with mock.patch.object(V, "tool_version", return_value=None):
            return V.build_manifest(
                args, V.QUERY_ROOT / "expanded", {"sourceSha256": "deadbeef"},
                engine_description={"engine": "comunica"},
                materialization={"materialized": materialized},
            )

    def test_the_manifest_records_both_inputs_by_digest(self):
        """A manifest must identify exactly which bytes were validated."""
        manifest = self.build()
        self.assertEqual(manifest["sourceVcf"]["sha256"], "deadbeef")
        self.assertEqual(manifest["sourceRdf"]["sha256"], V.sha256_file(self.rdf))
        self.assertEqual(manifest["sourceRdf"]["format"], "nt")

    def test_every_executed_query_is_pinned_by_digest(self):
        """A changed query must be visible in the manifest, not silently rerun."""
        queries = self.build()["queries"]
        self.assertEqual(len(queries), len(set(V.PREFLIGHT_QUERIES + V.CORE_QUERIES)))
        for query_id, entry in queries.items():
            with self.subTest(query=query_id):
                self.assertEqual(len(entry["sha256"]), 64)

    def test_a_plain_ntriples_source_declares_no_decode(self):
        """Reading .nt in place must not claim a temporary decode happened."""
        manifest = self.build(materialized=False)
        self.assertFalse(manifest["temporaryRdf"]["decompressedInsideContainer"])
        self.assertTrue(manifest["temporaryRdf"]["cleanupConfirmed"])

    def test_a_materialized_source_records_the_decode(self):
        """Anything decoded into scratch is declared as such."""
        manifest = self.build(rdf_format="hdt", materialized=True)
        self.assertTrue(manifest["temporaryRdf"]["decompressedInsideContainer"])

    def test_the_legacy_gzip_key_appears_only_for_a_gzip_source(self):
        """sourceRdfGzip is a compatibility alias, not a field of every manifest."""
        self.assertNotIn("sourceRdfGzip", self.build(rdf_format="nt"))
        gzip_manifest = self.build(rdf_format="nt.gz")
        self.assertEqual(gzip_manifest["sourceRdfGzip"], gzip_manifest["sourceRdf"])

    def test_the_manifest_serializes_as_json(self):
        """The manifest is written to disk, so every value must be encodable."""
        json.dumps(self.build())


class ResolvedArgumentTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.vcf = self.root / "cohort.vcf"
        self.vcf.write_text("##fileformat=VCFv4.3\n#CHROM\tPOS\n", encoding="utf-8")
        self.rdf = self.root / "cohort.nt"
        self.rdf.write_text("", encoding="utf-8")

    def base(self, **overrides):
        argv = {
            "--vcf": str(self.vcf), "--rdf": str(self.rdf),
            "--scratch-dir": str(self.root), "--results-dir": str(self.root),
            "--representation": "expanded", "--dataset-id": "cohort-1",
        }
        argv.update(overrides)
        return [item for pair in argv.items() for item in pair]

    def resolve(self, argv):
        return V.resolve_args(V.build_arg_parser(), argv)

    def rejection(self, argv):
        """Return the message argparse exits with, for a rejected argument set."""
        captured = io.StringIO()
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(captured):
            self.resolve(argv)
        return captured.getvalue()

    def test_a_valid_invocation_resolves_paths_and_the_primary_engine(self):
        """The default run resolves to one absolute source and one engine."""
        args = self.resolve(self.base())
        self.assertEqual(args.engines, ["comunica"])
        self.assertEqual(args.engine, "comunica")
        self.assertTrue(args.vcf.is_absolute())
        self.assertTrue(args.rdf.is_absolute())

    def test_the_format_is_inferred_from_the_artifact_suffix(self):
        """A recognised extension means --rdf-format is optional."""
        self.assertEqual(self.resolve(self.base()).rdf_format, "nt")

    def test_an_unrecognised_suffix_names_the_supported_formats(self):
        """An inference failure must tell the operator what to pass instead."""
        artifact = self.root / "cohort.xyz"
        artifact.write_text("", encoding="utf-8")
        message = self.rejection(self.base(**{"--rdf": str(artifact)}))
        self.assertIn("Could not infer an RDF format", message)
        for fmt in V.RDF_FORMATS:
            self.assertIn(fmt, message)

    def test_the_gzip_alias_selects_its_format_and_collapses_into_rdf(self):
        """--rdf-gz is a spelling of --rdf, and implies the gzip format."""
        gzipped = self.root / "cohort.nt.gz"
        gzipped.write_text("", encoding="utf-8")
        argv = self.base()
        argv[argv.index("--rdf")] = "--rdf-gz"
        argv[argv.index(str(self.rdf))] = str(gzipped)
        args = self.resolve(argv)
        self.assertEqual(args.rdf_format, "nt.gz")
        self.assertEqual(args.rdf.name, "cohort.nt.gz")

    def test_an_explicit_format_is_not_overridden_by_the_suffix(self):
        """A deliberate --rdf-format survives a misleading filename."""
        self.assertEqual(self.resolve(self.base(**{"--rdf-format": "nt.gz"})).rdf_format, "nt.gz")

    def test_all_expands_to_every_supported_engine_in_registry_order(self):
        """'all' is a shorthand, and the first engine stays the primary."""
        args = self.resolve(self.base(**{"--engine": "all"}))
        self.assertEqual(args.engines, list(V.SPARQL_ENGINES))
        self.assertEqual(args.engine, V.SPARQL_ENGINES[0])

    def test_an_unknown_engine_is_rejected_with_the_supported_set(self):
        """A typo must name the alternatives rather than failing opaquely."""
        message = self.rejection(self.base(**{"--engine": "comunca"}))
        self.assertIn("unknown SPARQL engine", message)
        self.assertIn("comunica", message)

    def test_a_missing_input_is_named(self):
        """Both inputs are checked before any container work starts."""
        self.assertIn("VCF does not exist", self.rejection(
            self.base(**{"--vcf": str(self.root / "absent.vcf")})))
        self.assertIn("RDF artifact does not exist", self.rejection(
            self.base(**{"--rdf": str(self.root / "absent.nt")})))

    def test_a_missing_scratch_directory_is_named(self):
        """Scratch is a mount point; a missing one is a configuration error."""
        self.assertIn("Scratch directory does not exist", self.rejection(
            self.base(**{"--scratch-dir": str(self.root / "absent")})))

    def test_the_dataset_id_is_restricted_to_iri_safe_characters(self):
        """The dataset id reaches IRIs and filenames, so it is constrained."""
        self.assertIn("--dataset-id may contain only", self.rejection(
            self.base(**{"--dataset-id": "bad id!"})))
        self.assertEqual(self.resolve(self.base(**{"--dataset-id": "a.b_c-1"})).dataset_id, "a.b_c-1")

    def test_every_duration_must_be_positive(self):
        """A zero timeout would abandon a query before it started."""
        for flag in ("--query-timeout", "--qlever-memory-gb", "--qlever-startup-timeout",
                     "--comunica-bind-timeout", "--comunica-warmup-timeout"):
            with self.subTest(flag=flag):
                self.assertIn(f"{flag} must be a positive integer",
                              self.rejection(self.base(**{flag: "0"})))

    def test_the_time_budget_treats_zero_as_no_ceiling(self):
        """Zero is meaningful here, so only a negative budget is refused."""
        self.assertEqual(self.resolve(
            self.base(**{"--validation-time-budget": "0"})).validation_time_budget, 0)
        self.assertIn("must be zero or a positive integer",
                      self.rejection(self.base(**{"--validation-time-budget": "-1"})))

    def test_every_port_must_be_in_range(self):
        """An out-of-range port fails here rather than when the server binds."""
        for flag in ("--qlever-port", "--comunica-port", "--hdt-port"):
            with self.subTest(flag=flag):
                self.assertIn(f"{flag} must be between 1 and 65535",
                              self.rejection(self.base(**{flag: "0"})))
                self.assertIn(f"{flag} must be between 1 and 65535",
                              self.rejection(self.base(**{flag: "70000"})))

    def test_the_three_engine_ports_must_differ(self):
        """Two engines sharing a port would silently query the wrong server."""
        message = self.rejection(self.base(**{"--comunica-port": "7100", "--qlever-port": "7100"}))
        self.assertIn("must all differ", message)

    def test_a_missing_shacl_shapes_file_is_named(self):
        """Requesting conformance against shapes that do not exist is refused."""
        self.assertIn("SHACL shapes file does not exist", self.rejection(
            self.base(**{"--shacl-shapes": str(self.root / "absent.ttl")})))

    def test_supplied_shapes_and_progress_paths_are_resolved(self):
        """Both optional paths become absolute for the container's benefit."""
        shapes = self.root / "shapes.ttl"
        shapes.write_text("", encoding="utf-8")
        args = self.resolve(self.base(**{
            "--shacl-shapes": str(shapes), "--progress-path": str(self.root / "progress.json"),
        }))
        self.assertTrue(args.shacl_shapes.is_absolute())
        self.assertTrue(args.progress_path.is_absolute())

    def test_no_progress_path_stays_none(self):
        """Progress reporting is opt-in and must not be invented."""
        self.assertIsNone(self.resolve(self.base()).progress_path)


if __name__ == "__main__":
    unittest.main()
