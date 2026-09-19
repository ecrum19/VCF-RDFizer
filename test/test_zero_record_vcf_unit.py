"""A header-only VCF still has a header to represent.

``awkward_no_records`` declares one sample column on its ``#CHROM`` line and
contains zero data records. The graph omitted the ``vcfc:SampleSet`` and
``vcfc:VCFSample`` resources that line declares, and the validation stage
caught it: Q9 reported six missing predicates (``hasGenotypeColumns``,
``hasSample``, ``hasSampleSet``, ``representationProfile``, ``sampleIndex``,
``sampleName``) and an ``rdf:type`` count of 40 against an expected 42; Q10
reported ``SampleSet`` and ``VCFSample`` missing. The run exited non-zero
rather than reporting success on a silently incomplete graph.

The cause was narrow. ``SampleRecordStream`` learns the source file name from
the first data row, so with no rows it stays empty -- and all three emitters
key their file IRI on it and return immediately. Sample columns are declared by
the header, not by the data, so the fallback reads the name from the
header-lines table instead.
"""

import csv
import tempfile
import unittest
from pathlib import Path

import vcf_rdfizer
from test.helpers import VerboseTestCase

RECORDS_HEADER = [
    "SOURCE_FILE", "ROW_ID", "CHROM", "POS", "ID", "REF", "ALT",
    "QUAL", "FILTER", "INFO", "FORMAT",
]


def write_tsvs(directory: Path, *, sample_ids: list[str], records: int,
               source_file: str = "sample.vcf") -> tuple[Path, Path]:
    """A records/header TSV pair, optionally with zero data rows."""
    records_tsv = directory / "sample.records.tsv"
    headers_tsv = directory / "sample.header_lines.tsv"
    with records_tsv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(RECORDS_HEADER + [" ".join(sample_ids) or "SAMPLES"])
        for index in range(records):
            writer.writerow([
                source_file, str(index + 1), "20", str(100 + index), ".",
                "A", "G", "50", "PASS", "DP=30", "GT:DP",
                " ".join("0/1:30" for _ in sample_ids),
            ])
    with headers_tsv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["SOURCE_FILE", "LINE_INDEX", "KEY", "VALUE", "RAW"])
        writer.writerow([source_file, "1", "fileformat", "VCFv4.2",
                         "fileformat=VCFv4.2"])
        writer.writerow([
            source_file, "2", "FORMAT",
            '<ID=GT,Number=1,Type=String,Description="Genotype">',
            'FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
        ])
    return records_tsv, headers_tsv


class SourceFileFallbackTests(VerboseTestCase):
    def test_the_source_file_is_recovered_from_the_header_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            _records, headers = write_tsvs(
                Path(tmp), sample_ids=["SAMPLE_A"], records=0
            )
            self.assertEqual(
                vcf_rdfizer.source_file_from_header_lines(headers), "sample.vcf"
            )

    def test_a_missing_header_table_returns_an_empty_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                vcf_rdfizer.source_file_from_header_lines(Path(tmp) / "gone.tsv"), ""
            )

    def test_a_header_table_with_only_its_column_row_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "h.tsv"
            path.write_text("SOURCE_FILE\tLINE_INDEX\tKEY\tVALUE\tRAW\n",
                            encoding="utf-8")
            self.assertEqual(vcf_rdfizer.source_file_from_header_lines(path), "")


class ZeroRecordEmissionTests(VerboseTestCase):
    def _emit(self, tmp, representation, *, sample_ids, records):
        directory = Path(tmp)
        records_tsv, headers_tsv = write_tsvs(
            directory, sample_ids=sample_ids, records=records
        )
        rdf = directory / "out.nt"
        rdf.write_bytes(b"")
        if representation == "expanded":
            stats = vcf_rdfizer.append_expanded_sample_rdf(
                records_tsv, rdf, headers_tsv, version=None
            )
        else:
            stats = vcf_rdfizer.append_condensed_sample_rdf(
                records_tsv, headers_tsv, rdf
            )
        return stats, rdf.read_text(encoding="utf-8")

    def test_expanded_emits_the_sample_set_for_a_zero_record_vcf(self):
        """The six predicates Q9 reported missing must all come back."""
        with tempfile.TemporaryDirectory() as tmp:
            stats, text = self._emit(tmp, "expanded", sample_ids=["SAMPLE_A"],
                                     records=0)
            self.assertGreater(stats["triples"], 0)
            for term in ("hasSampleSet", "hasSample>", "sampleName",
                         "sampleIndex", "hasGenotypeColumns",
                         "representationProfile"):
                self.assertIn(term, text, term)

    def test_expanded_emits_both_classes_q10_reported_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            _stats, text = self._emit(tmp, "expanded", sample_ids=["SAMPLE_A"],
                                      records=0)
            self.assertIn("#SampleSet", text)
            self.assertIn("#VCFSample", text)

    def test_condensed_emits_the_sample_set_for_a_zero_record_vcf(self):
        with tempfile.TemporaryDirectory() as tmp:
            stats, text = self._emit(tmp, "condensed", sample_ids=["SAMPLE_A"],
                                     records=0)
            self.assertGreater(stats["triples"], 0)
            self.assertIn("#SampleSet", text)
            self.assertIn("#VCFSample", text)

    def test_the_sample_name_is_the_one_the_chrom_line_declared(self):
        with tempfile.TemporaryDirectory() as tmp:
            _stats, text = self._emit(tmp, "expanded", sample_ids=["NA12878"],
                                      records=0)
            self.assertIn('"NA12878"', text)

    def test_several_declared_samples_all_appear(self):
        with tempfile.TemporaryDirectory() as tmp:
            _stats, text = self._emit(
                tmp, "expanded", sample_ids=["NA1", "NA2", "NA3"], records=0
            )
            for name in ("NA1", "NA2", "NA3"):
                self.assertIn(f'"{name}"', text)

    def test_a_sites_only_zero_record_vcf_still_emits_no_sample_set(self):
        """No declared columns means no set to emit; that part was correct."""
        with tempfile.TemporaryDirectory() as tmp:
            _stats, text = self._emit(tmp, "expanded", sample_ids=[], records=0)
            self.assertNotIn("#SampleSet", text)
            self.assertNotIn("#VCFSample", text)

    def test_a_file_with_records_is_unchanged_by_the_fallback(self):
        """The fallback must only fire when the stream has no source file."""
        with tempfile.TemporaryDirectory() as tmp:
            stats, text = self._emit(tmp, "expanded", sample_ids=["SAMPLE_A"],
                                     records=3)
            self.assertGreater(stats["triples"], 8)
            self.assertIn("#SampleSet", text)
            self.assertIn("file://sample.vcf", text)




class MissingTokenConformanceScopeTests(VerboseTestCase):
    """"." is not always a missing value, and the check said it was.

    The published shapes constrain two predicates whose lexical space includes
    a bare dot: ``vcfc:fieldNumber`` (VCF's ``Number=.`` variable-cardinality
    token, an arity declaration) and ``vcfc:genotypeString`` (a fully-missing
    call is literally "." or "./."). Both are conformant and neither is typed
    ``vcfc:Null``, so the check reported every one of them. On the
    100,000-record HG005 benchmark cell it returned 20 such rows.
    """

    QUERY_DIR = (
        Path(vcf_rdfizer.__file__).resolve().parent
        / "src" / "validation" / "queries" / "common"
    )

    def test_both_queries_exclude_the_conformant_predicates(self):
        for name in (
            "preflight_missing_token_conformance.rq",
            "preflight_missing_token_conformance_count.rq",
        ):
            text = (self.QUERY_DIR / name).read_text(encoding="utf-8")
            self.assertIn("vcfc:fieldNumber", text, name)
            self.assertIn("vcfc:genotypeString", text, name)
            self.assertIn("NOT IN", text, name)

    def test_the_sample_and_its_count_filter_identically(self):
        """Different populations would make the count meaningless."""
        def filters(name):
            text = (self.QUERY_DIR / name).read_text(encoding="utf-8")
            return [
                line.strip()
                for line in text.splitlines()
                if line.strip().startswith("FILTER(")
            ]

        self.assertEqual(
            filters("preflight_missing_token_conformance.rq"),
            filters("preflight_missing_token_conformance_count.rq"),
        )

    def test_the_exclusions_match_what_the_bundled_shapes_permit(self):
        """If a shape stops allowing a dot, this exclusion must be revisited."""
        shapes = (
            Path(vcf_rdfizer.__file__).resolve().parent
            / "vcf_rdfizer_data" / "shacl" / "vcf-core-vocabulary.shacl.ttl"
        ).read_text(encoding="utf-8")
        for predicate in ("vcfc:fieldNumber", "vcfc:genotypeString"):
            self.assertIn(predicate, shapes, predicate)
        self.assertIn("|M|[.])$", shapes)


if __name__ == "__main__":
    unittest.main()
