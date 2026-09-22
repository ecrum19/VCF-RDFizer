"""Triple-count growth laws for the expanded and condensed sample profiles.

`docs/sample-representation-guide.md` states closed forms for how many triples
each sample representation emits. Those numbers are cited outside this
repository, so they are pinned here rather than left to drift: every constant
below is asserted against what the real emitters produce over synthetic
records.tsv input, and a change to any per-resource triple count fails a test
that names the resource it belongs to.

The grid is deliberately small. These laws are linear in V, S and F, so a few
well-separated points determine them exactly; the largest case here is 5,000
sample calls, which keeps the file fast enough to run on every push.
"""

import tempfile
import unittest
from pathlib import Path

import vcf_rdfizer
from test.helpers import VerboseTestCase


RECORDS_HEADER = (
    "SOURCE_FILE\tROW_ID\tCHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\t"
    "INFO\tFORMAT\t{samples}\n"
)

#: Triples both profiles emit before any record is read: the representation
#: profile, vcfc:SampleSet and its rdf:type.
FILE_CONSTANT = 3
#: Per sample column: hasSample, rdf:type, sampleName, sampleIndex and the
#: #CHROM line's hasGenotypeColumns.
PER_SAMPLE = 5
#: A declared FORMAT key costs one rdf:type here; its attributes belong to the
#: header emitter, which owns them.
PER_DECLARED_DEFINITION = 1
#: An undeclared key has no header line to own them, so the sample emitter
#: synthesizes rdf:type plus fieldId, fieldNumber, fieldArity, fieldType and
#: fieldDescription.
PER_SYNTHESIZED_DEFINITION = 6
#: vcfc:CohortCallMatrix: hasCallMatrix, rdf:type, appliesToSampleSet.
PER_MATRIX = 3
#: vcfc:FormatValueVector: hasFormatValueVector, rdf:type, declaredBy,
#: valueEncoding, encodedValues.
PER_VECTOR = 5
#: vcfc:SampleCall: hasSampleCall, rdf:type, sampleId, forSample.
PER_SAMPLE_CALL = 4
#: vcfc:FormatFieldValue: hasFormatValue, rdf:type, declaredBy.
PER_FORMAT_VALUE = 3
#: vcfc:Genotype: hasGenotype, rdf:type, genotypeString, ploidy, phasingStatus.
PER_GENOTYPE = 5
#: vcfc:GenotypeAlleleCall: hasAlleleCall, rdf:type, callIndex, isNoCall,
#: phaseIndicator. calledAllele is counted separately, because a no-call and an
#: out-of-range allele index do not get one.
PER_ALLELE_CALL = 5
#: vcfc:FieldValueItem: hasValueItem, rdf:type, valueIndex, itemValue, and the
#: forAllele or forGenotypeIndex link.
PER_VALUE_ITEM = 5


def expected_condensed(v, s, f, *, declared=True):
    """The closed form in the guide, for f single-valued keys per record."""
    definition = PER_DECLARED_DEFINITION if declared else PER_SYNTHESIZED_DEFINITION
    return (
        FILE_CONSTANT
        + PER_SAMPLE * s
        + definition * f
        + v * (PER_MATRIX + PER_VECTOR * f)
    )


def expected_expanded(v, s, f, *, per_key, declared=True):
    """The closed form in the guide. ``per_key`` is the sum of c_k over f keys."""
    definition = PER_DECLARED_DEFINITION if declared else PER_SYNTHESIZED_DEFINITION
    return (
        FILE_CONSTANT
        + PER_SAMPLE * s
        + definition * f
        + v * s * (PER_SAMPLE_CALL + per_key)
    )


def write_inputs(tmp_path, v, s, keys, *, cell=None, alt="G", declare=True):
    """A records.tsv of v records over s samples, plus its header lines.

    records.tsv is twelve columns wide: the twelfth holds every sample for that
    record, space separated, and the header's twelfth cell names the columns
    the same way. Packing them into separate tab columns instead silently
    yields a single-sample file.
    """
    if cell is None:
        cell = ":".join("0/1" if key == "GT" else "7" for key in keys)
    names = " ".join(f"S{index}" for index in range(s))
    rows = [RECORDS_HEADER.format(samples=names)]
    for row_id in range(1, v + 1):
        rows.append(
            f"s.vcf\t{row_id}\t1\t{100 + row_id}\t.\tA\t{alt}\t50\tPASS\t.\t"
            + ":".join(keys)
            + "\t"
            + " ".join([cell] * s)
            + "\n"
        )
    records_tsv = tmp_path / "s.records.tsv"
    records_tsv.write_text("".join(rows), encoding="utf-8")

    lines = [
        "SOURCE_FILE\tHEADER_INDEX\tHEADER_KEY\tHEADER_VALUE\tRAW_LINE\n",
        "s.vcf\t1\tfileformat\tVCFv4.5\tx\n",
    ]
    if declare:
        for index, key in enumerate(keys, start=2):
            value_type = "String" if key == "GT" else "Integer"
            lines.append(
                f"s.vcf\t{index}\tFORMAT\t<ID={key},Number=1,"
                f"Type={value_type},Description=d>\tx\n"
            )
    header_lines_tsv = tmp_path / "s.header_lines.tsv"
    header_lines_tsv.write_text("".join(lines), encoding="utf-8")
    return records_tsv, header_lines_tsv


def emitted_triples(emitter, tmp_path, name, *args, **kwargs):
    """Run one emitter into a fresh file and return its triple count."""
    rdf_path = tmp_path / f"{name}.nt"
    rdf_path.write_text("", encoding="utf-8")
    emitter(*args, rdf_path=rdf_path, progress_interval_records=0, **kwargs)
    lines = [line for line in rdf_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return lines


class RepresentationGrowthTests(VerboseTestCase):
    """The documented growth laws, measured rather than asserted."""

    #: Well separated in every dimension, so no coefficient can hide.
    GRID = ((1, 1, 1), (1, 2, 1), (2, 1, 1), (1, 1, 2), (2, 3, 2),
            (3, 5, 4), (10, 10, 3), (5, 100, 3), (50, 20, 5), (100, 50, 3))

    def _counts(self, v, s, f, *, with_gt):
        keys = (["GT"] if with_gt else []) + [
            f"K{index}" for index in range(f - (1 if with_gt else 0))
        ]
        self.assertEqual(len(keys), f)
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            records_tsv, header_lines_tsv = write_inputs(tmp_path, v, s, keys)
            expanded = emitted_triples(
                vcf_rdfizer.append_expanded_sample_rdf, tmp_path, "expanded",
                records_tsv, header_lines_tsv=header_lines_tsv,
            )
            condensed = emitted_triples(
                vcf_rdfizer.append_condensed_sample_rdf, tmp_path, "condensed",
                records_tsv, header_lines_tsv,
            )
        for label, lines in (("expanded", expanded), ("condensed", condensed)):
            self.assertEqual(
                len(lines), len(set(lines)),
                f"{label} emitted a duplicate triple at V={v} S={s} F={f}",
            )
        return len(expanded), len(condensed)

    def test_condensed_growth_matches_the_documented_closed_form(self):
        """Condensed: 3 + 5S + F + V(3 + 5F), for single-valued keys."""
        for v, s, f in self.GRID:
            with self.subTest(V=v, S=s, F=f):
                _, condensed = self._counts(v, s, f, with_gt=False)
                self.assertEqual(condensed, expected_condensed(v, s, f))

    def test_expanded_growth_matches_the_documented_closed_form(self):
        """Expanded: 3 + 5S + F + V*S(4 + 5F), for single-valued typed keys."""
        for v, s, f in self.GRID:
            with self.subTest(V=v, S=s, F=f):
                expanded, _ = self._counts(v, s, f, with_gt=False)
                # Each key: 3 structural + fieldValue + the typed companion an
                # Integer Number=1 cell earns.
                per_key = f * (PER_FORMAT_VALUE + 1 + 1)
                self.assertEqual(expanded, expected_expanded(v, s, f, per_key=per_key))

    def test_the_genotype_layer_is_expanded_only(self):
        """A condensed graph does not change when GT is among the FORMAT keys.

        This is the property that makes the condensed side independent of
        ploidy: the values stay inside the vector rather than becoming
        per-sample resources.
        """
        for v, s, f in self.GRID:
            with self.subTest(V=v, S=s, F=f):
                _, without_gt = self._counts(v, s, f, with_gt=False)
                _, with_gt = self._counts(v, s, f, with_gt=True)
                self.assertEqual(with_gt, without_gt)

    def test_a_diploid_gt_adds_the_documented_genotype_layer(self):
        """GT costs 4 + (5 + 5p + r) per sample call; 21 for a resolvable diploid."""
        for v, s, f in self.GRID:
            with self.subTest(V=v, S=s, F=f):
                expanded, _ = self._counts(v, s, f, with_gt=True)
                # GT is a String, so it earns no typed companion; the other
                # keys are Integer Number=1 and do.
                gt_key = PER_FORMAT_VALUE + 1 + (PER_GENOTYPE + 2 * PER_ALLELE_CALL + 2)
                per_key = gt_key + (f - 1) * (PER_FORMAT_VALUE + 1 + 1)
                self.assertEqual(expanded, expected_expanded(v, s, f, per_key=per_key))

    def test_the_genotype_layer_follows_ploidy_and_resolvable_calls(self):
        """G = 5 + 5p + r: r is the calls that resolve to an allele, r <= p."""
        # (GT, ALT, ploidy, resolvable calls)
        cases = (
            ("0", "G", 1, 1),
            ("0/1", "G", 2, 2),
            ("0|1", "G", 2, 2),
            ("0/1/1", "G", 3, 3),
            ("0/1/1/1", "G", 4, 4),
            # A no-call position calls no allele.
            ("./.", "G", 2, 0),
            # Allele index 2 with one ALT names an allele the record does not
            # have, so it gets no calledAllele either.
            ("0/2", "G", 2, 1),
            ("0/2", "G,T", 2, 2),
        )
        for genotype, alt, ploidy, resolvable in cases:
            with self.subTest(GT=genotype, ALT=alt):
                with tempfile.TemporaryDirectory() as directory:
                    tmp_path = Path(directory)
                    records_tsv, header_lines_tsv = write_inputs(
                        tmp_path, 1, 1, ["GT"], cell=genotype, alt=alt,
                    )
                    lines = emitted_triples(
                        vcf_rdfizer.append_expanded_sample_rdf, tmp_path, "expanded",
                        records_tsv, header_lines_tsv=header_lines_tsv,
                    )
                layer = PER_GENOTYPE + PER_ALLELE_CALL * ploidy + resolvable
                per_key = PER_FORMAT_VALUE + 1 + layer
                self.assertEqual(
                    len(lines), expected_expanded(1, 1, 1, per_key=per_key)
                )

    def test_a_multi_valued_key_adds_five_triples_per_value_item(self):
        """Number=A/R items are expanded-only: condensed keeps them in the vector."""
        # (Number, cell, ALT, items)
        cases = (("R", "30,12", "G", 2), ("R", "30,12,9", "G,T", 3),
                 ("A", "12,9", "G,T", 2))
        for number, cell, alt, items in cases:
            with self.subTest(Number=number, value=cell):
                with tempfile.TemporaryDirectory() as directory:
                    tmp_path = Path(directory)
                    records_tsv, header_lines_tsv = write_inputs(
                        tmp_path, 1, 1, ["AD"], cell=cell, alt=alt, declare=False,
                    )
                    header_lines_tsv.write_text(
                        "SOURCE_FILE\tHEADER_INDEX\tHEADER_KEY\tHEADER_VALUE\tRAW_LINE\n"
                        "s.vcf\t1\tfileformat\tVCFv4.5\tx\n"
                        f"s.vcf\t2\tFORMAT\t<ID=AD,Number={number},"
                        "Type=Integer,Description=d>\tx\n",
                        encoding="utf-8",
                    )
                    expanded = emitted_triples(
                        vcf_rdfizer.append_expanded_sample_rdf, tmp_path, "expanded",
                        records_tsv, header_lines_tsv=header_lines_tsv,
                    )
                    condensed = emitted_triples(
                        vcf_rdfizer.append_condensed_sample_rdf, tmp_path, "condensed",
                        records_tsv, header_lines_tsv,
                    )
                # A comma in the cell suppresses the typed companion, so the
                # key is 3 structural + fieldValue before its items.
                per_key = PER_FORMAT_VALUE + 1 + PER_VALUE_ITEM * items
                self.assertEqual(
                    len(expanded), expected_expanded(1, 1, 1, per_key=per_key)
                )
                # The vector is one literal however many values it holds.
                self.assertEqual(len(condensed), expected_condensed(1, 1, 1))

    def test_an_undeclared_key_synthesizes_its_own_definition(self):
        """Without a ##FORMAT line the sample emitter owns the attributes."""
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            records_tsv, _ = write_inputs(tmp_path, 1, 1, ["ZZ"], declare=False)
            lines = emitted_triples(
                vcf_rdfizer.append_expanded_sample_rdf, tmp_path, "expanded",
                records_tsv, header_lines_tsv=None,
            )
        # An undeclared key defaults to String, so no typed companion.
        per_key = PER_FORMAT_VALUE + 1
        self.assertEqual(
            len(lines), expected_expanded(1, 1, 1, per_key=per_key, declared=False)
        )

    def test_the_file_level_term_is_identical_in_both_profiles(self):
        """The whole difference is per-record: the shared base cancels.

        This is what lets the guide state the difference as V[S(4 + sum c_k) -
        (3 + 5F)] rather than carrying a constant through it.
        """
        for v, s, f in self.GRID:
            with self.subTest(V=v, S=s, F=f):
                expanded, condensed = self._counts(v, s, f, with_gt=False)
                base = FILE_CONSTANT + PER_SAMPLE * s + PER_DECLARED_DEFINITION * f
                self.assertEqual(
                    expanded - condensed,
                    v * (s * (PER_SAMPLE_CALL + f * (PER_FORMAT_VALUE + 2))
                         - (PER_MATRIX + PER_VECTOR * f)),
                )
                self.assertEqual(condensed - base, v * (PER_MATRIX + PER_VECTOR * f))


if __name__ == "__main__":
    unittest.main()
