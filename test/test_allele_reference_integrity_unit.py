"""Every allele a graph points at must also be described in that graph.

Two layers join to ``<record>/allele/<index>``: the structured INFO value items
(Number=A/R/G) and the expanded sample layer's per-call ``vcfc:calledAllele``.
The allele layer used to be minted only for the structured INFO representation,
so ``--info-representation raw --sample-representation expanded`` emitted
``vcfc:calledAllele`` edges whose targets carried no ``vcfc:alleleIndex``,
``vcfc:alleleValue`` or ``vcfc:alleleKind`` -- 1,972 dangling references per
1,000 records in the covering-set benchmark. The SPARQL oracle counts the edges
without following the join, so only the shape layer saw it.

These tests pin the disjunction directly and then check the property it exists
to protect, across all four representation combinations.
"""

import re
import tempfile
import unittest
from pathlib import Path

import vcf_rdfizer
from vcf_rdfizer_vocab import VCFC_NAMESPACE
from test.helpers import VerboseTestCase

RECORDS_HEADER = (
    "SOURCE_FILE\tROW_ID\tCHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\t"
    "INFO\tFORMAT\tS1\n"
)
RECORDS_ROWS = (
    "s.vcf\t1\t1\t100\t.\tA\tG,T\t50\tPASS\tDP=7;AF=0.4,0.6\tGT:DP\t1/2:7\n"
    "s.vcf\t2\t1\t200\t.\tC\tCTT\t30\tPASS\tDP=9\tGT:DP\t0/1:9\n"
)
HEADERS_TSV = (
    "SOURCE_FILE\tHEADER_INDEX\tHEADER_KEY\tHEADER_VALUE\tRAW_LINE\n"
    "s.vcf\t1\tfileformat\tVCFv4.5\tx\n"
    "s.vcf\t2\tcontig\t<ID=1,length=1000>\tx\n"
    "s.vcf\t3\tINFO\t<ID=DP,Number=1,Type=Integer,Description=Depth>\tx\n"
    "s.vcf\t4\tINFO\t<ID=AF,Number=A,Type=Float,Description=Frequency>\tx\n"
    "s.vcf\t5\tFORMAT\t<ID=GT,Number=1,Type=String,Description=Genotype>\tx\n"
    "s.vcf\t6\tFORMAT\t<ID=DP,Number=1,Type=Integer,Description=Depth>\tx\n"
)

TRIPLE = re.compile(r"^<([^>]+)>\s+<([^>]+)>\s+(.+?)\s*\.$")


def parse_triples(path: Path) -> list[tuple[str, str, str]]:
    """Parse the emitters' N-Triples output into (subject, predicate, object)."""
    triples = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        match = TRIPLE.match(line)
        if match is None:
            continue
        subject, predicate, obj = match.groups()
        if obj.startswith("<") and obj.endswith(">"):
            obj = obj[1:-1]
        triples.append((subject, predicate, obj))
    return triples


def emit_graph(
    tmp_path: Path, info_representation: str, sample_representation: str
) -> list[tuple[str, str, str]]:
    """Run the record-detail and sample emitters the way full mode chains them."""
    records_tsv = tmp_path / "s.records.tsv"
    records_tsv.write_text(RECORDS_HEADER + RECORDS_ROWS, encoding="utf-8")
    headers_tsv = tmp_path / "s.header_lines.tsv"
    headers_tsv.write_text(HEADERS_TSV, encoding="utf-8")
    rdf_path = tmp_path / f"{info_representation}-{sample_representation}.nt"
    rdf_path.write_text("", encoding="utf-8")

    if sample_representation == "expanded":
        vcf_rdfizer.append_expanded_sample_rdf(
            records_tsv, rdf_path, headers_tsv, progress_interval_records=0
        )
    else:
        vcf_rdfizer.append_condensed_sample_rdf(
            records_tsv, rdf_path, headers_tsv, progress_interval_records=0
        )
    vcf_rdfizer.emit_record_detail(
        info_representation,
        records_tsv=records_tsv,
        header_lines_tsv=headers_tsv,
        rdf_path=rdf_path,
        sample_representation=sample_representation,
    )
    return parse_triples(rdf_path)


COMBINATIONS = [
    (info, sample)
    for info in ("structured", "raw")
    for sample in ("expanded", "condensed")
]


class AlleleLayerRequiredTest(VerboseTestCase):
    """The decision is a disjunction over both representation axes."""

    def test_either_consumer_alone_requires_the_allele_layer(self):
        self.assertTrue(vcf_rdfizer.allele_layer_required("structured", "condensed"))
        self.assertTrue(vcf_rdfizer.allele_layer_required("raw", "expanded"))
        self.assertTrue(vcf_rdfizer.allele_layer_required("structured", "expanded"))

    def test_no_consumer_leaves_the_allele_layer_out(self):
        """Condensed samples carry no calledAllele, so raw INFO needs no alleles."""
        self.assertFalse(vcf_rdfizer.allele_layer_required("raw", "condensed"))

    def test_unknown_representations_are_rejected(self):
        with self.assertRaises(ValueError):
            vcf_rdfizer.allele_layer_required("nested", "expanded")
        with self.assertRaises(ValueError):
            vcf_rdfizer.allele_layer_required("raw", "flattened")


class AlleleReferenceIntegrityTest(VerboseTestCase):
    """No representation combination may reference an undescribed allele."""

    def described_alleles(self, triples) -> set[str]:
        return {
            subject
            for subject, predicate, _ in triples
            if predicate == f"{VCFC_NAMESPACE}alleleIndex"
        }

    def referenced_alleles(self, triples) -> set[str]:
        return {
            obj
            for _, predicate, obj in triples
            if predicate == f"{VCFC_NAMESPACE}calledAllele"
        }

    def test_every_called_allele_is_described(self):
        for info, sample in COMBINATIONS:
            with self.subTest(info=info, sample=sample):
                with tempfile.TemporaryDirectory() as td:
                    triples = emit_graph(Path(td), info, sample)
                dangling = self.referenced_alleles(triples) - self.described_alleles(triples)
                self.assertEqual(
                    dangling,
                    set(),
                    f"{info}/{sample} references alleles it never describes",
                )

    def test_raw_expanded_still_emits_the_called_alleles(self):
        """The fix must describe the targets, not silence the references."""
        with tempfile.TemporaryDirectory() as td:
            triples = emit_graph(Path(td), "raw", "expanded")
        referenced = self.referenced_alleles(triples)
        # Allele IRIs are record-scoped: row 1 calls 1/2 over REF=A ALT=G,T and
        # row 2 calls 0/1 over REF=C ALT=CTT, so four distinct resources.
        self.assertEqual(len(referenced), 4, sorted(referenced))
        for subject in referenced:
            described = {
                predicate
                for triple_subject, predicate, _ in triples
                if triple_subject == subject
            }
            for term in ("alleleIndex", "alleleValue", "alleleKind"):
                self.assertIn(f"{VCFC_NAMESPACE}{term}", described, f"{subject} {term}")

    def test_raw_expanded_withholds_the_structured_info_layer(self):
        """Fixing the alleles must not smuggle structured INFO into raw mode."""
        with tempfile.TemporaryDirectory() as td:
            triples = emit_graph(Path(td), "raw", "expanded")
        predicates = {predicate for _, predicate, _ in triples}
        self.assertNotIn(f"{VCFC_NAMESPACE}hasInfoFieldValue", predicates)
        self.assertIn(f"{VCFC_NAMESPACE}alleleIndex", predicates)

    def test_raw_condensed_remains_allele_free(self):
        """Nothing joins to an allele there, so the layer stays out."""
        with tempfile.TemporaryDirectory() as td:
            triples = emit_graph(Path(td), "raw", "condensed")
        self.assertEqual(self.described_alleles(triples), set())
        self.assertEqual(self.referenced_alleles(triples), set())


if __name__ == "__main__":
    unittest.main()
