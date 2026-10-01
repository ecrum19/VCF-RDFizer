"""The streaming path's fast decision and its parallel writer.

`Evaluation.released` must give `decide`'s answer for every term, and
`stream_view` the same bytes whether it runs in one process or several.
"""

from collections import namedtuple
from contextlib import redirect_stdout
from io import StringIO
import gzip
from pathlib import Path
import tempfile
import unittest

from test.helpers import VerboseTestCase
from vcf_rdfizer_policies.engine import Evaluation, Partition, stream_view
from vcf_rdfizer_policies.graphs import ancestors

Rule = namedtuple("Rule", "kind label")


class Profile:
    def __init__(self, iri_subtree):
        self.iri_subtree = iri_subtree


class StubPartition:
    contains = Partition.contains

    def __init__(self, iri_subtree):
        self.profile = Profile(iri_subtree)


def evaluation(iri_subtree=True):
    return Evaluation(StubPartition(iri_subtree), [
        (Rule("permission", "P1"), frozenset({"file://a.vcf#record/1", "file://a.vcf#record/2"})),
        (Rule("permission", "P2"), frozenset({"file://b.vcf#record/1"})),
        (Rule("prohibition", "X"), frozenset({"file://a.vcf#record/2/call/0"}))])


TERMS = ["file://a.vcf#record/1", "file://a.vcf#record/1/call/0", "file://a.vcf#record/2",
         "file://a.vcf#record/2/call/0", "file://a.vcf#record/2/call/0/x", "file://a.vcf#record/3",
         "file://b.vcf#record/1/info/AF", "file://c.vcf#record/1", "https://example.org/gene"]


class ReleasedTests(VerboseTestCase):
    def test_released_is_decide_for_every_term(self):
        for iri_subtree in (True, False):
            e = evaluation(iri_subtree)
            for term in TERMS:
                with self.subTest(term=term, iri_subtree=iri_subtree):
                    self.assertEqual(e.released(term), e.decide(term)[0])

    def test_deny_wins_beneath_a_permitted_record(self):
        e = evaluation()
        self.assertTrue(e.released("file://a.vcf#record/2"))
        self.assertFalse(e.released("file://a.vcf#record/2/call/0/x"))

    def test_ancestors_walk_to_the_authority(self):
        self.assertEqual(list(ancestors("file://P1.vcf#record/9/allele/0")), [
            "file://P1.vcf#record/9/allele/0", "file://P1.vcf#record/9/allele",
            "file://P1.vcf#record/9", "file://P1.vcf#record", "file://P1.vcf"])
        self.assertEqual(list(ancestors("urn:x")), ["urn:x"])


class StreamViewTests(VerboseTestCase):
    A = ('<file://a.vcf#record/1> <urn:p> "x" .\n'
         '<file://a.vcf#record/1> <urn:p> <file://a.vcf#record/3> .\n'      # object withheld: dropped
         '<file://a.vcf#record/1> <urn:p> <https://example.org/gene> .\n'   # outside the node space
         '<file://a.vcf#record/2/call/0> <urn:p> "y" .\n'                   # prohibited
         '<file://a.vcf#record/1/call/0> <urn:p> "é" .\n')
    B = '# comment\n<file://b.vcf#record/1/info/AF> <urn:p> "0.1"^^<urn:t> .\n<file://c.vcf#record/1> <urn:p> "z" .'

    def run_view(self, workers):
        with tempfile.TemporaryDirectory() as work:
            work = Path(work)
            with gzip.open(work / "a.nt.gz", "wt", encoding="utf-8") as handle:
                handle.write(self.A)
            (work / "b.nt").write_text(self.B, encoding="utf-8")
            counts = stream_view([work / "a.nt.gz", work / "b.nt"], evaluation(), "file://",
                                 work / "view.nt.gz", workers=workers)
            self.assertEqual(sorted(p.name for p in work.iterdir()), ["a.nt.gz", "b.nt", "view.nt.gz"])
            with gzip.open(work / "view.nt.gz", "rb") as handle:
                return counts, handle.read()

    def test_released_lines_in_input_order_byte_for_byte(self):
        counts, data = self.run_view(workers=1)
        lines = self.A.splitlines(keepends=True)
        expected = lines[0] + lines[2] + lines[4] + '<file://b.vcf#record/1/info/AF> <urn:p> "0.1"^^<urn:t> .\n'
        self.assertEqual(data.decode("utf-8"), expected)
        self.assertEqual(counts, (4, 3))

    def test_parallel_workers_write_the_same_file(self):
        self.assertEqual(self.run_view(workers=2), self.run_view(workers=1))


class OracleOutputTests(VerboseTestCase):
    VCF = ("##fileformat=VCFv4.2\n##reference=GRCh38\n"
           "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS\n"
           "chr1\t10\trs1\tA\tG\t50\tPASS\t.\tGT\t0/1\n")

    def test_a_gz_name_writes_the_same_triples_compressed(self):
        import vcf_rdfizer_policy
        with tempfile.TemporaryDirectory() as work:
            work = Path(work)
            (work / "P.vcf").write_text(self.VCF, encoding="utf-8")
            for name in ("oracle.nt", "oracle.nt.gz"):
                with redirect_stdout(StringIO()):
                    self.assertEqual(vcf_rdfizer_policy.main(
                        ["oracle", "--vcf", str(work / "P.vcf"), "-o", str(work / name)]), 0)
            plain = (work / "oracle.nt").read_bytes()
            self.assertTrue(plain)
            self.assertEqual(gzip.open(work / "oracle.nt.gz").read(), plain)


if __name__ == "__main__":
    unittest.main()
