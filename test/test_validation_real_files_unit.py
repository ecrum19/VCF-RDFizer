"""Validation on real files: three gaps a real genome exposed.

Paired validation of 250,000 records of a real GATK genome (NG131FQA1I, 58.2M
triples) reported MISMATCH although every field of every record equals the VCF
text. Two of the causes were the validator's, and a third made the run itself
impossible:

* **The census did not count phase sets** (30,910 from GATK's PS), nor the SV
  and gVCF carriers -- reference blocks, events, confidence intervals, IMPRECISE,
  SVLEN -- so any file using them failed with extra rows.
* **The record digest hashed STR(QUAL)**, which QLever returns canonically
  ("30.1") and other engines lexically ("30.10"): 1,998 records hashed apart.
* **pyshacl loaded the whole graph**, and the 277 MB artifact exhausted a 31 GB
  host. Node-level shapes are now validated a batch of records at a time.
"""

import importlib.util
import os
import sys
import tempfile
import types
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

import vcf_rdfizer
import vcf_rdfizer_vocab as vocab
from test.helpers import VerboseTestCase

RUNNER_PATH = Path(__file__).resolve().parents[1] / "src" / "validation" / "validation_runner.py"
_spec = importlib.util.spec_from_file_location("validation_runner_real_files", RUNNER_PATH)
V = importlib.util.module_from_spec(_spec)
# Registered so the parallel shape workers can unpickle their task function.
sys.modules[_spec.name] = V
_spec.loader.exec_module(V)

VCFC = vocab.VCFC_NAMESPACE
RDF_TYPE = vocab.RDF_TYPE_URI
FALDO = vocab.FALDO_NAMESPACE

try:
    import pyshacl  # noqa: F401
    HAVE_PYSHACL = True
except ImportError:
    HAVE_PYSHACL = False


def _emit(tmp: Path, file_format: str, rows: list[list[str]], headers: list[tuple[str, str]]):
    """Run the expanded-sample and structured-INFO emitters the way full mode does."""
    records = tmp / "s.records.tsv"
    records.write_text(
        "SOURCE_FILE\tROW_ID\tCHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n"
        + "".join(f"s.vcf\t{i}\t" + "\t".join(row) + "\n" for i, row in enumerate(rows, 1)),
        encoding="utf-8",
    )
    lines = [("fileformat", file_format), *headers]
    header_tsv = tmp / "s.header_lines.tsv"
    header_tsv.write_text(
        "SOURCE_FILE\tHEADER_INDEX\tHEADER_KEY\tHEADER_VALUE\tRAW_LINE\n"
        + "".join(f"s.vcf\t{i}\t{key}\t{value}\tx\n" for i, (key, value) in enumerate(lines, 1)),
        encoding="utf-8",
    )
    rdf = tmp / "s.nt"
    rdf.write_text("", encoding="utf-8")
    # The wrapper resolves the version from the header and passes it on; the
    # emitters fall back to the newest version otherwise.
    version, _ = vocab.resolve_vcf_version(file_format)
    vcf_rdfizer.append_expanded_sample_rdf(
        records, rdf, header_tsv, progress_interval_records=0, version=version)
    vcf_rdfizer.emit_record_detail(
        "structured", records_tsv=records, header_lines_tsv=header_tsv, rdf_path=rdf,
        sample_representation="expanded", version=version,
    )
    return rdf


class SvGvcfPhaseSetCensusTests(VerboseTestCase):
    """The oracle counts what the emitters write, term by term, for every family."""

    HEADERS = [
        ("contig", "<ID=chr1,length=10000>"),
        ("INFO", "<ID=SVLEN,Number=A,Type=Integer,Description=x>"),
        ("INFO", "<ID=END,Number=1,Type=Integer,Description=x>"),
        ("INFO", "<ID=CIPOS,Number=.,Type=Integer,Description=x>"),
        ("FORMAT", "<ID=GT,Number=1,Type=String,Description=x>"),
        ("FORMAT", "<ID=PS,Number=1,Type=Integer,Description=x>"),
    ]
    ROWS = [
        # Two SV alleles in one event: the event is one resource with two links.
        ["chr1", "100", ".", "A", "<DEL>,<INS>", "50", "PASS",
         "SVLEN=-100,50;IMPRECISE;CIPOS=-5,5,-3,3;CIEND=-2,2,-1,1;CILEN=-10,10,-4,4;"
         "SVCLAIM=D,J;EVENT=ev1,ev1;EVENTTYPE=DEL,DEL", "GT:PS", "0/1:100"],
        # gVCF reference blocks, alone and beside an explicit ALT.
        ["chr1", "200", ".", "G", "<*>", "0", ".", "END=250", "GT:PS:PSO", "0/0:.:."],
        ["chr1", "400", ".", "T", "C,<NON_REF>", "20", ".", "END=450", "GT", "0/1"],
        # Phase sets: complete, name-only, and missing (no carrier).
        ["chr1", "300", ".", "C", "T", "40", "PASS", "DP=5",
         "GT:PS:PSL:PSO:PSQ", "0|1:300:blockA:2:30"],
        ["chr1", "310", ".", "C", "G", "40", "PASS", "DP=5", "GT:PS:PSL", "0|1:.:blockB"],
        ["chr1", "320", ".", "C", "A", "40", "PASS", "DP=5", "GT:PS", "0|1:."],
        # SV INFO on a record with no ALT: there is no allele to carry it.
        ["chr1", "500", ".", "A", ".", "10", "PASS", "SVLEN=5;IMPRECISE;CIPOS=-1,1", "GT", "0/0"],
    ]
    PREDICATES = {
        *(VCFC + name for name in (
            "isImprecise", "isNovel", "svLength", "svClaim", "endPosition",
            "referenceBlockLength", "isReferenceBlockStart", "inEvent", "eventType",
            "posConfidenceInterval", "endConfidenceInterval", "lenConfidenceInterval",
            "copyNumberConfidenceInterval", "ciLower", "ciUpper", "inPhaseSet",
            "phaseSetId", "phaseSetName", "phaseSetOrdinal", "phaseSetQuality")),
        FALDO + "begin", FALDO + "end",
    }
    CLASSES = {
        *(VCFC + name for name in (
            "ReferenceBlock", "VariantEvent", "ConfidenceInterval", "PhaseSet")),
        FALDO + "InRangePosition",
    }

    def emitted(self, file_format):
        """Per-term counts in the emitted graph, as a set of triples."""
        with tempfile.TemporaryDirectory() as td:
            lines = set(_emit(Path(td), file_format, self.ROWS, self.HEADERS)
                        .read_text(encoding="utf-8").splitlines())
        predicates, classes = Counter(), Counter()
        for line in lines:
            parts = line.split(" ", 2)
            if len(parts) < 3:
                continue
            predicate, obj = parts[1][1:-1], parts[2].rsplit(" .", 1)[0]
            if predicate in self.PREDICATES:
                predicates[predicate] += 1
            if predicate == RDF_TYPE and obj[1:-1] in self.CLASSES:
                classes[obj[1:-1]] += 1
        return predicates, classes

    def counted(self, file_format):
        """The same terms as the oracle counts them from the VCF columns."""
        version, _ = vocab.resolve_vcf_version(file_format)
        out = V.emitted_record_counters(
            [list(row) for row in self.ROWS], ["S1"], version=version,
            contig_ids={"chr1"}, alt_declaration_ids=set(),
            info_numbers={"SVLEN": "A", "END": "1", "CIPOS": "."},
            format_numbers={"GT": "1", "PS": "1"},
        )
        predicates, classes = Counter(), Counter()
        for bucket, target, terms in (
            ("emittedRecordPredicates", predicates, self.PREDICATES),
            ("emittedGenotypePredicates", predicates, self.PREDICATES),
            ("emittedRecordClasses", classes, self.CLASSES),
            ("emittedGenotypeClasses", classes, self.CLASSES),
        ):
            for name, count in out[bucket].items():
                if V._census_iri(name) in terms and count:
                    target[V._census_iri(name)] += count
        return predicates, classes

    def test_oracle_and_emitter_agree_on_every_term(self):
        for file_format in ("VCFv4.2", "VCFv4.5"):
            with self.subTest(version=file_format):
                emitted_predicates, emitted_classes = self.emitted(file_format)
                counted_predicates, counted_classes = self.counted(file_format)
                self.assertEqual(dict(counted_predicates), dict(emitted_predicates))
                self.assertEqual(dict(counted_classes), dict(emitted_classes))

    def test_the_fixture_exercises_every_family(self):
        """A family the fixture never emits would agree vacuously."""
        predicates, classes = self.emitted("VCFv4.5")
        for term in ("PhaseSet", "ReferenceBlock", "VariantEvent", "ConfidenceInterval"):
            self.assertIn(VCFC + term, classes, term)
        self.assertIn(FALDO + "InRangePosition", classes)
        for term in ("isImprecise", "svLength", "svClaim", "phaseSetName", "phaseSetOrdinal"):
            self.assertIn(VCFC + term, predicates, term)

    def test_a_shared_event_is_one_resource(self):
        _predicates, classes = self.counted("VCFv4.5")
        self.assertEqual(classes[VCFC + "VariantEvent"], 1)

    def test_events_need_vcf_4_4(self):
        """Before 4.4 there is no EVENTTYPE, so EVENT stays an ordinary value."""
        _predicates, classes = self.counted("VCFv4.2")
        self.assertNotIn(VCFC + "VariantEvent", classes)

    def test_counter_keys_become_iris(self):
        self.assertEqual(V._census_iri("inPhaseSet"), VCFC + "inPhaseSet")
        self.assertEqual(V._census_iri(FALDO + "begin"), FALDO + "begin")


class QualDigestTests(VerboseTestCase):
    """Q11 compares QUAL values, not the spelling an engine returns."""

    def test_trailing_fractional_zeros_are_dropped(self):
        for value, expected in (
            ("30.10", "30.1"), ("100.0", "100"), ("100.00", "100"), ("0.50", "0.5"),
            ("100", "100"), ("10", "10"), (".", "."), ("30.1", "30.1"), ("1e10", "1e10"),
        ):
            with self.subTest(value=value):
                self.assertEqual(V.digest_qual(value), expected)

    def test_the_query_lands_in_the_oracle_bucket_for_every_spelling(self):
        """The real Q11 text, run by rdflib's engine, against the oracle's hash."""
        from rdflib import Graph

        query = (V.QUERY_ROOT / "common" / "q11_record_digest.rq").read_text(encoding="utf-8")
        record, call = "file://s.vcf#record/1", "file://s.vcf#call/1"
        for qual in ("30.10", "30.1", "100.0", "100", "7"):
            with self.subTest(qual=qual):
                graph = Graph()
                graph.parse(data="\n".join([
                    f"<{record}> <{RDF_TYPE}> <{VCFC}VCFRecord> .",
                    f'<{record}> <{VCFC}chrom> "1" .',
                    f'<{record}> <{VCFC}pos> "5"^^<http://www.w3.org/2001/XMLSchema#integer> .',
                    f'<{record}> <{VCFC}recordId> "." .',
                    f'<{record}> <{VCFC}ref> "A" .',
                    f'<{record}> <{VCFC}alt> "G" .',
                    f"<{record}> <{VCFC}hasCall> <{call}> .",
                    f'<{call}> <{VCFC}qual> "{qual}"^^<http://www.w3.org/2001/XMLSchema#decimal> .',
                    f'<{call}> <{VCFC}filter> "PASS" .',
                    f'<{call}> <{VCFC}infoRaw> "." .',
                ]), format="nt")
                buckets = [str(row.bucket) for row in graph.query(query)]
                expected = V.record_digest_bucket(
                    [record, "1", "5", ".", "A", "G", V.digest_qual(qual), "PASS", "."])
                self.assertEqual(buckets, [expected])


class ShaclBatchTests(VerboseTestCase):
    """Node-level shapes are validated in record batches with the same verdict."""

    GRAPH = "\n".join([
        "<file://s.vcf> <urn:p> <file://s.vcf#header/1> .",
        "<file://s.vcf#samples/S1> <urn:p> \"S1\" .",
        *(f"<file://s.vcf> <{VCFC}hasRecord> <file://s.vcf#record/{n}> ." for n in range(1, 6)),
        *(f"<file://s.vcf#record/{n}> <urn:p> <file://s.vcf#call/{n}> ." for n in range(1, 6)),
        *(f"<file://s.vcf#sample/{n}/S1> <urn:p> <file://s.vcf#samples/S1> ." for n in range(1, 6)),
    ]) + "\n"

    def test_every_triple_goes_to_exactly_one_place(self):
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            source = tmp / "g.nt"
            source.write_text(self.GRAPH, encoding="utf-8")
            context, batches = V.write_shacl_batches(source, tmp, records_per_batch=2)
            parts = [context.read_text().splitlines()] + [b.read_text().splitlines() for b in batches]
        self.assertEqual(len(batches), 3)
        self.assertEqual(sorted(sum(parts, [])), sorted(self.GRAPH.splitlines()))
        self.assertEqual(len(parts[0]), 2, "only the file-level triples are context")
        # hasRecord follows its object: records 1-2 land in the first batch.
        self.assertTrue(any("hasRecord> <file://s.vcf#record/2>" in line for line in parts[1]))
        self.assertFalse(any("#record/3" in line or "#sample/3/" in line for line in parts[1]))

    def test_batching_is_chosen_only_where_it_is_exact_and_needed(self):
        rpb = V.shacl_records_per_batch
        self.assertEqual(rpb(1000, 10_000_000, 2_000_000, True), 200)
        self.assertEqual(rpb(10, 10_000_000, 2_000_000, True), 2)
        self.assertEqual(rpb(10, 100_000_000, 2_000_000, True), 1)
        self.assertIsNone(rpb(1000, 1_000_000, 2_000_000, True), "fits one batch")
        self.assertIsNone(rpb(1000, 10_000_000, 0, True), "0 disables batching")
        self.assertIsNone(rpb(1000, 10_000_000, 2_000_000, False), "SPARQL shapes")
        self.assertIsNone(rpb(None, 10_000_000, 2_000_000, True))
        self.assertIsNone(rpb(1000, None, 2_000_000, True))
        # The real genome at the default: the 117 batches of its end-to-end run.
        self.assertEqual(rpb(250_000, 58_231_176, V.DEFAULT_SHACL_BATCH_TRIPLES, True), 2146)

    def test_buffered_lines_are_appended_not_overwritten(self):
        """Flushing after every line must give the same batches as one flush."""
        def split(flush_lines):
            with tempfile.TemporaryDirectory() as td, \
                    mock.patch.object(V, "SHACL_BATCH_FLUSH_LINES", flush_lines):
                tmp = Path(td)
                source = tmp / "g.nt"
                source.write_text(self.GRAPH, encoding="utf-8")
                context, batches = V.write_shacl_batches(source, tmp, records_per_batch=2)
                return context.read_text(), [b.read_text() for b in batches]

        self.assertEqual(split(1), split(V.SHACL_BATCH_FLUSH_LINES))

    def test_only_sparql_free_shapes_are_node_local(self):
        core, _ = vcf_rdfizer.resolve_default_shacl_shapes(Path(vcf_rdfizer.__file__).parent, "core")
        full, _ = vcf_rdfizer.resolve_default_shacl_shapes(Path(vcf_rdfizer.__file__).parent, "full")
        self.assertTrue(V.shapes_are_node_local(core))
        self.assertFalse(V.shapes_are_node_local(full))
        with tempfile.TemporaryDirectory() as td:
            broken = Path(td) / "broken.ttl"
            broken.write_text("this is not turtle", encoding="utf-8")
            self.assertFalse(V.shapes_are_node_local([broken]))

    @unittest.skipUnless(HAVE_PYSHACL, "pyshacl is in the image, not necessarily on the host")
    def test_batched_and_whole_validation_agree(self):
        """Same verdict and same violations, on a clean graph and a broken one."""
        from test import validation_fixtures as fixtures

        root = Path(vcf_rdfizer.__file__).parent
        shapes, ontology = vcf_rdfizer.resolve_default_shacl_shapes(root, "core")
        clean = fixtures.build_graph("expanded")
        # Drop one record's POS: vcfc:pos is required, so this is a violation.
        broken = "\n".join(
            line for line in clean.splitlines()
            if not (line.startswith("<file://fixture.vcf#record/2>") and f"<{VCFC}pos>" in line)
        ) + "\n"
        for name, graph in (("clean", clean), ("broken", broken)):
            with self.subTest(graph=name), tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                source = tmp / "g.nt"
                source.write_text(graph, encoding="utf-8")
                runs = {}
                for label, rpb, workers in (("whole", None, 1), ("batched", 1, 2)):
                    out = tmp / label
                    out.mkdir()
                    runs[label] = V.validate_shacl(
                        source, shapes, out, ontology, records_per_batch=rpb, workers=workers)
                whole, batched = runs["whole"], runs["batched"]
                self.assertGreater(batched["batches"], 1)
                self.assertEqual(batched["status"], whole["status"])
                self.assertEqual(batched["violationCount"], whole["violationCount"])
                self.assertEqual(sorted(batched["sample"]), sorted(whole["sample"]))
                self.assertEqual(whole["status"], "PASS" if name == "clean" else "FAIL")


def _report_pid(_task):
    """A shape task that only says which process ran it."""
    return True, f"pid {os.getpid()}\n"


def _report(*results):
    """A pyshacl text report holding the given (severity, focus node) results."""
    blocks = "".join(
        "Validation Result in MinCountConstraintComponent "
        "(http://www.w3.org/ns/shacl#MinCountConstraintComponent):\n"
        f"\tSeverity: sh:{severity}\n\tFocus Node: <{node}>\n"
        "\tResult Path: vcfc:pos\n\tMessage: m\n"
        for severity, node in results)
    return f"Validation Report\nConforms: {not results}\nResults ({len(results)}):\n{blocks}"


class ShaclBatchMergeTests(VerboseTestCase):
    """How batch outcomes become one report. The task is stubbed, so no pyshacl is needed."""

    def setUp(self):
        stub = mock.patch.dict(sys.modules, {"pyshacl": types.ModuleType("pyshacl")})
        stub.start()
        self.addCleanup(stub.stop)
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        self.tmp = Path(work.name)
        self.source = self.tmp / "g.nt"
        self.source.write_text(ShaclBatchTests.GRAPH, encoding="utf-8")

    def validate(self, outcomes, source=None):
        with mock.patch.object(V, "_validate_shacl_task", side_effect=outcomes) as task:
            result = V.validate_shacl(source or self.source, [Path("shapes.ttl")], self.tmp,
                                      records_per_batch=2, workers=1)
        return result, task

    def test_a_result_repeated_across_batches_is_reported_once(self):
        """Each batch carries the header, so its warnings recur in every batch."""
        header = _report(("Warning", "file://s.vcf#header/1"))
        result, _task = self.validate([(False, header)] * 3)
        self.assertEqual(result["batches"], 3)
        self.assertEqual(result["advisoryCount"], 1)
        self.assertEqual((result["status"], result["conforms"]), ("PASS", False),
                         "warnings alone do not fail the graph")

    def test_one_violating_batch_fails_the_graph(self):
        bad = _report(("Violation", "file://s.vcf#record/3"))
        result, _task = self.validate([(True, _report()), (False, bad), (True, _report())])
        self.assertEqual((result["status"], result["violationCount"]), ("FAIL", 1))
        self.assertIn("#record/3", result["sample"][0])

    def test_a_batch_that_cannot_run_is_an_execution_failure(self):
        result, _task = self.validate([(True, _report()), RuntimeError("parse failed"), (True, _report())])
        self.assertEqual(result["status"], "EXECUTION_FAILED")
        self.assertIn("parse failed", result["error"])

    def test_a_graph_without_records_is_validated_as_its_context(self):
        header_only = self.tmp / "header.nt"
        header_only.write_text("<file://s.vcf> <urn:p> <file://s.vcf#header/1> .\n", encoding="utf-8")
        result, task = self.validate([(True, _report())], source=header_only)
        self.assertEqual(result["batches"], 1)
        _context, batch, _shapes, _ontology = task.call_args.args[0]
        self.assertIsNone(batch)

    def test_every_batch_gets_a_fresh_process(self):
        """rdflib keeps freed memory, so a reused worker would hold its largest batch."""
        with mock.patch.object(V, "_validate_shacl_task", _report_pid):
            result = V.validate_shacl(self.source, [Path("shapes.ttl")], self.tmp,
                                      records_per_batch=1, workers=2)
        pids = Path(result["report"]).read_text(encoding="utf-8").split()[1::2]
        self.assertEqual((result["batches"], result["workers"]), (5, 2))
        self.assertEqual(len(set(pids)), 5)
        self.assertNotIn(str(os.getpid()), pids)


if __name__ == "__main__":
    unittest.main()
