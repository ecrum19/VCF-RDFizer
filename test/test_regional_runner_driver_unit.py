"""The regional runner's driver, index preparation and failure paths.

test_regional_runner_unit.py covers agreement between the arms' folding and
classification logic. This module covers what surrounds it: building the
bgzip + index copy the indexed arms seek through, dispatching one timed
execution to the right arm, and the driver end to end -- the files it writes,
the exit status it returns when arms disagree, and how a failing arm or engine
is recorded rather than aborting the run.

The index and bcftools tests use the real binaries. They are present in the
VCF-RDFizer image, which is where these tests are meant to run in full; on a
machine without them those tests skip rather than pass vacuously. The SPARQL
engine plumbing is driven through a stand-in engine, because what is under test
there is the runner's handling of an engine's envelope, not the engine.
"""

from __future__ import annotations

import csv
import gzip
import json
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "validation"))

from test.helpers import VerboseTestCase  # noqa: E402

try:
    import regional_runner as R
except ImportError:  # pragma: no cover - the module must import to test it
    R = None

FIXTURES = Path(__file__).resolve().parent / "test_vcf_files"
SMALL_VCF = FIXTURES / "test-1k.vcf"

cyvcf2_available = R is not None and R.V.VCF is not None
have_bgzip = shutil.which("bgzip") is not None
have_tabix = shutil.which("tabix") is not None
have_bcftools = shutil.which("bcftools") is not None

HEADER = (
    "##fileformat=VCFv4.2\n"
    "##contig=<ID=1,length={length}>\n"
    "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n"
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n"
)


def write_vcf(path: Path, *, length: int = 1_000_000, records: int = 3) -> Path:
    lines = [HEADER.format(length=length)]
    for index in range(records):
        lines.append(f"1\t{100 + index * 10}\t.\tA\tG\t50\tPASS\t.\tGT\t0/1\n")
    path.write_text("".join(lines), encoding="utf-8")
    return path


def run_main(argv: list[str]) -> tuple[int, str]:
    out = StringIO()
    with redirect_stdout(out):
        status = R.main(argv)
    return status, out.getvalue()


# ---------------------------------------------------------------------------
# Index preparation
# ---------------------------------------------------------------------------
@unittest.skipUnless(R is not None, "regional_runner must import")
class BgzfDetectionTests(VerboseTestCase):
    def test_a_missing_file_is_not_bgzf(self):
        self.assertFalse(R.is_bgzf(Path("/nonexistent/input.vcf.gz")))

    def test_a_file_shorter_than_a_gzip_header_is_not_bgzf(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "short.gz"
            path.write_bytes(b"\x1f\x8b\x08")
            self.assertFalse(R.is_bgzf(path))

    def test_plain_text_is_not_bgzf(self):
        self.assertFalse(R.is_bgzf(SMALL_VCF))

    def test_plain_gzip_is_not_bgzf(self):
        """A plain .vcf.gz cannot be seeked, and tabix refuses it."""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "plain.vcf.gz"
            with gzip.open(path, "wb") as handle:
                handle.write(SMALL_VCF.read_bytes())
            self.assertFalse(R.is_bgzf(path))

    @unittest.skipUnless(have_bgzip, "bgzip is required to produce a real BGZF file")
    def test_real_bgzip_output_is_bgzf(self):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "small.vcf.gz"
            R._bgzip(SMALL_VCF, target)
            self.assertTrue(R.is_bgzf(target))


@unittest.skipUnless(R is not None, "regional_runner must import")
class IndexKindTests(VerboseTestCase):
    """A .tbi cannot address a contig longer than 2^29-1 bp; a .csi can."""

    def test_a_contig_within_the_tbi_limit_does_not_need_csi(self):
        with tempfile.TemporaryDirectory() as td:
            path = write_vcf(Path(td) / "short.vcf", length=2 ** 29 - 1)
            self.assertFalse(R._needs_csi(path))

    def test_a_contig_beyond_the_tbi_limit_needs_csi(self):
        with tempfile.TemporaryDirectory() as td:
            path = write_vcf(Path(td) / "long.vcf", length=2 ** 29)
            self.assertTrue(R._needs_csi(path))

    def test_an_unparseable_length_is_ignored_rather_than_fatal(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "odd.vcf"
            path.write_text(HEADER.replace("length={length}", "length=unknown"),
                            encoding="utf-8")
            self.assertFalse(R._needs_csi(path))

    def test_an_unreadable_header_falls_back_to_tbi(self):
        self.assertFalse(R._needs_csi(Path("/nonexistent/input.vcf.gz")))


@unittest.skipUnless(R is not None, "regional_runner must import")
class ToolFallbackTests(VerboseTestCase):
    """What the runner does when a binary it prefers is missing."""

    def test_bgzip_without_bgzip_or_bcftools_is_a_clear_error(self):
        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(R.shutil, "which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "neither bgzip nor bcftools"):
                R._bgzip(SMALL_VCF, Path(td) / "out.vcf.gz")

    def test_indexing_without_tabix_or_bcftools_is_a_clear_error(self):
        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(R.shutil, "which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "neither tabix nor bcftools"):
                R._index(Path(td) / "in.vcf.gz", "tbi")

    def test_an_index_that_was_not_written_is_reported(self):
        """A tool can exit 0 and still leave nothing behind; that must not pass."""
        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(R.shutil, "which", return_value="/usr/bin/tabix"), \
                mock.patch.object(R.subprocess, "run") as run:
            with self.assertRaisesRegex(RuntimeError, "index was not produced"):
                R._index(Path(td) / "in.vcf.gz", "csi")
        self.assertIn("-C", run.call_args.args[0])

    @unittest.skipUnless(have_bcftools and have_bgzip, "bcftools and bgzip are required")
    def test_bcftools_builds_the_index_when_tabix_is_absent(self):
        """The fallback keeps the runner usable on an image without tabix."""
        real_which = shutil.which
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / "small.vcf.gz"
            R._bgzip(write_vcf(Path(td) / "small.vcf"), target)
            with mock.patch.object(
                    R.shutil, "which",
                    side_effect=lambda name: None if name == "tabix" else real_which(name)):
                for kind, suffix in (("tbi", ".tbi"), ("csi", ".csi")):
                    with self.subTest(kind=kind):
                        index = R._index(target, kind)
                        self.assertEqual(index, Path(str(target) + suffix))
                        self.assertGreater(index.stat().st_size, 0)


@unittest.skipUnless(R is not None, "regional_runner must import")
class UnsortedInputTests(VerboseTestCase):
    """An index needs coordinate order; a VCF is not obliged to have it."""

    def error(self, stderr):
        return R.subprocess.CalledProcessError(1, ["tabix"], stderr=stderr)

    def test_both_tools_unsorted_messages_are_recognised(self):
        self.assertTrue(R._is_unsorted_error(self.error(b"[E::hts_idx_push] Chromosome blocks not continuous")))
        self.assertTrue(R._is_unsorted_error(self.error("[E::hts_idx_push] Unsorted positions on sequence #1")))

    def test_any_other_index_failure_is_not_read_as_unsorted(self):
        self.assertFalse(R._is_unsorted_error(self.error(b"[E::hts_open] fail to open file")))
        self.assertFalse(R._is_unsorted_error(self.error(None)))

    def test_an_unrelated_index_failure_is_raised_not_sorted_around(self):
        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(R, "_bgzip", side_effect=lambda s, t: t.write_bytes(b"x")), \
                mock.patch.object(R, "_needs_csi", return_value=False), \
                mock.patch.object(R, "_index", side_effect=self.error(b"fail to open file")), \
                mock.patch.object(R, "_sort") as sort:
            with self.assertRaises(R.subprocess.CalledProcessError):
                R.prepare_indexed_vcf(SMALL_VCF, Path(td) / "indexed")
        sort.assert_not_called()

    def test_sorting_without_bcftools_is_a_clear_error(self):
        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(R.shutil, "which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "not coordinate-sorted"):
                R._sort(SMALL_VCF, Path(td) / "out.vcf.gz", Path(td))


@unittest.skipUnless(R is not None and have_bgzip and have_tabix,
                     "bgzip and tabix are required")
class PrepareIndexedVcfTests(VerboseTestCase):
    """The VCF side's one-time cost, built for real and reported separately."""

    def test_a_sorted_vcf_is_indexed_without_sorting(self):
        with tempfile.TemporaryDirectory() as td:
            report = R.prepare_indexed_vcf(write_vcf(Path(td) / "sorted.vcf"), Path(td) / "indexed")
            self.assertNotIn("sorted", report)
            self.assertEqual(report["sortSeconds"], 0.0)

    @unittest.skipUnless(have_bcftools, "bcftools is required to sort")
    def test_an_unsorted_vcf_is_sorted_before_indexing(self):
        """test-1k interleaves contigs, which tabix refuses to index as it stands."""
        with tempfile.TemporaryDirectory() as td:
            report = R.prepare_indexed_vcf(SMALL_VCF, Path(td) / "indexed")
            self.assertIn("not coordinate-sorted", report["sorted"])
            self.assertGreater(report["sortSeconds"], 0.0)
            self.assertTrue(report["indexedVcf"].endswith(".sorted.vcf.gz"))
            self.assertTrue(Path(report["indexPath"]).is_file())
            self.assertAlmostEqual(
                report["totalSetupSeconds"],
                report["bgzipSeconds"] + report["sortSeconds"] + report["indexSeconds"])
            # Only the sorted copy is kept, so a reader cannot pick up the wrong one.
            self.assertEqual(sorted(p.name for p in (Path(td) / "indexed").glob("*.vcf.gz")),
                             ["test-1k.sorted.vcf.gz"])

    @unittest.skipUnless(have_bcftools, "bcftools is required to sort")
    def test_a_plain_vcf_is_bgzipped_and_indexed(self):
        with tempfile.TemporaryDirectory() as td:
            report = R.prepare_indexed_vcf(SMALL_VCF, Path(td) / "indexed")
            self.assertEqual(report["compression"], "bgzip")
            self.assertEqual(report["indexKind"], "tbi")
            self.assertEqual(report["indexKindReason"], "every contig fits a .tbi")
            self.assertTrue(R.is_bgzf(Path(report["indexedVcf"])))
            self.assertTrue(Path(report["indexPath"]).is_file())
            self.assertGreater(report["indexBytes"], 0)
            self.assertAlmostEqual(report["totalSetupSeconds"],
                                   report["bgzipSeconds"] + report["sortSeconds"]
                                   + report["indexSeconds"])

    def test_an_already_bgzf_input_is_copied_not_recompressed(self):
        """Recompressing would charge the VCF side for work no user repeats."""
        with tempfile.TemporaryDirectory() as td:
            source = Path(td) / "source.vcf.gz"
            R._bgzip(write_vcf(Path(td) / "source.vcf"), source)
            report = R.prepare_indexed_vcf(source, Path(td) / "indexed")
            self.assertEqual(report["compression"], "already-bgzf (copied)")
            self.assertNotIn("sorted", report)
            self.assertEqual(Path(report["indexedVcf"]).read_bytes(), source.read_bytes())

    def test_auto_picks_csi_for_a_contig_a_tbi_cannot_address(self):
        with tempfile.TemporaryDirectory() as td:
            source = write_vcf(Path(td) / "long.vcf", length=2 ** 29 + 10)
            report = R.prepare_indexed_vcf(source, Path(td) / "indexed")
            self.assertEqual(report["indexKind"], "csi")
            self.assertTrue(report["indexPath"].endswith(".csi"))
            self.assertIn("2^29-1", report["indexKindReason"])

    def test_an_explicit_index_kind_is_honoured(self):
        with tempfile.TemporaryDirectory() as td:
            report = R.prepare_indexed_vcf(write_vcf(Path(td) / "sorted.vcf"),
                                           Path(td) / "indexed", index_kind="csi")
            self.assertEqual(report["indexKind"], "csi")
            self.assertNotIn("indexKindReason", report)


# ---------------------------------------------------------------------------
# The indexed arms against a real index
# ---------------------------------------------------------------------------
@unittest.skipUnless(cyvcf2_available and have_bgzip and have_tabix and have_bcftools,
                     "cyvcf2, bgzip, tabix and bcftools are required")
class IndexedArmTests(VerboseTestCase):
    """The seek path must return exactly what the scan path returns."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        report = R.prepare_indexed_vcf(SMALL_VCF, Path(cls._tmp.name) / "indexed")
        cls.indexed = Path(report["indexedVcf"])
        cls.samples = R.vcf_sample_names(SMALL_VCF)
        positions = R.contig_record_positions(SMALL_VCF)
        cls.windows = R.draw_windows(positions, (1_000, 100_000, 1_000_000), 3, R.DEFAULT_SEED)
        R.windows_expected_counts(positions, cls.windows)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_cyvcf2_seek_agrees_with_the_scan_on_every_question(self):
        for query_id in R.REGIONAL_QUERIES:
            scan = R.answer_cyvcf2(SMALL_VCF, query_id, self.windows, self.samples, indexed=False)
            seek = R.answer_cyvcf2(self.indexed, query_id, self.windows, self.samples, indexed=True)
            for window in self.windows:
                with self.subTest(query=query_id, window=window["window_id"]):
                    self.assertEqual(R.canonical(seek[window["window_id"]]),
                                     R.canonical(scan[window["window_id"]]))

    @unittest.skipUnless(have_bcftools, "bcftools is required")
    def test_bcftools_query_agrees_with_the_scan_on_every_question(self):
        for query_id in R.REGIONAL_QUERIES:
            scan = R.answer_cyvcf2(SMALL_VCF, query_id, self.windows, self.samples, indexed=False)
            for window in self.windows:
                with self.subTest(query=query_id, window=window["window_id"]):
                    answer = R.answer_bcftools(self.indexed, query_id, window, self.samples)
                    self.assertEqual(R.canonical(answer),
                                     R.canonical(scan[window["window_id"]]))


# ---------------------------------------------------------------------------
# Dispatch and small helpers
# ---------------------------------------------------------------------------
@unittest.skipUnless(R is not None, "regional_runner must import")
class DispatchTests(VerboseTestCase):
    WINDOW = {"window_id": "w1", "chrom": "1", "start": 1, "end": 100, "size": 100}

    def test_an_unknown_arm_is_refused(self):
        with self.assertRaisesRegex(ValueError, "not a VCF arm"):
            R.timed_vcf_arm("qlever", SMALL_VCF, SMALL_VCF, "r01_region_record_count",
                            self.WINDOW, [])

    def test_each_arm_reads_the_file_it_is_meant_to(self):
        """The scan reads the source; both indexed arms read the indexed copy."""
        source, indexed = Path("source.vcf"), Path("indexed.vcf.gz")
        with mock.patch.object(R, "answer_cyvcf2", return_value={"w1": "a"}) as cy, \
                mock.patch.object(R, "answer_bcftools", return_value="b") as bc:
            self.assertEqual(R.timed_vcf_arm("cyvcf2-scan", source, indexed,
                                             "r01_region_record_count", self.WINDOW, [])[0], "a")
            self.assertEqual(cy.call_args.args[0], source)
            self.assertFalse(cy.call_args.kwargs["indexed"])
            R.timed_vcf_arm("cyvcf2-indexed", source, indexed,
                            "r01_region_record_count", self.WINDOW, [])
            self.assertEqual(cy.call_args.args[0], indexed)
            self.assertTrue(cy.call_args.kwargs["indexed"])
            self.assertEqual(R.timed_vcf_arm("bcftools-indexed", source, indexed,
                                             "r01_region_record_count", self.WINDOW, [])[0], "b")
            self.assertEqual(bc.call_args.args[0], indexed)

    def test_the_timing_covers_only_the_execution(self):
        with mock.patch.object(R, "answer_bcftools", return_value={}):
            _, seconds = R.timed_vcf_arm("bcftools-indexed", SMALL_VCF, SMALL_VCF,
                                         "r01_region_record_count", self.WINDOW, [])
        self.assertGreaterEqual(seconds, 0.0)

    def test_an_empty_window_has_a_well_formed_answer_for_every_question(self):
        self.assertEqual(R._empty_answer("r01_region_record_count"), {"recordCount": 0})
        self.assertEqual(R._empty_answer("r03_region_titv"),
                         {"biallelicSnvCount": 0, "transitionCount": 0, "transversionCount": 0})
        for query_id in ("r02_region_variant_shape_counts", "r04_region_filter_distribution",
                         "r05_region_sample_genotype_counts"):
            self.assertEqual(R._empty_answer(query_id), [])

    def test_a_sites_only_header_has_no_samples(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "sites.vcf"
            path.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n",
                            encoding="utf-8")
            self.assertEqual(R.vcf_sample_names(path), [])

    def test_a_genotype_row_of_the_wrong_width_is_an_error(self):
        """A sample/genotype mismatch must not be silently truncated by zip()."""
        accumulator = R._Accumulator("r05_region_sample_genotype_counts", ["A", "B"])
        variant = mock.Mock(FORMAT=["GT"], genotypes=[[0, 1, False]], CHROM="1", POS=5)
        with self.assertRaisesRegex(ValueError, "length mismatch"):
            accumulator.add_genotypes(variant)

    def test_a_missing_filter_reads_back_as_pass(self):
        """cyvcf2 reports PASS as None; the lexical form has to be restored."""
        self.assertEqual(R._filter_lexical(mock.Mock(FILTER=None)), "PASS")
        self.assertEqual(R._filter_lexical(mock.Mock(FILTER="q10;s50")), "q10;s50")


# ---------------------------------------------------------------------------
# The driver end to end
# ---------------------------------------------------------------------------
def base_argv(results: Path, scratch: Path, arms: str, **extra) -> list[str]:
    argv = [
        "--vcf", str(SMALL_VCF),
        "--arms", arms,
        "--window-sizes", "1000,100000",
        "--windows-per-size", "2",
        "--scan-windows-per-size", "1",
        "--replicates", "1",
        "--results-dir", str(results),
        "--scratch-dir", str(scratch),
        "--dataset-id", "test-1k",
    ]
    for key, value in extra.items():
        argv += [f"--{key.replace('_', '-')}", str(value)]
    return argv


@unittest.skipUnless(cyvcf2_available, "cyvcf2 is required")
class DriverTests(VerboseTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.results = self.root / "results"
        self.scratch = self.root / "scratch"

    def tearDown(self):
        self._tmp.cleanup()

    def read(self, name: str):
        return json.loads((self.results / name).read_text(encoding="utf-8"))

    def test_a_scan_only_run_writes_every_output_and_agrees(self):
        status, _ = run_main(base_argv(self.results, self.scratch, "cyvcf2-scan"))
        self.assertEqual(status, 0)
        for name in ("regional.csv", "regional.json", "windows.json", "mismatches.json"):
            self.assertTrue((self.results / name).is_file(), name)

        report = self.read("regional.json")
        # One scan window per size, five questions, one replicate.
        self.assertEqual(report["executions"], 2 * 5)
        self.assertEqual(report["failures"], 0)
        self.assertEqual(report["disagreements"], 0)
        self.assertEqual(report["windowCount"], 4)
        self.assertEqual(set(report["summary"]["cyvcf2-scan"]), set(R.REGIONAL_QUERIES))
        self.assertNotIn("vcfIndex", report["setup"])  # no indexed arm, no index built

        windows = self.read("windows.json")
        self.assertEqual(windows["seed"], R.DEFAULT_SEED)
        self.assertEqual(len(windows["windows"]), 4)
        self.assertEqual(self.read("mismatches.json")["count"], 0)

        with (self.results / "regional.csv").open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 10)
        self.assertTrue(all(row["agrees_with_reference"] == "True" for row in rows))

    def test_a_disagreeing_arm_fails_the_run_and_is_written_down(self):
        """A speed from arms that disagree is a bug, not a result."""
        with mock.patch.object(R, "timed_vcf_arm", return_value=({"recordCount": -1}, 0.01)):
            status, output = run_main(base_argv(self.results, self.scratch, "cyvcf2-scan",
                                                queries="r01_region_record_count"))
        self.assertEqual(status, 1)
        self.assertIn("no speed comparison", output)
        mismatches = self.read("mismatches.json")
        self.assertEqual(mismatches["count"], 2)
        self.assertEqual(mismatches["mismatches"][0]["observed"], {"recordCount": -1})

    def test_a_failing_arm_is_recorded_without_aborting_the_run(self):
        with mock.patch.object(R, "timed_vcf_arm", side_effect=OSError("disk vanished")):
            status, _ = run_main(base_argv(self.results, self.scratch, "cyvcf2-scan",
                                           queries="r01_region_record_count"))
        # Failures are not disagreements: nothing was compared, so exit 0.
        self.assertEqual(status, 0)
        report = self.read("regional.json")
        self.assertEqual(report["failures"], 2)
        self.assertEqual(report["disagreements"], 0)
        with (self.results / "regional.csv").open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual({row["status"] for row in rows}, {"FAILED"})
        self.assertEqual({row["error"] for row in rows}, {"disk vanished"})

    def test_an_unknown_arm_or_question_is_refused_before_any_work(self):
        with self.assertRaises(SystemExit):
            run_main(base_argv(self.results, self.scratch, "cyvcf2-scan,duckdb"))
        with self.assertRaises(SystemExit):
            run_main(base_argv(self.results, self.scratch, "cyvcf2-scan", queries="r99_nothing"))

    def test_a_sparql_arm_without_a_graph_is_refused(self):
        with self.assertRaisesRegex(SystemExit, "--rdf is required"):
            run_main(base_argv(self.results, self.scratch, "qlever"))

    def test_a_vcf_without_records_is_an_error_exit_not_a_traceback(self):
        path = self.root / "empty.vcf"
        path.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n",
                        encoding="utf-8")
        argv = base_argv(self.results, self.scratch, "cyvcf2-scan")
        argv[argv.index("--vcf") + 1] = str(path)
        from contextlib import redirect_stderr
        err = StringIO()
        with redirect_stderr(err):
            status, _ = run_main(argv)
        self.assertEqual(status, 2)
        self.assertIn("no records", err.getvalue())

    @unittest.skipUnless(have_bgzip and have_tabix and have_bcftools,
                         "bgzip, tabix and bcftools are required")
    def test_all_three_vcf_arms_agree_end_to_end(self):
        status, _ = run_main(base_argv(
            self.results, self.scratch, "cyvcf2-scan,cyvcf2-indexed,bcftools-indexed"))
        self.assertEqual(status, 0)
        report = self.read("regional.json")
        self.assertEqual(report["disagreements"], 0)
        self.assertEqual(report["failures"], 0)
        self.assertIn("vcfIndex", report["setup"])
        # scan: 2 windows; the indexed arms: all 4 windows; five questions each.
        self.assertEqual(report["executions"], (2 + 4 + 4) * 5)
        self.assertEqual(set(report["summary"]),
                         {"cyvcf2-scan", "cyvcf2-indexed", "bcftools-indexed"})


class _StandInEngine:
    """Answers each rendered query from the VCF, so agreement is testable."""

    def __init__(self, results: Path, mode: str):
        self.results = results
        self.mode = mode
        self.setup_seconds = 0.25

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, name: str, query_path: Path) -> dict:
        query_id, window_id, _ = name.split("__")
        if self.mode == "fail":
            return {"status": "FAILED", "wallSeconds": 0.01, "error": "engine said no"}
        raw = query_path.with_suffix(".json")
        if self.mode == "garbage":
            raw.write_text("{}", encoding="utf-8")
        else:
            windows = json.loads((self.results / "windows.json").read_text())["windows"]
            window = next(w for w in windows if w["window_id"] == window_id)
            answer = R.answer_cyvcf2(SMALL_VCF, query_id, [window], [], indexed=False)[window_id]
            raw.write_text(json.dumps({"head": {"vars": list(answer)}, "results": {
                "bindings": [{k: {"type": "literal", "value": str(v)} for k, v in answer.items()}]
            }}), encoding="utf-8")
        return {"status": "PASS", "wallSeconds": 0.02, "rawResult": str(raw)}


@unittest.skipUnless(cyvcf2_available, "cyvcf2 is required")
class EngineArmTests(VerboseTestCase):
    """How the driver handles an engine: agreement, failure, unreadable output."""

    QUERIES = "r01_region_record_count,r03_region_titv"

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.results = self.root / "results"
        self.scratch = self.root / "scratch"
        self.graph = self.root / "graph.nt"
        self.graph.write_text("", encoding="utf-8")

    def tearDown(self):
        self._tmp.cleanup()

    def run_with(self, mode: str, build=None, *, rdf=None, rdf_format="nt"):
        build = build or (lambda *a, **k: _StandInEngine(self.results, mode))
        with mock.patch.object(R.V, "build_engine", side_effect=build):
            status, _ = run_main(base_argv(self.results, self.scratch, "qlever",
                                           queries=self.QUERIES, rdf=rdf or self.graph,
                                           rdf_format=rdf_format))
        report = json.loads((self.results / "regional.json").read_text())
        with (self.results / "regional.csv").open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        return status, report, rows

    def test_an_agreeing_engine_is_timed_on_every_window(self):
        status, report, rows = self.run_with("agree")
        self.assertEqual(status, 0)
        self.assertEqual(report["setup"]["engineSetupSeconds"], {"qlever": 0.25})
        self.assertEqual(len(rows), 4 * 2)  # every window, two questions
        self.assertTrue(all(row["agrees_with_reference"] == "True" for row in rows))

    def test_an_engine_failure_is_recorded_per_execution(self):
        status, report, rows = self.run_with("fail")
        self.assertEqual(status, 0)
        self.assertEqual(report["failures"], 8)
        self.assertEqual({row["error"] for row in rows}, {"engine said no"})

    def test_an_unreadable_result_is_not_mistaken_for_an_answer(self):
        status, report, rows = self.run_with("garbage")
        self.assertEqual({row["status"] for row in rows}, {"UNREADABLE"})
        self.assertEqual(report["disagreements"], 0)

    def test_an_engine_that_cannot_start_is_reported_and_skipped(self):
        def refuse(*args, **kwargs):
            raise RuntimeError("qlever-index not on PATH")

        status, report, rows = self.run_with("agree", build=refuse)
        self.assertEqual(status, 0)
        self.assertEqual(report["setup"]["engineErrors"], {"qlever": "qlever-index not on PATH"})
        self.assertEqual(rows, [])

    def test_any_start_up_failure_is_the_engines_not_the_runs(self):
        """Found on bench-1: the COTTAS builder raised KeyError on an .nt.gz."""
        class Crashes(_StandInEngine):
            def __enter__(self):
                raise KeyError(".gz")

        status, report, rows = self.run_with(
            "agree", build=lambda *a, **k: Crashes(self.results, "agree"))
        self.assertEqual(status, 0)
        self.assertEqual(report["setup"]["engineErrors"], {"qlever": "KeyError: '.gz'"})
        self.assertEqual(rows, [])

    def test_a_packaged_graph_is_decoded_once_before_any_engine_sees_it(self):
        """qlever-index, Comunica and the COTTAS builder all refuse an .nt.gz."""
        packed = self.root / "graph.nt.gz"
        with gzip.open(packed, "wt", encoding="utf-8") as handle:
            handle.write('<urn:s> <urn:p> "o" .\n')
        seen = {}

        def build(name, source, **kwargs):
            seen["source"] = Path(source)
            seen["readable"] = Path(source).read_text(encoding="utf-8")
            seen["options"] = kwargs["options"]
            return _StandInEngine(self.results, "agree")

        status, report, _ = self.run_with("agree", build=build, rdf=packed, rdf_format="nt.gz")
        self.assertEqual(status, 0)
        self.assertEqual(seen["source"].suffix, ".nt")
        self.assertIn("<urn:s>", seen["readable"])
        self.assertTrue(report["setup"]["rdfMaterialization"]["materialized"])
        self.assertGreaterEqual(report["setup"]["rdfMaterialization"]["wallSeconds"], 0.0)
        # The native artifact is still offered, so HDT and COTTAS can use one.
        self.assertEqual(seen["options"]["artifact_path"], str(packed))
        self.assertEqual(seen["options"]["artifact_format"], "nt.gz")

    def test_a_plain_graph_is_used_in_place(self):
        seen = {}

        def build(name, source, **kwargs):
            seen["source"] = Path(source)
            return _StandInEngine(self.results, "agree")

        self.run_with("agree", build=build)
        self.assertEqual(seen["source"], self.graph)


@unittest.skipUnless(R is not None, "regional_runner must import")
class EngineOptionTests(VerboseTestCase):
    def test_the_engines_are_given_the_option_names_they_read(self):
        """The runner's own flag names were silently ignored by the engines."""
        args = R.build_parser().parse_args([
            "--vcf", "x.vcf", "--results-dir", "r", "--dataset-id", "d",
            "--rdf", "g.hdt", "--rdf-format", "hdt",
            "--qlever-memory-gb", "12", "--qlever-port", "7100",
            "--qlever-startup-timeout", "60", "--query-timeout", "30",
        ])
        self.assertEqual(R.engine_options(args), {
            "query_timeout": 30, "memory_gb": 12, "port": 7100, "startup_timeout": 60,
            "artifact_path": "g.hdt", "artifact_format": "hdt",
        })


if __name__ == "__main__":
    unittest.main()
