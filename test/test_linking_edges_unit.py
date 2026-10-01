"""The linking framework's refusals and edge paths, one test per rule.

test_linking_unit.py pins the known answers. This file pins what the
framework refuses, and why: every malformed manifest, input, reference or
service reply must stop the run with a message naming the problem, never link
something plausible. No Docker, no network.
"""

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
import gzip
from io import BytesIO, StringIO
import json
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock
from urllib.error import URLError

try:
    import rdflib
except ModuleNotFoundError:  # pragma: no cover - exercised only without rdflib
    rdflib = None

import vcf_rdfizer_link
from vcf_rdfizer_linking import LinkKey
from vcf_rdfizer_linking.inputs import Record, Source, read_rdf, read_vcf
from vcf_rdfizer_linking.manifest import VCFL, absolute_iri, discover, load_manifest, select
from vcf_rdfizer_linking.reference import IntervalIndex, acquire_reference, check_assembly
from vcf_rdfizer_linking.runner import keys_for, load_resolver, run_linkers
from vcf_rdfizer_linking.session import CachedSession, NetworkPolicy, NoRedirects

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/linking/example.vcf"
LINKERS = ROOT / "vcf_rdfizer_data/linkers"
VCFC = "https://w3id.org/vcf-core/vocab#"
GFF3_SHA = "c343c83fbb27ca6a22079fccab7e385fc3e9c89967b521fd77bd9b0930b87aa3"   # gene-demo's genes.gff3
EMIT = '[ vcfl:subject vcfl:VariantCall ; vcfl:predicate vcfl:sameVariantAs ; vcfl:objectTemplate "https://example.org/{TOKEN}" ]'
GFF3 = f'[ vcfl:url <genes.gff3> ; vcfl:sha256 "{GFF3_SHA}" ; vcfl:assembly "GRCh38" ; vcfl:format vcfl:GFF3 ]'
LIVE = ('; vcfl:endpoint <https://example.org/api> ; vcfl:maxRequestsPerSecond 1 ; vcfl:maxRequestsPerRun 5 ; '
        'vcfl:batchSize 10 ; vcfl:contactEmail "a@example.org"')


def manifest_text(*, id='"t"', join="[ a vcfl:TokenJoin ]", emit=EMIT, extra=""):
    return ("@prefix vcfl: <https://w3id.org/vcf-rdfizer/linking#> .\n"
            f'<#l> a vcfl:Linker ; vcfl:id {id} ; vcfl:version "1.0.0" ; vcfl:title "T" ; '
            f"vcfl:join {join} ; vcfl:emit {emit} {extra} .\n")


def record(chrom="1", pos="10", ref="A", alt="G", *, source="file://a.vcf", row=1, reference="GRCh38"):
    return Record(source, f"{source}#record/{row}", f"{source}#call/{row}", reference, chrom, pos, ref, alt, ".", ".")


class Case(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def linker(self, text, *, resolver=None, gff3=True):
        directory = Path(tempfile.mkdtemp(dir=self.root))
        (directory / "linker.ttl").write_text(text, encoding="utf-8")
        if gff3:
            shutil.copy(LINKERS / "gene-demo" / "genes.gff3", directory)
        if resolver is not None:
            (directory / "resolver.py").write_text(resolver, encoding="utf-8")
        return directory

    def refused(self, pattern, text, **options):
        with self.assertRaisesRegex(ValueError, pattern):
            load_manifest(self.linker(text, **options))


@unittest.skipIf(rdflib is None, "rdflib is required for the linking tests")
class ManifestRefusals(Case):
    def test_the_minimal_manifest_loads(self):
        manifest = load_manifest(self.linker(manifest_text()))
        self.assertEqual((manifest.id, manifest.tier, manifest.strategy), ("t", 1, "token"))

    def test_a_manifest_file_path_reads_its_directory(self):
        directory = self.linker(manifest_text())
        self.assertEqual(load_manifest(directory / "linker.ttl").directory, directory.resolve())

    def test_structure(self):
        cases = {
            "exactly one vcfl:Linker": manifest_text() + "<#m> a vcfl:Linker .\n",
            "Expected one vcfl:title": manifest_text(extra='; vcfl:title "U"'),
            "must be a literal": manifest_text(id="<https://example.org/id>"),
            "must be a resource": manifest_text(join='"token"'),
            "Supported joins": manifest_text(join="[ a vcfl:FooJoin ]"),
            "vcfl:subject must be": manifest_text(emit=EMIT.replace("vcfl:VariantCall", "vcfl:Sample")),
            "splitOn cannot be empty": manifest_text(join='[ a vcfl:TokenJoin ; vcfl:splitOn "" ]'),
        }
        for pattern, text in cases.items():
            with self.subTest(pattern):
                self.refused(pattern, text)

    def test_reference_bundles(self):
        interval = "[ a vcfl:IntervalJoin ]"
        gene = EMIT.replace("sameVariantAs", "overlapsGene").replace("{TOKEN}", "{ID}")
        cases = {
            "GFF3 or vcfl:SequenceMap": GFF3.replace("vcfl:GFF3", "vcfl:BED"),
            "64 lowercase": GFF3.replace(GFF3_SHA, "ABC"),
            "nonempty assembly": GFF3.replace('"GRCh38"', '""'),
            "must be a URL IRI or string": GFF3.replace("<genes.gff3>", "[ ]"),
        }
        for pattern, bundle in cases.items():
            with self.subTest(pattern):
                self.refused(pattern, manifest_text(join=interval, emit=gene, extra=f"; vcfl:reference {bundle}"))
        with self.subTest("token join with a bundle"):
            self.refused("TokenJoin takes no reference", manifest_text(extra=f"; vcfl:reference {GFF3}"))
        with self.subTest("alias digest"):
            aliases = '[ vcfl:url <genes.gff3> ; vcfl:sha256 "nothex" ]'
            self.refused("64 lowercase", manifest_text(join=interval, emit=gene,
                                                       extra=f"; vcfl:reference {GFF3} ; vcfl:contigAliases {aliases}"))

    def test_live_services(self):
        resolver = "def resolve(batch, ctx):\n    return []\n"
        live = manifest_text(emit=EMIT.replace(' ; vcfl:objectTemplate "https://example.org/{TOKEN}"', ""), extra=LIVE)
        self.assertEqual(load_manifest(self.linker(live, resolver=resolver)).tier, 3)
        cases = {
            "omit objectTemplate": manifest_text(extra=LIVE),
            "Invalid network budget": live.replace("vcfl:maxRequestsPerSecond 1", 'vcfl:maxRequestsPerSecond "fast"'),
            "email address": live.replace('"a@example.org"', '"nobody"'),
            "number of seconds": live[:-len(" .\n")] + ' ; vcfl:requestTimeout "soon" .\n',
        }
        for pattern, text in cases.items():
            with self.subTest(pattern):
                self.refused(pattern, text, resolver=resolver)

    def test_iris_need_a_host(self):
        with self.assertRaisesRegex(ValueError, "no host"):
            absolute_iri("https:///path")
        self.assertIn("_LazyNamespace", repr(VCFL))


@unittest.skipIf(rdflib is None, "rdflib is required for the linking tests")
class DiscoveryRefusals(Case):
    def test_a_search_path_must_be_a_directory(self):
        with self.assertRaisesRegex(ValueError, "not a directory"):
            discover([self.root / "missing"])

    def test_an_entry_point_must_return_a_linker_directory(self):
        entry = SimpleNamespace(name="broken", load=lambda: (lambda: self.root))
        with mock.patch("vcf_rdfizer_linking.manifest.importlib.metadata.entry_points", return_value=[entry]):
            with self.assertRaisesRegex(ValueError, "did not return a linker directory"):
                discover()

    def test_selection_needs_known_comma_separated_ids(self):
        for raw, pattern in (("", "comma-separated"), ("spdi,,gene-demo", "comma-separated"), ("nope", "Unknown linker")):
            with self.subTest(raw=raw):
                with self.assertRaisesRegex(ValueError, pattern):
                    select(raw)


class VcfInputs(Case):
    def vcf(self, text):
        path = self.root / "in.vcf"
        path.write_text(text, encoding="utf-8")
        return path

    def test_malformed_vcfs_are_refused(self):
        header = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        cases = {
            "conflicting ##reference": "##reference=GRCh38\n##reference=GRCh37\n" + header,
            "missing the #CHROM header": "##fileformat=VCFv4.3\n1\t1\t.\tA\tG\t.\t.\t.\n",
            "fewer than eight columns": header + "1\t1\t.\tA\tG\n",
        }
        for pattern, text in cases.items():
            with self.subTest(pattern):
                with self.assertRaisesRegex(ValueError, pattern):
                    list(read_vcf(self.vcf(text)))
        with self.assertRaisesRegex(ValueError, "missing the #CHROM header"):
            list(read_vcf(self.vcf("##fileformat=VCFv4.3\n")))

    def test_limit_stops_after_n_records(self):
        rows = list(read_vcf(EXAMPLE, limit=2))
        self.assertEqual((type(rows[0]), sum(isinstance(r, Record) for r in rows)), (Source, 2))


class RdfInputs(Case):
    RECORD = (f'<file://a.vcf> <{VCFC}hasRecord> <file://a.vcf#record/1> .\n'
              f'<file://a.vcf#record/1> <{VCFC}hasCall> <file://a.vcf#call/1> .\n'
              f'<file://a.vcf#call/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{VCFC}VariantCall> .\n'
              f'<file://a.vcf#record/1> <{VCFC}chrom> "1" .\n')

    def nt(self, text, name="in.nt"):
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_other_triples_are_dropped_before_the_store(self):
        genotype = '<file://a.vcf#call/1> <https://example.org/genotype> "0/1" .\n'
        rows = list(read_rdf(self.nt(self.RECORD + genotype)))
        self.assertEqual([r.chrom for r in rows if isinstance(r, Record)], ["1"])

    def test_a_graph_with_no_vcf_triples_never_reaches_the_bulk_loader(self):
        """The empty batch must be refused by us, not by the Rust loader.

        pyoxigraph's bulk_extend rejects an empty batch with "Invalid argument:
        ingestion arg list is empty" on Linux and accepts it on macOS, same
        0.5.11. CI caught this and local development could not, so the contract
        is pinned here rather than left to whichever platform runs the suite:
        an input whose triples are all filtered out is a graph that simply
        carries no VCF vocabulary, and the caller is owed read_store's
        ValueError about it -- not a RuntimeError from the loader.
        """
        import pyoxigraph as ox

        batches = []
        original = ox.Store.bulk_extend

        def spy(self, quads):
            items = list(quads)
            batches.append(len(items))
            return original(self, items)

        with mock.patch.object(ox.Store, "bulk_extend", spy):
            with self.assertRaises(ValueError) as caught:
                list(read_rdf(self.nt('<urn:a> <urn:b> "c" .\n')))

        self.assertIn("No VCFFile/hasRecord", str(caught.exception))
        self.assertNotIn(0, batches,
                         "bulk_extend was handed an empty batch; on Linux that "
                         "raises RuntimeError instead of the intended ValueError")

    def test_the_first_kept_triple_is_not_dropped(self):
        """The empty check pulls one triple off the stream; it must go back.

        Guards the obvious way to get the fix wrong -- loading the remainder
        and silently losing the triple that was peeked at.
        """
        rows = list(read_rdf(self.nt(self.RECORD)))
        self.assertEqual([r.chrom for r in rows if isinstance(r, Record)], ["1"])

    def test_a_syntax_error_on_the_first_line_is_translated_too(self):
        """Parsing is lazy, so a bad first line fails on the peek, not in bulk_extend.

        Both points translate the parser's SyntaxError; this is the one that
        only a malformed opening line reaches.
        """
        with self.assertRaisesRegex(ValueError, "Not valid N-Triples"):
            list(read_rdf(self.nt("<file://a.vcf> no brackets .\n" + self.RECORD)))

    def test_the_pre_0_4_parse_signature_still_works(self):
        """pyoxigraph < 0.4 has no RdfFormat and takes a MIME string instead.

        Hide RdfFormat and the fallback runs for real -- current pyoxigraph still
        accepts the legacy positional form -- so this is the shim working, not a
        mock of it.
        """
        import pyoxigraph

        with mock.patch.object(pyoxigraph, "RdfFormat", None):
            rows = list(read_rdf(self.nt(self.RECORD)))
        self.assertEqual([r.chrom for r in rows if isinstance(r, Record)], ["1"])

    def test_refusals(self):
        with self.assertRaisesRegex(ValueError, "existing .nt or .nt.gz"):
            list(read_rdf(self.nt(self.RECORD, "in.ttl")))
        with self.assertRaisesRegex(ValueError, "IRI subjects"):
            list(read_rdf(self.nt(self.RECORD + f'_:b <{VCFC}chrom> "1" .\n')))
        with self.assertRaisesRegex(ValueError, "Not valid N-Triples"):
            list(read_rdf(self.nt(self.RECORD + "<file://a.vcf> no brackets .\n")))
        with self.assertRaisesRegex(ValueError, "No VCFFile/hasRecord"):
            list(read_rdf(self.nt('<urn:a> <urn:b> "c" .\n')))


@unittest.skipIf(rdflib is None, "rdflib is required for the linking tests")
class References(Case):
    def gff3(self, *lines):
        path = self.root / "genes.gff3"
        path.write_text("##gff-version 3\n" + "".join(line + "\n" for line in lines), encoding="utf-8")
        return path

    def test_malformed_gff3_is_refused(self):
        reference = SimpleNamespace(feature_type="gene", id_attribute="ID")
        cases = {
            "expected nine columns": "1\tx\tgene\t1\t10\t.\t+\t.",
            "invalid coordinates": "1\tx\tgene\tone\t10\t.\t+\t.\tID=g",
            "invalid interval": "1\tx\tgene\t10\t5\t.\t+\t.\tID=g",
            "missing ID": "1\tx\tgene\t1\t10\t.\t+\t.\tName=g",
            "contains no 'gene' features": "1\tx\texon\t1\t10\t.\t+\t.\tID=e",
        }
        for pattern, line in cases.items():
            with self.subTest(pattern):
                with self.assertRaisesRegex(ValueError, pattern):
                    IntervalIndex(self.gff3(line), reference)

    def test_parsing_stops_at_the_fasta_section(self):
        reference = SimpleNamespace(feature_type="gene", id_attribute="ID")
        index = IntervalIndex(self.gff3("1\tx\tgene\t1\t10\t.\t+\t.\tID=g", "##FASTA", ">1", "ACGT"), reference)
        self.assertEqual(list(index.overlaps(LinkKey(chrom="1", start=5, end=5))), ["g"])

    def test_an_ambiguous_assembly_name_is_refused(self):
        with self.assertRaisesRegex(ValueError, "Ambiguous reference assembly"):
            check_assembly("GRCh37 lifted to GRCh38", "GRCh38")

    def test_reference_urls_must_be_local_files_or_https(self):
        reference = SimpleNamespace(url="file://remote-host/genes.gff3", sha256="0" * 64)
        with self.assertRaisesRegex(ValueError, "must be local"):
            acquire_reference(reference, self.root)
        with self.assertRaisesRegex(ValueError, "file: or https:"):
            acquire_reference(replace_url(reference, "ftp://example.org/genes.gff3"), self.root)

    def test_an_https_fetch_is_accounted_and_verified(self):
        body = (LINKERS / "gene-demo" / "genes.gff3").read_bytes()
        reference = SimpleNamespace(url="https://example.org/genes.gff3", sha256=GFF3_SHA)
        stats = {"requests": 0, "bytes_transferred": 0, "final_service_status": None, "cache_hits": 0}
        opener = mock.Mock()
        opener.open.return_value = response(body)
        with mock.patch("vcf_rdfizer_linking.reference.build_opener", return_value=opener):
            path = acquire_reference(reference, self.root, stats=stats)
        self.assertEqual(path.read_bytes(), body)
        self.assertEqual(stats, {"requests": 1, "bytes_transferred": len(body), "final_service_status": 200, "cache_hits": 0})
        opener.open.return_value = response(b"tampered")
        with mock.patch("vcf_rdfizer_linking.reference.build_opener", return_value=opener):
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                acquire_reference(replace_url(reference, reference.url), self.root / "other")


def replace_url(reference, url):
    return SimpleNamespace(url=url, sha256=reference.sha256)


def response(body, code=200, headers=None):
    handle = BytesIO(body)
    handle.code, handle.headers = code, headers or {}
    return handle


@unittest.skipIf(rdflib is None, "rdflib is required for the linking tests")
class RunnerRefusals(Case):
    def setUp(self):
        super().setUp()
        self.manifests = discover()

    def test_keys_need_integer_positions_and_a_contig(self):
        interval, allele = self.manifests["gene-demo"], self.manifests["spdi"]
        for manifest in (interval, allele):
            with self.subTest(manifest.id):
                with self.assertRaisesRegex(ValueError, "Invalid POS"):
                    keys_for(record(pos="ten"), manifest)
        for bad in (record(chrom="."), record(pos="0")):
            with self.subTest(bad.chrom + ":" + bad.pos):
                with self.assertRaisesRegex(ValueError, "Invalid interval key"):
                    keys_for(bad, interval)

    def test_a_resolver_must_define_resolve(self):
        directory = self.linker(manifest_text(), resolver="RESOLVE = None\n")
        with self.assertRaisesRegex(ValueError, "must define resolve"):
            load_resolver(SimpleNamespace(id="t", directory=directory))

    def test_preflight(self):
        token, output = self.manifests["rsid-dbsnp"], self.root / "out.nt"
        with self.assertRaisesRegex(ValueError, "at least one linker"):
            run_linkers([], [], output)
        with self.assertRaisesRegex(ValueError, "output path is required"):
            run_linkers([], [token], None)
        with self.assertRaisesRegex(ValueError, "must end in .nt"):
            run_linkers([], [token], self.root / "out.ttl")

    def test_one_source_cannot_declare_two_references(self):
        rows = [record(reference="GRCh38"), record(reference="GRCh37", row=2)]
        with self.assertRaisesRegex(Exception, "Conflicting reference metadata"):
            run_linkers(rows, [self.manifests["rsid-dbsnp"]], self.root / "out.nt", cache_dir=self.root)

    def test_progress_is_reported_every_ten_thousand_records(self):
        progress = mock.Mock()
        rows = [record(row=n, pos=str(n)) for n in range(1, 10_001)]
        run_linkers(rows, [self.manifests["rsid-dbsnp"]], self.root / "out.nt", cache_dir=self.root, progress=progress)
        self.assertIn(mock.call("Indexed 10000 records for linking"), progress.call_args_list)


@unittest.skipIf(rdflib is None, "rdflib is required for the linking tests")
class SessionEdges(Case):
    def setUp(self):
        super().setUp()
        self.live = replace(discover()["rsid-myvariant"], contact_email="a@example.org")
        self.sleeps = []
        self.policy = NetworkPolicy([self.live], clock=lambda: 0, sleep=self.sleeps.append)

    def session(self, transport):
        return CachedSession(self.live, self.root, self.policy, transport=transport)

    def test_get_parameters_are_sorted_into_the_url(self):
        transport = mock.Mock(return_value=response(b"{}"))
        self.session(transport).get(self.live.endpoint, params={"b": "2", "a": "1"})
        self.assertEqual(transport.call_args.args[0].full_url, self.live.endpoint + "?a=1&b=2")

    def test_an_oversized_reply_is_refused(self):
        transport = mock.Mock(return_value=response(b"x" * (16 * 1024 * 1024 + 1)))
        with self.assertRaisesRegex(ValueError, "exceeds 16 MiB"):
            self.session(transport).post(self.live.endpoint, json={})

    def test_a_network_failure_is_reported_not_retried(self):
        session = self.session(mock.Mock(side_effect=URLError("unreachable")))
        with self.assertRaisesRegex(ValueError, "network request failed"):
            session.post(self.live.endpoint, json={})
        self.assertEqual((session.stats["final_service_status"], session.stats["requests"]), ("network-error", 1))

    def test_an_unreadable_retry_after_falls_back_to_backoff(self):
        replies = iter([response(b"", 503, {"Retry-After": "whenever"}), response(b"[]")])
        session = self.session(lambda request, timeout: next(replies))
        self.assertEqual(session.post(self.live.endpoint, json={}).json(), [])
        self.assertEqual(session.stats["requests"], 2)

    def test_retries_run_out_and_a_non_finite_retry_after_is_ignored(self):
        session = self.session(lambda request, timeout: response(b"", 503, {"Retry-After": "inf"}))
        with self.assertRaisesRegex(ValueError, "HTTP 503"):
            session.post(self.live.endpoint, json={})
        self.assertEqual(session.stats["requests"], 4)

    def test_redirects_are_refused(self):
        self.assertIsNone(NoRedirects().redirect_request(None, None, 302, "Found", {}, "https://elsewhere.org/"))


@unittest.skipIf(rdflib is None, "rdflib is required for the linking tests")
class ResolverEdges(Case):
    def test_an_ensembl_error_entry_links_nothing(self):
        from vcf_rdfizer_linking import LinkerContext
        resolver = load_resolver(discover()["rsid-ensembl"])
        session = mock.Mock()
        session.post.return_value.json.return_value = {"rs1": {"error": "rs1 not found"}}
        self.assertEqual(list(resolver([LinkKey(token="rs1")], LinkerContext(session))), [])


@unittest.skipIf(rdflib is None, "rdflib is required for the linking tests")
class Cli(Case):
    def main(self, *argv):
        out, err = StringIO(), StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = vcf_rdfizer_link.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_keys_and_list(self):
        code, out, _ = self.main("keys")
        self.assertEqual((code, [line.split(":")[0] for line in out.splitlines()]),
                         (0, ["TokenJoin", "IntervalJoin", "AlleleJoin", "Subjects"]))
        code, out, _ = self.main("list")
        self.assertEqual(code, 0)
        self.assertIn("spdi 1.0.0 — tier 2", out)
        self.assertIn("Reference: file://", out)
        code, out, _ = self.main("list", "--json")
        self.assertIn("rsid-myvariant", {m["id"] for m in json.loads(out)})

    def test_init_copies_without_bytecode(self):
        target = self.root / "copy"
        self.assertEqual(self.main("init", "--example", "rsid-myvariant", "-o", str(target))[0], 0)
        self.assertEqual(sorted(p.name for p in target.iterdir()), ["README.md", "linker.ttl", "resolver.py"])

    def test_check_errors_go_to_json_when_asked(self):
        directory = self.linker(manifest_text().replace("vcfl:TokenJoin", "vcfl:FooJoin"))
        code, out, _ = self.main("check", str(directory), "--json")
        self.assertEqual((code, json.loads(out)["ok"]), (1, False))
        code, _, err = self.main("check", str(LINKERS / "gene-demo"), "--assembly", "GRCh37",
                                 "--links-cache", str(self.root))
        self.assertEqual(code, 1)
        self.assertIn("Assembly mismatch", err)

    def test_dry_run_needs_a_positive_limit(self):
        code, _, err = self.main("dry-run", str(LINKERS / "rsid-dbsnp"), "-i", str(EXAMPLE), "--limit", "0")
        self.assertEqual(code, 1)
        self.assertIn("--limit must be positive", err)

    def test_run_from_vcf_rdf_and_endpoint(self):
        from test.test_linking_unit import base_graph
        from vcf_rdfizer_policies.store import MemoryStore
        rdf = self.root / "base.nt"
        rdf.write_text(base_graph().serialize(format="nt"), encoding="utf-8")
        common = ("--link", "rsid-dbsnp", "--offline", "--links-cache", str(self.root))
        results = {}
        for name, source in (("vcf", ("-i", str(EXAMPLE))), ("rdf", ("--rdf", str(rdf))),
                             ("endpoint", ("--endpoint", "http://127.0.0.1:1/"))):
            output = self.root / f"{name}.links.nt"
            with mock.patch("vcf_rdfizer_policies.store.EndpointStore", lambda url: MemoryStore(base_graph())):
                code, out, _ = self.main("run", *source, *common, "-o", str(output))
            self.assertEqual(code, 0, name)
            self.assertTrue(output.with_suffix(".json").is_file())
            results[name] = json.loads(output.with_suffix(".json").read_text())["link_triples"]
        self.assertEqual(results, {"vcf": 3, "rdf": 3, "endpoint": 3})

    def test_run_refusals(self):
        existing = self.root / "done.links.nt"
        existing.with_suffix(".json").write_text("{}")
        for argv, pattern in (
                (("-o", str(self.root / "x.ttl")), "must end in .nt"),
                (("-o", str(existing)), "existing report"),
                (("--links-contact-email", "nobody", "-o", str(self.root / "y.nt")), "email address")):
            with self.subTest(pattern):
                code, _, err = self.main("run", "-i", str(EXAMPLE), "--link", "rsid-dbsnp", *argv)
                self.assertEqual(code, 1)
                self.assertIn(pattern, err)

    def test_a_contact_email_reaches_only_live_linkers(self):
        args = vcf_rdfizer_link.build_parser().parse_args(
            ["run", "-i", str(EXAMPLE), "-o", "x.nt", "--link", "rsid-dbsnp,rsid-myvariant",
             "--links-contact-email", "me@example.org"])
        contacts = {m.id: m.contact_email for m in vcf_rdfizer_link.selected_linkers(args)}
        self.assertEqual(contacts["rsid-myvariant"], "me@example.org")
        self.assertNotEqual(contacts["rsid-dbsnp"], "me@example.org")

    def test_a_missing_rdflib_is_an_instruction_not_a_traceback(self):
        with mock.patch.dict(sys.modules, {"rdflib": None}):
            with self.assertRaisesRegex(ValueError, "pip install"):
                vcf_rdfizer_link._require_rdflib()

    def test_link_mode_reports_a_failed_or_interrupted_run(self):
        rdf = self.root / "g.nt"
        rdf.write_text("")
        args = SimpleNamespace(rdf=str(rdf), link="rsid-dbsnp", out=str(self.root / "out"), linker_path=[],
                               links_contact_email=None, links_cache=str(self.root), offline=True,
                               links_cache_only=False, assembly=None)
        for error, code, message in ((ValueError("bad graph"), 1, "error: bad graph"),
                                     (KeyboardInterrupt(), 130, "no partial side-graph")):
            with self.subTest(code=code):
                with mock.patch("vcf_rdfizer_link.run_stage", side_effect=error), redirect_stderr(StringIO()) as err:
                    self.assertEqual(vcf_rdfizer_link.run_posthoc(args), code)
                self.assertIn(message, err.getvalue())
        summaries = list((self.root / "out" / "run_metrics").rglob("summary.json"))
        self.assertEqual(len(summaries), 2)                 # each run still records how it ended

    def test_link_mode_refuses_before_writing_anything(self):
        base = {"rdf": None, "link": "rsid-dbsnp", "out": str(self.root / "out"), "linker_path": [],
                "links_contact_email": None, "links_cache": str(self.root), "offline": True,
                "links_cache_only": False, "assembly": None}
        existing = self.root / "out" / "g.links.nt"
        existing.parent.mkdir()
        existing.write_text("")
        (self.root / "g.nt").write_text("")
        for rdf in (None, str(self.root / "g.ttl"), str(self.root / "g.nt")):
            with self.subTest(rdf=rdf):
                with redirect_stderr(StringIO()) as err:
                    self.assertEqual(vcf_rdfizer_link.run_posthoc(SimpleNamespace(**{**base, "rdf": rdf})), 2)
                self.assertTrue(err.getvalue().startswith("error:"))
        self.assertFalse((self.root / "out" / "run_metrics").exists())


if __name__ == "__main__":
    unittest.main()
