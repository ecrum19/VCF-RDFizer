"""Known-answer linking and failure tests. No Docker or public API calls."""

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
import gzip
import hashlib
from io import BytesIO, StringIO
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock
from urllib.error import HTTPError

# These tests exercise data linking itself, which requires rdflib. The rest of
# the suite runs without it, so this module skips rather than erroring the whole
# discovery run -- the same pattern test_validation_mutation_unit.py uses.
try:
    import rdflib
    from rdflib import Graph, Literal, RDF, URIRef
except ModuleNotFoundError:  # pragma: no cover - exercised only without rdflib
    rdflib = None
    Graph = Literal = RDF = URIRef = None

import vcf_rdfizer
import vcf_rdfizer_link
from vcf_rdfizer_linking import Link, LinkKey
from vcf_rdfizer_linking.inputs import Record, Source, read_rdf, read_tsv, read_vcf
from vcf_rdfizer_linking.manifest import VCFL, VCFR, absolute_iri, discover, load_manifest, select
from vcf_rdfizer_linking.reference import IntervalIndex, acquire_reference, check_assembly
from vcf_rdfizer_linking.runner import LinkRunError, keys_for, run_linkers, run_stage
from vcf_rdfizer_linking.session import CachedSession, NetworkPolicy

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/linking/example.vcf"


def base_graph(path=EXAMPLE):
    graph = Graph()
    for row in read_vcf(path):
        source = URIRef(row.source)
        graph.add((source, RDF.type, VCFR.VCFFile))
        graph.add((source, VCFR.referenceGenome, Literal(row.reference)))
        if isinstance(row, Source):
            continue
        record, call = URIRef(row.record), URIRef(row.call)
        graph.add((source, VCFR.hasRecord, record))
        graph.add((record, RDF.type, VCFR.VCFRecord))
        graph.add((record, VCFR.hasCall, call))
        graph.add((call, RDF.type, VCFR.VariantCall))
        for key, value in (("chrom", row.chrom), ("pos", row.pos), ("ref", row.ref), ("alt", row.alt), ("recordId", row.id)):
            graph.add((record, VCFR[key], Literal(value)))
        graph.add((call, VCFR.infoRaw, Literal(row.info)))
    return graph


class HTTPResponse(BytesIO):
    def __init__(self, body, code=200, headers=None):
        super().__init__(body if isinstance(body, bytes) else json.dumps(body).encode())
        self.code, self.headers = code, headers or {}


class FakeClock:
    def __init__(self):
        self.now, self.delays = 0, []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.delays.append(seconds)
        self.now += seconds


@unittest.skipIf(rdflib is None, "rdflib is required for the linking tests")
class LinkingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cache = self.root / "cache"
        self.output = self.root / "example.links.nt"
        self.manifests = discover()
        self.token = self.manifests["rsid-dbsnp"]
        self.interval = self.manifests["gene-demo"]
        self.live = replace(self.manifests["rsid-ensembl"], contact_email="test@example.org")
        self.clock = FakeClock()

    def policy(self, *manifests):
        return NetworkPolicy(manifests or [self.live], clock=self.clock, sleep=self.clock.sleep)

    def run_examples(self, manifests=None, records=None, **options):
        return run_linkers(records if records is not None else read_vcf(EXAMPLE),
                           manifests or [self.token, self.interval], self.output, cache_dir=self.cache, **options)

    def copy_manifest(self, manifest, transform=lambda text: text):
        directory = self.root / manifest.id
        shutil.copytree(manifest.directory, directory)
        path = directory / "linker.ttl"
        path.write_text(transform(path.read_text()))
        return directory

    def session(self, transport, *, manifest=None, offline=False, policy=None):
        return CachedSession(manifest or self.live, self.cache, policy or self.policy(manifest or self.live), offline=offline, transport=transport)

    def test_tier_1_and_2_known_answers_provenance_and_unique_terms(self):
        report = self.run_examples(offline=True)
        graph = Graph().parse(self.output, format="nt")
        self.assertEqual(report["records"], 4)
        self.assertEqual(report["link_triples"], 7)
        self.assertEqual([m["unique_keys"] for m in report["linkers"]], [2, 4])
        self.assertEqual(set(graph.objects(URIRef("file://example.vcf#call/2"), VCFL.overlapsGene)),
                         {URIRef("https://example.org/genes/demo-gene-A"), URIRef("https://example.org/genes/demo-gene-B")})
        self.assertFalse(list(graph.objects(URIRef("file://example.vcf#call/3"), VCFL.overlapsGene)))
        self.assertEqual(len(graph), report["triples"])
        self.assertEqual(len(self.output.read_text().splitlines()), len(graph))
        self.assertTrue(all(isinstance(s, URIRef) and isinstance(o, (URIRef, Literal)) for s, _, o in graph))
        for manifest, expected in ((self.token, 3), (self.interval, 4)):
            node = URIRef("file://example.vcf#linkset/" + manifest.id)
            self.assertEqual(int(graph.value(node, VCFL.linkCount)), expected)
        self.assertEqual(str(graph.value(URIRef("file://example.vcf#linkset/gene-demo"), VCFL.referenceDigest)), "sha256:" + self.interval.reference.sha256)

    def test_info_tokens_missing_values_and_uri_encoding(self):
        m = replace(self.token, field="INFO/RS", split_on=",", accept=".+")
        record = next(r for r in read_vcf(EXAMPLE) if isinstance(r, Record))
        record = replace(record, info="RS=rs1,rs1,.,a/b?c,,é")
        report = self.run_examples([m], [record])
        self.assertEqual(report["link_triples"], 3)
        self.assertIn("dbsnp:a%2Fb%3Fc", self.output.read_text())
        self.assertIn("dbsnp:%C3%A9", self.output.read_text())
        self.assertEqual(keys_for(replace(record, info="DB;XX=x"), m), [])
        with self.assertRaisesRegex(ValueError, "Duplicate INFO"):
            keys_for(replace(record, info="RS=rs1;RS=rs2"), m)

    def test_token_regex_matches_whole_token(self):
        record = next(r for r in read_vcf(EXAMPLE) if isinstance(r, Record))
        self.assertEqual(keys_for(replace(record, id="rs334;RS1;xrs2;rs3x;.;"), self.token), [LinkKey(token="rs334")])

    def test_interval_closed_boundaries_ref_span_and_exact_chromosome(self):
        path = acquire_reference(self.interval.reference, self.cache, offline=True)
        index = IntervalIndex(path, self.interval.reference)
        expected = {99: set(), 100: {"demo-gene-A"}, 200: {"demo-gene-A", "demo-gene-B"}, 250: {"demo-gene-B"}, 251: set()}
        for pos, matches in expected.items():
            self.assertEqual(set(index.overlaps(LinkKey(chrom="1", start=pos, end=pos))), matches)
        self.assertEqual(set(index.overlaps(LinkKey(chrom="1", start=99, end=100))), {"demo-gene-A"})
        self.assertEqual(list(index.overlaps(LinkKey(chrom="chr1", start=100, end=100))), [])
        row = next(r for r in read_vcf(EXAMPLE) if isinstance(r, Record))
        self.assertEqual(keys_for(replace(row, pos="99", ref="AT"), self.interval), [LinkKey(chrom="1", start=99, end=100)])
        for alt in ("<DEL>", "*", "N]2:20]"):
            self.assertEqual(keys_for(replace(row, alt=alt), self.interval), [])

    def test_nested_gff_features_are_not_missed(self):
        gff = self.root / "nested.gff3"
        gff.write_text("1\tx\tgene\t1\t1000\t.\t+\t.\tID=long\n1\tx\tgene\t5\t10\t.\t+\t.\tID=short\n")
        index = IntervalIndex(gff, self.interval.reference)
        self.assertEqual(list(index.overlaps(LinkKey(chrom="1", start=999, end=999))), ["long"])

    def test_assembly_missing_conflicting_and_override(self):
        for reference in ("GRCh38", "file:///genomes/Homo_sapiens.GRCh38.dna.fa", "hg38"):
            check_assembly(reference, "GRCh38")
        check_assembly("unknown.fa", "GRCh38", "GRCh38")
        for reference, override in (("GRCh37", None), ("GRCh37", "GRCh38"), ("GRCh38", "GRCh37"), (".", None)):
            with self.assertRaises(ValueError):
                check_assembly(reference, "GRCh38", override)

    def test_assembly_failure_precedes_any_resolution(self):
        records = [replace(row, reference="GRCh37") for row in read_vcf(EXAMPLE)]
        with mock.patch("vcf_rdfizer_linking.runner.load_resolver") as resolver:
            with self.assertRaisesRegex(LinkRunError, "Assembly mismatch"):
                self.run_examples([self.live, self.interval], records)
            resolver.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_digest_mismatch_and_corrupt_reference_cache_fail_closed(self):
        directory = self.copy_manifest(self.interval)
        (directory / "genes.gff3").write_text("tampered")
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            acquire_reference(load_manifest(directory).reference, self.cache, offline=True)
        path = acquire_reference(self.interval.reference, self.cache, offline=True)
        path.write_text("tampered cache")
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            acquire_reference(self.interval.reference, self.cache, offline=True)

    def test_https_bundle_stream_cache_gzip_and_offline_miss(self):
        raw = gzip.compress((self.interval.directory / "genes.gff3").read_bytes())
        reference = replace(self.interval.reference, url="https://example.org/genes.gff3.gz", sha256=hashlib.sha256(raw).hexdigest())
        with self.assertRaisesRegex(ValueError, "cache miss"):
            acquire_reference(reference, self.cache, offline=True)
        opener = mock.Mock()
        opener.open.return_value = HTTPResponse(raw)
        with mock.patch("vcf_rdfizer_linking.reference.build_opener", return_value=opener):
            path = acquire_reference(reference, self.cache)
            self.assertEqual(path, acquire_reference(reference, self.cache, offline=True))
        self.assertEqual(opener.open.call_count, 1)
        self.assertEqual(list(IntervalIndex(path, reference).overlaps(LinkKey(chrom="2", start=20, end=20))), ["demo-gene-C"])

    def test_tier_3_real_resolver_batches_deduplicates_and_replays_offline(self):
        calls = []
        def transport(request, timeout):
            calls.append(request)
            self.assertEqual(json.loads(request.data), {"ids": ["rs334", "rs699"]})
            return HTTPResponse({"rs334": {"name": "rs334"}, "rs699": {"name": "rs699"}})
        def factory(m, cache, policy, offline):
            return CachedSession(m, cache, policy, offline=offline, transport=transport)
        report = self.run_examples([self.live], session_factory=factory)
        self.assertEqual(len(calls), 1)
        self.assertIn("test@example.org", calls[0].get_header("User-agent"))
        self.assertEqual(report["link_triples"], 3)
        self.assertEqual(report["linkers"][0]["requests"], 1)
        first = set(Graph().parse(self.output, format="nt").triples((None, VCFL.sameVariantAs, None)))
        self.output.unlink()
        report = self.run_examples([self.live], session_factory=factory, offline=True)
        self.assertEqual(len(calls), 1)
        self.assertEqual(report["linkers"][0]["cache_hits"], 1)
        self.assertEqual(first, set(Graph().parse(self.output, format="nt").triples((None, VCFL.sameVariantAs, None))))

    def test_live_missing_identifier_emits_nothing_and_bad_payload_fails(self):
        from vcf_rdfizer_linking.runner import load_resolver
        from vcf_rdfizer_linking import LinkerContext
        resolver = load_resolver(self.live)
        session = mock.Mock()
        session.post.return_value.json.return_value = {"rs334": {"name": "rs334"}}
        self.assertEqual(len(list(resolver([LinkKey(token="rs334"), LinkKey(token="rs699")], LinkerContext(session)))), 1)
        for payload in ([], {"rs334": "bad"}, {"rs334": {}}):
            session.post.return_value.json.return_value = payload
            with self.assertRaises(ValueError):
                list(resolver([LinkKey(token="rs334")], LinkerContext(session)))

    def test_offline_cache_miss_issues_no_request(self):
        transport = mock.Mock()
        with self.assertRaisesRegex(ValueError, "offline/cache-only"):
            self.session(transport, offline=True).get(self.live.endpoint)
        transport.assert_not_called()

    def test_retry_after_and_per_run_budget_include_retries(self):
        transport = mock.Mock(side_effect=[HTTPResponse({}, 429, {"Retry-After": "4.5"}), HTTPResponse({}, 503), HTTPResponse({})])
        session = self.session(transport)
        session.get(self.live.endpoint)
        self.assertEqual(session.stats["requests"], 3)
        self.assertEqual(self.clock.delays, [4.5, 2.0])
        self.assertEqual(session.stats["final_service_status"], 200)
        manifest = replace(self.live, max_requests=1)
        session = self.session(mock.Mock(return_value=HTTPResponse({}, 503)), manifest=manifest)
        with self.assertRaisesRegex(ValueError, "maxRequestsPerRun=1 exhausted"):
            session.get(self.live.endpoint + "/different")
        self.assertEqual(session.stats["requests"], 1)

    def test_retry_after_http_date_is_honoured(self):
        transport = mock.Mock(side_effect=[HTTPResponse({}, 429, {"Retry-After": "Wed, 21 Oct 2099 07:28:00 GMT"}), HTTPResponse({})])
        self.session(transport).get(self.live.endpoint)
        self.assertGreater(self.clock.delays[0], 1_000_000)

    def test_httperror_response_is_retried(self):
        error = HTTPError(self.live.endpoint, 429, "slow", {"Retry-After": "3"}, BytesIO(b"{}"))
        session = self.session(mock.Mock(side_effect=[error, HTTPResponse({})]))
        session.get(self.live.endpoint)
        self.assertEqual(session.stats["requests"], 2)
        self.assertEqual(self.clock.delays, [3.0])

    def test_host_rate_shared_across_linkers_and_budget_across_sessions(self):
        other = replace(self.live, id="second", requests_per_second=0.5)
        policy = self.policy(self.live, other)
        self.session(mock.Mock(return_value=HTTPResponse({})), policy=policy).get(self.live.endpoint)
        self.session(mock.Mock(return_value=HTTPResponse({})), manifest=other, policy=policy).get(other.endpoint)
        self.assertEqual(self.clock.delays, [2.0])
        limited = replace(self.live, max_requests=1)
        with self.assertRaisesRegex(ValueError, "exhausted"):
            self.session(mock.Mock(), manifest=limited, policy=policy).get(self.live.endpoint + "/other")

    def test_cache_separates_version_method_and_request_body_and_checks_digest(self):
        transport = mock.Mock(side_effect=lambda *a, **k: HTTPResponse({}))
        for manifest, method, body in ((self.live, "GET", None), (self.live, "POST", {"ids": ["rs1"]}),
                                       (self.live, "POST", {"ids": ["rs2"]}), (replace(self.live, version="2"), "GET", None)):
            session = self.session(transport, manifest=manifest)
            session.get(manifest.endpoint) if method == "GET" else session.post(manifest.endpoint, json=body)
        self.assertEqual(transport.call_count, 4)
        files = list(self.cache.rglob("*.json"))
        self.assertEqual(len(files), 4)
        for file in files:
            cached = json.loads(file.read_text())
            cached["sha256"] = "0" * 64
            file.write_text(json.dumps(cached))
        with self.assertRaisesRegex(ValueError, "Corrupt response cache"):
            self.session(transport, offline=True).get(self.live.endpoint)

    def test_undeclared_hosts_paths_redirects_and_placeholder_contact(self):
        transport = mock.Mock()
        for url in ("https://other.org/variation/homo_sapiens", "https://rest.ensembl.org/other", "http://rest.ensembl.org/variation/homo_sapiens"):
            with self.assertRaisesRegex(ValueError, "outside declared"):
                self.session(transport).get(url)
        transport.assert_not_called()
        with self.assertRaisesRegex(ValueError, "contact"):
            self.session(transport, manifest=self.manifests["rsid-ensembl"]).get(self.live.endpoint)
        with self.assertRaisesRegex(ValueError, "HTTP 302"):
            self.session(mock.Mock(return_value=HTTPResponse({}, 302))).get(self.live.endpoint)

    def test_resolver_invalid_key_and_iri_leave_no_partial_output(self):
        for bad in (Link(LinkKey(token="rs999"), "https://example.org/ok"), Link(LinkKey(token="rs334"), 'https://example.org/> <bad>')):
            with mock.patch("vcf_rdfizer_linking.runner.load_resolver", return_value=lambda batch, ctx: [bad]):
                with self.assertRaises(LinkRunError):
                    self.run_examples([self.token, self.live])
            self.assertFalse(self.output.exists())

    def test_resolver_exception_records_failure_and_never_changes_base_graph(self):
        rdf = self.root / "base.nt"
        original = base_graph().serialize(format="nt").encode()
        rdf.write_bytes(original)
        with mock.patch("vcf_rdfizer_linking.runner.load_resolver", return_value=mock.Mock(side_effect=ValueError("broken resolver"))):
            with self.assertRaisesRegex(LinkRunError, "broken resolver"):
                run_stage(rdf, [self.token, self.live], self.output, metrics_dir=self.root / "metrics", cache_dir=self.cache)
        self.assertEqual(rdf.read_bytes(), original)
        self.assertFalse(self.output.exists())
        report = json.loads(next((self.root / "metrics/stages/linking").glob("*.json")).read_text())
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["linkers"][1]["status"], "failed")
        self.assertIn("linking", json.loads((self.root / "metrics/run.json").read_text()))

    def test_unordered_plain_and_gz_rdf_preserve_custom_subjects(self):
        graph = base_graph()
        old = URIRef("file://example.vcf#call/1")
        custom = URIRef("https://example.org/custom-call")
        renamed = Graph()
        for s, p, o in graph:
            renamed.add((custom if s == old else s, p, custom if o == old else o))
        graph = renamed
        nt = "\n".join(reversed(graph.serialize(format="nt").splitlines())) + "\n"
        for suffix in (".nt", ".nt.gz"):
            with self.subTest(suffix=suffix):
                path = self.root / ("base" + suffix)
                path.write_bytes(gzip.compress(nt.encode()) if suffix.endswith("gz") else nt.encode())
                report = self.run_examples(records=read_rdf(path), offline=True)
                linked = Graph().parse(self.output, format="nt")
                self.assertEqual(report["link_triples"], 7)
                self.assertTrue(list(linked.objects(custom, VCFL.sameVariantAs)))
                self.assertTrue(all(s in set(graph.subjects()) for s, p, o in linked if p in {VCFL.sameVariantAs, VCFL.overlapsGene}))
                self.output.unlink()

    def test_rdf_missing_call_subject_and_ambiguous_fields_fail(self):
        for mutation in ("call", "id"):
            graph = base_graph()
            if mutation == "call":
                graph.remove((URIRef("file://example.vcf#call/1"), None, None))
            else:
                graph.add((URIRef("file://example.vcf#record/1"), VCFR.recordId, Literal("rs999")))
            path = self.root / "bad.nt"
            path.write_text(graph.serialize(format="nt"))
            with self.assertRaises(ValueError):
                list(read_rdf(path))

    def test_empty_vcf_and_rdf_emit_zero_count_provenance(self):
        empty = self.root / "empty.vcf"
        empty.write_text("##fileformat=VCFv4.3\n##reference=GRCh38\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        rdf = self.root / "empty.nt"
        rdf.write_text(base_graph(empty).serialize(format="nt"))
        for records in (read_vcf(empty), read_rdf(rdf)):
            report = self.run_examples(records=records, offline=True)
            self.assertEqual(report["link_triples"], 0)
            self.assertEqual(len(list(Graph().parse(self.output, format="nt").subjects(RDF.type, VCFL.Linkset))), 2)
            self.output.unlink()

    def test_preview_uses_no_network_no_resolver_import_and_no_cache_writes(self):
        for manifest in (self.token, self.interval, self.live):
            with mock.patch("vcf_rdfizer_linking.runner.load_resolver", side_effect=AssertionError("must not load")):
                report = run_linkers(read_vcf(EXAMPLE, limit=2), [manifest], None, cache_dir=self.cache, dry_run=True)
            self.assertEqual(report["records"], 2)
        self.assertEqual(report["linkers"][0]["batches"], 1)
        self.assertEqual(report["linkers"][0]["status"], "planned")
        self.assertFalse(self.cache.exists())

    def test_output_collision_preserves_existing_file(self):
        self.output.write_text("keep me")
        with self.assertRaisesRegex(ValueError, "overwrite"):
            self.run_examples()
        self.assertEqual(self.output.read_text(), "keep me")

    def test_publication_failure_removes_temporary_graph(self):
        with mock.patch("vcf_rdfizer_linking.runner.os.link", side_effect=OSError("cannot publish")):
            with self.assertRaisesRegex(LinkRunError, "cannot publish"):
                self.run_examples([self.token])
        self.assertEqual(list(self.root.glob("*.nt")), [])

    def test_manifest_errors_are_early_and_actionable(self):
        changes = [('"rsid-dbsnp"', '"../bad"'), ('"1.0.0"', '"../version"'), ("TokenJoin", "AlleleJoin"),
                   ('"ID"', '"FORMAT/GT"'), ("{TOKEN}", "{BAD}"), ("sameVariantAs", "bad predicate"),
                   ("splitOn", "spltiOn"), ('"^rs[0-9]+$"', '"["')]
        for before, after in changes:
            with self.subTest(change=before):
                directory = self.copy_manifest(self.token, lambda t: t.replace(before, after))
                with self.assertRaises(ValueError):
                    load_manifest(directory)
                shutil.rmtree(directory)
        for before, after in (("batchSize 100", "batchSize 0"), ("maxRequestsPerRun 100", "maxRequestsPerRun -1"),
                              ("maxRequestsPerSecond 2", 'maxRequestsPerSecond "nan"'), ("https://rest", "http://rest")):
            directory = self.copy_manifest(self.live, lambda t: t.replace(before, after))
            with self.assertRaises(ValueError):
                load_manifest(directory)
            shutil.rmtree(directory)

    def test_discovery_path_environment_entry_point_and_duplicates(self):
        directory = self.copy_manifest(self.token, lambda t: t.replace('"rsid-dbsnp"', '"custom"'))
        self.assertIn("custom", discover([directory]))
        with mock.patch.dict("os.environ", {"VCF_RDFIZER_LINKER_PATH": str(directory)}):
            self.assertEqual(select("custom,custom")[0].id, "custom")
        entry = mock.Mock()
        entry.load.return_value = lambda: directory
        with mock.patch("vcf_rdfizer_linking.manifest.importlib.metadata.entry_points", return_value=[entry]):
            self.assertIn("custom", discover())
        (directory / "linker.ttl").write_text((directory / "linker.ttl").read_text().replace('"custom"', '"rsid-dbsnp"'))
        with self.assertRaisesRegex(ValueError, "Ambiguous"):
            discover([directory])

    def test_cli_init_check_and_dry_run(self):
        directory = self.root / "my-linker"
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            self.assertEqual(vcf_rdfizer_link.main(["init", "--example", "gene-demo", "-o", str(directory)]), 0)
            self.assertEqual(vcf_rdfizer_link.main(["check", str(directory), "--offline", "--links-cache", str(self.cache)]), 0)
            self.assertEqual(vcf_rdfizer_link.main(["dry-run", str(directory), "-i", str(EXAMPLE), "--limit", "2", "--links-cache", str(self.cache)]), 0)
            self.assertEqual(vcf_rdfizer_link.main(["init", "-o", str(directory)]), 1)

    def test_main_link_mode_has_no_docker_dependency_and_writes_metrics(self):
        rdf = self.root / "base.nt"
        rdf.write_text(base_graph().serialize(format="nt"))
        args = ["vcf-rdfizer", "--mode", "link", "--rdf", str(rdf), "--link", "rsid-dbsnp,gene-demo", "--offline",
                "--links-cache", str(self.cache), "-o", str(self.root / "results")]
        with mock.patch.object(sys, "argv", args), mock.patch.object(vcf_rdfizer, "check_docker", side_effect=AssertionError("Docker must not run")), redirect_stdout(StringIO()):
            self.assertEqual(vcf_rdfizer.main(), 0)
        self.assertTrue((self.root / "results/base.links.nt").is_file())
        run = json.loads(next((self.root / "results/run_metrics").rglob("run.json")).read_text())
        self.assertEqual(run["mode"], "link")
        self.assertEqual(next(iter(run["linking"].values()))["link_triples"], 7)

    def test_full_mode_links_plain_and_gzip_aggregates_without_changing_them(self):
        from test.test_vcf_rdfizer_unit import invoke_main, mocked_triplets
        input_dir = self.root / "inputs"
        input_dir.mkdir()
        vcf = input_dir / "sample.vcf"
        shutil.copyfile(EXAMPLE, vcf)
        rules = self.root / "rules.ttl"
        rules.write_text("@prefix ex: <https://example.org/> .\n")
        original = base_graph(vcf).serialize(format="nt").encode()
        for storage in ("plain", "space-optimized"):
            out = self.root / storage
            def fake_run(cmd, **kwargs):
                if "/opt/vcf-rdfizer/run_conversion.sh" in cmd:
                    directory = out / "sample"
                    directory.mkdir(parents=True, exist_ok=True)
                    if storage == "plain":
                        (directory / "sample.nt").write_bytes(original)
                    else:
                        (directory / "sample.nt.gz").write_bytes(gzip.compress(original))
                return 0
            with mock.patch.object(vcf_rdfizer, "run", side_effect=fake_run), \
                 mock.patch.object(vcf_rdfizer, "check_docker", return_value=True), \
                 mock.patch.object(vcf_rdfizer, "docker_image_exists", return_value=True), \
                 mock.patch.object(vcf_rdfizer, "discover_tsv_triplets", return_value=mocked_triplets()), \
                 mock.patch.object(vcf_rdfizer, "emit_record_detail", return_value=None), \
                 redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                code = invoke_main(["--input", str(input_dir), "--rules", str(rules), "--rdf-storage-mode", storage,
                                    "--header-representation", "basic", "--compression", "none", "--out", str(out),
                                    "--keep-tsv", "--link", "rsid-dbsnp,gene-demo", "--offline", "--links-cache", str(self.cache)])
            self.assertEqual(code, 0)
            linked = Graph().parse(out / "sample/sample.links.nt", format="nt")
            self.assertEqual(len(list(linked.triples((None, VCFL.overlapsGene, None)))), 4)
            actual = (out / "sample/sample.nt").read_bytes() if storage == "plain" else gzip.decompress((out / "sample/sample.nt.gz").read_bytes())
            self.assertEqual(actual, original)


if __name__ == "__main__":
    unittest.main()
