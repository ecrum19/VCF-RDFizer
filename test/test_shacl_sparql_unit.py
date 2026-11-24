"""The SPARQL constraints of the full shape profile, run once over the whole graph.

pyshacl evaluates a SPARQL-based constraint once per focus node, in memory, so
the full profile was validated whole and only up to 10M triples. The validator
now runs each constraint once over a QLever index of the graph instead
(src/validation/shacl_sparql.py). These tests pin the parts that can be checked
without QLever:

* splitting a profile into pyshacl's share and the SPARQL constraints;
* turning a per-focus-node query into a whole-graph one;
* the RDFS typing that stands in for pyshacl's inference;
* the four scalable equivalents, which must find exactly the focus nodes the
  published queries find, on every fault-injection graph (rdflib evaluates
  both here);
* how run outcomes become the report.

The comparison with pyshacl itself, on the same graphs, needs the image (pyshacl
and QLever) and is recorded in docs/validation.md.
"""
from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
import urllib.error
from io import BytesIO
from pathlib import Path
from unittest import mock

import rdflib
from rdflib.plugins.sparql import prepareQuery

from test.helpers import VerboseTestCase

REPO = Path(__file__).resolve().parents[1]
SHACL = REPO / "vcf_rdfizer_data" / "shacl"
ONTOLOGY = REPO / "vcf_rdfizer_data" / "ontology" / "vcf-core-vocabulary.bundle.ttl"
FULL = [SHACL / name for name in (
    "vcf-core-vocabulary.shacl.ttl", "vcf-core-consistency.shacl.ttl",
    "vcf-core-vocabulary-sparql.shacl.ttl")]

RUNNER_PATH = REPO / "src" / "validation" / "validation_runner.py"
_spec = importlib.util.spec_from_file_location("validation_runner_shacl_sparql", RUNNER_PATH)
V = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(V)
S = V.shacl_sparql
SH = rdflib.Namespace(S.SH)


def _full_profile():
    return S.split_shapes(FULL)


class SplitTests(VerboseTestCase):
    def test_the_full_profile_has_forty_sparql_constraints(self):
        _rest, constraints = _full_profile()
        self.assertEqual(len(constraints), 40)

    def test_pyshacl_keeps_everything_but_the_sparql_constraints(self):
        rest, _constraints = _full_profile()
        self.assertNotIn((None, SH.sparql, None), rest)
        core = rdflib.Graph().parse(str(FULL[0]), format="turtle")
        # Blank nodes get new labels in every parse, so compare the named triples.
        missing = [triple for triple in core if triple not in rest
                   and not any(isinstance(term, rdflib.BNode) for term in triple)]
        self.assertEqual(missing, [], "the core shapes must reach pyshacl unchanged")

    def test_the_core_profile_alone_has_none(self):
        rest, constraints = S.split_shapes(FULL[:1])
        self.assertEqual(constraints, [])
        self.assertEqual(len(rest), len(rdflib.Graph().parse(str(FULL[0]), format="turtle")))

    def test_severity_comes_from_the_shape(self):
        _rest, constraints = _full_profile()
        warnings = [c for c in constraints if c.severity == "Warning"]
        self.assertEqual({c.shape.rsplit("#", 1)[1] for c in warnings}, {"RecommendedHeaderSparqlShape"})
        self.assertTrue(all(c.severity in ("Violation", "Warning") for c in constraints))

    def test_every_query_declares_the_prefixes_it_relies_on(self):
        """pyshacl supplies the shapes file's prefixes; an endpoint needs them written."""
        _rest, constraints = _full_profile()
        for constraint in constraints:
            with self.subTest(shape=constraint.shape):
                self.assertIn("PREFIX vcfc: <https://w3id.org/vcf-core/vocab#>", constraint.select)
                # Declared once, even where the published query already declares it.
                self.assertEqual(constraint.select.count("PREFIX vcfc:"), 1)

    def test_every_constraint_can_run_whole(self):
        _rest, constraints = _full_profile()
        self.assertEqual([c.shape for c in constraints if c.unsupported], [])
        for constraint in constraints:
            with self.subTest(shape=constraint.shape):
                prepareQuery(constraint.query)

    def test_a_deactivated_constraint_is_not_run(self):
        shapes = _write_shapes("""
            ex:S a sh:NodeShape ; sh:targetClass ex:C ;
              sh:sparql [ sh:select "SELECT $this WHERE { $this ex:p ?o }" ; sh:deactivated true ] .
        """)
        _rest, constraints = S.split_shapes([shapes])
        self.assertEqual(constraints, [])


class EquivalentTests(VerboseTestCase):
    def test_each_equivalent_matches_a_published_query(self):
        """A vocabulary update that changes one must not leave a stale rewrite in use."""
        _rest, constraints = _full_profile()
        used = sorted(c.equivalent for c in constraints if c.equivalent)
        self.assertEqual(used, [
            "chrom-contiguous-index-ranges",
            "field-cardinality-sample-optionals-in-format-branch",
            "field-cardinality-sample-optionals-in-format-branch",
            "pos-nondecreasing-neighbours-and-segments",
            "record-id-unique-grouped",
        ])

    def test_a_changed_query_falls_back_to_its_own_text(self):
        query = "SELECT $this WHERE { $this <http://example.org/p> ?o }"
        self.assertEqual(S.scalable_equivalent(query), (query, None))

    def test_whitespace_does_not_change_the_match(self):
        _rest, constraints = _full_profile()
        published = next(c for c in constraints if c.equivalent == "record-id-unique-grouped").published
        reflowed = "\n  ".join(published.split())
        self.assertEqual(S.scalable_equivalent(reflowed)[1], "record-id-unique-grouped")

    def test_the_cardinality_rewrite_only_moves_the_optionals(self):
        _rest, constraints = _full_profile()
        constraint = next(c for c in constraints if c.equivalent and "cardinality" in c.equivalent)
        rewritten, _name = S.scalable_equivalent(constraint.published)
        branch = rewritten[rewritten.index("UNION {"):]
        branch = branch[: branch.index("BIND")]
        self.assertIn("?sample vcfc:hasGenotype/vcfc:ploidy ?ploidy", branch)
        self.assertIn('vcfc:fieldId "LAA"', branch)
        self.assertEqual(sorted(rewritten.split()), sorted(constraint.published.split()))


class WholeGraphQueryTests(VerboseTestCase):
    def test_this_is_unbound_and_joined_with_the_targets(self):
        query = S.whole_graph_query(
            "PREFIX ex: <http://example.org/>\nSELECT $this WHERE { $this ex:p ?o . }",
            ["?this a ex:C .", "?this ex:q ?t ."])
        self.assertNotIn("$this", query)
        self.assertTrue(query.startswith("PREFIX ex: <http://example.org/>\nSELECT DISTINCT ?this WHERE {"))
        self.assertIn("{ { ?this a ex:C . } UNION { ?this ex:q ?t . } }", query)
        prepareQuery(query)

    def test_solution_modifiers_stay_outside(self):
        query = S.whole_graph_query(
            "SELECT $this WHERE { $this <http://e/p> ?o } GROUP BY $this HAVING (COUNT(?o) > 1)",
            ["?this a <http://e/C> ."])
        self.assertTrue(query.rstrip().endswith("GROUP BY ?this HAVING (COUNT(?o) > 1)"))
        prepareQuery(query)

    def test_braces_inside_strings_do_not_end_the_group(self):
        query = S.whole_graph_query(
            'SELECT $this WHERE { $this <http://e/p> ?o FILTER (?o != "}{") }', ["?this a <http://e/C> ."])
        prepareQuery(query)
        self.assertIn('"}{"', query)

    def test_a_prebound_variable_other_than_this_is_unsupported(self):
        constraint = S.prepare(S.SparqlConstraint(
            shape="http://e/S", message=None, severity="Violation",
            select="SELECT $this WHERE { $this $PATH ?o }", targets=["?this a <http://e/C> ."]))
        self.assertIn("$PATH", constraint.unsupported)

    def test_a_shape_without_targets_is_unsupported(self):
        constraint = S.prepare(S.SparqlConstraint(
            shape="http://e/S", message=None, severity="Violation",
            select="SELECT $this WHERE { $this <http://e/p> ?o }", targets=[]))
        self.assertIn("no target", constraint.unsupported)

    def test_every_kind_of_target(self):
        shapes = _write_shapes("""
            ex:S a sh:NodeShape, rdfs:Class ; sh:targetClass ex:C ; sh:targetNode ex:n ;
              sh:targetSubjectsOf ex:s ; sh:targetObjectsOf ex:o ;
              sh:sparql [ sh:select "SELECT $this WHERE { $this ?p ?v }" ] .
        """)
        _rest, (constraint,) = S.split_shapes([shapes])
        self.assertEqual(len(constraint.targets), 5)
        graph = rdflib.Graph().parse(data="""
            @prefix ex: <http://example.org/> .
            ex:a a ex:C . ex:b a ex:S . ex:c ex:s ex:x . ex:y ex:o ex:d . ex:n ex:p 1 .
            ex:z ex:p 2 .
        """, format="turtle")
        found = {str(row[0]).rsplit("/", 1)[1] for row in graph.query(constraint.query)}
        # Each target kind contributes its node; d and x are targeted but have
        # no outgoing triple, so the constraint body does not match them.
        self.assertEqual(found, {"a", "b", "c", "n"})


class TypingTests(VerboseTestCase):
    VOCABULARY = """
        @prefix ex: <http://example.org/> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:B rdfs:subClassOf ex:A . ex:C rdfs:subClassOf ex:B .
        ex:has rdfs:domain ex:C ; rdfs:range ex:B .
        ex:label rdfs:range xsd:string .
        ex:child rdfs:subPropertyOf ex:has .
    """

    def _typing(self, data: str) -> set[str]:
        vocabulary = S.Vocabulary(rdflib.Graph().parse(data=self.VOCABULARY, format="turtle"))
        with tempfile.TemporaryDirectory() as td:
            source, target = Path(td) / "d.nt", Path(td) / "t.nt"
            source.write_text(data, encoding="utf-8")
            count = S.write_rdfs_typing(source, vocabulary, target)
            lines = target.read_text(encoding="utf-8").splitlines()
        self.assertEqual(count, len(lines))
        return set(lines)

    def test_domain_and_range_types_come_with_their_superclasses(self):
        lines = self._typing("<http://example.org/s> <http://example.org/has> <http://example.org/o> .\n")
        t = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        self.assertEqual(lines, {
            f"<http://example.org/s> {t} <http://example.org/C> .",
            f"<http://example.org/s> {t} <http://example.org/B> .",
            f"<http://example.org/s> {t} <http://example.org/A> .",
            f"<http://example.org/o> {t} <http://example.org/B> .",
            f"<http://example.org/o> {t} <http://example.org/A> .",
        })

    def test_an_explicit_type_gets_its_superclasses(self):
        t = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        lines = self._typing(f"<http://example.org/s> {t} <http://example.org/C> .\n")
        self.assertEqual(lines, {f"<http://example.org/s> {t} <http://example.org/B> .",
                                 f"<http://example.org/s> {t} <http://example.org/A> ."})

    def test_a_subproperty_implies_its_parent_and_its_parents_typing(self):
        lines = self._typing("<http://example.org/s> <http://example.org/child> <http://example.org/o> .\n")
        self.assertIn("<http://example.org/s> <http://example.org/has> <http://example.org/o> .", lines)
        self.assertIn("<http://example.org/o> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
                      "<http://example.org/B> .", lines)

    def test_literals_and_datatype_ranges_are_not_typed(self):
        lines = self._typing('<http://example.org/s> <http://example.org/label> "x" .\n'
                             '<http://example.org/s> <http://example.org/has> "y" .\n')
        self.assertFalse(any(line.startswith('"') for line in lines))
        self.assertFalse(any("string" in line for line in lines))

    def test_a_repeated_subject_or_object_is_typed_once(self):
        data = "".join(f"<http://example.org/s> <http://example.org/has> <http://example.org/o> .\n"
                       for _ in range(3))
        vocabulary = S.Vocabulary(rdflib.Graph().parse(data=self.VOCABULARY, format="turtle"))
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "d.nt").write_text(data, encoding="utf-8")
            count = S.write_rdfs_typing(Path(td) / "d.nt", vocabulary, Path(td) / "t.nt")
        self.assertEqual(count, 5)

    def test_a_compressed_graph_streams_with_its_typing_interleaved(self):
        import gzip
        import io

        vocabulary = S.Vocabulary(rdflib.Graph().parse(data=self.VOCABULARY, format="turtle"))
        with tempfile.TemporaryDirectory() as td:
            source = Path(td) / "g.nt.gz"
            with gzip.open(source, "wt", encoding="utf-8") as handle:
                handle.write("<http://example.org/s> <http://example.org/has> <http://example.org/o> .\n\n")
            out = io.StringIO()
            counts = S.stream_with_typing([source], vocabulary, out)
        lines = out.getvalue().splitlines()
        self.assertEqual(counts, {"inputTriples": 1, "rdfsTypingTriples": 5})
        self.assertEqual(lines[0], "<http://example.org/s> <http://example.org/has> <http://example.org/o> .")
        self.assertEqual(len(lines), 6)

    def test_the_bundled_vocabulary_types_a_record(self):
        vocabulary = S.Vocabulary(rdflib.Graph().parse(str(ONTOLOGY), format="turtle"))
        self.assertIn("https://w3id.org/vcf-core/vocab#VCFRecord",
                      vocabulary.domains["https://w3id.org/vcf-core/vocab#recordIndex"])


def _typed_fixture_graphs():
    """Every fault-injection graph, with the vocabulary and its RDFS typing."""
    from test import validation_fixtures as fixtures
    from test import validation_mutations as mutations

    vocabulary_graph = rdflib.Graph().parse(str(ONTOLOGY), format="turtle")
    vocabulary = S.Vocabulary(vocabulary_graph)
    cache: dict = {}
    with tempfile.TemporaryDirectory() as td:
        for representation in ("expanded", "condensed"):
            def base(options):
                if options not in cache:
                    cache[options] = fixtures.build_graph(representation, **dict(options))
                return cache[options]
            cache.clear()
            items = [(f"{representation}__control", base(()))]
            items += [(f"{representation}__{m.id}", m.apply(base(m.graph_options)))
                      for m in mutations.for_representation(representation)]
            for name, text in items:
                source, typing = Path(td) / "g.nt", Path(td) / "t.nt"
                source.write_text(text, encoding="utf-8")
                S.write_rdfs_typing(source, vocabulary, typing)
                graph = rdflib.Graph()
                graph.parse(str(source), format="nt")
                graph.parse(str(typing), format="nt")
                graph += vocabulary_graph
                yield name, graph


class EquivalenceOnFixturesTests(VerboseTestCase):
    """The rewrites find the same focus nodes as the published queries.

    Both run whole on every fault-injection graph and both controls. rdflib is
    slow on the published cubic query, but these graphs are small.
    """

    def test_every_rewrite_agrees_with_its_published_query(self):
        _rest, constraints = _full_profile()
        rewritten = [c for c in constraints if c.equivalent]
        pairs = [(c, S.whole_graph_query(c.select, c.targets), c.query) for c in rewritten]
        checked = 0
        for name, graph in _typed_fixture_graphs():
            for constraint, published, scalable in pairs:
                with self.subTest(graph=name, rewrite=constraint.equivalent, shape=constraint.shape):
                    expected = {str(row[0]) for row in graph.query(published)}
                    actual = {str(row[0]) for row in graph.query(scalable)}
                    self.assertEqual(actual, expected)
                    checked += 1
        self.assertGreater(checked, 100 * len(pairs))


class EquivalenceOnRandomRecordsTests(VerboseTestCase):
    """The record-level rewrites on arrangements the fixtures do not contain.

    Interleaved CHROMs, a CHROM split into several blocks, unsorted blocks,
    repeated and missing IDs: random files of up to eight records, each checked
    with the published query and its rewrite.
    """

    def test_ordering_and_identifier_rewrites_agree_on_random_files(self):
        import random

        _rest, constraints = _full_profile()
        pairs = [(c, S.whole_graph_query(c.select, c.targets), c.query)
                 for c in constraints if c.equivalent and "cardinality" not in c.equivalent]
        self.assertEqual(len(pairs), 3)
        vcfc = "https://w3id.org/vcf-core/vocab#"
        rng = random.Random(20261124)
        seen = {id(c): set() for c, _p, _s in pairs}
        for case in range(600):
            lines = [f"<file://f> <{S.RDF}type> <{vcfc}VCFFile> ."]
            # Mostly consecutive indices, with the gaps and repeats a broken
            # graph can have one case in three.
            steps = [1] if case % 3 else [0, 1, 1, 1, 2, 3]
            index = 0
            for row in range(1, rng.randint(1, 8) + 1):
                index += rng.choice(steps) if row > 1 else 1
                record = f"<file://f#record/{row}>"
                lines += [
                    f"<file://f> <{vcfc}hasRecord> {record} .",
                    f'{record} <{vcfc}recordIndex> "{index}"^^<{S.XSD}integer> .',
                    f'{record} <{vcfc}chrom> "{rng.choice("123")}" .',
                    f'{record} <{vcfc}pos> "{rng.randint(1, 6)}"^^<{S.XSD}integer> .',
                    f'{record} <{vcfc}recordId> "{rng.choice([".", ".", "a", "b"])}" .',
                ]
            graph = rdflib.Graph().parse(data="\n".join(lines) + "\n", format="nt")
            for constraint, published, scalable in pairs:
                with self.subTest(case=case, rewrite=constraint.equivalent):
                    expected = {str(row[0]) for row in graph.query(published)}
                    self.assertEqual({str(row[0]) for row in graph.query(scalable)}, expected)
                    seen[id(constraint)].add(bool(expected))
        # Each published query both fired and stayed quiet somewhere, so the
        # agreement is not the trivial one of two empty answers.
        self.assertTrue(all(outcomes == {True, False} for outcomes in seen.values()))


class ReportTests(VerboseTestCase):
    def test_focus_nodes_are_terms(self):
        document = {"results": {"bindings": [
            {"this": {"type": "uri", "value": "http://e/a"}},
            {"this": {"type": "bnode", "value": "b0"}},
            {"this": {"type": "uri", "value": "http://e/a"}},
            {},
        ]}}
        self.assertEqual(S.focus_nodes(document), ["<http://e/a>", "_:b0"])

    def test_a_result_parses_like_a_pyshacl_result(self):
        constraint = S.SparqlConstraint(shape="http://e/S", message="m", severity="Warning",
                                        select="", targets=[])
        (entry,) = V.parse_shacl_results(S.result_text(constraint, "<http://e/a>"))
        self.assertEqual(entry["severity"], "Warning")
        self.assertIn("Focus Node: <http://e/a>", entry["text"])


class _FakeIndex:
    """Stands in for ShaclQleverIndex: answers from a table, records its inputs."""

    instances: list["_FakeIndex"] = []

    def __init__(self, sources, vocabulary, *, raw_dir, scratch, options):
        self.sources, self.vocabulary, self.query_timeout = sources, vocabulary, 5
        self.streamed = {"inputTriples": 1, "rdfsTypingTriples": 0}
        self.index_seconds = self.setup_seconds = 0.1
        self.stopped = False
        _FakeIndex.instances.append(self)

    def prepare(self):
        if self.answers == "fail":
            raise RuntimeError("qlever-index exited 1")

    def describe(self):
        return {"version": "test", "memoryLimitGb": 4, "commands": {}, "buildStatus": "ok"}

    def _post(self, query, *, timeout):
        for marker, answer in self.answers.items():
            if marker in query:
                if isinstance(answer, BaseException):
                    raise answer
                return json.dumps({"results": {"bindings": [
                    {"this": {"type": "uri", "value": node}} for node in answer]}}).encode()
        return b'{"results": {"bindings": []}}'

    def stop(self):
        self.stopped = True


def _write_shapes(body: str) -> Path:
    directory = Path(tempfile.mkdtemp())
    path = directory / "shapes.ttl"
    path.write_text(
        "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix ex: <http://example.org/> .\n" + body, encoding="utf-8")
    return path


class QleverLayerTests(VerboseTestCase):
    SHAPES = """
        ex:One a sh:NodeShape ; sh:targetClass ex:C ;
          sh:sparql [ sh:message "one" ; sh:select "SELECT $this WHERE { $this ex:one ?o }" ] .
        ex:Two a sh:NodeShape ; sh:targetClass ex:C ; sh:severity sh:Warning ;
          sh:sparql [ sh:message "two" ; sh:select "SELECT $this WHERE { $this ex:two ?o }" ] .
    """

    def _run(self, answers, *, ontology=None):
        _FakeIndex.instances = []
        _FakeIndex.answers = answers
        _rest, constraints = S.split_shapes([_write_shapes(self.SHAPES)])
        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(V, "ShaclQleverIndex", _FakeIndex):
            source = Path(td) / "g.nt"
            source.write_text("<http://example.org/a> <http://example.org/one> <http://example.org/b> .\n")
            report = V.validate_shacl_sparql_qlever(
                source, constraints, ontology, work=Path(td), raw_dir=Path(td) / "raw", options={})
            written = sorted(path.name for path in (Path(td) / "raw").iterdir())
        return report, written

    def test_violations_become_results_and_the_queries_are_kept(self):
        report, written = self._run({"ex:one": ["http://example.org/a"]})
        self.assertEqual(report["status"], "PASS")
        self.assertEqual([row["focusNodes"] for row in report["constraints"]], [1, 0])
        (text,) = report["resultTexts"]
        self.assertIn("Severity: sh:Violation", text)
        self.assertIn("Message: one", text)
        self.assertEqual(written, ["c00.rq", "c01.rq", "constraints.json"])
        self.assertTrue(_FakeIndex.instances[0].stopped)

    def test_a_timeout_or_an_error_leaves_the_layer_incomplete(self):
        import socket

        report, _ = self._run({
            "ex:one": urllib.error.URLError(socket.timeout("timed out")),
            "ex:two": urllib.error.HTTPError("http://x", 500, "x", {}, BytesIO(b"out of memory")),
        })
        self.assertEqual(report["status"], "EXECUTION_FAILED")
        self.assertEqual([row["status"] for row in report["constraints"]], ["TIMEOUT", "EXECUTION_FAILED"])
        self.assertEqual(report["constraints"][1]["error"], "out of memory")

    def test_an_index_that_cannot_be_built_says_how_to_get_the_old_path(self):
        report, _ = self._run("fail")
        self.assertEqual(report["status"], "EXECUTION_FAILED")
        self.assertIn("--shacl-sparql-engine pyshacl", report["error"])
        self.assertTrue(_FakeIndex.instances[0].stopped)

    def test_the_vocabulary_and_its_typing_are_indexed(self):
        report, _ = self._run({}, ontology=ONTOLOGY)
        index = _FakeIndex.instances[0]
        self.assertEqual([path.name for path in index.sources], ["g.nt", "vocabulary.nt"])
        self.assertIsInstance(index.vocabulary, S.Vocabulary)
        self.assertEqual(report["inputTriples"], 1)

    def test_without_a_vocabulary_nothing_is_typed(self):
        self._run({})
        index = _FakeIndex.instances[0]
        self.assertEqual([path.name for path in index.sources], ["g.nt"])
        self.assertIsNone(index.vocabulary)

    def test_the_index_builder_reads_stdin(self):
        index = V.ShaclQleverIndex([Path("/x/g.nt.gz")], None, raw_dir=Path("/x/raw"),
                                   scratch=Path("/x"), options={"memory_gb": 3})
        command = index.index_command("qlever-index")
        self.assertEqual(command[command.index("-f") + 1], "-")
        self.assertEqual(command[-2:], ["-m", "3G"])


class ValidateShaclSplitTests(VerboseTestCase):
    """validate_shacl hands pyshacl everything but the SPARQL constraints."""

    def _validate(self, sparql_report, *, engine="qlever", shapes=FULL):
        seen = {}

        def task(arguments):
            seen["shapes"] = arguments[2]
            seen["graph"] = S.merge_graphs(arguments[2])
            return True, "Validation Report\nConforms: True\n"

        with tempfile.TemporaryDirectory() as td, \
                mock.patch.dict("sys.modules", {"pyshacl": mock.MagicMock()}), \
                mock.patch.object(V, "_validate_shacl_task", side_effect=task), \
                mock.patch.object(V, "validate_shacl_sparql_qlever",
                                  return_value=dict(sparql_report)) as layer:
            source = Path(td) / "g.nt"
            source.write_text("<http://e/a> <http://e/p> <http://e/b> .\n")
            result = V.validate_shacl(source, list(shapes), Path(td), ONTOLOGY,
                                      sparql_engine=engine, qlever_options={"port": 7019})
        return result, seen, layer

    def test_pyshacl_never_sees_a_sparql_constraint(self):
        result, seen, layer = self._validate({"status": "PASS", "constraints": [], "resultTexts": []})
        self.assertNotIn((None, SH.sparql, None), seen["graph"])
        layer.assert_called_once()
        self.assertEqual(len(layer.call_args[0][1]), 40)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["sparqlEngine"], "qlever")
        self.assertNotIn("resultTexts", result["sparqlConstraints"])

    def test_a_sparql_violation_fails_the_shapes(self):
        constraint = S.SparqlConstraint(shape="http://e/S", message="m", severity="Violation",
                                        select="", targets=[])
        result, _seen, _layer = self._validate({
            "status": "PASS", "constraints": [],
            "resultTexts": [S.result_text(constraint, "<http://e/a>")]})
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["violationCount"], 1)
        self.assertFalse(result["conforms"])

    def test_a_sparql_warning_is_an_advisory(self):
        constraint = S.SparqlConstraint(shape="http://e/S", message="m", severity="Warning",
                                        select="", targets=[])
        result, _seen, _layer = self._validate({
            "status": "PASS", "constraints": [],
            "resultTexts": [S.result_text(constraint, "<http://e/a>")]})
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["advisoryCount"], 1)

    def test_an_unrun_constraint_is_not_a_pass(self):
        result, _seen, _layer = self._validate({
            "status": "EXECUTION_FAILED", "resultTexts": [],
            "constraints": [{"id": "c03", "status": "TIMEOUT"}]})
        self.assertEqual(result["status"], "EXECUTION_FAILED")
        self.assertIn("c03 TIMEOUT", result["error"])

    def test_pyshacl_mode_is_the_old_path(self):
        result, seen, layer = self._validate({}, engine="pyshacl")
        layer.assert_not_called()
        self.assertIn((None, SH.sparql, None), seen["graph"])
        self.assertNotIn("sparqlConstraints", result)

    def test_the_core_profile_needs_no_index(self):
        _result, _seen, layer = self._validate({}, shapes=FULL[:1])
        layer.assert_not_called()


class WrapperTests(VerboseTestCase):
    def test_the_full_profile_applies_at_any_size_on_qlever(self):
        import vcf_rdfizer

        for size in (vcf_rdfizer.FULL_SHACL_MAX_SOURCE_BYTES + 1, 200 * 1024 ** 3, None):
            with self.subTest(size=size):
                self.assertTrue(vcf_rdfizer.shacl_default_applies(size, "full", "qlever"))
                self.assertFalse(vcf_rdfizer.shacl_default_applies(size, "full", "pyshacl"))

    def test_the_wrapper_and_the_runner_agree_on_the_choices(self):
        import vcf_rdfizer

        self.assertEqual(vcf_rdfizer.SHACL_SPARQL_ENGINE_CHOICES, V.SHACL_SPARQL_ENGINES)
        self.assertEqual(vcf_rdfizer.DEFAULT_SHACL_SPARQL_ENGINE, V.DEFAULT_SHACL_SPARQL_ENGINE)


if __name__ == "__main__":
    unittest.main()
