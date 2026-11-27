"""Mutation testing for the semantic validation suite.

Each catalogued mutation is applied to a correct graph, the real validation
queries are evaluated over the result, and the shipped decision logic
(``evaluate_validation``) is asked for a verdict. A mutation is "detected" when
that verdict stops being PASS.

Queries run under rdflib here so the harness needs no Docker and stays fast
enough for the normal test loop. rdflib is a third independent engine, which
also cross-checks that the queries are not accidentally engine-specific; the
container job in ``.github/workflows/validation-mutation.yml`` replays the same
catalogue under Comunica and QLever as the authority.

The run writes ``mutation-score.json`` next to the repository root when
``VCF_RDFIZER_MUTATION_REPORT`` is set, which is the artifact the coverage
documentation quotes.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import tempfile
import unittest
from pathlib import Path

from test import validation_fixtures as fixtures
from test import validation_mutations as mutations
from test.helpers import VerboseTestCase

try:
    import rdflib
except ImportError:  # pragma: no cover - optional test-only dependency
    rdflib = None

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = REPO_ROOT / "src" / "validation" / "validation_runner.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("validation_runner_mutation", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()


class RdflibEngine:
    """Evaluate the validation queries in-process and emit SPARQL Results JSON.

    The output is written in exactly the shape the real engines produce, so the
    normalization and comparison layers below are the shipped ones, unmodified.
    """

    def __init__(self, graph_text: str, raw_dir: Path):
        self.graph = rdflib.Graph()
        self.graph.parse(data=graph_text, format="nt")
        self.raw_dir = raw_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def execute(self, query_id: str, query_path: Path) -> dict:
        raw_path = self.raw_dir / f"{query_id}.sparql.json"
        try:
            result = self.graph.query(query_path.read_text(encoding="utf-8"))
            raw_path.write_bytes(result.serialize(format="json"))
            return {"status": "PASS", "exitCode": 0, "rawResult": str(raw_path),
                    "engine": "rdflib"}
        except Exception as error:  # noqa: BLE001 - surfaced as EXECUTION_FAILED
            raw_path.write_text("{}", encoding="utf-8")
            return {"status": "EXECUTION_FAILED", "exitCode": 1,
                    "rawResult": str(raw_path), "engine": "rdflib", "error": str(error)}


def shacl_profile_paths() -> list[Path] | None:
    """The bundled shape profiles named by VCF_RDFIZER_MUTATION_SHACL, or None.

    The query layer and the shape layer catch different things, and until now
    this harness measured only the first: every score it has ever reported is a
    query-only score. Setting the variable to `full` or `core` folds the shape
    verdict in, which is what makes the shape layer's contribution measurable
    instead of predicted.
    """
    profile = os.environ.get("VCF_RDFIZER_MUTATION_SHACL", "").strip().lower()
    if profile not in {"core", "full"}:
        return None
    import vcf_rdfizer

    repo_root = Path(vcf_rdfizer.__file__).resolve().parent
    shapes, _ontology = vcf_rdfizer.resolve_default_shacl_shapes(repo_root, profile)
    return shapes


def shacl_sparql_engine() -> str:
    """Where the profile's SPARQL constraints run: VCF_RDFIZER_MUTATION_SHACL_ENGINE.

    ``pyshacl`` (default) needs nothing beyond pyshacl, so it runs on any host.
    ``qlever`` is the validator's default path and needs the image's QLever.
    """
    engine = os.environ.get("VCF_RDFIZER_MUTATION_SHACL_ENGINE", "").strip().lower()
    return engine if engine in V.SHACL_SPARQL_ENGINES else "pyshacl"


_SHACL_FIELD = re.compile(r"^\s*(Severity|Focus Node|Source Shape|Result Path|Message):\s*(.*)$")


def _shacl_term(value: str) -> str:
    """One spelling per term: IRIs without brackets, every blank node alike."""
    value = value.strip()
    if value.startswith("<") and value.endswith(">"):
        return value[1:-1]
    prefix, colon, local = value.partition(":")
    namespaces = {"vcfc": V.VCFC, "sh": "http://www.w3.org/ns/shacl#",
                  "xsd": "http://www.w3.org/2001/XMLSchema#"}
    if colon and prefix in namespaces:
        return namespaces[prefix] + local
    if value.startswith("_:") or re.fullmatch(r"[A-Za-z]?[0-9a-f]{16,}", value):
        return "_:b"
    return value


def shacl_violation_keys(report_text: str) -> list[tuple[str, ...]]:
    """The blocking results of a SHACL report, comparable across graphs and runs.

    Keyed by constraint component, focus node, source shape, path and message.
    The value is left out on purpose: a mutation that only changes the value of
    a check the control already fails has not been detected by that check.
    """
    keys = set()
    for entry in V.parse_shacl_results(report_text):
        if entry["severity"] not in ("Violation", "Unknown"):
            continue
        lines = entry["text"].splitlines()
        fields: dict[str, str] = {}
        for line in lines[1:]:
            match = _SHACL_FIELD.match(line)
            if match and match.group(1) not in fields:
                fields[match.group(1)] = match.group(2).strip()
        keys.add((
            lines[0].split("(", 1)[-1].rstrip("):"),
            _shacl_term(fields.get("Focus Node", "")),
            _shacl_term(fields.get("Source Shape", "")),
            _shacl_term(fields.get("Result Path", "")),
            fields.get("Message", ""),
        ))
    return sorted(keys)


def shacl_ontology_path() -> Path | None:
    import vcf_rdfizer

    repo_root = Path(vcf_rdfizer.__file__).resolve().parent
    _shapes, ontology = vcf_rdfizer.resolve_default_shacl_shapes(repo_root, "core")
    return ontology


def validate_graph(
    graph_text: str, representation: str, *, strict_conformance: bool = False,
    include_qual: bool = True, mapping_policy: str = "strict",
    shacl_shapes: list[Path] | None = None,
    shacl_baseline: list | None = None,
) -> dict:
    """Run the whole validation decision over one graph.

    With ``shacl_shapes``, the shape verdict counts as a detection only when the
    graph has a blocking result its unmutated control (``shacl_baseline``) does
    not. A control can fail a profile on its own -- the expanded one does, see
    docs/shacl-engines.md -- and a check that already fails on the clean graph
    cannot tell a corrupted one from it.
    """
    parser = fixtures.parser_summary(representation, include_qual=include_qual)
    query_dir = V.QUERY_ROOT / representation
    with tempfile.TemporaryDirectory() as td:
        engine = RdflibEngine(graph_text, Path(td) / "raw")
        executions = {
            query_id: engine.execute(query_id, V.query_path(query_dir, query_id))
            for query_id in V.PREFLIGHT_QUERIES + V.PREFLIGHT_COUNT_QUERIES + V.CORE_QUERIES
        }
        if any(item["status"] != "PASS" for item in executions.values()):
            failed = {k: v.get("error", "") for k, v in executions.items() if v["status"] != "PASS"}
            return {"status": "EXECUTION_FAILED", "queryErrors": failed}
        # rapper is a container tool, so the harness supplies the parsed
        # statement count itself. For well-formed N-Triples that is exactly the
        # number of non-empty lines, which is what rapper would report.
        parsed = sum(1 for line in graph_text.splitlines() if line.strip())
        verdict = V.evaluate_validation(
            executions, parser, representation, strict_conformance=strict_conformance,
            mapping_policy=mapping_policy, parsed_triple_count=parsed,
        )
        if shacl_shapes:
            graph_path = Path(td) / "graph.nt"
            graph_path.write_text(graph_text, encoding="utf-8")
            results_dir = Path(td) / "shacl"
            results_dir.mkdir()
            shacl = V.validate_shacl(
                graph_path, shacl_shapes, results_dir, shacl_ontology_path(),
                sparql_engine=shacl_sparql_engine(), scratch_dir=Path(td),
                qlever_options={"port": int(os.environ.get("VCF_RDFIZER_MUTATION_QLEVER_PORT", "7019")),
                                "memory_gb": 2, "query_timeout": 600},
            )
            report = results_dir / "shacl-report.txt"
            keys = shacl_violation_keys(report.read_text(encoding="utf-8")) if report.exists() else []
            new = [key for key in keys if key not in set(map(tuple, shacl_baseline or []))]
            verdict = dict(verdict)
            verdict["shacl"] = shacl
            verdict["shaclViolationKeys"] = keys
            verdict["newShaclViolations"] = new
            # A shape violation is a detection in its own right when it is new.
            # An execution failure is NOT: pyshacl being absent or erroring
            # would otherwise inflate the score with detections nothing made.
            if shacl.get("status") == "FAIL" and new and verdict["status"] == "PASS":
                verdict["status"] = "SHACL_VIOLATION"
        return verdict


@unittest.skipIf(rdflib is None, "rdflib is required for the host mutation harness")
class MutationHarnessTests(VerboseTestCase):
    """The harness itself must agree with a correct graph before it can judge."""

    def test_unmutated_expanded_graph_passes(self):
        """A correct expanded graph validates cleanly against the fixture oracle."""
        verdict = validate_graph(fixtures.build_graph("expanded"), "expanded")
        self.assertEqual(verdict["status"], "PASS", json.dumps(verdict.get("comparison"), indent=2)[:2000])

    def test_unmutated_condensed_graph_passes(self):
        """The same holds for the condensed representation."""
        verdict = validate_graph(fixtures.build_graph("condensed"), "condensed")
        self.assertEqual(verdict["status"], "PASS", json.dumps(verdict.get("comparison"), indent=2)[:2000])

    def test_shacl_keys_ignore_values_blank_labels_and_warnings(self):
        """Detection compares checks, not the value or the parser's blank-node labels."""
        def block(value, shape, severity="Violation"):
            return (f"Validation Result in MinCountConstraintComponent "
                    f"(http://www.w3.org/ns/shacl#MinCountConstraintComponent):\n"
                    f"\tSeverity: sh:{severity}\n\tSource Shape: {shape}\n"
                    f"\tFocus Node: <file://f#record/1>\n\tValue Node: {value}\n"
                    f"\tResult Path: vcfc:pos\n\tMessage: m\n")
        one = shacl_violation_keys(block('"1"', "_:N0a1b2c3d4e5f60718"))
        two = shacl_violation_keys(block('"2"', "_:N9f8e7d6c5b4a30201")
                                   + block('"3"', "_:Nffff", severity="Warning"))
        self.assertEqual(one, two)
        self.assertEqual(one[0][3], V.VCFC + "pos")

    def test_fixture_graph_and_oracle_describe_the_same_data(self):
        """A guard that the fixture's two halves cannot drift apart silently."""
        oracle = fixtures.parser_summary("expanded")
        self.assertEqual(oracle["totalRecords"], len(fixtures.RECORDS))
        self.assertEqual(oracle["headerLineCount"], len(fixtures.HEADER_LINES))
        self.assertEqual(
            sum(row["recordCount"] for row in oracle["q01_record_density_1mb"]),
            oracle["totalRecords"],
        )


@unittest.skipIf(rdflib is None, "rdflib is required for the host mutation harness")
class MutationDetectionTests(VerboseTestCase):
    """Every catalogued mutation, and whether the suite notices it."""

    @classmethod
    def setUpClass(cls):
        cls.graphs: dict[tuple, str] = {}
        cls.results: list[dict] = []
        cls.controls: dict[tuple, dict] = {}

    @classmethod
    def control_for(cls, representation: str, options: tuple) -> dict:
        """The shape results of the unmutated graph a mutation starts from."""
        key = (representation, options)
        if key not in cls.controls:
            verdict = validate_graph(
                cls.graph_for(representation, options), representation,
                include_qual=dict(options).get("include_qual", True),
                shacl_shapes=cls.shacl_shapes,
            )
            cls.controls[key] = {
                "representation": representation, "graphOptions": dict(options),
                "queryStatus": verdict["status"],
                "shaclStatus": verdict.get("shacl", {}).get("status"),
                "shaclViolationKeys": verdict.get("shaclViolationKeys", []),
            }
        return cls.controls[key]

    @classmethod
    def graph_for(cls, representation: str, options: tuple) -> str:
        """Build (and cache) the fixture graph a mutation needs."""
        key = (representation, options)
        if key not in cls.graphs:
            cls.graphs[key] = fixtures.build_graph(representation, **dict(options))
        return cls.graphs[key]

    #: Resolved once. None unless VCF_RDFIZER_MUTATION_SHACL names a profile,
    #: so the default run stays exactly the query-only measurement it has
    #: always been and every archived score remains comparable.
    shacl_shapes = shacl_profile_paths()

    @classmethod
    def tearDownClass(cls):
        detected = [r for r in cls.results if r["detected"]]
        by_shacl = [r for r in cls.results if r.get("detectedByShacl")]
        report = {
            "total": len(cls.results),
            "detected": len(detected),
            "score": round(len(detected) / len(cls.results), 4) if cls.results else 0.0,
            "knownUndetected": [r["id"] for r in cls.results if not r["detected"]],
            # Which layer did the detecting. Without this the score is one
            # number that cannot be attributed, and the shape layer's
            # contribution stays a claim rather than a measurement.
            "shaclProfile": os.environ.get("VCF_RDFIZER_MUTATION_SHACL") or None,
            "shaclSparqlEngine": shacl_sparql_engine() if cls.shacl_shapes else None,
            # What each unmutated graph already reports. SHACL detection is
            # counted against these, never against an assumed-clean control.
            "controls": list(cls.controls.values()),
            "detectedByShacl": len(by_shacl),
            "detectedByShaclIds": sorted({r["id"] for r in by_shacl}),
            "detectedByQueriesOnly": len(detected) - len(by_shacl),
            "mutations": cls.results,
        }
        destination = os.environ.get("VCF_RDFIZER_MUTATION_REPORT")
        if destination:
            Path(destination).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(
            f"\n  mutation score: {report['detected']}/{report['total']} "
            f"({report['score']:.0%} detected)"
        )

    def _check(self, mutation: mutations.Mutation, representation: str):
        options = dict(mutation.graph_options)
        original = self.graph_for(representation, mutation.graph_options)
        mutated = mutation.apply(original)
        self.assertNotEqual(mutated, original, f"{mutation.id} did not change the graph")
        # The conformance mutation is only a failure under the strict policy.
        strict = mutation.id == "plain_dot_literal"
        baseline = (self.control_for(representation, mutation.graph_options)["shaclViolationKeys"]
                    if self.shacl_shapes else None)
        verdict = validate_graph(
            mutated, representation, strict_conformance=strict,
            include_qual=options.get("include_qual", True),
            mapping_policy=mutation.mapping_policy,
            shacl_shapes=self.shacl_shapes,
            shacl_baseline=baseline,
        )
        detected = verdict["status"] != "PASS"
        detected_by_shacl = verdict["status"] == "SHACL_VIOLATION"
        self.results.append({
            "id": mutation.id,
            "representation": representation,
            "vcfElement": mutation.vcf_element,
            "expectedDetectedBy": mutation.expected_detected_by,
            "detected": detected,
            "detectedByShacl": detected_by_shacl,
            "status": verdict["status"],
            "shaclStatus": verdict.get("shacl", {}).get("status"),
            "newShaclViolations": verdict.get("newShaclViolations"),
            "knownUndetected": mutation.known_undetected,
            "graphOptions": dict(mutation.graph_options),
        })
        if mutation.known_undetected:
            if self.shacl_shapes and detected_by_shacl:
                # Closing one of these is the entire point of enabling the
                # shape layer, so it is recorded rather than failed. The
                # catalogue still describes the QUERY layer's coverage, which
                # is what an unqualified run measures.
                return
            self.assertFalse(
                detected,
                f"{mutation.id} is now DETECTED. This gap has been closed - remove "
                f"`known_undetected` from the catalogue and update "
                f"docs/vcf-coverage.md. Recorded reason was: {mutation.known_undetected}",
            )
        else:
            self.assertTrue(
                detected,
                f"{mutation.id} was NOT detected (status={verdict['status']}). "
                f"Expected {mutation.expected_detected_by} to catch it.",
            )


def _attach_mutation_tests():
    """Generate one test per (mutation, representation) so failures are named."""
    for representation in ("expanded", "condensed"):
        for mutation in mutations.for_representation(representation):
            def test(self, _m=mutation, _r=representation):
                self._check(_m, _r)

            test.__doc__ = f"[{representation}] {mutation.description}"
            setattr(MutationDetectionTests, f"test_{representation}_{mutation.id}", test)


_attach_mutation_tests()


if __name__ == "__main__":
    unittest.main()
