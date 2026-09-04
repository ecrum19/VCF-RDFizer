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


def validate_graph(
    graph_text: str, representation: str, *, strict_conformance: bool = False,
    include_qual: bool = True, mapping_policy: str = "strict",
) -> dict:
    """Run the whole validation decision over one graph."""
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
        return V.evaluate_validation(
            executions, parser, representation, strict_conformance=strict_conformance,
            mapping_policy=mapping_policy, parsed_triple_count=parsed,
        )


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

    @classmethod
    def graph_for(cls, representation: str, options: tuple) -> str:
        """Build (and cache) the fixture graph a mutation needs."""
        key = (representation, options)
        if key not in cls.graphs:
            cls.graphs[key] = fixtures.build_graph(representation, **dict(options))
        return cls.graphs[key]

    @classmethod
    def tearDownClass(cls):
        detected = [r for r in cls.results if r["detected"]]
        report = {
            "total": len(cls.results),
            "detected": len(detected),
            "score": round(len(detected) / len(cls.results), 4) if cls.results else 0.0,
            "knownUndetected": [r["id"] for r in cls.results if not r["detected"]],
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
        verdict = validate_graph(
            mutated, representation, strict_conformance=strict,
            include_qual=options.get("include_qual", True),
            mapping_policy=mutation.mapping_policy,
        )
        detected = verdict["status"] != "PASS"
        self.results.append({
            "id": mutation.id,
            "representation": representation,
            "vcfElement": mutation.vcf_element,
            "expectedDetectedBy": mutation.expected_detected_by,
            "detected": detected,
            "status": verdict["status"],
            "knownUndetected": mutation.known_undetected,
            "graphOptions": dict(mutation.graph_options),
        })
        if mutation.known_undetected:
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
