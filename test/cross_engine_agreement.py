#!/usr/bin/env python3
"""Assert every validation query returns the same values on every engine.

Run inside the VCF-RDFizer image, where Comunica and QLever both exist:

    docker run --rm -v "$PWD:/repo:ro" <image> \\
      /opt/pycottas-venv/bin/python /repo/test/cross_engine_agreement.py

This exists because an engine disagreement is silent and severe: QLever
canonicalises numeric literals at index time, which once made a POS datatype
preflight fail every QLever run while passing on Comunica. Only the values the
validator actually consumes are compared - a store is free to report its own
datatype IRI for a count, and the normalization layer is datatype-agnostic by
design.

Exits non-zero on the first disagreement, printing both sides.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


def load_runner():
    path = REPO_ROOT / "src" / "validation" / "validation_runner.py"
    spec = importlib.util.spec_from_file_location("validation_runner_agreement", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


V = load_runner()
ENGINES = ("comunica", "qlever")


def evaluate(engine_name: str, representation: str, source: Path, scratch: Path) -> dict:
    results: dict[str, object] = {}
    raw_dir = scratch / f"raw-{engine_name}-{representation}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    engine = V.build_engine(
        engine_name, source, raw_dir=raw_dir, scratch=scratch,
        options={"memory_gb": 2, "startup_timeout": 300, "query_timeout": 300},
    )
    with engine:
        for query_id in V.PREFLIGHT_QUERIES + V.PREFLIGHT_COUNT_QUERIES + V.CORE_QUERIES:
            execution = engine.execute(query_id, V.query_path(V.QUERY_ROOT / representation, query_id))
            if execution["status"] != "PASS":
                results[query_id] = {
                    "__executionFailed__": Path(execution["stderr"]).read_text(
                        encoding="utf-8", errors="replace")[:400]
                }
                continue
            raw = Path(execution["rawResult"])
            if query_id in V.QUERY_SCHEMAS:
                results[query_id] = V.normalize(query_id, raw)
            else:
                results[query_id] = [
                    {name: binding["value"] for name, binding in row.items()}
                    for row in V.bindings(raw)
                ]
    return results


def verdict_for(engine_name: str, representation: str, source: Path, scratch: Path) -> dict:
    """Run the shipped validation decision for one engine against the fixture."""
    from test import validation_fixtures as fixtures

    raw_dir = scratch / f"verdict-{engine_name}-{representation}"
    raw_dir.mkdir(parents=True, exist_ok=True)
    engine = V.build_engine(
        engine_name, source, raw_dir=raw_dir, scratch=scratch,
        options={"memory_gb": 2, "startup_timeout": 300, "query_timeout": 300},
    )
    with engine:
        executions = {
            query_id: engine.execute(query_id, V.query_path(V.QUERY_ROOT / representation, query_id))
            for query_id in V.PREFLIGHT_QUERIES + V.PREFLIGHT_COUNT_QUERIES + V.CORE_QUERIES
        }
    # Supply the parsed statement count so the duplicate check is exercised
    # here too; for well-formed N-Triples it equals the non-empty line count,
    # which is what rapper reports on the real path.
    parsed = sum(
        1 for line in source.read_text(encoding="utf-8").splitlines() if line.strip()
    )
    return V.evaluate_validation(
        executions, fixtures.parser_summary(representation), representation,
        parsed_triple_count=parsed,
    )


def main() -> int:
    from test import validation_fixtures as fixtures

    disagreements = 0
    with tempfile.TemporaryDirectory(dir="/work") as td:
        scratch = Path(td)
        for representation in ("expanded", "condensed"):
            source = scratch / f"{representation}.nt"
            source.write_text(fixtures.build_graph(representation), encoding="utf-8")
            per_engine = {
                engine: evaluate(engine, representation, source, scratch) for engine in ENGINES
            }
            print(f"\n=== {representation} ===")
            for query_id in per_engine[ENGINES[0]]:
                values = [per_engine[engine][query_id] for engine in ENGINES]
                if all(value == values[0] for value in values):
                    print(f"  {query_id:44s} agree")
                    continue
                disagreements += 1
                print(f"  {query_id:44s} *** DIFFER ***")
                for engine, value in zip(ENGINES, values):
                    print(f"      {engine:9s} {json.dumps(value)[:300]}")

            # Engines agreeing with each other is not enough: the digests and
            # censuses are compared against values Python computes, so each
            # engine must also agree with that oracle. Running the real
            # decision proves it end to end.
            for engine_name in ENGINES:
                verdict = verdict_for(engine_name, representation, source, scratch)
                status = verdict["status"]
                print(f"  {'full validation verdict: ' + engine_name:44s} {status}")
                if status != "PASS":
                    disagreements += 1
                    print(f"      {json.dumps(verdict.get('comparison'))[:500]}")

    if disagreements:
        print(f"\n{disagreements} disagreement(s) found.")
        return 1
    print("\nAll validation queries agree across " + ", ".join(ENGINES) + ",")
    print("and every engine agrees with the Python oracle.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
