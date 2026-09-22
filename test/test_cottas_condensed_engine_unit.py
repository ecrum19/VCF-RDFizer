"""COTTAS must stay in the engine set for a condensed graph.

There was a guard here that refused `--validation-engine cottas` against a
condensed graph, and silently dropped cottas from `--validation-engine all`.
It was right when it was written: native pycottas answered 18 of 27 queries and
then ran 41 hours, and ~10 hours on a second attempt, on
q05_sample_genotype_counts without finishing.

That engine is gone. COTTAS is now answered through comunica over DuckDB, and
the same cell -- test-10k, condensed, all four engines -- completes in 99.8
minutes with every engine agreeing on every artifact.

The guard therefore has to go too, and the danger is specific: 06_equivalence
runs with `--validation-engine all`, so a surviving drop would remove cottas
from exactly the two condensed cells the experiment exists to compare, and do it
with a warning rather than a failure. The result would look complete and be
missing an engine. These tests exist so that cannot come back quietly.
"""
from __future__ import annotations

import unittest
from pathlib import Path

import vcf_rdfizer
from test.helpers import VerboseTestCase


class CottasCondensedEngineTests(VerboseTestCase):
    def test_no_condensed_guard_survives(self):
        """Any reintroduction must be deliberate, not a merge artifact."""
        for name in (
            "cottas_condensed_resolution",
            "cottas_condensed_engine_rejection",
        ):
            self.assertFalse(
                hasattr(vcf_rdfizer, name),
                f"{name} is back: COTTAS on a condensed graph is supported now, "
                "and a silent drop would hollow out 06_equivalence",
            )

    def test_the_opt_in_flag_is_gone_with_it(self):
        """--allow-cottas-condensed only made sense while the refusal existed.

        The parser is built inside main(), so there is nothing to introspect;
        the source is the only thing to assert against.
        """
        source = Path(vcf_rdfizer.__file__).read_text(encoding="utf-8")
        self.assertNotIn("--allow-cottas-condensed", source)
        self.assertNotIn("allow_cottas_condensed", source)

    def test_all_keeps_every_engine_for_a_condensed_run(self):
        """`all` means all: no engine may be removed behind a warning."""
        engines = vcf_rdfizer.parse_validation_engines("all")
        self.assertIn("cottas", engines)
        self.assertEqual(
            sorted(engines), sorted(vcf_rdfizer.VALIDATION_ENGINE_CHOICES)
        )


if __name__ == "__main__":
    unittest.main()
