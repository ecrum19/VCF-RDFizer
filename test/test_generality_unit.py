"""The plug-ins stay general: no shipped code or profile names the paper's use case.

Policies, gene panels, cohorts and requesters are configuration. If one of
them leaks into the engine or a bundled profile, the tool silently becomes a
demo. This fails when that happens (workstream A, section 2 of the plan).
"""

from pathlib import Path
import re
import unittest

from test.helpers import VerboseTestCase

ROOT = Path(__file__).resolve().parents[1]
SHIPPED = [*ROOT.glob("vcf_rdfizer_policies/*.py"), ROOT / "vcf_rdfizer_policy.py",
           *ROOT.glob("vcf_rdfizer_linking/*.py"), ROOT / "vcf_rdfizer_link.py",
           *ROOT.glob("vcf_rdfizer_data/policy/*.ttl"), *ROOT.glob("vcf_rdfizer_data/linkers/*/linker.ttl")]
#: The use case's vocabulary: its question, its data sources, its participants.
FORBIDDEN = re.compile(r"ACMG|ClinVar|NB72462M|NG131FQA1I|HG00[245]\b|secondary.findings", re.IGNORECASE)


class GeneralityTests(VerboseTestCase):
    def test_no_shipped_module_or_profile_names_the_use_case(self):
        self.assertTrue(SHIPPED)
        hits = [f"{path.relative_to(ROOT)}:{n}: {line.strip()}" for path in SHIPPED
                for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
                if FORBIDDEN.search(line)]
        self.assertEqual(hits, [])


if __name__ == "__main__":
    unittest.main()
