import csv
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

import vcf_rdfizer
import vcf_rdfizer_rules
from test.helpers import VerboseTestCase

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULES = REPO_ROOT / "rules" / "default_rules.ttl"
VCF_AS_TSV = REPO_ROOT / "src" / "vcf_as_tsv.sh"

SAMPLE_VCF = """\
##fileformat=VCFv4.2
##fileDate=20260101
##source=unittest
##reference=GRCh38
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tHG001\tHG002
20\t100\trs1\tA\tG\t50\tPASS\tAC=1\tGT\t0|1\t1/1
"""


def invoke(argv):
    """Run the rules CLI, returning (exit_code, stdout, stderr)."""
    out, err = StringIO(), StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = vcf_rdfizer_rules.main(argv)
    return code, out.getvalue(), err.getvalue()


class RulesHelperUnitTests(VerboseTestCase):
    def test_documented_columns_match_what_vcf_as_tsv_actually_writes(self):
        """The documented TSV schemas cannot drift from the generator script."""
        if sys.platform.startswith("win"):
            self.skipTest("vcf_as_tsv.sh requires a POSIX shell")
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            vcf_path = tmp_path / "sample.vcf"
            vcf_path.write_text(SAMPLE_VCF, encoding="utf-8")
            out_dir = tmp_path / "tsv"
            result = subprocess.run(
                ["bash", str(VCF_AS_TSV), str(vcf_path), str(out_dir)],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            emitted = {}
            for name in ("records", "header_lines", "file_metadata"):
                path = out_dir / f"sample.{name}.tsv"
                with path.open(newline="", encoding="utf-8") as handle:
                    emitted[name] = next(csv.reader(handle, delimiter="\t"))

            # The records header's final column is the whitespace-joined sample
            # ids, which is exactly why it is modelled as a dynamic tail.
            records = vcf_rdfizer_rules.TSV_SOURCES["records"]
            self.assertEqual(emitted["records"][:-1], list(records.columns))
            self.assertEqual(emitted["records"][-1], "HG001 HG002")

            for name in ("header_lines", "file_metadata"):
                self.assertEqual(
                    emitted[name],
                    list(vcf_rdfizer_rules.TSV_SOURCES[name].columns),
                    f"{name}.tsv header drifted from the documented columns",
                )

    def test_helper_table_columns_come_from_the_wrapper_constants(self):
        """The two helper tables reuse the wrapper's own header definitions."""
        self.assertEqual(
            list(vcf_rdfizer_rules.TSV_SOURCES["sample_calls"].columns),
            list(vcf_rdfizer.SAMPLE_CALLS_HEADER),
        )
        self.assertEqual(
            list(vcf_rdfizer_rules.TSV_SOURCES["sample_format_values"].columns),
            list(vcf_rdfizer.SAMPLE_FORMAT_HEADER),
        )

    def test_documented_source_paths_match_the_wrapper_rewrite_targets(self):
        """Every advertised source path is one render_rules_for_triplet rewrites."""
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            rendered = tmp_path / "rendered.ttl"
            vcf_rdfizer.render_rules_for_triplet(
                DEFAULT_RULES,
                rendered,
                "s.records.tsv",
                "s.header_lines.tsv",
                "s.file_metadata.tsv",
                "s.sample_calls.tsv",
                "s.sample_format_values.tsv",
            )
            text = rendered.read_text(encoding="utf-8")
        for path in vcf_rdfizer_rules.CANONICAL_SOURCE_PATHS:
            self.assertNotIn(
                f'csvw:url "{path}"',
                text,
                f"{path} survived rendering, so the wrapper does not rewrite it",
            )

    def test_check_accepts_the_shipped_default_mapping(self):
        """The default mapping satisfies the contract the checker enforces."""
        code, stdout, _ = invoke(["check", str(DEFAULT_RULES)])
        self.assertEqual(code, 0)
        self.assertIn("Result: OK", stdout)
        self.assertIn("--sample-representation condensed: usable", stdout)

    def test_check_rejects_non_canonical_source_paths(self):
        """A hard-coded TSV path the wrapper cannot rewrite is an error."""
        with tempfile.TemporaryDirectory() as td:
            rules = Path(td) / "custom.ttl"
            rules.write_text(
                DEFAULT_RULES.read_text(encoding="utf-8").replace(
                    "/data/tsv/records.tsv", "/data/tsv/my_records.tsv"
                ),
                encoding="utf-8",
            )
            code, stdout, _ = invoke(["check", str(rules)])
        self.assertEqual(code, 1)
        self.assertIn("Result: FAILED", stdout)
        self.assertIn("/data/tsv/my_records.tsv", stdout)

    def test_check_rejects_columns_no_tsv_provides(self):
        """A misspelled column reference is caught before the pipeline runs."""
        with tempfile.TemporaryDirectory() as td:
            rules = Path(td) / "custom.ttl"
            rules.write_text(
                DEFAULT_RULES.read_text(encoding="utf-8").replace(
                    'rml:reference "CHROM"', 'rml:reference "CHROMOSOME"'
                ),
                encoding="utf-8",
            )
            code, stdout, _ = invoke(["check", str(rules)])
        self.assertEqual(code, 1)
        self.assertIn("CHROMOSOME", stdout)

    def test_check_reports_template_variables_as_referenced_columns(self):
        """Columns used only inside rr:template are checked too."""
        with tempfile.TemporaryDirectory() as td:
            rules = Path(td) / "custom.ttl"
            rules.write_text(
                DEFAULT_RULES.read_text(encoding="utf-8").replace(
                    "file://{SOURCE_FILE}#call/{ROW_ID}",
                    "file://{SOURCE_FILE}#call/{NO_SUCH_COLUMN}",
                ),
                encoding="utf-8",
            )
            code, stdout, _ = invoke(["check", str(rules)])
        self.assertEqual(code, 1)
        self.assertIn("NO_SUCH_COLUMN", stdout)

    def test_check_warns_about_materialized_helper_tables(self):
        """A custom helper-table consumer is flagged, and blocks condensed mode."""
        with tempfile.TemporaryDirectory() as td:
            rules = Path(td) / "custom.ttl"
            rules.write_text(
                DEFAULT_RULES.read_text(encoding="utf-8")
                + """
<#ExtraSampleMap> a rr:TriplesMap;
  rml:logicalSource [
    rml:source [ a csvw:Table; csvw:url "/data/tsv/sample_calls.tsv" ];
    rml:referenceFormulation ql:CSV
  ];
  rr:subjectMap [ rr:template "file://{SOURCE_FILE}#extra/{ROW_ID}" ];
  rr:predicateObjectMap [
    rr:predicate vcfr:rawPayload;
    rr:objectMap [ rml:reference "SAMPLE_PAYLOAD" ]
  ].
""",
                encoding="utf-8",
            )
            code, stdout, _ = invoke(["check", str(rules)])
        # Materialization is a cost, not a contract violation.
        self.assertEqual(code, 0)
        self.assertIn("materialized", stdout)
        self.assertIn("--sample-representation condensed: NOT usable", stdout)

    def test_check_rejects_a_file_that_is_not_an_rml_mapping(self):
        """A Turtle file with no logical source fails rather than silently passing."""
        with tempfile.TemporaryDirectory() as td:
            rules = Path(td) / "plain.ttl"
            rules.write_text(
                "@prefix ex: <http://example.org/> .\nex:a ex:b ex:c .\n", encoding="utf-8"
            )
            code, stdout, _ = invoke(["check", str(rules)])
        self.assertEqual(code, 1)
        self.assertIn("does not look like an RML mapping", stdout)

    def test_check_emits_machine_readable_json(self):
        """--json exposes the same findings for scripted use."""
        code, stdout, _ = invoke(["check", str(DEFAULT_RULES), "--json"])
        payload = json.loads(stdout)
        self.assertEqual(code, 0)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["errors"], [])
        self.assertEqual(
            sorted(payload["sources"]), sorted(vcf_rdfizer_rules.CANONICAL_SOURCE_PATHS)
        )

    def test_init_scaffolds_a_checkable_mapping_and_refuses_to_clobber(self):
        """init produces a file that passes check, and never overwrites silently."""
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / "mine.ttl"
            code, _, _ = invoke(["init", "-o", str(output)])
            self.assertEqual(code, 0)
            self.assertTrue(output.is_file())
            self.assertIn("Custom VCF-RDFizer RML mapping", output.read_text(encoding="utf-8"))

            check_code, stdout, _ = invoke(["check", str(output)])
            self.assertEqual(check_code, 0, stdout)

            clobber_code, _, stderr = invoke(["init", "-o", str(output)])
            self.assertEqual(clobber_code, 2)
            self.assertIn("already exists", stderr)

            force_code, _, _ = invoke(["init", "-o", str(output), "--force"])
            self.assertEqual(force_code, 0)

    def test_columns_lists_every_source_and_supports_json(self):
        """columns documents all five sources the pipeline generates."""
        code, stdout, _ = invoke(["columns"])
        self.assertEqual(code, 0)
        for name, source in vcf_rdfizer_rules.TSV_SOURCES.items():
            self.assertIn(name, stdout)
            self.assertIn(source.container_path, stdout)

        code, stdout, _ = invoke(["columns", "--json"])
        payload = json.loads(stdout)
        self.assertEqual(code, 0)
        self.assertEqual(sorted(payload), sorted(vcf_rdfizer_rules.TSV_SOURCES))
        self.assertEqual(payload["records"]["dynamic_tail_column"], "SAMPLES")


if __name__ == "__main__":
    unittest.main()
