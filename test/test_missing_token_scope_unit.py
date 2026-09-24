"""The missing-token check must not report a header's own Number=. declaration.

``preflight_missing_token_conformance`` looks for bare "." literals that should
have been typed ``vcfc:Null``. It already excluded ``vcfc:fieldNumber`` and
``vcfc:genotypeString``, where a dot is conformant. The same token also reaches
the graph a third way: the structured-header attribute layer carries every
declaration's attributes verbatim, so ``Number=.`` becomes a
``vcfc:attributeValue`` of "." on the attribute whose key is "Number". The
shapes require that value to be exactly ``xsd:string``, so it can never be
``vcfc:Null``.

On the v3.1.0 benchmark the narrowed check still returned ten rows on the
100,000-record HG005 cell -- one per ``Number=.`` INFO declaration in its
header. These tests run both forms of the query against a graph produced by the
real header emitter, so the exclusion is checked end to end rather than by
string inspection alone.
"""

import tempfile
import unittest
from pathlib import Path

# The behavioural tests need rdflib to evaluate SPARQL; the rest of the suite
# runs without it, so they skip rather than erroring discovery -- the same
# pattern test_linking_unit.py and test_validation_mutation_unit.py use.
try:
    import rdflib
except ModuleNotFoundError:  # pragma: no cover - exercised only without rdflib
    rdflib = None

import vcf_rdfizer
from test.helpers import VerboseTestCase

VCFC = "https://w3id.org/vcf-core/vocab#"
QUERY_DIR = (
    Path(vcf_rdfizer.__file__).resolve().parent
    / "src" / "validation" / "queries" / "common"
)
SAMPLE_QUERY = QUERY_DIR / "preflight_missing_token_conformance.rq"
COUNT_QUERY = QUERY_DIR / "preflight_missing_token_conformance_count.rq"

HEADERS_TSV = (
    "SOURCE_FILE\tHEADER_INDEX\tHEADER_KEY\tHEADER_VALUE\tRAW_LINE\n"
    "s.vcf\t1\tfileformat\tVCFv4.2\tx\n"
    "s.vcf\t2\tINFO\t<ID=platformnames,Number=.,Type=String,Description=\"Platforms\">\tx\n"
    "s.vcf\t3\tINFO\t<ID=callsets,Number=1,Type=Integer,Description=\"Call sets\">\tx\n"
    "s.vcf\t4\tFORMAT\t<ID=AD,Number=.,Type=Integer,Description=\"Allelic depths\">\tx\n"
)


def header_graph(tmp_path: Path) -> "rdflib.Graph":
    """Emit the structured header layer exactly as a conversion would."""
    headers_tsv = tmp_path / "s.header_lines.tsv"
    headers_tsv.write_text(HEADERS_TSV, encoding="utf-8")
    rdf_path = tmp_path / "s.nt"
    rdf_path.write_text("", encoding="utf-8")
    vcf_rdfizer.append_header_representation_rdf(headers_tsv, rdf_path)
    graph = rdflib.Graph()
    graph.parse(rdf_path, format="nt")
    return graph


def sample_rows(graph) -> list:
    return list(graph.query(SAMPLE_QUERY.read_text(encoding="utf-8")))


def anomaly_count(graph) -> int:
    rows = list(graph.query(COUNT_QUERY.read_text(encoding="utf-8")))
    return int(rows[0][0])


@unittest.skipIf(rdflib is None, "rdflib is required to evaluate the queries")
class HeaderNumberAttributeTests(VerboseTestCase):
    """The header's Number=. is a declaration, not a missing value."""

    def test_the_emitter_really_writes_the_dot_this_test_is_about(self):
        """Guard the premise: without it the other tests would pass vacuously."""
        with tempfile.TemporaryDirectory() as td:
            graph = header_graph(Path(td))
        number_values = {
            str(value)
            for attribute, _, key in graph.triples(
                (None, rdflib.URIRef(VCFC + "attributeKey"), None))
            if str(key) == "Number"
            for value in graph.objects(attribute, rdflib.URIRef(VCFC + "attributeValue"))
        }
        self.assertIn(".", number_values)

    def test_number_dot_declarations_are_not_reported(self):
        with tempfile.TemporaryDirectory() as td:
            graph = header_graph(Path(td))
        self.assertEqual(sample_rows(graph), [])
        self.assertEqual(anomaly_count(graph), 0)

    def test_a_bare_dot_elsewhere_is_still_reported(self):
        """The exclusion is the Number attribute only, not the whole layer."""
        with tempfile.TemporaryDirectory() as td:
            graph = header_graph(Path(td))
        # A genuinely untyped missing value on a record field ...
        graph.add((
            rdflib.URIRef("file://s.vcf#record/1"),
            rdflib.URIRef(VCFC + "recordId"),
            rdflib.Literal("."),
        ))
        # ... and a dot on a header attribute that is not Number.
        attribute = rdflib.URIRef("file://s.vcf#header/line/3/attribute/9")
        graph.add((attribute, rdflib.URIRef(VCFC + "attributeKey"), rdflib.Literal("Source")))
        graph.add((attribute, rdflib.URIRef(VCFC + "attributeValue"), rdflib.Literal(".")))

        reported = {(str(row[0]), str(row[1])) for row in sample_rows(graph)}
        self.assertEqual(reported, {
            ("file://s.vcf#record/1", VCFC + "recordId"),
            (str(attribute), VCFC + "attributeValue"),
        })
        self.assertEqual(anomaly_count(graph), 2)


class ExclusionTextTests(VerboseTestCase):
    """Static guarantees that hold without rdflib."""

    def test_both_queries_exclude_only_the_number_attribute(self):
        for path in (SAMPLE_QUERY, COUNT_QUERY):
            text = path.read_text(encoding="utf-8")
            self.assertIn('STR(?attributeKey) != "Number"', text, path.name)
            self.assertIn("OPTIONAL { ?s vcfc:attributeKey ?attributeKey }", text, path.name)

    def test_the_bundled_shapes_still_require_attribute_values_as_strings(self):
        """If a shape ever allowed vcfc:Null here, the exclusion must be revisited."""
        shapes = (
            Path(vcf_rdfizer.__file__).resolve().parent
            / "vcf_rdfizer_data" / "shacl" / "vcf-core-vocabulary.shacl.ttl"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "sh:path vcfc:attributeValue ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string",
            shapes,
        )


if __name__ == "__main__":
    unittest.main()
