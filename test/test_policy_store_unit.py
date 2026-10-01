"""Where the policy engine's queries run: escaping, parameter placement, and both stores.

Two properties here are safety properties rather than conveniences, and each
has a test that would fail if it were undone:

* `iri` refuses any IRI that could close its angle brackets. Resource IRIs come
  from the data, and they are spliced into query text; one that broke out could
  rewrite the query that decides what is released.
* `with_parameters` puts a selector's parameters in an inline VALUES block at
  the start of the outer WHERE group, not in a trailing VALUES clause. SPARQL
  joins a trailing clause after the group is evaluated, so a FILTER inside the
  group sees the parameters unbound and selects nothing -- and a prohibition
  that selects nothing releases everything it was meant to protect. On the demo
  cohort that is all 46 BRCA1 records.
"""

import io
import unittest
from unittest import mock
import urllib.error

from test import policy_fixtures as F
from test.helpers import VerboseTestCase

try:
    import rdflib
except ModuleNotFoundError:  # pragma: no cover - exercised only without rdflib
    rdflib = None

from vcf_rdfizer_policies import PolicyError
from vcf_rdfizer_policies.store import EndpointStore, MemoryStore, as_store, iri, with_parameters

REGION = "https://w3id.org/vcf-rdfizer/policy#RegionSelector"


class IriTests(VerboseTestCase):
    def test_an_ordinary_iri_is_bracketed(self):
        self.assertEqual(iri("file://P001.vcf#record/1"), "<file://P001.vcf#record/1>")

    def test_every_character_that_could_break_out_is_refused(self):
        for bad in (" ", "<", ">", '"', "{", "}", "|", "^", "`", "\\", "\n", "\t", "\x00"):
            with self.subTest(character=repr(bad)):
                with self.assertRaises(PolicyError):
                    iri(f"file://x{bad}y")

    def test_an_injection_attempt_is_refused_rather_than_spliced(self):
        """The case the escaping exists for: an IRI that would close the VALUES block."""
        hostile = "file://x> } ?s ?p ?o . FILTER(true) } #"
        with self.assertRaises(PolicyError) as caught:
            iri(hostile)
        self.assertIn("not a safe IRI", str(caught.exception))


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class WithParametersTests(VerboseTestCase):
    QUERY = "SELECT ?resource WHERE { ?resource <urn:pos> ?p . FILTER(?p >= ?lo) }"

    def test_no_bindings_leaves_the_query_alone(self):
        self.assertEqual(with_parameters(self.QUERY, ()), self.QUERY)

    def test_the_values_block_opens_the_outer_where_group(self):
        out = with_parameters(self.QUERY, (("lo", rdflib.Literal(10)),))
        where = out.index("WHERE {") + len("WHERE {")
        self.assertTrue(out[where:].lstrip().startswith("VALUES ?lo {"),
                        "the parameters must be the first thing inside the group")
        self.assertTrue(out.rstrip().endswith("}"), "nothing may trail the group")

    def test_a_list_value_becomes_one_block_of_several_terms(self):
        out = with_parameters(self.QUERY, (("lo", (rdflib.Literal(1), rdflib.Literal(2))),))
        self.assertIn('VALUES ?lo { "1"^^<http://www.w3.org/2001/XMLSchema#integer> '
                      '"2"^^<http://www.w3.org/2001/XMLSchema#integer> }', out)

    def test_where_is_matched_case_insensitively(self):
        out = with_parameters("select ?r where { ?r ?p ?o }", (("o", rdflib.URIRef("urn:x")),))
        self.assertIn("VALUES ?o { <urn:x> }", out)

    def test_a_parameterised_query_without_a_where_group_is_refused(self):
        with self.assertRaises(PolicyError):
            with_parameters("ASK { ?s ?p ?o }", (("o", rdflib.URIRef("urn:x")),))

    def test_the_inline_form_selects_and_the_trailing_form_fails_open(self):
        """Why with_parameters exists, measured on the real region selector.

        A trailing VALUES clause is valid SPARQL and parses without complaint;
        it simply selects nothing, because the FILTER ran before the join. A
        prohibition written that way would protect nothing at all.
        """
        _, profile, _, rules = F.setup()
        selector = profile.selectors[REGION]
        brca1 = next(r for r in rules if getattr(r.target, "asset", "").endswith("brca1"))
        bindings = brca1.target.bindings
        store = MemoryStore(F.shared_graph())

        inline = {row["resource"] for row in store.rows(with_parameters(selector.query, bindings))}
        trailing = selector.query + " VALUES (" + " ".join("?" + n for n, _ in bindings) + ") { (" \
            + " ".join(v.n3() for _, v in bindings) + ") }"

        expected = {iri for path in F.VCFS for iri, *fields in F.vcf_records(path) if F.in_brca1(*fields)}
        self.assertEqual(inline, expected)
        self.assertEqual(len(expected), 46)
        self.assertEqual(list(store.rows(trailing)), [],
                         "if this ever selects records, the fail-open argument no longer holds "
                         "and the docstring is wrong -- but the inline form must still be used")


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class MemoryStoreTests(VerboseTestCase):
    def setUp(self):
        self.graph = rdflib.Graph().parse(
            data='<urn:a> <urn:p> <urn:b> .\n<urn:a> <urn:q> "x"@en .\n', format="nt")

    def test_rows_are_plain_strings_and_unbound_is_none(self):
        rows = list(MemoryStore(self.graph).rows(
            "SELECT ?o ?m WHERE { <urn:a> ?p ?o OPTIONAL { ?o <urn:z> ?m } } ORDER BY ?o"))
        self.assertEqual(rows, [{"o": "urn:b", "m": None}, {"o": "x", "m": None}])
        self.assertTrue(all(type(v) is str for row in rows for v in row.values() if v is not None))

    def test_as_store_wraps_a_graph_and_passes_a_store_through(self):
        wrapped = as_store(self.graph)
        self.assertIsInstance(wrapped, MemoryStore)
        self.assertIs(as_store(wrapped), wrapped)


@unittest.skipIf(rdflib is None, "rdflib is required for the policy tests")
class EndpointStoreTests(VerboseTestCase):
    """EndpointStore against a real local HTTP endpoint, then against canned responses."""

    def setUp(self):
        self.graph = rdflib.Graph().parse(
            data='<urn:a> <urn:p> <urn:b> .\n<urn:a> <urn:q> "x" .\n', format="nt")

    def test_it_answers_exactly_as_the_memory_store_does(self):
        """The engine relies on the two being interchangeable; check it on one query."""
        query = "SELECT ?s ?o ?m WHERE { ?s ?p ?o OPTIONAL { ?o <urn:z> ?m } } ORDER BY ?o"
        with F.SparqlEndpoint(self.graph) as endpoint:
            remote = list(EndpointStore(endpoint.url).rows(query))
        self.assertEqual(remote, list(MemoryStore(self.graph).rows(query)))

    def test_a_refused_query_becomes_a_policy_error_naming_the_endpoint(self):
        with F.SparqlEndpoint(self.graph) as endpoint:
            endpoint.refuse()
            with self.assertRaises(PolicyError) as caught:
                list(EndpointStore(endpoint.url).rows("SELECT * WHERE { ?s ?p ?o }"))
        self.assertIn(endpoint.url, str(caught.exception))
        self.assertIn("syntax error near VALUES", str(caught.exception))

    def test_a_question_mark_in_the_header_is_stripped(self):
        """SPARQL CSV omits the '?'; some endpoints send it anyway. Both must read the same."""
        body = io.BytesIO(b"?s,?o\r\nurn:a,\r\n")
        with mock.patch("urllib.request.urlopen", return_value=body):
            rows = list(EndpointStore("http://unused").rows("SELECT ?s ?o WHERE { ?s ?p ?o }"))
        self.assertEqual(rows, [{"s": "urn:a", "o": None}])

    def test_an_empty_answer_yields_no_rows(self):
        with mock.patch("urllib.request.urlopen", return_value=io.BytesIO(b"")):
            self.assertEqual(list(EndpointStore("http://unused").rows("SELECT * WHERE {}")), [])

    def test_the_request_asks_for_csv_and_posts_the_query(self):
        captured = {}

        def fake(request, timeout):
            captured.update(accept=request.get_header("Accept"), data=request.data, timeout=timeout)
            return io.BytesIO(b"s\r\n")

        with mock.patch("urllib.request.urlopen", side_effect=fake):
            list(EndpointStore("http://unused", timeout=7).rows("SELECT ?s WHERE { ?s ?p ?o }"))
        self.assertEqual(captured["accept"], "text/csv")
        self.assertIn(b"query=SELECT", captured["data"])
        self.assertEqual(captured["timeout"], 7)

    def test_an_http_error_is_wrapped_without_a_long_traceback(self):
        error = urllib.error.HTTPError("http://unused", 500, "boom", {}, io.BytesIO(b"x" * 1000))
        with mock.patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaises(PolicyError) as caught:
                list(EndpointStore("http://unused").rows("SELECT * WHERE {}"))
        self.assertIsNone(caught.exception.__cause__, "raised 'from None' so the operator sees one line")
        self.assertLess(len(str(caught.exception)), 400, "the body is truncated, not dumped")


if __name__ == "__main__":
    unittest.main()
