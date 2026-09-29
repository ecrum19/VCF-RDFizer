"""Join-key inputs, preserving the wrapper's file/record/call identities."""

from dataclasses import dataclass
import gzip
from pathlib import Path
import tempfile


from .manifest import VCFR


@dataclass(frozen=True)
class Source:
    source: str
    reference: str


@dataclass(frozen=True)
class Record:
    source: str
    record: str
    call: str
    reference: str
    chrom: str
    pos: str
    ref: str
    alt: str
    id: str
    info: str


def open_text(path):
    return gzip.open(path, "rt", encoding="utf-8") if path.name.endswith(".gz") else path.open(encoding="utf-8")


def make_record(source_file, row_id, reference, fields):
    from vcf_rdfizer import _rml_uri_component

    source = "file://" + _rml_uri_component(source_file)
    row = _rml_uri_component(str(row_id))
    return Record(source, f"{source}#record/{row}", f"{source}#call/{row}", reference,
                  *(fields.get(k, ".") for k in ("CHROM", "POS", "REF", "ALT", "ID", "INFO")))


def read_vcf(path: Path, limit=None):
    reference, row, header = "", 0, False
    with open_text(path) as handle:
        for line in handle:
            if line.startswith("##reference="):
                value = line.strip().split("=", 1)[1]
                if reference and value != reference:
                    raise ValueError("VCF declares conflicting ##reference values")
                reference = value
            if line.startswith("#CHROM\t"):
                header = True
            if line.startswith("#") or not line.strip():
                continue
            if not header:
                raise ValueError("VCF is missing the #CHROM header")
            if limit is not None and row >= limit:
                break
            if row == 0:
                from vcf_rdfizer import _rml_uri_component
                yield Source("file://" + _rml_uri_component(path.name), reference)
            fields = line.rstrip("\r\n").split("\t")
            row += 1
            if len(fields) < 8:
                raise ValueError(f"VCF record {row} has fewer than eight columns")
            yield make_record(path.name, row, reference,
                              dict(zip(("CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO"), fields)))
    if not header:
        raise ValueError("VCF is missing the #CHROM header")
    if row == 0:
        from vcf_rdfizer import _rml_uri_component
        yield Source("file://" + _rml_uri_component(path.name), reference)


PREFIX = f"PREFIX vcfc: <{VCFR}> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
FIELDS = "vcfc:chrom vcfc:pos vcfc:ref vcfc:alt vcfc:recordId vcfc:infoRaw vcfc:referenceGenome"

#: Each query returns a row when the graph breaks an assumption the reader rests on.
VIOLATIONS = (
    ("Each hasRecord object must be an IRI belonging to one source file",
     "SELECT ?x WHERE { ?s vcfc:hasRecord ?x FILTER(!isIRI(?x)) } LIMIT 1"),
    ("Each hasRecord object must belong to one source file",
     "SELECT ?x WHERE { ?s vcfc:hasRecord ?x } GROUP BY ?x HAVING (COUNT(DISTINCT ?s) > 1) LIMIT 1"),
    ("Expected one IRI hasCall on each record",
     "SELECT ?x WHERE { ?s vcfc:hasRecord ?x FILTER NOT EXISTS { ?x vcfc:hasCall ?c FILTER(isIRI(?c)) } } LIMIT 1"),
    ("Expected one IRI hasCall on each record",
     "SELECT ?x WHERE { ?x vcfc:hasCall ?c } GROUP BY ?x HAVING (COUNT(?c) > 1) LIMIT 1"),
    ("Expected one IRI hasCall on each record",
     "SELECT ?x WHERE { ?x vcfc:hasCall ?c FILTER(!isIRI(?c)) } LIMIT 1"),
    ("Call subject does not exist in the base graph",
     "SELECT ?x WHERE { ?r vcfc:hasCall ?x FILTER NOT EXISTS { ?x ?p ?o } } LIMIT 1"),
    ("Expected at most one literal value of each join field",
     f"SELECT ?x WHERE {{ VALUES ?p {{ {FIELDS} }} ?x ?p ?v }} GROUP BY ?x ?p HAVING (COUNT(?v) > 1) LIMIT 1"),
    ("Expected at most one literal value of each join field",
     f"SELECT ?x WHERE {{ VALUES ?p {{ {FIELDS} }} ?x ?p ?v FILTER(!isLiteral(?v)) }} LIMIT 1"),
)
SOURCES = """SELECT ?source ?reference WHERE {
  { SELECT DISTINCT ?source WHERE { { ?source vcfc:hasRecord ?r } UNION { ?source rdf:type vcfc:VCFFile } } }
  OPTIONAL { ?source vcfc:referenceGenome ?reference } } ORDER BY ?source"""
RECORDS = """SELECT ?source ?record ?call ?chrom ?pos ?ref ?alt ?id ?info WHERE {
  ?source vcfc:hasRecord ?record . ?record vcfc:hasCall ?call .
  OPTIONAL { ?record vcfc:chrom ?chrom } OPTIONAL { ?record vcfc:pos ?pos }
  OPTIONAL { ?record vcfc:ref ?ref } OPTIONAL { ?record vcfc:alt ?alt }
  OPTIONAL { ?record vcfc:recordId ?id } OPTIONAL { ?call vcfc:infoRaw ?info } } ORDER BY ?source ?record"""


def read_store(store):
    """Sources and records from a SPARQL store holding a VCF-RDFizer graph.

    `store` is anything with `rows(query)` (vcf_rdfizer_policies.store): an
    endpoint serving the graph, or the local store `read_rdf` builds. The
    graph's own hasRecord/hasCall edges are followed; no subject is guessed
    from its IRI.
    """
    for message, query in VIOLATIONS:
        row = next(iter(store.rows(PREFIX + query)), None)
        if row is not None:
            raise ValueError(f"{message}: {row['x']}")
    references = {row["source"]: row["reference"] or "." for row in store.rows(PREFIX + SOURCES)}
    if not references:
        raise ValueError("No VCFFile/hasRecord edges found; the RDF must retain the VCF-RDFizer vocabulary")
    for source, reference in references.items():
        yield Source(source, reference)
    for row in store.rows(PREFIX + RECORDS):
        yield Record(row["source"], row["record"], row["call"], references[row["source"]],
                     *(row[k] or "." for k in ("chrom", "pos", "ref", "alt", "id", "info")))


class _OxigraphStore:
    """A pyoxigraph store answering `rows(query)` as the policy stores do."""

    def __init__(self, store):
        self.store = store

    def rows(self, query):
        solutions = self.store.query(query)
        names = [v.value for v in solutions.variables]
        for solution in solutions:
            yield {n: None if solution[n] is None else solution[n].value for n in names}


def read_rdf(path: Path):
    """Sources and records from an N-Triples file, through a temporary on-disk store.

    Only the join fields and the file/record/call edges are loaded (genotype
    triples are parsed and dropped), so memory stays flat; then `read_store`
    reads them with SPARQL. For a graph already served by an endpoint, call
    `read_store` on it directly.
    """
    import pyoxigraph as ox

    if not path.is_file() or not path.name.endswith((".nt", ".nt.gz")):
        raise ValueError("Link input must be an existing .nt or .nt.gz file")
    rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    keep = {str(VCFR[name]) for name in ("hasRecord", "hasCall", "referenceGenome", "chrom", "pos", "ref", "alt", "recordId", "infoRaw")}
    types = {str(VCFR[name]) for name in ("VCFFile", "VCFRecord", "VariantCall")}

    def wanted(triples):
        for t in triples:
            if t.predicate.value not in keep and not (t.predicate.value == rdf_type and t.object.value in types):
                continue
            if not isinstance(t.subject, ox.NamedNode) or not isinstance(t.object, (ox.NamedNode, ox.Literal)):
                raise ValueError("Linking requires IRI subjects and IRI/literal join fields")
            yield ox.Quad(t.subject, t.predicate, t.object)

    with tempfile.TemporaryDirectory(prefix="vcfr-link-input-") as work, \
            (gzip.open(path, "rb") if path.name.endswith(".gz") else path.open("rb")) as handle:
        store = ox.Store(str(Path(work) / "store"))
        try:
            triples = ox.parse(handle, format=ox.RdfFormat.N_TRIPLES)
        except AttributeError:                      # pyoxigraph < 0.4
            triples = ox.parse(handle, "application/n-triples")
        try:
            store.bulk_extend(wanted(triples))
        except SyntaxError as error:
            raise ValueError(f"Not valid N-Triples: {error}") from None
        yield from read_store(_OxigraphStore(store))
        del store
