"""Join-key inputs, preserving the wrapper's file/record/call identities."""

import csv
from dataclasses import dataclass
import gzip
from pathlib import Path
import sqlite3
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


def read_tsv(records: Path, metadata: Path):
    with metadata.open(encoding="utf-8", newline="") as handle:
        references = {r["SOURCE_FILE"]: r["REFERENCE_GENOME"] for r in csv.DictReader(handle, delimiter="\t")}
    from vcf_rdfizer import _rml_uri_component
    for source, reference in references.items():
        yield Source("file://" + _rml_uri_component(source), reference)
    with records.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            yield make_record(row["SOURCE_FILE"], row["ROW_ID"], references[row["SOURCE_FILE"]], row)


def read_rdf(path: Path):
    """Stream unordered N-Triples into a disk-backed index of just join fields.

    Follow hasRecord/hasCall edges rather than guessing a subject from its IRI.
    Genotype triples are parsed but never retained. RDFLib owns term decoding.
    """
    # Deferred so the module imports without rdflib: the CLI loads it while
    # building its argument parser, long before any linking runs.
    from rdflib import Literal, RDF, URIRef
    from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
    if not path.is_file() or not path.name.endswith((".nt", ".nt.gz")):
        raise ValueError("Link input must be an existing .nt or .nt.gz file")
    keep = {str(VCFR[name]) for name in ("hasRecord", "hasCall", "referenceGenome", "chrom", "pos", "ref", "alt", "recordId", "infoRaw")}
    types = {VCFR.VCFFile, VCFR.VCFRecord, VCFR.VariantCall}
    with tempfile.TemporaryDirectory(prefix="vcfr-link-input-") as work:
        db = sqlite3.connect(str(Path(work) / "input.sqlite"))
        try:
            db.execute("CREATE TABLE terms (s TEXT, p TEXT, o TEXT, iri INTEGER, PRIMARY KEY(s,p,o,iri))")
            db.execute("CREATE INDEX predicates ON terms(p,s)")

            class Sink:
                def triple(self, s, p, o):
                    if str(p) not in keep and not (p == RDF.type and o in types):
                        return
                    if not isinstance(s, URIRef) or not isinstance(o, (URIRef, Literal)):
                        raise ValueError("Linking requires IRI subjects and IRI/literal join fields")
                    db.execute("INSERT OR IGNORE INTO terms VALUES (?,?,?,?)", (str(s), str(p), str(o), isinstance(o, URIRef)))

            with open_text(path) as handle:
                W3CNTriplesParser(sink=Sink()).parse(handle)
            db.commit()

            def one(subject, predicate, *, iri=False, required=True):
                rows = db.execute("SELECT o,iri FROM terms WHERE s=? AND p=?", (subject, str(predicate))).fetchall()
                if not rows and not required:
                    return "."
                if len(rows) != 1 or bool(rows[0][1]) != iri:
                    raise ValueError(f"Expected one {'IRI' if iri else 'literal'} {predicate} on {subject}")
                return rows[0][0]

            sources = db.execute("SELECT DISTINCT s FROM terms WHERE p=?", (str(VCFR.hasRecord),)).fetchall()
            if not sources:
                # A typed empty VCF graph is a valid zero-link input.
                sources = db.execute("SELECT s FROM terms WHERE p=? AND o=?", (str(RDF.type), str(VCFR.VCFFile))).fetchall()
                if not sources:
                    raise ValueError("No VCFFile/hasRecord edges found; the RDF must retain the VCF-RDFizer vocabulary")
            duplicate_owner = db.execute("SELECT o FROM terms WHERE p=? GROUP BY o HAVING COUNT(*) > 1 LIMIT 1", (str(VCFR.hasRecord),)).fetchone()
            if duplicate_owner:
                raise ValueError("Each hasRecord object must belong to one source file")
            for (source,) in sources:
                reference = one(source, VCFR.referenceGenome, required=False)
                yield Source(source, reference)
                for record, iri in db.execute("SELECT o,iri FROM terms WHERE s=? AND p=?", (source, str(VCFR.hasRecord))):
                    if not iri:
                        raise ValueError("Each hasRecord object must be an IRI belonging to one source file")
                    call = one(record, VCFR.hasCall, iri=True)
                    if not db.execute("SELECT 1 FROM terms WHERE s=? LIMIT 1", (call,)).fetchone():
                        raise ValueError(f"Call subject does not exist in the base graph: {call}")
                    yield Record(source, record, call, reference,
                                 *(one(record, VCFR[k], required=False) for k in ("chrom", "pos", "ref", "alt", "recordId")),
                                 one(call, VCFR.infoRaw, required=False))
        finally:
            db.close()
