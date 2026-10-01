"""Shared fixtures for the policy plug-in tests: the demo cohort, an independent
oracle of what each request should release, and a local SPARQL endpoint.

The cohort is examples/policy: five synthetic single-sample VCFs (139 records),
their VCF-RDFizer conversions, and a policy with per-participant consents, one
withdrawal, a region prohibition and a variant prohibition.

`expected_released` is the point of this module. It derives which records each
request should see from the VCF text and a hand-written reading of policy.ttl,
with no call into the engine, so a test that compares the engine against it is
checking the engine rather than restating it.

`SparqlEndpoint` serves an rdflib graph as a SPARQL 1.1 endpoint returning CSV,
which is what EndpointStore reads. It lets the streaming code paths
(`evaluate_stream`, `check_stream`, the CLI's --endpoint) run end to end,
through real HTTP, without QLever.
"""

from contextlib import contextmanager
from functools import lru_cache
import gzip
import http.server
import json
from pathlib import Path
import shutil
import threading
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "examples" / "policy"
POLICY = DEMO / "policy.ttl"
VCFS = sorted(DEMO.glob("P00*.vcf"))
RDF = sorted((DEMO / "converted" / "expanded").glob("P00*.nt.gz"))
REQUESTERS = json.loads((DEMO / "fixture.json").read_text(encoding="utf-8"))["requesters"]

# --- What policy.ttl says, read by hand ---------------------------------------
#
# The bundled DUO subset orders the purposes as
#
#     DUO_0000007 (disease-specific) < DUO_0000006 (health/medical) < DUO_0000042 (general)
#     DUO_0000043 (clinical care) on a separate branch
#
# and a purpose satisfies a term when it is that term or narrower. So:
#
#   consent  P001, P002   isAnyOf {0042, 0043}   gru yes   alz yes   clinical yes
#   consent  P003         isAnyOf {0006}         gru no    alz yes   clinical no
#   consent  P004         withdrawn: an unconditional prohibition    -- never
#   consent  P005         isAnyOf {0007}         gru no    alz yes   clinical no
#
#   BRCA1 region          prohibited unless the purpose is within 0043
#   APOE e4 variant       prohibited unless the purpose is within 0007
#
RELEASED_FILES = {
    "gru": {"P001.vcf", "P002.vcf"},
    "alz": {"P001.vcf", "P002.vcf", "P003.vcf", "P005.vcf"},
    "clinical": {"P001.vcf", "P002.vcf"},
}
WITHHOLDS_BRCA1 = {"gru": True, "alz": True, "clinical": False}
WITHHOLDS_APOE_E4 = {"gru": True, "alz": False, "clinical": True}


def in_brca1(chrom, pos, ref, alts):
    return chrom == "chr17" and 43044295 <= pos <= 43125483


def is_apoe_e4(chrom, pos, ref, alts):
    return chrom == "chr19" and pos == 44908684 and ref == "T" and "C" in alts


def vcf_records(path):
    """(record IRI, chrom, pos, ref, alts) for each data row, numbered as VCF-RDFizer numbers them."""
    rows, n = [], 0
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.startswith("#"):
            continue
        n += 1
        chrom, pos, _, ref, alt = line.split("\t")[:5]
        rows.append((f"file://{Path(path).name}#record/{n}", chrom, int(pos), ref, alt.split(",")))
    return rows


def expected_released(requester):
    """The record IRIs a correct view releases for `requester`, from the VCF text alone."""
    found = set()
    for path in VCFS:
        if path.name not in RELEASED_FILES[requester]:
            continue
        for iri, *fields in vcf_records(path):
            if WITHHOLDS_BRCA1[requester] and in_brca1(*fields):
                continue
            if WITHHOLDS_APOE_E4[requester] and is_apoe_e4(*fields):
                continue
            found.add(iri)
    return found


def first_record(path, predicate):
    """The IRI of the first record of `path` matching `predicate(chrom, pos, ref, alts)`."""
    return next(iri for iri, *fields in vcf_records(path) if predicate(*fields))


# --- Loading, once per test run -----------------------------------------------

@lru_cache(maxsize=None)
def setup():
    """(policy graph, profile, vocabulary, rules) for the demo policy."""
    from vcf_rdfizer_policies.policy import load_rules, read_graph
    from vcf_rdfizer_policies.profile import load_profile
    from vcf_rdfizer_policies.vocabulary import Vocabulary

    graph = read_graph(POLICY)
    profile = load_profile([], extra_graph=graph)
    vocabulary = Vocabulary.load()
    return graph, profile, vocabulary, tuple(load_rules(graph, profile, vocabulary))


def cohort_graph():
    """A fresh rdflib graph of the converted cohort. Fresh, because attach mutates it."""
    from vcf_rdfizer_policies.graphs import load

    return load(RDF)


@lru_cache(maxsize=None)
def _shared_graph():
    return cohort_graph()


def shared_graph():
    """The converted cohort, parsed once. Read-only: do not pass it to attach."""
    return _shared_graph()


def request(requester):
    from vcf_rdfizer_policies.engine import Request

    _, _, vocabulary, _ = setup()
    spec = REQUESTERS[requester]
    return Request(spec["assignee"], vocabulary.resolve(spec["purpose"]))


def gzip_text(path, text):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(text)


def read_gzip_lines(path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [line for line in handle if line.strip()]


def copy_view(source_dir, target_dir):
    shutil.copytree(source_dir, target_dir)
    return Path(target_dir)


# --- A SPARQL endpoint over an rdflib graph ------------------------------------

@contextmanager
def SparqlEndpoint(graph):
    """Serve `graph` at http://127.0.0.1:<port>/sparql, answering POSTed queries as SPARQL CSV.

    Counts the queries it answers (`.queries`), and can be told to refuse the
    next one (`.refuse_next = True`) to exercise EndpointStore's HTTP-error path.
    """
    state = {"queries": 0, "refuse_next": False}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            query = urllib.parse.parse_qs(self.rfile.read(length).decode())["query"][0]
            if state["refuse_next"]:
                state["refuse_next"] = False
                self._send(400, b"syntax error near VALUES")
                return
            state["queries"] += 1
            self._send(200, graph.query(query).serialize(format="csv"), "text/csv")

        def _send(self, status, body, content_type="text/plain"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):        # keep the test output readable
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    class Handle:
        url = f"http://127.0.0.1:{server.server_address[1]}/sparql"

        @property
        def queries(self):
            return state["queries"]

        def refuse(self):
            state["refuse_next"] = True

    try:
        yield Handle()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
