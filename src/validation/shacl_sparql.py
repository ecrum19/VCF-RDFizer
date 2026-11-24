"""SHACL-SPARQL constraints as whole-graph queries.

A SHACL engine evaluates a SPARQL-based constraint once per focus node, with
``$this`` pre-bound. pyshacl does that over an in-memory graph, so the full
shape profile -- whose 40 ``sh:sparql`` constraints compare records with each
other -- could only be validated whole and only up to ``--shacl-max-triples``.

Here every constraint runs once over the whole graph instead: ``$this`` is left
unbound and the shape's target is added as a pattern, so the solutions are the
violating focus nodes. The graph is served by QLever, which indexes it on disk,
so memory no longer follows the graph. The node-local shapes are unaffected:
pyshacl still validates them in record batches (see validation_runner).

pyshacl validates with the vocabulary loaded and RDFS inference on. The same
facts are given to QLever by :func:`write_rdfs_typing`: the vocabulary's own
triples, plus the types that ``rdfs:domain``, ``rdfs:range``,
``rdfs:subClassOf`` and ``rdfs:subPropertyOf`` derive from the data.

Four published constraints cannot run whole on a large graph as written, in any
engine: two compare every pair or triple of records, and two build a cross
product before they filter it. :data:`SCALABLE_EQUIVALENTS` replaces each with a
query that decides the same thing in linear time. Each replacement is pinned to
the exact published query by digest, so a vocabulary update that changes one
falls back to the published text rather than to a stale rewrite, and the
equivalence is checked on every fault-injection graph in the test suite.

This module has no dependency beyond rdflib, so it is importable on the host.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

SH = "http://www.w3.org/ns/shacl#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
XSD = "http://www.w3.org/2001/XMLSchema#"
RDF_TYPE = f"<{RDF}type>"
SEVERITIES = {f"{SH}Violation": "Violation", f"{SH}Warning": "Warning", f"{SH}Info": "Info"}

#: Pre-bound variables other than $this. A constraint using one cannot be
#: evaluated whole, because its value depends on the shape, not the data.
UNSUPPORTED_PREBOUND = ("$PATH", "$value", "$currentShape", "$shapesGraph")


@dataclass
class SparqlConstraint:
    """One sh:sparql constraint, ready to run whole."""

    shape: str
    message: str | None
    severity: str
    #: The query as published, with the shapes graph's prefixes declared.
    select: str
    #: Group patterns binding ?this to the shape's focus nodes (a union).
    targets: list[str]
    #: The query exactly as the shapes file states it; equivalents match on it.
    published: str | None = None
    prefixes: dict[str, str] = field(default_factory=dict)
    #: Name of the scalable equivalent used instead of ``select``, if any.
    equivalent: str | None = None
    query: str | None = field(default=None, repr=False)
    unsupported: str | None = None


def normalized_digest(query: str) -> str:
    """Digest of a query with all whitespace runs collapsed."""
    return hashlib.sha256(" ".join(query.split()).encode("utf-8")).hexdigest()


# --- The four scalable equivalents -------------------------------------------

#: POS non-decreasing within a CHROM. Published: every pair of records of a file
#: (O(n^2)). Call a segment a run of records of one CHROM at consecutive record
#: indices. A pair breaks the order exactly when (a) two neighbours of a segment
#: (index i and i + 1) decrease, or, with no such pair, (b) a segment ends above
#: where a later segment of the same CHROM starts: every segment is then
#: sorted, so its end is its largest POS and the later start its smallest. A
#: segment ends where no record of its CHROM sits at index i + 1, and starts
#: where none sits at i - 1, so gaps and repeats in the indices are handled.
#: Both branches report only genuine violating pairs, and (b) compares segment
#: boundaries, of which a sorted file has one pair per CHROM.
_NONDECREASING = """
      SELECT $this WHERE {
        {
          $this vcfc:hasRecord ?earlier .
          ?earlier vcfc:recordIndex ?earlierIndex ; vcfc:chrom ?chrom ; vcfc:pos ?earlierPos .
          BIND (?earlierIndex + 1 AS ?laterIndex)
          ?later vcfc:recordIndex ?laterIndex ; vcfc:chrom ?chrom ; vcfc:pos ?laterPos .
          $this vcfc:hasRecord ?later .
          FILTER (?earlierPos > ?laterPos)
        } UNION {
          {
            $this vcfc:hasRecord ?end .
            ?end vcfc:recordIndex ?endIndex ; vcfc:chrom ?chrom ; vcfc:pos ?endPos .
            BIND (?endIndex + 1 AS ?afterIndex)
            OPTIONAL {
              ?after vcfc:recordIndex ?afterIndex ; vcfc:chrom ?chrom .
              $this vcfc:hasRecord ?after .
            }
            FILTER (!BOUND(?after))
          }
          {
            $this vcfc:hasRecord ?start .
            ?start vcfc:recordIndex ?startIndex ; vcfc:chrom ?chrom ; vcfc:pos ?startPos .
            BIND (?startIndex - 1 AS ?beforeIndex)
            OPTIONAL {
              ?before vcfc:recordIndex ?beforeIndex ; vcfc:chrom ?chrom .
              $this vcfc:hasRecord ?before .
            }
            FILTER (!BOUND(?before))
          }
          FILTER (?endIndex < ?startIndex && ?endPos > ?startPos)
        }
      }
"""

#: Records of one CHROM are contiguous. Published: every triple of records
#: (O(n^3)). A CHROM is broken exactly when a record of another CHROM has an
#: index strictly between that CHROM's lowest and highest record index, which
#: needs one pass for the ranges and one join of records with CHROMs.
_CONTIGUOUS = """
      SELECT $this WHERE {
        {
          SELECT $this ?chrom (MIN(?index) AS ?low) (MAX(?index) AS ?high) WHERE {
            $this vcfc:hasRecord ?record .
            ?record vcfc:recordIndex ?index ; vcfc:chrom ?chrom .
          } GROUP BY $this ?chrom
        }
        $this vcfc:hasRecord ?middle .
        ?middle vcfc:recordIndex ?middleIndex ; vcfc:chrom ?otherChrom .
        FILTER (?otherChrom != ?chrom && ?middleIndex > ?low && ?middleIndex < ?high)
      }
"""

#: Non-missing record IDs are unique. Published: pairs records on ?id before
#: filtering out ".", which is most IDs in a real VCF, so it builds nearly every
#: record pair. Grouping on the ID decides the same thing in one pass.
_DUPLICATE_ID = """
      SELECT $this WHERE {
        {
          SELECT $this ?id WHERE {
            $this vcfc:hasRecord ?record .
            ?record vcfc:recordId ?id .
            FILTER (STR(?id) != ".")
          } GROUP BY $this ?id HAVING (COUNT(?record) > 1)
        }
      }
"""

_PLOIDY = "OPTIONAL { ?sample vcfc:hasGenotype/vcfc:ploidy ?ploidy }\n"
_LAA = ('OPTIONAL { ?sample vcfc:hasFormatValue ?laa . ?laa vcfc:declaredBy/vcfc:fieldId "LAA" ; '
        "vcfc:fieldValue ?local }\n")
_FORMAT_BRANCH = "UNION { ?c vcfc:hasSampleCall ?sample . ?sample vcfc:hasFormatValue $this }"


def _cardinality(query: str) -> str:
    """Field cardinality agrees with Number, alleles and ploidy.

    Published: the ploidy and LAA OPTIONALs follow the INFO/FORMAT UNION, so for
    an INFO value ?sample is unbound and they match every sample of the file.
    They belong to the FORMAT branch, where ?sample is bound; an INFO value then
    takes the default ploidy 2, as the COALESCE intends.
    """
    moved = query.replace(_PLOIDY, "").replace(_LAA, "")
    return moved.replace(_FORMAT_BRANCH, f"{_FORMAT_BRANCH[:-1]}\n  {_PLOIDY}  {_LAA}}}")


#: digest of the published query -> (name, rewrite). See the module docstring.
SCALABLE_EQUIVALENTS: dict[str, tuple[str, Callable[[str], str]]] = {
    "1aa29f335c416592f23e2c628395fb6214e3173287d0e96236a552076a3026db":
        ("pos-nondecreasing-neighbours-and-segments", lambda _query: _NONDECREASING),
    "43088a7c2d337058f9654350ac9b56008849481fad0b54551effe6382e73f94a":
        ("chrom-contiguous-index-ranges", lambda _query: _CONTIGUOUS),
    "b7e4d64c2b3b032886335a7ced7ed4720f7544117fe20c8d7e0479a0e1b57715":
        ("record-id-unique-grouped", lambda _query: _DUPLICATE_ID),
    "be87ec3498bdecd2146bb06b6bcfa3015872b7e2da46e500046647471d977ac3":
        ("field-cardinality-sample-optionals-in-format-branch", _cardinality),
}


def scalable_equivalent(query: str) -> tuple[str, str | None]:
    """The query to run in place of a published one, and the rewrite's name."""
    entry = SCALABLE_EQUIVALENTS.get(normalized_digest(query))
    if entry is None:
        return query, None
    name, rewrite = entry
    return rewrite(query), name


# --- Reading the shapes -------------------------------------------------------

def merge_graphs(paths: Iterable[Path]):
    from rdflib import Graph

    graph = Graph()
    for path in paths:
        graph.parse(str(path), format="turtle")
    return graph


def _closure(graph, node, into: set) -> None:
    from rdflib import BNode

    for predicate, obj in graph.predicate_objects(node):
        into.add((node, predicate, obj))
        if isinstance(obj, BNode):
            _closure(graph, obj, into)


def _declared_prefixes(graph, constraint) -> dict[str, str]:
    """sh:prefixes/sh:declare for a constraint, as SHACL-SPARQL defines them."""
    from rdflib import URIRef

    declared: dict[str, str] = {}
    for holder in graph.objects(constraint, URIRef(f"{SH}prefixes")):
        for declaration in graph.objects(holder, URIRef(f"{SH}declare")):
            prefix = graph.value(declaration, URIRef(f"{SH}prefix"))
            namespace = graph.value(declaration, URIRef(f"{SH}namespace"))
            if prefix is not None and namespace is not None:
                declared[str(prefix)] = str(namespace)
    return declared


_PREFIX_LINE = re.compile(r"^\s*PREFIX\s+([A-Za-z][\w.-]*)?:", re.IGNORECASE | re.MULTILINE)


def with_prefixes(query: str, prefixes: dict[str, str]) -> str:
    """Declare every prefix the query does not declare itself.

    The published constraints rely on the shapes file's own prefixes, which
    pyshacl supplies to every query. A SPARQL endpoint needs them spelled out.
    """
    own = {match.group(1) or "" for match in _PREFIX_LINE.finditer(query)}
    head = "".join(
        f"PREFIX {prefix}: <{namespace}>\n"
        for prefix, namespace in sorted(prefixes.items()) if prefix not in own
    )
    return head + query


def target_patterns(graph, shape) -> list[str]:
    """Group patterns binding ?this to a shape's focus nodes, as SHACL targets them."""
    from rdflib import URIRef

    patterns = []
    for cls in graph.objects(shape, URIRef(f"{SH}targetClass")):
        patterns.append(f"?this {RDF_TYPE} <{cls}> .")
    # An implicit class target: a shape that is also a class targets its instances.
    if (shape, URIRef(f"{RDF}type"), URIRef(f"{RDFS}Class")) in graph:
        patterns.append(f"?this {RDF_TYPE} <{shape}> .")
    for prop in graph.objects(shape, URIRef(f"{SH}targetSubjectsOf")):
        patterns.append(f"?this <{prop}> ?targetObject .")
    for prop in graph.objects(shape, URIRef(f"{SH}targetObjectsOf")):
        patterns.append(f"?targetSubject <{prop}> ?this .")
    for node in graph.objects(shape, URIRef(f"{SH}targetNode")):
        term = f"<{node}>" if isinstance(node, URIRef) else node.n3()
        patterns.append(f"VALUES ?this {{ {term} }}")
    return sorted(patterns)


def split_shapes(paths: Iterable[Path]):
    """Separate the SPARQL-based constraints from everything else.

    Returns the shapes graph without its sh:sparql constraints, for pyshacl to
    validate in record batches, and the constraints themselves.
    """
    from rdflib import Literal, URIRef

    graph = merge_graphs(paths)
    prefixes = {prefix: str(namespace) for prefix, namespace in graph.namespaces() if prefix}
    sparql, select = URIRef(f"{SH}sparql"), URIRef(f"{SH}select")
    constraints: list[SparqlConstraint] = []
    removed: set = set()
    for shape, constraint in sorted(graph.subject_objects(sparql), key=lambda pair: (str(pair[0]), str(pair[1]))):
        removed.add((shape, sparql, constraint))
        _closure(graph, constraint, removed)
        if graph.value(constraint, URIRef(f"{SH}deactivated")) == Literal(True) or \
                graph.value(shape, URIRef(f"{SH}deactivated")) == Literal(True):
            continue
        text = graph.value(constraint, select)
        if text is None:  # sh:ask and friends are not SHACL-SPARQL constraints
            continue
        severity = graph.value(constraint, URIRef(f"{SH}severity")) or \
            graph.value(shape, URIRef(f"{SH}severity"))
        message = graph.value(constraint, URIRef(f"{SH}message")) or \
            graph.value(shape, URIRef(f"{SH}message"))
        merged = dict(prefixes)
        merged.update(_declared_prefixes(graph, constraint))
        constraints.append(SparqlConstraint(
            shape=str(shape),
            message=str(message) if message is not None else None,
            severity=SEVERITIES.get(str(severity), "Violation") if severity is not None else "Violation",
            select=with_prefixes(str(text), merged),
            targets=target_patterns(graph, shape),
            published=str(text),
            prefixes=merged,
        ))
    rest = graph.__class__()
    for prefix, namespace in graph.namespaces():
        rest.bind(prefix, namespace, override=True)
    for triple in graph:
        if triple not in removed:
            rest.add(triple)
    for constraint in constraints:
        prepare(constraint)
    return rest, constraints


# --- Building the whole-graph query -------------------------------------------

def _skip_string(text: str, index: int) -> int:
    """Index just past the string literal starting at ``index``."""
    quote = text[index]
    if text.startswith(quote * 3, index):
        end = text.find(quote * 3, index + 3)
        return len(text) if end < 0 else end + 3
    index += 1
    while index < len(text):
        if text[index] == "\\":
            index += 2
            continue
        if text[index] == quote:
            return index + 1
        index += 1
    return index


def outer_group(query: str) -> tuple[int, int]:
    """Offsets of the outer SELECT's WHERE group: its '{' and its matching '}'."""
    start = re.search(r"\bSELECT\b", query, re.IGNORECASE)
    if start is None:
        raise ValueError("not a SELECT query")
    index, depth, opening = start.end(), 0, None
    while index < len(query):
        char = query[index]
        if char in "\"'":
            index = _skip_string(query, index)
            continue
        if char == "#":
            newline = query.find("\n", index)
            index = len(query) if newline < 0 else newline + 1
            continue
        if char == "{":
            if depth == 0:
                opening = index
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0 and opening is not None:
                return opening, index
        index += 1
    raise ValueError("unbalanced braces in SELECT query")


def whole_graph_query(select: str, targets: list[str]) -> str:
    """Run a constraint once over the graph rather than once per focus node.

    ``$this`` becomes an ordinary variable, joined with the shape's targets, and
    the projection becomes the distinct focus nodes. The prologue (PREFIX
    lines) and any solution modifiers after the WHERE group are kept.
    """
    query = re.sub(r"\$this\b", "?this", select)
    opening, closing = outer_group(query)
    selected = re.search(r"\bSELECT\b", query, re.IGNORECASE)
    prologue = query[: selected.start()]
    group = query[opening: closing + 1]
    modifiers = query[closing + 1:].strip()
    union = " UNION ".join(f"{{ {pattern} }}" for pattern in targets)
    body = f"{{ {union} }}\n{group}" if targets else group
    return f"{prologue}SELECT DISTINCT ?this WHERE {{\n{body}\n}}{(' ' + modifiers) if modifiers else ''}\n"


def prepare(constraint: SparqlConstraint) -> SparqlConstraint:
    """Fill in the query to run, or why the constraint cannot run whole."""
    used = [name for name in UNSUPPORTED_PREBOUND if re.search(re.escape(name) + r"\b", constraint.select)]
    if used:
        constraint.unsupported = f"uses pre-bound {', '.join(used)}, which a whole-graph query cannot bind"
        return constraint
    if not constraint.targets:
        constraint.unsupported = "its shape declares no target, so it has no focus nodes"
        return constraint
    if constraint.published is None:
        text, constraint.equivalent = scalable_equivalent(constraint.select)
    else:
        text, constraint.equivalent = scalable_equivalent(constraint.published)
        text = with_prefixes(text, constraint.prefixes)
    try:
        constraint.query = whole_graph_query(text, constraint.targets)
    except ValueError as error:
        constraint.unsupported = str(error)
    return constraint


# --- RDFS typing for the index ------------------------------------------------

class Vocabulary:
    """The RDFS facts of the vocabulary that type the data."""

    def __init__(self, graph) -> None:
        from rdflib import BNode, URIRef

        def iri(term) -> bool:
            return isinstance(term, URIRef)

        sub_class, sub_prop = URIRef(f"{RDFS}subClassOf"), URIRef(f"{RDFS}subPropertyOf")
        parents: dict[str, set[str]] = {}
        for child, parent in graph.subject_objects(sub_class):
            if iri(child) and iri(parent):
                parents.setdefault(str(child), set()).add(str(parent))
        prop_parents: dict[str, set[str]] = {}
        for child, parent in graph.subject_objects(sub_prop):
            if iri(child) and iri(parent):
                prop_parents.setdefault(str(child), set()).add(str(parent))
        self.superclasses = _transitive(parents)
        self.superproperties = _transitive(prop_parents)
        datatype = URIRef(f"{RDFS}Datatype")

        def is_class(term) -> bool:
            return iri(term) and str(term) != f"{RDFS}Literal" and not str(term).startswith(XSD) \
                and (term, URIRef(f"{RDF}type"), datatype) not in graph and not isinstance(term, BNode)

        direct_domain: dict[str, set[str]] = {}
        direct_range: dict[str, set[str]] = {}
        for prop, cls in graph.subject_objects(URIRef(f"{RDFS}domain")):
            if iri(prop) and iri(cls):
                direct_domain.setdefault(str(prop), set()).add(str(cls))
        for prop, cls in graph.subject_objects(URIRef(f"{RDFS}range")):
            if iri(prop) and is_class(cls):
                direct_range.setdefault(str(prop), set()).add(str(cls))
        props = set(direct_domain) | set(direct_range) | set(self.superproperties)
        self.domains: dict[str, set[str]] = {}
        self.ranges: dict[str, set[str]] = {}
        for prop in props:
            lineage = {prop} | self.superproperties.get(prop, set())
            domain = set().union(*(direct_domain.get(p, set()) for p in lineage))
            rng = set().union(*(direct_range.get(p, set()) for p in lineage))
            if domain:
                self.domains[prop] = self.with_superclasses(domain)
            if rng:
                self.ranges[prop] = self.with_superclasses(rng)

    def with_superclasses(self, classes: Iterable[str]) -> set[str]:
        out = set()
        for cls in classes:
            out.add(cls)
            out |= self.superclasses.get(cls, set())
        return out


def _transitive(parents: dict[str, set[str]]) -> dict[str, set[str]]:
    closed: dict[str, set[str]] = {}

    def ancestors(node: str, trail: frozenset) -> set[str]:
        if node in closed:
            return closed[node]
        found: set[str] = set()
        for parent in parents.get(node, ()):
            if parent in trail:
                continue
            found.add(parent)
            found |= ancestors(parent, trail | {parent})
        closed[node] = found
        return found

    for node in parents:
        ancestors(node, frozenset({node}))
    return closed


#: Range types of an object are written once while it stays in this many recent
#: objects. Hot objects (a field declaration every value points at) stay in it.
#: A miss only writes a duplicate, which QLever drops at index time.
TYPING_RECENT_OBJECTS = 1_000_000


class RdfsTyping:
    """The RDFS consequences of N-Triples lines that SHACL can see, line by line.

    For each triple: the superproperty triples it implies (rdfs7), its
    subject's domain types and its object's range types (rdfs2, rdfs3), and,
    for an rdf:type triple, the superclasses (rdfs9). Literal-subject and
    resource-level entailments (rdfs4, rdfs8 ...) are left out: no shape
    targets them. It keeps only the current subject and a bounded set of recent
    objects, so it streams at any graph size. A type it cannot tell is already
    known is written again, and QLever drops the duplicate at index time.
    """

    def __init__(self, vocabulary: Vocabulary) -> None:
        self.vocabulary = vocabulary
        self.subject: str | None = None
        self.subject_types: set[str] = set()
        self.recent: dict[str, None] = {}
        self.written = 0

    def consequences(self, line: str) -> list[str]:
        line = line.strip()
        if not line or line.startswith("#"):
            return []
        subject, _, rest = line.partition(" ")
        predicate, _, obj = rest.lstrip().partition(" ")
        obj = obj.strip()
        if obj.endswith("."):
            obj = obj[:-1].rstrip()
        if not predicate.startswith("<"):
            return []
        prop = predicate[1:-1]
        vocabulary = self.vocabulary
        if subject != self.subject:
            self.subject, self.subject_types = subject, set()
        out: list[str] = []
        types: set[str] = set(vocabulary.domains.get(prop, ()))
        if prop == f"{RDF}type" and obj.startswith("<"):
            # The stated type itself is already in the data.
            self.subject_types.add(obj[1:-1])
            types |= vocabulary.superclasses.get(obj[1:-1], set())
        out.extend(f"{subject} {RDF_TYPE} <{cls}> .\n" for cls in types - self.subject_types)
        self.subject_types |= types
        out.extend(f"{subject} <{parent}> {obj} .\n"
                   for parent in vocabulary.superproperties.get(prop, ()))
        ranges = vocabulary.ranges.get(prop)
        if ranges and (obj.startswith("<") or obj.startswith("_:")):
            key = obj + predicate
            if key not in self.recent:
                if len(self.recent) >= TYPING_RECENT_OBJECTS:
                    self.recent.clear()
                self.recent[key] = None
                out.extend(f"{obj} {RDF_TYPE} <{cls}> .\n" for cls in ranges)
        self.written += len(out)
        return out


def open_ntriples(path: Path):
    """Read N-Triples, gzip-compressed or not, as text."""
    import gzip

    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open(encoding="utf-8")


def write_rdfs_typing(source: Path, vocabulary: Vocabulary, target: Path) -> int:
    """Write :class:`RdfsTyping` for a whole file. Returns the lines written."""
    typing = RdfsTyping(vocabulary)
    with open_ntriples(source) as lines, target.open("w", encoding="utf-8") as out:
        for line in lines:
            out.writelines(typing.consequences(line))
    return typing.written


def stream_with_typing(sources: Iterable[Path], vocabulary: Vocabulary | None, out) -> dict[str, int]:
    """Write each source's lines to ``out``, each followed by its RDFS typing.

    This is what QLever's index builder reads on stdin, so neither a decoded
    graph nor a typing file has to exist on disk. Returns line counts.
    """
    typing = RdfsTyping(vocabulary) if vocabulary is not None else None
    lines_in = 0
    for source in sources:
        with open_ntriples(source) as lines:
            for line in lines:
                if not line.strip():
                    continue
                lines_in += 1
                out.write(line if line.endswith("\n") else line + "\n")
                if typing is not None:
                    out.writelines(typing.consequences(line))
    return {"inputTriples": lines_in, "rdfsTypingTriples": typing.written if typing else 0}


# --- Results ------------------------------------------------------------------

def focus_nodes(document: dict) -> list[str]:
    """The ?this values of a SPARQL Results JSON document, as N-Triples terms."""
    nodes = []
    for binding in document.get("results", {}).get("bindings", []):
        term = binding.get("this")
        if not term:
            continue
        if term.get("type") == "uri":
            nodes.append(f"<{term['value']}>")
        elif term.get("type") == "bnode":
            nodes.append(f"_:{term['value']}")
        else:
            nodes.append(f'"{term["value"]}"')
    return sorted(set(nodes))


def result_text(constraint: SparqlConstraint, node: str) -> str:
    """One result, in the block format pyshacl writes, so one parser reads both."""
    lines = [
        f"Validation Result in SPARQLConstraintComponent ({SH}SPARQLConstraintComponent):",
        f"\tSeverity: sh:{constraint.severity}",
        f"\tSource Shape: <{constraint.shape}>",
        f"\tFocus Node: {node}",
    ]
    if constraint.message:
        lines.append(f"\tMessage: {constraint.message}")
    return "\n".join(lines)
