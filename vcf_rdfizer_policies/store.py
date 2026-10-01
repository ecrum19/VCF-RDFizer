"""Where the engine's queries run: an in-memory rdflib graph, or any SPARQL 1.1 endpoint.

Both stores answer the same query text and return rows of plain strings
(None for an unbound variable), so the engine does not know which it has.
Selector parameters reach both as inline VALUES (with_parameters): rdflib's
initBindings has no equivalent on an endpoint, and the obvious substitute, a
trailing VALUES clause, fails open (see with_parameters).
"""

import csv
import io
import re
import urllib.error
import urllib.parse
import urllib.request

from . import PolicyError

_WHERE = re.compile(r"\bWHERE\s*\{", re.IGNORECASE)
_UNSAFE_IRI = re.compile(r'[\x00-\x20<>"{}|^`\\]')


def iri(value: str) -> str:
    """An IRI as a SPARQL term, refusing one that could break out of the brackets."""
    if _UNSAFE_IRI.search(value):
        raise PolicyError(f"not a safe IRI: {value!r}")
    return f"<{value}>"


def _values(value) -> tuple:
    return tuple(value) if isinstance(value, (tuple, list)) else (value,)


def with_parameters(query: str, bindings) -> str:
    """`query` with each parameter as an inline VALUES block opening its outer WHERE group.

    Not a trailing VALUES clause: SPARQL joins that after the WHERE group has
    been evaluated, so a FILTER inside it sees the parameter unbound. A region
    selector written that way selects nothing, and a prohibition on the region
    releases it. Parameters must therefore be used in the outer group, not
    inside a subquery, whose scope an outer VALUES block does not reach.
    `bindings` is ((name, term or tuple of terms), ...); terms are rdflib terms.
    """
    if not bindings:
        return query
    match = _WHERE.search(query)
    if match is None:
        raise PolicyError("a query with parameters needs an explicit WHERE { ... }")
    blocks = " ".join(f"VALUES ?{name} {{ {' '.join(term.n3() for term in _values(value))} }}"
                      for name, value in bindings)
    return f"{query[:match.end()]} {blocks} {query[match.end():]}"


class MemoryStore:
    """An rdflib graph. For fixtures and small inputs; the reference implementation."""

    def __init__(self, graph):
        self.graph = graph

    def rows(self, query: str):
        result = self.graph.query(query)
        names = [str(v) for v in result.vars]
        for row in result:
            yield {n: None if row[n] is None else str(row[n]) for n in names}


class EndpointStore:
    """A SPARQL 1.1 endpoint, read as SPARQL CSV results so large answers stream."""

    def __init__(self, url: str, timeout: int = 3600):
        self.url, self.timeout = url, timeout

    def rows(self, query: str):
        request = urllib.request.Request(
            self.url, data=urllib.parse.urlencode({"query": query}).encode(),
            headers={"Accept": "text/csv", "Content-Type": "application/x-www-form-urlencoded"})
        try:
            response = urllib.request.urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as error:
            raise PolicyError(f"{self.url} refused a query: {error.read()[:300]!r}") from None
        with response:
            reader = csv.reader(io.TextIOWrapper(response, encoding="utf-8", newline=""))
            names = [n.lstrip("?") for n in next(reader, [])]
            for row in reader:
                yield {n: v or None for n, v in zip(names, row)}


def as_store(source):
    """A store for `source`: one already, or an rdflib graph to wrap."""
    return source if hasattr(source, "rows") else MemoryStore(source)
