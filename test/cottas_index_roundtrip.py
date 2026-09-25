"""Verify a small COTTAS index family against RDF, including native queries.

Run inside the built Docker image with its /opt/pycottas-venv/bin/python.
"""
import argparse
import gzip
import itertools
import json
import subprocess
import tempfile
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pycottas
import rdflib

from vcf_rdfizer_cottas import cottas_index_paths, parse_cottas_indexes


class BlankNodeLabels(dict):
    def get(self, key, default=None):
        return key


def read_rdf(text, dataset):
    rdflib.NORMALIZE_LITERALS = False
    graph = rdflib.Dataset() if dataset else rdflib.Graph()
    if dataset:
        graph.default_context = rdflib.Graph(store=graph.store, identifier=rdflib.BNode())
    graph.parse(data=text, format='nquads' if dataset else 'nt', bnode_context=BlankNodeLabels())
    if dataset:
        return {(s, p, o, None if g == graph.default_context.identifier else g) for s, p, o, g in graph.quads()}
    return set(graph)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--cottas', type=Path, required=True)
    parser.add_argument('--indexes', type=parse_cottas_indexes, default='all')
    parser.add_argument('--packages', action='store_true')
    args = parser.parse_args()
    dataset = args.source.name.endswith(('.nq', '.nq.gz'))
    opener = gzip.open if args.source.name.endswith('.gz') else open
    with opener(args.source, 'rt') as handle:
        expected = read_rdf(handle.read(), dataset)
    report = {}
    for order, path in cottas_index_paths(args.cottas, args.indexes).items():
        table = pq.read_table(path)
        assert table.schema.metadata[b'index'].decode() == order, path
        columns = 'spog' if 'g' in order else 'spo'
        rows = list(zip(*(table[name].to_pylist() for name in columns)))
        assert rows == sorted(set(rows), key=lambda row: tuple((row[columns.index(c)] is None, row[columns.index(c)] or '') for c in order)), path
        with tempfile.TemporaryDirectory() as td:
            decoded = Path(td) / ('decoded.nq' if 'g' in order else 'decoded.nt')
            subprocess.run([sys.executable, '/opt/vcf-rdfizer/cottas_tool.py', 'decompress', str(path), str(decoded)], check=True)
            wanted = expected if dataset or 'g' not in order else {(*triple, None) for triple in expected}
            assert read_rdf(decoded.read_text(), 'g' in order) == wanted, path
        # Exercise every bound/unbound shape with a named-graph probe when present.
        probe = next((row for row in rows if all(term is not None and not term.startswith('_:') for term in row)), rows[0])
        shapes = 0
        for bound in itertools.product((False, True), repeat=len(columns)):
            if any(bound[i] and probe[i] is None for i in range(len(columns))):
                continue  # None denotes a wildcard in pycottas, not a graph constant.
            # pycottas 1.1.0 truncates string patterns to three terms; tuples
            # are its documented API for a four-term quad pattern.
            pattern = tuple(rdflib.util.from_n3(probe[i]) if bound[i] else None for i in range(len(columns)))
            wanted = {row for row in rows if all(not bound[i] or row[i] == probe[i] for i in range(len(columns)))}
            assert set(pycottas.search(str(path), pattern)) == wanted, (path, pattern)
            shapes += 1
        if args.packages:
            assert gzip.decompress(Path(str(path)+'.gz').read_bytes()) == path.read_bytes(), path
            assert subprocess.check_output(['brotli', '-d', '-c', str(path)+'.br']) == path.read_bytes(), path
        report[order] = {'quads' if 'g' in order else 'triples': len(rows), 'bytes': path.stat().st_size, 'native_query_shapes': shapes}
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
