"""Verify a small COTTAS index family against RDF, including native queries.

Run inside the built Docker image with its /opt/pycottas-venv/bin/python.
"""
import argparse
import gzip
import itertools
import json
import subprocess
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pycottas
from rdflib import Graph

from vcf_rdfizer_cottas import cottas_index_paths, parse_cottas_indexes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--cottas', type=Path, required=True)
    parser.add_argument('--indexes', type=parse_cottas_indexes, default='all')
    parser.add_argument('--packages', action='store_true')
    args = parser.parse_args()
    opener = gzip.open if args.source.name.endswith('.gz') else open
    with opener(args.source, 'rt') as handle:
        expected = set(Graph().parse(data=handle.read(), format='nt'))
    report = {}
    for order, path in cottas_index_paths(args.cottas, args.indexes).items():
        table = pq.read_table(path)
        assert table.schema.metadata[b'index'].decode() == order, path
        rows = list(zip(*(table[name].to_pylist() for name in 'spo')))
        assert rows == sorted(set(rows), key=lambda row: tuple(row['spo'.index(c)] for c in order)), path
        with tempfile.TemporaryDirectory() as td:
            decoded = Path(td) / 'decoded.nt'
            pycottas.cottas2rdf(str(path), str(decoded))
            assert set(Graph().parse(decoded, format='nt')) == expected, path
        # Exercise each possible bound/unbound triple-pattern shape.
        probe = rows[0]
        for bound in itertools.product((False, True), repeat=3):
            pattern = ' '.join(probe[i] if bound[i] else '?'+name for i, name in enumerate('spo'))
            wanted = {row for row in rows if all(not bound[i] or row[i] == probe[i] for i in range(3))}
            assert set(pycottas.search(str(path), pattern)) == wanted, (path, pattern)
        if args.packages:
            assert gzip.decompress(Path(str(path)+'.gz').read_bytes()) == path.read_bytes(), path
            assert subprocess.check_output(['brotli', '-d', '-c', str(path)+'.br']) == path.read_bytes(), path
        report[order] = {'triples': len(rows), 'bytes': path.stat().st_size, 'native_query_shapes': 8}
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
