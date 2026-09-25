"""Dataset ordering and identity through chunk conversion, merging and export."""
import contextlib
import gzip
import io
import itertools
import os
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import vcf_rdfizer as host
from vcf_rdfizer_cottas import COTTAS_INDEXES, COTTAS_QUAD_INDEXES, cottas_index_paths, parse_cottas_indexes
from test.test_cottas_tool import load_cottas_tool
from test.test_partitioned_compression_unit import load_runner_module

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pycottas
except ImportError:
    pa = None

FIXTURE = Path(__file__).with_name('fixtures') / 'cottas-dataset.nq'


class DatasetSelectionTests(unittest.TestCase):
    def test_all_permutations_and_names(self):
        self.assertEqual(len(set(COTTAS_QUAD_INDEXES)), 24)
        self.assertTrue(all(set(index) == set('spog') for index in COTTAS_QUAD_INDEXES))
        self.assertEqual(parse_cottas_indexes(' ALL-QUADS '), COTTAS_QUAD_INDEXES)
        self.assertEqual(parse_cottas_indexes(' GsPo,SPoG,gspo '), ('gspo', 'spog'))
        self.assertEqual(parse_cottas_indexes('all'), COTTAS_INDEXES)
        for selection in ('g', 'spg', 'spogg', 'all-quads,spo'):
            with self.assertRaises(ValueError):
                parse_cottas_indexes(selection)
        for name in ('sample.nq', 'sample.nq.gz'):
            source = Path(name)
            self.assertEqual(host.rdf_output_basename(source), 'sample')
            self.assertEqual(host.compression_artifact_name_for_method(source, 'gzip'), 'sample.nq.gz')
            self.assertEqual(host.compression_artifact_name_for_method(source, 'brotli'), 'sample.nq.br')
            self.assertEqual(host.compression_method_label_for_path(source, 'gzip'), 'gzip (.nq.gz)')
            planned = host.planned_output_paths(out_dir=Path('/out'), output_name='sample', rdf_name=None,
                        source_rdf_name=name, methods=['gzip', 'brotli', 'cottas'], partitioned=True, cottas_indexes=('gspo', 'spog'))
            self.assertIn(Path('/out/sample/sample.nq.gz'), planned)
            self.assertIn(Path('/out/sample/sample.spog.cottas'), planned)
            self.assertNotIn(Path('/out/sample/sample.nq'), planned)
        for codec in ('gzip', 'brotli'):
            suffix = 'gz' if codec == 'gzip' else 'br'
            self.assertEqual(host.default_decompressed_name(Path('sample.nq.' + suffix), codec), 'sample.nq')

    def test_dataset_preflight_prevents_graph_loss(self):
        for representations, indexes in [('hdt', 'spo'), ('cottas', 'spo'), ('cottas', 'gspo,spo')]:
            with tempfile.TemporaryDirectory() as td, self.subTest(representations=representations, indexes=indexes):
                argv = ['vcf-rdfizer', '-m', 'compress', '--rdf', str(FIXTURE), '-o', td,
                        '--representations', representations, '--cottas-indexes', indexes]
                with mock.patch.object(sys, 'argv', argv), mock.patch.object(host, 'check_docker') as docker, contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(host.main(), 2)
                    docker.assert_not_called()

    def test_gzip_dataset_chunks_keep_format_and_contents(self):
        runner = load_runner_module()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / 'dataset.nq.gz'
            source.write_bytes(gzip.compress(FIXTURE.read_bytes()))
            chunks = root / 'chunks'; chunks.mkdir()
            iterator, plan = runner.stream_chunks(source, chunks, target_bytes=100, min_bytes=1, max_bytes=200)
            paths = [path for path, metadata in iterator]
            self.assertGreater(len(paths), 1)
            self.assertTrue(all(path.suffix == '.nq' for path in paths))
            self.assertEqual(b''.join(path.read_bytes() for path in paths), FIXTURE.read_bytes())
            self.assertEqual(plan['record_count'], 10)


@unittest.skipIf(pa is None, 'install pycottas and pyarrow for real Parquet tests')
class QuadIndexTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.tool = load_cottas_tool()
        patch = mock.patch.dict(os.environ, {'COTTAS_SCRATCH_DIR': str(self.root / 'scratch'), 'COTTAS_MERGE_BATCH_ROWS': '2'})
        patch.start(); self.addCleanup(patch.stop)
        self.rows = [('<urn:s>', '<urn:p>', '"same"', graph) for graph in ('<urn:g1>', '<urn:g2>', None, '_:graph')]
        self.rows += [('_:shared', '<urn:p>', '_:object', '_:graph')]

    def main(self, *args):
        with mock.patch.object(sys, 'argv', ['cottas_tool', *map(str, args)]):
            return self.tool.main()

    def ordered(self, rows, order):
        return sorted(set(rows), key=lambda row: tuple((row['spog'.index(c)] is None, row['spog'.index(c)] or '') for c in order))

    def write(self, path, rows, order='spog', columns='spog'):
        table = pa.table({name: pa.array([row[i] for row in rows], type=pa.string()) for i, name in enumerate(columns)})
        pq.write_table(table.replace_schema_metadata({b'index': order.encode()}), path)

    def assert_rows(self, path, order, rows):
        table = pq.read_table(path)
        self.assertEqual(table.schema.metadata[b'index'], order.encode())
        self.assertEqual(list(zip(*(table[name].to_pylist() for name in 'spog'))), self.ordered(rows, order))

    def test_all_quad_orders_parse_once_and_preserve_lexical_terms(self):
        self.tool.COTTAS_OUTPUT_BATCH_ROWS = 3
        output = self.root / 'all.cottas'
        with mock.patch.object(self.tool, 'parse_nquads_chunk', wraps=self.tool.parse_nquads_chunk) as parse:
            self.assertEqual(self.main('convert', FIXTURE, output, 'all-quads'), 0)
        self.assertEqual(parse.call_count, 1)
        rows = list(zip(*(pq.read_table(output)[name].to_pylist() for name in 'spog')))
        self.assertEqual(len(rows), 10)
        self.assertIn(('_:shared', '<urn:p>', '_:object', '_:graph'), rows)
        self.assertIn(('<urn:z>', '<urn:a>', '"01"^^<http://www.w3.org/2001/XMLSchema#integer>', '<urn:g2>'), rows)
        self.assertIn(('<urn:a>', '<urn:z>', '"é"@fr', '<urn:g1>'), rows)
        for order, path in cottas_index_paths(output, COTTAS_QUAD_INDEXES).items():
            self.assert_rows(path, order, rows)
            import rdflib
            probe = self.rows[0]
            for bound in itertools.product((False, True), repeat=4):
                pattern = tuple(rdflib.util.from_n3(probe[i]) if bound[i] else None for i in range(4))
                expected = {row for row in rows if all(not bound[i] or row[i] == probe[i] for i in range(4))}
                self.assertEqual(set(pycottas.search(str(path), pattern)), expected)
        decoded = self.root / 'decoded.nq'
        self.assertEqual(self.main('decompress', output, decoded), 0)
        expected = {line for line in FIXTURE.read_text().splitlines() if not line.startswith('#')}
        self.assertEqual(set(decoded.read_text().splitlines()), expected)
        self.assertEqual(list((self.root / 'scratch').iterdir()), [])

    def test_every_single_quad_order(self):
        source = self.root / 'input.nq'
        source.write_text('<urn:s> <urn:p> "same" .\n' * 2)
        for order in COTTAS_QUAD_INDEXES:
            path = self.root / f'{order}.cottas'
            self.main('convert', source, path, order)
            self.assert_rows(path, order, [self.rows[2]])

    def test_named_graph_can_use_rdflib_default_identifier(self):
        source = self.root / 'default.nq'
        source.write_text('<urn:s> <urn:p> "same" <urn:x-rdflib:default> .\n<urn:s> <urn:p> "same" .\n')
        output = self.root / 'default.cottas'
        self.main('convert', source, output, 'gspo')
        self.assert_rows(output, 'gspo', [self.rows[2], self.rows[2][:3] + ('<urn:x-rdflib:default>',)])

    def test_chunk_merge_all_orders_retains_graph_identity(self):
        self.tool.COTTAS_OUTPUT_BATCH_ROWS = 2
        for order in COTTAS_QUAD_INDEXES:
            paths = [self.root / f'{order}-{n}.cottas' for n in range(3)]
            for path, rows in zip(paths, (self.rows[:4], self.rows[1:] + self.rows[1:], [])):
                self.write(path, self.ordered(rows, order), order)
            output = self.root / f'{order}.cottas'
            self.tool.streaming_cottas_merge(list(map(str, paths)), str(output), index=order, remove_input_files=True)
            self.assert_rows(output, order, self.rows)

    def test_reindex_legacy_default_and_unsorted_quads_all_orders(self):
        self.tool.COTTAS_OUTPUT_BATCH_ROWS = 2
        self.tool.COTTAS_REINDEX_FAN_IN = 2
        source = self.root / 'legacy.cottas'
        rows = [row[:3] + ('DEFAULT' if row[3] is None else row[3],) for row in self.rows]
        self.write(source, rows * 2, 'gspo')
        self.main('reindex', source, 'all-quads')
        for order, path in cottas_index_paths(source, COTTAS_QUAD_INDEXES).items():
            self.assert_rows(path, order, self.rows)

    def test_triples_can_gain_default_graph_and_mix_index_families(self):
        source = self.root / 'input.nt'
        source.write_text('<urn:s> <urn:p> "same" .\n')
        for selection in ('spo,gspo', 'gspo,spo'):
            output = self.root / (selection.replace(',', '-') + '.cottas')
            self.main('convert', source, output, selection)
            for order, path in cottas_index_paths(output, parse_cottas_indexes(selection)).items():
                self.assertEqual(pq.read_table(path).num_rows, 1)
                if 'g' in order:
                    self.assert_rows(path, order, [self.rows[2]])
        triple = self.root / 'triple.cottas'
        self.write(triple, [self.rows[2][:3]], 'spo', columns='spo')
        self.main('decompress', triple, self.root / 'triple.nt')
        self.assertEqual((self.root / 'triple.nt').read_text(), source.read_text())
        self.main('reindex', triple, 'gspo,spog')
        self.assert_rows(triple, 'gspo', [self.rows[2]])
        decoded = self.root / 'default.nt'
        self.main('decompress', triple, decoded)
        self.assertEqual(decoded.read_text(), source.read_text())

    def test_empty_dataset_conversion_and_reindex(self):
        source = self.root / 'empty.nq'; source.touch()
        output = self.root / 'empty.cottas'
        self.main('convert', source, output, 'gspo')
        self.assert_rows(output, 'gspo', [])
        self.main('reindex', output, 'spog,gspo')
        self.assert_rows(output, 'spog', [])

    def test_legacy_default_only_chunk_without_graph_column(self):
        source = self.root / 'triple.cottas'
        self.write(source, [self.rows[2][:3]], 'gspo', columns='spo')
        output = self.root / 'out.cottas'
        self.tool.streaming_cottas_merge([str(source)], str(output), index='gspo', remove_input_files=False)
        self.assert_rows(output, 'gspo', [self.rows[2]])
        self.write(source, [self.rows[2][:3] + ('DEFAULT',)], 'gspo')
        self.tool.streaming_cottas_merge([str(source)], str(output), index='gspo', remove_input_files=False)
        self.assert_rows(output, 'gspo', [self.rows[2]])
        self.main('decompress', source, self.root / 'legacy.nq')
        self.assertNotIn('DEFAULT', (self.root / 'legacy.nq').read_text())

    def test_quad_safety_guards(self):
        source = self.root / 'quads.cottas'
        output = self.root / 'out.cottas'
        self.write(source, self.rows, 'spo')
        for action in (lambda: self.main('convert', FIXTURE, output, 'spo'),
                       lambda: self.main('reindex', source, 'spo'),
                       lambda: self.tool.sort_cottas_chunk(source, {'spo': output})):
            with self.assertRaisesRegex(ValueError, 'containing g'):
                action()
        with self.assertRaisesRegex(RuntimeError, 'containing g'):
            self.tool.streaming_cottas_merge([str(source)], str(output), index='spo', remove_input_files=False)
        with self.assertRaisesRegex(ValueError, 'N-Quads'):
            self.main('decompress', source, self.root / 'bad.nt')
        self.write(source, [(None, '<urn:p>', '<urn:o>', None)])
        with self.assertRaisesRegex(RuntimeError, 'null RDF term'):
            self.main('decompress', source, self.root / 'bad.nq')

    def test_decoder_stdout_pipe_for_validation(self):
        source = self.root / 'quads.cottas'
        self.write(source, self.rows)
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        result = subprocess.run([sys.executable, self.tool.__file__, 'decompress', str(source), '/dev/stdout'],
                                env=environment, check=True, capture_output=True, text=True)
        self.assertEqual(len(result.stdout.splitlines()), len(self.rows))
        self.assertIn('<urn:s> <urn:p> "same" .', result.stdout)

    def test_parse_failure_restores_rdflib_setting(self):
        import rdflib
        source = self.root / 'bad.nq'; source.write_text('not RDF')
        before = rdflib.NORMALIZE_LITERALS
        with self.assertRaises(Exception):
            self.main('convert', source, self.root / 'bad.cottas', 'gspo')
        self.assertEqual(rdflib.NORMALIZE_LITERALS, before)
