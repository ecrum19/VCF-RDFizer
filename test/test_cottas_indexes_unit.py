"""Index selection, real Parquet ordering, and the chunked pipeline contract."""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import vcf_rdfizer as host
from vcf_rdfizer_cottas import COTTAS_INDEXES, cottas_index_paths, parse_cottas_indexes
from test.test_cottas_tool import load_cottas_tool
from test.test_partitioned_compression_unit import load_runner_module

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pycottas
except ImportError:
    pa = None


class IndexSelectionTests(unittest.TestCase):
    def test_selection_and_names(self):
        self.assertEqual(parse_cottas_indexes(' SPO, pso,spo,POS '), ('spo', 'pso', 'pos'))
        self.assertEqual(parse_cottas_indexes(' ALL '), COTTAS_INDEXES)
        for value in ('', 'spo,', ',spo', 'sp', 'sspo', 'spog', 'all,spo', 'spp', 'sp o'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_cottas_indexes(value)
        self.assertEqual(cottas_index_paths(Path('a.cottas'), ('pos', 'spo')),
                         {'pos': Path('a.cottas'), 'spo': Path('a.spo.cottas')})

    def test_planned_outputs_include_every_index_and_package(self):
        paths = host.planned_output_paths(out_dir=Path('/out'), output_name='a', rdf_name=None,
                    methods=['cottas', 'cottas_gzip', 'cottas_brotli'], partitioned=True,
                    cottas_indexes=('pos', 'pso'))
        for name in ('a.cottas', 'a.pso.cottas'):
            for suffix in ('', '.gz', '.br'):
                self.assertIn(Path('/out/a') / (name + suffix), paths)
        with tempfile.TemporaryDirectory() as td:
            collision = Path(td) / 'a.pso.cottas'
            collision.touch()
            with self.assertRaises(ValueError):
                host.validate_no_output_collisions({'indexes': {collision}})

    def test_merge_commands_forward_orders(self):
        runner = load_runner_module()
        for order in COTTAS_INDEXES:
            self.assertEqual(runner.cottas_merge_command('python', Path('a'), Path('b'), Path('c'), order)[-1], order)
            self.assertEqual(runner.cottas_merge_many_command('python', [Path('a'), Path('b')], Path('c'), index=order)[-1], order)

    def test_cli_rejects_invalid_and_irrelevant_indexes(self):
        with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            with mock.patch.object(sys, 'argv', ['vcf-rdfizer', '--cottas-indexes', 'spp']), self.assertRaises(SystemExit):
                host.main()
            for mode in ('tsv', 'decompress', 'validation', 'index'):
                with mock.patch.object(sys, 'argv', ['vcf-rdfizer', '--mode', mode, '--cottas-indexes', 'pos', '-o', '/tmp/unused']):
                    self.assertEqual(host.main(), 2)

    def test_cli_requires_selected_cottas_representation(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / 'input.nt'
            source.write_text('<urn:s> <urn:p> <urn:o> .\n')
            vcf = root / 'input.vcf'
            vcf.write_text('##fileformat=VCFv4.3\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n')
            for mode, option, path in [('full','-i',vcf), ('compress','--rdf',source)]:
                argv = ['vcf-rdfizer', '-m', mode, option, str(path), '-o', str(root/'out'), '--cottas-indexes', 'pos', '--representations', 'none']
                with self.subTest(mode=mode), mock.patch.object(sys, 'argv', argv), mock.patch.object(host, 'check_docker') as docker, contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(host.main(), 2)
                    docker.assert_not_called()


@unittest.skipIf(pa is None, 'install pycottas and pyarrow for real Parquet tests')
class CottasIndexTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.tool = load_cottas_tool()
        self.env = mock.patch.dict(os.environ, {'COTTAS_SCRATCH_DIR': str(self.root / 'scratch'), 'COTTAS_MERGE_BATCH_ROWS': '2'})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.rows = [('<urn:z>', '<urn:a>', '"é"'), ('<urn:a>', '<urn:z>', '<urn:o>'),
                     ('<urn:a>', '<urn:a>', '"escaped \\"quote\\""'), ('<urn:z>', '<urn:z>', '"1"^^<urn:integer>')]

    def write(self, path, rows, order='spo', metadata=True):
        fields = {name: pa.array([row[i] for row in rows], type=pa.string()) for i, name in enumerate('spo')}
        table = pa.table(fields)
        if metadata:
            table = table.replace_schema_metadata({b'index': order.encode()})
        pq.write_table(table, path)

    def sorted(self, rows, order):
        return sorted(rows, key=lambda row: tuple(row['spo'.index(c)] for c in order))

    def assert_graph(self, path, order, rows=None):
        table = pq.read_table(path)
        self.assertEqual(table.schema.metadata[b'index'], order.encode())
        actual = list(zip(*(table[name].to_pylist() for name in 'spo')))
        self.assertEqual(actual, self.sorted(set(self.rows if rows is None else rows), order))

    def main(self, *args):
        with mock.patch.object(sys, 'argv', ['cottas_tool.py', *map(str, args)]):
            return self.tool.main()

    def test_all_conversion_indexes_parse_rdf_once(self):
        source = self.root / 'input.nt'
        source.write_text('\n'.join(' '.join(row) + ' .' for row in self.rows + self.rows[:1]) + '\n')
        output = self.root / 'out.cottas'
        with mock.patch.object(pycottas, 'rdf2cottas', wraps=pycottas.rdf2cottas) as convert:
            self.assertEqual(self.main('convert', source, output, 'ALL'), 0)
        self.assertEqual(convert.call_count, 1)
        for order, path in cottas_index_paths(output, COTTAS_INDEXES).items():
            self.assert_graph(path, order)
        self.assertEqual(list((self.root / 'scratch').iterdir()), [])

    def test_each_single_conversion_order(self):
        source = self.root / 'input.nt'
        source.write_text('<urn:z> <urn:a> "value" .\n<urn:a> <urn:z> <urn:o> .\n')
        rows = [('<urn:z>', '<urn:a>', '"value"'), ('<urn:a>', '<urn:z>', '<urn:o>')]
        for order in COTTAS_INDEXES:
            output = self.root / f'{order}.cottas'
            self.assertEqual(self.main('convert', source, output, order.upper()), 0)
            self.assert_graph(output, order, rows)

    def test_merge_all_orders_batches_duplicates_and_progress(self):
        self.tool.COTTAS_OUTPUT_BATCH_ROWS = 2
        self.tool.COTTAS_MERGE_PROGRESS_ROWS = 2
        for order in COTTAS_INDEXES:
            inputs = [self.root / f'{order}-{n}.cottas' for n in range(3)]
            for path, rows in zip(inputs, (self.rows[:3], self.rows[1:], [])):
                self.write(path, self.sorted(rows + rows[:1], order), order)
            output = self.root / f'{order}.cottas'
            progress = self.root / f'{order}.jsonl'
            self.tool.streaming_cottas_merge(list(map(str, inputs)), str(output), index=order.upper(), remove_input_files=True, progress_path=progress)
            self.assert_graph(output, order)
            self.assertFalse(any(path.exists() for path in inputs))
            events = [json.loads(line) for line in progress.read_text().splitlines()]
            self.assertEqual(events[-1]['phase'], 'complete')
            self.assertIn('merging', [event['phase'] for event in events])

    def test_merge_rejects_bad_inputs_and_preserves_output(self):
        good = self.root / 'good.cottas'
        bad = self.root / 'bad.cottas'
        output = self.root / 'output.cottas'
        self.write(good, self.sorted(self.rows, 'spo'))
        for defect in ('missing_index', 'wrong_index', 'unsorted', 'null', 'missing_column', 'type'):
            with self.subTest(defect=defect):
                if defect == 'missing_column':
                    pq.write_table(pa.table({'s': ['a'], 'p': ['b']}).replace_schema_metadata({b'index': b'spo'}), bad)
                elif defect == 'type':
                    pq.write_table(pa.table({'s': [1], 'p': [2], 'o': [3]}).replace_schema_metadata({b'index': b'spo'}), bad)
                else:
                    rows = [(None, '<urn:p>', '<urn:o>')] if defect == 'null' else self.sorted(self.rows, 'spo')
                    self.write(bad, rows[::-1] if defect == 'unsorted' else rows,
                               'pos' if defect == 'wrong_index' else 'spo', metadata=defect != 'missing_index')
                output.write_bytes(b'original')
                with self.assertRaises(RuntimeError):
                    self.tool.streaming_cottas_merge([str(good), str(bad)], str(output), index='spo', remove_input_files=True)
                self.assertEqual(output.read_bytes(), b'original')
                self.assertTrue(good.exists() and bad.exists())
                self.assertFalse(list(self.root.glob('.output.cottas.merge-*')))

    def test_merge_flushes_final_partial_batch(self):
        self.tool.COTTAS_OUTPUT_BATCH_ROWS = 3
        source = self.root / 'source.cottas'
        self.write(source, self.sorted(self.rows, 'spo'))
        output = self.root / 'output.cottas'
        self.tool.streaming_cottas_merge([str(source)], str(output), index='spo', remove_input_files=False)
        self.assert_graph(output, 'spo')
        self.assertEqual(pq.ParquetFile(output).metadata.num_row_groups, 2)

    def test_empty_batches_and_metadata_variants(self):
        from types import SimpleNamespace
        parquet = SimpleNamespace(metadata=SimpleNamespace(metadata={'index': 'POS'}))
        self.assertEqual(self.tool.cottas_file_index(parquet), 'pos')
        schema = pa.schema([(name, pa.string()) for name in 'spo'])
        empty = pa.RecordBatch.from_arrays([pa.array([], type=pa.string()) for _ in 'spo'], schema=schema)
        row = pa.RecordBatch.from_arrays([pa.array([term]) for term in self.rows[0]], schema=schema)
        parquet = SimpleNamespace(schema_arrow=schema, metadata=SimpleNamespace(metadata={b'index': b'spo'}, num_rows=1), iter_batches=lambda **kwargs: iter([empty, row]))
        stream = self.tool.CottasTripleStream(SimpleNamespace(ParquetFile=lambda path: parquet), Path('empty-batch'), 'spo', 2)
        self.assertEqual(stream.current, self.rows[0])
        stream.close()  # compatible with readers that do not expose close()

    def test_empty_merge_and_invalid_configuration(self):
        empty = self.root / 'empty.cottas'
        self.write(empty, [])
        output = self.root / 'out.cottas'
        self.tool.streaming_cottas_merge([str(empty)], str(output), index='spo', remove_input_files=False)
        self.assert_graph(output, 'spo', [])
        for inputs, order in (([], 'spo'), ([str(empty)], ''), ([str(empty)], 'spp')):
            with self.assertRaises(ValueError):
                self.tool.streaming_cottas_merge(inputs, str(output), index=order, remove_input_files=False)
        for value in ('0', '-2', 'abc'):
            with mock.patch.dict(os.environ, {'COTTAS_MERGE_BATCH_ROWS': value}), self.assertRaises(ValueError):
                self.tool.cottas_merge_batch_rows()
        self.tool.emit_merge_progress(self.root / 'absent' / 'progress', 'test', completed=0, total=0, detail='')
        with self.tool.cottas_scratch_workspace(), self.assertRaises(ValueError):
            self.tool.sort_cottas_chunk(empty, {'bad': output})

    def test_reindex_all_orders_and_same_order_rewrite(self):
        self.tool.COTTAS_OUTPUT_BATCH_ROWS = 2
        self.tool.COTTAS_REINDEX_FAN_IN = 2
        source = self.root / 'source.cottas'
        self.write(source, self.rows + self.rows, 'pos')  # deliberately unsorted: reindex repairs it
        self.assertEqual(self.main('reindex', source, 'all'), 0)
        for order, path in cottas_index_paths(source, COTTAS_INDEXES).items():
            self.assert_graph(path, order)
        self.assertEqual(self.main('reindex', source, 'spo'), 0)
        self.assert_graph(source, 'spo')
        self.assertEqual(list((self.root / 'scratch').iterdir()), [])

    def test_reindex_empty_without_metadata(self):
        source = self.root / 'empty.cottas'
        self.write(source, [], metadata=False)
        self.assertEqual(self.main('reindex', source, 'pos,ops'), 0)
        for order, path in cottas_index_paths(source, ('pos', 'ops')).items():
            self.assert_graph(path, order, [])

    def test_reindex_failure_and_collision_preserve_original(self):
        source = self.root / 'source.cottas'
        self.write(source, self.sorted(self.rows, 'spo'))
        original = source.read_bytes()
        with mock.patch.object(self.tool, 'reindex_cottas', side_effect=RuntimeError('failure')):
            with self.assertRaisesRegex(RuntimeError, 'failure'):
                self.main('reindex', source, 'pos,pso')
        with mock.patch.object(self.tool, 'reindex_cottas'):
            with self.assertRaisesRegex(RuntimeError, 'non-empty'):
                self.main('reindex', source, 'pos')
        sibling = self.root / 'source.pso.cottas'
        sibling.write_bytes(b'keep')
        with self.assertRaises(FileExistsError):
            self.main('reindex', source, 'pos,pso')
        self.assertEqual(source.read_bytes(), original)
        self.assertEqual(sibling.read_bytes(), b'keep')
        self.assertFalse(list(self.root.glob('.*.reindex-*')))
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.main('reindex', self.root / 'absent'), 2)
            self.assertEqual(self.main('merge-many', '--input-cottas-files', source, '--output-cottas-file', sibling), 2)

    def test_missing_dependencies(self):
        with mock.patch.dict(sys.modules, {'pycottas': None}), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.main('convert', 'a', 'b'), 127)
        with mock.patch.dict(sys.modules, {'pyarrow': None}), self.assertRaisesRegex(RuntimeError, 'PyArrow'):
            self.tool.streaming_cottas_merge(['a'], 'b', index='spo', remove_input_files=False)


class PartitionedIndexTests(unittest.TestCase):
    def run_pipeline(self, root, *, indexes=('pos', 'spo', 'ops'), chunk_bytes=55, failure=None, allow=False, methods='cottas,cottas_gzip,cottas_brotli'):
        runner = load_runner_module()
        work, out = root / 'work', root / 'out'
        work.mkdir(); out.mkdir()
        source = root / 'source.nt'
        source.write_text(''.join(f'<urn:s{i}> <urn:p> <urn:o> .\n' for i in range(7)))
        result_path = out / 'result.json'
        calls = []
        class StageRunner(runner.StageRunner):
            def run(self, name, command, output_path=None, *args):
                calls.append((name, command))
                failed = failure and failure in name
                if 'validate' in name:
                    output_path.write_text(json.dumps({'valid': not failed, 'count_match': not failed}))
                elif not failed and output_path:
                    output_path.write_bytes(b'artifact')
                    if 'build' in name:
                        for path in cottas_index_paths(output_path, parse_cottas_indexes(command[-1])).values():
                            path.write_bytes(b'chunk')
                stage = {'name': name, 'exit_code': int(bool(failed)), 'wall_seconds': 1, 'output_path': str(output_path), 'output_size_bytes': 8}
                self.stages.append(stage)
                return stage
        argv = ['runner', '--source', str(source), '--output-dir', str(out), '--output-name', 'sample', '--methods', methods,
                '--cottas-indexes', ','.join(indexes), '--target-chunk-bytes', str(chunk_bytes),
                '--min-chunk-bytes', '1', '--max-chunk-bytes', str(chunk_bytes*2), '--result-path', str(result_path)]
        if allow: argv.append('--allow-index-failures')
        with mock.patch.object(runner, 'StageRunner', StageRunner), mock.patch.object(runner, 'Path', wraps=Path, side_effect=lambda p: work if p == '/work' else Path(p)), mock.patch.object(sys, 'argv', argv), contextlib.redirect_stderr(io.StringIO()):
            code = runner.main()
        return code, json.loads(result_path.read_text()), calls, work, out

    def test_multiple_indexes_merge_validate_and_package(self):
        for chunk_bytes in (55, 10000):
            with self.subTest(chunk_bytes=chunk_bytes), tempfile.TemporaryDirectory() as td:
                code, report, calls, work, out = self.run_pipeline(Path(td), chunk_bytes=chunk_bytes)
                self.assertEqual(code, 0, report)
                details = report['methods']['cottas']['details']
                self.assertEqual(list(details['indexes']), ['pos', 'spo', 'ops'])
                self.assertEqual(details['index'], 'pos')
                self.assertEqual(details['merge_rounds'], int(chunk_bytes == 55))
                self.assertFalse(list(work.glob('*.cottas')))
                for order, path in cottas_index_paths(out / 'sample.cottas', ('pos','spo','ops')).items():
                    for suffix in ('', '.gz', '.br'):
                        self.assertTrue(Path(str(path)+suffix).exists())
                merges = [command for name, command in calls if name.startswith('cottas-merge')]
                self.assertEqual([command[command.index('--index')+1] for command in merges], ['pos','spo','ops'] if chunk_bytes==55 else [])
                builds = [command for name, command in calls if name.startswith('cottas-build')]
                self.assertEqual(len(builds), details['chunk_count'])
                self.assertTrue(all(command[-1] == 'pos,spo,ops' for command in builds))

    def test_failures_skip_indexes_and_packages_or_fail_strictly(self):
        for failure in ('cottas-build', 'cottas-merge-ops', 'cottas-validate-ops'):
            for allow in (True, False):
                with self.subTest(failure=failure, allow=allow), tempfile.TemporaryDirectory() as td:
                    code, report, calls, work, out = self.run_pipeline(Path(td), failure=failure, allow=allow)
                    self.assertEqual(code, 0 if allow else 1, report)
                    if allow:
                        self.assertEqual(report['methods']['cottas']['details']['index_status'], 'failed')
                        self.assertFalse(list(out.glob('*.cottas')))
                        self.assertFalse(list(work.glob('*.cottas')))
                        self.assertEqual(report['methods']['cottas_gzip']['source'], 'index_unavailable')
                    self.assertFalse(any(name.startswith('cottas-gzip') for name, _ in calls))
        with tempfile.TemporaryDirectory() as td:
            code, report, *_ = self.run_pipeline(Path(td), failure='cottas-gzip-spo')
            self.assertEqual(code, 1)
            self.assertIn('packaging failed for spo', report['error'])

class HostIndexRoutingTests(unittest.TestCase):
    def test_host_forwards_indexes_and_translates_every_artifact(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / 'source.nt'
            source.write_text('<urn:s> <urn:p> <urn:o> .\n')
            out = root / 'out'
            def run(command, **kwargs):
                if host.PARTITIONED_COMPRESSION_RUNNER_CONTAINER in command:
                    self.assertEqual(command[command.index('--cottas-indexes')+1], 'pos,pso')
                    indexes = {}
                    for order, name in [('pos','sample.cottas'), ('pso','sample.pso.cottas')]:
                        (out / name).write_bytes(b'cottas')
                        indexes[order] = {'output_path': '/data/out/'+name, 'output_size_bytes': 6}
                    payload = {'exit_code': 0, 'methods': {'cottas': {'details': {'index': 'pos', 'indexes': indexes}}}}
                    (out / '.sample.partitioned-results.json').write_text(json.dumps(payload))
                return 0
            with mock.patch.object(host, 'run', side_effect=run):
                ok, results = host.run_partitioned_representation_methods_for_rdf_files(
                    rdf_paths=[source], out_dir=out, image_ref='test', methods=['cottas'],
                    wrapper_log_path=root/'log', output_name='sample', target_chunk_bytes=100,
                    min_chunk_bytes=1, max_chunk_bytes=200, cottas_indexes=('pos','pso'))
            self.assertTrue(ok)
            for item in results['cottas']['details']['indexes'].values():
                self.assertTrue(Path(item['output_path']).is_file())
                self.assertEqual(Path(item['output_path']).parent, out)

    def test_index_mode_records_requested_indexes(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / 'source.cottas'
            source.write_bytes(b'old')
            def run(command, **kwargs):
                self.assertIn('pos,pso', command[-1])
                (root / 'source.pso.cottas').write_bytes(b'new')
                return 0
            with mock.patch.object(host, 'run', side_effect=run), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(host.run_index_mode(index_path=source, index_format='cottas',
                    metrics_dir=root/'metrics', image_ref='test', wrapper_log_path=root/'log',
                    cottas_indexes=('pos','pso')), 0)
            payload = json.loads((root/'metrics/stages/index/cottas-source.cottas.json').read_text())
            self.assertEqual(payload['indexes'], {'pos': str(source), 'pso': str(root/'source.pso.cottas')})

    def test_failed_reindex_reports_failure_even_when_original_survives(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / 'source.cottas'
            source.write_bytes(b'original')
            for code in (0, 1):
                with self.subTest(code=code), mock.patch.object(host, 'run', return_value=code), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(host.run_index_mode(index_path=source, index_format='cottas',
                        metrics_dir=root/'metrics', image_ref='test', wrapper_log_path=root/'log',
                        cottas_indexes=('pos','pso')), 1)
                payload = json.loads((root/'metrics/stages/index/cottas-source.cottas.json').read_text())
                self.assertEqual(payload['index_status'], 'failed')
                self.assertEqual(source.read_bytes(), b'original')


class AdapterEntryPointTests(unittest.TestCase):
    def test_script_entry_point_returns_dependency_exit_code(self):
        import runpy
        from test.test_cottas_tool import COTTAS_TOOL_PATH
        with mock.patch.dict(sys.modules, {'pycottas': None}), mock.patch.object(sys, 'argv', ['cottas_tool.py', 'convert', 'a', 'b']), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            runpy.run_path(str(COTTAS_TOOL_PATH), run_name='__main__')
        self.assertEqual(raised.exception.code, 127)
