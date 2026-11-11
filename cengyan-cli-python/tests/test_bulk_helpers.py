import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def load_module():
    common = types.ModuleType("ys_bigdata_tools.common")
    common.create_spark_session = lambda **kwargs: None
    common.safe_stop_spark_session = lambda spark: None
    package = types.ModuleType("ys_bigdata_tools")
    package.common = common
    sys.modules.setdefault("ys_bigdata_tools", package)
    sys.modules.setdefault("ys_bigdata_tools.common", common)
    path = Path(__file__).parents[2] / "dev-scripts" / "data_init_node_org.py"
    spec = importlib.util.spec_from_file_location("org_loader", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BulkHelperTest(unittest.TestCase):
    def test_partition_checks_target_and_submits_all_batches(self):
        module = load_module()
        calls = []
        class Client:
            def __init__(self, *args):
                calls.append(('init', args))
            def connect(self):
                pass
            def login(self):
                return {'code': 'OK'}
            def validate_target(self, graph, node_type):
                calls.append(('target', graph, node_type))
            def bulk_insert(self, kind, node_type, rows, **kwargs):
                calls.append(('batch', rows, kwargs))
                return {'code': 'OK', 'rowsAffected': len(rows)}
            def close(self):
                calls.append(('close',))
        config = dict(host='localhost', port=19669, user='root', password='test-only',
                      timeout_ms=30000, space='Company', node_type='org', batch=2,
                      id_field='eid', property_fields=['entname'], id_type='STRING', id_length=64,
                      job_id='org-test-run')
        # 被测脚本 import 的是旧名 cengyan，桩必须按它的导入名注册。
        with patch.dict(sys.modules, {'cengyan': types.SimpleNamespace(Client=Client)}):
            module.cengyan_udf(iter([{'eid': str(i), 'entname': 'name'} for i in range(3)]), config=config)
        self.assertIn(('target', 'Company', 'org'), calls)
        batches = [call[1] for call in calls if call[0] == 'batch']
        self.assertEqual([2, 1], [len(batch) for batch in batches])
        ids = [call[2]['request_id'] for call in calls if call[0] == 'batch']
        self.assertEqual(2, len(set(ids)))
        self.assertTrue(all(value.startswith('org-v2-') for value in ids))
        self.assertEqual(('close',), calls[-1])

    def test_driver_run_id_reaches_partition_config(self):
        module = load_module()
        server = dict(host='h', port=19669, user='u', password='test-only', space='G',
                      node_type='org', id_field='eid', id_type='STRING', id_length=64, job_id='new-run')
        config = module._partition_config(server, ['entname'], 512, 600000)
        self.assertEqual('new-run', config['job_id'])
        self.assertEqual(512, config['batch'])
        rows = [{'id': 'a', 'entname': 'A'}]
        self.assertEqual(module._batch_request_id(config, 0, rows),
                         module._batch_request_id(dict(config), 0, rows))

    def test_missing_run_id_is_not_silently_local(self):
        module = load_module()
        with self.assertRaisesRegex(ValueError, 'job_id'):
            module._batch_request_id(dict(space='G', node_type='org'), 0, [{'id': 'a'}])

    def test_identity_binds_run_target_position_and_content_not_attempt_number(self):
        module = load_module()
        context = types.SimpleNamespace(stageId=lambda: 0, partitionId=lambda: 2,
                                        getLocalProperty=lambda key: None, attemptNumber=lambda: 0)
        spark = types.SimpleNamespace(TaskContext=types.SimpleNamespace(get=lambda: context))
        config = dict(job_id='org-run-1', space='G', node_type='org')
        rows = [{'id': 'a', 'name': 'A'}, {'id': 'b', 'name': 'B'}]
        with patch.dict(sys.modules, {'pyspark': spark}):
            original = module._batch_request_id(config, 0, rows)
            context.attemptNumber = lambda: 3
            self.assertEqual(original, module._batch_request_id(config, 0, rows))
            self.assertEqual(original, module._batch_request_id(config, 0,
                             [{'name': 'A', 'id': 'a'}, {'name': 'B', 'id': 'b'}]))
            variants = [module._batch_request_id(dict(config, job_id='org-run-2'), 0, rows),
                        module._batch_request_id(dict(config, space='Other'), 0, rows),
                        module._batch_request_id(dict(config, node_type='person'), 0, rows),
                        module._batch_request_id(config, 1, rows),
                        module._batch_request_id(config, 0, list(reversed(rows))),
                        module._batch_request_id(config, 0, rows[:1])]
            context.partitionId = lambda: 3
            variants.append(module._batch_request_id(config, 0, rows))
            self.assertTrue(all(value != original for value in variants))
            self.assertEqual(len(set(variants)), len(variants))
            self.assertLessEqual(len(original), 120)

    def test_long_common_run_prefix_does_not_truncate_partition_identity(self):
        module = load_module()
        config = dict(job_id='x' * 200, space='G', node_type='org')
        self.assertNotEqual(module._batch_request_id(config, 0, [{'id': 'a'}]),
                            module._batch_request_id(config, 1, [{'id': 'a'}]))

    def test_org_row_uses_eid_as_id(self):
        module = load_module()
        row = module.build_vertex_row(
            {"eid": "e_001", "entname": "Acme"},
            id_field="eid",
            property_fields=["eid", "entname"],
        )
        self.assertEqual(row, {"id": "e_001", "entname": "Acme"})
