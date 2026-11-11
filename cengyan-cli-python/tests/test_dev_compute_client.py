"""The standalone cengyan.py shipped to Spark executors must use explicit graph context."""
import importlib.util
import json
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('spark_compute_client', Path(__file__).parents[2] / 'dev-scripts/cengyan.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class WireSocket:
    def __init__(self):
        self.sent = []
        self.code = 'OK'

    def settimeout(self, timeout):
        pass

    def sendall(self, data):
        self.sent.append(json.loads(data))

    def recv(self, size):
        return (json.dumps({'code': self.code}) + '\n').encode()


class SparkClientTest(unittest.TestCase):
    def client(self):
        client = module.Client('localhost', 19669, 'root', 'not-a-real-password', 30000)
        client._sock = WireSocket()
        client.session = 'test-token'
        return client

    def test_use_and_each_bulk_explicitly_send_graph_and_unique_request_id(self):
        client = self.client()
        client.execute('USE ys_relationship_v20261029;')
        for i in range(2):
            client.bulk_insert('node', 'org', [{'id': 'e_' + str(i)}])
        requests = client._sock.sent
        self.assertTrue(all(r['graph'] == 'ys_relationship_v20261029' for r in requests))
        self.assertEqual(3, len({r['requestId'] for r in requests}))
        self.assertEqual('test-token', requests[-1]['session'])

    def test_bulk_can_use_caller_owned_request_id_for_task_retries(self):
        client = self.client()
        client.graph = 'Company'
        client.bulk_insert('node', 'org', [{'id': 'e_1'}], request_id='spark-job-stage-partition-0')
        self.assertEqual('spark-job-stage-partition-0', client._sock.sent[-1]['requestId'])

    def test_missing_graph_fails_without_sending_write(self):
        client = self.client()
        with self.assertRaisesRegex(ValueError, 'graph'):
            client.bulk_insert('node', 'org', [{'id': 'a'}])
        self.assertEqual([], client._sock.sent)

    def test_failed_use_does_not_replace_current_graph(self):
        client = self.client()
        client.execute('USE Existing')
        client._sock.code = 'GRAPH_NOT_FOUND'
        client.execute('USE Missing')
        self.assertEqual('Existing', client.graph)

    def test_preflight_checks_type_in_explicit_graph(self):
        client = self.client()
        client.validate_target('Company', 'org')
        self.assertEqual('DESCRIBE NODE TYPE org', client._sock.sent[-1]['query'])
        self.assertEqual('Company', client._sock.sent[-1]['graph'])
        self.assertTrue(all(r['op'] == 'execute' for r in client._sock.sent))

    def test_preflight_missing_graph_stops_before_type_lookup(self):
        client = self.client()
        client._sock.code = 'GRAPH_NOT_FOUND'
        with self.assertRaises(module.CengyanError):
            client.validate_target('Missing', 'org')
        self.assertEqual(1, len(client._sock.sent))
