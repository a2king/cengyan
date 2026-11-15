import unittest

import pycengyan


class ClusterOnlyTest(unittest.TestCase):
    def test_only_compute_client_api_is_exported(self):
        self.assertTrue(callable(pycengyan.ClusterClient))
        self.assertTrue(callable(pycengyan.ClusterGraph))
        self.assertFalse(hasattr(pycengyan, "Client"))
        self.assertFalse(hasattr(pycengyan, "GraphSession"))
