import unittest
from cengyan import Property, NodeRow


class ModelTest(unittest.TestCase):
    def test_models_are_lightweight_dataclasses(self):
        self.assertTrue(Property("name", "STRING").nullable)
        self.assertEqual(NodeRow("p-1").properties, {})
