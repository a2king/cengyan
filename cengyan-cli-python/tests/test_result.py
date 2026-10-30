import unittest
from cengyan import Result


class ResultTest(unittest.TestCase):
    def test_result_dict_rows_and_scalar(self):
        result = Result({"code": "OK", "columns": ["id", "name"], "rows": [[1, "a"]]})
        self.assertEqual(result.first(), {"id": 1, "name": "a"})
        self.assertEqual(result.scalar(), 1)
        self.assertEqual(result.to_tuples(), [(1, "a")])
