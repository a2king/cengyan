import unittest

from pycengyan.partition import part_of, xxhash64


class PartitionHashTest(unittest.TestCase):
    def test_matches_java_partition_router_vectors(self):
        self.assertEqual(0xEF46DB3751D8E999, xxhash64(b""))
        self.assertEqual(0xD24EC4F1A98C6E5B, xxhash64(b"a"))
        self.assertEqual(0x44BC2CF5AD770999, xxhash64(b"abc"))

    def test_string_and_int_ids_stay_inside_partition_count(self):
        self.assertEqual(part_of("123", "STRING", 32), part_of("123", "STRING", 32))
        self.assertLess(part_of("123", "STRING", 32), 32)
        self.assertEqual(part_of(123, "INT64", 32), xxhash64(b"\x01" + (123).to_bytes(8, "big")) % 32)
