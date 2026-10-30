import importlib.util
import sys
import types
import unittest
from pathlib import Path


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
    def test_org_row_uses_eid_as_id(self):
        module = load_module()
        row = module.build_vertex_row(
            {"eid": "e_001", "entname": "Acme"},
            id_field="eid",
            property_fields=["eid", "entname"],
        )
        self.assertEqual(row, {"id": "e_001", "entname": "Acme"})
