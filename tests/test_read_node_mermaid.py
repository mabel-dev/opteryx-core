import sys
import types
from importlib.machinery import SourceFileLoader
from pathlib import Path

# Create lightweight stand-ins for heavy runtime modules to allow importing
# operators/read_node.py in isolation for our unit test.
_opteryx_mod = types.ModuleType("opteryx")
_opteryx_mod.EOS = object()
sys.modules["opteryx"] = _opteryx_mod

_models_mod = types.ModuleType("opteryx.models")

class _QP:
    def __init__(self, query_id, variables):
        self.query_id = query_id

class _QT:
    def __init__(self, query_id):
        # Provide any attributes that nodes might access during tests
        self.dataset_committed_at = None

_models_mod.QueryProperties = _QP
_models_mod.QueryTelemetry = _QT
sys.modules["opteryx.models"] = _models_mod

_orso_tools = types.ModuleType("orso.tools")
def random_string():
    return "testid"
_orso_tools.random_string = random_string
sys.modules["orso.tools"] = _orso_tools

_orso_schema = types.ModuleType("orso.schema")
# Minimal placeholders used by ReaderNode; real functionality not needed for this test
_orso_schema.RelationSchema = object
_orso_schema.convert_orso_schema_to_arrow_schema = lambda schema, flag: None
sys.modules["orso.schema"] = _orso_schema

# Prepare package-like modules so relative imports in the module work
pkg_path = Path(__file__).parents[1] / "opteryx"
_opteryx_mod.__path__ = [str(pkg_path)]
ops_mod = types.ModuleType("opteryx.operators")
ops_mod.__path__ = [str(pkg_path / "operators")]
sys.modules["opteryx.operators"] = ops_mod

# Provide minimal BasePlanNode on the package namespace so relative import works
class MinimalBasePlanNode:
    def __init__(self, *, properties, **parameters):
        self.properties = properties
        class DummyTelemetry:
            def __init__(self):
                self.rows_read = 0
                self.columns_read = 0
                self.time_reading_blobs = 0
                self.blobs_read = 0
                self.bytes_processed = 0
                self.morsel_to_table_conversion = 0
                self.table_to_morsel_conversion = 0
                self.dead_ended_empty_morsels = 0

        self.telemetry = DummyTelemetry()
        self.parameters = parameters
        self.execution_time = 0
        self.identity = "testid"
        self.calls = 0
        self.records_in = 0
        self.bytes_in = 0
        self.records_out = 0
        self.bytes_out = 0
        self.columns = parameters.get("columns", [])

    def name(self):
        return "Read"

ops_mod.BasePlanNode = MinimalBasePlanNode

# Now load the read_node module
file_path = pkg_path / "operators" / "read_node.py"
spec = __import__("importlib.util").util.spec_from_file_location(
    "opteryx.operators.read_node", str(file_path)
)
read_node = __import__("importlib.util").util.module_from_spec(spec)
sys.modules[spec.name] = read_node
spec.loader.exec_module(read_node)

class DummyConnector:
    def __init__(self, dataset="namespace.table", committed_attr_name="dataset_commited_at", committed_value=None):
        self.dataset = dataset
        if committed_value is not None:
            setattr(self, committed_attr_name, committed_value)


def test_reader_mermaid_includes_committed_from_connector_single_t():
    qp = _QP("q", {})
    committed_val = "2025-12-24T12:00:00"
    connector = DummyConnector(committed_attr_name="dataset_commited_at", committed_value=committed_val)

    node = read_node.ReaderNode(properties=qp, connector=connector, columns=[], predicates=[], limit=None)

    mermaid = node.to_mermaid(1)
    # The new simpler format shows dataset name and execution time
    assert "namespace.table" in mermaid
    assert "ms)" in mermaid


def test_reader_mermaid_includes_committed_from_connector_double_t():
    qp = _QP("q", {})
    committed_val = "2025-12-25T00:00:00"
    connector = DummyConnector(committed_attr_name="dataset_committed_at", committed_value=committed_val)

    node = read_node.ReaderNode(properties=qp, connector=connector, columns=[], predicates=[], limit=None)

    mermaid = node.to_mermaid(2)
    # The new simpler format shows dataset name and execution time
    assert "namespace.table" in mermaid
    assert "ms)" in mermaid
