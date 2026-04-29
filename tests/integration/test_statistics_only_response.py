import pytest
import opteryx
from tests.helpers import execute_and_get_arrow, execute_and_get_rowcount, execute_and_get_shape, execute_and_fetch_all


def test_count_testdata_missions_value():
    # Ensure COUNT(*) from testdata.missions returns a single-row with the expected value
    table = execute_and_get_arrow("SELECT COUNT(*) FROM testdata.missions")
    assert table is not None
    assert getattr(table, "num_rows", 0) == 1
    # value should match known dataset size
    val = table.column(0).to_pylist()[0]
    assert val == 4630


def test_count_varlog_if_present():
    # Some environments may not have the ops catalog available; skip if not present
    try:
        table = execute_and_get_arrow("SELECT COUNT(*) FROM opteryx.ops.varlog")
    except Exception as e:
        pytest.skip(f"opteryx.ops.varlog not available: {e}")

    assert table is not None
    assert getattr(table, "num_rows", 0) == 1
    val = table.column(0).to_pylist()[0]
    assert isinstance(val, int)
