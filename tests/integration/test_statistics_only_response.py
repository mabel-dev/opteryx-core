import pytest
import opteryx


def test_count_testdata_missions_value():
    # Ensure COUNT(*) from testdata.missions returns a single-row with the expected value
    table = opteryx.query_to_arrow("SELECT COUNT(*) FROM testdata.missions")
    assert table is not None
    assert getattr(table, "num_rows", 0) == 1
    # value should match known dataset size
    val = table.column(0).to_pylist()[0]
    assert val == 4630


def test_count_varlog_if_present():
    # Some environments may not have the ops catalog available; skip if not present
    try:
        table = opteryx.query_to_arrow("SELECT COUNT(*) FROM opteryx.ops.varlog")
    except Exception as e:
        pytest.skip(f"opteryx.ops.varlog not available: {e}")

    assert table is not None
    assert getattr(table, "num_rows", 0) == 1
    val = table.column(0).to_pylist()[0]
    assert isinstance(val, int)
