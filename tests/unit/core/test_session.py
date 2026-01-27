import os
import sys

import pyarrow
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import InvalidCursorStateError, MissingSqlStatement, UnsupportedSyntaxError
from opteryx.constants import ResultType


def setup_function():
    # Setup for each test, create a new session
    session = opteryx.Session()
    return session


def test_execute():
    session = opteryx.Session()
    session.execute("SELECT * FROM $planets")
    # session can now be re-used for additional queries
    session.execute("SELECT name FROM $planets LIMIT 1")
    result = session.fetchone()
    assert result[0] == "Mercury"


def test_rowcount():
    cursor = opteryx.query("SELECT * FROM $planets")
    assert cursor.rowcount == 9


def test_shape():
    cursor = opteryx.query("SELECT * FROM $planets")
    assert cursor.shape == (9, 20), cursor.shape


def test_fetchone():
    cursor = opteryx.query("SELECT * FROM $planets")
    one = cursor.fetchone()
    assert one[1] == "Mercury"


def test_fetchmany():
    cursor = opteryx.query("SELECT * FROM $planets")
    dual = cursor.fetchmany(2)
    assert len(dual) == 2


def test_fetchall():
    cursor = opteryx.query("SELECT * FROM $planets")
    all_rows = cursor.fetchall()
    assert len(all_rows) == 9, len(all_rows)


def test_execute_error():
    session = opteryx.Session()
    with pytest.raises(Exception):
        session.execute("SELECT * FROM non_existent_table")


def test_cursor_init():
    cursor = setup_function()
    assert not cursor  # __bool__ should be False before execution


def test_execute_to_arrow():
    cursor = setup_function()
    results = cursor.execute_to_arrow("SELECT * FROM $planets")
    assert results.shape == (9, 20)
    assert isinstance(results, pyarrow.Table)


def test_query_to_arrow():
    results = opteryx.query_to_arrow("SELECT * FROM $planets")
    assert results.shape == (9, 20)
    assert isinstance(results, pyarrow.Table)


def test_execute_to_arrow_batches():
    cursor = setup_function()
    batches = list(cursor.execute_to_arrow_batches("SELECT * FROM $planets", batch_size=3))
    assert all(isinstance(b, pyarrow.RecordBatch) for b in batches)
    assert sum(b.num_rows for b in batches) == 9


def test_execute_to_arrow_batches_limit():
    cursor = setup_function()
    batches = list(cursor.execute_to_arrow_batches("SELECT * FROM $planets", batch_size=2, limit=3))
    assert sum(b.num_rows for b in batches) == 3


def test_query_to_arrow_batches():
    batches = list(opteryx.query_to_arrow_batches("SELECT * FROM $planets", batch_size=4))
    assert all(isinstance(b, pyarrow.RecordBatch) for b in batches)


def test_execute_to_arrow_batches_consolidate():
    cursor = setup_function()
    # create two morsels 50 and 100 rows
    t1 = pyarrow.Table.from_pydict({"a": [1] * 50})
    t2 = pyarrow.Table.from_pydict({"a": [2] * 100})

    def fake_execute_statements(operation, params, visibility_filters):
        return (iter([t1, t2]), ResultType.TABULAR)

    cursor._execute_statements = fake_execute_statements

    batches = list(cursor.execute_to_arrow_batches("SELECT fakes", batch_size=150))
    assert len(batches) == 1
    assert batches[0].num_rows == 150

    cursor = setup_function()
    cursor._execute_statements = fake_execute_statements
    batches = list(cursor.execute_to_arrow_batches("SELECT fakes", batch_size=100))
    assert [b.num_rows for b in batches] == [100, 50]


def test_execute_to_arrow_batches_sets_description():
    cursor = setup_function()
    batches = cursor.execute_to_arrow_batches("SELECT * FROM $planets", batch_size=3)
    next(batches)
    assert cursor.description is not None


def test_execute_missing_sql_statement():
    cursor = setup_function()
    with pytest.raises(MissingSqlStatement):
        cursor.execute("")


def test_execute_unsupported_syntax_error():
    cursor = setup_function()
    with pytest.raises(UnsupportedSyntaxError):
        cursor.execute("SELECT * FROM table; SELECT * FROM table2", params=[1])


def test_non_tabular_result():
    cursor = setup_function()
    cursor.execute("SET @name = 'tim'")
    cursor.fetchall()


def test_limit():
    cursor = setup_function()
    dataset = cursor.execute_to_arrow("SELECT * FROM $planets", limit=3)
    assert dataset.num_rows == 3


def test_cursor_close_blocks_further_commands():
    cursor = setup_function()
    cursor.close()
    with pytest.raises(InvalidCursorStateError):
        cursor.execute("SELECT * FROM $planets")


def test_execute_to_arrow_can_repeat():
    cursor = setup_function()
    result_first = cursor.execute_to_arrow("SELECT * FROM $planets")
    assert result_first.shape == (9, 20)
    result_second = cursor.execute_to_arrow("SELECT name FROM $planets LIMIT 2")
    assert result_second.num_rows == 2


def test_cursor_truthiness_after_close():
    cursor = setup_function()
    cursor.execute("SELECT * FROM $planets")
    assert cursor
    cursor.close()
    assert not cursor


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
