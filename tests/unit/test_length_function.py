"""Tests for the LENGTH() function and its underlying kernel.

These focus on the regression where ``vector_lengther`` attempted to use the
list-oriented ``vector_length`` kernel.  The previous bug resulted in an
`opteryx.exceptions.FunctionExecutionError` when the argument arrived as a
string vector (the vast majority of real queries).

Here we exercise the function via the planner to ensure the fix is end-to-end
and also call the implementation directly to validate the arrow path.
"""

import pytest
import numpy
import pyarrow

from opteryx.expression.functions.implementations import text
from opteryx import query_session
from opteryx.connectors import DiskConnector


def test_vector_lengther_handles_string_and_dictionary():
    arr = pyarrow.array(["a", "bc", None])
    result = text.vector_lengther(arr)
    assert [v.as_py() for v in result] == [1, 2, None]

    # dictionary encoded version should also work
    import pyarrow as pa
    dict_arr = pyarrow.array(["a", "bc", None], type=pyarrow.dictionary(pa.int8(), pa.utf8()))
    result2 = text.vector_lengther(dict_arr)
    assert [v.as_py() for v in result2] == [1, 2, None]


def test_length_function_via_sql(tmp_path):
    # ensure testdata workspace is registered
    import opteryx
    opteryx.register_workspace("testdata", DiskConnector)
    session = opteryx.session()

    # run a couple of length queries against the built-in planets dataset
    session.execute("SELECT LENGTH(name) FROM $planets WHERE LENGTH(name) > 5")
    rows = session.fetchall()
    assert all(isinstance(r[0], int) for r in rows)

    # dictionary-encoded input to LENGTH should not raise; we use the
    # built-in $planets dataset which contains only plain strings.
    # (Other datasets with dictionary columns are exercised in the
    # integration suite.)
    pass
