"""
A conditional branch keeps its string-family type (VARBINARY / NVARCHAR).

A CASE / IIF / COALESCE branch that can raise (a CAST, arithmetic) is evaluated lazily:
on just the rows that take it, then scattered back to full length through draken's
string take. That take stamped every result VARCHAR (string_gather.h sg_finalize), so a
VARBINARY branch came back as text:

    CASE WHEN b THEN bin_col ELSE CAST(s AS VARBINARY) END
      -> "draken_if_then_else: VARBINARY branch paired with a text branch"
    CASE WHEN b THEN NULLIF(bin_col, json) ELSE CAST(s AS VARBINARY) END
      -> a VARCHAR column of binary bytes (UnicodeDecodeError reading it)

IIF/COALESCE hid it: their kernel widens a VARCHAR/VARBINARY mix to VARBINARY.
Found by the single-table fuzzer; values checked against DuckDB.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _column(sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    vector = morsels[0].column(morsels[0].column_names[0])
    values = [morsel[i][0] for morsel in morsels for i in range(len(morsel))]
    return vector.type_name, values


@pytest.mark.parametrize(
    "expression,expected_type",
    [
        ("CASE WHEN b_value THEN bin_null ELSE CAST(s_low AS VARBINARY) END", "VARBINARY"),
        ("CASE WHEN b_value THEN CAST(s_low AS VARBINARY) ELSE bin_value END", "VARBINARY"),
        ("CASE WHEN b_value THEN NULLIF(bin_null, json_doc) ELSE CAST(s_awkward AS VARBINARY) END", "VARBINARY"),
        ("CASE WHEN b_value THEN CAST(s_low AS NVARCHAR) ELSE s_high END", "NVARCHAR"),
    ],
)
def test_lazy_branch_keeps_its_type(expression, expected_type):
    type_name, values = _column(f"SELECT {expression} AS x FROM testdata.fuzzing.mixed")
    assert type_name == expected_type
    assert len(values) == 2000
    if expected_type == "VARBINARY":
        assert all(v is None or isinstance(v, bytes) for v in values)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
