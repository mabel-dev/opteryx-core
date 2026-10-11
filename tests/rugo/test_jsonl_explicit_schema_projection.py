"""explicit_schema declares a type only for a column that is READ.

Under a projection (`columns=[...]`), a declared column the caller did not request was
appended to the result anyway: read_jsonl's "a DECLARED column is always built" loop added
every explicit_schema key back after column discovery had narrowed to the projection. The
CSV reader declares types only for the columns it reads; JSONL now does the same.

What the declared-column contract still guarantees (and these tests pin):
  - columns=None: every declared column is built, even one no record carries (typed,
    all-null, listed in absent_columns) -- a schema pinned from another chunk relies on it;
  - a declared column that IS projected but absent from the data is built all-null and
    listed in absent_columns;
  - an unread declaration is still validated eagerly (a bad type name raises);
  - a predicate on a declared-but-unprojected column still filters, and its literal is
    still checked against the declared type.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from rugo import csv
from rugo import jsonl
from rugo.rugo_native import read_jsonl as native_read_jsonl

DATA = (
    b'{"a":1,"b":"x","t":"2024-01-02T03:04:05"}\n'
    b'{"a":2,"b":"y","t":"2024-02-03T04:05:06"}\n'
    b'{"a":3,"b":"z","t":null}\n'
)
CSV_DATA = b"a,b,t\n1,x,2024-01-02T03:04:05\n2,y,2024-02-03T04:05:06\n3,z,\n"


def _names(morsel):
    return [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]


def _read(data=DATA, **kwargs):
    with jsonl.read_jsonl(data, **kwargs) as reader:
        (morsel,) = list(reader)
    return morsel


def test_unread_declared_column_is_not_returned():
    morsel = _read(columns=["a", "b"], explicit_schema={"t": "TIMESTAMP[us]"})
    assert _names(morsel) == ["a", "b"]


def test_unread_declared_column_absent_from_data_is_not_returned():
    morsel = _read(columns=["a"], explicit_schema={"missing": "INT64"})
    assert _names(morsel) == ["a"]


def test_unread_declared_column_not_in_schema_or_absent_columns():
    result = native_read_jsonl(
        DATA, columns=["a"], explicit_schema={"t": "TIMESTAMP[us]", "missing": "INT64"}
    )
    assert result["column_names"] == ["a"]
    assert "t" not in result["schema"]
    assert "missing" not in result["schema"]
    assert result["absent_columns"] == []


def test_read_declared_column_is_typed_strictly():
    morsel = _read(columns=["a", "t"], explicit_schema={"a": "FLOAT64", "t": "TIMESTAMP[us]"})
    assert _names(morsel) == ["a", "t"]
    assert morsel.column(morsel.column_names[0]).to_pylist() == [1.0, 2.0, 3.0]
    assert morsel.column(morsel.column_names[1]).to_pylist()[2] is None


def test_projected_declared_column_absent_from_data_is_built_all_null():
    result = native_read_jsonl(DATA, columns=["a", "missing"], explicit_schema={"missing": "INT64"})
    assert result["column_names"] == ["a", "missing"]
    assert result["columns"][1].to_pylist() == [None, None, None]
    assert result["schema"]["missing"] == "INT64"
    assert result["absent_columns"] == ["missing"]


def test_no_projection_builds_every_declared_column():
    result = native_read_jsonl(DATA, explicit_schema={"missing": "INT64", "a": "INT64"})
    assert result["column_names"] == ["a", "b", "t", "missing"]
    assert result["columns"][3].to_pylist() == [None, None, None]
    assert result["absent_columns"] == ["missing"]


def test_projection_order_is_kept_whatever_the_declaration_order():
    morsel = _read(columns=["b", "a"], explicit_schema={"a": "INT64", "b": "VARCHAR", "t": "TIMESTAMP[us]"})
    assert _names(morsel) == ["b", "a"]


def test_unread_declaration_with_bad_type_still_raises():
    with pytest.raises(ValueError, match="explicit_schema"):
        _read(columns=["a"], explicit_schema={"t": "NOT_A_TYPE"})


def test_unread_nested_declaration_is_not_returned():
    data = b'{"a":1,"c":{"x":"p"}}\n{"a":2,"c":{"x":"q"}}\n'
    morsel = _read(data, columns=["a"], explicit_schema={"c->>'x'": "NVARCHAR"})
    assert _names(morsel) == ["a"]


def test_predicate_on_unread_declared_column_filters_without_returning_it():
    morsel = _read(columns=["b"], predicates=[("a", ">=", 2)], explicit_schema={"a": "INT64"})
    assert _names(morsel) == ["b"]
    assert morsel.column(morsel.column_names[0]).to_pylist() == ["y", "z"]


def test_predicate_literal_checked_against_unread_declared_type():
    with pytest.raises(ValueError):
        _read(columns=["b"], predicates=[("a", "==", "two")], explicit_schema={"a": "INT64"})


def test_csv_and_jsonl_agree_on_unread_declarations():
    schema = {"t": "TIMESTAMP[us]", "a": "INT64"}
    with csv.read_csv(CSV_DATA, columns=["a", "b"], explicit_schema=schema) as reader:
        (csv_morsel,) = list(reader)
    jsonl_morsel = _read(columns=["a", "b"], explicit_schema=schema)
    assert _names(csv_morsel) == _names(jsonl_morsel) == ["a", "b"]
    for name in csv_morsel.column_names:
        assert csv_morsel.column(name).to_pylist() == jsonl_morsel.column(name).to_pylist()


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
