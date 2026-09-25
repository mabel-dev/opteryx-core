"""
CSV and JSONL predicate literals must match their column's type — fail loud.

A predicate whose literal is of a different type from the column raises ValueError
naming the column, its type and the literal. It never answers "no rows" (JSONL used
to), never returns a zero-column morsel (CSV used to), and never coerces (both used
to compare the string '1' against a numeric column as the number 1). The contract
lives in rugo/src/predicate_literal.hpp.
"""

import pytest

import draken  # noqa: F401 — must precede rugo to resolve draken symbols
from rugo import csv, jsonl

CSV = b"a,f,s\n1,1.5,x\n,,y\n3,3.5,\n"
JSONL = (
    b'{"a":1,"f":1.5,"s":"x","b":true}\n'
    b'{"a":null,"f":null,"s":"y","b":false}\n'
    b'{"a":3,"f":3.5,"s":null,"b":null}\n'
)


def _rows(reader):
    return [
        {(k if isinstance(k, str) else k.decode()): m.column(k).to_pylist() for k in m.column_names}
        for m in reader
    ]


# ---------------------------------------------------------------------------
# Mismatches raise, naming the column, its type and the value
# ---------------------------------------------------------------------------

STRING_COLUMN_BAD = [1, 1.5, True, False]
NUMERIC_COLUMN_BAD = ["1", b"1", "x", True]


@pytest.mark.parametrize("value", STRING_COLUMN_BAD)
def test_csv_string_column_refuses_non_string(value):
    with pytest.raises(ValueError, match=r"column 's' \(VARCHAR\)"):
        _rows(csv.read_csv(CSV, predicates=[("s", "==", value)]))


@pytest.mark.parametrize("column", ["a", "f"])
@pytest.mark.parametrize("value", NUMERIC_COLUMN_BAD)
def test_csv_numeric_column_refuses_non_number(column, value):
    with pytest.raises(ValueError, match=rf"column '{column}' \((INT64|FLOAT64)\)"):
        _rows(csv.read_csv(CSV, predicates=[(column, "==", value)]))


@pytest.mark.parametrize("value", STRING_COLUMN_BAD)
def test_jsonl_string_column_refuses_non_string(value):
    with pytest.raises(ValueError, match=r"column 's' \(JSON string\)"):
        _rows(jsonl.read_jsonl(JSONL, predicates=[("s", "==", value)]))


@pytest.mark.parametrize("column", ["a", "f"])
@pytest.mark.parametrize("value", NUMERIC_COLUMN_BAD)
def test_jsonl_numeric_column_refuses_non_number(column, value):
    with pytest.raises(ValueError, match=rf"column '{column}' \(JSON number\)"):
        _rows(jsonl.read_jsonl(JSONL, predicates=[(column, "==", value)]))


@pytest.mark.parametrize("value", [1, 1.0, "true", b"true"])
def test_jsonl_boolean_column_refuses_non_bool(value):
    with pytest.raises(ValueError, match=r"column 'b' \(JSON boolean\)"):
        _rows(jsonl.read_jsonl(JSONL, predicates=[("b", "==", value)]))


def test_message_names_the_value():
    with pytest.raises(ValueError, match=r"int value 1\b"):
        _rows(csv.read_csv(CSV, predicates=[("s", "==", 1)]))
    with pytest.raises(ValueError, match=r"string value '1'"):
        _rows(jsonl.read_jsonl(JSONL, predicates=[("a", "==", "1")]))


@pytest.mark.parametrize("op", ["==", "!=", "<", "<=", ">", ">="])
def test_every_comparison_operator_is_checked(op):
    with pytest.raises(ValueError):
        _rows(csv.read_csv(CSV, predicates=[("s", op, 1)]))
    with pytest.raises(ValueError):
        _rows(jsonl.read_jsonl(JSONL, predicates=[("s", op, 1)]))


def test_jsonl_membership_members_are_checked():
    with pytest.raises(ValueError, match=r"column 's'"):
        _rows(jsonl.read_jsonl(JSONL, predicates=[("s", "in", ["x", 1])]))
    with pytest.raises(ValueError, match=r"column 'a'"):
        _rows(jsonl.read_jsonl(JSONL, predicates=[("a", "not in", ["1"])]))


@pytest.mark.parametrize("reader,data", [(csv.read_csv, CSV), (jsonl.read_jsonl, JSONL)])
def test_none_is_refused(reader, data):
    with pytest.raises(ValueError, match="is null"):
        _rows(reader(data, predicates=[("a", "==", None)]))


@pytest.mark.parametrize("reader,data", [(csv.read_csv, CSV), (jsonl.read_jsonl, JSONL)])
def test_unsupported_literal_type_is_refused(reader, data):
    with pytest.raises(ValueError, match="unsupported literal type"):
        _rows(reader(data, predicates=[("a", "==", object())]))


# ---------------------------------------------------------------------------
# The check does not depend on which rows another predicate keeps
# ---------------------------------------------------------------------------


def test_csv_raises_even_when_another_predicate_rejects_every_row():
    with pytest.raises(ValueError, match=r"column 's'"):
        _rows(csv.read_csv(CSV, predicates=[("a", "==", 99), ("s", "==", 1)]))


def test_jsonl_raises_even_when_another_predicate_rejects_every_row():
    with pytest.raises(ValueError, match=r"column 's'"):
        _rows(jsonl.read_jsonl(JSONL, predicates=[("a", "==", 99), ("s", "==", 1)]))


def test_csv_checks_a_predicate_only_column():
    with pytest.raises(ValueError, match=r"column 's' \(VARCHAR\)"):
        _rows(csv.read_csv(CSV, columns=["a"], predicates=[("s", "==", 1)]))


def test_csv_checks_an_empty_body():
    # Header only: the column has no values, so its type is the sniffer's default.
    with pytest.raises(ValueError, match=r"column 's'"):
        _rows(csv.read_csv(b"a,s\n", explicit_schema={"s": "VARCHAR"}, predicates=[("s", "==", 1)]))


def test_jsonl_value_past_the_sample_window_raises():
    # The head sample is all numbers; the mismatch only appears later.
    data = b'{"a":1}\n' * 10 + b'{"a":"x"}\n'
    with pytest.raises(ValueError, match=r"column 'a' \(JSON string\)"):
        _rows(jsonl.read_jsonl(data, predicates=[("a", ">=", 1)]))


def test_jsonl_threaded_mismatch_raises():
    # Large enough to split across threads; the mismatch is in the last range.
    data = b"".join(b'{"a":%d}\n' % i for i in range(400_000)) + b'{"a":"oops"}\n'
    with pytest.raises(ValueError, match=r"column 'a' \(JSON string\)"):
        _rows(jsonl.read_jsonl(data, predicates=[("a", ">", 5)]))


def test_jsonl_prefilter_does_not_hide_a_mismatch():
    # A long int literal on a string column: the raw prefilter must not drop every
    # record before the mismatch can be seen.
    data = b'{"s":"abcdefghij"}\n' * 3
    with pytest.raises(ValueError, match=r"column 's' \(JSON string\)"):
        _rows(jsonl.read_jsonl(data, predicates=[("s", "==", 12345678901)]))


def test_jsonl_object_column_compares_with_nothing():
    with pytest.raises(ValueError, match=r"column 'o' \(JSON object\)"):
        _rows(jsonl.read_jsonl(b'{"o":{"k":1}}\n', predicates=[("o", "==", "x")]))


def test_jsonl_declared_column_checked_on_empty_input():
    with pytest.raises(ValueError, match=r"column 's' \(VARCHAR\)"):
        _rows(jsonl.read_jsonl(b"", explicit_schema={"s": "VARCHAR"}, predicates=[("s", "==", 1)]))


# ---------------------------------------------------------------------------
# Matching literals still answer, and nothing is coerced
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("reader,data", [(csv.read_csv, CSV), (jsonl.read_jsonl, JSONL)])
@pytest.mark.parametrize(
    "predicate,column,expected",
    [
        (("a", "==", 1), "a", [1]),
        (("a", "<", 2.5), "a", [1]),
        (("f", ">", 2), "f", [3.5]),
        (("f", "==", 1.5), "f", [1.5]),
        (("s", "==", "x"), "s", ["x"]),
        (("s", "==", b"y"), "s", ["y"]),
    ],
)
def test_matching_literals_filter(reader, data, predicate, column, expected):
    rows = _rows(reader(data, predicates=[predicate]))
    assert rows[0][column] == expected


def test_csv_string_literal_is_compared_as_a_string():
    # '01' is not '1' on a VARCHAR column — the literal is not read as a number.
    rows = _rows(csv.read_csv(b"s\n01\nx\n1\n1.0\n", predicates=[("s", "==", "1")]))
    assert rows == [{"s": ["1"]}]


def test_jsonl_boolean_literal():
    rows = _rows(jsonl.read_jsonl(JSONL, predicates=[("b", "==", True)]))
    assert rows[0]["b"] == [True]


def test_csv_declared_bool_column():
    data = b"b\ntrue\nfalse\n"
    rows = _rows(csv.read_csv(data, explicit_schema={"b": "BOOL"}, predicates=[("b", "==", True)]))
    assert rows == [{"b": [True]}]
    with pytest.raises(ValueError, match=r"column 'b' \(BOOL\)"):
        _rows(csv.read_csv(data, explicit_schema={"b": "BOOL"}, predicates=[("b", "==", 1)]))


def test_csv_declared_ipv4_is_not_a_numeric_column():
    # IPV4 shares UINT32's physical tag; its predicate is outside the contract and
    # keeps the original byte-wise compare.
    rows = _rows(
        csv.read_csv(b"ip\n10.0.0.1\n10.0.0.2\n", explicit_schema={"ip": "IPV4"},
                     predicates=[("ip", "==", "10.0.0.1")])
    )
    assert len(rows[0]["ip"]) == 1


# ---------------------------------------------------------------------------
# CSV: never a zero-column morsel; every predicate on a column applies
# ---------------------------------------------------------------------------


def test_csv_zero_surviving_rows_keeps_every_column():
    rows = _rows(csv.read_csv(CSV, predicates=[("a", "==", 99)]))
    assert rows == [{"a": [], "f": [], "s": []}]


def test_csv_header_only_keeps_every_column():
    assert _rows(csv.read_csv(b"a,s\n")) == [{"a": [], "s": []}]


def test_csv_predicate_on_unknown_column_raises():
    with pytest.raises(ValueError, match=r"predicate on column 'zz', which is not in this CSV"):
        _rows(csv.read_csv(CSV, predicates=[("zz", "==", 1)]))


def test_csv_several_predicates_on_one_column_all_apply():
    rows = _rows(csv.read_csv(b"a\n1\n3\n9\n", predicates=[("a", ">", 2), ("a", "<", 5)]))
    assert rows == [{"a": [3]}]


def test_csv_empty_input_raises():
    with pytest.raises(ValueError, match="input is empty"):
        _rows(csv.read_csv(b""))


def test_csv_all_unknown_projection_raises():
    with pytest.raises(ValueError, match="none of the requested columns"):
        _rows(csv.read_csv(CSV, columns=["x", "y"]))
