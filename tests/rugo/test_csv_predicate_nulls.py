"""
CSV predicate pushdown under SQL three-valued logic.

A comparison against NULL is UNKNOWN and a filter drops UNKNOWN rows, so a
NULL field (empty, quoted-empty, or missing trailing field) must satisfy NO
comparison operator -- `!=` included. This matches rugo.parquet (Draken compare
kernels + filter_mask) and rugo.jsonl.
"""

import operator

import pytest

import draken  # noqa: F401 — must precede rugo.csv to resolve draken symbols
from rugo.rugo_native import read_csv
from rugo import jsonl

OPS = {
    "==": operator.eq,
    "=": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}

# (csv column text, python values with None for NULL, predicate literal)
COLUMNS = {
    "int": (["1", "", "3", "", "5"], [1, None, 3, None, 5], 3),
    "float": (["1.5", "", "3.5", "", "5.5"], [1.5, None, 3.5, None, 5.5], 3.5),
    "string": (["apple", "", "cherry", "", "fig"], ["apple", None, "cherry", None, "fig"], "cherry"),
}


def _expected(values, op, literal):
    # NULL satisfies nothing; a non-NULL value is kept only when the comparison is TRUE.
    return [v for v in values if v is not None and OPS[op](v, literal)]


@pytest.mark.parametrize("kind", sorted(COLUMNS))
@pytest.mark.parametrize("op", sorted(OPS))
def test_empty_field_is_null_for_every_operator(kind, op):
    texts, values, literal = COLUMNS[kind]
    csv = ("v,k\n" + "".join(f"{t},{i}\n" for i, t in enumerate(texts))).encode()
    r = read_csv(csv, predicates=[("v", op, literal)])
    assert r["success"]
    assert r["columns"][0].to_pylist() == _expected(values, op, literal)


@pytest.mark.parametrize("kind", sorted(COLUMNS))
@pytest.mark.parametrize("op", sorted(OPS))
def test_empty_last_field_is_null_for_every_operator(kind, op):
    # The predicate column is last on the line, so its NULL is an empty field
    # immediately before the newline.
    texts, values, literal = COLUMNS[kind]
    csv = ("k,v\n" + "".join(f"{i},{t}\n" for i, t in enumerate(texts))).encode()
    r = read_csv(csv, columns=["v"], predicates=[("v", op, literal)])
    assert r["success"]
    assert r["columns"][0].to_pylist() == _expected(values, op, literal)


@pytest.mark.parametrize("kind", sorted(COLUMNS))
@pytest.mark.parametrize("op", sorted(OPS))
def test_missing_trailing_field_is_null_for_every_operator(kind, op):
    # Short rows: the predicate column is absent entirely (end_row fill path).
    texts, values, literal = COLUMNS[kind]
    lines = [f"{i},{t}" if t != "" else f"{i}" for i, t in enumerate(texts)]
    csv = ("k,v\n" + "".join(line + "\n" for line in lines)).encode()
    r = read_csv(csv, predicates=[("v", op, literal)])
    assert r["success"]
    assert r["columns"][1].to_pylist() == _expected(values, op, literal)


@pytest.mark.parametrize("op", sorted(OPS))
def test_quoted_empty_string_is_not_null(op):
    # A quoted "" is an empty string, not NULL: it compares as '' and is kept
    # wherever that comparison is TRUE (e.g. != 'cherry', < 'cherry').
    csv = b'v\napple\n""\ncherry\n\nfig\n'
    r = read_csv(csv, predicates=[("v", op, "cherry")])
    assert r["success"]
    values = ["apple", "", "cherry", None, "fig"]
    assert r["columns"][0].to_pylist() == _expected(values, op, "cherry")


def test_not_equal_matches_jsonl():
    C = b"a,s\n1,x\n,y\n3,\n"
    J = b'{"a":1,"s":"x"}\n{"a":null,"s":"y"}\n{"a":3,"s":null}\n'
    csv_rows = read_csv(C, predicates=[("a", "!=", 1)])["columns"][0].to_pylist()
    jsonl_rows = [m.column(b"a").to_pylist() for m in jsonl.read_jsonl(J, predicates=[("a", "!=", 1)])]
    assert csv_rows == [3]
    assert jsonl_rows == [[3]]


def test_equals_alias():
    csv = b"id\n1\n2\n3\n"
    assert read_csv(csv, predicates=[("id", "=", 2)])["columns"][0].to_pylist() == [2]


def test_unknown_operator_still_rejected():
    with pytest.raises(ValueError, match="Unknown predicate operator"):
        read_csv(b"id\n1\n", predicates=[("id", "<>", 1)])
