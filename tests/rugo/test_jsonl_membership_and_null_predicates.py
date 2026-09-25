"""JSONL predicates: `in`, `not in`, `is null`, `is not null`, and unknown operators.

The op parser used to map ANY unrecognised operator to EQ, so `("a", "in", [1, 3])`
ran as `a == "[1, 3]"` and returned zero rows -- indistinguishable from a legitimate
"nothing matched". These four operators are now evaluated natively with SQL
three-valued logic, matching rugo.parquet's row filter:

- `in` is an OR of equality; `not in` is an AND of inequality.
- A NULL (JSON null, or key absent from the record) satisfies neither `in` nor
  `not in`, and satisfies `is null` only.
- `x IN ()` matches nothing; `x NOT IN ()` matches everything, nulls included.
- An unknown operator raises ValueError.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo import jsonl
from rugo import parquet

# Row 2 has a JSON null in both columns' turn; row 4 omits "a" and "s" entirely.
DATA = (
    b'{"a":1,"s":"x","f":1.5}\n'
    b'{"a":null,"s":"y","f":null}\n'
    b'{"a":3,"s":null,"f":2.5}\n'
    b'{"id":4}\n'
    b'{"a":5,"s":"zz","f":3.0}\n'
)


def _read(predicates, columns=("a", "s", "f")):
    morsels = list(jsonl.read_jsonl(DATA, columns=list(columns), predicates=predicates))
    out = {c: [] for c in columns}
    for m in morsels:
        for c in columns:
            out[c].extend(m.column(c.encode()).to_pylist())
    return out


# (predicate, expected values of column "a")
NUMERIC_CASES = [
    (("a", "in", [1, 3]), [1, 3]),
    (("a", "in", (5,)), [5]),
    (("a", "in", {1, 5, 99}), [1, 5]),
    (("a", "in", [99]), []),
    (("a", "not in", [1]), [3, 5]),
    (("a", "not in", [1, 3, 5]), []),
    (("a", "not in", [99]), [1, 3, 5]),
    (("a", "is null", None), [None, None]),
    (("a", "is not null", None), [1, 3, 5]),
]


@pytest.mark.parametrize("predicate,expected", NUMERIC_CASES)
def test_numeric_column(predicate, expected):
    assert _read([predicate])["a"] == expected


STRING_CASES = [
    (("s", "in", ["x", "zz"]), ["x", "zz"]),
    (("s", "in", [b"y"]), ["y"]),
    (("s", "in", ["nope"]), []),
    (("s", "not in", ["x"]), ["y", "zz"]),
    (("s", "not in", ["x", "y", "zz"]), []),
    (("s", "is null", None), [None, None]),
    (("s", "is not null", None), ["x", "y", "zz"]),
]


@pytest.mark.parametrize("predicate,expected", STRING_CASES)
def test_string_column(predicate, expected):
    assert _read([predicate])["s"] == expected


FLOAT_CASES = [
    (("f", "in", [1.5, 3]), [1.5, 3.0]),
    (("f", "not in", [2.5]), [1.5, 3.0]),
    (("f", "is null", None), [None, None]),
    (("f", "is not null", None), [1.5, 2.5, 3.0]),
]


@pytest.mark.parametrize("predicate,expected", FLOAT_CASES)
def test_float_column(predicate, expected):
    assert _read([predicate])["f"] == expected


def test_empty_in_matches_nothing():
    assert _read([("a", "in", [])])["a"] == []


def test_empty_not_in_matches_everything_including_nulls():
    assert _read([("a", "not in", [])])["a"] == [1, None, 3, None, 5]


def test_null_predicate_column_not_projected():
    # The predicate column is read internally and projected away.
    assert _read([("a", "is null", None)], columns=("s",))["s"] == ["y", None]
    assert _read([("a", "in", [1, 3])], columns=("s",))["s"] == ["x", None]


def test_combined_with_comparison():
    got = _read([("a", "is not null", None), ("s", "not in", ["x"])])
    assert got["a"] == [5]


def test_in_matches_bool_column():
    data = b'{"b":true}\n{"b":false}\n{"b":null}\n'
    got = list(jsonl.read_jsonl(data, columns=["b"], predicates=[("b", "in", [True])]))
    assert [v for m in got for v in m.column(b"b").to_pylist()] == [True]
    got = list(jsonl.read_jsonl(data, columns=["b"], predicates=[("b", "not in", [True])]))
    assert [v for m in got for v in m.column(b"b").to_pylist()] == [False]


@pytest.mark.parametrize("op", ["like", "=", "IN", "between", ""])
def test_unknown_operator_raises(op):
    with pytest.raises(ValueError, match="Unknown predicate operator"):
        list(jsonl.read_jsonl(DATA, columns=["a"], predicates=[("a", op, 1)]))


@pytest.mark.parametrize("op", ["in", "not in"])
@pytest.mark.parametrize("value", [1, "x", b"x", None])
def test_membership_requires_collection(op, value):
    with pytest.raises(ValueError, match="takes a list"):
        list(jsonl.read_jsonl(DATA, columns=["a"], predicates=[("a", op, value)]))


@pytest.mark.parametrize("op", ["in", "not in"])
def test_membership_rejects_none_member(op):
    with pytest.raises(ValueError, match="cannot take None"):
        list(jsonl.read_jsonl(DATA, columns=["a"], predicates=[("a", op, [1, None])]))


@pytest.mark.parametrize("op", ["is null", "is not null"])
def test_null_tests_take_no_value(op):
    with pytest.raises(ValueError, match="takes no value"):
        list(jsonl.read_jsonl(DATA, columns=["a"], predicates=[("a", op, 1)]))


@pytest.mark.parametrize("op", ["==", "!=", "<"])
def test_comparison_rejects_none(op):
    with pytest.raises(ValueError, match="cannot take None"):
        list(jsonl.read_jsonl(DATA, columns=["a"], predicates=[("a", op, None)]))


PARITY_CASES = [
    ("a", "in", [1, 3]),
    ("a", "not in", [1]),
    ("a", "is null", None),
    ("a", "is not null", None),
    ("s", "in", ["x", "zz"]),
    ("s", "not in", ["x"]),
    ("s", "is null", None),
    ("s", "is not null", None),
    ("a", "in", []),
    ("a", "not in", []),
]


@pytest.mark.parametrize("predicate", PARITY_CASES)
def test_parity_with_parquet(predicate):
    """The same predicate over the same rows answers the same through both readers."""
    full = list(jsonl.read_jsonl(DATA, columns=["a", "s"]))
    assert len(full) == 1
    buf = parquet.write_parquet(full[0])
    col = predicate[0].encode()
    pq = [v for m in parquet.read_parquet(buf, columns=["a", "s"], predicates=[predicate])
          for v in m.column(col).to_pylist()]
    js = _read([predicate], columns=("a", "s"))[predicate[0]]
    assert js == pq


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
