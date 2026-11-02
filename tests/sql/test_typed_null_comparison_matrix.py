"""
Typed NULL x comparison operator matrix.

Every type in reference/types.json against every comparison operator in
reference/operators.json, generated — no hand-written expectations. The oracle is the
BOOLEAN rule (see test_typed_null_literals.py): a typed NULL behaves like a value of
its type. So for each (type, operator), the same comparison over two NON-null values
is run first:

* refused  -> a NULL form may still run (a typed NULL is a constant, so it can pass a
  constant-only operand the value form failed), but then it answers NULL and
  WHERE / WHERE NOT match nothing — never a non-NULL answer;
* answered -> NULL on the left, the right or both answers NULL; a column holding
  NULLs answers NULL on those rows in a projection, and WHERE and WHERE NOT both
  drop them (three-valued logic: NOT NULL is NULL, not TRUE).
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

_REFERENCE = os.path.join(os.path.dirname(__file__), "..", "..", "reference")

with open(os.path.join(_REFERENCE, "types.json")) as f:
    _TYPES = json.load(f)
with open(os.path.join(_REFERENCE, "operators.json")) as f:
    _OPERATORS = json.load(f)

# NULL has no non-null value to compare against; VARIANT's example is an extraction.
_SKIPPED_TYPES = {"null", "variant"}

# A value valid for the operator, where the type's own example is not: `@?` needs a
# JSON document, and the IP operators compare an address with a network.
_JSON_DOC = "'{\"a\": 1}'"
_NETWORK = ("'192.168.0.0/16'", "VARCHAR")


def _cast_target(name, spec):
    """The type as written in CAST: the example's `::` suffix when it is parameterised
    (DECIMAL(10,2), ARRAY<INTEGER>), else the canonical name."""
    example = spec["metadata"]["example"]
    return example.split("::", 1)[1] if "::" in example else spec["canonical_name"]


def _value(name, spec, op):
    if op == "AtQuestion" and spec["family"] in ("text", "binary"):
        return f"CAST({_JSON_DOC} AS {_cast_target(name, spec)})"
    example = spec["metadata"]["example"]
    # parenthesised only where `::` needs it: a parenthesised literal is not a constant
    return f"({example})" if "::" in example else example


def _cases():
    for op, op_spec in sorted(_OPERATORS.items()):
        if op_spec["category"] != "comparison":
            continue
        symbol = op_spec["sql_symbol"]
        for name, spec in sorted(_TYPES.items()):
            if name in _SKIPPED_TYPES:
                continue
            a_value, a_type = _value(name, spec, op), _cast_target(name, spec)
            # {a} is the typed side under test, {b} its partner.
            if op in ("InList", "NotInList"):
                template = "{a} " + symbol + " ({b})"
                b_value, b_type = a_value, a_type
            elif op == "IPContainedBy":
                template = "{a} <<= {b}"
                b_value, b_type = _NETWORK
            elif op == "IPContains":
                template = "{b} >>= {a}"
                b_value, b_type = _NETWORK
            elif op == "AtQuestion":
                template = "{a} @? {b}"
                b_value, b_type = "'$.a'", "VARCHAR"
            else:
                template = "{a} " + symbol + " {b}"
                b_value, b_type = a_value, a_type
            yield pytest.param(
                template, a_value, a_type, b_value, b_type, id=f"{name}-{op}"
            )


_CASES = list(_cases())


def _column(sql):
    session = opteryx.session()
    return [v for m in session.execute_to_morsels(sql) for v in m.column(m.column_names[0]).to_pylist()]


def _runs(sql):
    try:
        return True, _column(sql)
    except Exception as err:  # the oracle: refused or not
        return False, err


@pytest.mark.parametrize("template,a_value,a_type,b_value,b_type", _CASES)
def test_typed_null_comparison(template, a_value, a_type, b_value, b_type):
    null_a, null_b = f"CAST(NULL AS {a_type})", f"CAST(NULL AS {b_type})"
    base_ok, base = _runs(f"SELECT {template.format(a=a_value, b=b_value)} AS x")

    column_source = (
        f"(SELECT id, CASE WHEN id > 4 THEN {a_value} END AS c FROM $planets) AS t"
    )
    expr_col = template.format(a="c", b=b_value)
    forms = {
        "null left": f"SELECT {template.format(a=null_a, b=b_value)} AS x",
        "null right": f"SELECT {template.format(a=a_value, b=null_b)} AS x",
        "null both": f"SELECT {template.format(a=null_a, b=null_b)} AS x",
        "column projection": f"SELECT {expr_col} AS x FROM {column_source} ORDER BY id",
        "column where": f"SELECT COUNT(*) AS x FROM {column_source} WHERE {expr_col}",
        "column where not": f"SELECT COUNT(*) AS x FROM {column_source} WHERE NOT ({expr_col})",
    }

    if not base_ok:
        answer = None
    else:
        (answer,) = base
    expected = {
        "null left": [None],
        "null right": [None],
        "null both": [None],
        "column projection": [None] * 4 + [answer] * 5,
        "column where": [5 if answer is True else 0],
        "column where not": [5 if answer is False else 0],
    }
    wrong = {}
    for label, sql in forms.items():
        ok, got = _runs(sql)
        if not ok and not base_ok:
            continue
        if not ok:
            wrong[label] = f"raised {type(got).__name__}: {str(got)[:160]}"
        elif got != expected[label]:
            wrong[label] = f"got {got}, expected {expected[label]}"
    assert not wrong, (wrong if base_ok else {"value form refused": str(base)[:160], **wrong})


# CASE cannot produce an ARRAY, so the matrix's column forms never reach an array
# column; JSONB_OBJECT_KEYS gives one. An untyped NULL is outside the matrix too.
_KEYS = (
    "(SELECT id, JSONB_OBJECT_KEYS(IIF(id > 4, '{\"a\": 1}', '{\"b\": 2}')) AS k "
    "FROM $planets) AS t"
)


@pytest.mark.parametrize(
    "sql,expected",
    [
        (f"SELECT k @> CAST(NULL AS ARRAY<VARCHAR>) AS x FROM {_KEYS}", [None] * 9),
        (f"SELECT k @>> CAST(NULL AS ARRAY<VARCHAR>) AS x FROM {_KEYS}", [None] * 9),
        (f"SELECT COUNT(*) AS x FROM {_KEYS} WHERE NOT (k @> CAST(NULL AS ARRAY<VARCHAR>))", [0]),
        ("SELECT ['a', 'b'] @> NULL AS x", [None]),
        ("SELECT '{\"a\": 1}' @? NULL AS x", [None]),
        ("SELECT CAST(167772161 AS IPV4) <<= NULL AS x", [None]),
        ("SELECT NULL >>= CAST(167772161 AS IPV4) AS x", [None]),
    ],
)
def test_null_constant_operand_answers_null(sql, expected):
    assert _column(sql) == expected


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
