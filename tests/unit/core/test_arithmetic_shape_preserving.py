"""
Shape-aware int64 arithmetic (WP-11) — SQL-level correctness.

Constant-operand folding + dict/constant-preserving scalar/unary ops must
produce results identical to the uniform per-row path. The kernel-level
differential parity harness lives at dev/parity/int64_arith_shape_parity.cpp;
these pin the behaviour through the live evaluator (column op literal, unary
negation, chained arithmetic, nulls).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _rows(sql):
    s = opteryx.session()
    out = []
    for m in s.execute_to_morsels(sql):
        if m.num_rows:
            names = [n.decode() if isinstance(n, bytes) else n for n in m.column_names]
            cols = {n: m.column(n.encode()).to_pylist() for n in names}
            for i in range(m.num_rows):
                out.append({n: cols[n][i] for n in names})
    return out


def test_column_plus_minus_times_literal():
    rows = _rows("SELECT id, id+100 AS a, id-5 AS b, id*3 AS c FROM $planets")
    for r in rows:
        assert r["a"] == r["id"] + 100
        assert r["b"] == r["id"] - 5
        assert r["c"] == r["id"] * 3


def test_literal_minus_column_non_commutative():
    rows = _rows("SELECT id, 1000-id AS d FROM $planets")
    for r in rows:
        assert r["d"] == 1000 - r["id"]


def test_integer_div_mod_literal():
    rows = _rows("SELECT id, id DIV 2 AS q, id % 3 AS m FROM $planets")
    for r in rows:
        assert r["q"] == r["id"] // 2
        assert r["m"] == r["id"] % 3


def test_negation_via_zero_minus():
    # `SELECT -id` trips a pre-existing planner AST quirk; 0 - id exercises the
    # constant-LHS subtraction fold (kernel i64_neg itself is covered by the
    # dev/parity/int64_arith_shape_parity.cpp harness).
    rows = _rows("SELECT id, 0-id AS n FROM $planets")
    for r in rows:
        assert r["n"] == -r["id"]


def test_chained_arithmetic_preserves_correctness():
    rows = _rows("SELECT id, (id+1)*2-3 AS e FROM $planets")
    for r in rows:
        assert r["e"] == (r["id"] + 1) * 2 - 3


def test_arithmetic_with_nulls():
    # gravity has nulls in some datasets; use a CASE to inject a null column.
    rows = _rows(
        "SELECT id, (CASE WHEN id > 4 THEN id ELSE NULL END) + 10 AS v FROM $planets"
    )
    for r in rows:
        if r["id"] > 4:
            assert r["v"] == r["id"] + 10
        else:
            assert r["v"] is None


if __name__ == "__main__":  # pragma: no cover
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✓ {name}")
    print("✅ okay")
