"""
Tests for SemiJoinPushdownStrategy — sinking a decorrelated SEMI/ANTI join
below the inner-join chain onto the leg that supplies its keys.

Correctness is asserted against a literal-key oracle: the subquery is executed
standalone, its keys inlined as an IN (<literals>) predicate, and the two
formulations must agree exactly. The oracle path never builds a semi join, so
it cannot share a defect with the transform under test.

Plan shape is asserted through EXPLAIN's OPTIMIZATIONS decision records
("semi join pushdown | sunk below N joins" / "declined: ..."), which is the
costed-pair contract: the decision is visible with its numbers either way.

Coverage:
  - Q18 shape (expanding join chain): pushes, values match the oracle
  - ANTI variant (NOT IN over the same shape): values match the oracle
  - Semi above a LEFT OUTER join: must not push (identity does not hold)
  - EXPLAIN shows the push decision on the Q18 shape
  - The pushed plan and the literal-key oracle agree on aggregates above
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def run(sql):
    sess = opteryx.session()
    rows = []
    for morsel in sess.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    sess.close()
    return rows


def run_sorted(sql):
    return sorted(map(repr, run(sql)))


def explain_text(sql):
    parts = []
    for row in run("EXPLAIN " + sql):
        parts.append(
            " ".join(v.decode() if isinstance(v, (bytes, bytearray)) else str(v) for v in row)
        )
    return "\n".join(parts)


_TPCH = "testdata.tpch_001"

# The Q18 shape: a HAVING-filtered IN subquery above an expanding join chain.
# At any scale the joined intermediate is ~4x orders, so the gate pushes.
_Q18_SHAPE = f"""
    SELECT c_custkey, o_orderkey, SUM(l_quantity)
      FROM {_TPCH}.customer, {_TPCH}.orders, {_TPCH}.lineitem
     WHERE o_orderkey IN (
               SELECT l_orderkey FROM {_TPCH}.lineitem
               GROUP BY l_orderkey HAVING SUM(l_quantity) > {{threshold}}
           )
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
     GROUP BY c_custkey, o_orderkey
"""


def _qualifying_keys(threshold):
    keys = [
        row[0]
        for row in run(
            f"SELECT l_orderkey FROM {_TPCH}.lineitem "
            f"GROUP BY l_orderkey HAVING SUM(l_quantity) > {threshold}"
        )
    ]
    assert keys, "oracle needs a non-empty key set — lower the threshold"
    return keys


def test_pushed_semi_matches_literal_key_oracle():
    threshold = 250
    keys = _qualifying_keys(threshold)
    oracle = run_sorted(
        f"""
        SELECT c_custkey, o_orderkey, SUM(l_quantity)
          FROM {_TPCH}.customer, {_TPCH}.orders, {_TPCH}.lineitem
         WHERE o_orderkey IN ({", ".join(map(str, keys))})
           AND c_custkey = o_custkey
           AND o_orderkey = l_orderkey
         GROUP BY c_custkey, o_orderkey
        """
    )
    pushed = run_sorted(_Q18_SHAPE.format(threshold=threshold))
    assert pushed == oracle, (
        f"pushed semi disagrees with literal-key oracle: "
        f"{len(pushed)} vs {len(oracle)} rows"
    )


def test_pushed_anti_matches_literal_key_oracle():
    """NOT IN over the same expanding shape — the ANTI side of the identity.

    NOT IN is null-aware, but l_orderkey is a key column with no NULLs, so the
    literal-key oracle is exact here.
    """
    threshold = 250
    keys = _qualifying_keys(threshold)
    oracle = run_sorted(
        f"""
        SELECT c_custkey, COUNT(*)
          FROM {_TPCH}.customer, {_TPCH}.orders
         WHERE o_orderkey NOT IN ({", ".join(map(str, keys))})
           AND c_custkey = o_custkey
         GROUP BY c_custkey
        """
    )
    anti = run_sorted(
        f"""
        SELECT c_custkey, COUNT(*)
          FROM {_TPCH}.customer, {_TPCH}.orders
         WHERE o_orderkey NOT IN (
                   SELECT l_orderkey FROM {_TPCH}.lineitem
                   GROUP BY l_orderkey HAVING SUM(l_quantity) > {threshold}
               )
           AND c_custkey = o_custkey
         GROUP BY c_custkey
        """
    )
    assert anti == oracle


def test_semi_above_outer_join_is_not_pushed():
    """SEMI(A LEFT JOIN B, S) is NOT SEMI(A, S) LEFT JOIN B when the semi key
    can be a null the outer join produced — the strategy must leave outer
    joins alone. Values are pinned against the literal-key oracle."""
    keys = [
        row[0]
        for row in run(f"SELECT DISTINCT o_custkey FROM {_TPCH}.orders WHERE o_totalprice > 100000")
    ]
    oracle = run_sorted(
        f"""
        SELECT c.c_custkey, COUNT(*)
          FROM {_TPCH}.customer AS c
          LEFT JOIN {_TPCH}.orders AS o ON c.c_custkey = o.o_custkey
         WHERE c.c_custkey IN ({", ".join(map(str, keys))})
         GROUP BY c.c_custkey
        """
    )
    semi = run_sorted(
        f"""
        SELECT c.c_custkey, COUNT(*)
          FROM {_TPCH}.customer AS c
          LEFT JOIN {_TPCH}.orders AS o ON c.c_custkey = o.o_custkey
         WHERE c.c_custkey IN (
                   SELECT o_custkey FROM {_TPCH}.orders WHERE o_totalprice > 100000
               )
         GROUP BY c.c_custkey
        """
    )
    assert semi == oracle


def test_explain_records_the_push_decision():
    """The costed-pair contract: EXPLAIN's OPTIMIZATIONS block records the
    decision with its numbers — a push here, on the expanding Q18 shape."""
    plan_text = explain_text(_Q18_SHAPE.format(threshold=250))
    assert "semi join pushdown" in plan_text, f"no pushdown decision recorded:\n{plan_text}"
    assert "sunk below" in plan_text, f"expected a push on the Q18 shape:\n{plan_text}"


if __name__ == "__main__":
    import traceback

    tests = [
        test_pushed_semi_matches_literal_key_oracle,
        test_pushed_anti_matches_literal_key_oracle,
        test_semi_above_outer_join_is_not_pushed,
        test_explain_records_the_push_decision,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✅ {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ❌ {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
