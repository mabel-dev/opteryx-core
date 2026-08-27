"""
WITH RECURSIVE — SQL-level coverage of the fixpoint (docs/RECURSIVE_CTE_DESIGN.md).

The engine mechanics (span-jump loop, WORKING <- DELTA swap, RESULT accumulation,
iteration ceiling, buffer liveness across passes) are proven in
tests/unit/execution/test_engine_loop_span.py against a hand-built NativePlan.
This file proves the SQL rail on top: extract-time splitting, the resolver's
pending-marker exemption, two-phase binding, the compiler's LoopSpan emission —
and every v1 rejection, by exact message form, because a shape the fixpoint
cannot run must be refused by name, never computed wrongly.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import DataError
from opteryx.exceptions import UnsupportedSyntaxError


def results(sql):
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return out


# --- happy paths ---------------------------------------------------------------------


def test_counting_recursion():
    out = results(
        "WITH RECURSIVE r (n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5) "
        "SELECT n FROM r ORDER BY n"
    )
    assert out["n"] == [1, 2, 3, 4, 5], out


def test_graph_reachability_over_edge_list():
    # transitive closure from node 1 over a DAG, via an INNER join in the term
    out = results(
        """
        WITH RECURSIVE reach (node) AS (
          SELECT 1 AS node
          UNION ALL
          SELECT e.dst AS node
          FROM reach INNER JOIN (
              SELECT * FROM (VALUES (1,2),(2,3),(3,4),(2,5)) AS edges(src, dst)
          ) AS e ON e.src = reach.node
        )
        SELECT node FROM reach ORDER BY node
        """
    )
    assert out["node"] == [1, 2, 3, 4, 5], out


def test_string_accumulation():
    out = results(
        "WITH RECURSIVE p (name, depth) AS ("
        " SELECT 'root' AS name, 0 AS depth"
        " UNION ALL"
        " SELECT name || '.x' AS name, depth + 1 AS depth FROM p WHERE depth < 3)"
        "SELECT name FROM p ORDER BY depth"
    )
    assert out["name"] == ["root", "root.x", "root.x.x", "root.x.x.x"], out


def test_fibonacci_with_carried_state():
    # two mutually-updating columns carried across iterations — the classic
    # smoke test that the WORKING frontier is the PREVIOUS delta, not a stale
    # or accumulated one
    out = results(
        """
        WITH RECURSIVE fib AS (
            SELECT 1 AS pos, 0 AS val, 1 AS next_val
            UNION ALL
            SELECT pos + 1, next_val, val + next_val
            FROM fib
            WHERE pos < 10
        )
        SELECT pos, val AS fibonacci_number
        FROM fib
        ORDER BY pos
        """
    )
    assert out["pos"] == list(range(1, 11)), out
    assert out["fibonacci_number"] == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34], out


def test_empty_anchor_yields_empty_result():
    out = results(
        "WITH RECURSIVE r (n) AS ("
        " SELECT n FROM (VALUES (1)) AS v(n) WHERE n < 0"
        " UNION ALL SELECT n + 1 FROM r WHERE n < 3)"
        "SELECT n FROM r"
    )
    assert out.get("n", []) == [], out


def test_multiple_references_share_one_fixpoint():
    out = results(
        "WITH RECURSIVE r (n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
        "SELECT a.n AS an, b.n AS bn FROM r AS a CROSS JOIN r AS b ORDER BY an, bn"
    )
    assert len(out["an"]) == 9, out


def test_recursive_keyword_on_plain_cte_is_permission_not_obligation():
    out = results("WITH RECURSIVE r AS (SELECT 1 AS n) SELECT n FROM r")
    assert out["n"] == [1], out


def test_recursive_alongside_ordinary_cte():
    out = results(
        "WITH RECURSIVE base AS (SELECT 2 AS start), "
        "r (n) AS (SELECT start AS n FROM base UNION ALL SELECT n + 1 FROM r WHERE n < 4) "
        "SELECT n FROM r ORDER BY n"
    )
    assert out["n"] == [2, 3, 4], out


def test_reader_side_limit_and_filter():
    out = results(
        "WITH RECURSIVE r (n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 100) "
        "SELECT n FROM r WHERE n % 2 = 0 ORDER BY n LIMIT 3"
    )
    assert out["n"] == [2, 4, 6], out


# --- guards and rejections -----------------------------------------------------------


def test_union_terminates_on_a_cyclic_graph():
    # 1->2->3->1 is a cycle; UNION's visited set is what makes this converge
    out = results(
        """
        WITH RECURSIVE reach (node) AS (
          SELECT 1 AS node
          UNION
          SELECT e.dst AS node
          FROM reach INNER JOIN (
              SELECT * FROM (VALUES (1,2),(2,3),(3,1),(2,4)) AS edges(src, dst)
          ) AS e ON e.src = reach.node
        )
        SELECT node FROM reach ORDER BY node
        """
    )
    assert out["node"] == [1, 2, 3, 4], out


def test_union_deduplicates_the_anchor():
    out = results(
        "WITH RECURSIVE r (n) AS ("
        " SELECT n FROM (VALUES (1),(1),(2)) AS v(n)"
        " UNION SELECT n + 1 AS n FROM r WHERE n < 3)"
        "SELECT n FROM r ORDER BY n"
    )
    assert out["n"] == [1, 2, 3], out


def test_union_null_rows_dedup_as_equal():
    out = results(
        "WITH RECURSIVE r (n) AS ("
        " SELECT CAST(NULL AS INTEGER) AS n UNION SELECT n FROM r)"
        "SELECT n FROM r"
    )
    assert out["n"] == [None], out


def test_union_fixpoint_self_stabilises():
    # UNION over an identity term converges immediately (rows already seen)
    out = results(
        "WITH RECURSIVE r (n) AS (SELECT 1 AS n UNION SELECT n FROM r) SELECT n FROM r"
    )
    assert out["n"] == [1], out


# --- guards and rejections -----------------------------------------------------------


def test_non_converging_union_all_hits_the_ceiling():
    with pytest.raises(DataError, match="did not converge within"):
        results("WITH RECURSIVE r (n) AS (SELECT 1 AS n UNION ALL SELECT n FROM r) SELECT n FROM r")


def test_cyclic_graph_under_union_all_hits_the_ceiling():
    with pytest.raises(DataError, match="did not converge within"):
        results(
            """WITH RECURSIVE reach (node) AS (
              SELECT 1 AS node
              UNION ALL
              SELECT e.dst AS node FROM reach INNER JOIN (
                  SELECT * FROM (VALUES (1,2),(2,1)) AS edges(src, dst)
              ) AS e ON e.src = reach.node)
            SELECT node FROM reach"""
        )


def test_self_reference_in_anchor_rejected():
    with pytest.raises(UnsupportedSyntaxError, match="references itself in the anchor"):
        results(
            "WITH RECURSIVE r (n) AS (SELECT n FROM r UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
            "SELECT n FROM r"
        )


def test_self_reference_without_union_rejected():
    with pytest.raises(UnsupportedSyntaxError, match="UNION ALL"):
        results("WITH RECURSIVE r (n) AS (SELECT n FROM r) SELECT n FROM r")


def test_aggregate_over_self_reference_rejected():
    with pytest.raises(UnsupportedSyntaxError, match="aggregation over its own reference"):
        results(
            "WITH RECURSIVE r (n) AS "
            "(SELECT 1 AS n UNION ALL SELECT MAX(n) + 1 AS n FROM r WHERE n < 3) "
            "SELECT n FROM r"
        )


def test_outer_join_over_self_reference_rejected():
    with pytest.raises(UnsupportedSyntaxError, match="only INNER"):
        results(
            "WITH RECURSIVE r (n) AS (SELECT 1 AS n UNION ALL "
            "SELECT r.n + 1 AS n FROM r LEFT JOIN (SELECT 1 AS m) AS o ON o.m = r.n "
            "WHERE r.n < 3) SELECT n FROM r"
        )


def test_order_or_limit_on_recursive_body_rejected():
    with pytest.raises(UnsupportedSyntaxError, match="ORDER BY / LIMIT"):
        results(
            "WITH RECURSIVE r (n) AS "
            "(SELECT 1 AS n UNION ALL SELECT n + 1 AS n FROM r WHERE n < 3 LIMIT 2) "
            "SELECT n FROM r"
        )


def test_anchor_term_type_mismatch_names_the_column():
    with pytest.raises(UnsupportedSyntaxError, match="add an explicit CAST"):
        results(
            "WITH RECURSIVE r (n) AS "
            "(SELECT 1 AS n UNION ALL SELECT n + 0.5 AS n FROM r WHERE n < 3) "
            "SELECT n FROM r"
        )


def test_anchor_duplicate_identity_rejected_with_the_rewrite():
    # `SELECT n, n AS p` shares one column identity across two anchor
    # positions; a recursive CTE's positions diverge over iterations, so a
    # shared identity silently collapsed `p` onto `n` (wrong answers, not an
    # error). Must refuse and name the CAST rewrite that makes the copy
    # independent.
    with pytest.raises(UnsupportedSyntaxError, match="same underlying column"):
        results(
            "WITH RECURSIVE t (n, p) AS ("
            " SELECT v AS n, v AS p FROM (VALUES (1)) AS s(v)"
            " UNION ALL SELECT n + 1, p + 10 FROM t WHERE n < 3)"
            "SELECT n, p FROM t"
        )


def test_anchor_cast_copy_evolves_independently():
    # the sanctioned spelling of the same intent: CAST mints an independent
    # column, and the two positions genuinely diverge
    out = results(
        "WITH RECURSIVE t (n, p) AS ("
        " SELECT v AS n, CAST(v AS INTEGER) AS p FROM (VALUES (1)) AS s(v)"
        " UNION ALL SELECT n + 1, p + 10 FROM t WHERE n < 3)"
        "SELECT n, p FROM t ORDER BY n"
    )
    assert out["n"] == [1, 2, 3], out
    assert out["p"] == [1, 11, 21], out


def test_anchor_term_column_count_mismatch_rejected():
    with pytest.raises(UnsupportedSyntaxError):
        results(
            "WITH RECURSIVE r (n) AS "
            "(SELECT 1 AS n UNION ALL SELECT n + 1 AS n, n AS m FROM r WHERE n < 3) "
            "SELECT n FROM r"
        )


# --- EXPLAIN and telemetry -----------------------------------------------------------


def _explain_lines(sql):
    out = results(sql)
    return [
        (t.decode("utf-8") if isinstance(t, bytes) else t, d)
        for t, d in zip(out["tree"], out["details"])
    ]


def test_explain_renders_the_recursive_structure():
    lines = _explain_lines(
        "EXPLAIN WITH RECURSIVE r (n) AS "
        "(SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r"
    )
    trees = [t for t, _ in lines]
    assert any(t == "RECURSIVE CTE r" for t in trees), trees
    assert any("ANCHOR" in t for t in trees), trees
    assert any("RECURSIVE TERM" in t for t in trees), trees
    header_detail = next(d for t, d in lines if t == "RECURSIVE CTE r")
    assert header_detail == "UNION ALL", header_detail


def test_explain_analyze_reports_iterations_and_visited_rows():
    lines = _explain_lines(
        "EXPLAIN ANALYZE WITH RECURSIVE r (n) AS "
        "(SELECT 1 AS n UNION SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r"
    )
    header_detail = next(d for t, d in lines if t == "RECURSIVE CTE r")
    assert "iterations" in header_detail, header_detail
    assert "distinct rows" in header_detail, header_detail
    assert "ceiling" in header_detail, header_detail


def test_explain_analyze_on_a_shared_cte_runs():
    # regression: EXPLAIN ANALYZE's inner run copies the plan, and Graph.copy()
    # drops instance attributes — shared_ctes/recursive_ctes must be re-carried
    # or the compiler refuses every CTE reference
    lines = _explain_lines(
        "EXPLAIN ANALYZE WITH c AS (SELECT 1 AS n) "
        "SELECT a.n FROM c AS a CROSS JOIN c AS b"
    )
    assert any("Cross Join" in t for t, _ in lines), lines


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
