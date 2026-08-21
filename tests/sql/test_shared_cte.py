"""A CTE referenced two or more times executes ONCE.

The Relation Resolver registers a multiply-referenced CTE's body as a shared
plan (`plan.shared_ctes`) and turns each reference into a MaterializedCteRef
leaf; the engine materializes the body's result into one buffer and every
reference reads that buffer (per-run cursor — each reference sees every
morsel). A single-reference CTE is still spliced inline, exactly as before.

The correctness laws pinned here:

- a self-join of two references is a join of two DISTINCT relations (each
  reference mints its own column identities) over ONE result — the TPC-DS
  Q47/Q57 shape, which the old per-reference expansion recomputed three times
  and (via a shared window-output identity) answered with zero rows;
- references inside expression subqueries share the same one body;
- a filter present above EVERY reference may move into the body (it is common
  work), but a filter above ONE reference must not leak into the others.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
        names = morsel.column_names
        columns = [morsel.column(n) for n in names]
        for i in range(morsel.num_rows):
            out.append(tuple(columns[j][i] for j in range(len(names))))
    return out


def test_self_join_of_shared_cte():
    result = rows(
        "WITH c AS (SELECT id, name FROM $planets) "
        "SELECT a.name FROM c AS a, c AS b WHERE a.id = b.id ORDER BY a.name"
    )
    assert len(result) == 9, result


def test_three_references_with_aggregate_body_match_inline_spelling():
    body = "SELECT id % 3 AS grp, COUNT(*) AS n, SUM(id) AS s FROM $planets GROUP BY id % 3"
    shared = rows(
        f"WITH v AS ({body}) "
        "SELECT a.grp, a.n, b.s, c.s FROM v AS a, v AS b, v AS c "
        "WHERE a.grp = b.grp AND a.grp = c.grp ORDER BY a.grp"
    )
    inline = rows(
        f"SELECT a.grp, a.n, b.s, c.s FROM ({body}) a, ({body}) b, ({body}) c "
        "WHERE a.grp = b.grp AND a.grp = c.grp ORDER BY a.grp"
    )
    assert shared == inline, (shared, inline)
    assert shared == [(0, 3, 18, 18), (1, 3, 12, 12), (2, 3, 15, 15)], shared


def test_ranking_window_self_join_offset_pairs():
    # the Q47/Q57 shape: rank in the body, offset self-join outside it —
    # each reference must expose DISTINCT column identities over ONE execution
    result = rows(
        "WITH v AS (SELECT id, name, RANK() OVER (ORDER BY id) AS rn FROM $planets) "
        "SELECT a.name, b.name FROM v a, v b WHERE a.rn = b.rn + 1 ORDER BY a.rn"
    )
    assert len(result) == 8, result
    assert result[0] == ("Venus", "Mercury"), result
    assert result[-1] == ("Pluto", "Neptune"), result


def test_nested_shared_ctes():
    result = rows(
        "WITH base AS (SELECT id, mass FROM $planets WHERE id > 1), "
        "     agg AS (SELECT COUNT(*) AS n FROM base) "
        "SELECT x.n + y.n + b.id FROM agg x, agg y, base b WHERE b.id = 2"
    )
    assert result == [(18,)], result


def test_reference_inside_expression_subquery():
    # `one` is referenced once (inside a scalar subquery) -> still inlined;
    # `two` is referenced twice -> shared. Both must answer correctly.
    result = rows(
        "WITH one AS (SELECT id FROM $planets WHERE id < 3), "
        "     two AS (SELECT id FROM $planets WHERE id >= 3) "
        "SELECT (SELECT COUNT(*) FROM one) AS a, t1.id "
        "FROM two t1, two t2 WHERE t1.id = t2.id ORDER BY t1.id LIMIT 2"
    )
    assert result == [(2, 3), (2, 4)], result


def test_common_predicate_above_every_reference():
    result = rows(
        "WITH v AS (SELECT id, name, mass FROM $planets) "
        "SELECT a.name FROM v a, v b WHERE a.id = b.id AND a.id > 4 AND b.id > 4 "
        "ORDER BY a.name"
    )
    assert [r[0] for r in result] == ["Jupiter", "Neptune", "Pluto", "Saturn", "Uranus"], result


def test_asymmetric_predicate_stays_with_its_reference():
    # a.id > 4 constrains only `a`; `b` must still see every row
    result = rows(
        "WITH v AS (SELECT id FROM $planets) "
        "SELECT a.id, b.id FROM v a, v b WHERE a.id > 4 AND b.id = a.id - 4 ORDER BY a.id"
    )
    assert result == [(5, 1), (6, 2), (7, 3), (8, 4), (9, 5)], result


def test_single_reference_cte_still_inlines():
    result = rows(
        "WITH c AS (SELECT id, name FROM $planets WHERE id > 3) "
        "SELECT name FROM c ORDER BY name LIMIT 3"
    )
    assert len(result) == 3, result


def test_explain_shows_cte_references():
    result = rows(
        "EXPLAIN WITH c AS (SELECT id FROM $planets) "
        "SELECT a.id FROM c a, c b WHERE a.id = b.id"
    )
    text = " ".join(str(row[0]) for row in result)
    assert "CTE Reference" in text, text


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
