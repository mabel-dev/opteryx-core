"""A window function is computed OVER the grouped rows, not under them.

    SELECT i_class, SUM(x) AS revenue,
           SUM(x) * 100 / SUM(SUM(x)) OVER (PARTITION BY i_class) AS ratio
    FROM ... GROUP BY i_item_id, i_class

was refused outright — "Window functions cannot be combined with **GROUP BY**" —
along with the same arrangement beside a bare aggregate, in QUALIFY, and in ORDER
BY. Ten of the 99 TPC-DS queries are written this way (Q12, Q20, Q47, Q51, Q53,
Q57, Q63, Q70, Q89, Q98), the largest single failure bucket in that suite.

The refusal was NOT arbitrary. The Window step was planned UNDER the aggregate
step, so the window would have been computed over the rows the aggregate
collapses and could never see the aggregated result — and a wrong number is worse
than a refusal. What was missing was the plan shape, which is the one the refusal
used to advise the caller to write by hand:

    <source> -> Aggregate[AndGroup] -> Project -> Subquery -> Window(s) -> ...

The Subquery boundary is load-bearing twice over. `window_to_join` copies the
sub-plan below the Window node as the window's input and requires it to expose
exactly ONE relation name, which an aggregate over a join does not; and past the
boundary only the group keys and the aggregates have names, which is what makes
the standard's rule enforceable — a column read there that was neither grouped by
nor aggregated is refused, in the same words DuckDB and PostgreSQL use.

THE NESTED AGGREGATE IS THE CRUX, not an oddity. In `SUM(SUM(x)) OVER (...)` the
inner SUM is the GROUP BY aggregate and the outer SUM is the window over those
group results. The inner one reached `_aggregates` from nowhere — the hoist had
already spliced the window out of the projection before the collection walk ran —
so it had to be gathered from the window's own arguments, and from a window's
PARTITION BY and ORDER BY too.

Every expected value in this file was taken from DuckDB on the same rows.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.exceptions import UnsupportedSyntaxError


def rows(statement):
    """Every row of `statement`, as tuples in column order."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(statement):
        morsel.materialize()
        names = list(morsel.column_names)
        out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
    return out


def names(statement):
    """The output column names, as strings."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels(statement):
        return [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
    return []


def rounded(statement, places=4):
    """Rows sorted and rounded — the grouped result reaches the windows through a join,
    which promises no order, so a statement without an explicit ORDER BY has none."""
    return sorted(
        tuple(round(value, places) if isinstance(value, float) else value for value in row)
        for row in rows(statement)
    )


def test_ratio_to_partition_total():
    """FLAVOUR (a): each group's share of its partition's total. TPC-DS Q12/Q20/Q98.

    Three buckets of three planets; `SUM(SUM(mass)) OVER (PARTITION BY id % 3)` is the
    bucket total against which each planet's mass is measured.
    """
    assert rounded(
        "SELECT id % 3 AS bucket, name, SUM(mass) AS m, "
        "SUM(SUM(mass)) OVER (PARTITION BY id % 3) AS bucket_total, "
        "SUM(mass) * 100.0 / SUM(SUM(mass)) OVER (PARTITION BY id % 3) AS pct "
        "FROM $planets GROUP BY id % 3, name"
    ) == [
        (0, "Earth", 5.97, 573.9846, 1.0401),
        (0, "Pluto", 0.0146, 573.9846, 0.0025),
        (0, "Saturn", 568.0, 573.9846, 98.9574),
        (1, "Mars", 0.642, 87.7720, 0.7314),
        (1, "Mercury", 0.33, 87.7720, 0.376),
        (1, "Uranus", 86.8, 87.7720, 98.8926),
        (2, "Jupiter", 1898.0, 2004.87, 94.6695),
        (2, "Neptune", 102.0, 2004.87, 5.0876),
        (2, "Venus", 4.87, 2004.87, 0.2429),
    ]


def test_average_over_the_group_results():
    """The Q47/Q53/Q57/Q63/Q89 spelling: `AVG(SUM(x)) OVER (...)`.

    With no PARTITION BY there is one partition, so every row carries the mean of the
    three bucket totals — 888.8755, not the mean of the nine underlying masses (98.76).
    Getting the second number would mean the window had been computed under the
    aggregate, which is exactly the failure the old refusal existed to prevent.
    """
    assert rounded(
        "SELECT id % 3 AS bucket, SUM(mass) AS m, AVG(SUM(mass)) OVER () AS avg_group "
        "FROM $planets GROUP BY id % 3"
    ) == [(0, 573.9846, 888.8755), (1, 87.772, 888.8755), (2, 2004.87, 888.8755)]


def test_ranking_window_over_the_group_results():
    """A RANKING window over the grouped rows, ordered by an aggregate.

    The other half of TPC-DS Q47/Q57. `ORDER BY COUNT(*)` inside the OVER clause names a
    value that only exists after the grouping, so the ranking Window node has to be
    planned above the aggregate exactly as the aggregate windows are.
    """
    assert rows(
        "SELECT number_of_moons, COUNT(*) AS c, "
        "RANK() OVER (ORDER BY COUNT(*) DESC, number_of_moons) AS r "
        "FROM $planets GROUP BY number_of_moons ORDER BY r"
    ) == [
        (0, 2, 1),
        (1, 1, 2),
        (2, 1, 3),
        (5, 1, 4),
        (14, 1, 5),
        (27, 1, 6),
        (79, 1, 7),
        (82, 1, 8),
    ]


def test_having_is_applied_before_the_window():
    """HAVING filters GROUPS, and the standard evaluates it BEFORE window functions.

    Planned in its usual place — a Filter above the Project, which is above the windows —
    it filtered nothing the window had not already counted: this answered 45, the total
    over all nine groups, where DuckDB and the standard say 35. The lowering plans it
    inside the boundary instead, below the windows.
    """
    assert rows(
        "SELECT number_of_moons, SUM(id) AS s, SUM(SUM(id)) OVER () AS t "
        "FROM $planets GROUP BY number_of_moons HAVING SUM(id) > 4 ORDER BY number_of_moons"
    ) == [(5, 9, 35), (14, 8, 35), (27, 7, 35), (79, 5, 35), (82, 6, 35)]


def test_having_may_name_a_select_alias():
    """`SUM(id) AS s ... HAVING s > 4` means the same as `HAVING SUM(id) > 4`.

    HAVING is planned BELOW the Project that creates the alias, so the alias has to be
    resolved to the grouped column it stands for rather than left to bind against a name
    that does not exist yet.
    """
    assert rows(
        "SELECT number_of_moons, SUM(id) AS s, SUM(SUM(id)) OVER () AS t "
        "FROM $planets GROUP BY number_of_moons HAVING s > 4 ORDER BY number_of_moons"
    ) == [(5, 9, 35), (14, 8, 35), (27, 7, 35), (79, 5, 35), (82, 6, 35)]


def test_qualify_filters_the_grouped_rows():
    """QUALIFY filters on a window's value, and here that window is over the groups."""
    assert rows(
        "SELECT number_of_moons, SUM(id) AS s FROM $planets GROUP BY number_of_moons "
        "QUALIFY RANK() OVER (ORDER BY SUM(id) DESC) <= 3 ORDER BY s DESC"
    ) == [(5, 9), (14, 8), (27, 7)]


def test_window_in_order_by_over_the_grouped_rows():
    """A window written only in ORDER BY is hoisted the same way and ordered on."""
    assert rows(
        "SELECT number_of_moons, SUM(id) AS s FROM $planets GROUP BY number_of_moons "
        "ORDER BY RANK() OVER (ORDER BY SUM(id) DESC) LIMIT 3"
    ) == [(5, 9), (14, 8), (27, 7)]


def test_window_over_a_grouped_join():
    """The window's source is a multi-table join.

    Every other window arrangement refuses that (`_find_base_scan`, and
    `window_to_join._source_relation` behind it) because the rewrite rebuilds the outer
    leg of its join as a qualified wildcard and needs one relation to qualify it with.
    The grouped result IS one relation however many tables the grouping read — and TPC-DS
    needs this: all ten of those queries join.
    """
    assert rows(
        "SELECT p.name, COUNT(*) AS c, SUM(COUNT(*)) OVER (PARTITION BY p.name) AS t "
        "FROM $planets AS p INNER JOIN testdata.satellites AS s ON s.planetId = p.id "
        "GROUP BY p.name HAVING COUNT(*) > 20 ORDER BY p.name"
    ) == [("Jupiter", 67, 67), ("Saturn", 61, 61), ("Uranus", 27, 27)]


def test_two_partition_specs_over_one_grouping():
    """Two windows with different partitions are two CTEs joined onto one grouped result,
    not two copies of the grouping."""
    assert rows(
        "SELECT number_of_moons AS m, SUM(id) AS s, "
        "SUM(SUM(id)) OVER (PARTITION BY number_of_moons) AS by_moons, "
        "SUM(SUM(id)) OVER () AS overall "
        "FROM $planets GROUP BY number_of_moons ORDER BY m"
    ) == [
        (0, 3, 3, 45),
        (1, 3, 3, 45),
        (2, 4, 4, 45),
        (5, 9, 9, 45),
        (14, 8, 8, 45),
        (27, 7, 7, 45),
        (79, 5, 5, 45),
        (82, 6, 6, 45),
    ]


def test_output_names_survive_the_boundary():
    """The grouped rows become a relation, and a relation renames things.

    `visit_project` records an `AS` as an extra alias on the schema column rather than
    renaming it, and the boundary publishes `schema_column.name` and drops the aliases —
    so the boundary is crossed under the EXPRESSION's name and the caller's alias is
    re-applied above it. Named `s` here, not `SUM(id)`; and an unaliased aggregate still
    answers to its rendering.
    """
    assert names(
        "SELECT number_of_moons AS m, SUM(id) AS s, SUM(SUM(id)) OVER () AS t "
        "FROM $planets GROUP BY number_of_moons"
    ) == ["m", "s", "t"]
    assert names(
        "SELECT number_of_moons, SUM(id), SUM(SUM(id)) OVER (PARTITION BY number_of_moons) "
        "FROM $planets GROUP BY number_of_moons"
    ) == [
        "number_of_moons",
        "SUM(id)",
        "SUM(SUM(id)) OVER (PARTITION BY number_of_moons)",
    ]


def test_one_aggregate_however_many_spellings():
    """`SUM(mass) AS m` and the bare `SUM(mass)` inside the window are ONE grouped column.

    Deduped on the rendering, with the aliased spelling winning — the relation exposes it
    once and every reference reads that column.
    """
    assert rounded(
        "SELECT id % 3 AS bucket, SUM(mass) AS m, SUM(SUM(mass)) OVER () AS t "
        "FROM $planets GROUP BY id % 3"
    ) == [(0, 573.9846, 2666.6266), (1, 87.772, 2666.6266), (2, 2004.87, 2666.6266)]


@pytest.mark.parametrize(
    "statement, column",
    [
        # The window's ARGUMENT is where this most often hides: `mass` is read raw at a
        # level where only the group keys and the aggregates exist.
        ("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets", "mass"),
        ("SELECT id, SUM(gravity) OVER (PARTITION BY id) FROM $planets GROUP BY id", "gravity"),
        # A window's PARTITION BY and ORDER BY are read at that level too.
        ("SELECT MAX(id) OVER (PARTITION BY gravity), COUNT(*) FROM $planets", "id"),
        ("SELECT COUNT(*), ROW_NUMBER() OVER (ORDER BY id) FROM $planets", "id"),
        # And so is the SELECT list beside the window.
        (
            "SELECT name, SUM(SUM(id)) OVER () FROM $planets GROUP BY number_of_moons",
            "name",
        ),
    ],
)
def test_ungrouped_column_is_refused_by_name(statement, column):
    """The standard's rule still governs, and names the column the caller wrote.

    DuckDB and PostgreSQL refuse every one of these in the same terms. The minted
    `$win_<random>` join key must never appear: it is a column nobody typed and it is
    different on every execution.
    """
    with pytest.raises(SqlError) as raised:
        rows(statement)
    message = str(raised.value)
    assert f"Column '{column}' must appear in the `GROUP BY` clause" in message, message
    assert "$win_" not in message, message


@pytest.mark.parametrize(
    "statement, named",
    [
        # (1) THE REPORTED SHAPE — GROUP BY an output ALIAS of a window.
        (
            "SELECT NTILE(4) OVER (ORDER BY gravity) AS decile, COUNT(*) "
            "FROM $planets GROUP BY decile",
            "`decile`",
        ),
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, COUNT(*) "
            "FROM $planets GROUP BY rn",
            "`rn`",
        ),
        # An EXPRESSION over a window's output is no more groupable than the output.
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, COUNT(*) "
            "FROM $planets GROUP BY rn + 1",
            "`rn`",
        ),
        # (2) GROUP BY a POSITION landing on a window.
        (
            "SELECT NTILE(4) OVER (ORDER BY gravity), COUNT(*) FROM $planets GROUP BY 1",
            "position 1",
        ),
        # An AGGREGATE window is still an AGGREGATOR node at this point, not a
        # hoisted reference — the arm that refuses "an aggregate in the SELECT list"
        # sits right beside this one and would name the wrong rule for it.
        (
            "SELECT SUM(id) OVER (PARTITION BY name) AS w, COUNT(*) "
            "FROM $planets GROUP BY 1",
            "position 1",
        ),
        # (3) A window written DIRECTLY in GROUP BY. Reached the compiler as
        # "a GROUP BY key the engine could not resolve here", which names nothing.
        (
            "SELECT COUNT(*) FROM $planets GROUP BY NTILE(4) OVER (ORDER BY gravity)",
            "`NTILE(4) OVER (ORDER BY gravity)`",
        ),
        (
            "SELECT COUNT(*) FROM $planets GROUP BY SUM(id) OVER (PARTITION BY name)",
            "`SUM(id) OVER (PARTITION BY name)`",
        ),
    ],
)
def test_window_as_a_group_by_key_is_refused_by_name(statement, named):
    """A window cannot BE a group key, and the refusal must say so.

    This is the counterpart of `test_ungrouped_column_is_refused_by_name` above, and
    the two were being answered by the SAME message — which is right for exactly one
    of them. `GROUP BY <window>` fell through to the window-over-grouped-result
    lowering, which rebased the window's own ORDER BY over the grouped rows, found
    that column was not a group key, and reported the OTHER rule:

        SELECT NTILE(4) OVER (ORDER BY gravity) AS decile, COUNT(*)
        FROM $planets GROUP BY decile
        -> Column 'gravity' must appear in the `GROUP BY` clause ...

    Everything about that is a dead end. `gravity` is not what the caller got wrong;
    adding it to the GROUP BY does not help; and the suggested `MIN(gravity)` produces
    a different query. The reader is sent to fix the half that was fine.

    DuckDB refuses all of these too ("GROUP BY clause cannot contain window
    functions"), so the rule is not an Opteryx narrowing — only the message was.
    """
    with pytest.raises(UnsupportedSyntaxError) as raised:
        rows(statement)
    message = str(raised.value)

    assert named in message, message
    assert "window function" in message.lower(), message
    # The remedy has to be the one that works. The subquery rewrite is the ONLY way
    # to group on a window's output, and the test below proves it runs.
    assert "subquery" in message, message
    # The old message's advice, which does not apply here — naming a column the caller
    # cannot act on is the whole defect.
    assert "must appear in the `GROUP BY` clause" not in message, message
    # A minted `$win_<random>` join key is a column nobody typed and differs per run.
    assert "$win_" not in message, message


def test_the_subquery_rewrite_the_refusal_recommends_actually_runs():
    """The remedy named in the message, executed.

    A refusal that recommends a rewrite is only honest if the rewrite works — so the
    query the caller is sent to write is pinned here, not just described.
    """
    assert rows(
        "SELECT decile, COUNT(*) AS n FROM ("
        "  SELECT NTILE(4) OVER (ORDER BY gravity) AS decile FROM $planets"
        ") AS d GROUP BY decile ORDER BY decile"
    ) == [(1, 3), (2, 2), (3, 2), (4, 2)]


def test_group_by_all_still_ignores_window_outputs():
    """`GROUP BY ALL` already excluded window outputs, and must keep doing so.

    The new refusal shares its test — and its `_window_output_aliases` set — with
    `_group_by_all_keys`, deliberately, so the two cannot decide differently about
    what a window output is. If that sharing ever turned ALL's silent exclusion into
    an error, this is what would catch it.
    """
    assert rows(
        "SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn FROM $planets "
        "GROUP BY ALL ORDER BY name LIMIT 2"
    ) == [("Earth", 1), ("Jupiter", 2)]


def test_cumulative_window_over_groups():
    """FLAVOUR (b) — TPC-DS Q51 — a window FRAME (native_window_frame.hpp's
    FramedWindowSink) combined with this file's GROUP BY boundary.

        SELECT ws_item_sk, d_date,
               SUM(SUM(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date
                                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        FROM ... GROUP BY ws_item_sk, d_date

    PARTITION BY and ORDER BY are the same key below (one row per partition), so the
    running total degenerates to that partition's own SUM(id) — still exercises the
    framed-window-over-grouped-rows plan shape (Aggregate -> Project -> Subquery ->
    FramedWindow), not just PARTITION BY passthrough. A genuine multi-row running total
    (PARTITION BY separate from ORDER BY) is covered without GROUP BY in
    tests/sql/test_decimal128_compare.py and the shape battery.
    """
    assert rows(
        "SELECT number_of_moons, SUM(SUM(id)) OVER ("
        "PARTITION BY number_of_moons ORDER BY number_of_moons "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c "
        "FROM $planets GROUP BY number_of_moons ORDER BY number_of_moons"
    ) == [(0, 3), (1, 3), (2, 4), (5, 9), (14, 8), (27, 7), (79, 5), (82, 6)]

    # A GENUINE running total: id is unique per row, so PARTITION BY id % 3 groups three
    # planets per bucket and ORDER BY id orders the running sum within it. Values are
    # SUM(mass) partial sums in id order — arithmetic on the same masses
    # test_ratio_to_partition_total above uses, not independently sourced.
    assert (
        sorted(
            (b, n, round(m, 4), round(r, 4))
            for b, n, m, r in rows(
                "SELECT id % 3 AS bucket, name, SUM(mass) AS m, "
                "SUM(SUM(mass)) OVER (PARTITION BY id % 3 ORDER BY id "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running "
                "FROM $planets GROUP BY id % 3, id, name"
            )
        )
        == [
            (0, "Earth", 5.97, 5.97),
            (0, "Pluto", 0.0146, 573.9846),
            (0, "Saturn", 568.0, 573.97),
            (1, "Mars", 0.642, 0.972),
            (1, "Mercury", 0.33, 0.33),
            (1, "Uranus", 86.8, 87.772),
            (2, "Jupiter", 1898.0, 1902.87),
            (2, "Neptune", 102.0, 2004.87),
            (2, "Venus", 4.87, 4.87),
        ]
    )


def test_a_window_in_having_is_still_refused():
    """HAVING filters groups and windows are computed after that filter, so a window in
    HAVING asks for a value that does not exist yet. Unchanged by this work, and asserted
    here because the lowering moved HAVING's Filter next to the windows."""
    with pytest.raises(UnsupportedSyntaxError) as raised:
        rows("SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () > 100")
    assert "cannot appear in **HAVING**" in str(raised.value), raised.value


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
