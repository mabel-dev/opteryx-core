"""An outer WHERE must be able to read a column across a LIMITed CTE boundary.

    WITH c AS (SELECT k1, k2, COUNT(x) AS a FROM t WHERE ... GROUP BY k1, k2
               HAVING COUNT(*) <= n LIMIT m)
    SELECT a AS e FROM c WHERE k2 BETWEEN lo AND hi

raised `NotSupportedError: projecting a column the engine could not resolve here`.
The outer SELECT projects `a` and nothing else, so `k2` reaches the outer Filter as
a PASSTHROUGH column of the Project below it; and a predicate cannot push below a
LIMIT without changing which rows it sees, so that Filter is stranded above the
CTE's Limit and has to read `k2` across the boundary rather than being folded into
the CTE's own WHERE.

Both halves of `ProjectionPushdownStrategy.collect_columns`
(planner/optimizer/strategies/projection_pushdown.py) are load-bearing here, and
each was checked by reverting it on its own — either alone brings the identical
refusal back:

  * a Project emits `columns ∪ passthrough_columns`, so collecting from
    `node.columns` alone under-counts what the node above will demand;
  * a COMPUTED column's OWN identity is live as well as its inputs, because
    whether the compiler recomputes it or reads the already-materialised column
    off the stream is decided later.

The strategy prunes on `pre_update_columns`, so an under-count there tells a node
below that a live column is dead. That is the whole class this message names.

VALUES, NOT ROW COUNTS. The shape's failure mode after the pruning is a LOST
column, and a filter that lost its column returns MORE rows, not an error — so a
test that only asserted "it runs" would pass on the wrong answer. Every assertion
here compares against the flat spelling of the same question, and the filtered
answer is required to be a strict subset of the unfiltered one so a filter that
did nothing cannot pass.

Registered as `outer-filter-on-a-limited-grouped-cte-column` in the single-table
fuzzer's defect register and deleted from it when it stopped reproducing; this is
where it is pinned.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

# The CTE body: two group keys, a WHERE, a HAVING and an aggregate. Every one of
# them was an ingredient of the original repro.
BODY = (
    "SELECT planetId, magnitude, COUNT(id) AS a2 FROM testdata.satellites "
    "WHERE id > 4 GROUP BY planetId, magnitude HAVING COUNT(*) <= 3"
)
# Comfortably above the 112 groups the body produces, so the LIMIT is in the plan —
# which is what the defect needed — without making the answer an arbitrary subset
# (see RATIFIED/limit-and-offset-select-an-arbitrary-subset in the register).
LIMIT = 500


def values(sql):
    """Every value of a single-column result, sorted.

    Sorted because nothing here orders its output and row order is not promised;
    a multiset comparison is the strongest assertion the SQL entitles us to.
    """
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for column in morsel.to_arrow().to_pydict().values():
            out.extend(column)
    return sorted(out)


def flat_answer(low, high):
    """The same question with no CTE, no LIMIT and the range applied in Python."""
    session = opteryx.session()
    keys = []
    counts = []
    for morsel in session.execute_to_morsels(BODY):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        keys.extend(table["planetId"])
        counts.extend(table["a2"])
    return sorted(
        count
        for key, count in zip(keys, counts)
        if key is not None and low <= key <= high
    )


def cte(low, high):
    return (
        f"WITH c AS ({BODY} LIMIT {LIMIT}) "
        f"SELECT a2 AS e FROM c WHERE planetId BETWEEN {low} AND {high}"
    )


def derived(low, high):
    return (
        f"SELECT a2 AS e FROM ({BODY} LIMIT {LIMIT}) AS c "
        f"WHERE planetId BETWEEN {low} AND {high}"
    )


def test_outer_filter_matches_the_flat_answer():
    expected = flat_answer(5, 6)
    assert expected, "the fixture selects no rows — the test would assert nothing"
    assert values(cte(5, 6)) == expected
    assert values(derived(5, 6)) == expected, "the derived-table spelling must agree"


def test_the_outer_filter_actually_bites():
    """A lost predicate column produces MORE rows, so the subset is the assertion."""
    filtered = values(cte(5, 6))
    unfiltered = values(f"WITH c AS ({BODY} LIMIT {LIMIT}) SELECT a2 AS e FROM c")
    assert filtered, filtered
    assert len(filtered) < len(unfiltered), (filtered, unfiltered)
    assert values(cte(1000, 2000)) == [], "a range no row satisfies must return nothing"


def test_the_filtered_column_may_also_be_projected():
    """Projecting the predicate's column too must not change which rows survive."""
    also_projected = (
        f"WITH c AS ({BODY} LIMIT {LIMIT}) "
        "SELECT planetId, a2 AS e FROM c WHERE planetId BETWEEN 5 AND 6"
    )
    session = opteryx.session()
    counts = []
    for morsel in session.execute_to_morsels(also_projected):
        if morsel is None:
            continue
        counts.extend(morsel.to_arrow().to_pydict()["e"])
    assert sorted(counts) == flat_answer(5, 6)


def test_the_registered_repro_runs_as_written():
    """The register's own statement, unchanged.

    It selects on a literal no row in `wide` carries, so the honest assertion is
    that it compiles and returns nothing — and the CTE body is checked to return
    nothing on its own, so the emptiness is the data's and not a filter that ate
    the answer. The value assertions above are what pin the behaviour.
    """
    body = (
        "SELECT grp_wide, val_special, COUNT(cat) AS a2 "
        "FROM testdata.fuzzing.wide WHERE val_special IN (-830422.625879) "
        "GROUP BY grp_wide, val_special HAVING COUNT(*) <= 1"
    )
    assert values(body) == []
    assert (
        values(
            f"WITH c AS ({body} LIMIT 1) SELECT a2 AS e FROM c "
            "WHERE val_special BETWEEN -260351.4 AND 271709.7"
        )
        == []
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
