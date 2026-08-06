"""
Regression coverage for correlations that reach past the immediately enclosing
subquery to a GRANDPARENT scope.

Such a correlation used to be added to the inner GROUP BY but never bound as a
join key: `extract_join_fields` keeps a pair only when one side names the join's
left leg and the other its right, and the grandparent's relation is on neither.
The equality survived in `on` and vanished from the key lists, so it neither
filtered nor deduplicated — and the widened grouping MULTIPLIED rows into the
aggregate above it. The result set grew instead of shrinking.

Expected values are cross-checked against DuckDB over the same parquet files.

These live here rather than in shapes_joins_subqueries.slt deliberately. That
file's `OverflowError: int8` abort at line 147 is fixed and it now runs end to
end, but an SLT assertion is a SHAPE assertion: it cannot carry the DuckDB
oracle counts or the reasoning above, and the runner still aborts a whole file
at its first failure, so a shape drift anywhere upstream would silently retire
this coverage again — which is exactly how it was lost the first time.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import opteryx
from opteryx.connectors import DiskConnector
from opteryx.exceptions import UnsupportedSyntaxError

opteryx.register_workspace("testdata", DiskConnector)

T = "testdata.tpch_001"


def _rows(sql):
    session = opteryx.session()
    return sum(morsel.num_rows for morsel in session.execute_to_morsels(sql))


def test_skip_level_correlation_is_enforced():
    """The grandparent correlation must filter. DuckDB oracle: 239 rows."""
    assert (
        _rows(f"""
        SELECT c_custkey FROM {T}.customer
        WHERE (SELECT COUNT(*) FROM {T}.orders
               WHERE o_custkey = c_custkey
                 AND (SELECT SUM(l_quantity) FROM {T}.lineitem
                      WHERE l_orderkey = o_orderkey
                        AND l_linenumber = c_nationkey) > 20) > 1
        """)
        == 239
    )


def test_two_skip_level_correlations():
    """Several deferred keys are each bound on the ancestor. Oracle: 16 rows."""
    assert (
        _rows(f"""
        SELECT c_custkey FROM {T}.customer
        WHERE (SELECT COUNT(*) FROM {T}.orders
               WHERE o_custkey = c_custkey
                 AND (SELECT SUM(l_quantity) FROM {T}.lineitem
                      WHERE l_orderkey = o_orderkey
                        AND l_linenumber = c_nationkey
                        AND l_suppkey = c_nationkey) > 5) > 0
        """)
        == 16
    )


def test_skip_level_correlation_is_monotonic():
    """
    Adding a conjunct to the innermost WHERE can only ever REDUCE the result.

    This is the property the bug violated (190 -> 198), and it needs no oracle.
    """
    without = _rows(f"""
        SELECT c_custkey FROM {T}.customer
        WHERE (SELECT COUNT(*) FROM {T}.orders
               WHERE o_custkey = c_custkey
                 AND (SELECT SUM(l_quantity) FROM {T}.lineitem
                      WHERE l_orderkey = o_orderkey) > 20) > 1
    """)
    with_extra = _rows(f"""
        SELECT c_custkey FROM {T}.customer
        WHERE (SELECT COUNT(*) FROM {T}.orders
               WHERE o_custkey = c_custkey
                 AND (SELECT SUM(l_quantity) FROM {T}.lineitem
                      WHERE l_orderkey = o_orderkey
                        AND l_linenumber = c_nationkey) > 20) > 1
    """)
    assert with_extra <= without, (
        f"adding a conjunct increased the result: {without} -> {with_extra}"
    )


def _raises_unsupported(sql) -> bool:
    try:
        _rows(sql)
    except UnsupportedSyntaxError:
        return True
    return False


def test_skip_level_exists_refuses_rather_than_answering_wrongly():
    """
    EXISTS cannot defer to an ancestor: SEMI/ANTI emit left-side columns only, so
    the carried inner column cannot survive the join to be bound higher up. That
    needs a materialised domain; until then it must refuse, not guess.
    """
    assert _raises_unsupported(f"""
        SELECT c_custkey FROM {T}.customer
        WHERE EXISTS (SELECT 1 FROM {T}.orders
                      WHERE o_custkey = c_custkey
                        AND EXISTS (SELECT 1 FROM {T}.lineitem
                                    WHERE l_orderkey = o_orderkey
                                      AND l_linenumber = c_nationkey))
    """)


def test_skip_level_in_refuses_rather_than_answering_wrongly():
    assert _raises_unsupported(f"""
        SELECT c_custkey FROM {T}.customer
        WHERE c_custkey IN (SELECT o_custkey FROM {T}.orders
                            WHERE o_orderkey IN (SELECT l_orderkey FROM {T}.lineitem
                                                 WHERE l_linenumber = c_nationkey))
    """)


if __name__ == "__main__":
    for name, case in sorted(globals().items()):
        if name.startswith("test_") and callable(case):
            case()
            print(f"{name}: OK")
    print("\nall passed")
