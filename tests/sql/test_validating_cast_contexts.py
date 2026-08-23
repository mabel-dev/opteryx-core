"""A CAST that can reject its input must reject it in EVERY context.

The defect this file pins: `CastSimplificationStrategy` collapsed
`CAST(CAST(x AS T1) AS T2)` to `CAST(x AS T2)` — dropping the inner cast's
kernel entirely — and it did so only on Filter nodes. So

    WHERE  LENGTH(CAST(CAST(a AS IPV4) AS VARCHAR)) = 9      -- returned the row
    SELECT LENGTH(CAST(CAST(a AS IPV4) AS VARCHAR))          -- raised DataError

for the same value. Two spellings that differ only in WHERE the cast sits got
different answers, and the predicate form was the wrong one: '999.1.1.1' has an
octet > 255 and a plain CAST is contracted to fail loud so a typo'd ACL cannot
quietly become a rule matching 0.0.0.0.

This is not an IPv4 defect — every validating cast was affected (DATE, INTEGER,
DECIMAL...), and the collapse also dropped *value* transformations, not just
rejections: `CAST(CAST(1.9 AS INTEGER) AS VARCHAR)` is '1' in a projection but
compared as '1.9' in a predicate. Both halves are asserted below.

The rule being defended: a cast that can raise raises wherever it appears, and
TRY_CAST is the only way to get NULL instead. An OUTER TRY_CAST does not launder
an inner plain CAST — the TRY_ applies to the cast it is written on, not to the
whole chain.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import DataError


def _rows(sql):
    """Execute and return the first column's values, as a list."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel.num_rows == 0:
            continue
        out.extend(morsel.column(morsel.column_names[0]).to_pylist())
    return out


# The inner cast's target, a value that target must REFUSE, and a projection of
# the round trip that is a plain `CAST` at both levels. The predicate form wraps
# the identical expression in LENGTH(...) = n so the whole chain sits inside a
# Filter condition.
REJECTING_ROUND_TRIPS = [
    ("IPV4", "999.1.1.1"),
    ("IPV4", "1.2.3"),
    ("DATE", "not-a-date"),
    ("INTEGER", "abc"),
    ("FLOAT64", "not-a-number"),
]


@pytest.mark.parametrize("target,bad_value", REJECTING_ROUND_TRIPS)
def test_rejecting_cast_raises_in_a_projection(target, bad_value):
    sql = (
        f"SELECT CAST(CAST(a AS {target}) AS VARCHAR) "
        f"FROM (SELECT '{bad_value}' AS a) AS t"
    )
    with pytest.raises(DataError):
        _rows(sql)


@pytest.mark.parametrize("target,bad_value", REJECTING_ROUND_TRIPS)
def test_rejecting_cast_raises_in_a_predicate(target, bad_value):
    """The regression. Before the fix these returned the row instead of raising."""
    sql = (
        f"SELECT * FROM (SELECT '{bad_value}' AS a) AS t "
        f"WHERE LENGTH(CAST(CAST(a AS {target}) AS VARCHAR)) = {len(bad_value)}"
    )
    with pytest.raises(DataError):
        _rows(sql)


@pytest.mark.parametrize("target,bad_value", REJECTING_ROUND_TRIPS)
def test_an_outer_try_cast_does_not_launder_an_inner_plain_cast(target, bad_value):
    """TRY_ applies to the cast it is written on, not to everything beneath it.
    The inner plain CAST still rejects — anything else would make the failure
    escapable by a cast the user wrote for an unrelated reason."""
    sql = (
        f"SELECT * FROM (SELECT '{bad_value}' AS a) AS t "
        f"WHERE LENGTH(TRY_CAST(CAST(a AS {target}) AS VARCHAR)) = {len(bad_value)}"
    )
    with pytest.raises(DataError):
        _rows(sql)


@pytest.mark.parametrize("target,bad_value", REJECTING_ROUND_TRIPS)
def test_try_cast_is_the_opt_in_to_nulling_the_row(target, bad_value):
    """TRY_CAST on the *validating* cast NULLs the value; LENGTH(NULL) is NULL,
    so the row does not match. No raise, no row."""
    sql = (
        f"SELECT * FROM (SELECT '{bad_value}' AS a) AS t "
        f"WHERE LENGTH(TRY_CAST(TRY_CAST(a AS {target}) AS VARCHAR)) = {len(bad_value)}"
    )
    assert _rows(sql) == []


def test_a_valid_address_still_round_trips_in_both_contexts():
    """The negative control — the fix must not have turned the round trip off."""
    assert _rows(
        "SELECT CAST(CAST(a AS IPV4) AS VARCHAR) FROM (SELECT '192.168.1.1' AS a) AS t"
    ) == ["192.168.1.1"]
    assert _rows(
        "SELECT * FROM (SELECT '192.168.1.1' AS a) AS t "
        "WHERE LENGTH(CAST(CAST(a AS IPV4) AS VARCHAR)) = 11"
    ) == ["192.168.1.1"]


def test_an_inner_cast_that_changes_the_value_is_not_dropped_either():
    """The other half of the class: the collapse dropped value transformations,
    not only rejections. FLOAT -> INTEGER truncates, so the VARCHAR the outer
    cast produces is '1', not '1.9' — in the predicate exactly as in the
    projection. Before the fix the predicate compared '1.9'."""
    assert _rows("SELECT CAST(CAST(a AS INTEGER) AS VARCHAR) FROM (SELECT 1.9 AS a) AS t") == ["1"]
    assert _rows(
        "SELECT * FROM (SELECT 1.9 AS a) AS t WHERE CAST(CAST(a AS INTEGER) AS VARCHAR) = '1'"
    ) == [1.9]
    assert (
        _rows(
            "SELECT * FROM (SELECT 1.9 AS a) AS t WHERE CAST(CAST(a AS INTEGER) AS VARCHAR) = '1.9'"
        )
        == []
    )


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
