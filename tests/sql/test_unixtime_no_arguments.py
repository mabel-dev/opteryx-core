"""Regression tests for the zero-argument form of UNIXTIME.

UNIXTIME() is folded to a constant at plan time
(opteryx/expression/functions/registrar/constant.pyx, fixed_value_function).
It was folded from `connected_at.timestamp()` - a FLOAT - into a literal
declared INT64, so the binder refused it before the query ran:

    Literal #5 of type INT64 holds a float (1791019201.200554); a INT64
    literal holds its native value

The registered function is "Convert TIMESTAMP to Unix epoch seconds" returning
INT64, so the bare form must be the same whole-seconds INT64 as UNIXTIME(NOW()).

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx

_SESSION = opteryx.session()


def _row(sql):
    rows = []
    for morsel in _SESSION.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in (b"a", b"b")]
        rows.extend(zip(*columns))
    assert len(rows) == 1
    return rows[0]


def test_unixtime_bare_plans_and_is_an_integer():
    bare, now = _row("SELECT UNIXTIME() AS a, UNIXTIME(NOW()) AS b")
    assert isinstance(bare, int) and not isinstance(bare, bool)
    assert isinstance(now, int)
    assert abs(bare - now) <= 1


def test_unixtime_bare_matches_to_unixtime_alias():
    bare, alias = _row("SELECT UNIXTIME() AS a, TO_UNIXTIME(NOW()) AS b")
    assert abs(bare - alias) <= 1


def test_to_unixtime_alias_bare_is_folded():
    """The zero-argument overload is declared on the definition, so the alias
    resolves to it too. It must be folded like UNIXTIME() - reaching the
    placeholder kernel instead crashed the process."""
    alias, now = _row("SELECT TO_UNIXTIME() AS a, UNIXTIME(NOW()) AS b")
    assert isinstance(alias, int)
    assert abs(alias - now) <= 1


@pytest.mark.parametrize(
    "bare_expr,now_expr,tolerance",
    [
        ("UNIXTIME() - 183 * 86400", "UNIXTIME(NOW() - INTERVAL '183' DAY)", 1),
        ("(UNIXTIME() - 183 * 86400) * 1000", "UNIXTIME(NOW() - INTERVAL '183' DAY) * 1000", 1000),
        ("UNIXTIME() + 1", "UNIXTIME(NOW()) + 1", 1),
    ],
)
def test_unixtime_bare_inside_arithmetic(bare_expr, now_expr, tolerance):
    bare, now = _row(f"SELECT {bare_expr} AS a, {now_expr} AS b")
    assert isinstance(bare, int)
    assert abs(bare - now) <= tolerance


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
