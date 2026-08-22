"""End-to-end tests for `CAST(... AS <type> FORMAT '<pattern>')`.

The whole FORMAT surface was untested, which is how a memory-safety defect in
the parse kernels survived: `sql_compile` fills `SqlToken.lit` with pointers
INTO the format buffer (draken/ops/sql_temporal_format.h), and both parse
kernels built that buffer as a `std::string` scoped inside the `if (use_fmt)`
block, then walked the program after the block exited. The literal pointers
dangled into reclaimed stack, so whether a pattern parsed depended on where its
first literal token happened to sit:

    CAST(x AS TIMESTAMP FORMAT 'YYYY-MM-DD')  worked   (literal at offset 4)
    CAST(x AS TIMESTAMP FORMAT 'DD-MM-YYYY')  failed   (literal at offset 2)
    CAST(x AS TIMESTAMP FORMAT 'DDMMYYYY')    worked   (no literal token at all)

DATE passed the same patterns only because its stack frame reclaimed those bytes
differently — it was luck, not correctness, and a different platform or
optimisation level moves the boundary. So `_LITERAL_OFFSETS` below deliberately
sweeps patterns whose first literal sits at offsets 0..4, on BOTH targets.

One token program serves both directions — parse when the target is
DATE/TIMESTAMP, render when it is VARCHAR/BLOB (and INTERVAL -> VARCHAR, where
the tokens mean duration magnitudes) — so the round-trip tests are the strongest
oracle available: they compare against the engine's own column values rather
than hand-written expected strings.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError

_SESSION = opteryx.session()

# DATE32 column and TIMESTAMP64 column, both with no NULLs.
_DATE_SOURCE = ("testdata.astronauts", "birth_date")
_TIMESTAMP_SOURCE = ("testdata.missions", "Lauched_at")


def _col(sql, name="r"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(name.encode()).to_pylist())
    return out


def _one(sql):
    values = _col(sql)
    assert len(values) == 1, f"expected exactly one row, got {len(values)}"
    return values[0]


def _literal_row(value):
    """A one-row VARCHAR source. VALUES keeps the string a real vector element
    rather than a constant the planner could fold — a FORMAT-bearing CAST is
    never folded (logical_planner_builders.cast), and this keeps it that way
    even if that changes."""
    return f"(VALUES ('{value}')) AS t(s)"


# ---------------------------------------------------------------------------
# Parse direction: VARCHAR -> DATE / TIMESTAMP
#
# (pattern, input, expected y/m/d) — sweeping the offset of the FIRST literal
# token, which is what the dangling-pointer defect was sensitive to.
# ---------------------------------------------------------------------------

_LITERAL_OFFSETS = [
    ("xDD-MM-YYYY", "x15-01-2024", (2024, 1, 15)),  # literal at offset 0
    ("DD-MM-YYYY", "15-01-2024", (2024, 1, 15)),  # literal at offset 2
    ("MM-DD-YYYY", "01-15-2024", (2024, 1, 15)),
    ("DD/MM/YYYY", "15/01/2024", (2024, 1, 15)),
    ("YY-MM-DD", "24-01-15", (2024, 1, 15)),
    ("YYYY-MM-DD", "2024-01-15", (2024, 1, 15)),  # literal at offset 4
    ("YYYY-DD-MM", "2024-15-01", (2024, 1, 15)),
    ("YYYY/MM/DD", "2024/01/15", (2024, 1, 15)),
    ("DDMMYYYY", "15012024", (2024, 1, 15)),  # no literal token
    ("YYYYMMDD", "20240115", (2024, 1, 15)),
]


@pytest.mark.parametrize("pattern,text,expected", _LITERAL_OFFSETS)
def test_parse_to_date(pattern, text, expected):
    result = _one(f"SELECT CAST(s AS DATE FORMAT '{pattern}') AS r FROM {_literal_row(text)}")
    assert (result.year, result.month, result.day) == expected


@pytest.mark.parametrize("pattern,text,expected", _LITERAL_OFFSETS)
def test_parse_to_timestamp(pattern, text, expected):
    result = _one(f"SELECT CAST(s AS TIMESTAMP FORMAT '{pattern}') AS r FROM {_literal_row(text)}")
    assert (result.year, result.month, result.day) == expected
    assert (result.hour, result.minute, result.second) == (0, 0, 0)


def test_parse_to_timestamp_with_time_tokens():
    result = _one(
        "SELECT CAST(s AS TIMESTAMP FORMAT 'DD-MM-YYYY HH24:MI:SS') AS r "
        f"FROM {_literal_row('15-01-2024 09:30:45')}"
    )
    assert (result.year, result.month, result.day) == (2024, 1, 15)
    assert (result.hour, result.minute, result.second) == (9, 30, 45)


def test_parse_defaults_unsupplied_fields_to_the_epoch():
    """A pattern need not name every field. Whatever it omits keeps
    sql_parse_exec's defaults — 1970-01-01 00:00:00 — rather than being an
    error or picking up today's date."""
    result = _one(f"SELECT CAST(s AS TIMESTAMP FORMAT 'HH24:MI') AS r FROM {_literal_row('09:30')}")
    assert (result.year, result.month, result.day) == (1970, 1, 1)
    assert (result.hour, result.minute) == (9, 30)


def test_parse_requires_the_pattern_to_consume_the_whole_input():
    """Trailing input is a parse failure, not a silent prefix match — otherwise
    'YYYY-MM-DD' would quietly discard the time component."""
    with pytest.raises(Exception) as caught:
        _col(
            "SELECT CAST(s AS TIMESTAMP FORMAT 'YYYY-MM-DD') AS r "
            f"FROM {_literal_row('2024-01-15 10:20:30')}"
        )
    assert "2024-01-15 10:20:30" in str(caught.value)


def test_parse_rejects_input_that_does_not_match_the_pattern():
    """The pattern is authoritative: an ISO string is a failure under a
    day-first pattern. If this passes, the pattern is being ignored and the
    default ISO parser is running instead."""
    with pytest.raises(Exception) as caught:
        _col(f"SELECT CAST(s AS DATE FORMAT 'DD-MM-YYYY') AS r FROM {_literal_row('2024-01-15')}")
    assert "2024-01-15" in str(caught.value)


# ---------------------------------------------------------------------------
# Render direction: DATE / TIMESTAMP / INTERVAL -> VARCHAR
# ---------------------------------------------------------------------------


def test_render_date_to_varchar():
    assert (
        _one("SELECT CAST(CAST(s AS DATE) AS VARCHAR FORMAT 'DD-MM-YYYY') AS r "
             f"FROM {_literal_row('2024-01-15')}")
        == "15-01-2024"
    )


def test_render_timestamp_to_varchar():
    assert (
        _one("SELECT CAST(CAST(s AS TIMESTAMP) AS VARCHAR FORMAT 'YYYY-MM-DD HH24:MI') AS r "
             f"FROM {_literal_row('2024-01-15 09:30:45')}")
        == "2024-01-15 09:30"
    )


def test_varbinary_target_is_refused_despite_the_kernels_existing():
    """OPEN DEFECT, recorded rather than silently fixed. The planner's FORMAT
    allowlist names "BLOB" (logical_planner_builders.cast), but
    `_normalize_cast_type` rejects that spelling under the canonical-only ruling
    and produces VARBINARY — so the allowlist entry is dead and the binary
    target is unreachable, even though draken_cast_date_to_blob /
    draken_cast_timestamp_to_blob / draken_cast_interval_to_blob are all in
    _CAST_FORMAT_AWARE_KERNELS and implemented. The refusal message also names
    the non-canonical `BLOB`. This test pins the CURRENT behaviour so the change
    is deliberate when it is made."""
    with pytest.raises(UnsupportedSyntaxError) as caught:
        _col("SELECT CAST(CAST(s AS DATE) AS VARBINARY FORMAT 'DD-MM-YYYY') AS r "
             f"FROM {_literal_row('2024-01-15')}")
    assert "not VARBINARY" in str(caught.value)


def test_render_interval_tokens_are_duration_magnitudes():
    """INTERVAL reuses the same token program with a different field source:
    DD is a count of days, not a calendar day-of-month."""
    assert _one("SELECT CAST(INTERVAL '1' DAY AS VARCHAR FORMAT 'DD HH24:MI:SS') AS r") == (
        "01 00:00:00"
    )


def test_render_hh12_wraps_the_afternoon():
    assert (
        _one("SELECT CAST(CAST(s AS TIMESTAMP) AS VARCHAR FORMAT 'HH12:MI') AS r "
             f"FROM {_literal_row('2024-01-15 13:05:00')}")
        == "01:05"
    )


# ---------------------------------------------------------------------------
# Round-trip over real columns — non-constant sources, whole-column coverage.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pattern", ["DD-MM-YYYY", "MM/DD/YYYY", "YYYY-MM-DD", "YYYYMMDD"])
def test_date_column_round_trips(pattern):
    table, column = _DATE_SOURCE
    rendered_then_parsed = _col(
        f"SELECT CAST(CAST({column} AS VARCHAR FORMAT '{pattern}') "
        f"AS DATE FORMAT '{pattern}') AS r FROM {table}"
    )
    original = _col(f"SELECT {column} AS r FROM {table}")
    assert rendered_then_parsed == original
    assert len(original) > 100


@pytest.mark.parametrize("pattern", ["DD-MM-YYYY HH24:MI:SS", "YYYY-MM-DD HH24:MI:SS"])
def test_timestamp_column_round_trips(pattern):
    table, column = _TIMESTAMP_SOURCE
    rendered_then_parsed = _col(
        f"SELECT CAST(CAST({column} AS VARCHAR FORMAT '{pattern}') "
        f"AS TIMESTAMP FORMAT '{pattern}') AS r FROM {table}"
    )
    original = _col(f"SELECT {column} AS r FROM {table}")
    assert rendered_then_parsed == original
    assert len(original) > 100


# ---------------------------------------------------------------------------
# TRY_CAST / SAFE_CAST
#
# The type catalog used to publish "CAST ... FORMAT is not yet supported
# combined with TRY_CAST/SAFE_CAST" on DATE, TIMESTAMP and INTERVAL. It runs:
# every format-aware kernel reads the disposition from format_ctx.safe. The
# claim was a restriction the engine did not have, so these tests exist to keep
# it from being re-asserted.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("target", ["DATE", "TIMESTAMP"])
def test_try_cast_with_format_nulls_unparseable_input(target):
    assert (
        _one(f"SELECT TRY_CAST(s AS {target} FORMAT 'DD-MM-YYYY') AS r FROM {_literal_row('nope')}")
        is None
    )


@pytest.mark.parametrize("target", ["DATE", "TIMESTAMP"])
def test_try_cast_with_format_still_parses_good_input(target):
    result = _one(
        f"SELECT TRY_CAST(s AS {target} FORMAT 'DD-MM-YYYY') AS r FROM {_literal_row('15-01-2024')}"
    )
    assert (result.year, result.month, result.day) == (2024, 1, 15)


def test_try_cast_with_format_renders():
    assert (
        _one("SELECT TRY_CAST(CAST(s AS DATE) AS VARCHAR FORMAT 'DD-MM-YYYY') AS r "
             f"FROM {_literal_row('2024-01-15')}")
        == "15-01-2024"
    )


# ---------------------------------------------------------------------------
# Fail-loud surface
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["DD Mon YYYY", "YYY-MM-DD", "HHH:MI"])
def test_unrecognised_token_fails_loud(bad):
    """A reserved-letter run that is not an exact keyword is a compile error,
    not silent literal passthrough — the point is to catch a typo rather than
    emit the typo verbatim."""
    with pytest.raises(Exception) as caught:
        _col(f"SELECT CAST(s AS DATE FORMAT '{bad}') AS r FROM {_literal_row('whatever')}")
    assert "unrecognized format token" in str(caught.value)


@pytest.mark.parametrize("target", ["INTEGER", "FLOAT64", "BOOLEAN"])
def test_format_is_rejected_for_non_temporal_targets(target):
    with pytest.raises(UnsupportedSyntaxError) as caught:
        _col(f"SELECT CAST(s AS {target} FORMAT '999') AS r FROM {_literal_row('12')}")
    assert "VARCHAR/BLOB/TIMESTAMP/DATE" in str(caught.value)


def test_lowercase_pattern_text_is_literal_passthrough():
    """Tokens are uppercase-only, so lowercase never collides with one."""
    assert (
        _one("SELECT CAST(CAST(s AS DATE) AS VARCHAR FORMAT 'day DD of YYYY') AS r "
             f"FROM {_literal_row('2024-01-15')}")
        == "day 15 of 2024"
    )


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
