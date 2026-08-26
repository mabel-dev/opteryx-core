"""Running/framed MIN and MAX return the RIGHT VALUES, per argument type.

Two silent wrong answers lived in `FramedWindowSink` (native_window_frame.hpp),
both of them "the query runs, the numbers are wrong" — which is why every
assertion here pins VALUES, never just that the statement executes.

1. FLOAT32/FLOAT64.  MIN/MAX wrote the winner through `agg2_read_raw`, which
   returns a float as the DOUBLE's BIT PATTERN in an int64 container, and parked
   those bits in the `res_i64` lane.  `emit_framed_column` reads `res_f64` for a
   float output type — a lane nothing had written — so every row of

       SELECT MAX(mass) OVER (ORDER BY id) FROM $planets

   came back 0.0.  The integer/temporal/DECIMAL forms of the same query were
   correct throughout, which is what made it track the argument's TYPE rather
   than the frame shape: an explicit `ROWS BETWEEN ...` was wrong for DOUBLE and
   right for INTEGER, and the unframed `OVER (PARTITION BY ...)` form was right
   for both.

2. VARCHAR/NVARCHAR/VARBINARY.  `sort_num_key` has no string arm and returns 0
   for one, so every key compared EQUAL, the monotonic deque popped its whole
   tail on each push, and the "extreme" was always the row just pushed —
   `MAX(name) OVER (ORDER BY id)` returned each row's own name.  The string
   family reached the sink because compiler.py fell through to `_check_key_type`,
   whose `_KEY_COLUMN_TYPES` admits it; the same hole let `SUM(varchar) OVER
   (...)` return garbage strings and `AVG(varchar) OVER (...)` SEGFAULT.
   MIN/MAX now compare byte-wise (`framed_cmp_str`, the ordering SortKeyCmp's
   string arm defines) and emit a real consolidated arena; SUM/AVG over a string
   are refused by name and type at plan time, as the unframed aggregate gate
   already refused them.

Every expected value below is the running extreme over $planets in `id` order,
computed by hand from the column itself — not from a previous engine run.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import NotSupportedError


def col(statement, name):
    """One named output column of `statement`, in row order."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(statement):
        morsel.materialize()
        out.extend(morsel.column(name).to_pylist())
    return out


# $planets in `id` order, for the hand-computed expectations below.
MASSES = [0.33, 4.87, 5.97, 0.642, 1898.0, 568.0, 86.8, 102.0, 0.0146]
NAMES = ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"]


def running(values, pick):
    return [pick(values[: i + 1]) for i in range(len(values))]


def test_running_max_double_is_the_running_maximum():
    """The reported P0: every row came back 0.0."""
    assert col("SELECT MAX(mass) OVER (ORDER BY id) AS mx FROM $planets", "mx") == running(
        MASSES, max
    )


def test_running_min_double_is_the_running_minimum():
    assert col("SELECT MIN(mass) OVER (ORDER BY id) AS mn FROM $planets", "mn") == running(
        MASSES, min
    )


def test_running_minmax_double_under_an_explicit_frame():
    """An explicit ROWS frame was wrong the same way — it was never the frame shape."""
    frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    assert col(
        f"SELECT MAX(mass) OVER (ORDER BY id {frame}) AS mx FROM $planets", "mx"
    ) == running(MASSES, max)


def test_sliding_double_frame_recomputes_after_the_extreme_leaves():
    """A two-row sliding frame — the deque must drop the front once it falls out of
    the window, so this fails differently from the unbounded case if the fix were
    only "write the right lane once"."""
    expected = [max(MASSES[max(0, i - 1) : i + 1]) for i in range(len(MASSES))]
    assert col(
        "SELECT MAX(mass) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mx "
        "FROM $planets",
        "mx",
    ) == expected


def test_running_minmax_double_over_negative_values():
    """Sign is where a float total order goes wrong if the bits are compared naively."""
    negated = [0.0 - m for m in MASSES]
    assert col("SELECT MIN(0.0 - mass) OVER (ORDER BY id) AS mn FROM $planets", "mn") == running(
        negated, min
    )
    assert col("SELECT MAX(0.0 - mass) OVER (ORDER BY id) AS mx FROM $planets", "mx") == running(
        negated, max
    )


def test_running_minmax_double_skips_nulls_and_is_null_until_the_first_value():
    """A NULL is not a zero and not a value: the frame is empty — NULL — until the
    first non-null row enters it."""
    evens = [m if (i + 1) % 2 == 0 else None for i, m in enumerate(MASSES)]
    seen = []
    expected_min = []
    for value in evens:
        if value is not None:
            seen.append(value)
        expected_min.append(min(seen) if seen else None)
    assert (
        col(
            "SELECT MIN(CASE WHEN id % 2 = 0 THEN mass END) OVER (ORDER BY id) AS mn "
            "FROM $planets",
            "mn",
        )
        == expected_min
    )


def test_running_max_double_all_null_argument_is_null_throughout():
    assert col(
        "SELECT MAX(CASE WHEN id < 0 THEN mass END) OVER (ORDER BY id) AS mx FROM $planets",
        "mx",
    ) == [None] * len(MASSES)


def test_running_max_float32_is_the_running_maximum():
    """FLOAT32 shares the broken lane with FLOAT64 (`is_float` covers both) and has
    its OWN emit arm, which narrows the f64 lane back down — so it is pinned
    separately. Compared against the same float32 round-trip the cast produces."""
    import struct

    def f32(value):
        return struct.unpack("f", struct.pack("f", value))[0]

    assert col(
        "SELECT MAX(mass::FLOAT32) OVER (ORDER BY id) AS mx FROM $planets", "mx"
    ) == running([f32(m) for m in MASSES], max)


def test_running_minmax_integer_still_correct():
    """The type that was always right — pinned so a fix to the float lane cannot
    regress the int64 one it shares a loop with."""
    assert col("SELECT MAX(id) OVER (ORDER BY id) AS mx FROM $planets", "mx") == list(range(1, 10))
    assert col("SELECT MIN(10 - id) OVER (ORDER BY id) AS mn FROM $planets", "mn") == running(
        [10 - i for i in range(1, 10)], min
    )


def test_running_minmax_decimal_still_correct():
    from decimal import Decimal

    expected = running([Decimal(str(m)).quantize(Decimal("0.0001")) for m in MASSES], max)
    assert col(
        "SELECT MAX(mass::DECIMAL(20,4)) OVER (ORDER BY id) AS mx FROM $planets", "mx"
    ) == expected


def test_running_max_varchar_is_the_running_maximum():
    """Was: each row's own name."""
    assert col("SELECT MAX(name) OVER (ORDER BY id) AS mx FROM $planets", "mx") == running(
        NAMES, max
    )


def test_running_min_varchar_is_the_running_minimum():
    assert col("SELECT MIN(name) OVER (ORDER BY id) AS mn FROM $planets", "mn") == running(
        NAMES, min
    )


def test_running_max_varchar_over_out_of_line_strings():
    """Longer than STR_INLINE_MAX (12 bytes), so the winner lives in the arena and
    the emit must copy the payload, not just clone the slot."""
    long_names = [f"{n}-{n}-planet" for n in NAMES]
    assert col(
        "SELECT MAX(name || '-' || name || '-planet') OVER (ORDER BY id) AS mx FROM $planets",
        "mx",
    ) == running(long_names, max)


def test_sliding_varchar_frame_recomputes_after_the_extreme_leaves():
    expected = [min(NAMES[max(0, i - 1) : i + 1]) for i in range(len(NAMES))]
    assert col(
        "SELECT MIN(name) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mn "
        "FROM $planets",
        "mn",
    ) == expected


def test_running_minmax_varchar_skips_nulls():
    evens = [n if (i + 1) % 2 == 0 else None for i, n in enumerate(NAMES)]
    seen = []
    expected = []
    for value in evens:
        if value is not None:
            seen.append(value)
        expected.append(min(seen) if seen else None)
    assert (
        col(
            "SELECT MIN(CASE WHEN id % 2 = 0 THEN name END) OVER (ORDER BY id) AS mn FROM $planets",
            "mn",
        )
        == expected
    )


def test_running_max_varbinary_is_the_running_maximum():
    assert col(
        "SELECT MAX(name::VARBINARY) OVER (ORDER BY id) AS mx FROM $planets", "mx"
    ) == running([n.encode() for n in NAMES], max)


def test_partitioned_running_max_varchar():
    """PARTITION BY resets the deque; the output is in sorted (partition, id) order."""
    odds = [n for i, n in enumerate(NAMES) if (i + 1) % 2 == 1]
    evens = [n for i, n in enumerate(NAMES) if (i + 1) % 2 == 0]
    # `id % 2` sorts 0 (the even ids) before 1.
    assert col(
        "SELECT MAX(name) OVER (PARTITION BY id % 2 ORDER BY id) AS mx FROM $planets", "mx"
    ) == running(evens, max) + running(odds, max)


def test_count_over_a_varchar_argument_still_counts():
    """COUNT reads validity, never the value — the string family stays admissible."""
    assert col("SELECT COUNT(name) OVER (ORDER BY id) AS c FROM $planets", "c") == list(
        range(1, 10)
    )


@pytest.mark.parametrize("func", ["SUM", "AVG"])
def test_sum_and_avg_over_a_varchar_argument_are_refused(func):
    """Was: SUM returned garbage strings and AVG segfaulted. The unframed aggregate
    already refused both in these words; the framed one now does too."""
    with pytest.raises(NotSupportedError) as raised:
        col(f"SELECT {func}(name) OVER (ORDER BY id) AS c FROM $planets", "c")
    message = str(raised.value)
    assert f"**{func}**" in message, message
    assert "VARCHAR" in message, message


# The chunked emit runs one worker per 131072-row chunk (`chunk_rows`) and only goes
# multi-threaded above 200000 rows, so the single-chunk $planets cases above never
# reach either path. These two do — and the string emit, which builds a fresh arena
# per chunk out of winners that live in ARBITRARY source morsels, is the new code
# that path most needs pinned.
_BIG = "testdata/tpcds_1/customer_demographics"


def _running_extreme_matches(statement, result_name, key_name, value_name, pick, cast=None):
    """Compare the engine's per-row extreme against one computed here from the same
    rows, re-sorted by the window's own ORDER BY key."""
    session = opteryx.session()
    got, pairs = [], []
    for morsel in session.execute_to_morsels(statement):
        morsel.materialize()
        got.extend(morsel.column(result_name).to_pylist())
        pairs.extend(
            zip(morsel.column(key_name).to_pylist(), morsel.column(value_name).to_pylist())
        )
    assert got, "no rows"
    running, expected = None, []
    for _key, value in sorted(pairs):
        value = cast(value) if cast is not None else value
        running = value if running is None else pick(running, value)
        expected.append(running)
    return got == expected, len(got)


@pytest.mark.skipif(not os.path.isdir(_BIG), reason=f"{_BIG} not present")
def test_running_max_varchar_across_many_chunks():
    ok, rows = _running_extreme_matches(
        f"SELECT MAX(cd_education_status) OVER (ORDER BY cd_demo_sk) AS mx, "
        f"cd_demo_sk AS k, cd_education_status AS v FROM '{_BIG}'",
        "mx", "k", "v", max,
    )
    assert rows > 200000, rows
    assert ok


@pytest.mark.skipif(not os.path.isdir(_BIG), reason=f"{_BIG} not present")
def test_running_min_double_across_many_chunks():
    ok, rows = _running_extreme_matches(
        f"SELECT MIN(cd_dep_count::FLOAT64) OVER (ORDER BY cd_demo_sk) AS mn, "
        f"cd_demo_sk AS k, cd_dep_count AS v FROM '{_BIG}'",
        "mn", "k", "v", min, cast=float,
    )
    assert rows > 200000, rows
    assert ok


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
