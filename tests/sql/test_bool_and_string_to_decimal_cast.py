"""CAST(BOOL AS DECIMAL) and CAST(STRING AS DECIMAL).

These were the two remaining holes in the `→ DECIMAL` column of the cast table.
Every other numeric source already had a kernel (INT64, the narrow signed and
unsigned families, FLOAT32/64, and DECIMAL itself), so DECIMAL was reachable from
everything EXCEPT a boolean and a piece of text.

BOOL surfaced through UNION leg coercion rather than an explicit cast:
`find_compatible_type` promotes BOOL + DECIMAL to DECIMAL, the binder inserts a
CAST for the boolean leg, and lowering then had no arm for it — so the query died
at the compiler's c-native admission gate ("a CAST in `id > 3::DECIMAL(38,18)`,
outside the c-native kernel set"). An honest refusal, but a missing capability:
BOOL + INT64, BOOL + FLOAT64 and BOOL + VARCHAR all worked, and DECIMAL was the
only pairing in the union type matrix that did not.

What is defended here:

  1. true is the decimal 1 and false the decimal 0 — the same integer promotion
     BOOL already has to every other numeric target, at the target's own scale
     (so DECIMAL(3,1) stores 10, not 1; reading the raw payload back as 1 would
     be a wrong answer wearing a cast's clothes).

  2. STRING → DECIMAL parses EXACTLY. It is deliberately not composed from
     draken_cast_string_to_float64 the way the narrow-int and float32 targets are
     composed from their parsers: a double holds ~15-17 significant digits, so a
     detour through one would silently corrupt the low digits of exactly the
     values DECIMAL exists to keep. An 18-digit round trip is the regression test
     for that.

  3. The declared type is a CONTRACT, not a hint. Fractional digits past the
     declared scale fail loud rather than rounding away; a magnitude past the
     declared precision fails loud rather than wrapping. TRY_CAST maps both to
     NULL instead, and the two dispositions must never disagree about what
     "converts" means.

  4. A cast over a COLUMN and the same cast over a LITERAL agree. The literal
     path is a Python closure (_build_decimal_closure, casts.pyx) and the column
     path is the new kernel; two implementations of one contract can drift, so
     the accepted syntax is pinned on both sides.

Both physical tiers are covered: DECIMAL (int64-backed, p<=18) and DECIMAL128
(int128-backed, p>18).

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

from decimal import Decimal

import pytest

import opteryx

_SESSION = opteryx.session()


def _col(sql, colname="x"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(colname).to_pylist())
    return out


# --- BOOL → DECIMAL ------------------------------------------------------------


def test_union_of_decimal_and_bool_legs_runs():
    """The original repro. $planets.gravity is DECIMAL(3,1), `id > 3` is BOOL.

    Compared as a multiset — UNION ALL guarantees the rows, not their order.
    """
    got = _col(
        "SELECT gravity AS x FROM $planets "
        "UNION ALL SELECT id > 3 AS x FROM $planets"
    )
    gravity = _col("SELECT gravity AS x FROM $planets")
    ids = _col("SELECT id AS x FROM $planets")
    expected = list(gravity) + [Decimal(1) if i > 3 else Decimal(0) for i in ids]
    assert len(got) == 18, len(got)
    assert sorted(got) == sorted(expected), (got, expected)


def test_union_of_decimal_and_bool_legs_runs_in_either_order():
    """The coercion is symmetric; so must the kernel selection be."""
    a = _col("SELECT gravity AS x FROM $planets UNION ALL SELECT id > 3 AS x FROM $planets")
    b = _col("SELECT id > 3 AS x FROM $planets UNION ALL SELECT gravity AS x FROM $planets")
    assert sorted(a) == sorted(b), (a, b)


def test_bool_column_to_decimal_uses_the_target_scale():
    """true stores as 10^scale, not as the raw 1 — the wrong-answer case."""
    got = _col("SELECT CAST(id > 3 AS DECIMAL(3,1)) AS x FROM $planets")
    assert got == [Decimal("0.0")] * 3 + [Decimal("1.0")] * 6, got
    # Compared as VALUES, not payloads: a scale-blind kernel would give 0.1 here.
    assert got[-1] == Decimal(1), got[-1]


def test_bool_to_decimal_both_physical_tiers():
    """p<=18 is int64-backed, p>18 int128-backed — separate destination tiers."""
    narrow = _col("SELECT CAST(id > 3 AS DECIMAL(18,4)) AS x FROM $planets")
    wide = _col("SELECT CAST(id > 3 AS DECIMAL(38,18)) AS x FROM $planets")
    assert narrow[-1] == Decimal(1) and wide[-1] == Decimal(1), (narrow[-1], wide[-1])
    assert narrow[0] == Decimal(0) and wide[0] == Decimal(0), (narrow[0], wide[0])


def test_bool_to_decimal_preserves_nulls():
    """A null boolean stays null; it does not become 0."""
    got = _col(
        "SELECT CAST(CASE WHEN id > 3 THEN true END AS DECIMAL(5,2)) AS x FROM $planets"
    )
    assert got[:3] == [None, None, None], got[:3]
    assert got[3:] == [Decimal(1)] * 6, got[3:]


def test_bool_to_decimal_overflow_is_loud():
    """DECIMAL(1,1) spans -0.9..0.9 and genuinely cannot represent 1."""
    with pytest.raises(Exception):
        _col("SELECT CAST(id > 3 AS DECIMAL(1,1)) AS x FROM $planets")


def test_bool_to_decimal_overflow_under_try_cast_is_null():
    """Same kernel, other disposition — a raise and a NULL must agree on WHICH
    rows convert. false fits DECIMAL(1,1); only true overflows."""
    got = _col("SELECT TRY_CAST(id > 3 AS DECIMAL(1,1)) AS x FROM $planets")
    assert got == [Decimal(0)] * 3 + [None] * 6, got


def test_bool_to_decimal_works_in_a_predicate():
    """Not just projection — the same program shape has to admit in a filter."""
    assert _col(
        "SELECT COUNT(*) AS x FROM $planets WHERE CAST(id > 3 AS DECIMAL(5,2)) > 0.5"
    ) == [6]


# --- STRING → DECIMAL ----------------------------------------------------------


def test_decimal_survives_a_round_trip_through_varchar():
    """The pairing that was refused outright, over a real string COLUMN."""
    gravity = _col("SELECT gravity AS x FROM $planets")
    got = _col("SELECT CAST(CAST(gravity AS VARCHAR) AS DECIMAL(3,1)) AS x FROM $planets")
    assert got == gravity, (got, gravity)


def test_string_to_decimal_keeps_all_eighteen_digits():
    """The reason this is not composed from the float64 parser: a double cannot
    hold 18 significant digits, so a detour through one loses the low end."""
    got = _col(
        "SELECT CAST(CAST(id AS VARCHAR) || '23456789012345678' AS DECIMAL(18,0)) AS x "
        "FROM $planets"
    )
    assert got[0] == Decimal("123456789012345678"), got[0]
    assert got[8] == Decimal("923456789012345678"), got[8]


def test_string_to_decimal_both_physical_tiers():
    """p>18 crosses into the int128 tier and must keep every digit.

    Read under a widened decimal context ON PURPOSE. opteryx/__init__.py pins
    `getcontext().prec = 28` globally, so a DECIMAL128 value carrying more than 28
    significant digits is rounded by the PYTHON conversion on the way out — 35
    digits come back as 1.234567890123456789012345679E+34. That is a boundary
    defect (the engine stores 38 digits; the readback caps at 28), not a property
    of this kernel, and this test is here to pin the kernel. Reading it under a
    context wide enough to represent the value is what separates the two: if the
    kernel itself lost digits, no context would bring them back.
    """
    import decimal

    with decimal.localcontext() as ctx:
        ctx.prec = 60
        got = _col(
            "SELECT CAST(CAST(id AS VARCHAR) || "
            "'2345678901234567890123456789012345' AS DECIMAL(38,0)) AS x FROM $planets"
        )
        assert got[0] == Decimal("12345678901234567890123456789012345"), got[0]
        assert got[8] == Decimal("92345678901234567890123456789012345"), got[8]


def test_string_to_decimal_rescales_to_the_declared_scale():
    """Trailing zeros re-pad silently — they drop no digits."""
    got = _col("SELECT CAST(CAST(gravity AS VARCHAR) AS DECIMAL(10,4)) AS x FROM $planets")
    gravity = _col("SELECT gravity AS x FROM $planets")
    assert got == gravity, (got, gravity)
    assert got[0] == Decimal("3.7"), got[0]


def test_string_to_decimal_preserves_nulls():
    got = _col(
        "SELECT CAST(CASE WHEN id > 3 THEN CAST(id AS VARCHAR) END AS DECIMAL(5,1)) AS x "
        "FROM $planets"
    )
    assert got[:3] == [None, None, None], got[:3]
    assert got[3:] == [Decimal(i) for i in range(4, 10)], got[3:]


def test_string_to_decimal_excess_scale_is_loud():
    """A declared scale is a contract: digits that would be DROPPED fail loud
    rather than being silently rounded away."""
    with pytest.raises(Exception):
        _col("SELECT CAST(CAST(gravity AS VARCHAR) AS DECIMAL(10,0)) AS x FROM $planets")


def test_string_to_decimal_excess_scale_under_try_cast_is_null():
    got = _col("SELECT TRY_CAST(CAST(gravity AS VARCHAR) AS DECIMAL(10,0)) AS x FROM $planets")
    gravity = _col("SELECT gravity AS x FROM $planets")
    # Whole-number gravities convert exactly (a dropped trailing zero loses nothing);
    # anything with a real fractional digit goes NULL rather than being rounded.
    expected = [
        g if g is not None and g == g.to_integral_value() else None for g in gravity
    ]
    assert got == expected, (got, expected)
    assert any(v is None for v in got), got       # the scale contract actually bit
    assert any(v is not None for v in got), got   # ...and did not swallow everything


def test_string_to_decimal_malformed_is_loud():
    """$planets.name is text that is not a number."""
    with pytest.raises(Exception):
        _col("SELECT CAST(name AS DECIMAL(10,2)) AS x FROM $planets")


def test_string_to_decimal_malformed_under_try_cast_is_null():
    got = _col("SELECT TRY_CAST(name AS DECIMAL(10,2)) AS x FROM $planets")
    assert got == [None] * 9, got


def test_string_to_decimal_overflow_is_loud():
    """A magnitude past the declared precision never wraps."""
    with pytest.raises(Exception):
        _col(
            "SELECT CAST(CAST(id AS VARCHAR) || '00000' AS DECIMAL(3,0)) AS x FROM $planets"
        )


def test_string_to_decimal_works_in_a_predicate():
    assert _col(
        "SELECT COUNT(*) AS x FROM $planets "
        "WHERE CAST(CAST(gravity AS VARCHAR) AS DECIMAL(4,1)) > 5"
    ) == [6]


# --- literal / column parity ---------------------------------------------------
#
# The literal path folds in Python (_build_decimal_closure); the column path runs
# the kernel. Same SQL, same answer — otherwise the accepted syntax has drifted.

_PARITY_TEXTS = [
    ("1.25", "DECIMAL(10,2)", Decimal("1.25")),
    ("-1.25", "DECIMAL(10,2)", Decimal("-1.25")),
    ("+1.25", "DECIMAL(10,2)", Decimal("1.25")),
    (" 1.25 ", "DECIMAL(10,2)", Decimal("1.25")),
    ("1.250", "DECIMAL(10,2)", Decimal("1.25")),
    ("1.", "DECIMAL(10,2)", Decimal("1")),
    (".5", "DECIMAL(10,2)", Decimal("0.5")),
    ("1e5", "DECIMAL(10,2)", Decimal("100000")),
    ("1E5", "DECIMAL(10,2)", Decimal("100000")),
    ("1e+5", "DECIMAL(10,2)", Decimal("100000")),
    ("125e-2", "DECIMAL(10,2)", Decimal("1.25")),
    ("0", "DECIMAL(10,2)", Decimal("0")),
    ("-0", "DECIMAL(10,2)", Decimal("0")),
]


@pytest.mark.parametrize("text,target,expected", _PARITY_TEXTS)
def test_string_to_decimal_literal_and_column_agree(text, target, expected):
    literal = _col(f"SELECT CAST('{text}' AS {target}) AS x")
    # A single-row string COLUMN carrying the same text — the kernel path.
    column = _col(
        f"SELECT CAST(s AS {target}) AS x FROM "
        f"(SELECT '{text}' || CASE WHEN id < 0 THEN 'x' ELSE '' END AS s "
        f"FROM $planets WHERE id = 1) AS t"
    )
    assert literal[0] == expected, (text, literal[0])
    assert column[0] == expected, (text, column[0])
    assert literal[0] == column[0], (text, literal[0], column[0])


@pytest.mark.parametrize("text", ["abc", "", "1.2.3", "1e", "--1", "1e5x", "Infinity", "NaN"])
def test_string_to_decimal_rejects_the_same_text_the_literal_path_rejects(text):
    """Neither path may quietly accept what the other refuses."""
    with pytest.raises(Exception):
        _col(
            f"SELECT CAST(s AS DECIMAL(10,2)) AS x FROM "
            f"(SELECT '{text}' || CASE WHEN id < 0 THEN 'x' ELSE '' END AS s "
            f"FROM $planets WHERE id = 1) AS t"
        )


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            marks = getattr(fn, "pytestmark", [])
            if marks:
                for mark in marks:
                    for case in mark.args[1]:
                        fn(*(case if isinstance(case, tuple) else (case,)))
            else:
                fn()
            print(f"✅ {name}")
    print("All BOOL/STRING → DECIMAL cast tests passed.")
