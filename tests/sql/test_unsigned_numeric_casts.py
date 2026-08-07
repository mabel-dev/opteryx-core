"""Unsigned integers as a cast SOURCE: → unsigned (any width), and → FLOAT64.

Unsigned columns used to be nearly one-way. From an unsigned source only INT64,
DECIMAL and text were reachable, which meant:

  1. **No route to floating point at all.** The only way out was INT64, and that
     RAISES above 2^63-1 — so the top half of the UINT64 range could not enter
     float arithmetic by any path.
  2. **No width changes.** `UINT32 → UINT64` is a widening that cannot fail, and
     it was refused, because the draken_cast_integer_to_uint* family takes SIGNED
     sources only and rejects an unsigned one outright.

Both are now kernels. What must stay true: narrowings are RANGE-CHECKED (loud,
never wrapped), and the float conversion is NOT range-checked because it does not
need to be — every uint64 is representable as a double.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx

_SESSION = opteryx.session()

UINT64_MAX = 18446744073709551615


def _col(sql, colname="x"):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        out.extend(morsel.column(colname).to_pylist())
    return out


def _lit(value, source):
    """A one-row unsigned column of `source` width — not a folded literal."""
    return f"(SELECT CAST(a AS {source}) AS u FROM (SELECT {value} AS a) AS i) AS t"


def test_unsigned_widens_to_every_wider_width():
    """Cannot fail, and was refused anyway."""
    for source, target in (("UINT8", "UINT16"), ("UINT8", "UINT64"),
                           ("UINT16", "UINT32"), ("UINT32", "UINT64")):
        sql = f"SELECT CAST(u AS {target}) AS x FROM {_lit(200, source)}"
        assert _col(sql) == [200], (source, target)


def test_unsigned_narrows_when_the_value_fits():
    assert _col(f"SELECT CAST(u AS UINT8) AS x FROM {_lit(200, 'UINT32')}") == [200]
    assert _col(f"SELECT CAST(u AS UINT16) AS x FROM {_lit(60000, 'UINT64')}") == [60000]


def test_unsigned_narrowing_is_range_checked_not_wrapped():
    """300 must NOT become 44. Loud is the only acceptable outcome."""
    with pytest.raises(Exception):
        _col(f"SELECT CAST(u AS UINT8) AS x FROM {_lit(300, 'UINT64')}")
    with pytest.raises(Exception):
        _col(f"SELECT CAST(u AS UINT32) AS x FROM {_lit(4294967296, 'UINT64')}")


def test_unsigned_same_width_is_a_copy():
    for width in ("UINT8", "UINT16", "UINT32", "UINT64"):
        assert _col(f"SELECT CAST(u AS {width}) AS x FROM {_lit(7, width)}") == [7], width


def test_unsigned_to_double():
    assert _col(f"SELECT CAST(u AS FLOAT64) AS x FROM {_lit(5, 'UINT64')}") == [5.0]
    assert _col("SELECT CAST(CAST(id AS UINT32) AS FLOAT64) AS x FROM $planets")[:3] == [
        1.0,
        2.0,
        3.0,
    ]


def test_unsigned_to_double_covers_the_range_int64_cannot():
    """The point of the kernel: UINT64_MAX has no INT64 route (that raises), so
    before this it could not reach float at all. Above 2^53 a double loses low
    bits — that is floating point, not an error."""
    got = _col(f"SELECT CAST(u AS FLOAT64) AS x FROM {_lit(UINT64_MAX, 'UINT64')}")
    assert got == [float(UINT64_MAX)], got
    with pytest.raises(Exception):
        _col(f"SELECT CAST(u AS INTEGER) AS x FROM {_lit(UINT64_MAX, 'UINT64')}")


def test_foreign_unsigned_spellings_are_rejected_never_silently_signed():
    """`UINTEGER` must not come back as a SIGNED INT64.

    The cast target used to be matched by SUBSTRING, and "uinteger" contains
    "integer" — so `CAST(x AS UINTEGER)` silently answered with INT64, turning an
    unsigned request into a signed one with no error. Every spelling here belongs
    to another engine's vocabulary, not ours (we spell these UINT8..UINT64), so
    the only acceptable outcome is a loud refusal.
    """
    for spelling in ("UINTEGER", "UBIGINT", "USMALLINT", "UTINYINT"):
        with pytest.raises(Exception) as err:
            _col(f"SELECT CAST('42' AS {spelling}) AS x")
        assert "Unsupported type for CAST" in str(err.value), (spelling, err.value)
        assert spelling in str(err.value), (spelling, err.value)


def test_uinteger_is_reported_as_a_typo_and_named_canonically():
    """The suggestion engine is a TYPO detector, not intent inference.

    `UINTEGER` is one inserted character away from the INTEGER spelling, so it
    gets a suggestion. The other three are not near-misses of any name we have —
    they are a different type system's words — and guessing which of our widths
    they meant is inference this deliberately does not do.

    The suggestion names INT64, never INTEGER. INTEGER is an implied alias the
    dialect accepts; INT64 is the canonical name `str(ColumnType)` renders and the
    catalog stores, so it is the only one worth teaching.
    """
    with pytest.raises(Exception) as err:
        _col("SELECT CAST('42' AS UINTEGER) AS x")
    assert "did you mean 'INT64'" in str(err.value), err.value
    assert "INTEGER'" not in str(err.value).replace("'UINTEGER'", ""), err.value

    for spelling in ("UBIGINT", "USMALLINT", "UTINYINT"):
        with pytest.raises(Exception) as err:
            _col(f"SELECT CAST('42' AS {spelling}) AS x")
        assert "did you mean" not in str(err.value), (spelling, err.value)


def test_a_name_that_merely_contains_a_type_name_is_not_that_type():
    """MANDATE is not DATE. Cast targets are matched EXACTLY, case-insensitively.

    The matcher used substrings, so any name CONTAINING a type name became that
    type with no error — UPDATEDAT and SANDATE were DATE, SUBSTRUCTURE was STRUCT,
    SUPERVECTOR was VECTOR, MY_INTEGER was INTEGER. SUBARRAY was worse: it matched
    "array" and then indexed the absent Array node, raising a raw KeyError.
    """
    for spelling in (
        "MANDATE", "UPDATEDAT", "SANDATE", "SUBSTRUCTURE", "SUPERVECTOR",
        "MY_INTEGER", "SUBARRAY", "MY_ARRAY", "MYNVARCHAR", "TABOOLI",
    ):
        with pytest.raises(Exception) as err:
            _col(f"SELECT CAST('42' AS {spelling}) AS x")
        # An SqlError naming the type — never a KeyError from indexing an AST node
        # that a substring match wrongly promised was there.
        assert "Unsupported type for CAST" in str(err.value), (spelling, repr(err.value))
        assert spelling in str(err.value), (spelling, repr(err.value))


def test_real_type_names_are_matched_whatever_the_case():
    """Exact must not mean case-sensitive."""
    assert _col("SELECT CAST('42' AS vArChAr) AS x") == ["42"]
    assert _col("SELECT CAST('42' AS UiNt64) AS x") == [42]
    assert _col("SELECT CAST('42' AS nvarchar) AS x") == ["42"]
    assert _col("SELECT CAST('4.5' AS FLOAT64) AS x") == [4.5]


def test_our_own_unsigned_spellings_still_work():
    """The guard above must not have made the real names unreachable."""
    for spelling, expected in (("UINT8", 42), ("UINT16", 42), ("UINT32", 42), ("UINT64", 42)):
        assert _col(f"SELECT CAST('42' AS {spelling}) AS x") == [expected], spelling


def test_int64_the_canonical_name_is_castable():
    """A user must be able to type back the name the engine showed them.

    INT64 is what `str(ColumnType)` renders and what the catalog stores, but it
    was the ONE widthed numeric spelling the dialect rejected — and the typo
    detector answered it with "did you mean 'UINT64'?", pointing a SIGNED request
    at the UNSIGNED type. INTEGER stays accepted as an implied alias.
    """
    assert _col("SELECT CAST('42' AS INT64) AS x") == [42]
    assert _col("SELECT CAST('42' AS INTEGER) AS x") == [42]
    assert _col("SELECT CAST(id AS INT64) * 2 AS x FROM $planets LIMIT 3") == [2, 4, 6]


def test_suggestions_never_name_a_non_canonical_type():
    """We recommend INT64, never INTEGER — accepted is not the same as taught."""
    for spelling, expected in (("INT", "INT64"), ("BIGINT", "INT64"),
                               ("TINYINT", "INT8"), ("SMALLINT", "INT16"),
                               ("REAL", "FLOAT32"), ("DOUBEL", "FLOAT64")):
        with pytest.raises(Exception) as err:
            _col(f"SELECT CAST('1' AS {spelling}) AS x")
        assert f"did you mean '{expected}'" in str(err.value), (spelling, err.value)


def test_unsigned_casts_preserve_nulls():
    sql = (
        "SELECT CAST(u AS {t}) AS x FROM "
        "(SELECT CASE WHEN id > 4 THEN CAST(id AS UINT32) ELSE NULL END AS u "
        "FROM $planets) AS s"
    )
    for target in ("UINT64", "UINT8", "FLOAT64"):
        got = _col(sql.format(t=target))
        assert got[:4] == [None, None, None, None], (target, got[:4])
        assert got[4] is not None, (target, got[4])


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✅ {name}")
    print("All unsigned numeric cast tests passed.")
