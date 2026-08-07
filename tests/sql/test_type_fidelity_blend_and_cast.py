"""Regression tests for type fidelity in CASE/COALESCE blends and literal casts.

Three defects, all found together while fixing IPv4 constant folding, all the
same shape: a private copy of a rule that had drifted from the canonical one.

1. Fixed-element WIDTH was duplicated. `case_helpers.pyx` kept a table listing
   only the signed types, so a folded CASE sized and memcpy'd every UNSIGNED
   result at 1 byte — CAST(70000 AS UINT32) came back as 112, its low byte.
   `function_kernels.cpp` kept another that omitted unsigned entirely, so the
   runtime blend refused them outright ("unsupported branch type"). Both now
   call draken_type_fixed_itemsize (draken/core/buffers.h).

2. `find_compatible_type` collapsed identical inputs to their LogicalCategory's
   canonical ColumnType. IPV4's category is INTEGER, so COALESCE(ipv4, ipv4)
   resolved to INT64 — descriptor lost AND the declared physical type no longer
   matching the UINT32 the kernel produces.

3. `_parse_blob` fell through to bytes(value), whose int overload builds a zero
   buffer of that LENGTH: CAST(42 AS VARBINARY) folded to 42 zero bytes, and
   CAST(3232235777 AS VARBINARY) allocated ~3GB at plan time.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from opteryx.exceptions import IncompatibleTypesError
from opteryx.types.logical_type import FLOAT64
from opteryx.types.logical_type import INT32
from opteryx.types.logical_type import INT64
from opteryx.types.logical_type import IPV4
from opteryx.types.logical_type import NULL
from opteryx.types.logical_type import UINT8
from opteryx.types.logical_type import UINT16
from opteryx.types.logical_type import UINT32
from opteryx.types.logical_type import UINT64
from opteryx.types.logical_type import VARCHAR
from opteryx.types.logical_type import find_compatible_type

# A two-row source, so blends run on the RUNTIME path (a single all-literal
# expression would be constant-folded before it ever reaches a kernel).
TWO_ROWS = "(SELECT '192.168.1.1' AS a UNION ALL SELECT '10.0.0.1') AS t"


def _values(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        column = morsel.column(morsel.column_names[0])
        out.extend(column.to_pylist())
    return out


# ---------------------------------------------------------------------------
# 1. Fixed-element width — unsigned branches in a CASE blend.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", [300, 70000, 4294967295])
@pytest.mark.parametrize("width", ["UINT32", "UINT64"])
def test_folded_case_preserves_wide_unsigned(value, width):
    # Sized at 1 byte, 300 came back as 44 and 70000 as 112.
    sql = f"SELECT CASE WHEN true THEN CAST({value} AS {width}) ELSE CAST(1 AS {width}) END AS v"
    assert _values(sql) == [value]


@pytest.mark.parametrize("width", ["UINT32", "UINT64"])
def test_runtime_case_accepts_unsigned_branches(width):
    # The runtime blend refused these outright: "draken_if_then_else: unsupported
    # branch type", because its width table had no unsigned entries at all.
    sql = (
        f"SELECT CASE WHEN a IS NOT NULL THEN CAST(70000 AS {width}) "
        f"ELSE CAST(1 AS {width}) END AS v FROM {TWO_ROWS}"
    )
    assert _values(sql) == [70000, 70000]


def test_folded_case_signed_and_float_unaffected():
    assert _values("SELECT CASE WHEN true THEN 70000 ELSE 1 END AS v") == [70000]
    assert _values("SELECT CASE WHEN true THEN 1.5 ELSE 2.5 END AS v") == [1.5]
    assert _values("SELECT CASE WHEN true THEN 'a' ELSE 'b' END AS v") == ["a"]
    assert _values("SELECT CASE WHEN true THEN true ELSE false END AS v") == [True]


def test_case_over_ipv4_renders_dotted_decimal_on_both_paths():
    # IPv4 is UINT32, so it rode both width bugs down.
    folded = "SELECT CASE WHEN true THEN CAST('192.168.1.1' AS IPV4) ELSE CAST('10.0.0.1' AS IPV4) END AS ip"
    assert _values(folded) == ["192.168.1.1"]
    runtime = (
        f"SELECT CASE WHEN a = '10.0.0.1' THEN CAST(a AS IPV4) "
        f"ELSE CAST('0.0.0.0' AS IPV4) END AS ip FROM {TWO_ROWS}"
    )
    assert sorted(_values(runtime)) == ["0.0.0.0", "10.0.0.1"]


# ---------------------------------------------------------------------------
# 2. find_compatible_type — identical inputs pass through.
# ---------------------------------------------------------------------------


def test_identical_ipv4_inputs_keep_the_descriptor():
    result = find_compatible_type([IPV4, IPV4])
    assert result.physical == DrakenType.UINT32
    assert result.logical is not None
    assert result.logical.kind == LogicalKind.IPV4


def test_identical_unsigned_inputs_keep_their_physical_type():
    # Collapsing to INT64 declared a type the blend kernel never produces.
    assert find_compatible_type([UINT32, UINT32]) == UINT32


def test_narrow_signed_ints_still_widen_to_int64():
    # Deliberate exception: the kernel's nc_canon_fixed widens INT8/16/32, so the
    # declared type must follow the data.
    assert find_compatible_type([INT32, INT32]) == INT64


def test_identical_wide_and_string_inputs_pass_through():
    assert find_compatible_type([INT64, INT64]) == INT64
    assert find_compatible_type([VARCHAR, VARCHAR]) == VARCHAR


def test_mixed_and_null_blends_unchanged():
    assert find_compatible_type([INT32, FLOAT64]) == FLOAT64
    assert find_compatible_type([NULL, INT64]) == INT64
    assert find_compatible_type([]) is None


@pytest.mark.parametrize("fn", ["COALESCE({0}, {1})", "IFNULL({0}, {1})"])
def test_null_conditionals_keep_ipv4(fn):
    literal = fn.format("CAST('192.168.1.1' AS IPV4)", "CAST('10.0.0.1' AS IPV4)")
    assert _values(f"SELECT {literal} AS ip") == ["192.168.1.1"]
    column = fn.format("CAST(a AS IPV4)", "CAST(a AS IPV4)")
    assert sorted(_values(f"SELECT {column} AS ip FROM {TWO_ROWS}")) == [
        "10.0.0.1",
        "192.168.1.1",
    ]


def test_iif_keeps_ipv4():
    sql = f"SELECT IIF(a IS NOT NULL, CAST(a AS IPV4), CAST(a AS IPV4)) AS ip FROM {TWO_ROWS}"
    assert sorted(_values(sql)) == ["10.0.0.1", "192.168.1.1"]


def test_coalesce_over_unsigned_keeps_the_value():
    assert _values("SELECT COALESCE(CAST(70000 AS UINT32), CAST(1 AS UINT32)) AS v") == [70000]


def test_coalesce_ordinary_types_unaffected():
    assert _values("SELECT COALESCE(1, 2) AS v") == [1]
    assert _values("SELECT COALESCE('a', 'b') AS v") == ["a"]
    assert _values("SELECT COALESCE(1.5, 2) AS v") == [1.5]
    assert _values("SELECT COALESCE(NULL, 3) AS v") == [3]


# ---------------------------------------------------------------------------
# 2b. Mixed unsigned widths blend to the WIDEST unsigned, never through INT64.
#
# COALESCE(uint32, uint64) used to be refused at bind time: the declared type
# came out INT64 (every unsigned's category is INTEGER) and the kernel refused
# the pair. Routing through INT64 is not an option — it cannot hold the top half
# of UINT64 — so both sides now widen unsigned->unsigned, which is exact.
# ---------------------------------------------------------------------------

U64_MAX = 18446744073709551615
U32_MAX = 4294967295

# ORDER BY z: UNION ALL does not promise an order, and these assert per-row which
# BRANCH was taken, so the rows have to be pinned.
TWO_ROWS_ORDERED = "(SELECT 1 AS z UNION ALL SELECT 2) AS t"


def test_find_compatible_type_widest_unsigned():
    assert find_compatible_type([UINT32, UINT64]) == UINT64
    assert find_compatible_type([UINT8, UINT32]) == UINT32
    assert find_compatible_type([UINT8, UINT16, UINT64]) == UINT64
    assert find_compatible_type([UINT64, UINT8]) == UINT64


def test_signed_unsigned_mix_still_unpromotable():
    # No fixed-width type holds both negatives and the top half of UINT64, so
    # this must stay refused rather than pick a side to corrupt.
    assert find_compatible_type([INT32, UINT64]) == INT64
    with pytest.raises(IncompatibleTypesError):
        _values("SELECT COALESCE(CAST(5 AS INTEGER), CAST(9 AS UINT64)) AS v")


def test_ipv4_does_not_widen_into_a_plain_unsigned():
    # An IPv4 is a UINT32, but blending an address with a bare integer is a
    # category error, not a widening — descriptor-bearing types are excluded.
    assert find_compatible_type([IPV4, UINT64]) != UINT64
    with pytest.raises(IncompatibleTypesError):
        _values(
            "SELECT COALESCE(CAST(a AS IPV4), CAST(5 AS UINT64)) AS v "
            "FROM (SELECT '10.0.0.1' AS a) AS t"
        )


@pytest.mark.parametrize(
    "expression",
    [
        "COALESCE(CAST(70000 AS UINT32), CAST(5 AS UINT64))",
        "COALESCE(CAST(70000 AS UINT64), CAST(5 AS UINT32))",
        "COALESCE(CAST(70000 AS UINT32), CAST(5 AS UINT8))",
        "IFNULL(CAST(70000 AS UINT32), CAST(5 AS UINT64))",
    ],
)
def test_mixed_unsigned_null_conditionals_run(expression):
    assert _values(f"SELECT {expression} AS v FROM {TWO_ROWS_ORDERED}") == [70000, 70000]


def test_mixed_unsigned_declares_the_type_it_produces():
    # Declared vs actual: the vector must BE what the plan says it is.
    session = opteryx.session()
    sql = (
        "SELECT COALESCE(CAST(70000 AS UINT32), CAST(5 AS UINT64)) AS v "
        "FROM (SELECT 1 AS z) AS t"
    )
    for morsel in session.execute_to_morsels(sql):
        assert morsel.column("v").type == DrakenType.UINT64


def test_uint64_above_int64_max_survives_a_mixed_blend():
    # The reason the unsigned family promotes among itself: through INT64 this
    # value would come back negative.
    sql = (
        f"SELECT IIF(z=1, CAST({U64_MAX} AS UINT64), CAST({U32_MAX} AS UINT32)) AS v "
        f"FROM {TWO_ROWS_ORDERED} ORDER BY z"
    )
    assert _values(sql) == [U64_MAX, U32_MAX]


@pytest.mark.parametrize(
    "narrow,wide,expected_narrow",
    [
        ("CAST(255 AS UINT8)", f"CAST({U32_MAX} AS UINT32)", 255),
        ("CAST(0 AS UINT8)", f"CAST({U64_MAX} AS UINT64)", 0),
        ("CAST(65535 AS UINT16)", f"CAST({U64_MAX} AS UINT64)", 65535),
    ],
)
def test_narrow_unsigned_branch_is_zero_extended(narrow, wide, expected_narrow):
    # Exercises nc_read_uint: nc_read_int has no unsigned arms and would have
    # written 0 for every one of these.
    sql = f"SELECT IIF(z=1, {narrow}, {wide}) AS v FROM {TWO_ROWS_ORDERED} ORDER BY z"
    result = _values(sql)
    assert result[0] == expected_narrow, result


def test_case_aligns_mixed_unsigned_branches():
    # CASE reaches the same rule through find_compatible_type, which the binder
    # uses to CAST-align the branches before draken_if_then_else sees them.
    sql = (
        f"SELECT CASE WHEN z=1 THEN CAST(255 AS UINT8) ELSE CAST({U32_MAX} AS UINT32) END AS v "
        f"FROM {TWO_ROWS_ORDERED} ORDER BY z"
    )
    assert _values(sql) == [255, U32_MAX]


# ---------------------------------------------------------------------------
# 3. Literal CAST to the binary family.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "literal,expected",
    [
        ("42", b"42"),
        ("3232235777", b"3232235777"),
        ("1.5", b"1.5"),
        ("'abc'", b"abc"),
    ],
)
@pytest.mark.parametrize("target", ["VARBINARY"])
def test_literal_cast_to_binary_encodes_the_rendering(literal, expected, target):
    # bytes(42) is 42 zero bytes, not b'42'; bytes(1.5) raised outright.
    assert _values(f"SELECT CAST({literal} AS {target}) AS v") == [expected]


def test_literal_binary_cast_matches_the_column_path():
    # The column path was always right — b'1', b'2', b'3' — so this is the oracle.
    assert _values("SELECT CAST(id AS VARBINARY) AS v FROM $planets LIMIT 3") == [b"1", b"2", b"3"]
    assert _values("SELECT CAST(1 AS VARBINARY) AS v") == [b"1"]
    assert _values("SELECT CAST(gravity AS VARBINARY) AS v FROM $planets LIMIT 2") == [b"3.7", b"8.9"]
    assert _values("SELECT CAST(3.7 AS VARBINARY) AS v") == [b"3.7"]


def test_large_integer_to_blob_does_not_allocate_by_value():
    # The real hazard: this used to build a ~3GB zero buffer at plan time.
    result = _values("SELECT CAST(3232235777 AS VARBINARY) AS v")
    assert len(result[0]) == 10, len(result[0])


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
