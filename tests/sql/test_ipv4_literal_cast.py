"""Regression tests for CAST(<literal> AS IPV4) — the plan-time fold.

IPv4 is DRAKEN_UINT32 refined by a LogicalKind.IPV4 descriptor, and its
LogicalCategory is deliberately INTEGER so that ordering, grouping, joins and
comparison all run on the raw uint32. That made the literal fold pick the
INTEGER parser and call `int('192.168.1.1')`: the descriptor, not the category,
is the discriminant.

The fold parses through `draken.draken_native.ipv4_parse`, which is a thin
wrapper over `draken::ipv4::parse` (draken/core/ipv4.h) — the SAME parser the
runtime `draken_cast_string_to_ipv4` kernel runs on a column. A second parser
written in Python would be free to drift on exactly the forms ipv4.h refuses
(inet_aton shorthand, leading zeros, trailing junk, out-of-range octets), and a
planner and an engine disagreeing about what '010.1' means is a security bug in
an ACL-style predicate. The parity tests below pin that agreement.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.draken_native import ipv4_parse
from opteryx.exceptions import SqlError, UnsupportedSyntaxError
from opteryx.planner.logical_planner import logical_planner_builders as builders
from opteryx.third_party import sqloxide

# 192.168.1.1 == 0xC0A80101. Octet A occupies bits 31..24 (draken/core/ipv4.h),
# which is what makes unsigned integer order and IPv4 address order the same.
LOCAL_NET_HOST = 3232235777

# Forms draken::ipv4::parse deliberately refuses.
STRICT_REJECTIONS = [
    "10.1",  # inet_aton shorthand for 10.0.0.1
    "010.0.0.1",  # leading zero (octal-by-leading-zero)
    "1.2.3.4.5",  # five octets
    "256.0.0.1",  # octet out of range
]


def _fold(sql):
    """Return the folded projection Node for a single-expression SELECT."""
    ast = sqloxide.parse_sql(sql, _dialect="opteryx")
    projection = ast[0]["Query"]["body"]["Select"]["projection"][0]
    expression = projection.get("UnnamedExpr", projection)
    return builders.build(expression)


def _values(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        column = morsel.column(morsel.column_names[0])
        out.extend(column.to_pylist())
    return out


# ---------------------------------------------------------------------------
# Plan-time fold: value AND type move together.
# ---------------------------------------------------------------------------


def test_literal_folds_to_integer_with_ipv4_type():
    # A folded literal whose value is still the dotted-decimal string but whose
    # tag says IPV4 (or whose value is an int tagged VARCHAR) is the literal
    # value/type-tag divergence class of bug — it produces wrong rows later
    # rather than failing. Both halves are asserted here.
    node = _fold("SELECT CAST('192.168.1.1' AS IPV4)")
    assert node.value == LOCAL_NET_HOST, node.value
    assert isinstance(node.value, int), type(node.value)
    assert node.type.physical == DrakenType.UINT32, node.type.physical
    assert node.type.logical is not None
    assert node.type.logical.kind == LogicalKind.IPV4, node.type.logical.kind


def test_try_cast_literal_folds_to_integer_with_ipv4_type():
    node = _fold("SELECT TRY_CAST('192.168.1.1' AS IPV4)")
    assert node.value == LOCAL_NET_HOST
    assert node.type.logical.kind == LogicalKind.IPV4


def test_integer_literal_still_folds_to_ipv4():
    # The int → IPV4 fold already worked (the INTEGER parser is correct for an
    # int input); this pins that the string branch did not displace it.
    node = _fold("SELECT CAST(3232235777 AS IPV4)")
    assert node.value == LOCAL_NET_HOST
    assert node.type.logical.kind == LogicalKind.IPV4


# ---------------------------------------------------------------------------
# End to end: a folded literal renders as dotted-decimal.
# ---------------------------------------------------------------------------


def test_literal_renders_as_dotted_decimal():
    assert _values("SELECT CAST('192.168.1.1' AS IPV4)") == ["192.168.1.1"]
    assert _values("SELECT CAST('10.0.0.1' AS IPV4) AS ip") == ["10.0.0.1"]
    assert _values("SELECT TRY_CAST('192.168.1.1' AS IPV4)") == ["192.168.1.1"]


def test_integer_literal_renders_as_dotted_decimal():
    assert _values("SELECT CAST(3232235777 AS IPV4)") == ["192.168.1.1"]


# ---------------------------------------------------------------------------
# Failure behaviour: CAST raises, TRY_CAST yields NULL.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("address", STRICT_REJECTIONS)
def test_strict_rejection_raises_under_cast(address):
    with pytest.raises(SqlError):
        _values(f"SELECT CAST('{address}' AS IPV4)")


@pytest.mark.parametrize("address", STRICT_REJECTIONS)
def test_strict_rejection_is_null_under_try_cast(address):
    assert _values(f"SELECT TRY_CAST('{address}' AS IPV4)") == [None]


@pytest.mark.parametrize("address", ["not-an-address", "", "1.2.3", "1.2.3.-4"])
def test_malformed_raises_under_cast(address):
    with pytest.raises(SqlError):
        _values(f"SELECT CAST('{address}' AS IPV4)")


@pytest.mark.parametrize("address", ["not-an-address", "", "1.2.3"])
def test_malformed_is_null_under_try_cast(address):
    assert _values(f"SELECT TRY_CAST('{address}' AS IPV4)") == [None]


# ---------------------------------------------------------------------------
# The column path is untouched, and the two paths agree.
# ---------------------------------------------------------------------------

_TWO_ADDRESSES = (
    "SELECT CAST(a AS IPV4) FROM "
    "(SELECT '192.168.1.1' AS a UNION ALL SELECT '10.0.0.1') AS t"
)


def test_column_cast_unchanged():
    # Sorted because UNION ALL does not promise an order; the content is the point.
    assert sorted(_values(_TWO_ADDRESSES)) == ["10.0.0.1", "192.168.1.1"]


def test_literal_and_column_folds_agree_on_value():
    # The folded constant must compare equal to the kernel's output for the same
    # text — same bits, not merely the same rendering.
    sql = (
        "SELECT CAST('192.168.1.1' AS IPV4) = CAST(a AS IPV4) AS eq FROM "
        "(SELECT '192.168.1.1' AS a UNION ALL SELECT '10.0.0.1') AS t"
    )
    # UNION ALL does not promise an order, so compare as a multiset: exactly one
    # of the two addresses matches the folded constant.
    assert sorted(_values(sql)) == [False, True]


@pytest.mark.parametrize("address", STRICT_REJECTIONS)
def test_column_path_refuses_the_same_forms(address):
    # Parity, the point of sharing the parser: what the planner refuses, the
    # kernel refuses. A form accepted by one and not the other is the security
    # bug ipv4.h's strictness exists to prevent.
    sql = (
        f"SELECT CAST(a AS IPV4) FROM "
        f"(SELECT '{address}' AS a UNION ALL SELECT '10.0.0.1') AS t"
    )
    with pytest.raises(Exception):
        _values(sql)


# ---------------------------------------------------------------------------
# ConstantFoldingStrategy: an all-literal expression that types as IPV4.
#
# These used to raise `vector_attach_logical_type: unsupported LogicalKind for
# attach`. The descriptor is now deliberately NOT attached before the readback,
# because an attached IPv4 vector reads back as dotted-decimal text while a
# folded IPv4 literal is the raw uint32 (see the comment in constant_folding.py).
#
# NOTE: these use addresses whose uint32 value is < 256. A separate, pre-existing
# bug truncates a folded CASE over UINT32 constants to 8 bits (SELECT CASE WHEN
# true THEN CAST(70000 AS UINT32) ELSE CAST(1 AS UINT32) END returns 112), and
# the runtime CASE-over-UINT32 path raises outright. Narrow values dodge both, so
# these tests pin the attach fix and NOT the wider CASE behaviour.
# ---------------------------------------------------------------------------


def test_case_over_ipv4_literals_folds_and_renders():
    sql = "SELECT CASE WHEN true THEN CAST('0.0.0.1' AS IPV4) ELSE CAST('0.0.0.2' AS IPV4) END AS ip"
    assert _values(sql) == ["0.0.0.1"]


def test_case_over_ipv4_literals_else_branch():
    sql = "SELECT CASE WHEN false THEN CAST('0.0.0.1' AS IPV4) ELSE CAST('0.0.0.2' AS IPV4) END AS ip"
    assert _values(sql) == ["0.0.0.2"]


def test_case_over_integer_ipv4_literals_folds():
    sql = "SELECT CASE WHEN true THEN CAST(1 AS IPV4) ELSE CAST(2 AS IPV4) END AS ip"
    assert _values(sql) == ["0.0.0.1"]


# ---------------------------------------------------------------------------
# IPV4 -> string family is refused, matching the engine.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("target", ["VARCHAR", "BLOB"])
def test_ipv4_literal_to_string_family_is_refused(target):
    # The engine refuses this cast on a column ("No native CAST UINT32 →
    # VARCHAR"). The literal path must refuse identically rather than folding to
    # str(uint32) — '3232235777' — or, for BLOB, to bytes(3232235777): a 3GB
    # zero buffer allocated at plan time.
    with pytest.raises(UnsupportedSyntaxError):
        _values(f"SELECT CAST(CAST('192.168.1.1' AS IPV4) AS {target})")


@pytest.mark.parametrize("target", ["VARCHAR", "BLOB"])
def test_ipv4_literal_to_string_family_is_refused_under_try_cast(target):
    # TRY_ does NOT soften this to NULL: an unsupported conversion is not a bad
    # value, and the column path refuses it for TRY_CAST too.
    with pytest.raises(UnsupportedSyntaxError):
        _values(f"SELECT TRY_CAST(CAST('192.168.1.1' AS IPV4) AS {target})")


def test_ipv4_literal_to_integer_still_folds():
    # The raw uint32 IS the value, so this one is supported and agrees with the
    # column path.
    assert _values("SELECT CAST(CAST('192.168.1.1' AS IPV4) AS INTEGER)") == [3232235777]


def test_plain_integer_to_varchar_unaffected():
    assert _values("SELECT CAST(3232235777 AS VARCHAR)") == ["3232235777"]


# ---------------------------------------------------------------------------
# The parser binding itself.
# ---------------------------------------------------------------------------


def test_ipv4_parse_accepts_str_and_bytes():
    assert ipv4_parse("192.168.1.1") == LOCAL_NET_HOST
    assert ipv4_parse(b"192.168.1.1") == LOCAL_NET_HOST
    assert ipv4_parse("0.0.0.0") == 0
    assert ipv4_parse("255.255.255.255") == 0xFFFFFFFF


@pytest.mark.parametrize(
    "address", STRICT_REJECTIONS + [" 1.2.3.4", "1.2.3.4 ", "", "1.2.3.4.", "1..2.3"]
)
def test_ipv4_parse_rejects(address):
    with pytest.raises(ValueError):
        ipv4_parse(address)


def test_ipv4_parse_rejects_non_text():
    with pytest.raises(ValueError):
        ipv4_parse(3232235777)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
