"""IPv4 logical type — everything except literal CAST folding.

Literal-cast folding has its own file (test_ipv4_literal_cast.py); this covers
the type system, the catalog round-trip, the rendering surfaces, the `<<=` /
`>>=` containment operators and IP_TRUNC.

The single idea being defended throughout: an IPv4 column IS a uint32, and its
`LogicalCategory` is INTEGER on purpose. That is what makes ordering, grouping,
joining and comparison work with no IPv4-specific code at all. Only rendering
and casting read the descriptor. Several tests below assert the *negative* form
of that — that a plain uint32 is left alone — because the failure mode when
someone keys on the category instead of the descriptor is that every integer
column in the schema silently becomes an address.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from draken.draken_native import DrakenType, LogicalKind, LogicalType
from draken.draken_native import vector_retag_uint32_as_ipv4, vector_uint32_from_sequence
from draken.vectors.vector import Vector
from opteryx.types.logical_type import (
    INT64,
    IPV4,
    UINT32,
    ColumnType,
    LogicalCategory,
    parse_column_type,
    serialize_column_type,
)


def ip(text: str) -> int:
    """Dotted-decimal -> the uint32 the address is. Octet A is bits 31..24."""
    a, b, c, d = (int(part) for part in text.split("."))
    return (a << 24) | (b << 16) | (c << 8) | d


def ipv4_vector(addresses):
    """A draken IPv4 vector (uint32 + IPV4 descriptor) from dotted-decimal text."""
    raw = [None if a is None else ip(a) for a in addresses]
    return Vector(vector_retag_uint32_as_ipv4(vector_uint32_from_sequence(raw)))


def rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        out.extend(morsel.column(morsel.column_names[0]).to_pylist())
    return out


def one(sql):
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        for name in morsel.column_names:
            return morsel.column(name).to_pylist()
    return []


# ---------------------------------------------------------------------------
# Type system
# ---------------------------------------------------------------------------


def test_ipv4_is_uint32_physically():
    assert IPV4.physical == DrakenType.UINT32


def test_ipv4_category_is_integer():
    """Deliberate: ordering/grouping/joins operate on the raw uint32, and dotted-
    decimal order IS unsigned integer order."""
    assert IPV4.category == LogicalCategory.INTEGER


def test_ipv4_is_distinct_from_plain_uint32():
    """Same physical tag, different type. This comparison used to raise TypeError
    because LogicalType.__eq__ refused None."""
    assert IPV4 != UINT32
    assert UINT32 != IPV4
    assert len({IPV4, UINT32, INT64}) == 3


def test_plain_uint32_carries_no_descriptor():
    assert UINT32.logical is None
    assert IPV4.logical is not None
    assert IPV4.logical.kind == LogicalKind.IPV4


def test_ipv4_serializes_as_its_own_name_not_uint32():
    assert str(IPV4) == "IPV4"
    assert str(UINT32) == "UINT32"


@pytest.mark.parametrize("column_type", [IPV4, UINT32, INT64])
def test_column_type_round_trips(column_type):
    assert parse_column_type(serialize_column_type(column_type)) == column_type


def test_uint32_permits_only_the_ipv4_descriptor():
    """The refinable-physical allowance is narrow on purpose — a UINT32 carrying
    a DECIMAL descriptor is nonsense and must not construct."""
    for kind in (LogicalKind.DECIMAL, LogicalKind.TIMESTAMP, LogicalKind.VECTOR):
        with pytest.raises(ValueError):
            ColumnType(DrakenType.UINT32, LogicalType(kind=kind))


def test_non_refinable_physical_still_rejects_a_descriptor():
    with pytest.raises(ValueError):
        ColumnType(DrakenType.INT64, LogicalType(kind=LogicalKind.IPV4))


def test_parameterized_types_still_require_their_descriptor():
    """Relaxing the invariant for IPv4 must not have relaxed it for DECIMAL."""
    with pytest.raises(ValueError):
        ColumnType(DrakenType.DECIMAL)


# ---------------------------------------------------------------------------
# Catalog chain — Parquet has no IP type; the catalog is what declares IPV4.
# ---------------------------------------------------------------------------


def test_catalog_type_string_resolves_to_ipv4():
    """The catalog stores column types as text and passes unknown names through
    verbatim, so 'IPV4' needs no catalog-side change to survive."""
    column_type = parse_column_type("IPV4")
    assert column_type == IPV4
    assert column_type.logical.kind == LogicalKind.IPV4


def test_only_ipv4_is_descriptor_distinguishable_from_other_integers():
    """The scan retag, the cast dispatch and the writers all discriminate on the
    DESCRIPTOR rather than the category, because the category cannot tell these
    three apart. This asserts the property they rely on — it does NOT exercise
    the scan itself, which needs a catalog-backed dataset (see the module note
    in parquet_read.pyx)."""
    integers = [("addr", IPV4), ("plain_u32", UINT32), ("n", INT64)]
    assert {name for name, ct in integers if ct.category == LogicalCategory.INTEGER} == {
        "addr",
        "plain_u32",
        "n",
    }
    assert {
        name
        for name, ct in integers
        if ct.logical is not None and ct.logical.kind == LogicalKind.IPV4
    } == {"addr"}


def test_catalog_connector_preserves_ipv4_through_schema_normalization():
    """The catalog connector rebuilds column types from LogicalCategory, and IPv4's
    category is INTEGER — so a column the catalog declares IPV4 came back out as
    plain INT64 with the descriptor destroyed, and the scan retag could never fire.
    Recovered from the raw type name instead. DECIMAL and ARRAY are special-cased
    in the same place for the same reason: the category round-trip is lossy for any
    type carrying what the category cannot hold."""
    from opteryx.connectors.opteryx_connector import OpteryxTable

    class _Col:
        def __init__(self, **kw):
            self.__dict__.update(kw)
            self.element_type = kw.get("element_type")

    class _Schema:
        name = "t"
        columns = [
            _Col(name="addr", type="IPV4"),
            _Col(name="n", type="INTEGER"),
            _Col(name="ts", type="TIMESTAMP"),
        ]

    by_name = {c.name: c.column_type for c in OpteryxTable._normalize_schema(_Schema(), "t").columns}
    assert by_name["addr"] == IPV4
    assert by_name["addr"].physical == DrakenType.UINT32
    assert by_name["addr"].logical.kind == LogicalKind.IPV4
    # neighbours unaffected
    assert by_name["n"] == INT64
    assert by_name["ts"].logical.kind == LogicalKind.TIMESTAMP


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


TICKET_EXAMPLES = [(0, "0.0.0.0"), (3232235777, "192.168.1.1"), (4294967295, "255.255.255.255")]


@pytest.mark.parametrize("stored,rendered", TICKET_EXAMPLES)
def test_readback_renders_dotted_decimal(stored, rendered):
    vector = Vector(vector_retag_uint32_as_ipv4(vector_uint32_from_sequence([stored])))
    assert vector.to_pylist() == [rendered]
    assert vector[0] == rendered


def test_readback_preserves_nulls():
    assert ipv4_vector(["10.0.0.1", None]).to_pylist() == ["10.0.0.1", None]


def test_min_max_render_as_addresses():
    """MIN of an address column is an address. If only __getitem__ rendered, the
    same column would read back two different ways."""
    vector = ipv4_vector(["10.0.0.1", "192.168.1.1", "0.0.0.0"])
    assert vector.min() == "0.0.0.0"
    assert vector.max() == "192.168.1.1"


def test_json_render_is_dotted_decimal():
    assert ipv4_vector(["192.168.1.1", None])._to_json() == b'["192.168.1.1",null]'


def test_plain_uint32_still_renders_numerically():
    """The negative case: a uint32 with no descriptor is an integer everywhere."""
    vector = Vector(vector_uint32_from_sequence([3232235777, None]))
    assert vector.to_pylist() == [3232235777, None]
    assert vector._to_json() == b"[3232235777,null]"


# ---------------------------------------------------------------------------
# CIDR containment — `<<=` and `>>=`
# ---------------------------------------------------------------------------


ADDRESSES = "SELECT '10.0.0.1' AS a UNION ALL SELECT '10.255.255.254' UNION ALL SELECT '192.168.1.1'"


@pytest.mark.parametrize(
    "cidr,expected",
    [
        ("10.0.0.0/8", 2),
        ("192.168.1.0/24", 1),
        ("0.0.0.0/0", 3),
        ("192.168.1.1/32", 1),
        ("172.16.0.0/12", 0),
    ],
)
def test_contained_by_counts(cidr, expected):
    sql = (
        f"SELECT COUNT(*) AS c FROM ({ADDRESSES}) AS t "
        f"WHERE CAST(a AS IPV4) <<= '{cidr}'"
    )
    assert one(sql) == [expected]


def test_contains_is_the_mirror_of_contained_by():
    left = one(
        f"SELECT COUNT(*) AS c FROM ({ADDRESSES}) AS t WHERE CAST(a AS IPV4) <<= '10.0.0.0/8'"
    )
    right = one(
        f"SELECT COUNT(*) AS c FROM ({ADDRESSES}) AS t WHERE '10.0.0.0/8' >>= CAST(a AS IPV4)"
    )
    assert left == right == [2]


def test_host_bits_in_the_cidr_base_are_masked_off():
    """`10.1.2.3/8` means the 10/8 network, not a network starting at 10.1.2.3."""
    sql = (
        f"SELECT COUNT(*) AS c FROM ({ADDRESSES}) AS t WHERE CAST(a AS IPV4) <<= '10.1.2.3/8'"
    )
    assert one(sql) == [2]


def test_containment_does_not_swallow_a_following_and():
    """`<<=` parses with proper precedence; a trailing AND binds outside it."""
    sql = (
        f"SELECT COUNT(*) AS c FROM ({ADDRESSES}) AS t "
        f"WHERE CAST(a AS IPV4) <<= '10.0.0.0/8' AND a = '10.0.0.1'"
    )
    assert one(sql) == [1]


@pytest.mark.parametrize(
    "cidr", ["10.0.0.0", "10.0.0.0/33", "10.0.0.0/x", "999.1.1.1/8", "10.0.0.0/", "010.0.0.0/8"]
)
def test_malformed_cidr_is_rejected(cidr):
    """Including a prefix-less address: silently treating it as /32 would turn a
    typo into a single-host match instead of an error."""
    with pytest.raises(Exception):
        one(f"SELECT COUNT(*) AS c FROM ({ADDRESSES}) AS t WHERE CAST(a AS IPV4) <<= '{cidr}'")


def test_containment_on_a_non_address_column_fails_loud():
    with pytest.raises(Exception):
        one("SELECT COUNT(*) AS c FROM $planets WHERE name <<= '10.0.0.0/8'")


# ---------------------------------------------------------------------------
# IP_TRUNC
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "address,prefix,expected",
    [
        ("192.168.1.1", 24, "192.168.1.0"),
        ("192.168.1.1", 16, "192.168.0.0"),
        ("10.1.2.3", 8, "10.0.0.0"),
        ("1.2.3.4", 32, "1.2.3.4"),
        ("1.2.3.4", 0, "0.0.0.0"),
        ("255.255.255.255", 24, "255.255.255.0"),
    ],
)
def test_ip_trunc(address, prefix, expected):
    assert one(f"SELECT IP_TRUNC(CAST('{address}' AS IPV4), {prefix}) AS n") == [expected]


def test_ip_trunc_over_a_column():
    sql = (
        f"SELECT IP_TRUNC(CAST(a AS IPV4), 24) AS n FROM ({ADDRESSES}) AS t ORDER BY n"
    )
    assert one(sql) == ["10.0.0.0", "10.255.255.0", "192.168.1.0"]


@pytest.mark.parametrize("prefix", [33, -1, 64])
def test_ip_trunc_rejects_out_of_range_prefix(prefix):
    """Rejected, not clamped: a /33 is a mistake in the query, and treating it as
    /32 would return plausible rows answering a different question."""
    with pytest.raises(Exception):
        one(f"SELECT IP_TRUNC(CAST('1.2.3.4' AS IPV4), {prefix}) AS n")


def test_ip_trunc_composes_with_containment():
    sql = (
        f"SELECT COUNT(*) AS c FROM ({ADDRESSES}) AS t "
        f"WHERE IP_TRUNC(CAST(a AS IPV4), 8) <<= '10.0.0.0/8'"
    )
    assert one(sql) == [2]


# ---------------------------------------------------------------------------
# Ordering / grouping / comparison run on the integer — the acceptance criterion
# that IPv4 needs no type-specific support in those paths.
# ---------------------------------------------------------------------------


def test_ordering_is_numeric_and_therefore_correct_for_addresses():
    """9.0.0.0 < 10.0.0.0 < 100.0.0.0 numerically AND as addresses; lexicographic
    string ordering would put 100 before 9."""
    src = "SELECT '10.0.0.0' AS a UNION ALL SELECT '9.0.0.0' UNION ALL SELECT '100.0.0.0'"
    sql = f"SELECT CAST(a AS IPV4) AS ip FROM ({src}) AS t ORDER BY ip"
    assert one(sql) == ["9.0.0.0", "10.0.0.0", "100.0.0.0"]


def test_grouping_collapses_equal_addresses():
    src = (
        "SELECT '10.0.0.1' AS a UNION ALL SELECT '10.0.0.1' UNION ALL SELECT '10.0.0.2'"
    )
    sql = f"SELECT COUNT(*) AS c FROM ({src}) AS t GROUP BY CAST(a AS IPV4) ORDER BY c"
    assert one(sql) == [1, 2]


def test_comparison_operators_work_on_addresses():
    src = "SELECT '10.0.0.1' AS a UNION ALL SELECT '192.168.1.1'"
    sql = (
        f"SELECT COUNT(*) AS c FROM ({src}) AS t "
        f"WHERE CAST(a AS IPV4) > CAST('11.0.0.0' AS IPV4)"
    )
    assert one(sql) == [1]


# ---------------------------------------------------------------------------
# IPV4 -> VARCHAR / BLOB — the second half of the ticket's "IPv4 <-> VARCHAR".
#
# One renderer serves every surface: draken::ipv4::format (draken/core/ipv4.h)
# backs to_pylist, the JSON/text writers, the runtime kernel
# draken_cast_ipv4_to_string, and — through the draken.ipv4_format binding — the
# plan-time literal fold. The tests below pin that they all agree.
#
# The failure mode this section exists to catch is NOT an exception. An IPv4
# column and a plain unsigned column are BOTH physically DRAKEN_UINT32, and a
# DrakenVector carries no descriptor, so the kernel cannot tell them apart: pick
# the wrong one at bind time and '192.168.1.1' silently becomes '3232235777' (or
# the reverse). Every render assertion here therefore has a negative twin.
# ---------------------------------------------------------------------------


RENDER_EXAMPLES = ["0.0.0.0", "10.0.0.1", "192.168.1.1", "255.255.255.255"]


@pytest.mark.parametrize("address", RENDER_EXAMPLES)
def test_literal_ipv4_to_varchar_renders_dotted_decimal(address):
    # Folded at plan time. '255.255.255.255' is 15 bytes, past STR_INLINE_MAX, so
    # the set spans both the inline and the arena slot forms.
    assert rows(f"SELECT CAST(CAST('{address}' AS IPV4) AS VARCHAR)") == [address]


@pytest.mark.parametrize("address", RENDER_EXAMPLES)
def test_column_ipv4_to_varchar_renders_dotted_decimal(address):
    sql = f"SELECT CAST(CAST(a AS IPV4) AS VARCHAR) FROM (SELECT '{address}' AS a) AS t"
    assert rows(sql) == [address]


def test_literal_and_column_ipv4_to_varchar_agree():
    """The whole point of routing both through draken::ipv4::format. A planner and
    an engine printing the same address differently is a wrong answer."""
    for address in RENDER_EXAMPLES:
        literal = rows(f"SELECT CAST(CAST('{address}' AS IPV4) AS VARCHAR)")
        column = rows(
            f"SELECT CAST(CAST(a AS IPV4) AS VARCHAR) FROM (SELECT '{address}' AS a) AS t"
        )
        assert literal == column == [address]


def test_round_trip_through_ipv4_returns_the_original_text():
    src = " UNION ALL ".join(f"SELECT '{a}'" for a in RENDER_EXAMPLES[1:])
    sql = (
        "SELECT CAST(CAST(a AS IPV4) AS VARCHAR) = a AS eq FROM "
        f"(SELECT '{RENDER_EXAMPLES[0]}' AS a UNION ALL {src}) AS t"
    )
    assert rows(sql) == [True] * len(RENDER_EXAMPLES)


def test_ipv4_to_varchar_preserves_nulls():
    # A cast that cannot fail must not introduce or drop a null either.
    sql = (
        "SELECT CAST(CAST(a AS IPV4) AS VARCHAR) AS s FROM "
        "(SELECT '10.0.0.1' AS a UNION ALL SELECT NULL UNION ALL SELECT '0.0.0.0') AS t "
        "ORDER BY s"
    )
    assert rows(sql) == [None, "0.0.0.0", "10.0.0.1"]


def test_ipv4_to_varchar_over_repeated_values():
    """A repeated address may reach the kernel dictionary-encoded, where it renders
    the K physical slots and keeps the selection. A shape-dependent answer here
    would be the `ptr.data == NULL` class of bug (CLAUDE.md §11)."""
    src = (
        "SELECT '10.0.0.1' AS a UNION ALL SELECT '10.0.0.1' "
        "UNION ALL SELECT '192.168.1.1' UNION ALL SELECT '10.0.0.1'"
    )
    sql = f"SELECT CAST(CAST(a AS IPV4) AS VARCHAR) AS s FROM ({src}) AS t ORDER BY s"
    assert rows(sql) == ["10.0.0.1", "10.0.0.1", "10.0.0.1", "192.168.1.1"]


@pytest.mark.parametrize("address", ["192.168.1.1", "255.255.255.255"])
def test_ipv4_to_blob_is_the_same_bytes(address):
    """VARBINARY is the VARCHAR bytes with a different tag. Routing a BLOB target
    at the `_to_string` kernel would hand back a VARCHAR-tagged result."""
    expected = [address.encode("utf-8")]
    assert rows(f"SELECT CAST(CAST('{address}' AS IPV4) AS BLOB)") == expected
    assert (
        rows(f"SELECT CAST(CAST(a AS IPV4) AS BLOB) FROM (SELECT '{address}' AS a) AS t")
        == expected
    )


def test_try_cast_ipv4_to_varchar_renders_too():
    """TRY_CAST is not a different conversion — it only changes what a BAD VALUE
    does, and this cast has none. It also takes the closure path rather than the
    C-native one (`_c_native_cast` declines safe=True), so this pins that the
    nanobind wrapper and the C-ABI kernel agree."""
    assert rows("SELECT TRY_CAST(CAST('192.168.1.1' AS IPV4) AS VARCHAR)") == ["192.168.1.1"]
    assert rows(
        "SELECT TRY_CAST(CAST(a AS IPV4) AS VARCHAR) FROM (SELECT '192.168.1.1' AS a) AS t"
    ) == ["192.168.1.1"]


# --- the negative twin: a descriptor-less uint32 is an integer ---------------


def test_plain_uint32_column_to_varchar_renders_the_integer():
    """THE discriminant test. This column is physically UINT32, exactly like an
    address column, and differs only in carrying no IPV4 descriptor. It must
    render 3232235777 — if it renders '192.168.1.1', the bind-time discriminant in
    casts.pyx is keying on the physical type instead of the LogicalKind."""
    sql = "SELECT CAST(CAST(a AS UINT32) AS VARCHAR) FROM (SELECT 3232235777 AS a) AS t"
    assert rows(sql) == ["3232235777"]


def test_plain_uint32_literal_to_varchar_renders_the_integer():
    assert rows("SELECT CAST(CAST(3232235777 AS UINT32) AS VARCHAR)") == ["3232235777"]
    assert rows("SELECT CAST(3232235777 AS VARCHAR)") == ["3232235777"]


def test_plain_uint32_to_varchar_preserves_nulls():
    sql = (
        "SELECT CAST(CAST(a AS UINT32) AS VARCHAR) AS s FROM "
        "(SELECT 3232235777 AS a UNION ALL SELECT NULL) AS t ORDER BY s"
    )
    assert rows(sql) == [None, "3232235777"]


def test_uint64_above_int64_max_formats_unsigned():
    """The unsigned family gets its own kernel rather than borrowing the signed
    one: 0xFFFFFFFFFFFFFFFF through the signed path prints '-1'."""
    assert rows("SELECT CAST(CAST(18446744073709551615 AS UINT64) AS VARCHAR)") == [
        "18446744073709551615"
    ]


def test_ipv4_to_integer_still_yields_the_raw_address():
    """The raw uint32 IS the value, so this pairing must keep matching the unsigned
    arms — which is why the IPv4 discriminant is a separate argument to the
    resolvers rather than a synthetic 'IPV4' source name."""
    assert rows("SELECT CAST(CAST('192.168.1.1' AS IPV4) AS UINT32)") == [3232235777]
    assert rows(
        "SELECT CAST(CAST(a AS IPV4) AS UINT32) FROM (SELECT '192.168.1.1' AS a) AS t"
    ) == [3232235777]


def test_ipv4_format_binding_matches_the_vector_renderer():
    """The planner's scalar renderer and draken's own vector readback are the same
    writer; assert it directly rather than only through SQL."""
    from draken.draken_native import ipv4_format

    for address in RENDER_EXAMPLES:
        assert ipv4_format(ip(address)) == address
    assert ipv4_vector(RENDER_EXAMPLES).to_pylist() == [
        ipv4_format(ip(a)) for a in RENDER_EXAMPLES
    ]


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
