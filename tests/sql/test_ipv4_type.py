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


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
