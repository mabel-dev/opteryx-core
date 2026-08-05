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
    ARRAY,
    BOOLEAN,
    DECIMAL,
    FLOAT32,
    FLOAT64,
    INT64,
    INTERVAL,
    IPV4,
    NVARCHAR,
    TIMESTAMP,
    UINT32,
    VARBINARY,
    VARCHAR,
    VARIANT,
    ColumnType,
    LogicalCategory,
    column_type_from_vector,
    morsel_column_types,
    parse_column_type,
    serialize_column_type,
    try_parse_column_type,
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


@pytest.mark.parametrize("type_name", ["IPV4", "UINT32", "INT64"])
def test_column_type_round_trips(type_name):
    """Parametrized over the type NAME, and the ColumnType looked up inside the
    test, because pytest retains argvalues and per-item callspec params for the
    whole session. A retained IPV4 ColumnType keeps its Draken LogicalType alive
    past the point nanobind counts live instances during interpreter shutdown,
    which surfaced as `nanobind: leaked 1 instances` on the full suite. Module
    globals hold IPV4 too, but those are cleared in time; pytest's are not."""
    column_type = {"IPV4": IPV4, "UINT32": UINT32, "INT64": INT64}[type_name]
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


def _normalized(*columns):
    """Run a duck-typed catalog schema through the connector's normalizer."""
    from opteryx.connectors.opteryx_connector import OpteryxTable

    class _Col:
        def __init__(self, **kw):
            self.__dict__.update(kw)
            self.element_type = kw.get("element_type")

    class _Schema:
        name = "t"

    schema = _Schema()
    schema.columns = [_Col(**kw) for kw in columns]
    return {
        c.name: c.column_type for c in OpteryxTable._normalize_schema(schema, "t").columns
    }


def test_catalog_connector_preserves_ipv4_through_schema_normalization():
    """The connector used to rebuild column types from LogicalCategory. IPv4's
    category is INTEGER, so a column the catalog declares IPV4 came back out as
    plain INT64 with the descriptor destroyed and the scan retag never firing.
    The stored name is now parsed directly, which is exact for IPV4."""
    by_name = _normalized(
        dict(name="addr", type="IPV4"),
        dict(name="n", type="INTEGER"),
        dict(name="ts", type="TIMESTAMP"),
    )
    assert by_name["addr"] == IPV4
    assert by_name["addr"].physical == DrakenType.UINT32
    assert by_name["addr"].logical.kind == LogicalKind.IPV4
    # neighbours unaffected
    assert by_name["n"] == INT64
    assert by_name["ts"].logical.kind == LogicalKind.TIMESTAMP


def test_schema_normalization_does_not_widen_unsigned_columns():
    """The same category round-trip that destroyed IPv4's descriptor also WIDENED
    every unsigned width — UINT32/UINT64's category is INTEGER too, so both read
    back as signed INT64. Parsing the stored name is exact for these as well, so
    a plain unsigned column stays unsigned and is NOT turned into an address."""
    by_name = _normalized(
        dict(name="u32", type="UINT32"),
        dict(name="u64", type="UINT64"),
        dict(name="i32", type="INT32"),
        dict(name="f32", type="FLOAT32"),
    )
    assert by_name["u32"] == UINT32
    assert by_name["u32"].physical == DrakenType.UINT32
    assert by_name["u32"].logical is None, "a plain uint32 must carry NO descriptor"
    assert by_name["u64"].physical == DrakenType.UINT64
    assert by_name["i32"].physical == DrakenType.INT32
    assert by_name["f32"].physical == DrakenType.FLOAT32


def test_schema_normalization_is_unchanged_for_every_name_the_catalog_stores_today():
    """The catalog persists LogicalCategory names (`_core_type_to_stored` returns
    `column_type.category.name`), so parsing the name first must be a no-op for
    all of them — this change is only allowed to matter once the catalog starts
    storing exact type strings."""
    expected = {
        "INTEGER": INT64, "VARCHAR": VARCHAR, "NVARCHAR": NVARCHAR,
        "VARBINARY": VARBINARY, "BOOLEAN": BOOLEAN, "FLOAT": FLOAT64,
        "TIMESTAMP": TIMESTAMP(), "INTERVAL": INTERVAL, "VARIANT": VARIANT,
    }
    by_name = _normalized(*[dict(name=n, type=n) for n in expected])
    for stored, want in expected.items():
        assert by_name[stored] == want, f"{stored} drifted to {by_name[stored]}"


@pytest.mark.xfail(
    strict=True,
    reason="PRE-EXISTING, unrelated to the parse-first change and NOT fixed here: "
    "the DECIMAL and ARRAY branches of _normalize_schema are unreachable for the "
    "catalog's actual stored format. Both gate on `_ot`, which comes from "
    "_normalize_type(raw) -- and bare 'DECIMAL'/'ARRAY' have never resolved to a "
    "type (they are stored bare, with precision/scale and element-type in SEPARATE "
    "columns), so _ot falls to the VARCHAR default and neither branch fires. A "
    "catalog DECIMAL column therefore reads back as VARCHAR. Verified identical "
    "under the pre-change code. Remove this marker when the read path is fixed.",
)
def test_bare_decimal_and_array_still_read_their_separate_parameter_columns():
    """DECIMAL and ARRAY are stored BARE, with precision/scale and element-type in
    separate catalog columns. Those bare names must NOT parse — they have to fall
    through to the parameter-aware branches, which are the only correct readers."""
    by_name = _normalized(
        dict(name="d", type="DECIMAL", precision=10, scale=2),
        dict(name="a", type="ARRAY", element_type="VARCHAR"),
    )
    assert by_name["d"] == DECIMAL(10, 2)
    assert by_name["a"] == ARRAY(VARCHAR)


def test_parameterized_names_are_parsed_rather_than_falling_through():
    """If the catalog ever stores the full form instead, it must be read exactly —
    and must not reach the separate-parameter branches at all."""
    by_name = _normalized(
        dict(name="d", type="DECIMAL(10, 2)"),
        dict(name="a", type="ARRAY<VARCHAR>"),
    )
    assert by_name["d"] == DECIMAL(10, 2)
    assert by_name["a"] == ARRAY(VARCHAR)


@pytest.mark.parametrize(
    "spelling,physical",
    [
        ("TINYINT", DrakenType.INT8),
        ("SMALLINT", DrakenType.INT16),
        ("REAL", DrakenType.FLOAT32),
        ("FLOAT32", DrakenType.FLOAT32),  # canonical already — no alias needed
        ("INT8", DrakenType.INT8),
        ("UINT32", DrakenType.UINT32),
    ],
)
def test_width_bearing_spellings_resolve_to_the_exact_width(spelling, physical):
    """A catalog storing exact widths may use the natural SQL spelling. Without
    these aliases the name does not parse AT ALL and the reader falls back to its
    VARCHAR default — a narrow int column silently becoming a STRING, which is
    worse than the INT64 widening the exact widths are meant to fix."""
    assert try_parse_column_type(spelling).physical == physical
    assert _normalized(dict(name="c", type=spelling))["c"].physical == physical


def test_float_stays_double_while_real_is_single():
    """REAL is single-precision per the SQL standard, but bare FLOAT must NOT be
    re-pointed to match it: FLOAT is what the catalog actually persists for the
    FLOAT category today, so narrowing it would silently truncate every stored
    float column."""
    assert try_parse_column_type("REAL") == FLOAT32
    assert try_parse_column_type("FLOAT") == FLOAT64
    assert try_parse_column_type("DOUBLE") == FLOAT64


@pytest.mark.parametrize("spelling", ["TINYINT", "SMALLINT", "REAL"])
def test_schema_alias_spellings_do_not_widen_the_cast_dialect(spelling):
    """`_SQL_NAME_ALIASES` is read-side only. Cast targets go through
    `_extract_data_type`'s own mapping, which never consults it — so teaching the
    schema READER an alias spelling must not teach the SQL dialect one.

    The CANONICAL widths (INT8/INT16/INT32/FLOAT32) are a different matter: they
    became real cast targets once per-width kernels existed, and are asserted
    below. These three are aliases for those widths, not names of their own, and
    are still rejected — with a suggestion naming the exact width."""
    import opteryx
    from opteryx.exceptions import SqlError

    with pytest.raises(SqlError):
        list(opteryx.session().execute_to_morsels(f"SELECT CAST(1 AS {spelling}) AS v"))


@pytest.mark.parametrize(
    "spelling, physical",
    [
        ("INT8", DrakenType.INT8),
        ("INT16", DrakenType.INT16),
        ("INT32", DrakenType.INT32),
        ("FLOAT32", DrakenType.FLOAT32),
    ],
)
def test_canonical_widths_are_cast_targets_and_produce_that_width(spelling, physical):
    """The declared type and the ACTUAL vector type must agree. This is the whole
    reason these names were refused until per-width kernels existed: the target
    arm mapped them onto INT64/FLOAT64-producing kernels, so accepting the name
    would have declared INT32 and produced INT64."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels(f"SELECT CAST(1 AS {spelling}) AS v"):
        assert morsel.column("v").type == physical, (spelling, morsel.column("v").type)


def test_unparseable_stored_type_still_falls_back_rather_than_raising():
    """Reading a schema is not the place to fail loud on an unknown name — that
    would make an old or hand-edited catalog entry unreadable. The pre-existing
    VARCHAR default is preserved, which is why this path uses
    `try_parse_column_type` and not the raising entry point."""
    by_name = _normalized(dict(name="mystery", type="NOT_A_REAL_TYPE"))
    assert by_name["mystery"] == VARCHAR


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
# CIDR containment against a LITERAL network is rewritten to a range so it can
# prune at the scan (PredicateRewriteStrategy.rewrite_cidr_to_range). These
# defend the EQUIVALENCE of the two forms — the rewrite is only legitimate
# because `(ip & mask) == base` and `base <= ip <= broadcast` select the same
# rows, and a rewrite that is merely *faster* is a wrong-answer bug.
# ---------------------------------------------------------------------------

# Addresses chosen to sit on the boundaries the rewrite is most likely to get
# wrong: either side of 10/8, either side of the signed/unsigned midpoint
# (128.0.0.0 is 2**31, which is negative if anything treats it as int32), and
# both extremes of the space.
_RANGE_ADDRESSES = [
    "0.0.0.0",
    "9.255.255.255",
    "10.0.0.0",
    "10.0.0.1",
    "10.255.255.255",
    "11.0.0.0",
    "127.255.255.255",
    "128.0.0.0",
    "192.168.1.1",
    "255.255.255.255",
]
_RANGE_SOURCE = " UNION ALL ".join(f"SELECT '{a}' AS a" for a in _RANGE_ADDRESSES)


@pytest.mark.parametrize(
    "cidr",
    [
        "10.0.0.0/8",
        "0.0.0.0/0",  # /0 — netmask() special-case; must match everything
        "192.168.1.0/24",
        "192.168.1.1/32",  # /32 — rewritten to Eq, not a range
        "128.0.0.0/1",  # bounds above INT32_MAX
        "172.16.0.0/12",  # matches nothing
        "255.255.255.255/32",  # the very top of the space
        "0.0.0.0/32",  # the very bottom
        "10.0.0.0/31",  # a two-host network
    ],
)
def test_containment_range_rewrite_matches_the_mask_and_compare(cidr):
    """The rewritten range must select exactly the addresses the network holds.

    Truth is computed here from the closed interval rather than by re-running
    the kernel, so this fails if the rewrite and the kernel ever disagree about
    what a network contains.
    """
    from draken.draken_native import ipv4_parse, ipv4_parse_cidr

    base, upper, _ = ipv4_parse_cidr(cidr)
    expected = sorted(a for a in _RANGE_ADDRESSES if base <= ipv4_parse(a) <= upper)

    contained = rows(f"SELECT a FROM ({_RANGE_SOURCE}) AS t WHERE CAST(a AS IPV4) <<= '{cidr}'")
    contains = rows(f"SELECT a FROM ({_RANGE_SOURCE}) AS t WHERE '{cidr}' >>= CAST(a AS IPV4)")

    assert sorted(contained) == expected
    assert sorted(contains) == expected, "`>>=` must rewrite identically to `<<=`"


def test_containment_rewrite_actually_fires():
    """Guards the optimization itself: if the rewrite silently stops matching,
    the queries above still pass (the kernel answers them) and the pruning win
    is lost with no test failing."""
    from opteryx.models import QueryTelemetry

    session = opteryx.session()
    list(
        session.execute_to_morsels(
            f"SELECT a FROM ({_RANGE_SOURCE}) AS t WHERE CAST(a AS IPV4) <<= '10.0.0.0/8'"
        )
    )
    telemetry = QueryTelemetry(getattr(session, "query_id", ""))
    assert telemetry.optimization_predicate_rewriter_cidr_to_range == 1


def test_containment_rewrite_declines_an_invalid_cidr_rather_than_raising_early():
    """A malformed CIDR must still fail, and must fail with the kernel's error —
    the rewrite is an optimization and does not get to change when or how a bad
    query breaks."""
    with pytest.raises(Exception):
        one(f"SELECT COUNT(*) AS c FROM ({_RANGE_SOURCE}) AS t WHERE CAST(a AS IPV4) <<= '10.0.0.0/33'")


def test_containment_rewrite_preserves_null_handling():
    """A NULL address is contained by nothing. The kernel returns FALSE and the
    range returns NULL; WHERE discards both, which is the only reason the
    rewrite is sound. If containment ever escapes a Filter, this stops holding."""
    src = (
        "SELECT '10.0.0.1' AS a UNION ALL SELECT NULL UNION ALL SELECT '192.168.1.1'"
    )
    assert one(f"SELECT COUNT(*) AS c FROM ({src}) AS t WHERE CAST(a AS IPV4) <<= '10.0.0.0/8'") == [1]
    assert one(f"SELECT COUNT(*) AS c FROM ({src}) AS t WHERE CAST(a AS IPV4) <<= '0.0.0.0/0'") == [2]


# ---------------------------------------------------------------------------
# Literal comparison against an address column.
#
# These pin a SAFETY property, not a feature: an address column compared against
# dotted-decimal TEXT must never quietly answer. The column is a uint32 and the
# literal is a string, so any path that reinterpreted one as the other would
# return wrong rows in exactly the ACL-shaped query this type exists to serve.
#
# Today every shape below fails. One is an open gap worth knowing:
#   * `IN (a, b)` with 2+ members is unsupported for EVERY unsigned integer
#     width (UINT8..UINT64), not just IPv4 — the in-list kernel path admits
#     signed integers only. Signed columns run the same query fine.
# The single-member and multi-member string forms now fail at BIND time with
# IncompatibleTypesError — the binder validates IN-list element types against
# the left operand's type, the same as it does for `=`. What these tests
# defend is that they stay LOUD, at plan time or otherwise.
# ---------------------------------------------------------------------------


_IN_LIST_SOURCE = (
    "SELECT '10.0.0.1' AS a UNION ALL SELECT '10.0.0.2' UNION ALL SELECT '192.168.1.1'"
)


@pytest.mark.parametrize(
    "predicate",
    [
        "CAST(a AS IPV4) = '10.0.0.1'",
        "CAST(a AS IPV4) IN ('10.0.0.1')",
        "CAST(a AS IPV4) IN ('10.0.0.1', '10.0.0.2')",
    ],
)
def test_address_column_against_dotted_text_never_silently_answers(predicate):
    """Dotted text is not an address until something parses it. Until the
    coercion exists, these must raise — a result here would mean a uint32 and a
    string were compared as if one were the other."""
    with pytest.raises(Exception):
        one(f"SELECT COUNT(*) AS c FROM ({_IN_LIST_SOURCE}) AS t WHERE {predicate}")


def test_address_column_against_an_integer_literal_works():
    """The control for the tests above: the same comparison in the address's own
    domain resolves, so what fails there is the string coercion and not the
    comparison itself. 167772161 is 10.0.0.1."""
    assert one(
        f"SELECT COUNT(*) AS c FROM ({_IN_LIST_SOURCE}) AS t "
        "WHERE CAST(a AS IPV4) = 167772161"
    ) == [1]
    assert one(
        f"SELECT COUNT(*) AS c FROM ({_IN_LIST_SOURCE}) AS t "
        "WHERE CAST(a AS IPV4) IN (167772161)"
    ) == [1]


def test_multi_member_in_list_on_an_address_column():
    assert one(
        f"SELECT COUNT(*) AS c FROM ({_IN_LIST_SOURCE}) AS t "
        "WHERE CAST(a AS IPV4) IN (167772161, 167772162)"
    ) == [2]


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


def test_every_integer_width_casts_to_ipv4():
    """An address IS a uint32, so the integer spelling of one must reach IPV4 from
    every width — signed and unsigned, column and literal. Before this, storing an
    address as an INT64 was a one-way door: it rendered as a number for ever."""
    assert rows("SELECT CAST(3232235777 AS IPV4)") == ["192.168.1.1"]
    assert rows("SELECT CAST(CAST(3232235777 AS UINT32) AS IPV4)") == ["192.168.1.1"]
    assert rows("SELECT CAST(CAST(3232235777 AS UINT64) AS IPV4)") == ["192.168.1.1"]
    # Columns, not just folded literals — the kernel path, not the bind-time one.
    assert rows(
        "SELECT CAST(a AS IPV4) FROM (SELECT 3232235777 AS a) AS t"
    ) == ["192.168.1.1"]
    assert rows(
        "SELECT CAST(CAST(a AS UINT32) AS IPV4) FROM (SELECT 16843009 AS a) AS t"
    ) == ["1.1.1.1"]


def test_integer_to_ipv4_round_trips_through_integer():
    """The two directions must compose: an address rendered as a number and read
    back is the same address."""
    assert rows(
        "SELECT CAST(CAST(CAST('192.168.1.1' AS IPV4) AS INTEGER) AS IPV4)"
    ) == ["192.168.1.1"]


def test_integer_to_ipv4_refuses_a_value_that_is_not_an_address():
    """Range-checked, never wrapped — a negative or >2^32-1 integer is not an
    address and must fail loud rather than silently become one."""
    with pytest.raises(Exception):
        rows("SELECT CAST(4294967296 AS IPV4)")
    with pytest.raises(Exception):
        rows("SELECT CAST(-1 AS IPV4)")
    with pytest.raises(Exception):
        rows("SELECT CAST(0 - a AS IPV4) FROM (SELECT 1 AS a) AS t")


# ---------------------------------------------------------------------------
# Result schema — what a consumer of query RESULTS is told the columns are
#
# Everything above tests IPv4 inside the engine. These test the SCHEMA-level
# report, which is all a consumer outside the process ever sees: a job runner
# writing a sidecar next to a result file has nothing else to record, and
# Parquet cannot express IPv4, so a schema that says UINT32 makes the address
# unrecoverable for ever.
# ---------------------------------------------------------------------------


def schema_types(sql):
    """The ColumnTypes of a query's result columns, as a consumer would read them."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        return morsel_column_types(morsel)
    return []


def column_names(sql):
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        return [
            n.decode("utf-8") if isinstance(n, bytes) else n for n in morsel.column_names
        ]
    return []


def test_column_type_from_vector_reads_the_descriptor_not_the_tag():
    """The shared reconstructor, on the two vectors that are physically the same
    32 bits. This is the single place the (tag, descriptor) pair becomes a
    ColumnType — the four independent copies of this logic are what put UINT32
    in the sidecar."""
    assert column_type_from_vector(ipv4_vector(["192.168.1.1"])) == IPV4
    assert (
        column_type_from_vector(Vector(vector_uint32_from_sequence([3232235777]))) == UINT32
    )


def test_result_schema_reports_ipv4_not_uint32():
    """`Morsel.column_types` reports the bare DrakenType, and an address and an
    unsigned integer share one — so the tag alone cannot say which this is.
    `morsel_column_types` reads the descriptor beside it."""
    session = opteryx.session()
    for morsel in session.execute_to_morsels("SELECT CAST('192.168.1.1' AS IPV4) AS ip"):
        assert morsel.column_types == [DrakenType.UINT32]  # the tag, still just the tag
        assert morsel_column_types(morsel) == [IPV4]
        break


def test_result_schema_round_trips_ipv4_through_a_string():
    """The whole point: a consumer serializes the schema, and reading it back
    yields the same type. Without this the sidecar says UINT32 and the address
    is gone."""
    types = schema_types("SELECT CAST('192.168.1.1' AS IPV4) AS ip")
    serialized = [serialize_column_type(t) for t in types]
    assert serialized == ["IPV4"]
    assert [parse_column_type(s) for s in serialized] == [IPV4]


def test_result_schema_round_trips_a_plain_uint32_as_an_integer():
    """The negative case. The failure mode of every bug in this family is reading
    the descriptor's ABSENCE as if it were present — every integer column
    becoming an address."""
    types = schema_types("SELECT CAST(3232235777 AS UINT32) AS n")
    serialized = [serialize_column_type(t) for t in types]
    assert serialized == ["UINT32"]
    assert [parse_column_type(s) for s in serialized] == [UINT32]
    assert types != [IPV4]
    assert types[0].logical is None


def test_result_schema_reports_both_side_by_side():
    """The two columns are physically identical uint32s. Reported side by side,
    nothing may blur them together."""
    types = schema_types(
        "SELECT CAST('192.168.1.1' AS IPV4) AS ip, CAST(3232235777 AS UINT32) AS n"
    )
    assert [serialize_column_type(t) for t in types] == ["IPV4", "UINT32"]


def test_unaliased_ipv4_expression_is_named_as_an_address():
    """An unaliased expression is named after its own rendering, and an IPv4
    literal folds to the uint32 the address IS — so a category-keyed renderer
    names the column '3232235777'."""
    assert column_names("SELECT CAST('192.168.1.1' AS IPV4)") == ["192.168.1.1"]
    assert column_names("SELECT CAST(16843009 AS IPV4)") == ["1.1.1.1"]


def test_aliased_ipv4_expression_keeps_its_alias():
    """An alias is the user's name for the column and outranks any rendering."""
    assert column_names("SELECT CAST('192.168.1.1' AS IPV4) AS ip") == ["ip"]


def test_unaliased_uint32_expression_is_named_with_its_integer():
    """The negative case, again — a plain integer names itself with its integer.
    An address-shaped name here would mean the renderer keyed on the category
    (INTEGER, which IPv4 shares) instead of the descriptor."""
    assert column_names("SELECT CAST(3232235777 AS UINT32)") == ["3232235777"]
    assert column_names("SELECT 3232235777") == ["3232235777"]


def test_ipv4_and_same_valued_integer_literals_are_distinct_columns():
    """They render differently BECAUSE they are different types. Sharing a
    rendering made them one column — an expression's rendering is its identity."""
    assert column_names("SELECT 3232235777, CAST('192.168.1.1' AS IPV4)") == [
        "3232235777",
        "192.168.1.1",
    ]


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
