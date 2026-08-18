# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`explicit_schema` on the JSONL and CSV readers: the full canonical type
vocabulary, parsed strictly.

The gap these close: a caller that already knows its destination schema (the
upload service reads it from the catalog) could only declare "int64", "double",
"boolean" or "string". A column the catalog declares IPV4 therefore had to be
inferred and then cast, and inference produced INT64 for a numeric JSON value or
VARCHAR for a dotted-quad one -- silently, with the mismatch surfacing much later
as `cast ipv4->string: expected UINT32, got 4`.

Two things are asserted throughout, and the second is the one that actually
catches drift:

  * the PHYSICAL type identity (DrakenType), because a value comparison alone
    passes whenever two representations happen to coincide, and

  * the LOGICAL descriptor, because IPV4 *is* UINT32 -- it is the descriptor,
    carried out-of-band on the vector's owner, that distinguishes an address
    column from an unsigned integer column. A reader that produced the right 32
    bits and no descriptor would satisfy every value assertion here and still be
    the bug.

Strictness is asserted as hard as correctness. A declared type is a contract:
every refusal below must RAISE, never quietly yield NULL or 0. For IPv4 that is
a security property -- shorthand ("10.1") and leading-zero/octal forms
("010.1.1.1") are refused because a reader and an access rule disagreeing about
which address a string denotes is a security bug, not a formatting one.
"""

import datetime
import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from draken.draken_native import LogicalKind  # noqa: E402
from rugo.rugo_native import read_csv  # noqa: E402
from rugo.rugo_native import read_jsonl  # noqa: E402


def _jsonl_column(body: str, declared: dict):
    """Read a one-column JSONL buffer under `declared`; return (vector, values)."""
    result = read_jsonl(body.encode() + b"\n", explicit_schema=declared)
    assert result["success"]
    vector = result["columns"][0]
    return vector, [vector[i] for i in range(result["num_rows"])]


def _csv_column(body: str, declared: dict):
    """Read a one-column CSV buffer under `declared`; return (vector, values)."""
    result = read_csv(body.encode(), explicit_schema=declared)
    assert result["success"]
    vector = result["columns"][0]
    return vector, [vector[i] for i in range(result["num_rows"])]


# ---------------------------------------------------------------------------
# IPV4 -- the reported gap
# ---------------------------------------------------------------------------


def test_jsonl_declared_ipv4_is_uint32_carrying_the_descriptor():
    vector, values = _jsonl_column(
        '{"source_ip": "10.0.0.7"}\n{"source_ip": "192.168.1.1"}\n{"source_ip": null}',
        {"source_ip": "IPV4"},
    )
    assert vector.type.value == 106, vector.type          # DRAKEN_UINT32
    assert vector.logical_type_kind == LogicalKind.IPV4
    # Rendering dotted-decimal (rather than 167772167) is the descriptor being
    # read: it is what every consumer of an IPv4 column depends on.
    assert values == ["10.0.0.7", "192.168.1.1", None]


def test_csv_declared_ipv4_is_uint32_carrying_the_descriptor():
    vector, values = _csv_column(
        "source_ip\n10.0.0.7\n\n192.168.1.1\n", {"source_ip": "IPV4"}
    )
    assert vector.type.value == 106, vector.type
    assert vector.logical_type_kind == LogicalKind.IPV4
    assert values == ["10.0.0.7", None, "192.168.1.1"]


def test_declared_ipv4_survives_the_text_render_path():
    """The renderer is a SECOND consumer of the descriptor, reached separately
    from vector readback (rugo/src/_text_render.hpp gates on LogicalKind::IPV4).
    A descriptor attached for readback but lost on the way to a writer would pass
    the test above and still emit integers here."""
    from draken.morsels.morsel import Morsel
    from rugo.rugo_native import write_csv

    result = read_jsonl(
        b'{"source_ip": "10.0.0.7"}\n{"source_ip": "192.168.1.1"}\n',
        explicit_schema={"source_ip": "IPV4"},
    )
    morsel = Morsel.from_vectors(result["column_names"], result["columns"])
    assert write_csv(morsel).decode() == "source_ip\n10.0.0.7\n192.168.1.1\n"


def test_undeclared_ipv4_still_infers_varchar():
    """The behaviour being worked around, pinned so the contrast stays visible:
    without a declaration the same bytes are an ordinary string column."""
    vector, _ = _jsonl_column('{"source_ip": "10.0.0.7"}', {})
    assert vector.type.value == 60                        # DRAKEN_VARCHAR
    assert vector.logical_type_kind is None


@pytest.mark.parametrize(
    "text",
    [
        "10.1",             # inet_aton shorthand
        "010.1.1.1",        # leading zero / octal
        "10.0.0.256",       # octet out of range
        "10.0.0.1 ",        # trailing space
        "10.0.0",           # too few octets
        "10.0.0.1.5",       # too many octets
        "not-an-address",
    ],
)
def test_jsonl_declared_ipv4_refuses_non_canonical_text(text):
    with pytest.raises(ValueError):
        _jsonl_column('{"ip": "%s"}' % text, {"ip": "IPV4"})


def test_jsonl_declared_ipv4_refuses_a_bare_integer():
    """Architect's ruling 2026-08-18: dotted-quad ONLY. The 32 bits would be
    unambiguous, but the integer is a storage spelling, not an address."""
    with pytest.raises(ValueError):
        _jsonl_column('{"ip": 2130706433}', {"ip": "IPV4"})


def test_csv_declared_ipv4_refuses_a_bare_integer():
    with pytest.raises(RuntimeError):
        _csv_column("ip\n2130706433\n", {"ip": "IPV4"})


# ---------------------------------------------------------------------------
# Fixed widths
# ---------------------------------------------------------------------------

# (declared name, JSON/CSV text, DrakenType ordinal, value)
_WIDTHS = [
    ("INT8", "-128", 1, -128),
    ("INT16", "-32768", 2, -32768),
    ("INT32", "-2147483648", 3, -2147483648),
    ("INT64", "-9223372036854775808", 4, -9223372036854775808),
    ("UINT8", "255", 104, 255),
    ("UINT16", "65535", 105, 65535),
    ("UINT32", "4294967295", 106, 4294967295),
    ("UINT64", "18446744073709551615", 107, 18446744073709551615),
    ("FLOAT32", "1.5", 20, 1.5),
    ("FLOAT64", "1.25", 21, 1.25),
    ("BOOL", "true", 50, True),
]


@pytest.mark.parametrize("declared,text,tag,expected", _WIDTHS)
def test_jsonl_declared_width_is_exactly_that_width(declared, text, tag, expected):
    vector, values = _jsonl_column('{"c": %s}' % text, {"c": declared})
    assert vector.type.value == tag, vector.type
    assert vector.logical_type_kind is None
    assert values == [expected]


@pytest.mark.parametrize("declared,text,tag,expected", _WIDTHS)
def test_csv_declared_width_is_exactly_that_width(declared, text, tag, expected):
    vector, values = _csv_column("c\n%s\n" % text, {"c": declared})
    assert vector.type.value == tag, vector.type
    assert values == [expected]


@pytest.mark.parametrize(
    "declared,text",
    [
        ("UINT8", "256"),
        ("UINT16", "65536"),
        ("UINT32", "4294967296"),
        ("INT8", "128"),
        ("INT8", "-129"),
        ("INT16", "32768"),
        # A negative in an unsigned column must FAIL, not wrap to a huge value.
        ("UINT8", "-1"),
        ("UINT32", "-1"),
        ("UINT64", "-1"),
    ],
)
def test_declared_width_refuses_out_of_range_rather_than_wrapping(declared, text):
    with pytest.raises(ValueError):
        _jsonl_column('{"c": %s}' % text, {"c": declared})
    with pytest.raises(RuntimeError):
        _csv_column("c\n%s\n" % text, {"c": declared})


# ---------------------------------------------------------------------------
# Temporal -- ISO-8601 text only
# ---------------------------------------------------------------------------


def test_jsonl_declared_date_parses_iso_text():
    vector, values = _jsonl_column('{"d": "2026-08-18"}', {"d": "DATE"})
    assert vector.type.value == 30                        # DRAKEN_DATE32
    assert values == [datetime.date(2026, 8, 18)]


def test_jsonl_declared_timestamp_carries_its_unit():
    vector, values = _jsonl_column(
        '{"t": "2026-08-18T13:51:38.636416"}', {"t": "TIMESTAMP[us]"}
    )
    assert vector.type.value == 40                        # DRAKEN_TIMESTAMP64
    assert vector.logical_type_kind == LogicalKind.TIMESTAMP
    assert values[0] == datetime.datetime(
        2026, 8, 18, 13, 51, 38, 636416, tzinfo=datetime.timezone.utc
    )


def test_bare_timestamp_means_microseconds():
    """The canonical default, and what every schema persisted before the unit was
    serialized says -- re-reading those must not change their meaning."""
    bare, _ = _jsonl_column('{"t": "2026-08-18T00:00:00"}', {"t": "TIMESTAMP"})
    spelled, _ = _jsonl_column('{"t": "2026-08-18T00:00:00"}', {"t": "TIMESTAMP[us]"})
    assert bare.logical_type_unit == spelled.logical_type_unit


@pytest.mark.parametrize(
    "declared,text",
    [
        ("TIMESTAMP[us]", "1755525098636416"),   # epoch integer
        ("DATE", "20684"),                       # epoch day count
        ("TIMESTAMP[us]", '"2026-08-18T00:00:00Z"'),   # zone suffix: naive only
        ("TIMESTAMP[us]", '"2026-08-18T00:00:00+01:00"'),
        ("DATE", '"18/08/2026"'),
    ],
)
def test_declared_temporal_refuses_non_iso_forms(declared, text):
    with pytest.raises(ValueError):
        _jsonl_column('{"c": %s}' % text, {"c": declared})


def test_declared_timestamp_unit_conversion_is_exact_or_refused():
    """A value carrying more precision than the declared unit can hold FAILS
    rather than truncating -- the same policy DECIMAL applies to fractional
    digits beyond its scale."""
    vector, values = _jsonl_column(
        '{"t": "2026-08-18T00:00:00"}', {"t": "TIMESTAMP[s]"}
    )
    assert values[0] == datetime.datetime(2026, 8, 18, tzinfo=datetime.timezone.utc)
    with pytest.raises(ValueError):
        _jsonl_column('{"t": "2026-08-18T00:00:00.5"}', {"t": "TIMESTAMP[s]"})


# ---------------------------------------------------------------------------
# DECIMAL -- tier chosen by precision, value policy inherited from the CAST
# ---------------------------------------------------------------------------


def test_declared_decimal_uses_the_int64_tier_up_to_18_digits():
    vector, values = _jsonl_column('{"m": "1.250"}', {"m": "DECIMAL(10, 2)"})
    assert vector.type.value == 5                         # DRAKEN_DECIMAL
    assert vector.logical_type_kind == LogicalKind.DECIMAL
    assert (vector.logical_type_precision, vector.logical_type_scale) == (10, 2)
    # Trailing zeros re-pad silently; this is not a scale violation.
    assert values == [Decimal("1.25")]


def test_declared_decimal_uses_the_128_tier_past_18_digits():
    vector, values = _jsonl_column(
        '{"m": "123456789012345678901.5"}', {"m": "DECIMAL(30, 4)"}
    )
    assert vector.type.value == 103                       # DRAKEN_DECIMAL128
    assert vector.logical_type_kind == LogicalKind.DECIMAL
    assert values == [Decimal("123456789012345678901.5000")]


def test_declared_decimal_accepts_an_unquoted_json_number():
    _, values = _jsonl_column('{"m": 12.34}', {"m": "DECIMAL(18, 2)"})
    assert values == [Decimal("12.34")]


@pytest.mark.parametrize(
    "declared,text",
    [
        ("DECIMAL(10, 2)", '"1.005"'),   # digits that would be DROPPED
        ("DECIMAL(4, 0)", '"99999"'),    # magnitude past the precision
        ("DECIMAL(10, 2)", '"abc"'),
    ],
)
def test_declared_decimal_refuses_rather_than_rounding_or_wrapping(declared, text):
    with pytest.raises(ValueError):
        _jsonl_column('{"m": %s}' % text, {"m": declared})


# ---------------------------------------------------------------------------
# The original four names, and the vocabulary's edges
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "declared,text,tag,expected",
    [
        ("int64", "7", 4, 7),
        ("double", "1.5", 21, 1.5),
        ("boolean", "true", 50, True),
        ("string", '"hi"', 60, "hi"),
    ],
)
def test_the_four_original_names_still_work_unchanged(declared, text, tag, expected):
    """These predate the canonical vocabulary and resolve through it
    case-insensitively (DOUBLE -> FLOAT64, BOOLEAN -> BOOL, STRING -> VARCHAR).
    No existing caller changes."""
    vector, values = _jsonl_column('{"c": %s}' % text, {"c": declared})
    assert vector.type.value == tag, vector.type
    assert values == [expected]


def test_declared_names_are_echoed_back_verbatim_in_the_schema():
    result = read_jsonl(
        b'{"ip": "10.0.0.7", "n": 1}\n', explicit_schema={"ip": "IPV4"}
    )
    assert result["schema"] == {"ip": "IPV4", "n": "int64"}


@pytest.mark.parametrize("spelling", ["ipv4", "IPv4", " IPV4 ", "IPV4"])
def test_type_names_are_case_insensitive_and_trimmed(spelling):
    vector, _ = _jsonl_column('{"ip": "10.0.0.7"}', {"ip": spelling})
    assert vector.logical_type_kind == LogicalKind.IPV4


@pytest.mark.parametrize(
    "declared",
    [
        "UBIGINT",       # a foreign type system's spelling
        "DECIMAL",       # parameters are not optional
        "DECIMAL(x, 2)",
        "DECIMAL(0, 0)",
        "DECIMAL(4, 6)",  # scale past precision
        "TIMESTAMP[weeks]",
        "ARRAY<INT64>",   # inference-only outcome, never declarable
        "VARIANT",
        "",
    ],
)
def test_unsupported_type_names_fail_eagerly_naming_the_vocabulary(declared):
    """Eagerly: before a byte is read, so a typo does not surface part-way
    through a multi-gigabyte file."""
    with pytest.raises(ValueError) as err:
        read_jsonl(b'{"c": 1}\n', explicit_schema={"c": declared})
    assert "supported types are" in str(err.value)

    with pytest.raises(ValueError):
        read_csv(b"c\n1\n", explicit_schema={"c": declared})


# ---------------------------------------------------------------------------
# CSV-specific: a declaration displaces sniffing, and ignore_errors cannot
# soften it
# ---------------------------------------------------------------------------


def test_csv_declared_column_is_not_softened_by_fail_on_error_false():
    """`fail_on_error=False` exists to soften a GUESS made from a sample window.
    A declared type is not a guess, so it still fails loud."""
    with pytest.raises(RuntimeError):
        read_csv(b"ip\n10.1\n", explicit_schema={"ip": "IPV4"}, fail_on_error=False)


def test_csv_undeclared_columns_still_sniff_alongside_a_declared_one():
    result = read_csv(
        b"ip,n,s\n10.0.0.7,1,x\n192.168.1.1,2,y\n", explicit_schema={"ip": "IPV4"}
    )
    by_name = dict(zip(result["column_names"], result["columns"]))
    assert by_name["ip"].logical_type_kind == LogicalKind.IPV4
    assert by_name["n"].type.value == 4                   # sniffed INT64
    assert by_name["s"].type.value == 60                  # sniffed VARCHAR


def test_csv_declared_column_ignores_what_sniffing_would_have_chosen():
    """Every value here sniffs as INT64; the declaration must win outright rather
    than being widened into or reconciled with the sniffed type."""
    vector, values = _csv_column("c\n1\n2\n3\n", {"c": "UINT8"})
    assert vector.type.value == 104
    assert values == [1, 2, 3]


# ---------------------------------------------------------------------------
# Threaded merge -- the per-thread buffers are concatenated on the way out, and
# BOOL is bit-packed, so a misaligned merge is a silent wrong answer
# ---------------------------------------------------------------------------


def _threaded_rows(count: int = 60_000):
    return [
        (i % 3 == 0, i % 65536, "10.0.%d.%d" % ((i // 256) % 256, i % 256))
        for i in range(count)
    ]


def test_csv_declared_columns_merge_correctly_across_threads():
    rows = _threaded_rows()
    body = "b,u,ip\n" + "".join(
        "%s,%d,%s\n" % ("true" if b else "false", u, ip) for b, u, ip in rows
    )
    result = read_csv(
        body.encode(), explicit_schema={"b": "BOOL", "u": "UINT16", "ip": "IPV4"}
    )
    assert result["num_rows"] == len(rows)
    by_name = dict(zip(result["column_names"], result["columns"]))
    assert [by_name["b"][i] for i in range(len(rows))] == [r[0] for r in rows]
    assert [by_name["u"][i] for i in range(len(rows))] == [r[1] for r in rows]
    assert [by_name["ip"][i] for i in range(len(rows))] == [r[2] for r in rows]


def test_jsonl_declared_columns_merge_correctly_across_threads():
    rows = _threaded_rows()
    body = "".join(
        '{"b": %s, "u": %d, "ip": "%s"}\n' % ("true" if b else "false", u, ip)
        for b, u, ip in rows
    )
    result = read_jsonl(
        body.encode(), explicit_schema={"b": "BOOL", "u": "UINT16", "ip": "IPV4"}
    )
    assert result["num_rows"] == len(rows)
    by_name = dict(zip(result["column_names"], result["columns"]))
    assert [by_name["b"][i] for i in range(len(rows))] == [r[0] for r in rows]
    assert [by_name["u"][i] for i in range(len(rows))] == [r[1] for r in rows]
    assert [by_name["ip"][i] for i in range(len(rows))] == [r[2] for r in rows]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
