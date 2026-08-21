# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Equality against an UNSIGNED, dictionary-encoded parquet column in a WHERE.

The defect this pins returned ZERO rows, silently, for

    WHERE src_addr = '192.168.4.136'::IPV4
    WHERE src_addr <<= '192.168.4.136/32'

against a live IPv4 column holding 92_077 matching rows, while
`CAST(src_addr AS VARCHAR) = '192.168.4.136'` and
`COUNT(*) FILTER (WHERE ...)` over the same predicate both answered correctly.

CAUSE — not IPv4 at all, and not the comparison. Parquet has no unsigned and no
narrow physical storage: UINT8/16/32 all travel as physical int32. The scan's
dictionary decode-skip (rugo `decode_column.cpp`) probes a row group's dictionary
for the pushed equality needle and, when disjoint, skips decoding that row
group's data pages. It compared the dictionary entries as SIGNED int32, so
192.168.4.136 — bit pattern 0xC0A80488 — read back as -1062730616, matched no
needle, and EVERY dictionary-encoded row group was declared disjoint and skipped.

The three asymmetries in the bug report all fall out of that one cause, and each
is asserted below because each is what made it look like something else:

  * only NARROW prefixes failed — a `/16` or `/24` rewrites to a BETWEEN, and
    only Eq/IN feed the decode-skip probe;
  * `COUNT(*) FILTER` was right — the rewrite only visits WHERE conditions, so
    FILTER keeps the native `ipv4_in_cidr` kernel and never pushes;
  * the value was above 127.255.255.255 — below the signed midpoint the two
    readings agree, so half the address space worked.

`plain_u32` is the control: an IPv4 column IS a uint32 (see test_ipv4_type.py),
so the same rows must answer identically through a column with no descriptor.
That is what proves a future fix must live in the unsigned widening and not in
anything IPv4-shaped.

THE SAME DEFECT AT THE OTHER WIDTH — UINT64 is the one column type whose values
do not all fit an int64 slot, and the needle plumbing (`pool_reader.pyx`) converted
them with a bare `cdef int64_t`. `WHERE u64col = 18446744073709551611` therefore
raised `OverflowError: Python int too large to convert to C long` at scan-open and
the query could not run at all — loud rather than silent, but the same root
mistake: the needle was not expressed in the column's representation. Needles now
ride the slot as their two's-complement bit pattern, which is exactly what the
dictionary entries are. Covered by the `u64` tests at the bottom.
"""

import os
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn
import opteryx
import rugo.parquet as rp
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector

# Above the signed int32 midpoint: 192.168.4.136 is 0xC0A80488, which is
# NEGATIVE read as int32. An address below 128.0.0.0 would pass even unfixed.
TARGET_TEXT = "192.168.4.136"
ROWS = 2000


def _ip(text: str) -> int:
    """Dotted-decimal -> the uint32 the address is. Octet A is bits 31..24."""
    a, b, c, d = (int(part) for part in text.split("."))
    return (a << 24) | (b << 16) | (c << 8) | d


TARGET = _ip(TARGET_TEXT)


def _addresses():
    """High-repeat addresses so every row group dictionary-encodes.

    A third of the rows sit in 10.0.0.0/8 (BELOW the signed midpoint) and the
    rest in 192.168.4.0/24 (above it), so one file carries values on both sides
    of the boundary — a sign-extension bug cannot be masked by a column that is
    uniformly one side or the other.
    """
    out = []
    for i in range(ROWS):
        if i % 10 == 0:
            out.append(TARGET)
        elif i % 3:
            out.append(_ip("192.168.4.%d" % (i % 200 + 1)))
        else:
            out.append(_ip("10.0.%d.%d" % (i % 250, i % 251)))
    return out


ADDRESSES = _addresses()
EXPECTED = ADDRESSES.count(TARGET)
EXPECTED_SLASH_24 = sum(1 for a in ADDRESSES if a >> 8 == _ip("192.168.4.0") >> 8)


@pytest.fixture(scope="module")
def dataset():
    """A one-file dataset with the addresses under BOTH an IPV4 column and a
    bare uint32 one. Written by rugo's own writer: it is the writer that
    produced the affected table, and it is the only one that can express the
    IPV4 descriptor at all (parquet has no logical type for it)."""
    morsel = Morsel.from_vectors(
        ["src_addr", "plain_u32"],
        [
            Vector(dn.vector_retag_uint32_as_ipv4(dn.vector_uint32_from_sequence(ADDRESSES))),
            Vector(dn.vector_uint32_from_sequence(ADDRESSES)),
        ],
    )
    # dictionary=True is the whole point — the decode-skip probe only runs on a
    # dictionary-encoded chunk, and the same file written PLAIN always answered
    # correctly. Left explicit rather than relying on the writer's default.
    buffer = rp.write_parquet(morsel, compression="none", dictionary=True)
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, "netflow")
        os.makedirs(data_dir)
        with open(os.path.join(data_dir, "data.parquet"), "wb") as handle:
            handle.write(buffer)
        yield data_dir


def _count(dataset, where=None, select="COUNT(*) AS c", source=None):
    """`source` overrides the FROM clause — used to build the un-pushed oracle
    (a LIMIT subquery keeps predicate pushdown away from the scan)."""
    sql = f"SELECT {select} FROM {source if source is not None else chr(39) + dataset + chr(39)}"
    if where is not None:
        sql += f" WHERE {where}"
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        for name in morsel.column_names:
            return morsel.column(name).to_pylist()[0]
    return None


# --- the reported defect ------------------------------------------------------


@pytest.mark.parametrize(
    "predicate",
    [
        f"src_addr = '{TARGET_TEXT}'::IPV4",
        f"src_addr = CAST('{TARGET_TEXT}' AS IPV4)",
        f"src_addr <<= '{TARGET_TEXT}/32'",
        f"'{TARGET_TEXT}/32' >>= src_addr",
        f"src_addr IN ('{TARGET_TEXT}'::IPV4)",
    ],
)
def test_host_route_where_matches_the_varchar_cast(dataset, predicate):
    """Every spelling of "this one address" agrees with the VARCHAR oracle.

    The VARCHAR cast is the oracle because it renders the address and compares
    text: it cannot be pushed onto the raw column, so it never reaches the
    decode-skip probe and answered correctly throughout the defect.
    """
    oracle = _count(dataset, f"CAST(src_addr AS VARCHAR) = '{TARGET_TEXT}'")
    assert oracle == EXPECTED, oracle
    assert _count(dataset, predicate) == EXPECTED, predicate


def test_where_and_filter_agree(dataset):
    """WHERE and COUNT(*) FILTER over the same predicate are the same question.

    They took different routes — FILTER is never rewritten to a range and never
    pushed — and the divergence was the first sign the comparison itself was
    fine. Parity here is the assertion that the two routes stay one answer.
    """
    filtered = _count(
        dataset,
        select=f"COUNT(*) FILTER (WHERE src_addr <<= '{TARGET_TEXT}/32') AS c",
    )
    assert filtered == EXPECTED
    assert _count(dataset, f"src_addr <<= '{TARGET_TEXT}/32'") == filtered


def test_a_plain_uint32_answers_identically(dataset):
    """The control: the defect was in unsigned widening, not in IPv4.

    An IPv4 column IS a uint32 with a descriptor, so the same predicate over the
    descriptor-less twin must return the same rows. If this one fails and the
    IPV4 ones pass, the fix went in the wrong place.
    """
    assert _count(dataset, f"plain_u32 = {TARGET}") == EXPECTED
    assert _count(dataset, f"plain_u32 IN ({TARGET}, 1)") == EXPECTED


# --- the asymmetries that disguised it ----------------------------------------


def test_broader_prefixes_still_agree(dataset):
    """A /24 and a /16 were always right (they become a BETWEEN, which does not
    feed the decode-skip probe). Pinned so a fix cannot trade one for the other."""
    assert _count(dataset, "src_addr <<= '192.168.4.0/24'") == EXPECTED_SLASH_24
    assert _count(dataset, "src_addr <<= '192.168.0.0/16'") == EXPECTED_SLASH_24


def test_range_and_negated_forms_bracket_the_equality(dataset):
    """`>=`/`<=`/`!=` never went through the probe and stayed correct; they
    bracket the equality exactly, so a wrong Eq cannot hide inside a consistent
    set of answers."""
    total = _count(dataset)
    assert _count(dataset, f"plain_u32 BETWEEN {TARGET} AND {TARGET}") == EXPECTED
    assert _count(dataset, f"plain_u32 >= {TARGET} AND plain_u32 <= {TARGET}") == EXPECTED
    assert _count(dataset, f"plain_u32 != {TARGET}") == total - EXPECTED


def test_an_absent_address_is_still_zero(dataset):
    """The decode-skip is an optimisation and must keep working: an address that
    is genuinely absent still returns nothing. Widening the probe must not have
    been achieved by making it match everything."""
    absent = "192.168.9.9"
    assert absent not in {TARGET_TEXT}
    assert _count(dataset, f"src_addr = '{absent}'::IPV4") == 0
    assert _count(dataset, f"CAST(src_addr AS VARCHAR) = '{absent}'") == 0


# --- the same defect at 64 bits -----------------------------------------------

# Above INT64_MAX, so it does not fit the int64 needle slot as a signed number.
U64_BIG = (1 << 64) - 5
# Just above the boundary — the smallest interesting value, and a different bit
# pattern from U64_BIG, so a fix that happened to work for one is not assumed to
# work for the other.
U64_MID = (1 << 63) + 7
U64_SMALL = 12345
U64_ROWS = 1400


def _u64_values():
    out = []
    for i in range(U64_ROWS):
        if i % 7 == 0:
            out.append(U64_BIG)
        elif i % 7 == 1:
            out.append(U64_MID)
        else:
            out.append(U64_SMALL + i)
    return out


U64_VALUES = _u64_values()


@pytest.fixture(scope="module")
def u64_dataset():
    """A dictionary-encoded UINT64 column straddling INT64_MAX."""
    morsel = Morsel.from_vectors(
        ["u64"], [Vector(dn.vector_uint64_from_sequence(U64_VALUES))]
    )
    buffer = rp.write_parquet(morsel, compression="none", dictionary=True)
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, "wide")
        os.makedirs(data_dir)
        with open(os.path.join(data_dir, "data.parquet"), "wb") as handle:
            handle.write(buffer)
        yield data_dir


@pytest.mark.parametrize("value", [U64_BIG, U64_MID, U64_SMALL + 2])
def test_uint64_equality_runs_and_is_right(u64_dataset, value):
    """Above INT64_MAX this raised OverflowError out of the scan's needle
    conversion, so the query did not run. The oracle is the same predicate with
    the pushdown blocked by a LIMIT — that route was correct throughout, as were
    `>=` and `MAX`, which is what localised the defect to the needle plumbing."""
    expected = U64_VALUES.count(value)
    oracle = _count(
        u64_dataset,
        f"t.u64 = {value}",
        select="COUNT(*) AS c",
        source=f"(SELECT u64 FROM '{u64_dataset}' LIMIT {U64_ROWS}) t",
    )
    assert oracle == expected, oracle
    assert _count(u64_dataset, f"u64 = {value}") == expected


def test_uint64_in_list_converts_every_member(u64_dataset):
    """Needles are converted one at a time, so a list is where a per-member
    conversion bug shows up as a partial answer rather than a crash. Both members
    are above INT64_MAX and have DIFFERENT bit patterns, so neither can be
    answered by the other's."""
    expected = U64_VALUES.count(U64_BIG) + U64_VALUES.count(U64_MID)
    assert _count(u64_dataset, f"u64 IN ({U64_BIG}, {U64_MID})") == expected


def test_in_list_mixing_widths_is_refused_by_the_planner(u64_dataset):
    """PRE-EXISTING and UNRELATED, pinned so it is not mistaken for this defect.

    `u64 IN (<above INT64_MAX>, <small>)` never reaches the scan: the two literals
    bind as different types and the logical planner refuses the list outright. The
    needle path handles mixed members fine (it sees plain ints), so this is a
    binder limitation, not a scan one — it fails LOUDLY and returns no rows rather
    than wrong ones. If this starts passing, delete this test and assert the
    count instead."""
    from opteryx.exceptions import ArrayWithMixedTypesError

    with pytest.raises(ArrayWithMixedTypesError):
        _count(u64_dataset, f"u64 IN ({U64_BIG}, {U64_SMALL + 2})")


def test_an_absent_wide_value_is_still_zero(u64_dataset):
    """The decode-skip must still eliminate: a neighbouring value that is not in
    the column returns nothing. Without this, "carry the bit pattern" could be
    satisfied by never skipping at all."""
    absent = U64_BIG - 1
    assert absent not in U64_VALUES
    assert _count(u64_dataset, f"u64 = {absent}") == 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
