"""
Native unit tests for draken/core/ipv4.h — the ONE place the IPv4 <-> uint32
mapping and the parse strictness rules are written down.

These exist because parse() and format() are no longer a single scalar loop
each. parse() classifies all sixteen bytes of the address in one vector
(NEON / SSE2, with a scalar path for anything else) and format() renders each
octet with one 4-byte table store. Three consequences are pinned here:

  1. STRICTNESS IS UNCHANGED. The accept/reject decision is checked against an
     independent Python oracle that implements the documented rules directly —
     four decimal octets 0..255, single dots, no leading zeros, no shorthand,
     no trailing junk. A vector path that quietly accepted "1.2.3.04" or
     "010.1.1.1" would be a security bug (an ACL and a parser disagreeing), not
     a performance regression.
  2. THE WIDTH RULE IS WRITTEN TWICE. text_length() computes an octet's width
     with threshold tests, format() with a table, because they measure
     differently on x86 and ARM. Every length here is checked against the text
     actually rendered.
  3. THE 15-BYTE ADDRESS IS THE OVERSHOOT CASE. format() stores four bytes per
     octet, so "255.255.255.255" touches byte 15 while reporting 15 — the
     reason buffers are sized by FORMAT_SCRATCH_BYTES. It is rendered here on
     every path that can produce it.

The exhaustive check (all 2^32 values through format/text_length/parse, and the
new implementation against the pre-vector one) lives in test_ipv4_exhaustive.cpp
alongside this file — it takes minutes, so it is a dev tool, not a pytest.
"""

import random

import pytest

import draken.draken_native as dn


# ---------------------------------------------------------------------------
# Independent oracle — the documented rules, spelled out, no shared code with
# the header. Deliberately naive: if this and the vector parser disagree, the
# question is which one is right, and this one is readable.
# ---------------------------------------------------------------------------
def oracle_parse(text: str):
    parts = text.split(".")
    if len(parts) != 4:
        return None
    value = 0
    for part in parts:
        if not (1 <= len(part) <= 3):
            return None
        if not all(c in "0123456789" for c in part):
            return None
        if len(part) > 1 and part[0] == "0":
            return None            # leading zero — no octal, no ambiguity
        octet = int(part)
        if octet > 255:
            return None
        value = (value << 8) | octet
    return value


def parse(text):
    """dn.ipv4_parse, with the raise folded into None so it can be compared."""
    try:
        return dn.ipv4_parse(text)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# 1.  The mapping itself
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text,value",
    [
        ("0.0.0.0", 0),
        ("0.0.0.1", 1),
        ("1.2.3.4", 0x01020304),
        ("192.168.1.1", 0xC0A80101),
        ("10.0.0.1", 0x0A000001),
        ("255.255.255.255", 0xFFFFFFFF),
        ("127.0.0.1", 0x7F000001),
    ],
)
def test_parse_maps_octets_to_bits(text, value):
    assert dn.ipv4_parse(text) == value
    assert dn.ipv4_format(value) == text


# ---------------------------------------------------------------------------
# 2.  Strictness — the forms that must be REFUSED
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "01.2.3.4",              # leading zero
        "192.168.001.1",
        "1.2.3.04",
        "00.0.0.0",
        "000.0.0.0",
        "010.1.1.1",             # octal-looking
        "256.1.1.1",             # out of range
        "255.256.1.1",
        "1.2.3.256",
        "999.999.999.999",
        "1.2.3",                 # too few octets
        "1.2.3.4.5",             # too many
        "10.1",                  # inet_aton shorthand
        "1.2.3.4abc",            # trailing junk
        " 1.2.3.4",              # surrounding space
        "1.2.3.4 ",
        "1..2.3",                # empty octet
        "1.2.3.",
        ".1.2.3",
        "1.2.3.4.",
        "...",
        "",
        "1234.1.1.1",            # overlong octet
        "+1.2.3.4",
        "-1.2.3.4",
        "1.2.3.-4",
        "0x1.2.3.4",
        "1e2.3.4.5",
        "192.168.1.1/24",        # a CIDR is not an address
        "1.2.3.4\n",
    ],
)
def test_strict_forms_are_refused(text):
    with pytest.raises(ValueError):
        dn.ipv4_parse(text)
    assert oracle_parse(text) is None, "oracle disagrees — check the test, not the parser"


# ---------------------------------------------------------------------------
# 3.  Length boundaries — 7 bytes is the shortest address, 15 the longest, and
#     15 is the one that makes format() touch its sixteenth byte.
# ---------------------------------------------------------------------------
def test_length_boundaries():
    assert dn.ipv4_format(0) == "0.0.0.0"                        # 7 bytes
    assert dn.ipv4_parse("0.0.0.0") == 0
    assert dn.ipv4_format(0xFFFFFFFF) == "255.255.255.255"       # 15 bytes
    assert dn.ipv4_parse("255.255.255.255") == 0xFFFFFFFF
    # Every width combination that can end an address, so the last octet is
    # rendered at each cursor position the 4-byte store can land on.
    for last in (0, 9, 10, 99, 100, 255):
        for first in (0, 9, 10, 99, 100, 255):
            value = (first << 24) | (7 << 16) | (77 << 8) | last
            text = dn.ipv4_format(value)
            assert dn.ipv4_parse(text) == value
            assert text == f"{first}.7.77.{last}"


# ---------------------------------------------------------------------------
# 4.  Differential fuzz against the oracle — mutations of valid addresses hit
#     the shapes that separate the paths: dot counts, octet widths, stray bytes.
# ---------------------------------------------------------------------------
def test_mutation_fuzz_matches_the_oracle():
    rng = random.Random(20260822)
    seeds = ["192.168.1.1", "0.0.0.0", "255.255.255.255", "1.2.3.4", "10.0.0.1",
             "9.99.199.255", "100.10.1.0"]
    cases = list(seeds)
    for seed in seeds:
        for i in range(len(seed)):
            for ch in ".0159 /abc":
                cases.append(seed[:i] + ch + seed[i + 1:])      # substitute
                cases.append(seed[:i] + ch + seed[i:])          # insert
            cases.append(seed[:i] + seed[i + 1:])               # delete
    for _ in range(20000):
        n = rng.randint(0, 20)
        cases.append("".join(rng.choice("0123456789../ a") for _ in range(n)))
    for text in cases:
        assert parse(text) == oracle_parse(text), f"disagreement on {text!r}"


def test_random_roundtrip():
    rng = random.Random(1234)
    for _ in range(50000):
        value = rng.getrandbits(32)
        text = dn.ipv4_format(value)
        assert dn.ipv4_parse(text) == value
        assert oracle_parse(text) == value
        assert len(text) == len(".".join(str((value >> s) & 0xFF) for s in (24, 16, 8, 0)))


# ---------------------------------------------------------------------------
# 5.  CIDR — same parser for the address half, so the strictness above must
#     still hold once a '/prefix' is attached.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text,base,broadcast,prefix",
    [
        ("0.0.0.0/0", 0, 0xFFFFFFFF, 0),
        ("10.0.0.0/8", 0x0A000000, 0x0AFFFFFF, 8),
        ("192.168.1.0/24", 0xC0A80100, 0xC0A801FF, 24),
        ("192.168.1.7/24", 0xC0A80100, 0xC0A801FF, 24),   # base is masked
        ("255.255.255.255/32", 0xFFFFFFFF, 0xFFFFFFFF, 32),
    ],
)
def test_cidr_bounds(text, base, broadcast, prefix):
    assert dn.ipv4_parse_cidr(text) == (base, broadcast, prefix)


@pytest.mark.parametrize(
    "text",
    [
        "192.168.1.0",        # no '/'
        "192.168.1.0/",       # no prefix
        "192.168.1.0/33",     # prefix out of range
        "192.168.1.0/99",
        "192.168.1.0/999",
        "/24",
        "192.168.001.0/24",   # the address half is still strict
        "010.0.0.0/8",
        "1.2.3/24",
        "192.168.1.0/24x",
        "192.168.1.0//24",
    ],
)
def test_cidr_refusals(text):
    with pytest.raises(ValueError):
        dn.ipv4_parse_cidr(text)
