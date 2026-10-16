# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Float columns render as the shortest round-trip decimal, laid out the way
CPython's repr() does (fixed for -4 <= exp < 16, else scientific with a signed
two-digit-minimum exponent), on every text-export path.

fmt_double / fmt_float (draken/interop/value_format.hpp) build that text
straight from ryu's integer decimal (d2s_decimal / f2s_decimal, Opteryx
additions to third_party/ulfjack/ryu) instead of printing ryu's scientific text
and parsing it back. The layout is hand-written, so it is pinned here against
an independent oracle over many values and every layout boundary:

* FLOAT64: repr(float) IS the oracle — CPython's repr is the shortest string
  that round-trips, with this exact layout.
* FLOAT32: the shortest %.Ne text that round-trips through a 32-bit float
  (struct), laid out by the same rules.
"""

import os
import random
import struct
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.csv import write_csv
from rugo.jsonl import write_jsonl


def _f64_from_bits(b):
    return struct.unpack("<d", struct.pack("<Q", b))[0]


def _f32_from_bits(b):
    return struct.unpack("<f", struct.pack("<I", b))[0]


def _round_f32(v):
    return struct.unpack("<f", struct.pack("<f", v))[0]


def _layout(neg, digits, exp):
    """CPython repr() layout of a shortest digit run (digits, decimal exponent)."""
    nd = len(digits)
    if -4 <= exp < 16:
        if exp < 0:
            s = "0." + "0" * (-exp - 1) + digits
        elif nd <= exp + 1:
            s = digits + "0" * (exp + 1 - nd) + ".0"
        else:
            s = digits[: exp + 1] + "." + digits[exp + 1 :]
    else:
        s = digits[0] + ("." + digits[1:] if nd > 1 else "")
        s += "e" + ("-" if exp < 0 else "+") + "%02d" % abs(exp)
    return ("-" if neg else "") + s


def _f32_oracle(v):
    """Shortest decimal that round-trips through FLOAT32, repr-laid-out."""
    if v == 0.0:
        return "-0.0" if struct.pack("<f", v)[3] & 0x80 else "0.0"
    for p in range(1, 10):
        sci = "%.*e" % (p - 1, abs(v))
        if _round_f32(float(sci)) == abs(v):
            mant, e = sci.split("e")
            digits = mant.replace(".", "").rstrip("0") or "0"
            return _layout(v < 0, digits, int(e))
    raise AssertionError("no round-trip decimal for %r" % v)


def _edge_doubles():
    # -0.0 is left out: draken's vector_float64_from_sequence normalises it to
    # 0.0 before any writer sees it (zero takes ryu's unchanged special path).
    vals = [0.0, 1.0, -1.0, 0.5, 0.1, 0.2, 0.3, 1 / 3, 2 / 3, 123.456, -9.75]
    for k in range(-30, 31):
        for m in (1.0, 1.5, 9.999999999999999, 2.5):
            vals.append(m * 10.0**k)
    # the fixed/scientific boundaries on both sides
    vals += [1e-5, 1e-4, 9.99e-5, 0.0001, 0.00012345, 1e15, 9999999999999998.0,
             1e16, 1.2345678901234567e16, 123456789012345.6, float(2**53), float(2**53) + 2.0]
    vals += [5e-324, 2.2250738585072014e-308, 1.7976931348623157e308, -5e-324]
    vals += [float(i) for i in range(0, 2000)]
    return vals


def _random_doubles(n, seed):
    rnd = random.Random(seed)
    out = []
    while len(out) < n:
        v = _f64_from_bits(rnd.getrandbits(64))
        # finite and not a zero (see _edge_doubles on -0.0)
        if v == v and v not in (float("inf"), float("-inf")) and v != 0.0:
            out.append(v)
    # values like the benchmark's: few significant digits, the common case
    out += [round(rnd.random() * 100, 3) for _ in range(n // 2)]
    out += [rnd.randrange(10**6) / 1000 for _ in range(n // 4)]
    return out


def _random_floats(n, seed):
    rnd = random.Random(seed)
    out = []
    while len(out) < n:
        v = _f32_from_bits(rnd.getrandbits(32))
        if v == v and v not in (float("inf"), float("-inf")) and v != 0.0:
            out.append(v)
    out += [_round_f32(round(rnd.random() * 100, 3)) for _ in range(n // 2)]
    return out


def _csv_lines(ctor, values):
    m = Morsel.from_vectors(["v"], [Vector(ctor(values))])
    return write_csv(m, header=False).decode().splitlines()


def test_float64_csv_matches_repr():
    values = _edge_doubles() + _random_doubles(100_000, 20261016)
    got = _csv_lines(dn.vector_float64_from_sequence, values)
    want = [repr(v) for v in values]
    bad = [(v, g, w) for v, g, w in zip(values, got, want) if g != w]
    assert not bad, bad[:10]


def test_float32_csv_matches_shortest_roundtrip():
    # tiny doubles round to (signed) zero in FLOAT32; keep one plain zero
    values = [0.0] + [x for x in (_round_f32(v) for v in _edge_doubles() if abs(v) < 3e38) if x != 0.0]
    values += _random_floats(100_000, 61016)
    got = _csv_lines(dn.vector_float32_from_sequence, values)
    want = [_f32_oracle(v) for v in values]
    bad = [(v, g, w) for v, g, w in zip(values, got, want) if g != w]
    assert not bad, bad[:10]


def test_float64_jsonl_matches_repr():
    values = _edge_doubles() + _random_doubles(20_000, 7)
    m = Morsel.from_vectors(["v"], [Vector(dn.vector_float64_from_sequence(values))])
    lines = write_jsonl(m).decode().splitlines()
    assert lines == ['{"v":%s}' % repr(v) for v in values]


@pytest.mark.parametrize("ctor", [dn.vector_float64_from_sequence, dn.vector_float32_from_sequence])
def test_non_finite_and_null_unchanged(ctor):
    # NaN / Infinity are not numbers in either format: empty CSV field, JSON null.
    values = [float("nan"), float("inf"), float("-inf"), None, 1.5]
    assert _csv_lines(ctor, values) == ["", "", "", "", "1.5"]
    m = Morsel.from_vectors(["v"], [Vector(ctor(values))])
    assert write_jsonl(m).decode().splitlines() == ['{"v":null}'] * 4 + ['{"v":1.5}']


if __name__ == "__main__":  # pragma: no cover
    test_float64_csv_matches_repr()
    test_float32_csv_matches_shortest_roundtrip()
    test_float64_jsonl_matches_repr()
    print("okay")
