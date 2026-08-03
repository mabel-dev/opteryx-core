"""
Native correctness tests for vector_misc: vector_log, vector_in_list.

Milestone E.14: C′ nanobind consumers replacing vector_log.pyx, vector_in_list.pyx.

IPv4 CIDR containment moved to the `<<=` / `>>=` operators over a native uint32
IPv4 column (vector_ipv4_in_cidr / draken_ipv4_in_cidr); the string-based `|`
overload this file used to cover was removed with it.
"""

import importlib.util
import math
import os
import sys

import pytest

import draken.draken_native as dn

# Load vector_misc directly by file path so opteryx/__init__.py is not executed.
# The shared library uses RTLD_GLOBAL symbols from draken_native (already loaded above).
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.join(_HERE, "..", "..", "..")
_SO = None
for _fname in os.listdir(os.path.join(_ROOT, "opteryx/compiled/nanobind")):
    if _fname.startswith("vectors") and _fname.endswith(".so"):
        _SO = os.path.join(_ROOT, "opteryx/compiled/nanobind", _fname)
        break
if _SO is None:
    pytest.skip("vector_misc.so not found; run DRAKEN_BUILD=1 make c first", allow_module_level=True)

_spec = importlib.util.spec_from_file_location("opteryx.compiled.nanobind.vectors", _SO)
_vm = importlib.util.module_from_spec(_spec)
sys.modules["opteryx.compiled.nanobind.vectors"] = _vm
_spec.loader.exec_module(_vm)

vector_in_list = _vm.vector_in_list
vector_log = _vm.vector_log


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_int(lst):
    return dn.vector_from_sequence(lst)


def make_float(lst):
    return dn.vector_float64_from_sequence(lst)


def make_str(lst):
    # For VARCHAR vectors: edge function requires bytes; encode str inputs.
    byte_lst = [None if v is None else (v.encode("utf-8") if isinstance(v, str) else v) for v in lst]
    return dn.vector_from_string_sequence(byte_lst)


def bool_to_list(bv):
    return bv.to_pylist()


def float_to_list(fv):
    return fv.to_pylist()


# ---------------------------------------------------------------------------
# vector_log — LOG(v, base) = ln(v)/ln(base)
# ---------------------------------------------------------------------------


class TestVectorLog:
    def test_log_base10(self):
        v = make_float([10.0, 100.0, 1000.0])
        base = make_float([10.0])  # constant
        result = float_to_list(vector_log(v, base))
        assert result[0] == pytest.approx(1.0, abs=1e-9)
        assert result[1] == pytest.approx(2.0, abs=1e-9)
        assert result[2] == pytest.approx(3.0, abs=1e-9)

    def test_log_natural(self):
        e = math.e
        v = make_float([1.0, e, e * e])
        base = make_float([e])
        result = float_to_list(vector_log(v, base))
        assert result[0] == pytest.approx(0.0, abs=1e-9)
        assert result[1] == pytest.approx(1.0, abs=1e-9)
        assert result[2] == pytest.approx(2.0, abs=1e-9)

    def test_log_base2(self):
        v = make_float([1.0, 2.0, 4.0, 8.0])
        base = make_float([2.0])
        result = float_to_list(vector_log(v, base))
        assert result[0] == pytest.approx(0.0, abs=1e-9)
        assert result[1] == pytest.approx(1.0, abs=1e-9)
        assert result[2] == pytest.approx(2.0, abs=1e-9)
        assert result[3] == pytest.approx(3.0, abs=1e-9)

    def test_log_ieee_zero(self):
        v = make_float([0.0])
        base = make_float([10.0])
        result = float_to_list(vector_log(v, base))
        assert math.isinf(result[0]) and result[0] < 0  # -inf

    def test_log_ieee_negative(self):
        v = make_float([-1.0])
        base = make_float([10.0])
        result = float_to_list(vector_log(v, base))
        assert math.isnan(result[0])

    def test_log_ieee_one(self):
        v = make_float([1.0])
        base = make_float([10.0])
        result = float_to_list(vector_log(v, base))
        assert result[0] == pytest.approx(0.0, abs=1e-15)

    def test_log_null_tvl(self):
        v = make_float([None, 10.0])
        base = make_float([10.0])
        result = float_to_list(vector_log(v, base))
        assert result[0] is None
        assert result[1] == pytest.approx(1.0, abs=1e-9)

    def test_log_int_inputs(self):
        v = make_int([1, 10, 100])
        base = make_int([10])
        result = float_to_list(vector_log(v, base))
        assert result[0] == pytest.approx(0.0, abs=1e-9)
        assert result[1] == pytest.approx(1.0, abs=1e-9)
        assert result[2] == pytest.approx(2.0, abs=1e-9)

    def test_log_broadcast_base(self):
        v = make_float([10.0, 100.0])
        base = make_float([10.0])  # scalar broadcast
        result = float_to_list(vector_log(v, base))
        assert len(result) == 2

    def test_log_broadcast_value(self):
        v = make_float([100.0])  # scalar broadcast
        base = make_float([10.0, 100.0])
        result = float_to_list(vector_log(v, base))
        assert len(result) == 2
        assert result[0] == pytest.approx(2.0, abs=1e-9)
        assert result[1] == pytest.approx(1.0, abs=1e-9)


# ---------------------------------------------------------------------------
# vector_in_list — membership via hash (§1 exception: hash-only)
# ---------------------------------------------------------------------------


class TestVectorInList:
    def test_int_basic(self):
        v = make_int([1, 2, 3, 4, 5])
        result = bool_to_list(vector_in_list(v, [2, 4]))
        assert result == [False, True, False, True, False]

    def test_int_empty_list(self):
        v = make_int([1, 2, 3])
        result = bool_to_list(vector_in_list(v, []))
        assert result == [False, False, False]

    def test_int_negate(self):
        v = make_int([1, 2, 3, 4, 5])
        result = bool_to_list(vector_in_list(v, [2, 4], True))
        assert result == [True, False, True, False, True]

    def test_int_null_row(self):
        v = make_int([None, 2, 3])
        result = bool_to_list(vector_in_list(v, [2]))
        assert result[0] is None  # null row stays null
        assert result[1] is True
        assert result[2] is False

    def test_string_basic(self):
        v = make_str([b"apple", b"banana", b"cherry"])
        result = bool_to_list(vector_in_list(v, [b"banana", b"cherry"]))
        assert result == [False, True, True]

    def test_string_null_row(self):
        v = make_str([None, b"hello", b"world"])
        result = bool_to_list(vector_in_list(v, [b"hello"]))
        assert result[0] is None
        assert result[1] is True
        assert result[2] is False

    def test_string_negate(self):
        v = make_str([b"a", b"b", b"c"])
        result = bool_to_list(vector_in_list(v, [b"b"], True))
        assert result == [True, False, True]

    def test_float_basic(self):
        v = make_float([1.0, 2.5, 3.0])
        result = bool_to_list(vector_in_list(v, [2.5, 3.0]))
        assert result == [False, True, True]

    def test_none_in_list_skipped(self):
        v = make_int([1, 2, 3])
        # None in the literals should be skipped — does not match any row
        result = bool_to_list(vector_in_list(v, [1, None, 3]))
        assert result == [True, False, True]


