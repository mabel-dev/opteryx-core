"""
Native correctness tests for Milestone E.16: vector_replace + vector_cosine_similarity +
vector_cosine_distance — mixed string/fp16 ops, pure nanobind C++.

Coverage:
  vector_replace:
    basic replace: "hello" needle="l" repl="L" → "heLLo"
    no match: needle absent → original returned
    replacement longer than needle
    replacement shorter than needle (including empty replacement → deletion)
    empty needle: no-op (PostgreSQL convention)
    full string match
    multiple non-overlapping occurrences
    long strings (>12 bytes — extern slot path)
    null haystack → null output row
    null needle → null output row
    null replacement → null output row
    non-Vector input → TypeError
    length mismatch → invalid_argument

  vector_cosine_similarity:
    orthogonal vectors → 0.0
    identical vectors → 1.0
    opposite vectors → -1.0
    unit vector vs itself → 1.0
    null in a → null output row
    null in b → null output row
    both null → null output row
    zero-norm vector → NaN output row
    dimension mismatch → ValueError / invalid_argument
    non-VECTOR_FP16 input → TypeError
    batch: multiple rows

  vector_cosine_distance:
    identical vectors → 0.0  (1 - 1 = 0)
    orthogonal vectors → 1.0  (1 - 0 = 1)
    opposite vectors → 2.0   (1 - (-1) = 2)
    null propagation
    zero-norm → NaN
"""

import importlib.util
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Module loading (spec_from_file_location pattern — no opteryx import)
# ---------------------------------------------------------------------------

def _load_module(name, rel_path):
    base = os.path.join(os.path.dirname(__file__), "..", "..", "..", rel_path)
    import glob
    candidates = glob.glob(base + "*.so") + glob.glob(base + "*.pyd")
    if not candidates:
        raise FileNotFoundError(f"Compiled module not found: {base}*.so")
    spec = importlib.util.spec_from_file_location(name, candidates[0])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_misc2 = _load_module(
    "vector_string_misc2",
    "opteryx/compiled/nanobind/vector_string_misc2.cpython",
)
vector_replace             = _misc2.vector_replace
vector_cosine_similarity   = _misc2.vector_cosine_similarity
vector_cosine_distance     = _misc2.vector_cosine_distance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make(lst):
    """Build a VARCHAR Vector from a Python list of strings/None."""
    return dn.vector_from_string_sequence(lst)


def make_fp16(rows, dim=None):
    """Build a VECTOR_FP16 Vector from a list of float lists / None."""
    if dim is None:
        for r in rows:
            if r is not None:
                dim = len(r)
                break
        if dim is None:
            raise ValueError("cannot infer dimension")
    return dn.vector_fp16_from_sequence(rows, dim)


def fp16_tol(val, rel=1e-3, abs_floor=1e-6):
    return max(abs(val) * rel, abs_floor)


# ---------------------------------------------------------------------------
# vector_replace
# ---------------------------------------------------------------------------

class TestVectorReplace:
    def test_basic_replace_l_to_upper(self):
        res = vector_replace(make(["hello"]), make(["l"]), make(["L"]))
        assert res.to_pylist() == ["heLLo"]

    def test_no_match_returns_original(self):
        res = vector_replace(make(["hello"]), make(["x"]), make(["Z"]))
        assert res.to_pylist() == ["hello"]

    def test_replacement_longer_than_needle(self):
        res = vector_replace(make(["abc"]), make(["b"]), make(["BBB"]))
        assert res.to_pylist() == ["aBBBc"]

    def test_replacement_shorter_than_needle(self):
        res = vector_replace(make(["aabba"]), make(["bb"]), make(["B"]))
        assert res.to_pylist() == ["aaBa"]

    def test_empty_replacement_deletes_needle(self):
        res = vector_replace(make(["hello"]), make(["l"]), make([""]))
        assert res.to_pylist() == ["heo"]

    def test_empty_needle_is_noop(self):
        # Empty needle: no-op (PostgreSQL convention).
        res = vector_replace(make(["hello"]), make([""]), make(["X"]))
        assert res.to_pylist() == ["hello"]

    def test_empty_needle_empty_replacement_is_noop(self):
        res = vector_replace(make(["hello"]), make([""]), make([""]))
        assert res.to_pylist() == ["hello"]

    def test_full_string_match(self):
        res = vector_replace(make(["abc"]), make(["abc"]), make(["XYZ"]))
        assert res.to_pylist() == ["XYZ"]

    def test_multiple_occurrences(self):
        res = vector_replace(make(["ababab"]), make(["ab"]), make(["X"]))
        assert res.to_pylist() == ["XXX"]

    def test_non_overlapping_only(self):
        # "aaa" with needle "aa" → replace first "aa" only (left-to-right, non-overlapping)
        res = vector_replace(make(["aaa"]), make(["aa"]), make(["B"]))
        assert res.to_pylist() == ["Ba"]

    def test_long_string_extern_slot(self):
        # >12 bytes — exercises extern slot path
        haystack = "abcdefghijklmnopqrstuvwxyz"
        expected = "abcdefghijklmXopqrstuvwxyz"
        res = vector_replace(make([haystack]), make(["n"]), make(["X"]))
        assert res.to_pylist() == [expected]

    def test_long_string_no_match(self):
        haystack = "abcdefghijklmnopqrstuvwxyz"
        res = vector_replace(make([haystack]), make(["Z"]), make(["X"]))
        assert res.to_pylist() == [haystack]

    def test_batch(self):
        hay = make(["hello", "world", "foo"])
        ndl = make(["l",     "o",     "x"])
        rep = make(["L",     "0",     "Y"])
        assert vector_replace(hay, ndl, rep).to_pylist() == ["heLLo", "w0rld", "foo"]

    def test_null_haystack_is_null_output(self):
        res = vector_replace(make([None, "abc"]), make(["a", "a"]), make(["X", "X"]))
        lst = res.to_pylist()
        assert lst[0] is None
        assert lst[1] == "Xbc"

    def test_null_needle_is_null_output(self):
        res = vector_replace(make(["abc", "abc"]), make([None, "a"]), make(["X", "X"]))
        lst = res.to_pylist()
        assert lst[0] is None
        assert lst[1] == "Xbc"

    def test_null_replacement_is_null_output(self):
        res = vector_replace(make(["abc", "abc"]), make(["a", "a"]), make([None, "X"]))
        lst = res.to_pylist()
        assert lst[0] is None
        assert lst[1] == "Xbc"

    def test_all_null(self):
        res = vector_replace(make([None, None]), make([None, None]), make([None, None]))
        assert res.to_pylist() == [None, None]

    def test_output_type_is_varchar(self):
        res = vector_replace(make(["abc"]), make(["a"]), make(["X"]))
        assert "VARCHAR" in str(res.type)

    def test_empty_input(self):
        res = vector_replace(make([]), make([]), make([]))
        assert res.to_pylist() == []

    def test_non_vector_haystack_raises(self):
        with pytest.raises(TypeError):
            vector_replace("not_a_vector", make(["a"]), make(["b"]))

    def test_length_mismatch_raises(self):
        with pytest.raises(Exception):
            vector_replace(make(["a", "b"]), make(["a"]), make(["X", "X"]))


# ---------------------------------------------------------------------------
# vector_cosine_similarity
# ---------------------------------------------------------------------------

class TestCosineSimilarity:
    def test_orthogonal_vectors(self):
        a = make_fp16([[1.0, 0.0]])
        b = make_fp16([[0.0, 1.0]])
        result = vector_cosine_similarity(a, b).to_pylist()
        assert abs(result[0]) < 1e-3

    def test_identical_vectors(self):
        a = make_fp16([[1.0, 2.0, 3.0]])
        result = vector_cosine_similarity(a, a).to_pylist()
        assert abs(result[0] - 1.0) < 1e-3

    def test_opposite_vectors(self):
        a = make_fp16([[1.0, 0.0]])
        b = make_fp16([[-1.0, 0.0]])
        result = vector_cosine_similarity(a, b).to_pylist()
        assert abs(result[0] - (-1.0)) < 1e-3

    def test_unit_vector(self):
        # Unit vector dotted with itself = 1
        v = make_fp16([[1.0, 0.0, 0.0]])
        result = vector_cosine_similarity(v, v).to_pylist()
        assert abs(result[0] - 1.0) < 1e-3

    def test_zero_norm_returns_nan(self):
        a = make_fp16([[0.0, 0.0]])
        b = make_fp16([[1.0, 0.0]])
        result = vector_cosine_similarity(a, b).to_pylist()
        assert result[0] is None or math.isnan(result[0])

    def test_both_zero_norm_returns_nan(self):
        a = make_fp16([[0.0, 0.0]])
        result = vector_cosine_similarity(a, a).to_pylist()
        assert result[0] is None or math.isnan(result[0])

    def test_null_a_is_null_output(self):
        a = dn.vector_fp16_from_sequence([None, [1.0, 0.0]], 2)
        b = make_fp16([[1.0, 0.0], [1.0, 0.0]])
        result = vector_cosine_similarity(a, b).to_pylist()
        assert result[0] is None
        assert abs(result[1] - 1.0) < 1e-3

    def test_null_b_is_null_output(self):
        a = make_fp16([[1.0, 0.0], [1.0, 0.0]])
        b = dn.vector_fp16_from_sequence([[1.0, 0.0], None], 2)
        result = vector_cosine_similarity(a, b).to_pylist()
        assert abs(result[0] - 1.0) < 1e-3
        assert result[1] is None

    def test_both_null_is_null_output(self):
        a = dn.vector_fp16_from_sequence([None], 2)
        b = dn.vector_fp16_from_sequence([None], 2)
        result = vector_cosine_similarity(a, b).to_pylist()
        assert result[0] is None

    def test_batch(self):
        a = make_fp16([[1.0, 0.0], [1.0, 0.0], [0.0, 1.0]])
        b = make_fp16([[1.0, 0.0], [0.0, 1.0], [0.0, 1.0]])
        result = vector_cosine_similarity(a, b).to_pylist()
        assert abs(result[0] - 1.0) < 1e-3   # identical
        assert abs(result[1] - 0.0) < 1e-3   # orthogonal
        assert abs(result[2] - 1.0) < 1e-3   # identical

    def test_output_type_is_float64(self):
        a = make_fp16([[1.0, 0.0]])
        result = vector_cosine_similarity(a, a)
        assert "FLOAT64" in str(result.type)

    def test_empty_vectors(self):
        a = dn.vector_fp16_from_sequence([], 2)
        b = dn.vector_fp16_from_sequence([], 2)
        result = vector_cosine_similarity(a, b)
        assert result.to_pylist() == []

    def test_dimension_mismatch_raises(self):
        a = make_fp16([[1.0, 0.0]])
        b = make_fp16([[1.0, 0.0, 0.0]])
        with pytest.raises(Exception):
            vector_cosine_similarity(a, b)

    def test_non_fp16_input_raises(self):
        a = make_fp16([[1.0, 0.0]])
        with pytest.raises(TypeError):
            vector_cosine_similarity(make(["hello"]), a)

    def test_length_mismatch_raises(self):
        a = make_fp16([[1.0, 0.0], [0.0, 1.0]])
        b = make_fp16([[1.0, 0.0]])
        with pytest.raises(Exception):
            vector_cosine_similarity(a, b)

    def test_high_dimension(self):
        import random
        random.seed(99)
        dim = 128
        ra = [random.gauss(0, 1) for _ in range(dim)]
        rb = [random.gauss(0, 1) for _ in range(dim)]
        a = make_fp16([ra])
        b = make_fp16([rb])
        result = vector_cosine_similarity(a, b).to_pylist()
        assert result[0] is not None
        assert -1.1 < result[0] < 1.1

    def test_broadcast_constant_shape(self):
        # Length-1 vectors used as broadcast constant — lengths must match for this API.
        a = make_fp16([[1.0, 0.0]])
        b = make_fp16([[1.0, 0.0]])
        result = vector_cosine_similarity(a, b).to_pylist()
        assert abs(result[0] - 1.0) < 1e-3


# ---------------------------------------------------------------------------
# vector_cosine_distance
# ---------------------------------------------------------------------------

class TestCosineDistance:
    def test_identical_vectors_zero_distance(self):
        a = make_fp16([[1.0, 0.0]])
        result = vector_cosine_distance(a, a).to_pylist()
        assert abs(result[0] - 0.0) < 1e-3

    def test_orthogonal_vectors_unit_distance(self):
        a = make_fp16([[1.0, 0.0]])
        b = make_fp16([[0.0, 1.0]])
        result = vector_cosine_distance(a, b).to_pylist()
        assert abs(result[0] - 1.0) < 1e-3

    def test_opposite_vectors_two_distance(self):
        a = make_fp16([[1.0, 0.0]])
        b = make_fp16([[-1.0, 0.0]])
        result = vector_cosine_distance(a, b).to_pylist()
        assert abs(result[0] - 2.0) < 1e-3

    def test_null_propagation(self):
        a = dn.vector_fp16_from_sequence([None, [1.0, 0.0]], 2)
        b = make_fp16([[1.0, 0.0], [1.0, 0.0]])
        result = vector_cosine_distance(a, b).to_pylist()
        assert result[0] is None
        assert abs(result[1] - 0.0) < 1e-3

    def test_zero_norm_distance_is_nan(self):
        a = make_fp16([[0.0, 0.0]])
        b = make_fp16([[1.0, 0.0]])
        result = vector_cosine_distance(a, b).to_pylist()
        assert result[0] is None or math.isnan(result[0])

    def test_output_type_is_float64(self):
        a = make_fp16([[1.0, 0.0]])
        result = vector_cosine_distance(a, a)
        assert "FLOAT64" in str(result.type)

    def test_dimension_mismatch_raises(self):
        a = make_fp16([[1.0, 0.0]])
        b = make_fp16([[1.0, 0.0, 0.0]])
        with pytest.raises(Exception):
            vector_cosine_distance(a, b)

    def test_non_fp16_raises(self):
        a = make_fp16([[1.0, 0.0]])
        with pytest.raises(TypeError):
            vector_cosine_distance(make(["hello"]), a)
