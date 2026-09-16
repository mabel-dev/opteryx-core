"""
Native correctness tests for the DRAKEN_BOOL compare_scalar kernel
(draken/ops/bool_compare.h — bool_compare_scalar).

BACKGROUND
----------
The ops table registered only `hash` and `ordinalize` for DRAKEN_BOOL, so every
`bool_col OP literal` predicate died in draken_compare_scalar with
"unsupported type". Two ops — (b > True) and (b < False) — appeared to work
through the rugo facade, but only because row-group min/max pruning eliminated
every row group before the row-level filter ran; they would have raised the
moment a file's statistics stopped pruning. These tests exercise the kernel
directly, so nothing here can be answered by pruning.

COVERAGE (mirrors test_int64_compare.py's matrix)
  ops:         all 6 (eq / ne / gt / ge / lt / le)
  scalars:     both (True and False)
  nullability: no-null / some-null / all-null
  shapes:      dense / constant / dict — the three DrakenVector encodings
  sizes:       0 / 1 / 2..7 (tail only) / 8 (byte boundary) / 9 / 17 / large
  ordering:    SQL's — FALSE < TRUE
  3VL:         NULL op x = NULL (validity bit 0, value bit 0)
  domain:      a non-bool scalar is REJECTED, never coerced. `bool` subclasses
               `int`, so `bool_col = 5` would otherwise sail into the integer
               path and compare a stored 0/1 bit against 5.

Bit-boundary focus: `data` is a BITMAP (one bit per stored value), so the
partial trailing byte is the classic bug surface here as well.
"""

import pytest

import draken.draken_native as dn

# Op codes — same across every compare kernel.
EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5
ALL_OPS = (EQ, NE, GT, GE, LT, LE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bvec(lst):
    """Dense bool vector from list[bool | None]."""
    return dn.vector_from_bool_sequence(lst)


def bconst(value, length):
    """Constant-shape bool vector (value may be None → all-null)."""
    return dn.vector_from_bool_constant(value, length)


def bdict(values, codes, nullable=None):
    """Dict-encoded bool vector."""
    return dn.vector_from_bool_dict(values, codes, nullable)


def cmp_s(v, scalar, op):
    return v.compare_scalar(scalar, op).to_pylist()


def ref(op, a, b):
    """Reference answer: Python bool comparison, None propagating (3VL).

    Python orders False < True, which is SQL's ordering for booleans, so the
    built-in operators are a valid oracle.
    """
    if a is None:
        return None
    return {EQ: a == b, NE: a != b, GT: a > b, GE: a >= b, LT: a < b, LE: a <= b}[op]


def expect(values, scalar, op):
    return [ref(op, a, scalar) for a in values]


# Every (op, scalar) pair, for exhaustive parametrisation.
OP_SCALAR = [(op, s) for op in ALL_OPS for s in (True, False)]


# ---------------------------------------------------------------------------
# 1. Dense shape — all six ops × both scalars, over every (value, null) state
# ---------------------------------------------------------------------------

class TestDense:
    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_all_states(self, op, scalar):
        """Every input state {True, False, None} against both scalars."""
        values = [True, False, None]
        assert cmp_s(bvec(values), scalar, op) == expect(values, scalar, op)

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_no_nulls(self, op, scalar):
        values = [True, False, False, True, True, False, True, False]
        assert cmp_s(bvec(values), scalar, op) == expect(values, scalar, op)

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_all_null(self, op, scalar):
        values = [None] * 5
        assert cmp_s(bvec(values), scalar, op) == [None] * 5

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_empty(self, op, scalar):
        assert cmp_s(bvec([]), scalar, op) == []

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    @pytest.mark.parametrize("n", list(range(1, 18)))
    def test_bit_boundary_sizes(self, op, scalar, n):
        """Sizes 1..17 cover every partial-trailing-byte combination, with
        nulls interleaved so the validity bitmap's tail is exercised too."""
        values = [None if i % 3 == 2 else (i % 2 == 0) for i in range(n)]
        assert cmp_s(bvec(values), scalar, op) == expect(values, scalar, op)

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_large(self, op, scalar):
        n = 100_000
        values = [None if i % 7 == 3 else (i % 5 < 2) for i in range(n)]
        assert cmp_s(bvec(values), scalar, op) == expect(values, scalar, op)


# ---------------------------------------------------------------------------
# 2. Constant shape — data_length == 1, selection is the global zero vector
# ---------------------------------------------------------------------------

class TestConstant:
    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    @pytest.mark.parametrize("value", [True, False])
    @pytest.mark.parametrize("n", [1, 3, 8, 9, 17])
    def test_constant(self, op, scalar, value, n):
        assert cmp_s(bconst(value, n), scalar, op) == expect([value] * n, scalar, op)

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_null_constant(self, op, scalar):
        assert cmp_s(bconst(None, 6), scalar, op) == [None] * 6

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_constant_empty(self, op, scalar):
        assert cmp_s(bconst(True, 0), scalar, op) == []


# ---------------------------------------------------------------------------
# 3. Dict shape — data_length < length, selection is an owned code array
# ---------------------------------------------------------------------------

class TestDict:
    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_dict_no_nulls(self, op, scalar):
        codes = [0, 1, 1, 0, 1, 0, 0, 1, 1]
        dictionary = [False, True]
        values = [dictionary[c] for c in codes]
        assert cmp_s(bdict(dictionary, codes), scalar, op) == expect(values, scalar, op)

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_dict_with_nulls(self, op, scalar):
        codes = [1, 0, 1, 0, 1, 1, 0]
        valid = [True, False, True, True, False, True, True]
        dictionary = [False, True]
        values = [dictionary[c] if v else None for c, v in zip(codes, valid)]
        assert cmp_s(bdict(dictionary, codes, valid), scalar, op) == expect(values, scalar, op)

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_dict_reversed_dictionary(self, op, scalar):
        """Dictionary order is not value order — code 0 is TRUE here. A kernel
        that read the code instead of data[code] would pass the test above and
        fail this one."""
        codes = [0, 1, 0, 1, 1]
        dictionary = [True, False]
        values = [dictionary[c] for c in codes]
        assert cmp_s(bdict(dictionary, codes), scalar, op) == expect(values, scalar, op)

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_dict_single_entry(self, op, scalar):
        codes = [0] * 11
        assert cmp_s(bdict([True], codes), scalar, op) == expect([True] * 11, scalar, op)


# ---------------------------------------------------------------------------
# 4. Result contract — type, length, and shape-independence
# ---------------------------------------------------------------------------

class TestResultContract:
    def test_result_type_is_bool(self):
        r = bvec([True, False]).compare_scalar(True, EQ)
        assert r.type == dn.DrakenType.BOOL

    def test_result_length(self):
        r = bvec([True, False, None, True, False]).compare_scalar(False, NE)
        assert len(r) == 5

    @pytest.mark.parametrize("op,scalar", OP_SCALAR)
    def test_shapes_agree(self, op, scalar):
        """The same logical column in all three encodings must give the same
        answer — the uniform data[selection[i]] contract (CLAUDE.md §11)."""
        values = [True, False, True, False, True, False, True, False, True]
        codes = [1 if v else 0 for v in values]
        dense = cmp_s(bvec(values), scalar, op)
        dict_ = cmp_s(bdict([False, True], codes), scalar, op)
        assert dense == dict_ == expect(values, scalar, op)

        const_values = [True] * 9
        assert cmp_s(bvec(const_values), scalar, op) \
            == cmp_s(bconst(True, 9), scalar, op) \
            == expect(const_values, scalar, op)


# ---------------------------------------------------------------------------
# 5. Scalar domain — reject, never coerce
# ---------------------------------------------------------------------------

class TestScalarDomain:
    @pytest.mark.parametrize("bad", [0, 1, 5, -1, 1.0, 0.0, "true", b"true"])
    @pytest.mark.parametrize("op", ALL_OPS)
    def test_non_bool_scalar_is_rejected(self, bad, op):
        """`bool` subclasses `int`, so 0/1 would otherwise be accepted silently
        and 5 would be compared against a stored bit. Fail clean instead."""
        v = bvec([True, False, None])
        with pytest.raises(Exception) as exc:
            v.compare_scalar(bad, op)
        assert "bool" in str(exc.value).lower()

    @pytest.mark.parametrize("op", ALL_OPS)
    def test_none_scalar_is_rejected(self, op):
        """A None scalar never reaches the BOOL branch — compare_scalar's
        binding does not accept None for any type — but it must still fail
        rather than be read as FALSE."""
        v = bvec([True, False, None])
        with pytest.raises(TypeError):
            v.compare_scalar(None, op)

    @pytest.mark.parametrize("op", ALL_OPS)
    @pytest.mark.parametrize("scalar", [True, False])
    def test_bool_scalar_is_accepted(self, op, scalar):
        bvec([True, False, None]).compare_scalar(scalar, op)


# ---------------------------------------------------------------------------
# 6. The two ops that used to "work" by accident
# ---------------------------------------------------------------------------

class TestPreviouslyAccidental:
    """(b > True) and (b < False) reached the consumer as 0-row answers only
    because min/max pruning discarded every row group first. Nothing prunes
    here, so these assert the kernel's real answer."""

    def test_gt_true_is_false_on_every_valid_row(self):
        values = [True, False, None, True, False]
        assert cmp_s(bvec(values), True, GT) == [False, False, None, False, False]

    def test_lt_false_is_false_on_every_valid_row(self):
        values = [True, False, None, True, False]
        assert cmp_s(bvec(values), False, LT) == [False, False, None, False, False]

    def test_gt_false_selects_true_rows(self):
        values = [True, False, None, True, False]
        assert cmp_s(bvec(values), False, GT) == [True, False, None, True, False]

    def test_lt_true_selects_false_rows(self):
        values = [True, False, None, True, False]
        assert cmp_s(bvec(values), True, LT) == [False, True, None, False, True]
