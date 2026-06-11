"""WP-13 — single-column key path: null semantics, hash bit-parity, differential.

These tests are written BEFORE the implementation (tests-first).  The null-key
inner-join cases assert the CORRECT SQL semantics (null keys match NOTHING), so
they are RED on the pre-WP-13 code (which has the OPEN NULL=NULL P0) and turn
GREEN once the validity-based + k-probe path lands.

Everything here drives the compiled join kernels directly so the test is
independent of SQL planning / VALUES-with-NULL quirks:

    build_side_carchar_morsel_map(left_morsel, [key], plf)
    inner_join_carchar_morsel_aligned(left_morsel, right_morsel, [key], ht)

and constructs morsels with explicit encoding shapes via the draken factories.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from draken.morsels.morsel import Morsel
from draken.draken_native import (
    vector_from_sequence,
    vector_from_dict,
    vector_from_constant,
)
from opteryx.operators._operators import (
    build_side_carchar_morsel_map,
    inner_join_carchar_morsel_aligned,
)

KEY = b"k"
PAY_L = b"lp"
PAY_R = b"rp"
MASK = (1 << 64) - 1


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _dense(values):
    return vector_from_sequence(list(values))


def _dict(uniques, codes, nullable=None):
    if nullable is None:
        return vector_from_dict(list(uniques), list(codes))
    return vector_from_dict(list(uniques), list(codes), list(nullable))


def _const(value, n):
    return vector_from_constant(value, n)


def _morsel(key_vec, n):
    """A 2-column morsel: the key column plus a dense payload = row index."""
    pay = vector_from_sequence(list(range(n)))
    return Morsel.from_vectors([KEY, PAY_L], [key_vec, pay])


def _morsel_r(key_vec, n):
    pay = vector_from_sequence(list(range(n)))
    return Morsel.from_vectors([KEY, PAY_R], [key_vec, pay])


def _result_pairs(left_morsel, right_morsel):
    """Run the kernels and return the join result as a SET of (lp, rp) payload
    pairs — order-independent, which is the correct contract for a join."""
    ht = build_side_carchar_morsel_map(left_morsel, [KEY], 0.35)
    out = inner_join_carchar_morsel_aligned(left_morsel, right_morsel, [KEY], ht)
    if out is None:
        return set()
    lp = out.column(PAY_L).to_pylist()
    rp = out.column(PAY_R).to_pylist()
    return set(zip(lp, rp))


def _reference_pairs(left_keys, right_keys):
    """Pure-Python inner-join reference. None == SQL NULL, matches nothing.
    Returns set of (left_row_index, right_row_index)."""
    out = set()
    for li, lk in enumerate(left_keys):
        if lk is None:
            continue
        for ri, rk in enumerate(right_keys):
            if rk is None:
                continue
            if lk == rk:
                out.add((li, ri))
    return out


# ---------------------------------------------------------------------------
# 1. Hash bit-parity — hash() (per-row) vs hash_keys() (shaped), uint64-exact.
#    Build uses hash(); probe will use hash_keys(); they MUST interoperate.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name, vec_factory",
    [
        ("dense_no_null",  lambda: _dense([10, 20, 30, 20, 10])),
        ("dense_null",     lambda: _dense([10, None, 30, None, 10])),
        ("dict_no_null",   lambda: _dict([100, 200, 300], [0, 1, 0, 2, 1])),
        ("dict_null",      lambda: _dict([100, 200, 300], [0, 1, 0, 2, 1],
                                         [True, False, True, True, False])),
        ("const_value",    lambda: _const(42, 5)),
        ("const_null",     lambda: _const(None, 5)),
    ],
)
def test_hash_bit_parity(name, vec_factory):
    n = 5
    m = Morsel.from_vectors([KEY], [vec_factory()])
    per_row = [h & MASK for h in m.hash([KEY])]
    shaped = [v & MASK for v in m.hash_keys([KEY]).to_pylist()]
    assert per_row == shaped, f"{name}: hash() and hash_keys() disagree bit-exactly"


# ---------------------------------------------------------------------------
# 2. Null-key inner-join semantics (CORRECT semantics; null matches nothing).
# ---------------------------------------------------------------------------

def test_null_on_build_side_only():
    # left has a null key, right does not. null must not match.
    left = _morsel(_dense([1, None, 2]), 3)
    right = _morsel_r(_dense([1, 2, 3]), 3)
    got = _result_pairs(left, right)
    ref = _reference_pairs([1, None, 2], [1, 2, 3])
    assert got == ref
    # explicit: row 1 (null) contributes nothing
    assert all(lp != 1 for (lp, rp) in got)


def test_null_on_probe_side_only():
    left = _morsel(_dense([1, 2, 3]), 3)
    right = _morsel_r(_dense([None, 2, 3]), 3)
    got = _result_pairs(left, right)
    ref = _reference_pairs([1, 2, 3], [None, 2, 3])
    assert got == ref
    assert all(rp != 0 for (lp, rp) in got)  # right row 0 is null


def test_null_on_both_sides():
    left = _morsel(_dense([1, None, 2]), 3)
    right = _morsel_r(_dense([None, 2, 3]), 3)
    got = _result_pairs(left, right)
    ref = _reference_pairs([1, None, 2], [None, 2, 3])
    # Only 2==2 → exactly one pair (left row 2, right row 1)
    assert got == ref == {(2, 1)}


def test_dict_key_with_null_slot():
    # dict-encoded key with an actually-null row mid-stream.
    left = _morsel(_dict([10, 20], [0, 1, 0], [True, False, True]), 3)   # 10, NULL, 10
    right = _morsel_r(_dict([10, 20], [1, 0, 1], [True, True, True]), 3)  # 20, 10, 20
    got = _result_pairs(left, right)
    ref = _reference_pairs([10, None, 10], [20, 10, 20])
    # left rows 0,2 (==10) match right row 1 (==10): {(0,1),(2,1)}
    assert got == ref == {(0, 1), (2, 1)}


def test_dict_unreferenced_null_slot_hazard():
    # nullable=all-True: a null slot may exist in the shaped hash even though no
    # row is actually null. Must NOT spuriously drop or match rows.
    left = _morsel(_dict([10, 20], [0, 1, 1], [True, True, True]), 3)    # 10,20,20
    right = _morsel_r(_dict([10, 20], [1, 0, 1], [True, True, True]), 3)  # 20,10,20
    got = _result_pairs(left, right)
    ref = _reference_pairs([10, 20, 20], [20, 10, 20])
    assert got == ref


def test_constant_key_matched():
    # k=1 constant key on both sides, value matches.
    left = _morsel(_const(7, 4), 4)
    right = _morsel_r(_const(7, 3), 3)
    got = _result_pairs(left, right)
    ref = _reference_pairs([7, 7, 7, 7], [7, 7, 7])
    assert got == ref
    assert len(got) == 12  # full 4x3 cross on the single matching key


def test_constant_key_unmatched():
    left = _morsel(_const(7, 4), 4)
    right = _morsel_r(_const(9, 3), 3)
    assert _result_pairs(left, right) == set()


def test_constant_null_key():
    # constant NULL on probe side → matches nothing.
    left = _morsel(_const(7, 4), 4)
    right = _morsel_r(_const(None, 3), 3)
    assert _result_pairs(left, right) == set()


def test_one_to_many_expansion():
    # one build key matching many probe rows AND vice versa, on a compressed key.
    left = _morsel(_dict([5], [0, 0, 0], None), 3)         # 5,5,5  (constant-ish dict)
    right = _morsel_r(_dict([5], [0, 0], None), 2)          # 5,5
    got = _result_pairs(left, right)
    ref = _reference_pairs([5, 5, 5], [5, 5])
    assert got == ref
    assert len(got) == 6


# ---------------------------------------------------------------------------
# 3. Differential — randomized dense/dict/constant keys with nulls; the kernel
#    result SET must equal the pure-Python reference SET.
# ---------------------------------------------------------------------------

def _random_key_vector(rng, n, shape):
    """Return (vec, py_keys) where py_keys is the logical key list (None=null)."""
    domain = [None, 0, 1, 2, 3]  # small domain → lots of repeats + collisions
    if shape == "dense":
        keys = [rng.choice(domain) for _ in range(n)]
        return _dense(keys), keys
    if shape == "constant":
        val = rng.choice(domain)
        keys = [val] * n
        if val is None:
            return _const(None, n), keys
        return _const(val, n), keys
    # dict
    uniques = [1, 2, 3]
    keys = []
    codes = []
    nullable = []
    for _ in range(n):
        pick = rng.choice(domain)
        if pick is None or pick == 0:
            # represent as null (code irrelevant) — use code 0, validity False
            codes.append(0)
            nullable.append(False)
            keys.append(None)
        else:
            codes.append(pick - 1)
            nullable.append(True)
            keys.append(pick)
    return _dict(uniques, codes, nullable), keys


@pytest.mark.parametrize("seed", range(25))
def test_differential_random(seed):
    rng = random.Random(seed)
    shapes = ["dense", "dict", "constant"]
    lshape = rng.choice(shapes)
    rshape = rng.choice(shapes)
    ln = rng.randint(1, 30)
    rn = rng.randint(1, 30)

    lvec, lkeys = _random_key_vector(rng, ln, lshape)
    rvec, rkeys = _random_key_vector(rng, rn, rshape)

    left = _morsel(lvec, ln)
    right = _morsel_r(rvec, rn)

    got = _result_pairs(left, right)
    ref = _reference_pairs(lkeys, rkeys)
    assert got == ref, (
        f"seed={seed} lshape={lshape} rshape={rshape}\n"
        f"lkeys={lkeys}\nrkeys={rkeys}\nref-got={ref - got}\ngot-ref={got - ref}"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
