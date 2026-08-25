"""Composite join keys must not match NULL to NULL.

SQL says NULL = NULL is unknown, so a row whose key is partly NULL matches
nothing — `(2, NULL)` and `(2, NULL)` are not equal. Single-column keys were
fixed by filtering on key-column validity; this pins the MULTI-COLUMN case,
which used to compare a composite HASH and so matched a partially-null row
against any other row that mixed to the same value.

The stakes are not only a spurious result row. MERGE keys on this: a source row
that should take NOT MATCHED taking MATCHED instead marks an unrelated target
row deleted and replaces it. That is why this is a differential test against a
reference implementation over random data rather than a handful of examples.
"""

import os
import random
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import opteryx  # noqa: E402


def _write(directory, rows, columns):
    """One relation, as a directory holding a parquet file — the shape the
    filesystem connector reads (a bare file path is not a dataset)."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    os.makedirs(directory, exist_ok=True)
    morsel = Morsel()
    for index, name in enumerate(columns):
        morsel.append_vector(
            name, vector_from_sequence([r[index] for r in rows], dtype="INTEGER")
        )
    with open(os.path.join(directory, "part.parquet"), "wb") as f:
        f.write(write_parquet(morsel, compression="zstd"))
    return directory


def _make(seed, n_left=40, n_right=40, domain=4, null_rate=0.35):
    """Rows over a SMALL domain with frequent NULLs — the shape that makes
    collisions and partial nulls common rather than incidental."""
    rng = random.Random(seed)

    def value():
        return None if rng.random() < null_rate else rng.randrange(domain)

    left = [(i, value(), value()) for i in range(n_left)]
    right = [(i, value(), value()) for i in range(n_right)]
    return left, right


def _reference_inner(left, right):
    """SQL semantics, stated plainly: a pair matches when EVERY key comparison
    is TRUE. Any NULL operand makes its comparison unknown, so the pair drops."""
    out = []
    for lid, lk1, lk2 in left:
        for rid, rk1, rk2 in right:
            if lk1 is None or rk1 is None or lk2 is None or rk2 is None:
                continue
            if lk1 == rk1 and lk2 == rk2:
                out.append((lid, rid))
    return sorted(out)


def _engine_inner(left_path, right_path):
    sql = (
        f"SELECT l.id, r.id FROM '{left_path}' l "
        f"INNER JOIN '{right_path}' r ON l.k1 = r.k1 AND l.k2 = r.k2"
    )
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            row = morsel[i]
            out.append((row[0], row[1]))
    return sorted(out)


@pytest.mark.parametrize("seed", list(range(12)))
def test_composite_key_join_matches_the_reference(seed, tmp_path):
    left, right = _make(seed)
    lp = str(tmp_path / f"l{seed}")
    rp = str(tmp_path / f"r{seed}")
    _write(lp, left, ["id", "k1", "k2"])
    _write(rp, right, ["id", "k1", "k2"])

    assert _engine_inner(lp, rp) == _reference_inner(left, right)


def test_a_partially_null_key_matches_nothing_at_all(tmp_path):
    """The exact shape the old hash-sentinel path got wrong: identical rows
    whose key is partly NULL. They must not match each other, and must not
    match a row that shares only the non-null half."""
    rows = [(1, 2, None), (2, 2, None), (3, 2, 7)]
    lp = str(tmp_path / "l")
    rp = str(tmp_path / "r")
    _write(lp, rows, ["id", "k1", "k2"])
    _write(rp, rows, ["id", "k1", "k2"])

    # Only (3, 2, 7) has a fully-known key, so it matches only itself.
    assert _engine_inner(lp, rp) == [(3, 3)]


def test_all_null_keys_produce_no_rows(tmp_path):
    rows = [(1, None, None), (2, None, None)]
    lp = str(tmp_path / "l")
    rp = str(tmp_path / "r")
    _write(lp, rows, ["id", "k1", "k2"])
    _write(rp, rows, ["id", "k1", "k2"])
    assert _engine_inner(lp, rp) == []
