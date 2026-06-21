"""
Phase 1 — row-routing scatter kernel (cxx_scatter / CxxMorsel.scatter).

The kernel partitions a morsel into W disjoint sub-morsels by hash(key) % W,
reusing the SAME keying hash so every occurrence of a key routes to one bin.
These tests assert the invariants the parallel grouped aggregate depends on:

  - multiset preservation: the W bins together are exactly the input rows;
  - disjointness BY KEY: all rows sharing a key land in one bin (this is what
    makes finalize a concat, not a merge);
  - NULL keys land in a single bin (all NULLs hash identically);
  - multi-column composite keys route on the tuple;
  - W=1 is the identity routing (the path that runs when MAX_EXECUTION_WORKERS=1).

Correctness of the resulting aggregates at scale is covered by make q / tpch /
clickbench once the engine is wired (Phase 2); here we test the router alone.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.interop.vector_sequence import vector_from_sequence

DT = dn.DrakenType


def _morsel(names, vs):
    return dn.cxx_morsel_from_vectors(vs, names)


def _bin_rows(cxx_bin, ncols):
    """All rows of a sub-morsel as a list of tuples (one per row)."""
    vecs = cxx_bin.to_vectors()
    cols = [vecs[i].to_pylist() for i in range(ncols)]
    n = len(cols[0]) if cols else 0
    return [tuple(c[r] for c in cols) for r in range(n)]


def _scatter(cxx, key_cols, W):
    return cxx.scatter(key_cols, W)


def test_multiset_preserved_and_disjoint_by_key_single_col():
    # Repeated keys so disjointness is meaningful; 1000 rows across ~13 keys.
    keys = [(i * 7) % 13 for i in range(1000)]
    vals = list(range(1000))
    vs = [
        vector_from_sequence(keys, dtype=DT.INT64),
        vector_from_sequence(vals, dtype=DT.INT64),
    ]
    names = [b"k", b"v"]
    W = 8
    bins = _scatter(_morsel(names, vs), [0], W)
    assert len(bins) == W

    all_rows = []
    key_to_bin = {}
    for b, cxx_bin in enumerate(bins):
        rows = _bin_rows(cxx_bin, 2)
        all_rows.extend(rows)
        for k, _v in rows:
            # every occurrence of a key must route to exactly one bin
            assert key_to_bin.setdefault(k, b) == b, f"key {k} split across bins"

    # multiset of (k, v) rows is exactly the input — nothing lost, nothing duplicated
    assert sorted(all_rows) == sorted(zip(keys, vals))


def test_null_keys_land_in_one_bin():
    keys = [None, 5, None, 5, None, 7, None]
    vs = [
        vector_from_sequence(keys, dtype=DT.INT64),
        vector_from_sequence(list(range(len(keys))), dtype=DT.INT64),
    ]
    names = [b"k", b"v"]
    bins = _scatter(_morsel(names, vs), [0], 8)

    null_bins = set()
    total = 0
    for b, cxx_bin in enumerate(bins):
        rows = _bin_rows(cxx_bin, 2)
        total += len(rows)
        if any(k is None for k, _ in rows):
            null_bins.add(b)
    assert len(null_bins) == 1, "all NULL keys must route to a single bin"
    assert total == len(keys)


def test_multi_column_composite_key():
    a = [i % 3 for i in range(600)]
    b = [i % 5 for i in range(600)]
    v = list(range(600))
    vs = [
        vector_from_sequence(a, dtype=DT.INT64),
        vector_from_sequence(b, dtype=DT.INT64),
        vector_from_sequence(v, dtype=DT.INT64),
    ]
    names = [b"a", b"b", b"v"]
    W = 4
    bins = _scatter(_morsel(names, vs), [0, 1], W)

    all_rows = []
    tuple_to_bin = {}
    for bn, cxx_bin in enumerate(bins):
        rows = _bin_rows(cxx_bin, 3)
        all_rows.extend(rows)
        for ka, kb, _vv in rows:
            assert tuple_to_bin.setdefault((ka, kb), bn) == bn, "composite key split"
    assert sorted(all_rows) == sorted(zip(a, b, v))


def test_string_keys_route_and_preserve():
    keys = [b"alpha", b"beta", b"alpha", b"gamma", b"beta", b"alpha"]
    vs = [
        vector_from_sequence(keys, dtype=DT.VARCHAR),
        vector_from_sequence(list(range(len(keys))), dtype=DT.INT64),
    ]
    names = [b"k", b"v"]
    src = _morsel(names, vs)
    expected = _bin_rows(src, 2)  # read source identically — representation-agnostic
    bins = _scatter(src, [0], 8)
    all_rows = []
    key_to_bin = {}
    for b, cxx_bin in enumerate(bins):
        rows = _bin_rows(cxx_bin, 2)
        all_rows.extend(rows)
        for k, _v in rows:
            assert key_to_bin.setdefault(k, b) == b, f"string key {k!r} split"
    assert sorted(all_rows) == sorted(expected)


def test_w1_is_identity_routing():
    keys = [(i * 3) % 11 for i in range(200)]
    vs = [
        vector_from_sequence(keys, dtype=DT.INT64),
        vector_from_sequence(list(range(200)), dtype=DT.INT64),
    ]
    names = [b"k", b"v"]
    bins = _scatter(_morsel(names, vs), [0], 1)
    assert len(bins) == 1
    rows = _bin_rows(bins[0], 2)
    # single worker sees every row, in order — same path runs, just one slice
    assert rows == list(zip(keys, range(200)))


def test_empty_morsel_yields_empty_bins():
    vs = [
        vector_from_sequence([], dtype=DT.INT64),
        vector_from_sequence([], dtype=DT.INT64),
    ]
    names = [b"k", b"v"]
    W = 4
    bins = _scatter(_morsel(names, vs), [0], W)
    assert len(bins) == W
    assert all(len(_bin_rows(b, 2)) == 0 for b in bins)


if __name__ == "__main__":
    test_multiset_preserved_and_disjoint_by_key_single_col()
    test_null_keys_land_in_one_bin()
    test_multi_column_composite_key()
    test_string_keys_route_and_preserve()
    test_w1_is_identity_routing()
    test_empty_morsel_yields_empty_bins()
    print("✅ scatter kernel — disjoint-by-key routing, multiset-preserving, NULL/multi-col/W1")
