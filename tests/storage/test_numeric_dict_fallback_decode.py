# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression: numeric column chunks with dictionary FALLBACK (mixed
RLE_DICTIONARY + PLAIN data pages — what Arrow / Spark / DuckDB emit once a
dictionary outgrows its page-size limit).

The reader used to re-intern every PLAIN-page value back into the dictionary
via a per-value hash probe to preserve the Dict vector shape. That was the
dominant scan cost on high-NDV columns (~40ns/value) and carried a latent
correctness bug: `code_width` is frozen at dict-page decode, so interning
that grew the dictionary past a packed-width boundary (e.g. past 256 codes
with 1-byte packed codes) silently truncated codes → wrong values with
success=true. The fix materialises the dict-decoded prefix to dense at the
first PLAIN/DELTA page and decodes everything after it dense (string parity:
"we never intern a PLAIN page into a dictionary on read").

These tests oracle-compare decoded values against the Python-side source list
for every container variant of the transition, and pin that fully-dict chunks
still arrive Dict-shaped (the ClickBench-class wins must not regress).

PyArrow is the writer here (test-only dependency) because it produces the
dictionary-fallback page layout our own writer never emits.
"""

import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

# Small dict/data pages force the dictionary to spill quickly: a
# low-cardinality prefix produces genuine dict-encoded pages, then the
# high-cardinality tail overflows the dictionary and lands in PLAIN pages.
_SPILL_KW = dict(use_dictionary=True, dictionary_pagesize_limit=4096, data_page_size=4096)


def _mixed_values(null_stride=0):
    """8k low-NDV prefix (dict pages) + 12k high-NDV tail (PLAIN spill pages).

    null_stride > 0 sprinkles Nones through BOTH regions so the nullable
    transition sees nulls in the dict prefix and in the spill pages.
    """
    vals = [i % 50 for i in range(8000)] + [100 + i * 7 for i in range(12000)]
    if null_stride:
        vals = [None if i % null_stride == 3 else v for i, v in enumerate(vals)]
    return vals


def _write(folder, arrays, **writer_kwargs):
    os.makedirs(folder, exist_ok=True)
    pq.write_table(pa.table(arrays), os.path.join(folder, "p.parquet"), **writer_kwargs)


def _column(dataset, sql):
    session = opteryx.session()
    out = []
    for m in session.execute_to_morsels(sql):
        out.extend(m.column(b"v").to_pylist())
    return out


@pytest.mark.parametrize(
    "dtype, cast",
    [
        (pa.int32(), int),
        (pa.int64(), int),
        (pa.float64(), float),
    ],
    ids=["int32", "int64", "float64"],
)
@pytest.mark.parametrize("null_stride", [0, 7], ids=["non_nullable", "nullable"])
def test_mixed_dict_plain_chunk_values_exact(dtype, cast, null_stride):
    """Every value of a spilled chunk must round-trip exactly — dict prefix,
    PLAIN tail, and (for the nullable variant) nulls in both regions."""
    folder = f"numdictfb_{dtype}_{null_stride}_tmp"
    vals = [None if v is None else cast(v) for v in _mixed_values(null_stride)]
    try:
        _write(folder, {"v": pa.array(vals, dtype)}, **_SPILL_KW)
        got = _column(folder, f"SELECT v FROM {folder}")
        assert got == vals
    finally:
        shutil.rmtree(folder, ignore_errors=True)


@pytest.mark.parametrize("null_stride", [0, 7], ids=["non_nullable", "nullable"])
def test_mixed_dict_plain_chunk_masked_decode(null_stride):
    """Pass-2 (row-masked) decode of a spilled chunk: filter on a sibling
    column so the mixed dict column is decoded under a row mask."""
    folder = f"numdictfb_mask_{null_stride}_tmp"
    vals = _mixed_values(null_stride)
    sel = [i % 100 for i in range(len(vals))]
    try:
        _write(
            folder,
            {"v": pa.array(vals, pa.int64()), "k": pa.array(sel, pa.int64())},
            **_SPILL_KW,
        )
        session = opteryx.session()
        got = []
        for m in session.execute_to_morsels(f"SELECT v FROM {folder} WHERE k = 42"):
            got.extend(m.column(b"v").to_pylist())
        expected = [v for v, s in zip(vals, sel) if s == 42]
        assert got == expected
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def test_filter_on_plain_page_value_still_matches():
    """Equality filter targeting a value that only exists in a PLAIN spill
    page (the dict-skip predicate must not prune it)."""
    folder = "numdictfb_filter_tmp"
    vals = _mixed_values()
    needle = vals[-1]  # deep in the PLAIN tail, not in the dictionary
    try:
        _write(folder, {"v": pa.array(vals, pa.int64())}, **_SPILL_KW)
        got = _column(folder, f"SELECT v FROM {folder} WHERE v = {needle}")
        assert got == [needle]
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def test_nullable_spill_past_code_width_boundary():
    """The old intern path's truncation bug: a nullable chunk whose dictionary
    holds <256 entries (1-byte packed codes) spilling to PLAIN pages with NDV
    far past 256. Interning grew codes past the frozen 1-byte width and
    silently truncated them; the dense transition must decode every value
    exactly."""
    folder = "numdictfb_codewidth_tmp"
    # 250-entry dictionary prefix, then a tail with thousands of new values.
    vals = [i % 250 for i in range(8000)] + [1000 + i * 3 for i in range(12000)]
    vals = [None if i % 11 == 5 else v for i, v in enumerate(vals)]
    try:
        _write(folder, {"v": pa.array(vals, pa.int64())}, **_SPILL_KW)
        got = _column(folder, f"SELECT v FROM {folder}")
        assert got == vals
    finally:
        shutil.rmtree(folder, ignore_errors=True)


@pytest.mark.parametrize(
    "dtype, cast",
    [(pa.int32(), int), (pa.int64(), int), (pa.float64(), float)],
    ids=["int32", "int64", "float64"],
)
def test_dict_shape_only_dropped_on_spill(dtype, cast):
    """The scan must produce a Dict-shaped vector for a chunk whose every data
    page is dictionary-encoded, and a Dense one only when the writer spilled to
    PLAIN. This is the invariant the dense-on-spill transition rests on: the
    dict-aware operator fast paths (dict compare, k-probe group-by) must keep
    firing for genuinely dictionary-encoded data.

    Asserted at the scan boundary (iter_row_groups_ipc) because that is where
    the shape decision is made; later pipeline stages may densify for their own
    reasons, which this test is not about.
    """
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    base = f"numdictfb_shape_{dtype}_tmp"
    pure = [cast(i % 40) for i in range(20000)]
    spilled = [cast(v) for v in _mixed_values()]
    try:
        for name, vals, kw in (
            ("pure", pure, dict(use_dictionary=True)),
            ("spill", spilled, _SPILL_KW),
        ):
            # distinct directories: the reader caches footers per path
            folder = os.path.join(base, name)
            _write(folder, {"v": pa.array(vals, dtype)}, **kw)
            for _rg, cols in iter_row_groups_ipc(None, [f"{folder}/p.parquet"], ["v"]):
                v = cols[next(iter(cols))]
                assert v.to_pylist() == vals, name
                if name == "pure":
                    assert v._nb.is_dict, "fully-dict chunk lost its Dict shape"
                else:
                    assert not v._nb.is_dict, "spilled chunk should decode dense"
                break
    finally:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
