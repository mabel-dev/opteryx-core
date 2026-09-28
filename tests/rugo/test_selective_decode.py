"""Masked (row-selected) column decode must equal unmasked decode, filtered.

Selective decode emits only a page's selected rows: dict codes are decoded
selectively, PLAIN values are compacted per page, byte_array values skip
unselected entries and def levels are compacted per page. This pins every
column shape (required / nullable x dictionary / PLAIN x int / float / bool /
string) against the simplest oracle — decode everything, then filter in Python
— over mask shapes that hit whole-page skips, partial pages and full pages.
"""

import os
import random
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import rugo.rugo_native as rp

from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info

N = 20_000
COLUMNS = ["k", "n_int", "n_big", "n_float", "n_bool", "s_dict", "s_plain", "req_str", "d_int"]


def _table():
    rnd = random.Random(7)

    def maybe(v, p=0.2):
        return None if rnd.random() < p else v

    return pa.table(
        {
            "k": [rnd.randint(0, 999) for _ in range(N)],
            "n_int": [maybe(rnd.randint(-5, 5)) for _ in range(N)],
            "n_big": [maybe(rnd.randint(-(10**12), 10**12), 0.1) for _ in range(N)],
            "n_float": [maybe(rnd.random()) for _ in range(N)],
            "n_bool": [maybe(rnd.random() < 0.5) for _ in range(N)],
            "s_dict": [maybe(rnd.choice(["alpha", "beta", "a-much-longer-string-value", "z"])) for _ in range(N)],
            "s_plain": [maybe("row-%d-%s" % (i, "x" * (i % 40))) for i in range(N)],
            "req_str": ["r%d" % (i % 7) for i in range(N)],
            "d_int": [rnd.randint(0, 20) for _ in range(N)],
        }
    )


def _masks():
    rnd = random.Random(11)
    yield "none", [0] * N
    yield "all", [1] * N
    yield "sparse", [1 if rnd.random() < 0.01 else 0 for _ in range(N)]
    yield "half", [1 if rnd.random() < 0.5 else 0 for _ in range(N)]
    yield "dense", [0 if rnd.random() < 0.05 else 1 for _ in range(N)]
    # Clustered: long selected / unselected blocks, so whole pages skip.
    yield "blocks", [1 if (i // 1500) % 3 == 0 else 0 for i in range(N)]
    yield "one", [1 if i == N - 3 else 0 for i in range(N)]


@pytest.fixture(scope="module", params=["dict", "plain"])
def written(request):
    use_dictionary = ["k", "n_int", "n_big", "s_dict", "req_str", "d_int", "n_bool"] if request.param == "dict" else False
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        path = f.name
    pq.write_table(
        _table(),
        path,
        row_group_size=N,
        data_page_size=1024,
        use_dictionary=use_dictionary,
        compression="zstd",
    )
    with open(path, "rb") as f:
        raw = f.read()
    yield path, raw
    os.unlink(path)


def _chunk(path, raw, col):
    stats = next(c for c in rp.read_rowgroup_stats(raw)[0]["columns"] if c["name"] == col)
    info = fetch_column_chunk_info(path, 0, [col])[col]
    col_stats = {**stats, **info}
    dict_off = col_stats.get("dictionary_page_offset")
    data_off = col_stats["data_page_offset"]
    base = dict_off if dict_off is not None and 0 <= dict_off < data_off else data_off
    return raw[base : base + col_stats["total_compressed_size"]], col_stats


@pytest.mark.parametrize("col", COLUMNS)
def test_masked_decode_equals_filtered_full_decode(written, col):
    path, raw = written
    chunk, col_stats = _chunk(path, raw, col)
    full = rp.decode_column_from_chunk(chunk, dict(col_stats)).to_pylist()
    assert len(full) == N
    for name, mask in _masks():
        expected = [v for v, m in zip(full, mask) if m]
        if not expected:
            # A mask with no selected row: every page is skipped.
            continue
        got = rp.decode_column_from_chunk(chunk, dict(col_stats), row_mask=bytearray(mask))
        assert got is not None, f"{col}/{name}: decode failed"
        assert got.to_pylist() == expected, f"{col}/{name}: masked decode differs"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
