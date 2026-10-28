# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""ORC streams in the cross-query chunk cache (docs/ORC_CHUNK_CACHE_DESIGN.md).

A compressed ORC file's WHOLE streams are cached decompressed, keyed by the path and the
stream's offset; a row-index seek reads from a cached whole stream through its chunk index and
never fills. The cache is process-wide, so every test restores the configuration it found.
Every answer is checked against what pyarrow.orc wrote (test-only, CLAUDE.md §4)."""

import datetime
import os
import sys
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa
import pyarrow.orc as po

import opteryx
from opteryx import config
from opteryx.compiled.platform import cgroup_memory_limit_bytes, physical_memory_total_bytes
from opteryx.compiled.structures.memory_pool import chunk_cache_stats
from opteryx.compiled.structures.memory_pool import configure_chunk_cache
from opteryx.compiled.structures.memory_pool import flush_chunk_cache

GIB = 1 << 30
UTC = datetime.timezone.utc
ROWS = 120_011


def _table():
    ids = range(ROWS)
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "n": pa.array([None if i % 7 == 0 else (i * 31) % 1000 for i in ids], pa.int32()),
            "s": pa.array([None if i % 5 == 0 else f"value-{i}-{'x' * (i % 17)}" for i in ids], pa.string()),
            "few": pa.array([f"c{i % 30}" for i in ids], pa.string()),
            "ts": pa.array([datetime.datetime(2024, 1, 1) + datetime.timedelta(seconds=i) for i in ids], pa.timestamp("us")),
        }
    )


TABLE = _table()
WANT = {r["id"]: r for r in TABLE.to_pylist()}
for _r in WANT.values():
    _r["ts"] = _r["ts"].replace(tzinfo=UTC)
NAMES = TABLE.column_names


def _write(path, **options):
    with open(path, "wb") as f:
        po.write_table(TABLE, f, **options)
    return path


def _rows(sql, session=None):
    session = session or opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        names = [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
        columns = [morsel.column(n).to_pylist() for n in morsel.column_names]
        out.extend(dict(zip(names, values)) for values in zip(*columns))
    return sorted(out, key=lambda r: r["id"])


def _check(rows, ids):
    assert [r["id"] for r in rows] == sorted(ids)
    for row in rows:
        assert row == {k: WANT[row["id"]][k] for k in row}, row["id"]


@pytest.fixture
def cache_on():
    """A cache with a known 8 GiB budget, empty at the start of the test."""
    container = cgroup_memory_limit_bytes() or physical_memory_total_bytes()
    configure_chunk_cache(12 * GIB, 100, 4 * GIB)
    flush_chunk_cache()
    yield
    flush_chunk_cache()
    configure_chunk_cache(container, config.CHUNK_CACHE_MEMORY_PERCENT, config.CHUNK_CACHE_RESERVE_BYTES)


@pytest.mark.parametrize("codec", ["zstd", "zlib", "snappy", "lz4"])
def test_a_repeat_scan_is_served_from_the_cache_with_the_same_answer(cache_on, tmp_path, codec):
    path = _write(tmp_path / "t.orc", compression=codec, stripe_size=64 * 1024)
    sql = f"SELECT {', '.join(NAMES)} FROM READ_ORC('{path}')"
    before = chunk_cache_stats()
    _check(_rows(sql), WANT.keys())
    filled = chunk_cache_stats()
    assert filled["inserts"] > before["inserts"], "the first scan filled nothing"
    _check(_rows(sql), WANT.keys())
    hit = chunk_cache_stats()
    assert hit["hits"] > filled["hits"], "the repeat scan was not served from the cache"
    assert hit["inserts"] == filled["inserts"], "a hit must not refill"


def test_a_seek_reads_from_a_cached_whole_stream_and_never_fills(cache_on, tmp_path):
    # One stripe, 10,000-row row groups: the WHERE's second pass seeks to one row group.
    path = _write(tmp_path / "t.orc", compression="zstd", stripe_size=512 * 1024 * 1024, row_index_stride=10_000)
    seek_sql = f"SELECT id, s, ts FROM READ_ORC('{path}') WHERE id BETWEEN 70000 AND 70100"
    want = [i for i in WANT if 70000 <= i <= 70100]

    # Nothing cached: every read is a seek (pass 1 too: `id BETWEEN` prunes row groups by their
    # statistics), so the query decompresses its windows and fills nothing.
    before = chunk_cache_stats()
    _check(_rows(seek_sql), want)
    assert chunk_cache_stats()["inserts"] == before["inserts"], "a seek must never fill"

    # Fill every stream with a whole scan, then seek again: served by the cached streams.
    _check(_rows(f"SELECT {', '.join(NAMES)} FROM READ_ORC('{path}')"), WANT.keys())
    full = chunk_cache_stats()
    _check(_rows(seek_sql), want)
    warm = chunk_cache_stats()
    assert warm["hits"] > full["hits"]
    assert warm["inserts"] == full["inserts"], "a seek must never fill"


def test_admit_false_flushes_and_never_fills(cache_on, tmp_path):
    path = _write(tmp_path / "t.orc", compression="zstd", stripe_size=64 * 1024)
    sql = f"SELECT id, s FROM READ_ORC('{path}')"
    _rows(sql)
    assert chunk_cache_stats()["bytes"] > 0
    session = opteryx.session()
    for _ in session.execute_to_morsels("SET chunk_cache_admit = false"):
        pass
    before = chunk_cache_stats()
    _check(_rows(sql, session), WANT.keys())
    after = chunk_cache_stats()
    assert after["bytes"] == 0, "admit=false must flush the cache before the statement runs"
    assert after["inserts"] == before["inserts"], "admit=false must never fill"


def test_an_uncompressed_file_is_never_cached(cache_on, tmp_path):
    path = _write(tmp_path / "t.orc", compression="uncompressed")
    before = chunk_cache_stats()
    _check(_rows(f"SELECT {', '.join(NAMES)} FROM READ_ORC('{path}')"), WANT.keys())
    after = chunk_cache_stats()
    assert after["inserts"] == before["inserts"] and after["misses"] == before["misses"]


def test_with_the_cache_off_nothing_is_looked_up(tmp_path):
    container = cgroup_memory_limit_bytes() or physical_memory_total_bytes()
    configure_chunk_cache(container, 0, 0)   # budget 0 = off
    try:
        path = _write(tmp_path / "t.orc", compression="zstd", stripe_size=64 * 1024)
        before = chunk_cache_stats()
        _check(_rows(f"SELECT {', '.join(NAMES)} FROM READ_ORC('{path}')"), WANT.keys())
        after = chunk_cache_stats()
        assert after["inserts"] == before["inserts"] and after["misses"] == before["misses"]
    finally:
        configure_chunk_cache(container, config.CHUNK_CACHE_MEMORY_PERCENT, config.CHUNK_CACHE_RESERVE_BYTES)
