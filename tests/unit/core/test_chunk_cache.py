"""Cross-query decompressed parquet chunk cache (C7, docs/C7_PAGE_CACHE_DESIGN.md).

The cache is process-wide, so every test restores the configuration it found.
The fixture is a Snappy-compressed parquet file: an uncompressed column is never
cached (nothing to save), so a compressed file is what exercises it.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx import config
from opteryx.compiled.platform import cgroup_memory_limit_bytes, physical_memory_total_bytes
from opteryx.compiled.structures.memory_pool import (
    chunk_cache_stats,
    configure_chunk_cache,
    flush_chunk_cache,
)

GIB = 1 << 30
DATASET = "testdata.flat.formats.parquet_snappy"
SQL = f"SELECT user_name, COUNT(*) AS c FROM {DATASET} GROUP BY user_name ORDER BY c DESC, user_name LIMIT 20"


def _rows(sql, session=None):
    if session is None:  # a fresh Session is falsy (it has a length)
        session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        out.extend(tuple(morsel[i]) for i in range(morsel.num_rows))
    return out


@pytest.fixture
def cache_on():
    """A cache with a known 8 GiB budget, empty at the start of the test."""
    container = cgroup_memory_limit_bytes() or physical_memory_total_bytes()
    configure_chunk_cache(12 * GIB, 100, 4 * GIB)
    flush_chunk_cache()
    yield
    flush_chunk_cache()
    configure_chunk_cache(container, config.CHUNK_CACHE_MEMORY_PERCENT, config.CHUNK_CACHE_RESERVE_BYTES)


def test_repeat_scan_is_served_from_the_cache_with_the_same_answer(cache_on):
    before = chunk_cache_stats()
    first = _rows(SQL)
    filled = chunk_cache_stats()
    assert filled["inserts"] > before["inserts"], "the first scan filled nothing"
    assert filled["bytes"] > 0
    second = _rows(SQL)
    hit = chunk_cache_stats()
    assert hit["hits"] > filled["hits"], "the repeat scan was not served from the cache"
    assert hit["inserts"] == filled["inserts"], "a hit must not refill"
    assert first == second


def test_admit_false_flushes_and_never_fills(cache_on):
    _rows(SQL)
    assert chunk_cache_stats()["bytes"] > 0
    session = opteryx.session()
    _rows("SET chunk_cache_admit = false", session)
    before = chunk_cache_stats()
    rows = _rows(SQL, session)
    after = chunk_cache_stats()
    assert after["bytes"] == 0, "admit=false must flush the cache before the statement runs"
    assert after["inserts"] == before["inserts"], "admit=false must never fill"
    assert rows == _rows(SQL)


def test_budget_under_two_gib_turns_the_cache_off(cache_on):
    _rows(SQL)
    assert chunk_cache_stats()["bytes"] > 0
    # 5 GiB container, 100%, 4 GiB reserve -> 1 GiB budget -> off (< 2 GiB).
    assert configure_chunk_cache(5 * GIB, 100, 4 * GIB) == 0
    stats = chunk_cache_stats()
    assert stats["budget"] == 0
    assert stats["bytes"] == 0, "turning the cache off must flush it"
    before = chunk_cache_stats()
    _rows(SQL)
    after = chunk_cache_stats()
    assert after["inserts"] == before["inserts"]
    assert after["hits"] == before["hits"]


def test_configuration_out_of_range_is_refused():
    with pytest.raises(ValueError):
        configure_chunk_cache(8 * GIB, 101, 0)
    with pytest.raises(ValueError):
        configure_chunk_cache(-1, 70, 0)


def test_budget_is_visible_in_variables(cache_on):
    names = {row[0] for row in _rows("SHOW VARIABLES")}
    assert {"chunk_cache_memory_percent", "chunk_cache_reserve_bytes",
            "chunk_cache_budget_bytes", "chunk_cache_admit"} <= names


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
