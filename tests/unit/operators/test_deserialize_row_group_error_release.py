"""
P2 fix — deserialize_row_group must release every committed ref_id even when
a column fails to parse partway through the batch.

deserialize_row_group_fixed (C++) reads/parses/unlatches every column in the
batch in one nogil call before Python ever sees the results. If the Python
loop raises on column i (bad tag, OOM, unknown status), ref_ids[i:] were
never passed to pool.release() and stayed committed for the rest of the
pool's lifetime. Garbage (non-IPC) bytes committed via py_commit() reliably
fail to parse as fixed-width IPC on the first column, exercising the same
leak path without needing to force a real OOM.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.compiled.structures.column_deserializer import deserialize_row_group
from opteryx.compiled.structures.memory_pool import MemoryPool


def test_error_path_releases_all_committed_refs():
    pool = MemoryPool(1024 * 1024, name="test-pool")
    baseline = pool.py_available_space()

    ref_ids = {}
    for i in range(5):
        ref_id = pool.py_commit(b"not a valid ipc column blob")
        ref_ids[f"col{i}"] = ref_id

    assert pool.py_available_space() < baseline

    with pytest.raises(Exception):
        deserialize_row_group(ref_ids, pool)

    assert pool.py_available_space() == baseline, (
        "committed ref_ids from the failed batch were not released back to the pool"
    )


if __name__ == "__main__":  # pragma: no cover
    test_error_path_releases_all_committed_refs()
    print("ok")
