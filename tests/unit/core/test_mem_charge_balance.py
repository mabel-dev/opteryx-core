# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Group-by and join memory charged through draken::MemCharge must balance.

The group-by pool now charges blocks below DRAKEN_MEM_ACCOUNT_MIN and the join
build charges its std::vector / new[] arrays (mem_account.h), so the chunk
cache's give-way sees that memory. Every such charge has to come off the account
again when the memory goes: a leak would lower the cache's limit for good, query
after query.

The group-by pool keeps the blocks it takes from the system across queries (up to
1 GiB) and they stay charged - real memory, measured 2026-10-19 to grow ~5 MiB on
the second run of the group-by below and ~0.4 MiB a run by the tenth. So the
invariant is the account LESS what the pool holds (its system bytes less its
freed bytes) LESS what the slab pool holds (the bytes of its slots it has faulted,
which it charges and keeps for re-use): that must not move, run after run. Measured
2026-10-25 over 40 runs of the group-by below it is exactly zero every run, while the
slab pool's own figure climbs from 116 to 200 MiB as it warms.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.compiled.structures.memory_pool import process_memory_charged
from opteryx.compiled.structures.memory_pool import process_memory_peak
from opteryx.compiled.structures.memory_pool import reset_process_memory_peak
from opteryx.compiled.structures.memory_pool import slab_pool_stats
from opteryx.operators._operators import get_groupby_telemetry

ROWS = 3_000_000
GROUP_BY = f"SELECT g % 1000003 AS k, COUNT(*) FROM generate_series(1, {ROWS}) AS g GROUP BY k"
JOIN = (
    f"SELECT COUNT(*) FROM generate_series(1, {ROWS}) AS a "
    f"INNER JOIN generate_series(1, {ROWS}) AS b ON a = b"
)
SLACK = 1 << 20   # account noise between runs (allocator usable-size rounding)


def _run(sql):
    for _ in opteryx.session().execute_to_morsels(sql):
        pass


def _charged_beyond_pool():
    """The account less the group-by and slab pools' held memory (see the module note)."""
    tel = get_groupby_telemetry()
    held = tel["pool_os_bytes"] - tel["pool_freed_bytes"]
    return process_memory_charged() - held - slab_pool_stats()["faulted_bytes"]


@pytest.mark.parametrize("sql", [GROUP_BY, JOIN], ids=["group_by", "join"])
def test_repeated_queries_leave_the_account_where_they_found_it(sql):
    _run(sql)
    settled = _charged_beyond_pool()
    for _ in range(3):
        _run(sql)
        drift = _charged_beyond_pool() - settled
        assert abs(drift) <= SLACK, f"charged memory drifted {drift / 2**20:+.1f} MiB beyond the pool"


def test_join_build_memory_is_charged():
    # 3M build rows hold row addresses (8 B), hashes (8 B) and the radix table
    # (12 B) - ~84 MB charged through MemCharge on top of the vectors the
    # allocator already charges. Before, the peak missed all of it.
    reset_process_memory_peak()
    base = process_memory_charged()
    _run(JOIN)
    assert process_memory_peak() - base >= ROWS * 28, (
        f"join peak only {(process_memory_peak() - base) / 2**20:.0f} MiB above the start"
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
