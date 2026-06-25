# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Generic pipeline-parallel executor — the DOP=1 golden differential gate.

The prime constraint of the generic ``PipelineSink`` executor (Steps 2/3/4,
``docs/GENERIC_PIPELINE_PARALLELISM_DESIGN.md`` §3) is that the generic path is
**byte-identical to serial** at every DOP. This test enforces it for the three real
breaker shapes the contract carries:

  * SCALAR_MERGE      — ungrouped aggregate (``_ScalarMergeSink``)
  * HASH_REPARTITION  — grouped aggregate (``_HashRepartitionSink``)
  * HASH_REPARTITION  — DISTINCT          (``_DistinctSink``)

For each query it asserts the sorted-tuple md5 of the result is identical across:
    serial  ==  generic @ DOP=1  ==  generic @ DOP=8

and that the generic path actually **fires** (the ``_run_breaker_segment`` skeleton
is entered with the expected recombination class) — so a silent serial fallback can
never pass the gate green. Grouped-agg is the catastrophic-bug operator, so the
queries cover the hard cases: multi-column GROUP BY, a NULL-bearing key via NULLIF,
AVG (recompute-at-finalize), and COUNT(DISTINCT) (a holistic aggregate). DISTINCT
covers single-col, multi-col, a NULL-bearing key, and DISTINCT ON.

DISTINCT only engages the parallel sink at W>=2 (serial dedup is byte-identical and
strictly cheaper at W=1 — the design's "DOP=1 drives the ORIGINAL breaker" applied
verbatim), so its DOP=1 run is asserted to run SERIAL (the sink does NOT fire) while
DOP=8 fires the ``_DistinctSink``. The result must still be byte-identical across all
three. ``min_workers`` records the W>=2 floor per query.
"""

import hashlib
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

import opteryx
from opteryx import config
from opteryx.managers.execution import parallel_engine
from opteryx.managers.execution.pipeline_sink import RecombClass


# A 100k-row real parquet dataset — large enough to exercise the parallel sink with
# multiple morsels per worker (the floor is forced to 0 in the test).
_DS = "testdata.flat.formats.parquet"

# (name, sql, expected_recomb_class, min_workers)
#
# `min_workers` = the smallest DOP at which the generic sink engages. Agg sinks fire
# at W>=1; DISTINCT only at W>=2 (W=1 runs the byte-identical serial dedup — the
# prime constraint applied to distinct). At DOP below `min_workers` the run is SERIAL
# (the sink must NOT fire) but the result must still be byte-identical.
_QUERIES = [
    # --- SCALAR_MERGE (ungrouped aggregate) ---------------------------------------
    (
        "ungrouped_sum_min_max",
        f"SELECT SUM(followers) s, MIN(followers) mn, MAX(followers) mx FROM {_DS}",
        RecombClass.SCALAR_MERGE,
        1,
    ),
    (
        "ungrouped_avg",
        f"SELECT AVG(followers) a FROM {_DS}",
        RecombClass.SCALAR_MERGE,
        1,
    ),
    # --- HASH_REPARTITION (grouped aggregate) -------------------------------------
    (
        "grouped_single_count",
        f"SELECT user_verified, COUNT(*) c FROM {_DS} GROUP BY user_verified",
        RecombClass.HASH_REPARTITION,
        1,
    ),
    (
        "grouped_multi_col",
        f"SELECT user_verified, is_quoting, COUNT(*) c, SUM(followers) s "
        f"FROM {_DS} GROUP BY user_verified, is_quoting",
        RecombClass.HASH_REPARTITION,
        1,
    ),
    (
        "grouped_null_key_nullif",
        f"SELECT NULLIF(user_verified, true) k, COUNT(*) c "
        f"FROM {_DS} GROUP BY NULLIF(user_verified, true)",
        RecombClass.HASH_REPARTITION,
        1,
    ),
    (
        "grouped_avg",
        f"SELECT user_verified, AVG(followers) a FROM {_DS} GROUP BY user_verified",
        RecombClass.HASH_REPARTITION,
        1,
    ),
    (
        "grouped_count_distinct",
        f"SELECT user_verified, COUNT(DISTINCT followers) cd "
        f"FROM {_DS} GROUP BY user_verified",
        RecombClass.HASH_REPARTITION,
        1,
    ),
    # --- HASH_REPARTITION (DISTINCT) ----------------------------------------------
    (
        "distinct_single_col",
        f"SELECT DISTINCT user_id FROM {_DS}",
        RecombClass.HASH_REPARTITION,
        2,
    ),
    (
        "distinct_multi_col",
        f"SELECT DISTINCT user_verified, is_quoting, followers FROM {_DS}",
        RecombClass.HASH_REPARTITION,
        2,
    ),
    (
        # `is_reply_to` is NULL-bearing — exercises NULL dedup-key handling.
        "distinct_null_bearing",
        f"SELECT DISTINCT is_reply_to FROM {_DS}",
        RecombClass.HASH_REPARTITION,
        2,
    ),
    (
        "distinct_on",
        f"SELECT DISTINCT ON (user_verified) user_verified, followers FROM {_DS} "
        f"ORDER BY user_verified, followers",
        RecombClass.HASH_REPARTITION,
        2,
    ),
]


def _run_rows(sql):
    """Execute ``sql`` and return every output row as a tuple (row-access via the
    Morsel subscript). The current config (flag / DOP / floor) selects the path."""
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            rows.append(tuple(morsel[i]))
    return rows


def _md5_sorted(rows):
    """Order-insensitive fingerprint — the parallel sinks emit groups/partitions in
    a non-deterministic order, so we sort before hashing (the values must match)."""
    ordered = sorted(rows, key=lambda r: repr(r))
    return hashlib.md5(repr(ordered).encode()).hexdigest()


class _Cfg:
    """Save/restore the three config knobs the paths read, so the test is hermetic."""

    def __enter__(self):
        self._saved = (
            config.M4_GENERIC_PIPELINE,
            config.MAX_EXECUTION_WORKERS,
            config.PARALLEL_MIN_ROWS,
        )
        return self

    def set(self, *, generic, workers, floor):
        config.M4_GENERIC_PIPELINE = generic
        config.MAX_EXECUTION_WORKERS = workers
        config.PARALLEL_MIN_ROWS = floor

    def __exit__(self, *exc):
        (
            config.M4_GENERIC_PIPELINE,
            config.MAX_EXECUTION_WORKERS,
            config.PARALLEL_MIN_ROWS,
        ) = self._saved


def _run_with_spy(sql):
    """Run ``sql`` through the generic path, recording every
    ``_run_breaker_segment`` entry as ``(recomb_class, workers)``. The spy is
    signature-agnostic (``*args/**kwargs``) so it captures the agg sinks (no
    ``exit_id``) and the distinct sink (which threads ``exit_id``) alike."""
    fired = []
    original = parallel_engine._run_breaker_segment

    def spy(plan, scan_id, middle_ids, breaker_id, recomb_class, workers, *args, **kwargs):
        fired.append((recomb_class, workers))
        return original(
            plan, scan_id, middle_ids, breaker_id, recomb_class, workers, *args, **kwargs
        )

    parallel_engine._run_breaker_segment = spy
    try:
        rows = _run_rows(sql)
    finally:
        parallel_engine._run_breaker_segment = original
    return rows, fired


@pytest.mark.parametrize(
    "name,sql,expected_class,min_workers", _QUERIES, ids=[q[0] for q in _QUERIES]
)
def test_generic_pipeline_byte_identical(name, sql, expected_class, min_workers):
    with _Cfg() as cfg:
        # --- serial reference: generic OFF, floor above the input so the row-floor
        #     branch drives the ORIGINAL un-cloned breaker (the serial truth). ------
        cfg.set(generic=False, workers=1, floor=10**12)
        serial = _md5_sorted(_run_rows(sql))

        # --- generic @ DOP=1, floor 0 (force the parallel sink at W=1) -------------
        cfg.set(generic=True, workers=1, floor=0)
        g1_rows, g1_fired = _run_with_spy(sql)
        g1 = _md5_sorted(g1_rows)

        # --- generic @ DOP=8, floor 0 ---------------------------------------------
        cfg.set(generic=True, workers=8, floor=0)
        g8_rows, g8_fired = _run_with_spy(sql)
        g8 = _md5_sorted(g8_rows)

    # Byte-identity across all three paths (the prime constraint). For DISTINCT the
    # DOP=1 run is the serial dedup (the sink only engages at W>=2) — still must be
    # byte-identical.
    assert serial == g1 == g8, (
        f"{name}: serial={serial} generic@DOP1={g1} generic@DOP8={g8}"
    )

    # The generic sink must FIRE at any DOP >= min_workers — no silent serial
    # fallback may pass green there. Below min_workers (distinct @ W=1) the run is
    # SERIAL by design, so the sink must NOT fire.
    expected_g1 = [(expected_class, 1)] if min_workers <= 1 else []
    assert g1_fired == expected_g1, f"{name}: DOP=1 fired {g1_fired}"
    assert g8_fired == [(expected_class, 8)], f"{name}: DOP=8 fired {g8_fired}"


if __name__ == "__main__":  # pragma: no cover
    for _name, _sql, _cls, _mw in _QUERIES:
        test_generic_pipeline_byte_identical(_name, _sql, _cls, _mw)
        print(f"OK  {_name}")
    print("\nAll generic-pipeline golden differentials passed.")
