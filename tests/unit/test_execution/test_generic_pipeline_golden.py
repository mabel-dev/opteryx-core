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
    """Save/restore the two config knobs the SOLE engine reads, so the test is hermetic.

    The M4 ``scheduler_engine`` is now the ONLY data engine (Step 7 — it hosts the
    ``dispatch_data_pipeline`` substrate under its Event/Executor DAG; no flag, no dual
    path). The serial REFERENCE is taken by forcing ``PARALLEL_MIN_ROWS`` above the
    input so the row-floor branch drives the ORIGINAL un-cloned breaker (the serial
    truth, byte-identical to the parallel sink by the prime constraint); the parallel
    path is forced with ``floor=0``. ``generic`` is accepted but no longer toggles a
    flag — it is asserted True (the serial reference is the floor, not a flag flip)."""

    def __enter__(self):
        self._saved = (
            config.MAX_EXECUTION_WORKERS,
            config.PARALLEL_MIN_ROWS,
        )
        return self

    def set(self, *, generic, workers, floor):
        config.MAX_EXECUTION_WORKERS = workers
        config.PARALLEL_MIN_ROWS = floor

    def __exit__(self, *exc):
        (
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


# =============================================================================
# Step 5 — join → agg golden differential (SHARED_SOURCE join composed with the
# generic agg sink through the ONE _run_breaker_segment skeleton).
#
# Every TPC-H query is `join → agg`; this is the benchmark-payoff shape AND the
# catastrophic-bug operators (inner join + grouped agg). The composition must be
# byte-identical to serial at every DOP, fire the SAME agg sink the plain-agg path
# uses (HASH_REPARTITION grouped / SCALAR_MERGE ungrouped), and — critically — go
# through the ONE skeleton with a `source_prep` (NOT a forked `_join_agg_stream`),
# proven by the spy seeing `source_prep is not None`.
#
# Uses testdata.tpch_1 (in the main checkout): orders ⋈ lineitem on orderkey, the
# real Q12-shaped join. The grouped key is small (l_shipmode = 7 values) so the
# join probe dominates — the case that should scale.
# =============================================================================

_O = "testdata.tpch_1.orders"
_L = "testdata.tpch_1.lineitem"
_J = f"FROM {_O} o INNER JOIN {_L} l ON o.o_orderkey = l.l_orderkey"

# (name, sql, expected_recomb_class)  — all fire at W>=1 (agg sinks, like plain agg).
_JOIN_AGG_QUERIES = [
    # The gate query (TPC-H Q12 shape): GROUP BY single col, COUNT + SUM.
    (
        "joinagg_q12_shape",
        f"SELECT l.l_shipmode, COUNT(*) c, SUM(l.l_quantity) q {_J} "
        f"GROUP BY l.l_shipmode",
        RecombClass.HASH_REPARTITION,
    ),
    # multi-column GROUP BY over join output.
    (
        "joinagg_multi_col",
        f"SELECT l.l_shipmode, l.l_returnflag, COUNT(*) c, SUM(l.l_quantity) q {_J} "
        f"GROUP BY l.l_shipmode, l.l_returnflag",
        RecombClass.HASH_REPARTITION,
    ),
    # NULL-bearing GROUP BY key (NULLIF synthesises NULLs into the key).
    (
        "joinagg_null_key",
        f"SELECT NULLIF(l.l_shipmode, 'AIR') k, COUNT(*) c {_J} "
        f"GROUP BY NULLIF(l.l_shipmode, 'AIR')",
        RecombClass.HASH_REPARTITION,
    ),
    # AVG (recompute-at-finalize) + SUM over join output.
    (
        "joinagg_avg_sum",
        f"SELECT l.l_shipmode, AVG(l.l_quantity) a, SUM(l.l_extendedprice) s {_J} "
        f"GROUP BY l.l_shipmode",
        RecombClass.HASH_REPARTITION,
    ),
    # Wide payload incl. MIN/MAX over a TEMPORAL (DATE) column — the type-preserving
    # grouped MIN/MAX path (the corruption-prone operator) over join output.
    (
        "joinagg_minmax_temporal",
        f"SELECT l.l_shipmode, MIN(l.l_shipdate) mn, MAX(l.l_commitdate) mx, "
        f"COUNT(*) c, SUM(l.l_quantity) q {_J} GROUP BY l.l_shipmode",
        RecombClass.HASH_REPARTITION,
    ),
]


def _run_join_agg_with_spy(sql):
    """Run ``sql`` through the generic path, recording every ``_run_breaker_segment``
    entry as ``(recomb_class, workers, used_source_prep)``. ``used_source_prep`` is the
    Step-5 proof: the join→agg composition drives the SAME skeleton with a
    ``source_prep`` (a ``_JoinSourcePrep``) — never a bespoke fusion function."""
    fired = []
    original = parallel_engine._run_breaker_segment

    def spy(plan, scan_id, middle_ids, breaker_id, recomb_class, workers, *args, **kwargs):
        fired.append((recomb_class, workers, kwargs.get("source_prep") is not None))
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
    "name,sql,expected_class", _JOIN_AGG_QUERIES, ids=[q[0] for q in _JOIN_AGG_QUERIES]
)
def test_join_agg_byte_identical(name, sql, expected_class):
    with _Cfg() as cfg:
        # --- serial reference: generic OFF, floor above the input so the join + agg
        #     run through the ORIGINAL un-cloned operators (the serial truth). --------
        cfg.set(generic=False, workers=1, floor=10**12)
        serial = _md5_sorted(_run_rows(sql))

        # --- generic @ DOP=1, floor 0 (force the composed sink at W=1) -------------
        cfg.set(generic=True, workers=1, floor=0)
        g1_rows, g1_fired = _run_join_agg_with_spy(sql)
        g1 = _md5_sorted(g1_rows)

        # --- generic @ DOP=8, floor 0 ---------------------------------------------
        cfg.set(generic=True, workers=8, floor=0)
        g8_rows, g8_fired = _run_join_agg_with_spy(sql)
        g8 = _md5_sorted(g8_rows)

    # Byte-identity across all three paths (the prime constraint), incl. NULL keys,
    # multi-col, temporal MIN/MAX, AVG over join output.
    assert serial == g1 == g8, (
        f"{name}: serial={serial} generic@DOP1={g1} generic@DOP8={g8}"
    )

    # The composition must FIRE through the ONE skeleton WITH a source_prep at every
    # DOP — the Step-5 single-skeleton invariant (no _join_agg_stream). A silent serial
    # fallback (source_prep False / no fire) can never pass green.
    assert g1_fired == [(expected_class, 1, True)], f"{name}: DOP=1 fired {g1_fired}"
    assert g8_fired == [(expected_class, 8, True)], f"{name}: DOP=8 fired {g8_fired}"


# =============================================================================
# Step 6 — SORT-TAIL TOLERANCE + MULTI-JOIN build prelude.
#
# Real TPC-H queries are `[multi-]join → grouped-agg → ORDER BY [LIMIT] → exit`. Two
# generalisations make them parallelise end-to-end through the SOLE engine:
#
#   A. SORT-TAIL TOLERANCE — the AGG breaker may be followed by a SERIAL TAIL
#      (Sort / HeapSort / Limit / stateless ops) before Exit, not only the Exit
#      directly. The agg is parallelised; the sort runs SERIALLY on the recombined
#      grouped output, so ORDER BY ordering is preserved. (Single-join + agg + ORDER BY.)
#   B. MULTI-JOIN build prelude — the TOP join feeding the agg may have a BUILD leg
#      that is itself a join subtree; the build subtree is driven ONCE serially, and
#      only the TOP join's fact-scan PROBE parallelises. (Multi-join + agg + ORDER BY.)
#
# These cases carry an ORDER BY, so the test asserts BOTH the order-insensitive md5
# (the values match) AND the LIST-ORDER-PRESERVING compare (the ORDER BY / LIMIT
# ordering is byte-identical to serial — the sort runs after the parallel agg
# recombines). The spy proves the parallel path FIRES (the agg sink through the ONE
# skeleton with a source_prep for the join cases).
# =============================================================================

# (name, sql, expected_recomb_class, expects_join, expects_multi_join)
_SORT_TAIL_QUERIES = [
    # --- A: grouped agg + ORDER BY, NO join (single-scan, sort tail) --------------
    (
        "grouped_orderby_sort_tail",
        f"SELECT user_verified, COUNT(*) c FROM {_DS} "
        f"GROUP BY user_verified ORDER BY c DESC, user_verified",
        RecombClass.HASH_REPARTITION,
        False,
        False,
    ),
    # --- A: single-join + grouped agg + ORDER BY + LIMIT (TPC-H Q12-ish + sort) ----
    (
        "joinagg_orderby_limit_q12like",
        f"SELECT l.l_shipmode, COUNT(*) c, SUM(l.l_quantity) q {_J} "
        f"GROUP BY l.l_shipmode ORDER BY l.l_shipmode LIMIT 5",
        RecombClass.HASH_REPARTITION,
        True,
        False,
    ),
]

# Multi-join + agg + ORDER BY — the TPC-H Q10 shape: a TOP inner join whose BUILD leg
# is a join subtree (customer ⋈ orders [⋈ nation]) and whose PROBE leg is the lineitem
# FACT scan, feeding a grouped agg with an ORDER BY [LIMIT] tail. The selective filters
# (orderdate range, returnflag) drive the optimizer to put the big fact (lineitem) on the
# probe (right) side of the TOP join — the shape this engine parallelises (the dimension
# join subtree is the small BUILD side, built once serially; only the fact probe scales).
# These are the REAL Q10 plan shape; the un-filtered variant where the optimizer puts the
# nested join on the probe side stays serial (probe must be a raw fact scan), by design.
_C = "testdata.tpch_1.customer"
_N = "testdata.tpch_1.nation"
_MULTI_JOIN_QUERIES = [
    # The real TPC-H Q10 (customer ⋈ orders ⋈ lineitem ⋈ nation, GROUP BY + ORDER BY +
    # LIMIT) — the canonical multi-join + grouped-agg + sort-tail benchmark query.
    (
        "tpch_q10",
        "SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, "
        "c_acctbal, n_name, c_address, c_phone, c_comment "
        f"FROM {_C}, {_O}, {_L}, {_N} "
        "WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey "
        "AND o_orderdate >= '1993-07-01'::DATE AND o_orderdate < '1993-10-01'::DATE "
        "AND l_returnflag = 'R' AND c_nationkey = n_nationkey "
        "GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment "
        "ORDER BY revenue DESC LIMIT 20",
        RecombClass.HASH_REPARTITION,
    ),
    # A Q10-shaped variant grouping by nation (4-table build subtree, fact probe).
    (
        "multijoin_nation_grouped",
        "SELECT n.n_name, COUNT(*) n_cnt, SUM(l.l_quantity) q "
        f"FROM {_C} c, {_O} o, {_L} l, {_N} n "
        "WHERE c.c_custkey = o.o_custkey AND l.l_orderkey = o.o_orderkey "
        "AND c.c_nationkey = n.n_nationkey "
        "AND o.o_orderdate >= '1994-01-01'::DATE AND o.o_orderdate < '1994-04-01'::DATE "
        "AND l.l_returnflag = 'R' "
        "GROUP BY n.n_name ORDER BY q DESC, n.n_name LIMIT 10",
        RecombClass.HASH_REPARTITION,
    ),
    # The real TPC-H Q3 (customer ⋈ orders ⋈ lineitem, GROUP BY + ORDER BY + LIMIT) — the
    # canonical 3-table left-deep shape whose BUILD prelude (customer ⋈ orders) is itself a
    # parallelisable inner join. The build-prelude parallelization probes `orders` across
    # workers (the Amdahl-anchor fix); the result must stay byte-identical to serial,
    # order-preserving for the ORDER BY + LIMIT.
    (
        "tpch_q3",
        "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) revenue, "
        "o_orderdate, o_shippriority "
        f"FROM {_C}, {_O}, {_L} "
        "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey "
        "AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-22'::DATE "
        "AND l_shipdate > '1995-03-22'::DATE "
        "GROUP BY l_orderkey, o_orderdate, o_shippriority "
        "ORDER BY revenue DESC, o_orderdate LIMIT 10",
        RecombClass.HASH_REPARTITION,
    ),
]


def _md5_listorder(rows):
    """ORDER-PRESERVING fingerprint — the ORDER BY / LIMIT output order must match
    serial exactly (the serial sort runs AFTER the parallel agg recombines)."""
    return hashlib.md5(repr(rows).encode()).hexdigest()


@pytest.mark.parametrize(
    "name,sql,expected_class,expects_join,expects_multi",
    _SORT_TAIL_QUERIES,
    ids=[q[0] for q in _SORT_TAIL_QUERIES],
)
def test_sort_tail_byte_identical(name, sql, expected_class, expects_join, expects_multi):
    with _Cfg() as cfg:
        cfg.set(generic=False, workers=1, floor=10**12)
        serial_rows = _run_rows(sql)
        serial_sorted = _md5_sorted(serial_rows)
        serial_order = _md5_listorder(serial_rows)

        cfg.set(generic=True, workers=1, floor=0)
        g1_rows, g1_fired = _run_join_agg_with_spy(sql)

        cfg.set(generic=True, workers=8, floor=0)
        g8_rows, g8_fired = _run_join_agg_with_spy(sql)

    # Order-insensitive (values) AND order-preserving (the ORDER BY/LIMIT) byte-identity.
    assert serial_sorted == _md5_sorted(g1_rows) == _md5_sorted(g8_rows), f"{name}: values differ"
    assert serial_order == _md5_listorder(g1_rows) == _md5_listorder(g8_rows), (
        f"{name}: ORDER BY output order differs from serial"
    )

    # The parallel agg sink must FIRE at every DOP (a source_prep iff there is a join).
    assert g1_fired == [(expected_class, 1, expects_join)], f"{name}: DOP=1 fired {g1_fired}"
    assert g8_fired == [(expected_class, 8, expects_join)], f"{name}: DOP=8 fired {g8_fired}"


@pytest.mark.parametrize(
    "name,sql,expected_class", _MULTI_JOIN_QUERIES, ids=[q[0] for q in _MULTI_JOIN_QUERIES]
)
def test_multi_join_agg_byte_identical(name, sql, expected_class):
    fired = []
    original = parallel_engine._run_breaker_segment

    def spy(plan, scan_id, middle_ids, breaker_id, recomb_class, workers, *args, **kwargs):
        sp = kwargs.get("source_prep")
        is_multi = sp is not None and sp.shared._shape.build_scan_ids is not None
        fired.append((recomb_class, workers, sp is not None, is_multi))
        return original(
            plan, scan_id, middle_ids, breaker_id, recomb_class, workers, *args, **kwargs
        )

    with _Cfg() as cfg:
        cfg.set(generic=False, workers=1, floor=10**12)
        serial_rows = _run_rows(sql)
        serial_sorted = _md5_sorted(serial_rows)
        serial_order = _md5_listorder(serial_rows)

        parallel_engine._run_breaker_segment = spy
        try:
            cfg.set(generic=True, workers=1, floor=0)
            fired.clear()
            g1_rows = _run_rows(sql)
            g1_fired = list(fired)

            cfg.set(generic=True, workers=8, floor=0)
            fired.clear()
            g8_rows = _run_rows(sql)
            g8_fired = list(fired)
        finally:
            parallel_engine._run_breaker_segment = original

    assert serial_sorted == _md5_sorted(g1_rows) == _md5_sorted(g8_rows), f"{name}: values differ"
    assert serial_order == _md5_listorder(g1_rows) == _md5_listorder(g8_rows), (
        f"{name}: ORDER BY output order differs from serial"
    )

    # The MULTI-JOIN composition must fire the grouped agg sink through the ONE skeleton
    # WITH a source_prep AND the multi-join build prelude (build_scan_ids set) at every DOP.
    assert g1_fired == [(expected_class, 1, True, True)], f"{name}: DOP=1 fired {g1_fired}"
    assert g8_fired == [(expected_class, 8, True, True)], f"{name}: DOP=8 fired {g8_fired}"


# =============================================================================
# Build-prelude parallelization — the TOP join's BUILD leg is itself a parallelisable
# inner join (Q3: customer ⋈ orders feeding (customer⋈orders) ⋈ lineitem). The inner
# join's big PROBE (orders) is parallel-probed instead of driven serially — the Amdahl
# anchor (Q3: 29% of wall). This test proves the inner-join parallel path FIRES at DOP>1
# (and does NOT at DOP=1 — the serial prelude, byte-identical), with the result
# byte-identical AND order-preserving (ORDER BY + LIMIT) across serial == DOP1 == DOP8.
# =============================================================================

_BUILD_PRELUDE_Q3 = (
    "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) revenue, "
    "o_orderdate, o_shippriority "
    f"FROM {_C}, {_O}, {_L} "
    "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey "
    "AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-22'::DATE "
    "AND l_shipdate > '1995-03-22'::DATE "
    "GROUP BY l_orderkey, o_orderdate, o_shippriority "
    "ORDER BY revenue DESC, o_orderdate LIMIT 10"
)


def test_build_prelude_inner_join_parallelizes():
    """The build prelude's inner ``customer ⋈ orders`` join must parallel-probe at DOP>1
    (firing ``_SharedSourceJoin._build_parallel_inner``) and stay SERIAL at DOP=1, with
    the Q3 result byte-identical and order-preserving across serial == DOP1 == DOP8."""
    sql = _BUILD_PRELUDE_Q3
    inner_fired = {"count": 0}
    orig_bpi = parallel_engine._SharedSourceJoin._build_parallel_inner

    def spy_bpi(self, *args, **kwargs):
        inner_fired["count"] += 1
        return orig_bpi(self, *args, **kwargs)

    with _Cfg() as cfg:
        # serial reference
        cfg.set(generic=False, workers=1, floor=10**12)
        serial_rows = _run_rows(sql)
        serial_sorted = _md5_sorted(serial_rows)
        serial_order = _md5_listorder(serial_rows)

        parallel_engine._SharedSourceJoin._build_parallel_inner = spy_bpi
        try:
            # DOP=1 → the build prelude drives serially (the inner-join parallel path must
            # NOT fire — the prime constraint, byte-identical to serial).
            cfg.set(generic=True, workers=1, floor=0)
            inner_fired["count"] = 0
            g1_rows = _run_rows(sql)
            g1_inner = inner_fired["count"]

            # DOP=8 → the inner join's PROBE parallelises (the anchor fix fires exactly
            # once: the single multi-join build prelude).
            cfg.set(generic=True, workers=8, floor=0)
            inner_fired["count"] = 0
            g8_rows = _run_rows(sql)
            g8_inner = inner_fired["count"]
        finally:
            parallel_engine._SharedSourceJoin._build_parallel_inner = orig_bpi

    assert serial_sorted == _md5_sorted(g1_rows) == _md5_sorted(g8_rows), "Q3 values differ"
    assert serial_order == _md5_listorder(g1_rows) == _md5_listorder(g8_rows), (
        "Q3 ORDER BY output order differs from serial"
    )
    assert g1_inner == 0, f"DOP=1 must NOT parallelise the build prelude (fired {g1_inner})"
    assert g8_inner == 1, (
        f"DOP=8 must parallelise the build prelude's inner join exactly once "
        f"(fired {g8_inner})"
    )


if __name__ == "__main__":  # pragma: no cover
    test_build_prelude_inner_join_parallelizes()
    print("OK  build_prelude_inner_join_parallelizes")
    for _name, _sql, _cls, _mw in _QUERIES:
        test_generic_pipeline_byte_identical(_name, _sql, _cls, _mw)
        print(f"OK  {_name}")
    for _name, _sql, _cls in _JOIN_AGG_QUERIES:
        test_join_agg_byte_identical(_name, _sql, _cls)
        print(f"OK  {_name}")
    for _name, _sql, _cls, _ej, _em in _SORT_TAIL_QUERIES:
        test_sort_tail_byte_identical(_name, _sql, _cls, _ej, _em)
        print(f"OK  {_name}")
    for _name, _sql, _cls in _MULTI_JOIN_QUERIES:
        test_multi_join_agg_byte_identical(_name, _sql, _cls)
        print(f"OK  {_name}")
    print("\nAll generic-pipeline golden differentials passed.")
