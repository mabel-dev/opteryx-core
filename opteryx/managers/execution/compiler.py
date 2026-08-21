# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The plan compiler — the LAST Python before execution.

Walks a compiled physical plan and builds the native pipeline graph (``NativePlan``,
the Cython edge over the C++ ``Engine`` in ``src/cpp/engine/engine.hpp``). This is
PLANNING: every decision — pipeline decomposition, column indices, output names and
types, aggregate specs, join wiring — is made here, once, in Python. Execution is
then 100% native: ``native_plan_execute`` submits one detached native driver that
runs the whole graph and streams the terminal pipeline into a ``MorselQueue`` the
cursor drains.

There is NO fallback. A plan node (or node configuration) the native engine has no
operator for raises ``NotSupportedError`` HERE, at plan time, naming exactly what is
missing. That is the ratified cutover posture (see the ``engine_cutover_decisions``
memory): broken and honest while coverage burns down, never a silent second engine.

Special (non-relational) operations — EXPLAIN / SET / SHOW / INSERT / DDL — never
reach this module; ``managers/execution/__init__.py`` routes them to serial_engine.
"""

import os
import threading

from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from opteryx.constants import ResultType
from opteryx.exceptions import CidrAggTypeError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import VariantKeyError
from opteryx.exceptions import compose
from opteryx.exceptions import md_code
from opteryx.exceptions import md_column
from opteryx.exceptions import md_syntax
from opteryx.expression import NodeType
from opteryx.operators.window.helpers import WINDOW_FUNCTIONS
from opteryx.types.logical_type import rescale_decimal_literal as _rescale_decimal_literal

_MAX_WORKER_CAP = 16
_QUEUE_DEPTH = 4

# Per-driver-thread engine pool cache. The engine pool is driven via
# submit_native + wait_native — and wait_native() is a POOL-GLOBAL barrier
# (BS::thread_pool::wait waits for EVERY task in the pool). That is only correct
# while a pool holds ONE query's tasks at a time, so the pool cannot be shared
# across concurrent queries. A thread-local cache preserves exactly that
# invariant: each driver thread reuses its own pool, and because a thread is
# occupied for the full duration of its query (the cursor blocks in out_q.get()
# until FINISHED), that pool only ever holds one query's tasks at a time — the
# same isolation a fresh-per-query pool gave, minus the per-query spawn/join of
# `dop` OS threads (~0.7ms at dop=16). The pool is returned to idle (not shut
# down) between queries and lives until its owning thread — or the interpreter —
# exits, when CppThreadPool.__dealloc__ joins its workers.
_engine_pool_tls = threading.local()


def _acquire_engine_pool(dop: int):
    """Return this driver thread's reusable engine pool for `dop` workers,
    constructing it on first use. Keyed by dop so a config change that alters the
    worker count yields a correctly-sized pool rather than a stale one."""
    from opteryx.compiled.thread_pool import CppThreadPool

    cache = getattr(_engine_pool_tls, "by_dop", None)
    if cache is None:
        cache = {}
        _engine_pool_tls.by_dop = cache
    pool = cache.get(dop)
    if pool is None:
        pool = CppThreadPool(dop, "engine")
        cache[dop] = pool
    return pool

_INT_TYPES = (
    DrakenType.INT8, DrakenType.INT16, DrakenType.INT32, DrakenType.INT64,
    # E33 — unsigned ints are aggregate/comparison operands the same way the
    # signed family is; the native engine's per-row read/emit paths (SUM/MIN/MAX,
    # GROUP BY / ORDER BY keys) already handle all eight widths correctly.
    DrakenType.UINT8, DrakenType.UINT16, DrakenType.UINT32, DrakenType.UINT64,
)
_NUMERIC_TYPES = _INT_TYPES + (DrakenType.FLOAT32, DrakenType.FLOAT64)
_COMPARE_OPS = {"Eq", "NotEq", "Lt", "Gt", "LtEq", "GtEq"}

_oversubscribe_warned = False


def resolve_worker_count(requested) -> int:
    """Effective degree of parallelism. Unset/"auto" is softcoded (cpu-2, capped);
    an explicit positive request is HONOURED EXACTLY (warned if oversubscribed,
    never silently reduced). DOP is a number, never a code-path selector."""
    cpu = os.cpu_count() or 1
    if requested is None or requested <= 0:
        return max(1, min(cpu - 2, _MAX_WORKER_CAP))
    requested = int(requested)
    if requested > cpu:
        global _oversubscribe_warned
        if not _oversubscribe_warned:
            _oversubscribe_warned = True
            import warnings

            warnings.warn(
                f"MAX_EXECUTION_WORKERS={requested} exceeds {cpu} physical cores "
                f"(oversubscribed). Honouring the explicit request as set.",
                stacklevel=2,
            )
    return max(1, requested)


def _skene_row_group_count(manifest, file_count: int) -> int:
    """Row groups a skene scan will read — its work item count, not its file count.

    A .skene file holds up to 16 row groups and the scan claims row groups, so
    reporting the file count here would understate the work by the packing
    factor. The manifest knows because the connector reads every file's footer to
    build it (FileEntry.row_group_count).

    The fallback is the file count, and it is a FLOOR rather than a guess: every
    file holds at least one row group. It is only reachable from a manifest
    producer that reads no skene footer, of which there is none today — a skene
    manifest is built exactly one way (FileSystemConnector's SKENE branch).
    """
    if manifest is None:
        return file_count
    total = manifest.get_row_group_count()
    return total if total is not None else file_count


def _fold_skene_scan_facts(nplan, telemetry) -> None:
    """Copy each skene scan's RUN-TIME row-group counts into `native_scan_facts`.

    `scan_facts` is built during compilation, so it can only carry plan-time
    numbers. Skene's row-group skipping is decided in the Source's claim builder,
    from footer statistics the plan never reads — leaving the fact at its 0
    placeholder would report "pruned nothing" for a scan that skipped most of its
    work, which reads as the optimization not existing.

    Called once, after the driver is finished, which is what orders the Source's
    single write (inside its `call_once`) before this read. A plan whose scan never
    ran reports `row_group_counts is None` and is left alone rather than being
    stamped with a fabricated 0.
    """
    if telemetry is None:
        return
    plans = getattr(nplan, "skene_scan_plans", None)
    if not plans:
        return
    facts = telemetry._reading.get("native_scan_facts")
    if not facts:
        return
    for plan in plans:
        identity = getattr(plan, "scan_identity", None)
        counts = getattr(plan, "row_group_counts", None)
        if identity is None or counts is None:
            continue
        entry = facts.get(identity)
        if entry is None:
            continue
        total, pruned = counts
        entry["row_groups_pruned"] = pruned
        # The claim builder counted the row groups it actually saw in the file
        # footers. That is a better number than the manifest's, and it is the
        # denominator `row_groups_pruned` is a fraction OF — reporting a pruned
        # count against a differently-derived total invites a nonsense ratio.
        entry["row_groups_read"] = total - pruned


def _and_conjuncts(node):
    """Flatten an ON tree's AND spine into its leaf conjuncts.

    Only the AND spine is walked: an OR anywhere in an ON clause is ONE leaf here,
    and — carrying no extractable equi key — is refused by the caller, which is the
    correct answer for it too.
    """
    from opteryx.expression import NodeType

    if node is None:
        return []
    if node.node_type == NodeType.AND:
        return _and_conjuncts(node.left) + _and_conjuncts(node.right)
    return [node]


# Said by CORR, MEDIAN and APPROX_PERCENTILE, which share one restriction: the sinks
# never descale DECIMAL's unscaled integer, so reading it as a raw double would
# compute the wrong numbers' statistics. One constant so the three cannot drift.
_NUMERIC_ONLY = (
    "Only numeric columns are accepted here - cast a DECIMAL column to "
    "`DOUBLE` first, for example `column::DOUBLE`"
)


def _type_name(schema_column, physical=None) -> str:
    """The reader-facing spelling of a column's type.

    `DrakenType.VARCHAR` is an internal enum's repr, and it was reaching people who
    had asked a question about their SQL. A ColumnType knows the canonical spelling
    INCLUDING any parameters (`DECIMAL(10, 2)`, `TIMESTAMP[ms]`), so it is preferred;
    the physical tag's own name is the fallback when no ColumnType is in hand.
    """
    column_type = getattr(schema_column, "column_type", None)
    if column_type is not None:
        # ColumnType's __str__ IS the canonical SQL type name - it delegates to
        # draken, which owns that mapping (see logical_type.py).
        return str(column_type)
    if physical is not None:
        return physical.name
    return "an unknown type"


def _live_positions(layout, live):
    """Positions of `layout` still wanted after the operator that owns `live`.

    `live` is a node's `pre_update_columns` — projection_pushdown's record of the
    active column set, snapshotted BEFORE the node's own columns are collected, so it
    never contains that operator's own working columns (join keys, ORDER BY keys,
    GROUP BY keys). That is the point: it is what survives once those columns' purpose
    is spent.

    Callers must not reach here with an empty `live`: empty means UNKNOWN, not
    "nothing is wanted", and dropping every column on an absent set would be a silent
    wrong answer. Asserted rather than defaulted, so a caller that forgets the guard
    fails loudly at plan time instead of quietly emitting nothing."""
    if not live:
        raise InvalidInternalStateError(
            "_live_positions called with an empty active-column set — empty means "
            "UNKNOWN, and the caller must keep every column rather than ask here"
        )
    return [i for i, identity in enumerate(layout) if identity in live]


# The synthetic GROUP BY ROLLUP key: which grouping set a result row belongs to, as a
# bitmask over the key list. Created by the native grouping-expand operator, so it is a
# stream identity the planner never mints and nothing below the aggregate can collide
# with. See src/cpp/engine/native_grouping_expand.hpp.
_GROUPING_ID_IDENTITY = "$grouping_id"


def _unsupported(what: str, remedy: str = None):
    """Refuse a query the engine cannot run, saying what and - where we know it - how.

    Reached from more than fifty gates, which makes this the most-read refusal in
    the system. It used to open with "native engine:" and close with "hard-cutover
    posture; coverage is being burned down" - a component name and a note to
    ourselves about the roadmap, neither of which is the reader's business or any
    help to them.

    `remedy` is separate from `what` so the advice is a sentence of its own rather
    than being welded onto the end of the description with a dash.
    """
    raise NotSupportedError(
        compose(
            f"{what} is not supported",
            remedy
            or "This query cannot run as written - it will need rewriting to avoid "
            "that construct",
        )
    )


_INT64_MAX = (1 << 63) - 1


def _estimate_to_int64(value, what: str) -> int:
    """Planner estimates cross the native boundary as ``int64_t`` here.

    ``None`` means unknown and crosses as -1, the setters' sentinel. Anything
    else outside [0, INT64_MAX] is refused loudly: an estimator emitting more
    than ~9.2e18 rows (or a negative count) is always an estimator bug, never
    a real workload, and saturating it to INT64_MAX would hide the next such
    bug behind a plausible-looking plan. Before this guard, a 3.6e19 estimate
    (TPC-DS Q54, DNF selectivity) died in the setter's implicit coercion with
    a bare OverflowError naming neither the operator nor the number.
    """
    if value is None:
        return -1
    estimate = int(value)
    if estimate < 0 or estimate > _INT64_MAX:
        raise InvalidInternalStateError(
            compose(
                f"The planner's {what} is {md_code(estimate)}, which is "
                "outside the signed 64-bit range the engine carries estimates in",
                "An estimate this size is always a cost-estimator bug, never a "
                "real workload - the estimator needs fixing, not the query",
            )
        )
    return estimate


# Physical types the native scan reads through its plain "int" kind: every integer
# width and signedness, plus TIME32/TIME64 (parquet TIME binds to a TIME32/TIME64
# schema type but decodes as a plain int stream — the binder models no TIME
# coercion, so the trampoline emits it as an int too).
_INT_KIND_TYPES = frozenset((
    DrakenType.INT8, DrakenType.INT16, DrakenType.INT32, DrakenType.INT64,
    DrakenType.UINT8, DrakenType.UINT16, DrakenType.UINT32, DrakenType.UINT64,
    DrakenType.TIME32, DrakenType.TIME64,
))


def _physical_type(schema_column):
    ct = getattr(schema_column, "column_type", None)
    return ct.physical if ct is not None else None


def _computed_array_subexpression(node, _depth=0):
    """The first COMPUTED (non-column, non-literal) ARRAY sub-expression in `node`,
    or None.

    An ARRAY's elements hang off the column owner, not off the 40-byte
    DrakenVector, so every element-reading op resolves them by column identity
    against the morsel - which a mid-expression intermediate does not have. The
    compiler materializes such an operand into its own column where it can
    (_hoist_array_operands), but the hoist only runs on the WHERE and HAVING
    predicates. Everywhere else the refusal stands, and the ARRAY is the reason -
    so name it, and name the rewrite that gives it the column it needs.
    """
    if node is None or _depth > 32:
        return None
    if node.node_type not in (
        NodeType.IDENTIFIER, NodeType.EVALUATED, NodeType.AGGREGATOR, NodeType.LITERAL
    ):
        sc = getattr(node, "schema_column", None)
        if sc is not None and _physical_type(sc) == DrakenType.ARRAY:
            return node
    for child in (getattr(node, "parameters", None) or []):
        found = _computed_array_subexpression(child, _depth + 1)
        if found is not None:
            return found
    for attr in ("left", "right"):
        found = _computed_array_subexpression(getattr(node, attr, None), _depth + 1)
        if found is not None:
            return found
    return None


def _predicate_remedy(expr):
    """The rewrite for a refused predicate, when we know one; None for the default.

    Only the computed-ARRAY case is answered here, because it is the only one where
    a mechanical rewrite is always available AND always equivalent: project the
    array in a subquery, filter outside it."""
    from opteryx.expression.formatter import format_expression

    array_node = _computed_array_subexpression(expr)
    if array_node is None:
        return None
    rendered = format_expression(array_node)
    return (
        f"{md_code(rendered)} builds an array mid-expression, and array element "
        "tests need it as a column. Project it in a subquery and filter outside "
        f"that subquery: {md_code(f'SELECT * FROM (SELECT *, {rendered} AS keys FROM ...) AS s WHERE ...')}"
    )


def _logical_tuple(ct):
    """(kind, unit, precision, scale, dimension) ints for a ColumnType's descriptor,
    or None when the type carries no logical type — same shape NativePlan's native
    calls (e.g. add_expr_project) already accept, so callers pass it straight through."""
    if ct is None or ct.logical is None:
        return None
    lg = ct.logical
    return (int(lg.kind.value), int(getattr(lg.unit, "value", 0)),
            int(lg.precision), int(lg.scale), int(getattr(lg, "dimension", 0) or 0))


def _element_chain(ct):
    """An ARRAY ColumnType's element subtree, flattened for the native wire.

    SIX ints per nesting level — (physical type, kind, unit, precision, scale,
    dimension) — outermost element first, so ARRAY<VARCHAR> is one level and
    ARRAY<ARRAY<INT64>> is two. Empty for every non-ARRAY column.

    A zero-row ARRAY column is only well-formed with a child vector (buffers.h /
    vector_owner.h), and gather_rows builds one even when every row it is asked for
    is NULL — an all-NULL ARRAY half still emits a typed, empty child. Without the
    element type there is nothing to type that child as, which is what made a FULL
    OUTER join with an ARRAY in the probe payload unrunnable. An unresolved element
    type yields an empty chain here and the native side fails loud on it; it is never
    guessed. See ``engine.hpp::decode_elem_chain``."""
    chain = []
    while ct is not None and ct.physical == DrakenType.ARRAY:
        ct = ct.element
        if ct is None:
            break
        lg = _logical_tuple(ct) or (0, 0, 0, 0, 0)
        chain.extend((int(ct.physical.value),) + lg)
    return chain


# WP-11 logical-coercion packing — mirrors the LC_* enum in
# src/cpp/engine/native_parquet_scan_source.hpp exactly:
#   packed = kind | (unit << 4) | (precision << 8) | (scale << 16)
_LC_DECIMAL64 = 1
_LC_DATE = 3
_LC_TIMESTAMP = 4
# R6: ARRAY whose ELEMENT is a TIMESTAMP — the parquet list<timestamp> leaf decodes
# as physical int64 and the IPC wire format carries no logical type, so the CHILD
# vector needs the same unit-carrying retag the scalar case gets. Mirrors the
# trampoline scan's `_sp_array_ts_unit_map` (parquet_read.pyx coerce op kind 4).
_LC_ARRAY_TIMESTAMP = 5
# IPV4 refines UINT32 rather than completing a parameterized physical type, so the
# wire carries a plain unsigned integer and nothing in the footer says "address".
# Without this retag a catalog-declared IPV4 column reaches execution as bare
# UINT32: it renders as an integer instead of dotted-decimal, and CIDR_AGG — which
# requires the descriptor — refuses it. See logical_type.h on why IPV4 is
# nonetheless carried rather than treated as droppable.
_LC_IPV4 = 6
# TimestampUnit enum-name → draken unit code (matches logical_type.h TimestampUnit).
_TS_UNIT_TO_INT = {"SECONDS": 0, "MILLISECONDS": 1, "MICROSECONDS": 2, "NANOSECONDS": 3}


def _ipv4_coerce(sc, pt):
    """Packed IPV4 retag for an integer read-set column (0 = none).

    Separate from `_wp11_logical_coerce` because an IPv4 column takes the ORDINARY
    int decode path — its footer annotation is `uint32`, and the native scan gate
    admits it as kind "int". Only the descriptor is added; the physical tag,
    width and decode are untouched."""
    if pt != DrakenType.UINT32:
        return 0
    ct = sc.column_type
    lg = ct.logical if ct is not None else None
    if lg is None or lg.kind != LogicalKind.IPV4:
        return 0
    return _LC_IPV4


def _wp11_unit(sc):
    """draken unit code (0=s,1=ms,2=us,3=ns) for a TIMESTAMP read-set column, from
    its schema logical unit; defaults to microseconds (matches the trampoline scan's
    "us" fallback in _sp_timestamp_unit_map)."""
    ct = sc.column_type
    lg = ct.logical if ct is not None else None
    if lg is None or lg.unit is None:
        return 2
    return _TS_UNIT_TO_INT.get(lg.unit.name, 2)


def _r6_array_element_coerce(sc):
    """R6: packed logical-coercion for an ARRAY read-set column (0 = none).

    The ONLY element coercion the trampoline scan performs is ARRAY<TIMESTAMP>:
    `_sp_array_ts_unit_map` in parquet_read.pyx retags the INT64 leaf to
    TIMESTAMP64 with the ELEMENT's unit, because parquet stores a list<timestamp>
    leaf as physical int64 and the IPC list format carries no logical type. Every
    other element type (including ARRAY<DATE>, which the trampoline also leaves as
    its raw int32 leaf) passes through untouched — parity, not judgement."""
    ct = sc.column_type
    element = ct.element if ct is not None else None
    if element is None or element.physical != DrakenType.TIMESTAMP64:
        return 0
    lg = element.logical
    unit = 2 if lg is None or lg.unit is None else _TS_UNIT_TO_INT.get(lg.unit.name, 2)
    return _LC_ARRAY_TIMESTAMP | (unit << 4)


def _wp11_logical_coerce(sc, pt):
    """WP-11: for a DECIMAL / DATE / TIMESTAMP read-set column, return
    ``(kind_str, is_int64_decimal, packed_logical_coerce)`` describing the native
    retag; return None for any other physical type, and None (fail-closed) for an
    int64-backed DECIMAL whose plan-time precision/scale is missing or out of the
    int64 range (>18) — the native decimal wire format carries no descriptor, so
    without a valid plan-time precision/scale the scan must stay on the trampoline.

    Parquet TIME is NOT handled here: the binder decodes it as plain INT64 (no TIME
    logical type is modelled from a scan), so a time column reaches the ordinary int
    path — see native_scan_supported's "int" footer branch, which admits a
    time-annotated int column.

    `kind_str` feeds native_scan_supported's footer gate; `packed` feeds the native
    Source's build_column retag (LC_* packing)."""
    if pt == DrakenType.DATE32:
        return ("date", False, _LC_DATE)
    if pt == DrakenType.TIMESTAMP64:
        return ("timestamp", False, _LC_TIMESTAMP | (_wp11_unit(sc) << 4))
    if pt == DrakenType.DECIMAL:
        ct = sc.column_type
        lg = ct.logical if ct is not None else None
        if lg is None or lg.precision is None or lg.scale is None or lg.precision > 18:
            return None
        return ("decimal64", True,
                _LC_DECIMAL64 | (int(lg.precision) << 8) | (int(lg.scale) << 16))
    if pt == DrakenType.DECIMAL128:
        # Self-describing: rugo fills ColumnOut.dec_precision/dec_scale from the
        # footer, and build_column attaches the descriptor from those — no packing.
        return ("decimal128", False, 0)
    return None


def _const_scalar_vector(dtype, value):
    """Build a length-1 constant-encoded Vector for an `IDENTIFIER = LITERAL`
    const-replacement column (see the FilterNode branch below). Returns None for a
    dtype/value combination this can't safely represent (temporal, decimal, a
    literal Python type that doesn't map onto dtype, etc.) — the caller skips the
    broadcast optimization for that column and it takes the ordinary gather path.

    Mirrors opteryx/operators/filter/filter.pyx's `_build_constant_vector_for_type`
    (same dispatch, same supported-type set) — that Cython copy is unreachable from
    the native engine's execution path (FilterNode.push is never called; only its
    plan-time attributes are read here), so this is deliberately NOT a shared
    import across the two — see the engine-cutover note this function was added
    alongside."""
    import draken.draken_native as _draken_native
    from draken.vectors.vector import Vector

    if dtype == DrakenType.BOOL:
        if not isinstance(value, bool):
            return None
        return Vector(_draken_native.vector_from_bool_constant(value, 1))
    if dtype == DrakenType.INT64:
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return Vector(_draken_native.vector_from_constant(value, 1))
    if dtype == DrakenType.FLOAT64:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return Vector(_draken_native.vector_float64_from_constant(float(value), 1))
    if dtype in (DrakenType.VARCHAR, DrakenType.NVARCHAR):
        if not isinstance(value, (str, bytes)):
            return None
        if isinstance(value, str):
            value = value.encode("utf-8")
        if dtype == DrakenType.NVARCHAR:
            return Vector(_draken_native.vector_nvarchar_from_constant(value, 1))
        return Vector(_draken_native.vector_varchar_from_constant(value, 1))
    if dtype == DrakenType.VARBINARY:
        if not isinstance(value, (str, bytes)):
            return None
        if isinstance(value, str):
            value = value.encode()
        return Vector(_draken_native.vector_varbinary_from_constant(value, 1))
    return None


class _Compiler:
    def __init__(self, plan, nplan, pool=None):
        self.plan = plan
        self.nplan = nplan
        # Gap #3 Phase 2b: the query's exec CppThreadPool, if constructed early
        # enough to be available at scan-compile time (see compile_to_native /
        # execute_native). Forwarded to open_native_scan_plan so the scan's decode
        # pool is SHARED with execution instead of a second, uncoordinated pool.
        # None is a fully supported value — every scan-opening call site falls back
        # to its own self-constructed pool, unchanged behaviour (EXPLAIN, tests that
        # compile a plan directly, any caller outside execute_native).
        self._pool = pool
        # WP-INSTR (instrument 2): per-scan Source-type selection, keyed by scan
        # node identity. "NativeParquetScanSource" == zero-Python native pull;
        # "StreamingScanSource" == the GIL trampoline. Later work packages assert
        # string/predicate scans migrate from the latter to the former.
        self.scan_sources: dict = {}
        # WP-02: per-native-scan relocated residual filter, keyed by scan node
        # identity. When a pushed predicate is lowered to a c-native span and its
        # scan admitted natively, this carries the wiring from _native_scan_plan to
        # _compile_scan: (filter_bc, read_layout, emit_indices, emit_ids,
        # need_select). Built per execute() and discarded with the compiler — no
        # cross-query shared state.
        self._relocated_scan_filters: dict = {}
        # Per-native-scan plan-time facts, keyed by scan node identity. On the
        # native path the Cython ParquetReadNode never executes, so its
        # ScanReadings (row_groups_read/files_read/…) stay zero — these carry the
        # real values, harvested into telemetry and overlaid by mermaid.py.
        self.scan_facts: dict = {}
        # A0 acceptance gate: per-scan residual-reason code, keyed by scan node
        # identity, recorded when a parquet scan falls back to the per-morsel
        # Python trampoline (StreamingScanSource). The value is the stable string
        # for WHICH `_native_scan_plan` guard fired (one of the R1..R7 codes — see
        # that method). Parallel to `scan_sources`: every "StreamingScanSource"
        # entry has exactly one reason here. Plan-time only; never touched per
        # morsel. Folded into telemetry `_reading["scan_residual_reasons"]`.
        self.scan_residual_reasons: dict = {}
        # Wall time (ns) spent inside open_native_scan_plan's cold-cache footer
        # fetch/parse, summed across every scan this compile touches. This is
        # network IO, not plan compilation — execute_native subtracts it out of
        # time_engine_compile and reports it separately (time_engine_footer_fetch)
        # so "compile" stops silently meaning "compile plus however many blobs'
        # footers were uncached."
        self.footer_fetch_ns = 0
        # Shared CTE result buffers: cte_key -> (buffer handle, body output
        # layout). Written by compile_to_native when it lowers each shared body
        # (producer pipelines, created BEFORE the main plan's so run()'s
        # creation-order execution fills every buffer before anything reads it);
        # read by the CteRefNode arm of compile_node. The dict OBJECT is shared
        # between the body compilers and the main compiler.
        self.cte_buffers: dict = {}

    # ---- expression lowering ------------------------------------------------------
    # Expressions are lowered ONCE, at plan time, to the phase-9 flat bytecode whose
    # compute instructions carry C kernel fn pointers, then resolved against the
    # stream layout (column indices, bind-time literal vectors). Only all-c-native
    # programs are admitted — anything else fails loud here, before execution.

    def _hash_key_identities(self):
        """E37: the set of column IDENTITIES consumed as a GROUP BY / JOIN / DISTINCT
        key ANYWHERE in the plan. A scan column whose identity is in this set has its
        hash seed carried (keyhash_buf) for hash-once-use-many reuse; every other
        string column builds no sidecar (the pay-for-use default). Computed once per
        compile over the whole plan graph, then cached.

        A TRANSFORMED key (e.g. GROUP BY UPPER(url)) keys on the UPPER *output*
        identity, not the scan column — so the scan column is correctly NOT marked;
        carrying the seed for computed keys is the string-kernel step (E37 step 2).
        A bare `SELECT DISTINCT` (empty `_distinct_on`, dedup over all columns) is
        left unmarked here — correct but unoptimized, refined later."""
        ids = getattr(self, "_hash_key_ids_cache", None)
        if ids is not None:
            return ids
        ids = set()
        for _, node in self.plan.nodes(True):
            for ident in (getattr(node, "group_by_columns", None) or []):
                ids.add(ident)
            for ident in (getattr(node, "left_columns", None) or []):
                ids.add(ident)
            for ident in (getattr(node, "right_columns", None) or []):
                ids.add(ident)
            for ident in (getattr(node, "_distinct_on", None) or []):
                ids.add(ident)
        self._hash_key_ids_cache = ids
        return ids

    def _rewrite_between(self, expr):
        """PLAN-TIME tree rewrite: BETWEEN(operand; lower, upper) becomes
        AND(operand >= lower, operand <= upper) (bounds' inclusivity respected).

        Delegates to the canonical `expand_between` — `lower()` applies the same
        rewrite unconditionally, so this call is only needed to run it BEFORE
        `_rewrite_decimal_compares`, which must see the expanded compares to
        rescale decimal bounds."""
        from opteryx.compiled.expression.compiled_expression import expand_between

        return expand_between(expr)

    def _rewrite_case(self, expr):
        """PLAN-TIME tree rewrite: CASE becomes a right-folded IF_THEN_ELSE
        function chain over the c-native blend kernel (draken_if_then_else).
        The kernel blends same-type raw domains only, so literal THEN/ELSE
        branches are retyped to the CASE's bound output ColumnType (DECIMAL:
        exact quantize only; FLOAT: numeric literals). A missing ELSE becomes
        a typed NULL literal. The outermost node keeps the CASE's
        schema_column so downstream identity references still resolve."""
        import decimal as _dec

        from opteryx.compiled.structures.node import Node

        if not isinstance(expr, Node):
            return expr
        if expr.node_type == NodeType.CASE:
            conditions = [self._rewrite_case(c) for c in expr.conditions]
            results = [self._rewrite_case(r) for r in expr.results]
            els = (self._rewrite_case(expr.else_result)
                   if expr.else_result is not None else None)
            sc = getattr(expr, "schema_column", None)
            out_ct = getattr(sc, "column_type", None)

            def _coerce(r):
                if out_ct is None or getattr(r, "node_type", None) != NodeType.LITERAL:
                    return r
                v = r.value
                phys = getattr(out_ct.physical, "name", "")
                if v is None:
                    nl = Node(NodeType.LITERAL, value=None)
                    nl.type = out_ct
                    return nl
                if isinstance(v, bool) or not isinstance(v, (int, float, _dec.Decimal)):
                    return r
                if phys in ("DECIMAL", "DECIMAL128") and out_ct.logical is not None:
                    scale = int(out_ct.logical.scale)
                    q = _dec.Decimal(v) if isinstance(v, float) else _dec.Decimal(str(v))
                    rescaled = q.quantize(_dec.Decimal(1).scaleb(-scale))
                    if rescaled != q:
                        return r   # inexact — leave it, kernel fails loud
                    nl = Node(NodeType.LITERAL, value=rescaled)
                    # The literal carries the CASE's own declared type, both tiers
                    # alike. It used to be pinned to an int64-tier DECIMAL(18, scale)
                    # for a DECIMAL128 target, on the grounds that a DECIMAL128
                    # literal could not be materialised — no longer true, and the
                    # pin was not merely redundant: precision 18 with scale 18 can
                    # represent nothing but a fraction, so a CASE blending a DECIMAL
                    # column with a literal (result DECIMAL(38,18), the ordinary
                    # shape for `ELSE 1.5000`) rescaled 1.5 to 19 unscaled digits and
                    # raised a raw OverflowError, "decimal: value exceeds declared
                    # precision". _materialise_constant_literal routes on the tag and
                    # on precision > 18, so a DECIMAL128-typed literal materialises
                    # through vector_decimal128_from_constant.
                    nl.type = out_ct
                    return nl
                if phys in ("FLOAT64", "FLOAT32"):
                    nl = Node(NodeType.LITERAL, value=float(v))
                    nl.type = out_ct
                    return nl
                return r

            # Same-scale contract: the blend kernel widens {DECIMAL, DECIMAL128}
            # raw values assuming equal scales. A non-literal decimal branch at a
            # DIFFERENT scale cannot blend correctly — leave the CASE unrewritten
            # (it fails loud downstream) rather than answer wrong.
            if (out_ct is not None and out_ct.logical is not None
                    and getattr(out_ct.physical, "name", "") in ("DECIMAL", "DECIMAL128")):
                for br in list(results) + ([els] if els is not None else []):
                    if getattr(br, "node_type", None) == NodeType.LITERAL:
                        continue
                    br_ct = getattr(getattr(br, "schema_column", None), "column_type", None)
                    if (br_ct is not None and br_ct.logical is not None
                            and getattr(br_ct.physical, "name", "") in ("DECIMAL", "DECIMAL128")
                            and int(br_ct.logical.scale) != int(out_ct.logical.scale)):
                        return expr

            acc = _coerce(els) if els is not None else _coerce(
                Node(NodeType.LITERAL, value=None))
            for cond, res in zip(reversed(conditions), reversed(results)):
                f = Node(NodeType.FUNCTION, value="IF_THEN_ELSE",
                         parameters=[cond, _coerce(res), acc])
                acc = f
            acc.schema_column = sc
            acc.alias = getattr(expr, "alias", None)
            return acc
        rebuilt = None
        for attr in ("left", "right", "centre"):
            child = getattr(expr, attr)
            if isinstance(child, Node):
                new_child = self._rewrite_case(child)
                if new_child is not child:
                    if rebuilt is None:
                        rebuilt = expr.copy()
                    setattr(rebuilt, attr, new_child)
        params = expr.parameters
        if isinstance(params, list):
            new_params = [self._rewrite_case(c) if isinstance(c, Node) else c
                          for c in params]
            if any(x is not y for x, y in zip(new_params, params)):
                if rebuilt is None:
                    rebuilt = expr.copy()
                rebuilt.parameters = new_params
        return rebuilt if rebuilt is not None else expr

    def _rewrite_decimal_compares(self, expr):
        """PLAN-TIME literal rescale: `decimal_col <op> numeric_literal` becomes a
        same-type, same-scale compare by quantizing the literal to the column's
        declared scale (the raw int64 domain then orders correctly — the compare
        kernel's contract). INEXACT ordering bounds (folded double noise such as
        0.06 - 0.01 = 0.049999...) are rounded DIRECTION-AWARE — exact, because
        column values are integer multiples of 10^-scale (col >= 0.049999...
        iff unscaled >= ceil(bound * 10^scale)). Inexact Eq/NotEq are skipped
        and stay loud. DECIMAL128 columns are not rewritten (the constant would
        materialize at the int64 tier)."""
        import decimal as _dec

        from opteryx.compiled.structures.node import Node

        if not isinstance(expr, Node):
            return expr
        if expr.node_type == NodeType.COMPARISON_OPERATOR:
            new = None
            for a, b in (("left", "right"), ("right", "left")):
                col = getattr(expr, a)
                lit = getattr(expr, b)
                # duck-typed: identifiers may not be compiled-Node instances
                if col is None or lit is None:
                    continue
                if getattr(lit, "node_type", None) != NodeType.LITERAL:
                    continue
                sc = getattr(col, "schema_column", None)
                ct = getattr(sc, "column_type", None) if sc is not None else None
                if (ct is None or ct.logical is None
                        or getattr(ct.physical, "name", "") != "DECIMAL"):
                    continue
                v = lit.value
                if isinstance(v, bool) or not isinstance(v, (int, float, _dec.Decimal)):
                    continue
                # `str()` first, for floats too. Python's float repr is
                # shortest-roundtrip, so it recovers the decimal the user actually
                # WROTE for a source literal, and keeps the noise for a genuinely
                # computed one:
                #
                #   typed    9.8        -> '9.8'                  (on a scale-1 gridline)
                #   computed 0.06-0.01  -> '0.049999999999999996' (not on a scale-2 one)
                #
                # Taking the exact binary rational instead made every source decimal
                # literal look inexact — `Decimal(9.8)` is 9.80000000000000071 — so the
                # direction-aware branch below fired on literals it was never meant for
                # and rounded the bound to the NEXT gridline: `gravity < 9.8` became
                # `gravity < 9.9` and returned the 9.8 row. `<` and `<=` gave identical
                # answers, and `<`/`=`/`>` over a NULL-free column summed to MORE than
                # the table (the 9.8 row landed in both `<` and `=`) — trichotomy broken.
                #
                # This does NOT weaken the folded-double case the rounding exists for:
                # that value's shortest repr is still off-gridline (see above), so the
                # branch still fires and still rounds direction-aware.
                #
                # It is a mitigation, not the root fix. Opteryx types `9.8` and `9.8e0`
                # both as FLOAT64, where the SQL standard makes the first an EXACT
                # numeric literal (DECIMAL) and only the second approximate. Fixing that
                # removes the float from this path altogether — tracked separately.
                # The rule itself lives in types/logical_type.py, shared with
                # Manifest's bound pruning — a pruner that rounded a boundary
                # literal differently from this rewrite would drop a file or row
                # group whose rows this predicate then keeps. `None` back means the
                # literal cannot be put on the column's scale gridline without
                # changing what it matches (an inexact Eq/NotEq): leave it alone
                # and let the kernel fail loud, exactly as before.
                #
                # The op passed is the effective one on the COLUMN, swapped when
                # the literal is the left operand.
                eff = expr.value if a == "left" else {
                    "Lt": "Gt", "Gt": "Lt", "LtEq": "GtEq", "GtEq": "LtEq"}.get(
                        expr.value, expr.value)
                rescaled = _rescale_decimal_literal(ct, v, eff)
                if rescaled is None:
                    continue
                # The literal need not FIT the column's declared precision:
                # `gravity DECIMAL(3,1) = 999` is a perfectly legal predicate that
                # simply matches nothing. Stamping the column's ColumnType on an
                # out-of-range literal made the constant materialiser raise
                # OverflowError("decimal: value exceeds declared precision") and
                # killed the whole query — an unrunnable predicate, not a wrong
                # answer, but still a valid query refused.
                #
                # Only SCALE carries the kernel contract (equal scale => the raw
                # int64 domain orders correctly); precision is a declared width.
                # So widen precision just enough to hold the literal and keep the
                # scale, which leaves the compare exact. Past the int64 tier's 18
                # digits we skip instead: DECIMAL128 here would break the
                # same-tier assumption this rewrite is built on (see docstring).
                _scale = int(ct.logical.scale)
                _unscaled = abs(int(rescaled.scaleb(_scale)))
                _needed_p = len(str(_unscaled))
                if _needed_p <= int(ct.logical.precision):
                    _lit_type = ct
                elif _needed_p <= 18:
                    from opteryx.types import logical_type as _lt
                    _lit_type = _lt.DECIMAL(_needed_p, _scale)
                else:
                    continue
                nl = Node(NodeType.LITERAL, value=rescaled)
                nl.type = _lit_type
                if new is None:
                    new = expr.copy()
                setattr(new, b, nl)
            return new if new is not None else expr
        rebuilt = None
        for attr in ("left", "right", "centre"):
            child = getattr(expr, attr)
            if isinstance(child, Node):
                new_child = self._rewrite_decimal_compares(child)
                if new_child is not child:
                    if rebuilt is None:
                        rebuilt = expr.copy()
                    setattr(rebuilt, attr, new_child)
        params = expr.parameters
        if isinstance(params, list):
            new_params = [self._rewrite_decimal_compares(c) if isinstance(c, Node) else c
                          for c in params]
            if any(x is not y for x, y in zip(new_params, params)):
                if rebuilt is None:
                    rebuilt = expr.copy()
                rebuilt.parameters = new_params
        return rebuilt if rebuilt is not None else expr

    def _lower_bytecode(self, expr):
        """Lower `expr` to a `CompiledBytecode` through the standard plan-time
        rewrites (CASE→IF_THEN_ELSE, BETWEEN→compares, decimal-literal rescale).
        Does NOT gate on c-nativeness — the caller decides whether a non-c-native
        program is a hard error (`_lower_expression`) or a fail-closed signal
        (`_native_scan_plan`, WP-02)."""
        from opteryx.compiled.expression.compiled_expression import build_bytecode
        from opteryx.compiled.expression.compiled_expression import lower

        return build_bytecode(lower(
            self._rewrite_decimal_compares(self._rewrite_between(
                self._rewrite_case(expr)))))

    def _lower_expression(self, expr, what):
        # Every caller lowers a BOOLEAN predicate (WHERE / HAVING / nested-loop
        # `on`) that is handed to add_expr_filter. Admit exactly what the engine's
        # filter admits: bytecode_is_c_native_predicate — every op c-native AND a
        # bool-mask-final program. That is STRICTLY broader than is_all_c_native,
        # which additionally forbids BC_C_NATIVE_DESC (DECIMAL / TIMESTAMP64 cast)
        # results because it ALSO gates the GIL evaluate_c_native path, which has no
        # descriptor re-attachment point. Inside a predicate a DESC result is only
        # consumed by a compare that folds it RAW and yields a bool mask — the
        # descriptor is never surfaced — so `col::TIMESTAMP[s] >= <ts>` is safe and
        # correct here even though it is not is_all_c_native. (The engine's own
        # add_expr_filter enforces this same bytecode_is_c_native_predicate gate.)
        #
        # On refusal, name the OPERATION and the sub-expression it sits in, the way
        # the projection gate already does (_add_computed). "a filter predicate
        # outside the c-native kernel set ... it will need rewriting to avoid that
        # construct" named no construct and no rewrite, so every refusal here read
        # identically and left the reader nothing to act on.
        from opteryx.expression.formatter import format_expression
        from opteryx.operators._operators import bytecode_is_c_native_predicate
        from opteryx.operators._operators import bytecode_non_c_native_op

        bc = self._lower_bytecode(expr)
        if not bytecode_is_c_native_predicate(bc):
            # The gate has two independent halves; say which one said no rather than
            # blaming an operation when the program was in fact non-bool-final.
            offending_op = bytecode_non_c_native_op(bc)
            if offending_op:
                _unsupported(
                    f"{offending_op} in {what} {md_code(format_expression(expr))}, "
                    "outside the c-native kernel set,",
                    _predicate_remedy(expr),
                )
            _unsupported(
                f"{what} that does not produce a true/false result "
                f"({md_code(format_expression(expr))})",
                f"Wrap it as {md_code('<expression> IS TRUE')} to make the "
                "true/false test explicit",
            )
        return bc

    def _lower_scan_predicate(self, predicates):
        """Lower a scan-PUSHED predicate — the ONE admission point for the three
        plans that consume one (`_native_scan_plan`'s relocated ExprFilter, both
        latmat pass-1 spans, and the trampoline scan's `compiled_predicate`).

        Those three gate c-nativeness themselves and DECLINE to a broader path when
        it fails — that is a routing decision, not a refusal. Bool-finalness is not
        routable: every path that consumes a predicate needs a mask, so a program
        that does not end in one is unrunnable everywhere and belongs to the user as
        a refusal here. It used to reach `add_expr_filter`, which re-tested the same
        thing and raised a raw ValueError saying the compiler should have rejected it
        earlier — or, on the latmat path, reached `_ensure_dense_bitmap_c`, which
        reads a non-mask result's bytes AS a bitmap and answers wrongly in silence.

        What lands here is NOT a non-boolean WHERE clause — `visit_filter` already
        refuses those by declared type, naming the expression. It is an expression
        the binder typed BOOLEAN whose final opcode is not one of the recognised mask
        producers, i.e. a bool-returning kernel not yet marked BC_RESULT_WRAP_AS_BOOL
        (COALESCE/IFNULL over BOOLEAN branches, today). `IS TRUE` over the same
        expression is a real remedy — it appends a BC_UNARY_OP, which IS a mask — so
        the message offers it, but as an instruction rather than as a rewritten
        query: `format_expression` renders the plan's spelling, not the user's (the
        pushed predicate is post-rewrite, so a COALESCE comes back as an IFNULL), and
        that is fine for locating the clause but not for pasting back in."""
        from opteryx.expression import format_expression
        from opteryx.operators._operators import bytecode_is_bool_final

        root = self._compose_predicate_nodes(predicates)
        bc = self._lower_bytecode(root)
        if not bytecode_is_bool_final(bc):
            _unsupported(
                f"filtering on {md_code(format_expression(root))} directly",
                "Make the condition explicit - compare the expression, or add "
                f"{md_code('IS TRUE')} to it",
            )
        return bc

    def _compose_predicate_nodes(self, predicates):
        """AND-compose a list of pushed predicate nodes into one right-leaning tree.

        The SOLE composer for a pushed predicate: both the relocated native filter
        (`_native_scan_plan`) and the trampoline scan's `compiled_predicate` (bound
        in `_compile_scan`) lower this same tree through `_lower_scan_predicate`, so
        the two paths run identical bytecode. The scan used to re-compose and re-lower
        the predicate itself at execute() time, skipping the rewrite chain — which
        silently returned wrong rows for off-scale decimal compares. `predicates`
        is non-empty (the caller guards)."""
        from opteryx.compiled.structures.node import Node
        from opteryx.utils import random_string

        nodes = [p.copy() for p in predicates if p is not None]
        root = nodes.pop()
        while nodes:
            right = nodes.pop()
            root = Node(
                NodeType.AND,
                left=root,
                right=right,
                schema_column=Node("schema_column", identity=random_string()),
            )
        return root

    def _resolve_const_replacements(self, node, layout):
        """Resolve a FilterNode's `IDENTIFIER = LITERAL` const-replacements (already
        extracted at plan-node construction time — see
        opteryx/operators/filter/filter.pyx's `_extract_constant_replacements`,
        which populates `node._const_replacements`) against this pipeline's layout.

        Returns parallel (const_col_idx, const_scalar_vecs) lists for
        NativePlan.add_expr_filter: a column proven constant on every surviving row
        is broadcast O(1) from the scalar Vector instead of being gathered and
        discarded. A replacement is skipped (falls through to the ordinary gather,
        same answer, just without the optimization) when its column isn't in this
        layout or `_const_scalar_vector` doesn't support the concrete
        type/literal combination (temporal, decimal, etc.)."""
        replacements = getattr(node, "_const_replacements", None)
        if not replacements:
            return [], []
        const_col_idx = []
        const_scalar_vecs = []
        for identity, value in replacements:
            if identity not in layout:
                continue
            dtype = self._layout_type(node, identity)
            if dtype is None:
                continue
            vec = _const_scalar_vector(dtype, value)
            if vec is None:
                continue
            const_col_idx.append(layout.index(identity))
            const_scalar_vecs.append(vec)
        return const_col_idx, const_scalar_vecs

    # Functions that CONSUME an ARRAY operand. SORT/GREATEST/LEAST read it
    # element-wise and need the child vector; LENGTH reads only the offsets. Both
    # still require a COLUMN operand — see _hoist_array_operands for why the second
    # case is not an exception.
    _ARRAY_CONSUMING_FNS = {"SORT", "GREATEST", "LEAST", "LENGTH"}

    # Comparison operators that CONSUME an ARRAY operand element-wise, mapped to the
    # side that operand sits on. Side-specific by necessity, not tidiness: `@>`/`@>>`
    # carry an ARRAY-typed *literal* needle set on the right, so a both-sides probe
    # would hoist that literal into a column and destroy the bind-time
    # membership-blob lowering it is supposed to feed.
    #
    #   AnyOpEq            `item = ANY(arr)` — array on the RIGHT.
    #   AtArrow            `arr @> (…)`  contains-any; array on the LEFT.
    #   ArrayContainsAll   `arr @>> (…)` contains-all; array on the LEFT.
    _ARRAY_CONSUMING_COMPARISONS = {
        "AnyOpEq": "right",
        "AtArrow": "left",
        "ArrayContainsAll": "left",
    }

    def _hoist_array_operands(self, p, eval_nodes, layout):
        """Materialize a COMPUTED ARRAY operand into its own ExprProject column, then
        point the consuming op at that column. Covers SORT/GREATEST/LEAST/LENGTH, the
        `arr[i]` subscript, and the containment comparisons (`= ANY`, `@>`, `@>>`).

        Two independent reasons an ARRAY operand must be a column, not an intermediate:

        1. Element access. An ARRAY's elements hang off the column owner's
           child_owner, not off the 40-byte DrakenVector, so the VM resolves them by
           column identity against the morsel (cxx_column_child_vec). An arena
           intermediate has neither identity nor owner, so there is nothing to
           resolve — hence the bind-time gate demanding a plain column.

        2. Child ownership, which binds even LENGTH (whose kernel needs no child).
           c_execute_dv_inner carries a produced ARRAY's element vector out in a
           SINGLE out_child slot, on the standing invariant that an ARRAY is only ever
           a program's FINAL result (evaluation.pyx). Compiling LENGTH(SPLIT(x)) as one
           program breaks that: SPLIT sets out_child, LENGTH then returns INT64, and
           the stale child gets adopted onto an INT64 column. Splitting the program in
           two restores the invariant exactly — SPLIT's ARRAY is terminal in its own
           program, and LENGTH's operand is a column.

        Projecting the inner array gives it what it lacked: an ExprProject output column
        adopts the ARRAY child (native_expression.hpp), so it is indistinguishable from a
        native ARRAY column. The operand is then flipped to EVALUATED — the node type
        that lowers to BC_LOAD_COL against its own already-bound identity.

        Same shape as _project_agg_operands, which hoists SUM(a * b) for the same
        reason: the consumer takes a column, so give it one."""
        for node_ in eval_nodes or []:
            sc = getattr(node_, "schema_column", None)
            if sc is not None and sc.identity is not None and sc.identity in layout:
                # Already a materialized column in the incoming stream — e.g. a
                # GROUP BY ALL key expression re-read in the final projection, which
                # shares the AST node with the key already computed pre-aggregate.
                # Descending into its subtree would chase an inner ARRAY operand
                # (e.g. SPLIT(name,'/')) back to a raw source column the aggregate
                # has already legitimately dropped. Same check _add_computed's
                # compile loop applies per-node, just needed here first too.
                continue
            layout = self._hoist_array_in_tree(p, node_, layout)
        return layout

    def _array_operand_of(self, node):
        """The ARRAY-typed operand this node consumes, or None if it consumes none."""
        if node.node_type == NodeType.FUNCTION:
            params = getattr(node, "parameters", None) or []
            if (node.value or "").upper() in self._ARRAY_CONSUMING_FNS and len(params) == 1:
                return params[0]
            return None
        if node.node_type == NodeType.EXTRACTION_OPERATOR and node.value == "MapAccess":
            return node.left
        if node.node_type == NodeType.COMPARISON_OPERATOR:
            side = self._ARRAY_CONSUMING_COMPARISONS.get(node.value)
            if side is None:
                return None
            operand = getattr(node, side)
            # A LITERAL array is not a per-row array and has no column to become:
            # `x = ANY([1,2,3])` lowers to draken_in_list, and a fully-literal
            # comparison constant-folds. Hoisting either would replace a bind-time
            # answer with a materialized column. Only a COMPUTED array needs this.
            if operand is None or operand.node_type == NodeType.LITERAL:
                return None
            return operand
        return None

    def _hoist_array_in_tree(self, p, node, layout):
        """Depth-first: inner arrays hoist before the outer ones that read them, so
        SORT(SORT(SPLIT(x)))[0] resolves one level at a time. Returns the grown
        layout."""
        if node is None:
            return layout
        for child in (getattr(node, "parameters", None) or []):
            layout = self._hoist_array_in_tree(p, child, layout)
        for attr in ("left", "right"):
            layout = self._hoist_array_in_tree(p, getattr(node, attr, None), layout)

        operand = self._array_operand_of(node)
        if operand is None:
            return layout
        if operand.node_type in (
            NodeType.IDENTIFIER, NodeType.EVALUATED, NodeType.AGGREGATOR
        ):
            return layout   # already lowers to BC_LOAD_COL — nothing to hoist
        sc = getattr(operand, "schema_column", None)
        if sc is None or sc.identity is None:
            return layout   # unbound: leave it for the gate to reject, don't guess
        if _physical_type(sc) != DrakenType.ARRAY:
            return layout   # not an array operand — LENGTH(string), str[i], and the
            # literal-array forms all land here and are unaffected

        if sc.identity not in layout:
            layout = self._add_computed(p, [operand], layout)
        # Compiled and projected under its own identity, so reading it as a column is
        # now the truth rather than a rewrite — anything else pointing at this node
        # wants the projected column too.
        operand.node_type = NodeType.EVALUATED
        return layout

    # `->` and `->>`. MapAccess is excluded: it is INTEGER-keyed subscripting, not a
    # JSON path, and does not go through the JSON parse this fusion exists to share.
    _JSON_EXTRACT_OPS = frozenset({"Arrow", "LongArrow"})
    _JSON_SOURCE_TYPES = frozenset({
        DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY, DrakenType.VARIANT,
    })

    def _collect_json_extractions(self, node, layout, groups):
        """Depth-first: record every `->`/`->>` whose operand is a column the stream
        already carries, keyed by that operand's identity, then by the extraction's
        own identity (two Node objects for the same expression — the WHERE copy and
        the SELECT copy — share an identity and want ONE output column between them).

        Descends into an extraction's own operand, so the inner half of a chained
        `(x -> 'a') -> 'b'` can still group with other extractions on `x`. The outer
        half is skipped this pass: its operand is not in `layout` yet."""
        if node is None:
            return
        for child in (getattr(node, "parameters", None) or []):
            self._collect_json_extractions(child, layout, groups)
        for attr in ("left", "right"):
            self._collect_json_extractions(getattr(node, attr, None), layout, groups)

        if node.node_type != NodeType.EXTRACTION_OPERATOR:
            return
        if node.value not in self._JSON_EXTRACT_OPS:
            return

        left = node.left
        if left is None or left.node_type not in (
            NodeType.IDENTIFIER, NodeType.EVALUATED, NodeType.AGGREGATOR
        ):
            return   # not a plain column — nothing to share a parse against
        src_sc = getattr(left, "schema_column", None)
        if src_sc is None or src_sc.identity is None or src_sc.identity not in layout:
            return
        if _physical_type(src_sc) not in self._JSON_SOURCE_TYPES:
            return   # binder should have rejected this; don't guess, just don't fuse

        out_sc = getattr(node, "schema_column", None)
        if out_sc is None or out_sc.identity is None:
            return   # unbound — leave it for the normal path to handle or reject
        if node.right is None or node.right.value is None:
            return   # pathless — the normal lowering raises on this, not us

        groups.setdefault(src_sc.identity, {}).setdefault(
            out_sc.identity, {"nodes": [], "op": node.value, "path": node.right.value}
        )["nodes"].append(node)

    def _fuse_json_extractions(self, p, eval_nodes, layout):
        """Compute 2+ `->`/`->>` on the same column with ONE parse per row.

        Parsing dominates JSON extraction — navigation and emit are noise beside it —
        so N extractions on one column cost N parses today and barely more than one
        fused. This appends a JsonExtractMultiOperator producing all N columns, then
        rewrites each extraction node to EVALUATED so it reads its column, exactly as
        _hoist_array_operands does for ARRAY operands.

        Scope is deliberately ONE compile point: only extractions that would already
        run together, over the same rows, are fused. Nothing moves across an
        operator, so this can only ever do strictly less work — never the same
        extraction on more rows than before. Returns the grown layout."""
        groups = {}
        for node_ in eval_nodes or []:
            self._collect_json_extractions(node_, layout, groups)

        layout = list(layout)
        for src_identity, by_out in groups.items():
            fusable = [(out_id, info) for out_id, info in by_out.items()
                       if out_id not in layout]
            if len(fusable) < 2:
                continue   # one path (or already materialized) — the normal path is fine

            from draken.ops.kernels._kernel_registry import alloc_extraction_ctx
            from opteryx.compiled.expression.compiled_expression import (
                _KernelContextWrapper,
            )

            ctx_ptrs = []
            holders = []
            names = []
            for out_id, info in fusable:
                path = info["path"]
                nav = path if isinstance(path, bytes) else str(path).encode("utf-8")
                # BC_EXTR_JSON_PTR = 3 (`->`), BC_EXTR_JSON_KEY = 4 (`->>`) —
                # compiled_expression.pxd's BCExtractionOpCode. Same allocator the
                # single-extraction bind uses, so there is no second bind-time path
                # for a JSON path to be resolved by.
                sub_op = 3 if info["op"] == "Arrow" else 4
                ctx_ptr = alloc_extraction_ctx(sub_op, nav, 0)
                if ctx_ptr is None:
                    raise NotSupportedError(
                        "Memory could not be allocated to read this JSON path. "
                        "Selecting fewer JSON fields in one query may let it run."
                    )
                ctx_ptrs.append(ctx_ptr)
                holders.append(_KernelContextWrapper(ctx_ptr))
                names.append(out_id)

            self.nplan.add_json_extract_multi(
                p, layout.index(src_identity), ctx_ptrs, names, holders
            )
            layout.extend(names)

            # Compiled and projected under their own identities, so reading them as
            # columns is now the truth rather than a rewrite — same handover
            # _hoist_array_in_tree performs for a materialized ARRAY operand.
            for _out_id, info in fusable:
                for n_ in info["nodes"]:
                    n_.node_type = NodeType.EVALUATED

        return layout

    def _add_computed(self, p, eval_nodes, layout, preserve_shape=False):
        """Append one ExprProject per computed expression (bind order preserved —
        later programs may reference earlier outputs). DECIMAL/TIMESTAMP results
        get their plan-declared logical descriptor re-attached natively at the
        operator boundary. Returns the grown layout.

        ``preserve_shape`` keeps a compressed computed result's encoding at the
        projection boundary (no force-densify). Set ONLY when every column added by
        this call feeds a compression-aware consumer — currently just computed
        GROUP BY / DISTINCT keys, whose sole consumer is the group/distinct sink."""
        from opteryx.expression import should_evaluate
        from opteryx.expression.evaluator import compile_eval_nodes
        from opteryx.expression.formatter import format_expression
        from opteryx.operators._operators import bytecode_non_c_native_op
        from opteryx.operators._operators import bytecode_ops_all_c_native

        layout = self._hoist_array_operands(p, eval_nodes, layout)
        # NOT fused here — deliberately. _fuse_json_extractions is called from the
        # FilterNode path only; see the note on that call site for the measurement
        # that decided it. Fusing a PROJECTION's extractions measured SLOWER at the
        # degrees of parallelism we actually run at (+6.7% at dop 8, +8.3% at dop 16
        # over 1M rows), even though it does a quarter of the parsing: the unfused
        # form spreads its extra parses across workers, while the fused operator's
        # per-morsel cost does not amortize once the query is scan-bound. It only
        # wins where there is nothing to parallelize into (-47.7% at dop 1).

        # Same plan-time tree rewrites the filter path gets (CASE→IF_THEN_ELSE,
        # BETWEEN→compares, decimal literal rescale) — applied BEFORE lowering.
        eval_nodes = [self._rewrite_decimal_compares(self._rewrite_between(
            self._rewrite_case(node_))) for node_ in eval_nodes]

        ct_by_identity = {}
        node_by_identity = {}
        for node_ in eval_nodes:
            sc = getattr(node_, "schema_column", None)
            if sc is not None and sc.identity is not None:
                ct_by_identity[sc.identity] = getattr(sc, "column_type", None)
                node_by_identity[sc.identity] = node_

        layout = list(layout)

        # Settle what the stream ALREADY carries before building any bytecode.
        # This used to be a `continue` inside the compile loop below, which meant
        # `compile_eval_nodes` had already lowered and built every node — including
        # the ones about to be skipped. That is not merely wasted work: a node whose
        # value is already in the stream is not required to be fully bound, because
        # the binder resolves a repeated expression to the EXISTING column and stops
        # (binder.py's "early exit for calculated columns" — it leaves the children
        # unbound on purpose, since nothing is going to evaluate them). Building
        # bytecode for one of those reaches an IDENTIFIER with no schema_column and
        # dies. `SELECT id + 1 AS u FROM $planets ORDER BY id + 1` was exactly that:
        # the sort key resolves to the projection's own output column, and trying to
        # recompute it raised
        #   ValueError: compiled_expression: IDENTIFIER node missing schema_column
        # `should_evaluate` is applied here so this pre-pass sees exactly the node
        # set the compile loop used to see, and no more.
        pending = []
        for node_ in eval_nodes:
            if not should_evaluate(node_):
                continue
            sc = getattr(node_, "schema_column", None)
            identity = getattr(sc, "identity", None) if sc is not None else None
            if identity is None or identity not in layout:
                pending.append(node_)
                continue
            # The stream already carries this identity — legal ONLY when it is
            # the same-typed column (an earlier program's output, or a plain
            # passthrough). When the binder assigns a COMPUTED node the same
            # identity as a raw column of a DIFFERENT type (observed:
            # `SELECT EventTime, EventTime::TIMESTAMP[s]` — the cast shares
            # the raw column's identity but declares unit=us while the stream
            # carries unit=s), silently skipping the computation displayed the
            # raw values under the computed descriptor = WRONG ANSWER. Fail
            # loud on descriptor mismatch instead.
            declared = ct_by_identity.get(identity)
            stream_lt = self._layout_type(None, identity)
            if declared is not None and stream_lt is not None:
                declared_pt = getattr(declared, "physical", None)
                if declared_pt is not None and declared_pt != stream_lt:
                    _unsupported(
                        "a computed column whose identity collides with a "
                        "differently-typed stream column (binder identity reuse)")
            declared_lg = getattr(declared, "logical", None) if declared is not None else None
            stream_ct = (getattr(self, "_cts", None) or {}).get(identity)
            if declared_lg is not None and stream_ct is not None:
                stream_lg = getattr(stream_ct, "logical", None)
                if stream_lg is not None and str(stream_lg) != str(declared_lg):
                    _unsupported(
                        "a computed column whose identity collides with a "
                        "same-physical, different-descriptor stream column "
                        "(binder identity reuse)")

        for identity, bc in compile_eval_nodes(pending):
            if not bytecode_ops_all_c_native(bc):
                # Name the expression AND the operation inside it. The refusal is
                # correct either way, but "a computed expression outside the
                # c-native kernel set" alone gave the reader nothing to act on —
                # not which of a SELECT's expressions, and not which part of it.
                _offender = node_by_identity.get(identity)
                _where = (
                    f" in `{format_expression(_offender)}`" if _offender is not None else ""
                )
                _unsupported(
                    f"{bytecode_non_c_native_op(bc)}{_where}, "
                    "outside the c-native kernel set,")
            logical = None
            ct = ct_by_identity.get(identity)
            if ct is not None and ct.logical is not None:
                lg = ct.logical
                # dimension completes the descriptor channel: a computed VECTOR column
                # (EMBED) carries its width here and nowhere else — the physical
                # DrakenVector has no field for it, and a VECTOR_FP16 whose owner has
                # no width is a hard error at the first read.
                logical = (int(lg.kind.value), int(getattr(lg.unit, "value", 0)),
                           int(lg.precision), int(lg.scale),
                           int(getattr(lg, "dimension", 0) or 0))
            if ct is not None:
                self._types = getattr(self, "_types", None) or {}
                self._types[identity] = ct.physical
            self.nplan.add_expr_project(p, bc, layout, identity, logical,
                                        preserve_shape=preserve_shape)
            layout.append(identity)
        return layout

    # ---- aggregate parsing --------------------------------------------------------

    # AggSpec2.col_idx sentinels — MUST match src/cpp/engine/native_group_sinks.hpp's
    # kAggNoOperand / kAggWholeRow exactly (named there for the same reason: a bare
    # -1/-2 is never left for a future reader to decode). Never a real column index.
    _AGG_NO_OPERAND = -1   # CountStar: no operand column
    _AGG_WHOLE_ROW = -2    # CountDistinct: dedup over every column (COUNT(DISTINCT *))

    _AGG_FNS = {"COUNT", "SUM", "AVG", "MIN", "MAX", "ARRAY_AGG", "STDDEV", "MEDIAN",
                "ANY_VALUE", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE", "CORR",
                "CIDR_AGG", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP"}
    # STDDEV_POP is a pure alias for STDDEV (population stddev, N denominator);
    # STDDEV_SAMP/VAR_POP/VAR_SAMP are real new finalizations, but all five
    # accumulate the identical Σx/Σx²/count lanes (agg2_update_stddev in
    # native_group_sinks.hpp) and share its DECIMAL rejection below.
    _STDDEV_FAMILY_FUNCS = frozenset(
        {"STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP"}
    )
    # MEDIAN is numeric-only (native_group_sinks.hpp's median_operand_supported) —
    # narrower than _AGG_OPERAND_TYPES (which also allows DECIMAL/BOOL/temporal for
    # SUM/AVG/MIN/MAX/STDDEV). Matches the legacy Cython median collectors exactly.
    _MEDIAN_OPERAND_TYPES = _NUMERIC_TYPES
    _AGG_OPERAND_TYPES = _NUMERIC_TYPES + (
        DrakenType.DECIMAL, DrakenType.DECIMAL128, DrakenType.DATE32,
        DrakenType.TIMESTAMP64, DrakenType.TIME32, DrakenType.TIME64,
        DrakenType.BOOL,
    )
    # ARRAY_AGG copies values instead of ordering/summing them, so it takes the
    # string family on top of the scalar-aggregate set. Mirrors the sink's
    # aa_operand_supported() — keep the two in step.
    _ARRAY_AGG_OPERAND_TYPES = _AGG_OPERAND_TYPES + (
        DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY,
    )
    # GROUP BY / DISTINCT / ORDER BY / PARTITION BY key types — mirrors
    # sort_key_type_supported() in src/cpp/engine/native_sort.hpp exactly
    # (native_group_sinks.hpp's key_append/key_append_phys reuse the same
    # header's gather_elem_size() for the identical purpose, over the same
    # excluded set). ARRAY, INTERVAL, VARIANT, NULL, VECTOR_FP16
    # are excluded: unhashable/unorderable as a plain key, and today they only
    # fail loud deep in the native sink/sort with a generic RuntimeError.
    # Catching it HERE, at plan time, names the actual column and type.
    _KEY_COLUMN_TYPES = _NUMERIC_TYPES + (
        DrakenType.DECIMAL, DrakenType.DECIMAL128,
        DrakenType.DATE32, DrakenType.TIMESTAMP64, DrakenType.TIME32, DrakenType.TIME64,
        DrakenType.BOOL,
        DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY,
    )
    # WindowTopKSink's ORDER BY key: sort_num_key()'s fixed-width numeric path
    # (native_group_sinks.hpp) — _KEY_COLUMN_TYPES minus the string family and
    # DECIMAL128 (both need the row-comparator machinery WindowSink's full sort
    # already has; WindowTopKSink deliberately doesn't reimplement it). A query
    # outside this set still gets WindowTopKFusionStrategy's filter-fusion win via
    # the ordinary WindowSink path — it just skips this sink specifically.
    _TOPK_FAST_KEY_TYPES = tuple(
        t for t in _KEY_COLUMN_TYPES
        if t not in (DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY,
                     DrakenType.DECIMAL128)
    )
    # Kind code from the window-function registry (opteryx/operators/window/helpers.py)
    _RANK_ROW_NUMBER = WINDOW_FUNCTIONS["ROW_NUMBER"]

    def _check_key_type(self, what, name, pt):
        if pt is None or pt in self._KEY_COLUMN_TYPES:
            return
        if pt == DrakenType.VARIANT:
            # Backstop only — the binder (visit_distinct / visit_aggregate_and_group)
            # already rejects a VARIANT key at bind time, before the optimizer or
            # this compiler ever run. This catches any plan-construction path that
            # bypasses normal binding; the message lives once, on the exception.
            raise VariantKeyError(what, name)
        _unsupported(
            f"{md_syntax(what)} on {md_column(name)} (type "
            f"{md_code(pt.name if pt is not None else 'unknown')})",
        )

    @staticmethod
    def _array_agg_options(agg):
        """ARRAY_AGG's DISTINCT / ORDER BY / LIMIT modifiers, as the sink's spec wants
        them. The binder has already rejected ORDER BY on anything but the aggregated
        column, so `order` is at most one entry here.

        No memory guard travels in the spec: ARRAY_AGG's retained bytes are bounded by
        a global budget the sink owns natively (kArrayAggBudgetBytes), not by anything
        planning decides per query."""
        order = getattr(agg, "order", None)
        descending = False
        if order:
            if len(order) != 1:
                _unsupported("ARRAY_AGG ordered by more than one column")
            # order entries are (node, ascending: bool)
            descending = not bool(order[0][1])
        limit = getattr(agg, "limit", None)
        if limit is not None:
            limit = int(limit)
            if limit < 0:
                _unsupported("ARRAY_AGG with a negative LIMIT")
        return {
            "distinct": getattr(agg, "duplicate_treatment", None) == "Distinct",
            "ordered": bool(order),
            "descending": descending,
            "limit": limit,
        }

    def _project_agg_operands(self, p, node, layout):
        """Aggregate operands that are computed expressions (SUM(a * b)) become
        ExprProject columns first; the sink then aggregates a plain column."""
        computed = []
        for agg in getattr(node, "aggregates", None) or []:
            params = getattr(agg, "parameters", None) or []
            if agg.value == "APPROX_PERCENTILE":
                # 2 params: the column expression + a percentile literal —
                # only params[0] is a projectable operand.
                if len(params) != 2:
                    continue
                operands = [params[0]]
            elif agg.value == "CORR":
                # 2 params, BOTH operand columns (the only two-column aggregate)
                # — both are projectable.
                if len(params) != 2:
                    continue
                operands = list(params)
            elif len(params) == 1:
                operands = [params[0]]
            else:
                continue
            for operand in operands:
                if operand.node_type in (NodeType.WILDCARD, NodeType.IDENTIFIER):
                    continue
                sc = getattr(operand, "schema_column", None)
                if sc is not None and sc.identity is not None and sc.identity not in layout:
                    computed.append(operand)
        if computed:
            layout = self._add_computed(p, computed, layout)
        return layout

    def _parse_aggregates(self, aggs, layout, grouped=True):
        """Any mix of COUNT(*) / COUNT(col) / SUM / AVG / MIN / MAX / ARRAY_AGG over
        plain columns. Returns [(identity, fn, operand_idx | -1[, options]), ...] in
        output order. ``grouped`` is False for the ungrouped sink, which ARRAY_AGG
        cannot use."""
        if not aggs:
            _unsupported("an aggregate node with no aggregates")
        specs = []
        for agg in aggs:
            func = agg.value
            sc = agg.schema_column
            if sc is None or sc.identity is None:
                _unsupported("an aggregate without a bound schema column")
            if func not in self._AGG_FNS:
                _unsupported(f"the aggregate function {func}")
            params = getattr(agg, "parameters", None) or []
            percentile = None
            if func == "APPROX_PERCENTILE":
                # APPROX_PERCENTILE(expr, percentile) — a second, query-time-
                # constant argument (not a second operand column — CORR is the
                # only aggregate with one of those). Matches the legacy Cython
                # _extract_percentile_option validation exactly.
                if len(params) != 2:
                    _unsupported("APPROX_PERCENTILE requires two arguments: the "
                                 "column and the percentile")
                pct_node = params[1]
                if pct_node.node_type != NodeType.LITERAL:
                    _unsupported("APPROX_PERCENTILE percentile argument must be a "
                                 "literal")
                percentile = float(pct_node.value)
                if not (0.0 <= percentile <= 1.0):
                    _unsupported("APPROX_PERCENTILE percentile must be between 0.0 "
                                 "and 1.0")
            elif func == "CORR":
                # CORR(x, y) — the only aggregate with a second operand COLUMN
                # (AggSpec2.col_idx2 in the native sink).
                if len(params) != 2:
                    _unsupported("CORR requires two arguments: the x and y columns")
            elif len(params) != 1:
                _unsupported(f"{func} with {len(params)} parameters")
            operand = params[0]
            distinct = getattr(agg, "duplicate_treatment", None) == "Distinct"
            if distinct and func not in ("COUNT", "ARRAY_AGG"):
                _unsupported(f"{func}(DISTINCT ...)")
            if func == "COUNT" and operand.node_type == NodeType.WILDCARD:
                if distinct:
                    # COUNT(DISTINCT *): whole-row dedup — the native sink
                    # key_appends EVERY stream column (same as plain SELECT
                    # DISTINCT's empty on_idx path), so every column must
                    # clear the same key-type gate DISTINCT/GROUP BY do.
                    for identity in layout:
                        self._check_key_type(
                            "COUNT(DISTINCT *)", self._layout_name(identity),
                            self._layout_type(None, identity))
                    specs.append((sc.identity, "CountDistinct", self._AGG_WHOLE_ROW))
                    continue
                specs.append((sc.identity, "CountStar", self._AGG_NO_OPERAND))
                continue
            psc = getattr(operand, "schema_column", None)
            if psc is None:
                _unsupported(f"{func} over an unbound operand")
            if psc.identity not in layout:
                _unsupported(f"{func} over a column the engine could not resolve here")
            idx = layout.index(psc.identity)
            pt = _physical_type(psc)
            if func == "CIDR_AGG":
                # Works grouped AND ungrouped — collapsing a whole address column
                # to one CIDR list is the primary use. Its state is a Roaring
                # bitmap held in a side-vector parallel to the ungrouped sink's
                # cells (the shape MEDIAN/HLL/t-digest already use), so nothing
                # has to fit in the fixed-width AggCell.
                #
                # The operand must carry the IPV4 descriptor, not merely be
                # UINT32: that descriptor is the only thing separating an address
                # from any other 32-bit unsigned number, and folding ids or counts
                # into network ranges would be a well-formed, confident, wrong
                # answer. The native sink refuses too; rejecting here as well
                # means the author gets a plan-time error naming the column
                # instead of an engine error partway through the run.
                #
                # CidrAggTypeError, not _unsupported: this is a permanent type
                # restriction, and NotSupportedError's "not supported yet ...
                # coverage is being burned down" told the reader to wait for a
                # feature that is never coming. The message lives on the exception
                # so it cannot drift from the sink's backstop copy.
                ct = psc.column_type
                logical = ct.logical if ct is not None else None
                if pt != DrakenType.UINT32 or logical is None or logical.kind != LogicalKind.IPV4:
                    # The operand kind picks the message's rationale and its fix —
                    # only the text and integer families have a cast to IPV4 at all
                    # (casts.pyx's IPV4 target), so anything else must not be told
                    # to use one. See CidrAggTypeError.
                    if pt in _INT_TYPES:
                        kind = CidrAggTypeError.INTEGER
                    elif pt in (DrakenType.VARCHAR, DrakenType.NVARCHAR,
                                DrakenType.VARBINARY):
                        kind = CidrAggTypeError.TEXT
                    else:
                        kind = CidrAggTypeError.OTHER
                    raise CidrAggTypeError(psc.name,
                                           str(ct) if ct is not None else None,
                                           operand_kind=kind)
                specs.append((sc.identity, "CidrAgg", idx))
                continue
            if func == "ARRAY_AGG":
                # Grouped-only: an ARRAY_AGG list is per group, and the ungrouped
                # sink's fixed-width AggCell has nowhere to put one. The binder
                # rejects this first; this is the engine's own gate.
                if not grouped:
                    _unsupported("ARRAY_AGG without a GROUP BY")
                if pt not in self._ARRAY_AGG_OPERAND_TYPES:
                    _unsupported(
                        f"{md_syntax('ARRAY_AGG')} over a column of type "
                        f"{md_code(_type_name(psc, pt))}"
                    )
                specs.append((sc.identity, "ArrayAgg", idx, self._array_agg_options(agg)))
                continue
            if func == "CORR":
                # Both operands numeric-only — same restriction (and reasoning)
                # as MEDIAN: no DECIMAL descale, never a mis-scaled answer.
                # Mirrors the sink's corr_capture_meta gate.
                if pt not in self._MEDIAN_OPERAND_TYPES:
                    _unsupported(
                        f"{md_syntax('CORR')} over a column of type {md_code(_type_name(psc, pt))}",
                        _NUMERIC_ONLY,
                    )
                operand2 = params[1]
                psc2 = getattr(operand2, "schema_column", None)
                if psc2 is None:
                    _unsupported("CORR over an unbound operand")
                if psc2.identity not in layout:
                    _unsupported("CORR over a column the engine could not resolve here")
                idx2 = layout.index(psc2.identity)
                pt2 = _physical_type(psc2)
                if pt2 not in self._MEDIAN_OPERAND_TYPES:
                    _unsupported(
                        f"{md_syntax('CORR')} over a column of type {md_code(_type_name(psc2, pt2))}",
                        _NUMERIC_ONLY,
                    )
                specs.append((sc.identity, "Corr", idx, {"col_idx2": idx2}))
                continue
            if func == "COUNT":
                # COUNT(DISTINCT col) dedups on serialized VALUE bytes in the
                # sinks — silently lowering it as plain COUNT was a wrong answer.
                specs.append((sc.identity, "CountDistinct" if distinct else "Count", idx))
                continue
            if func == "APPROX_COUNT_DISTINCT":
                # Same as COUNT(DISTINCT col): hashes the operand (draken's own
                # hash, any type), so it bypasses the numeric/string operand-type
                # gate below entirely — not just the string subset ANY_VALUE/MIN/
                # MAX get.
                specs.append((sc.identity, "ApproxCountDistinct", idx))
                continue
            if func == "APPROX_PERCENTILE":
                if pt not in self._MEDIAN_OPERAND_TYPES:
                    # Numeric-only, same restriction as MEDIAN (its exact sibling)
                    # — matches the legacy t-digest collector's contract.
                    _unsupported(
                        f"{md_syntax('APPROX_PERCENTILE')} over a "
                        f"{md_code(_type_name(psc, pt))} column",
                        _NUMERIC_ONLY,
                    )
                specs.append((sc.identity, "ApproxPercentile", idx,
                             {"percentile": percentile}))
                continue
            if pt not in self._AGG_OPERAND_TYPES:
                # MIN/MAX/ANY_VALUE over strings: the sinks keep a parallel
                # byte-lexicographic extreme (agg2_update_str) — ANY_VALUE reuses
                # that same lane (see AggFn::AnyValue) — SUM/AVG over strings
                # stays rejected.
                _string_minmax = func in ("MIN", "MAX", "ANY_VALUE") and pt in (
                    DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY)
                if not _string_minmax:
                    _unsupported(
                        f"{md_syntax(func)} over a column of type {md_code(_type_name(psc, pt))}"
                    )
            if func in self._STDDEV_FAMILY_FUNCS and pt in (
                    DrakenType.DECIMAL, DrakenType.DECIMAL128):
                # The sink never descales DECIMAL's unscaled integer for the
                # STDDEV family — reading it as a raw double would compute the
                # wrong numbers' variance. CAST to DOUBLE first (same posture as
                # the sink's own fail-loud guard — this is just the friendlier
                # plan-time version).
                _unsupported(
                    f"{md_syntax(func)} over a column of type {md_code(_type_name(psc, pt))}",
                    f"Cast the column to {md_code('DOUBLE')} first, for example "
                    f"{md_code(f'{func}(column::DOUBLE)')}",
                )
            if func == "MEDIAN" and pt not in self._MEDIAN_OPERAND_TYPES:
                # MEDIAN is numeric-only — narrower than the generic operand-type
                # gate above (which already let DECIMAL/BOOL/temporal through for
                # SUM/AVG/MIN/MAX/STDDEV). Matches the legacy Cython median
                # collectors' restriction exactly (see median_operand_supported).
                _unsupported(
                    f"{md_syntax('MEDIAN')} over a column of type {md_code(_type_name(psc, pt))}",
                    _NUMERIC_ONLY,
                )
            fn = {"SUM": "Sum", "AVG": "Avg", "MIN": "Min", "MAX": "Max",
                  "STDDEV": "Stddev", "STDDEV_POP": "Stddev",
                  "STDDEV_SAMP": "StddevSamp", "VAR_POP": "VarPop",
                  "VAR_SAMP": "VarSamp",
                  "MEDIAN": "Median", "ANY_VALUE": "AnyValue"}[func]
            specs.append((sc.identity, fn, idx))
        return specs

    # ---- node dispatch --------------------------------------------------------------

    def compile_node(self, nid):
        """Compile the subplan rooted at ``nid``. Returns ``(pipeline_idx, layout)``
        where layout is the identity list of the pipeline's stream, in column order."""
        node = self.plan[nid]
        if len(list(self.plan.outgoing_edges(nid))) > 1:
            _unsupported(
                "a query shape that feeds one intermediate result into more than one place"
            )
        kind = type(node).__name__

        if kind == "CteRefNode":
            # One reference to a shared CTE: a pipeline over the body's result
            # buffer (filled by the producer pipeline compile_to_native created
            # first), selecting the body's output columns and renaming them to
            # THIS reference's identities. Each reference gets its own pipeline
            # — BufferSource claims morsels per-run (its cursor lives in the
            # pipeline run's GlobalSourceState), so N references read the one
            # materialized result N times without re-executing the body.
            entry = self.cte_buffers.get(node.cte_key)
            if entry is None:
                _unsupported(
                    f"a CTE reference ({node.cte_name}) whose shared body was not compiled"
                )
            buf, body_layout = entry
            mapping = node.cte_column_map or {}
            out_ids = []
            indices = []
            for col in node.columns or []:
                identity = col.schema_column.identity
                body_identity = mapping.get(identity)
                if body_identity is None or body_identity not in body_layout:
                    _unsupported(
                        f"a CTE reference column the shared body does not carry"
                    )
                out_ids.append(identity)
                indices.append(body_layout.index(body_identity))
            p = self.nplan.new_pipeline()
            self.nplan.set_current_identity(node.identity)
            self.nplan.set_current_display_name(kind)
            self.nplan.set_buffer_source(p, buf)
            self.nplan.add_select(p, indices, out_ids)
            self._remember_types(node.columns)
            return p, out_ids

        if getattr(node, "is_scan", False):
            # `nid` so a scan can inspect what CONSUMES it — the skene two-pass path
            # reads its predicate and top-n spec off the Filter/HeapSort above,
            # because unlike parquet a skene scan carries no pushed predicate. See
            # _skene_latmat_consumers.
            return self._compile_scan(node, kind, nid)

        in_edges = list(self.plan.ingoing_edges(nid))

        if kind == "FilterNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            # A computed ARRAY operand inside the predicate (e.g. `SPLIT(x)[i] = ...`)
            # needs the same materialize-and-reference hoist projections get — see
            # _hoist_array_operands. Narrow back to the original layout afterward: the
            # hoisted column is a filter-internal helper, not something anything above
            # the filter asked for.
            hoisted_layout = self._hoist_array_operands(p, [node.filter], list(layout))
            # A predicate with 2+ `->`/`->>` on one column (the shape jsonbench Q3/Q4/Q5
            # have) parses each document once per extraction. Fuse them to one parse;
            # the narrow-back below drops the helper columns again, so nothing above
            # the filter sees them.
            #
            # THE FILTER PATH ONLY. Measured over 1M Bluesky rows, fusion here is
            # -3.9%/-4.3% at dop 8 and -5.6% at dop 16; doing the same to a
            # PROJECTION's extractions measured +6.7%/+8.3% at those same degrees of
            # parallelism (see the note in _add_computed). The difference is what the
            # produced columns are for: here they are consumed by the predicate and
            # discarded at the narrow-back, so the fused operator replaces work the
            # filter program was doing anyway. Do not add the projection call site
            # back without re-measuring at the dop we actually run at — the kernel
            # being 2.6x faster in isolation is NOT sufficient evidence, and was
            # exactly what made the projection version look like a free win.
            hoisted_layout = self._fuse_json_extractions(p, [node.filter], hoisted_layout)
            bc = self._lower_expression(node.filter, "a filter predicate")
            const_col_idx, const_scalar_vecs = self._resolve_const_replacements(node, hoisted_layout)
            self.nplan.add_expr_filter(p, bc, hoisted_layout, const_col_idx, const_scalar_vecs)
            if hoisted_layout != layout:
                indices = [hoisted_layout.index(identity) for identity in layout]
                self.nplan.add_select(p, indices, list(layout))
            return p, layout

        if kind == "ProjectionNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            # Computed expressions come from the FULL projection list — hoisted
            # internal-only columns (a fused Project's shared subexpression, computed
            # once and referenced by 2+ of this node's own columns — see
            # project_fusion.py) first so dependents can load them, then the SELECT
            # columns, then any ORDER BY keys the planner routed through this node
            # (mirrors ProjectionNode.__init__'s own eval-node derivation).
            proj_exprs = (
                list(node.parameters.get("hoisted_columns") or [])
                + list(node.parameters.get("projection") or [])
                + list(node.parameters.get("passthrough_columns") or [])
            )
            eval_nodes = [col for col in proj_exprs
                          if col.node_type != NodeType.IDENTIFIER]
            if eval_nodes:
                layout = self._add_computed(p, eval_nodes, layout)
            out_ids = list(node.projection)
            for identity in out_ids:
                if identity not in layout:
                    _unsupported("projecting a column the engine could not resolve here")
            indices = [layout.index(identity) for identity in out_ids]
            self.nplan.add_select(p, indices, out_ids)
            return p, out_ids

        if kind == "UngroupedAggregateNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            layout = self._project_agg_operands(p, node, layout)
            specs = self._parse_aggregates(
                getattr(node, "aggregates", None) or [], layout, grouped=False)
            buf = self.nplan.new_buffer()
            self.nplan.set_agg_sink(p, specs, buf)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            out_layout = [spec[0] for spec in specs]
            self._apply_having(p2, node, out_layout)
            return p2, out_layout

        if kind == "GroupedAggregateHashedNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            group_cols = getattr(node, "group_by_columns", None) or []
            if not group_cols:
                _unsupported("a GROUP BY with no keys")
            # GROUP BY over a computed key (SUBSTRING(...), REGEXP_REPLACE(...)):
            # project the key expression to a stream column first, then group on it.
            computed_keys = []
            group_key_names = {}
            for grp in getattr(node, "groups", None) or []:
                sc = getattr(grp, "schema_column", None)
                if sc is not None and sc.identity is not None:
                    group_key_names[sc.identity] = getattr(sc, "name", None)
                if getattr(grp, "node_type", None) in (None, NodeType.IDENTIFIER,
                                                       NodeType.WILDCARD):
                    continue
                if sc is not None and sc.identity is not None and sc.identity not in layout:
                    computed_keys.append(grp)
            if computed_keys:
                # Computed GROUP BY keys feed ONLY the group/distinct sink, which is
                # compression-aware (native_group_sinks.hpp keys each physical unique
                # once) — keep the key column's compressed shape end-to-end instead of
                # force-densifying at the projection boundary.
                layout = self._add_computed(p, computed_keys, layout,
                                            preserve_shape=True)
            key_idx = []
            for key_identity in group_cols:
                if key_identity not in layout:
                    _unsupported("a GROUP BY key the engine could not resolve here")
                self._check_key_type(
                    "GROUP BY", group_key_names.get(key_identity) or key_identity,
                    self._layout_type(None, key_identity))
                key_idx.append(layout.index(key_identity))
            # GROUP BY with NO aggregate functions is a DISTINCT over the keys —
            # route to the DistinctSink (emits the distinct key rows unchanged).
            raw_aggs = getattr(node, "aggregates", None) or []
            set_masks = getattr(node, "grouping_set_masks", None)
            # GROUPING(col) is not a per-group REDUCTION (it has no entry in
            # AGGREGATORS / _AGG_FNS) — it is a lookup against the grouping set
            # that produced the row, so it is split out here and lowered
            # separately below, once grouping_id is known to be emitted.
            # `group_cols` (unmutated at this point) is the SAME list, in the
            # SAME order, _grouped_agg.pyx used to build grouping_set_masks's
            # bits — see the comment there — so a key's position in it here IS
            # its bit position in every entry of set_masks.
            grouping_calls = [agg for agg in raw_aggs if agg.value == "GROUPING"]
            aggs = [agg for agg in raw_aggs if agg.value != "GROUPING"]
            grouping_bits = []
            for call in grouping_calls:
                if not set_masks:
                    raise InvalidInternalStateError(
                        "GROUPING() reached the physical compiler without a GROUP "
                        "BY ROLLUP/CUBE/GROUPING SETS in scope — this should have "
                        "been refused at bind time."
                    )
                operand = call.parameters[0]
                identity = getattr(getattr(operand, "schema_column", None), "identity", None)
                if identity is None or identity not in group_cols:
                    _unsupported(
                        "GROUPING() over a column that is not a GROUP BY key of "
                        "this query")
                bit_pos = group_cols.index(identity)
                # $grouping_id carries the grouping SET's ordinal (its position in
                # set_masks), not the mask itself — two different sets can share one
                # mask (ROLLUP(a, a)'s first two), and the ordinal is what keeps them
                # apart (see GroupingExpandOperator). So the bit can't be recovered by
                # shifting grouping_id directly; precompute it per ordinal instead —
                # one 0/1 entry per grouping set — and hand the native side a lookup
                # table, not a shift amount.
                bit_by_ordinal = [(mask >> bit_pos) & 1 for mask in set_masks]
                grouping_bits.append((call.schema_column.identity, bit_by_ordinal))
            if grouping_bits:
                # Registers each GROUPING() output identity's type (INT64) for
                # _layout_type/_check_key_type, same as _add_computed does for a
                # computed column — without it, e.g. `ORDER BY GROUPING(x)`
                # resolves to an unknown type and skips its key-type gate.
                self._remember_types(grouping_calls)
            if not aggs:
                if set_masks:
                    # A no-aggregate GROUP BY is a DISTINCT over the keys, and the
                    # DistinctSink has no key beyond the columns themselves — it would
                    # collapse two grouping sets that produce the same key row (the
                    # rolled-up NULLs of `(a)` and of `()`) into one, losing a row the
                    # standard says is there. Refused rather than answered wrongly.
                    _unsupported(
                        "GROUP BY ROLLUP with no aggregate function",
                        "add an aggregate, or list the grouping columns without ROLLUP",
                    )
                if getattr(node, "_having_condition", None) is not None:
                    _unsupported("a HAVING on a no-aggregate GROUP BY")
                buf = self.nplan.new_buffer()
                # No-aggregate GROUP BY routes to the DistinctSink — the group
                # count estimate is the distinct-count estimate here.
                ndv_estimate = getattr(node, "groupby_ndv_estimate", None)
                self.nplan.set_distinct_sink(
                    p, key_idx, buf,
                    _estimate_to_int64(ndv_estimate, "group-count estimate for GROUP BY"))
                p2 = self.nplan.new_pipeline()
                self.nplan.set_buffer_source(p2, buf)
                return p2, list(layout)
            layout = self._project_agg_operands(p, node, layout)
            specs = self._parse_aggregates(aggs, layout)
            key_emit = self._group_key_emit(node, group_cols)
            # Planner NDV estimate for the grouped keys (hash_map_variant strategy);
            # -1 = unknown. Gates the sink's per-partition parvi front maps.
            ndv_estimate = getattr(node, "groupby_ndv_estimate", None)
            if set_masks:
                # GROUP BY ROLLUP(...): expand each morsel into one morsel per grouping
                # set — keys the set does not name masked to NULL, plus the grouping_id
                # key — and let an ORDINARY grouped aggregate consume the result. The
                # sink is not told that grouping sets exist; below it, the scan and the
                # joins still run exactly once.
                #
                # This must come AFTER _project_agg_operands and _parse_aggregates: both
                # index into `layout`, and the expand only APPENDS grouping_id, so every
                # index resolved above stays valid.
                self.nplan.add_grouping_expand(p, key_idx, set_masks, _GROUPING_ID_IDENTITY)
                layout = list(layout) + [_GROUPING_ID_IDENTITY]
                key_idx = list(key_idx) + [len(layout) - 1]
                # grouping_id is a KEY, not a passenger: without it the rolled-up NULLs
                # of two different sets are one group. Emitted only when a GROUPING()
                # call needs to read it back post-aggregate (see below) — otherwise the
                # sink still has to hash it to keep the sets apart, which costs its
                # hash and nothing else.
                group_cols = list(group_cols) + [_GROUPING_ID_IDENTITY]
                key_emit = list(key_emit) + [bool(grouping_bits)]
                if ndv_estimate is not None:
                    # Every set gets its own groups, so the group count is up to the
                    # per-set estimate times the number of sets. Under-stating it here
                    # would gate the sink's small-map front onto a map that overflows.
                    ndv_estimate = int(ndv_estimate) * len(set_masks)
            buf = self.nplan.new_buffer()
            self.nplan.set_groupby_sink(
                p, key_idx, group_cols, key_emit, specs, buf,
                _estimate_to_int64(ndv_estimate, "group-count estimate for GROUP BY"))
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            out_layout = [identity for identity, emit in zip(group_cols, key_emit) if emit]
            out_layout += [spec[0] for spec in specs]
            if grouping_bits:
                # One GroupingBitOperator per GROUPING() call — same "one op per
                # computed expression, appended in order" shape as _add_computed —
                # reading the sink's now-emitted $grouping_id key back as a plain
                # post-aggregate column.
                gid_idx = out_layout.index(_GROUPING_ID_IDENTITY)
                for out_identity, bit_by_ordinal in grouping_bits:
                    self.nplan.add_grouping_bit(p2, gid_idx, bit_by_ordinal, out_identity)
                    out_layout.append(out_identity)
                # $grouping_id itself is internal-only — nothing above this node can
                # name it — so drop it before returning the layout upward.
                keep = [i for i, identity in enumerate(out_layout)
                       if identity != _GROUPING_ID_IDENTITY]
                self.nplan.add_select(p2, keep, [out_layout[i] for i in keep])
                out_layout = [out_layout[i] for i in keep]
            self._apply_having(p2, node, out_layout)
            return p2, out_layout

        if kind == "DistinctNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            on = getattr(node, "_distinct_on", None)
            on_exprs = getattr(node, "_distinct_on_exprs", None) or []
            # Friendly name per key identity (e.g. "payload -> status_code") for
            # error messages — mirrors GROUP BY's group_key_names above; without
            # it a computed key falls back to its opaque internal identity.
            on_key_names = {}
            for expr in on_exprs:
                sc = getattr(expr, "schema_column", None)
                if sc is not None and sc.identity is not None:
                    on_key_names[sc.identity] = getattr(sc, "name", None)
            if on:
                # DISTINCT ON over a computed expression (e.g. `payload->'x'`,
                # `UPPER(url)`): project the key expression to a stream column
                # first, then dedup on it — mirrors GroupedAggregateHashedNode's
                # computed_keys handling above.
                computed_keys = []
                for expr in on_exprs:
                    sc = getattr(expr, "schema_column", None)
                    if getattr(expr, "node_type", None) in (None, NodeType.IDENTIFIER,
                                                             NodeType.WILDCARD):
                        continue
                    if sc is not None and sc.identity is not None and sc.identity not in layout:
                        computed_keys.append(expr)
                if computed_keys:
                    layout = self._add_computed(p, computed_keys, layout,
                                                preserve_shape=True)
            on_idx = []
            if on:
                for identity in on:
                    if identity not in layout:
                        _unsupported("a DISTINCT ON column the engine could not resolve here")
                    self._check_key_type(
                        "DISTINCT ON", on_key_names.get(identity) or self._layout_name(identity),
                        self._layout_type(None, identity))
                    on_idx.append(layout.index(identity))
            else:
                # Empty on_idx means the native DistinctSink dedups on EVERY
                # column of the stream (native_group_sinks.hpp DistinctSink:
                # "empty = all columns") — plain `SELECT DISTINCT ...` (no ON)
                # takes this path, so every stream column is a key and must be
                # checked here too, not just an explicit DISTINCT ON list.
                for identity in layout:
                    self._check_key_type(
                        "DISTINCT", self._layout_name(identity),
                        self._layout_type(None, identity))
            buf = self.nplan.new_buffer()
            # Planner NDV estimate for the dedup keys (hash_map_variant strategy);
            # -1 = unknown. Gates the sink's parvi front set.
            ndv_estimate = getattr(node, "distinct_ndv_estimate", None)
            self.nplan.set_distinct_sink(
                p, on_idx, buf,
                _estimate_to_int64(ndv_estimate, "distinct-count estimate for DISTINCT"))
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            return p2, layout

        if kind == "SortNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            spec, sink_layout = self._sort_spec(p, node.order_by, layout)
            emit, layout = self._emit_subset(node, sink_layout)
            spec, emit, _ = self._narrow_sink_input(p, sink_layout, spec, emit)
            buf = self.nplan.new_buffer()
            self.nplan.set_sort_sink(p, spec, buf, emit)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            # Order-sensitive from here to the queue: one worker preserves the
            # sorted morsel order end to end.
            self.nplan.set_pipeline_dop(p2, 1)
            return p2, layout

        if kind == "HeapSortNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            limit = getattr(node, "limit", None)
            if limit is None or int(limit) < 0:
                _unsupported("a HeapSort without a positive LIMIT")
            spec, sink_layout = self._sort_spec(p, node.order_by, layout)
            emit, layout = self._emit_subset(node, sink_layout)
            spec, emit, _ = self._narrow_sink_input(p, sink_layout, spec, emit)
            buf = self.nplan.new_buffer()
            self.nplan.set_topn_sink(p, spec, int(limit), buf, emit)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            self.nplan.set_pipeline_dop(p2, 1)
            return p2, layout

        if kind == "WindowNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            part_cols = list(getattr(node, "_partition_columns", None) or [])
            order_cols = list(getattr(node, "_order_columns", None) or [])
            order_asc = list(getattr(node, "_order_ascending", None) or [])
            funcs = list(getattr(node, "_functions", None) or [])
            if not funcs:
                _unsupported("a window node with no functions")
            # PARTITION BY / ORDER BY over a computed key (CAST(...), arithmetic,
            # etc.), and a LAG/LEAD ARGUMENT that is itself an expression: project
            # each to a stream column first, then resolve by identity — mirrors
            # GroupedAggregateHashedNode's computed_keys and _sort_spec's
            # `computed` handling above. The bound argument NODES live in
            # node.parameters["window_functions"]; `funcs` (from the plan-time
            # WindowNode) carries their identities.
            partition_by = list(node.parameters.get("partition_by") or [])
            order_by = list(node.parameters.get("order_by") or [])
            window_fn_nodes = list(node.parameters.get("window_functions") or [])
            computed = [col for col in partition_by
                        if col.node_type != NodeType.IDENTIFIER]
            computed += [col for col, _asc in order_by
                         if col.node_type != NodeType.IDENTIFIER]
            computed += [arg for _k, _o, arg, _off in window_fn_nodes
                         if arg is not None and arg.node_type != NodeType.IDENTIFIER]
            if computed:
                layout = self._add_computed(p, computed, layout)
            # sort_spec = partition keys (ASC) then order keys (their direction);
            # the WindowSink assigns ranks per partition over that ordering.
            sort_spec = []
            for identity in part_cols:
                if identity not in layout:
                    _unsupported("a PARTITION BY column the engine could not resolve here")
                self._check_key_type(
                    "PARTITION BY", self._layout_name(identity),
                    self._layout_type(None, identity))
                sort_spec.append((layout.index(identity), True))
            for identity, asc in zip(order_cols, order_asc):
                if identity not in layout:
                    _unsupported("a window ORDER BY column the engine could not resolve here")
                self._check_key_type(
                    "window ORDER BY", self._layout_name(identity),
                    self._layout_type(None, identity))
                sort_spec.append((layout.index(identity), bool(asc)))
            fn_kinds = [int(k) for k, _out, _arg, _off in funcs]
            fn_names = [out for _k, out, _arg, _off in funcs]
            fn_offsets = [int(off) for _k, _out, _arg, off in funcs]
            # A navigation function's argument is a VALUE column, not a sort key:
            # it joins the sink's READ set (it must be buffered and survive input
            # narrowing) but never `sort_spec`, and key-type checking does not
            # apply — any gatherable type is legal.
            fn_args = []
            for _k, _out, arg_identity, _off in funcs:
                if arg_identity is None:
                    fn_args.append(-1)
                else:
                    if arg_identity not in layout:
                        _unsupported("a window function argument the engine could not resolve here")
                    fn_args.append(layout.index(arg_identity))
            top_k = int(getattr(node, "_top_k", -1))

            # WindowTopKFusionStrategy's fused `rank <= K`, restricted to the shape
            # WindowTopKSink actually implements: a single ROW_NUMBER (not RANK/
            # DENSE_RANK — ties need every row's exact rank first, see WindowSink),
            # a single ORDER BY column of a fixed-width key type. Anything else
            # still gets the top_k win via WindowSink's post-rank filter, just not
            # this sink's O(n log K)-instead-of-O(n log n) win.
            use_topk_sink = (
                top_k >= 1
                and len(funcs) == 1
                and fn_kinds[0] == self._RANK_ROW_NUMBER
                and len(order_cols) == 1
                and self._layout_type(None, order_cols[0]) in self._TOPK_FAST_KEY_TYPES
            )
            buf = self.nplan.new_buffer()
            if use_topk_sink:
                part_idx = [idx for idx, _asc in sort_spec[: len(part_cols)]]
                order_idx, order_asc0 = sort_spec[len(part_cols)]
                self.nplan.set_window_topk_sink(
                    p, part_idx, order_idx, bool(order_asc0), top_k, fn_names[0], buf)
                p2 = self.nplan.new_pipeline()
                self.nplan.set_buffer_source(p2, buf)
                # No dop=1 pin: WindowTopKSink never produces a globally sorted
                # stream the way WindowSink does (each partition is independently
                # ranked), and ROW_NUMBER's OVER carries no outer ordering promise
                # — nothing downstream relies on this pipeline's emit order.
                # No emit subset: WindowTopKSink is a different sink with its own
                # streaming materialization, untouched by this work.
                return p2, list(layout) + list(fn_names)

            # PARTITION BY / ORDER BY keys are spent once the ranks are assigned —
            # drop any the plan above does not read, over the INPUT layout only (the
            # window-function columns are appended after the gather, so they are not
            # in `emit`). A LAG/LEAD argument column is READ at finalize, so it rides
            # through the narrowing via `extra_read` even when nothing above emits it.
            sink_layout = layout
            emit, layout = self._emit_subset(node, sink_layout)
            sort_spec, emit, fn_args = self._narrow_sink_input(
                p, sink_layout, sort_spec, emit, fn_args)
            self.nplan.set_window_sink(p, sort_spec, len(part_cols),
                                       fn_kinds, fn_names, fn_args, fn_offsets,
                                       top_k, buf, emit)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            self.nplan.set_pipeline_dop(p2, 1)   # emits sorted — preserve the order
            return p2, list(layout) + list(fn_names)

        if kind == "FramedWindowNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            part_cols = list(getattr(node, "_partition_columns", None) or [])
            order_cols = list(getattr(node, "_order_columns", None) or [])
            order_asc = list(getattr(node, "_order_ascending", None) or [])
            funcs = list(getattr(node, "_functions", None) or [])
            if not funcs:
                _unsupported("a framed window node with no functions")
            if not order_cols:
                _unsupported("a window FRAME with no ORDER BY")

            # PARTITION BY / ORDER BY over a computed key, and a computed function
            # ARGUMENT (`SUM(a + b) OVER (...)`): project each to a stream column
            # first, then resolve by identity — mirrors WindowNode's identical need.
            partition_by = list(node.parameters.get("partition_by") or [])
            order_by = list(node.parameters.get("order_by") or [])
            computed = [col for col in partition_by if col.node_type != NodeType.IDENTIFIER]
            computed += [col for col, _asc in order_by if col.node_type != NodeType.IDENTIFIER]
            computed += [
                arg for _k, _out, arg, _frame in funcs
                if arg is not None and arg.node_type != NodeType.IDENTIFIER
            ]
            if computed:
                layout = self._add_computed(p, computed, layout)

            sort_spec = []
            for identity in part_cols:
                if identity not in layout:
                    _unsupported("a PARTITION BY column the engine could not resolve here")
                self._check_key_type(
                    "PARTITION BY", self._layout_name(identity),
                    self._layout_type(None, identity))
                sort_spec.append((layout.index(identity), True))
            for identity, asc in zip(order_cols, order_asc):
                if identity not in layout:
                    _unsupported("a window ORDER BY column the engine could not resolve here")
                self._check_key_type(
                    "window ORDER BY", self._layout_name(identity),
                    self._layout_type(None, identity))
                sort_spec.append((layout.index(identity), bool(asc)))

            # Each function's OUTPUT identity was pre-minted at plan time and its
            # true ColumnType resolved at bind time (`_aggregate_return_type`, off
            # the bound argument) — carried on `node.parameters["outputs"]`'s
            # SchemaColumns (`window_functions` only has the bare identity). Folded
            # into the same identity -> (physical type, ColumnType) tracking every
            # other branch uses (`_layout_type`/`self._cts`), rather than a
            # bespoke lookup just for this node.
            self._types = getattr(self, "_types", None) or {}
            self._cts = getattr(self, "_cts", None) or {}
            for _kind, sc, _params, _frame in node.parameters.get("outputs") or []:
                if sc.column_type is not None:
                    self._types[sc.identity] = sc.column_type.physical
                    self._cts[sc.identity] = sc.column_type

            fn_args = []
            for _kind_code, _out_identity, arg_node, _frame in funcs:
                if arg_node is None:
                    fn_args.append(-1)
                    continue
                arg_identity = arg_node.schema_column.identity
                if arg_identity not in layout:
                    _unsupported("a window function argument the engine could not resolve here")
                arg_idx = layout.index(arg_identity)
                arg_type = self._layout_type(None, arg_identity)
                if arg_type not in self._AGG_OPERAND_TYPES:
                    self._check_key_type("window aggregate", self._layout_name(arg_identity), arg_type)
                fn_args.append(arg_idx)

            py_funcs = []
            for (_kind_code, out_identity, arg_node, frame), arg_idx in zip(funcs, fn_args):
                out_type = self._layout_type(None, out_identity)
                if out_type is None:
                    _unsupported("a window aggregate output the engine could not type here")
                out_logical = _logical_tuple(self._cts.get(out_identity))
                py_funcs.append((
                    int(_kind_code), out_identity, arg_idx, int(out_type.value), out_logical, frame
                ))

            sink_layout = layout
            emit, layout = self._emit_subset(node, sink_layout)
            sort_spec, emit, fn_args = self._narrow_sink_input(
                p, sink_layout, sort_spec, emit, fn_args)
            for i, (_kc, out_identity, _arg_idx, _ot, _ol, _fr) in enumerate(py_funcs):
                py_funcs[i] = (_kc, out_identity, fn_args[i], _ot, _ol, _fr)
            buf = self.nplan.new_buffer()
            self.nplan.set_framed_window_sink(p, sort_spec, len(part_cols), py_funcs, buf, emit)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            self.nplan.set_pipeline_dop(p2, 1)   # emits sorted — preserve the order
            return p2, list(layout) + [out_identity for _kc, out_identity, _a, _ot, _ol, _fr in py_funcs]

        if kind == "ScalarGuardNode":
            # Runtime cardinality guard on an uncorrelated scalar subquery
            # (decorrelate_subquery's ScalarSubqueryGuard step). The decision
            # needs the WHOLE subquery result — the zero-row case must be told
            # apart from "no morsel yet" — so the leg is materialized into a
            # buffer (an ordinary breaker) and read back through the engine's
            # ScalarGuardSource: >1 row raises SQL's cardinality violation as a
            # DataError, 0 rows emits one all-NULL row, 1 row passes through.
            # Types are handed down plan-side for the NULL row, same plumbing
            # (and reason) as set_unmatched_build_source: they must not be
            # learned from a stream that legitimately carries nothing.
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            buf = self.nplan.new_buffer()
            self.nplan.set_buffer_append_sink(p, buf)
            types, logical, element = self._payload_types(in_edges[0][0], layout)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_current_identity(node.identity)
            self.nplan.set_current_display_name(type(node).__name__)
            self.nplan.set_scalar_guard_source(p2, buf, list(layout),
                                               types, logical, element)
            # At most one row can flow — nothing to parallelise.
            self.nplan.set_pipeline_dop(p2, 1)
            return p2, layout

        if kind == "LimitNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            # "First N of the stream" is only deterministic when one worker claims
            # morsels in stream order — LIMIT pins its pipeline to dop 1 (halt stops
            # the source early, so this is bounded work, not a full serial scan).
            self.nplan.set_pipeline_dop(p, 1)
            limit = getattr(node, "limit", None)
            if limit is not None:
                # The planner encodes "no limit" (OFFSET-only) as float('inf').
                limit = None if limit == float("inf") else int(limit)
            offset = getattr(node, "offset", None)
            self.nplan.add_limit(p, None if offset is None else int(offset), limit)
            return p, layout

        if kind == "UnionNode":
            # Positional alignment (mirrors UnionNode._push_impl): each leg's first
            # N columns become the union's column_ids; legs stream into ONE shared
            # buffer (UNION ALL — any DISTINCT sits above as its own node).
            ids = list(node.column_ids)
            if not ids:
                _unsupported("a UNION with no output columns")
            buf = self.nplan.new_buffer()
            if len(in_edges) < 2:
                _unsupported(f"a UNION with {len(in_edges)} legs")
            for provider, _target, _label in in_edges:
                (lp, llayout) = self.compile_node(provider)
                if len(llayout) < len(ids):
                    _unsupported("a UNION leg narrower than the union schema")
                # The per-leg align/append is this UNION's plumbing — attribute it here,
                # not to the leg whose identity compile_node just left current.
                self.nplan.set_current_identity(node.identity)
                self.nplan.set_current_display_name(type(node).__name__)
                self.nplan.add_select(lp, list(range(len(ids))), ids)
                self.nplan.set_buffer_append_sink(lp, buf)
            self.nplan.set_current_identity(node.identity)
            self.nplan.set_current_display_name(type(node).__name__)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            return p2, ids

        if kind == "UnnestJoinNode":
            return self._compile_unnest(in_edges, node)

        if getattr(node, "is_join", False):
            return self._compile_join(nid, node, in_edges)

        _unsupported(f"the {kind} operator")

    def _group_key_emit(self, node, group_cols):
        """One flag per GROUP BY key: does anything above the aggregate read its VALUES?

        Grouping identity in the native sink is the 64-bit key hash — the same contract
        DISTINCT uses — so a key nothing above reads still has to be HASHED to separate
        the groups, but its values never have to be stored. A False here kills the
        per-group key store's copy of that column (`GBPartition::keycols`), not merely
        the output column: the store is what makes the extra key expensive, and the
        extra key is precisely what drives the group count.

        `pre_update_columns` is snapshotted BEFORE this node's own columns are
        collected, so it is exactly "what is still wanted after the grouping" and never
        contains the GROUP BY keys themselves. Empty means UNKNOWN — keep everything.

        HAVING is the one thing that is live but not in that set: predicate_pushdown
        folds it ONTO this node, so it is part of the node's own columns, and
        `_apply_having` lowers it over the aggregate's OUTPUT layout. A key referenced
        only there (`HAVING COUNT(*) > 1 OR l_orderkey > 5` — the AND form gets pushed
        below the aggregate instead) is still live at that point and must be emitted.
        """
        from opteryx.expression import get_all_nodes_of_type

        live = set(getattr(node, "pre_update_columns", None) or ())
        if not live:
            return [True] * len(group_cols)
        having = getattr(node, "_having_condition", None)
        if having is not None:
            live.update(
                reference.schema_column.identity
                for reference in get_all_nodes_of_type(having, (NodeType.IDENTIFIER,))
                if reference.schema_column is not None
            )
        emitted = set(_live_positions(group_cols, live))
        return [position in emitted for position in range(len(group_cols))]

    def _apply_having(self, p, node, layout):
        """HAVING rides ON the aggregate plan node (`_having_condition` — the old
        operator applied it internally via `_apply_having_filter`; there is NO
        separate FilterNode in the plan). Silently ignoring it returned UNFILTERED
        groups — a wrong answer. Lower it as a post-aggregate c-native filter over
        the aggregate's output layout (group keys + agg result columns)."""
        having = getattr(node, "_having_condition", None)
        if having is None:
            return
        # Same computed-ARRAY-operand hoist as the FilterNode branch (see
        # compile_node) — HAVING lowers through the identical _lower_expression
        # gate, so it needs the identical fix. Narrow back afterward; callers don't
        # capture a returned layout from this method, so `layout` must stay accurate
        # for whatever runs after add_expr_filter on this same pipeline.
        hoisted_layout = self._hoist_array_operands(p, [having], list(layout))
        bc = self._lower_expression(having, "a HAVING predicate")
        self.nplan.add_expr_filter(p, bc, hoisted_layout)
        if hoisted_layout != layout:
            indices = [hoisted_layout.index(identity) for identity in layout]
            self.nplan.add_select(p, indices, list(layout))

    def _emit_subset(self, node, layout):
        """The EMIT set for a sort/window sink, plus the layout it produces.

        Returns ``(emit, layout)`` where ``emit`` is ``None`` (emit every column) or
        the list of `layout` positions still wanted above this node, in order.

        These operators buffer their input, build keys from it, order it, and then
        materialize the result with a row gather. The gather is the expensive half —
        a two-pass arena rebuild per string column — so the ORDER BY / PARTITION BY
        key has to be dropped HERE, the moment the permutation exists. A select above
        the sink would only free a buffer that had already been built and paid for.

        Reading the key and emitting it are separate questions: the sort spec still
        indexes the full input layout, and the sink still reads the key off the
        buffered input, so a key excluded here is sorted on exactly as before.

        An empty `pre_update_columns` means UNKNOWN, not "nothing is wanted", so it
        keeps every column. An empty RESULT is different and is honoured: a
        `COUNT(*)` over an ordered subquery genuinely wants zero columns out."""
        live = getattr(node, "pre_update_columns", None) or set()
        if not live:
            return None, layout
        emit = _live_positions(layout, live)
        if len(emit) == len(layout):
            return None, layout      # nothing dead — stay on the untouched path
        return emit, [layout[i] for i in emit]

    def _narrow_sink_input(self, p, layout, spec, emit, extra_read=None):
        """Drop what a sort/window sink would BUFFER for nothing, before it buffers it.

        ``extra_read`` names layout positions the sink READS beyond the sort keys —
        a window function's argument column — which must survive the narrowing and
        be remapped like the spec. It is returned remapped as the third element;
        ``None`` in, ``None`` out.

        `_emit_subset` above stops a spent column being MATERIALIZED into the output.
        This stops a different column being HELD AT ALL. These sinks are breakers:
        every input morsel is retained until finalize, so a column that is neither a
        sort key (the READ set) nor in `emit` (the EMIT set) is kept alive for the
        whole query and then dropped. The clearest case is a semi-join probe key —
        the probe stream is not narrowed by the join, so the key rides into the sort
        with no reader left.

        A select AFTER the sink cannot fix this and a narrower emit set cannot
        either: the memory is spent at sink time. ColumnSelectOperator can, and is
        the right tool — it is zero-copy (it moves the column's shared owner), so
        this costs one pointer copy per kept column per morsel and lets the dead
        column's buffers go as each morsel passes through.

        Returns the remapped ``(spec, emit)``, both indexing the NARROWED input. The
        layout the sink PRODUCES is unchanged — `emit` still names the same
        identities in the same order — so callers keep using `_emit_subset`'s layout.

        ``emit is None`` means the live set is unknown, and unknown means keep
        everything: with no EMIT set there is no column this can prove dead."""
        extra = [i for i in (extra_read or []) if i >= 0]
        if emit is None:
            return spec, emit, extra_read
        keep = sorted({idx for idx, _ascending in spec} | set(emit) | set(extra))
        if len(keep) == len(layout):
            return spec, emit, extra_read
        self.nplan.add_select(p, keep, [layout[i] for i in keep])
        position = {old: new for new, old in enumerate(keep)}
        return ([(position[idx], ascending) for idx, ascending in spec],
                [position[i] for i in emit],
                None if extra_read is None
                else [position[i] if i >= 0 else i for i in extra_read])

    def _sort_spec(self, p, order_by, layout):
        if not order_by:
            _unsupported("an ORDER BY with no keys")
        computed = [col for col, _asc in order_by
                    if col.node_type != NodeType.IDENTIFIER]
        if computed:
            layout = self._add_computed(p, computed, layout)
        spec = []
        for col, ascending in order_by:
            identity = col.schema_column.identity
            if identity not in layout:
                _unsupported("an ORDER BY key the engine could not resolve here")
            self._check_key_type(
                "ORDER BY", getattr(col.schema_column, "name", None) or identity,
                self._layout_type(None, identity))
            spec.append((layout.index(identity), bool(ascending)))
        return spec, layout

    def _compile_only_child(self, in_edges, kind, node):
        if len(in_edges) != 1:
            _unsupported(f"a {kind} with {len(in_edges)} inputs")
        result = self.compile_node(in_edges[0][0])
        # The child's own compile stamped ITS identity as current; restore this node's
        # so the operators/sink this branch is about to build are attributed here.
        self.nplan.set_current_identity(node.identity)
        self.nplan.set_current_display_name(type(node).__name__)
        return result

    def _classify_scan_columns(self, read_scs):
        """Per-column native-decode classification for a scan read-set.

        Returns ``(kinds, string_types, decimal_columns, logical_coerce, bad_type)``,
        the four plan-time arrays the native Source needs (all parallel to
        ``read_scs``) plus, on refusal, the offending DrakenType's NAME (or "NONE"
        when the column carries no physical tag at all) so the caller can record a
        `non_admissible_kind:<T>` residual. ``bad_type`` is None on success.

        Shared by `_native_scan_plan` (one read-set) and `_latmat_scan_plan` (which
        classifies its pass-1 and pass-2 read-sets separately) so the two cannot
        disagree about what a column decodes to.
        """
        kinds = []
        string_types = []
        # WP-11: parallel to read_scs. decimal_columns[i]=1 routes an int64-backed
        # DECIMAL DK_POOL column to the native decimal decoder; logical_coerce[i]
        # packs the DATE/TIMESTAMP/TIME/DECIMAL retag kind + unit / precision-scale
        # (LC_* packing mirrored from native_parquet_scan_source.hpp). 0 = none.
        decimal_columns = []
        logical_coerce = []
        for sc in read_scs:
            pt = _physical_type(sc)
            coerce = _wp11_logical_coerce(sc, pt)
            # WP-11: parquet TIME binds to a TIME32/TIME64 schema type but decodes as
            # plain INT64 (the binder models no TIME coercion) — the trampoline emits
            # it as INT64. Route it through the int path so the native scan emits the
            # identical INT64 column; the "int" footer gate admits the time[...]
            # logical annotation.
            if pt in _INT_KIND_TYPES:
                # Every integer width, signed and unsigned. The schema declares the
                # column's REAL width now (see _rugo_schema._integer_column_type),
                # so this can no longer key on INT64 alone — a narrow column would
                # otherwise fail the scan closed as non-admissible. The native
                # Source decodes each width exactly (DK_INT8/16/32, DK_UINT8/16/32/64)
                # and the "int" footer gate admits the whole int/uint logical family.
                kinds.append("int")
                string_types.append(0)
                decimal_columns.append(0)
                logical_coerce.append(_ipv4_coerce(sc, pt))
            elif pt == DrakenType.FLOAT32:
                kinds.append("float32")
                string_types.append(0)
                decimal_columns.append(0)
                logical_coerce.append(0)
            elif pt == DrakenType.FLOAT64:
                kinds.append("float64")
                string_types.append(0)
                decimal_columns.append(0)
                logical_coerce.append(0)
            elif pt in (DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY):
                # WP-01: string columns decode natively (DK_VARCHAR / DK_VARCHAR_DICT
                # / DK_POOL string). Carry the declared physical type so the native
                # Source tags each vector byte-identically to the trampoline path.
                kinds.append("varchar")
                string_types.append(pt.value)
                decimal_columns.append(0)
                logical_coerce.append(0)
            elif pt == DrakenType.BOOL:
                # WP-11: BOOLEAN → DK_BOOL dense, self-describing (no descriptor).
                kinds.append("bool")
                string_types.append(0)
                decimal_columns.append(0)
                logical_coerce.append(0)
            elif pt == DrakenType.ARRAY:
                # R6: a parquet LIST column. It ALWAYS lands DK_POOL (repetition
                # levels ⇒ direct_kind_for has no direct kind for it) and is
                # serialized as TAG_ARRAY; `array_columns` (built by the caller from
                # the same read_scs order) routes it to the native TAG_ARRAY decoder.
                # The element type is carried on the WIRE (the child_type_tag byte),
                # so nothing needs packing for it — except ARRAY<TIMESTAMP>, whose
                # int64 leaf needs the unit-carrying retag the trampoline applies.
                kinds.append("array")
                string_types.append(0)
                decimal_columns.append(0)
                logical_coerce.append(_r6_array_element_coerce(sc))
            elif coerce is not None:
                # WP-11: DECIMAL / DATE / TIMESTAMP / TIME. `coerce` is (kind_str,
                # is_int64_decimal, packed) — None here means the logical descriptor
                # was missing/out-of-range, so fail the scan closed (below).
                kind_str, is_int64_decimal, packed = coerce
                kinds.append(kind_str)
                string_types.append(0)
                decimal_columns.append(1 if is_int64_decimal else 0)
                logical_coerce.append(packed)
            else:
                # R6: a read-set column (projected OR role-3 filter-only) of a
                # not-yet-admissible kind fails the whole scan closed. Deliberate
                # strict check: role-3 columns must also be native-admissible.
                # Carry the offending DrakenType so R6 can be sub-censused by type.
                # `pt` is the column's physical type; it is None when the column's
                # ColumnType carries no physical tag at all, so guard the lookup.
                #
                # ARRAY used to be the dominant reason code here and is now CLOSED
                # (see the DrakenType.ARRAY branch above). What still lands here is
                # STRUCT/MAP (`json`-annotated), a DECIMAL or temporal column whose
                # logical descriptor is missing/out of range (`_wp11_logical_coerce`
                # returning None), and anything else with no native decode.
                return kinds, string_types, decimal_columns, logical_coerce, (
                    pt.name if pt is not None else "NONE")
        return kinds, string_types, decimal_columns, logical_coerce, None

    def _skene_scan_plan(self, scan):
        """Plan-time setup for the zero-Python skene Source.

        Returns `(SkeneScanPlan, filter_bc | None, read_layout, emit_ids)`, or None
        when this scan is not a shape the native Source serves — today only the
        zero-projection, zero-predicate (bare COUNT(*)) case, which needs the
        materialized path's genuine zero-column morsel. Declining is a fallback to
        a slower CORRECT path, never a wrong answer.

        The scan's READ SET is the projection plus any column only a pushed
        predicate touches (`_skene_scan_config` builds it, projection first). The
        Source decodes the read set, filters it, then emits `emit_indices` — so a
        predicate-only column never leaves the scan and no downstream Select is
        needed.

        `filter_bc` is the PUSHED predicate. `FileSystemTable.can_push` accepts for
        skene (architect ruling, 2026-08-21), which means the pushdown strategy
        CONSUMED the Filter node: nothing above this scan re-applies the predicate,
        so lowering it here is a correctness obligation, not an optimization. It is
        lowered through `_lower_expression` — the same admission gate
        `add_expr_filter` enforces for a Filter node — so a predicate that can be a
        Filter can be a reader-side filter, and there is no shape that pushes but
        cannot then run. (`_lower_scan_predicate` is deliberately NOT used: its
        three consumers all DECLINE to a broader path on a non-c-native program,
        and this one has no broader path to decline to.)

        `retag_units` is the ONE sanctioned type divergence between the plan and
        a skene footer: a column the plan declares TIMESTAMP64 may be stored as
        INT64, because TimestampCastSinkStrategy sank a `col::TIMESTAMP[unit]`
        into the scan and the temporal-ness comes from SQL, not the file. The
        entry carries the unit for that column (draken's code, as `_wp11_unit`)
        and -1 for every other column, so the Source can permit exactly this
        retag and keep failing loud on every other mismatch.
        """
        from opteryx.operators._operators import SkeneScanPlan

        read_columns = getattr(scan, "skene_read_schema_columns", None) or []
        predicates = list(getattr(scan, "predicates", None) or [])
        if not read_columns:
            # No projection AND no predicate columns == bare COUNT(*). A pushed
            # predicate always contributes its columns to the read set, so this
            # cannot be a case of silently dropping one.
            if predicates:
                raise RuntimeError(
                    "compiler: skene scan carries pushed predicates but an empty "
                    "read set — _skene_scan_config must add every predicate "
                    "column, and the materialized path cannot apply a predicate"
                )
            return None

        read_layout = [sc.identity for sc in read_columns]
        emit_ids = [col.schema_column.identity for col in (scan.columns or [])]
        emit_indices = [read_layout.index(identity) for identity in emit_ids]
        filter_bc = None
        zone_terms = []
        if predicates:
            filter_bc = self._lower_expression(
                self._compose_predicate_nodes(predicates), "a WHERE predicate"
            )
            # ROW-GROUP zone map. A .skene file footer carries per-row-group
            # min/max ordinals, so a row group provably holding no matching row is
            # never decoded. The terms are resolved by the Manifest, which owns the
            # ordinal dialect and every soundness rule around it; the Source does
            # integer comparisons and nothing else.
            #
            # These terms are an OPTIMIZATION on top of the reader-side filter, not
            # a substitute for it: a surviving row group is still filtered row by
            # row, so a term this cannot express costs a decode and never an
            # answer. Absent for a manifest whose bounds are not ordinal — and a
            # skene manifest's always are (FileSystemConnector's SKENE branch).
            manifest = getattr(scan, "manifest", None)
            if manifest is not None:
                # By PHYSICAL name: the Source matches these against each file's
                # own footer schema, which is file-named. `sc.name` is the same
                # spelling `read_columns` uses.
                zone_terms = manifest.ordinal_zone_map_terms(predicates)
        splan = SkeneScanPlan(
            list(scan.skene_files),
            [sc.name for sc in read_columns],
            read_layout,
            [sc.column_type.physical.value for sc in read_columns],
            [
                _wp11_unit(sc) if sc.column_type.physical == DrakenType.TIMESTAMP64 else -1
                for sc in read_columns
            ],
            emit_indices,
            zone_terms,
        )
        splan.scan_identity = scan.identity
        return splan, filter_bc, read_layout, emit_ids

    def _skene_latmat_consumers(self, nid, has_pushed_predicate):
        """The residual Filter chain and the HeapSort sitting directly above the
        skene scan at ``nid``, or None when the plan is not that shape.

        Shape: ``SkeneReadNode -> FilterNode* -> HeapSortNode``. The chain is 0..N
        Filters. ZERO is now the ordinary case: `FileSystemTable.can_push` accepts
        for skene (architect ruling, 2026-08-21), so the pushdown strategy CONSUMES
        the Filter and the predicate arrives on `scan.predicates` instead — which
        is why ``has_pushed_predicate`` is required to accept an empty chain.
        Accepting it unconditionally would admit an unfiltered
        ``ORDER BY ... LIMIT`` scan as a two-pass shape with no predicate to
        evaluate. 1..N Filters still occur, for conjuncts the connector declined
        (SplitConjunctivePredicatesStrategy makes one Filter per conjunct;
        PredicateOrderingStrategy may merge them back into one AND tree — both
        shapes are accepted). Anything else between the scan and the sort (a
        Projection, a second consumer, a Join) declines.

        A residual Filter that is left here STAYS in the plan and re-runs; a pushed
        predicate does not exist anywhere else, so pass 1 applying it is the only
        thing that applies it. Both are composed into the pass-1 program by
        `_skene_latmat_scan_plan`. This is the physical plan, so what it reads is
        FINAL — no optimizer strategy can rewrite it afterwards."""
        filters = []
        node_id = nid
        while True:
            out_edges = list(self.plan.outgoing_edges(node_id))
            # travers edges are (source, target, relationship).
            if len(out_edges) != 1:
                return None
            node_id = out_edges[0][1]
            consumer = self.plan[node_id]
            if consumer is None:
                return None
            consumer_kind = type(consumer).__name__
            if consumer_kind == "FilterNode":
                filters.append(consumer)
                continue
            if consumer_kind == "HeapSortNode" and (filters or has_pushed_predicate):
                return filters, consumer
            return None

    def _skene_latmat_scan_plan(self, scan, nid):
        """Plan-time setup for the two-pass late-materialization skene Source
        (`NativeSkeneLatmatScanSource`), or None when this scan is not that shape.

        The shape is `SELECT <wide> FROM t WHERE <pred> ORDER BY <col> LIMIT n` — the
        parquet path's R3 (`fused_topn`), which is the whole of skene's ClickBench
        deficit (Q24: 7755ms skene against 787ms parquet on the same data, measured
        2026-08-08). Pass 1 decodes only the predicate columns plus the sort key for
        every file; pass 2 decodes the full projection for just the files still
        holding a top-n candidate.

        This is a DIFFERENT saving from the single-pass Source's reader-side filter,
        and the two compose. The reader-side filter saves the engine Filter's work;
        late materialization saves DECODE work — the 104 columns Q24 never looks at,
        for the 99M rows it discards. Both routes now take their predicate from the
        same place: `scan.predicates` (pushed — `can_push` accepts for skene since
        the 2026-08-21 ruling) plus any residual Filter the connector declined,
        which `_skene_latmat_consumers` reads off the plan above the scan.

        Safety. Pass 1 drops a row only if it fails the predicate or is strictly
        worse than the n-th best surviving sort key (the downstream TopNSink would
        have dropped that one). A residual Filter left in the plan re-runs over the
        survivors and cannot change the answer. A PUSHED conjunct has no such
        backstop — pass 1 is the only thing that applies it — which is why the
        program below composes the pushed conjuncts and the residual ones together
        and why an inadmissible one must DECLINE (falling through to the single-pass
        Source, which applies it) rather than be skipped.

        Gates, and why each one is here:
          * `skene_late_materialization_min_deferred_columns` — the projection has to
            be materially wider than the pass-1 set. This is what keeps two passes
            away from the narrow-projection shapes (Q25/26/27, one projected column),
            where deferring buys nothing and costs a second open. It also bounds the
            downside: pass 2 re-decodes the pass-1 columns as part of the projection,
            so the worst case is one full scan PLUS pass 1 — capped at the pass-1
            columns' share, which this gate keeps small.
          * `skene_late_materialization_max_selectivity` — mirrors the parquet gate.
            A weak predicate makes two passes cost more than one, and it is also what
            bounds pass 1's live set (one sort key + one row position per survivor,
            held across the barrier until the boundary is known).

        There is no runtime abandon-after-N counterpart to the parquet trampoline's
        `parquet_late_materialization_abandon_after`: the native parquet Source has
        none either, and skene's per-file barrier has no incremental point at which
        to abandon. The two plan-time gates above are the guards, and the bounded
        worst case above is why that is enough.

        Declining is a fallback to the ordinary single-pass native skene Source doing
        exactly the work it does today — never a wrong answer.

        Returns a tuple of everything `_compile_scan` needs, or None."""
        from opteryx import config
        from opteryx.expression import get_all_nodes_of_type
        from opteryx.expression.evaluator.evaluation import Pass1PredResolver
        from opteryx.operators._operators import SkeneLatmatScanPlan
        from opteryx.operators._operators import bytecode_is_all_c_native
        from opteryx.planner.optimizer.strategies.split_conjunctive_predicates import (
            _inner_split,
        )
        from opteryx.variables import resolve as _resolve_var

        if not config.features.skene_late_materialization:
            return None
        read_columns = getattr(scan, "skene_read_schema_columns", None) or []
        if not read_columns:
            # Zero-projection (COUNT(*)) — nothing to defer, and it needs the
            # materialized path's genuine zero-column morsel anyway.
            return None
        manifest = getattr(scan, "manifest", None)
        if manifest is None or manifest.get_file_count() == 0:
            return None

        pushed = list(getattr(scan, "predicates", None) or [])
        shape = self._skene_latmat_consumers(nid, bool(pushed))
        if shape is None:
            return None
        filter_nodes, heapsort = shape

        # ── the top-n spec, read off the HeapSort ──────────────────────────────────
        # Same deliberately narrow scope as TopNScanPushdownStrategy: one ORDER BY
        # key, and it must be a plain column reference this scan emits.
        limit = getattr(heapsort, "limit", None)
        order_by = getattr(heapsort, "order_by", None) or []
        if limit is None or int(limit) <= 0 or len(order_by) != 1:
            return None
        sort_expression, ascending = order_by[0]
        if sort_expression.node_type != NodeType.IDENTIFIER:
            return None
        sort_sc = getattr(sort_expression, "schema_column", None)
        if sort_sc is None:
            return None
        read_by_identity = {sc.identity: sc for sc in read_columns}
        if sort_sc.identity not in read_by_identity:
            return None
        # No plan-time check that the sort key's type is one draken can build a sort
        # key from: the HeapSort directly above sorts the SAME column with the SAME
        # `build_sort_keys`, so a key type this Source could not reduce on is a query
        # that already fails at the TopNSink. Duplicating `sort_key_type_supported`
        # here would add a list that can drift, to gate a case that cannot arise.

        # ── the predicate ──────────────────────────────────────────────────────────
        # Pushed conjuncts FIRST (they have no Filter left to fall back on), then
        # whatever the connector declined and is still a Filter node above.
        predicates = pushed + [node.filter for node in filter_nodes]
        # Every column the predicate touches must be a column this scan reads —
        # otherwise pass 1 cannot evaluate it (a hoisted/computed operand lands as an
        # EVALUATED node referring to a column that only exists above the scan).
        # Checked before resolving, so an inadmissible shape DECLINES rather than
        # raising out of Pass1PredResolver's identity lookup.
        for pred in predicates:
            for reference in get_all_nodes_of_type(
                pred, (NodeType.IDENTIFIER, NodeType.EVALUATED)
            ):
                referenced_sc = getattr(reference, "schema_column", None)
                if referenced_sc is None or referenced_sc.identity not in read_by_identity:
                    return None

        # Pass 1 evaluates the predicate through opteryx_pass1_predicate_eval's C ABI
        # — the same entry the parquet latmat Source uses — which runs the c-native
        # bytecode VM and nothing else. Not lowerable → decline, single pass.
        filter_bc = self._lower_scan_predicate(predicates)
        if not bytecode_is_all_c_native(filter_bc):
            return None

        # ── the pass-1 column set: the predicate's columns, plus the sort key ───────
        # Pass1PredResolver owns the literal vectors + col_idx arrays the C ABI
        # dereferences; the NativePlan holds it for the run (see
        # set_skene_latmat_scan_source). `col_names` are PHYSICAL (in-file) names, in
        # the order the ctx's col_idx expects.
        resolver = Pass1PredResolver(
            filter_bc,
            {sc.identity: sc.name for sc in read_columns},
            {sc.identity: sc.column_type.physical.value for sc in read_columns},
        )
        p1_names = list(resolver.col_names)
        if not p1_names:
            return None
        read_by_name = {sc.name: sc for sc in read_columns}
        p1_scs = [read_by_name[name] for name in p1_names]
        if sort_sc.name not in p1_names:
            p1_names.append(sort_sc.name)
            p1_scs.append(sort_sc)
        p1_index_by_name = {name: i for i, name in enumerate(p1_names)}
        pred_col_to_p1 = [p1_index_by_name[name] for name in resolver.col_names]

        # ── the gates ──────────────────────────────────────────────────────────────
        query_variables = getattr(scan.properties, "variables", None)
        deferred = [sc for sc in read_columns if sc.name not in p1_index_by_name]
        min_deferred = int(_resolve_var(
            "skene_late_materialization_min_deferred_columns",
            query_variables,
            config.SKENE_LATE_MATERIALIZATION_MIN_DEFERRED_COLUMNS,
        ))
        # At least one deferred column is a hard floor whatever the knob says: with
        # none, pass 2 reads exactly what pass 1 read and the split is pure loss.
        if len(deferred) < max(1, min_deferred):
            return None

        # Estimate each CONJUNCT on its own and combine multiplicatively. Splitting
        # is not optional: SplitConjunctivePredicatesStrategy breaks a WHERE into one
        # Filter node per conjunct, but PredicateOrderingStrategy merges them back
        # into a single AND tree — and estimate_selectivity returns its 1.0 "unknown"
        # default for an AND node, which would decline every multi-conjunct query
        # here. `_inner_split` is the one existing definition of that split, imported
        # rather than re-written so the two cannot disagree about what a conjunct is.
        # estimate_selectivity never raises and never returns None (it degrades
        # through stat tiers to a constant), so there is nothing to guard.
        selectivity = 1.0
        for pred in predicates:
            for conjunct in _inner_split(pred):
                selectivity *= manifest.estimate_selectivity(conjunct)
        if selectivity > _resolve_var(
            "skene_late_materialization_max_selectivity",
            query_variables,
            config.SKENE_LATE_MATERIALIZATION_MAX_SELECTIVITY,
        ):
            return None

        # ROW-GROUP zone map, from the SAME conjunct set pass 1 evaluates. Pruning
        # on a RESIDUAL conjunct is sound as well as on a pushed one: the residual
        # Filter above still runs, so a row group its bounds exclude holds no row
        # that survives to the answer either way. The terms are a conjunction and
        # every conjunct here is ANDed into the effective WHERE.
        zone_terms = manifest.ordinal_zone_map_terms(predicates)

        splan = SkeneLatmatScanPlan(
            list(scan.skene_files),
            p1_names,
            [sc.column_type.physical.value for sc in p1_scs],
            [
                _wp11_unit(sc) if sc.column_type.physical == DrakenType.TIMESTAMP64 else -1
                for sc in p1_scs
            ],
            [sc.name for sc in read_columns],
            [sc.identity for sc in read_columns],
            [sc.column_type.physical.value for sc in read_columns],
            [
                _wp11_unit(sc) if sc.column_type.physical == DrakenType.TIMESTAMP64 else -1
                for sc in read_columns
            ],
            pred_col_to_p1,
            zone_terms,
        )
        splan.scan_identity = scan.identity
        return (splan, resolver, p1_index_by_name[sort_sc.name], bool(ascending),
                int(limit), len(p1_names))

    def _native_scan_plan(self, scan):
        """Plan-time setup for the zero-Python scan Source (NativeParquetScanSource)
        when this scan is PROVABLY within its increment-1 scope, else None and the
        scan stays on the trampoline Source. This is a static physical-plan choice
        made once here, from schema + footer metadata — whichever Source is built
        is the one that runs; there is no runtime fallback (an unsupported column
        kind reaching the native Source is a gate bug and fails the query loud).

        Scope: local files, plus remote files the connector's filesystem can rewrite
        to a signed fetch URL (an unsignable remote path stays on the trampoline —
        the C++ fetches carry no auth header); columns that are numeric (schema INT64/FLOAT32/FLOAT64 —
        parquet int32 widens to INT64 on decode) or string (VARCHAR/NVARCHAR/
        VARBINARY, decoded natively via the DK_VARCHAR / DK_VARCHAR_DICT /
        DK_POOL-string paths — WP-01); no scan-pushed LIMIT (R2, still open); and
        the footer gate (native_scan_supported) proves every column of every row
        group eligible — no schema evolution, no DECIMAL/temporal/BOOL logical
        types. A scan-fused TopN hint (R3) is admitted (see below) — it is
        ignored, not honoured, because the real sort/limit already happens in a
        downstream native operator regardless of scan source.

        WP-02 — pushed predicates: the per-row residual is RELOCATED to a native
        downstream ExprFilter (see `_compile_scan`) instead of blocking admission.
        The scan reads the READ-SET (projected ∪ predicate-input columns) so a
        role-3 filter-only column is decoded and available to the filter, and
        EMITS only the projection (via a trailing Select when read-set ⊋ emit-set).
        Row-group / bloom PRUNING stays at the scan — the same
        `extract_predicate_stats` triples the trampoline path uses are passed to
        `open_native_scan_plan`, so bytes-read / row-groups-scanned are unchanged.
        A predicate that does not lower to a c-native span fails CLOSED (returns
        None → trampoline Source keeps the predicate on the old path).

        A2 — zero-projection COUNT(*) WITH a pushed predicate: emit-set is empty
        (`scan.columns` is `[]`), read-set is role-3 predicate columns only. This
        is the WP-02 read-set ⊋ emit-set degenerate case at its limit (emit-set =
        ∅): the trailing Select's `indices`/`names` are both empty, which
        `ColumnSelectOperator` already handles — it emits a genuine zero-column
        morsel carrying the post-filter row count in `zero_col_rows`, the same
        contract `UngroupedAggSink`'s CountStar already reads. No engine change
        needed; only this guard had to stop bailing early. The bare
        `COUNT(*)` no-predicate shape never reaches this method — it is rewritten
        to a manifest-count literal upstream by `StatisticsOnlyResponseStrategy`
        — so a zero-projection scan with NO predicate reaching here has no
        read-set to build from and stays on the trampoline (see the guard
        above)."""
        from opteryx import config
        from opteryx.connectors.parquet_io.pool_reader import native_scan_supported
        from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan
        from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
        from opteryx.operators._operators import resolve_scan_filesystem
        from opteryx.operators._operators import scan_footer_bytes_cache
        from opteryx.expression import get_all_nodes_of_type
        from opteryx.operators._operators import bytecode_is_all_c_native
        from opteryx.variables import resolve as _resolve_var

        if not scan.columns and not getattr(scan, "predicates", None):
            # R1: zero-projection, no predicate — a bare COUNT(*) shape with
            # nothing to read and no filter to relocate. Note: this is NOT the
            # common bare-`SELECT COUNT(*) FROM t` form — that short-circuits to
            # a literal manifest-count response in the optimizer
            # (StatisticsOnlyResponseStrategy) and never reaches a scan at all.
            # This guard only fires when that rewrite couldn't apply (e.g. no
            # manifest stats) and there is truly no column to admit a read-set
            # from. A2 closes the WITH-predicate zero-projection shape below —
            # read-set = role-3 predicate columns, emit-set = empty, row count
            # rides on the ColumnSelectOperator's zero_col_rows degenerate path.
            self.scan_residual_reasons[scan.identity] = "zero_projection"
            return None
        # R2 (CLOSED): a scan-pushed LIMIT is now enforced natively —
        # NativeParquetScanSource carries `row_limit`, claims each morsel's share
        # under its global mutex, truncates the morsel that crosses the boundary,
        # and stops submitting new row-group work once the quota is met (so a
        # `LIMIT 10` over a large file no longer runs the prefetch window across
        # the whole scan). This is a correctness obligation, not just an I/O win:
        # LimitPushdownStrategy REMOVES the Limit node when it pushes into a scan,
        # so nothing downstream truncates. Row identity is unspecified for a
        # LIMIT without ORDER BY, and the trampoline is equally order-
        # nondeterministic at dop>1, so no guarantee is lost.
        # R3 (fused_topn) — CLOSED. `scan._topn_sort_name`/`_topn_limit` is a
        # decode-skip HINT: the downstream HeapSortNode always compiles to a real
        # native `set_topn_sink` operator (see the HeapSortNode branch below in
        # `_compile_scan`) that performs the actual sort/limit/tie-break/null-order
        # generically over the incoming layout, independent of which scan Source
        # feeds it. So the hint never changes WHICH rows reach the client — only
        # how much gets decoded to find them.
        #
        # With NO predicate, no late-materialization happens on either path (it
        # requires a pushed WHERE — see `two_pass_eligible` in parquet_read.pyx),
        # so a plain single-pass native scan is exactly equivalent; that sub-case
        # was admitted by A3.
        #
        # WITH a predicate the decode-skip is load-bearing — ClickBench Q24
        # (`SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT
        # 10`) reads only the predicate + sort-key columns for the whole table,
        # then the other ~100 columns for the handful of survivors; ignoring the
        # hint and single-passing it measured ~400% slower on Q24. That shape is
        # now handled NATIVELY, by LatmatScanSource, and is routed BEFORE this
        # method (`_compile_scan` → `_latmat_scan_plan`), so it never reaches
        # here. What still arrives here carrying a topn hint AND a predicate is
        # the set of shapes where the trampoline would not have late-materialized
        # either (no pass-2-only columns, or the manifest selectivity estimate
        # says the predicate does not prune enough) — for those a single-pass
        # native scan is the same work the trampoline would have done.
        manifest = getattr(scan, "manifest", None)
        if manifest is None or manifest.get_file_count() == 0:
            # R7a: no manifest / zero files
            self.scan_residual_reasons[scan.identity] = "no_manifest"
            return None

        predicates = getattr(scan, "predicates", None)

        # WP-02 fail-closed gate: lower the AND-composed pushed predicate to a
        # c-native span (the VERBATIM tree the trampoline lowers). Not lowerable →
        # None → StreamingScanSource keeps the predicate. build_bytecode cannot
        # raise here — the trampoline already builds the identical bytecode
        # unconditionally at execute time — so `bytecode_is_all_c_native` is the
        # only "not lowerable" signal and no try/except is needed. (A non-bool-final
        # predicate raises out of `_lower_scan_predicate` and never reaches this
        # routing decision — there is no path it could be routed TO.)
        filter_bc = None
        if predicates:
            filter_bc = self._lower_scan_predicate(predicates)
            if not bytecode_is_all_c_native(filter_bc):
                # R4: pushed predicate does not lower to a c-native span
                self.scan_residual_reasons[scan.identity] = "unlowerable_predicate"
                return None

        # Read-set = projected columns (in scan.columns order), then predicate-only
        # (role-3) columns appended. Deduped by identity; a pushed predicate on a
        # projected column resolves to the same schema_column identity, so it is
        # not re-added.
        read_scs = [col.schema_column for col in scan.columns]
        seen = {sc.identity for sc in read_scs}
        if predicates:
            for pred in predicates:
                for ident in get_all_nodes_of_type(pred, select_nodes=(NodeType.IDENTIFIER,)):
                    sc = getattr(ident, "schema_column", None)
                    if sc is None:
                        continue
                    # NOTE (was "R5 / WP-11 fail-closed"): a BOOL column used as a
                    # predicate input used to fail the whole scan closed here
                    # (`bool_predicate_input`), because draken_compare_dv's type
                    # switch had no DRAKEN_BOOL branch — every bool comparison
                    # declined to nullptr, which on the relocated ExprFilter (no
                    # fallback) raised err_op=11. draken/ops/bool_compare.h now
                    # supplies that branch: BOOL is BIT-PACKED, so it needs its own
                    # kernel rather than a fixed-width instantiation, reading
                    # bit `selection[i]` of the bitmap for every logical row (the
                    # uniform §11 access contract, no shape discriminant) with
                    # FALSE < TRUE ordering and NULL-if-either-operand-NULL. So a
                    # BOOL predicate input is admitted, same as a projected one.
                    # NOTE (was "A2 fail-closed"): a DATE32/TIMESTAMP64 predicate input
                    # used to fail the scan closed here, on the premise that the
                    # relocated ExprFilter's kernel could not evaluate a temporal
                    # vector (err_op=11). That premise no longer holds —
                    # draken_compare_dv handles DATE32 and TIMESTAMP64 (same-domain),
                    # and draken_temporal_cmp handles the mixed DATE-vs-TIMESTAMP /
                    # unit-mismatched cases by promoting both sides to nanoseconds. The
                    # relocated filter runs the SAME bytecode VM, so temporal predicate
                    # inputs are admitted. (Row-group PRUNING still declines a
                    # cross-domain temporal compare — see `_temporal_domain_mismatch` in
                    # connectors/parquet_io/predicates.py — so pruning stays sound and
                    # the residual filter produces the answer.)
                    if sc.identity not in seen:
                        seen.add(sc.identity)
                        read_scs.append(sc)

        kinds, string_types, decimal_columns, logical_coerce, bad_type = (
            self._classify_scan_columns(read_scs))
        if bad_type is not None:
            self.scan_residual_reasons[scan.identity] = "non_admissible_kind:" + bad_type
            return None
        # E37: mark which read columns are consumed as a GROUP BY/JOIN/DISTINCT key
        # downstream — only those carry the hash seed (keyhash_buf). Parallel to
        # read_scs; all-zero when nothing keys (SELECT */LIKE) → no sidecar built.
        _key_ids = self._hash_key_identities()
        hash_key_columns = [1 if sc.identity in _key_ids else 0 for sc in read_scs]
        # R6: which read columns are ARRAY (parquet LIST). Parallel to read_scs, and
        # exactly the columns the loop above classified kind "array" — the native
        # Source needs the positional flag because all three pool shapes (decimal /
        # varchar / array) arrive as an indistinguishable DK_POOL blob.
        array_columns = [1 if _physical_type(sc) == DrakenType.ARRAY else 0 for sc in read_scs]
        # Columns the optimizer proved are read ONLY through length-answerable
        # operations (LengthOnlyColumnStrategy) — the decoder records each value's
        # length but skips copying long-value payloads, which nothing reads.
        # Parallel to read_scs; all-zero when nothing qualifies. This is the
        # identity -> positional translation point (identities do not cross the
        # native boundary), mirroring hash_key_columns above.
        _length_only_ids = getattr(scan, "_length_only_columns", None) or frozenset()
        length_only_columns = [1 if sc.identity in _length_only_ids else 0 for sc in read_scs]
        paths = manifest.get_file_paths()
        names = [sc.name for sc in read_scs]
        file_sizes = {}
        files = getattr(manifest, "files", None)
        if files:
            for entry in files:
                size = getattr(entry, "file_size_in_bytes", None)
                if isinstance(size, int) and size > 0:
                    file_sizes.setdefault(entry.file_path, size)
        # Resolved once and shared by the gate and the plan below: the filesystem
        # supplies the signed-URL rewrite that makes a remote (gs://) scan eligible,
        # and the connector type picks the IO worker budget. Same helper the
        # trampoline's `_ensure_scan_started` uses, so the two paths cannot disagree
        # about which filesystem a scan has.
        filesystem, connector_type = resolve_scan_filesystem(scan.connector, paths)
        if not native_scan_supported(paths, names, kinds, file_sizes or None,
                                     filesystem=filesystem,
                                     footer_bytes_cache=scan_footer_bytes_cache()):
            # R7b: the footer gate (native_scan_supported) rejected the scan — schema
            # evolution, a row group whose types are not all eligible, or a remote path
            # the filesystem could not sign (an unsigned remote fetch has no auth header
            # and would 401 at execution time). Signable remote paths ARE admitted.
            self.scan_residual_reasons[scan.identity] = "footer_gate"
            return None
        # Pruning triples — identical to the trampoline path's `_sp_predicate_stats`
        # so row groups excluded / bytes read are unchanged. Only pruning; the
        # per-row residual is the relocated ExprFilter, not the scan.
        pruning = extract_predicate_stats(predicates) if predicates else None
        splan = open_native_scan_plan(
            paths,
            names,
            # Remote scans are admitted now that the gate accepts signable paths, so the
            # worker budget branches on the connector type exactly as the trampoline path
            # does — a remote scan (GCS, S3) is latency-bound and wants the wider budget.
            decode_workers=_resolve_var(
                "parquet_gcs_io_workers",
                getattr(scan.properties, "variables", None),
                config.PARQUET_GCS_IO_WORKERS,
            ) if connector_type in ("GCS", "GS", "S3") else _resolve_var(
                "parquet_local_io_workers",
                getattr(scan.properties, "variables", None),
                config.PARQUET_LOCAL_IO_WORKERS,
            ),
            predicates=pruning or None,
            file_sizes=file_sizes or None,
            string_types=string_types,
            decimal_columns=decimal_columns,
            array_columns=array_columns,
            logical_coerce=logical_coerce,
            hash_key_columns=hash_key_columns,
            length_only_columns=length_only_columns,
            # Gap #3 Phase 2b Step 2: the query's exec pool is SHARED with this scan's
            # decode work (one CPU budget, decode tagged high-priority). The reentrant-
            # pool deadlock this originally hit (an exec worker blocking in
            # wait_and_get_result for a decode task on the same pool, with no free
            # worker to run it) is fixed in ParquetIOPipeline by the help-loop: a
            # blocked puller decodes a pending item itself instead of waiting. See
            # docs/DUCKDB_GAP3_DECODE_BUDGET_PLAN.md §Phase 2b Step 2.
            # Gap #3 Phase 2b Step 2 — pool sharing DISABLED (2026-07-07). Passing
            # self._pool here shares the exec pool with this scan's decode work. It
            # was measured neutral-to-slightly-negative on every real payload (positive
            # only under absurd decode-worker over-provisioning; -68% at DOP=1) — the
            # premise (decode contending for cores) does not hold: decode is either
            # memory-bandwidth-bound or idle, never a core hog. So sharing is off.
            # This is the safe first step of reverting the whole Phase 2b arc; the C++
            # plumbing (io_pipeline.hpp help-loop, Step 1/2) is a separate revert,
            # entangled with pre-existing uncommitted work, pending a git-diff carve.
            # See docs/DUCKDB_GAP3_DECODE_BUDGET_PLAN.md.
            pool=None,
            # Signs gs:// paths into the fetch URLs the C++ pipeline resolves. The
            # gate above already proved every path is local or signable.
            filesystem=filesystem,
            footer_bytes_cache=scan_footer_bytes_cache(),
        )
        self.footer_fetch_ns += splan.footer_fetch_ns

        if filter_bc is not None:
            # Wire the relocated residual for _compile_scan. The native Source emits
            # the read-set in `names` order; read_layout is the parallel identities.
            read_layout = [sc.identity for sc in read_scs]
            emit_ids = [col.schema_column.identity for col in scan.columns]
            emit_indices = [read_layout.index(identity) for identity in emit_ids]
            # Projected columns lead read_layout, so read-set ⊋ emit-set iff there
            # are appended role-3 columns — then a Select narrows back; else the
            # Select would be the identity permutation and is elided (§3).
            need_select = len(read_layout) > len(emit_ids)
            self._relocated_scan_filters[scan.identity] = (
                filter_bc, read_layout, emit_indices, emit_ids, need_select)
        return splan

    def _latmat_scan_plan(self, scan):
        """R3 (`fused_topn`) plan-time setup for the two-pass late-materialization
        native Source (`LatmatScanSource`), or None when this scan is not that shape.

        The shape is `WHERE <pushed predicate> ORDER BY <col> LIMIT n` fused into a
        parquet scan — `TopNScanPushdownStrategy` stamps `_topn_sort_name` /
        `_topn_limit` / `_topn_descending` on it. Pass 1 reads the predicate columns
        plus the sort key for the whole table; the survivors are reduced to the
        top-n boundary; pass 2 reads the remaining projected columns masked to just
        those rows. It replaces the trampoline's `_run_pass1` / `_apply_topn` /
        `_combine_pass1_pass2_row_group`, which drove this same algorithm from Python.

        **Eligibility deliberately mirrors the trampoline's own `two_pass_eligible`
        + `topn_active` (parquet_read.pyx::_ensure_scan_started).** That is the whole
        safety argument for returning None: every shape refused here is a shape the
        trampoline would NOT have late-materialized either, so it falls through to
        the ordinary single-pass native scan doing exactly the work the trampoline
        would have done — never a silent loss of a decode-skip. In particular the
        selectivity gate is replicated, because a weak predicate makes two passes
        cost more than one (and it is also what bounds pass-1's live survivor set).

        Returns a tuple of everything `_compile_scan` needs, or None.
        """
        from opteryx import config
        from opteryx.connectors.parquet_io.pass1_predicate_gate import (
            pass1_worker_predicate_admissible,
        )
        from opteryx.connectors.parquet_io.pool_reader import native_scan_supported
        from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan
        from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
        from opteryx.expression import get_all_nodes_of_type
        from opteryx.expression.evaluator.evaluation import Pass1PredResolver
        from opteryx.expression.evaluator.evaluation import get_pass1_eval_fn_ptr
        from opteryx.operators._operators import bytecode_is_all_c_native
        from opteryx.operators._operators import resolve_scan_filesystem
        from opteryx.operators._operators import scan_footer_bytes_cache
        from opteryx.variables import resolve as _resolve_var

        sort_name = getattr(scan, "_topn_sort_name", None)
        topn_limit = getattr(scan, "_topn_limit", None)
        predicates = getattr(scan, "predicates", None)
        if sort_name is None or topn_limit is None or not predicates:
            return None
        if not config.features.parquet_late_materialization:
            return None
        manifest = getattr(scan, "manifest", None)
        if manifest is None or manifest.get_file_count() == 0:
            return None
        if not scan.columns:
            return None

        # ── the pass-1 / pass-2 column split (by physical name, as the scan reads) ──
        projected_scs = [col.schema_column for col in scan.columns]
        projected_by_name = {sc.name: sc for sc in projected_scs}
        if sort_name not in projected_by_name:
            # TopNScanPushdownStrategy only stamps a HeapSort reading DIRECTLY from
            # this scan, so the sort key must be one of the columns the scan emits —
            # otherwise the HeapSort has nothing to sort on. A scan that reached here
            # without it means that invariant broke upstream; fail loud rather than
            # quietly planning a scan whose top-n reduction has no key.
            raise RuntimeError(
                "compiler: scan carries a fused top-N hint on column "
                f"{sort_name!r}, which is not in the scan's projection — the "
                "TopNScanPushdownStrategy invariant (HeapSort reads directly from "
                "this scan) does not hold"
            )

        p1_scs = []
        p1_seen = set()
        for pred in predicates:
            for ident in get_all_nodes_of_type(pred, select_nodes=(NodeType.IDENTIFIER,)):
                sc = getattr(ident, "schema_column", None)
                if sc is None or sc.name in p1_seen:
                    continue
                p1_seen.add(sc.name)
                p1_scs.append(sc)
        if not p1_scs:
            return None
        if sort_name not in p1_seen:
            p1_seen.add(sort_name)
            p1_scs.append(projected_by_name[sort_name])
        p2_scs = [sc for sc in projected_scs if sc.name not in p1_seen]
        if not p2_scs:
            # Nothing left for pass 2 to fetch — one pass reads everything anyway.
            # (The trampoline refuses the same way: `bool(_pass2_names)`.)
            return None

        # ── the selectivity gate, mirrored from parquet_read.pyx ────────────────────
        # `predicates` is a list of separately-pushed conjuncts (implicitly ANDed),
        # so each is estimated on its own and combined multiplicatively — passing the
        # list to estimate_selectivity would silently return the 1.0 "unknown"
        # default. estimate_selectivity never raises and never returns None (it
        # degrades through stat tiers to a constant), so there is nothing to guard.
        selectivity = 1.0
        for pred in predicates:
            selectivity *= manifest.estimate_selectivity(pred)
        if selectivity > _resolve_var(
            "parquet_late_materialization_max_selectivity",
            getattr(scan.properties, "variables", None),
            config.PARQUET_LATE_MATERIALIZATION_MAX_SELECTIVITY,
        ):
            return None

        # ── the predicate must lower to a c-native span ─────────────────────────────
        # Pass 1 evaluates it through opteryx_pass1_predicate_eval's C ABI (the same
        # entry rugo's decode workers already call), which runs the c-native bytecode
        # VM and nothing else. Not lowerable → None → `_native_scan_plan` records the
        # `unlowerable_predicate` (R4) residual on the ordinary path.
        filter_bc = self._lower_scan_predicate(predicates)
        if not bytecode_is_all_c_native(filter_bc):
            return None

        p1_kinds, p1_string_types, p1_decimals, p1_coerce, bad = (
            self._classify_scan_columns(p1_scs))
        if bad is not None:
            self.scan_residual_reasons[scan.identity] = "non_admissible_kind:" + bad
            return None
        p2_kinds, p2_string_types, p2_decimals, p2_coerce, bad = (
            self._classify_scan_columns(p2_scs))
        if bad is not None:
            self.scan_residual_reasons[scan.identity] = "non_admissible_kind:" + bad
            return None

        p1_names = [sc.name for sc in p1_scs]
        p2_names = [sc.name for sc in p2_scs]
        paths = manifest.get_file_paths()
        file_sizes = {}
        files = getattr(manifest, "files", None)
        if files:
            for entry in files:
                size = getattr(entry, "file_size_in_bytes", None)
                if isinstance(size, int) and size > 0:
                    file_sizes.setdefault(entry.file_path, size)
        filesystem, connector_type = resolve_scan_filesystem(scan.connector, paths)
        # One gate over BOTH read-sets: every column either pass touches has to be
        # provably decodable, and schema evolution disqualifies the scan outright.
        if not native_scan_supported(paths, p1_names + p2_names, p1_kinds + p2_kinds,
                                     file_sizes or None, filesystem=filesystem,
                                     footer_bytes_cache=scan_footer_bytes_cache()):
            self.scan_residual_reasons[scan.identity] = "footer_gate"
            return None

        decode_workers = _resolve_var(
            "parquet_gcs_io_workers",
            getattr(scan.properties, "variables", None),
            config.PARQUET_GCS_IO_WORKERS,
        ) if connector_type in ("GCS", "GS", "S3") else _resolve_var(
            "parquet_local_io_workers",
            getattr(scan.properties, "variables", None),
            config.PARQUET_LOCAL_IO_WORKERS,
        )
        # Row-group pruning triples — identical to the single-pass path, applied to
        # BOTH plans so the two agree on which row groups exist. Pass 2 re-submits
        # only the row groups pass 1 leaves standing, so its own work-item list is
        # unused at run time; its footer map and pipeline are what matter.
        pruning = extract_predicate_stats(predicates) or None
        _key_ids = self._hash_key_identities()

        def _open(names, scs, string_types, decimals, coerce, preds):
            return open_native_scan_plan(
                paths,
                names,
                decode_workers=decode_workers,
                predicates=preds,
                file_sizes=file_sizes or None,
                string_types=string_types,
                decimal_columns=decimals,
                array_columns=[1 if _physical_type(sc) == DrakenType.ARRAY else 0
                               for sc in scs],
                logical_coerce=coerce,
                hash_key_columns=[1 if sc.identity in _key_ids else 0 for sc in scs],
                length_only_columns=[
                    1 if sc.identity in (getattr(scan, "_length_only_columns", None)
                                         or frozenset()) else 0 for sc in scs],
                pool=None,
                filesystem=filesystem,
                footer_bytes_cache=scan_footer_bytes_cache(),
            )

        p1_plan = _open(p1_names, p1_scs, p1_string_types, p1_decimals, p1_coerce, pruning)
        # Pass 2 must NOT re-prune: its work items come from pass 1's survivors, and a
        # second pruning pass would only cost footer work for a list nothing reads.
        p2_plan = _open(p2_names, p2_scs, p2_string_types, p2_decimals, p2_coerce, None)
        self.footer_fetch_ns += p1_plan.footer_fetch_ns + p2_plan.footer_fetch_ns

        # ── the pushed pass-1 predicate ────────────────────────────────────────────
        # Pass1PredResolver turns the lowered bytecode into the Pass1PredCtx the C ABI
        # reads, and OWNS the literal vectors + col_idx arrays the worker dereferences
        # — the NativePlan holds it for the run (see set_latmat_scan_source).
        identity_to_physical = {sc.identity: sc.name for sc in p1_scs}
        identity_to_type = {sc.identity: sc.column_type.physical.value for sc in p1_scs}
        resolver = Pass1PredResolver(filter_bc, identity_to_physical, identity_to_type)
        p1_index_by_name = {name: i for i, name in enumerate(p1_names)}
        pred_col_to_p1 = [p1_index_by_name[n] for n in resolver.col_names]
        # Hand it to rugo as well, so the match runs on the decode workers (in
        # parallel, nogil) for the column shapes rugo can view without a copy. When it
        # declines, LatmatScanSource runs the identical program itself — same ctx,
        # same bytecode, same answer.
        #
        # rugo's view is tagged from the decoded buffers, so the resolver carries the
        # plan's type per column and the eval entry stamps it before running — the
        # worker sees the same operands this thread's fallback would. Not handed over
        # at all when a predicate column's type does not fit in a DrakenVector
        # (DECIMAL scale, TIMESTAMP unit). See pass1_predicate_gate.
        _p1_sc_by_name = {sc.name: sc for sc in p1_scs}
        if pass1_worker_predicate_admissible(
            _p1_sc_by_name[n].column_type for n in resolver.col_names
        ):
            p1_plan.set_pass1_predicate(get_pass1_eval_fn_ptr(), resolver.ctx_ptr(),
                                        resolver.col_names)

        # ── output assembly ───────────────────────────────────────────────────────
        p2_index_by_name = {name: i for i, name in enumerate(p2_names)}
        out_from_p1 = []
        out_from_p2 = []
        for sc in projected_scs:
            if sc.name in p1_index_by_name:
                out_from_p1.append(p1_index_by_name[sc.name])
                out_from_p2.append(-1)
            else:
                out_from_p1.append(-1)
                out_from_p2.append(p2_index_by_name[sc.name])
        emit_ids = [sc.identity for sc in projected_scs]
        return (p1_plan, p2_plan, resolver, pred_col_to_p1,
                p1_index_by_name[sort_name],
                not bool(getattr(scan, "_topn_descending", False)),
                int(topn_limit), out_from_p1, out_from_p2, emit_ids)

    def _compile_scan(self, scan, kind, nid):
        # Tag the scan Source (and any materialized buffer source) with the scan node's
        # identity so its per-operator readings attribute back to the ReadRel node.
        self.nplan.set_current_identity(scan.identity)
        self.nplan.set_current_display_name(type(scan).__name__)
        # ReaderNode = the generic non-parquet connector scan ($planets and the other
        # sample/virtual/in-memory relations). Its content is fully read either way
        # (no native streaming exists for it); materializing at plan time keeps
        # execution 100%% native.
        # JsonlReadNode (READ_JSONL, Stage 1): no native JSONL scan source exists
        # yet either, so it goes through the same materialize-at-compile-time path
        # -- read_morsels() streams newline-chunk Morsels out of rugo, and every
        # one is buffered here before native execution starts (same legitimacy as
        # the virtual datasets above; true native streaming is a later stage).
        # CsvReadNode (READ_CSV): same story, except read_morsels() yields one
        # whole-file Morsel per file rather than one per newline-chunk -- rugo's
        # CSV reader has no chunked entry point (see CsvReadNode's docstring).
        if kind == "SkeneReadNode":
            # The composed `WHERE ... ORDER BY ... LIMIT` shape over a wide
            # projection gets the two-pass late-materialization Source. Tried
            # FIRST — it is a strictly narrower shape than the single-pass path
            # below, and when it declines the scan falls through to that path,
            # which is exactly the work skene does today. The Filter node above
            # STAYS in the plan (it is the correctness backstop, and it is why
            # this needs no change to skene's predicate-pushdown decline). See
            # `_skene_latmat_scan_plan`.
            lat = self._skene_latmat_scan_plan(scan, nid)
            if lat is not None:
                from opteryx.expression.evaluator.evaluation import get_pass1_eval_fn_ptr

                (lat_plan, resolver, sort_p1_index, sort_ascending, topn_limit,
                 p1_column_count) = lat
                self.scan_sources[scan.identity] = "NativeSkeneLatmatScanSource"
                manifest = getattr(scan, "manifest", None)
                file_count = manifest.get_file_count() if manifest is not None else 0
                row_group_count = _skene_row_group_count(manifest, file_count)
                record_count = (
                    manifest.get_record_count() if manifest is not None else None
                )
                self.scan_facts[scan.identity] = {
                    # Pass 1 reads every row group; pass 2 re-reads only the ones
                    # still holding a top-n candidate. This counts the pass-1
                    # sweep — the work the scan is responsible for — the same
                    # number the single-pass path reports for the same query.
                    "files_read": file_count,
                    # Both overwritten by `_fold_skene_scan_facts` once the driver
                    # is done — row-group skipping is a run-time decision here too.
                    "row_groups_read": row_group_count,
                    "row_groups_pruned": 0,
                    # Pass 1 sweeps every row group it CLAIMS and applies the
                    # predicate, so rows-in is the manifest's record count across the
                    # surviving files — the same plan-time number the single-pass
                    # path reports for the same query. It overstates by whatever the
                    # zone map skipped, which `row_groups_pruned` below is the
                    # honest record of.
                    "parquet_rows_before_filter": record_count or 0,
                    # The WIDEST read: pass 2's full projection. Pass 1 reads only
                    # `p1_column_count` of these, which is the whole point — one
                    # number cannot say both, and the projection is what the
                    # single-pass path would have decoded for every row.
                    "columns_read": len(scan.skene_read_schema_columns or []),
                }
                p = self.nplan.new_pipeline()
                self.nplan.set_skene_latmat_scan_source(
                    p, lat_plan, get_pass1_eval_fn_ptr(), resolver.ctx_ptr(),
                    resolver, sort_p1_index, sort_ascending, topn_limit)
                self._remember_types(scan.columns)
                # This Source emits the READ SET (projection ∪ predicate-only
                # columns). A pushed predicate can add a column nobody projects, so
                # narrow back — unlike the single-pass Source, which does it inside
                # itself, this is a Select. It runs on top-n survivors only (a
                # handful of rows), so the operator is not worth avoiding here, and
                # the alternative would be a second emit-mapping in the two-pass C++.
                read_layout = [sc.identity for sc in scan.skene_read_schema_columns]
                emit_ids = [col.schema_column.identity for col in (scan.columns or [])]
                if read_layout != emit_ids:
                    self.nplan.add_select(
                        p, [read_layout.index(identity) for identity in emit_ids],
                        emit_ids)
                return p, emit_ids
            # Zero-Python skene Source: workers claim ROW GROUPS from an atomic
            # counter and decode them independently (skene::read_morsel is a
            # pure function over a buffer). Replaces the compile-time
            # materialized path, which decoded every file serially on the
            # driver thread and held the whole read set resident.
            plan = self._skene_scan_plan(scan)
            if plan is not None:
                splan, filter_bc, read_layout, emit_ids = plan
                self.scan_sources[scan.identity] = "NativeSkeneScanSource"
                manifest = getattr(scan, "manifest", None)
                file_count = manifest.get_file_count() if manifest is not None else 0
                record_count = (
                    manifest.get_record_count() if manifest is not None else None
                )
                self.scan_facts[scan.identity] = {
                    "files_read": file_count,
                    # A .skene file holds up to 16 row groups, so this is NOT
                    # file_count — it is the manifest's row group total, which is
                    # also the scan's work item count. Files pruned at plan time
                    # are already absent from the manifest, hence 0 pruned HERE:
                    # the manifest pruning strategy reports what it dropped, and
                    # counting it twice would double-report.
                    "row_groups_read": _skene_row_group_count(manifest, file_count),
                    # Row-group SKIPPING happens at RUN time, in the Source's
                    # claim builder, off each file's footer statistics — so this
                    # placeholder is overwritten by `_fold_skene_scan_facts` once
                    # the driver is done. FILE-level pruning is separate, already
                    # applied at plan time, and already reflected in the manifest
                    # counts above; this is never a stand-in for it.
                    "row_groups_pruned": 0,
                    # Rows fed INTO the scan, before its reader-side predicate. With
                    # a pushed predicate that is no longer the same as rows out, so
                    # it is the manifest's record count across the surviving files —
                    # the plan-time number, exactly as the parquet Source reports
                    # `splan.surviving_row_count`. 0 when nothing is pushed
                    # (rows-in == rows-out) or the manifest cannot say.
                    "parquet_rows_before_filter": (
                        record_count if filter_bc is not None and record_count else 0
                    ),
                    # The read set, which is what the Source decodes — projection
                    # plus predicate-only columns, not just the projection.
                    "columns_read": len(scan.skene_read_schema_columns or []),
                }
                p = self.nplan.new_pipeline()
                self.nplan.set_native_skene_scan_source(p, splan, filter_bc, read_layout)
                self._remember_types(scan.columns)
                # The Source emits the PROJECTION: it applies the pushed predicate
                # over the read set and drops predicate-only columns itself, so
                # there is no relocated filter and no trailing Select here.
                return p, emit_ids
            # Zero-projection (COUNT(*)) and anything else the native Source
            # declines fall through to the materialized path below, which
            # handles the zero-column morsel shape.
            return self._compile_materialized_source(scan)
        if kind in ("FunctionDatasetNode", "NullReaderNode", "ReaderNode", "JsonlReadNode", "CsvReadNode"):
            return self._compile_materialized_source(scan)
        if kind != "ParquetReadNode":
            _unsupported(f"the {kind} source")
        # R3: the composed `WHERE ... ORDER BY ... LIMIT` shape gets the two-pass
        # late-materialization Source. Tried FIRST — it is a strictly narrower shape
        # than the single-pass path below, and when it declines the scan falls through
        # to that path, which is exactly the work the trampoline would have done for
        # the shapes it declines on. See `_latmat_scan_plan`.
        lat = self._latmat_scan_plan(scan)
        if lat is not None:
            (p1_plan, p2_plan, resolver, pred_col_to_p1, sort_p1_index, sort_ascending,
             topn_limit, out_from_p1, out_from_p2, emit_ids) = lat
            from opteryx.expression.evaluator.evaluation import get_pass1_eval_fn_ptr
            self.scan_sources[scan.identity] = "LatmatScanSource"
            manifest = getattr(scan, "manifest", None)
            self.scan_facts[scan.identity] = {
                "files_read": manifest.get_file_count() if manifest is not None else 0,
                "row_groups_read": p1_plan.row_group_count,
                "row_groups_pruned": p1_plan.pruned_row_group_count,
                "parquet_rows_before_filter": p1_plan.surviving_row_count,
                # Both passes' column sets — what the scan actually decodes, though
                # pass 2's columns are only decoded for the top-n candidate rows.
                "columns_read": len(out_from_p1),
            }
            p = self.nplan.new_pipeline()
            self.nplan.set_latmat_scan_source(
                p, p1_plan, p2_plan, get_pass1_eval_fn_ptr(), resolver.ctx_ptr(),
                resolver, pred_col_to_p1, sort_p1_index, sort_ascending, topn_limit,
                out_from_p1, out_from_p2, emit_ids)
            self._remember_types(scan.columns)
            # The predicate is fully applied in pass 1, and the Source emits the
            # projection directly — no relocated ExprFilter, no trailing Select.
            return p, emit_ids
        splan = self._native_scan_plan(scan)
        if splan is not None:
            # Zero-Python Source: workers pull decoded row groups straight from
            # the rugo IO pipeline (no GIL trampoline, no per-morsel attach).
            # Same emit order and layout contract as the trampoline path below.
            self.scan_sources[scan.identity] = "NativeParquetScanSource"
            manifest = getattr(scan, "manifest", None)
            reloc = self._relocated_scan_filters.get(scan.identity)
            self.scan_facts[scan.identity] = {
                "files_read": manifest.get_file_count() if manifest is not None else 0,
                "row_groups_read": splan.row_group_count,
                # WP-02: pushed-predicate min/max + bloom pruning at plan time.
                "row_groups_pruned": splan.pruned_row_group_count,
                # Rows fed into this scan, i.e. across the surviving (non-pruned)
                # row groups only — plan-time, before any relocated residual
                # filter runs downstream (that filter's own records_in/out carry
                # its selectivity; this scan node doesn't duplicate it).
                "parquet_rows_before_filter": splan.surviving_row_count,
                # Read-set width (projected ∪ role-3 filter-only), not just the
                # projection — that is what the native Source actually decodes.
                "columns_read": len(reloc[1]) if reloc is not None else len(scan.columns),
            }
            p = self.nplan.new_pipeline()
            # R2: a scan-pushed LIMIT is enforced BY the scan — LimitPushdownStrategy
            # removes the Limit node from the plan when it pushes (limit_pushdown.py
            # `_apply_to_scan`), so no downstream LimitOperator truncates. Pushdown
            # only fires with no pushed predicate and no OFFSET.
            self.nplan.set_native_scan_source(p, splan, getattr(scan, "limit", None))
            self._remember_types(scan.columns)
            if reloc is None:
                return p, [col.schema_column.identity for col in scan.columns]
            # WP-02: the native Source emits the read-set; apply the relocated
            # residual filter natively over that layout, then Select back to the
            # projection (drops role-3 filter-only columns). The identity Select is
            # elided when read-set == emit-set (need_select False).
            filter_bc, read_layout, emit_indices, emit_ids, need_select = reloc
            self.nplan.add_expr_filter(p, filter_bc, read_layout)
            if need_select:
                self.nplan.add_select(p, emit_indices, emit_ids)
            return p, emit_ids
        self.scan_sources[scan.identity] = "StreamingScanSource"
        # Lower the pushed predicate HERE, at plan time, and hand the bytecode to the
        # scan. The scan used to lower it itself at execute() time, bypassing this
        # rewrite chain: CASE stayed on the GIL BC_CASE VM, and a decimal-column
        # compare reached the c-native kernel with an off-scale literal, violating
        # its same-type/same-scale contract and silently dropping rows
        # (`d > 1.49` lost `1.50`). One lowering, one rewrite chain.
        if getattr(scan, "predicates", None):
            scan.compiled_predicate = self._lower_scan_predicate(scan.predicates)
        p = self.nplan.new_pipeline()
        # A scan that is not concurrent-pull safe (two-pass latmat, fallback
        # generator) gets its pull mutex-serialised inside the Source; the rest of
        # the pipeline still runs at full dop. (Pinning these pipelines to dop 1
        # was tried 2026-07-02 and REVERTED: parked pull-waiters cost nothing —
        # sample counts blocked threads — while dop 1 serialised the downstream
        # filter/sink work and regressed Q40-class queries ~2x.)
        self.nplan.set_scan_source(p, scan, not scan.is_concurrent_pull_safe())
        # The scan emits its projected columns in scan.columns order (the scan's own
        # _sp_output_identity_order) and applies its pushed-down predicates itself.
        # A zero-projection scan (bare COUNT(*)) is legal: it emits zero-column
        # morsels whose row count rides on zero_col_rows.
        layout = [col.schema_column.identity for col in (scan.columns or [])]
        self._remember_types(scan.columns)
        return p, layout

    def _compile_join(self, nid, node, in_edges):
        """Hash joins via the generalized native join (serialized multi-column keys
        of any supported type; INNER / LEFT OUTER / FULL OUTER / SEMI / null-aware
        ANTI modes). The PROBE side is always the streamed side; for LEFT OUTER the
        plan's preserved (left) leg maps to the probe so unmatched rows emit with
        NULL build payload. FULL OUTER = LEFT OUTER probing (with build-side match
        tracking) plus a tail pipeline whose UnmatchedBuildSource emits the build
        rows no probe matched, NULL-padded — both legs append into one shared
        buffer, the engine's UNION plumbing. CROSS = a zero-key inner join (every
        build row shares one empty key → cartesian). nested_loop = an equi-join
        with a residual `on` predicate applied as a post-join filter."""
        join_type = getattr(node, "join_type", None)
        if join_type == "asof":
            return self._compile_asof_join(node, in_edges)
        # THREE key rules live here and none of them is interchangeable with another;
        # each disagrees with the others only on NULL, which is why substituting one
        # for another is a silent wrong answer rather than an error.
        #   "left anti null-aware" (NOT IN)   — one NULL on the build side makes every
        #                                        comparison UNKNOWN, emptying the result.
        #   "left anti" / "left semi"          — NOT EXISTS / EXISTS: a NULL key simply
        #                                        matches nothing.
        #   "* not-distinct"                   — INTERSECT / EXCEPT: NULL is a VALUE that
        #                                        equals itself (IS NOT DISTINCT FROM).
        # Mapping the first two onto each other made NOT EXISTS emit nothing whenever
        # the inner key held a single NULL. Using either in place of the third made
        # `A EXCEPT A` non-empty on any nullable column. See native_join2.hpp's JoinMode.
        modes = {"inner": 0, "left outer": 1, "left semi": 2,
                 "left anti null-aware": 3, "left anti": 4,
                 "cross": 0, "nested_loop": 0, "full outer": 5,
                 "left semi not-distinct": 6, "left anti not-distinct": 7}
        if join_type not in modes:
            _unsupported(f"a {join_type} join")
        mode = modes[join_type]
        # The EXISTENCE-FILTER family: every mode that emits probe rows rather than
        # joined rows. Named once because five separate decisions below key off it
        # (residual handling, build payload, emit narrowing, probe payload, return
        # shape) and a mode missing from any ONE of them is silently compiled as an
        # inner join — it would emit pair rows and the wrong cardinality.
        semi_anti_modes = (2, 3, 4, 6, 7)
        is_cross = join_type == "cross"
        legs = {}
        for idx, (provider, _target, label) in enumerate(in_edges):
            if not label:
                label = "left" if idx == 0 else "right"
            legs[label] = provider
        if "left" not in legs or "right" not in legs:
            _unsupported("a join without labelled left/right legs")

        left_cols = list(getattr(node, "left_columns", None) or [])
        right_cols = list(getattr(node, "right_columns", None) or [])
        # A pure theta nested_loop join (e.g. `ON a > b`, no equi conjunct at all) has
        # no columns to key on — extract_join_fields only ever populates left_columns/
        # right_columns from Eq conjuncts (opteryx/planner/binder/join_helpers.py), so
        # this is the same zero-key shape CROSS already uses (every build row shares
        # one empty key -> every probe row matches every build row); the nested_loop
        # residual filter below then narrows that cartesian product down to the real
        # theta predicate. A MIXED equi+theta join still has real keys here and takes
        # the normal keyed path, with the untouched theta conjunct applied as the same
        # residual filter.
        zero_key = is_cross or (join_type == "nested_loop" and not left_cols and not right_cols)
        if zero_key:
            left_cols, right_cols = [], []
        elif not left_cols or len(left_cols) != len(right_cols):
            _unsupported("a join without aligned key lists")

        # A nested_loop node carries an `on` COMPARISON referencing BOTH legs — it
        # cannot be a pre-scan filter. Applied as a post-join filter over the
        # combined layout below (fails loud if not c-native).
        residual = getattr(node, "on", None) if join_type == "nested_loop" else None

        # A SEMI/ANTI node may carry a CORRELATED NON-EQUALITY residual, split off the
        # EXISTS subquery by decorrelate_subquery, post-bind (TPC-H Q21). Unlike nested_loop's,
        # this one CANNOT be a post-join filter: SEMI/ANTI emit probe rows already
        # collapsed to existence, so the predicate has to gate the existence test
        # inside the probe. It therefore needs the build payload the plain SEMI/ANTI
        # path deliberately drops.
        filter_residual = getattr(node, "residual", None) if mode in semi_anti_modes else None

        # Every conjunct of the ON clause must be evaluated by SOMETHING.
        #
        # `left_cols`/`right_cols` are populated only from Eq conjuncts spanning the
        # two legs (binder/join_helpers.extract_join_fields), so a theta conjunct —
        # `a.x > b.y` — leaves no trace in them. INNER re-plans such a join as
        # `nested_loop` and applies the whole ON as a post-join residual filter, so it
        # is honoured there. Every OTHER join type kept its own join_type, had no
        # residual channel, and SILENTLY DROPPED the conjunct — returning, exactly, the
        # answer to the equi-only join:
        #
        #   planets JOIN satellites ON planets.id = satellites.planetId
        #                          AND planets.mass > satellites.radius
        #                     engine   truth
        #     INNER               156     156
        #     LEFT                179     161   <- the equi-only LEFT join's answer
        #     FULL                179     182
        #     LEFT SEMI             7       4
        #     LEFT ANTI             2       5
        #
        # and it was invisible to the join-algebra oracles because dropping the
        # conjunct UNIFORMLY leaves the identities intact (SEMI + ANTI still equalled
        # |planets|). Per architect ruling only INNER supports a theta conjunct; the
        # rest refuse. A bare theta ON already refused here ("aligned key lists") — the
        # loud path and the silent one sat next to each other.
        on_condition = getattr(node, "on", None)
        if on_condition is not None and residual is None and filter_residual is None:
            unkeyed = len(_and_conjuncts(on_condition)) - len(left_cols)
            if unkeyed > 0:
                # Deliberately NOT named by `join_type`: a user-written RIGHT JOIN
                # arrives here as "left outer" (the planner swaps the legs), so
                # echoing it would tell the user their query says something it does
                # not. State the rule instead.
                _unsupported(
                    f"an ON clause with {unkeyed} condition(s) that are not an equality "
                    "between the two relations, on a join that is not an INNER join "
                    "(INNER is the only join type that supports a non-equality join "
                    "condition)"
                )

        # RIGHT SEMI / RIGHT ANTI: the same answer with the legs exchanged, taken when
        # JoinOrderingStrategy found the leg this join would otherwise MATERIALISE to
        # be far larger than the one it streams. The rule below pins the emitted leg to
        # the probe, which also pins the other leg into the hash table — two decisions
        # that only look like one. See native_join2.hpp's Join2MarkSink.
        #
        # Restricted to plain Semi (2) and plain Anti (4). AntiNullAware and the
        # NotDistinct set-operation modes read their answer from a property of the
        # BUILD side, and exchanging the legs changes which relation that property
        # describes — a wrong answer, not a slower one.
        if mode in (2, 4) and getattr(node, "swap_build_side", False):
            return self._compile_swapped_semi_anti(
                node, legs, mode, left_cols, right_cols, filter_residual
            )

        # INNER / CROSS: build = left leg (CROSS builds right for the scalar side).
        # LEFT OUTER / SEMI / ANTI: the LEFT leg is the preserved/filtered side —
        # it must be the PROBE; the RIGHT leg builds the table.
        if is_cross:
            build_id, probe_id = legs["right"], legs["left"]
            build_keys, probe_keys = [], []
        elif mode == 0:
            build_id, probe_id = legs["left"], legs["right"]
            build_keys, probe_keys = left_cols, right_cols
        else:
            build_id, probe_id = legs["right"], legs["left"]
            build_keys, probe_keys = right_cols, left_cols

        # Keys whose two sides disagree on numeric category get a materialized CAST
        # column so both sides hash the same representation (see _join_key_coercions).
        coercions = self._join_key_coercions(node, build_keys, probe_keys)

        bp, blayout = self.compile_node(build_id)
        self.nplan.set_current_identity(node.identity)  # own the build sink + probe below
        self.nplan.set_current_display_name(type(node).__name__)
        # `blayout` is the leg's real output; `bkeyout` may carry extra synthetic cast
        # columns at the end. Payload/output use the former, key indices the latter.
        bkeyout, build_keys = self._coerce_join_keys(bp, blayout, build_keys, coercions)
        build_key_idx = []
        for identity in build_keys:
            if identity not in bkeyout:
                _unsupported("a build-side join key the engine could not resolve here")
            build_key_idx.append(bkeyout.index(identity))
        ref = self.nplan.new_join2_ref()
        # A join KEY column's purpose is spent the moment it is hashed. The build sink
        # reads its keys off the input morsel at sink time and retains only
        # `payload_col_idx` (equality downstream is 64-bit hash identity, never a value
        # comparison), so a key excluded from the payload is released with the morsel —
        # the operator already implements "kill it when its purpose is spent". What
        # defeated it was this compiler handing it EVERY column as payload, so the pair
        # width was carried to the top of the plan and dropped by the Exit select: 235
        # of the 498 columns joins emit across the 22 TPC-H queries were dead, and the
        # gather is the expensive half (~4.7ns/row for a fixed-width column, ~122ns/row
        # for a VARCHAR, at 15M output rows).
        #
        # `pre_update_columns` is snapshotted BEFORE this join's own columns are
        # collected, so it holds exactly what is still wanted AFTER the join and never
        # the keys the join itself needs — the two are separate sets and the key
        # indices below are untouched (they address `bkeyout`/`pkeyout`, not payload).
        #
        # Two shapes keep the full width because their own consumers read the unpruned
        # pair layout: a nested_loop residual (`add_expr_filter` over `out_layout`) and
        # a SEMI/ANTI correlated residual (lowered against `blayout + playout` inside
        # the probe).
        live = getattr(node, "pre_update_columns", None) or set()
        prune_payload = bool(live) and residual is None and filter_residual is None
        # SEMI/ANTI emit probe rows only — no build payload needed, UNLESS a
        # correlated residual has to read build-side columns to decide existence.
        semi_no_payload = mode in semi_anti_modes and filter_residual is None
        if semi_no_payload:
            build_payload = []
            build_types, build_logical, build_element = [], [], []
        else:
            build_payload = _live_positions(blayout, live) if prune_payload \
                else list(range(len(blayout)))
            build_types, build_logical, build_element = self._payload_types(
                build_id, [blayout[i] for i in build_payload])
        # `join_output_rows_estimate` (JoinBuildShapeStrategy) is how many rows this
        # join is expected to EMIT — the one number the build sink cannot measure for
        # itself when it decides whether consolidating its retained payload beats
        # re-copying it per output row. -1 means unknown, which keeps the sink on its
        # existing gather; never fabricate a number here.
        est_rows = getattr(node, "join_output_rows_estimate", None)
        # The set-operation key rule has to be given to BOTH halves. The probe derives
        # it from `mode`, but the build sink never sees the mode — and it is the build
        # side that decides whether a NULL-keyed row enters the table at all. Passing
        # it here (rather than letting the sink default) is what keeps the two in step:
        # a build that drops NULL keys under a not-distinct probe would lose every NULL
        # row silently, which is the original bug wearing a different hat.
        null_equal = mode in (6, 7)
        self.nplan.set_join2_build_sink(bp, build_key_idx, build_payload, ref,
                                        build_types, build_logical, build_element,
                                        mode == 5,   # FULL OUTER: track matches
                                        _estimate_to_int64(
                                            est_rows,
                                            f"output-row estimate for the {join_type} join"),
                                        null_equal)

        pp, playout = self.compile_node(probe_id)
        self.nplan.set_current_identity(node.identity)  # probe op belongs to the join
        self.nplan.set_current_display_name(type(node).__name__)
        pkeyout, probe_keys = self._coerce_join_keys(pp, playout, probe_keys, coercions)
        probe_key_idx = []
        for identity in probe_keys:
            if identity not in pkeyout:
                _unsupported("a probe-side join key the engine could not resolve here")
            probe_key_idx.append(pkeyout.index(identity))
        probe_payload = _live_positions(playout, live) if prune_payload \
            else list(range(len(playout)))
        # SEMI/ANTI EMIT set. An existence filter emits surviving PROBE rows, so its
        # payload is not the question — what it re-gathers is. Its probe key is read
        # on every row and, unless something above also selects it, is wanted by
        # nothing once `survivors` exists: the join is where it dies, one operator
        # earlier than the sort/window fix could kill it.
        #
        # This is deliberately NOT gated on `prune_payload`. That gate exists because
        # a residual reads the unpruned PAIR layout — a different set, built by
        # build_output from `probe_payload`, which stays full width below. Narrowing
        # what the filter OUTPUTS cannot affect what the residual READS.
        #
        # None = emit everything (unknown live set, or nothing dead — the latter
        # keeps the untouched path rather than paying for an identity subset).
        semi_emit = None
        if mode in semi_anti_modes and live:
            semi_emit = _live_positions(playout, live)
            if len(semi_emit) == len(playout):
                semi_emit = None
        semi_layout = list(playout) if semi_emit is None \
            else [playout[i] for i in semi_emit]
        if filter_residual is not None:
            # The residual reads one column from each side, so the probe needs the
            # FULL probe payload as well as the build payload retained above. It is
            # lowered against the pair layout the probe materializes internally —
            # build payload first, then probe payload — the same order
            # Join2ProbeOperator::build_output emits.
            pair_layout = list(blayout) + list(playout)
            bc = self._lower_expression(
                filter_residual, "a correlated EXISTS residual condition"
            )
            self.nplan.add_join2_probe_residual(pp, ref, probe_key_idx, probe_payload,
                                                mode, bc, pair_layout, semi_emit)
            return pp, semi_layout            # existence filter — probe rows, narrowed
        self.nplan.add_join2_probe(pp, ref, probe_key_idx,
                                   [] if mode in semi_anti_modes else probe_payload, mode,
                                   semi_emit)
        if mode in semi_anti_modes:
            return pp, semi_layout            # existence filter — probe rows, narrowed
        # Join2ProbeOperator emits build payload columns first, then probe payload — in
        # payload-index order, so the output layout is the two RETAINED lists.
        out_layout = [blayout[i] for i in build_payload] + [playout[j] for j in probe_payload]
        if residual is not None:
            # nested_loop residual `on` predicate over the combined layout. Lower
            # it (fails loud if not c-native), resolve column refs against the
            # joined stream, and append a filter to the probe pipeline.
            bc = self._lower_expression(residual, "a nested-loop join condition")
            self.nplan.add_expr_filter(pp, bc, out_layout)
        if mode == 5:
            # FULL OUTER tail: the probe leg and the unmatched-build leg stream
            # into ONE shared buffer (the UNION plumbing). The tail pipeline is
            # created AFTER the probe pipeline — pipelines run in creation
            # order, so by the time UnmatchedBuildSource pulls, every probe
            # worker has finished and the matched[] flags are complete.
            probe_types, probe_logical, probe_element = self._payload_types(
                probe_id, [playout[j] for j in probe_payload])
            buf = self.nplan.new_buffer()
            self.nplan.set_buffer_append_sink(pp, buf)
            tail = self.nplan.new_pipeline()
            self.nplan.set_current_identity(node.identity)
            self.nplan.set_current_display_name(type(node).__name__)
            self.nplan.set_unmatched_build_source(tail, ref, probe_types, probe_logical,
                                                  probe_element)
            self.nplan.set_buffer_append_sink(tail, buf)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            return p2, out_layout
        return pp, out_layout

    def _compile_swapped_semi_anti(self, node, legs, mode, left_cols, right_cols,
                                   filter_residual):
        """RIGHT SEMI / RIGHT ANTI — build the LEFT leg, stream the right one.

        Three pipelines, and their ORDER is the mechanism, not a detail: pipelines run
        in creation order, so the build is complete before the stream marks against it,
        and the stream is complete before the source reads the marks. This mirrors how
        the FULL OUTER tail above is sequenced, for the same reason.

            build   left leg  -> Join2BuildSink(track_matches=True)
            stream  right leg -> Join2MarkSink            (emits nothing)
            emit              -> SemiAntiBuildSource(emit_matched)

        The emitted rows are identical to the LEFT form's; what changes is which leg is
        materialised, that nothing emits until the stream is drained, and that rows
        arrive in build order. JoinOrderingStrategy owns those consequences — by the
        time we are here the decision is made.
        """
        build_id, probe_id = legs["left"], legs["right"]
        build_keys, probe_keys = left_cols, right_cols

        coercions = self._join_key_coercions(node, build_keys, probe_keys)
        bp, blayout = self.compile_node(build_id)
        self.nplan.set_current_identity(node.identity)
        self.nplan.set_current_display_name(type(node).__name__)
        bkeyout, build_keys = self._coerce_join_keys(bp, blayout, build_keys, coercions)
        build_key_idx = []
        for identity in build_keys:
            if identity not in bkeyout:
                _unsupported("a build-side join key the engine could not resolve here")
            build_key_idx.append(bkeyout.index(identity))
        ref = self.nplan.new_join2_ref()

        # Both payloads stay FULL WIDTH, the same call the LEFT form makes whenever a
        # correlated residual exists and for the same reason: the residual is lowered
        # against the pair layout, so narrowing either half moves the indices it reads.
        # The cost is different here and much smaller — this build side is the SMALL
        # leg (that is why the swap was taken), and the streamed side retains nothing
        # at all, so its width is per-morsel work rather than a resident table.
        # Narrowing to `live` U `residual reads` is a real optimisation, and a separate
        # one; doing it inline here would be a column-index bug waiting to happen.
        build_payload = list(range(len(blayout)))
        build_types, build_logical, build_element = self._payload_types(
            build_id, list(blayout))
        # track_matches=True is the whole mechanism: it allocates the matched[] flags
        # the mark sink writes and the source reads. est_output_rows stays unknown —
        # payload consolidation is a fan-out trade, and an existence filter has none.
        self.nplan.set_join2_build_sink(bp, build_key_idx, build_payload, ref,
                                        build_types, build_logical, build_element,
                                        True, -1, False)

        sp, playout = self.compile_node(probe_id)
        self.nplan.set_current_identity(node.identity)
        self.nplan.set_current_display_name(type(node).__name__)
        pkeyout, probe_keys = self._coerce_join_keys(sp, playout, probe_keys, coercions)
        probe_key_idx = []
        for identity in probe_keys:
            if identity not in pkeyout:
                _unsupported("a streamed-side join key the engine could not resolve here")
            probe_key_idx.append(pkeyout.index(identity))
        probe_payload = list(range(len(playout)))

        if filter_residual is None:
            self.nplan.set_join2_mark_sink(sp, ref, probe_key_idx, probe_payload)
        else:
            # Pair layout is build payload then streamed payload — the order
            # Join2ProbeOperator::build_output emits, which Join2MarkSink reuses.
            bc = self._lower_expression(
                filter_residual, "a correlated EXISTS residual condition"
            )
            self.nplan.set_join2_mark_sink(sp, ref, probe_key_idx, probe_payload,
                                           bc, list(blayout) + list(playout))

        ep = self.nplan.new_pipeline()
        self.nplan.set_current_identity(node.identity)
        self.nplan.set_current_display_name(type(node).__name__)
        # SEMI emits the build rows that were matched; ANTI the ones that were not.
        self.nplan.set_semi_anti_build_source(ep, ref, mode == 2)
        return ep, list(blayout)

    # ---- implicit numeric join-key coercion ---------------------------------------
    #
    # A join keys on COLUMNS, by index — the native build sink and probe hash the raw
    # buffers. So two keys of different physical types never match: INT64 `2` and
    # FLOAT64 `2.0` hash differently and the probe finds nothing. Left unhandled that
    # is a SILENT WRONG ANSWER (`k IN (SELECT k)` returned no rows; `NOT IN` returned
    # rows it should have excluded), which is why the coercion happens here rather
    # than being left to the operator.
    #
    # The fix materializes a CAST column on whichever side is narrower and keys on
    # THAT, so both sides hash the same representation. The synthetic column is
    # internal to the join: it is appended after the leg's real columns and excluded
    # from the payload and the output layout, so `SELECT *` is unchanged.
    #
    # SCOPE — the decision keys off PHYSICAL type, not category, with one verified
    # exception:
    #
    #   * INTEGER x INTEGER is left alone. All 64 signed/unsigned width mixes
    #     (int8..int64 x uint8..uint64) already join correctly — the native key hash
    #     canonicalises integer width — and `test_join_key_integer_widths_interoperate`
    #     pins that, so this exception cannot rot silently into a wrong answer.
    #   * FLOAT32 x FLOAT64 is NOT the same story and IS coerced: floats are hashed at
    #     their stored width, so the two never match. Same category, still broken.
    #   * NON-numeric keys are untouched — this fixes the reported numeric bug without
    #     changing string / temporal / DECIMAL-tier join behaviour.

    _JOIN_CAST_TARGETS = {
        "FLOAT": "DOUBLE",
        "INTEGER": "INTEGER",
        "DECIMAL": "DECIMAL",
    }

    def _join_key_coercions(self, node, build_keys, probe_keys):
        """Map key identity -> (bound IDENTIFIER node, cast target name, target type)
        for the keys whose two sides would hash differently. Read off ``node.on``'s
        bound schema columns, so it does not depend on either leg having compiled."""
        from opteryx.expression import NodeType, get_all_nodes_of_type
        from opteryx.operators._operators import JoinNode
        from opteryx.types.logical_type import LogicalCategory
        from opteryx.types.logical_type import find_compatible_type as _lt_find_compatible

        on = getattr(node, "on", None)
        if on is None or not build_keys:
            return {}

        by_identity = {}
        for comparison in get_all_nodes_of_type(on, (NodeType.COMPARISON_OPERATOR,)):
            if comparison.value != "Eq":
                continue
            for side in (comparison.left, comparison.right):
                if side is None or side.node_type != NodeType.IDENTIFIER:
                    continue
                schema_column = getattr(side, "schema_column", None)
                if schema_column is not None:
                    by_identity[schema_column.identity] = side

        numeric = (LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL)
        coercions = {}
        for build_identity, probe_identity in zip(build_keys, probe_keys):
            build_node = by_identity.get(build_identity)
            probe_node = by_identity.get(probe_identity)
            if build_node is None or probe_node is None:
                continue
            build_ct = build_node.schema_column.column_type
            probe_ct = probe_node.schema_column.column_type
            if build_ct is None or probe_ct is None:
                continue
            if build_ct.physical == probe_ct.physical:
                continue                                  # already hash-compatible
            build_category, probe_category = build_ct.category, probe_ct.category
            if build_category not in numeric or probe_category not in numeric:
                continue                                  # out of scope, unchanged
            if build_category == LogicalCategory.INTEGER == probe_category:
                continue                                  # verified interoperable

            if build_category == probe_category:
                # Same category, different width — only FLOAT reaches here (INTEGER
                # returned above). Widen both sides to the category's widest so the
                # narrow side is promoted rather than the wide one truncated.
                target = _lt_find_compatible([build_category, probe_category])
            else:
                target = JoinNode._join_numeric_target_type(build_category, probe_category)
            if target is None:
                continue
            target_name = self._JOIN_CAST_TARGETS.get(target.category.name)
            if target_name is None:
                _unsupported(
                    "a join between %s and %s keys" % (build_category.name, probe_category.name))
            for identity, key_node, column_type in (
                (build_identity, build_node, build_ct),
                (probe_identity, probe_node, probe_ct),
            ):
                if column_type.physical != target.physical:
                    coercions[identity] = (key_node, target_name, target)
        return coercions

    def _asof_match_coercions(self, node):
        """Key-coercion entries for an ASOF MATCH_CONDITION whose two sides differ.

        An ASOF match column is a join key by another name, and it has the identical
        failure mode `_join_key_coercions` exists for: the ASOF bisect orders rows by
        `sort_num_key`, which normalises each column by ITS OWN physical type — a
        sign-flip for signed ints, the IEEE order transform for doubles. Each encoding
        is order-preserving on its own and the two are NOT comparable to each other,
        so a cross-type MATCH_CONDITION emitted matches that violated the condition
        the user wrote:

          satellites AS a ASOF JOIN planets AS b MATCH_CONDITION(a.id < b.orbital_velocity)
            -> 173 of 177 matched rows had a.id >= b.orbital_velocity

        Same-type match columns were, and are, correct. Rather than refuse the
        cross-type form this reuses the coercion the equi-key path already ratified:
        CAST the narrower side and order on that, so both sides normalise through one
        encoding.

        Returns the same {identity: (key_node, target_name, target_ct)} shape
        `_coerce_join_keys` consumes. The IDENTIFIER operands are SYNTHESISED — an
        AsofJoinNode keeps only the two column identities, not the bound comparison
        (`node.on` is None for ASOF) — but they carry the real identity and the real
        ColumnType, which is all the lowering resolves against.
        """
        from opteryx.expression import Node, NodeType
        from opteryx.types.logical_type import LogicalCategory
        from opteryx.types.schema import SchemaColumn

        left_identity = getattr(node, "asof_left_column", None)
        right_identity = getattr(node, "asof_right_column", None)
        if left_identity is None or right_identity is None:
            return {}
        cts = getattr(self, "_cts", None) or {}
        left_ct, right_ct = cts.get(left_identity), cts.get(right_identity)
        if left_ct is None or right_ct is None:
            return {}
        if left_ct.physical == right_ct.physical:
            return {}
        numeric = (LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL)
        left_category, right_category = left_ct.category, right_ct.category
        if left_category not in numeric or right_category not in numeric:
            # No coercion exists for this pair, so the two sides would be ordered
            # through different normalisations and the join would emit matches that
            # violate the MATCH_CONDITION. REFUSE rather than answer wrongly.
            #
            # DATE against TIMESTAMP is the reachable case and it is NOT fixed by
            # reaching for `find_compatible_type`: that returns VARCHAR for the pair,
            # which is not an ordering anyone asked for. Casting DATE to TIMESTAMP
            # would be a new coercion the equi-key path does not perform either, so it
            # is a decision to take deliberately rather than a mechanical extension.
            _unsupported(
                "an ASOF join whose MATCH_CONDITION compares %s to %s (only numeric "
                "match columns are coerced; CAST one side explicitly)"
                % (left_category.name, right_category.name)
            )
        if left_category == LogicalCategory.INTEGER == right_category:
            # Integer widths interoperate: the ASOF key normalisation canonicalises
            # them, exactly as the equi-key hash does.
            return {}

        from opteryx.operators._operators import JoinNode
        from opteryx.types.logical_type import find_compatible_type as _lt_find_compatible

        if left_category == right_category:
            target = _lt_find_compatible([left_category, right_category])
        else:
            target = JoinNode._join_numeric_target_type(left_category, right_category)
        if target is None:
            return {}
        target_name = self._JOIN_CAST_TARGETS.get(target.category.name)
        if target_name is None:
            _unsupported(
                "an ASOF join between %s and %s match columns"
                % (left_category.name, right_category.name)
            )

        names = getattr(self, "_names", None) or {}
        coercions = {}
        for identity, column_type in ((left_identity, left_ct), (right_identity, right_ct)):
            if column_type.physical == target.physical:
                continue
            key_node = Node(
                NodeType.IDENTIFIER,
                value=names.get(identity, identity),
                schema_column=SchemaColumn(
                    name=names.get(identity, "asof key"),
                    identity=identity,
                    column_type=column_type,
                ),
            )
            coercions[identity] = (key_node, target_name, target)
        return coercions

    def _coerce_join_keys(self, p, layout, keys, coercions):
        """Append a CAST column for every key in ``keys`` that needs coercing.

        Returns ``(grown_layout, keys)`` where ``keys`` names the columns to hash —
        the synthetic identity where one was minted, the original otherwise. The
        caller keeps the PRE-growth layout for payload/output purposes."""
        if not coercions:
            return layout, keys
        from opteryx.expression import Node, NodeType
        from opteryx.planner import build_literal_node
        from opteryx.types.schema import FunctionColumn

        layout = list(layout)
        out_keys = []
        for identity in keys:
            entry = coercions.get(identity)
            if entry is None or identity not in layout:
                out_keys.append(identity)
                continue
            key_node, target_name, target_ct = entry
            schema_column = FunctionColumn(
                name="%s::%s(join key)" % (self._layout_name(identity), target_name),
                column_type=target_ct,
                aliases=[],
            )
            # A DECIMAL cast is parameterized: the kernel reads (precision, scale)
            # off the CAST node's `parameters`, exactly as the SQL path supplies them
            # for `CAST(x AS DECIMAL(p, s))`. Minting the node with no parameters
            # made every implicit DECIMAL join-key coercion — any equi-join pairing a
            # DECIMAL column with an INTEGER or FLOAT one — die at plan time with
            # `ValueError: CAST to DECIMAL requires (precision, scale)`. The target
            # ColumnType already carries the descriptor; hand it down rather than
            # letting it stop at the type object.
            cast_parameters = []
            if target_ct.logical is not None and target_name == "DECIMAL":
                cast_parameters = [
                    build_literal_node(int(target_ct.logical.precision)),
                    build_literal_node(int(target_ct.logical.scale)),
                ]
            cast_node = Node(
                NodeType.CAST,
                value=target_name,
                left=key_node,
                parameters=cast_parameters,
                schema_column=schema_column,
            )
            layout = self._add_computed(p, [cast_node], layout)
            if schema_column.identity not in layout:
                _unsupported("an implicit join-key cast the projection layer declined")
            out_keys.append(schema_column.identity)
        return layout, out_keys

    def _compile_unnest(self, in_edges, node):
        """CROSS JOIN UNNEST (single input): expand the source ARRAY column into one
        row per element via the native UnnestOperator, which repeats each parent row
        by its array length and appends the flattened element under the target
        identity.

        Row-count semantics (NULL/empty arrays, INNER vs OUTER) are draken's rule,
        stated in full above cxx_unnest in draken/draken_native.cpp. This compiler
        does not restate it and must not encode an assumption about it.

        A pushed value-filter (`WHERE unnested IN (...)`, folded to `node.filters` by
        predicate_pushdown) or pushed DISTINCT (`node.distinct`) are NOT yet folded
        into the native operator — the optimizer is configured to leave them as
        standalone FilterNode/DistinctNode operators after the unnest, which compile
        natively already. If one is present here the plan is inconsistent; fail loud
        rather than silently drop the filter/dedup."""
        if getattr(node, "_filters", None):
            _unsupported("a CROSS JOIN UNNEST with a value filter folded into the node")
        if getattr(node, "_distinct", False):
            _unsupported("a CROSS JOIN UNNEST with DISTINCT folded into the node")

        source = node._unnest_column
        if source is None:
            _unsupported("a CROSS JOIN UNNEST without a source")
        target_identity = node._unnest_target.identity

        if getattr(node, "_unnest_function", "UNNEST") == "CIDR_UNNEST":
            return self._compile_cidr_unnest(in_edges, node, source, target_identity)

        if source.node_type == NodeType.LITERAL:
            return self._compile_unnest_literal(in_edges, node, source, target_identity)

        source_sc = getattr(source, "schema_column", None)
        if source_sc is None:
            _unsupported("a CROSS JOIN UNNEST source column without a bound identity")

        (p, layout) = self._compile_only_child(in_edges, "UnnestJoinNode", node)
        array_identity = source_sc.identity
        if array_identity not in layout:
            # A COMPUTED source (UNNEST(SPLIT(name,'a'))): nothing projected it, so the
            # stream genuinely does not carry it — the operator addresses its array by
            # COLUMN INDEX and flattens the child off that column's owner, neither of
            # which an arena intermediate has. Project it first and the operator gets
            # the plain column it requires; an ExprProject output adopts the ARRAY child
            # (native_expression.hpp), so it is indistinguishable from a native ARRAY
            # column here. Mirrors _project_agg_operands and the SORT operand hoist.
            #
            # A non-c-native source still fails, one level down, inside _add_computed's
            # own gate — which is the honest place for it to fail.
            if _physical_type(source_sc) != DrakenType.ARRAY:
                _unsupported("a CROSS JOIN UNNEST over a source that is not an array")
            layout = self._add_computed(p, [source], layout)
            if array_identity not in layout:
                _unsupported("a CROSS JOIN UNNEST source array the engine could not resolve here")

        # Prune dead parent columns before the fan-out replicates them (see
        # _narrow_unnest_input). Must run AFTER the computed-source hoist above, or
        # the array column it just added would not be in the layout being narrowed.
        layout = self._narrow_unnest_input(p, layout, node, keep_identities=(array_identity,))
        array_idx = layout.index(array_identity)

        # Drop the consumed source array unless something ABOVE the unnest still
        # reads it (`pre_update_columns` is projection_pushdown's liveness set).
        # Dropping matters: a replicated ARRAY column cannot pass through a
        # downstream gather_rows join/sort. Keeping it is required by `SELECT *`.
        # An empty/absent liveness set means "unknown" — keep, never lose a column.
        needed = getattr(node, "pre_update_columns", None) or set()
        drop_source = bool(needed) and array_identity not in needed

        # A WHERE on the unnested column, folded here by predicate_pushdown. The
        # native operator evaluates it over the array's CHILD vector before the
        # fan-out, so the rows it rejects are never built — an unnest explodes, and a
        # filter above it pays for every row it is about to throw away.
        #
        # The program is resolved against the ONE-COLUMN layout `[target_identity]`
        # because that is what the child vector is presented as. predicate_pushdown
        # only folds predicates reading nothing but the target, so nothing else can
        # need resolving; if that ever changes, _resolve_bc_for_layout fails loudly on
        # the unresolvable identity rather than reading a wrong column.
        folded = list(getattr(node, "filter_conditions", None) or [])
        bytecode = None
        if folded:
            from opteryx.compiled.structures.node import Node

            condition = folded[0]
            for extra in folded[1:]:
                conjunction = Node(NodeType.AND)
                conjunction.left = condition
                conjunction.right = extra
                condition = conjunction
            bytecode = self._lower_expression(
                condition, "a filter pushed into a CROSS JOIN UNNEST")

        # Work out the output layout BEFORE deciding on the pushed DISTINCT — the
        # precondition is a statement about that layout.
        new_layout = list(layout)
        if drop_source:
            new_layout[array_idx] = target_identity
        else:
            new_layout.append(target_identity)

        # PUSHED DISTINCT (distinct_pushdown set the intent; this is the veto).
        # Honoured ONLY when the target is the sole column leaving the unnest: with
        # any other column present, two rows sharing a target value are DIFFERENT
        # rows and dropping one deletes a distinct result. The optimizer cannot make
        # this test — a no-ON Distinct dedups on whatever reaches it, which its
        # `.columns` does not describe — so the check belongs here, against the real
        # layout, and a flag that fails it is silently and correctly ignored.
        #
        # The Distinct node is NOT removed. This is a per-worker pre-reduction that
        # shrinks what the DistinctSink has to dedup; only the sink dedups ACROSS
        # workers.
        distinct_target = (
            bool(getattr(node, "distinct_target", False))
            and len(new_layout) == 1
            and new_layout[0] == target_identity
        )

        if bytecode is not None or distinct_target:
            self.nplan.add_unnest_filtered(p, array_idx, target_identity, drop_source,
                                           bytecode, [target_identity], distinct_target)
        else:
            self.nplan.add_unnest(p, array_idx, target_identity, drop_source)
        return p, new_layout

    def _narrow_unnest_input(self, p, layout, node, keep_identities=()):
        """Drop parent columns the fan-out would replicate for nothing, BEFORE it fans out.

        CROSS JOIN UNNEST does not expand, it EXPLODES: rows out is the SUM of the
        array lengths. A parent column that nothing above the unnest reads is still
        replicated across every expanded row and then discarded by the Project above,
        so the waste is paid at the EXPANDED row count. A select placed AFTER the
        unnest cannot recover it — the copies have already been built.

        `drop_source` (below) already removes the consumed source ARRAY. This removes
        the parent's OTHER dead columns, which `drop_source` never covered. The
        clearest case is a COMPUTED source, `UNNEST(SPLIT(s, ','))`: `s` is live
        BELOW the unnest because SPLIT reads it, and dead ABOVE it, so projection
        pushdown cannot prune it at the scan the way it prunes an unread column out
        of a plain `UNNEST(arr)`. Measured on this corpus that is one ~24-byte VARCHAR
        replicated across 1.7M expanded rows to be thrown away.

        `keep_identities` are columns the OPERATOR still needs — the source array.
        They are live by definition but are deliberately absent from
        `pre_update_columns`, which is snapshotted before the node's own columns are
        collected precisely so it holds what survives once those columns' purpose is
        spent.

        `ColumnSelectOperator` is zero-copy (it moves each kept column's shared
        owner), so on the no-dead-column path this costs nothing and is skipped
        outright.

        An empty `pre_update_columns` means UNKNOWN, not "nothing is wanted", so it
        keeps every column — never lose a column to an assumption."""
        live = getattr(node, "pre_update_columns", None) or set()
        if not live:
            return layout
        keep = [
            i for i, identity in enumerate(layout)
            if identity in live or identity in keep_identities
        ]
        if len(keep) == len(layout):
            return layout      # nothing dead — stay on the untouched path
        narrowed = [layout[i] for i in keep]
        self.nplan.add_select(p, keep, narrowed)
        return narrowed

    def _compile_cidr_unnest(self, in_edges, node, source, target_identity):
        """CROSS JOIN CIDR_UNNEST: expand text CIDR blocks into one IPV4 row each.

        There is deliberately NO literal variant to mirror _compile_unnest_literal.
        A literal CIDR is projected as a constant column and expanded through the
        same streaming operator, because the plan-time route is wrong twice: it
        would need an arbitrary size limit (a literal /0 is 4.3 billion addresses
        materialized during compilation), and vector_from_sequence cannot attach
        the IPV4 descriptor — so the column would DECLARE IPV4 while carrying a
        bare integer vector, render as numbers, and be refused by CIDR_AGG.
        """
        source_sc = getattr(source, "schema_column", None)
        if source_sc is None:
            _unsupported("a CROSS JOIN CIDR_UNNEST source without a bound identity")

        (p, layout) = self._compile_only_child(in_edges, "UnnestJoinNode", node)
        cidr_identity = source_sc.identity
        if cidr_identity not in layout:
            # Literal or computed source: nothing projected it, and the operator
            # addresses its source by COLUMN INDEX. Project it first — the same
            # hoist the array form does one branch down.
            layout = self._add_computed(p, [source], layout)
            if cidr_identity not in layout:
                _unsupported("a CROSS JOIN CIDR_UNNEST source the engine could not resolve here")

        # Same fan-out prune as the array form — and CIDR_UNNEST is the extreme of
        # the shape, expanding one row to a whole subnet.
        layout = self._narrow_unnest_input(p, layout, node, keep_identities=(cidr_identity,))
        cidr_idx = layout.index(cidr_identity)

        # Same liveness rule as the array form: drop the consumed source only when
        # projection_pushdown proves nothing above reads it. Absent set == unknown,
        # so keep it — never lose a column to an assumption.
        needed = getattr(node, "pre_update_columns", None) or set()
        drop_source = bool(needed) and cidr_identity not in needed

        self.nplan.add_cidr_unnest(p, cidr_idx, target_identity, drop_source)
        new_layout = list(layout)
        if drop_source:
            new_layout[cidr_idx] = target_identity
        else:
            new_layout.append(target_identity)
        return p, new_layout

    def _compile_unnest_literal(self, in_edges, node, source, target_identity):
        """CROSS JOIN UNNEST over a LITERAL array (`T CROSS JOIN UNNEST((a,b,c)) AS x`).
        The literal is a PLAN CONSTANT: materialize its elements once, here, into a
        one-column morsel (the same legitimacy as `_compile_materialized_source`'s
        virtual datasets) and let the native operator tile it across the input rows.
        Unlike the column form there is no source ARRAY column to consume, so the
        target column is APPENDED to the layout."""
        from draken.interop.vector_sequence import vector_from_sequence
        from draken.morsels.morsel import Morsel

        # UnnestJoinNode.__init__ has already wrapped a bare scalar into a tuple.
        values = list(source.value)
        physical = _physical_type(node._unnest_target)
        if physical is None or physical == DrakenType.VARIANT:
            _unsupported("a CROSS JOIN UNNEST over a literal array of untyped elements")

        literal_morsel = Morsel.from_vectors(
            [target_identity], [vector_from_sequence(values, dtype=physical)])

        (p, layout) = self._compile_only_child(in_edges, "UnnestJoinNode", node)

        # This form never had a `drop_source` at all — it only ever APPENDS, so
        # every parent column rode the fan-out regardless of whether anything read
        # it. There is no source array to exempt here, so the keep-set is purely
        # what is live above.
        layout = self._narrow_unnest_input(p, layout, node)

        self.nplan.add_unnest_literal(p, literal_morsel, target_identity)
        return p, list(layout) + [target_identity]

    def _compile_asof_join(self, node, in_edges):
        """ASOF JOIN: LEFT-preserving nearest-match by the MATCH_CONDITION column
        within optional USING equi partitions (mirrors the legacy operator's bisect
        semantics). LEFT leg = probe/preserved, RIGHT leg = build; per probe row
        exactly one build match (or NULL build payload)."""
        asof_left = getattr(node, "asof_left_column", None)
        asof_right = getattr(node, "asof_right_column", None)
        asof_op = getattr(node, "asof_op", None)
        op_codes = {"GtEq": 0, "Gt": 1, "LtEq": 2, "Lt": 3}
        if asof_left is None or asof_right is None or asof_op not in op_codes:
            _unsupported("an ASOF join without a supported MATCH_CONDITION")
        left_cols = list(getattr(node, "left_columns", None) or [])
        right_cols = list(getattr(node, "right_columns", None) or [])
        if len(left_cols) != len(right_cols):
            _unsupported("an ASOF join with unaligned USING key lists")
        legs = {}
        for idx, (provider, _target, label) in enumerate(in_edges):
            if not label:
                label = "left" if idx == 0 else "right"
            legs[label] = provider
        if "left" not in legs or "right" not in legs:
            _unsupported("an ASOF join without labelled left/right legs")

        # BOTH legs are compiled before anything is wired. The match-column coercion
        # below reads each side's ColumnType out of the compiler's identity->type map,
        # which is populated as a node compiles — computing it any earlier saw an
        # empty map and silently coerced nothing.
        bp, blayout = self.compile_node(legs["right"])
        build_key_idx = []
        for identity in right_cols:
            if identity not in blayout:
                _unsupported("an ASOF build-side key the engine could not resolve here")
            build_key_idx.append(blayout.index(identity))
        if asof_right not in blayout:
            _unsupported("an ASOF match column the build stream does not carry")

        pp, playout = self.compile_node(legs["left"])
        probe_key_idx = []
        for identity in left_cols:
            if identity not in playout:
                _unsupported("an ASOF probe-side key the engine could not resolve here")
            probe_key_idx.append(playout.index(identity))
        if asof_left not in playout:
            _unsupported("an ASOF match column the probe stream does not carry")

        # A cross-type MATCH_CONDITION orders the two sides through different
        # normalisations and silently emits matches that violate it — see
        # _asof_match_coercions. The CAST columns are appended AFTER each leg's real
        # columns, so `blayout`/`playout` (payload and output) stay untouched and only
        # the match-column index moves onto the coerced column.
        coercions = self._asof_match_coercions(node)
        bmatchout, (asof_right,) = self._coerce_join_keys(
            bp, blayout, [asof_right], coercions)
        pmatchout, (asof_left,) = self._coerce_join_keys(
            pp, playout, [asof_left], coercions)

        ref = self.nplan.new_join2_ref()
        self.nplan.set_current_identity(node.identity)  # own the asof build sink + probe
        self.nplan.set_current_display_name(type(node).__name__)
        # `blayout` is the leg's REAL output; `bmatchout` may carry a synthetic cast
        # column at the end. Payload and output use the former — letting the cast
        # column into the payload would emit a column the declared output layout does
        # not have, shifting every column after it — and only the match-column INDEX
        # uses the latter, because the sink reads that one off the INPUT morsel.
        build_types, build_logical, build_element = self._payload_types(
            legs["right"], blayout)
        # The match column's type AFTER coercion decides how the native bisect orders
        # keys (64-bit numeric / DECIMAL128 / string-family). It is passed explicitly
        # rather than read out of `build_types`, because a coerced match column is a
        # synthetic CAST appended past the payload and has no entry there.
        asof_type = self._layout_type(self.plan[legs["right"]], asof_right)
        if asof_type is None:
            _unsupported("an ASOF match column whose type the compiler cannot resolve")
        asof_est_rows = getattr(node, "join_output_rows_estimate", None)
        self.nplan.set_asof_build_sink(bp, build_key_idx, list(range(len(blayout))),
                                       bmatchout.index(asof_right), ref,
                                       build_types, build_logical, build_element,
                                       asof_type.value,
                                       _estimate_to_int64(
                                           asof_est_rows,
                                           "output-row estimate for the asof join"))
        self.nplan.set_current_identity(node.identity)  # probe op belongs to the join
        self.nplan.set_current_display_name(type(node).__name__)
        self.nplan.add_asof_probe(pp, ref, probe_key_idx, list(range(len(playout))),
                                  pmatchout.index(asof_left), op_codes[asof_op])
        # AsofProbeOperator emits build payload columns first, then probe payload —
        # the SYNTHETIC cast columns are excluded from both, so `SELECT *` is
        # unchanged by the coercion.
        return pp, list(blayout) + list(playout)

    def _compile_materialized_source(self, node):
        """Virtual datasets ($planets, VALUES, GENERATE_SERIES, contradiction-empty
        relations): their content is a PLAN CONSTANT — materialize it once, here, at
        compile time into a native buffer. Execution reads the buffer natively; no
        Python runs. (Same legitimacy as bind-time literal materialization.)"""
        buf = self.nplan.new_buffer()
        expected = [col.schema_column.identity for col in (node.columns or [])]
        morsel_names = None
        for morsel in node.read_morsels():
            self.nplan.add_buffer_morsel(buf, morsel)
            if morsel_names is None:
                # Raw names — identities are bytes plan-wide; keep them comparable.
                morsel_names = list(morsel.column_names)
        # The materialized morsel's own column order IS the stream layout (it may
        # legitimately differ from the plan's projection list order); every plan
        # column must be present in it — anything extra is simply never selected.
        layout = morsel_names or expected or []
        missing = [identity for identity in expected if identity not in layout]
        if missing:
            _unsupported(f"a virtual dataset missing plan columns {missing}")
        if not layout and expected:
            # An empty layout while the plan still expects columns is a real defect —
            # the source produced nothing the plan can read from. (Unreachable via
            # `layout`'s own definition above, which falls back to `expected`; kept as
            # a guard so a future change to that fallback cannot pass silently.)
            _unsupported("a zero-column virtual dataset")
        # An empty layout with NOTHING expected is the legal zero-projection shape —
        # bare `COUNT(*)` over READ_CSV/READ_JSONL, whose scan nodes emit genuine
        # zero-column morsels carrying their row count in `zero_col_rows` (see
        # csv_read.pyx / jsonl_read.pyx). BufferSource hands those morsels through
        # verbatim and CxxMorsel::num_rows() reads `zero_col_rows` when there are no
        # columns, so the downstream UngroupedAggSink CountStar sees the right count —
        # the same contract `_compile_scan`'s parquet path already relies on when it
        # returns an empty layout for a zero-projection scan.
        #
        # The other materialized sources ($planets, VALUES, GENERATE_SERIES) never
        # reach here with an empty layout: they ignore the projection and materialize
        # every column, so a zero-projection query over them arrives with a NON-empty
        # layout and is reduced to zero columns by the downstream Select instead.
        p = self.nplan.new_pipeline()
        self.nplan.set_buffer_source(p, buf)
        self._remember_types(node.columns)
        return p, layout

    # ---- identity -> physical type tracking ------------------------------------

    def _remember_types(self, columns):
        types = getattr(self, "_types", None)
        if types is None:
            types = self._types = {}
        cts = getattr(self, "_cts", None)
        if cts is None:
            cts = self._cts = {}
        names = getattr(self, "_names", None)
        if names is None:
            names = self._names = {}
        for col in columns or []:
            sc = col.schema_column
            pt = _physical_type(sc)
            if pt is not None:
                types[sc.identity] = pt
            ct = getattr(sc, "column_type", None)
            if ct is not None:
                cts[sc.identity] = ct
            name = getattr(sc, "name", None)
            if name is not None:
                names[sc.identity] = name

    def _payload_types(self, node_id, layout):
        """Physical DrakenType (int) + logical tuple + ARRAY element chain for each
        identity in ``layout``, for the native join build sinks
        (``set_join2_build_sink``/``set_asof_build_sink``) and the FULL OUTER tail
        source (``set_unmatched_build_source``): the compiler already knows every
        payload column's type from binding — same source/shape as
        ``compile_to_native``'s ``final_types``/``final_logical`` — so it hands the
        type down instead of the C++ build sink ever needing to learn it from data.
        That learn-from-first-morsel path never runs when the side genuinely streams
        zero rows (a filtered-to-empty subquery), which is exactly the shape that
        broke LEFT OUTER's unmatched-row emit, and it never runs AT ALL for the FULL
        OUTER tail's probe half, which retains no probe morsel to learn from."""
        node = self.plan[node_id]
        by_identity = {}
        for col in getattr(node, "columns", None) or []:
            sc = getattr(col, "schema_column", None)
            if sc is not None:
                by_identity[sc.identity] = getattr(sc, "column_type", None)
        cts = getattr(self, "_cts", None) or {}
        types_map = getattr(self, "_types", None) or {}
        types, logical, element = [], [], []
        for identity in layout:
            ct = by_identity.get(identity) or cts.get(identity)
            pt = ct.physical if ct is not None else types_map.get(identity)
            # When a build-payload column's type is unresolvable here (e.g. an
            # aggregate output whose result type the binder never threaded into the
            # compiler's type maps), the value defaults to VARCHAR. The native build
            # sink treats these plan types as a fallback ONLY: it learns each payload
            # column's real type from the first non-empty morsel (Join2BuildSink),
            # so a wrong default here cannot mis-materialize a non-empty build side.
            types.append(pt.value if pt is not None else DrakenType.VARCHAR.value)
            logical.append(_logical_tuple(ct))
            element.append(_element_chain(ct))
        return types, logical, element

    def _layout_type(self, node, identity):
        types = getattr(self, "_types", None) or {}
        if identity in types:
            return types[identity]
        # Fall back to the node's own column bindings.
        for col in getattr(node, "columns", None) or []:
            sc = getattr(col, "schema_column", None)
            if sc is not None and sc.identity == identity:
                return _physical_type(sc)
        return None

    def _layout_name(self, identity):
        """User-facing column name for an identity, when known — used only to name
        the offending column in a plan-time NotSupportedError; falls back to the
        (opaque) identity when no scan/materialized-source column recorded a name
        for it (e.g. a computed key with no simple source column)."""
        names = getattr(self, "_names", None) or {}
        return names.get(identity, identity)


def compile_to_native(plan, pool=None):
    """Compile ``plan`` into a runnable ``(NativePlan, PyMorselQueue, scan_sources)``.
    ``scan_sources`` maps each parquet scan node identity to the Source it was wired
    to ("NativeParquetScanSource" or "StreamingScanSource") — WP-INSTR instrument 2.
    Raises ``NotSupportedError`` at once — before anything runs — for any shape the
    native engine has no operator for.

    ``pool``: the query's exec CppThreadPool (Gap #3 Phase 2b), if the caller
    constructed it before compiling — see execute_native. None (the default) is
    fully supported: every scan falls back to its own self-constructed decode pool,
    identical to pre-Phase-2b behaviour."""
    from opteryx.compiled.morsel_queue import PyMorselQueue
    from opteryx.operators._operators import NativePlan

    heads = list(set(plan.get_exit_points()))
    if len(heads) != 1:
        _unsupported(f"a plan with {len(heads)} heads")
    exit_id = heads[0]
    exit_node = plan[exit_id]
    if type(exit_node).__name__ != "ExitNode":
        _unsupported(f"a plan headed by {type(exit_node).__name__}")

    nplan = NativePlan()
    compiler = _Compiler(plan, nplan, pool=pool)

    # Shared CTE bodies FIRST, in dependency order: run() executes pipelines in
    # creation order, so every body's buffer is fully materialized before any
    # pipeline that reads it exists. Each body is a plan of its own (no Exit);
    # its head streams into a buffer-append sink, and the CteRefNode arm of
    # compile_node wires each reference to that buffer.
    for cte_key, body_plan in (getattr(plan, "shared_ctes", None) or {}).items():
        body_compiler = _Compiler(body_plan, nplan, pool=pool)
        body_compiler.cte_buffers = compiler.cte_buffers
        body_heads = list(set(body_plan.get_exit_points()))
        if len(body_heads) != 1:
            _unsupported(f"a shared CTE body with {len(body_heads)} heads")
        body_pipeline, body_layout = body_compiler.compile_node(body_heads[0])
        buf = nplan.new_buffer()
        nplan.set_buffer_append_sink(body_pipeline, buf)
        compiler.cte_buffers[cte_key] = (buf, list(body_layout))
        # fold the body's plan-time facts into the facts this compile returns
        compiler.scan_sources.update(body_compiler.scan_sources)
        compiler.scan_facts.update(body_compiler.scan_facts)
        compiler.scan_residual_reasons.update(body_compiler.scan_residual_reasons)
        compiler.footer_fetch_ns += body_compiler.footer_fetch_ns

    in_edges = list(plan.ingoing_edges(exit_id))
    if len(in_edges) != 1:
        _unsupported(f"an Exit with {len(in_edges)} inputs")
    p, layout = compiler.compile_node(in_edges[0][0])
    nplan.set_current_identity(exit_node.identity)  # exit select + queue sink
    nplan.set_current_display_name(type(exit_node).__name__)

    # Exit semantics: select final_columns (identities) in order, rename to final_names.
    indices = []
    for identity in exit_node.final_columns:
        if identity not in layout:
            _unsupported("an output column the engine could not resolve here")
        indices.append(layout.index(identity))
    nplan.add_select(p, indices, list(exit_node.final_names))

    out_q = PyMorselQueue(_QUEUE_DEPTH)
    nplan.set_queue_sink(p, out_q)

    final_types = []
    final_logical = []
    for col in exit_node.columns:
        ct = getattr(col.schema_column, "column_type", None)
        pt = ct.physical if ct is not None else None
        final_types.append(pt.value if pt is not None else DrakenType.VARCHAR.value)
        final_logical.append(_logical_tuple(ct))
    nplan.set_final_schema(list(exit_node.final_names), final_types, final_logical)
    return (nplan, out_q, compiler.scan_sources, compiler.scan_facts,
            compiler.scan_residual_reasons, compiler.footer_fetch_ns)


def execute_native(plan, telemetry=None, trace_sink=None):
    """THE data executor: compile to the native pipeline graph and run it. Returns the
    ``(generator, ResultType)`` contract the cursor consumes. The generator drains the
    engine's output queue; the engine runs on its own native driver + worker pool.

    ``trace_sink``: an optional opteryx.models.trace_bundle.TraceBundle. When
    tracing is armed (config.OPTERYX_TRACE), the drained span blob and symbol
    tables are written onto it at teardown — NOT onto ``telemetry`` (see
    TraceBundle's docstring for why trace data must not live in telemetry)."""
    from opteryx import config
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.operators._operators import NativeErrorSlot
    from opteryx.operators._operators import build_terminal_exc
    from opteryx.operators._operators import native_plan_execute
    from opteryx.operators._operators import native_trace_drain
    from opteryx.operators._operators import native_trace_drain_file_symbols
    from opteryx.operators._operators import native_trace_host_info
    from opteryx.operators._operators import native_trace_set_enabled
    from opteryx.operators._operators import native_trace_start_query
    from opteryx.variables import resolve as _resolve_var

    import time as _t

    # Every physical node in `plan` shares the SAME QueryProperties instance (each
    # was built via `creator(logical_node, query_properties, registry)` in
    # create_physical_plan), so any one node's `.properties.variables` is this
    # query's resolved session variables. `plan.nodes()` yields ids; take the first.
    _first_nid = next(iter(plan.nodes()), None)
    _query_variables = (
        getattr(plan[_first_nid].properties, "variables", None) if _first_nid is not None else None
    )
    dop = resolve_worker_count(
        _resolve_var("max_execution_workers", _query_variables, config.MAX_EXECUTION_WORKERS)
    )

    # Gap #3 Phase 2b: the exec pool is now constructed BEFORE compilation (moved
    # up from after) so it can be handed to compile_to_native and shared with any
    # parquet scan's decode pool, instead of the scan self-constructing its own
    # uncoordinated one. dop does not depend on the compiled plan — only on config
    # and cpu count — so this reorder changes nothing about what dop resolves to.
    #
    # The pool is thread-local and REUSED across queries on this driver thread
    # (see _acquire_engine_pool): first query on a thread pays the spawn, every
    # subsequent one skips it — and skips the join at teardown too (~0.7ms/query
    # at dop=16). _pool_create_ns therefore reads ~0 after the first query, which
    # is the point.
    _pool0 = _t.perf_counter_ns()
    pool = _acquire_engine_pool(dop)
    _pool_create_ns = _t.perf_counter_ns() - _pool0

    # Native plan compilation (the _Compiler walk + bytecode lowering + operator
    # instantiation) runs synchronously here, in the execution phase but BEFORE the
    # driver generator produces anything — so it is inside time_executing yet
    # invisible to time_engine_generator_total. Cost is ~independent of row count,
    # so it dominates cheap queries. Timed as an always-on driver span.
    #
    # compile_to_native also does cold-cache footer fetch/parse for every native
    # parquet scan it touches (open_native_scan_plan) — real network IO, not plan
    # compilation, and on a large/uncached file set it can dominate this whole
    # span (one serial round-trip per uncached file). compiler.footer_fetch_ns
    # carries that cost back out so it's subtracted below: time_engine_compile
    # reports actual compile cost, and time_engine_footer_fetch reports the IO
    # separately instead of one hiding inside the other's name.
    _compile0 = _t.perf_counter_ns()
    (nplan, out_q, scan_sources, scan_facts, scan_residual_reasons,
     _footer_fetch_ns) = compile_to_native(plan, pool=pool)
    _compile_ns = _t.perf_counter_ns() - _compile0 - _footer_fetch_ns
    # WP-INSTR instrument 2: which Source each parquet scan selected (a plan-time
    # fact — recorded whether or not the GIL instrumentation is armed; the dict is
    # tiny and costs nothing to attach).
    if telemetry is not None:
        telemetry._reading["native_engine_engaged"] = 1
        telemetry._reading["native_engine_dop"] = dop
        telemetry._reading["scan_sources"] = dict(scan_sources)
        # A0 acceptance gate: WHY each trampoline (StreamingScanSource) scan fell
        # back — the stable R1..R7 reason code from _native_scan_plan, keyed by
        # scan identity, parallel to scan_sources. Plan-time fact, always recorded
        # (tiny dict), so a close-out chip can assert "reason Rx now shows zero".
        if scan_residual_reasons:
            telemetry._reading["scan_residual_reasons"] = dict(scan_residual_reasons)
        # Native-scan plan-time facts (files/row-groups/columns read), keyed by
        # scan identity — overlaid onto the scan's sensor row by mermaid.py to
        # replace the always-zero ScanReadings fields on the native path.
        if scan_facts:
            telemetry._reading["native_scan_facts"] = dict(scan_facts)

    # WP-INSTR instruments 1 & 4: arm the execution-time GIL instrumentation for the
    # span of this run when the config flag is set. Disarmed by default → the
    # instrumented sites pay a single-branch check and nothing else.
    instrument_gil = bool(config.OPTERYX_INSTRUMENT_ENGINE)
    # docs/EXECUTION_TRACING_DESIGN.md: arm native span recording for this run when
    # the caller gave us somewhere to put the result (trace_sink) — recording
    # with nowhere to drain to would just be wasted work. The `trace` session
    # variable decision (opteryx/variables.py; defaults to OPTERYX_TRACE, settable
    # per-statement via `SET trace TO ...`) already happened in query_session's
    # _inner_execute: trace_sink is None here unless that check passed, so this
    # is the only gate needed. Runtime-gated (the bridge's g_trace_enabled), not
    # compile-time — off by default costs one predicted atomic-load branch per
    # span site and nothing else.
    trace_enabled = trace_sink is not None

    # Completion + terminal-error coordination is fully native: the detached driver
    # signals completion by finishing ``out_q`` (its last act) and records any terminal
    # error in this native ``errslot`` (a C int + std::string, no Python object touched
    # from the driver thread). No ``threading.Event`` and no borrowed error list.
    errslot = NativeErrorSlot()

    def generator():
        # Always-on driver-span instrumentation. time_executing (measured around
        # the whole generator in query_session) spans regions no operator counter
        # covers: submit, first-morsel latency, cumulative queue-wait, the
        # consumer's between-pull work (downstream of the yield), and teardown.
        # perf_counter_ns is ~tens of ns; called O(1) per query + 2x per morsel,
        # so it is noise. Keys are time_engine_* so as_dict auto-converts to s.
        import time as _t

        _gen_start = _t.perf_counter_ns()
        _queue_wait_ns = 0
        _consumer_ns = 0
        _first_morsel_ns = 0
        _got_first = False
        # Did we drain the queue all the way to its native FINISHED signal? If so, the
        # driver is provably done (finish() is its last act) and teardown needs no
        # further wait. If we broke out early (consumer abandon / GeneratorExit), we
        # must wait for the driver's finish() natively before teardown (see finally).
        _saw_finished = False

        if instrument_gil:
            from opteryx.operators._operators import instr_gil_reset
            from opteryx.operators._operators import instr_gil_set_enabled

            # Arm BEFORE the driver submits so every worker sees the flag set.
            instr_gil_reset()
            instr_gil_set_enabled(True)
        _trace_query_seq = 0
        if trace_enabled:
            # Bump the generation and arm the gate BEFORE the driver submits, same
            # ordering requirement as the GIL instrument above — every worker must
            # see the new generation/enabled flag before it starts recording.
            _trace_query_seq = native_trace_start_query()
            native_trace_set_enabled(True)
        _t0 = _t.perf_counter_ns()
        handle = native_plan_execute(pool, nplan, dop, out_q, errslot)
        _submit_ns = _t.perf_counter_ns() - _t0
        try:
            while True:
                _g0 = _t.perf_counter_ns()
                item = out_q.get()
                _g1 = _t.perf_counter_ns()
                _queue_wait_ns += _g1 - _g0
                if not _got_first:
                    # First morsel out of the queue: submit + engine spin-up +
                    # source setup (footer fetch etc.) latency to first output.
                    _first_morsel_ns = _g1 - _gen_start
                    _got_first = True
                if item is None or item is MQ_FINISHED:
                    # MQ_FINISHED is the driver's native completion signal (its last
                    # act) — reaching it proves every worker joined, so teardown skips
                    # the wait below. (None = queue already closed, which we only do in
                    # our own finally, so it is not expected mid-loop.)
                    _saw_finished = item is MQ_FINISHED
                    break
                _y0 = _t.perf_counter_ns()
                yield item
                # Wall time the consumer spent before pulling the next morsel —
                # generator is suspended here, so this is downstream-of-yield work
                # (materialize, GCS part uploads, etc.) that also lands in
                # time_executing despite the engine being idle.
                _consumer_ns += _t.perf_counter_ns() - _y0
        finally:
            # Close first (unblocks any backpressured producer put), then wait for the
            # driver to stop touching the pool before tearing the pool down. Native
            # scan plans (rugo IO pipelines the Source borrows) are only safe to
            # close once the driver — and therefore every engine worker — is done.
            out_q.close()
            _tw0 = _t.perf_counter_ns()
            # Normal path: we already drained to MQ_FINISHED, so the driver is proven
            # done — no wait. Abandon path (GeneratorExit / early break): close() above
            # fast-stopped the producer; wait natively for the driver to unwind
            # eng.run() (every worker joined) and call finish() before tearing down the
            # pool and scans. This is the native replacement for the old done.wait().
            if not _saw_finished:
                out_q.wait_finished()
            _done_wait_ns = _t.perf_counter_ns() - _tw0
            # Trampoline scans (StreamingScanSource) accumulate ScanReadings during
            # next_morsel but only flush them into node.readings in close_source() —
            # which the native engine's pull loop never calls (it just detects EOS).
            # The driver is done (queue FINISHED, or wait_finished returned → every
            # worker finished), so it is safe to close each scan on this thread:
            # flush_into populates the readings sensors()/mermaid read, and the source
            # is released. Idempotent
            # (close_source guards on _scan_finished), so scans that self-closed are
            # untouched. Native-parquet scans have no ScanReadings to flush — their
            # facts come from native_scan_facts — and their close_source is a no-op.
            # IO diagnostics for trampoline scans must be read BEFORE close_source
            # drops the source (and its IO pipeline) — collect them here, merged
            # later with the native-scan diagnostics into one io_scan_diagnostics.
            _io_diags: list = []
            _scans = getattr(nplan, "scans", None)
            if _scans:
                if telemetry is not None:
                    for _scan in _scans:
                        _io_fn = getattr(_scan, "io_diagnostics", None)
                        if _io_fn is not None:
                            _diag = _io_fn()
                            if _diag:
                                _io_diags.append(_diag)
                for _scan in _scans:
                    _scan.close_source()
            # Skene row-group SKIPPING is a run-time decision (the Source's claim
            # builder reads each file's footer statistics), so unlike parquet's
            # plan-time pruning its count does not exist when scan_facts is built.
            # Fold it in here, at the same "driver is done, counters are final"
            # point the operator stats below use.
            _fold_skene_scan_facts(nplan, telemetry)
            # The driver is done, so every operator's counters are final: harvest the
            # per-operator telemetry and fold it onto the session telemetry, keyed by
            # plan-node identity (mermaid's get_node_stats reads it back for the
            # ``operations`` breakdown). Several native operators/sources/sinks can
            # share one identity: either a sequential chain within one pipeline (a
            # scan with its residual filter relocated onto it, WP-02 — source ->
            # operator -> operator, each stage feeding the next), or independent
            # pipelines fanning into a shared buffer (each UNION leg tags its own
            # pipeline's select+append-sink with the union node's identity, then the
            # buffer-reading pipeline that follows also carries it). In both shapes
            # collect_op_stats (engine.hpp) emits rows in pipeline-creation / role
            # order, so the LAST row for an identity is always the terminal stage —
            # its records_in/records_out/bytes_in/bytes_out are the node's real
            # input/output (summing them, as before, double-counts every
            # intermediate stage's output as if it were additional data). calls/
            # execution_time/cpu_time genuinely are additive work done across every
            # stage (chain or fan-in), so those keep being summed.
            _th0 = _t.perf_counter_ns()
            if telemetry is not None:
                op_stats: dict = {}
                for row in nplan.collect_op_stats():
                    ident = row["identity"]
                    if not ident:
                        continue
                    agg = op_stats.get(ident)
                    if agg is None:
                        op_stats[ident] = {
                            "records_in": row["records_in"],
                            "records_out": row["records_out"],
                            "bytes_in": row["bytes_in"],
                            "bytes_out": row["bytes_out"],
                            "calls": row["calls"],
                            "execution_time": row["execution_time"],
                            "cpu_time": row["cpu_time"],
                        }
                    else:
                        # records_out/bytes_out: last-row-wins is correct — the
                        # terminal stage's OUTPUT is the node's real output.
                        # records_in/bytes_in: a "source" row structurally has no
                        # input (a Source has no upstream — see executor.hpp) so
                        # its 0 must not clobber a real reading an earlier
                        # sink/operator row already captured for this identity
                        # (DISTINCT/GROUP BY/SORT: the sink measures real input,
                        # then a later buffer-reading pipeline carries the SAME
                        # identity as a "source" continuation).
                        if row["role"] != "source":
                            agg["records_in"] = row["records_in"]
                            agg["bytes_in"] = row["bytes_in"]
                        agg["records_out"] = row["records_out"]
                        agg["bytes_out"] = row["bytes_out"]
                        agg["calls"] += row["calls"]
                        agg["execution_time"] += row["execution_time"]
                        agg["cpu_time"] += row["cpu_time"]
                telemetry._reading["native_op_stats"] = op_stats
                # Per-pipeline wall/CPU. Pipelines run one at a time (engine.hpp's
                # run()), so cpu_time/wall_time is the mean cores that pipeline kept
                # busy — the only reading that can show the pool standing idle, which
                # a per-operator stat structurally cannot.
                telemetry._reading["native_pipeline_stats"] = nplan.collect_pipeline_stats()
            _harvest_ns = _t.perf_counter_ns() - _th0
            # WP-INSTR instruments 1 & 4: harvest the execution-time GIL readings
            # after the driver (and therefore every worker) is done, so the
            # accumulators are final. Then disarm — the flag is process-global.
            if instrument_gil:
                from opteryx.operators._operators import instr_gil_set_enabled
                from opteryx.operators._operators import instr_gil_total_ns
                from opteryx.operators._operators import instr_gil_worker_report

                if telemetry is not None:
                    telemetry._reading["gil_held_ns"] = instr_gil_total_ns()
                    telemetry._reading["worker_gil_sites"] = instr_gil_worker_report()
                instr_gil_set_enabled(False)
            if trace_enabled:
                # Drain after every worker has joined — the same precondition
                # collect_op_stats relies on below, so this reads finalized arenas.
                # Disarm before draining is unnecessary (drain doesn't race new
                # spans: no worker is running), but disarm promptly regardless so a
                # pooled worker picking up the NEXT (untraced) query's first task
                # can't record into a stale generation before that query arms its
                # own start_query().
                native_trace_set_enabled(False)
                _trace_blob, _trace_truncated = native_trace_drain(_trace_query_seq)
                if trace_sink is not None:
                    trace_sink.blob = _trace_blob
                    trace_sink.node_symbols = nplan.collect_trace_symbols()
                    trace_sink.file_symbols = native_trace_drain_file_symbols()
                    trace_sink.truncated = _trace_truncated
                    trace_sink.host_info = native_trace_host_info()
                if _trace_truncated and telemetry is not None:
                    # Truncation is a fact about how the query ran (a warning),
                    # not trace payload — reported through telemetry.messages
                    # like any other execution warning, not onto trace_sink.
                    telemetry.add_message(
                        "trace truncated: a worker exceeded its span arena "
                        "capacity (OPTERYX_TRACE_ARENA_SPANS) — timeline is incomplete"
                    )
            _ts0 = _t.perf_counter_ns()
            # Return the thread-local engine pool to idle for the NEXT query on
            # this thread — do NOT shut it down (which would join its `dop` worker
            # threads, ~0.7ms at dop=16, only to respawn them next query). By this
            # point the driver has signalled FINISHED (normal path) or
            # out_q.wait_finished() confirmed it unwound (abandon path), so
            # eng.run() has returned and every worker task this query submitted is
            # complete — the pool holds nothing and is safe to hand to the next
            # query. The pool's workers are joined only when its owning thread (or
            # the interpreter) exits, via CppThreadPool.__dealloc__.
            _shutdown_ns = _t.perf_counter_ns() - _ts0
            # Per-scan IO-pipeline diagnostics (GCS/HTTP request count, retries,
            # latency histogram, worker_blocked_ns) — the scan network visibility.
            # Native scan plans are read here BEFORE close_scan_plans tears their
            # pipelines down, and merged with the trampoline diagnostics collected
            # above (before close_source) into one io_scan_diagnostics list.
            if telemetry is not None:
                _scan_plans = getattr(nplan, "scan_plans", None)
                if _scan_plans:
                    for _sp in _scan_plans:
                        _diag = _sp.diagnostics()
                        if _diag:
                            _io_diags.append(_diag)
                if _io_diags:
                    telemetry._reading["io_scan_diagnostics"] = _io_diags
                    # The scan's true IO volume, measured at transfer by the rugo IO
                    # pipeline. Accumulated (not assigned): virtual-dataset scans
                    # (operators/read/read.pyx) add their own materialized bytes to
                    # this same counter, and a query can mix the two. The native
                    # engine's per-operator bytes_in/bytes_out cannot be used here —
                    # they are a rows*cols*8 estimate of the post-filter, post-LIMIT
                    # morsel, so on this query they read 7.6KB for a 309-blob scan.
                    telemetry.increase(
                        "bytes_processed",
                        sum(d.get("bytes_fetched", 0) for d in _io_diags),
                    )
                    telemetry._reading["io_http_request_count"] = sum(
                        d.get("http_request_count", 0) for d in _io_diags
                    )
                    telemetry._reading["io_http_retries"] = sum(
                        d.get("http_retries", 0) for d in _io_diags
                    )
                    # ns → auto-converted to seconds in as_dict (time_ prefix).
                    telemetry._reading["time_engine_io_worker_blocked"] = sum(
                        d.get("worker_blocked_ns", 0) for d in _io_diags
                    )
                    # Footer caches (see footer_remote_cache.py). Each key is surfaced only
                    # when at least one scan this query reported it — the -1 "not
                    # applicable" sentinel is dropped by IpcRowGroupSource.diagnostics()
                    # rather than summed as 0, so an all-local query doesn't misreport
                    # "0 hits" as a failing cache.
                    #
                    # Read the two together. `footer_process_cache_hits` alone (no remote
                    # pair) means a warm process served every footer in-process and the
                    # remote tier was never needed. A remote pair with 0 hits and non-zero
                    # misses means footers WERE fetched from origin — either a cold shared
                    # tier or no tier configured at all.
                    _footer_diags = [d for d in _io_diags if "footer_cache_hits" in d]
                    if _footer_diags:
                        telemetry._reading["footer_cache_hits"] = sum(
                            d["footer_cache_hits"] for d in _footer_diags
                        )
                        telemetry._reading["footer_cache_misses"] = sum(
                            d["footer_cache_misses"] for d in _footer_diags
                        )
                    _process_diags = [
                        d for d in _io_diags if "footer_process_cache_hits" in d
                    ]
                    if _process_diags:
                        telemetry._reading["footer_process_cache_hits"] = sum(
                            d["footer_process_cache_hits"] for d in _process_diags
                        )
            _tc0 = _t.perf_counter_ns()
            nplan.close_scan_plans()
            _close_scans_ns = _t.perf_counter_ns() - _tc0
            del handle

            if telemetry is not None:
                _gen_total_ns = _t.perf_counter_ns() - _gen_start
                # Portion of the generator's wall span NOT in the queue-wait,
                # consumer, or teardown buckets — i.e. residual driver overhead
                # between accounted spans (loop bookkeeping, native_plan_execute
                # handle, etc.). Clamped at 0 to absorb clock jitter.
                _accounted = (
                    _queue_wait_ns + _consumer_ns + _done_wait_ns
                    + _harvest_ns + _shutdown_ns + _close_scans_ns
                )
                _residual_ns = _gen_total_ns - _accounted
                if _residual_ns < 0:
                    _residual_ns = 0
                r = telemetry._reading
                # Pre-driver, per-query fixed costs (measured in execute_native's
                # body, outside the generator span above).
                r["time_engine_compile"] = _compile_ns
                r["time_engine_footer_fetch"] = _footer_fetch_ns
                r["time_engine_pool_create"] = _pool_create_ns
                r["time_engine_generator_total"] = _gen_total_ns
                r["time_engine_submit"] = _submit_ns
                r["time_engine_first_morsel"] = _first_morsel_ns
                r["time_engine_queue_wait"] = _queue_wait_ns
                r["time_engine_consumer_downstream"] = _consumer_ns
                r["time_engine_teardown_done_wait"] = _done_wait_ns
                r["time_engine_teardown_harvest"] = _harvest_ns
                r["time_engine_teardown_shutdown"] = _shutdown_ns
                r["time_engine_teardown_close_scans"] = _close_scans_ns
                r["time_engine_residual"] = _residual_ns

            # Terminal error (if any) is raised on THIS consumer thread — the
            # legitimate result-marshaling edge — built from the native errslot the
            # driver populated before it finished the queue. build_terminal_exc prefers
            # a scan's stashed rich exception, else a RuntimeError from code+msg.
            _terminal_exc = build_terminal_exc(nplan, errslot)
            if _terminal_exc is not None:
                raise _terminal_exc

    return generator(), ResultType.TABULAR
