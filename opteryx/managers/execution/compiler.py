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
from opteryx.constants import ResultType
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import VariantKeyError
from opteryx.expression import NodeType

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


def _unsupported(what: str):
    raise NotSupportedError(
        f"native engine: {what} is not supported yet. This query cannot run — there "
        "is no fallback engine (hard-cutover posture; coverage is being burned down)."
    )


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


def _logical_tuple(ct):
    """(kind, unit, precision, scale, dimension) ints for a ColumnType's descriptor,
    or None when the type carries no logical type — same shape NativePlan's native
    calls (e.g. add_expr_project) already accept, so callers pass it straight through."""
    if ct is None or ct.logical is None:
        return None
    lg = ct.logical
    return (int(lg.kind.value), int(getattr(lg.unit, "value", 0)),
            int(lg.precision), int(lg.scale), int(getattr(lg, "dimension", 0) or 0))


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
# TimestampUnit enum-name → draken unit code (matches logical_type.h TimestampUnit).
_TS_UNIT_TO_INT = {"SECONDS": 0, "MILLISECONDS": 1, "MICROSECONDS": 2, "NANOSECONDS": 3}


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
                    if phys == "DECIMAL128":
                        # DECIMAL128 literals cannot materialize (int64 tier only);
                        # pin to an int64-tier DECIMAL at the SAME SCALE — the blend
                        # kernel widens {DECIMAL, DECIMAL128} raw-exactly.
                        from opteryx.types.logical_type import DECIMAL as _mk_decimal

                        nl.type = _mk_decimal(18, scale)
                    else:
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
                q = _dec.Decimal(str(v))
                quantum = _dec.Decimal(1).scaleb(-int(ct.logical.scale))
                rescaled = q.quantize(quantum)
                if rescaled != q:
                    op = expr.value
                    if op not in ("Lt", "LtEq", "Gt", "GtEq"):
                        continue   # inexact Eq/NotEq — leave it alone, fail loud
                    # effective op on the COLUMN (swap when literal is the left leg)
                    eff = op if a == "left" else {
                        "Lt": "Gt", "Gt": "Lt", "LtEq": "GtEq", "GtEq": "LtEq"}[op]
                    rounding = (_dec.ROUND_CEILING if eff in ("GtEq", "Lt")
                                else _dec.ROUND_FLOOR)
                    rescaled = q.quantize(quantum, rounding=rounding)
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
        from opteryx.operators._operators import bytecode_is_c_native_predicate

        bc = self._lower_bytecode(expr)
        if not bytecode_is_c_native_predicate(bc):
            _unsupported(f"{what} outside the c-native kernel set")
        return bc

    def _compose_predicate_nodes(self, predicates):
        """AND-compose a list of pushed predicate nodes into one right-leaning tree.

        The SOLE composer for a pushed predicate: both the relocated native filter
        (`_native_scan_plan`) and the trampoline scan's `compiled_predicate` (bound
        in `_compile_scan`) lower this same tree through `_lower_bytecode`, so the
        two paths run identical bytecode. The scan used to re-compose and re-lower
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

    def _hoist_array_operands(self, p, eval_nodes, layout):
        """Materialize a COMPUTED ARRAY operand into its own ExprProject column, then
        point the consuming op at that column. Covers SORT/GREATEST/LEAST/LENGTH and
        the `arr[i]` subscript.

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
                        "native engine: could not allocate a JSON extraction context"
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
        for identity, bc in compile_eval_nodes(eval_nodes):
            if identity in layout:
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
                continue
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
                "ANY_VALUE", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE"}
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
    _RANK_ROW_NUMBER = 0  # row_number.pyx's _RANK_ROW_NUMBER kind code

    def _check_key_type(self, what, name, pt):
        if pt is None or pt in self._KEY_COLUMN_TYPES:
            return
        if pt == DrakenType.VARIANT:
            # Backstop only — the binder (visit_distinct / visit_aggregate_and_group)
            # already rejects a VARIANT key at bind time, before the optimizer or
            # this compiler ever run. This catches any plan-construction path that
            # bypasses normal binding; the message lives once, on the exception.
            raise VariantKeyError(what, name)
        _unsupported(f"{what} on column '{name}' ({pt})")

    @staticmethod
    def _array_agg_options(agg):
        """ARRAY_AGG's DISTINCT / ORDER BY / LIMIT modifiers, as the sink's spec wants
        them. The binder has already rejected ORDER BY on anything but the aggregated
        column, so `order` is at most one entry here."""
        from opteryx import config

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
            "max_per_group": int(config.ARRAY_AGG_MAX_VALUES_PER_GROUP),
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
                operand = params[0]
            elif len(params) == 1:
                operand = params[0]
            else:
                continue
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
                # APPROX_PERCENTILE(expr, percentile) — the only aggregate with a
                # second, query-time-constant argument (not a second operand
                # column, see CORR's still-deferred design question). Matches the
                # legacy Cython _extract_percentile_option validation exactly.
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
                _unsupported(f"{func} over a column the stream does not carry")
            idx = layout.index(psc.identity)
            pt = _physical_type(psc)
            if func == "ARRAY_AGG":
                # Grouped-only: an ARRAY_AGG list is per group, and the ungrouped
                # sink's fixed-width AggCell has nowhere to put one. The binder
                # rejects this first; this is the engine's own gate.
                if not grouped:
                    _unsupported("ARRAY_AGG without a GROUP BY")
                if pt not in self._ARRAY_AGG_OPERAND_TYPES:
                    _unsupported(f"ARRAY_AGG over a {pt} column")
                specs.append((sc.identity, "ArrayAgg", idx, self._array_agg_options(agg)))
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
                    _unsupported(f"APPROX_PERCENTILE over a {pt} column — only "
                                 "numeric inputs are accepted (CAST DECIMAL to "
                                 "DOUBLE first)")
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
                    _unsupported(f"{func} over a {pt} column")
            if func == "STDDEV" and pt in (DrakenType.DECIMAL, DrakenType.DECIMAL128):
                # The sink never descales DECIMAL's unscaled integer for STDDEV —
                # reading it as a raw double would compute the wrong numbers'
                # variance. CAST to DOUBLE first (same posture as the sink's own
                # fail-loud guard — this is just the friendlier plan-time version).
                _unsupported(f"STDDEV over a {pt} column — CAST to DOUBLE first")
            if func == "MEDIAN" and pt not in self._MEDIAN_OPERAND_TYPES:
                # MEDIAN is numeric-only — narrower than the generic operand-type
                # gate above (which already let DECIMAL/BOOL/temporal through for
                # SUM/AVG/MIN/MAX/STDDEV). Matches the legacy Cython median
                # collectors' restriction exactly (see median_operand_supported).
                _unsupported(f"MEDIAN over a {pt} column — only numeric inputs are "
                             "accepted (CAST DECIMAL to DOUBLE first)")
            fn = {"SUM": "Sum", "AVG": "Avg", "MIN": "Min", "MAX": "Max",
                  "STDDEV": "Stddev", "MEDIAN": "Median", "ANY_VALUE": "AnyValue"}[func]
            specs.append((sc.identity, fn, idx))
        return specs

    # ---- node dispatch --------------------------------------------------------------

    def compile_node(self, nid):
        """Compile the subplan rooted at ``nid``. Returns ``(pipeline_idx, layout)``
        where layout is the identity list of the pipeline's stream, in column order."""
        node = self.plan[nid]
        if len(list(self.plan.outgoing_edges(nid))) > 1:
            _unsupported("a plan node feeding more than one consumer (shared subplan)")
        kind = type(node).__name__

        if getattr(node, "is_scan", False):
            return self._compile_scan(node, kind)

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
                    _unsupported("projecting a column the stream does not carry")
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
                    _unsupported("a GROUP BY key the stream does not carry")
                self._check_key_type(
                    "GROUP BY", group_key_names.get(key_identity) or key_identity,
                    self._layout_type(None, key_identity))
                key_idx.append(layout.index(key_identity))
            # GROUP BY with NO aggregate functions is a DISTINCT over the keys —
            # route to the DistinctSink (emits the distinct key rows unchanged).
            aggs = getattr(node, "aggregates", None) or []
            if not aggs:
                if getattr(node, "_having_condition", None) is not None:
                    _unsupported("a HAVING on a no-aggregate GROUP BY")
                buf = self.nplan.new_buffer()
                self.nplan.set_distinct_sink(p, key_idx, buf)
                p2 = self.nplan.new_pipeline()
                self.nplan.set_buffer_source(p2, buf)
                return p2, list(layout)
            layout = self._project_agg_operands(p, node, layout)
            specs = self._parse_aggregates(aggs, layout)
            buf = self.nplan.new_buffer()
            self.nplan.set_groupby_sink(p, key_idx, group_cols, specs, buf)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            out_layout = list(group_cols) + [spec[0] for spec in specs]
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
                        _unsupported("a DISTINCT ON column the stream does not carry")
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
            self.nplan.set_distinct_sink(p, on_idx, buf)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            return p2, layout

        if kind == "SortNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            spec, layout = self._sort_spec(p, node.order_by, layout)
            buf = self.nplan.new_buffer()
            self.nplan.set_sort_sink(p, spec, buf)
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
            spec, layout = self._sort_spec(p, node.order_by, layout)
            buf = self.nplan.new_buffer()
            self.nplan.set_topn_sink(p, spec, int(limit), buf)
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
            # etc.): project the key expression to a stream column first, then
            # resolve by identity — mirrors GroupedAggregateHashedNode's
            # computed_keys and _sort_spec's `computed` handling above.
            partition_by = list(node.parameters.get("partition_by") or [])
            order_by = list(node.parameters.get("order_by") or [])
            computed = [col for col in partition_by
                        if col.node_type != NodeType.IDENTIFIER]
            computed += [col for col, _asc in order_by
                         if col.node_type != NodeType.IDENTIFIER]
            if computed:
                layout = self._add_computed(p, computed, layout)
            # sort_spec = partition keys (ASC) then order keys (their direction);
            # the WindowSink assigns ranks per partition over that ordering.
            sort_spec = []
            for identity in part_cols:
                if identity not in layout:
                    _unsupported("a PARTITION BY column the stream does not carry")
                self._check_key_type(
                    "PARTITION BY", self._layout_name(identity),
                    self._layout_type(None, identity))
                sort_spec.append((layout.index(identity), True))
            for identity, asc in zip(order_cols, order_asc):
                if identity not in layout:
                    _unsupported("a window ORDER BY column the stream does not carry")
                self._check_key_type(
                    "window ORDER BY", self._layout_name(identity),
                    self._layout_type(None, identity))
                sort_spec.append((layout.index(identity), bool(asc)))
            fn_kinds = [int(k) for k, _out in funcs]
            fn_names = [out for _k, out in funcs]
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
                return p2, list(layout) + list(fn_names)

            self.nplan.set_window_sink(p, sort_spec, len(part_cols),
                                       fn_kinds, fn_names, top_k, buf)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            self.nplan.set_pipeline_dop(p2, 1)   # emits sorted — preserve the order
            return p2, list(layout) + list(fn_names)

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
                _unsupported("an ORDER BY key the stream does not carry")
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
                logical_coerce.append(0)
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
        # only "not lowerable" signal and no try/except is needed.
        filter_bc = None
        if predicates:
            filter_bc = self._lower_bytecode(self._compose_predicate_nodes(predicates))
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
            # does — a GCS scan is latency-bound and wants the wider budget.
            decode_workers=_resolve_var(
                "parquet_gcs_io_workers",
                getattr(scan.properties, "variables", None),
                config.PARQUET_GCS_IO_WORKERS,
            ) if connector_type in ("GCS", "GS") else _resolve_var(
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
        filter_bc = self._lower_bytecode(self._compose_predicate_nodes(predicates))
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
        ) if connector_type in ("GCS", "GS") else _resolve_var(
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
        resolver = Pass1PredResolver(filter_bc, identity_to_physical)
        p1_index_by_name = {name: i for i, name in enumerate(p1_names)}
        pred_col_to_p1 = [p1_index_by_name[n] for n in resolver.col_names]
        # Hand it to rugo as well, so the match runs on the decode workers (in
        # parallel, nogil) for the column shapes rugo can view without a copy. When it
        # declines, LatmatScanSource runs the identical program itself — same ctx,
        # same bytecode, same answer.
        #
        # Not handed over at all when a predicate column is retagged after decode
        # (DATE / TIMESTAMP / DECIMAL) or declared NVARCHAR / VARBINARY: rugo tags its
        # view from the decoded buffers, which for those columns is a DIFFERENT type
        # than the one the predicate is compiled against. See pass1_predicate_gate.
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

    def _compile_scan(self, scan, kind):
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
            scan.compiled_predicate = self._lower_bytecode(
                self._compose_predicate_nodes(scan.predicates)
            )
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
        of any supported type; INNER / LEFT OUTER / SEMI / null-aware ANTI modes).
        The PROBE side is always the streamed side; for LEFT OUTER the plan's
        preserved (left) leg maps to the probe so unmatched rows emit with NULL
        build payload. CROSS = a zero-key inner join (every build row shares one
        empty key → cartesian). nested_loop = an equi-join with a residual `on`
        predicate applied as a post-join filter. full-outer joins fail loud."""
        join_type = getattr(node, "join_type", None)
        if join_type == "asof":
            return self._compile_asof_join(node, in_edges)
        # "left anti null-aware" (NOT IN) and "left anti" (NOT EXISTS / EXCEPT / a full
        # outer's unmatched leg) are DIFFERENT modes — they disagree on NULLs. Mapping
        # both to the null-aware mode made NOT EXISTS emit nothing at all whenever the
        # inner key held a single NULL. See native_join2.hpp's JoinMode comment.
        modes = {"inner": 0, "left outer": 1, "left semi": 2,
                 "left anti null-aware": 3, "left anti": 4,
                 "cross": 0, "nested_loop": 0}
        if join_type not in modes:
            _unsupported(f"a {join_type} join")
        mode = modes[join_type]
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
        filter_residual = getattr(node, "residual", None) if mode in (2, 3, 4) else None

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
                _unsupported("a build-side join key the stream does not carry")
            build_key_idx.append(bkeyout.index(identity))
        ref = self.nplan.new_join2_ref()
        # SEMI/ANTI emit probe rows only — no build payload needed, UNLESS a
        # correlated residual has to read build-side columns to decide existence.
        semi_no_payload = mode in (2, 3, 4) and filter_residual is None
        build_payload = [] if semi_no_payload else list(range(len(blayout)))
        if semi_no_payload:
            build_types, build_logical = [], []
        else:
            build_types, build_logical = self._payload_types(build_id, blayout)
        self.nplan.set_join2_build_sink(bp, build_key_idx, build_payload, ref,
                                        build_types, build_logical)

        pp, playout = self.compile_node(probe_id)
        self.nplan.set_current_identity(node.identity)  # probe op belongs to the join
        self.nplan.set_current_display_name(type(node).__name__)
        pkeyout, probe_keys = self._coerce_join_keys(pp, playout, probe_keys, coercions)
        probe_key_idx = []
        for identity in probe_keys:
            if identity not in pkeyout:
                _unsupported("a probe-side join key the stream does not carry")
            probe_key_idx.append(pkeyout.index(identity))
        probe_payload = list(range(len(playout)))
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
                                                mode, bc, pair_layout)
            return pp, list(playout)          # existence filter — probe stream unchanged
        self.nplan.add_join2_probe(pp, ref, probe_key_idx,
                                   [] if mode in (2, 3, 4) else probe_payload, mode)
        if mode in (2, 3, 4):
            return pp, list(playout)          # existence filter — probe stream unchanged
        # Join2ProbeOperator emits build payload columns first, then probe payload.
        out_layout = list(blayout) + list(playout)
        if residual is not None:
            # nested_loop residual `on` predicate over the combined layout. Lower
            # it (fails loud if not c-native), resolve column refs against the
            # joined stream, and append a filter to the probe pipeline.
            bc = self._lower_expression(residual, "a nested-loop join condition")
            self.nplan.add_expr_filter(pp, bc, out_layout)
        return pp, out_layout

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

    def _coerce_join_keys(self, p, layout, keys, coercions):
        """Append a CAST column for every key in ``keys`` that needs coercing.

        Returns ``(grown_layout, keys)`` where ``keys`` names the columns to hash —
        the synthetic identity where one was minted, the original otherwise. The
        caller keeps the PRE-growth layout for payload/output purposes."""
        if not coercions:
            return layout, keys
        from opteryx.expression import Node, NodeType
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
            cast_node = Node(
                NodeType.CAST,
                value=target_name,
                left=key_node,
                parameters=[],
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
        identity. NULL/empty array rows contribute no rows (INNER unnest semantics).

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
                _unsupported("a CROSS JOIN UNNEST source array the stream does not carry")
        array_idx = layout.index(array_identity)

        # Drop the consumed source array unless something ABOVE the unnest still
        # reads it (`pre_update_columns` is projection_pushdown's liveness set).
        # Dropping matters: a replicated ARRAY column cannot pass through a
        # downstream gather_rows join/sort. Keeping it is required by `SELECT *`.
        # An empty/absent liveness set means "unknown" — keep, never lose a column.
        needed = getattr(node, "pre_update_columns", None) or set()
        drop_source = bool(needed) and array_identity not in needed

        self.nplan.add_unnest(p, array_idx, target_identity, drop_source)
        new_layout = list(layout)
        if drop_source:
            new_layout[array_idx] = target_identity
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

        bp, blayout = self.compile_node(legs["right"])
        build_key_idx = []
        for identity in right_cols:
            if identity not in blayout:
                _unsupported("an ASOF build-side key the stream does not carry")
            build_key_idx.append(blayout.index(identity))
        if asof_right not in blayout:
            _unsupported("an ASOF match column the build stream does not carry")
        ref = self.nplan.new_join2_ref()
        self.nplan.set_current_identity(node.identity)  # own the asof build sink + probe
        self.nplan.set_current_display_name(type(node).__name__)
        build_types, build_logical = self._payload_types(legs["right"], blayout)
        self.nplan.set_asof_build_sink(bp, build_key_idx, list(range(len(blayout))),
                                       blayout.index(asof_right), ref,
                                       build_types, build_logical)

        pp, playout = self.compile_node(legs["left"])
        self.nplan.set_current_identity(node.identity)  # probe op belongs to the join
        self.nplan.set_current_display_name(type(node).__name__)
        probe_key_idx = []
        for identity in left_cols:
            if identity not in playout:
                _unsupported("an ASOF probe-side key the stream does not carry")
            probe_key_idx.append(playout.index(identity))
        if asof_left not in playout:
            _unsupported("an ASOF match column the probe stream does not carry")
        self.nplan.add_asof_probe(pp, ref, probe_key_idx, list(range(len(playout))),
                                  playout.index(asof_left), op_codes[asof_op])
        # AsofProbeOperator emits build payload columns first, then probe payload.
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
        if not layout:
            _unsupported("a zero-column virtual dataset")
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
        """Physical DrakenType (int) + logical tuple for each identity in ``layout``,
        for the native join build sinks (``set_join2_build_sink``/``set_asof_build_sink``):
        the compiler already knows every build-side column's type from binding — same
        source/shape as ``compile_to_native``'s ``final_types``/``final_logical`` — so
        it hands the type down instead of the C++ build sink ever needing to learn it
        from data. That learn-from-first-morsel path never runs when the build side
        genuinely streams zero rows (a filtered-to-empty subquery), which is exactly
        the shape that broke LEFT OUTER's unmatched-row emit."""
        node = self.plan[node_id]
        by_identity = {}
        for col in getattr(node, "columns", None) or []:
            sc = getattr(col, "schema_column", None)
            if sc is not None:
                by_identity[sc.identity] = getattr(sc, "column_type", None)
        cts = getattr(self, "_cts", None) or {}
        types_map = getattr(self, "_types", None) or {}
        types, logical = [], []
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
        return types, logical

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
            _unsupported("an output column the stream does not carry")
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
