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

from draken.draken_native import DrakenType
from opteryx.constants import ResultType
from opteryx.exceptions import NotSupportedError
from opteryx.expression import NodeType

_MAX_WORKER_CAP = 8
_QUEUE_DEPTH = 4

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


def _physical_type(schema_column):
    ct = getattr(schema_column, "column_type", None)
    return ct.physical if ct is not None else None


# WP-11 logical-coercion packing — mirrors the LC_* enum in
# src/cpp/engine/native_parquet_scan_source.hpp exactly:
#   packed = kind | (unit << 4) | (precision << 8) | (scale << 16)
_LC_DECIMAL64 = 1
_LC_DATE = 3
_LC_TIMESTAMP = 4
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
    def __init__(self, plan, nplan):
        self.plan = plan
        self.nplan = nplan
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

    # ---- expression lowering ------------------------------------------------------
    # Expressions are lowered ONCE, at plan time, to the phase-9 flat bytecode whose
    # compute instructions carry C kernel fn pointers, then resolved against the
    # stream layout (column indices, bind-time literal vectors). Only all-c-native
    # programs are admitted — anything else fails loud here, before execution.

    def _rewrite_between(self, expr):
        """PLAN-TIME tree rewrite: BETWEEN(operand; lower, upper) becomes
        AND(operand >= lower, operand <= upper) (bounds' inclusivity respected).
        The optimizer produces BETWEEN from exactly this shape; un-rewriting it
        lets the c-native ordinal-compare path run it — BC_BETWEEN itself carries
        runtime PyObject bounds and can never be nogil. Non-mutating: parents of
        a changed child are rebuilt via Node.copy()."""
        from opteryx.compiled.structures.node import Node

        if not isinstance(expr, Node):
            return expr
        if expr.node_type == NodeType.BETWEEN:
            lower_incl, upper_incl = expr.value
            operand = self._rewrite_between(expr.left)
            lo = Node(NodeType.COMPARISON_OPERATOR,
                      value=("GtEq" if lower_incl else "Gt"))
            lo.left = operand
            lo.right = expr.right      # the lower-bound literal node, reused as-is
            hi = Node(NodeType.COMPARISON_OPERATOR,
                      value=("LtEq" if upper_incl else "Lt"))
            hi.left = operand
            hi.right = expr.centre     # the upper-bound literal node
            both = Node(NodeType.AND)
            both.left = lo
            both.right = hi
            return both
        rebuilt = None
        for attr in ("left", "right", "centre"):
            child = getattr(expr, attr)
            if isinstance(child, Node):
                new_child = self._rewrite_between(child)
                if new_child is not child:
                    if rebuilt is None:
                        rebuilt = expr.copy()
                    setattr(rebuilt, attr, new_child)
        params = expr.parameters
        if isinstance(params, list):
            new_params = [self._rewrite_between(c) if isinstance(c, Node) else c
                          for c in params]
            if any(a is not b for a, b in zip(new_params, params)):
                if rebuilt is None:
                    rebuilt = expr.copy()
                rebuilt.parameters = new_params
        return rebuilt if rebuilt is not None else expr

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
                # Decimal(float) is the EXACT binary rational — never str(), the
                # shortest repr can sit on the other side of a scale gridline.
                q = _dec.Decimal(v) if isinstance(v, float) else _dec.Decimal(str(v))
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
                nl = Node(NodeType.LITERAL, value=rescaled)
                nl.type = ct
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
        from opteryx.operators._operators import bytecode_is_all_c_native

        bc = self._lower_bytecode(expr)
        if not bytecode_is_all_c_native(bc):
            _unsupported(f"{what} outside the c-native kernel set")
        return bc

    def _compose_predicate_nodes(self, predicates):
        """AND-compose a list of pushed predicate nodes into one right-leaning
        tree — the VERBATIM composition the trampoline scan uses to build
        `_compiled_predicate` (opteryx/operators/parquet_read/parquet_read.pyx's
        `_compose_predicates`). Lowering this same tree is what keeps the relocated
        native filter byte-identical to the trampoline path. `predicates` is
        non-empty (the caller guards)."""
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

    def _add_computed(self, p, eval_nodes, layout):
        """Append one ExprProject per computed expression (bind order preserved —
        later programs may reference earlier outputs). DECIMAL/TIMESTAMP results
        get their plan-declared logical descriptor re-attached natively at the
        operator boundary. Returns the grown layout."""
        from opteryx.expression.evaluator import compile_eval_nodes
        from opteryx.operators._operators import bytecode_ops_all_c_native

        # Same plan-time tree rewrites the filter path gets (CASE→IF_THEN_ELSE,
        # BETWEEN→compares, decimal literal rescale) — applied BEFORE lowering.
        eval_nodes = [self._rewrite_decimal_compares(self._rewrite_between(
            self._rewrite_case(node_))) for node_ in eval_nodes]

        ct_by_identity = {}
        for node_ in eval_nodes:
            sc = getattr(node_, "schema_column", None)
            if sc is not None and sc.identity is not None:
                ct_by_identity[sc.identity] = getattr(sc, "column_type", None)

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
                _unsupported("a computed expression outside the c-native kernel set")
            logical = None
            ct = ct_by_identity.get(identity)
            if ct is not None and ct.logical is not None:
                lg = ct.logical
                logical = (int(lg.kind.value), int(getattr(lg.unit, "value", 0)),
                           int(lg.precision), int(lg.scale))
            if ct is not None:
                self._types = getattr(self, "_types", None) or {}
                self._types[identity] = ct.physical
            self.nplan.add_expr_project(p, bc, layout, identity, logical)
            layout.append(identity)
        return layout

    # ---- aggregate parsing --------------------------------------------------------

    _AGG_FNS = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
    _AGG_OPERAND_TYPES = _NUMERIC_TYPES + (
        DrakenType.DECIMAL, DrakenType.DECIMAL128, DrakenType.DATE32,
        DrakenType.TIMESTAMP64, DrakenType.TIME32, DrakenType.TIME64,
        DrakenType.BOOL,
    )

    def _project_agg_operands(self, p, node, layout):
        """Aggregate operands that are computed expressions (SUM(a * b)) become
        ExprProject columns first; the sink then aggregates a plain column."""
        computed = []
        for agg in getattr(node, "aggregates", None) or []:
            params = getattr(agg, "parameters", None) or []
            if len(params) != 1:
                continue
            operand = params[0]
            if operand.node_type in (NodeType.WILDCARD, NodeType.IDENTIFIER):
                continue
            sc = getattr(operand, "schema_column", None)
            if sc is not None and sc.identity is not None and sc.identity not in layout:
                computed.append(operand)
        if computed:
            layout = self._add_computed(p, computed, layout)
        return layout

    def _parse_aggregates(self, aggs, layout):
        """Any mix of COUNT(*) / COUNT(col) / SUM / AVG / MIN / MAX over plain
        columns. Returns [(identity, fn, operand_idx | -1), ...] in output order."""
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
            if len(params) != 1:
                _unsupported(f"{func} with {len(params)} parameters")
            operand = params[0]
            distinct = getattr(agg, "duplicate_treatment", None) == "Distinct"
            if distinct and func != "COUNT":
                _unsupported(f"{func}(DISTINCT ...)")
            if func == "COUNT" and operand.node_type == NodeType.WILDCARD:
                if distinct:
                    _unsupported("COUNT(DISTINCT *)")
                specs.append((sc.identity, "CountStar", -1))
                continue
            psc = getattr(operand, "schema_column", None)
            if psc is None:
                _unsupported(f"{func} over an unbound operand")
            if psc.identity not in layout:
                _unsupported(f"{func} over a column the stream does not carry")
            idx = layout.index(psc.identity)
            pt = _physical_type(psc)
            if func == "COUNT":
                # COUNT(DISTINCT col) dedups on serialized VALUE bytes in the
                # sinks — silently lowering it as plain COUNT was a wrong answer.
                specs.append((sc.identity, "CountDistinct" if distinct else "Count", idx))
                continue
            if pt not in self._AGG_OPERAND_TYPES:
                # MIN/MAX over strings: the sinks keep a parallel byte-lexicographic
                # extreme (agg2_update_str) — SUM/AVG over strings stays rejected.
                _string_minmax = func in ("MIN", "MAX") and pt in (
                    DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY)
                if not _string_minmax:
                    _unsupported(f"{func} over a {pt} column")
            fn = {"SUM": "Sum", "AVG": "Avg", "MIN": "Min", "MAX": "Max"}[func]
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
            bc = self._lower_expression(node.filter, "a filter predicate")
            const_col_idx, const_scalar_vecs = self._resolve_const_replacements(node, layout)
            self.nplan.add_expr_filter(p, bc, layout, const_col_idx, const_scalar_vecs)
            return p, layout

        if kind == "ProjectionNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            # Computed expressions come from the FULL projection list — the SELECT
            # columns plus any ORDER BY keys the planner routed through this node
            # (mirrors ProjectionNode.__init__'s own eval-node derivation).
            proj_exprs = list(node.parameters.get("projection") or []) + list(
                node.parameters.get("order_by_columns") or []
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
            specs = self._parse_aggregates(getattr(node, "aggregates", None) or [], layout)
            buf = self.nplan.new_buffer()
            self.nplan.set_agg_sink(p, specs, buf)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            out_layout = [identity for identity, _fn, _i in specs]
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
            for grp in getattr(node, "groups", None) or []:
                if getattr(grp, "node_type", None) in (None, NodeType.IDENTIFIER,
                                                       NodeType.WILDCARD):
                    continue
                sc = getattr(grp, "schema_column", None)
                if sc is not None and sc.identity is not None and sc.identity not in layout:
                    computed_keys.append(grp)
            if computed_keys:
                layout = self._add_computed(p, computed_keys, layout)
            key_idx = []
            for key_identity in group_cols:
                if key_identity not in layout:
                    _unsupported("a GROUP BY key the stream does not carry")
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
            self.nplan.set_groupby_sink(p, key_idx, specs, buf)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            out_layout = list(group_cols) + [identity for identity, _fn, _i in specs]
            self._apply_having(p2, node, out_layout)
            return p2, out_layout

        if kind == "DistinctNode":
            (p, layout) = self._compile_only_child(in_edges, kind, node)
            on = getattr(node, "_distinct_on", None)
            on_idx = []
            if on:
                for identity in on:
                    if identity not in layout:
                        _unsupported("a DISTINCT ON column the stream does not carry")
                    on_idx.append(layout.index(identity))
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
                sort_spec.append((layout.index(identity), True))
            for identity, asc in zip(order_cols, order_asc):
                if identity not in layout:
                    _unsupported("a window ORDER BY column the stream does not carry")
                sort_spec.append((layout.index(identity), bool(asc)))
            fn_kinds = [int(k) for k, _out in funcs]
            fn_names = [out for _k, out in funcs]
            buf = self.nplan.new_buffer()
            self.nplan.set_window_sink(p, sort_spec, len(part_cols),
                                       fn_kinds, fn_names, buf)
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
                self.nplan.add_select(lp, list(range(len(ids))), ids)
                self.nplan.set_buffer_append_sink(lp, buf)
            self.nplan.set_current_identity(node.identity)
            p2 = self.nplan.new_pipeline()
            self.nplan.set_buffer_source(p2, buf)
            return p2, ids

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
        bc = self._lower_expression(having, "a HAVING predicate")
        self.nplan.add_expr_filter(p, bc, layout)

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
            spec.append((layout.index(identity), bool(ascending)))
        return spec, layout

    def _compile_only_child(self, in_edges, kind, node):
        if len(in_edges) != 1:
            _unsupported(f"a {kind} with {len(in_edges)} inputs")
        result = self.compile_node(in_edges[0][0])
        # The child's own compile stamped ITS identity as current; restore this node's
        # so the operators/sink this branch is about to build are attributed here.
        self.nplan.set_current_identity(node.identity)
        return result

    def _native_scan_plan(self, scan):
        """Plan-time setup for the zero-Python scan Source (NativeParquetScanSource)
        when this scan is PROVABLY within its increment-1 scope, else None and the
        scan stays on the trampoline Source. This is a static physical-plan choice
        made once here, from schema + footer metadata — whichever Source is built
        is the one that runs; there is no runtime fallback (an unsupported column
        kind reaching the native Source is a gate bug and fails the query loud).

        Scope: local files; columns that are numeric (schema INT64/FLOAT32/FLOAT64 —
        parquet int32 widens to INT64 on decode) or string (VARCHAR/NVARCHAR/
        VARBINARY, decoded natively via the DK_VARCHAR / DK_VARCHAR_DICT /
        DK_POOL-string paths — WP-01); no scan-pushed LIMIT/TopN, no zero-column
        projection; and the footer gate (native_scan_supported) proves every column
        of every row group eligible — no schema evolution, no DECIMAL/temporal/BOOL
        logical types.

        WP-02 — pushed predicates: the per-row residual is RELOCATED to a native
        downstream ExprFilter (see `_compile_scan`) instead of blocking admission.
        The scan reads the READ-SET (projected ∪ predicate-input columns) so a
        role-3 filter-only column is decoded and available to the filter, and
        EMITS only the projection (via a trailing Select when read-set ⊋ emit-set).
        Row-group / bloom PRUNING stays at the scan — the same
        `extract_predicate_stats` triples the trampoline path uses are passed to
        `open_native_scan_plan`, so bytes-read / row-groups-scanned are unchanged.
        A predicate that does not lower to a c-native span fails CLOSED (returns
        None → trampoline Source keeps the predicate on the old path)."""
        from opteryx import config
        from opteryx.connectors.parquet_io.pool_reader import native_scan_supported
        from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan
        from opteryx.connectors.parquet_io.predicates import extract_predicate_stats
        from opteryx.expression import get_all_nodes_of_type
        from opteryx.operators._operators import bytecode_is_all_c_native

        if not scan.columns:
            return None  # zero-projection COUNT(*) shape (b'*' constant column)
        if getattr(scan, "limit", None) is not None:
            return None  # scan-pushed LIMIT semantics live in the trampoline scan
        if getattr(scan, "_topn_sort_name", None) is not None:
            return None  # scan-fused TopN stays on the trampoline
        manifest = getattr(scan, "manifest", None)
        if manifest is None or manifest.get_file_count() == 0:
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
                    # WP-11 fail-closed: a BOOL column used as a predicate input is not
                    # safely evaluable by the relocated c-native ExprFilter (bool
                    # comparison raises err_op=11), even though bytecode_is_all_c_native
                    # reports it lowerable. Rather than relocate and crash, fail the
                    # whole scan closed so the predicate stays on the trampoline (which
                    # evaluates it correctly). BOOL columns that are only PROJECTED (not
                    # a predicate input) are unaffected — they decode natively. A native
                    # c-native bool comparison kernel is a follow-on.
                    if _physical_type(sc) == DrakenType.BOOL:
                        return None
                    if sc.identity not in seen:
                        seen.add(sc.identity)
                        read_scs.append(sc)

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
            if pt == DrakenType.INT64 or pt == DrakenType.TIME32 or pt == DrakenType.TIME64:
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
                # A read-set column (projected OR role-3 filter-only) of a
                # not-yet-admissible kind fails the whole scan closed. Deliberate
                # strict check: role-3 columns must also be native-admissible.
                return None
        paths = manifest.get_file_paths()
        names = [sc.name for sc in read_scs]
        file_sizes = {}
        files = getattr(manifest, "files", None)
        if files:
            for entry in files:
                size = getattr(entry, "file_size_in_bytes", None)
                if isinstance(size, int) and size > 0:
                    file_sizes.setdefault(entry.file_path, size)
        if not native_scan_supported(paths, names, kinds, file_sizes or None):
            return None
        # Pruning triples — identical to the trampoline path's `_sp_predicate_stats`
        # so row groups excluded / bytes read are unchanged. Only pruning; the
        # per-row residual is the relocated ExprFilter, not the scan.
        pruning = extract_predicate_stats(predicates) if predicates else None
        splan = open_native_scan_plan(
            paths,
            names,
            decode_workers=config.PARQUET_LOCAL_IO_WORKERS,
            predicates=pruning or None,
            file_sizes=file_sizes or None,
            string_types=string_types,
            decimal_columns=decimal_columns,
            logical_coerce=logical_coerce,
        )

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

    def _compile_scan(self, scan, kind):
        # Tag the scan Source (and any materialized buffer source) with the scan node's
        # identity so its per-operator readings attribute back to the ReadRel node.
        self.nplan.set_current_identity(scan.identity)
        # ReaderNode = the generic non-parquet connector scan ($planets and the other
        # sample/virtual/in-memory relations). Its content is fully read either way
        # (no native streaming exists for it); materializing at plan time keeps
        # execution 100%% native.
        if kind in ("FunctionDatasetNode", "NullReaderNode", "ReaderNode"):
            return self._compile_materialized_source(scan)
        if kind != "ParquetReadNode":
            _unsupported(f"the {kind} source")
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
                # Read-set width (projected ∪ role-3 filter-only), not just the
                # projection — that is what the native Source actually decodes.
                "columns_read": len(reloc[1]) if reloc is not None else len(scan.columns),
            }
            p = self.nplan.new_pipeline()
            self.nplan.set_native_scan_source(p, splan)
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
        modes = {"inner": 0, "left outer": 1, "left semi": 2,
                 "left anti null-aware": 3, "left anti": 3,
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
        if is_cross:
            # No keys: build the RIGHT leg (typically the small/scalar side, e.g.
            # a single-row aggregate) and probe with the LEFT.
            left_cols, right_cols = [], []
        elif not left_cols or len(left_cols) != len(right_cols):
            _unsupported("a join without aligned key lists")

        # A nested_loop node carries an `on` COMPARISON referencing BOTH legs — it
        # cannot be a pre-scan filter. Applied as a post-join filter over the
        # combined layout below (fails loud if not c-native).
        residual = getattr(node, "on", None) if join_type == "nested_loop" else None

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

        bp, blayout = self.compile_node(build_id)
        self.nplan.set_current_identity(node.identity)  # own the build sink + probe below
        build_key_idx = []
        for identity in build_keys:
            if identity not in blayout:
                _unsupported("a build-side join key the stream does not carry")
            build_key_idx.append(blayout.index(identity))
        ref = self.nplan.new_join2_ref()
        # SEMI/ANTI emit probe rows only — no build payload needed.
        build_payload = [] if mode in (2, 3) else list(range(len(blayout)))
        self.nplan.set_join2_build_sink(bp, build_key_idx, build_payload, ref)

        pp, playout = self.compile_node(probe_id)
        self.nplan.set_current_identity(node.identity)  # probe op belongs to the join
        probe_key_idx = []
        for identity in probe_keys:
            if identity not in playout:
                _unsupported("a probe-side join key the stream does not carry")
            probe_key_idx.append(playout.index(identity))
        probe_payload = list(range(len(playout)))
        self.nplan.add_join2_probe(pp, ref, probe_key_idx,
                                   [] if mode in (2, 3) else probe_payload, mode)
        if mode in (2, 3):
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
        self.nplan.set_asof_build_sink(bp, build_key_idx, list(range(len(blayout))),
                                       blayout.index(asof_right), ref)

        pp, playout = self.compile_node(legs["left"])
        self.nplan.set_current_identity(node.identity)  # probe op belongs to the join
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
        for col in columns or []:
            sc = col.schema_column
            pt = _physical_type(sc)
            if pt is not None:
                types[sc.identity] = pt
            ct = getattr(sc, "column_type", None)
            if ct is not None:
                cts[sc.identity] = ct

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


def compile_to_native(plan):
    """Compile ``plan`` into a runnable ``(NativePlan, PyMorselQueue, scan_sources)``.
    ``scan_sources`` maps each parquet scan node identity to the Source it was wired
    to ("NativeParquetScanSource" or "StreamingScanSource") — WP-INSTR instrument 2.
    Raises ``NotSupportedError`` at once — before anything runs — for any shape the
    native engine has no operator for."""
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
    compiler = _Compiler(plan, nplan)

    in_edges = list(plan.ingoing_edges(exit_id))
    if len(in_edges) != 1:
        _unsupported(f"an Exit with {len(in_edges)} inputs")
    p, layout = compiler.compile_node(in_edges[0][0])
    nplan.set_current_identity(exit_node.identity)  # exit select + queue sink

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
    for col in exit_node.columns:
        pt = _physical_type(col.schema_column)
        final_types.append(pt.value if pt is not None else DrakenType.VARCHAR.value)
    nplan.set_final_schema(list(exit_node.final_names), final_types)
    return nplan, out_q, compiler.scan_sources, compiler.scan_facts


def execute_native(plan, telemetry=None):
    """THE data executor: compile to the native pipeline graph and run it. Returns the
    ``(generator, ResultType)`` contract the cursor consumes. The generator drains the
    engine's output queue; the engine runs on its own native driver + worker pool."""
    from opteryx import config
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.operators._operators import native_plan_execute

    import time as _t

    # Native plan compilation (the _Compiler walk + bytecode lowering + operator
    # instantiation) runs synchronously here, in the execution phase but BEFORE the
    # driver generator produces anything — so it is inside time_executing yet
    # invisible to time_engine_generator_total. Cost is ~independent of row count,
    # so it dominates cheap queries. Timed as an always-on driver span.
    _compile0 = _t.perf_counter_ns()
    nplan, out_q, scan_sources, scan_facts = compile_to_native(plan)
    _compile_ns = _t.perf_counter_ns() - _compile0
    dop = resolve_worker_count(config.MAX_EXECUTION_WORKERS)
    # WP-INSTR instrument 2: which Source each parquet scan selected (a plan-time
    # fact — recorded whether or not the GIL instrumentation is armed; the dict is
    # tiny and costs nothing to attach).
    if telemetry is not None:
        telemetry._reading["native_engine_engaged"] = 1
        telemetry._reading["native_engine_dop"] = dop
        telemetry._reading["scan_sources"] = dict(scan_sources)
        # Native-scan plan-time facts (files/row-groups/columns read), keyed by
        # scan identity — overlaid onto the scan's sensor row by mermaid.py to
        # replace the always-zero ScanReadings fields on the native path.
        if scan_facts:
            telemetry._reading["native_scan_facts"] = dict(scan_facts)

    # WP-INSTR instruments 1 & 4: arm the execution-time GIL instrumentation for the
    # span of this run when the config flag is set. Disarmed by default → the
    # instrumented sites pay a single-branch check and nothing else.
    instrument_gil = bool(config.OPTERYX_INSTRUMENT_ENGINE)

    import threading

    # A fresh thread pool is spawned PER QUERY — dop OS threads created here, on the
    # calling thread, before the driver runs. Another fixed per-query cost inside
    # time_executing but outside time_engine_generator_total.
    _pool0 = _t.perf_counter_ns()
    pool = CppThreadPool(dop, "engine")
    _pool_create_ns = _t.perf_counter_ns() - _pool0
    errors: list = []
    done = threading.Event()

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

        if instrument_gil:
            from opteryx.operators._operators import instr_gil_reset
            from opteryx.operators._operators import instr_gil_set_enabled

            # Arm BEFORE the driver submits so every worker sees the flag set.
            instr_gil_reset()
            instr_gil_set_enabled(True)
        _t0 = _t.perf_counter_ns()
        handle = native_plan_execute(pool, nplan, dop, out_q, errors, done)
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
            done.wait()
            _done_wait_ns = _t.perf_counter_ns() - _tw0
            # Trampoline scans (StreamingScanSource) accumulate ScanReadings during
            # next_morsel but only flush them into node.readings in close_source() —
            # which the native engine's pull loop never calls (it just detects EOS).
            # The driver is done (done.wait returned → every worker finished), so it
            # is now safe to close each scan on this thread: flush_into populates the
            # readings sensors()/mermaid read, and the source is released. Idempotent
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
            # ``operations`` breakdown). Several native operators can share one identity
            # (a plan node lowered to multiple operators, operator fusion) — sum them.
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
                        }
                    else:
                        agg["records_in"] += row["records_in"]
                        agg["records_out"] += row["records_out"]
                        agg["bytes_in"] += row["bytes_in"]
                        agg["bytes_out"] += row["bytes_out"]
                        agg["calls"] += row["calls"]
                        agg["execution_time"] += row["execution_time"]
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
            _ts0 = _t.perf_counter_ns()
            pool.shutdown(wait=True)
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

            if errors:
                raise errors[0]

    return generator(), ResultType.TABULAR
