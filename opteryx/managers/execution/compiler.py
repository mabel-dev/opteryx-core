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


class _Compiler:
    def __init__(self, plan, nplan):
        self.plan = plan
        self.nplan = nplan

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

    def _lower_expression(self, expr, what):
        from opteryx.compiled.expression.compiled_expression import build_bytecode
        from opteryx.compiled.expression.compiled_expression import lower
        from opteryx.operators._operators import bytecode_is_all_c_native

        bc = build_bytecode(lower(
            self._rewrite_decimal_compares(self._rewrite_between(
                self._rewrite_case(expr)))))
        if not bytecode_is_all_c_native(bc):
            _unsupported(f"{what} outside the c-native kernel set")
        return bc

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
            (p, layout) = self._compile_only_child(in_edges, kind)
            bc = self._lower_expression(node.filter, "a filter predicate")
            self.nplan.add_expr_filter(p, bc, layout)
            return p, layout

        if kind == "ProjectionNode":
            (p, layout) = self._compile_only_child(in_edges, kind)
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
            (p, layout) = self._compile_only_child(in_edges, kind)
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
            (p, layout) = self._compile_only_child(in_edges, kind)
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
            (p, layout) = self._compile_only_child(in_edges, kind)
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
            (p, layout) = self._compile_only_child(in_edges, kind)
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
            (p, layout) = self._compile_only_child(in_edges, kind)
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
            (p, layout) = self._compile_only_child(in_edges, kind)
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
            (p, layout) = self._compile_only_child(in_edges, kind)
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
                self.nplan.add_select(lp, list(range(len(ids))), ids)
                self.nplan.set_buffer_append_sink(lp, buf)
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

    def _compile_only_child(self, in_edges, kind):
        if len(in_edges) != 1:
            _unsupported(f"a {kind} with {len(in_edges)} inputs")
        return self.compile_node(in_edges[0][0])

    def _native_scan_plan(self, scan):
        """Plan-time setup for the zero-Python scan Source (NativeParquetScanSource)
        when this scan is PROVABLY within its increment-1 scope, else None and the
        scan stays on the trampoline Source. This is a static physical-plan choice
        made once here, from schema + footer metadata — whichever Source is built
        is the one that runs; there is no runtime fallback (an unsupported column
        kind reaching the native Source is a gate bug and fails the query loud).

        Increment-1 scope: local files, plain numeric projected columns only
        (schema INT64/FLOAT32/FLOAT64 — parquet int32 widens to INT64 on decode),
        no pushed row-level predicates, no scan-pushed LIMIT/TopN, no zero-column
        projection, and the footer gate (native_scan_supported) proves every
        column of every row group eligible — no schema evolution, no
        DECIMAL/temporal/BOOL/string logical types."""
        from opteryx import config
        from opteryx.connectors.parquet_io.pool_reader import native_scan_supported
        from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

        if not scan.columns:
            return None  # zero-projection COUNT(*) shape (b'*' constant column)
        if getattr(scan, "predicates", None):
            return None  # predicate relocation to an engine filter = next increment
        if getattr(scan, "limit", None) is not None:
            return None  # scan-pushed LIMIT semantics live in the trampoline scan
        if getattr(scan, "_topn_sort_name", None) is not None:
            return None  # scan-fused TopN stays on the trampoline
        manifest = getattr(scan, "manifest", None)
        if manifest is None or manifest.get_file_count() == 0:
            return None
        kinds = []
        for col in scan.columns:
            pt = _physical_type(col.schema_column)
            if pt == DrakenType.INT64:
                kinds.append("int")
            elif pt == DrakenType.FLOAT32:
                kinds.append("float32")
            elif pt == DrakenType.FLOAT64:
                kinds.append("float64")
            else:
                return None
        paths = manifest.get_file_paths()
        names = [col.schema_column.name for col in scan.columns]
        file_sizes = {}
        files = getattr(manifest, "files", None)
        if files:
            for entry in files:
                size = getattr(entry, "file_size_in_bytes", None)
                if isinstance(size, int) and size > 0:
                    file_sizes.setdefault(entry.file_path, size)
        if not native_scan_supported(paths, names, kinds, file_sizes or None):
            return None
        return open_native_scan_plan(
            paths,
            names,
            decode_workers=config.PARQUET_LOCAL_IO_WORKERS,
            predicates=None,
            file_sizes=file_sizes or None,
        )

    def _compile_scan(self, scan, kind):
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
            p = self.nplan.new_pipeline()
            self.nplan.set_native_scan_source(p, splan)
            layout = [col.schema_column.identity for col in scan.columns]
            self._remember_types(scan.columns)
            return p, layout
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
        self.nplan.set_asof_build_sink(bp, build_key_idx, list(range(len(blayout))),
                                       blayout.index(asof_right), ref)

        pp, playout = self.compile_node(legs["left"])
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
    """Compile ``plan`` into a runnable ``(NativePlan, PyMorselQueue)``. Raises
    ``NotSupportedError`` at once — before anything runs — for any shape the native
    engine has no operator for."""
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
    return nplan, out_q


def execute_native(plan, telemetry=None):
    """THE data executor: compile to the native pipeline graph and run it. Returns the
    ``(generator, ResultType)`` contract the cursor consumes. The generator drains the
    engine's output queue; the engine runs on its own native driver + worker pool."""
    from opteryx import config
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.operators._operators import native_plan_execute

    nplan, out_q = compile_to_native(plan)
    dop = resolve_worker_count(config.MAX_EXECUTION_WORKERS)
    if telemetry is not None:
        telemetry._reading["native_engine_engaged"] = 1
        telemetry._reading["native_engine_dop"] = dop

    import threading

    pool = CppThreadPool(dop, "engine")
    errors: list = []
    done = threading.Event()

    def generator():
        handle = native_plan_execute(pool, nplan, dop, out_q, errors, done)
        try:
            while True:
                item = out_q.get()
                if item is None or item is MQ_FINISHED:
                    break
                yield item
        finally:
            # Close first (unblocks any backpressured producer put), then wait for the
            # driver to stop touching the pool before tearing the pool down. Native
            # scan plans (rugo IO pipelines the Source borrows) are only safe to
            # close once the driver — and therefore every engine worker — is done.
            out_q.close()
            done.wait()
            pool.shutdown(wait=True)
            nplan.close_scan_plans()
            del handle
            if errors:
                raise errors[0]

    return generator(), ResultType.TABULAR
