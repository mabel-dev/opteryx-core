# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Groupjoin

Type: Cost-based (reads leg statistics for the recorded decision; `optimization_technique = "cost"`)
Goal: Remove the pair materialisation between a join and the aggregate it feeds

Fuses an equi-join with the aggregation that consumes it into one native operator
(src/cpp/engine/native_groupjoin.hpp; docs/GROUPJOIN_DESIGN.md). Two shapes, both
ANNOTATIONS only — the plan's nodes and edges are unchanged and the compiler lowers
the annotated pair:

ORGANIC  `Γ_G;F(R ⋈ S)` / `Γ_G;F(R ⟕ S)` where G is exactly R's join-key column(s)
         and every aggregate reads S. Accumulators live per distinct build key and the
         emit multiplies duplicate-sensitive partials by the key's build multiplicity
         (Yan–Larson F⊗c), so the result is exact with no GROUP BY above and no
         uniqueness proof. Annotated on the AggregateAndGroup.

OUTER LEG AS BUILD  `O ⋈_k Π*(Γ_k;F(S [⋉ reducer]))` (INNER) — a join to a relation grouped on
         exactly its join key (what scalar decorrelation produces). The outer leg O is
         built and S streams past it; each O row is emitted with its key's aggregate,
         so the reducer copy, the GROUP BY and the join back all disappear. A key
         reducer is dropped because it can never change the answer (it removes only S
         rows whose key no O row holds). Annotated on the Join.

Only the plain row-kind aggregates are fused (COUNT(*), COUNT, SUM, AVG, MIN, MAX,
ANY_VALUE over fixed-width numeric/temporal operands, no DISTINCT). Anything else
keeps today's plan — a planning decision, never a runtime fallback.

Order:
    After:  ProjectFusion (requires project-fused: Subquery/Project boundary nodes
            between an aggregate and its join are gone), PredicatePushdown (requires
            predicates-pushed), JoinPlanning, JoinAlgorithm and SemiJoinReducer (the
            join order, build sides and any reducer are final before the pair is
            judged).
    Before: nothing depends on it; annotation only, so VectorSearch and the
            scan-absorbing pushdowns that follow see an unchanged plan.
    Declares: requires project-fused, predicates-pushed.

Rewrite:
    SELECT r.k, SUM(s.v) FROM r JOIN s ON r.k = s.k GROUP BY r.k
    ->
    the same plan; the AggregateAndGroup is annotated and the compiler lowers the
    join and the aggregate as ONE native groupjoin operator (no pair materialisation).

    SELECT o.*, agg FROM o JOIN (SELECT k, SUM(v) AS agg FROM s GROUP BY k) g ON o.k = g.k
    ->
    the same plan; the Join is annotated: o is built, s streams past it and each o
    row is emitted with its key's aggregate (the GROUP BY, the reducer copy and the
    join back disappear at compile time).
"""

from typing import Optional

from draken.draken_native import DrakenType

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import PlanStep

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

_GJ_FUNCS = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX", "ANY_VALUE"})

# Operand types the native accumulator admits (gb_kind_row_resolvable kinds). Wider
# kinds (DECIMAL128, strings, UUID) keep today's plan.
_GJ_OPERAND_TYPES = frozenset(
    {
        DrakenType.INT8, DrakenType.INT16, DrakenType.INT32, DrakenType.INT64,
        DrakenType.UINT8, DrakenType.UINT16, DrakenType.UINT32, DrakenType.UINT64,
        DrakenType.FLOAT32, DrakenType.FLOAT64, DrakenType.DECIMAL,
        DrakenType.DATE32, DrakenType.TIMESTAMP64, DrakenType.TIME32, DrakenType.TIME64,
        DrakenType.BOOL,
    }
)

# COST GATE (docs/GROUPJOIN_DESIGN.md §13). An EXPRESSION operand is evaluated only on
# streamed rows that match (the compiler puts a SEMI probe against the same build table
# ahead of the operand projection), so it costs what the join + GROUP BY plan pays.
# Before that, it ran on every streamed row: TPC-H Q12 measured 1.237x slower fused.
# The groupjoin pins R (the side holding the group columns) as the BUILD. Declined when
# that leg is estimated more than this many times the streamed leg — building the far
# larger relation is what the join's own build-side choice exists to avoid.
_BUILD_RATIO = 4.0
# Expression operands are pre-filtered to matched rows unless the join is estimated to
# keep at least this fraction of the streamed rows.
_PREFILTER_KEEP_RATIO = 0.5
# Outer-leg build with no copy to remove is declined when the aggregate is estimated to
# fold more than this many input rows per group (its GROUP BY output is then tiny and
# the replaced plan's probe of it nearly free).
_FEW_GROUPS_RATIO = 100
# PER-ROW emit pays only by shrinking the GROUP BY's input from the join's pairs to the
# matched build rows and by skipping the per-pair gather of the group columns. Two
# shapes cannot repay its overhead (the extra SEMI probe for an expression operand, and
# per-worker lanes merged over the whole build), measured TPC-H SF10 at 4 workers:
#   * the STREAMED join key is (near) unique, so every build row matches at most one
#     streamed row and the GROUP BY input cannot shrink — Q12 (o_orderkey) 1.038x;
#   * every group column is fixed-width, so the pair gather and the GROUP BY are
#     already cheap — Q3 (orderkey, date, int) 1.012x, 1.070x at 16 workers.
# Q10 and Q18 (string group columns, streamed key ~4 rows per value) win 0.886x/0.974x.
# Both tests read statistics as COST inputs only — never as a uniqueness proof.
# The losses are inherent to the per-row shape, not the per-worker lane merge: profiled
# on Q3, the probe+accumulate pipeline costs the same as the join's; the extra is the
# emit walking all 1.47M build entries for ~140K matched rows plus the GROUP BY above,
# against a 0.1 ms GROUP BY over 302K pairs. (A touched-slot merge was tried and
# changed nothing for Q3/Q12, so shared owner-tagged lanes would not either.)
_UNIQUE_STREAM_KEY = 0.9   # streamed key distinct / streamed rows at or above = unique
_VARIABLE_WIDTH = frozenset({DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY})

# Joins whose output is a pair row (the ones a groupjoin can replace).
_PAIR_JOINS = ("inner", "left outer", "nested loop")


def _conjuncts(node) -> list:
    if node is None:
        return []
    if node.node_type == NodeType.AND:
        return _conjuncts(node.left) + _conjuncts(node.right)
    return [node]


def _keyed_only(join) -> bool:
    """Every ON conjunct is a key equality (no residual for the pair to answer)."""
    keys = list(join.left_columns or [])
    return bool(keys) and len(keys) == len(join.right_columns or []) and len(
        _conjuncts(join.on)
    ) == len(keys)


def _leg_of(identifier, left_names, right_names) -> Optional[str]:
    if identifier.source in left_names:
        return "left"
    if identifier.source in right_names:
        return "right"
    return None


def _aggregates_eligible(
    aggregates, operand_leg, left_names, right_names, allow_avg=True
) -> Optional[str]:
    """None when every aggregate can be fused reading only `operand_leg`; otherwise
    the reason it cannot."""
    if not aggregates:
        return "no aggregate functions"
    for agg in aggregates:
        if agg.node_type != NodeType.AGGREGATOR or agg.value not in _GJ_FUNCS:
            return f"aggregate {agg.value} is not fusable"
        if agg.duplicate_treatment == "Distinct":
            return "DISTINCT aggregate"
        params = agg.parameters or []
        if len(params) != 1:
            return f"{agg.value} with {len(params)} parameters"
        operand = params[0]
        if agg.value == "COUNT" and operand.node_type == NodeType.WILDCARD:
            continue
        if operand.schema_column is None:
            return f"{agg.value} over an unbound operand"
        if operand_leg is not None:
            # Every column the operand reads must come from the streamed leg. `.source`
            # is the alias the column was written with — what the join's relation
            # names (and extract_join_fields) use.
            refs = (
                [operand]
                if operand.node_type == NodeType.IDENTIFIER
                else get_all_nodes_of_type(operand, (NodeType.IDENTIFIER,))
            )
            if not refs:
                return f"{agg.value} over a constant"
            for ref in refs:
                if _leg_of(ref, left_names, right_names) != operand_leg:
                    return f"{agg.value} reads the build side"
        elif operand.node_type != NodeType.IDENTIFIER:
            return f"{agg.value} over an expression"
        if agg.value == "AVG" and not allow_avg:
            return "AVG under per-row emit (needs a sum/count pair above)"
        if agg.value != "COUNT":
            ct = operand.schema_column.column_type
            physical = ct.physical if ct is not None else None
            # DECIMAL128 has exact SUM/AVG lanes; its MIN/MAX lane is not fused.
            fusable = physical in _GJ_OPERAND_TYPES or (
                physical == DrakenType.DECIMAL128 and agg.value in ("SUM", "AVG")
            )
            if not fusable:
                return f"{agg.value} operand type is not fusable"
    return None


class GroupJoinStrategy(OptimizationStrategy):
    optimization_technique = "cost"
    requires = ("project-fused", "predicates-pushed")

    def should_i_run(self, plan: LogicalPlan) -> bool:
        has_agg = False
        has_join = False
        for _nid, node in plan.nodes(True):
            if node.node_type == LogicalPlanStepType.AggregateAndGroup:
                has_agg = True
            elif node.node_type == LogicalPlanStepType.Join and node.type in _PAIR_JOINS:
                has_join = True
        return has_agg and has_join

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        self._store = context.plan_context.statistics
        joins = [
            nid
            for nid, node in plan.nodes(True)
            if node.node_type == LogicalPlanStepType.Join and node.type in _PAIR_JOINS
        ]
        for join_nid in joins:
            self._outer_leg_as_build(plan, join_nid)
        aggregates = [
            nid
            for nid, node in plan.nodes(True)
            if node.node_type == LogicalPlanStepType.AggregateAndGroup
        ]
        for agg_nid in aggregates:
            self._organic(plan, agg_nid)
        return plan

    # ---- organic: Γ over a join, grouped on the build side's join key ---------------

    def _organic(self, plan: LogicalPlan, agg_nid: str) -> None:
        agg = plan[agg_nid]
        if agg.grouping_set_identities is not None or agg.groupjoin is not None:
            return
        providers = [src for src, _t, _r in plan.ingoing_edges(agg_nid)]
        if len(providers) != 1:
            return
        join_nid = providers[0]
        join = plan[join_nid]
        if join.node_type != LogicalPlanStepType.Join or join.type not in ("inner", "left outer"):
            return
        label = "groupjoin"
        if join.groupjoin is not None:
            return
        if not _keyed_only(join) or join.using_merged:
            self.record_decision(label, "declined: join has a non-key ON condition")
            return
        left_names = join.left_relation_names or []
        right_names = join.right_relation_names or []
        groups = list(agg.groups or [])
        if not groups or any(
            g.node_type != NodeType.IDENTIFIER or g.schema_column is None for g in groups
        ):
            self.record_decision(label, "declined: GROUP BY is not plain columns")
            return
        group_ids = [g.schema_column.identity for g in groups]
        left_keys = list(join.left_columns)
        right_keys = list(join.right_columns)
        # EMIT PER KEY (x d): the GROUP BY is exactly R's join key — no GROUP BY above.
        # LEFT OUTER: only the preserved left leg may be R (the streamed leg's key is
        # NULL on padded rows, so it cannot stand for the group).
        build = None
        per_key = False
        if sorted(group_ids) == sorted(left_keys) and len(group_ids) == len(left_keys):
            build, per_key = "left", True
        elif join.type == "inner" and sorted(group_ids) == sorted(right_keys) and len(
            group_ids
        ) == len(right_keys):
            build, per_key = "right", True
        # A NULL-keyed preserved row joins nothing and is one group per distinct key
        # tuple; per-key emit collapses every NULL-keyed row into ONE group, which is
        # only that answer for a single key column. Per-row emit handles it (the GROUP
        # BY above groups the NULL tuples by value).
        if per_key and join.type == "left outer" and len(left_keys) != 1:
            per_key = False
            build = None
        # EMIT PER ROW + GROUP BY above: every group column comes from one leg (for
        # INNER, a streamed join-key column stands for its build-side equal — equal on
        # every joined row). Γ_G;F(R ⋈ S) = Γ_G;F²(per-build-row F¹), M&N Eq. 3: grouping
        # by the build row is finer than any G drawn from R.
        alias = {}
        if build is None:
            legs = [_leg_of(g, left_names, right_names) for g in groups]
            for candidate in (("left", "right") if join.type == "inner" else ("left",)):
                build_keys = left_keys if candidate == "left" else right_keys
                stream_keys = right_keys if candidate == "left" else left_keys
                ok = True
                cand_alias = {}
                for g, leg in zip(groups, legs):
                    if leg == candidate:
                        continue
                    sid = g.schema_column.identity
                    if join.type == "inner" and sid in stream_keys:
                        cand_alias[sid] = build_keys[stream_keys.index(sid)]
                        continue
                    ok = False
                    break
                if ok:
                    build, alias = candidate, cand_alias
                    break
        if build is None:
            self.record_decision(label, "declined: GROUP BY mixes both sides of the join")
            return
        stream = "right" if build == "left" else "left"
        reason = _aggregates_eligible(
            agg.aggregates, stream, left_names, right_names, allow_avg=per_key
        )
        if reason is not None:
            self.record_decision(label, f"declined: {reason}")
            return
        # The groupjoin builds R; the join planner already chose which leg to build on
        # the same (and better-tested) statistics. Disagreeing with it means building
        # the leg it judged larger: TPC-H Q2's per-key form would build all of partsupp
        # to stream a few thousand suppliers (1.270x, its leg estimates 12x off).
        # INNER builds the LEFT leg; LEFT OUTER builds the preserved leg only when the
        # join exchanged (swap_build_side).
        join_builds_left = (join.type == "inner") or bool(join.swap_build_side)
        if (build == "left") != join_builds_left:
            self.record_decision(
                label, "declined: the join chose to build the other leg"
            )
            return
        gate = self._cost_gate(plan, join_nid, build, per_key, groups, alias,
                               left_keys if build == "right" else right_keys)
        if gate is not None:
            self.record_decision(label, f"declined: {gate}")
            return
        keys = left_keys if build == "left" else right_keys
        # An expression operand is pre-filtered to matched rows by a SEMI probe (see
        # the compiler) — worth it only when most streamed rows are discarded. When
        # nearly all match it filters nothing and costs a second probe and a copy:
        # TPC-DS Q23 (582K of 588K streamed rows join) measured 1.060x with it.
        join_rows = self._rows(join_nid)
        stream_rows = self._rows(plan.legs(join_nid)[1 if build == "left" else 0])
        prefilter = not (
            join_rows is not None and stream_rows
            and join_rows >= stream_rows * _PREFILTER_KEEP_RATIO
        )
        # payload: the build columns the output needs, in GROUP BY order (a streamed
        # key stand-in is carried as its build-side equal and re-labelled above).
        payload = list(dict.fromkeys(alias.get(gid, gid) for gid in group_ids))
        agg.groupjoin = {
            "join": join_nid,
            "build": build,
            "payload": payload,
            "alias": alias,
            "per_key": per_key,
            "multiply_d": per_key,
            # 0 drop (INNER), 1 organic LEFT (COUNT(*) counts the padded row)
            "unmatched": 1 if join.type == "left outer" else 0,
            "keys": keys,
            "prefilter": prefilter,
        }
        plan[agg_nid] = agg
        mode = "per key x d" if per_key else "per row + GROUP BY"
        join_rows = self._rows(join_nid)
        self.record_decision(
            label,
            f"applied organic ({join.type}, build {build}, {mode}): "
            f"{self._sides(plan, join_nid)}, join "
            + (f"{join_rows:,} rows" if join_rows is not None else "rows unknown"),
        )

    # ---- outer leg as build: O ⋈_k Γ_k(S) ------------------------------------------

    def _outer_leg_as_build(self, plan: LogicalPlan, join_nid: str) -> None:
        join = plan[join_nid]
        if join.groupjoin is not None or join.using_merged:
            return
        left_keys = list(join.left_columns or [])
        right_keys = list(join.right_columns or [])
        if not left_keys or len(left_keys) != len(right_keys):
            return
        # INNER only. Under LEFT OUTER the decorrelated plan tests the INNER key for
        # NULL above the join (decorrelate_subquery's _empty_value_case), and an
        # emitted row has no inner key of its own — only O's.
        # "nested loop" = an INNER equi-join with a residual applied as a post-join
        # filter; the residual is evaluated over the groupjoin's output instead.
        if join.type not in ("inner", "nested loop"):
            return
        left_nid, right_nid = plan.legs(join_nid)
        sides = [
            ("right", right_nid, right_keys, "left", left_nid),
            ("left", left_nid, left_keys, "right", right_nid),
        ]
        for agg_side, agg_leg_nid, agg_keys, outer_side, outer_nid in sides:
            found = self._grouped_on_keys(plan, agg_leg_nid, agg_keys)
            if found is None:
                continue
            projects, agg_nid, stream_nid, reduced = found
            # The replaced plan is "aggregate the input, probe the groups with the
            # outer leg". With a handful of groups that probe is nearly free, so unless
            # there is duplicated work to remove (a key reducer's copy of the outer
            # leg, or the window rewrite's copy of its source), building the outer leg
            # loses: TPC-DS Q1 (shared CTE, ~49K outer rows, a dozen store keys) 1.163x.
            # TPC-H Q2 (no copy, ~2M groups) won 0.86x — so the test is group count.
            outer_rows = self._rows(outer_nid)
            input_rows = self._rows(stream_nid)
            groups_est = self._rows(agg_nid)
            if not reduced and not join.is_window_join and groups_est is not None \
                    and input_rows is not None and groups_est * _FEW_GROUPS_RATIO < input_rows:
                self.record_decision(
                    "groupjoin",
                    f"declined outer-leg build: {groups_est:,} groups from {input_rows:,} "
                    "aggregate input rows and no copy to remove",
                )
                return
            # The outer leg becomes the BUILD and the aggregate's input streams past it.
            # Building an outer leg far larger than that input loses to the plan it
            # replaces (aggregate the small input to a few groups, probe them): TPC-DS
            # Q6 (382K outer rows vs 18K items) measured 1.078x, Q1 1.163x.
            if outer_rows is not None and input_rows is not None and input_rows > 0 \
                    and outer_rows > input_rows * _BUILD_RATIO:
                self.record_decision(
                    "groupjoin",
                    f"declined outer-leg build: outer {outer_rows:,} rows > "
                    f"{_BUILD_RATIO:g}x aggregate input {input_rows:,} rows",
                )
                return
            agg = plan[agg_nid]
            # The aggregate's own operands all come from its single input.
            reason = _aggregates_eligible(agg.aggregates, None, [], [])
            if reason is not None:
                self.record_decision("groupjoin", f"declined outer-leg build: {reason}")
                return
            # S keys in the aggregate's GROUP BY order, paired with O's join keys.
            outer_keys = left_keys if outer_side == "left" else right_keys
            pair = dict(zip(agg_keys, outer_keys))
            join.groupjoin = {
                "outer": outer_side,
                "outer_leg": outer_nid,
                "aggregate": agg_nid,
                "projects": projects,
                "stream": stream_nid,
                "stream_keys": list(agg_keys),
                "outer_keys": [pair[k] for k in agg_keys],
                "unmatched": 0,
            }
            plan[join_nid] = join
            self.record_decision(
                "groupjoin",
                f"applied outer leg as build ({join.type}): outer "
                + (f"{outer_rows:,}" if outer_rows is not None else "unknown")
                + " rows, aggregate input "
                + (f"{input_rows:,}" if input_rows is not None else "unknown")
                + " rows",
            )
            return

    @staticmethod
    def _grouped_on_keys(plan: LogicalPlan, leg_nid: str, keys: list):
        """Walk `leg_nid` down through Project nodes to an AggregateAndGroup grouped on
        exactly `keys`. Returns (project nids top-down, aggregate nid, stream nid) —
        the stream skipping a key reducer — or None."""
        projects = []
        nid = leg_nid
        while plan[nid].node_type == LogicalPlanStepType.Project:
            projects.append(nid)
            providers = [src for src, _t, _r in plan.ingoing_edges(nid)]
            if len(providers) != 1:
                return None
            nid = providers[0]
        agg = plan[nid]
        if agg.node_type != LogicalPlanStepType.AggregateAndGroup:
            return None
        if agg.grouping_set_identities is not None or agg.having_condition is not None:
            return None
        groups = list(agg.groups or [])
        if any(g.node_type != NodeType.IDENTIFIER or g.schema_column is None for g in groups):
            return None
        group_ids = [g.schema_column.identity for g in groups]
        if sorted(group_ids) != sorted(keys) or len(group_ids) != len(keys):
            return None
        providers = [src for src, _t, _r in plan.ingoing_edges(nid)]
        if len(providers) != 1:
            return None
        stream = providers[0]
        below = plan[stream]
        reduced = False
        if below.node_type == LogicalPlanStepType.Join and below.key_reducer:
            stream = plan.legs(stream)[0]
            reduced = True
        return projects, nid, stream, reduced

    def _rows(self, nid) -> Optional[int]:
        return self._store.row_count(nid) if self._store.has(nid) else None

    def _cost_gate(self, plan, join_nid, build, per_key, groups, alias, stream_keys):
        """None when fusing is expected to win, else the reason (with its numbers).
        Missing estimates decline only the rule that needs them."""
        left_nid, right_nid = plan.legs(join_nid)
        build_nid, stream_nid = (left_nid, right_nid) if build == "left" else (right_nid, left_nid)
        build_rows = self._rows(build_nid)
        stream_rows = self._rows(stream_nid)
        if build_rows is not None and stream_rows is not None and stream_rows > 0:
            if build_rows > stream_rows * _BUILD_RATIO:
                return (f"build leg {build_rows:,} rows > {_BUILD_RATIO:g}x streamed "
                        f"{stream_rows:,} rows")
        if not per_key:
            widths = []
            for g in groups:
                ct = g.schema_column.column_type
                widths.append(ct.physical if ct is not None else None)
            if not any(w in _VARIABLE_WIDTH for w in widths):
                return "per-row emit over fixed-width group columns (GROUP BY already cheap)"
            if len(stream_keys) == 1 and stream_rows and self._store.has(stream_nid):
                ndv = self._store.distinct_count(stream_nid, stream_keys[0])
                if ndv is not None and ndv >= stream_rows * _UNIQUE_STREAM_KEY:
                    return (f"per-row emit with a unique streamed key ({ndv:,} distinct of "
                            f"{stream_rows:,} rows): the GROUP BY input cannot shrink")
        return None

    def _sides(self, plan: LogicalPlan, join_nid: str) -> str:
        """The legs' estimated rows, for the recorded decision ("unknown" when a leg
        has no estimate — nothing here is decided on it yet)."""
        out = []
        for side, nid in zip(("left", "right"), plan.legs(join_nid)):
            rows = self._store.row_count(nid) if self._store.has(nid) else None
            out.append(f"{side} {rows:,} rows" if rows is not None else f"{side} rows unknown")
        return ", ".join(out)
