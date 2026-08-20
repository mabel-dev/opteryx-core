# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Semi-Join Pushdown

Type: Cost-based (a costed pair — see OptimizationStrategy.record_decision)
Goal: Filter before the joins multiply rows, not after

A decorrelated IN/EXISTS subquery arrives as a SEMI (or ANTI) join sitting ABOVE
the query's whole inner-join chain, filtering the joined intermediate on a key
that one leg of that chain supplies by itself. TPC-H Q18 at SF100: the semi
tests `o_orderkey` against ~6k qualifying keys — but only after customer ⋈
orders ⋈ lineitem has materialized 600M rows for it to discard. Applied to
`orders` directly, the same test leaves ~6k rows and every operator above runs
on thousands of rows instead of hundreds of millions.

The transform is the identity  SEMI(A ⋈ B, S) = SEMI(A, S) ⋈ B,  legal exactly
when every column the semi's condition reads from its probe side resolves to
ONE leg of the inner join below. A semi/anti join only removes probe rows, and
each row's fate depends only on its A-columns, so filtering A before or after
the join keeps precisely the same rows. Inner joins only — pushing into an
outer join's null-producing side changes which rows exist to be preserved.

⛔ NOT an unconditional rewrite. TPC-H Q21 is the mirror image: its semi/anti
keys also resolve to one leg (lineitem, 380M rows) but the chain above REDUCES
— nation cuts 25× — so the semi probes 1.6M rows where it stands and would
probe 380M pushed down. The gate compares, per sink step, the estimated rows
arriving at the semi now (`est(join output)`) against the estimated rows of the
key-supplying leg (`est(leg)`), and sinks only while the leg is not materially
bigger. Both failure directions are fail-safe: a garbage-low input estimate
declines (keeps today's plan), a garbage-high leg estimate declines. Missing
statistics decline — a missing count must never be read as "cheap".

The sink iterates: Q18's semi passes two joins (60M > 15M, then 15M ≈ 15M with
the join above already saved) and lands directly on the orders scan. Each
accepted step keeps the probe count roughly non-increasing while strictly
moving the filter below one more join, whose input then shrinks by the semi's
selectivity — bounded downside (the margin), unbounded upside.

Runs before SemiJoinReducerStrategy: a pushed semi then sits on a bare scan
whose leg the reducer's `_is_restricted` gate correctly refuses to copy, and
JoinOrderingStrategy later still owns the build-side decision for the new
shape. The decision — either way — is recorded with its numbers in EXPLAIN's
OPTIMIZATIONS block.
"""

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.planner.binder.join_helpers import extract_join_fields
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .semi_join_reducer import _collect_relations
from .semi_join_reducer import _equi_pairs

# Plain semi/anti only. "left anti null-aware" (NOT IN) and the not-distinct
# set-operation joins decide their answer from a property of the build side;
# they are excluded by exact match, the same posture JoinOrderingStrategy takes.
_PUSHABLE_TYPES = ("left semi", "left anti")

# Sink one level only while the key-supplying leg is not materially bigger than
# the semi's current input. >1 so the sink continues through joins that neither
# grow nor shrink (Q18's customer join: 15M ≈ 15M — the join above still saves);
# small so a genuinely reducing chain (Q21: 380M vs 1.6M) declines with room
# for estimate noise.
_SINK_MARGIN = 1.2

# A sunk semi walks a linear join spine; 16 out-levels a pathological plan.
_MAX_SINK_LEVELS = 16


def _fmt_rows(value: float) -> str:
    """Compact row-count rendering for decision records: 601M, 1.6M, 15k, 624."""
    for scale, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if value >= scale:
            scaled = value / scale
            return f"{scaled:.0f}{suffix}" if scaled >= 10 else f"{scaled:.1f}{suffix}"
    return f"{value:.0f}"


def _est_rows(node) -> int:
    """Estimated output rows from the refreshed statistics, or None if absent."""
    stats = getattr(node, "statistics", None)
    rows = getattr(stats, "row_count", None) if stats is not None else None
    if not rows or rows <= 0:
        return None
    return rows


def _resolve_legs(plan, join_nid):
    """A join's (left_edge, right_edge) exactly as compiler.py will read them:
    the edge label wins, insertion order is the fallback for unlabelled edges.
    Each element is the full (source, target, relation) tuple; None if the node
    does not have exactly one leg per side."""
    in_edges = plan.ingoing_edges(join_nid)
    if len(in_edges) != 2:
        return None
    legs = {}
    for idx, edge in enumerate(in_edges):
        label = edge[2] or ("left" if idx == 0 else "right")
        legs[label] = edge
    if "left" not in legs or "right" not in legs:
        return None
    return legs["left"], legs["right"]


def _label_join_legs(plan, join_nid):
    """Stamp a join's resolved leg labels onto its edges explicitly.

    ⛔ Unlabelled join edges resolve by INSERTION ORDER (compiler.py falls back
    to `left if idx == 0 else right`), and this strategy's re-wiring changes
    insertion order. Any join whose edges the surgery touches gets its CURRENT
    resolution made explicit first, so leg identity survives the re-wire. Must
    only run once every guard has passed — add_edge relabelling is a mutation.
    """
    legs = _resolve_legs(plan, join_nid)
    if legs is None:
        return
    for edge, label in zip(legs, ("left", "right")):
        plan.add_edge(edge[0], join_nid, label)  # in-place relabel, order unchanged


def _collect_scan_uuids(plan, root_nid):
    """Scan node UUIDs under a subtree — what a join's `*_readers` lists hold."""
    uuids = []
    visited = set()
    frontier = [root_nid]
    while frontier:
        nid = frontier.pop()
        if nid in visited:
            continue
        visited.add(nid)
        node = plan[nid]
        uuid = getattr(node, "uuid", None)
        if node.node_type == LogicalPlanStepType.Scan and uuid is not None:
            uuids.append(uuid)
        for child, _target, _relation in plan.ingoing_edges(nid):
            frontier.append(child)
    return uuids


class SemiJoinPushdownStrategy(OptimizationStrategy):
    # Cost-typed so the driver refreshes statistics first; the semi and the join
    # chain both only exist in final form once predicates are pushed.
    optimization_technique = "cost"
    requires = ("predicates-pushed",)

    def should_i_run(self, plan: LogicalPlan) -> bool:
        # Load-bearing beyond skipping the walk: a `cost` strategy answering yes
        # puts a full statistics refresh on the query's path, so only answer yes
        # when an actual candidate exists — a semi/anti join whose probe leg is
        # an inner join.
        for nid, node in plan.nodes(True):
            if node.node_type != LogicalPlanStepType.Join:
                continue
            if node.type not in _PUSHABLE_TYPES:
                continue
            legs = _resolve_legs(plan, nid)
            if legs is None:
                continue
            probe = plan[legs[0][0]]
            if probe.node_type == LogicalPlanStepType.Join and probe.type == "inner":
                return True
        return False

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # Graph surgery is deferred to complete(): mutating during the traversal
        # corrupts the walk.
        targets = [
            nid
            for nid, node in plan.nodes(True)
            if node.node_type == LogicalPlanStepType.Join and node.type in _PUSHABLE_TYPES
        ]
        for join_nid in targets:
            self._sink(plan, join_nid)
        return plan

    def _sink(self, plan, join_nid: str) -> None:
        join = plan[join_nid]
        start_est = None
        final_est = None
        levels = 0
        decline_detail = None
        while levels < _MAX_SINK_LEVELS:
            step = self._sink_one_level(plan, join_nid, join)
            if isinstance(step, str):
                decline_detail = step
                break
            if step is None:
                break
            if start_est is None:
                start_est = step[0]
            final_est = step[1]
            levels += 1

        if levels:
            self.telemetry.optimization_semi_join_pushdown = (
                getattr(self.telemetry, "optimization_semi_join_pushdown", 0) + levels
            )
            self.record_decision(
                "semi join pushdown",
                f"sunk below {levels} join{'s' if levels > 1 else ''}: probe est "
                f"{_fmt_rows(start_est)} → {_fmt_rows(final_est)} rows",
            )
        elif decline_detail is not None:
            # Only a gate refusal is a decision worth recording — a semi with no
            # inner join below it was never a candidate at all.
            self.record_decision("semi join pushdown", decline_detail)

    def _sink_one_level(self, plan, join_nid: str, join):
        """One sink step. Returns (probe_est, leg_est) on success, a decline
        detail string when the COST gate said no, and None when the shape was
        never a candidate. Every guard runs before the first mutation."""
        legs = _resolve_legs(plan, join_nid)
        if legs is None:
            return None
        (probe_root, _pt, probe_label), (build_root, _bt, _build_label) = legs

        below = plan[probe_root]
        if below.node_type != LogicalPlanStepType.Join or below.type != "inner":
            return None
        # Trees only: a probe leg with a second consumer cannot be re-wired.
        if len(plan.outgoing_edges(probe_root)) != 1:
            return None
        out_edges = plan.outgoing_edges(join_nid)
        if len(out_edges) != 1:
            return None
        _join_src, parent_nid, parent_label = out_edges[0]

        below_legs = _resolve_legs(plan, probe_root)
        if below_legs is None:
            return None

        # Every probe-side column the semi's condition reads must resolve to ONE
        # leg of the join below — keys and residual conjuncts alike.
        #
        # ⛔ Membership in left_relation_names alone cannot classify a column: a
        # decorrelated subquery typically RE-READS a relation the outer query
        # also reads (Q18's build side is an aggregate over `lineitem`, which
        # the probe side joins too), so the build key's source name sits in
        # BOTH name sets. Equi pairs are ORIENTED instead — the member on the
        # probe side names the probe key, whichever names its partner shares —
        # and residual references must classify unambiguously or the push is
        # refused.
        pairs = _equi_pairs(getattr(join, "on", None))
        if not pairs:
            return None
        left_names = set(join.left_relation_names or [])
        right_names = set(join.right_relation_names or [])
        probe_keys = []
        for first, second in pairs:
            first_left = getattr(first, "source", None) in left_names
            second_left = getattr(second, "source", None) in left_names
            first_right = getattr(first, "source", None) in right_names
            second_right = getattr(second, "source", None) in right_names
            if first_left and second_right and not (first_right and second_left):
                probe_keys.append(first)
            elif second_left and first_right and not (second_right and first_left):
                probe_keys.append(second)
            elif first_left and not second_left:
                probe_keys.append(first)
            elif second_left and not first_left:
                probe_keys.append(second)
            else:
                return None
        # Both members of every oriented pair are classified; only residual
        # references outside the equi pairs still need the unambiguous test.
        pair_member_ids = {id(member) for pair in pairs for member in pair}
        probe_sources = {key.source for key in probe_keys}
        for identifier in get_all_nodes_of_type(join.on, (NodeType.IDENTIFIER,)):
            if id(identifier) in pair_member_ids:
                continue
            source = getattr(identifier, "source", None)
            in_left = source in left_names
            in_right = source in right_names
            if in_left and in_right:
                return None  # ambiguous residual reference — refuse, don't guess
            if in_left:
                probe_sources.add(source)
        if not probe_sources:
            return None

        target_edge = None
        for edge in below_legs:
            leg_relations, _schemas = _collect_relations(plan, edge[0])
            if probe_sources <= leg_relations:
                if target_edge is not None:
                    # Both legs claim every key relation — ambiguous, leave it.
                    return None
                target_edge = edge
        if target_edge is None:
            return None
        target_root, _tt, _target_label = target_edge

        # ── The costed pair ─────────────────────────────────────────────────
        # Probe rows where the semi stands now vs probe rows on the leg below.
        # `node.statistics` estimates, refreshed by the driver before this
        # strategy; either one missing is fail-safe: no move.
        probe_est = _est_rows(below)
        leg_est = _est_rows(plan[target_root])
        if probe_est is None or leg_est is None:
            return None
        if leg_est > probe_est * _SINK_MARGIN:
            return (
                f"declined: leg est {_fmt_rows(leg_est)} > "
                f"{_SINK_MARGIN}× input est {_fmt_rows(probe_est)}"
            )

        # Rebind the semi's bookkeeping to its narrower probe side, and verify
        # the keys still resolve BEFORE touching the graph — a key naming
        # neither leg is the silent-wrong-answer case, not something to push
        # on through.
        target_relations, target_schemas = _collect_relations(plan, target_root)
        right_names = list(join.right_relation_names or [])
        new_left_names = sorted(target_relations)
        left_columns, right_columns = extract_join_fields(join.on, new_left_names, right_names)
        if len(left_columns) != len(pairs) or len(right_columns) != len(pairs):
            return None

        # ── Surgery: J drops from above `below` onto its `target` leg ───────
        #     before:  target → below → J → parent      after:  target → J → below → parent
        # Every join whose edges move is labelled explicitly FIRST (see
        # _label_join_legs) — leg identity must never rest on edge insertion
        # order, which the re-wiring below changes.
        _label_join_legs(plan, probe_root)
        parent = plan[parent_nid]
        if parent.node_type == LogicalPlanStepType.Join:
            _label_join_legs(plan, parent_nid)
            # The relabel just rewrote the J→parent edge's label; re-read it —
            # remove_edge silently no-ops on a label that doesn't match.
            parent_label = next(
                relation
                for source, _target, relation in plan.ingoing_edges(parent_nid)
                if source == join_nid
            )
        target_resolved = "left" if target_edge is below_legs[0] else "right"
        plan.remove_edge(probe_root, join_nid, probe_label)
        plan.remove_edge(target_root, probe_root, target_resolved)
        plan.remove_edge(join_nid, parent_nid, parent_label)
        plan.add_edge(target_root, join_nid, "left")
        plan.add_edge(build_root, join_nid, "right")  # relabels the existing edge
        plan.add_edge(join_nid, probe_root, target_resolved)
        plan.add_edge(probe_root, parent_nid, parent_label)

        removed_names = left_names - target_relations
        join.left_relation_names = new_left_names
        join.all_relations = (set(join.all_relations or set()) - removed_names) | target_relations
        join.schemas = {
            **{k: v for k, v in (join.schemas or {}).items() if k not in removed_names},
            **target_schemas,
        }
        join.left_columns = left_columns
        join.right_columns = right_columns
        if getattr(join, "left_readers", None):
            join.left_readers = _collect_scan_uuids(plan, target_root)
        plan[join_nid] = join

        # `below` now consumes the semi's output in the target leg's place; its
        # own relation names, schemas and readers are unchanged — the semi is
        # transparent to the join's resolution (it only removes rows). The
        # other leg was never touched.
        return (probe_est, leg_est)
