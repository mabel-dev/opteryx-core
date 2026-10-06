# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Key Bound for GROUP BY ... LIMIT (no ORDER BY)

Type: Heuristic
Goal: Read only the files that can hold the groups a LIMIT will return

    SELECT k, COUNT(*) FROM t GROUP BY k LIMIT 10

With no ORDER BY, ANY 10 groups are a correct answer, so the planner may choose
which. It chooses the ones a manifest can PROVE exist below a bound T, and adds
`k <= T` ahead of predicate pushdown so the ordinary manifest and row-group
pruning drop every file that cannot hold a qualifying key.

The proof. A file's min and max are values that exist in it. Collect the distinct
witness values across files; the K-th smallest (K = LIMIT + OFFSET) is T. At
least K distinct keys are <= T, so the filtered aggregate still produces >= K
groups. For integer keys a second proof competes: a file whose footers PROVE >= K
distinct values holds K keys within [min, max], so T may be that file's max
(the smaller proof wins; an estimated count never qualifies). The filter keeps EVERY row of each surviving key, so each group's
aggregates are complete - this is a choice of groups, never a partial count.

Row groups. For an integer key on a reader that serves them
(`supports_row_group_key_bounds`), the same proof is re-run with every surviving
file's ROW GROUPS as the witnesses and the T it yields replaces the file-level T when
tighter. T stays a plan-time literal, so the row filter and the scan's row-group
pruning agree on it and every kept key keeps all of its rows.

Strings. Stored string bounds may be truncated: a stored min is a byte PREFIX p
of the true min, so the true min is < inc(p) (p with its last byte bumped) - a
real value no greater than inc(p). Only mins are used as string witnesses, and
only a prefix-free set of them (a witness that is a prefix of another could be
the same true value), so the K witnesses are distinct real values all <= the
largest inc(p). Over-capturing is fine; the rule needs "at least K", never
"exactly K".

Abandoned - quickly, and silently (no behaviour changes) - unless ALL hold:
- Limit -> Project* -> AggregateAndGroup -> Project* -> Scan, nothing else between;
  a Filter, Join, Sort, Distinct or HAVING anywhere in that chain removes rows or
  groups and voids "at least K exist".
- The aggregate has plain GROUP BY keys (no grouping sets).
- The Scan has no predicate, limit or top-N spec, and its manifest is
  authoritative, has no deletes, and bounds the key for EVERY file with values.
- The key is an INTEGER or string column (floats: NaN; temporals: unit encodings
  - not v1).
- At least K distinct provable witnesses exist, and the bound really prunes a
  file.

NULL keys are irrelevant here: no ordering means the NULL group is simply not one
of the groups chosen, and `k <= T` excludes it.
"""

from bisect import bisect_left
from bisect import bisect_right

from draken.draken_native import DrakenType
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.plan_steps import FilterStep
from opteryx.expression import NodeType
from opteryx.planner import build_literal_node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import PlanStep
from opteryx.types.logical_type import LogicalCategory

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

_STRING_CATEGORIES = (
    LogicalCategory.VARCHAR,
    LogicalCategory.NVARCHAR,
    LogicalCategory.VARBINARY,
)


# The value tags a manifest gives string bounds: text columns are "text", binary "bytes".
_STRING_TAGS = ("text", "bytes")


def _increment(prefix: bytes):
    """The smallest byte string greater than every string that starts with `prefix`,
    or None when there is none (all 0xFF)."""
    stripped = prefix.rstrip(b"\xff")
    if not stripped:
        return None
    return stripped[:-1] + bytes([stripped[-1] + 1])


# Windows scored exactly by distinct files: only the best few by unit count.
_FINALISTS = 16


def _integer_window(units, k, within=None):
    """The best proven window ``[lower, upper]`` of an integer key, or None.

    `units` are ``(lo, hi, proven_distinct, file)`` - a file or a row group, with the
    min/max of values that EXIST in it, a distinct count its footer PROVES (0 when
    none), and the file it lives in. A window is sound when it holds at least K
    distinct keys, because the filter keeps every row of every key in it:
    - K consecutive distinct witnesses (unit mins and maxes) lie in
      [witness i, witness i+K-1] - a window from EITHER end of the key space or the
      middle, not only ``<= T``;
    - a unit whose footer proves >= K distinct values holds them in its own [lo, hi].
    The window touching the FEWEST FILES wins (then fewest units): that is the cost,
    and where witnesses cluster in a file or a layout is sorted it is far fewer than
    the K files a plain ``<= T`` reads. A unit is touched when its [lo, hi] overlaps
    the window. `within` ``(lower, upper)`` confines the search to a sub-window of an
    earlier answer (None = unbounded), which is what keeps the units seen a complete
    account of what the window touches.

    Returns ``(lower, upper, kept_units, kept_files)``."""
    witnesses = sorted({v for lo, hi, _p, _f in units for v in (lo, hi)})
    candidates = {(witnesses[i], witnesses[i + k - 1]) for i in range(len(witnesses) - k + 1)}
    candidates.update((lo, hi) for lo, hi, proven, _f in units if proven >= k)
    if within is not None:
        floor, ceiling = within
        candidates = {(a, b) for a, b in candidates if a >= floor and b <= ceiling}
    if not candidates:
        return None
    los = sorted(lo for lo, _hi, _p, _f in units)
    his = sorted(hi for _lo, hi, _p, _f in units)
    scored = sorted(
        ((bisect_right(los, b) - bisect_left(his, a), a, b) for a, b in candidates)
    )[:_FINALISTS]
    best = None
    for kept_units, a, b in scored:
        files = {f for lo, hi, _p, f in units if lo <= b and hi >= a}
        rank = (len(files), kept_units, b - a)
        if best is None or rank < best[0]:
            best = (rank, a, b, kept_units, len(files))
    _rank, a, b, kept_units, kept_files = best
    return a, b, kept_units, kept_files


def _string_bound(bounds, k):
    """(T, survivor_count) for a string key, or None."""
    if any(lo[0] not in _STRING_TAGS for lo, _hi in bounds):
        return None
    chosen = []
    for prefix in sorted({lo[1] for lo, _hi in bounds}):
        # sorted order: if any chosen witness is a prefix of this one, the LAST chosen is
        if chosen and prefix.startswith(chosen[-1]):
            continue
        chosen.append(prefix)
        if len(chosen) == k:
            break
    if len(chosen) < k:
        return None
    bound = _increment(chosen[-1])
    if bound is None:
        return None
    return bound, sum(1 for lo, _hi in bounds if lo[1] <= bound)


class GroupLimitKeyBoundStrategy(OptimizationStrategy):
    """Bound the scanned keys of `GROUP BY k LIMIT n` using manifest min/max."""

    def should_i_run(self, plan: LogicalPlan) -> bool:
        return any(
            node.limit is not None
            for _, node in get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Limit,))
        )

    def _producer(self, plan, nid):
        ingoing = plan.ingoing_edges(nid)
        return ingoing[0][0] if len(ingoing) == 1 else None

    def _skip_projects(self, plan, nid):
        """nid, walking down through Project nodes; (nid, node) or (None, None)."""
        while nid is not None:
            node = plan[nid]
            if node is None:
                return None, None
            if node.node_type != LogicalPlanStepType.Project:
                return nid, node
            nid = self._producer(plan, nid)
        return None, None

    def _row_group_units(self, scan, manifest, name, bounds, floors, window):
        """Row-group units ``(lo, hi, proven, file)`` of the files `window` touches, or
        None when the reader cannot describe row groups.

        A file whose row groups are not ALL described contributes its own min/max as
        one unit, as at file level. Files outside `window` are not read: a sub-window
        of it cannot touch them."""
        connector = scan.connector
        if connector is None or not connector.supports_row_group_key_bounds:
            return None
        lower, upper = window
        rows = [row for row, (lo, hi) in enumerate(bounds) if lo[1] <= upper and hi[1] >= lower]
        paths = manifest.get_file_paths()
        sizes = manifest.file_sizes()
        described = connector.row_group_key_bounds(
            name, [paths[row] for row in rows], [sizes[row] for row in rows]
        )
        units = []
        for row, groups in zip(rows, described, strict=True):
            if groups is None:
                units.append((bounds[row][0][1], bounds[row][1][1], 0 if floors is None else floors[row], row))
                continue
            units.extend((lo, hi, proven, row) for lo, hi, proven in groups)
        return units

    def _integer_candidate(self, scan, manifest, name, physical, bounds, k):
        """(lower, upper, description, kept_files) for an integer key, or None.
        `lower` is None when no file sits wholly below it, so no filter is needed."""
        floors = manifest.file_distinct_floors(name)
        units = [(lo[1], hi[1], 0 if floors is None else floors[row], row) for row, (lo, hi) in enumerate(bounds)]
        found = _integer_window(units, k)
        if found is None:
            return None
        lower, upper, kept_units, kept_files = found
        text = f"{kept_files} of {len(bounds)} files"
        if physical != DrakenType.UINT64:
            row_groups = self._row_group_units(scan, manifest, name, bounds, floors, (lower, upper))
            if row_groups is not None:
                refined = _integer_window(row_groups, k, within=(lower, upper))
                if refined is not None and refined[:2] != (lower, upper):
                    lower, upper, kept_units, kept_files = refined
                    text = f"{kept_files} of {len(bounds)} files, {kept_units} of {len(row_groups)} row groups"
        # judged against EVERY file: a file outside the window is still a file the
        # filter must exclude, whichever units the refinement happened to read
        pruning_below = any(hi[1] < lower for _lo, hi in bounds)
        pruning_above = any(lo[1] > upper for lo, _hi in bounds)
        if not pruning_below and not pruning_above and kept_units >= len(units):
            return None
        return (lower if pruning_below else None), upper, text, kept_files

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        if node.node_type != LogicalPlanStepType.Limit:
            return context
        limit, offset = node.limit, node.offset
        if type(limit) is not int or limit <= 0 or (offset is not None and type(offset) is not int):
            return context
        k = limit + (offset or 0)

        plan = context.optimized_plan
        agg_nid, agg = self._skip_projects(plan, self._producer(plan, context.node_id))
        if agg is None or agg.node_type != LogicalPlanStepType.AggregateAndGroup:
            return context
        if agg.having_condition is not None or agg.grouping_sets is not None or not agg.groups:
            return context

        scan_nid, scan = self._skip_projects(plan, self._producer(plan, agg_nid))
        if scan is None or scan.node_type != LogicalPlanStepType.Scan:
            return context
        if scan.predicates or scan.limit is not None or scan.topn_limit is not None:
            return context
        manifest = scan.manifest
        if manifest is None or not manifest.stats_are_authoritative or manifest.has_deletes():
            return context
        if manifest.get_file_count() < 2:
            return context

        scan_identities = {c.identity for c in scan.schema.columns}
        best = None  # (kept files, key, lower, upper, description)
        for key in agg.groups:
            if key.node_type != NodeType.IDENTIFIER or key.schema_column is None:
                continue
            if key.schema_column.identity not in scan_identities:
                continue
            column_type = key.schema_column.column_type
            category = column_type.category
            name = key.schema_column.name
            bounds = manifest.file_value_bounds(name)
            if bounds is None:
                continue
            if category == LogicalCategory.INTEGER:
                if any(lo[0] != "int" or hi[0] != "int" for lo, hi in bounds):
                    continue
                found = self._integer_candidate(scan, manifest, name, column_type.physical, bounds, k)
            elif category in _STRING_CATEGORIES:
                strings = _string_bound(bounds, k)
                if strings is None or strings[1] >= len(bounds):
                    continue
                found = (None, strings[0], f"{strings[1]} of {len(bounds)} files", strings[1])
            else:
                continue
            if found is None:
                continue
            if best is None or found[3] < best[0]:
                best = (found[3], key, found[0], found[1], found[2])
        if best is None:
            return context

        _files, key, lower, upper, kept_text = best
        for op, value in (("LtEq", upper), ("GtEq", lower)):
            if value is None:
                continue
            condition = Comparison(
                value=op,
                left=key,
                right=build_literal_node(
                    value, suggested_type=key.schema_column.column_type, plan_context=context.plan_context
                ),
                arena=context.plan_context.expressions,
            )
            filter_node = FilterStep(
                condition=condition,
                columns=[key],
                relations={key.source},
                all_relations={key.source},
            )
            plan.insert_node_after(filter_node, scan_nid)
        self.telemetry.increase("optimization_group_limit_key_bound")
        window = f"<= {upper}" if lower is None else f"in [{lower}, {upper}]"
        self.record_decision(
            "group-limit key bound",
            f"{key.schema_column.name} {window} keeps {kept_text} for {k} group(s)",
        )
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
