# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Per-query planning state that DESCRIBES the plan but is not PART of any node.

Plan and expression nodes carry only what a node IS (architect ruling
2026-09-25: node attributes are fixed and enforced). Everything a pass computes
ABOUT nodes — estimated statistics, the scan base-statistics memo, the
estimates stamped onto shared-CTE references — lives here, owned by the query
and passed EXPLICITLY to every producer and consumer. There is no global and no
default instance: a function that reads estimates takes the context that holds
them.

Keyed by node OBJECT identity, the same identity `node.statistics` had when
the estimate was an attribute: two nodes are two entries even when a `copy()`
gave them the same `uuid`. Each entry holds a strong reference to its node, so
an `id()` cannot be reused by another object while the entry exists.

This is the Python seed of the native per-query PlanContext in the plan-graph
design (native_plan_graph_proposal): integer NodeIds replace object identity
there, and the statistics store becomes native.
"""

from typing import TYPE_CHECKING
from typing import Dict
from typing import Optional
from typing import Tuple

from opteryx.compiled.planner.column_table import ColumnTable
from opteryx.compiled.planner.plan_graph import NodeIds

if TYPE_CHECKING:  # annotation only: importing the optimizer package here is a cycle
    from opteryx.planner.optimizer.statistics import RelationStatistics


class PlanContext:
    __slots__ = (
        "_statistics",
        "_cte_statistics",
        "scan_stats_cache",
        "columns",
        "node_ids",
        "shared_ctes",
        "physical_shared_ctes",
        "recursive_ctes",
        "statistics_estimated_by_optimizer",
    )

    def __init__(self) -> None:
        # The query's bound columns — see ColumnTable. Created with the context at
        # the start of planning, before anything mints a column.
        self.columns = ColumnTable()
        # The query's plan node ids — every logical and physical plan of the query
        # draws from this one counter, so plans merge without colliding (native
        # plan graph P2, architect rulings 2026-09-27).
        self.node_ids = NodeIds()
        # The query's shared CTE bodies - CTEs referenced 2+ times, executed once
        # (relation_resolver) - keyed by cte_key, dependencies first. Each planning
        # phase (resolver, rewriter, binder, optimizer) replaces the bodies it
        # transforms; `physical_shared_ctes` holds their physical plans. Held here,
        # not on a plan object, because they belong to the query, not to one plan
        # (architect ruling 2026-09-27, native plan graph P2).
        self.shared_ctes: dict = {}
        self.physical_shared_ctes: dict = {}
        # Recursive CTE metadata: rcte_key -> its anchor/term leg keys (the legs
        # are shared_ctes entries). See docs/RECURSIVE_CTE_DESIGN.md.
        self.recursive_ctes: dict = {}
        # Whether the optimizer refreshed statistics - see
        # OptimizerVisitor.refreshed_statistics.
        self.statistics_estimated_by_optimizer: bool = False
        self._statistics: Dict[int, Tuple[object, "RelationStatistics"]] = {}
        self._cte_statistics: Dict[str, "RelationStatistics"] = {}
        # Memo of each scan's manifest-derived base statistics, shared by every
        # statistics refresh and the billing meter of one query. See
        # statistics_refresh.scan_base_statistics for the key.
        self.scan_stats_cache: dict = {}

    def statistics(self, node) -> Optional["RelationStatistics"]:
        """The estimate the last statistics refresh attached to `node`, or None
        when no refresh has reached it."""
        entry = self._statistics.get(id(node))
        return None if entry is None else entry[1]

    def set_statistics(self, node, statistics: "RelationStatistics") -> None:
        self._statistics[id(node)] = (node, statistics)

    def cte_statistics(self, cte_key: str) -> Optional["RelationStatistics"]:
        """The output estimate of the shared CTE `cte_key` (its body's, or for a
        recursive CTE its anchor's), or None when none was recorded. Keyed by the
        CTE, not by a reference node: every MaterializedCteRef naming `cte_key`
        reads the same body, wherever in the plan forest it sits and however
        often the optimizer copies it."""
        return self._cte_statistics.get(cte_key)

    def set_cte_statistics(self, cte_key: str, statistics: "RelationStatistics") -> None:
        self._cte_statistics[cte_key] = statistics
