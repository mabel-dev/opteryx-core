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

The estimated statistics are native (`StatisticsStore`, native plan graph P5):
keyed by the plan's integer node ids, read through typed accessors.
"""

from opteryx.compiled.planner.column_table import ColumnTable
from opteryx.compiled.planner.plan_graph import NodeIds
from opteryx.compiled.planner.statistics import StatisticsStore
from opteryx.compiled.structures.expressions import ExprArena


class PlanContext:
    __slots__ = (
        "statistics",
        "columns",
        "node_ids",
        "expressions",
        "constant_folded",
        "shared_ctes",
        "physical_shared_ctes",
        "recursive_ctes",
        "variables",
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
        # The query's expressions - every expression is minted in this arena and
        # identified by the int id it mints (native plan graph P3, architect
        # rulings 2026-09-27).
        self.expressions = ExprArena()
        self.expressions.bind_columns(self.columns)
        # The query's estimated statistics: every node the last refresh of its
        # plan reached (by node id), each shared CTE's output, and the scan base
        # memo the refreshes and the billing meter share.
        self.statistics = StatisticsStore(self.expressions)
        # expr_ids of the trees constant folding has folded - its second pass skips
        # them. Pass state, so it is held here, not on the expressions.
        self.constant_folded: set = set()
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
        # The session's variables, for a planning read of a tunable through
        # opteryx.variables.resolve (the vector index cost model reads nprobe and the
        # worker count). Set by whoever plans a session's query; None = no session, and
        # resolve() then answers each variable's default.
        self.variables = None
