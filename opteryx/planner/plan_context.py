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
from typing import List
from typing import Optional
from typing import Tuple

if TYPE_CHECKING:  # annotation only: importing the optimizer package here is a cycle
    from opteryx.planner.optimizer.statistics import RelationStatistics
    from opteryx.types.schema import SchemaColumn


class ColumnTable:
    """Every bound column of one query, in the order it was minted — a column's
    `slot` is its position here.

    This is the ONE place a bound column is created (architect ruling
    2026-09-26, option C): a connector describes its columns without identities,
    and the query — through this table, reached by every phase from the AST
    builders to the compiler — mints each column's identity and slot. The
    identity is the engine's column key (random bytes with a traceable prefix,
    unchanged by this); the slot is the column's number in its query, the handle
    the native plan graph will key its column table by.

    A copy of a column that keeps its identity (a binder branch copy, a
    subclass stripped to a plain column) is the same column and keeps its slot.
    A copy that takes a NEW identity is a new column, and `remint` makes it.
    """

    __slots__ = ("_columns",)

    def __init__(self) -> None:
        self._columns: List["SchemaColumn"] = []

    def __len__(self) -> int:
        return len(self._columns)

    def _register(self, column):
        column.slot = len(self._columns)
        self._columns.append(column)
        return column

    def relation_column(self, relation: Optional[str], name: str, **fields) -> "SchemaColumn":
        """A column read from (or produced as) `relation`: identity `rel_col_…`."""
        from opteryx.types.schema import SchemaColumn
        from opteryx.types.schema import mint_column_identity

        return self._register(
            SchemaColumn(name=name, identity=mint_column_identity(relation, name), **fields)
        )

    def constant(self, name: str, **fields) -> "SchemaColumn":
        """A constant (literal) column: identity `$const_…`."""
        from opteryx.types.schema import ConstantColumn
        from opteryx.types.schema import _mint_tagged_identity

        return self._register(
            ConstantColumn(name=name, identity=_mint_tagged_identity("$const"), **fields)
        )

    def computed(self, column_class, name: str, **fields) -> "SchemaColumn":
        """A computed column (a FunctionColumn or ExpressionColumn): identity
        `$derived_…`."""
        from opteryx.types.schema import _mint_tagged_identity

        return self._register(
            column_class(name=name, identity=_mint_tagged_identity("$derived"), **fields)
        )

    def remint(self, column, relation: Optional[str]) -> "SchemaColumn":
        """A copy of `column` that is a NEW column of `relation`: same metadata,
        fresh identity and slot. `column` is not modified."""
        import copy

        from opteryx.types.schema import mint_column_identity

        fresh = copy.copy(column)
        fresh.identity = mint_column_identity(relation, column.name)
        return self._register(fresh)


class PlanContext:
    __slots__ = ("_statistics", "_cte_statistics", "scan_stats_cache", "columns")

    def __init__(self) -> None:
        # The query's bound columns — see ColumnTable. Created with the context at
        # the start of planning, before anything mints a column.
        self.columns = ColumnTable()
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
