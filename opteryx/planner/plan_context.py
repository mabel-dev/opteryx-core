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
    from opteryx.types.schema import RelationSchema
    from opteryx.types.schema import SchemaColumn


_KEEP = object()  # alias(): "keep the source column's type"


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

    A column seen under a different NAME in another scope (a subquery or view
    output, a SELECT alias) is a new slot too - `alias` makes it - so that every
    slot has exactly one name set (architect ruling 2026-09-27, native plan
    graph P1). The alias keeps its source's identity: identity is the STREAM key,
    the handle the data is carried under, and an alias carries no data of its
    own. `alias_of` records the slot it renames.
    """

    __slots__ = ("_columns", "_slot_of", "_alias_of")

    def __init__(self) -> None:
        self._columns: List["SchemaColumn"] = []
        # identity -> ROOT slot (the slot that minted the identity). Aliases share
        # their root's identity and never replace this entry.
        self._slot_of: Dict[bytes, int] = {}
        # slot -> the slot it renames; absent for a root slot.
        self._alias_of: Dict[int, int] = {}

    def __len__(self) -> int:
        return len(self._columns)

    def _next_slot(self) -> int:
        return len(self._columns)

    def _register(self, column):
        """Record a ROOT column constructed with `slot=self._next_slot()`."""
        self._columns.append(column)
        self._slot_of[column.identity] = column.slot
        return column

    def alias_of(self, slot: int) -> Optional[int]:
        """The slot `slot` renames, or None for a root slot."""
        return self._alias_of.get(slot)

    def root_slot(self, slot: int) -> int:
        """The root of `slot`'s alias chain - the slot that minted its identity."""
        while slot in self._alias_of:
            slot = self._alias_of[slot]
        return slot

    def relation_column(self, relation: Optional[str], name: str, **fields) -> "SchemaColumn":
        """A column read from (or produced as) `relation`: identity `rel_col_…`."""
        from opteryx.types.schema import SchemaColumn
        from opteryx.types.schema import mint_column_identity

        return self._register(
            SchemaColumn(
                name=name,
                identity=mint_column_identity(relation, name),
                slot=self._next_slot(),
                **fields,
            )
        )

    def constant(self, name: str, **fields) -> "SchemaColumn":
        """A constant (literal) column: identity `$const_…`."""
        from opteryx.types.schema import ConstantColumn
        from opteryx.types.schema import _mint_tagged_identity

        return self._register(
            ConstantColumn(
                name=name,
                identity=_mint_tagged_identity("$const"),
                slot=self._next_slot(),
                **fields,
            )
        )

    def computed(self, column_class, name: str, **fields) -> "SchemaColumn":
        """A computed column (a FunctionColumn or ExpressionColumn): identity
        `$derived_…`."""
        from opteryx.types.schema import _mint_tagged_identity

        return self._register(
            column_class(
                name=name,
                identity=_mint_tagged_identity("$derived"),
                slot=self._next_slot(),
                **fields,
            )
        )

    def remint(self, column, relation: Optional[str], **fields) -> "SchemaColumn":
        """A copy of `column` that is a NEW column of `relation`: same metadata
        except `fields`, fresh identity and slot. `column` is not modified - a row
        is fixed once minted (architect ruling 2026-09-27), so a caller states
        what differs here rather than writing it afterwards."""
        import dataclasses

        from opteryx.types.schema import mint_column_identity

        return self._register(
            dataclasses.replace(
                column,
                identity=mint_column_identity(relation, column.name),
                slot=self._next_slot(),
                **fields,
            )
        )

    def alias(
        self, column, name: str, *, aliases=None, origin=None, column_type=_KEEP
    ) -> "SchemaColumn":
        """`column` seen under another name in another scope: a NEW slot with its
        own name set, the same identity (the stream key) and type facts, and
        `alias_of` pointing at `column`'s slot. `column` is not modified.

        `column_type` retypes the alias - a set operation's output settled to the
        type its legs were coerced to (architect ruling 2026-09-27: a retype is a
        retyped alias row, so each slot keeps one type)."""
        import copy

        renamed = copy.copy(column)
        renamed.name = name
        renamed.aliases = list(aliases) if aliases is not None else []
        renamed.origin = list(origin) if origin is not None else None
        if column_type is not _KEEP:
            renamed.column_type = column_type
        renamed.slot = self._next_slot()
        self._columns.append(renamed)
        self._alias_of[renamed.slot] = column.slot
        return renamed

    def retype(self, column, column_type) -> "SchemaColumn":
        """`column` settled to `column_type`: a retyped alias row - a new slot with
        `column`'s identity and names, `alias_of` its slot (architect ruling
        2026-09-27: a retype is a retyped alias row, so each slot keeps one type).
        `column` is not modified; the caller puts the new row wherever the settled
        type must be seen. A column already of `column_type` is returned as is:
        it is already that row."""
        if column.column_type == column_type:
            return column
        return self.alias(
            column,
            column.name,
            aliases=column.aliases,
            origin=column.origin,
            column_type=column_type,
        )

    def reference(self, identity: bytes, name: str, column_type) -> "SchemaColumn":
        """A plain column REFERRING to the already-minted column `identity` - its
        identity and slot, with the name and type the caller reads it under (e.g.
        a join key the compiler casts). An identity this query never minted is
        refused: it would be a column from some other binding."""
        from opteryx.exceptions import InvalidInternalStateError
        from opteryx.types.schema import SchemaColumn

        slot = self._slot_of.get(identity)
        if slot is None:
            raise InvalidInternalStateError(
                f"Column {name!r} ({identity!r}) was not minted in this query's column table."
            )
        return SchemaColumn(name=name, identity=identity, column_type=column_type, slot=slot)

    def bind_relation(self, descriptor, alias: str) -> "RelationSchema":
        """Bind a source's `RelationDescriptor` as relation `alias`: every column
        becomes a bound column of `alias` minted here, its origin `alias`.

        A source that hands over anything but a descriptor is refused - a bound
        schema from a connector would carry another binding's identities into this
        query (architect ruling 2026-09-26: no dual path)."""
        from opteryx.exceptions import InvalidInternalStateError
        from opteryx.types.schema import RelationDescriptor
        from opteryx.types.schema import RelationSchema

        if type(descriptor) is not RelationDescriptor:
            raise InvalidInternalStateError(
                f"Relation '{alias}' was described by a {type(descriptor).__name__}; "
                "sources describe relations with a RelationDescriptor and only the "
                "binder makes bound columns."
            )
        columns = []
        for column in descriptor.columns:
            bound = self.relation_column(
                alias,
                column.name,
                column_type=column.column_type,
                nullable=column.nullable,
                field_id=column.field_id,
                origin=[alias],
            )
            columns.append(bound)
        return RelationSchema(
            name=descriptor.name,
            columns=columns,
            aliases=list(descriptor.aliases),
            primary_key=descriptor.primary_key,
            row_count_metric=descriptor.row_count_metric,
            row_count_estimate=descriptor.row_count_estimate,
            data_size_metric=descriptor.data_size_metric,
            data_size_estimate=descriptor.data_size_estimate,
        )

    def adopt(self, column, owner, *, column_type=_KEEP) -> "SchemaColumn":
        """`column` standing in for `owner` (e.g. a folded literal answering the
        aggregate it replaces): `column`'s kind and metadata carried under
        `owner`'s identity - the stream key consumers match on - as a NEW slot
        aliasing `owner`'s, typed `column_type` when given. A new slot, because a
        slot is one row with one kind (architect ruling 2026-09-27); neither
        argument is modified."""
        import copy

        adopted = copy.copy(column)
        adopted.identity = owner.identity
        if column_type is not _KEEP:
            adopted.column_type = column_type
        adopted.slot = self._next_slot()
        self._columns.append(adopted)
        self._alias_of[adopted.slot] = owner.slot
        return adopted


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
