# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
JSONL Nested Extraction Strategy
================================

Goal — read `col ->> 'key'` / `col -> 'key'` out of a READ_JSONL object column at
SCAN time, instead of materialising the whole object and re-parsing it per row.

``SELECT commit ->> 'collection' ... FROM READ_JSONL(...)`` read every record's whole
``commit`` object as VARIANT text, then parsed that text again (yyjson, once per
extraction) in execution. rugo can read one level into an object directly — the
column named ``commit->>'collection'`` is the sub-value, found by the same marker walk
that bounds the container (rugo/src/jsonl/core/nested_column.hpp) — and renders it
byte-identically to draken's ``->`` / ``->>`` (verified against them; see
tests/rugo/test_jsonl_nested_columns.py).

What this strategy does
-----------------------
Each eligible extraction becomes a column the scan emits, under the extraction's OWN
bound identity:

* the extraction node, wherever it appears in the plan, is replaced by an IDENTIFIER
  reading that identity (so nothing above the scan computes it any more);
* the READ_JSONL step gains the column — in its schema (which is what makes it the
  column's origin for predicate pushdown), its columns, and its physical-name map,
  as ``<physical>->>'<key>'`` / ``<physical>->'<key>'``.

Runs BEFORE predicate and projection pushdown, so the ordinary machinery does the
rest: ``commit ->> 'operation' = 'create'`` is now a column-vs-literal comparison
that pushes into rugo's inline filter, and ``commit`` itself drops out of the read
once no raw use of it is left.

Eligibility — the answer can never change
-----------------------------------------
* The operand is a READ_JSONL column bound VARIANT: rugo materialises exactly a
  JSON object (or array) for it, so walking into it is walking into the same value
  draken would parse. A VARCHAR column holding JSON TEXT is a string to rugo, and
  is left alone.
* The path is ONE object-key token in draken's resolution (draken/ops/json_path.h):
  not empty, no ``$`` or ``/`` prefix, no ``.`` or ``[`` (each would split it into
  several tokens), and not an RFC 6901 array index (``0`` | ``[1-9][0-9]*``), which
  on an array container would select an element where rugo's object-only walk
  yields NULL. Anything else is simply not pushed — still evaluated as before.
* The extraction is bound (it carries its output schema column).
"""

from draken.draken_native import DrakenType

from opteryx.compiled.structures.expressions import expressions_with
from opteryx.compiled.structures.expressions import rewrite_children
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import PlanStep

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext

_OPERATORS = {"LongArrow": "->>", "Arrow": "->"}

# RFC 6901 array-index grammar, as draken's jsonptr_parse_index applies it.
_INDEX_MAX = 0xFFFFFFFF


def _is_array_index(token: str) -> bool:
    if not token or len(token) > 10 or any(c not in "0123456789" for c in token):
        return False
    if token[0] == "0":
        return len(token) == 1
    return int(token) < _INDEX_MAX


def _single_key(path):
    """The key, if draken resolves the path literal to exactly one object-key token;
    else None. VARCHAR literals are bound as UTF-8 bytes (draken resolves the same
    bytes — compiler._fuse_json_extractions encodes a str path the same way)."""
    if type(path) is bytes:
        text = path.decode("utf-8", "replace")
        if text.encode("utf-8") != path:
            return None  # not valid UTF-8: not a key rugo can be asked for by name
        path = text
    if type(path) is not str or not path:
        return None
    if path[0] in "$/" or "." in path or "[" in path:
        return None
    return None if _is_array_index(path) else path


def _is_count_distinct_star(aggregate) -> bool:
    """`COUNT(DISTINCT *)` — the binder's whole-row-dedup case (binder/aggregate.py)."""
    return (
        aggregate.value == "COUNT"
        and type(aggregate) in expressions_with("duplicate_treatment")
        and aggregate.duplicate_treatment == "Distinct"
        and bool(aggregate.parameters)
        and aggregate.parameters[0].node_type == NodeType.WILDCARD
    )


def _spec(physical: str, operator: str, key: str) -> str:
    """rugo's nested column name: `<physical>->>'<key>'` ('' escapes a quote)."""
    return physical + operator + "'" + key.replace("'", "''") + "'"


class JsonlNestedExtractionStrategy(OptimizationStrategy):
    """Push one-level `->` / `->>` over READ_JSONL object columns into the scan."""

    def visit(self, node: PlanStep, context: OptimizerContext) -> OptimizerContext:
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        """All rewriting happens here, once the tree is settled (never from `visit`)."""
        # identity -> (step nid, physical name) for every READ_JSONL VARIANT column.
        sources = {}
        for nid, node in plan.nodes(True):
            if node.node_type != LogicalPlanStepType.FunctionDataset or node.function != "READ_JSONL":
                continue
            physical = node.jsonl_physical_by_identity or {}
            for column in node.schema.columns:
                column_type = column.column_type
                if (
                    column_type is not None
                    and column_type.physical == DrakenType.VARIANT
                    and column.identity in physical
                ):
                    sources[column.identity] = (nid, physical[column.identity])
        if not sources:
            return plan

        # out identity -> (step nid, rugo spec, bound schema column), one per identity:
        # the WHERE copy and the SELECT copy of one expression share it.
        pushed = {}
        rewritten = {}

        def _rewrite(expression):
            done = rewritten.get(id(expression))
            if done is not None:
                return done
            done = None
            if expression.node_type == NodeType.EXTRACTION_OPERATOR and expression.value in _OPERATORS:
                operand, path = expression.left, expression.right
                out_column = expression.schema_column
                if (
                    operand is not None
                    and operand.node_type == NodeType.IDENTIFIER
                    and operand.schema_column is not None
                    and operand.schema_column.identity in sources
                    and path is not None
                    and path.node_type == NodeType.LITERAL
                    and out_column is not None
                ):
                    key = _single_key(path.value)
                else:
                    key = None
                if key is not None:
                    nid, physical = sources[operand.schema_column.identity]
                    pushed.setdefault(
                        out_column.identity,
                        (nid, _spec(physical, _OPERATORS[expression.value], key), out_column),
                    )
                    done = LogicalColumn(
                        node_type=NodeType.IDENTIFIER,
                        source_column=out_column.name,
                        source=operand.source,
                        alias=expression.alias,
                        schema_column=out_column,
                        query_column=expression.query_column,
                        arena=expression.arena,
                    )
            if done is None:
                done = rewrite_children(expression, _rewrite, share=True)
            rewritten[id(expression)] = done
            return done

        for _, node in plan.nodes(True):
            node.map_expressions(_rewrite)

        if not pushed:
            return plan

        # An aggregate's `columns` is bookkeeping DERIVED from its groups/aggregates
        # (binder/aggregate.py: the aggregates plus every IDENTIFIER they and the groups
        # reference) and projection pushdown reads demand from it. The rewrite changed
        # what the groups reference — the pushed column instead of the object — so it is
        # re-derived the binder's way, or the object stays "read" for nothing. The one
        # case the binder widens, `COUNT(DISTINCT *)` (every column in scope is part of
        # the dedup key), is left exactly as bound.
        pushed_identities = set(pushed)
        for _, node in plan.nodes(True):
            if node.node_type not in (
                LogicalPlanStepType.Aggregate,
                LogicalPlanStepType.AggregateAndGroup,
            ):
                continue
            fields = node.field_values()
            aggregates = list(fields.get("aggregates") or [])
            groups = list(fields.get("groups") or [])
            referenced = get_all_nodes_of_type(aggregates + groups, select_nodes=(NodeType.IDENTIFIER,))
            if not any(c.schema_column.identity in pushed_identities for c in referenced):
                continue
            if any(_is_count_distinct_star(aggregate) for aggregate in aggregates):
                continue
            node.columns = aggregates + referenced

        # The READ_JSONL steps emit the pushed columns: schema (origin, for predicate
        # pushdown), columns, and the physical name rugo is asked for.
        by_step = {}
        for out_identity, (nid, spec, out_column) in pushed.items():
            by_step.setdefault(nid, []).append((out_identity, spec, out_column))
        for nid, additions in by_step.items():
            node = plan[nid]
            physical = dict(node.jsonl_physical_by_identity or {})
            for out_identity, spec, _ in additions:
                physical[out_identity] = spec
            node.jsonl_physical_by_identity = physical
            node.schema = node.schema.with_columns(
                list(node.schema.columns) + [out_column for _, _, out_column in additions]
            )
            node.columns = list(node.columns or []) + [
                LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source_column=out_column.name,
                    schema_column=out_column,
                    arena=context.plan_context.expressions,
                )
                for _, _, out_column in additions
            ]
            plan[nid] = node
            self.telemetry.increase("optimization_jsonl_nested_extraction", len(additions))

        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        for _, node in plan.nodes(True):
            if node.node_type == LogicalPlanStepType.FunctionDataset and node.function == "READ_JSONL":
                return True
        return False
