# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The native expression arena holds what the expression objects say.

Every expression is a row of its query's arena (src/cpp/planner/expr_arena.hpp)
and its Python object is a view of that row (native plan graph P3-d). These
tests plan real queries and compare, for every expression left in the optimized
plan, the arena's row with the object: kind, name, children, naming, bound
column, relations, flags and - for a literal - its type and native value.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import decimal

import pytest

import opteryx.planner.physical_planner as physical_planner
from opteryx.compiled.structures import expressions as ex
from opteryx.compiled.structures.expressions import is_expression

_BINARY = lambda e: {"left": e.left, "right": e.right}  # noqa: E731

# Each kind's child fields, as the row records them: single children by field,
# list children as tuples.
_CHILDREN = {
    ex.Comparison: _BINARY,
    ex.BinaryOperator: _BINARY,
    ex.ExtractionOperator: _BINARY,
    ex.And: _BINARY,
    ex.Or: _BINARY,
    ex.Xor: _BINARY,
    ex.UnaryOperator: lambda e: {"centre": e.centre, "parameters": e.parameters},
    ex.Not: lambda e: {"centre": e.centre},
    ex.Nested: lambda e: {"centre": e.centre},
    ex.Dnf: lambda e: {"parameters": e.parameters},
    ex.Cnf: lambda e: {"parameters": e.parameters},
    ex.Between: lambda e: {"left": e.left, "right": e.right, "centre": e.centre},
    ex.Case: lambda e: {"conditions": e.conditions, "results": e.results, "else_result": e.else_result},
    ex.Cast: lambda e: {"left": e.left, "parameters": e.parameters, "format": e.format},
    ex.Function: lambda e: {"parameters": e.parameters},
    ex.Aggregator: lambda e: {"parameters": e.parameters},
}
_NAMED = (ex.Comparison, ex.BinaryOperator, ex.ExtractionOperator, ex.And, ex.Or, ex.Xor,
          ex.UnaryOperator, ex.Cast, ex.Function, ex.Aggregator)


def _id(expression):
    return 0 if expression is None else expression.expr_id


def _native_literal(value, column_type):
    """What the arena records for a literal value (see _literal_from_native)."""
    if column_type is None:
        return ("none",)
    if type(value) is tuple:
        if column_type.category.name == "INTERVAL":
            return value
        element = column_type.element or column_type
        return tuple(_native_literal(item, element) for item in value)
    if type(value) is decimal.Decimal:
        sign, digits, exponent = value.as_tuple()
        unscaled = int("".join(str(d) for d in digits) or "0") * (-1 if sign else 1)
        bits = unscaled & ((1 << 128) - 1)
        lo, hi = bits & 0xFFFFFFFFFFFFFFFF, bits >> 64
        lo = lo - (1 << 64) if lo >= 1 << 63 else lo
        hi = hi - (1 << 64) if hi >= 1 << 63 else hi
        return ("decimal", hi, lo, exponent)
    return value


def _check(expression):
    row = expression.arena.native_row(expression.expr_id)
    assert row["origin"] == expression.origin_id
    assert row["kind"] == expression.node_type.value
    assert row["alias"] == expression.alias
    assert row["query_column"] == expression.query_column
    column = expression.schema_column
    assert row["column_slot"] == (None if column is None else column.slot)
    assert row["relations"] == frozenset(expression.relations or ())
    kind = type(expression)
    children = _CHILDREN.get(kind)
    if children is not None:
        for field, child in children(expression).items():
            if type(child) is tuple or child is None and field in ("parameters", "conditions", "results"):
                assert row[field] == tuple(_id(c) for c in child or ()), field
            else:
                assert row[field] == _id(child), field
    if kind in _NAMED:
        assert row["value"] == (expression.value or ""), "value"
    if kind is ex.Literal:
        type_id = None if expression.type is None else expression.type.type_id
        assert row["type_id"] == type_id
        assert row["literal"] == _native_literal(expression.value, expression.type)
    elif kind is ex.LogicalColumn:
        assert row["source"] == (expression.source or "")
        assert row["source_column"] == (expression.source_column or "")
    elif kind is ex.Aggregator:
        assert row["order"] == tuple((child.expr_id, ascending) for child, ascending in expression.order or ())


def _plans_of(statement):
    plans = []
    original = physical_planner.create_physical_plan

    def capture(plan, properties, plan_context):
        plans.append(plan)
        return original(plan, properties, plan_context)

    physical_planner.create_physical_plan = capture
    try:
        for _ in opteryx.session().execute_to_morsels(statement):
            pass
    finally:
        physical_planner.create_physical_plan = original
    return plans


def _expressions(plan):
    seen = {}
    stack = []
    for _, step in plan.nodes(True):
        stack.extend(step.expressions(True))
    while stack:
        expression = stack.pop()
        if not is_expression(expression) or id(expression) in seen:
            continue
        seen[id(expression)] = expression
        stack.extend(expression.children())
    return list(seen.values())


import opteryx  # noqa: E402  (after the path insert)

STATEMENTS = [
    "SELECT name, id * 2 AS twice FROM $planets WHERE id BETWEEN 2 AND 5 AND name LIKE 'M%'",
    "SELECT p.name, COUNT(*) FROM $planets p JOIN testdata.satellites s ON p.id = s.planetId GROUP BY p.name",
    "SELECT name FROM $planets WHERE id IN (1, 2, 3) OR name = 'Pluto'",
    "SELECT CASE WHEN id > 3 THEN 'far' ELSE 'near' END AS reach FROM $planets",
    "SELECT name FROM $planets WHERE id IN (SELECT planetId FROM testdata.satellites WHERE radius > 100)",
    "SELECT CAST(id AS VARCHAR), 1.5, TRUE, NULL, CAST('2024-01-01' AS DATE), INTERVAL '1' MONTH, CAST('1.25' AS DECIMAL(10, 2)) FROM $planets",
    "SELECT id % 2 AS parity, ARRAY_AGG(name ORDER BY name DESC) FROM $planets GROUP BY id % 2",
    "SELECT name FROM $planets WHERE NOT EXISTS (SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)",
]


@pytest.mark.parametrize("statement", STATEMENTS)
def test_every_row_matches_its_expression(statement):
    plans = _plans_of(statement)
    assert plans, "the statement produced no plan"
    checked = 0
    for plan in plans:
        for expression in _expressions(plan):
            _check(expression)
            checked += 1
    assert checked > 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
