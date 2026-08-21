# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A cast to a parameterized ARRAY type must keep its element type through the planner.

sqlparser hands `ARRAY<VARCHAR>` back as `{"Array": {"AngleBracket": {"Varchar": None}}}`.
`_extract_data_type` flattens a dict-shaped data_type to its top-level key, so the element
type survives only because it is copied into `cast_parameters` — the same channel VECTOR's
width and DECIMAL's precision/scale use. It was then dropped again by the literal-fold
shortcut, which sees only the bare name "ARRAY": `ValueError: parse_column_type: unknown
type 'ARRAY'`.

Two routes now, split on the SOURCE literal, because only some sources are readable by the
native kernel (`draken_cast_to_array`, tests/sql/test_cast_to_array.py):

  * array-literal / NULL source -> FOLDED. The kernel reads elements from the column
    owner's CHILD vector; such a literal has no child, so it cannot see its own input and
    silently returns empty arrays. Folding is the only way these run — the same reason
    VECTOR folds. The fold is a RETYPE, never an element-by-element conversion, so the
    kernel's rule 3 (mismatch fails; no implicit stringification / parsing / truncation)
    and rule 4 (plain `::` raises, TRY_ nulls) hold identically on both routes.
  * every other literal source, and every column -> runtime CAST node carrying
    `parameters=[element]`, which the binder reads to build ARRAY<element>.
"""

import os
import sys
import uuid

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.models import ExecutionContext, QueryTelemetry
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.binder import do_bind_phase
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.plan_rewriter import do_plan_rewrite
from opteryx.planner.relation_resolver import do_resolve_relations
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide
from opteryx.types.logical_type import INT64, VARCHAR

# Sources that reach the native kernel — the cast stays a runtime CAST node.
RUNTIME_SOURCES = [
    "SELECT name::ARRAY<VARCHAR> AS v FROM $planets",
    "SELECT CAST(name AS ARRAY<VARCHAR>) AS v FROM $planets",
    """SELECT '["a","b"]'::ARRAY<VARCHAR> AS v FROM $planets""",
]

# Sources the kernel cannot read — the cast folds to a typed literal.
FOLDED_SOURCES = [
    "SELECT ['a','b']::ARRAY<VARCHAR> AS v FROM $planets",
    "SELECT CAST(['a','b'] AS ARRAY<VARCHAR>) AS v FROM $planets",
    "SELECT NULL::ARRAY<VARCHAR> AS v FROM $planets",
]


def _logical_plan(sql: str):
    telemetry = QueryTelemetry.detached()
    plan, _, ctes = do_logical_planning_phase(
        do_ast_rewriter(
            sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx"), parameters=[]
        )[0]
    )
    plan = do_resolve_relations(plan, ctes, telemetry)
    return do_plan_rewrite(plan, telemetry), telemetry


def _build_projection_expression(sql: str):
    """Build the single projected expression, bypassing plan-level projection rules."""
    from opteryx.planner.logical_planner import logical_planner_builders

    projected = sqloxide.parse_sql(do_sql_rewrite(sql), _dialect="opteryx")[0]["Query"]["body"][
        "Select"
    ]["projection"][0]
    expression = projected.get("UnnamedExpr")
    if expression is None:
        expression = projected["ExprWithAlias"]["expr"]
    return logical_planner_builders.build(expression)


def _projection(plan, node_type):
    for _, node in plan.nodes(True):
        for column in node.columns or []:
            if column.node_type == node_type and column.alias == "v":
                return column
    return None


def _bound_cast_type(sql: str):
    """The bound ColumnType of a runtime CAST node."""
    plan, telemetry = _logical_plan(sql)
    bound = do_bind_phase(
        plan,
        execution_context=ExecutionContext(),
        query_id=str(uuid.uuid4()),
        telemetry=telemetry,
    )
    node = _projection(bound, NodeType.CAST)
    assert node is not None, f"no bound CAST node aliased 'v' for {sql!r}"
    return node.schema_column.column_type


def _rows(sql: str):
    values = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        morsel.materialize()
        values += morsel.column("v").to_pylist()
    return values


# ---------------------------------------------------------------------------
# The element type survives the planner on both routes.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sql", RUNTIME_SOURCES)
def test_runtime_cast_carries_the_element_type_in_parameters(sql):
    node = _projection(_logical_plan(sql)[0], NodeType.CAST)
    assert node is not None, f"expected a runtime CAST node for {sql!r}"
    assert node.value == "ARRAY", node.value
    assert [p.value for p in node.parameters] == ["VARCHAR"], node.parameters


@pytest.mark.parametrize("sql", RUNTIME_SOURCES)
def test_runtime_cast_binds_to_the_declared_element_type(sql):
    column_type = _bound_cast_type(sql)
    assert str(column_type) == "ARRAY<VARCHAR>", str(column_type)
    assert column_type.element == VARCHAR, column_type.element


@pytest.mark.parametrize("sql", FOLDED_SOURCES)
def test_folded_literal_carries_the_element_type_in_its_type(sql):
    """A folded cast keeps ARRAY<element> on the literal — the element type is the whole
    point, so folding to an untyped literal would re-lose exactly what was being fixed.

    Built directly from the expression AST: an array literal cannot be a bare projection
    (see the parenthesised-values guard in logical_planner), and that is orthogonal to
    whether the fold carries the type.
    """
    node = _build_projection_expression(sql)
    assert node.node_type == NodeType.LITERAL, node.node_type
    assert str(node.type) == "ARRAY<VARCHAR>", str(node.type)
    assert node.type.element == VARCHAR, node.type.element


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT name::ARRAY<INTEGER> AS v FROM $planets", "ARRAY<INT64>"),
        ("SELECT name::ARRAY<DOUBLE> AS v FROM $planets", "ARRAY<FLOAT64>"),
        ("SELECT name::ARRAY<BOOLEAN> AS v FROM $planets", "ARRAY<BOOLEAN>"),
    ],
)
def test_element_type_is_read_not_defaulted(sql, expected):
    """Non-VARCHAR elements prove the type is carried, not assumed."""
    assert str(_bound_cast_type(sql)) == expected


def test_integer_element_resolves_to_the_canonical_type():
    assert _bound_cast_type("SELECT name::ARRAY<INTEGER> AS v FROM $planets").element == INT64


# ---------------------------------------------------------------------------
# Execution — the folded route answers the same way the native kernel does.
# ---------------------------------------------------------------------------


def test_null_source_casts_to_a_null_of_the_declared_type():
    assert _rows("SELECT NULL::ARRAY<VARCHAR> AS v FROM $planets LIMIT 1") == [None]
    assert _rows("SELECT NULL::ARRAY<INTEGER> AS v FROM $planets LIMIT 1") == [None]


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("(['a','b']::ARRAY<VARCHAR>)[0]", "a"),
        ("(['a','b']::ARRAY<VARCHAR>)[1]", "b"),
        ("LENGTH(['a','b']::ARRAY<VARCHAR>)", 2),
        ("LENGTH([1.0,2.0,3.0]::ARRAY<DOUBLE>)", 3),
    ],
)
def test_folded_array_literal_cast_keeps_its_elements(expression, expected):
    """The fold exists because the native kernel cannot read a childless literal — it
    returns empty arrays rather than refusing. Elements surviving is the whole point."""
    assert _rows(f"SELECT {expression} AS v FROM $planets LIMIT 1") == [expected]


@pytest.mark.parametrize(
    "expression",
    [
        "['1','2']::ARRAY<INTEGER>",  # no implicit parsing
        "[1.0,2.0]::ARRAY<VARCHAR>",  # no implicit stringification
    ],
)
def test_element_mismatch_fails_loud_on_the_folded_route(expression):
    """Kernel rule 3 holds identically here: an array literal is retyped, never converted
    element-by-element. Diverging would answer the same question two ways."""
    with pytest.raises(UnsupportedSyntaxError, match="element does not match"):
        _rows(f"SELECT LENGTH({expression}) AS v FROM $planets LIMIT 1")


def test_try_cast_nulls_an_element_mismatch_instead_of_raising():
    """Kernel rule 4: plain `::` raises, TRY_ nulls the row."""
    assert _rows("SELECT TRY_CAST(['1','2'] AS ARRAY<INTEGER>) AS v FROM $planets LIMIT 1") == [
        None
    ]


def test_json_text_literal_still_routes_to_the_native_kernel():
    """A VARCHAR literal holding JSON array text IS readable by the kernel, so it must not
    be folded — folding it would be a second, Python-side draken_cast_to_array."""
    assert _rows("""SELECT '["a","b"]'::ARRAY<VARCHAR> AS v FROM $planets LIMIT 1""") == [
        ["a", "b"]
    ]


# ---------------------------------------------------------------------------
# The parenthesised-values guard. `CAST(NULL AS ARRAY<E>)` folds to a LITERAL of category
# ARRAY, which trips that guard, so it was exempted by absence of a value. These pin what
# the exemption must NOT let through — the guard is load-bearing, and narrowing it further
# converts one clean error into a TypeError leak and a silently-empty array.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT (id, name) FROM $planets",
        "SELECT (1, 2) FROM $planets",
        "SELECT DISTINCT (id, name) FROM $planets",
        "SELECT DISTINCT (1, 2) FROM $planets",
    ],
)
def test_parenthesised_value_list_is_still_refused(sql):
    """The guard's original purpose — `SELECT (a, b)` is not a row constructor here."""
    with pytest.raises(UnsupportedSyntaxError, match="cannot be projected"):
        _rows(f"{sql} LIMIT 1")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ['a','b'] AS v FROM $planets",
        "SELECT ['a','b']::ARRAY<VARCHAR> AS v FROM $planets",
        "SELECT [1.0,2.0]::VECTOR(2) AS v FROM $planets",
    ],
)
def test_array_literal_cannot_be_projected_bare(sql):
    """An ARRAY/VECTOR literal with values still cannot be a bare projection.

    Not a preference — its materialization is broken three separate ways (a TypeError on
    the plain literal, an engine error on VECTOR, an empty array on a folded ARRAY), and
    this refusal is the only thing keeping all three a single clean error. Pinned so the
    guard is not narrowed before ARRAY-literal materialization is actually fixed.
    """
    with pytest.raises(UnsupportedSyntaxError, match="cannot be projected"):
        _rows(f"{sql} LIMIT 1")


@pytest.mark.parametrize(
    "target",
    [
        "ARRAY<ARRAY<VARCHAR>>",  # two dimensions — ruled out of scope, not supported
        "ARRAY<DECIMAL(10,2)>",  # parameterized element — same flattening, one level deeper
    ],
)
def test_unsupported_element_types_fail_loud(target):
    """`_extract_data_type` keeps only the element's top-level AST key, so a nested or
    parameterized element arrives as a bare name and no type can be built.

    Pinned as "raises", deliberately NOT as a message: these currently surface the raw
    `parse_column_type: unknown type` internals, and improving that text must not have to
    fight a test.
    """
    with pytest.raises(Exception):
        _rows(f"SELECT CAST(name AS {target}) AS v FROM $planets LIMIT 1")


def test_scalar_source_is_still_refused():
    """Kernel rule 1 — a scalar is never wrapped into a one-element array. Folding must not
    have opened a side door around the refusal."""
    with pytest.raises(Exception) as err:
        _rows("SELECT 1::ARRAY<INTEGER> AS v FROM $planets LIMIT 1")
    assert "ARRAY" in str(err.value)
    assert "unknown type" not in str(err.value)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
