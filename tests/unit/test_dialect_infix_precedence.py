"""
Parser-level precedence tests for the infix operators in the Opteryx dialect
(`src/opteryx_dialect.rs`). Two separate concerns:

1. Custom operators (`DIV`, `@>`, `@>>`, `<<=`, `>>=`) parse their right operand
   with `parse_subexpr(precedence)`. The bug guarded against is `parse_expr()`,
   which consumes the entire remaining expression regardless of binding power,
   so `a @> ['x'] AND b = 1` bound as `a @> (['x'] AND b = 1)` and `a DIV 2 = 1`
   bound as `a DIV (2 = 1)` - integer division by a boolean, a silently wrong
   query rather than an error.

2. The accessor / containment family (`->`, `->>`, `@>`, `@>>`, `@?`) is given a
   precedence above the cast/subscript band by `get_next_precedence`. At the old
   `PgOther` rating the right operand swallowed any trailing `=`, `LIKE`,
   `IS NULL`, arithmetic or cast. Raising it also made
   `ast_rewriter.rewrite_json_accessors` - which re-associated these downstream -
   redundant, and it has been deleted.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compute import parse_sql
from opteryx.compute import restore_ast
from opteryx.expression import format_expression
from opteryx.planner.logical_planner.logical_planner_builders import build


def _selection(sql: str):
    """Return the WHERE-clause expression node of a single SELECT."""
    ast = parse_sql(sql, "opteryx")
    return ast[0]["Query"]["body"]["Select"]["selection"]


def _projection(sql: str):
    """Return the first projection expression node of a single SELECT."""
    ast = parse_sql(sql, "opteryx")
    return ast[0]["Query"]["body"]["Select"]["projection"][0]["UnnamedExpr"]


def _op(node) -> str:
    """Return the operator of a BinaryOp node, unwrapping Custom operators.

    A Custom operator carries its SQL spelling (`@>>`), not its canonical name -
    that is what makes serialising an AST back to SQL round-trip. The logical
    planner maps the spelling to the name; these tests are upstream of that.
    """
    op = node["BinaryOp"]["op"]
    return op["Custom"] if isinstance(op, dict) else op


def _shape(node) -> str:
    """Render an expression as a fully parenthesised operator tree."""
    if not isinstance(node, dict):
        return "?"
    if "BinaryOp" in node:
        binop = node["BinaryOp"]
        return f"({_shape(binop['left'])} {_op(node)} {_shape(binop['right'])})"
    if "Identifier" in node:
        return node["Identifier"]["value"]
    if "Nested" in node:
        return _shape(node["Nested"])
    if "Array" in node:
        return "ARRAY"
    if "Cast" in node:
        data_type = node["Cast"]["data_type"]
        if isinstance(data_type, dict):
            data_type = next(iter(data_type))
        return f"CAST({_shape(node['Cast']['expr'])} AS {data_type})"
    if "JsonAccess" in node:
        return f"SUBSCRIPT({_shape(node['JsonAccess']['value'])})"
    if "IsNull" in node:
        return f"ISNULL({_shape(node['IsNull'])})"
    if "Like" in node:
        like = node["Like"]
        return f"LIKE({_shape(like['expr'])}, {_shape(like['pattern'])})"
    if "Value" in node:
        value = node["Value"]["value"]
        if "Number" in value:
            return value["Number"][0]
        if "SingleQuotedString" in value:
            return "'" + value["SingleQuotedString"] + "'"
        return "LITERAL"
    return "?"


# --- the bug: a trailing AND must not be swallowed into the right operand -----


def test_at_arrow_does_not_swallow_trailing_and():
    node = _selection("SELECT * FROM t WHERE a @> ['x'] AND b = 1")
    assert _op(node) == "And", _shape(node)
    assert _shape(node) == "((a AtArrow ARRAY) And (b Eq 1))"


def test_at_double_arrow_does_not_swallow_trailing_and():
    node = _selection("SELECT * FROM t WHERE a @>> ['x'] AND b = 1")
    assert _op(node) == "And", _shape(node)
    assert _shape(node) == "((a @>> ARRAY) And (b Eq 1))"


def test_div_does_not_swallow_trailing_and():
    node = _selection("SELECT * FROM t WHERE a DIV 2 = 1 AND b = 1")
    assert _op(node) == "And", _shape(node)
    # `a DIV (2 = 1)` - division by a boolean - was the old, silently wrong parse
    assert _shape(node) == "(((a MyIntegerDivide 2) Eq 1) And (b Eq 1))"


def test_operators_do_not_swallow_trailing_or():
    node = _selection("SELECT * FROM t WHERE a @> ['x'] OR b @>> ['y']")
    assert _op(node) == "Or", _shape(node)
    assert _shape(node) == "((a AtArrow ARRAY) Or (b @>> ARRAY))"


# --- each operator still parses correctly on its own -------------------------


def test_at_arrow_alone():
    node = _selection("SELECT * FROM t WHERE a @> ['x']")
    assert _shape(node) == "(a AtArrow ARRAY)"


def test_at_double_arrow_alone():
    node = _selection("SELECT * FROM t WHERE a @>> ['x']")
    assert _shape(node) == "(a @>> ARRAY)"


def test_div_alone():
    node = _selection("SELECT * FROM t WHERE a DIV 2 = 1")
    assert _shape(node) == "((a MyIntegerDivide 2) Eq 1)"


def test_ip_contained_by_alone():
    # the reference implementation these three were made consistent with
    node = _selection("SELECT * FROM t WHERE a <<= '10.0.0.0/8'")
    assert _shape(node) == "(a <<= '10.0.0.0/8')"


def test_ip_contains_does_not_swallow_trailing_and():
    node = _selection("SELECT * FROM t WHERE a >>= '10.0.0.0/8' AND b = 1")
    assert _op(node) == "And", _shape(node)
    assert _shape(node) == "((a >>= '10.0.0.0/8') And (b Eq 1))"


# --- chains bind sensibly ----------------------------------------------------


def test_div_chain_is_left_associative():
    node = _selection("SELECT * FROM t WHERE a DIV 2 DIV 3 = 1")
    assert _shape(node) == "(((a MyIntegerDivide 2) MyIntegerDivide 3) Eq 1)"


def test_div_binds_tighter_than_plus():
    node = _selection("SELECT * FROM t WHERE 1 + a DIV 2 = 3")
    assert _shape(node) == "((1 Plus (a MyIntegerDivide 2)) Eq 3)"


def test_array_operator_chain_is_left_associative():
    node = _selection("SELECT * FROM t WHERE a @>> ['x'] AND b @> ['y'] AND c = 1")
    assert _shape(node) == "(((a @>> ARRAY) And (b AtArrow ARRAY)) And (c Eq 1))"


def test_parentheses_still_force_the_wide_grouping():
    # the old (wrong) parse must remain reachable when explicitly written
    node = _selection("SELECT * FROM t WHERE a @> (['x'] AND b = 1)")
    assert _op(node) == "AtArrow", _shape(node)


# --- JSON access binds tighter than the cast / subscript band ----------------
#
# Deliberate deviation from Postgres, where `::` outranks `->` and
# `a->>'b'::INTEGER` casts the KEY. See the comment on `get_next_precedence`.


def test_cast_applies_to_the_extraction_not_the_key():
    node = _projection("SELECT a->'b'::VARCHAR FROM t")
    assert _shape(node) == "CAST((a Arrow 'b') AS Varchar)"


def test_long_arrow_cast_applies_to_the_extraction_not_the_key():
    node = _projection("SELECT a->>'b'::INTEGER FROM t")
    assert _shape(node) == "CAST((a LongArrow 'b') AS Integer)"


def test_subscript_applies_to_the_extraction_not_the_key():
    node = _projection("SELECT a->'b'[1] FROM t")
    assert _shape(node) == "SUBSCRIPT((a Arrow 'b'))"


def test_cast_on_the_left_operand_still_binds_first():
    # raising `->` must not steal the cast off its own left operand
    node = _projection("SELECT a::JSON->'b' FROM t")
    assert _shape(node) == "(CAST(a AS JSON) Arrow 'b')"


def test_json_access_alone_is_unchanged():
    node = _projection("SELECT a->'b' FROM t")
    assert _shape(node) == "(a Arrow 'b')"


def test_json_access_chain_is_left_associative():
    node = _projection("SELECT a->'b'->>'c' FROM t")
    assert _shape(node) == "((a Arrow 'b') LongArrow 'c')"


def test_json_access_binds_tighter_than_arithmetic():
    node = _projection("SELECT a->'b' * 2 FROM t")
    assert _shape(node) == "((a Arrow 'b') Multiply 2)"


def test_json_access_still_binds_looser_than_and():
    node = _selection("SELECT * FROM t WHERE a->>'b' = 'x' AND c = 1")
    assert _op(node) == "And", _shape(node)
    assert _shape(node) == "(((a LongArrow 'b') Eq 'x') And (c Eq 1))"


def test_parentheses_still_force_the_cast_onto_the_key():
    node = _projection("SELECT a->('b'::VARCHAR) FROM t")
    assert _shape(node) == "(a Arrow CAST('b' AS Varchar))"


# --- containment operators are in the same precedence band -------------------
#
# `@>`, `@>>` and `@?` are raised alongside `->`/`->>`. These replaced
# ast_rewriter.rewrite_json_accessors, which used to re-associate them
# downstream; it has been deleted.


def test_at_arrow_does_not_swallow_trailing_comparison():
    node = _selection("SELECT * FROM t WHERE a @> ['x'] = true")
    assert _shape(node) == "((a AtArrow ARRAY) Eq LITERAL)"


def test_at_double_arrow_does_not_swallow_trailing_comparison():
    node = _selection("SELECT * FROM t WHERE a @>> ['x'] = true")
    assert _shape(node) == "((a @>> ARRAY) Eq LITERAL)"


def test_at_question_does_not_swallow_trailing_comparison():
    node = _selection("SELECT * FROM t WHERE a @? 'x' = true")
    assert _shape(node) == "((a AtQuestion 'x') Eq LITERAL)"


def test_at_question_does_not_swallow_is_null():
    node = _selection("SELECT * FROM t WHERE a @? 'x' IS NULL")
    assert _shape(node) == "ISNULL((a AtQuestion 'x'))"


def test_at_arrow_does_not_swallow_like():
    node = _selection("SELECT * FROM t WHERE a @> ['x'] LIKE 'y%'")
    assert _shape(node) == "LIKE((a AtArrow ARRAY), 'y%')"


# --- custom operators survive the AST -> SQL round trip -----------------------
#
# A view is STORED by serialising its parsed AST back to SQL
# (ViewManagementNode -> opteryx.compute.restore_ast), so anything the dialect
# parses must also be printable as SQL that re-parses. sqlparser prints
# BinaryOperator::Custom verbatim, so when the variant carried the operator's
# internal name the stored view read `ip IPContainedBy '10/8'` - saved fine,
# unusable forever after. The variant carries the SQL spelling instead.


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM t WHERE ip::IPV4 <<= '141.92.0.0/16' LIMIT 50",
        "SELECT * FROM t WHERE '10.0.0.0/8' >>= ip::IPV4",
        "SELECT * FROM t WHERE a @>> ['x', 'y'] AND b = 1",
        "SELECT * FROM t WHERE a @> ['x'] OR b DIV 2 = 1",
    ],
)
def test_custom_operators_round_trip_to_reparsable_sql(sql):
    once = restore_ast(parse_sql(sql, "opteryx"))[0]
    twice = restore_ast(parse_sql(once, "opteryx"))[0]
    assert once == twice, f"{sql} -> {once} -> {twice}"


def test_round_tripped_predicate_builds_the_same_expression():
    """The spelling must map back to the canonical operator name in the planner."""
    sql = "SELECT * FROM t WHERE ip::IPV4 <<= '10.0.0.0/8'"
    stored = restore_ast(parse_sql(sql, "opteryx"))[0]
    assert format_expression(build(_selection(sql))) == format_expression(
        build(_selection(stored))
    )
