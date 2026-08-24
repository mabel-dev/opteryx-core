"""Helpers for exporting expression syntax capabilities for documentation.

The other reference catalogs partition SQL by clause, operator, function, type,
join, window and variable. Expression SYNTAX - `CAST`, `CASE WHEN`, `BETWEEN`,
`EXISTS`, `IN (subquery)`, `IS DISTINCT FROM`, `SIMILAR TO`, `INTERVAL` literals
- falls through every one of those, so before this catalog existed there was no
entry anywhere for any of them. A reader who took the catalogs as exhaustive
would have concluded the dialect has no CAST and no CASE.

The definitions below are hand-written, as in every sibling catalog, but they
are RECONCILED against `logical_planner_builders.BUILDERS` at export time by
`_check_exhaustive`. Every builder key must be either documented in
`EXPRESSION_DEFINITIONS` or listed in `NON_EXPRESSION_BUILDERS` with the reason
it is not an expression form; a key in neither, or a documented key that no
longer exists, fails the export. That is what makes "this list is exhaustive"
a true statement that stays true - the generator breaks when a builder is added
and nobody writes it up.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

_BUILDERS_MODULE = "opteryx.planner.logical_planner.logical_planner_builders"

# Builder keys that are not user-writable expression syntax. The value is the
# reason, so the exhaustiveness failure that sends a reader here also tells them
# which bucket a new key probably belongs in.
NON_EXPRESSION_BUILDERS: dict[str, str] = {
    # Covered by another catalog.
    "BinaryOp": "Binary operators are catalogued in operators.json.",
    "UnaryOp": "Unary operators are catalogued in unary_ops.json.",
    "IsFalse": "Catalogued in unary_ops.json.",
    "IsNotFalse": "Catalogued in unary_ops.json.",
    "IsNotNull": "Catalogued in unary_ops.json.",
    "IsNotTrue": "Catalogued in unary_ops.json.",
    "IsNull": "Catalogued in unary_ops.json.",
    "IsTrue": "Catalogued in unary_ops.json.",
    "Function": "Function calls are catalogued in function_signatures.json.",
    "Ceil": "CEIL is catalogued in function_signatures.json.",
    "Floor": "FLOOR is catalogued in function_signatures.json.",
    "Extract": "EXTRACT is catalogued in function_signatures.json.",
    "Position": "POSITION is catalogued in function_signatures.json.",
    "Substring": "SUBSTRING is catalogued in function_signatures.json.",
    "Trim": "TRIM/LTRIM/RTRIM are catalogued in function_signatures.json.",
    "Overlay": "OVERLAY is catalogued in function_signatures.json.",
    "Like": "LIKE is catalogued in operators.json.",
    "ILike": "ILIKE is catalogued in operators.json.",
    "RLike": "RLIKE is catalogued in operators.json.",
    # Clause-level constructs, not expressions.
    "All": "A `*` in a select list; the SELECT clause is catalogued in clauses.json.",
    "Wildcard": "A `*` in a select list; catalogued in clauses.json.",
    "QualifiedWildcard": "A `t.*` in a select list; catalogued in clauses.json.",
    "ExprWithAlias": "`expr AS name` - aliasing, catalogued with the SELECT clause.",
    # Names, not syntax forms.
    "Identifier": "A bare column reference.",
    "CompoundIdentifier": "A qualified column reference, `t.c`.",
    # AST plumbing - sqlparser wrapper keys that recurse straight back into build().
    "Expr": "sqlparser wrapper key; recurses into build().",
    "Expressions": "sqlparser wrapper key for an expression list.",
    "Unnamed": "sqlparser wrapper key; recurses into build().",
    "UnnamedExpr": "sqlparser wrapper key; recurses into build().",
    "Value": "sqlparser wrapper key; recurses into build().",
    "value": "sqlparser wrapper key; recurses into build().",
}

EXPRESSION_DEFINITIONS: dict[str, dict[str, Any]] = {
    "ARRAY_LITERAL": {
        "ast_symbols": ["Array"],
        "category": "literal",
        "documentation": (
            "A bracketed list of literals, `['a', 'b']`. Every element must share one "
            "type; a numeric array becomes ARRAY<FLOAT64> because the element width is "
            "not known at parse time."
        ),
        "implementation": f"{_BUILDERS_MODULE}.array",
        "lowering": None,
        "node_type": "LITERAL",
        "notes": (
            "Usable as an operand - the right side of `= ANY(...)`, `IN UNNEST(...)`, "
            "the array containment operators. It CANNOT be projected: `SELECT ['a','b']` "
            "is refused."
        ),
        "status": "active",
        "summary": "Array literal.",
        "syntax_forms": ["['a', 'b']", "ARRAY['a', 'b']"],
    },
    "BETWEEN": {
        "ast_symbols": ["Between"],
        "category": "comparison",
        "documentation": (
            "Inclusive range test. `NOT BETWEEN` is the negation, so its bounds are "
            "STRICT and its connective is OR."
        ),
        "implementation": f"{_BUILDERS_MODULE}.between",
        "lowering": "expr >= low AND expr <= high (negated: expr < low OR expr > high)",
        "node_type": "AND / OR",
        "notes": (
            "Lowered at build time, so the node renders as the rewrite rather than as "
            "the SQL that was written. Any alias must reach the outermost AND/OR node "
            "or the column is named after the rewrite."
        ),
        "status": "active",
        "summary": "Inclusive range test.",
        "syntax_forms": ["expr BETWEEN low AND high", "expr NOT BETWEEN low AND high"],
    },
    "CASE": {
        "ast_symbols": ["Case"],
        "category": "conditional",
        "documentation": (
            "Both the searched form (`CASE WHEN cond THEN ...`) and the simple form "
            "(`CASE operand WHEN value THEN ...`), which is lowered by comparing the "
            "operand to each branch value for equality."
        ),
        "implementation": f"{_BUILDERS_MODULE}.case_when",
        "lowering": "simple form: each WHEN value becomes `operand = value`",
        "node_type": "CASE",
        "notes": "ELSE is optional; its absence yields NULL.",
        "status": "active",
        "summary": "Conditional expression.",
        "syntax_forms": [
            "CASE WHEN cond THEN result [WHEN ...] [ELSE result] END",
            "CASE operand WHEN value THEN result [ELSE result] END",
        ],
    },
    "CAST": {
        "ast_symbols": ["Cast"],
        "category": "conversion",
        "documentation": (
            "Type conversion. `TRY_CAST` and `SAFE_CAST` are the non-raising forms - "
            "they yield NULL where `CAST` raises."
        ),
        "implementation": f"{_BUILDERS_MODULE}.cast",
        "lowering": "a typed function call, `<TYPE>(expr)` / `TRY_<TYPE>(expr)`",
        "node_type": "FUNCTION",
        "notes": (
            "A literal operand is converted at plan time. Parameterised targets take "
            "their parameters through the cast's parameter channel: "
            "`CAST(x AS ARRAY<VARCHAR>)`, `CAST(x AS VECTOR(2))`, "
            "`CAST(x AS DECIMAL(p, s))`."
        ),
        "status": "active",
        "summary": "Type conversion.",
        "syntax_forms": [
            "CAST(expr AS type)",
            "TRY_CAST(expr AS type)",
            "SAFE_CAST(expr AS type)",
        ],
    },
    "CUBE": {
        "ast_symbols": ["Cube"],
        "category": "grouping_construct",
        "documentation": "A GROUP BY grouping construct denoting every subset of the keys.",
        "implementation": f"{_BUILDERS_MODULE}.unsupported_grouping_construct",
        "lowering": None,
        "node_type": None,
        "notes": (
            "Parses - the dialect enables the whole production - but nothing lowers it, "
            "so it is refused by name. This is a lowering gap, not a design one: the "
            "internal representation (an explicit list of grouping sets) already "
            "accommodates it. ROLLUP is the supported construct."
        ),
        "status": "unsupported",
        "summary": "All-subsets grouping construct.",
        "syntax_forms": ["GROUP BY CUBE (a, b)"],
    },
    "EXISTS": {
        "ast_symbols": ["Exists"],
        "category": "subquery",
        "documentation": "Tests whether a subquery returns any row. `NOT EXISTS` is supported.",
        "implementation": f"{_BUILDERS_MODULE}.exists",
        "lowering": None,
        "node_type": "UNARY_OPERATOR (value 'Exists')",
        "notes": (
            "The subquery is embedded as a SUBQUERY parameter and bound in place; the "
            "optimizer then decorrelates it. In a SELECT list it ships as a left "
            "existence join that emits the verdict."
        ),
        "status": "active",
        "summary": "Subquery existence test.",
        "syntax_forms": ["EXISTS (SELECT ...)", "NOT EXISTS (SELECT ...)"],
    },
    "GROUPING_SETS": {
        "ast_symbols": ["GroupingSets"],
        "category": "grouping_construct",
        "documentation": "A GROUP BY grouping construct listing the key sets explicitly.",
        "implementation": f"{_BUILDERS_MODULE}.unsupported_grouping_construct",
        "lowering": None,
        "node_type": None,
        "notes": "As CUBE - parses, refused by name, no lowering. ROLLUP is the supported construct.",
        "status": "unsupported",
        "summary": "Explicit grouping-set construct.",
        "syntax_forms": ["GROUP BY GROUPING SETS ((a), (b))"],
    },
    "HEX_LITERAL": {
        "ast_symbols": ["HexStringLiteral"],
        "category": "literal",
        "documentation": "A hexadecimal integer literal.",
        "implementation": f"{_BUILDERS_MODULE}.hex_literal",
        "lowering": None,
        "node_type": "LITERAL",
        "notes": "Always INT64; this is a number, not a binary string.",
        "status": "active",
        "summary": "Hexadecimal integer literal.",
        "syntax_forms": ["0x1F"],
    },
    "IN_LIST": {
        "ast_symbols": ["InList"],
        "category": "comparison",
        "documentation": "Membership test against a constant list.",
        "implementation": f"{_BUILDERS_MODULE}.in_list",
        "lowering": "the list becomes one ARRAY literal on the right of InList/NotInList",
        "node_type": "COMPARISON_OPERATOR (value 'InList' / 'NotInList')",
        "notes": (
            "Every element must be a CONSTANT and they must share one type. Arithmetic "
            "elements (`d_year IN (1999, 1999+1)`) are constant-folded at build time; an "
            "element that does not fold is refused. The resulting operators are "
            "catalogued in operators.json."
        ),
        "status": "active",
        "summary": "Constant-list membership test.",
        "syntax_forms": ["expr IN (a, b)", "expr NOT IN (a, b)"],
    },
    "IN_SUBQUERY": {
        "ast_symbols": ["InSubquery"],
        "category": "subquery",
        "documentation": "Membership test against the rows a subquery returns.",
        "implementation": f"{_BUILDERS_MODULE}.in_subquery",
        "lowering": None,
        "node_type": "COMPARISON_OPERATOR (value 'InSubQuery')",
        "notes": (
            "A different builder from IN_LIST - the IN-list grammar admits no column "
            "references. In a SELECT list it ships as a left existence join."
        ),
        "status": "active",
        "summary": "Subquery membership test.",
        "syntax_forms": ["expr IN (SELECT ...)", "expr NOT IN (SELECT ...)"],
    },
    "IN_UNNEST": {
        "ast_symbols": ["InUnnest"],
        "category": "quantified_comparison",
        "documentation": "Membership test against the elements of an array expression.",
        "implementation": f"{_BUILDERS_MODULE}.in_unnest",
        "lowering": "AnyOpEq (negated: AllOpNotEq)",
        "node_type": "COMPARISON_OPERATOR",
        "notes": (
            "The positive form runs. The NEGATED form lowers to AllOpNotEq, which has no "
            "kernel, so `NOT IN UNNEST(...)` fails at execution - see the ALL entry."
        ),
        "status": "partial",
        "summary": "Array-element membership test.",
        "syntax_forms": ["expr IN UNNEST(array)", "expr NOT IN UNNEST(array)"],
    },
    "INTERVAL": {
        "ast_symbols": ["Interval"],
        "category": "literal",
        "documentation": (
            "An interval literal. Units are YEAR, MONTH, DAY, HOUR, MINUTE, SECOND; the "
            "compound form spans a contiguous run of them from the leading unit."
        ),
        "implementation": f"{_BUILDERS_MODULE}.literal_interval",
        "lowering": None,
        "node_type": "LITERAL (INTERVAL)",
        "notes": (
            "The value is a (months, microseconds) pair - the two components that cannot "
            "be reconciled without a calendar. The value must be QUOTED and a leading "
            "unit is required. This is the one type-prefixed string literal the dialect "
            "still accepts."
        ),
        "status": "active",
        "summary": "Interval literal.",
        "syntax_forms": ["INTERVAL '1' DAY", "INTERVAL '1 3' YEAR TO MONTH"],
    },
    "IS_DISTINCT_FROM": {
        "ast_symbols": ["IsDistinctFrom", "IsNotDistinctFrom"],
        "category": "comparison",
        "documentation": (
            "Null-safe comparison. TOTAL - it never answers UNKNOWN, which is the whole "
            "point of the operator: `NULL IS DISTINCT FROM NULL` is FALSE."
        ),
        "implementation": f"{_BUILDERS_MODULE}.distinct_from",
        "lowering": (
            "(a IS NULL AND b IS NOT NULL) OR (a IS NOT NULL AND b IS NULL) "
            "OR (a IS NOT NULL AND b IS NOT NULL AND a != b)"
        ),
        "node_type": "OR",
        "notes": (
            "The null-test guard around the comparison is load-bearing - drop it and the "
            "operator answers UNKNOWN where the standard requires FALSE. The lowering is "
            "deliberately built from comparisons, boolean algebra and null tests so the "
            "final opcode produces a mask the native filter accepts."
        ),
        "status": "active",
        "summary": "Null-safe comparison.",
        "syntax_forms": ["a IS DISTINCT FROM b", "a IS NOT DISTINCT FROM b"],
    },
    "MATCH_AGAINST": {
        "ast_symbols": ["MatchAgainst"],
        "category": "pattern",
        "documentation": "Similarity match of a column against a query string.",
        "implementation": f"{_BUILDERS_MODULE}.match_against",
        "lowering": "the _MATCH_AGAINST function",
        "node_type": "FUNCTION",
        "notes": (
            "Matching is by embedding cosine similarity, tuned with `SET match_threshold` "
            "- NOT MySQL full-text search. Exactly one column is accepted, and MySQL's "
            "search modifiers are refused rather than ignored."
        ),
        "status": "active",
        "summary": "Embedding similarity match.",
        "syntax_forms": ["MATCH (column) AGAINST ('text')"],
    },
    "NESTED": {
        "ast_symbols": ["Nested"],
        "category": "structural",
        "documentation": "A parenthesised expression.",
        "implementation": f"{_BUILDERS_MODULE}.nested",
        "lowering": None,
        "node_type": "NESTED",
        "notes": (
            "Kept as its own node rather than collapsed, because it is the outermost node "
            "of a parenthesised select item and so is what carries the alias."
        ),
        "status": "active",
        "summary": "Parenthesised expression.",
        "syntax_forms": ["(expr)"],
    },
    "NULL_LITERAL": {
        "ast_symbols": ["Null"],
        "category": "literal",
        "documentation": "The null literal.",
        "implementation": f"{_BUILDERS_MODULE}.literal_null",
        "lowering": None,
        "node_type": "LITERAL (NULL)",
        "notes": "Typed NULL, so two NULL literals in one select list stay distinct columns.",
        "status": "active",
        "summary": "Null literal.",
        "syntax_forms": ["NULL"],
    },
    "NUMERIC_LITERAL": {
        "ast_symbols": ["Number"],
        "category": "literal",
        "documentation": "An integer or floating-point literal.",
        "implementation": f"{_BUILDERS_MODULE}.literal_number",
        "lowering": None,
        "node_type": "LITERAL",
        "notes": "An exact integer takes the narrowest native tier that holds it.",
        "status": "active",
        "summary": "Numeric literal.",
        "syntax_forms": ["1", "1.5", "-1"],
    },
    "PLACEHOLDER": {
        "ast_symbols": ["Placeholder"],
        "category": "parameter",
        "documentation": "A bind parameter.",
        "implementation": f"{_BUILDERS_MODULE}.placeholder",
        "lowering": None,
        "node_type": None,
        "notes": (
            "Placeholders are substituted before the plan is built. Reaching this builder "
            "means one was left unbound, and it raises - a query is never planned with a "
            "hole in it."
        ),
        "status": "active",
        "summary": "Bind parameter.",
        "syntax_forms": ["?"],
    },
    "QUANTIFIED_ALL": {
        "ast_symbols": ["AllOp"],
        "category": "quantified_comparison",
        "documentation": "Comparison that must hold against every element of an array.",
        "implementation": f"{_BUILDERS_MODULE}.all_op",
        "lowering": "COMPARISON_OPERATOR named 'AllOp' + the comparison, e.g. AllOpNotEq",
        "node_type": "COMPARISON_OPERATOR",
        "notes": (
            "The PATTERN forms run natively (draken_like_any): `LIKE ALL` requires every "
            "pattern to match, and `NOT LIKE ALL` requires none of them to - the latter "
            "is the De Morgan dual of `LIKE ANY`, NOT the negation of `LIKE ALL`. The "
            "COMPARISON forms plan but do not run: the ordering comparisons are rejected "
            "as unknown operators, and AllOpEq/AllOpNotEq are rejected as outside the "
            "c-native predicate set. `NOT IN UNNEST(...)` lowers here and inherits that "
            "gap. The patterns must be bracketed."
        ),
        "status": "partial",
        "summary": "Universally-quantified comparison.",
        "syntax_forms": [
            "expr = ALL(array)",
            "expr <> ALL(array)",
            "expr > ALL(array)",
            "expr LIKE ALL (patterns)",
            "expr NOT LIKE ALL (patterns)",
            "expr ILIKE ALL (patterns)",
            "expr NOT ILIKE ALL (patterns)",
        ],
    },
    "QUANTIFIED_ANY": {
        "ast_symbols": ["AnyOp"],
        "category": "quantified_comparison",
        "documentation": "Comparison that must hold against at least one element of an array.",
        "implementation": f"{_BUILDERS_MODULE}.any_op",
        "lowering": "COMPARISON_OPERATOR named 'AnyOp' + the comparison, e.g. AnyOpEq",
        "node_type": "COMPARISON_OPERATOR",
        "notes": (
            "The pattern operators take the quantifier too - `name LIKE ANY ('a%', 'b%')` "
            "builds AnyOpLike. The patterns must be bracketed. `NOT LIKE ANY` and `NOT "
            "ILIKE ANY` are REFUSED: they decompose to `NOT(a AND b)`, which is true "
            "unless every pattern matches, and that is almost never what is meant. Write "
            "`NOT LIKE ALL (patterns)` for rows matching none of the patterns, or "
            "`NOT (LIKE ALL (patterns))` for the literal reading."
        ),
        "status": "active",
        "summary": "Existentially-quantified comparison.",
        "syntax_forms": [
            "expr = ANY(array)",
            "expr LIKE ANY (patterns)",
            "expr ILIKE ANY (patterns)",
        ],
    },
    "ROLLUP": {
        "ast_symbols": ["Rollup"],
        "category": "grouping_construct",
        "documentation": (
            "A GROUP BY grouping construct denoting the prefix hierarchy of its elements, "
            "coarsest last: `ROLLUP(a, b)` groups by (a, b), (a) and ()."
        ),
        "implementation": f"{_BUILDERS_MODULE}.rollup",
        "lowering": "a flat key list plus the grouping-set index tuples the aggregate carries",
        "node_type": "GroupingConstruct (not a Node)",
        "notes": (
            "The element nesting is load-bearing: `ROLLUP((a, b), c)` has TWO elements, "
            "the first composite, and rolls up to three sets, not four. Deliberately not "
            "a Node - it describes which sets to group by and only ever appears as a "
            "GROUP BY member, so anything else that receives one fails loudly."
        ),
        "status": "active",
        "summary": "Prefix-hierarchy grouping construct.",
        "syntax_forms": ["GROUP BY ROLLUP (a, b)", "GROUP BY ROLLUP ((a, b), c)"],
    },
    "SCALAR_SUBQUERY": {
        "ast_symbols": ["Subquery"],
        "category": "subquery",
        "documentation": "A subquery used as an expression value.",
        "implementation": f"{_BUILDERS_MODULE}.scalar_subquery",
        "lowering": None,
        "node_type": "SUBQUERY",
        "notes": (
            "Stays a subquery through the plan rewriter and is bound in place; the "
            "optimizer's decorrelation strategy then removes it, using the binder's "
            "resolution of each name to tell a correlated reference from a local one."
        ),
        "status": "active",
        "summary": "Subquery as a value.",
        "syntax_forms": ["(SELECT MAX(x) FROM t)"],
    },
    "SIMILAR_TO": {
        "ast_symbols": ["SimilarTo"],
        "category": "pattern",
        "documentation": "Refused. SQL-standard pattern match, which Opteryx does not implement.",
        "implementation": f"{_BUILDERS_MODULE}.pattern_match",
        "lowering": "none - raises UnsupportedSyntaxError",
        "node_type": "COMPARISON_OPERATOR",
        "notes": (
            "REFUSED as of 2026-08-24. SIMILAR TO is its own SQL pattern language - `%` "
            "and `_` are the wildcards, `.` is a literal dot - and Opteryx has no "
            "implementation of it. It previously parsed and then applied POSIX REGEX to "
            "the pattern, which agrees with the standard only by coincidence: "
            "`name SIMILAR TO '^C.'` matched C-initial names and would match nothing "
            "under the standard. Use RLIKE for regular expressions, LIKE for SQL "
            "wildcards. The PostgreSQL regex operators `~` and `!~` were withdrawn at "
            "the same time - they were undocumented synonyms for RLIKE / NOT RLIKE, and "
            "one spelling per behaviour is the rule. Unary `~` (bitwise NOT) is a "
            "different operator and is unaffected."
        ),
        "status": "unsupported",
        "summary": "Refused - use RLIKE or LIKE.",
        "syntax_forms": ["expr SIMILAR TO 'pattern'", "expr NOT SIMILAR TO 'pattern'"],
    },
    "STRING_LITERAL": {
        "ast_symbols": ["SingleQuotedString", "DoubleQuotedString"],
        "category": "literal",
        "documentation": "A string literal.",
        "implementation": f"{_BUILDERS_MODULE}.literal_string",
        "lowering": None,
        "node_type": "LITERAL (VARCHAR)",
        "notes": (
            "A DOUBLE-quoted string is a string literal here, not a quoted identifier. "
            "VARCHAR is ASCII bytes."
        ),
        "status": "active",
        "summary": "String literal.",
        "syntax_forms": ["'text'", '"text"'],
    },
    "SUBSCRIPT": {
        "ast_symbols": ["JsonAccess"],
        "category": "accessor",
        "documentation": "Positional element access on an array-typed expression.",
        "implementation": f"{_BUILDERS_MODULE}.json_access",
        "lowering": "the MapAccess extraction operator",
        "node_type": "EXTRACTION_OPERATOR",
        "notes": (
            "The subscript must be an INTEGER LITERAL. Field access on a struct uses the "
            "arrow operators in operators.json instead."
        ),
        "status": "active",
        "summary": "Array subscript.",
        "syntax_forms": ["expr[0]"],
    },
    "TRUE_FALSE_LITERAL": {
        "ast_symbols": ["Boolean"],
        "category": "literal",
        "documentation": "A boolean literal.",
        "implementation": f"{_BUILDERS_MODULE}.literal_boolean",
        "lowering": None,
        "node_type": "LITERAL (BOOLEAN)",
        "notes": None,
        "status": "active",
        "summary": "Boolean literal.",
        "syntax_forms": ["TRUE", "FALSE"],
    },
    "TUPLE_LITERAL": {
        "ast_symbols": ["Tuple"],
        "category": "literal",
        "documentation": "A parenthesised list of literals.",
        "implementation": f"{_BUILDERS_MODULE}.tuple_literal",
        "lowering": None,
        "node_type": "LITERAL (ARRAY)",
        "notes": (
            "ALWAYS an ARRAY - a literal is never a VECTOR - so the two literal spellings "
            "agree and both reach the ARRAY-typed containment operators. Like the bracket "
            "form, it cannot be projected."
        ),
        "status": "active",
        "summary": "Tuple literal.",
        "syntax_forms": ["(1, 2)"],
    },
    "TYPED_STRING": {
        "ast_symbols": ["TypedString"],
        "category": "literal",
        "documentation": "A type-prefixed string literal, `DATE '2020-01-01'`.",
        "implementation": f"{_BUILDERS_MODULE}.typed_string",
        "lowering": None,
        "node_type": None,
        "notes": (
            "Withdrawn - the builder exists only to refuse it by name and point at CAST. "
            "INTERVAL is the one prefix literal that remains."
        ),
        "status": "unsupported",
        "summary": "Type-prefixed string literal.",
        "syntax_forms": ["DATE '2020-01-01'", "TIMESTAMP '2020-01-01 00:00'"],
    },
}


def _check_exhaustive() -> None:
    """Reconcile the definitions against BUILDERS.

    This is the guarantee that makes the catalog's exhaustiveness claim true. It
    fails loudly rather than emitting a quietly incomplete catalog.
    """
    from opteryx.planner.logical_planner.logical_planner_builders import BUILDERS

    documented: dict[str, str] = {}
    for form, definition in EXPRESSION_DEFINITIONS.items():
        for symbol in definition["ast_symbols"]:
            if symbol in documented:
                raise ValueError(
                    f"Builder key '{symbol}' is claimed by both '{documented[symbol]}' "
                    f"and '{form}'. One builder key, one expression form."
                )
            documented[symbol] = form

    accounted = set(documented) | set(NON_EXPRESSION_BUILDERS)
    builders = set(BUILDERS)

    undocumented = sorted(builders - accounted)
    if undocumented:
        raise ValueError(
            f"Expression builders with no catalog entry: {', '.join(undocumented)}. "
            "Add each to EXPRESSION_DEFINITIONS, or to NON_EXPRESSION_BUILDERS with "
            "the reason it is not an expression form."
        )

    stale = sorted(accounted - builders)
    if stale:
        raise ValueError(
            f"Catalog entries for builders that no longer exist: {', '.join(stale)}."
        )


def export_expression_catalog() -> "OrderedDict[str, dict[str, Any]]":
    _check_exhaustive()

    ordered: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for name in sorted(EXPRESSION_DEFINITIONS):
        ordered[name] = EXPRESSION_DEFINITIONS[name]
    return ordered


def write_expression_catalog(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_expression_catalog(), indent=4) + "\n",
        encoding="utf8",
    )
