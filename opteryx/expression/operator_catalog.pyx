"""Curated operator metadata layered onto generated operator exports."""

from dataclasses import dataclass


@dataclass(frozen=True)
class OperandDefinition:
    """One side of a binary operator, as the documentation names it.

    `name` is the placeholder the syntax form uses (`haystack`), `documentation`
    says what belongs there. The ACCEPTED TYPES are deliberately absent: they are
    derived from the binder's OPERATOR_MAP when the catalog is exported
    (`reference/operator_catalog.py`), so the published documentation cannot claim
    a type the binder rejects, nor miss one it accepts.

    `constant_only` records that the binder REQUIRES a literal here. It is a
    constraint, not a hint: `@?` is the only operator that has one today, and
    setting it anywhere the binder does not enforce it publishes a restriction the
    engine does not have. A LIKE pattern, for one, may be a column.
    """

    name: str
    documentation: str
    constant_only: bool = False


@dataclass(frozen=True)
class ExampleDefinition:
    """One runnable example, and what the engine answers.

    `result` is one string per row (columns joined by ` | `), formatted by
    `format_example_rows` in tests/unit/test_documentation.py, which executes every
    example and asserts the engine still produces exactly this. The published page
    therefore shows an answer the engine gave, not one anybody reasoned out - the
    docs generator used to synthesize expected values by re-implementing SQL in
    Python, which could agree with the engine only by luck.

    Keep examples to a handful of rows; the test fails a long result rather than
    truncating it, because a truncated result on the page is a wrong result.
    """

    sql: str
    result: tuple = ()


@dataclass(frozen=True)
class OperatorDefinition:
    summary: str
    documentation: str
    token: str | None = None
    category: str | None = None
    node_kind: str | None = None
    friendly_name: str | None = None
    sql_symbol: str | None = None
    notes: str | None = None
    #: How the operator is written, in the notation the documentation site uses:
    #: UPPERCASE is a literal keyword, `<lowercase>` is a placeholder the reader
    #: fills in, `[ ... ]` is optional. One entry per accepted spelling - `!=` and
    #: `<>` are two forms of one operator, not two operators.
    syntax_forms: tuple = ()
    #: Left operand then right operand, naming the placeholders used above. Types
    #: are NOT recorded here - see OperandDefinition.
    operands: tuple = ()
    #: Runnable SQL. Every example is executed by
    #: tests/unit/reference/test_operator_catalog_examples.py, so an example that
    #: stops working fails the build rather than misleading a reader. Use
    #: `$planets` or literals - it is the one sample dataset every install has.
    examples: tuple = ()
    #: Related operators, by canonical name. Validated at import against this
    #: catalog, so a rename cannot leave a dangling cross-reference.
    see_also: tuple = ()
    #: False for an operator the DIALECT accepts but the execution engine cannot
    #: run. Every entry here was previously indistinguishable from a working
    #: operator in `reference/operators.json`, so the catalog claimed support the
    #: engine did not have. An unsupported operator parses and binds, then fails
    #: at plan-to-native lowering; `implemented` is how that is stated up front.
    #: Set it to False WITH a `notes` line saying what is missing.
    implemented: bool = True


cpdef str default_operator_friendly_name(str operator):
    cdef list words = []
    cdef str current = ""
    cdef Py_ssize_t index
    cdef str character
    for index, character in enumerate(operator):
        if index > 0 and character.isupper() and current:
            words.append(current)
            current = character
            continue
        current += character
    if current:
        words.append(current)
    return " ".join(words) if words else operator


cpdef get_operator_definition(str operator):
    return OPERATOR_DEFINITIONS.get(operator)


cpdef bint is_known_operator(str operator):
    return operator in OPERATOR_DEFINITIONS


cpdef get_operator_token(str operator):
    definition = get_operator_definition(operator)
    if definition and definition.token:
        return definition.token
    return None


cpdef get_operator_sql_symbol(str operator):
    definition = get_operator_definition(operator)
    if definition and definition.sql_symbol:
        return definition.sql_symbol
    return get_operator_token(operator)


cpdef get_operator_for_sql_symbol(str symbol):
    """Map a SQL spelling (`<<=`) back to its canonical operator name (`IPContainedBy`).

    The inverse of `get_operator_sql_symbol`. The dialect puts the SQL spelling of
    its custom operators into the AST so that serialising the AST back to SQL - how
    a view is stored - produces text that re-parses; this turns that spelling back
    into the name every downstream stage keys on. Returns None for an unknown
    symbol so the caller can report the text it actually received.
    """
    return SQL_SYMBOLS_TO_OPERATORS.get(symbol)


def get_operator_node_type(operator):
    from opteryx.expression import NodeType

    definition = get_operator_definition(operator)
    if definition is None:
        return None
    node_kind = definition.node_kind
    if node_kind == "binary":
        return NodeType.BINARY_OPERATOR
    if node_kind == "comparison":
        return NodeType.COMPARISON_OPERATOR
    if node_kind == "extraction":
        return NodeType.EXTRACTION_OPERATOR
    if operator == "And":
        return NodeType.AND
    if operator == "Or":
        return NodeType.OR
    if operator == "Xor":
        return NodeType.XOR
    return None


OPERATOR_DEFINITIONS = {
    "And": OperatorDefinition(
        summary="Logical conjunction.",
        documentation="Returns true only when both boolean operands evaluate to true.",
        token="AND",
        category="logical",
        node_kind="logical",
        friendly_name="Logical AND",
        syntax_forms=("<left> AND <right>",),
        operands=(
            OperandDefinition(
                "left",
                "A boolean expression. A comparison that produced NULL counts as unknown "
                "here, not as false.",
            ),
            OperandDefinition("right", "A boolean expression, evaluated under the same rules."),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE gravity > 5 AND number_of_moons = 0;",
                ("Venus",),
            ),
            ExampleDefinition(
                "SELECT FALSE AND NULL, TRUE AND NULL;",
                ("false | NULL",),
            ),
        ),
        see_also=("Or", "Xor"),
        notes=(
            "AND is three-valued. FALSE wins over an unknown - `FALSE AND NULL` is FALSE, "
            "because no value of the unknown side could make the pair true - while "
            "`TRUE AND NULL` is NULL. Only a TRUE result passes a WHERE clause, so a row "
            "whose condition is NULL is dropped exactly as a false one is."
        ),
    ),
    "Or": OperatorDefinition(
        summary="Logical disjunction.",
        documentation="Returns true when either boolean operand evaluates to true.",
        token="OR",
        category="logical",
        node_kind="logical",
        friendly_name="Logical OR",
        syntax_forms=("<left> OR <right>",),
        operands=(
            OperandDefinition(
                "left",
                "A boolean expression. A comparison that produced NULL counts as unknown "
                "here, not as false.",
            ),
            OperandDefinition("right", "A boolean expression, evaluated under the same rules."),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name = 'Earth' OR name = 'Mars';",
                ("Earth", "Mars"),
            ),
            ExampleDefinition(
                "SELECT TRUE OR NULL, FALSE OR NULL;",
                ("true | NULL",),
            ),
        ),
        see_also=("And", "Xor"),
        notes=(
            "OR is three-valued, and the mirror of AND: TRUE wins over an unknown - "
            "`TRUE OR NULL` is TRUE - while `FALSE OR NULL` is NULL, not FALSE."
        ),
    ),
    "Xor": OperatorDefinition(
        summary="Logical exclusive OR.",
        documentation="Returns true when exactly one boolean operand evaluates to true.",
        token="XOR",
        category="logical",
        node_kind="logical",
        friendly_name="Logical XOR",
        syntax_forms=("<left> XOR <right>",),
        operands=(
            OperandDefinition("left", "A boolean expression."),
            OperandDefinition("right", "A boolean expression."),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE (gravity > 5) XOR (number_of_moons > 10);",
                ("Venus", "Earth"),
            ),
            ExampleDefinition(
                "SELECT TRUE XOR NULL;",
                ("NULL",),
            ),
        ),
        see_also=("And", "Or"),
        notes=(
            "Unlike AND and OR, XOR has no dominant value: the answer always depends on "
            "both sides, so NULL on either side gives NULL."
        ),
    ),
    "Eq": OperatorDefinition(
        summary="Equality comparison.",
        documentation="Returns true when both operands compare equal.",
        token="=",
        category="comparison",
        node_kind="comparison",
        friendly_name="Equals",
        syntax_forms=("<left> = <right>",),
        operands=(
            OperandDefinition(
                "left",
                "The value to compare. Numeric types compare across the family, so "
                "`1 = 1.0` is true.",
            ),
            OperandDefinition(
                "right",
                "The value to compare it against. It must be type-compatible with the left "
                "- a number and a string are rejected rather than coerced.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name = 'Earth';",
                ("Earth",),
            ),
            ExampleDefinition(
                "SELECT 'Mars' = 'mars', 1 = 1.0, 1 = NULL;",
                ("false | true | NULL",),
            ),
        ),
        see_also=("NotEq", "InList"),
        notes=(
            "Comparison is three-valued: NULL on either side gives NULL, never true or "
            "false, and `NULL = NULL` is NULL too - `IS NULL` is the test for absence. "
            "String comparison is case-sensitive (`'Mars' = 'mars'` is false), unlike "
            "column NAMES, which are not."
        ),
    ),
    "NotEq": OperatorDefinition(
        summary="Inequality comparison.",
        documentation="Returns true when the operands do not compare equal.",
        token="!=",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not equals",
        syntax_forms=("<left> != <right>", "<left> <> <right>"),
        operands=(
            OperandDefinition("left", "The value to compare."),
            OperandDefinition(
                "right",
                "The value to compare it against. It must be type-compatible with the left.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name != 'Earth' LIMIT 3;",
                ("Mercury", "Venus", "Mars"),
            ),
            ExampleDefinition(
                "SELECT COUNT(*) FROM $planets WHERE surface_pressure != 0;",
                ("4",),
            ),
        ),
        see_also=("Eq", "NotInList"),
        notes=(
            "`!=` does not mean \"everything else\": a row whose value is NULL answers NULL, "
            "not true, so it is dropped by the WHERE clause. Of the nine planets one has a "
            "surface pressure of 0 and four have none recorded, so `surface_pressure != 0` "
            "returns four rows, not eight - `OR surface_pressure IS NULL` is how the "
            "unknown rows are kept."
        ),
    ),
    "Gt": OperatorDefinition(
        summary="Greater-than comparison.",
        documentation="Returns true when the left operand is greater than the right operand.",
        token=">",
        category="comparison",
        node_kind="comparison",
        friendly_name="Greater than",
        syntax_forms=("<left> > <right>",),
        operands=(
            OperandDefinition(
                "left",
                "The value to compare. Strings order byte-by-byte, so uppercase sorts "
                "before lowercase.",
            ),
            OperandDefinition("right", "The value to compare it against."),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE number_of_moons > 10;",
                ("Jupiter", "Saturn", "Uranus", "Neptune"),
            ),
        ),
        see_also=("GtEq", "Lt", "LtEq"),
        notes="NULL on either side gives NULL, so ordering comparisons never match an absent value.",
    ),
    "GtEq": OperatorDefinition(
        summary="Greater-than-or-equal comparison.",
        documentation="Returns true when the left operand is greater than or equal to the right operand.",
        token=">=",
        category="comparison",
        node_kind="comparison",
        friendly_name="Greater than or equal",
        syntax_forms=("<left> >= <right>",),
        operands=(
            OperandDefinition("left", "The value to compare."),
            OperandDefinition("right", "The value to compare it against."),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE number_of_moons >= 10;",
                ("Jupiter", "Saturn", "Uranus", "Neptune"),
            ),
        ),
        see_also=("Gt", "Lt", "LtEq"),
        notes="NULL on either side gives NULL, so ordering comparisons never match an absent value.",
    ),
    "Lt": OperatorDefinition(
        summary="Less-than comparison.",
        documentation="Returns true when the left operand is less than the right operand.",
        token="<",
        category="comparison",
        node_kind="comparison",
        friendly_name="Less than",
        syntax_forms=("<left> < <right>",),
        operands=(
            OperandDefinition(
                "left",
                "The value to compare. Strings order byte-by-byte, so uppercase sorts "
                "before lowercase.",
            ),
            OperandDefinition("right", "The value to compare it against."),
        ),
        examples=(ExampleDefinition(
                "SELECT name FROM $planets WHERE gravity < 5;",
                ("Mercury", "Mars", "Pluto"),
            ),),
        see_also=("LtEq", "Gt", "GtEq"),
        notes="NULL on either side gives NULL, so ordering comparisons never match an absent value.",
    ),
    "LtEq": OperatorDefinition(
        summary="Less-than-or-equal comparison.",
        documentation="Returns true when the left operand is less than or equal to the right operand.",
        token="<=",
        category="comparison",
        node_kind="comparison",
        friendly_name="Less than or equal",
        syntax_forms=("<left> <= <right>",),
        operands=(
            OperandDefinition("left", "The value to compare."),
            OperandDefinition("right", "The value to compare it against."),
        ),
        examples=(ExampleDefinition(
                "SELECT name FROM $planets WHERE gravity <= 5;",
                ("Mercury", "Mars", "Pluto"),
            ),),
        see_also=("Lt", "Gt", "GtEq"),
        notes="NULL on either side gives NULL, so ordering comparisons never match an absent value.",
    ),
    "InList": OperatorDefinition(
        summary="Membership comparison.",
        documentation="Returns true when the left operand matches any element in the right-hand list or array.",
        token="IN",
        category="comparison",
        node_kind="comparison",
        friendly_name="In list",
        syntax_forms=("<value> IN (<item> [, ...])", "<value> IN <array>"),
        operands=(
            OperandDefinition("value", "The value to look for."),
            OperandDefinition(
                "list",
                "The values to look in - a parenthesised list, or an array-valued "
                "expression. Every element must share one type; a mixed list, NULL "
                "included, is rejected at plan time rather than being silently skipped.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name IN ('Earth', 'Mars');",
                ("Earth", "Mars"),
            ),
            ExampleDefinition(
                "SELECT 2 IN (1, 2, 3), 9 IN (1, 2, 3);",
                ("true | false",),
            ),
        ),
        see_also=("NotInList", "Eq"),
        notes=(
            "IN is a shorthand for a chain of `=`, and inherits its rules: the comparison "
            "is exact and case-sensitive. A list mixing types - `IN (NULL, 2)` among them - "
            "is an error, not a match against the elements that do share a type."
        ),
    ),
    "NotInList": OperatorDefinition(
        summary="Negated membership comparison.",
        documentation="Returns true when the left operand does not match any element in the right-hand list or array.",
        token="NOT IN",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not in list",
        syntax_forms=("<value> NOT IN (<item> [, ...])", "<value> NOT IN <array>"),
        operands=(
            OperandDefinition("value", "The value to look for."),
            OperandDefinition(
                "list",
                "The values to look in. Every element must share one type; a mixed list is "
                "rejected at plan time.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name NOT IN ('Earth', 'Mars') LIMIT 3;",
                ("Mercury", "Venus", "Jupiter"),
            ),
        ),
        see_also=("InList", "NotEq"),
        notes="Like `!=`, a row whose value is NULL answers NULL rather than true, so it does not survive a WHERE clause.",
    ),
    "Like": OperatorDefinition(
        summary="Pattern match comparison.",
        documentation="Returns true when the left string matches the SQL LIKE pattern on the right.",
        token="LIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Like",
        syntax_forms=("<haystack> LIKE <pattern>",),
        operands=(
            OperandDefinition(
                "haystack",
                "The value tested against the pattern. VARBINARY is accepted as well as "
                "text, and is matched as bytes.",
            ),
            OperandDefinition(
                "pattern",
                "A SQL LIKE pattern: `%` matches any run of characters including none, `_` "
                "matches exactly one, and every other character matches itself. The whole "
                "value must match, not part of it - `'abcd' LIKE 'a_c'` is false. A column "
                "is accepted here, not only a literal; a literal is what lets the planner "
                "fuse the match into the scan.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name LIKE 'Ma%';",
                ("Mars",),
            ),
            ExampleDefinition(
                "SELECT 'abc' LIKE 'a_c', 'abcd' LIKE 'a_c';",
                ("true | false",),
            ),
        ),
        see_also=("NotLike", "ILike", "RLike"),
        notes=(
            "Matching is case-sensitive; ILIKE is the case-insensitive form. A NULL on "
            "either side is not matched."
        ),
    ),
    "NotLike": OperatorDefinition(
        summary="Negated pattern match comparison.",
        documentation="Returns true when the left string does not match the SQL LIKE pattern on the right.",
        token="NOT LIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not like",
        syntax_forms=("<haystack> NOT LIKE <pattern>",),
        operands=(
            OperandDefinition("haystack", "The value tested against the pattern."),
            OperandDefinition(
                "pattern",
                "A SQL LIKE pattern: `%` matches any run of characters, `_` matches exactly "
                "one. The whole value must match for the row to be excluded.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name NOT LIKE 'Ma%' LIMIT 3;",
                ("Mercury", "Venus", "Earth"),
            ),
        ),
        see_also=("Like", "NotILike"),
        notes="Case-sensitive, like `LIKE` itself. NOT ILIKE is the case-insensitive form.",
    ),
    "ILike": OperatorDefinition(
        summary="Case-insensitive pattern match comparison.",
        documentation="Returns true when the left string matches the SQL ILIKE pattern on the right without case sensitivity.",
        token="ILIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Case-insensitive like",
        syntax_forms=("<haystack> ILIKE <pattern>",),
        operands=(
            OperandDefinition("haystack", "The value tested against the pattern."),
            OperandDefinition(
                "pattern",
                "A SQL LIKE pattern, matched without regard to case: `%` matches any run of "
                "characters, `_` matches exactly one.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name ILIKE 'ma%';",
                ("Mars",),
            ),
            ExampleDefinition(
                "SELECT 'ABC' ILIKE 'abc', 'ÉCOLE' ILIKE 'école';",
                ("true | false",),
            ),
        ),
        see_also=("Like", "NotILike"),
        notes=(
            "Case folding is ASCII-only: `'ABC' ILIKE 'abc'` is true, but `'ÉCOLE' ILIKE "
            "'école'` is FALSE - accented and other non-ASCII letters are compared as "
            "written. Where that matters, fold explicitly with a function instead."
        ),
    ),
    "NotILike": OperatorDefinition(
        summary="Negated case-insensitive pattern match comparison.",
        documentation="Returns true when the left string does not match the SQL ILIKE pattern on the right.",
        token="NOT ILIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not case-insensitive like",
        syntax_forms=("<haystack> NOT ILIKE <pattern>",),
        operands=(
            OperandDefinition("haystack", "The value tested against the pattern."),
            OperandDefinition(
                "pattern",
                "A SQL LIKE pattern, matched without regard to ASCII case.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name NOT ILIKE 'ma%' LIMIT 3;",
                ("Mercury", "Venus", "Earth"),
            ),
        ),
        see_also=("ILike", "NotLike"),
        notes="Case folding is ASCII-only, exactly as for ILIKE.",
    ),
    "RLike": OperatorDefinition(
        summary="Regular expression match comparison.",
        documentation="Returns true when the left string matches the regular expression on the right.",
        token="RLIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Regex like",
        syntax_forms=("<haystack> RLIKE <regex>",),
        operands=(
            OperandDefinition("haystack", "The value tested against the expression."),
            OperandDefinition(
                "regex",
                "The regular expression to match. Unlike LIKE, it matches anywhere in the "
                "value unless anchored with `^` and `$`.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name RLIKE '^M.*s$';",
                ("Mars",),
            ),
        ),
        see_also=("NotRLike", "Like"),
        notes="A regular expression is more expressive than a LIKE pattern and more expensive to run; prefer LIKE when a prefix or suffix match is all that is needed.",
    ),
    "NotRLike": OperatorDefinition(
        summary="Negated regular expression match comparison.",
        documentation="Returns true when the left string does not match the regular expression on the right.",
        token="NOT RLIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not regex like",
        syntax_forms=("<haystack> NOT RLIKE <regex>",),
        operands=(
            OperandDefinition("haystack", "The value tested against the expression."),
            OperandDefinition(
                "regex",
                "The regular expression that must not match. It matches anywhere in the "
                "value unless anchored.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name FROM $planets WHERE name NOT RLIKE '^M' LIMIT 3;",
                ("Venus", "Earth", "Jupiter"),
            ),
        ),
        see_also=("RLike", "NotLike"),
    ),
    "Plus": OperatorDefinition(
        summary="Addition operator.",
        documentation="Returns the sum of two numeric or interval-compatible operands.",
        token="+",
        category="binary",
        node_kind="binary",
        friendly_name="Addition",
        syntax_forms=("<left> + <right>",),
        operands=(
            OperandDefinition(
                "left",
                "The value to add to. A DATE or TIMESTAMP is accepted only with an INTERVAL "
                "on the other side - two dates cannot be added.",
            ),
            OperandDefinition(
                "right",
                "The value to add. Mixing numeric types widens the result: INTEGER with "
                "FLOAT gives FLOAT, INTEGER with DECIMAL gives DECIMAL.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name, number_of_moons + 1 FROM $planets LIMIT 3;",
                ("Mercury | 1", "Venus | 1", "Earth | 2"),
            ),
            ExampleDefinition(
                "SELECT CAST('2026-01-01' AS DATE) + INTERVAL '1' MONTH;",
                ("2026-02-01 00:00:00+00:00",),
            ),
        ),
        see_also=("Minus", "Multiply", "Divide"),
        notes=(
            "Adding NULL gives NULL. Date arithmetic is only ever date-plus-interval, and "
            "the result is a TIMESTAMP even when the operand was a DATE - see Signatures."
        ),
    ),
    "Minus": OperatorDefinition(
        summary="Subtraction operator.",
        documentation="Returns the difference between two numeric, date, timestamp, or interval-compatible operands.",
        token="-",
        category="binary",
        node_kind="binary",
        friendly_name="Subtraction",
        syntax_forms=("<left> - <right>",),
        operands=(
            OperandDefinition("left", "The value to subtract from."),
            OperandDefinition(
                "right",
                "The value to subtract. Subtracting one TIMESTAMP from another gives an "
                "INTERVAL; subtracting an INTERVAL from a timestamp gives a timestamp.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name, aphelion - perihelion FROM $planets LIMIT 3;",
                ("Mercury | 23.800003051757812", "Venus | 1.4000015258789062", "Earth | 5.0"),
            ),
            ExampleDefinition(
                "SELECT CAST('2026-03-01' AS TIMESTAMP) - INTERVAL '1' MONTH;",
                ("2026-02-01 00:00:00+00:00",),
            ),
        ),
        see_also=("Plus", "Multiply", "Divide"),
        notes="Unlike addition, subtraction is not symmetric across types: a timestamp minus an interval is a timestamp, but an interval minus a timestamp is not accepted.",
    ),
    "Multiply": OperatorDefinition(
        summary="Multiplication operator.",
        documentation="Returns the product of two numeric operands.",
        token="*",
        category="binary",
        node_kind="binary",
        friendly_name="Multiplication",
        syntax_forms=("<left> * <right>",),
        operands=(
            OperandDefinition("left", "A numeric value."),
            OperandDefinition(
                "right",
                "A numeric value. The result takes the wider of the two types - see "
                "Signatures.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name, diameter * 2 FROM $planets LIMIT 3;",
                ("Mercury | 9758", "Venus | 24208", "Earth | 25512"),
            ),
        ),
        see_also=("Divide", "Plus", "Minus"),
    ),
    "Divide": OperatorDefinition(
        summary="Division operator.",
        documentation="Returns the quotient of two numeric operands.",
        token="/",
        category="binary",
        node_kind="binary",
        friendly_name="Division",
        syntax_forms=("<dividend> / <divisor>",),
        operands=(
            OperandDefinition(
                "dividend",
                "The value to divide. Two integers still divide to a FLOAT - use `DIV` for "
                "integer division.",
            ),
            OperandDefinition(
                "divisor",
                "The value to divide by. Dividing by zero yields infinity rather than "
                "raising, because the result is floating point.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT 5 / 2;",
                ("2.5",),
            ),
            ExampleDefinition(
                "SELECT CAST(1 AS DECIMAL(10,2)) / 3;",
                ("0.33333333",),
            ),
        ),
        see_also=("MyIntegerDivide", "Modulo", "Multiply"),
        notes=(
            "`/` is true division: `5 / 2` is 2.5, never 2. A DECIMAL operand keeps the "
            "result DECIMAL; every other combination gives FLOAT, and `1 / 0` is therefore "
            "`inf` rather than an error."
        ),
    ),
    "Modulo": OperatorDefinition(
        summary="Modulo operator.",
        documentation="Returns the remainder after division of the left numeric operand by the right numeric operand.",
        token="%",
        category="binary",
        node_kind="binary",
        friendly_name="Modulo",
        syntax_forms=("<dividend> % <divisor>",),
        operands=(
            OperandDefinition("dividend", "The value to divide. Its sign is the sign of the result."),
            OperandDefinition("divisor", "The value to divide by."),
        ),
        examples=(ExampleDefinition(
                "SELECT 7 % 2, -7 % 2, 7 % -2;",
                ("1 | -1 | 1",),
            ),),
        see_also=("Divide", "MyIntegerDivide"),
        notes=(
            "The remainder takes the sign of the DIVIDEND, not the divisor: `-7 % 2` is -1 "
            "and `7 % -2` is 1. That is C and Go's rule, not Python's, where `-7 % 2` is 1."
        ),
    ),
    "MyIntegerDivide": OperatorDefinition(
        summary="Integer division operator.",
        documentation="Divides two integers and truncates the result toward zero.",
        token="DIV",
        category="binary",
        node_kind="binary",
        friendly_name="Integer division",
        syntax_forms=("<dividend> DIV <divisor>",),
        operands=(
            OperandDefinition("dividend", "The integer to divide."),
            OperandDefinition("divisor", "The integer to divide by."),
        ),
        examples=(ExampleDefinition(
                "SELECT 7 DIV 2, -7 DIV 2;",
                ("3 | -3",),
            ),),
        see_also=("Divide", "Modulo"),
        notes=(
            "Truncation is toward zero, not toward minus infinity: `-7 DIV 2` is -3, where "
            "a floor division would give -4."
        ),
    ),
    "BitwiseOr": OperatorDefinition(
        summary="Bitwise OR operator.",
        documentation="Combines integer operands using a bitwise OR operation.",
        token="|",
        category="bitwise",
        node_kind="binary",
        friendly_name="Bitwise OR",
        syntax_forms=("<left> | <right>",),
        operands=(
            OperandDefinition("left", "An integer value."),
            OperandDefinition(
                "right",
                "An integer value. The result keeps the operands' integer width rather than "
                "widening to 64-bit.",
            ),
        ),
        examples=(ExampleDefinition(
                "SELECT 12 | 10;",
                ("14",),
            ),),
        see_also=("BitwiseAnd", "BitwiseXor"),
    ),
    "BitwiseAnd": OperatorDefinition(
        summary="Bitwise AND operator.",
        documentation="Combines integer operands using a bitwise AND operation.",
        token="&",
        category="bitwise",
        node_kind="binary",
        friendly_name="Bitwise AND",
        syntax_forms=("<left> & <right>",),
        operands=(
            OperandDefinition("left", "An integer value."),
            OperandDefinition(
                "right",
                "An integer value. The result keeps the operands' integer width rather than "
                "widening to 64-bit.",
            ),
        ),
        examples=(ExampleDefinition(
                "SELECT 12 & 10;",
                ("8",),
            ),),
        see_also=("BitwiseOr", "BitwiseXor"),
    ),
    "BitwiseXor": OperatorDefinition(
        summary="Bitwise XOR operator.",
        documentation="Combines integer operands using a bitwise exclusive OR operation.",
        token="^",
        category="bitwise",
        node_kind="binary",
        friendly_name="Bitwise XOR",
        syntax_forms=("<left> ^ <right>",),
        operands=(
            OperandDefinition("left", "An integer value."),
            OperandDefinition("right", "An integer value."),
        ),
        examples=(ExampleDefinition(
                "SELECT 12 ^ 10;",
                ("6",),
            ),),
        see_also=("BitwiseAnd", "BitwiseOr"),
        notes="`^` is exclusive OR, not exponentiation - a habit worth checking when porting SQL from systems where it raises to a power.",
    ),
    "ShiftLeft": OperatorDefinition(
        summary="Left shift operator.",
        documentation="Shifts the bits of the left integer operand left by the number of positions in the right operand.",
        token="<<",
        category="bitwise",
        node_kind="binary",
        friendly_name="Left shift",
        syntax_forms=("<value> << <count>",),
        operands=(
            OperandDefinition("value", "The integer whose bits are shifted."),
            OperandDefinition("count", "How many positions to shift by, 0..63."),
        ),
        examples=(ExampleDefinition(
                "SELECT 1 << 4;",
                ("16",),
            ),),
        see_also=("ShiftRight", "BitwiseAnd"),
        notes=(
            "The shift count must be 0..63 - the operands are 64-bit integers, and a "
            "count outside that range fails loud ('bitwise_shl: shift count out of range') "
            "rather than wrapping or saturating."
        ),
    ),
    "ShiftRight": OperatorDefinition(
        summary="Right shift operator.",
        documentation="Shifts the bits of the left integer operand right by the number of positions in the right operand.",
        token=">>",
        category="bitwise",
        node_kind="binary",
        friendly_name="Right shift",
        syntax_forms=("<value> >> <count>",),
        operands=(
            OperandDefinition(
                "value",
                "The integer whose bits are shifted. The shift is arithmetic, so the sign "
                "is preserved.",
            ),
            OperandDefinition("count", "How many positions to shift by, 0..63."),
        ),
        examples=(ExampleDefinition(
                "SELECT 256 >> 4, -1 >> 1;",
                ("16 | -1",),
            ),),
        see_also=("ShiftLeft", "BitwiseAnd"),
        notes=(
            "Right shift is ARITHMETIC, not logical: the sign bit is copied, so `-1 >> 1` "
            "is -1 and a negative value never becomes positive by shifting. "
            "The shift count must be 0..63 - the operands are 64-bit integers, and a "
            "count outside that range fails loud ('bitwise_shr: shift count out of range') "
            "rather than wrapping or saturating."
        ),
    ),
    "StringConcat": OperatorDefinition(
        summary="String concatenation operator.",
        documentation="Concatenates the left and right string or blob operands.",
        token="||",
        category="binary",
        node_kind="binary",
        friendly_name="Concatenation",
        syntax_forms=("<left> || <right>",),
        operands=(
            OperandDefinition("left", "The value to concatenate to."),
            OperandDefinition(
                "right",
                "The value to append. It must be the SAME string type as the left - VARCHAR "
                "with VARCHAR, VARBINARY with VARBINARY - and a number must be cast first.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT name || ' (planet)' FROM $planets LIMIT 3;",
                ("Mercury (planet)", "Venus (planet)", "Earth (planet)"),
            ),
            ExampleDefinition(
                "SELECT 'a' || NULL;",
                ("NULL",),
            ),
        ),
        see_also=("Plus",),
        notes=(
            "The operands must be the same string type; mixing VARCHAR and VARBINARY is "
            "rejected rather than silently coerced. `x || NULL` is NULL for every row - it "
            "is not treated as an empty string - but the expression still carries the "
            "string operand's type."
        ),
    ),
    "Arrow": OperatorDefinition(
        summary="JSON extraction operator.",
        documentation="Returns the selected JSON value from a document or JSON-like value.",
        token="->",
        category="extraction",
        node_kind="extraction",
        friendly_name="JSON extract",
        syntax_forms=("<document> -> <path>",),
        operands=(
            OperandDefinition("document", "The JSON document to read from."),
            OperandDefinition(
                "path",
                "The key or path to select. A bare key (`'city'`), a JSONPath "
                "(`'$.contact.email'`) and an RFC 6901 pointer (`'/contact/email'`) all "
                "name the same thing. A path that is not present gives NULL, not an error.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT '{\"name\": \"Earth\", \"moons\": 1}' -> 'name';",
                ("\"Earth\"",),
            ),
            ExampleDefinition(
                "SELECT '{\"a\": 1}' -> 'missing';",
                ("NULL",),
            ),
        ),
        see_also=("LongArrow", "AtQuestion", "MapAccess"),
        notes=(
            "`->` keeps the value as JSON, so a selected string arrives still quoted "
            "(`\"Earth\"`); `->>` is the form that gives the text itself (`Earth`). That is "
            "the difference between the two, and the usual cause of a comparison against a "
            "string literal not matching. The result type is dynamic because the selected "
            "JSON value may be scalar, object, array, or null."
        ),
    ),
    "LongArrow": OperatorDefinition(
        summary="JSON text extraction operator.",
        documentation="Returns the selected JSON value as text (nvarchar), from a document given as text or binary JSON.",
        token="->>",
        category="extraction",
        node_kind="extraction",
        friendly_name="JSON extract text",
        syntax_forms=("<document> ->> <path>",),
        operands=(
            OperandDefinition("document", "The JSON document to read from."),
            OperandDefinition(
                "path",
                "The key or path to select, in the same spellings `->` accepts. A path that "
                "is not present gives NULL.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT '{\"name\": \"Earth\", \"moons\": 1}' ->> 'name';",
                ("Earth",),
            ),
        ),
        see_also=("Arrow", "AtQuestion"),
        notes="Use `->>` when the value is going to be compared with a string literal: `->` would leave it JSON-quoted and the comparison would not match.",
    ),
    "MapAccess": OperatorDefinition(
        summary="Subscript access operator.",
        documentation="Returns the element at the requested index from an array, string, or blob-like value.",
        token="[]",
        category="extraction",
        node_kind="extraction",
        friendly_name="Subscript access",
        syntax_forms=("<value>[<index>]",),
        operands=(
            OperandDefinition("value", "The array, string or blob to read from."),
            OperandDefinition(
                "index",
                "The zero-based position to read: 0 is the first element. A negative "
                "index counts back from the end, so -1 is the last. An index past "
                "either end gives NULL rather than raising.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT ARRAY['a','b','c'][0];",
                ("a",),
            ),
            ExampleDefinition(
                "SELECT ARRAY['a','b','c'][-1];",
                ("c",),
            ),
            ExampleDefinition(
                "SELECT ARRAY['a','b'][9];",
                ("NULL",),
            ),
        ),
        see_also=("Arrow",),
        notes="Subscript access is ZERO-based and accepts negative indexes, which count back from the end: `[0]` is the first element and `[-1]` the last. Most SQL dialects index arrays from 1, so a query ported from one of those reads the WRONG element rather than erroring - an out-of-range index gives NULL, so nothing signals the mistake. For arrays the result type depends on the array element type, so the exported result type may be dynamic.",
    ),
    "AtQuestion": OperatorDefinition(
        summary="JSON path existence operator.",
        documentation="Returns true when the supplied JSON path expression matches within the left document.",
        token="@?",
        category="comparison",
        node_kind="comparison",
        friendly_name="JSON path exists",
        syntax_forms=("<document> @? <path>",),
        operands=(
            OperandDefinition("document", "The JSON document to test."),
            OperandDefinition(
                "path",
                "The path to look for. It is resolved to RFC 6901 tokens once, when the "
                "query is planned.",
                constant_only=True,
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT '{\"contact\": {\"email\": \"a@b.c\"}}' @? '$.contact.email';",
                ("true",),
            ),
            ExampleDefinition(
                "SELECT '{\"a\": null}' @? '$.a', '{\"a\": 1}' @? '$.b';",
                ("true | false",),
            ),
        ),
        see_also=("Arrow", "LongArrow"),
        notes=(
            "The path must be a literal — it is resolved to RFC 6901 tokens once when the "
            "query is planned, using the same resolver `->` uses, so `doc @? 'city'`, "
            "`doc @? '$.contact.email'` and `doc @? '/contact/email'` all name the same "
            "thing. Existence is not extraction: a path whose value is JSON `null` is TRUE "
            "here, while `doc->'key' IS NOT NULL` is FALSE. A NULL document row is NULL; a "
            "row whose bytes are not valid JSON is an error, never a silent false."
        ),
    ),
    "AtArrow": OperatorDefinition(
        summary="Array containment operator.",
        documentation="Returns true when the left array contains any of the values provided by the right array.",
        token="@>",
        category="comparison",
        node_kind="comparison",
        friendly_name="Array contains any",
        syntax_forms=("<array> @> <values>",),
        operands=(
            OperandDefinition("array", "The array to search."),
            OperandDefinition(
                "values",
                "The array of values to look for. ANY one of them being present is enough; "
                "an empty array on this side matches nothing.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT ['a','b'] @> ['a'];",
                ("true",),
            ),
            ExampleDefinition(
                "SELECT ['a','b'] @> ['a','z'], ['a'] @> [];",
                ("true | false",),
            ),
        ),
        see_also=("ArrayContainsAll", "InList"),
        notes=(
            "`@>` is ANY, `@>>` is ALL - the pair is easy to mix up. `['a','b'] @> ['a','z']` "
            "is TRUE because one value matched; the same operands under `@>>` are FALSE."
        ),
    ),
    "ArrayContainsAll": OperatorDefinition(
        summary="Array contains-all operator.",
        documentation="Returns true when the left array contains all values from the right array.",
        token="@>>",
        category="comparison",
        node_kind="comparison",
        friendly_name="Array contains all",
        syntax_forms=("<array> @>> <values>",),
        operands=(
            OperandDefinition("array", "The array to search."),
            OperandDefinition(
                "values",
                "The array of values that must ALL be present for the result to be true.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT ['a','b'] @>> ['a','b'];",
                ("true",),
            ),
            ExampleDefinition(
                "SELECT ['a','b'] @>> ['a','z'];",
                ("false",),
            ),
        ),
        see_also=("AtArrow", "InList"),
        notes="`@>>` is ALL, `@>` is ANY.",
    ),
    "IPContainedBy": OperatorDefinition(
        summary="IPv4 CIDR containment operator.",
        documentation=(
            "Returns true when the left IPv4 address falls inside the network given "
            "on the right in CIDR notation, for example `ip <<= '10.0.0.0/8'`. "
            "Comparison is on the underlying 32-bit address, so it is a single "
            "mask-and-compare per row with no text parsing."
        ),
        token="<<=",
        category="comparison",
        node_kind="comparison",
        friendly_name="IP contained by",
        syntax_forms=("<address> <<= <network>",),
        operands=(
            OperandDefinition(
                "address",
                "An IPv4 address. It is held as its 32-bit integer value, which is why the "
                "signature below reads INTEGER.",
            ),
            OperandDefinition(
                "network",
                "The network to test against, in CIDR notation. The prefix length is "
                "required: an address without one is rejected.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT CAST('10.0.0.1' AS IPV4) <<= '10.0.0.0/8';",
                ("true",),
            ),
        ),
        see_also=("IPContains",),
        notes=(
            "Spelling follows PostgreSQL, CockroachDB and DuckDB's inet extension. "
            "A NULL address is not contained by any network and yields false. An "
            "invalid or prefix-less CIDR raises rather than matching nothing."
        ),
    ),
    "IPContains": OperatorDefinition(
        summary="IPv4 CIDR containment operator, reversed.",
        documentation=(
            "Returns true when the network on the left, in CIDR notation, contains "
            "the IPv4 address on the right, for example `'10.0.0.0/8' >>= ip`. "
            "The mirror of `<<=`."
        ),
        token=">>=",
        category="comparison",
        node_kind="comparison",
        friendly_name="IP contains",
        syntax_forms=("<network> >>= <address>",),
        operands=(
            OperandDefinition(
                "network",
                "The network to test against, in CIDR notation, with its prefix length.",
            ),
            OperandDefinition(
                "address",
                "An IPv4 address. It is held as its 32-bit integer value, which is why the "
                "signature below reads INTEGER.",
            ),
        ),
        examples=(
            ExampleDefinition(
                "SELECT '10.0.0.0/8' >>= CAST('10.0.0.1' AS IPV4);",
                ("true",),
            ),
        ),
        see_also=("IPContainedBy",),
        notes="Spelling follows PostgreSQL, CockroachDB and DuckDB's inet extension.",
    ),
}


# Reverse index of the vocabulary above: SQL spelling -> canonical operator name.
# Built from the definitions rather than hand-listed so the two directions cannot
# drift. A spelling that names two operators is a defect in the catalog - it makes
# the mapping ambiguous - so it fails here, at import, rather than silently
# resolving to whichever definition happened to be last.
SQL_SYMBOLS_TO_OPERATORS = {}
for _operator, _definition in OPERATOR_DEFINITIONS.items():
    _symbol = _definition.sql_symbol or _definition.token
    if _symbol is None:
        continue
    if _symbol in SQL_SYMBOLS_TO_OPERATORS:
        raise ValueError(
            f"Operator catalog is ambiguous: SQL symbol '{_symbol}' is claimed by both "
            f"'{SQL_SYMBOLS_TO_OPERATORS[_symbol]}' and '{_operator}'."
        )
    SQL_SYMBOLS_TO_OPERATORS[_symbol] = _operator


# The documentation fields are only worth having if they are complete and
# internally consistent, so the catalog checks itself at import rather than
# letting a half-filled entry reach the published reference. Every operator here
# is binary, so two operands is the shape - one or three means an entry was
# edited without its syntax form.
for _operator, _definition in OPERATOR_DEFINITIONS.items():
    if len(_definition.operands) != 2:
        raise ValueError(
            f"Operator catalog entry '{_operator}' declares {len(_definition.operands)} "
            "operands; every operator in this catalog is binary and needs exactly 2."
        )
    if not _definition.syntax_forms:
        raise ValueError(f"Operator catalog entry '{_operator}' has no syntax form.")
    if not _definition.examples:
        raise ValueError(f"Operator catalog entry '{_operator}' has no example.")
    for _related in _definition.see_also:
        if _related not in OPERATOR_DEFINITIONS:
            raise ValueError(
                f"Operator catalog entry '{_operator}' points at '{_related}' in see_also, "
                "which is not an operator in this catalog."
            )
