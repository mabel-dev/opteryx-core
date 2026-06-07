"""Curated operator metadata layered onto generated operator exports."""

from dataclasses import dataclass


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
    ),
    "Or": OperatorDefinition(
        summary="Logical disjunction.",
        documentation="Returns true when either boolean operand evaluates to true.",
        token="OR",
        category="logical",
        node_kind="logical",
        friendly_name="Logical OR",
    ),
    "Xor": OperatorDefinition(
        summary="Logical exclusive OR.",
        documentation="Returns true when exactly one boolean operand evaluates to true.",
        token="XOR",
        category="logical",
        node_kind="logical",
        friendly_name="Logical XOR",
    ),
    "Eq": OperatorDefinition(
        summary="Equality comparison.",
        documentation="Returns true when both operands compare equal.",
        token="=",
        category="comparison",
        node_kind="comparison",
        friendly_name="Equals",
    ),
    "NotEq": OperatorDefinition(
        summary="Inequality comparison.",
        documentation="Returns true when the operands do not compare equal.",
        token="!=",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not equals",
    ),
    "Gt": OperatorDefinition(
        summary="Greater-than comparison.",
        documentation="Returns true when the left operand is greater than the right operand.",
        token=">",
        category="comparison",
        node_kind="comparison",
        friendly_name="Greater than",
    ),
    "GtEq": OperatorDefinition(
        summary="Greater-than-or-equal comparison.",
        documentation="Returns true when the left operand is greater than or equal to the right operand.",
        token=">=",
        category="comparison",
        node_kind="comparison",
        friendly_name="Greater than or equal",
    ),
    "Lt": OperatorDefinition(
        summary="Less-than comparison.",
        documentation="Returns true when the left operand is less than the right operand.",
        token="<",
        category="comparison",
        node_kind="comparison",
        friendly_name="Less than",
    ),
    "LtEq": OperatorDefinition(
        summary="Less-than-or-equal comparison.",
        documentation="Returns true when the left operand is less than or equal to the right operand.",
        token="<=",
        category="comparison",
        node_kind="comparison",
        friendly_name="Less than or equal",
    ),
    "InList": OperatorDefinition(
        summary="Membership comparison.",
        documentation="Returns true when the left operand matches any element in the right-hand list or array.",
        token="IN",
        category="comparison",
        node_kind="comparison",
        friendly_name="In list",
    ),
    "NotInList": OperatorDefinition(
        summary="Negated membership comparison.",
        documentation="Returns true when the left operand does not match any element in the right-hand list or array.",
        token="NOT IN",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not in list",
    ),
    "Like": OperatorDefinition(
        summary="Pattern match comparison.",
        documentation="Returns true when the left string matches the SQL LIKE pattern on the right.",
        token="LIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Like",
    ),
    "NotLike": OperatorDefinition(
        summary="Negated pattern match comparison.",
        documentation="Returns true when the left string does not match the SQL LIKE pattern on the right.",
        token="NOT LIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not like",
    ),
    "ILike": OperatorDefinition(
        summary="Case-insensitive pattern match comparison.",
        documentation="Returns true when the left string matches the SQL ILIKE pattern on the right without case sensitivity.",
        token="ILIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Case-insensitive like",
    ),
    "NotILike": OperatorDefinition(
        summary="Negated case-insensitive pattern match comparison.",
        documentation="Returns true when the left string does not match the SQL ILIKE pattern on the right.",
        token="NOT ILIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not case-insensitive like",
    ),
    "RLike": OperatorDefinition(
        summary="Regular expression match comparison.",
        documentation="Returns true when the left string matches the regular expression on the right.",
        token="RLIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Regex like",
    ),
    "NotRLike": OperatorDefinition(
        summary="Negated regular expression match comparison.",
        documentation="Returns true when the left string does not match the regular expression on the right.",
        token="NOT RLIKE",
        category="comparison",
        node_kind="comparison",
        friendly_name="Not regex like",
    ),
    "Plus": OperatorDefinition(
        summary="Addition operator.",
        documentation="Returns the sum of two numeric or interval-compatible operands.",
        token="+",
        category="binary",
        node_kind="binary",
        friendly_name="Addition",
    ),
    "Minus": OperatorDefinition(
        summary="Subtraction operator.",
        documentation="Returns the difference between two numeric, date, timestamp, or interval-compatible operands.",
        token="-",
        category="binary",
        node_kind="binary",
        friendly_name="Subtraction",
    ),
    "Multiply": OperatorDefinition(
        summary="Multiplication operator.",
        documentation="Returns the product of two numeric operands.",
        token="*",
        category="binary",
        node_kind="binary",
        friendly_name="Multiplication",
    ),
    "Divide": OperatorDefinition(
        summary="Division operator.",
        documentation="Returns the quotient of two numeric operands.",
        token="/",
        category="binary",
        node_kind="binary",
        friendly_name="Division",
    ),
    "Modulo": OperatorDefinition(
        summary="Modulo operator.",
        documentation="Returns the remainder after division of the left numeric operand by the right numeric operand.",
        token="%",
        category="binary",
        node_kind="binary",
        friendly_name="Modulo",
    ),
    "MyIntegerDivide": OperatorDefinition(
        summary="Integer division operator.",
        documentation="Divides two integers and truncates the result toward zero.",
        token="DIV",
        category="binary",
        node_kind="binary",
        friendly_name="Integer division",
    ),
    "BitwiseOr": OperatorDefinition(
        summary="Bitwise OR and IP containment operator.",
        documentation=(
            "Combines integer operands using a bitwise OR operation. "
            "When used with address and CIDR-compatible operands, the same `|` token "
            "acts as an IP containment predicate and returns a boolean result."
        ),
        token="|",
        category="bitwise",
        node_kind="binary",
        friendly_name="Bitwise OR",
        notes=(
            "This operator is overloaded by operand type: integer inputs perform bitwise OR, "
            "while address/CIDR-style inputs use `|` for containment checks."
        ),
    ),
    "BitwiseAnd": OperatorDefinition(
        summary="Bitwise AND operator.",
        documentation="Combines integer operands using a bitwise AND operation.",
        token="&",
        category="bitwise",
        node_kind="binary",
        friendly_name="Bitwise AND",
    ),
    "BitwiseXor": OperatorDefinition(
        summary="Bitwise XOR operator.",
        documentation="Combines integer operands using a bitwise exclusive OR operation.",
        token="^",
        category="bitwise",
        node_kind="binary",
        friendly_name="Bitwise XOR",
    ),
    "ShiftLeft": OperatorDefinition(
        summary="Left shift operator.",
        documentation="Shifts the bits of the left integer operand left by the number of positions in the right operand.",
        token="<<",
        category="bitwise",
        node_kind="binary",
        friendly_name="Left shift",
    ),
    "ShiftRight": OperatorDefinition(
        summary="Right shift operator.",
        documentation="Shifts the bits of the left integer operand right by the number of positions in the right operand.",
        token=">>",
        category="bitwise",
        node_kind="binary",
        friendly_name="Right shift",
    ),
    "StringConcat": OperatorDefinition(
        summary="String concatenation operator.",
        documentation="Concatenates the left and right string or blob operands.",
        token="||",
        category="binary",
        node_kind="binary",
        friendly_name="Concatenation",
    ),
    "Arrow": OperatorDefinition(
        summary="JSON extraction operator.",
        documentation="Returns the selected JSON value from a document or JSON-like value.",
        token="->",
        category="extraction",
        node_kind="extraction",
        friendly_name="JSON extract",
        notes="The result type is dynamic because the selected JSON value may be scalar, object, array, or null.",
    ),
    "LongArrow": OperatorDefinition(
        summary="JSON text extraction operator.",
        documentation="Returns the selected JSON value encoded as a blob or text-like binary value.",
        token="->>",
        category="extraction",
        node_kind="extraction",
        friendly_name="JSON extract text",
    ),
    "MapAccess": OperatorDefinition(
        summary="Subscript access operator.",
        documentation="Returns the element at the requested index from an array, string, or blob-like value.",
        token="[]",
        category="extraction",
        node_kind="extraction",
        friendly_name="Subscript access",
        notes="Subcript access is zero-based, the first element is at index 0. For arrays the result type depends on the array element type, so the exported result type may be dynamic.",
    ),
    "AtQuestion": OperatorDefinition(
        summary="JSON path existence operator.",
        documentation="Returns true when the supplied JSON path expression matches within the left document.",
        token="@?",
        category="comparison",
        node_kind="comparison",
        friendly_name="JSON path exists",
    ),
    "AtArrow": OperatorDefinition(
        summary="Array containment operator.",
        documentation="Returns true when the left array contains any of the values provided by the right array.",
        token="@>",
        category="comparison",
        node_kind="comparison",
        friendly_name="Array contains any",
    ),
    "ArrayContainsAll": OperatorDefinition(
        summary="Array contains-all operator.",
        documentation="Returns true when the left array contains all values from the right array.",
        token="@>>",
        category="comparison",
        node_kind="comparison",
        friendly_name="Array contains all",
    ),
}
