"""Curated operator metadata layered onto generated operator exports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from orso.types import OrsoTypes


@dataclass(frozen=True)
class OperatorSignatureDefinition:
    left_type: OrsoTypes
    right_type: OrsoTypes
    result_type: Optional[OrsoTypes]
    cost_estimate: float = 100.0


@dataclass(frozen=True)
class OperatorDefinition:
    summary: str
    documentation: str
    token: str | None = None
    category: str | None = None
    notes: str | None = None
    signatures: tuple[OperatorSignatureDefinition, ...] = ()


OPERATOR_DEFINITIONS = {
    "And": OperatorDefinition(
        summary="Logical conjunction.",
        documentation="Returns true only when both boolean operands evaluate to true.",
        token="AND",
        category="logical",
    ),
    "Or": OperatorDefinition(
        summary="Logical disjunction.",
        documentation="Returns true when either boolean operand evaluates to true.",
        token="OR",
        category="logical",
    ),
    "Xor": OperatorDefinition(
        summary="Logical exclusive OR.",
        documentation="Returns true when exactly one boolean operand evaluates to true.",
        token="XOR",
        category="logical",
        signatures=(
            OperatorSignatureDefinition(
                left_type=OrsoTypes.BOOLEAN,
                right_type=OrsoTypes.BOOLEAN,
                result_type=OrsoTypes.BOOLEAN,
            ),
        ),
    ),
    "Eq": OperatorDefinition(
        summary="Equality comparison.",
        documentation="Returns true when both operands compare equal.",
        token="=",
        category="comparison",
    ),
    "NotEq": OperatorDefinition(
        summary="Inequality comparison.",
        documentation="Returns true when the operands do not compare equal.",
        token="!=",
        category="comparison",
    ),
    "Gt": OperatorDefinition(
        summary="Greater-than comparison.",
        documentation="Returns true when the left operand is greater than the right operand.",
        token=">",
        category="comparison",
    ),
    "GtEq": OperatorDefinition(
        summary="Greater-than-or-equal comparison.",
        documentation="Returns true when the left operand is greater than or equal to the right operand.",
        token=">=",
        category="comparison",
    ),
    "Lt": OperatorDefinition(
        summary="Less-than comparison.",
        documentation="Returns true when the left operand is less than the right operand.",
        token="<",
        category="comparison",
    ),
    "LtEq": OperatorDefinition(
        summary="Less-than-or-equal comparison.",
        documentation="Returns true when the left operand is less than or equal to the right operand.",
        token="<=",
        category="comparison",
    ),
    "InList": OperatorDefinition(
        summary="Membership comparison.",
        documentation="Returns true when the left operand matches any element in the right-hand list or array.",
        token="IN",
        category="comparison",
    ),
    "NotInList": OperatorDefinition(
        summary="Negated membership comparison.",
        documentation="Returns true when the left operand does not match any element in the right-hand list or array.",
        token="NOT IN",
        category="comparison",
    ),
    "Like": OperatorDefinition(
        summary="Pattern match comparison.",
        documentation="Returns true when the left string matches the SQL LIKE pattern on the right.",
        token="LIKE",
        category="comparison",
    ),
    "NotLike": OperatorDefinition(
        summary="Negated pattern match comparison.",
        documentation="Returns true when the left string does not match the SQL LIKE pattern on the right.",
        token="NOT LIKE",
        category="comparison",
    ),
    "ILike": OperatorDefinition(
        summary="Case-insensitive pattern match comparison.",
        documentation="Returns true when the left string matches the SQL ILIKE pattern on the right without case sensitivity.",
        token="ILIKE",
        category="comparison",
    ),
    "NotILike": OperatorDefinition(
        summary="Negated case-insensitive pattern match comparison.",
        documentation="Returns true when the left string does not match the SQL ILIKE pattern on the right.",
        token="NOT ILIKE",
        category="comparison",
    ),
    "RLike": OperatorDefinition(
        summary="Regular expression match comparison.",
        documentation="Returns true when the left string matches the regular expression on the right.",
        token="RLIKE",
        category="comparison",
    ),
    "NotRLike": OperatorDefinition(
        summary="Negated regular expression match comparison.",
        documentation="Returns true when the left string does not match the regular expression on the right.",
        token="NOT RLIKE",
        category="comparison",
    ),
    "Plus": OperatorDefinition(
        summary="Addition operator.",
        documentation="Returns the sum of two numeric or interval-compatible operands.",
        token="+",
        category="binary",
    ),
    "Minus": OperatorDefinition(
        summary="Subtraction operator.",
        documentation="Returns the difference between two numeric, date, timestamp, or interval-compatible operands.",
        token="-",
        category="binary",
    ),
    "Multiply": OperatorDefinition(
        summary="Multiplication operator.",
        documentation="Returns the product of two numeric operands.",
        token="*",
        category="binary",
    ),
    "Divide": OperatorDefinition(
        summary="Division operator.",
        documentation="Returns the quotient of two numeric operands.",
        token="/",
        category="binary",
    ),
    "Modulo": OperatorDefinition(
        summary="Modulo operator.",
        documentation="Returns the remainder after division of the left numeric operand by the right numeric operand.",
        token="%",
        category="binary",
    ),
    "MyIntegerDivide": OperatorDefinition(
        summary="Integer division operator.",
        documentation="Divides two integers and truncates the result toward zero.",
        token="DIV",
        category="binary",
    ),
    "BitwiseOr": OperatorDefinition(
        summary="Bitwise OR operator.",
        documentation="Combines integer operands using a bitwise OR operation.",
        token="|",
        category="binary",
        notes="The same token may also appear in non-bitwise contexts depending on operand types.",
    ),
    "BitwiseAnd": OperatorDefinition(
        summary="Bitwise AND operator.",
        documentation="Combines integer operands using a bitwise AND operation.",
        token="&",
        category="binary",
    ),
    "BitwiseXor": OperatorDefinition(
        summary="Bitwise XOR operator.",
        documentation="Combines integer operands using a bitwise exclusive OR operation.",
        token="^",
        category="binary",
    ),
    "ShiftLeft": OperatorDefinition(
        summary="Left shift operator.",
        documentation="Shifts the bits of the left integer operand left by the number of positions in the right operand.",
        token="<<",
        category="binary",
    ),
    "ShiftRight": OperatorDefinition(
        summary="Right shift operator.",
        documentation="Shifts the bits of the left integer operand right by the number of positions in the right operand.",
        token=">>",
        category="binary",
    ),
    "StringConcat": OperatorDefinition(
        summary="String concatenation operator.",
        documentation="Concatenates the left and right string or blob operands.",
        token="||",
        category="binary",
    ),
    "Arrow": OperatorDefinition(
        summary="JSON extraction operator.",
        documentation="Returns the selected JSON value from a document or JSON-like value.",
        token="->",
        category="extraction",
        notes="The result type is dynamic because the selected JSON value may be scalar, object, array, or null.",
    ),
    "LongArrow": OperatorDefinition(
        summary="JSON text extraction operator.",
        documentation="Returns the selected JSON value encoded as a blob or text-like binary value.",
        token="->>",
        category="extraction",
    ),
    "MapAccess": OperatorDefinition(
        summary="Subscript access operator.",
        documentation="Returns the element at the requested index from an array, string, or blob-like value.",
        token="[]",
        category="extraction",
        notes="For arrays the result type depends on the array element type, so the exported result type may be dynamic.",
    ),
    "AtQuestion": OperatorDefinition(
        summary="JSON path existence operator.",
        documentation="Returns true when the supplied JSON path expression matches within the left document.",
        token="@?",
        category="comparison",
    ),
    "AtArrow": OperatorDefinition(
        summary="Array containment operator.",
        documentation="Returns true when the left array contains any of the values provided by the right array.",
        token="@>",
        category="comparison",
    ),
    "ArrayContainsAll": OperatorDefinition(
        summary="Array contains-all operator.",
        documentation="Returns true when the left array contains all values from the right array.",
        token="@>>",
        category="comparison",
    ),
}
