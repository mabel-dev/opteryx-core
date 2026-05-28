"""Operator sets for binder use.

Phase 6: Runtime dispatch moved to resolve_binary_op at bind time.
Phase 3: EXTRACTION_OPERATOR (MapAccess, Arrow, LongArrow) moved to bind-time
resolution in compiled_expression.pyx with direct native kernel calls.
"""


# Binary operators set for binder/planner use.
# Maps all supported binary operators.
BINARY_OPERATORS = {
    "Plus", "Minus", "Multiply", "Divide", "Modulo", "MyIntegerDivide",
    "StringConcat",
    "BitwiseOr", "BitwiseAnd", "BitwiseXor", "ShiftLeft", "ShiftRight",
}

# Phase 3: EXTRACTION_OPERATORS entries (Arrow, LongArrow, MapAccess) moved to
# bind-time resolution in compiled_expression.pyx; this set remains for binder use only.
EXTRACTION_OPERATORS = {"Arrow", "LongArrow", "MapAccess"}
