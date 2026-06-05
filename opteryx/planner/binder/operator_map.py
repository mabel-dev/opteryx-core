from typing import Callable, Dict, NamedTuple, Optional, Tuple

from opteryx.exceptions import IncorrectTypeError, UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression.operator_catalog import is_known_operator
from opteryx.types import OrsoTypes
from opteryx.types.logical_type import LogicalCategory
from opteryx.utils.sql import convert_camel_to_sql_case


class OperatorMapType(NamedTuple):
    result_type: OrsoTypes
    operation_function: Optional[Callable] = None
    cost_estimate: float = 100.0


# fmt: off
OPERATOR_MAP: Dict[Tuple[OrsoTypes, OrsoTypes, str], OperatorMapType] = {
    (OrsoTypes.ARRAY, OrsoTypes.ARRAY, "AtArrow"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.ARRAY, OrsoTypes.ARRAY, "ArrayContainsAll"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.ARRAY, OrsoTypes.INTEGER, "MapAccess"): OperatorMapType(OrsoTypes._MISSING_TYPE, None, 100.0),
    (OrsoTypes.VECTOR, OrsoTypes.INTEGER, "MapAccess"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "StringConcat"): OperatorMapType(OrsoTypes.BLOB, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.VARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.BLOB, "StringConcat"): OperatorMapType(OrsoTypes.BLOB, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.INTEGER, "MapAccess"): OperatorMapType(OrsoTypes.BLOB, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.BOOLEAN, "Or"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.BOOLEAN, "And"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.BOOLEAN, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BOOLEAN, OrsoTypes.BOOLEAN, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "Minus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.DATE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.INTERVAL, "Minus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.INTERVAL, "Plus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DATE, OrsoTypes.TIMESTAMP, "Minus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    # DECIMAL +/- DECIMAL is DECIMAL (the runtime add/sub kernels produce DECIMAL);
    # the old INTEGER result was a plain type error, inconsistent with Multiply/Divide
    # below and with the runtime.
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Plus"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Minus"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Divide"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DECIMAL, "Multiply"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Plus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Minus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Multiply"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.DOUBLE, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "Plus"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "Minus"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "Multiply"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DECIMAL, OrsoTypes.INTEGER, "Divide"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Plus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Minus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Multiply"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Plus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Minus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Multiply"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.DOUBLE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Multiply"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Plus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Minus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.DOUBLE, OrsoTypes.INTEGER, "Modulo"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    # INTEGER op DECIMAL is symmetric with DECIMAL op INTEGER (lines above) and the
    # runtime decimal kernels treat the int operand as a decimal — so the result is
    # DECIMAL, not DOUBLE. The old DOUBLE entries desynced the binder from the runtime
    # (an ungrouped MAX/MIN/SUM over such an expression routes by bind-time type and
    # then meets a DECIMAL vector — q15's `1 - l_discount`).
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Divide"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Multiply"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Plus"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Minus"): OperatorMapType(OrsoTypes.DECIMAL, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Multiply"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Plus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, "Minus"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Plus"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Minus"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Divide"): OperatorMapType(OrsoTypes.DOUBLE, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Multiply"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "Modulo"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "MyIntegerDivide"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "BitwiseOr"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "BitwiseAnd"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "BitwiseXor"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "ShiftLeft"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.INTEGER, "ShiftRight"): OperatorMapType(OrsoTypes.INTEGER, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTEGER, OrsoTypes.DECIMAL, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Plus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Minus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.INTERVAL, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.TIMESTAMP, "Plus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.TIMESTAMP, "Minus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.DATE, "Plus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.INTERVAL, OrsoTypes.DATE, "Minus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "StringConcat"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "StringConcat"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "StringConcat"): OperatorMapType(OrsoTypes.BLOB, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "StringConcat"): OperatorMapType(OrsoTypes.BLOB, None, 100.0),
    # MapAccess (string[i]) preserves the operand's string type (matches the
    # NVARCHAR StringConcat decision).
    (OrsoTypes.NVARCHAR, OrsoTypes.INTEGER, "MapAccess"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    # VARIANT (result of `->`) is extraction-only: it chains through the JSON
    # accessors with no user cast; every other operator falls through to the
    # map-miss "not supported / cast required" error.
    (OrsoTypes.VARIANT, OrsoTypes.VARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.VARIANT, OrsoTypes.VARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.VARIANT, OrsoTypes.VARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARIANT, OrsoTypes.NVARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.VARIANT, OrsoTypes.NVARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.VARIANT, OrsoTypes.NVARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARIANT, OrsoTypes.INTEGER, "MapAccess"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    # JSON / path operators — NVARCHAR accepted as document or key wherever
    # VARCHAR is (JSON is UTF-8 by spec). Result types mirror VARCHAR exactly.
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.VARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.NVARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.NVARCHAR, OrsoTypes.BLOB, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.BLOB, OrsoTypes.NVARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.NVARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.NVARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.NVARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.NVARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.NVARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.NVARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.VARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.VARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.VARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.BLOB, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.BLOB, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.STRUCT, OrsoTypes.BLOB, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.DATE, "Minus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL, "Minus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.INTERVAL, "Plus"): OperatorMapType(OrsoTypes.TIMESTAMP, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "Minus"): OperatorMapType(OrsoTypes.INTERVAL, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.ARRAY, "InList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.ARRAY, "NotInList"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "StringConcat"): OperatorMapType(OrsoTypes.BLOB, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.NVARCHAR, "StringConcat"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Eq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "NotEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Gt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "GtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Lt"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "LtEq"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Like"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "NotLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "ILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "NotILike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "RLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "NotRLike"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "BitwiseOr"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "StringConcat"): OperatorMapType(OrsoTypes.VARCHAR, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.INTEGER, "MapAccess"): OperatorMapType(OrsoTypes.VARCHAR, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.BLOB, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.VARCHAR, OrsoTypes.VARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.BLOB, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.BLOB, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.BLOB, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.VARCHAR, "Arrow"): OperatorMapType(OrsoTypes.VARIANT, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.VARCHAR, "LongArrow"): OperatorMapType(OrsoTypes.NVARCHAR, None, 100.0),
    (OrsoTypes.JSONB, OrsoTypes.VARCHAR, "AtQuestion"): OperatorMapType(OrsoTypes.BOOLEAN, None, 100.0),

}
# fmt:on

for _, _, _operator_name in OPERATOR_MAP:
    if not is_known_operator(_operator_name):
        raise UnsupportedSyntaxError(f"Operator map contains unknown operator '{_operator_name}'.")


# ---------------------------------------------------------------------------
# Decision B: operator dispatch keys on LogicalCategory, not OrsoTypes.
#
# Rather than hand-convert 348 dict-literal keys (a Python dict literal silently
# deduplicates collapsed keys — e.g. STRUCT/JSONB → NVARCHAR — which would drop
# entries unnoticed), the category-keyed map is DERIVED from OPERATOR_MAP with an
# explicit collision guard. Verified: the collapse produces ZERO conflicting results.
#
# EXIT PLAN (see "Exit Plan for Bridges & Shims"): OPERATOR_MAP (OrsoTypes-keyed)
# remains the human-edited source during migration; once operands are ColumnType
# everywhere and OrsoTypes is being deleted, the source map is re-authored directly on
# LogicalCategory and this derivation removed.
# ---------------------------------------------------------------------------
_ORSO_TO_CATEGORY: Dict[OrsoTypes, LogicalCategory] = {
    OrsoTypes.BOOLEAN: LogicalCategory.BOOLEAN,
    OrsoTypes.INTEGER: LogicalCategory.INTEGER,
    OrsoTypes.DOUBLE: LogicalCategory.FLOAT,
    OrsoTypes.DECIMAL: LogicalCategory.DECIMAL,
    OrsoTypes.VARCHAR: LogicalCategory.VARCHAR,
    OrsoTypes.NVARCHAR: LogicalCategory.NVARCHAR,
    OrsoTypes.BLOB: LogicalCategory.VARBINARY,
    OrsoTypes.DATE: LogicalCategory.DATE,
    OrsoTypes.TIME: LogicalCategory.TIME,
    OrsoTypes.TIMESTAMP: LogicalCategory.TIMESTAMP,
    OrsoTypes.INTERVAL: LogicalCategory.INTERVAL,
    OrsoTypes.ARRAY: LogicalCategory.ARRAY,
    OrsoTypes.VECTOR: LogicalCategory.VECTOR,
    OrsoTypes.VARIANT: LogicalCategory.VARIANT,
    OrsoTypes.NULL: LogicalCategory.NULL,
    OrsoTypes.STRUCT: LogicalCategory.NVARCHAR,  # collapse (Decision: STRUCT → JSON text)
    OrsoTypes.JSONB: LogicalCategory.NVARCHAR,   # collapse (Decision: JSONB alias NVARCHAR)
}

_CATEGORY_OPERATOR_MAP: Dict[Tuple[LogicalCategory, LogicalCategory, str], OperatorMapType] = {}
for (_lt, _rt, _op), _val in OPERATOR_MAP.items():
    _lc = _ORSO_TO_CATEGORY.get(_lt)
    _rc = _ORSO_TO_CATEGORY.get(_rt)
    if _lc is None or _rc is None:
        continue  # _MISSING_TYPE etc. — never a real operand at lookup time
    _key = (_lc, _rc, _op)
    _existing = _CATEGORY_OPERATOR_MAP.get(_key)
    if _existing is not None and (
        _existing.result_type != _val.result_type
        or _existing.operation_function != _val.operation_function
    ):
        raise ValueError(
            f"operator-map category collapse conflict at {_key}: "
            f"{_existing.result_type} vs {_val.result_type} — resolve before relabeling"
        )
    _CATEGORY_OPERATOR_MAP[_key] = _val


def _category_of(orso_type) -> Optional[LogicalCategory]:
    """Operand OrsoTypes → dispatch LogicalCategory (migration-time projection)."""
    return _ORSO_TO_CATEGORY.get(orso_type)


def _is_internal_operator(operator: str) -> bool:
    return operator.startswith(("AnyOp", "AllOp")) or operator in {
        "InSubQuery",
        "NotInSubQuery",
    }


def determine_type(node) -> OrsoTypes:
    # initial version, needs to be improved
    if node.node_type in (
        NodeType.UNARY_OPERATOR,
        NodeType.AND,
        NodeType.OR,
        NodeType.NOT,
        NodeType.XOR,
    ):
        if node.value in (
            "IsTrue",
            "IsFalse",
            "IsNotTrue",
            "IsNotFalse",
        ) and node.centre.schema_column.type not in (OrsoTypes.BOOLEAN, OrsoTypes._MISSING_TYPE, 0):
            raise IncorrectTypeError(
                f"Expected a BOOLEAN value for {convert_camel_to_sql_case(node.value)}, but received {node.centre.schema_column.type}."
            )
        if node.value == "BitwiseNot":
            operand_type = node.centre.schema_column.type
            if operand_type not in (OrsoTypes.INTEGER, OrsoTypes._MISSING_TYPE, 0):
                raise IncorrectTypeError(
                    f"Expected an INTEGER value for bitwise NOT (~), but received {operand_type}."
                )
            return OrsoTypes.INTEGER
        return OrsoTypes.BOOLEAN
    if node.node_type == NodeType.NESTED:
        return determine_type(node.centre)
    if node.node_type == NodeType.WILDCARD:
        return OrsoTypes._MISSING_TYPE
    if node.node_type == NodeType.EXPRESSION_LIST:
        if node.parameters[-1].type is not None:
            return node.parameters[-1].type
        return OrsoTypes._MISSING_TYPE  # we can work this out
    if node.node_type == NodeType.LITERAL:
        return node.type

    if node.value in ("NotInSubQuery", "InSubQuery"):
        return OrsoTypes.BOOLEAN

    if node.schema_column:
        return node.schema_column.type

    if node.left.node_type == NodeType.LITERAL:
        left_type = node.left.type
    elif node.left.schema_column:
        left_type = node.left.schema_column.type

    if node.right.node_type == NodeType.LITERAL:
        right_type = node.right.type
    elif node.right.schema_column:
        right_type = node.right.schema_column.type

    operator = node.value
    if not is_known_operator(operator) and not _is_internal_operator(operator):
        raise UnsupportedSyntaxError(f"Unsupported operator '{operator}'.")

    if left_type in (0, OrsoTypes._MISSING_TYPE, OrsoTypes.NULL):
        return OrsoTypes._MISSING_TYPE
    if right_type in (0, OrsoTypes._MISSING_TYPE, OrsoTypes.NULL):
        return OrsoTypes._MISSING_TYPE

    # Dispatch on LogicalCategory (Decision B): width-collapsed, JSON-family-aware.
    left_category = _category_of(left_type)
    right_category = _category_of(right_type)
    result = None
    if left_category is not None and right_category is not None:
        result = _CATEGORY_OPERATOR_MAP.get((left_category, right_category, operator))

    if result is None:
        from opteryx.expression import format_expression

        raise IncorrectTypeError(
            f"Unable to perform `{format_expression(node)}` because the values are not acceptable types for this operation. {left_type} and {right_type} were provided, you may need to cast one or both values to acceptable types."
        )

    if (
        operator == "MapAccess"
        and left_type in (OrsoTypes.ARRAY, OrsoTypes.VECTOR)
        and right_type == OrsoTypes.INTEGER
    ):
        # ARRAY<T>[INTEGER] resolves to T when we know the element type.
        element_type = None
        if node.left.schema_column is not None:
            element_type = node.left.schema_column.element_type
        if left_type == OrsoTypes.VECTOR:
            return (
                element_type
                if element_type not in (None, 0, OrsoTypes._MISSING_TYPE, OrsoTypes.NULL)
                else OrsoTypes.DOUBLE
            )
        return (
            element_type
            if element_type not in (None, 0, OrsoTypes._MISSING_TYPE, OrsoTypes.NULL)
            else OrsoTypes._MISSING_TYPE
        )

    return result.result_type
