from typing import Callable, Dict, NamedTuple, Optional, Tuple

from opteryx.exceptions import IncorrectTypeError, UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression.operator_catalog import is_known_operator
from opteryx.types.logical_type import LogicalCategory as OT
from opteryx.types.logical_type import LogicalCategory as LC
from opteryx.utils.sql import convert_camel_to_sql_case

# Phase 3: OPERATOR_MAP is now authored directly on LogicalCategory (LC) keys.
# Integer widths collapse to LC.INTEGER, floats to LC.FLOAT, JSONB/STRUCT to
# LC.NVARCHAR — the map carries 330 entries (vs the LogicalCategory-keyed 348 that had
# 18 duplicate-collapse entries). The derivation step (old _CATEGORY_OPERATOR_MAP)
# is deleted; this IS the map. The human-authorable source is now stable since
# logical categories don't change with Draken enum churn.
#
# result_type field: still LogicalCategory (Phase 4 will migrate to LogicalCategory
# when determine_type's callers are migrated off LogicalCategory).

# Shorten the verbose aliases in this file
OMT = NamedTuple(
    "OperatorMapType",
    [("result_type", OT), ("operation_function", Optional[Callable]), ("cost_estimate", float)],
)


class OperatorMapType(NamedTuple):
    result_type: Optional[OT]
    operation_function: Optional[Callable] = None
    cost_estimate: float = 100.0


OMT = OperatorMapType

# fmt: off
OPERATOR_MAP: Dict[Tuple[LC, LC, str], OperatorMapType] = {
    (LC.ARRAY       , LC.ARRAY       , 'ArrayContainsAll'        ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.ARRAY       , LC.ARRAY       , 'AtArrow'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.ARRAY       , LC.INTEGER     , 'MapAccess'               ): OMT(None, None, 100.0),
    (LC.BOOLEAN     , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.BOOLEAN     , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.BOOLEAN     , LC.BOOLEAN     , 'And'                     ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.BOOLEAN     , LC.BOOLEAN     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.BOOLEAN     , LC.BOOLEAN     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.BOOLEAN     , LC.BOOLEAN     , 'Or'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.DATE        , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.DATE        , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.DATE        , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.DATE        , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.DATE        , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.DATE        , 'Minus'                   ): OMT(OT.INTERVAL, None, 100.0),
    (LC.DATE        , LC.DATE        , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.INTERVAL    , 'Minus'                   ): OMT(OT.TIMESTAMP, None, 100.0),
    (LC.DATE        , LC.INTERVAL    , 'Plus'                    ): OMT(OT.TIMESTAMP, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'Minus'                   ): OMT(OT.INTERVAL, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Divide'                  ): OMT(OT.DECIMAL, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Minus'                   ): OMT(OT.DECIMAL, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Multiply'                ): OMT(OT.DECIMAL, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Plus'                    ): OMT(OT.DECIMAL, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Divide'                  ): OMT(OT.DOUBLE, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Minus'                   ): OMT(OT.DOUBLE, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Multiply'                ): OMT(OT.DOUBLE, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Plus'                    ): OMT(OT.DOUBLE, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Divide'                  ): OMT(OT.DECIMAL, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Minus'                   ): OMT(OT.DECIMAL, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Multiply'                ): OMT(OT.DECIMAL, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Plus'                    ): OMT(OT.DECIMAL, None, 100.0),
    (LC.FLOAT       , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Divide'                  ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Minus'                   ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Multiply'                ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Plus'                    ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Divide'                  ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Minus'                   ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Multiply'                ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Plus'                    ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Divide'                  ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Minus'                   ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Modulo'                  ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Multiply'                ): OMT(OT.DOUBLE, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Plus'                    ): OMT(OT.DOUBLE, None, 100.0),
    (LC.INTEGER     , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Divide'                  ): OMT(OT.DECIMAL, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Minus'                   ): OMT(OT.DECIMAL, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Multiply'                ): OMT(OT.DECIMAL, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Plus'                    ): OMT(OT.DECIMAL, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Divide'                  ): OMT(OT.DOUBLE, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Minus'                   ): OMT(OT.DOUBLE, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Multiply'                ): OMT(OT.DOUBLE, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Plus'                    ): OMT(OT.DOUBLE, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'BitwiseAnd'              ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'BitwiseOr'               ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'BitwiseXor'              ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Divide'                  ): OMT(OT.DOUBLE, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Minus'                   ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Modulo'                  ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Multiply'                ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'MyIntegerDivide'         ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Plus'                    ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'ShiftLeft'               ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'ShiftRight'              ): OMT(OT.INTEGER, None, 100.0),
    (LC.INTERVAL    , LC.DATE        , 'Minus'                   ): OMT(OT.TIMESTAMP, None, 100.0),
    (LC.INTERVAL    , LC.DATE        , 'Plus'                    ): OMT(OT.TIMESTAMP, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Minus'                   ): OMT(OT.INTERVAL, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Plus'                    ): OMT(OT.INTERVAL, None, 100.0),
    (LC.INTERVAL    , LC.TIMESTAMP   , 'Minus'                   ): OMT(OT.TIMESTAMP, None, 100.0),
    (LC.INTERVAL    , LC.TIMESTAMP   , 'Plus'                    ): OMT(OT.TIMESTAMP, None, 100.0),
    (LC.NVARCHAR    , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.INTEGER     , 'MapAccess'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'BitwiseOr'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'StringConcat'            ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'BitwiseOr'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'StringConcat'            ): OMT(OT.BLOB, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'BitwiseOr'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'StringConcat'            ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.TIMESTAMP   , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'Minus'                   ): OMT(OT.INTERVAL, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.INTERVAL    , 'Minus'                   ): OMT(OT.TIMESTAMP, None, 100.0),
    (LC.TIMESTAMP   , LC.INTERVAL    , 'Plus'                    ): OMT(OT.TIMESTAMP, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'Minus'                   ): OMT(OT.INTERVAL, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.INTEGER     , 'MapAccess'               ): OMT(OT.BLOB, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'BitwiseOr'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'StringConcat'            ): OMT(OT.BLOB, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'StringConcat'            ): OMT(OT.BLOB, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'BitwiseOr'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'StringConcat'            ): OMT(OT.BLOB, None, 100.0),
    (LC.VARCHAR     , LC.ARRAY       , 'InList'                  ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.ARRAY       , 'NotInList'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.INTEGER     , 'MapAccess'               ): OMT(OT.VARCHAR, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'BitwiseOr'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'StringConcat'            ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'BitwiseOr'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'StringConcat'            ): OMT(OT.BLOB, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'BitwiseOr'               ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Eq'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Gt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'GtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'ILike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Like'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Lt'                      ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'LtEq'                    ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'NotEq'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'NotILike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'NotLike'                 ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'NotRLike'                ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'RLike'                   ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'StringConcat'            ): OMT(OT.VARCHAR, None, 100.0),
    (LC.VARIANT     , LC.INTEGER     , 'MapAccess'               ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARIANT     , LC.NVARCHAR    , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARIANT     , LC.NVARCHAR    , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARIANT     , LC.NVARCHAR    , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VARIANT     , LC.VARCHAR     , 'Arrow'                   ): OMT(OT.VARIANT, None, 100.0),
    (LC.VARIANT     , LC.VARCHAR     , 'AtQuestion'              ): OMT(OT.BOOLEAN, None, 100.0),
    (LC.VARIANT     , LC.VARCHAR     , 'LongArrow'               ): OMT(OT.NVARCHAR, None, 100.0),
    (LC.VECTOR      , LC.INTEGER     , 'MapAccess'               ): OMT(OT.DOUBLE, None, 100.0),
}
# fmt: on

for _, _, _operator_name in OPERATOR_MAP:
    if not is_known_operator(_operator_name):
        raise UnsupportedSyntaxError(f"Operator map contains unknown operator \'{_operator_name}\'.")


# Static LogicalCategory → LogicalCategory projection for operator-map dispatch.
# This is the only surviving use of LogicalCategory in this file — once determine_type's
# callers are migrated to pass column_type.category directly, this table can be
# removed (Phase 4).
_SQL_TO_LC: Dict[OT, LC] = {
    OT.BOOLEAN: LC.BOOLEAN,
    OT.INTEGER: LC.INTEGER,
    OT.DOUBLE: LC.FLOAT,
    OT.DECIMAL: LC.DECIMAL,
    OT.VARCHAR: LC.VARCHAR,
    OT.NVARCHAR: LC.NVARCHAR,
    OT.BLOB: LC.VARBINARY,
    OT.DATE: LC.DATE,
    OT.TIME: LC.TIME,
    OT.TIMESTAMP: LC.TIMESTAMP,
    OT.INTERVAL: LC.INTERVAL,
    OT.ARRAY: LC.ARRAY,
    OT.VECTOR: LC.VECTOR,
    OT.VARIANT: LC.VARIANT,
    OT.NULL: LC.NULL,
    OT.STRUCT: LC.NVARCHAR,  # STRUCT → JSON text
    OT.JSONB: LC.NVARCHAR,   # JSONB alias NVARCHAR
}


def _is_internal_operator(operator: str) -> bool:
    return operator.startswith(("AnyOp", "AllOp")) or operator in {
        "InSubQuery",
        "NotInSubQuery",
    }


def determine_type(node):
    """Return the ColumnType for the expression rooted at *node*, or None when unknown."""
    from opteryx.types.logical_type import (
        BOOLEAN, INT64, FLOAT64, DATE, INTERVAL, VARCHAR, NVARCHAR,
        VARBINARY, VARIANT, NULL, TIMESTAMP, TIME, ARRAY, DECIMAL,
        ColumnType,
    )
    from opteryx.types.type_unification import _CANONICAL_BY_CATEGORY, compute_result_logical_type

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
        ) and node.centre.schema_column.category not in (OT.BOOLEAN, None):
            raise IncorrectTypeError(
                f"Expected a BOOLEAN value for {convert_camel_to_sql_case(node.value)}, but received {node.centre.schema_column.category}."
            )
        if node.value == "BitwiseNot":
            operand_type = node.centre.schema_column.category
            if operand_type not in (OT.INTEGER, None):
                raise IncorrectTypeError(
                    f"Expected an INTEGER value for bitwise NOT (~), but received {operand_type}."
                )
            return INT64
        return BOOLEAN
    if node.node_type == NodeType.NESTED:
        return determine_type(node.centre)
    if node.node_type == NodeType.WILDCARD:
        return None
    if node.node_type == NodeType.EXPRESSION_LIST:
        return node.parameters[-1].type  # ColumnType | None
    if node.node_type == NodeType.LITERAL:
        return node.type  # ColumnType

    if node.value in ("NotInSubQuery", "InSubQuery"):
        return BOOLEAN

    if node.schema_column:
        return node.schema_column.column_type  # ColumnType | None

    # Get left/right ColumnTypes
    left_ct = None
    right_ct = None
    if node.left is not None:
        if node.left.node_type == NodeType.LITERAL:
            left_ct = node.left.type
        elif node.left.schema_column:
            left_ct = node.left.schema_column.column_type

    if node.right is not None:
        if node.right.node_type == NodeType.LITERAL:
            right_ct = node.right.type
        elif node.right.schema_column:
            right_ct = node.right.schema_column.column_type

    # Extract LogicalCategory for operator map lookup
    left_lc = left_ct.category if left_ct is not None else None
    right_lc = right_ct.category if right_ct is not None else None

    operator = node.value
    if not is_known_operator(operator) and not _is_internal_operator(operator):
        raise UnsupportedSyntaxError(f"Unsupported operator \'{operator}\'.")

    if left_lc is None or left_lc == OT.NULL:
        return None
    if right_lc is None or right_lc == OT.NULL:
        return None

    # MapAccess special case: ARRAY<T>[index] → T
    if (
        operator == "MapAccess"
        and left_lc in (OT.ARRAY, OT.VECTOR)
        and right_lc == OT.INTEGER
    ):
        sc = node.left.schema_column
        if sc is not None and sc.column_type is not None and sc.column_type.element is not None:
            return sc.column_type.element  # already ColumnType
        if left_lc == OT.VECTOR:
            return FLOAT64
        return None

    # Dispatch on LogicalCategory via the operator map
    left_category = _SQL_TO_LC.get(left_lc, left_lc)
    right_category = _SQL_TO_LC.get(right_lc, right_lc)
    result = OPERATOR_MAP.get((left_category, right_category, operator))

    if result is None:
        from opteryx.expression import format_expression

        raise IncorrectTypeError(
            f"Unable to perform `{format_expression(node)}` because the values are not acceptable types for this operation. {left_lc} and {right_lc} were provided, you may need to cast one or both values to acceptable types."
        )

    result_lc = result.result_type  # LogicalCategory | None
    if result_lc is None:
        return None

    # Convert result LogicalCategory → ColumnType
    # For parameterized/width-dependent types, use compute_result_logical_type
    if result_lc == OT.DECIMAL and left_ct is not None and right_ct is not None:
        try:
            return compute_result_logical_type(left_ct, right_ct, operator, OT.DECIMAL)
        except Exception:
            return DECIMAL(38, 18)
    if result_lc == OT.INTEGER and left_ct is not None and right_ct is not None:
        try:
            return compute_result_logical_type(left_ct, right_ct, operator, OT.INTEGER)
        except Exception:
            return INT64
    if result_lc in (OT.FLOAT, OT.DOUBLE) and left_ct is not None and right_ct is not None:
        try:
            return compute_result_logical_type(left_ct, right_ct, operator, OT.FLOAT)
        except Exception:
            return FLOAT64
    if result_lc == OT.TIMESTAMP:
        if left_ct is not None and right_ct is not None:
            try:
                return compute_result_logical_type(left_ct, right_ct, operator, OT.TIMESTAMP)
            except Exception:
                pass
        return TIMESTAMP()

    # Non-parameterized: canonical instances
    canonical = _CANONICAL_BY_CATEGORY.get(result_lc)
    if canonical is not None:
        return canonical
    # Additional mappings
    if result_lc == OT.INTEGER:
        return INT64
    if result_lc in (OT.FLOAT, OT.DOUBLE):
        return FLOAT64
    if result_lc == OT.TIME:
        return TIME()
    return None
