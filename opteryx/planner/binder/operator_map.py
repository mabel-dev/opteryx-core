from typing import Callable, Dict, NamedTuple, Optional, Tuple

from opteryx.exceptions import IncorrectTypeError, UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression.operator_catalog import is_known_operator
from opteryx.types.logical_type import LogicalCategory as OT
from opteryx.types.logical_type import LogicalCategory as LC
from opteryx.types.logical_type import (
    ColumnType,
    BOOLEAN as _B, INT64 as _I, FLOAT64 as _F,
    VARCHAR as _S, NVARCHAR as _N, VARBINARY as _V,
    INTERVAL as _G, VARIANT as _A,
    TIMESTAMP, DECIMAL,
)
from opteryx.utils.sql import convert_camel_to_sql_case

# Phase 3: OPERATOR_MAP is now authored directly on LogicalCategory (LC) keys.
# Integer widths collapse to LC.INTEGER, floats to LC.FLOAT, JSONB/STRUCT to
# LC.NVARCHAR — the map carries 330 entries (vs the LogicalCategory-keyed 348 that had
# 18 duplicate-collapse entries). The derivation step (old _CATEGORY_OPERATOR_MAP)
# is deleted; this IS the map. The human-authorable source is now stable since
# logical categories don't change with Draken enum churn.
#
# result_type field: ColumnType instances (Phase 2 migration complete).
# DECIMAL(_D) and TIMESTAMP(_T) are placeholders; determine_type() may refine
# these via compute_result_logical_type when both operand types are known.


class OperatorMapType(NamedTuple):
    result_type: Optional[ColumnType]
    operation_function: Optional[Callable] = None
    cost_estimate: float = 100.0


OMT = OperatorMapType

# Parameterized placeholders for the map table; determine_type() refines as needed.
_D = DECIMAL(38, 18)
_T = TIMESTAMP()

# fmt: off
OPERATOR_MAP: Dict[Tuple[LC, LC, str], OperatorMapType] = {
    (LC.ARRAY       , LC.ARRAY       , 'ArrayContainsAll'        ): OMT(_B, None, 100.0),
    (LC.ARRAY       , LC.ARRAY       , 'AtArrow'                 ): OMT(_B, None, 100.0),
    (LC.ARRAY       , LC.INTEGER     , 'MapAccess'               ): OMT(None, None, 100.0),
    (LC.BOOLEAN     , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.BOOLEAN     , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.BOOLEAN     , LC.BOOLEAN     , 'And'                     ): OMT(_B, None, 100.0),
    (LC.BOOLEAN     , LC.BOOLEAN     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.BOOLEAN     , LC.BOOLEAN     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.BOOLEAN     , LC.BOOLEAN     , 'Or'                      ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.DATE        , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.DATE        , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.DATE        , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.DATE        , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.DATE        , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.DATE        , 'Minus'                   ): OMT(_G, None, 100.0),
    (LC.DATE        , LC.DATE        , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.INTERVAL    , 'Minus'                   ): OMT(_T, None, 100.0),
    (LC.DATE        , LC.INTERVAL    , 'Plus'                    ): OMT(_T, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'Minus'                   ): OMT(_G, None, 100.0),
    (LC.DATE        , LC.TIMESTAMP   , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Divide'                  ): OMT(_D, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Minus'                   ): OMT(_D, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Multiply'                ): OMT(_D, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.DECIMAL     , 'Plus'                    ): OMT(_D, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Divide'                  ): OMT(_F, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Minus'                   ): OMT(_F, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Multiply'                ): OMT(_F, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.FLOAT       , 'Plus'                    ): OMT(_F, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Divide'                  ): OMT(_D, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Minus'                   ): OMT(_D, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Multiply'                ): OMT(_D, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.DECIMAL     , LC.INTEGER     , 'Plus'                    ): OMT(_D, None, 100.0),
    (LC.FLOAT       , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Divide'                  ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Minus'                   ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Multiply'                ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.DECIMAL     , 'Plus'                    ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Divide'                  ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Minus'                   ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Multiply'                ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.FLOAT       , 'Plus'                    ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Divide'                  ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Minus'                   ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Modulo'                  ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Multiply'                ): OMT(_F, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.FLOAT       , LC.INTEGER     , 'Plus'                    ): OMT(_F, None, 100.0),
    (LC.INTEGER     , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Divide'                  ): OMT(_D, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Minus'                   ): OMT(_D, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Multiply'                ): OMT(_D, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.DECIMAL     , 'Plus'                    ): OMT(_D, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Divide'                  ): OMT(_F, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Minus'                   ): OMT(_F, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Multiply'                ): OMT(_F, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.FLOAT       , 'Plus'                    ): OMT(_F, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'BitwiseAnd'              ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'BitwiseOr'               ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'BitwiseXor'              ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Divide'                  ): OMT(_F, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Minus'                   ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Modulo'                  ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Multiply'                ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'MyIntegerDivide'         ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'Plus'                    ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'ShiftLeft'               ): OMT(_I, None, 100.0),
    (LC.INTEGER     , LC.INTEGER     , 'ShiftRight'              ): OMT(_I, None, 100.0),
    (LC.INTERVAL    , LC.DATE        , 'Minus'                   ): OMT(_T, None, 100.0),
    (LC.INTERVAL    , LC.DATE        , 'Plus'                    ): OMT(_T, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Minus'                   ): OMT(_G, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.INTERVAL    , LC.INTERVAL    , 'Plus'                    ): OMT(_G, None, 100.0),
    (LC.INTERVAL    , LC.TIMESTAMP   , 'Minus'                   ): OMT(_T, None, 100.0),
    (LC.INTERVAL    , LC.TIMESTAMP   , 'Plus'                    ): OMT(_T, None, 100.0),
    (LC.NVARCHAR    , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.INTEGER     , 'MapAccess'               ): OMT(_N, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'BitwiseOr'               ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.NVARCHAR    , 'StringConcat'            ): OMT(_N, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'BitwiseOr'               ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARBINARY   , 'StringConcat'            ): OMT(_V, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'BitwiseOr'               ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.NVARCHAR    , LC.VARCHAR     , 'StringConcat'            ): OMT(_N, None, 100.0),
    (LC.TIMESTAMP   , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'Minus'                   ): OMT(_G, None, 100.0),
    (LC.TIMESTAMP   , LC.DATE        , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.INTERVAL    , 'Minus'                   ): OMT(_T, None, 100.0),
    (LC.TIMESTAMP   , LC.INTERVAL    , 'Plus'                    ): OMT(_T, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'Minus'                   ): OMT(_G, None, 100.0),
    (LC.TIMESTAMP   , LC.TIMESTAMP   , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.INTEGER     , 'MapAccess'               ): OMT(_V, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'BitwiseOr'               ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.NVARCHAR    , 'StringConcat'            ): OMT(_V, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARBINARY   , 'StringConcat'            ): OMT(_V, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'BitwiseOr'               ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.VARBINARY   , LC.VARCHAR     , 'StringConcat'            ): OMT(_V, None, 100.0),
    (LC.VARCHAR     , LC.ARRAY       , 'InList'                  ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.ARRAY       , 'NotInList'               ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.INTEGER     , 'MapAccess'               ): OMT(_S, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'BitwiseOr'               ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.NVARCHAR    , 'StringConcat'            ): OMT(_N, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'BitwiseOr'               ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARBINARY   , 'StringConcat'            ): OMT(_V, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'BitwiseOr'               ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Eq'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Gt'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'GtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'ILike'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Like'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'Lt'                      ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'LtEq'                    ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'NotEq'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'NotILike'                ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'NotLike'                 ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'NotRLike'                ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'RLike'                   ): OMT(_B, None, 100.0),
    (LC.VARCHAR     , LC.VARCHAR     , 'StringConcat'            ): OMT(_S, None, 100.0),
    (LC.VARIANT     , LC.INTEGER     , 'MapAccess'               ): OMT(_A, None, 100.0),
    (LC.VARIANT     , LC.NVARCHAR    , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.VARIANT     , LC.NVARCHAR    , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.VARIANT     , LC.NVARCHAR    , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.VARIANT     , LC.VARCHAR     , 'Arrow'                   ): OMT(_A, None, 100.0),
    (LC.VARIANT     , LC.VARCHAR     , 'AtQuestion'              ): OMT(_B, None, 100.0),
    (LC.VARIANT     , LC.VARCHAR     , 'LongArrow'               ): OMT(_N, None, 100.0),
    (LC.VECTOR      , LC.INTEGER     , 'MapAccess'               ): OMT(_F, None, 100.0),
}
# fmt: on

for _, _, _operator_name in OPERATOR_MAP:
    if not is_known_operator(_operator_name):
        raise UnsupportedSyntaxError(f"Operator map contains unknown operator \'{_operator_name}\'.")


# Static LogicalCategory → LogicalCategory projection for operator-map dispatch.
# Alias members (DOUBLE, BLOB, STRUCT, JSONB) removed in Phase 4 — the enum
# no longer carries them, so no mapping entry is needed.
_SQL_TO_LC: Dict[OT, LC] = {
    OT.BOOLEAN: LC.BOOLEAN,
    OT.INTEGER: LC.INTEGER,
    OT.FLOAT: LC.FLOAT,
    OT.DECIMAL: LC.DECIMAL,
    OT.VARCHAR: LC.VARCHAR,
    OT.NVARCHAR: LC.NVARCHAR,
    OT.VARBINARY: LC.VARBINARY,
    OT.DATE: LC.DATE,
    OT.TIME: LC.TIME,
    OT.TIMESTAMP: LC.TIMESTAMP,
    OT.INTERVAL: LC.INTERVAL,
    OT.ARRAY: LC.ARRAY,
    OT.VECTOR: LC.VECTOR,
    OT.VARIANT: LC.VARIANT,
    OT.NULL: LC.NULL,
}


def _is_internal_operator(operator: str) -> bool:
    return operator.startswith(("AnyOp", "AllOp")) or operator in {
        "InSubQuery",
        "NotInSubQuery",
    }


def determine_type(node):
    """Return the ColumnType for the expression rooted at *node*, or None when unknown."""
    from opteryx.types.logical_type import BOOLEAN, INT64, FLOAT64
    from opteryx.types.type_unification import compute_result_logical_type

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

    result_ct = result.result_type  # ColumnType | None
    if result_ct is None:
        return None

    result_cat = result_ct.category

    # For parameterized types, compute_result_logical_type may refine the placeholder.
    if result_cat == OT.DECIMAL and left_ct is not None and right_ct is not None:
        try:
            return compute_result_logical_type(left_ct, right_ct, operator, OT.DECIMAL)
        except Exception:
            return result_ct  # placeholder DECIMAL(38, 18)
    if result_cat == OT.INTEGER and left_ct is not None and right_ct is not None:
        try:
            return compute_result_logical_type(left_ct, right_ct, operator, OT.INTEGER)
        except Exception:
            return result_ct  # placeholder INT64
    if result_cat == OT.FLOAT and left_ct is not None and right_ct is not None:
        try:
            return compute_result_logical_type(left_ct, right_ct, operator, OT.FLOAT)
        except Exception:
            return result_ct  # placeholder FLOAT64
    if result_cat == OT.TIMESTAMP:
        if left_ct is not None and right_ct is not None:
            try:
                return compute_result_logical_type(left_ct, right_ct, operator, OT.TIMESTAMP)
            except Exception:
                pass
        return result_ct  # placeholder TIMESTAMP()

    return result_ct  # non-parameterized: return the ColumnType singleton directly
