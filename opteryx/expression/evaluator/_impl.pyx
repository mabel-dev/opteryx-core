# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True
# cython: infer_types=True
# cython: cdivision=True

"""Expression evaluation engine — consolidated implementation module.

Compiled into opteryx.expression.evaluator._impl.so. The leaf .pyx files
(type_coercion / function_execution / temporal_ops /
string_ops / json_ops / case_eval / arithmetic / comparisons / evaluation)
are textually included below so the whole evaluator compiles to a single .so.

The sibling __init__.py imports this module and re-exports its public API.
(Phase 8c: tree-walker evaluate_case deleted; only compiled path remains.)

Layout note: this file used to be `__init__.pyx` and built straight to
`__init__.cpython-XXX.so`, but Cython 3.x emits an internal
`PyImport_ImportModule("<pkg>.__init__")` call whenever a module uses typed
memoryviews, and Python can't resolve that synthetic name. Splitting into
_impl.so + __init__.py avoids the issue.
"""

# ---------------------------------------------------------------------------
# Operator codes — integer identifiers for the operator strings the planner
# stamps onto Nodes. Placed before the leaf includes so the DEFs are in
# scope for every included file. Dispatchers translate `node.value` to a
# code once at entry and then branch on the int (Cython folds chains of
# `if code == OP_X` into a C switch via optimize.use_switch).
#
# 0 is reserved for "unknown" so a forgotten dispatcher branch fails loud
# rather than silently picking the OP_EQ path.
# ---------------------------------------------------------------------------
DEF OP_UNKNOWN          = 0
DEF OP_EQ               = 1
DEF OP_NOT_EQ           = 2
DEF OP_LT               = 3
DEF OP_GT               = 4
DEF OP_LT_EQ            = 5
DEF OP_GT_EQ            = 6
DEF OP_IN_LIST          = 7
DEF OP_NOT_IN_LIST      = 8
DEF OP_LIKE             = 9
DEF OP_NOT_LIKE         = 10
DEF OP_ILIKE            = 11
DEF OP_NOT_ILIKE        = 12
DEF OP_RLIKE            = 13
DEF OP_NOT_RLIKE        = 14
DEF OP_IN_STR           = 15
DEF OP_NOT_IN_STR       = 16
DEF OP_I_IN_STR         = 17
DEF OP_NOT_I_IN_STR     = 18
DEF OP_ANYOP_EQ         = 19
DEF OP_ANYOP_NOT_EQ     = 20
DEF OP_ANYOP_GT         = 21
DEF OP_ANYOP_LT         = 22
DEF OP_ANYOP_GT_EQ      = 23
DEF OP_ANYOP_LT_EQ      = 24
DEF OP_ALLOP_EQ         = 25
DEF OP_ALLOP_NOT_EQ     = 26
DEF OP_AT_ARROW         = 27
DEF OP_ARRAY_CONTAINS_ALL = 28
DEF OP_AT_QUESTION      = 29
DEF OP_ANYOP_LIKE       = 30
DEF OP_ANYOP_NOT_LIKE   = 31
DEF OP_ANYOP_ILIKE      = 32
DEF OP_ANYOP_NOT_ILIKE  = 33
DEF OP_IP_CONTAINED_BY  = 34
DEF OP_IP_CONTAINS      = 35
DEF OP_ALLOP_LIKE       = 36
DEF OP_ALLOP_NOT_LIKE   = 37
DEF OP_ALLOP_ILIKE      = 38
DEF OP_ALLOP_NOT_ILIKE  = 39

# Python-side mirror so dispatchers can resolve a string op once. Must stay
# in sync with the DEFs above; if they ever diverge the verification check
# below will catch it.
_OP_CODE = {
    "Eq": 1, "NotEq": 2, "Lt": 3, "Gt": 4, "LtEq": 5, "GtEq": 6,
    "InList": 7, "NotInList": 8,
    "Like": 9, "NotLike": 10, "ILike": 11, "NotILike": 12,
    "RLike": 13, "NotRLike": 14,
    "InStr": 15, "NotInStr": 16, "IInStr": 17, "NotIInStr": 18,
    "AnyOpEq": 19, "AnyOpNotEq": 20, "AnyOpGt": 21, "AnyOpLt": 22,
    "AnyOpGtEq": 23, "AnyOpLtEq": 24,
    "AllOpEq": 25, "AllOpNotEq": 26,
    "AtArrow": 27, "ArrayContainsAll": 28, "AtQuestion": 29,
    "AnyOpLike": 30, "AnyOpNotLike": 31, "AnyOpILike": 32, "AnyOpNotILike": 33,
    # Quantified-LIKE ALL forms. AnyOpNotLike/AnyOpNotILike above are no longer
    # reachable from SQL (the planner rejects `NOT LIKE ANY`); their codes are
    # kept rather than reused so a stale compiled artefact cannot silently take
    # on a different operator's meaning.
    "AllOpLike": 36, "AllOpNotLike": 37, "AllOpILike": 38, "AllOpNotILike": 39,
    "IPContainedBy": 34, "IPContains": 35,
}


# ---------------------------------------------------------------------------
# Translate our OP_* codes to Draken's internal compare op codes.
#
# Draken's _<type>_compare.hpp dispatchers use:
#     0=Eq, 1=Ne, 2=Gt, 3=Ge, 4=Lt, 5=Le
#
# Our OP_* numbering differs (Eq=1, Ne=2, Lt=3, Gt=4, LtEq=5, GtEq=6) for
# historical reasons (OP_UNKNOWN=0 sentinel). Rather than renumber every
# call site, translate at the call boundary into a small array indexed by
# our op_code. Negative entries flag "no Draken equivalent" so the caller
# can fall back. The body of the array is set once at module load.
# ---------------------------------------------------------------------------
cdef int _DRAKEN_CMP_OP[40]
_DRAKEN_CMP_OP[0]  = -1  # OP_UNKNOWN
_DRAKEN_CMP_OP[1]  =  0  # OP_EQ        → Draken Eq
_DRAKEN_CMP_OP[2]  =  1  # OP_NOT_EQ    → Draken Ne
_DRAKEN_CMP_OP[3]  =  4  # OP_LT        → Draken Lt
_DRAKEN_CMP_OP[4]  =  2  # OP_GT        → Draken Gt
_DRAKEN_CMP_OP[5]  =  5  # OP_LT_EQ     → Draken Le
_DRAKEN_CMP_OP[6]  =  3  # OP_GT_EQ     → Draken Ge
_DRAKEN_CMP_OP[7]  = -1  # OP_IN_LIST       — own kernel
_DRAKEN_CMP_OP[8]  = -1  # OP_NOT_IN_LIST   — own kernel
_DRAKEN_CMP_OP[9]  = -1  # OP_LIKE          — own kernel
_DRAKEN_CMP_OP[10] = -1  # OP_NOT_LIKE      — own kernel
_DRAKEN_CMP_OP[11] = -1  # OP_ILIKE         — own kernel
_DRAKEN_CMP_OP[12] = -1  # OP_NOT_ILIKE     — own kernel
_DRAKEN_CMP_OP[13] = -1  # OP_RLIKE         — own kernel
_DRAKEN_CMP_OP[14] = -1  # OP_NOT_RLIKE     — own kernel
_DRAKEN_CMP_OP[15] = -1  # OP_IN_STR        — own kernel
_DRAKEN_CMP_OP[16] = -1  # OP_NOT_IN_STR    — own kernel
_DRAKEN_CMP_OP[17] = -1  # OP_I_IN_STR      — own kernel
_DRAKEN_CMP_OP[18] = -1  # OP_NOT_I_IN_STR  — own kernel
_DRAKEN_CMP_OP[19] = -1  # OP_ANYOP_EQ      — own kernel
_DRAKEN_CMP_OP[20] = -1  # OP_ANYOP_NOT_EQ  — own kernel
_DRAKEN_CMP_OP[21] = -1  # OP_ANYOP_GT      — own kernel
_DRAKEN_CMP_OP[22] = -1  # OP_ANYOP_LT      — own kernel
_DRAKEN_CMP_OP[23] = -1  # OP_ANYOP_GT_EQ   — own kernel
_DRAKEN_CMP_OP[24] = -1  # OP_ANYOP_LT_EQ   — own kernel
_DRAKEN_CMP_OP[25] = -1  # OP_ALLOP_EQ      — own kernel
_DRAKEN_CMP_OP[26] = -1  # OP_ALLOP_NOT_EQ  — own kernel
_DRAKEN_CMP_OP[27] = -1  # OP_AT_ARROW      — own kernel
_DRAKEN_CMP_OP[28] = -1  # OP_ARRAY_CONTAINS_ALL — own kernel
_DRAKEN_CMP_OP[29] = -1  # OP_AT_QUESTION   — own kernel
_DRAKEN_CMP_OP[30] = -1  # OP_ANYOP_LIKE    — own kernel
_DRAKEN_CMP_OP[31] = -1  # OP_ANYOP_NOT_LIKE — own kernel
_DRAKEN_CMP_OP[32] = -1  # OP_ANYOP_ILIKE   — own kernel
_DRAKEN_CMP_OP[33] = -1  # OP_ANYOP_NOT_ILIKE — own kernel
_DRAKEN_CMP_OP[34] = -1  # OP_IP_CONTAINED_BY — own kernel
_DRAKEN_CMP_OP[35] = -1  # OP_IP_CONTAINS     — own kernel
_DRAKEN_CMP_OP[36] = -1  # OP_ALLOP_LIKE      — own kernel (draken_like_any)
_DRAKEN_CMP_OP[37] = -1  # OP_ALLOP_NOT_LIKE  — own kernel (draken_like_any)
_DRAKEN_CMP_OP[38] = -1  # OP_ALLOP_ILIKE     — own kernel (draken_like_any)
_DRAKEN_CMP_OP[39] = -1  # OP_ALLOP_NOT_ILIKE — own kernel (draken_like_any)

# Same table but with directional ops flipped — used when we dispatch the
# compare on the right-hand operand (e.g. Float64 < Int64 → Int64 > Float64).
# Eq/Ne are symmetric and unchanged.
cdef int _DRAKEN_CMP_OP_FLIPPED[40]
_DRAKEN_CMP_OP_FLIPPED[0]  = -1
_DRAKEN_CMP_OP_FLIPPED[1]  =  0   # Eq    (symmetric)
_DRAKEN_CMP_OP_FLIPPED[2]  =  1   # Ne    (symmetric)
_DRAKEN_CMP_OP_FLIPPED[3]  =  2   # OP_LT       → Draken Gt
_DRAKEN_CMP_OP_FLIPPED[4]  =  4   # OP_GT       → Draken Lt
_DRAKEN_CMP_OP_FLIPPED[5]  =  3   # OP_LT_EQ    → Draken Ge
_DRAKEN_CMP_OP_FLIPPED[6]  =  5   # OP_GT_EQ    → Draken Le
_DRAKEN_CMP_OP_FLIPPED[7]  = -1
_DRAKEN_CMP_OP_FLIPPED[8]  = -1
_DRAKEN_CMP_OP_FLIPPED[9]  = -1
_DRAKEN_CMP_OP_FLIPPED[10] = -1
_DRAKEN_CMP_OP_FLIPPED[11] = -1
_DRAKEN_CMP_OP_FLIPPED[12] = -1
_DRAKEN_CMP_OP_FLIPPED[13] = -1
_DRAKEN_CMP_OP_FLIPPED[14] = -1
_DRAKEN_CMP_OP_FLIPPED[15] = -1
_DRAKEN_CMP_OP_FLIPPED[16] = -1
_DRAKEN_CMP_OP_FLIPPED[17] = -1
_DRAKEN_CMP_OP_FLIPPED[18] = -1
_DRAKEN_CMP_OP_FLIPPED[19] = -1
_DRAKEN_CMP_OP_FLIPPED[20] = -1
_DRAKEN_CMP_OP_FLIPPED[21] = -1
_DRAKEN_CMP_OP_FLIPPED[22] = -1
_DRAKEN_CMP_OP_FLIPPED[23] = -1
_DRAKEN_CMP_OP_FLIPPED[24] = -1
_DRAKEN_CMP_OP_FLIPPED[25] = -1
_DRAKEN_CMP_OP_FLIPPED[26] = -1
_DRAKEN_CMP_OP_FLIPPED[27] = -1
_DRAKEN_CMP_OP_FLIPPED[28] = -1
_DRAKEN_CMP_OP_FLIPPED[29] = -1
_DRAKEN_CMP_OP_FLIPPED[30] = -1
_DRAKEN_CMP_OP_FLIPPED[31] = -1
_DRAKEN_CMP_OP_FLIPPED[32] = -1
_DRAKEN_CMP_OP_FLIPPED[33] = -1
_DRAKEN_CMP_OP_FLIPPED[34] = -1
_DRAKEN_CMP_OP_FLIPPED[35] = -1
_DRAKEN_CMP_OP_FLIPPED[36] = -1
_DRAKEN_CMP_OP_FLIPPED[37] = -1
_DRAKEN_CMP_OP_FLIPPED[38] = -1
_DRAKEN_CMP_OP_FLIPPED[39] = -1


# Include order: leaves with no intra-package deps first, then leaves that
# depend on earlier names. Within each tier the order doesn't matter.
# Phase 6: arithmetic_dispatch.pyx deleted — dispatch now at bind time via resolve_binary_op.
include "type_coercion.pyx"
include "temporal_ops.pyx"
include "string_ops.pyx"
include "json_ops.pyx"
include "case_eval.pyx"
include "arithmetic.pyx"
include "comparisons.pyx"
include "evaluation.pyx"


def _verify_node_type_constants():
    """Fail-fast: the compile-time DEF constants in evaluation must mirror the
    runtime NodeType enum. If this assertion fires, update the DEFs in
    evaluator/evaluation.pyx and rebuild.
    """
    from opteryx.expression import NodeType

    expected = {
        "UNKNOWN": 0,
        "AND": 17, "OR": 18, "XOR": 19, "NOT": 20, "DNF": 21, "CNF": 22,
        "CASE": 32, "WILDCARD": 33, "COMPARISON_OPERATOR": 34,
        "BINARY_OPERATOR": 35, "UNARY_OPERATOR": 36, "FUNCTION": 37,
        "IDENTIFIER": 38, "SUBQUERY": 39, "NESTED": 40, "AGGREGATOR": 41,
        "LITERAL": 42, "EXPRESSION_LIST": 43, "EVALUATED": 44, "CAST": 45,
        "EXTRACTION_OPERATOR": 46, "BETWEEN": 47,
    }
    for name, value in expected.items():
        actual = int(getattr(NodeType, name))
        if actual != value:
            raise AssertionError(
                f"NodeType.{name} = {actual}, but evaluation.pyx DEF expects {value}. "
                f"Update the DEF constants at the top of "
                f"opteryx/expression/evaluator/evaluation.pyx and rebuild."
            )


# Submodule aliases (legacy `from evaluator.LEAF import name`) and __all__
# are set up in the sibling __init__.py — that file is the package marker
# and runs with `__name__ == "opteryx.expression.evaluator"`. Doing it here
# would register aliases under "opteryx.expression.evaluator._impl.LEAF",
# which is the wrong location.
