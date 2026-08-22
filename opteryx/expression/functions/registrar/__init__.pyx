# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""
Registrar package initializer.

Holds shared registrar helper functions and the top-level collector
`get_builtin_functions()` that aggregates per-domain registrar modules.

Domain modules (text, arithmetic, utility, ...) are expected to provide
a getter function returning a list[FunctionDefinition], for example:

    def get_builtin_text_functions() -> list[FunctionDefinition]:
        ...

This module exposes helpers so domain modules can import them as:

    from opteryx.expression.functions.registrar import _make, _coalesce_return_type
"""

from typing import Any

from draken.draken_native import DrakenType
from opteryx.exceptions import IncompatibleTypesError
from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.logical_type import (
    BOOLEAN as _CT_BOOLEAN,
    INT64 as _CT_INT64,
    FLOAT64 as _CT_FLOAT64,
    VARCHAR as _CT_VARCHAR,
    NVARCHAR as _CT_NVARCHAR,
    VARBINARY as _CT_VARBINARY,
    DATE as _CT_DATE,
    INTERVAL as _CT_INTERVAL,
    VARIANT as _CT_VARIANT,
    IPV4 as _CT_IPV4,
    NULL as _CT_NULL,
    TIMESTAMP as _CT_TIMESTAMP,  # factory: _CT_TIMESTAMP() → ColumnType
    TIME as _CT_TIME,            # factory: _CT_TIME() → ColumnType
    ARRAY as _CT_ARRAY,          # factory: _CT_ARRAY(element) → ColumnType
    VECTOR as _CT_VECTOR,        # factory: _CT_VECTOR(dimensions) → ColumnType
)


# Kernel decorators for common iteration patterns
def _iterate_single_parameter(func):
    """Decorator for functions that iterate over a single array parameter."""

    def _inner(array):
        return list(map(func, array))

    return _inner


def _sort(func):
    """Decorator for sort/ordering functions that process arrays."""

    def _inner(array):
        return [func(item) for item in array]

    return _inner


def _iterate_double_parameter(func):
    """For functions called FUNCTION(field, literal)."""

    def _inner(array, literal):
        if isinstance(array, str):
            array = [array]
        return [func(item, literal[index]) for index, item in enumerate(array)]

    return _inner


def _iterate_double_parameter_swapped(func):
    """For functions called FUNCTION(literal, field) when planner supplies (literal_values, field_values)."""

    def _inner(array, literal):
        if isinstance(array, str):
            array = [array]
        return [func(literal[index], item) for index, item in enumerate(array)]

    return _inner


def _make(
    name: str,
    callable_or_ret: Any,
    maybe_ret: Any = None,
    params: tuple = (),
    *,
    aliases: tuple = (),
    category: str = "misc",
    volatility: Any = "immutable",
    deterministic: bool | None = None,
    lifecycle: LifecycleSpec | None = None,
    engine: Any = "draken",
    kernel_id: str = "default",
    id_suffix: str = "default",
    cost: float = 1.0,
    summary: str = "",
    documentation: str | None = None,
) -> FunctionDefinition:
    """
    Central helper to construct a FunctionDefinition with a single overload.

    Accepts either the (callable_ref, ret, params, ...) form or the
    shorthand (ret, ...) form used for zero-arg/constant definitions.

    Usage examples:
      _make("FOO", callable_ref, LogicalCategory.VARCHAR, (ParameterSpec(...),), cost=2.0)
      _make("NOW", LogicalCategory.TIMESTAMP, summary="Current timestamp.")
    """
    # Distinguish calling form: callable_or_ret is a callable -> full form,
    # otherwise it's the return-type shorthand.
    if callable(callable_or_ret):
        callable_ref = callable_or_ret
        ret = maybe_ret
        params = params or ()
    else:
        callable_ref = lambda *a: None
        ret = callable_or_ret
        params = () if maybe_ret is None else maybe_ret

    if deterministic is None:
        deterministic = volatility == "immutable"

    # Ensure `deterministic` is a plain bool before constructing FunctionDefinition.
    deterministic = bool(deterministic)
    assert isinstance(deterministic, bool)

    if lifecycle is None:
        lifecycle = LifecycleSpec(status="active")

    overload_id = f"{name}_{id_suffix}" if id_suffix else f"{name}_{kernel_id}"

    return FunctionDefinition(
        name=name,
        aliases=aliases,
        category=category,
        volatility=volatility,
        deterministic=deterministic,
        lifecycle=lifecycle,
        summary=summary or name,
        documentation=documentation or summary or name,
        overloads=(
            FunctionOverload(
                id=overload_id,
                parameters=params,
                return_spec=ReturnSpec(mode="fixed", fixed_type=ret),
                kernel=KernelSpec(
                    engine=engine,
                    id=kernel_id or "default",
                    callable_ref=callable_ref,
                    cost_us_per_million=cost,
                ),
            ),
        ),
    )


def _coalesce_return_type(arg_nodes, func_name="COALESCE"):
    """Return the first non-null compatible ColumnType across all args."""
    from opteryx.types import find_compatible_type
    from opteryx.types.type_unification import NOT_LITERAL, compute_selection_result_type

    branches = [
        (n, n.schema_column.column_type)
        for n in arg_nodes
        if getattr(n, "schema_column", None) is not None
        and n.schema_column.column_type is not None
        and n.schema_column.column_type.category not in (LogicalCategory.NULL, None)
    ]
    _check_blend_compatible(branches, func_name)
    cts = [ct for _, ct in branches]

    # COALESCE/IFNULL/IFNOTNULL/IIF hand back one argument VERBATIM — a
    # SELECTION, not a sum — so an all-DECIMAL/INTEGER mix uses the tighter
    # selection-sizing rule, exactly like CASE (binder.py's
    # _CASE_BLEND_FAMILIES resolution) does. find_compatible_type's Plus-style
    # DECIMAL promotion sizes `COALESCE(decimal_col, 0)` as if computing
    # decimal_col + 0, needlessly widening e.g. a DECIMAL(15,2) branch to
    # DECIMAL(22,2) — sometimes across the int64/int128 tier boundary — for no
    # representational reason. compute_selection_result_type returns None for
    # anything outside an all-DECIMAL/INTEGER mix (strings, temporals, a FLOAT
    # branch); find_compatible_type remains the fallback for those, unchanged.
    # node_type 42 == NodeType.LITERAL (matches _nc_describe_branch above —
    # the top-level NodeType import is circular from this package).
    result_ct = compute_selection_result_type(
        [(ct, n.value if n.node_type == 42 else NOT_LITERAL) for n, ct in branches]
    )
    if result_ct is None:
        result_ct = find_compatible_type(cts)
    return result_ct or _CT_NULL


def _iif_return_type(arg_nodes):
    """IIF(cond, when_true, when_false): common type of the two value branches,
    NULL-aware. The condition (arg 0) is excluded; a NULL branch defers to the
    other (so NULLIF's lowering IIF(a = b, NULL, a) resolves to a's type)."""
    return _coalesce_return_type(arg_nodes[1:], func_name="IIF")


# ---------------------------------------------------------------------------
# COALESCE/IFNULL/IFNOTNULL/IIF branch-type validation.
#
# The native kernel (draken/ops/kernels/function_null_conditional.cpp,
# nc_dispatch) only blends branches within one family — BOOLEAN, string
# (VARCHAR/NVARCHAR/VARBINARY), or a fixed-width numeric/temporal scalar —
# and fails loud with a bare "type <code> cannot be promoted with type <code>"
# message if a branch falls outside that family, or two fixed branches can't
# promote (e.g. a signed/unsigned int mix, or DATE vs INT64). That is a
# genuine runtime type mismatch, but by the time it reaches the kernel the
# offending SQL expression is gone — only DrakenType integers remain. This
# mirrors the same promotion rules here, against the same DrakenType.physical
# vocabulary (CLAUDE.md §14: one type object end to end), so an incompatible
# call is rejected at bind time, naming the actual branches.
# ---------------------------------------------------------------------------

_NC_STRING_TYPES = frozenset((DrakenType.VARCHAR, DrakenType.NVARCHAR, DrakenType.VARBINARY))
_NC_SIGNED_INT = frozenset((DrakenType.INT8, DrakenType.INT16, DrakenType.INT32, DrakenType.INT64))
_NC_FLOAT = frozenset((DrakenType.FLOAT32, DrakenType.FLOAT64))
_NC_UNSIGNED_WIDTH = {
    DrakenType.UINT8: 1,
    DrakenType.UINT16: 2,
    DrakenType.UINT32: 4,
    DrakenType.UINT64: 8,
}
_NC_UNSIGNED_INT = frozenset(_NC_UNSIGNED_WIDTH)
_NC_FIXED = _NC_SIGNED_INT | _NC_FLOAT | _NC_UNSIGNED_INT | frozenset((
    DrakenType.DATE32, DrakenType.TIME32, DrakenType.TIME64, DrakenType.TIMESTAMP64,
))

# DECIMAL/DECIMAL128 blend with each other and with signed INTEGER/FLOAT —
# mirrors _CASE_BLEND_FAMILIES' (INTEGER, FLOAT, DECIMAL) group in binder.py.
# This is a SEPARATE family from _NC_FIXED above, not a superset of it:
# unsigned ints and DATE/TIME stay out — DECIMAL x UNSIGNED has no promotion
# rule anywhere in the codebase (CASE included), and mixing it in here would
# invent one. The nc_dispatch kernel cannot promote DECIMAL itself (scale is
# out-of-band — a raw blend across differing scales is silently wrong), so
# unlike _NC_FIXED's nc_promote_fixed, this family does not compute an output
# type: find_compatible_type (FLOAT beats DECIMAL beats INTEGER,
# logical_type.py) resolves the actual result, and the binder CAST-aligns
# every branch (literal and column) to that exact ColumnType before the
# kernel ever runs — the kernel then requires an EXACT physical match.
_NC_DECIMAL_TYPES = frozenset((DrakenType.DECIMAL, DrakenType.DECIMAL128))
_NC_DECIMAL_FAMILY = _NC_DECIMAL_TYPES | _NC_SIGNED_INT | _NC_FLOAT


def _nc_promote_fixed(a, b):
    """Mirrors nc_promote_fixed; None is its DRAKEN_NULL "cannot promote" sentinel."""
    if a == b:
        return a
    if a in _NC_SIGNED_INT and b in _NC_SIGNED_INT:
        return DrakenType.INT64
    # Two unsigned widths widen to the WIDER of the two, never through INT64 —
    # INT64 cannot hold the top half of UINT64. Must stay identical to
    # nc_promote_fixed in function_null_conditional.cpp: this mirror exists only
    # so the rejection names the real SQL branches, so a rule that is stricter
    # here rejects something the kernel would have blended, and a rule that is
    # looser lets a plan through that dies in the kernel with only type codes.
    if a in _NC_UNSIGNED_INT and b in _NC_UNSIGNED_INT:
        return a if _NC_UNSIGNED_WIDTH[a] >= _NC_UNSIGNED_WIDTH[b] else b
    if (a in _NC_SIGNED_INT or a in _NC_FLOAT) and (b in _NC_SIGNED_INT or b in _NC_FLOAT):
        return DrakenType.FLOAT64
    # Signed/unsigned mixes stay unpromotable: no fixed-width type holds both
    # negatives and the top half of UINT64.
    return None


def _nc_describe_branch(node):
    if node.node_type == 42:  # NodeType.LITERAL
        return f"literal {node.value!r}"
    if node.node_type == 38:  # NodeType.IDENTIFIER
        name = node.query_column or node.source_column or node.value
        return f"column '{name}'"
    return "expression"


def _check_blend_compatible(branches, func_name):
    """branches: list[(node, ColumnType)], already filtered to non-NULL types.

    A SINGLE surviving branch is still checked. `branches` is what is left after
    the caller drops typed-NULL literals, so IIF(c, NULL, <array>) arrives here
    with one entry — and the family question ("can nc_dispatch blend this type at
    all?") is answered by that one branch alone, with no partner needed. Guarding
    this on len >= 2 let every unblendable family (ARRAY, INTERVAL, VARIANT,
    VECTOR_FP16) reach the kernel and die mid-execution with only a type
    code — the exact failure this mirror exists to prevent, reached by pairing the
    branch with NULL instead of with a second real branch.
    """
    if not branches:
        return
    node0, ct0 = branches[0]
    t0 = ct0.physical

    def _fail(node_i, ct_i):
        raise IncompatibleTypesError(
            message=(
                f"{func_name}: {_nc_describe_branch(node0)} is {ct0} but "
                f"{_nc_describe_branch(node_i)} is {ct_i} — {func_name} branches must "
                "share a compatible type (all BOOLEAN, all string, or a blendable "
                "numeric/temporal scalar mix). Use CAST to align the branches."
            )
        )

    if t0 == DrakenType.BOOL:
        for node_i, ct_i in branches[1:]:
            if ct_i.physical != DrakenType.BOOL:
                _fail(node_i, ct_i)
        return

    if t0 in _NC_STRING_TYPES:
        for node_i, ct_i in branches[1:]:
            if ct_i.physical not in _NC_STRING_TYPES:
                _fail(node_i, ct_i)
        return

    # DECIMAL/DECIMAL128/signed-INTEGER/FLOAT: checked as one family (matching
    # CASE's _CASE_BLEND_FAMILIES), not via _nc_promote_fixed's pairwise
    # widening — DECIMAL's scale is out-of-band, so there is no "promoted type"
    # this function can compute; find_compatible_type does that, and the
    # binder CAST-aligns every branch to its exact answer. This branch must be
    # checked BEFORE _NC_FIXED below: t0 in _NC_SIGNED_INT/_NC_FLOAT is also
    # true for a plain int/float blend with no DECIMAL anywhere, and for that
    # all-signed-or-float case membership here is equivalent to the pairwise
    # check below (nc_promote_fixed never refuses two signed/float members) —
    # so ordinary int/float COALESCE calls are unaffected.
    if t0 in _NC_DECIMAL_FAMILY:
        for node_i, ct_i in branches[1:]:
            if ct_i.physical not in _NC_DECIMAL_FAMILY:
                _fail(node_i, ct_i)
        return

    if t0 in _NC_FIXED:
        out = t0
        for node_i, ct_i in branches[1:]:
            ti = ct_i.physical
            if ti not in _NC_FIXED:
                _fail(node_i, ct_i)
            promoted = _nc_promote_fixed(out, ti)
            if promoted is None:
                _fail(node_i, ct_i)
            out = promoted
        return

    # t0 itself isn't in any blendable family (ARRAY, INTERVAL, VARIANT,
    # VECTOR_FP16) — the kernel rejects this regardless of the other
    # branches, so fail here without even looking at them.
    raise IncompatibleTypesError(
        message=(
            f"{func_name}: {_nc_describe_branch(node0)} is {ct0}, which {func_name} "
            "cannot blend — only BOOLEAN, string, and numeric/temporal scalar branches "
            "are supported."
        )
    )


# ---------------------------------------------------------------------------
# Consolidated leaf includes.
#
# The per-domain registrar files (aggregate / arithmetic / text / …) are
# textually included here so the package compiles to a single .so. Their
# getter functions land in this module's namespace, so get_builtin_functions
# below can call them directly without intermediate module objects.
# Order isn't significant for these — none reference each other.
# ---------------------------------------------------------------------------
include "aggregate.pyx"
include "arithmetic.pyx"
include "constant.pyx"
include "hash_encoding.pyx"
include "logical.pyx"
include "temporal.pyx"
include "temporal_extra.pyx"
include "text.pyx"
include "utility.pyx"
# `get_builtin_array_misc_functions` is defined directly in utility.pyx;
# the former array_misc.pyx forwarder was redundant and has been removed.


def get_builtin_functions() -> list[FunctionDefinition]:
    """Aggregate builtin function definitions across all registrar domains.

    Each domain's getter is defined directly in this module via the include
    statements above; no submodule round-trip is needed.
    """
    functions: list[FunctionDefinition] = []
    functions.extend(get_builtin_text_functions())
    functions.extend(get_builtin_text_extended_functions())
    functions.extend(get_builtin_arithmetic_functions())
    functions.extend(get_builtin_arithmetic_extended_functions())
    functions.extend(get_builtin_logical_functions())
    functions.extend(get_builtin_aggregate_functions())
    functions.extend(get_builtin_constant_functions())
    functions.extend(get_builtin_temporal_extra_functions())
    functions.extend(get_builtin_temporal_functions())
    functions.extend(get_builtin_utility_functions())
    functions.extend(get_builtin_hash_encoding_functions())
    functions.extend(get_builtin_array_misc_functions())
    return functions


# Re-export helpers for domain modules to use
__all__ = [
    "_coalesce_return_type",
    "_iterate_double_parameter",
    "_iterate_double_parameter_swapped",
    "_iterate_single_parameter",
    "_make",
    "_sort",
    "get_builtin_functions",
]
