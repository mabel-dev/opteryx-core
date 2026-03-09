"""Native function registrar.

Registers Opteryx's builtin scalar functions into the FunctionCatalog.
This module is responsible for wiring together FunctionDefinition metadata
with the actual kernel callables that live in implementations/.

The implementations themselves are in:
    opteryx/expression/functions/implementations/
        arithmetic.py, text.py, temporal.py, logical.py,
        hash_encoding.py, utility.py
"""

from orso.types import OrsoTypes

from opteryx.expression.functions import FunctionDefinition
from opteryx.expression.functions import FunctionOverload
from opteryx.expression.functions import KernelSpec
from opteryx.expression.functions import LifecycleSpec
from opteryx.expression.functions import ParameterSpec
from opteryx.expression.functions import ReturnSpec


def _builtin_text_functions() -> list[FunctionDefinition]:
    """Text/string manipulation functions."""
    # Import existing implementations
    from opteryx.expression.functions.implementations import text as string_functions
    from opteryx.expression.functions.implementations.text import to_lower
    from opteryx.expression.functions.implementations.text import to_upper
    from opteryx.expression.functions.implementations.text import vector_lengther

    return [
        FunctionDefinition(
            name="UPPER",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Convert string to uppercase.",
            documentation="Returns input string with all characters in uppercase.",
            overloads=(
                FunctionOverload(
                    id="UPPER_1",
                    parameters=(ParameterSpec(name="str", type_family="string"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.VARCHAR),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=to_upper,
                        cost_us_per_million=5.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="LOWER",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Convert string to lowercase.",
            documentation="Returns input string with all characters in lowercase.",
            overloads=(
                FunctionOverload(
                    id="LOWER_1",
                    parameters=(ParameterSpec(name="str", type_family="string"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.VARCHAR),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=to_lower,
                        cost_us_per_million=5.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="LENGTH",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return length of string.",
            documentation="Returns the number of characters in the input string.",
            overloads=(
                FunctionOverload(
                    id="LENGTH_1",
                    parameters=(ParameterSpec(name="str", type_family="string"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.INTEGER),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=vector_lengther,
                        cost_us_per_million=3.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="CONCAT",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Concatenate strings.",
            documentation="Returns concatenation of all input strings.",
            overloads=(
                FunctionOverload(
                    id="CONCAT_variadic",
                    parameters=(
                        ParameterSpec(name="str1", type_family="string"),
                        ParameterSpec(
                            name="more", type_family="string", variadic=True, optional=True
                        ),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.VARCHAR),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=string_functions.concat,
                        null_policy="passthrough",
                        cost_us_per_million=8.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="SUBSTRING",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Extract substring.",
            documentation="Returns substring starting at position with optional length.",
            overloads=(
                FunctionOverload(
                    id="SUBSTRING_1",
                    parameters=(
                        ParameterSpec(name="str", type_family="string"),
                        ParameterSpec(name="start", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.VARCHAR),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=string_functions.substring,
                        null_policy="passthrough",
                        cost_us_per_million=6.0,
                    ),
                ),
            ),
        ),
    ]


def _builtin_arithmetic_functions() -> list[FunctionDefinition]:
    """Arithmetic and numeric functions."""
    from pyarrow import compute

    from opteryx.expression.functions.implementations import arithmetic as number_functions

    return [
        FunctionDefinition(
            name="ROUND",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Round to nearest integer.",
            documentation="Rounds input number to nearest integer or specified decimal places.",
            overloads=(
                FunctionOverload(
                    id="ROUND_1",
                    parameters=(ParameterSpec(name="num", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=number_functions.round,
                        cost_us_per_million=2.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="ABS",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Absolute value.",
            documentation="Returns absolute value of input number.",
            overloads=(
                FunctionOverload(
                    id="ABS_1",
                    parameters=(ParameterSpec(name="num", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=compute.abs,
                        cost_us_per_million=1.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="CEIL",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Round up to nearest integer.",
            documentation="Returns smallest integer greater than or equal to input.",
            overloads=(
                FunctionOverload(
                    id="CEIL_1",
                    parameters=(ParameterSpec(name="num", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=number_functions.ceiling,
                        cost_us_per_million=2.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="FLOOR",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Round down to nearest integer.",
            documentation="Returns largest integer less than or equal to input.",
            overloads=(
                FunctionOverload(
                    id="FLOOR_1",
                    parameters=(ParameterSpec(name="num", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=number_functions.floor,
                        cost_us_per_million=2.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="SQRT",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Square root.",
            documentation="Returns square root of input number.",
            overloads=(
                FunctionOverload(
                    id="SQRT_1",
                    parameters=(ParameterSpec(name="num", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=compute.sqrt,
                        cost_us_per_million=3.0,
                    ),
                ),
            ),
        ),
    ]


def _builtin_type_conversion_functions() -> list[FunctionDefinition]:
    """Type casting functions (placeholder - REMOVED from public API).

    NOTE: Type conversions are handled via CAST(x AS type) and x::type syntax,
    which are processed as specialized unary-like operations in the planner/binder.

    The cast() builder in logical_planner_builders.py converts CAST nodes to
    function calls internally during transition, but these should NOT be exposed
    as public functions that users can call directly.

    Future work (Phase 2+):
    - Separate Cast nodes from FUNCTION nodes (create NodeType.CAST)
    - Implement Cast as unary-operator-like construct for performance
    - Remove function-call overhead from type conversions
    - This requires updates to: planner, binder, optimizer, evaluator
    """
    # Return empty list - type conversions handled separately
    return []


def _coalesce_return_type(arg_nodes) -> OrsoTypes:
    """Return the first non-null compatible type across all args."""
    from orso.types import find_compatible_type

    types = [
        n.schema_column.type
        for n in arg_nodes
        if getattr(n, "schema_column", None) is not None
        and n.schema_column.type not in (OrsoTypes.NULL, 0, OrsoTypes._MISSING_TYPE)
    ]
    return find_compatible_type(types) or OrsoTypes.NULL


def _case_return_type(arg_nodes) -> OrsoTypes:
    """Return the type of the first non-null THEN/ELSE branch.

    CASE node structure: parameters[0] is the scrutinee, parameters[1] is a
    node whose .parameters list holds the WHEN/THEN/ELSE expressions.
    """
    branches = getattr(arg_nodes[1], "parameters", []) if len(arg_nodes) > 1 else []
    for param in branches:
        sc = getattr(param, "schema_column", None)
        if sc is not None and sc.type not in (OrsoTypes.NULL, 0, OrsoTypes._MISSING_TYPE):
            return sc.type
    return OrsoTypes.NULL


def _builtin_logical_functions() -> list[FunctionDefinition]:
    """Logical and control flow functions."""
    import numpy

    from opteryx.expression.functions.implementations.logical import (
        array_contains as _lf_array_contains,
    )
    from opteryx.expression.functions.implementations.logical import if_null as _lf_if_null
    from opteryx.expression.functions.implementations.logical import null_if as _lf_null_if
    from opteryx.expression.functions.implementations.utility import (
        cosine_similarity as _lf_cosine_similarity,
    )
    from opteryx.expression.functions.implementations.utility import humanize as _lf_humanize
    from opteryx.expression.functions.implementations.utility import (
        jsonb_object_keys as _lf_jsonb_object_keys,
    )
    from opteryx.functions import _coalesce
    from opteryx.functions import select_values

    class other_functions:
        array_contains = staticmethod(_lf_array_contains)
        if_null = staticmethod(_lf_if_null)
        null_if = staticmethod(_lf_null_if)
        cosine_similarity = staticmethod(_lf_cosine_similarity)
        humanize = staticmethod(_lf_humanize)
        jsonb_object_keys = staticmethod(_lf_jsonb_object_keys)

    _coalesce_kernel = _coalesce
    _iif_kernel = numpy.where
    _case_kernel = select_values

    _variadic_any = (
        ParameterSpec(name="arg0", type_family="any"),
        ParameterSpec(name="args", type_family="any", variadic=True, optional=True),
    )

    return [
        FunctionDefinition(
            name="COALESCE",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return first non-null argument.",
            documentation="Returns the first non-null value from the list of arguments.",
            overloads=(
                FunctionOverload(
                    id="COALESCE_variadic",
                    parameters=_variadic_any,
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=_coalesce_kernel,
                        null_policy="passthrough",
                        cost_us_per_million=5.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IFNULL",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return value if not null, else default.",
            documentation="Returns first argument if not null, otherwise returns second argument.",
            overloads=(
                FunctionOverload(
                    id="IFNULL_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="default", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=other_functions.if_null,
                        null_policy="passthrough",
                        cost_us_per_million=4.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IFNOTNULL",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return second argument if first is not null.",
            documentation="Returns second argument if first argument is not null, otherwise null.",
            overloads=(
                FunctionOverload(
                    id="IFNOTNULL_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="result", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=other_functions.if_null,  # same kernel, different semantics handled by evaluator
                        null_policy="passthrough",
                        cost_us_per_million=4.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="NULLIF",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return null if equal, else first value.",
            documentation="Returns null if arguments are equal, otherwise returns first argument.",
            overloads=(
                FunctionOverload(
                    id="NULLIF_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="compare", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=other_functions.null_if,
                        cost_us_per_million=3.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IIF",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Inline if: return second or third arg based on condition.",
            documentation="Returns second argument if condition is true, otherwise third argument.",
            overloads=(
                FunctionOverload(
                    id="IIF_1",
                    parameters=(
                        ParameterSpec(name="condition", type_family="boolean"),
                        ParameterSpec(name="true_value", type_family="any"),
                        ParameterSpec(name="false_value", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=1),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=_iif_kernel,
                        null_policy="passthrough",
                        cost_us_per_million=2.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="PASSTHRU",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return input unchanged.",
            documentation="Returns the input value unchanged. Used for testing and compatibility.",
            overloads=(
                FunctionOverload(
                    id="PASSTHRU_1",
                    parameters=(ParameterSpec(name="value", type_family="any"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=lambda x: x,
                        cost_us_per_million=0.1,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="CASE",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Conditional value selection.",
            documentation="Returns a value based on conditional expressions.",
            overloads=(
                FunctionOverload(
                    id="CASE_variadic",
                    parameters=_variadic_any,
                    return_spec=ReturnSpec(mode="resolver", resolver=_case_return_type),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=_case_kernel,
                        null_policy="passthrough",
                        cost_us_per_million=3.0,
                    ),
                ),
            ),
        ),
    ]


def _builtin_aggregate_functions() -> list[FunctionDefinition]:
    # Aggregates are dispatched by draken via AGGREGATORS in aggregate_node.py,
    # not by the function catalog. Returning an empty list keeps callers intact
    # while ensuring aggregates are NOT visible to is_function() checks.
    return []


def _builtin_constant_functions() -> list[FunctionDefinition]:
    """Zero-parameter plan-time constants folded to literals by the binder.

    These functions carry no runtime kernel — the binder rewrites them to
    LITERAL nodes via fixed_value_function() before the expression evaluator
    sees them.  They must be present in the catalog so that is_function()
    recognises them as valid function names during AST construction.
    """
    _noop = lambda: None  # noqa: E731 — never called at runtime

    def _make(name, return_type, aliases=(), summary=""):
        return FunctionDefinition(
            name=name,
            aliases=aliases,
            category="constant",
            volatility="stable",
            deterministic=False,
            lifecycle=LifecycleSpec(status="active"),
            summary=summary,
            documentation=summary,
            overloads=(
                FunctionOverload(
                    id=f"{name}_0",
                    parameters=(),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=return_type),
                    kernel=KernelSpec(id="constant", callable_ref=_noop, cost_us_per_million=0.1),
                ),
            ),
        )

    return [
        _make("CURRENT_DATE", OrsoTypes.DATE, aliases=("TODAY",), summary="Current date."),
        _make("YESTERDAY", OrsoTypes.DATE, summary="Yesterday's date."),
        _make("CURRENT_TIME", OrsoTypes.TIME, summary="Current time."),
        _make(
            "NOW",
            OrsoTypes.TIMESTAMP,
            aliases=("UTC_TIMESTAMP", "CURRENT_TIMESTAMP"),
            summary="Current timestamp.",
        ),
        _make("VERSION", OrsoTypes.VARCHAR, summary="Database version string."),
        _make("CONNECTION_ID", OrsoTypes.INTEGER, summary="Current connection identifier."),
        _make("DATABASE", OrsoTypes.VARCHAR, summary="Current database name."),
        _make("USER", OrsoTypes.VARCHAR, summary="Current user name."),
        _make("PI", OrsoTypes.DOUBLE, summary="Mathematical constant π."),
        _make("PHI", OrsoTypes.DOUBLE, summary="Golden ratio φ."),
        _make("E", OrsoTypes.DOUBLE, summary="Euler's number e."),
    ]


def _datepart_return_type(arg_nodes) -> OrsoTypes:
    """DATEPART return type depends on the part name literal."""
    part_val = getattr(arg_nodes[0], "value", None) if arg_nodes else None
    if part_val is None:
        return OrsoTypes.INTEGER
    part = str(part_val).lower()
    if part in ("epoch", "julian"):
        return OrsoTypes.DOUBLE
    if part == "date":
        return OrsoTypes.DATE
    return OrsoTypes.INTEGER


def _builtin_temporal_extra_functions() -> list[FunctionDefinition]:
    """Temporal functions with parameter-dependent return types."""
    from opteryx.expression.functions.implementations import temporal as date_functions

    return [
        FunctionDefinition(
            name="DATEPART",
            aliases=(),
            category="temporal",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Extract a part from a date/timestamp.",
            documentation="Extracts a named part (year, month, day, epoch, etc.) from a date or timestamp.",
            overloads=(
                FunctionOverload(
                    id="DATEPART_2",
                    parameters=(
                        ParameterSpec(name="part", type_family="string", constant_only=True),
                        ParameterSpec(name="date", type_family="temporal"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_datepart_return_type),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=date_functions.date_part,
                        cost_us_per_million=4.0,
                    ),
                ),
            ),
        ),
    ]


def _builtin_utility_functions() -> list[FunctionDefinition]:
    """Utility functions: array ops, subscript, element access."""
    import numpy

    from opteryx.functions import _iterate_single_parameter as _isingle
    from opteryx.functions import _sort as _sort_factory

    _greatest_kernel = _isingle(numpy.nanmax)
    _least_kernel = _isingle(numpy.nanmin)
    _sort_kernel = _sort_factory(numpy.sort)

    def _element_type_return(arg_nodes) -> OrsoTypes:
        """Return the element type of the first arg (for GREATEST/LEAST/SORT)."""
        sc = getattr(arg_nodes[0], "schema_column", None)
        return sc.element_type if sc is not None else OrsoTypes.NULL

    def _array_literal_return_type(arg_nodes):
        """ARRAY(expr, type_name): return ARRAY<type_name>."""
        type_name = getattr(arg_nodes[1], "value", None) if len(arg_nodes) > 1 else None
        if type_name:
            result_type, _, _, _, element_type = OrsoTypes.from_name(f"ARRAY<{type_name}>")
            return (result_type, element_type)
        return (OrsoTypes.ARRAY, OrsoTypes.NULL)

    _variadic_any = (
        ParameterSpec(name="arg0", type_family="any"),
        ParameterSpec(name="args", type_family="any", variadic=True, optional=True),
    )

    return [
        FunctionDefinition(
            name="GREATEST",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return maximum element of an array.",
            documentation="Returns the maximum element from an array column.",
            overloads=(
                FunctionOverload(
                    id="GREATEST_1",
                    parameters=(ParameterSpec(name="arr", type_family="array"),),
                    return_spec=ReturnSpec(mode="resolver", resolver=_element_type_return),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=_greatest_kernel,
                        cost_us_per_million=3.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="LEAST",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return minimum element of an array.",
            documentation="Returns the minimum element from an array column.",
            overloads=(
                FunctionOverload(
                    id="LEAST_1",
                    parameters=(ParameterSpec(name="arr", type_family="array"),),
                    return_spec=ReturnSpec(mode="resolver", resolver=_element_type_return),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=_least_kernel,
                        cost_us_per_million=3.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="SORT",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Sort an array.",
            documentation="Returns a sorted version of an array column.",
            overloads=(
                FunctionOverload(
                    id="SORT_1",
                    parameters=(ParameterSpec(name="arr", type_family="array"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=_sort_kernel,
                        cost_us_per_million=5.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="ARRAY",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Construct a typed array.",
            documentation="Constructs an array of the specified element type.",
            overloads=(
                FunctionOverload(
                    id="ARRAY_2",
                    parameters=(
                        ParameterSpec(name="expr", type_family="any"),
                        ParameterSpec(name="type_name", type_family="string", constant_only=True),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_array_literal_return_type),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=lambda *a: None,  # constructed inline by evaluator
                        cost_us_per_million=2.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="TRY_ARRAY",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Construct a typed array, returning null on failure.",
            documentation="Like ARRAY but returns null on type conversion failure.",
            overloads=(
                FunctionOverload(
                    id="TRY_ARRAY_2",
                    parameters=(
                        ParameterSpec(name="expr", type_family="any"),
                        ParameterSpec(name="type_name", type_family="string", constant_only=True),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_array_literal_return_type),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=lambda *a: None,
                        cost_us_per_million=2.0,
                    ),
                ),
            ),
        ),
    ]


def _builtin_text_extended_functions() -> list[FunctionDefinition]:
    """Remaining string/text functions not in the core text group."""
    from pyarrow import compute

    from opteryx.expression.functions.implementations import text as string_functions
    from opteryx.functions import _get_string
    from opteryx.functions import _initcap
    from opteryx.functions import _iterate_double_parameter_swapped
    from opteryx.functions import _replace
    from opteryx.functions import _soundex
    from opteryx.functions import _string_slice_left
    from opteryx.functions import _string_slice_right

    _position_kernel = _iterate_double_parameter_swapped(string_functions.position)

    def _make(
        name,
        callable_ref,
        ret,
        params,
        aliases=(),
        cost=5.0,
        null_policy="strict",
        summary="",
        doc="",
    ):
        return FunctionDefinition(
            name=name,
            aliases=aliases,
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary=summary or name,
            documentation=doc or summary or name,
            overloads=(
                FunctionOverload(
                    id=f"{name}_default",
                    parameters=params,
                    return_spec=ReturnSpec(mode="fixed", fixed_type=ret),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=callable_ref,
                        null_policy=null_policy,
                        cost_us_per_million=cost,
                    ),
                ),
            ),
        )

    _s = ParameterSpec(name="str", type_family="string")
    _n = ParameterSpec(name="n", type_family="integer")
    _any = ParameterSpec(name="val", type_family="any")

    return [
        _make(
            "CHAR",
            string_functions.to_char,
            OrsoTypes.VARCHAR,
            (_any,),
            summary="Convert codepoint to character.",
        ),
        _make(
            "ASCII",
            string_functions.to_ascii,
            OrsoTypes.INTEGER,
            (_s,),
            summary="Return ASCII codepoint of first character.",
        ),
        _make(
            "LEFT",
            _string_slice_left,
            OrsoTypes.VARCHAR,
            (_s, _n),
            summary="Return leftmost N characters.",
        ),
        _make(
            "RIGHT",
            _string_slice_right,
            OrsoTypes.VARCHAR,
            (_s, _n),
            summary="Return rightmost N characters.",
        ),
        _make(
            "REVERSE", compute.utf8_reverse, OrsoTypes.VARCHAR, (_s,), summary="Reverse a string."
        ),
        _make(
            "SOUNDEX", _soundex, OrsoTypes.VARCHAR, (_s,), summary="Return Soundex phonetic code."
        ),
        _make(
            "TITLE",
            compute.utf8_title,
            OrsoTypes.VARCHAR,
            (_s,),
            aliases=("TITLECASE",),
            summary="Convert string to title case.",
        ),
        _make(
            "INITCAP",
            _initcap,
            OrsoTypes.VARCHAR,
            (_s,),
            summary="Capitalise first letter of each word.",
        ),
        _make(
            "CONCAT_WS",
            string_functions.concat_ws,
            OrsoTypes.VARCHAR,
            (
                ParameterSpec(name="sep", type_family="string"),
                ParameterSpec(name="str1", type_family="string"),
                ParameterSpec(name="more", type_family="string", variadic=True, optional=True),
            ),
            summary="Concatenate with separator.",
            null_policy="passthrough",
        ),
        _make(
            "POSITION",
            _position_kernel,
            OrsoTypes.INTEGER,
            (
                ParameterSpec(name="needle", type_family="string"),
                ParameterSpec(name="haystack", type_family="string"),
            ),
            summary="Find position of substring.",
        ),
        _make(
            "TRIM",
            string_functions.trim,
            OrsoTypes.VARCHAR,
            (_s, ParameterSpec(name="chars", type_family="string", optional=True)),
            null_policy="passthrough",
            summary="Trim leading and trailing characters.",
        ),
        _make(
            "LTRIM",
            string_functions.ltrim,
            OrsoTypes.VARCHAR,
            (_s, ParameterSpec(name="chars", type_family="string", optional=True)),
            null_policy="passthrough",
            summary="Trim leading characters.",
        ),
        _make(
            "RTRIM",
            string_functions.rtrim,
            OrsoTypes.VARCHAR,
            (_s, ParameterSpec(name="chars", type_family="string", optional=True)),
            null_policy="passthrough",
            summary="Trim trailing characters.",
        ),
        _make(
            "LPAD",
            string_functions.left_pad,
            OrsoTypes.VARCHAR,
            (
                _s,
                ParameterSpec(name="width", type_family="integer"),
                ParameterSpec(name="fill", type_family="string", optional=True),
            ),
            summary="Left-pad string to width.",
        ),
        _make(
            "RPAD",
            string_functions.right_pad,
            OrsoTypes.VARCHAR,
            (
                _s,
                ParameterSpec(name="width", type_family="integer"),
                ParameterSpec(name="fill", type_family="string", optional=True),
            ),
            summary="Right-pad string to width.",
        ),
        _make(
            "LEVENSHTEIN",
            string_functions.levenshtein,
            OrsoTypes.INTEGER,
            (
                ParameterSpec(name="a", type_family="string"),
                ParameterSpec(name="b", type_family="string"),
            ),
            cost=50.0,
            summary="Levenshtein edit distance between two strings.",
        ),
        _make(
            "SPLIT",
            string_functions.split,
            OrsoTypes.ARRAY,
            (
                _s,
                ParameterSpec(name="delimiter", type_family="string", optional=True),
                ParameterSpec(name="limit", type_family="integer", optional=True),
            ),
            null_policy="passthrough",
            summary="Split string into array.",
        ),
        _make(
            "MATCH_AGAINST",
            string_functions.match_against,
            OrsoTypes.BOOLEAN,
            (
                _s,
                ParameterSpec(name="pattern", type_family="string"),
            ),
            cost=20.0,
            summary="Full-text match.",
        ),
        _make(
            "REPLACE",
            _replace,
            OrsoTypes.VARCHAR,
            (
                _s,
                ParameterSpec(name="search", type_family="string"),
                ParameterSpec(name="replacement", type_family="string"),
            ),
            summary="Replace occurrences of substring.",
        ),
        _make(
            "REGEXP_REPLACE",
            string_functions.regex_replace,
            OrsoTypes.BLOB,
            (
                _s,
                ParameterSpec(name="pattern", type_family="string"),
                ParameterSpec(name="replacement", type_family="string"),
            ),
            cost=30.0,
            summary="Replace regex matches.",
        ),
        _make(
            "GET_STRING",
            _get_string,
            OrsoTypes.VARCHAR,
            (
                ParameterSpec(name="struct", type_family="any"),
                ParameterSpec(name="key", type_family="string"),
            ),
            summary="Extract string field from struct/map.",
        ),
    ]


def _builtin_hash_encoding_functions() -> list[FunctionDefinition]:
    """Hash, encoding, and random-generation functions."""
    from opteryx.expression.functions.implementations import arithmetic as number_functions
    from opteryx.expression.functions.implementations import text as string_functions
    from opteryx.functions import _iterate_single_parameter as _isingle
    from opteryx.functions import _md5
    from opteryx.functions import _sha1
    from opteryx.functions import _sha256
    from opteryx.functions import _sha512
    from opteryx.third_party.cyan4973.xxhash import hash_bytes

    _hash_kernel = _isingle(lambda x: hex(hash_bytes(str(x).encode()))[2:])
    _sha224_kernel = _isingle(string_functions.get_sha224)
    _sha384_kernel = _isingle(string_functions.get_sha384)
    _base85_enc_kernel = _isingle(string_functions.get_base85_encode)
    _base85_dec_kernel = _isingle(string_functions.get_base85_decode)
    _hex_enc_kernel = _isingle(string_functions.get_hex_encode)
    _hex_dec_kernel = _isingle(string_functions.get_hex_decode)

    def _make(
        name,
        callable_ref,
        ret,
        params,
        aliases=(),
        cost=10.0,
        volatility="immutable",
        null_policy="strict",
        summary="",
    ):
        return FunctionDefinition(
            name=name,
            aliases=aliases,
            category="hash_encoding",
            volatility=volatility,
            deterministic=volatility == "immutable",
            lifecycle=LifecycleSpec(status="active"),
            summary=summary or name,
            documentation=summary or name,
            overloads=(
                FunctionOverload(
                    id=f"{name}_default",
                    parameters=params,
                    return_spec=ReturnSpec(mode="fixed", fixed_type=ret),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=callable_ref,
                        null_policy=null_policy,
                        cost_us_per_million=cost,
                    ),
                ),
            ),
        )

    _any = ParameterSpec(name="val", type_family="any")
    _n = ParameterSpec(name="n", type_family="integer")
    _b = ParameterSpec(name="blob", type_family="any")

    return [
        _make("HASH", _hash_kernel, OrsoTypes.BLOB, (_any,), cost=15.0, summary="Generic hash."),
        _make("MD5", _md5, OrsoTypes.BLOB, (_any,), cost=12.0, summary="MD5 hash."),
        _make("SHA1", _sha1, OrsoTypes.BLOB, (_any,), cost=12.0, summary="SHA-1 hash."),
        _make(
            "SHA224", _sha224_kernel, OrsoTypes.BLOB, (_any,), cost=14.0, summary="SHA-224 hash."
        ),
        _make("SHA256", _sha256, OrsoTypes.BLOB, (_any,), cost=14.0, summary="SHA-256 hash."),
        _make(
            "SHA384", _sha384_kernel, OrsoTypes.BLOB, (_any,), cost=14.0, summary="SHA-384 hash."
        ),
        _make("SHA512", _sha512, OrsoTypes.BLOB, (_any,), cost=14.0, summary="SHA-512 hash."),
        _make(
            "RANDOM",
            number_functions.random_number,
            OrsoTypes.DOUBLE,
            (_n,),
            volatility="volatile",
            summary="Generate random numbers.",
        ),
        _make(
            "NORMAL",
            number_functions.random_normal,
            OrsoTypes.DOUBLE,
            (_n,),
            volatility="volatile",
            summary="Generate normally-distributed random numbers.",
        ),
        _make(
            "RANDOM_STRING",
            number_functions.random_strings,
            OrsoTypes.BLOB,
            (_n,),
            volatility="volatile",
            summary="Generate random strings.",
        ),
        _make(
            "BASE64_ENCODE",
            string_functions.base64_encode,
            OrsoTypes.BLOB,
            (_b,),
            summary="Base64 encode.",
        ),
        _make(
            "BASE64_DECODE",
            string_functions.base64_decode,
            OrsoTypes.BLOB,
            (_b,),
            summary="Base64 decode.",
        ),
        _make("BASE85_ENCODE", _base85_enc_kernel, OrsoTypes.BLOB, (_b,), summary="Base85 encode."),
        _make("BASE85_DECODE", _base85_dec_kernel, OrsoTypes.BLOB, (_b,), summary="Base85 decode."),
        _make("HEX_ENCODE", _hex_enc_kernel, OrsoTypes.BLOB, (_b,), summary="Hex encode."),
        _make("HEX_DECODE", _hex_dec_kernel, OrsoTypes.BLOB, (_b,), summary="Hex decode."),
    ]


def _builtin_array_misc_functions() -> list[FunctionDefinition]:
    """Array membership tests and miscellaneous column-level functions."""
    from opteryx.compiled.vector_ops import vector_contains_all
    from opteryx.compiled.vector_ops import vector_contains_any
    from opteryx.expression.functions.implementations.logical import (
        array_contains as _of_array_contains,
    )
    from opteryx.expression.functions.implementations.logical import if_null as _of_if_null
    from opteryx.expression.functions.implementations.logical import null_if as _of_null_if
    from opteryx.expression.functions.implementations.utility import (
        cosine_similarity as _of_cosine_similarity,
    )
    from opteryx.expression.functions.implementations.utility import humanize as _of_humanize
    from opteryx.expression.functions.implementations.utility import (
        jsonb_object_keys as _of_jsonb_object_keys,
    )
    from opteryx.functions import _iterate_double_parameter as _idouble

    class other_functions:
        array_contains = staticmethod(_of_array_contains)
        if_null = staticmethod(_of_if_null)
        null_if = staticmethod(_of_null_if)
        cosine_similarity = staticmethod(_of_cosine_similarity)
        humanize = staticmethod(_of_humanize)
        jsonb_object_keys = staticmethod(_of_jsonb_object_keys)

    _array_contains_kernel = _idouble(other_functions.array_contains)
    _array_contains_any_kernel = lambda x, y: vector_contains_any(x, set(y[0]))
    _array_contains_all_kernel = lambda x, y: vector_contains_all(x, set(y[0]))

    def _make(
        name, callable_ref, ret, params, aliases=(), cost=8.0, null_policy="strict", summary=""
    ):
        return FunctionDefinition(
            name=name,
            aliases=aliases,
            category="array",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary=summary or name,
            documentation=summary or name,
            overloads=(
                FunctionOverload(
                    id=f"{name}_default",
                    parameters=params,
                    return_spec=ReturnSpec(mode="fixed", fixed_type=ret),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=callable_ref,
                        null_policy=null_policy,
                        cost_us_per_million=cost,
                    ),
                ),
            ),
        )

    _arr = ParameterSpec(name="arr", type_family="array")
    _item = ParameterSpec(name="item", type_family="any")
    _set = ParameterSpec(name="items", type_family="array")

    return [
        _make(
            "ARRAY_CONTAINS",
            _array_contains_kernel,
            OrsoTypes.BOOLEAN,
            (_arr, _item),
            summary="Test if array contains item.",
        ),
        _make(
            "ARRAY_CONTAINS_ANY",
            _array_contains_any_kernel,
            OrsoTypes.BOOLEAN,
            (_arr, _set),
            null_policy="passthrough",
            summary="Test if array contains any item from set.",
        ),
        _make(
            "ARRAY_CONTAINS_ALL",
            _array_contains_all_kernel,
            OrsoTypes.BOOLEAN,
            (_arr, _set),
            null_policy="passthrough",
            summary="Test if array contains all items from set.",
        ),
        _make(
            "JSONB_OBJECT_KEYS",
            other_functions.jsonb_object_keys,
            OrsoTypes.ARRAY,
            (ParameterSpec(name="json", type_family="any"),),
            cost=15.0,
            summary="Extract keys from JSON object.",
        ),
        _make(
            "HUMANIZE",
            other_functions.humanize,
            OrsoTypes.VARCHAR,
            (ParameterSpec(name="val", type_family="any"),),
            cost=10.0,
            summary="Format number in human-readable form.",
        ),
        _make(
            "COSINE_SIMILARITY",
            other_functions.cosine_similarity,
            OrsoTypes.DOUBLE,
            (_arr, ParameterSpec(name="vec", type_family="array")),
            cost=30.0,
            summary="Cosine similarity between two vectors.",
        ),
    ]


def _builtin_arithmetic_extended_functions() -> list[FunctionDefinition]:
    """Numeric functions not in the core arithmetic group."""
    from pyarrow import compute

    from opteryx.expression.functions.implementations import arithmetic as number_functions

    def _make(
        name, callable_ref, ret, params, aliases=(), cost=2.0, null_policy="strict", summary=""
    ):
        return FunctionDefinition(
            name=name,
            aliases=aliases,
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary=summary or name,
            documentation=summary or name,
            overloads=(
                FunctionOverload(
                    id=f"{name}_default",
                    parameters=params,
                    return_spec=ReturnSpec(mode="fixed", fixed_type=ret),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=callable_ref,
                        null_policy=null_policy,
                        cost_us_per_million=cost,
                    ),
                ),
            ),
        )

    _num = ParameterSpec(name="num", type_family="numeric")

    return [
        _make(
            "SIGN", compute.sign, OrsoTypes.INTEGER, (_num,), summary="Sign of number (-1, 0, 1)."
        ),
        _make("TRUNC", compute.trunc, OrsoTypes.INTEGER, (_num,), summary="Truncate to integer."),
        _make(
            "POWER",
            number_functions.safe_power,
            OrsoTypes.DOUBLE,
            (_num, ParameterSpec(name="exp", type_family="numeric")),
            aliases=("POW",),
            cost=5.0,
            summary="Raise base to exponent.",
        ),
        _make("LN", compute.ln, OrsoTypes.DOUBLE, (_num,), summary="Natural logarithm."),
        _make("LOG10", compute.log10, OrsoTypes.DOUBLE, (_num,), summary="Base-10 logarithm."),
        _make("LOG2", compute.log2, OrsoTypes.DOUBLE, (_num,), summary="Base-2 logarithm."),
        _make(
            "LOG",
            compute.logb,
            OrsoTypes.DOUBLE,
            (_num, ParameterSpec(name="base", type_family="numeric")),
            summary="Logarithm with arbitrary base.",
        ),
    ]


def _builtin_temporal_functions() -> list[FunctionDefinition]:
    """Full temporal function set."""
    from opteryx.expression.functions.implementations import temporal as date_functions
    from opteryx.utils.dates import date_trunc

    def _make(
        name, callable_ref, ret, params, aliases=(), cost=4.0, null_policy="strict", summary=""
    ):
        return FunctionDefinition(
            name=name,
            aliases=aliases,
            category="temporal",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary=summary or name,
            documentation=summary or name,
            overloads=(
                FunctionOverload(
                    id=f"{name}_default",
                    parameters=params,
                    return_spec=ReturnSpec(mode="fixed", fixed_type=ret),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=callable_ref,
                        null_policy=null_policy,
                        cost_us_per_million=cost,
                    ),
                ),
            ),
        )

    _part = ParameterSpec(name="part", type_family="string", constant_only=True)
    _date = ParameterSpec(name="date", type_family="temporal")

    return [
        _make(
            "DATE_TRUNC",
            date_trunc,
            OrsoTypes.TIMESTAMP,
            (_part, _date),
            aliases=("DATETRUNC",),
            summary="Truncate date/timestamp to specified granularity.",
        ),
        _make(
            "TIME_BUCKET",
            date_functions.date_floor,
            OrsoTypes.TIMESTAMP,
            (
                ParameterSpec(name="magnitude", type_family="numeric"),
                ParameterSpec(name="units", type_family="string", constant_only=True),
                _date,
            ),
            summary="Bucket date into fixed-width intervals.",
        ),
        _make(
            "DATEDIFF",
            date_functions.date_diff,
            OrsoTypes.INTEGER,
            (_part, _date, ParameterSpec(name="end", type_family="temporal")),
            aliases=("DATE_DIFF",),
            cost=5.0,
            summary="Difference between two dates in the specified unit.",
        ),
        _make(
            "TIMEDIFF",
            date_functions.time_diff,
            OrsoTypes.INTEGER,
            (
                ParameterSpec(name="time1", type_family="temporal"),
                ParameterSpec(name="time2", type_family="temporal"),
            ),
            aliases=("TIME_DIFF",),
            cost=5.0,
            summary="Difference between two times.",
        ),
        _make(
            "DATE_FORMAT",
            date_functions.date_format,
            OrsoTypes.VARCHAR,
            (_date, ParameterSpec(name="pattern", type_family="string", constant_only=True)),
            cost=6.0,
            summary="Format date/timestamp as string.",
        ),
        _make(
            "FROM_UNIXTIME",
            date_functions.from_unixtimestamp,
            OrsoTypes.TIMESTAMP,
            (ParameterSpec(name="ts", type_family="numeric"),),
            cost=4.0,
            summary="Convert Unix timestamp to TIMESTAMP.",
        ),
        _make(
            "UNIXTIME",
            date_functions.unixtime,
            OrsoTypes.INTEGER,
            (_date,),
            aliases=("TO_UNIXTIME",),
            cost=3.0,
            summary="Convert TIMESTAMP to Unix epoch seconds.",
        ),
    ]


def get_builtin_functions() -> list[FunctionDefinition]:
    """Load all builtin function definitions."""
    functions = []
    functions.extend(_builtin_text_functions())
    functions.extend(_builtin_text_extended_functions())
    functions.extend(_builtin_arithmetic_functions())
    functions.extend(_builtin_arithmetic_extended_functions())
    functions.extend(_builtin_type_conversion_functions())
    functions.extend(_builtin_logical_functions())
    functions.extend(_builtin_aggregate_functions())
    functions.extend(_builtin_constant_functions())
    functions.extend(_builtin_temporal_extra_functions())
    functions.extend(_builtin_temporal_functions())
    functions.extend(_builtin_utility_functions())
    functions.extend(_builtin_hash_encoding_functions())
    functions.extend(_builtin_array_misc_functions())
    return functions
