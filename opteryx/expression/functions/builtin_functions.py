"""Builtin function definitions for the catalog.

This module defines FunctionDefinition entries for Opteryx's builtin functions,
wrapping existing implementations from opteryx.functions.

Functions are registered incrementally as they're needed. Start with high-value,
commonly used functions across all categories.
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
    from opteryx.functions import list_lengther
    from opteryx.functions import string_functions
    from opteryx.functions import to_lower
    from opteryx.functions import to_upper

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
                        callable_ref=list_lengther,
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
                    id="CONCAT_1",
                    parameters=(
                        ParameterSpec(name="str1", type_family="string"),
                        ParameterSpec(name="str2", type_family="string"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.VARCHAR),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=string_functions.concat,
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
                        cost_us_per_million=6.0,
                    ),
                ),
            ),
        ),
    ]


def _builtin_arithmetic_functions() -> list[FunctionDefinition]:
    """Arithmetic and numeric functions."""
    from pyarrow import compute

    from opteryx.functions import number_functions

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
            aliases=("CEILING",),
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


def _builtin_logical_functions() -> list[FunctionDefinition]:
    """Logical and control flow functions."""
    from opteryx.functions import other_functions

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
                    id="COALESCE_1",
                    parameters=tuple(
                        ParameterSpec(
                            name=f"arg{i}",
                            type_family="any",
                            optional=(i > 0),
                            variadic=(i == 1),
                        )
                        for i in range(2)
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.NULL),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=lambda *args: None,  # Placeholder; actual impl varies
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.NULL),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=other_functions.if_null,
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
            documentation="Returns null if two arguments are equal, otherwise returns first argument.",
            overloads=(
                FunctionOverload(
                    id="NULLIF_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="compare", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.NULL),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=other_functions.null_if,
                        cost_us_per_million=3.0,
                    ),
                ),
            ),
        ),
    ]


def get_builtin_functions() -> list[FunctionDefinition]:
    """Load all builtin function definitions."""
    functions = []
    functions.extend(_builtin_text_functions())
    functions.extend(_builtin_arithmetic_functions())
    functions.extend(_builtin_type_conversion_functions())
    functions.extend(_builtin_logical_functions())
    return functions
