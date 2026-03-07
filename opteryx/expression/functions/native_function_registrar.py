"""Native function registrar.

Registers Opteryx's builtin scalar functions into the FunctionCatalog.
This module is responsible for wiring together FunctionDefinition metadata
with the actual kernel callables that live in implementations/.

The implementations themselves are in:
    opteryx/expression/functions/implementations/
        arithmetic.py, text.py, temporal.py, logical.py,
        hash_encoding.py, utility.py

During the migration period, kernels are imported from the legacy
opteryx.functions module until implementations/ modules are complete.
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
    from opteryx.functions import string_functions
    from opteryx.functions import to_lower
    from opteryx.functions import to_upper
    from opteryx.functions import vector_lengther

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
    from opteryx.functions import FUNCTIONS as _LEGACY
    from opteryx.functions import other_functions

    _coalesce_kernel = _LEGACY["COALESCE"][0]
    _iif_kernel = _LEGACY["IIF"][0]
    _case_kernel = _LEGACY["CASE"][0]

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
    """Aggregate functions (GROUP BY targets)."""

    def _same_as_arg0(arg_nodes) -> OrsoTypes:
        sc = getattr(arg_nodes[0], "schema_column", None)
        return sc.type if sc is not None else OrsoTypes.NULL

    def _array_agg_return_type(arg_nodes):
        sc = getattr(arg_nodes[0], "schema_column", None)
        element_type = sc.type if sc is not None else OrsoTypes.NULL
        return (OrsoTypes.ARRAY, element_type)

    _placeholder = lambda *args: None  # aggregators dispatched by draken, not apply_function

    return [
        FunctionDefinition(
            name="COUNT",
            aliases=("COUNT_STAR",),
            category="aggregate",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Count rows.",
            documentation="Returns the number of non-null rows.",
            overloads=(
                FunctionOverload(
                    id="COUNT_variadic",
                    parameters=(
                        ParameterSpec(name="expr", type_family="any", variadic=True, optional=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.INTEGER),
                    kernel=KernelSpec(
                        id="default", callable_ref=_placeholder, cost_us_per_million=1.0
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="AVG",
            aliases=("MEAN",),
            category="aggregate",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Arithmetic mean.",
            documentation="Returns the arithmetic mean of non-null values.",
            overloads=(
                FunctionOverload(
                    id="AVG_1",
                    parameters=(ParameterSpec(name="expr", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        id="default", callable_ref=_placeholder, cost_us_per_million=2.0
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="SUM",
            aliases=(),
            category="aggregate",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Sum of values.",
            documentation="Returns the sum of non-null values.",
            overloads=(
                FunctionOverload(
                    id="SUM_1",
                    parameters=(ParameterSpec(name="expr", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="resolver", resolver=_same_as_arg0),
                    kernel=KernelSpec(
                        id="default", callable_ref=_placeholder, cost_us_per_million=2.0
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="MAX",
            aliases=(),
            category="aggregate",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Maximum value.",
            documentation="Returns the maximum non-null value.",
            overloads=(
                FunctionOverload(
                    id="MAX_1",
                    parameters=(ParameterSpec(name="expr", type_family="any"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        id="default", callable_ref=_placeholder, cost_us_per_million=2.0
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="MIN",
            aliases=(),
            category="aggregate",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Minimum value.",
            documentation="Returns the minimum non-null value.",
            overloads=(
                FunctionOverload(
                    id="MIN_1",
                    parameters=(ParameterSpec(name="expr", type_family="any"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        id="default", callable_ref=_placeholder, cost_us_per_million=2.0
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="ARRAY_AGG",
            aliases=(),
            category="aggregate",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Aggregate values into an array.",
            documentation="Collects all non-null values into an array.",
            overloads=(
                FunctionOverload(
                    id="ARRAY_AGG_1",
                    parameters=(ParameterSpec(name="expr", type_family="any"),),
                    return_spec=ReturnSpec(mode="resolver", resolver=_array_agg_return_type),
                    kernel=KernelSpec(
                        id="default", callable_ref=_placeholder, cost_us_per_million=5.0
                    ),
                ),
            ),
        ),
    ]


def _datepart_return_type(arg_nodes) -> OrsoTypes:
    """DATEPART return type depends on the part name literal."""
    part_val = getattr(arg_nodes[0], "value", None) if arg_nodes else None
    if part_val is None:
        return OrsoTypes.INTEGER
    part = str(part_val).lower()
    if part in ("epoch", "julian"):
        return OrsoTypes.DOUBLE
    if part == "day":
        return OrsoTypes.VARCHAR
    if part == "date":
        return OrsoTypes.DATE
    return OrsoTypes.INTEGER


def _builtin_temporal_extra_functions() -> list[FunctionDefinition]:
    """Temporal functions with parameter-dependent return types."""
    from opteryx.functions import date_functions

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
    from opteryx.functions import FUNCTIONS as _LEGACY

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
                        callable_ref=_LEGACY["GREATEST"][0],
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
                        callable_ref=_LEGACY["LEAST"][0],
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
                        callable_ref=_LEGACY["SORT"][0],
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


def get_builtin_functions() -> list[FunctionDefinition]:
    """Load all builtin function definitions."""
    functions = []
    functions.extend(_builtin_text_functions())
    functions.extend(_builtin_arithmetic_functions())
    functions.extend(_builtin_type_conversion_functions())
    functions.extend(_builtin_logical_functions())
    functions.extend(_builtin_aggregate_functions())
    functions.extend(_builtin_temporal_extra_functions())
    functions.extend(_builtin_utility_functions())
    return functions
