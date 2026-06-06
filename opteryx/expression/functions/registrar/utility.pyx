from typing import List

from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)

# Use package-level helper to construct concise FunctionDefinition entries.
from opteryx.types import SqlType


def get_builtin_utility_functions() -> List[FunctionDefinition]:
    """Utility functions: array ops, subscript, element access, and array constructors.

    This module merges the previous array_misc group into utility as requested.
    """
    from opteryx.expression.functions.registrar import _iterate_single_parameter as _isingle
    from opteryx.expression.functions.registrar import _sort as _sort_factory

    def _nanmax(arr):
        """Find maximum value ignoring NaNs, handling None values."""
        if not arr:
            return None
        valid = [x for x in arr if x is not None and x == x]  # x == x filters out NaN
        return max(valid) if valid else None

    def _nanmin(arr):
        """Find minimum value ignoring NaNs, handling None values."""
        if not arr:
            return None
        valid = [x for x in arr if x is not None and x == x]  # x == x filters out NaN
        return min(valid) if valid else None

    def _sort(arr):
        """Sort array, preserving None values at the end."""
        if not arr:
            return arr
        nones = [x for x in arr if x is None]
        valid = [x for x in arr if x is not None]
        try:
            return sorted(valid) + nones
        except TypeError:
            # Fallback for mixed types - return as-is
            return arr

    _greatest_kernel = _isingle(_nanmax)
    _least_kernel = _isingle(_nanmin)
    _sort_kernel = _sort_factory(_sort)

    def _element_type_return(arg_nodes) -> SqlType:
        """Return the element type of the first arg (for GREATEST/LEAST/SORT).

        D-4 Phase 2: the array element comes from the unified column_type
        (`column_type.element`), reverse-bridged to SqlType. NULL when unknown.
        """
        sc = getattr(arg_nodes[0], "schema_column", None)
        if sc is None or sc.column_type is None or sc.column_type.element is None:
            return SqlType.NULL
        from opteryx.types.sql_type import column_type_to_sql
        return column_type_to_sql(sc.column_type.element).get("type") or SqlType.NULL

    def _array_literal_return_type(arg_nodes):
        """ARRAY(expr, type_name): return ARRAY<type_name>."""
        type_name = getattr(arg_nodes[1], "value", None) if len(arg_nodes) > 1 else None
        if type_name:
            result_type, _, _, _, element_type = SqlType.from_name(f"ARRAY<{type_name}>")
            return (result_type, element_type)
        return (SqlType.ARRAY, SqlType.NULL)

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
                        engine="draken",
                        id="default",
                        callable_ref=_greatest_kernel,
                        cost_us_per_million=4482220.29,
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
                        engine="draken",
                        id="default",
                        callable_ref=_least_kernel,
                        cost_us_per_million=4419443.37,
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
                        engine="draken",
                        id="default",
                        callable_ref=_sort_kernel,
                        cost_us_per_million=4.92,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="_ARRAY",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Construct a typed array.",
            documentation="Constructs an array of the specified element type.",
            overloads=(
                FunctionOverload(
                    id="_ARRAY_2",
                    parameters=(
                        ParameterSpec(name="expr", type_family="any"),
                        ParameterSpec(name="type_name", type_family="string", constant_only=True),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_array_literal_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=lambda *a: None,  # constructed inline by evaluator
                        cost_us_per_million=0.25,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="_TRY_ARRAY",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Construct a typed array, returning null on failure.",
            documentation="Like ARRAY but returns null on type conversion failure.",
            overloads=(
                FunctionOverload(
                    id="_TRY_ARRAY_2",
                    parameters=(
                        ParameterSpec(name="expr", type_family="any"),
                        ParameterSpec(name="type_name", type_family="string", constant_only=True),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_array_literal_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=lambda *a: None,
                        cost_us_per_million=0.31,
                    ),
                ),
            ),
        ),
    ]


def get_builtin_array_misc_functions() -> List[FunctionDefinition]:
    """Array membership tests and miscellaneous column-level functions.

    These functions are grouped with utility functions per the requested merge.
    """
    # Local imports to keep startup lightweight
    from opteryx.compiled.nanobind.vector_string_search import (
        vector_contains_all,
        vector_contains_any,
    )
    from opteryx.expression.functions.implementations.logical import if_null as _of_if_null
    from opteryx.expression.functions.implementations.logical import null_if as _of_null_if
    from opteryx.expression.functions.implementations.utility import (
        array_contains as _of_array_contains,
    )
    from opteryx.expression.functions.implementations.utility import (
        array_contains_all as _of_array_contains_all,
    )
    from opteryx.expression.functions.implementations.utility import (
        array_contains_any as _of_array_contains_any,
    )
    from opteryx.expression.functions.implementations.utility import (
        cosine_distance as _of_cosine_distance,
    )
    from opteryx.expression.functions.implementations.utility import (
        cosine_similarity as _of_cosine_similarity,
    )
    from opteryx.expression.functions.implementations.utility import embed as _of_embed
    from opteryx.expression.functions.implementations.utility import humanize as _of_humanize
    from opteryx.expression.functions.implementations.utility import (
        jsonb_object_keys as _of_jsonb_object_keys,
    )

    class other_functions:
        array_contains = staticmethod(_of_array_contains)
        array_contains_all = staticmethod(_of_array_contains_all)
        array_contains_any = staticmethod(_of_array_contains_any)
        if_null = staticmethod(_of_if_null)
        null_if = staticmethod(_of_null_if)
        cosine_distance = staticmethod(_of_cosine_distance)
        cosine_similarity = staticmethod(_of_cosine_similarity)
        embed = staticmethod(_of_embed)
        humanize = staticmethod(_of_humanize)
        jsonb_object_keys = staticmethod(_of_jsonb_object_keys)

    # Parameter short-hands
    _arr = ParameterSpec(name="arr", type_family="array")
    _item = ParameterSpec(name="item", type_family="any")
    _set = ParameterSpec(name="items", type_family="array")
    _any = ParameterSpec(name="val", type_family="any")

    def _embed_return_type(_arg_nodes):
        return SqlType.VECTOR

    return [
        _make(
            "ARRAY_CONTAINS",
            other_functions.array_contains,
            SqlType.BOOLEAN,
            (_arr, _item),
            null_policy="passthru",
            summary="Test if array contains item.",
            cost=1.19,
        ),
        _make(
            "ARRAY_CONTAINS_ANY",
            other_functions.array_contains_any,
            SqlType.BOOLEAN,
            (_arr, _set),
            null_policy="passthru",
            summary="Test if array contains any item from set.",
            cost=1.02,
        ),
        _make(
            "ARRAY_CONTAINS_ALL",
            other_functions.array_contains_all,
            SqlType.BOOLEAN,
            (_arr, _set),
            null_policy="passthru",
            summary="Test if array contains all items from set.",
            cost=1.21,
        ),
        _make(
            "JSONB_OBJECT_KEYS",
            other_functions.jsonb_object_keys,
            SqlType.ARRAY,
            (ParameterSpec(name="json", type_family="any"),),
            cost=590.21,
            summary="Extract keys from JSON object.",
        ),
        _make(
            "HUMANIZE",
            other_functions.humanize,
            SqlType.VARCHAR,
            (ParameterSpec(name="val", type_family="any"),),
            cost=775947.17,
            summary="Format number in human-readable form.",
        ),
        FunctionDefinition(
            "EMBED",
            aliases=(),
            category="array",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Convert text to an embedding vector.",
            documentation="Embeds text using the configured engine embedding provider.",
            overloads=(
                FunctionOverload(
                    id="EMBED_TEXT",
                    parameters=(ParameterSpec(name="text", type_family="string"),),
                    return_spec=ReturnSpec(mode="resolver", resolver=_embed_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.embed,
                        null_policy="compress",
                        cost_us_per_million=1_000_000.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            "COSINE_SIMILARITY",
            aliases=(),
            category="array",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Cosine similarity between two vectors.",
            documentation="Cosine similarity over numeric vectors or semantic text inputs.",
            overloads=(
                FunctionOverload(
                    id="COSINE_SIMILARITY_VECTOR",
                    parameters=(
                        ParameterSpec(name="arr", type_family="numeric_vector"),
                        ParameterSpec(name="vec", type_family="numeric_vector"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=SqlType.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.cosine_similarity,
                        null_policy="compress",
                        cost_us_per_million=1.33,
                    ),
                ),
                FunctionOverload(
                    id="COSINE_SIMILARITY_TEXT",
                    parameters=(
                        ParameterSpec(name="arr", type_family="string"),
                        ParameterSpec(name="vec", type_family="string"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=SqlType.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.cosine_similarity,
                        null_policy="compress",
                        cost_us_per_million=1.33,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            "COSINE_DISTANCE",
            aliases=(),
            category="array",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Cosine distance between two vectors.",
            documentation="Cosine distance over numeric vectors or semantic text inputs.",
            overloads=(
                FunctionOverload(
                    id="COSINE_DISTANCE_VECTOR",
                    parameters=(
                        ParameterSpec(name="arr", type_family="numeric_vector"),
                        ParameterSpec(name="vec", type_family="numeric_vector"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=SqlType.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.cosine_distance,
                        null_policy="compress",
                        cost_us_per_million=1.17,
                    ),
                ),
                FunctionOverload(
                    id="COSINE_DISTANCE_TEXT",
                    parameters=(
                        ParameterSpec(name="arr", type_family="string"),
                        ParameterSpec(name="vec", type_family="string"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=SqlType.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.cosine_distance,
                        null_policy="compress",
                        cost_us_per_million=1.17,
                    ),
                ),
            ),
        ),
    ]
