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
# LogicalCategory imported via __init__.pyx (textually included); canonical ColumnTypes also in scope.



def get_builtin_utility_functions() -> List[FunctionDefinition]:
    """Utility functions: array ops, subscript, element access, and array constructors.

    This module merges the previous array_misc group into utility as requested.
    """
    import draken.draken_native as _dn

    from opteryx.compiled.nanobind.vectors import vector_ip_trunc as _ip_trunc_kernel

    _greatest_kernel = _dn.vector_array_greatest
    _least_kernel    = _dn.vector_array_least

    def _sort_kernel(vec):
        from draken.vectors.vector import Vector as _V
        rows = vec.to_pylist()
        results = []
        for arr in rows:
            if not arr:
                results.append(arr)
                continue
            nones = [x for x in arr if x is None]
            valid = [x for x in arr if x is not None]
            try:
                results.append(sorted(valid) + nones)
            except TypeError:
                results.append(arr)
        return _V(_dn.vector_array_from_sequence(results))

    def _element_type_return(arg_nodes):
        """Return the element ColumnType of the first arg (for GREATEST/LEAST/SORT).

        Phase 5: returns ColumnType directly. NULL ColumnType when element is unknown.
        """
        sc = getattr(arg_nodes[0], "schema_column", None)
        if sc is None or sc.column_type is None or sc.column_type.element is None:
            return _CT_NULL
        return sc.column_type.element  # ColumnType

    def _array_literal_return_type(arg_nodes):
        """ARRAY(expr, type_name): return ARRAY<type_name> ColumnType."""
        from opteryx.types.logical_type import parse_column_type
        type_name = getattr(arg_nodes[1], "value", None) if len(arg_nodes) > 1 else None
        if type_name:
            ct = parse_column_type(f"ARRAY<{type_name}>")
            # Return (ARRAY<X> ColumnType, element ColumnType) for catalog to split.
            elem = ct.element if ct.element is not None else _CT_VARIANT
            return (ct, elem)
        return (_CT_ARRAY(_CT_VARIANT), _CT_VARIANT)

    _variadic_any = (
        ParameterSpec(name="arg0", type_family="any"),
        ParameterSpec(name="args", type_family="any", variadic=True, optional=True),
    )

    return [
        FunctionDefinition(
            name="IP_TRUNC",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Network address of an IPv4 address for a given prefix length.",
            documentation=(
                "Applies a network mask to an IPv4 address, returning the network "
                "address: `IP_TRUNC(ip, 24)` on `192.168.1.1` returns `192.168.1.0`. "
                "The operation is a bitwise AND with the netmask for the prefix. "
                "Name and signature follow BigQuery's NET.IP_TRUNC — the prefix is an "
                "argument because an Opteryx IPv4 address carries no prefix length of "
                "its own, unlike a PostgreSQL `inet`."
            ),
            overloads=(
                FunctionOverload(
                    id="IP_TRUNC_2",
                    parameters=(
                        ParameterSpec(name="ip", type_family="integer"),
                        ParameterSpec(name="prefix", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_IPV4),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=_ip_trunc_kernel,
                    ),
                ),
            ),
        ),
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
    from opteryx.compiled.nanobind.vectors import (
        vector_contains_all,
        vector_contains_any,
    )
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
        # EMBED's real type IS known at bind time — the active capability declares the
        # width, and the binder hands that same number to the kernel in a
        # vector_dim_ctx, so the plan's type and the kernel's output cannot disagree.
        # The old VARIANT placeholder was not merely imprecise: it made EMBED
        # uncomposable, because COSINE_SIMILARITY's NUMERIC_VECTOR parameters reject
        # VARIANT, so COSINE_SIMILARITY(EMBED(a), EMBED(b)) failed to bind at all.
        from opteryx.types.vectors.embedding_capability import embedding_dimensions

        return _CT_VECTOR(embedding_dimensions())

    return [
        _make(
            "ARRAY_CONTAINS",
            other_functions.array_contains,
            _CT_BOOLEAN,
            (_arr, _item),
            summary="Test if array contains item.",
            cost=1.19,
        ),
        _make(
            "ARRAY_CONTAINS_ANY",
            other_functions.array_contains_any,
            _CT_BOOLEAN,
            (_arr, _set),
            summary="Test if array contains any item from set.",
            cost=1.02,
        ),
        _make(
            "ARRAY_CONTAINS_ALL",
            other_functions.array_contains_all,
            _CT_BOOLEAN,
            (_arr, _set),
            summary="Test if array contains all items from set.",
            cost=1.21,
        ),
        _make(
            "JSONB_OBJECT_KEYS",
            other_functions.jsonb_object_keys,
            _CT_ARRAY(_CT_VARIANT),
            # "string", not "any": it parses its argument as a JSON document, so
            # a non-string never worked — it failed inside the native engine as
            # `ExprMultiProjectOperator: error code 1`, which names nothing the
            # caller can act on. JSON documents arrive as VARCHAR/VARBINARY.
            (ParameterSpec(name="json", type_family="string"),),
            cost=590.21,
            summary="Extract keys from JSON object.",
        ),
        _make(
            "HUMANIZE",
            other_functions.humanize,
            _CT_VARCHAR,
            (ParameterSpec(name="val", type_family="numeric"),),
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.cosine_similarity,
                        cost_us_per_million=1.33,
                    ),
                ),
                FunctionOverload(
                    id="COSINE_SIMILARITY_TEXT",
                    parameters=(
                        ParameterSpec(name="arr", type_family="string"),
                        ParameterSpec(name="vec", type_family="string"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.cosine_similarity,
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.cosine_distance,
                        cost_us_per_million=1.17,
                    ),
                ),
                FunctionOverload(
                    id="COSINE_DISTANCE_TEXT",
                    parameters=(
                        ParameterSpec(name="arr", type_family="string"),
                        ParameterSpec(name="vec", type_family="string"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.cosine_distance,
                        cost_us_per_million=1.17,
                    ),
                ),
            ),
        ),
    ]
