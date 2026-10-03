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
        sc = arg_nodes[0].schema_column
        if sc is None or sc.column_type is None or sc.column_type.element is None:
            return _CT_NULL
        return sc.column_type.element  # ColumnType

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
    from opteryx.expression.functions.implementations.utility import humanize as _of_humanize
    from opteryx.expression.functions.implementations.utility import (
        jsonb_object_keys as _of_jsonb_object_keys,
    )
    from opteryx.expression.functions.implementations.utility import (
        generate_series as _of_generate_series,
    )

    class other_functions:
        null_if = staticmethod(_of_null_if)
        humanize = staticmethod(_of_humanize)
        jsonb_object_keys = staticmethod(_of_jsonb_object_keys)
        generate_series = staticmethod(_of_generate_series)

    # Parameter short-hands
    _any = ParameterSpec(name="val", type_family="any")

    # ARRAY_CONTAINS / _ANY / _ALL were removed: the operator spellings
    # (`item = ANY(arr)`, `arr @> (…)`, `arr @>> (…)`) are the supported
    # surface and reach the very same draken kernels through their own
    # bind-time lowerings in compiled_expression.pyx. The function names were
    # pure duplicate surface area.
    # GENERATE_SERIES's three arities are three OVERLOADS, not one variadic
    # signature, because the single-argument form means something different from
    # the others: GENERATE_SERIES(10) is GENERATE_SERIES(1, 10) — the lone
    # argument is the END, not the start. A variadic `stop, [start], [step]`
    # could not say that.
    #
    # Overload ids are GENERATE_SERIES_1/_2/_3 so that each resolves to
    # `draken_generate_series_{n}` in kernel_registry.cpp — the arity-suffixed
    # names the bind-time resolver probes (draken_{overload_id.lower()}).
    #
    # Every parameter is `constant_only`. A per-row series would give each row an
    # array of a different length, driven by column values with no bound, which is
    # a different computation rather than this one relaxed. The declaration is
    # ENFORCED at lowering (compiled_expression.pyx), so a column argument is
    # refused by name rather than silently taking row 0's value.
    #
    # `integer`, not `numeric`: the TABLE spelling
    # (`FROM GENERATE_SERIES(...) AS g`, opteryx/utils/series.py) also accepts
    # floats, deciding the last element's membership by an accumulate-and-
    # tolerance rule. A second implementation of a fuzzy boundary is how two
    # spellings of one name start disagreeing at the edges, so the scalar form
    # takes integers and a float argument is refused at bind time.
    _gs_docs = (
        "Builds the series as an ARRAY in a single row. `GENERATE_SERIES(10)` "
        "starts at 1; `GENERATE_SERIES(1, 10)` and `GENERATE_SERIES(1, 10, 2)` "
        "start where they say. `stop` is included when it falls on a step "
        "boundary. A step pointing away from `stop` yields an EMPTY array; a step "
        "of zero is refused. Arguments must be integer CONSTANTS. To get one ROW "
        "per value instead - which is what gap-filling and joining against a dense "
        "axis need - use the table spelling, `FROM GENERATE_SERIES(1, 10) AS g`, "
        "which also accepts floats and, with an INTERVAL step, timestamps."
    )
    _gs_start = ParameterSpec(
        name="start", type_family="integer", constant_only=True,
        documentation="First value of the series.",
    )
    _gs_stop = ParameterSpec(
        name="stop", type_family="integer", constant_only=True,
        documentation="Last value of the series, included when it falls on a step boundary.",
    )
    _gs_step = ParameterSpec(
        name="step", type_family="integer", constant_only=True,
        documentation="Distance between consecutive values; may be negative, never zero.",
    )

    def _generate_series_overload(overload_id, params, cost):
        return FunctionOverload(
            id=overload_id,
            parameters=params,
            return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_ARRAY(_CT_INT64)),
            kernel=KernelSpec(
                engine="draken",
                id="default",
                callable_ref=other_functions.generate_series,
                cost_us_per_million=cost,
            ),
        )

    return [
        FunctionDefinition(
            name="GENERATE_SERIES",
            aliases=(),
            category="utility",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Build an array of evenly spaced integers.",
            documentation=_gs_docs,
            overloads=(
                _generate_series_overload("GENERATE_SERIES_1", (_gs_stop,), 120.0),
                _generate_series_overload("GENERATE_SERIES_2", (_gs_start, _gs_stop), 130.0),
                _generate_series_overload(
                    "GENERATE_SERIES_3", (_gs_start, _gs_stop, _gs_step), 140.0),
            ),
        ),
        _make(
            "JSONB_OBJECT_KEYS",
            other_functions.jsonb_object_keys,
            _CT_ARRAY(_CT_VARIANT),
            # "string", not "any": it parses its argument as a JSON document, so
            # a non-string never worked — it failed inside the native engine as
            # `ExprMultiProjectOperator: error code 1`, which names nothing the
            # caller can act on. JSON documents arrive as VARCHAR/VARBINARY.
            (
                ParameterSpec(
                    name="json",
                    type_family="string",
                    # And it must PARSE as JSON. `string` alone made every VARCHAR
                    # a legal argument, and most of them fail at execution with a
                    # raw `jsonb_object_keys: invalid JSON`.
                    value_format="json",
                    documentation=(
                        "Must be text that parses as a JSON object; other input is "
                        "rejected at execution."
                    ),
                ),
            ),
            cost=590.21,
            summary="Extract keys from JSON object.",
        ),
        _make(
            "HUMANIZE",
            other_functions.humanize,
            _CT_VARCHAR,
            # `mode` names the scale system to render into — a CLOSED set
            # ('words' | 'compact' | 'bytes' | 'si' | 'time' | 'clock' |
            # 'percent' | 'odds'), consumed at bind time into the kernel's
            # binary_op_ctx.op_code and never pushed as an operand. The
            # authoritative spelling table is _HUMANIZE_MODES in
            # compiled_expression.pyx, which is also what rejects an unknown
            # mode — at PLAN time, before a row is touched.
            (
                ParameterSpec(name="val", type_family="numeric"),
                ParameterSpec(
                    name="mode",
                    type_family="string",
                    optional=True,
                    constant_only=True,
                    domain=(
                        "words", "compact", "bytes", "si",
                        "time", "clock", "percent", "odds",
                    ),
                    documentation=(
                        "Scale system to render into: 'words' (default), 'compact', "
                        "'bytes', 'si', 'time', 'clock', 'percent' or 'odds'."
                    ),
                ),
            ),
            cost=107778.29,
            summary="Format number in human-readable form.",
        ),
        FunctionDefinition(
            "COSINE_SIMILARITY",
            aliases=(),
            category="array",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Cosine similarity between the embeddings of two texts.",
            documentation="Embeds both texts with the active embedding provider and returns their cosine similarity.",
            overloads=(
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
                        callable_ref=None,   # c-native: draken_cosine_similarity_text
                        cost_us_per_million=893647.14,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            "APPROX_COSINE_DISTANCE",
            aliases=(),
            category="array",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Cosine distance, ranked through the column's vector index.",
            documentation=(
                "Valid only as the sole ORDER BY key of a query over one table with a LIMIT, "
                "on a text column with a vector index: the index decides which rows are "
                "candidates (approximate), and each candidate's distance is the EXACT cosine "
                "distance between the embeddings, as COSINE_DISTANCE returns it. Recall is "
                "set with the `nprobe` variable (default: the index's own)."
            ),
            overloads=(
                FunctionOverload(
                    id="APPROX_COSINE_DISTANCE_TEXT",
                    parameters=(
                        ParameterSpec(name="arr", type_family="string"),
                        ParameterSpec(name="vec", type_family="string"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_cosine_distance_text (aliased)
                        cost_us_per_million=884934.76,
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
            summary="Cosine distance between the embeddings of two texts.",
            documentation="Embeds both texts with the active embedding provider and returns 1 - cosine similarity.",
            overloads=(
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
                        callable_ref=None,   # c-native: draken_cosine_distance_text
                        cost_us_per_million=884934.76,
                    ),
                ),
            ),
        ),
    ]
