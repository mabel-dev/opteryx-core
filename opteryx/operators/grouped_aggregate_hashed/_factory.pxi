# cython: language_level=3

# create_collectors() — maps AggregationSpec list → list[BaseCollector]
# Also infers key_kinds from group-by column names and the first morsel.
#
# Type dispatch for SUM/MIN/MAX/AVG: we cannot know the column type until
# the first morsel arrives. The engine calls _init_collectors_from_morsel()
# on first ingest to swap generic collectors for typed ones.
#
# key_kinds inference similarly deferred to first morsel.

from libc.stdint cimport int64_t

from draken.vectors.vector cimport Vector
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.string_vector cimport StringVector
from draken.vectors._decimal_vector cimport DecimalVector


# ---------------------------------------------------------------------------
# Deferred-typed collectors for SUM / MIN / MAX
# These check the vector type on first accumulate() and self-replace inside
# the engine's collector list.
# ---------------------------------------------------------------------------

cdef class _DeferredSumCollector(BaseCollector):
    """Resolves to SumInt64Collector or SumFloat64Collector on first accumulate."""
    pass


cdef class _DeferredMinCollector(BaseCollector):
    """Resolves to MinMaxInt64/Float64/ObjectCollector (MIN) on first accumulate."""
    pass


cdef class _DeferredMaxCollector(BaseCollector):
    """Resolves to MinMaxInt64/Float64/ObjectCollector (MAX) on first accumulate."""
    pass


cdef class _DeferredAnyValueCollector(BaseCollector):
    """Resolves to AnyValueInt64/Float64/ObjectCollector on first accumulate."""
    pass


# _DeferredMedianCollector is declared in _collectors_buffered.pxi (it needs
# the MedianFloat64Collector type to resolve to).


# ---------------------------------------------------------------------------
# Factory function
# ---------------------------------------------------------------------------

cpdef tuple create_collectors(list aggregation_specs, list group_columns):
    """
    Build the list of collectors and key_kinds for GroupHashEngine.

    Returns:
        (collectors: list[BaseCollector], key_kinds: list[int])
        key_kinds contains KEY_MULTI_FIXED_INT or KEY_MULTI_ENCODED_STRING
        per group column — populated with KEY_MULTI_FIXED_INT as default;
        actual values are resolved at first-morsel time by the engine.
    """
    cdef list collectors = []
    cdef str fn
    cdef object spec
    cdef bytes col_name, result_name
    cdef BaseCollector c

    for spec in aggregation_specs:
        fn = spec.function.lower()
        col_name = (
            spec.column.encode("utf-8") if isinstance(spec.column, str)
            else spec.column
        ) if spec.column not in (None, "*", b"*") else b"*"
        result_name = (
            spec.alias.encode("utf-8") if isinstance(spec.alias, str)
            else spec.alias
        )

        if fn == "count" and (spec.column is None or spec.column in ("*", b"*")):
            c = CountStarCollector()
        elif fn == "count":
            c = CountValueCollector()
        elif fn == "sum":
            c = _DeferredSumCollector()
        elif fn in ("min",):
            c = _DeferredMinCollector()
        elif fn in ("max",):
            c = _DeferredMaxCollector()
        elif fn in ("avg", "mean"):
            c = AvgCollector()
        elif fn == "count_distinct":
            c = CountDistinctCollector()
        elif fn == "any_value":
            c = _DeferredAnyValueCollector()
        elif fn == "approx_count_distinct":
            c = ApproxCountDistinctCollector()
        elif fn == "approx_percentile":
            percentile = float(spec.options) if spec.options is not None else 0.5
            c = ApproxPercentileCollector(percentile)
        elif fn in ("array_agg", "hash_list"):
            c = ArrayAggCollector(spec.options)
        elif fn == "median":
            c = _DeferredMedianCollector()
        else:
            raise ValueError(f"unsupported aggregation function: {fn!r}")

        c.column_name = col_name
        c.result_name = result_name
        collectors.append(c)

    # Key kinds: default to KEY_MULTI_FIXED_INT; resolved at first-morsel by engine
    cdef list key_kinds = [KEY_MULTI_FIXED_INT] * len(group_columns)

    return collectors, key_kinds


cpdef void resolve_deferred_collectors(
    list collectors,
    object morsel,
    list group_columns,
    list key_kinds,
):
    """
    Called once on the first non-empty morsel to:
      1. Replace deferred collectors with typed ones.
      2. Fill in key_kinds based on actual column types.
    """
    cdef Py_ssize_t i
    cdef BaseCollector c, typed_c
    cdef Vector vec
    cdef str fn_tag

    for i in range(len(collectors)):
        c = <BaseCollector>collectors[i]

        if isinstance(c, _DeferredSumCollector):
            vec = morsel.column(c.column_name)
            if isinstance(vec, Integer64Vector):
                typed_c = SumInt64Collector()
            elif isinstance(vec, DecimalVector):
                typed_c = SumDecimalCollector()
                (<SumDecimalCollector>typed_c)._factor = 10.0 ** (-(<DecimalVector>vec)._scale)
            else:
                typed_c = SumFloat64Collector()
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredMinCollector):
            vec = morsel.column(c.column_name)
            if isinstance(vec, Integer64Vector):
                typed_c = MinMaxInt64Collector()
                (<MinMaxInt64Collector>typed_c)._direction = 1
            elif isinstance(vec, Float64Vector):
                typed_c = MinMaxFloat64Collector()
                (<MinMaxFloat64Collector>typed_c)._direction = 1
            elif isinstance(vec, DecimalVector):
                typed_c = MinMaxDecimalCollector()
                (<MinMaxDecimalCollector>typed_c)._direction = 1
                (<MinMaxDecimalCollector>typed_c)._factor = 10.0 ** (-(<DecimalVector>vec)._scale)
            else:
                typed_c = MinMaxObjectCollector()
                (<MinMaxObjectCollector>typed_c)._direction = 1
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredMaxCollector):
            vec = morsel.column(c.column_name)
            if isinstance(vec, Integer64Vector):
                typed_c = MinMaxInt64Collector()
                (<MinMaxInt64Collector>typed_c)._direction = -1
            elif isinstance(vec, Float64Vector):
                typed_c = MinMaxFloat64Collector()
                (<MinMaxFloat64Collector>typed_c)._direction = -1
            elif isinstance(vec, DecimalVector):
                typed_c = MinMaxDecimalCollector()
                (<MinMaxDecimalCollector>typed_c)._direction = -1
                (<MinMaxDecimalCollector>typed_c)._factor = 10.0 ** (-(<DecimalVector>vec)._scale)
            else:
                typed_c = MinMaxObjectCollector()
                (<MinMaxObjectCollector>typed_c)._direction = -1
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredAnyValueCollector):
            vec = morsel.column(c.column_name)
            if isinstance(vec, Integer64Vector):
                typed_c = AnyValueInt64Collector()
            elif isinstance(vec, Float64Vector):
                typed_c = AnyValueFloat64Collector()
            else:
                typed_c = AnyValueObjectCollector()
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredMedianCollector):
            vec = morsel.column(c.column_name)
            if isinstance(vec, DecimalVector):
                raise NotImplementedError(
                    "MEDIAN does not support DECIMAL inputs; CAST the column "
                    "to DOUBLE first (e.g. MEDIAN(CAST(col AS DOUBLE)))."
                )
            if not isinstance(vec, (Integer64Vector, Float64Vector)):
                # Allow integer-narrow vectors through — MedianFloat64Collector
                # has a to_pylist fallback for them. Reject obvious non-numerics.
                pass
            typed_c = MedianFloat64Collector()
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

    # Resolve key kinds from actual column types
    cdef Py_ssize_t ki
    for ki in range(len(group_columns)):
        col_name = group_columns[ki]
        vec = morsel.column(col_name)
        if isinstance(vec, StringVector):
            key_kinds[ki] = KEY_MULTI_ENCODED_STRING
        else:
            key_kinds[ki] = KEY_MULTI_FIXED_INT
