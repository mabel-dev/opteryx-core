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
from draken.core.buffers cimport (
    DrakenType,
    DRAKEN_INT8,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_FLOAT64,
    DRAKEN_VARCHAR,
    DRAKEN_NVARCHAR,
    DRAKEN_VARBINARY,
    DRAKEN_DECIMAL,
    DRAKEN_DECIMAL128,
    DRAKEN_TIMESTAMP64,
)


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


cdef class _DeferredAvgCollector(BaseCollector):
    """Resolves to AvgCollector (int/float) or AvgDecimalCollector on first accumulate."""
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
            c = _DeferredAvgCollector()
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
    cdef DrakenType t
    cdef str fn_tag

    for i in range(len(collectors)):
        c = <BaseCollector>collectors[i]

        # SUM/MIN/MAX/AVG over a DECIMAL128 input are handled below (int128 collectors).
        # ANY_VALUE / MEDIAN over DECIMAL128 are not yet wired (they would read raw
        # int64/float → garbage), so fail loud for those two.
        if isinstance(c, (_DeferredAnyValueCollector, _DeferredMedianCollector)):
            vec = morsel.column(c.column_name)
            if vec.unified().type == DRAKEN_DECIMAL128:
                raise NotImplementedError(
                    "ANY_VALUE / MEDIAN over a DECIMAL128 (precision > 18) input is "
                    "not yet supported; CAST to DECIMAL(18, s) or DOUBLE."
                )

        if isinstance(c, _DeferredSumCollector):
            vec = morsel.column(c.column_name)
            t = vec.unified().type
            if t == DRAKEN_INT64 or t == DRAKEN_INT8 or t == DRAKEN_INT16 or t == DRAKEN_INT32:
                # Narrow ints sum into an int64 accumulator (width-aware read),
                # matching the scalar SUM path which widens INT8/16/32 → INT64.
                typed_c = SumInt64Collector()
            elif t == DRAKEN_DECIMAL:
                typed_c = SumDecimalCollector()
                (<SumDecimalCollector>typed_c)._scale = vec._nb.logical_type_scale
            elif t == DRAKEN_DECIMAL128:
                typed_c = SumDecimal128Collector()
                (<SumDecimal128Collector>typed_c)._scale = vec._nb.logical_type_scale
                (<SumDecimal128Collector>typed_c)._precision = vec._nb.logical_type_precision
            else:
                typed_c = SumFloat64Collector()
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredMinCollector):
            vec = morsel.column(c.column_name)
            t = vec.unified().type
            if t == DRAKEN_INT64 or t == DRAKEN_INT8 or t == DRAKEN_INT16 or t == DRAKEN_INT32:
                typed_c = MinMaxInt64Collector()
                (<MinMaxInt64Collector>typed_c)._direction = 1
            elif t == DRAKEN_FLOAT64:
                typed_c = MinMaxFloat64Collector()
                (<MinMaxFloat64Collector>typed_c)._direction = 1
            elif t == DRAKEN_DECIMAL:
                typed_c = MinMaxDecimalCollector()
                (<MinMaxDecimalCollector>typed_c)._direction = 1
                (<MinMaxDecimalCollector>typed_c)._scale = vec._nb.logical_type_scale
                (<MinMaxDecimalCollector>typed_c)._precision = vec._nb.logical_type_precision
            elif t == DRAKEN_DECIMAL128:
                typed_c = MinMaxDecimal128Collector()
                (<MinMaxDecimal128Collector>typed_c)._direction = 1
                (<MinMaxDecimal128Collector>typed_c)._scale = vec._nb.logical_type_scale
                (<MinMaxDecimal128Collector>typed_c)._precision = vec._nb.logical_type_precision
            elif t == DRAKEN_VARCHAR or t == DRAKEN_NVARCHAR or t == DRAKEN_VARBINARY:
                typed_c = MinMaxVarcharCollector()
                (<MinMaxVarcharCollector>typed_c)._direction = 1
            else:
                typed_c = MinMaxObjectCollector()
                (<MinMaxObjectCollector>typed_c)._direction = 1
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredMaxCollector):
            vec = morsel.column(c.column_name)
            t = vec.unified().type
            if t == DRAKEN_INT64 or t == DRAKEN_INT8 or t == DRAKEN_INT16 or t == DRAKEN_INT32:
                typed_c = MinMaxInt64Collector()
                (<MinMaxInt64Collector>typed_c)._direction = -1
            elif t == DRAKEN_FLOAT64:
                typed_c = MinMaxFloat64Collector()
                (<MinMaxFloat64Collector>typed_c)._direction = -1
            elif t == DRAKEN_DECIMAL:
                typed_c = MinMaxDecimalCollector()
                (<MinMaxDecimalCollector>typed_c)._direction = -1
                (<MinMaxDecimalCollector>typed_c)._scale = vec._nb.logical_type_scale
                (<MinMaxDecimalCollector>typed_c)._precision = vec._nb.logical_type_precision
            elif t == DRAKEN_DECIMAL128:
                typed_c = MinMaxDecimal128Collector()
                (<MinMaxDecimal128Collector>typed_c)._direction = -1
                (<MinMaxDecimal128Collector>typed_c)._scale = vec._nb.logical_type_scale
                (<MinMaxDecimal128Collector>typed_c)._precision = vec._nb.logical_type_precision
            elif t == DRAKEN_VARCHAR or t == DRAKEN_NVARCHAR or t == DRAKEN_VARBINARY:
                typed_c = MinMaxVarcharCollector()
                (<MinMaxVarcharCollector>typed_c)._direction = -1
            else:
                typed_c = MinMaxObjectCollector()
                (<MinMaxObjectCollector>typed_c)._direction = -1
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredAnyValueCollector):
            vec = morsel.column(c.column_name)
            t = vec.unified().type
            if t == DRAKEN_INT64:
                typed_c = AnyValueInt64Collector()
            elif t == DRAKEN_FLOAT64:
                typed_c = AnyValueFloat64Collector()
            else:
                typed_c = AnyValueObjectCollector()
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredAvgCollector):
            vec = morsel.column(c.column_name)
            t = vec.unified().type
            if t == DRAKEN_DECIMAL:
                # Exact int64 sum, double divide (AVG is DOUBLE). The generic
                # AvgCollector's decimal path lost precision (per-row float sum).
                typed_c = AvgDecimalCollector()
                (<AvgDecimalCollector>typed_c)._scale = vec._nb.logical_type_scale
            elif t == DRAKEN_DECIMAL128:
                typed_c = AvgDecimal128Collector()
                (<AvgDecimal128Collector>typed_c)._scale = vec._nb.logical_type_scale
            else:
                typed_c = AvgCollector()
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

        elif isinstance(c, _DeferredMedianCollector):
            vec = morsel.column(c.column_name)
            t = vec.unified().type
            if t == DRAKEN_DECIMAL:
                raise NotImplementedError(
                    "MEDIAN does not support DECIMAL inputs; CAST the column "
                    "to DOUBLE first (e.g. MEDIAN(CAST(col AS DOUBLE)))."
                )
            # Narrow-int vectors fall through to MedianFloat64Collector,
            # which has a to_pylist fallback for them.
            typed_c = MedianFloat64Collector()
            typed_c.column_name = c.column_name
            typed_c.result_name = c.result_name
            collectors[i] = typed_c

    # Resolve key kinds from actual column types
    cdef Py_ssize_t ki
    for ki in range(len(group_columns)):
        col_name = group_columns[ki]
        vec = morsel.column(col_name)
        t = vec.unified().type
        if t == DRAKEN_VARCHAR or t == DRAKEN_NVARCHAR or t == DRAKEN_VARBINARY:
            key_kinds[ki] = KEY_MULTI_ENCODED_STRING
        elif t == DRAKEN_TIMESTAMP64:
            # 8-byte timestamp stores cleanly in the int64 key buffer; the unit
            # is captured by the engine and reapplied at reconstruct so the group
            # key emerges as TIMESTAMP, not raw int64 epoch.
            key_kinds[ki] = KEY_MULTI_FIXED_TIMESTAMP64
        elif t == DRAKEN_FLOAT64:
            # 8-byte double stores cleanly as raw bits in the int64 key buffer;
            # reconstruct re-tags the column FLOAT64 so the key emerges as a
            # double, not the raw IEEE-754 bits surfaced as a giant integer.
            key_kinds[ki] = KEY_MULTI_FIXED_FLOAT64
        elif t == DRAKEN_DECIMAL128:
            # 16-byte int128 decimal key. The key store holds the raw int128 unscaled
            # value in 16-byte slots and reapplies the (precision, scale) descriptor at
            # reconstruct (set below). Grouping is hash-only and hash_shaped is
            # cross-tier-consistent, so a DECIMAL128 key collides correctly with the
            # int64-decimal of the same value.
            key_kinds[ki] = KEY_MULTI_FIXED_DECIMAL128
        else:
            key_kinds[ki] = KEY_MULTI_FIXED_INT
