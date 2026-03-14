# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

from opteryx.compiled.aggregations.approximate_count import ApproximateCountState
from opteryx.compiled.aggregations.approximate_median import ApproximatePercentileState

cdef int AGG_COUNT_STAR = 1
cdef int AGG_COUNT = 2
cdef int AGG_SUM = 3
cdef int AGG_MIN = 4
cdef int AGG_MAX = 5
cdef int AGG_AVG = 6
cdef int AGG_COUNT_DISTINCT = 7
cdef int AGG_HASH_ONE = 8
cdef int AGG_APPROX_COUNT_DISTINCT = 9
cdef int AGG_APPROX_PERCENTILE = 10

cdef object _UNSET = object()


cdef inline object new_state(int function_code, object options):
    if function_code == AGG_COUNT_STAR or function_code == AGG_COUNT:
        return 0
    if function_code == AGG_SUM or function_code == AGG_MIN or function_code == AGG_MAX:
        return None
    if function_code == AGG_AVG:
        return [0, 0]
    if function_code == AGG_COUNT_DISTINCT:
        return set()
    if function_code == AGG_HASH_ONE:
        return _UNSET
    if function_code == AGG_APPROX_COUNT_DISTINCT:
        return ApproximateCountState()
    if function_code == AGG_APPROX_PERCENTILE:
        if options is None:
            options = 0.5
        return ApproximatePercentileState(float(options))
    raise ValueError(f"unsupported aggregation code '{function_code}'")


cdef inline object update_state(int function_code, object state, object value):
    if function_code == AGG_COUNT_STAR:
        return state + 1

    if function_code == AGG_COUNT:
        return state + 1 if value is not None else state

    if function_code == AGG_SUM:
        if value is None:
            return state
        return value if state is None else state + value

    if function_code == AGG_MIN:
        if value is None:
            return state
        if state is None:
            return value
        return value if value < state else state

    if function_code == AGG_MAX:
        if value is None:
            return state
        if state is None:
            return value
        return value if value > state else state

    if function_code == AGG_AVG:
        if value is None:
            return state
        state[0] += value
        state[1] += 1
        return state

    if function_code == AGG_COUNT_DISTINCT:
        if value is None:
            return state
        state.add(value)
        return state

    if function_code == AGG_HASH_ONE:
        if state is _UNSET and value is not None:
            return value
        return state

    if function_code == AGG_APPROX_COUNT_DISTINCT:
        if value is not None:
            state.add_value(value)
        return state

    if function_code == AGG_APPROX_PERCENTILE:
        if value is not None:
            state.add_value(value)
        return state

    raise ValueError(f"unsupported aggregation code '{function_code}'")


cdef inline object finalize_state(int function_code, object state):
    if function_code == AGG_COUNT_STAR or function_code == AGG_COUNT:
        return state
    if function_code == AGG_SUM or function_code == AGG_MIN or function_code == AGG_MAX:
        return state
    if function_code == AGG_AVG:
        return None if state[1] == 0 else state[0] / state[1]
    if function_code == AGG_COUNT_DISTINCT:
        return len(state)
    if function_code == AGG_HASH_ONE:
        return None if state is _UNSET else state
    if function_code == AGG_APPROX_COUNT_DISTINCT:
        return state.estimate()
    if function_code == AGG_APPROX_PERCENTILE:
        return state.quantile()
    raise ValueError(f"unsupported aggregation code '{function_code}'")
