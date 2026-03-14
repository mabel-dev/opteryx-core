# cython: language_level=3

cdef int AGG_COUNT_STAR
cdef int AGG_COUNT
cdef int AGG_SUM
cdef int AGG_MIN
cdef int AGG_MAX
cdef int AGG_AVG
cdef int AGG_COUNT_DISTINCT
cdef int AGG_HASH_ONE
cdef int AGG_APPROX_COUNT_DISTINCT
cdef int AGG_APPROX_PERCENTILE

cdef object new_state(int function_code, object options)
cdef object update_state(int function_code, object state, object value)
cdef object finalize_state(int function_code, object state)
