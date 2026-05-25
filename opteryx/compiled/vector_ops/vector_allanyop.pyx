# cython: language_level=3
"""ANY / ALL predicate operations on array-typed columns.

These implement SQL `= ANY(array_col)`, `= ALL(array_col)` etc.  Not yet
ported to the new DrakenVector API; calling any of these raises
NotImplementedError (fail-loud, not silent degradation).
"""


cpdef object vector_anyop_eq(object literal=None, object column=None):
    raise NotImplementedError("vector_anyop_eq not yet ported to DrakenVector API")


cpdef object vector_anyop_neq(object literal=None, object column=None):
    raise NotImplementedError("vector_anyop_neq not yet ported to DrakenVector API")


cpdef object vector_anyop_gt(object left, object right):
    raise NotImplementedError("vector_anyop_gt not yet ported to DrakenVector API")


cpdef object vector_anyop_lt(object left, object right):
    raise NotImplementedError("vector_anyop_lt not yet ported to DrakenVector API")


cpdef object vector_anyop_gte(object left, object right):
    raise NotImplementedError("vector_anyop_gte not yet ported to DrakenVector API")


cpdef object vector_anyop_lte(object left, object right):
    raise NotImplementedError("vector_anyop_lte not yet ported to DrakenVector API")


cpdef object vector_allop_eq(object left, object right):
    raise NotImplementedError("vector_allop_eq not yet ported to DrakenVector API")


cpdef object vector_allop_neq(object left, object right):
    raise NotImplementedError("vector_allop_neq not yet ported to DrakenVector API")


cpdef object vector_contains(object left, object right):
    raise NotImplementedError("vector_contains not yet ported to DrakenVector API")
