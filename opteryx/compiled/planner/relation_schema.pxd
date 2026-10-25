# cython: language_level=3
# distutils: language = c++

# A bound relation's schema - declared here so compiled planner code can read its
# fields in C (opteryx/compiled/planner/relation_schema.pyx).

from libc.stdint cimport int64_t


cdef class RelationSchema:
    cdef readonly str name
    cdef readonly tuple columns
    cdef readonly tuple aliases
    cdef readonly str primary_key
    cdef int64_t _row_count_metric
    cdef int64_t _row_count_estimate
    cdef int64_t _data_size_metric
    cdef int64_t _data_size_estimate
    cdef unsigned char _present        # bit per optional count: set = known
    cdef dict _lowercase                # lowercased name/alias -> column; None until first use

    cdef RelationSchema _clone(self)
    cdef dict _lowercase_index(self)
