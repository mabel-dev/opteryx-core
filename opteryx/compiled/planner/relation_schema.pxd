# cython: language_level=3
# distutils: language = c++

# A bound relation's schema - declared here so compiled planner code can read its
# fields in C (opteryx/compiled/planner/relation_schema.pyx).

from libc.stdint cimport int64_t


cdef class LazyColumns:
    cdef object table                  # the query's ColumnTable
    cdef str alias
    cdef tuple descriptor              # the source's descriptor columns, in order
    cdef list minted                   # SchemaColumn or None, per descriptor position
    cdef dict exact                    # name -> first position
    cdef dict lower                    # lowercased name -> first position
    cdef Py_ssize_t pending            # positions not yet minted
    cdef bint frozen                   # binding is over: minting again is a bug
    cdef object mint(self, Py_ssize_t position)
    cdef tuple all(self)


cdef class RelationSchema:
    cdef readonly str name
    cdef tuple _columns
    cdef LazyColumns _lazy             # not None: columns are minted on first use
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
    cdef void _force(self) except *
    cpdef object find_lower(self, str lowered)
    cdef bint shares_columns(self, RelationSchema other)
