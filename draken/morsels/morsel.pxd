# Morsel cdef-class stub for Cython consumers.
#
# Morsel is a nanobind-wrapped C++ class in draken_native.so.
# Declaring it as a cdef class here allows Cython consumers to type
# Morsel arguments and access the ptr attribute at C speed.
# NOTE: cdef methods are NOT declared here to avoid __pyx_vtable__ requirement
# on the nanobind class at runtime. Cdef method dispatch is deferred to E.21b.

from libc.stdint cimport int32_t

cdef class Morsel:
    cdef object _nb
    cdef public list _col_names
    cdef public list _columns


cpdef Morsel align_tables(Morsel left_morsel, Morsel right_morsel,
                           int32_t[::1] left_view, int32_t[::1] right_view)
