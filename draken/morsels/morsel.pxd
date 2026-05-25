# Morsel cdef-class stub for Cython consumers.
#
# Morsel is a nanobind-wrapped C++ class in draken_native.so.
# Declaring it as a cdef class here allows Cython consumers to type
# Morsel arguments and access the ptr attribute at C speed.
# NOTE: cdef methods are NOT declared here to avoid __pyx_vtable__ requirement
# on the nanobind class at runtime. Cdef method dispatch is deferred to E.21b.

cdef class Morsel:
    cdef object _nb
    cdef public list _col_names
    cdef public list _columns
