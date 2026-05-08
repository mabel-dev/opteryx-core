from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.float64_vector cimport Float64Vector
from libc.stdint cimport int64_t

cdef Int64Vector _int64_int64_add_dense(Int64Vector left, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_subtract_dense(Int64Vector left, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_multiply_dense(Int64Vector left, Int64Vector right, size_t length) except *
cdef Float64Vector _int64_int64_divide_dense(Int64Vector left, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_floordiv_dense(Int64Vector left, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_modulo_dense(Int64Vector left, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_scalar_int64_add_dense(int64_t scalar, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_add_scalar_dense(Int64Vector left, int64_t scalar, size_t length) except *
cdef Int64Vector _int64_scalar_int64_subtract_dense(int64_t scalar, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_subtract_scalar_dense(Int64Vector left, int64_t scalar, size_t length) except *
cdef Int64Vector _int64_scalar_int64_multiply_dense(int64_t scalar, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_multiply_scalar_dense(Int64Vector left, int64_t scalar, size_t length) except *
cdef Float64Vector _int64_scalar_int64_divide_dense(int64_t scalar, Int64Vector right, size_t length) except *
cdef Float64Vector _int64_int64_divide_scalar_dense(Int64Vector left, int64_t scalar, size_t length) except *
cdef Int64Vector _int64_scalar_int64_floordiv_dense(int64_t scalar, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_floordiv_scalar_dense(Int64Vector left, int64_t scalar, size_t length) except *
cdef Int64Vector _int64_scalar_int64_modulo_dense(int64_t scalar, Int64Vector right, size_t length) except *
cdef Int64Vector _int64_int64_modulo_scalar_dense(Int64Vector left, int64_t scalar, size_t length) except *
