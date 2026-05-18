from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.float64_vector cimport Float64Vector
from libc.stdint cimport int64_t

cdef Integer64Vector _int64_int64_add_dense(Integer64Vector left, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_subtract_dense(Integer64Vector left, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_multiply_dense(Integer64Vector left, Integer64Vector right, size_t length) except *
cdef Float64Vector _int64_int64_divide_dense(Integer64Vector left, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_floordiv_dense(Integer64Vector left, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_modulo_dense(Integer64Vector left, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_scalar_int64_add_dense(int64_t scalar, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_add_scalar_dense(Integer64Vector left, int64_t scalar, size_t length) except *
cdef Integer64Vector _int64_scalar_int64_subtract_dense(int64_t scalar, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_subtract_scalar_dense(Integer64Vector left, int64_t scalar, size_t length) except *
cdef Integer64Vector _int64_scalar_int64_multiply_dense(int64_t scalar, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_multiply_scalar_dense(Integer64Vector left, int64_t scalar, size_t length) except *
cdef Float64Vector _int64_scalar_int64_divide_dense(int64_t scalar, Integer64Vector right, size_t length) except *
cdef Float64Vector _int64_int64_divide_scalar_dense(Integer64Vector left, int64_t scalar, size_t length) except *
cdef Integer64Vector _int64_scalar_int64_floordiv_dense(int64_t scalar, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_floordiv_scalar_dense(Integer64Vector left, int64_t scalar, size_t length) except *
cdef Integer64Vector _int64_scalar_int64_modulo_dense(int64_t scalar, Integer64Vector right, size_t length) except *
cdef Integer64Vector _int64_int64_modulo_scalar_dense(Integer64Vector left, int64_t scalar, size_t length) except *
