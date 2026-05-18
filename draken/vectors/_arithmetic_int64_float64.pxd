from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.float64_vector cimport Float64Vector
from libc.stdint cimport int64_t

cdef Float64Vector _int64_float64_add_dense(Integer64Vector left, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_float64_subtract_dense(Integer64Vector left, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_float64_multiply_dense(Integer64Vector left, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_float64_divide_dense(Integer64Vector left, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_float64_floordiv_dense(Integer64Vector left, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_scalar_float64_add_dense(int64_t scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_scalar_float64_subtract_dense(int64_t scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_scalar_float64_multiply_dense(int64_t scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_scalar_float64_divide_dense(int64_t scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_scalar_float64_floordiv_dense(int64_t scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _int64_float64_scalar_subtract_dense(Integer64Vector left, double scalar, size_t length) except *
cdef Float64Vector _int64_float64_scalar_multiply_dense(Integer64Vector left, double scalar, size_t length) except *
cdef Float64Vector _int64_float64_scalar_divide_dense(Integer64Vector left, double scalar, size_t length) except *
cdef Float64Vector _int64_float64_scalar_floordiv_dense(Integer64Vector left, double scalar, size_t length) except *
cdef Float64Vector _int64_float64_scalar_add_dense(Integer64Vector left, double scalar, size_t length) except *
