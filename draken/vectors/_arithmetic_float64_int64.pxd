from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.float64_vector cimport Float64Vector
from libc.stdint cimport int64_t

cdef Float64Vector _float64_int64_scalar_add_dense(Float64Vector left, int64_t scalar, size_t length) except *
cdef Float64Vector _float64_int64_scalar_subtract_dense(Float64Vector left, int64_t scalar, size_t length) except *
cdef Float64Vector _float64_int64_scalar_multiply_dense(Float64Vector left, int64_t scalar, size_t length) except *
cdef Float64Vector _float64_int64_scalar_divide_dense(Float64Vector left, int64_t scalar, size_t length) except *
cdef Float64Vector _float64_int64_scalar_floordiv_dense(Float64Vector left, int64_t scalar, size_t length) except *
cdef Float64Vector _float64_scalar_int64_add_dense(double scalar, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_scalar_int64_subtract_dense(double scalar, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_scalar_int64_multiply_dense(double scalar, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_scalar_int64_divide_dense(double scalar, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_scalar_int64_floordiv_dense(double scalar, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_int64_add_dense(Float64Vector left, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_int64_subtract_dense(Float64Vector left, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_int64_multiply_dense(Float64Vector left, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_int64_divide_dense(Float64Vector left, Integer64Vector right, size_t length) except *
cdef Float64Vector _float64_int64_floordiv_dense(Float64Vector left, Integer64Vector right, size_t length) except *
