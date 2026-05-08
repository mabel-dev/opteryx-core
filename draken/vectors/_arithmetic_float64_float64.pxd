from draken.vectors.float64_vector cimport Float64Vector

cdef Float64Vector _float64_scalar_float64_add_dense(double scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _float64_float64_add_scalar_dense(Float64Vector left, double scalar, size_t length) except *
cdef Float64Vector _float64_scalar_float64_subtract_dense(double scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _float64_float64_subtract_scalar_dense(Float64Vector left, double scalar, size_t length) except *
cdef Float64Vector _float64_scalar_float64_multiply_dense(double scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _float64_float64_multiply_scalar_dense(Float64Vector left, double scalar, size_t length) except *
cdef Float64Vector _float64_scalar_float64_divide_dense(double scalar, Float64Vector right, size_t length) except *
cdef Float64Vector _float64_float64_divide_scalar_dense(Float64Vector left, double scalar, size_t length) except *
cdef Float64Vector _float64_float64_add_dense(Float64Vector left, Float64Vector right, size_t length) except *
cdef Float64Vector _float64_float64_subtract_dense(Float64Vector left, Float64Vector right, size_t length) except *
cdef Float64Vector _float64_float64_multiply_dense(Float64Vector left, Float64Vector right, size_t length) except *
cdef Float64Vector _float64_float64_divide_dense(Float64Vector left, Float64Vector right, size_t length) except *
