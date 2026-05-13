# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.math cimport log as c_log


def _as_list(object value):
    if hasattr(value, "to_pylist"):
        return value.to_pylist()
    try:
        return list(value)
    except TypeError:
        return [value]


cpdef list vector_log(object values, object bases):
    """
    Compute logarithms element-wise as log(values) / log(bases) using libc.

    Parameters may be Python sequences or scalars.
    The return value is a Python list.
    """
    cdef list value_list = _as_list(values)
    cdef list base_list = _as_list(bases)
    cdef Py_ssize_t value_count = len(value_list)
    cdef Py_ssize_t base_count = len(base_list)
    cdef Py_ssize_t result_count
    cdef Py_ssize_t i
    cdef bint value_scalar
    cdef bint base_scalar
    cdef object value_obj
    cdef object base_obj
    cdef double value_num
    cdef double base_num
    cdef list result

    if value_count == base_count:
        result_count = value_count
        value_scalar = False
        base_scalar = False
    elif value_count == 1:
        result_count = base_count
        value_scalar = True
        base_scalar = False
    elif base_count == 1:
        result_count = value_count
        value_scalar = False
        base_scalar = True
    else:
        raise ValueError("LOG arguments must have matching lengths or be scalar-broadcastable.")

    result = [None] * result_count

    for i in range(result_count):
        value_obj = value_list[0] if value_scalar else value_list[i]
        base_obj = base_list[0] if base_scalar else base_list[i]

        if value_obj is None or base_obj is None:
            continue

        value_num = float(value_obj)
        base_num = float(base_obj)
        result[i] = c_log(value_num) / c_log(base_num)

    return result
