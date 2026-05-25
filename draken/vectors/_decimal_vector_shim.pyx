# cython: language_level=3
# Cython shim for draken.vectors.decimal_vector — E.24 vtable bridge.

from draken.vectors.vector cimport Vector


cdef class DecimalVector(Vector):
    @classmethod
    def from_constant(cls, value, length, is_null=False):
        from draken.draken_native import vector_from_sequence
        if is_null:
            return cls(vector_from_sequence([None] * length))
        return cls(vector_from_sequence([int(value)] * length))


cpdef DecimalVector from_int64_vector(object vec, int precision=0, int scale=0):
    """Convert an Integer64Vector (unscaled ints) to a DecimalVector."""
    from draken.draken_native import vector_decimal_from_sequence
    from draken.vectors.vector import Vector as _SV
    from decimal import Decimal
    cdef int p = precision if precision > 0 else 18
    cdef int s = scale if scale >= 0 else 0
    cdef list values = (vec._nb if isinstance(vec, _SV) else vec).to_pylist()
    cdef list decimals = []
    cdef object v
    for v in values:
        if v is None:
            decimals.append(None)
        else:
            raw = int(v)
            if s > 0:
                decimals.append(Decimal(raw) / Decimal(10 ** s))
            else:
                decimals.append(Decimal(raw))
    return DecimalVector(vector_decimal_from_sequence(decimals, p, s))
