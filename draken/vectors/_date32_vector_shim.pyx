# cython: language_level=3
# Cython shim for draken.vectors.date32_vector — E.24 vtable bridge.

from draken.vectors.vector cimport Vector


cdef class Date32Vector(Vector):
    @classmethod
    def from_constant(cls, value, length, is_null=False):
        import datetime
        from draken.draken_native import vector_date32_from_constant
        if is_null:
            return cls(vector_date32_from_constant(None, length))
        epoch = datetime.date(1970, 1, 1)
        d = epoch + datetime.timedelta(days=int(value))
        return cls(vector_date32_from_constant(d, length))


cpdef Date32Vector from_int64_vector(object vec):
    """Convert an Integer64Vector (days-since-epoch ints) to a Date32Vector."""
    import datetime
    from draken.draken_native import vector_date32_from_sequence
    from draken.vectors.vector import Vector as _SV
    cdef object epoch = datetime.date(1970, 1, 1)
    cdef list values = (vec._nb if isinstance(vec, _SV) else vec).to_pylist()
    cdef list dates = []
    cdef object v
    for v in values:
        if v is None:
            dates.append(None)
        else:
            dates.append(epoch + datetime.timedelta(days=int(v)))
    return Date32Vector(vector_date32_from_sequence(dates))
