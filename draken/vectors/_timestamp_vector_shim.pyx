# cython: language_level=3
# Cython shim for draken.vectors.timestamp_vector — E.24 vtable bridge.

from draken.vectors.vector cimport Vector


cdef class TimestampVector(Vector):
    @classmethod
    def from_constant(cls, value, length, is_null=False, timestamp_unit="us"):
        import datetime
        from draken.draken_native import vector_timestamp_from_constant
        if is_null:
            return cls(vector_timestamp_from_constant(None, length, timestamp_unit))
        val = int(value)
        if timestamp_unit == "s":
            dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=val)
        elif timestamp_unit == "ms":
            dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=val)
        elif timestamp_unit == "ns":
            dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=val // 1000)
        else:
            dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=val)
        return cls(vector_timestamp_from_constant(dt, length, timestamp_unit))


cpdef TimestampVector from_int64_vector(object vec, str timestamp_unit="us"):
    """Convert an Integer64Vector (microsecond-epoch ints) to a TimestampVector."""
    import datetime
    from draken.draken_native import vector_timestamp_from_sequence
    cdef object epoch = datetime.datetime(1970, 1, 1)
    from draken.vectors.vector import Vector as _SV
    cdef list values = (vec._nb if isinstance(vec, _SV) else vec).to_pylist()
    cdef list dts = []
    cdef object v
    for v in values:
        if v is None:
            dts.append(None)
        elif timestamp_unit == "s":
            dts.append(epoch + datetime.timedelta(seconds=int(v)))
        elif timestamp_unit == "ms":
            dts.append(epoch + datetime.timedelta(milliseconds=int(v)))
        elif timestamp_unit == "ns":
            dts.append(epoch + datetime.timedelta(microseconds=int(v) // 1000))
        else:
            dts.append(epoch + datetime.timedelta(microseconds=int(v)))
    return TimestampVector(vector_timestamp_from_sequence(dts, timestamp_unit))
