# cython: language_level=3
# Cython shim for draken.interop.vector_sequence — E.24 vtable bridge.
# cpdef so it can be cimported at C speed by _operators.pyx.

from draken.core.buffers cimport DrakenType


cpdef object vector_from_sequence(object data, object dtype=None):
    from draken.draken_native import (
        vector_from_sequence as _nb_int64,
        vector_from_string_sequence as _nb_varchar,
        vector_from_bool_sequence as _nb_bool,
        vector_float64_from_sequence as _nb_float64,
        vector_timestamp_from_sequence as _nb_timestamp,
        vector_date32_from_sequence as _nb_date32,
    )
    if dtype is None:
        for item in data:
            if item is None:
                continue
            if isinstance(item, bool):
                return _nb_bool(data)
            if isinstance(item, float):
                return _nb_float64(data)
            if isinstance(item, str):
                return _nb_varchar(data)
            if isinstance(item, bytes):
                return _nb_varchar([v.decode("utf-8") if isinstance(v, bytes) else v for v in data])
            break
        return _nb_int64(data)
    # OrsoTypes extends str so dtype.value is the string name
    type_name = dtype.value
    if type_name in ("VARCHAR", "BLOB", "NVARCHAR"):
        return _nb_varchar(data)
    if type_name == "BOOLEAN":
        return _nb_bool(data)
    if type_name in ("DOUBLE", "FLOAT"):
        return _nb_float64(data)
    if type_name == "TIMESTAMP":
        return _nb_timestamp(data, "us")
    if type_name == "DATE":
        return _nb_date32(data)
    return _nb_int64(data)
