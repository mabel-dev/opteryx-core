# cython: language_level=3

from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal


cdef tuple _SCALAR_TYPES = (
    str,
    bytes,
    int,
    float,
    bool,
    date,
    datetime,
    time,
    Decimal,
)


cdef object _distinct_key(object value):
    cdef object item

    if value is None or isinstance(value, _SCALAR_TYPES):
        return value

    if isinstance(value, tuple):
        return tuple(_distinct_key(item) for item in value)

    if isinstance(value, list):
        return tuple(_distinct_key(item) for item in value)

    if isinstance(value, dict):
        return tuple(
            sorted((key, _distinct_key(item)) for key, item in value.items())
        )

    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


cdef class ArrayAggState:
    cdef list values
    cdef object seen
    cdef bint distinct
    cdef bint ordered
    cdef bint descending
    cdef object limit

    def __cinit__(self, object options=None):
        cdef object distinct
        cdef object ordered
        cdef object descending
        cdef object limit

        if options is None:
            options = {}

        distinct = options.get("distinct", False)
        ordered = options.get("ordered", False)
        descending = options.get("descending", False)
        limit = options.get("limit")

        self.distinct = bool(distinct)
        self.ordered = bool(ordered)
        self.descending = bool(descending)
        self.limit = None if limit is None else int(limit)
        self.values = []
        self.seen = set() if self.distinct else None

    cpdef void add_value(self, object value):
        cdef object key

        if self.distinct:
            key = _distinct_key(value)
            if key in self.seen:
                return
            self.seen.add(key)

        if not self.ordered and self.limit is not None and len(self.values) >= self.limit:
            return

        self.values.append(value)

    cpdef void add_repeated_value(self, object value, Py_ssize_t count):
        cdef Py_ssize_t index

        if count <= 0:
            return

        for index in range(count):
            self.add_value(value)
            if not self.ordered and self.limit is not None and len(self.values) >= self.limit:
                return

    cpdef void merge(self, ArrayAggState other):
        cdef object value

        for value in other.values:
            self.add_value(value)

    cpdef list finalize(self):
        cdef list output
        cdef list non_nulls
        cdef Py_ssize_t null_count
        cdef object value

        output = list(self.values)

        if self.ordered:
            non_nulls = []
            null_count = 0

            for value in output:
                if value is None:
                    null_count += 1
                else:
                    non_nulls.append(value)

            non_nulls.sort(reverse=self.descending)
            output = non_nulls
            if null_count:
                output.extend([None] * null_count)

        if self.limit is not None:
            return output[: self.limit]
        return output
