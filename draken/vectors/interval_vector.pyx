# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True
# cython: freethreading_compatible=True

"""
IntervalVector: two-component interval storage for Draken.

Draken stores INTERVAL values as a pair of int64 values representing
(months, microseconds). This diverges from Apache Arrow's internal
interval layout, so conversions to and from Arrow require light-weight
copying and normalization logic.

This module provides:
- IntervalVector class with hashing, null handling, and gather support
- Conversion helpers for Arrow month-day-nano interval arrays
- Conversion helpers for fixed-size binary (16-byte) arrays used for
  packed interval transport
"""

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.bytes cimport PyBytes_AsString

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.limits cimport LLONG_MAX
from libc.limits cimport LLONG_MIN
from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.string cimport memcpy
from libc.string cimport memset

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DRAKEN_INTERVAL
from draken.core.buffers cimport DRAKEN_TIMESTAMP64
from draken.core.buffers cimport draken_vector_from_dense, draken_vector_from_constant
from draken.core.fixed_vector cimport alloc_fixed_buffer
from draken.core.fixed_vector cimport buf_dtype
from draken.core.fixed_vector cimport buf_itemsize
from draken.core.fixed_vector cimport buf_length
from draken.core.fixed_vector cimport free_fixed_buffer
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.bool_vector cimport bool_vector_from_bits
from draken.vectors.date32_vector cimport Date32Vector
from draken.vectors.timestamp_vector cimport TimestampVector
from draken.vectors.vector cimport MIX_HASH_CONSTANT, NULL_HASH, Vector, mix_hash, simd_mix_hash, simd_popcount

DEF INTERVAL_HASH_CHUNK = 512

cdef const int64_t INT64_MIN_VALUE = <int64_t>0x8000000000000000

cdef struct IntervalValue:
    int64_t months
    int64_t microseconds

cdef struct ArrowMonthDayNanoValue:
    int32_t months
    int32_t days
    int64_t nanoseconds

cdef struct ArrowDayTimeValue:
    int32_t days
    int32_t milliseconds

cdef int64_t MICROSECONDS_PER_SECOND = 1000000
cdef int64_t MICROSECONDS_PER_MINUTE = 60 * MICROSECONDS_PER_SECOND
cdef int64_t MICROSECONDS_PER_HOUR = 60 * MICROSECONDS_PER_MINUTE
cdef int64_t MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR
cdef int64_t MICROSECONDS_PER_MILLISECOND = 1000
cdef int64_t NANOSECONDS_PER_MICROSECOND = 1000
cdef size_t INTERVAL_ITEMSIZE = sizeof(IntervalValue)
cdef int8_t INTERVAL_OP_EQ = 0
cdef int8_t INTERVAL_OP_NEQ = 1
cdef int8_t INTERVAL_OP_GT = 2
cdef int8_t INTERVAL_OP_GTE = 3
cdef int8_t INTERVAL_OP_LT = 4
cdef int8_t INTERVAL_OP_LTE = 5

cdef void _refresh_unified_Interval(IntervalVector vec) noexcept:
    cdef uint32_t n = <uint32_t>vec.ptr.length
    vec._unified_view = draken_vector_from_dense(vec.ptr.data, n, DRAKEN_INTERVAL, vec.ptr.null_bitmap)


cdef inline bint _is_valid(DrakenFixedBuffer* ptr, Py_ssize_t idx) nogil:
    if ptr.null_bitmap == NULL:
        return True
    return (ptr.null_bitmap[idx >> 3] >> (idx & 7)) & 1

cdef inline void _divmod_microseconds(int64_t total, int64_t* out_days, int64_t* out_remainder) noexcept nogil:
    cdef int64_t q = total / MICROSECONDS_PER_DAY
    cdef int64_t r = total - q * MICROSECONDS_PER_DAY
    if r < 0:
        q -= 1
        r += MICROSECONDS_PER_DAY
    out_days[0] = q
    out_remainder[0] = r


cdef inline int64_t _floor_div_int64(int64_t value, int64_t divisor) noexcept nogil:
    cdef int64_t quotient = value / divisor
    cdef int64_t remainder = value - quotient * divisor
    if remainder != 0 and ((value < 0 and divisor > 0) or (value > 0 and divisor < 0)):
        quotient -= 1
    return quotient


cdef inline bint _is_leap_year(int64_t year) noexcept nogil:
    if year % 4 != 0:
        return False
    if year % 100 != 0:
        return True
    return year % 400 == 0


cdef inline int64_t _days_in_month(int64_t year, int64_t month) noexcept nogil:
    if month == 2:
        if _is_leap_year(year):
            return 29
        return 28
    if month == 4 or month == 6 or month == 9 or month == 11:
        return 30
    return 31


cdef inline int64_t _days_from_civil(int64_t year, int64_t month, int64_t day) noexcept nogil:
    cdef int64_t y = year - (1 if month <= 2 else 0)
    cdef int64_t era = _floor_div_int64(y, 400)
    cdef int64_t yoe = y - era * 400
    cdef int64_t month_prime = month + (-3 if month > 2 else 9)
    cdef int64_t doy = (153 * month_prime + 2) / 5 + day - 1
    cdef int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
    return era * 146097 + doe - 719468


cdef inline void _civil_from_days(
    int64_t days,
    int64_t* out_year,
    int64_t* out_month,
    int64_t* out_day,
) noexcept nogil:
    cdef int64_t z = days + 719468
    cdef int64_t era = _floor_div_int64(z, 146097)
    cdef int64_t doe = z - era * 146097
    cdef int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
    cdef int64_t y = yoe + era * 400
    cdef int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
    cdef int64_t mp = (5 * doy + 2) / 153
    cdef int64_t day = doy - (153 * mp + 2) / 5 + 1
    cdef int64_t month = mp + (3 if mp < 10 else -9)
    y += 1 if month <= 2 else 0

    out_year[0] = y
    out_month[0] = month
    out_day[0] = day


cdef inline Py_ssize_t _resolve_broadcast_length(Py_ssize_t left_len, Py_ssize_t right_len) except -1:
    if left_len == right_len:
        return left_len
    if left_len == 1:
        return right_len
    if right_len == 1:
        return left_len
    raise ValueError(
        f"IntervalVector length mismatch: left={left_len}, right={right_len}. "
        "Lengths must match or one side must be scalar."
    )


cdef inline Py_ssize_t _broadcast_index(Py_ssize_t i, Py_ssize_t source_len) nogil:
    if source_len == 1:
        return 0
    return i

cdef inline int64_t _safe_mul_int64(int64_t value, int64_t factor) noexcept nogil:
    if factor > 0:
        if value > 0 and value > (<int64_t>LLONG_MAX) // factor:
            return <int64_t>LLONG_MAX
        if value < 0 and value < (<int64_t>LLONG_MIN) // factor:
            return <int64_t>LLONG_MIN
    return value * factor


cdef inline int64_t _timestamp_raw_to_microseconds(int64_t value, int unit_code) noexcept nogil:
    if unit_code == 0:  # ns
        return value // 1000
    if unit_code == 1:  # us
        return value
    if unit_code == 2:  # ms
        return _safe_mul_int64(value, 1000)
    return _safe_mul_int64(value, 1000000)  # s


cdef inline bint _is_valid_with_offset(uint8_t* bitmap, Py_ssize_t idx, Py_ssize_t bit_offset) noexcept nogil:
    cdef Py_ssize_t bit_index = idx + bit_offset
    if bitmap == NULL:
        return True
    return (bitmap[bit_index >> 3] >> (bit_index & 7)) & 1


cdef class IntervalVector(Vector):

    def __cinit__(self, size_t length=0, bint wrap=False):
        self._arrow_data_buf = None
        self._arrow_null_buf = None
        if wrap:
            self.ptr = NULL
            self.owns_data = False
            self._unified_view = draken_vector_from_dense(NULL, 0, DRAKEN_INTERVAL, NULL)
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_INTERVAL, length, INTERVAL_ITEMSIZE)
            self.owns_data = True
            _refresh_unified_Interval(self)

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    cdef DrakenVector* unified(self) noexcept:
        return &self._unified_view

    cdef void _set_null_bitmap(self, uint8_t* bm) noexcept:
        self.ptr.null_bitmap = bm
        self._unified_view.validity = bm

    @property
    def length(self):
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def itemsize(self):
        return 16

    @property
    def dtype(self):
        return DRAKEN_INTERVAL

    def __getitem__(self, Py_ssize_t i):
        cdef DrakenFixedBuffer* ptr = self.ptr
        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of bounds")
        if not _is_valid(ptr, i):
            return None
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        return (data[i].months, data[i].microseconds)

    def to_arrow(self):
        return self.to_arrow_interval()

    cpdef object to_arrow_interval(self):
        import pyarrow as pa

        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef list rows = [None] * n
        cdef Py_ssize_t i
        cdef int64_t days
        cdef int64_t remainder
        for i in range(n):
            if not _is_valid(ptr, i):
                continue
            _divmod_microseconds(data[i].microseconds, &days, &remainder)
            rows[i] = (
                data[i].months,
                days,
                remainder * NANOSECONDS_PER_MICROSECOND,
            )
        return pa.array(rows, type=pa.month_day_nano_interval())

    cpdef object to_arrow_binary(self):
        import pyarrow as pa

        cdef size_t nbytes = self.ptr.length * 16
        cdef intptr_t data_addr = <intptr_t> self.ptr.data
        data_buf = pa.foreign_buffer(data_addr, nbytes, base=self)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(
                pa.foreign_buffer(
                    <intptr_t> self.ptr.null_bitmap,
                    (self.ptr.length + 7) // 8,
                    base=self,
                )
            )
        else:
            buffers.append(None)
        buffers.append(data_buf)
        cdef object binary_factory = getattr(pa, "fixed_size_binary", None)
        cdef object binary_type
        if binary_factory is not None:
            binary_type = binary_factory(<int>INTERVAL_ITEMSIZE)
        else:
            binary_type = pa.binary(<int>INTERVAL_ITEMSIZE)

        return pa.Array.from_buffers(
            binary_type,
            self.ptr.length,
            buffers,
        )

    cpdef IntervalVector add_vector(self, IntervalVector other):
        cdef DrakenFixedBuffer* left_ptr = self.ptr
        cdef DrakenFixedBuffer* right_ptr = other.ptr
        cdef Py_ssize_t left_len = left_ptr.length
        cdef Py_ssize_t right_len = right_ptr.length
        cdef Py_ssize_t out_len = _resolve_broadcast_length(left_len, right_len)
        cdef IntervalVector out = IntervalVector(<size_t> out_len)
        cdef IntervalValue* left_data = <IntervalValue*> left_ptr.data
        cdef IntervalValue* right_data = <IntervalValue*> right_ptr.data
        cdef IntervalValue* out_data = <IntervalValue*> out.ptr.data
        cdef bint has_nulls = left_ptr.null_bitmap != NULL or right_ptr.null_bitmap != NULL
        cdef uint8_t* out_null = NULL
        cdef size_t nbytes = 0
        cdef Py_ssize_t i
        cdef Py_ssize_t left_index
        cdef Py_ssize_t right_index

        if has_nulls:
            nbytes = (out_len + 7) >> 3
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(out_len):
            left_index = _broadcast_index(i, left_len)
            right_index = _broadcast_index(i, right_len)

            if has_nulls and (not _is_valid(left_ptr, left_index) or not _is_valid(right_ptr, right_index)):
                out_data[i].months = 0
                out_data[i].microseconds = 0
                continue

            out_data[i].months = left_data[left_index].months + right_data[right_index].months
            out_data[i].microseconds = (
                left_data[left_index].microseconds + right_data[right_index].microseconds
            )
            if has_nulls:
                out_null[i >> 3] |= (1 << (i & 7))

        return out

    cpdef IntervalVector subtract_vector(self, IntervalVector other):
        cdef DrakenFixedBuffer* left_ptr = self.ptr
        cdef DrakenFixedBuffer* right_ptr = other.ptr
        cdef Py_ssize_t left_len = left_ptr.length
        cdef Py_ssize_t right_len = right_ptr.length
        cdef Py_ssize_t out_len = _resolve_broadcast_length(left_len, right_len)
        cdef IntervalVector out = IntervalVector(<size_t> out_len)
        cdef IntervalValue* left_data = <IntervalValue*> left_ptr.data
        cdef IntervalValue* right_data = <IntervalValue*> right_ptr.data
        cdef IntervalValue* out_data = <IntervalValue*> out.ptr.data
        cdef bint has_nulls = left_ptr.null_bitmap != NULL or right_ptr.null_bitmap != NULL
        cdef uint8_t* out_null = NULL
        cdef size_t nbytes = 0
        cdef Py_ssize_t i
        cdef Py_ssize_t left_index
        cdef Py_ssize_t right_index

        if has_nulls:
            nbytes = (out_len + 7) >> 3
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(out_len):
            left_index = _broadcast_index(i, left_len)
            right_index = _broadcast_index(i, right_len)

            if has_nulls and (not _is_valid(left_ptr, left_index) or not _is_valid(right_ptr, right_index)):
                out_data[i].months = 0
                out_data[i].microseconds = 0
                continue

            out_data[i].months = left_data[left_index].months - right_data[right_index].months
            out_data[i].microseconds = (
                left_data[left_index].microseconds - right_data[right_index].microseconds
            )
            if has_nulls:
                out_null[i >> 3] |= (1 << (i & 7))

        return out

    cpdef BoolVector compare_vector(
        self,
        IntervalVector other,
        int8_t operation,
        bint reject_month_components=False,
    ):
        cdef DrakenFixedBuffer* left_ptr = self.ptr
        cdef DrakenFixedBuffer* right_ptr = other.ptr
        cdef Py_ssize_t left_len = left_ptr.length
        cdef Py_ssize_t right_len = right_ptr.length
        cdef Py_ssize_t out_len = _resolve_broadcast_length(left_len, right_len)
        cdef IntervalValue* left_data = <IntervalValue*> left_ptr.data
        cdef IntervalValue* right_data = <IntervalValue*> right_ptr.data
        cdef bint has_nulls = left_ptr.null_bitmap != NULL or right_ptr.null_bitmap != NULL
        cdef size_t nbytes = (out_len + 7) >> 3
        cdef uint8_t* value_bits = <uint8_t*> malloc(nbytes if nbytes > 0 else 1)
        cdef uint8_t* valid_bits = NULL
        cdef Py_ssize_t i
        cdef Py_ssize_t left_index
        cdef Py_ssize_t right_index
        cdef int64_t left_months
        cdef int64_t right_months
        cdef bint comparison = False
        cdef BoolVector out

        if value_bits == NULL:
            raise MemoryError()
        memset(value_bits, 0, nbytes)

        if has_nulls:
            valid_bits = <uint8_t*> malloc(nbytes)
            if valid_bits == NULL:
                free(value_bits)
                raise MemoryError()
            memset(valid_bits, 0, nbytes)

        try:
            for i in range(out_len):
                left_index = _broadcast_index(i, left_len)
                right_index = _broadcast_index(i, right_len)

                if has_nulls and (not _is_valid(left_ptr, left_index) or not _is_valid(right_ptr, right_index)):
                    continue

                if has_nulls:
                    valid_bits[i >> 3] |= (1 << (i & 7))

                left_months = left_data[left_index].months
                right_months = right_data[right_index].months
                if reject_month_components and (left_months != 0 or right_months != 0):
                    raise ValueError("Cannot compare INTERVALs with MONTH or YEAR components.")

                if operation == INTERVAL_OP_EQ:
                    comparison = left_data[left_index].microseconds == right_data[right_index].microseconds
                elif operation == INTERVAL_OP_NEQ:
                    comparison = left_data[left_index].microseconds != right_data[right_index].microseconds
                elif operation == INTERVAL_OP_GT:
                    comparison = left_data[left_index].microseconds > right_data[right_index].microseconds
                elif operation == INTERVAL_OP_GTE:
                    comparison = left_data[left_index].microseconds >= right_data[right_index].microseconds
                elif operation == INTERVAL_OP_LT:
                    comparison = left_data[left_index].microseconds < right_data[right_index].microseconds
                elif operation == INTERVAL_OP_LTE:
                    comparison = left_data[left_index].microseconds <= right_data[right_index].microseconds
                else:
                    raise ValueError(f"Unsupported interval comparison operation code: {operation}")

                if comparison:
                    value_bits[i >> 3] |= (1 << (i & 7))

            out = bool_vector_from_bits(
                value_bits,
                valid_bits if has_nulls else NULL,
                out_len,
            )
            return out
        finally:
            free(value_bits)
            if valid_bits != NULL:
                free(valid_bits)

    cdef BoolVector _compare_scalar(
        self,
        int64_t sc_months,
        int64_t sc_microseconds,
        int8_t operation,
        bint reject_month_components,
    ):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef size_t nbytes = (n + 7) >> 3
        cdef uint8_t* value_bits = <uint8_t*> malloc(nbytes if nbytes > 0 else 1)
        cdef uint8_t* valid_bits = NULL
        cdef Py_ssize_t i
        cdef bint comparison

        if value_bits == NULL:
            raise MemoryError()
        memset(value_bits, 0, nbytes)

        if ptr.null_bitmap != NULL:
            valid_bits = <uint8_t*> malloc(nbytes if nbytes > 0 else 1)
            if valid_bits == NULL:
                free(value_bits)
                raise MemoryError()
            memset(valid_bits, 0, nbytes)

        try:
            if reject_month_components and sc_months != 0:
                raise ValueError("Cannot compare INTERVALs with MONTH or YEAR components.")

            for i in range(n):
                if not _is_valid(ptr, i):
                    continue

                if valid_bits != NULL:
                    valid_bits[i >> 3] |= (1 << (i & 7))

                if reject_month_components and data[i].months != 0:
                    raise ValueError("Cannot compare INTERVALs with MONTH or YEAR components.")

                if operation == INTERVAL_OP_EQ:
                    comparison = data[i].microseconds == sc_microseconds
                elif operation == INTERVAL_OP_NEQ:
                    comparison = data[i].microseconds != sc_microseconds
                elif operation == INTERVAL_OP_GT:
                    comparison = data[i].microseconds > sc_microseconds
                elif operation == INTERVAL_OP_GTE:
                    comparison = data[i].microseconds >= sc_microseconds
                elif operation == INTERVAL_OP_LT:
                    comparison = data[i].microseconds < sc_microseconds
                elif operation == INTERVAL_OP_LTE:
                    comparison = data[i].microseconds <= sc_microseconds
                else:
                    raise ValueError(f"Unsupported interval comparison code: {operation}")

                if comparison:
                    value_bits[i >> 3] |= (1 << (i & 7))

            return bool_vector_from_bits(
                value_bits,
                valid_bits if ptr.null_bitmap != NULL else NULL,
                n,
            )
        finally:
            free(value_bits)
            if valid_bits != NULL:
                free(valid_bits)

    cpdef BoolVector equals(self, object literal):
        """Return mask: 1 if element == literal, else 0. Propagates NULLs."""
        cdef int64_t sc_months = literal[0]
        cdef int64_t sc_microseconds = literal[1]
        return self._compare_scalar(sc_months, sc_microseconds, INTERVAL_OP_EQ, False)

    cpdef BoolVector not_equals(self, object literal):
        """Return mask: 1 if element != literal, else 0. Propagates NULLs."""
        cdef int64_t sc_months = literal[0]
        cdef int64_t sc_microseconds = literal[1]
        return self._compare_scalar(sc_months, sc_microseconds, INTERVAL_OP_NEQ, False)

    cpdef BoolVector less_than(self, object literal):
        """Return mask: 1 if element < literal, else 0. Propagates NULLs."""
        cdef int64_t sc_months = literal[0]
        cdef int64_t sc_microseconds = literal[1]
        return self._compare_scalar(sc_months, sc_microseconds, INTERVAL_OP_LT, True)

    cpdef BoolVector greater_than(self, object literal):
        """Return mask: 1 if element > literal, else 0. Propagates NULLs."""
        cdef int64_t sc_months = literal[0]
        cdef int64_t sc_microseconds = literal[1]
        return self._compare_scalar(sc_months, sc_microseconds, INTERVAL_OP_GT, True)

    cpdef BoolVector less_than_or_equals(self, object literal):
        """Return mask: 1 if element <= literal, else 0. Propagates NULLs."""
        cdef int64_t sc_months = literal[0]
        cdef int64_t sc_microseconds = literal[1]
        return self._compare_scalar(sc_months, sc_microseconds, INTERVAL_OP_LTE, True)

    cpdef BoolVector greater_than_or_equals(self, object literal):
        """Return mask: 1 if element >= literal, else 0. Propagates NULLs."""
        cdef int64_t sc_months = literal[0]
        cdef int64_t sc_microseconds = literal[1]
        return self._compare_scalar(sc_months, sc_microseconds, INTERVAL_OP_GTE, True)

    cpdef TimestampVector apply_to_temporal(self, Vector values, int8_t signum=1):
        """Apply this interval vector to DATE/TIMESTAMP vectors with SQL-style month/day handling."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef IntervalValue* intervals = <IntervalValue*> ptr.data
        cdef Py_ssize_t interval_len = ptr.length
        cdef Py_ssize_t row_len
        cdef Py_ssize_t out_len
        cdef Py_ssize_t i
        cdef Py_ssize_t value_index
        cdef Py_ssize_t interval_index
        cdef int64_t epoch_days
        cdef int64_t day_microseconds
        cdef int64_t days_offset
        cdef int64_t normalized_day_microseconds
        cdef int64_t year
        cdef int64_t month
        cdef int64_t day
        cdef int64_t month_index
        cdef int64_t month_delta
        cdef int64_t month_div
        cdef int64_t last_day
        cdef int64_t result_microseconds
        cdef Date32Vector date_values
        cdef DrakenFixedBuffer* date_ptr
        cdef int32_t* date_data
        cdef DrakenVector* uv_date
        cdef TimestampVector ts_values
        cdef DrakenFixedBuffer* ts_ptr
        cdef int64_t* ts_data
        cdef DrakenVector* uv_ts
        cdef int ts_unit_code
        cdef int64_t ts_raw
        cdef bint values_is_date = isinstance(values, Date32Vector)
        cdef bint values_is_timestamp = isinstance(values, TimestampVector)

        if not values_is_date and not values_is_timestamp:
            raise TypeError(
                "IntervalVector.apply_to_temporal expects Date32Vector or TimestampVector, "
                f"got {values.__class__.__name__}."
            )

        row_len = len(values)
        out_len = _resolve_broadcast_length(row_len, interval_len)

        cdef TimestampVector result = TimestampVector(out_len)
        cdef int64_t* out_data = <int64_t*> result.ptr.data
        cdef Py_ssize_t null_bytes = (out_len + 7) >> 3
        cdef uint8_t* out_null = NULL

        if out_len == 0:
            result.ptr.null_bitmap = NULL
            return result

        out_null = <uint8_t*> malloc(null_bytes if null_bytes > 0 else 1)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, null_bytes)
        result.ptr.null_bitmap = out_null

        if values_is_date:
            date_values = <Date32Vector> values
            date_ptr = date_values.ptr
            date_data = <int32_t*> date_ptr.data
            uv_date = date_values.unified()

            for i in range(out_len):
                value_index = _broadcast_index(i, row_len)
                interval_index = _broadcast_index(i, interval_len)

                if not _is_valid(ptr, interval_index):
                    continue

                if uv_date.validity != NULL and not ((uv_date.validity[value_index >> 3] >> (value_index & 7)) & 1):
                    continue
                epoch_days = <int64_t>(<int32_t*>uv_date.data)[uv_date.selection[value_index]]

                day_microseconds = 0

                _divmod_microseconds(
                    day_microseconds + intervals[interval_index].microseconds * signum,
                    &days_offset,
                    &normalized_day_microseconds,
                )
                epoch_days += days_offset
                day_microseconds = normalized_day_microseconds

                month_delta = intervals[interval_index].months * signum
                if month_delta != 0:
                    _civil_from_days(epoch_days, &year, &month, &day)
                    month_index = (month - 1) + month_delta
                    month_div = _floor_div_int64(month_index, 12)
                    year += month_div
                    month = month_index - month_div * 12 + 1
                    last_day = _days_in_month(year, month)
                    if day > last_day:
                        day = last_day
                    epoch_days = _days_from_civil(year, month, day)

                result_microseconds = epoch_days * MICROSECONDS_PER_DAY + day_microseconds
                out_data[i] = result_microseconds
                out_null[i >> 3] |= (<uint8_t>1 << (i & 7))
        else:
            ts_values = <TimestampVector> values
            ts_ptr = ts_values.ptr
            ts_data = <int64_t*> ts_ptr.data
            ts_unit_code = ts_values._unit_code
            uv_ts = ts_values.unified()

            for i in range(out_len):
                value_index = _broadcast_index(i, row_len)
                interval_index = _broadcast_index(i, interval_len)

                if not _is_valid(ptr, interval_index):
                    continue

                if uv_ts.validity != NULL and not ((uv_ts.validity[value_index >> 3] >> (value_index & 7)) & 1):
                    continue
                ts_raw = (<int64_t*>uv_ts.data)[uv_ts.selection[value_index]]

                _divmod_microseconds(
                    _timestamp_raw_to_microseconds(ts_raw, ts_unit_code),
                    &epoch_days,
                    &day_microseconds,
                )

                _divmod_microseconds(
                    day_microseconds + intervals[interval_index].microseconds * signum,
                    &days_offset,
                    &normalized_day_microseconds,
                )
                epoch_days += days_offset
                day_microseconds = normalized_day_microseconds

                month_delta = intervals[interval_index].months * signum
                if month_delta != 0:
                    _civil_from_days(epoch_days, &year, &month, &day)
                    month_index = (month - 1) + month_delta
                    month_div = _floor_div_int64(month_index, 12)
                    year += month_div
                    month = month_index - month_div * 12 + 1
                    last_day = _days_in_month(year, month)
                    if day > last_day:
                        day = last_day
                    epoch_days = _days_from_civil(year, month, day)

                result_microseconds = epoch_days * MICROSECONDS_PER_DAY + day_microseconds
                out_data[i] = result_microseconds
                out_null[i >> 3] |= (<uint8_t>1 << (i & 7))

        return result

    cpdef IntervalVector take(self, int32_t[::1] indices):
        cdef DrakenVector* uv = self.unified()
        cdef Py_ssize_t n = indices.shape[0]
        cdef Py_ssize_t i
        cdef IntervalVector out = IntervalVector(<size_t> n)
        cdef IntervalValue* data = <IntervalValue*>uv.data
        cdef IntervalValue* dst = <IntervalValue*> out.ptr.data
        cdef uint8_t* src_null = uv.validity
        cdef uint8_t* out_null = NULL
        cdef int32_t src_idx
        cdef size_t nbytes = 0

        if src_null != NULL and n > 0:
            nbytes = (<size_t>n + 7) >> 3
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)

        for i in range(n):
            src_idx = indices[i]
            if src_null != NULL and not ((src_null[src_idx >> 3] >> (src_idx & 7)) & 1):
                dst[i].months = 0
                dst[i].microseconds = 0
            else:
                dst[i] = data[<Py_ssize_t>uv.selection[<Py_ssize_t>src_idx]]
                if out_null != NULL:
                    out_null[i >> 3] |= <uint8_t>(1 << (i & 7))

        out.ptr.null_bitmap = out_null
        out._unified_view = draken_vector_from_dense(
            out.ptr.data, <uint32_t>n, DRAKEN_INTERVAL, out.ptr.null_bitmap)
        return out

    cpdef int8_t[::1] is_null(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte
        cdef uint8_t bit
        if buf == NULL:
            raise MemoryError()
        if ptr.null_bitmap == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1
        return <int8_t[:n]> buf

    @property
    def null_count(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        if ptr.null_bitmap == NULL:
            return 0
        return n - <Py_ssize_t>simd_popcount(ptr.null_bitmap, (<size_t>n + 7) >> 3)

    @property
    def nbytes(self):
        """Return the approximate memory footprint of this vector in bytes."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef uint64_t n = ptr.length
        cdef uint64_t data_bytes, bm_bytes
        data_bytes = <uint64_t>(buf_length(ptr) * buf_itemsize(ptr))
        bm_bytes = (n + 7) >> 3 if ptr.null_bitmap != NULL else 0
        return data_bytes + bm_bytes

    cpdef list to_pylist(self):
        cdef list out = []
        cdef IntervalValue* rle_vals_iv
        cdef int32_t* rle_lens_iv
        cdef size_t rle_runs_iv
        cdef uint8_t* rle_nulls_iv
        cdef Py_ssize_t iv_pos
        cdef size_t iv_r
        cdef int32_t iv_run_len
        cdef Py_ssize_t iv_j
        cdef DrakenFixedBuffer* ptr
        cdef IntervalValue* data
        cdef Py_ssize_t i, n

        ptr = self.ptr
        data = <IntervalValue*> ptr.data
        n = ptr.length
        for i in range(n):
            if not _is_valid(ptr, i):
                out.append(None)
            else:
                out.append((data[i].months, data[i].microseconds))
        return out

    cpdef uint64_t[::1] hash(self):
        cdef Py_ssize_t n = self.ptr.length
        cdef uint64_t* buf = <uint64_t*> PyMem_Malloc(n * sizeof(uint64_t))
        cdef Py_ssize_t i
        if buf == NULL:
            raise MemoryError()
        for i in range(n):
            buf[i] = 0
        cdef uint64_t[::1] view = <uint64_t[:n]> buf
        self.hash_into(view, 0)
        return view

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("IntervalVector.hash_into: output buffer too small")

        cdef Py_ssize_t i
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef uint64_t value
        cdef uint64_t partial
        cdef uint64_t* dst = &out_buf[offset]
        cdef bint has_nulls = ptr.null_bitmap != NULL
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef uint64_t[INTERVAL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if not has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > INTERVAL_HASH_CHUNK:
                    block = INTERVAL_HASH_CHUNK
                for j in range(block):
                    partial = mix_hash(0, <uint64_t>data[i + j].months)
                    scratch[j] = mix_hash(partial, <uint64_t>data[i + j].microseconds)
                simd_mix_hash(dst + i, scratch_ptr, <size_t> block)
                i += block
            return

        for i in range(n):
            if not _is_valid(ptr, i):
                value = NULL_HASH
            else:
                partial = mix_hash(0, <uint64_t>data[i].months)
                value = mix_hash(partial, <uint64_t>data[i].microseconds)
            dst[i] = mix_hash(dst[i], value)

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        cdef DrakenFixedBuffer* ptr = self.ptr
        if n == 0:
            return 0

        cdef Py_ssize_t i
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef uint64_t value
        cdef uint64_t partial
        cdef bint has_nulls = ptr.null_bitmap != NULL
        cdef Py_ssize_t block = 0
        cdef Py_ssize_t j = 0
        cdef uint64_t[INTERVAL_HASH_CHUNK] scratch
        cdef uint64_t* scratch_ptr = <uint64_t*> scratch

        if not has_nulls:
            i = 0
            while i < n:
                block = n - i
                if block > INTERVAL_HASH_CHUNK:
                    block = INTERVAL_HASH_CHUNK
                for j in range(block):
                    partial = mix_hash(0, <uint64_t>data[i + j].months)
                    scratch[j] = mix_hash(partial, <uint64_t>data[i + j].microseconds)
                simd_mix_hash(out + i, scratch_ptr, <size_t> block)
                i += block
            return 0

        for i in range(n):
            if not _is_valid(ptr, i):
                value = NULL_HASH
            else:
                partial = mix_hash(0, <uint64_t>data[i].months)
                value = mix_hash(partial, <uint64_t>data[i].microseconds)
            out[i] = mix_hash(out[i], value)
        return 0

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Fast compress for IntervalVector: use months component for ordering."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int64_t NULL_FLAG = INT64_MIN_VALUE

        if n == 0:
            return

        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("IntervalVector.compress: output buffer too small")

        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef int64_t* dst = &out_buf[offset]
        cdef bint has_nulls = ptr.null_bitmap != NULL
        cdef Py_ssize_t i

        if has_nulls:
            for i in range(n):
                if _is_valid(ptr, i):
                    # Use months as primary component for ordering
                    dst[i] = data[i].months
                else:
                    dst[i] = NULL_FLAG
        else:
            for i in range(n):
                dst[i] = data[i].months

    cpdef object min(self):
        """Return interval with minimum duration (by microseconds), or None if all null or empty."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef IntervalValue min_val
        cdef bint found = False

        for i in range(n):
            if not _is_valid(ptr, i):
                continue

            if not found:
                min_val = data[i]
                found = True
            else:
                if data[i].microseconds < min_val.microseconds:
                    min_val = data[i]

        if not found:
            return None
        return (min_val.months, min_val.microseconds)

    cpdef object max(self):
        """Return interval with maximum duration (by microseconds), or None if all null or empty."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef IntervalValue max_val
        cdef bint found = False

        for i in range(n):
            if not _is_valid(ptr, i):
                continue

            if not found:
                max_val = data[i]
                found = True
            else:
                if data[i].microseconds > max_val.microseconds:
                    max_val = data[i]

        if not found:
            return None
        return (max_val.months, max_val.microseconds)

    cpdef object sum(self):
        """Sum intervals by adding months and microseconds components separately."""
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        cdef IntervalValue* data = <IntervalValue*> ptr.data
        cdef int64_t sum_months = 0
        cdef int64_t sum_microseconds = 0
        cdef int64_t valid_count = 0

        for i in range(n):
            if not _is_valid(ptr, i):
                continue

            sum_months += data[i].months
            sum_microseconds += data[i].microseconds
            valid_count += 1

        if valid_count == 0:
            return None
        return (sum_months, sum_microseconds)

    def __str__(self):
        cdef list preview = []
        cdef Py_ssize_t i, n, limit
        n = self.ptr.length
        limit = n if n < 10 else 10
        for i in range(limit):
            preview.append(self[i])
        return f"<IntervalVector len={n} values={preview}>"
