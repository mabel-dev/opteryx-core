# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

from draken.vectors.timestamp_vector cimport TimestampVector
from draken.vectors.int64_vector cimport Int64Vector, from_sequence as int64_from_sequence
from draken.core.buffers cimport DrakenFixedBuffer




cdef const int64_t MICROSECONDS = 1
cdef const int64_t MILLISECONDS = 1000
cdef const int64_t SECONDS = 1000000
cdef const int64_t MINUTES = 60 * 1000000
cdef const int64_t HOURS = 3600 * 1000000
cdef const int64_t DAYS = 86400 * 1000000

cdef const int64_t NANOSECONDS_FACTOR = 1000  # us->ns
cdef const int64_t MILLISECONDS_FACTOR = 1000  # ms->us (inverse scale)


cdef inline int64_t get_divisor(str part) noexcept:
    if part == "microseconds":
        return MICROSECONDS
    if part == "milliseconds":
        return MILLISECONDS
    if part == "seconds":
        return SECONDS
    if part == "minutes":
        return MINUTES
    if part == "hours":
        return HOURS
    if part == "days":
        return DAYS
    return -1


cdef inline int64_t unit_to_us_factor(str unit) noexcept:
    """Return multiplier to convert native timestamp unit to microseconds."""
    if unit == "us":
        return 1
    if unit == "ms":
        return 1000
    if unit == "s":
        return 1000000
    if unit == "ns":
        return 0  # signal: divide by 1000 instead
    return 1


cdef inline int64_t days_to_months(int64_t days) noexcept nogil:
    """Approximate days to months (30.4375 days per month on average)."""
    return days // 30


cdef inline int64_t days_to_quarters(int64_t days) noexcept nogil:
    """Approximate days to quarters (91.3125 days per quarter on average)."""
    return days // 91


cdef inline int64_t days_to_years(int64_t days) noexcept nogil:
    """Approximate days to years (365.2425 days per year on average)."""
    return days // 365


cpdef Int64Vector vector_date_diff(TimestampVector start, TimestampVector end, str part):
    """
    Compute (end - start) in the requested unit.

    Parameters:
        start: TimestampVector of start times.
        end: TimestampVector of end times.
        part: one of 'microseconds', 'milliseconds', 'seconds', 'minutes', 'hours', 'days',
              'weeks', 'months', 'quarters', 'years'.

    Returns:
        Int64Vector of integer differences.
    """
    cdef DrakenFixedBuffer* sp = start.ptr
    cdef DrakenFixedBuffer* ep = end.ptr
    cdef Py_ssize_t n = sp.length

    if n != <Py_ssize_t>ep.length:
        raise ValueError("Mismatched array lengths")

    # Convert native units to microseconds for uniform arithmetic
    cdef str s_unit = start.timestamp_unit
    cdef str e_unit = end.timestamp_unit
    cdef int64_t s_factor = unit_to_us_factor(s_unit)
    cdef int64_t e_factor = unit_to_us_factor(e_unit)

    cdef int64_t* s_data = <int64_t*>sp.data
    cdef int64_t* e_data = <int64_t*>ep.data
    cdef uint8_t* s_null = sp.null_bitmap
    cdef uint8_t* e_null = ep.null_bitmap

    cdef int64_t* result_data = <int64_t*>malloc(n * sizeof(int64_t))
    if result_data == NULL:
        raise MemoryError()
    memset(result_data, 0, n * sizeof(int64_t))
    cdef int64_t[::1] result_view = <int64_t[:n]>result_data

    cdef Py_ssize_t i
    cdef int64_t sv, ev, days_diff, divisor

    try:
        if part in ("microseconds", "milliseconds", "seconds", "minutes", "hours", "days"):
            # Use the original divisor-based approach for these units
            divisor = get_divisor(part)
            if divisor == -1:
                raise ValueError(f"Unsupported unit: {part}")

            for i in range(n):
                if s_null != NULL and not ((s_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if e_null != NULL and not ((e_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if s_factor == 0:  # nanoseconds: divide to get microseconds
                    sv = s_data[i] // 1000
                else:
                    sv = s_data[i] * s_factor
                if e_factor == 0:
                    ev = e_data[i] // 1000
                else:
                    ev = e_data[i] * e_factor
                result_view[i] = (ev - sv) // divisor

        elif part == "weeks":
            # Convert to days, then to weeks
            for i in range(n):
                if s_null != NULL and not ((s_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if e_null != NULL and not ((e_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if s_factor == 0:
                    sv = s_data[i] // 1000
                else:
                    sv = s_data[i] * s_factor
                if e_factor == 0:
                    ev = e_data[i] // 1000
                else:
                    ev = e_data[i] * e_factor
                days_diff = (ev - sv) // DAYS
                result_view[i] = days_diff // 7

        elif part == "months":
            # Approximate: convert to days, then to months
            for i in range(n):
                if s_null != NULL and not ((s_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if e_null != NULL and not ((e_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if s_factor == 0:
                    sv = s_data[i] // 1000
                else:
                    sv = s_data[i] * s_factor
                if e_factor == 0:
                    ev = e_data[i] // 1000
                else:
                    ev = e_data[i] * e_factor
                days_diff = (ev - sv) // DAYS
                result_view[i] = days_to_months(days_diff)

        elif part == "quarters":
            # Approximate: convert to days, then to quarters
            for i in range(n):
                if s_null != NULL and not ((s_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if e_null != NULL and not ((e_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if s_factor == 0:
                    sv = s_data[i] // 1000
                else:
                    sv = s_data[i] * s_factor
                if e_factor == 0:
                    ev = e_data[i] // 1000
                else:
                    ev = e_data[i] * e_factor
                days_diff = (ev - sv) // DAYS
                result_view[i] = days_to_quarters(days_diff)

        elif part == "years":
            # Approximate: convert to days, then to years
            for i in range(n):
                if s_null != NULL and not ((s_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if e_null != NULL and not ((e_null[i >> 3] >> (i & 7)) & 1):
                    continue
                if s_factor == 0:
                    sv = s_data[i] // 1000
                else:
                    sv = s_data[i] * s_factor
                if e_factor == 0:
                    ev = e_data[i] // 1000
                else:
                    ev = e_data[i] * e_factor
                days_diff = (ev - sv) // DAYS
                result_view[i] = days_to_years(days_diff)

        else:
            raise ValueError(f"Unsupported unit: {part}")

        return int64_from_sequence(result_view)
    finally:
        free(result_data)
