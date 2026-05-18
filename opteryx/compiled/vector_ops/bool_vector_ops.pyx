# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int8_t, uint8_t
from libc.string cimport memset

from draken.vectors.bool_vector cimport BoolVector


cpdef BoolVector bool_vector_from_int8_mask(object mask_obj, Py_ssize_t n):
    """Build a BoolVector from a byte-per-element null mask (1=null, 0=valid).

    Used for IS NULL evaluation on fixed-buffer vector types (Integer64Vector,
    Float64Vector, TimestampVector, Date32Vector, etc.) which expose their null
    information via ``is_null() -> int8_t[::1]``.

    Args:
        mask_obj: A contiguous int8_t memoryview from ``vec.is_null()``.
                  Element 1 means NULL, 0 means valid.
        n:        Number of rows (must match len(mask_obj)).

    Returns:
        BoolVector where bit[i] = 1 when row i is SQL NULL.
    """
    cdef const int8_t[::1] mask = mask_obj
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(n)
    if n == 0:
        return out
    cdef uint8_t* data = <uint8_t*>out.ptr.data
    memset(data, 0, nbytes)
    cdef Py_ssize_t i
    for i in range(n):
        if mask[i]:
            data[i >> 3] |= (<uint8_t>1 << (i & 7))
    return out


cpdef BoolVector bool_vector_from_inverted_null_bitmap(object bitmap_mv, Py_ssize_t n):
    """Build a BoolVector from an inverted null bitmap.

    Bitmaps use bit=1 for VALID, bit=0 for NULL.
    IS NULL requires bit=1 where null_bitmap=0, so we invert.

    Used for StringVector and ArrayVector which expose null information via
    ``null_bitmap() -> memoryview | None``.

    Args:
        bitmap_mv: A memoryview of the null bitmap bytes (bit=1 valid, bit=0 null).
        n:         Number of rows.
    """
    cdef const uint8_t[::1] bitmap = bitmap_mv
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(n)
    if n == 0:
        return out
    cdef uint8_t* data = <uint8_t*>out.ptr.data
    cdef Py_ssize_t i
    # Invert: null_bitmap bit=0 (null) → IS NULL bit=1
    for i in range(nbytes):
        data[i] = ~bitmap[i]
    # Clear trailing bits beyond n in the last byte so they read as False
    cdef Py_ssize_t tail = n & 7
    if tail != 0:
        data[nbytes - 1] &= (<uint8_t>(1 << tail)) - 1
    return out


cpdef BoolVector bool_vector_all_true(Py_ssize_t n):
    """Build a BoolVector with all bits set (all True / IS NULL for null constants).

    Used when a column is known to be entirely SQL NULL (e.g. constant encoding
    with scalar_value() == None).

    Args:
        n: Number of rows.

    Returns:
        BoolVector where every bit is 1 (all True).
    """
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(n)
    if n == 0:
        return out
    cdef uint8_t* data = <uint8_t*>out.ptr.data
    memset(data, 0xFF, nbytes)
    # Clear trailing bits beyond n in the last byte
    cdef Py_ssize_t tail = n & 7
    if tail != 0:
        data[nbytes - 1] = (<uint8_t>(1 << tail)) - 1
    return out


cpdef BoolVector bool_vector_and_chain(list masks):
    """AND a list of BoolVectors with early-exit short-circuit on all-false.

    Advances through the list, ANDing each BoolVector into the running result.
    Stops as soon as the running result becomes all-false (.any() == 0), avoiding
    further calls to ``and_vector``.

    Intended for callers that have pre-computed a list of filter masks and want
    to reduce them to a single mask with maximum short-circuit benefit.

    Args:
        masks: List of BoolVector instances. Must all have identical length.
               The first element is used as the initial accumulator.

    Returns:
        BoolVector with the accumulated AND result, or None if masks is empty.
    """
    cdef Py_ssize_t i, n = len(masks)
    if n == 0:
        return None
    cdef BoolVector result = <BoolVector>masks[0]
    for i in range(1, n):
        if not result.any():
            return result
        result = result.and_vector(<BoolVector>masks[i])
    return result
