# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

import os

# This file is `include`d into vector_ops.pyx. malloc / free / int32_t /
# memset etc. are already in scope via earlier-included leaves; we re-state
# them here only to make the file self-documenting. malloc /
# free are used in this file but resolved via the earlier-included
# leaves' cimport — declaring them here triggers "ambiguous overloaded
# method" in the consolidated build.
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint64_t
from libc.string cimport memset

from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.integer_vector cimport IntegerVector
from draken.vectors.date32_vector cimport Date32Vector
from draken.vectors.timestamp_vector cimport TimestampVector
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.perfect_hash_set cimport PerfectHashSet

# Null hash sentinel — same value as filter_join.pyx and the Draken hash machinery.
cdef uint64_t _NULL_HASH = <uint64_t>0x73d59cff8f94d86cULL

# Range cap: 256K slots (32 KB at 1 bit/slot — fits L1).
cdef int64_t _PERFECT_HASH_CAP = 262_144


def build_in_list_carchar(values):
    """Build a membership set from a Python list of IN-list literal values.

    Returns a PerfectHashSet when OPTERYX_PERFECT_HASH=1, all values are
    non-null integers, and the value range fits within 256K slots.
    Otherwise returns a CarcharSetWrapper (hashed path).
    """
    from draken.vectors.scalar_constructors import from_scalar as _build_scalar

    if os.environ.get("OPTERYX_PERFECT_HASH") == "1" and values:
        try:
            if not any(v is None for v in values):
                import datetime as _dt
                _EPOCH = _dt.date(1970, 1, 1)
                def _physical(v):
                    if isinstance(v, _dt.datetime):
                        delta = v.replace(tzinfo=None) - _dt.datetime(1970, 1, 1)
                        return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
                    if isinstance(v, _dt.date):
                        return (v - _EPOCH).days
                    return int(v)
                int_values = [_physical(v) for v in values]
                min_v = min(int_values)
                max_v = max(int_values)
                if max_v - min_v < _PERFECT_HASH_CAP:
                    phs = PerfectHashSet(min_v, max_v)
                    for v in int_values:
                        phs.insert_i64(<int64_t>v)
                    return phs
        except (TypeError, ValueError, OverflowError):
            pass

    cdef CarcharSetWrapper result = CarcharSetWrapper(len(values) * 2 + 8)
    cdef uint64_t[::1] hash_buf
    for val in values:
        if val is None:
            result.insert(_NULL_HASH)
            continue
        scalar_vec = _build_scalar(val, 1)
        if scalar_vec is None:
            raise TypeError(
                f"build_in_list_carchar: unsupported IN list value type {type(val).__name__!r}"
            )
        hash_buf = (<Vector>scalar_vec).hash()
        result.insert(hash_buf[0])
    return result


cdef BoolVector _vector_in_list_phash(
    Vector arr,
    PerfectHashSet phs,
    bint negate,
):
    """PerfectHashSet probe — raw integer values, no hashing.

    Returns None if the vector encoding is unsupported (caller falls back).
    """
    cdef Py_ssize_t n = len(arr)
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef int32_t* idx_buf = <int32_t*>malloc(n * sizeof(int32_t))
    if idx_buf == NULL:
        raise MemoryError()

    cdef Py_ssize_t count = 0
    cdef Py_ssize_t i
    cdef void* dp
    cdef IntegerVector ivec_int
    cdef Int64Vector ivec64
    cdef Date32Vector ivec_d32
    cdef TimestampVector ivec_ts

    # Narrow integers (Int8 / Int16): type-safe, no-null guaranteed path
    if isinstance(arr, IntegerVector):
        ivec_int = <IntegerVector>arr
        if ivec_int.null_bitmap_ptr() != NULL:
            free(idx_buf)
            return None  # has nulls → fall back
        dp = ivec_int.dense_ptr()
        if dp == NULL:
            free(idx_buf)
            return None  # non-dense encoding → fall back
        # probe_found gives us matched row indices; probe_not_found gives unmatched.
        # For negate we want bits SET for unmatched rows. Compute probe_found and invert.
        with nogil:
            if ivec_int.ptr.type == DRAKEN_INT8:
                count = phs.probe_found_32_i8(<const int8_t*>dp, idx_buf, n)
            else:
                count = phs.probe_found_32_i16(<const int16_t*>dp, idx_buf, n)

    elif isinstance(arr, Int64Vector):
        ivec64 = <Int64Vector>arr
        if ivec64.null_bitmap_ptr() != NULL:
            free(idx_buf)
            return None  # has nulls → fall back
        dp = ivec64.dense_ptr()
        if dp == NULL:
            free(idx_buf)
            return None  # dict/RLE/const → fall back
        with nogil:
            count = phs.probe_found_32_i64(<const int64_t*>dp, idx_buf, n)

    elif isinstance(arr, Date32Vector):
        ivec_d32 = <Date32Vector>arr
        if ivec_d32.null_bitmap_ptr() != NULL:
            free(idx_buf)
            return None
        dp = ivec_d32.dense_ptr()
        if dp == NULL:
            free(idx_buf)
            return None
        with nogil:
            count = phs.probe_found_32_i32(<const int32_t*>dp, idx_buf, n)

    elif isinstance(arr, TimestampVector):
        ivec_ts = <TimestampVector>arr
        if ivec_ts.null_bitmap_ptr() != NULL:
            free(idx_buf)
            return None
        dp = ivec_ts.dense_ptr()
        if dp == NULL:
            free(idx_buf)
            return None
        with nogil:
            count = phs.probe_found_32_i64(<const int64_t*>dp, idx_buf, n)

    else:
        free(idx_buf)
        return None  # unsupported vector type

    # Materialise the result bitmap from probe_found indices
    if negate:
        # All-1 except where found
        memset(dst, 0xFF, nbytes)
        if n & 7:
            dst[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
        for i in range(count):
            dst[idx_buf[i] >> 3] &= ~(<uint8_t>(1 << (idx_buf[i] & 7)))
    else:
        memset(dst, 0, nbytes)
        for i in range(count):
            dst[idx_buf[i] >> 3] |= <uint8_t>(1 << (idx_buf[i] & 7))

    free(idx_buf)
    return out


cpdef BoolVector vector_in_list(Vector arr, object set_obj, bint negate=False):
    """Row-wise IN-list membership test using a pre-built set.

    Dispatches to PerfectHashSet (direct-address, no hashing) when the set was
    built with PerfectHashSet and the column has a supported dense encoding.
    Falls back to CarcharSetWrapper (hash) path otherwise.

    If `negate` is True, the result is the row-wise NotInList.
    """
    cdef BoolVector result
    cdef PerfectHashSet phs
    cdef CarcharSetWrapper fallback
    cdef CarcharSetWrapper carchar
    cdef Py_ssize_t i, n
    cdef Py_ssize_t nbytes
    cdef BoolVector out
    cdef uint8_t* dst
    cdef uint64_t[::1] hashes

    if isinstance(set_obj, PerfectHashSet):
        result = _vector_in_list_phash(arr, <PerfectHashSet>set_obj, negate)
        if result is not None:
            return result
        # Fallback: PerfectHashSet path couldn't handle this column encoding.
        # Rehash the set into a CarcharSetWrapper and use the hash path.
        # This should be rare (nullable or non-dense column with a PerfectHashSet).
        phs = <PerfectHashSet>set_obj
        fallback = _phash_to_carchar(phs)
        set_obj = fallback

    carchar = <CarcharSetWrapper>set_obj
    n = len(arr)
    nbytes = (n + 7) >> 3
    out = BoolVector(<size_t>n)
    dst = <uint8_t*>out.ptr.data
    hashes = arr.hash()

    if negate:
        memset(dst, 0xFF, nbytes)
        if n & 7:
            dst[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
        for i in range(n):
            if carchar.contains(hashes[i]):
                dst[i >> 3] &= ~(1 << (i & 7))
    else:
        memset(dst, 0, nbytes)
        for i in range(n):
            if carchar.contains(hashes[i]):
                dst[i >> 3] |= (1 << (i & 7))

    return out


cdef CarcharSetWrapper _phash_to_carchar(PerfectHashSet phs):
    """Rebuild a CarcharSetWrapper from a PerfectHashSet for the fallback path.

    Only called when a PerfectHashSet can't handle the column encoding (e.g.,
    nullable or non-dense). Iterates the bit-array and hashes each stored value.
    """
    from draken.vectors.scalar_constructors import from_scalar as _build_scalar
    cdef CarcharSetWrapper result = CarcharSetWrapper(phs._range * 2 + 8)
    cdef uint64_t[::1] hash_buf
    cdef int64_t slot
    cdef uint64_t word, mask
    cdef Py_ssize_t w
    for w in range(phs._n_words):
        word = phs._words[w]
        if word == 0:
            continue
        for bit in range(64):
            mask = <uint64_t>1 << bit
            if word & mask:
                slot = <int64_t>w * 64 + bit
                val = phs._min_val + slot
                scalar_vec = _build_scalar(val, 1)
                hash_buf = (<Vector>scalar_vec).hash()
                result.insert(hash_buf[0])
    return result
