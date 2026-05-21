# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# This file is `include`d into vector_ops.pyx. malloc / free / int32_t /
# memset etc. are already in scope via earlier-included leaves; we re-state
# them here only to make the file self-documenting. malloc /
# free are used in this file but resolved via the earlier-included
# leaves' cimport — declaring them here triggers "ambiguous overloaded
# method" in the consolidated build.
from libc.stdint cimport int32_t, int64_t, uint8_t, uint64_t
from libc.string cimport memset

from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.integer64_vector cimport Integer64Vector
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.perfect_hash_set cimport PerfectHashSet

# Null hash sentinel — same value as filter_join.pyx and the Draken hash machinery.
cdef uint64_t _NULL_HASH = <uint64_t>0x73d59cff8f94d86cULL

# Range cap: 256K slots (32 KB at 1 bit/slot — fits L1).
cdef int64_t _PERFECT_HASH_CAP = 262_144


def build_in_list_carchar(values):
    """Build a membership set from a Python list of IN-list literal values.

    Returns a PerfectHashSet when all values are non-null integers and the
    value range fits within 256K slots. Otherwise returns a CarcharSetWrapper
    (hashed path).
    """
    from draken.vectors.scalar_constructors import from_scalar as _build_scalar

    if values:
        try:
            if not any(v is None or isinstance(v, float) for v in values):
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


cpdef BoolVector vector_in_list(Vector arr, object set_obj, bint negate=False):
    """Row-wise IN-list membership test using a pre-built set.

    Always uses CarcharSetWrapper (hash) path. PerfectHashSet inputs are
    converted to CarcharSetWrapper via _phash_to_carchar.

    If `negate` is True, the result is the row-wise NotInList.
    """
    cdef CarcharSetWrapper carchar
    cdef Py_ssize_t i, n
    cdef Py_ssize_t nbytes
    cdef BoolVector out
    cdef uint8_t* dst
    cdef uint64_t[::1] hashes

    if isinstance(set_obj, PerfectHashSet):
        set_obj = _phash_to_carchar(<PerfectHashSet>set_obj)

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
