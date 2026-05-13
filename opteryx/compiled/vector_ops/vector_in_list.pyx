# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport uint8_t, uint64_t
from libc.string cimport memset

from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper

# Null hash sentinel — same value as filter_join.pyx and the Draken hash machinery.
# Computed as mix_hash(0, raw_NULL_HASH) where raw_NULL_HASH = 0x4c3f95a36ab8ecca.
cdef uint64_t _NULL_HASH = <uint64_t>0x73d59cff8f94d86cULL


def build_in_list_carchar(values):
    """Build a CarcharSetWrapper from a Python list of IN-list literal values.

    Called once at plan time. Each value is hashed via the same Draken hash
    machinery used by the column vectors, so hashes are directly comparable
    at evaluation time.
    """
    from draken.vectors.scalar_constructors import from_scalar as _build_scalar

    cdef CarcharSetWrapper result = CarcharSetWrapper(len(values) * 2 + 8)
    cdef uint64_t[::1] hash_buf
    cdef uint64_t h

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


cpdef BoolVector vector_in_list(Vector arr, CarcharSetWrapper carchar):
    """Row-wise IN-list membership test using a pre-built CarcharSetWrapper.

    Hashes each element of arr and probes the set. O(n) with O(1) probe cost.
    Works for any Draken Vector type — hash consistency is guaranteed because
    both the set and the column use the same Draken hash_into machinery.
    """
    cdef Py_ssize_t i, n = len(arr)
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint64_t[::1] hashes

    memset(dst, 0, nbytes)

    hashes = arr.hash()

    for i in range(n):
        if carchar.contains(hashes[i]):
            dst[i >> 3] |= (1 << (i & 7))

    return out
