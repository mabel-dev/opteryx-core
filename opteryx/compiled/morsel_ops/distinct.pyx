# cython: language_level=3
# distutils: language = c++
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

"""opteryx.compiled.morsel_ops.distinct

Carchar-backed DISTINCT for Draken Morsels.

    indices, seen_hashes = distinct(morsel, seen_hashes=None, columns=None)

Returns a uint32 array of row indices to keep, plus the (possibly new)
CarcharSetWrapper so the caller can persist state for streaming DISTINCT.

All hash-table probing and index-building runs without the GIL; the only
Python-level work is the single morsel.hash() call (one per batch) and
object creation on the first call.
"""

from array import array

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint8_t, uint32_t, uint64_t
from libc.stddef cimport size_t

from opteryx.draken.morsels.morsel cimport Morsel


# ── CarcharSet C++ binding ────────────────────────────────────────────────────
# Headers are on the include path at third_party/mabel/carchar/.

cdef extern from "carchar_set.hpp" namespace "opteryx::carchar" nogil:
    cdef cppclass CarcharSet:
        CarcharSet(size_t initial_capacity, double load_factor) except +
        void reserve(size_t expected_entries)
        size_t mark_new(const uint64_t* keys, uint8_t* out_is_new, size_t length) noexcept
        size_t size() noexcept


# ── Python-visible wrapper (holds state across streaming calls) ───────────────

cdef class CarcharSetWrapper:
    """Persistent Carchar hash set for streaming DISTINCT.

    Wraps a heap-allocated CarcharSet so the set survives across morsel
    boundaries.  Passed back to the caller and threaded through subsequent
    calls via the seen_hashes parameter.
    """
    cdef CarcharSet* _ptr

    def __cinit__(self, size_t initial_capacity=2048):
        self._ptr = new CarcharSet(initial_capacity, 0.80)

    def __dealloc__(self):
        del self._ptr

    def __len__(self):
        return <Py_ssize_t>self._ptr.size()


# ── Public API ────────────────────────────────────────────────────────────────

def distinct(Morsel morsel, CarcharSetWrapper seen_hashes, list columns=None):
    """
    Compute distinct indices for a Draken Morsel using Carchar SIMD hashing.

    Parameters
    ----------
    morsel : Morsel
        The morsel whose rows are to be de-duplicated.
    seen_hashes : CarcharSetWrapper
        Set that accumulates seen row hashes; mutated in place.  Create once
        with ``CarcharSetWrapper()`` and reuse across morsels for streaming
        DISTINCT.
    columns : list of bytes, optional
        Column names (as bytes) to include in the row hash.  Uses all
        columns when None.

    Returns
    -------
    array('I')
        uint32 row indices of rows to keep (first occurrence of each distinct
        key).  Empty array when all rows are duplicates.
    """
    cdef uint64_t[::1] row_hashes
    cdef Py_ssize_t n
    cdef CarcharSet* cs
    cdef uint64_t* hashes_ptr
    cdef uint8_t* mask
    cdef size_t count
    cdef uint32_t* out_ptr
    cdef Py_ssize_t i
    cdef Py_ssize_t j
    cdef unsigned int[::1] rv

    # Get per-row hashes from the morsel (one Python call per batch).
    if columns is None:
        row_hashes = morsel.hash()
    else:
        row_hashes = morsel.hash(columns=columns)

    n = row_hashes.shape[0]

    if n == 0:
        return array("I")

    cs = seen_hashes._ptr
    hashes_ptr = &row_hashes[0]

    # Allocate a per-row boolean mask on the heap (avoids any Python object).
    mask = <uint8_t*>PyMem_Malloc(<size_t>n)
    if mask == NULL:
        raise MemoryError()

    try:
        # ── Hot path: nogil ───────────────────────────────────────────────────
        # mark_new writes 1 for new keys, 0 for duplicates, returns new-key count.
        with nogil:
            count = cs.mark_new(hashes_ptr, mask, <size_t>n)

        if count == 0:
            return array("I")

        # Branchless scatter: write i unconditionally, advance j only when new.
        # mask[i] is 0 or 1, so true==1 / false==0 eliminates the branch.
        # When j reaches count the stray writes land at out_ptr[count]; allocate
        # n slots (not count) so that overshoot stays in-bounds.  Trim afterward.
        result = array("I", bytes(n * sizeof(uint32_t)))
        rv = result
        out_ptr = <uint32_t*>&rv[0]
        j = 0

        with nogil:
            for i in range(n):
                out_ptr[j] = <uint32_t>i
                j += <Py_ssize_t>mask[i]

        del result[j:]   # trim over-allocated tail (the "special last step")
        return result

    finally:
        PyMem_Free(mask)
