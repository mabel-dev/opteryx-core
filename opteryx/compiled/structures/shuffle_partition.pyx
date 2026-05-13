# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport uint64_t
from libc.stdlib cimport calloc, free
from cython cimport view


cpdef tuple row_indexes_by_bin_flat(const uint64_t[::1] hashes,
                                    Py_ssize_t num_bins,
                                    int shift_bits=0):
    """
    Partition row indices into bins, returning a flat array of indices
    and an array of bin start offsets.

    Parameters
    ----------
    hashes : uint64[::1]
        Hash values for each row.
    num_bins : Py_ssize_t
        Number of bins (must be a power of two).
    shift_bits : int, optional
        Number of bits to right-shift before masking.

    Returns
    -------
    flat_indices : cython.view.array
        1‑D array of row indices, grouped by bin.
    bin_offsets : cython.view.array
        1‑D array of length num_bins+1, where bin_offsets[bin]
        is the start index in flat_indices for bin `bin`, and
        bin_offsets[bin+1] is the end+1.
    """
    cdef Py_ssize_t n_rows = hashes.shape[0]
    cdef Py_ssize_t i, bin_id, pos
    cdef uint64_t mask
    cdef Py_ssize_t* counts = NULL
    cdef Py_ssize_t* current = NULL
    cdef view.array flat_arr
    cdef view.array offsets_arr
    cdef Py_ssize_t[:] flat
    cdef Py_ssize_t[:] offsets
    # pointer aliases for loops
    cdef Py_ssize_t *counts_p
    cdef Py_ssize_t *curr_p
    cdef Py_ssize_t *flat_p
    cdef const uint64_t *hash_p
    cdef int sb

    # ----- input validation (Python calls, executed only once) -----
    if num_bins <= 0:
        raise ValueError("num_bins must be positive")
    if num_bins & (num_bins - 1):
        raise ValueError("num_bins must be a power of two")
    if shift_bits < 0:
        raise ValueError("shift_bits must be zero or positive")

    # ----- handle empty input -----
    if n_rows == 0:
        flat_arr = view.array(
            shape=(0,),
            itemsize=sizeof(Py_ssize_t),
            format="l",
        )
        offsets_arr = view.array(
            shape=(num_bins + 1,),
            itemsize=sizeof(Py_ssize_t),
            format="l",
        )
        offsets = offsets_arr
        for i in range(num_bins + 1):
            offsets[i] = 0
        return (flat_arr, offsets_arr)

    # ----- allocate temporary C arrays -----
    counts = <Py_ssize_t*>calloc(num_bins, sizeof(Py_ssize_t))
    current = <Py_ssize_t*>calloc(num_bins, sizeof(Py_ssize_t))
    if counts == NULL or current == NULL:
        if counts != NULL:
            free(counts)
        if current != NULL:
            free(current)
        raise MemoryError()

    try:
        mask = <uint64_t>(num_bins - 1)

        # fast locals for the upcoming loops
        counts_p = counts
        hash_p = &hashes[0]
        sb = shift_bits  # keep as local C int

        # ----- first pass: count rows per bin (no GIL) -----
        with nogil:
            for i in range(n_rows):
                # arithmetic is the only work; use pointer loads
                bin_id = <int>((hash_p[i] >> sb) & mask)
                counts_p[bin_id] += 1

        # ----- allocate the result arrays (Python objects, need GIL) -----
        flat_arr = view.array(
            shape=(n_rows,),
            itemsize=sizeof(Py_ssize_t),
            format="l",
        )
        offsets_arr = view.array(
            shape=(num_bins + 1,),
            itemsize=sizeof(Py_ssize_t),
            format="l",
        )
        flat = flat_arr
        offsets = offsets_arr

        offsets[0] = 0
        # reuse locals for the second nogil section
        curr_p = current
        flat_p = &flat[0]

        with nogil:
            # ----- build prefix sums (offsets) and init current -----
            for i in range(num_bins):
                offsets[i + 1] = offsets[i] + counts_p[i]
                curr_p[i] = offsets[i]

            # ----- second pass: fill the flat array (no GIL) -----
            for i in range(n_rows):
                bin_id = <int>((hash_p[i] >> sb) & mask)
                pos = curr_p[bin_id]
                flat_p[pos] = i
                curr_p[bin_id] = pos + 1

        return (flat_arr, offsets_arr)

    finally:
        free(counts)
        free(current)
