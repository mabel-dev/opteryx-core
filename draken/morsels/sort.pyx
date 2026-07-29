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

"""draken.morsels.sort

Python-callable entrypoint over the ONE sort implementation — draken/morsels/sort.hpp:
a vergesort run-detection prepass, falling back to a comparison sort (parallel
std::stable_sort over the AoS short-circuit comparator, or plain per-column
SortKeyCmp for 5+ key columns). No sort logic lives in this file. It exists only
to marshal a Python Morsel list into that pure-C++ core and back — the same
core src/cpp/engine/native_sort.hpp re-exports for opteryx's SortSink/TopNSink,
and directly usable here with no opteryx/query-engine dependency at all (rugo's
use case).

    morsels_out = sort_morsels(morsels_in, column_names, ascending, limit=None)
    perm        = morsel_sort(morsel, column_names, ascending)   # single-morsel,
                                                                  # permutation only
"""

from array import array

from libc.stddef cimport size_t
from libc.stdint cimport uint32_t
from libc.string cimport memcpy
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr

from draken.morsels.cxx_morsel cimport CxxMorsel, ErrCtx
from draken.morsels.morsel cimport Morsel, morsel_to_cxx, cxx_to_morsel


cdef extern from "morsels/sort.hpp" nogil:
    cdef cppclass SortKeySpec:
        size_t col_idx
        bint ascending

    cdef cppclass SortKeyColumn:
        SortKeyColumn()

    bint build_sort_keys(
        const vector[shared_ptr[CxxMorsel]]& ms,
        const vector[SortKeySpec]& spec,
        size_t n,
        vector[SortKeyColumn]& out,
        ErrCtx& err,
    ) nogil

    void sort_perm(
        const vector[SortKeyColumn]& keys,
        vector[uint32_t]& perm,
        size_t take_first,
    ) nogil

    # Aliased to avoid colliding with this module's own `def sort_morsels` below —
    # this IS the C++ implementation; the Python def is a thin wrapper over it.
    bint c_sort_morsels "sort_morsels"(
        const vector[shared_ptr[CxxMorsel]]& ms,
        const vector[SortKeySpec]& spec,
        size_t take_first,
        size_t chunk_rows,
        vector[shared_ptr[CxxMorsel]]& out,
        ErrCtx& err,
    ) nogil


cdef size_t SIZE_MAX_C = <size_t>-1


cdef vector[SortKeySpec] _resolve_spec(list names, list column_names, list ascending) except *:
    """Resolve ORDER BY column names to positional indices against `names`
    (a Morsel's _col_names) and pack into the C++ spec vector. Pure lookup —
    no sort logic — done once per call under the GIL, never per row."""
    if len(column_names) != len(ascending):
        raise ValueError("column_names and ascending must have the same length")
    if not column_names:
        raise ValueError("at least one sort column is required")

    cdef vector[SortKeySpec] spec
    cdef SortKeySpec item
    cdef bytes key_bytes
    cdef list names_bytes = [n if isinstance(n, bytes) else n.encode() for n in names]

    for name, asc in zip(column_names, ascending):
        key_bytes = name if isinstance(name, bytes) else name.encode()
        try:
            idx = names_bytes.index(key_bytes)
        except ValueError:
            raise ValueError(f"unknown sort column {name!r}")
        item.col_idx = <size_t>idx
        item.ascending = bool(asc)
        spec.push_back(item)
    return spec


def sort_morsels(list morsels, list column_names, list ascending, limit=None,
                  size_t chunk_rows=131072):
    """
    Sort rows across one or more Morsels through the shared sort core.

    Parameters
    ----------
    morsels : list[Morsel]
        Input morsels, in any order; rows are sorted across all of them together.
    column_names : list[bytes | str]
        ORDER BY columns, most significant first.
    ascending : list[bool]
        Sort direction per column; True = ascending, False = descending.
    limit : int | None
        Keep only the first `limit` rows after sorting (TopN fusion). None = full sort.
    chunk_rows : int
        Max rows per output Morsel.

    Returns
    -------
    list[Morsel]
        Sorted rows, gathered into dense output morsels of at most `chunk_rows` rows.
    """
    if not morsels:
        return []

    cdef Morsel first = morsels[0]
    cdef vector[SortKeySpec] spec = _resolve_spec(first._col_names, column_names, ascending)

    cdef vector[shared_ptr[CxxMorsel]] cxx_in
    cdef Morsel m
    for m in morsels:
        cxx_in.push_back(morsel_to_cxx(m))

    cdef size_t take_first = SIZE_MAX_C if limit is None else <size_t>limit
    cdef vector[shared_ptr[CxxMorsel]] cxx_out
    cdef ErrCtx err
    cdef bint ok

    with nogil:
        ok = c_sort_morsels(cxx_in, spec, take_first, chunk_rows, cxx_out, err)

    if not ok:
        raise ValueError(err.msg.decode() if err.msg != NULL else "sort failed")

    cdef size_t i
    result = []
    for i in range(cxx_out.size()):
        result.append(cxx_to_morsel(cxx_out[i]))
    return result


cpdef morsel_sort(Morsel morsel, list column_names, list ascending):
    """
    Compute a sort permutation for a SINGLE Morsel — kept for callers that only
    want the permutation (cheaper than a full gather-into-new-Morsels when
    there's exactly one input morsel). Uses the same build_sort_keys/sort_perm
    core as sort_morsels above; no separate logic.

    Returns
    -------
    array('i')
        int32 permutation: result[i] is the original row index for sorted
        position i. Apply with ``morsel.take(perm)``.
    """
    cdef vector[SortKeySpec] spec = _resolve_spec(morsel._col_names, column_names, ascending)

    cdef vector[shared_ptr[CxxMorsel]] cxx_in
    cxx_in.push_back(morsel_to_cxx(morsel))

    cdef size_t n = <size_t>morsel.num_rows
    if n == 0:
        return array("i")

    cdef vector[SortKeyColumn] keys
    cdef ErrCtx err
    cdef bint ok
    cdef vector[uint32_t] perm
    perm.resize(n)

    cdef size_t i
    with nogil:
        ok = build_sort_keys(cxx_in, spec, n, keys, err)
        if ok:
            for i in range(n):
                perm[i] = <uint32_t>i
            sort_perm(keys, perm, SIZE_MAX_C)

    if not ok:
        raise ValueError(err.msg.decode() if err.msg != NULL else "sort failed")

    result = array("i", b"\x00" * (n * sizeof(uint32_t)))
    cdef int[::1] rv = result
    memcpy(&rv[0], &perm[0], n * sizeof(uint32_t))
    return result
