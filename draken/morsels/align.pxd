# cython: language_level=3

from libc.stdint cimport int32_t
from draken.morsels.morsel cimport Morsel


cpdef Morsel align_tables(
    Morsel source_morsel,
    Morsel append_morsel,
    int32_t[::1] source_indices,
    int32_t[::1] append_indices
)
