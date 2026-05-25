# Stub .pxd for draken.morsels.align.

from libc.stdint cimport int32_t

from draken.morsels.morsel cimport Morsel


cpdef Morsel align_tables(
    Morsel source_morsel,
    Morsel append_morsel,
    const int32_t[::1] source_indices,
    const int32_t[::1] append_indices,
)
