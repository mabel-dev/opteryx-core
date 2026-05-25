# cython: language_level=3
# Cython shim for draken.morsels.align — E.24 vtable bridge.

from libc.stdint cimport int32_t

from draken.morsels.morsel cimport Morsel


cpdef Morsel align_tables(
    Morsel source_morsel,
    Morsel append_morsel,
    const int32_t[::1] source_indices,
    const int32_t[::1] append_indices,
):
    raise NotImplementedError("align_tables not implemented in E.24 shim")
