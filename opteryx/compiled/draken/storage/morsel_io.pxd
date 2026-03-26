from opteryx.compiled.draken.morsels.morsel cimport Morsel

cpdef object write_morsel(object path_or_handle, Morsel morsel, dict options=*)
cpdef Morsel read_morsel(object path_or_handle, dict options=*)
