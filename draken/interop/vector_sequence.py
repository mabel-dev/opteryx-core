"""vector_from_sequence — re-export from draken_native nanobind module."""
from draken.draken_native import vector_from_sequence


def bool_vector_from_uint64_eq(row_hashes_view, target_hash):
    raise NotImplementedError("bool_vector_from_uint64_eq not implemented in E.24 shim")


__all__ = ["vector_from_sequence", "bool_vector_from_uint64_eq"]
