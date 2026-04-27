from libc.stdint cimport int64_t
from opteryx.compiled.structures.memory_pool cimport MemoryPool

cpdef object deserialize_column(int64_t ref_id, MemoryPool pool)
cpdef dict deserialize_row_group(dict ref_ids, MemoryPool pool)
