from libc.stdint cimport int64_t
from opteryx.compiled.structures.memory_pool cimport MemoryPool
from draken.core.buffers cimport DrakenType

cpdef object deserialize_column(int64_t ref_id, MemoryPool pool, DrakenType want_string_type=*)
cpdef dict deserialize_row_group(dict ref_ids, MemoryPool pool, dict string_types=*)
