# Cython declaration file for ColumnDeserializer

from opteryx.compiled.structures.memory_pool cimport MemoryPool
from opteryx.compiled.structures.column_descriptor cimport ColumnDescriptor

cpdef dict deserialize_column(ColumnDescriptor descriptor, MemoryPool pool)
cpdef dict deserialize_row_group(dict column_descriptors, MemoryPool pool)
