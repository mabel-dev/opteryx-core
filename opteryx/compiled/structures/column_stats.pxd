# column_stats.pxd — typed Cython interface for FileColumnStats
from libcpp.vector cimport vector
from libcpp.string cimport string
from libc.stdint cimport int64_t

from rugo.parquet_reader cimport AggColumnStat


cdef class FileColumnStats:
    cdef vector[AggColumnStat] _stats       # one entry per column, aggregated across row groups
    cdef dict _name_to_idx                  # {str name: int index into _stats}
    cdef dict _field_id_to_idx              # {int field_id: int index into _stats}, set by bind_schema

    cpdef void bind_schema(self, list column_names)
    cpdef object get_uncompressed_size(self, int field_id)


cdef FileColumnStats file_column_stats_from_agg(vector[AggColumnStat]& src)
