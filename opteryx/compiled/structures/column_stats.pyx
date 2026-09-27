# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
FileColumnStats — Cython wrapper around a C++ vector[AggColumnStat].

Holds per-column min/max/null_count aggregated across all row groups of one
Parquet file. The native manifest builder reads the aggregated vector directly
(NativeManifestBuilder.set_footer); Python reads only per-column sizes.
"""

from libc.stdint cimport int64_t
from libcpp.vector cimport vector

from rugo.parquet_reader cimport AggColumnStat


cdef class FileColumnStats:
    """Per-file column statistics from a Parquet footer.

    Wraps vector[AggColumnStat] (already aggregated across row groups by C++).
    """

    def __cinit__(self):
        self._name_to_idx = {}
        self._field_id_to_idx = {}

    cpdef void bind_schema(self, list column_names):
        """Map schema field_ids (positions) to stat vector indices.

        Called once while the schema is known; after it,
        get_uncompressed_size(field_id) works.
        """
        cdef int field_id
        cdef str name
        cdef object idx
        self._field_id_to_idx = {}
        for field_id, name in enumerate(column_names):
            idx = self._name_to_idx.get(name)
            if idx is not None:
                self._field_id_to_idx[field_id] = idx

    cpdef object get_uncompressed_size(self, int field_id):
        """Return total uncompressed byte size for field_id, or None if unknown.

        0 is a genuine "no data" signal here (AggColumnStat's default and what a
        row group with no matching leaf column contributes) -- unlike null_count,
        there's no separate completeness flag, so a column that never accumulated
        any size reports None rather than a misleading 0.
        """
        idx = self._field_id_to_idx.get(field_id)
        if idx is None:
            return None
        cdef AggColumnStat* s = &self._stats[<int>idx]
        if s.total_uncompressed_size <= 0:
            return None
        return s.total_uncompressed_size


cdef FileColumnStats file_column_stats_from_agg(vector[AggColumnStat]& src):
    """Module-level factory: build FileColumnStats from an AggColumnStat vector."""
    cdef FileColumnStats obj = FileColumnStats.__new__(FileColumnStats)
    cdef size_t i
    obj._stats = src
    for i in range(src.size()):
        name = src[i].name.decode('utf-8')
        obj._name_to_idx[name] = <int>i
    return obj
