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
FileColumnStats — lazy Cython wrapper around a C++ vector[AggColumnStat].

Holds per-column min/max/null_count aggregated across all row groups of one
Parquet file. min_bytes and max_bytes are NOT decoded into Python values until
get_min() / get_max() is called, and decoded results are cached so each column
is decoded at most once per FileEntry.
"""

from libc.stdint cimport int64_t
from libcpp.vector cimport vector

from rugo.parquet_reader cimport AggColumnStat

from rugo.parquet import decode_value as _decode_value


cdef class FileColumnStats:
    """Lazy per-file column statistics from a Parquet footer.

    Wraps vector[AggColumnStat] (already aggregated across row groups by C++).
    Decoded Python values are materialised on first access per column and cached.
    """

    def __cinit__(self):
        self._name_to_idx = {}
        self._field_id_to_idx = {}
        self._min_cache = {}
        self._max_cache = {}

    cpdef void bind_schema(self, list column_names):
        """Map schema field_ids (positions) to stat vector indices.

        Called once at FileEntry build time while the schema is known.
        After this, get_min(field_id) / get_max(field_id) work.
        """
        cdef int field_id
        cdef str name
        cdef object idx
        self._field_id_to_idx = {}
        for field_id, name in enumerate(column_names):
            idx = self._name_to_idx.get(name)
            if idx is not None:
                self._field_id_to_idx[field_id] = idx

    cdef object _decode(self, int stat_idx, bint want_min):
        """Decode min_bytes or max_bytes for one stat entry into a Python value."""
        cdef AggColumnStat* s = &self._stats[stat_idx]
        if want_min:
            if not s.has_min:
                return None
            raw = s.min_bytes
        else:
            if not s.has_max:
                return None
            raw = s.max_bytes
        log_type = s.logical_type
        prefer_text = (log_type == b"json" or log_type.startswith(b"array<"))
        return _decode_value(s.physical_type, log_type, raw, prefer_text)

    cpdef object get_min(self, int field_id):
        """Return decoded min value for schema field_id, or None."""
        if field_id in self._min_cache:
            return self._min_cache[field_id]
        idx = self._field_id_to_idx.get(field_id)
        if idx is None:
            return None
        val = self._decode(idx, True)
        self._min_cache[field_id] = val
        return val

    cpdef object get_max(self, int field_id):
        """Return decoded max value for schema field_id, or None."""
        if field_id in self._max_cache:
            return self._max_cache[field_id]
        idx = self._field_id_to_idx.get(field_id)
        if idx is None:
            return None
        val = self._decode(idx, False)
        self._max_cache[field_id] = val
        return val

    cpdef object get_null_count(self, int field_id):
        """Return null count for field_id if complete, else None."""
        idx = self._field_id_to_idx.get(field_id)
        if idx is None:
            return None
        cdef AggColumnStat* s = &self._stats[<int>idx]
        if not s.null_count_complete:
            return None
        return s.null_count

    cpdef bint has_any_null_counts(self):
        """True if at least one column has a complete null count."""
        cdef size_t i
        for i in range(self._stats.size()):
            if self._stats[i].null_count_complete:
                return True
        return False

    cpdef tuple get_range_by_name(self, str name):
        """Return (min, max) decoded for a column by name, or None.

        Name-keyed access is stable after projection pushdown.
        """
        idx = self._name_to_idx.get(name)
        if idx is None:
            return None
        cdef int i = <int>idx
        mn = self._decode(i, True)
        mx = self._decode(i, False)
        if mn is None and mx is None:
            return None
        return (mn, mx)

    cpdef bint has_stats(self):
        """True if this object holds any column stats at all."""
        return self._stats.size() > 0


cdef FileColumnStats file_column_stats_from_agg(vector[AggColumnStat]& src):
    """Module-level factory: build FileColumnStats from an AggColumnStat vector."""
    cdef FileColumnStats obj = FileColumnStats.__new__(FileColumnStats)
    cdef size_t i
    obj._stats = src
    for i in range(src.size()):
        name = src[i].name.decode('utf-8')
        obj._name_to_idx[name] = <int>i
    return obj
