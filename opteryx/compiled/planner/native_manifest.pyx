# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: auto_pickle=False

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
A relation's manifest as native rows (src/cpp/planner/native_manifest.hpp).

`decode_manifest_parquet` reads a manifest parquet - the format the catalog and
core both write - with rugo and decodes its columns, straight from the draken
vectors rugo produced, into a NativeManifest: one row per data file and, per
file, a cell per column of the schema it is loaded against, keyed by that
column's load-time position (native plan graph Q8, architect rulings
2026-09-27). The whole-column sketch vectors stay draken vectors, held here.
"""

from cpython.ref cimport PyObject
from cython.operator cimport dereference as deref
from cpython.ref cimport Py_DECREF
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr
from libcpp.optional cimport optional
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector

from draken.draken_native import DrakenType as _PyDrakenType
from draken.draken_native import Vector as _NativeVector

_INT64 = _PyDrakenType.INT64

from opteryx.compiled.planner.column_table cimport ColumnRows
from opteryx.compiled.planner.column_table cimport ColumnTable
from opteryx.compiled.planner.column_type cimport ColumnTypeTable
from opteryx.compiled.planner.column_type cimport column_type_table
from opteryx.compiled.structures.column_stats cimport FileColumnStats
from opteryx.compiled.structures.expressions cimport ExprArena
from opteryx.compiled.structures.expressions cimport ExprTable
from rugo.parquet_reader cimport AggColumnStat


cdef extern from "core/buffers.h":
    ctypedef enum DrakenType:
        DRAKEN_INT64
        DRAKEN_UINT64
        DRAKEN_VARCHAR
        DRAKEN_ARRAY

    ctypedef struct DrakenVector:
        uint32_t length
        DrakenType type


cdef extern from "core/string_slot.h":
    ctypedef struct DrakenStringSlot:
        pass


cdef extern from "core/vector_owner.h":
    cdef cppclass VectorOwner:
        pass


cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj) except NULL
    const VectorOwner* draken_owner_unwrap(PyObject* obj) except NULL
    PyObject* draken_vector_own_raw(void* data, uint8_t* validity, uint32_t length, DrakenType type) except NULL
    PyObject* draken_vector_own_string(DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
                                       uint8_t* validity, uint32_t length, DrakenType type) except NULL
    PyObject* draken_vector_own_array_child(int32_t* parent_offsets, PyObject* child_obj,
                                            uint8_t* parent_validity, uint32_t length) except NULL
    const DrakenVector* draken_array_child_unwrap(PyObject* obj) except NULL
    const DrakenVector* draken_array_grandchild_unwrap(PyObject* obj) except NULL


cdef extern from "planner/manifest_sketch.hpp" namespace "opteryx::planner":
    cdef cppclass NestedArrayView:
        NestedArrayView()
        NestedArrayView(const DrakenVector* outer, const DrakenVector* mid, const DrakenVector* leaf)
        cbool present()


cdef extern from "planner/native_manifest.hpp" namespace "opteryx::planner":
    cdef int64_t kUnknown
    cdef int64_t kNoBound

    cdef enum DecodedTag:
        DECODED_NONE
        DECODED_INT64
        DECODED_DOUBLE
        DECODED_TEXT
        DECODED_BYTES
        DECODED_DECIMAL
        DECODED_BOOL
        DECODED_OTHER
        DECODED_UINT64

    cdef cppclass Bounds:
        int64_t min_ordinal
        int64_t max_ordinal
        DecodedTag min_tag
        DecodedTag max_tag
        int64_t min_int
        int64_t max_int
        double min_double
        double max_double
        int32_t min_scale
        int32_t max_scale
        string min_text
        string max_text

    cdef cppclass FooterStats:
        Bounds bounds
        int64_t null_count
        int64_t distinct_count
        int64_t uncompressed_size

    cdef cppclass ManifestCell:
        Bounds bounds
        int64_t null_count
        int64_t min_length
        int64_t max_length
        int64_t char_total_bytes
        int64_t uncompressed_size
        int64_t distinct_count
        cbool distinct_exact
        int64_t distinct_floor
        cbool has_distinct_sketch
        vector[uint64_t] distinct_sketch
        FooterStats footer
        int64_t element_min
        int64_t element_max
        vector[uint64_t] element_min_k

    cdef cppclass ManifestFile:
        string path
        string format
        int64_t record_count
        int64_t file_size
        int64_t uncompressed_size
        int64_t row_group_count
        int64_t histogram_bins
        int64_t deleted_record_count
        string delete_file_path
        cbool delete_positions_resolved
        vector[int64_t] delete_positions
        int32_t distinct_sketch_family
        cbool has_footer
        uint32_t vector_row

    cdef cppclass OwnedNested:
        pass

    cdef cppclass SketchStaging:
        cbool carry(const NestedArrayView& view, uint32_t vector_row, size_t row, size_t columns)
        void carry_keyed(const NestedArrayView& view, uint32_t vector_row, size_t row, const vector[int64_t]& positions)
        cbool clear(size_t row, size_t column)
        cbool any_values()
        vector[optional[vector[uint64_t]]]& row_of(size_t row, size_t columns) except +
        shared_ptr[const OwnedNested] build(size_t rows, DrakenType leaf_type) except +

    cdef cppclass CNativeManifest "opteryx::planner::NativeManifest":
        CNativeManifest(vector[string] columns, cbool bounds_are_ordinal, cbool stats_are_authoritative)
        void set_bounds_are_ordinal(cbool ordinal)
        size_t add_file(ManifestFile file) except +
        size_t file_count()
        size_t column_count()
        cbool bounds_are_ordinal()
        cbool stats_are_authoritative()
        const unordered_map[string, size_t]& positions()
        int64_t find_file(const string& path)
        size_t add_file_cells_from(const CNativeManifest& src, size_t row, const vector[int64_t]& positions) except +
        void own_sketches(shared_ptr[const OwnedNested] k, shared_ptr[const OwnedNested] h,
                          shared_ptr[const OwnedNested] c)
        NestedArrayView min_k
        NestedArrayView histogram
        NestedArrayView char_class
        ManifestFile& file(size_t row) except +
        ManifestCell& cell(size_t row, size_t column) except +
        int64_t record_count()
        int64_t total_size()
        size_t resident_bytes()


    void set_ordinal_bound(Bounds& b, DrakenType physical, cbool is_min, int64_t ordinal)
    void set_int_bound(Bounds& b, DrakenType physical, cbool is_min, int64_t value)
    void set_uint_bound(Bounds& b, cbool is_min, uint64_t value)
    void set_double_bound(Bounds& b, cbool is_min, double value)
    void set_bytes_bound(Bounds& b, cbool is_min, cbool is_text, string value)
    void set_decimal_bound(Bounds& b, cbool is_min, int64_t unscaled, int32_t scale, double value)
    void set_bool_bound(Bounds& b, cbool is_min, cbool value)


cdef extern from "planner/manifest_estimates.hpp" namespace "opteryx::planner":
    cdef cppclass End "opteryx::planner::estimate_detail::End":
        DecodedTag tag
        int64_t i
        double d
        int32_t scale
        const string* text

    cdef cppclass HistogramPart:
        size_t begin
        size_t end
        double lo
        double hi

    int64_t row_group_count(const CNativeManifest& m)
    cbool has_deletes(const CNativeManifest& m)
    cbool ordinal_bounds(const CNativeManifest& m, size_t position, int64_t& lo, int64_t& hi) except +
    cbool length_bounds(const CNativeManifest& m, size_t position, int64_t& lo, int64_t& hi) except +
    cbool char_class_stats(const CNativeManifest& m, size_t position, int64_t (&totals)[8], int64_t& non_null_rows) except +
    void histogram_parts(const CNativeManifest& m, size_t position, vector[int64_t]& counts, vector[HistogramPart]& parts) except +
    int64_t exact_cardinality_from_footers(const CNativeManifest& m, size_t position) except +
    int64_t cardinality_from_sketches(const CNativeManifest& m, size_t position) except +
    int64_t estimate_cardinality(const CNativeManifest& m, size_t position, double& estimate) except +
    int64_t estimate_range_cardinality(const CNativeManifest& m, size_t position, cbool identity_category) except +
    int64_t total_null_count(const CNativeManifest& m, size_t position) except +
    cbool null_fraction(const CNativeManifest& m, size_t position, double& fraction) except +
    int64_t total_uncompressed_size(const CNativeManifest& m, size_t position) except +
    cbool value_range(const CNativeManifest& m, size_t position, cbool identity_category, End& min_value, End& max_value) except +
    cbool extreme_ends(const CNativeManifest& m, size_t position, End& min_value, End& max_value) except +
    cbool has_null_counts(const CNativeManifest& m) except +


cdef extern from "planner/predicate_bounds.hpp" namespace "opteryx::planner":
    cdef cppclass NodeKinds:
        int32_t and_ "and_"
        int32_t or_ "or_"
        int32_t dnf
        int32_t cnf
        int32_t comparison
        int32_t binary
        int32_t unary
        int32_t function
        int32_t identifier
        int32_t nested
        int32_t literal
        int32_t between

    cdef cppclass DeriveInputs:
        const ExprTable* exprs
        const ColumnRows* columns
        const ColumnTypeTable* types
        const NodeKinds* kinds
        const unordered_map[string, uint32_t]* live_types


    cdef enum BKind "opteryx::planner::BKind":
        BKIND_INT "opteryx::planner::BKind::INT"
        BKIND_FLOAT "opteryx::planner::BKind::FLOAT"
        BKIND_BYTES "opteryx::planner::BKind::BYTES"
        BKIND_BOOL "opteryx::planner::BKind::BOOL"
        BKIND_OPAQUE "opteryx::planner::BKind::OPAQUE"

    cdef cppclass BVal:
        BKind kind
        double d
        string bytes

    cdef enum TermOp:
        OP_EQ
        OP_NOTEQ
        OP_GT
        OP_GTEQ
        OP_LT
        OP_LTEQ
        OP_BETWEEN
        OP_NONE

    cdef cppclass BoundTerm:
        string column
        TermOp op
        BVal value
        BVal upper
        cbool derived

    cdef cppclass NullTerm:
        string column
        cbool requires_null

    cdef cppclass FoldTerm:
        string column
        cbool lower
        vector[BoundTerm] terms

    vector[int64_t] split_conjuncts(const DeriveInputs& inputs, const vector[int64_t]& predicates) except +
    vector[BoundTerm] derive_bound_terms(const DeriveInputs& inputs, const vector[int64_t]& conjuncts) except +
    vector[NullTerm] derive_null_terms(const DeriveInputs& inputs, const vector[int64_t]& conjuncts) except +
    vector[FoldTerm] derive_fold_terms(const DeriveInputs& inputs, const vector[int64_t]& conjuncts) except +


cdef extern from *:
    """
    // A BVal's integer as two words, for the Python int it becomes.
    static inline int64_t bval_int_hi(const opteryx::planner::BVal& v) { return static_cast<int64_t>(v.i >> 64); }
    static inline uint64_t bval_int_lo(const opteryx::planner::BVal& v) { return static_cast<uint64_t>(v.i); }
    """
    int64_t bval_int_hi(const BVal& v)
    uint64_t bval_int_lo(const BVal& v)


cdef extern from "planner/manifest_prune.hpp" namespace "opteryx::planner":
    cdef cppclass PruneColumns:
        const unordered_map[string, size_t]* position
        const unordered_map[string, uint32_t]* live_types
        const ColumnTypeTable* types

    cdef cppclass ZoneTerm:
        string column
        uint8_t op
        # the ordinal is an int128; read through zone_ordinal_*

    vector[size_t] prune(const CNativeManifest& m, const DeriveInputs& inputs, const PruneColumns& columns,
                         const vector[int64_t]& predicates) except +
    vector[ZoneTerm] zone_terms(const CNativeManifest& m, const DeriveInputs& inputs, const PruneColumns& columns,
                                const vector[int64_t]& predicates) except +
    vector[size_t] prune_files_for_topn(const CNativeManifest& m, const PruneColumns& columns, const string& column,
                                        cbool descending, int64_t limit) except +
    cbool file_key_range(const CNativeManifest& m, size_t row, size_t position, End& lo, End& hi) except +


cdef extern from *:
    """
    // An int128 zone-term ordinal as two words, for the Python int it becomes.
    static inline int64_t zone_ordinal_hi(const opteryx::planner::ZoneTerm& t) {
        return static_cast<int64_t>(t.ordinal >> 64);
    }
    static inline uint64_t zone_ordinal_lo(const opteryx::planner::ZoneTerm& t) {
        return static_cast<uint64_t>(t.ordinal);
    }
    """
    int64_t zone_ordinal_hi(const ZoneTerm& t)
    uint64_t zone_ordinal_lo(const ZoneTerm& t)


cdef extern from "planner/manifest_encode.hpp" namespace "opteryx::planner":
    cdef cppclass EncodedScalar:
        void* data
        uint8_t* validity

    cdef cppclass EncodedStrings:
        DrakenStringSlot* slots
        uint8_t* arena
        size_t arena_len

    cdef cppclass EncodedList:
        int depth
        int32_t* offsets
        int32_t* mid_offsets
        uint8_t* mid_validity
        uint32_t mid_length
        void* leaf
        uint8_t* leaf_validity
        uint32_t leaf_length
        DrakenType leaf_type

    cdef cppclass EncodedManifest:
        uint32_t rows
        EncodedStrings file_path
        EncodedStrings file_format
        EncodedStrings delete_file_path
        uint8_t* delete_file_path_validity
        EncodedScalar deleted_record_count
        EncodedScalar record_count
        EncodedScalar file_size
        EncodedScalar uncompressed_size
        EncodedScalar histogram_bins
        EncodedList column_sizes
        EncodedList null_counts
        EncodedList min_k
        EncodedList histogram_counts
        EncodedList min_values
        EncodedList max_values
        EncodedList field_ids
        EncodedList min_lengths
        EncodedList max_lengths
        EncodedList char_class_counts
        EncodedList char_total_bytes
        EncodedList distinct_counts
        EncodedList element_min_values
        EncodedList element_max_values
        EncodedList element_min_k_hashes

    EncodedManifest encode_manifest(const CNativeManifest& m, const vector[int64_t]& field_ids) except +


cdef extern from "planner/file_stats.hpp" namespace "opteryx::planner":
    cdef cppclass FileStatsAccumulator:
        FileStatsAccumulator(vector[DrakenType] physical)
        size_t column_count()
        int64_t uncompressed_size()
        void add(size_t position, const VectorOwner& owner) except +
        void write(CNativeManifest& m, size_t row, vector[vector[uint64_t]]& min_k,
                   vector[vector[int64_t]]& histogram, vector[vector[int64_t]]& char_class) except +


cdef extern from *:
    """
    // A file's sketch slices (one per column, empty when it has none) into a
    // builder's staging row. int64 counts are staged as their bits.
    static inline void stage_u64(opteryx::planner::SketchStaging& s, size_t row,
                                 const std::vector<std::vector<uint64_t>>& slices) {
        auto& out = s.row_of(row, slices.size());
        for (size_t k = 0; k < slices.size(); ++k) out[k] = slices[k];
    }
    static inline void stage_i64(opteryx::planner::SketchStaging& s, size_t row,
                                 const std::vector<std::vector<int64_t>>& slices) {
        auto& out = s.row_of(row, slices.size());
        for (size_t k = 0; k < slices.size(); ++k) {
            out[k] = std::vector<uint64_t>(slices[k].begin(), slices[k].end());
        }
    }
    """
    void stage_u64(SketchStaging& s, size_t row, const vector[vector[uint64_t]]& slices) except +
    void stage_i64(SketchStaging& s, size_t row, const vector[vector[int64_t]]& slices) except +


cdef extern from "skene/format.h" namespace "skene":
    cdef uint32_t kStatNdv "skene::kStatNdv"
    cdef uint32_t kStatNdvExact "skene::kStatNdvExact"

    cdef cppclass SkeneColumnStatistics "skene::ColumnStatistics":
        uint32_t flags
        int64_t min_ordinal
        int64_t max_ordinal
        uint64_t null_count
        uint64_t ndv


cdef extern from "skene/reader.h" namespace "skene":
    cdef cppclass SkeneRowGroupColumnStatistics "skene::RowGroupColumnStatistics":
        cbool present
        SkeneColumnStatistics statistics

    cdef cppclass SkeneRowGroupSummary "skene::RowGroupSummary":
        uint64_t row_count
        vector[SkeneRowGroupColumnStatistics] column_statistics

    cdef cppclass SkeneColumnSketch "skene::ColumnSketch":
        uint8_t hash_family
        uint32_t k
        vector[uint64_t] hashes

    cdef cppclass SkeneFileMetadata "skene::FileMetadata":
        uint64_t row_count
        vector[SkeneRowGroupSummary] row_groups
        vector[SkeneColumnSketch] sketches


cdef extern from "planner/skene_stats.hpp" namespace "opteryx::planner":
    cdef cppclass SkeneApplied:
        cbool any_bounds
        cbool any_nulls

    string read_skene_footer_into(CNativeManifest& m, size_t row, const void* file, size_t bytes,
                                  const vector[DrakenType]& physical, SkeneApplied& applied) except +
    SkeneApplied apply_skene_footer(CNativeManifest& m, size_t row, const SkeneFileMetadata& meta,
                                    const vector[DrakenType]& physical, const vector[int64_t]& positions) except +


cdef extern from "planner/manifest_footer.hpp" namespace "opteryx::planner":
    void apply_footer_stat(const AggColumnStat& stat, ManifestCell& cell) except +


cdef extern from "planner/manifest_decode.hpp" namespace "opteryx::planner":
    cdef cppclass ManifestArrayColumn:
        const DrakenVector* outer
        const DrakenVector* child
        const DrakenVector* grandchild

    cdef cppclass ManifestColumnsIn:
        const DrakenVector* file_path
        const DrakenVector* file_format
        const DrakenVector* record_count
        const DrakenVector* file_size
        const DrakenVector* uncompressed_size
        const DrakenVector* histogram_bins
        const DrakenVector* deleted_record_count
        const DrakenVector* delete_file_path
        ManifestArrayColumn column_uncompressed_sizes
        ManifestArrayColumn null_counts
        ManifestArrayColumn min_values
        ManifestArrayColumn max_values
        ManifestArrayColumn min_lengths
        ManifestArrayColumn max_lengths
        ManifestArrayColumn field_ids
        ManifestArrayColumn char_total_bytes
        ManifestArrayColumn distinct_counts
        ManifestArrayColumn element_min_values
        ManifestArrayColumn element_max_values
        ManifestArrayColumn element_min_k_hashes

    cdef cppclass ManifestSchemaIn:
        vector[string] columns
        vector[DrakenType] physical
        unordered_map[int64_t, size_t] position_of_field_id
        cbool bounds_are_ordinal
        cbool stats_are_authoritative

    CNativeManifest* new_decoded_manifest(const ManifestColumnsIn& columns, const ManifestSchemaIn& schema, uint32_t rows) except +


# The whole-column sketch vectors the planner reduces natively; held as draken
# vectors, one row per file (NativeManifest's ManifestFile.vector_row).
SKETCH_COLUMNS = ("min_k_hashes", "histogram_counts", "char_class_counts")


cdef inline const DrakenVector* _scalar(dict vectors, str name, bint required) except? NULL:
    vector_object = vectors.get(name)
    if vector_object is None:
        if required:
            raise ValueError(f"the manifest has no `{name}` column")
        return NULL
    return draken_vector_unwrap(<PyObject*>vector_object)


cdef inline void _array(ManifestArrayColumn& out, dict vectors, str name, bint required) except *:
    vector_object = vectors.get(name)
    if vector_object is None:
        if required:
            raise ValueError(f"the manifest has no `{name}` column")
        out.outer = NULL
        out.child = NULL
        return
    out.outer = draken_vector_unwrap(<PyObject*>vector_object)
    out.child = draken_array_child_unwrap(<PyObject*>vector_object)


cdef inline void _nested_array(ManifestArrayColumn& out, dict vectors, str name) except *:
    """An OPTIONAL array<array<T>> column; absent - or holding no list at all,
    which a writer types with no element shape - leaves `out` empty."""
    vector_object = vectors.get(name)
    out.outer = NULL
    out.child = NULL
    out.grandchild = NULL
    if vector_object is None:
        return
    cdef const DrakenVector* outer = draken_vector_unwrap(<PyObject*>vector_object)
    if outer.length == 0 or outer.type != DRAKEN_ARRAY:
        return
    if draken_array_child_unwrap(<PyObject*>vector_object).length == 0:
        return
    out.outer = outer
    out.child = draken_array_child_unwrap(<PyObject*>vector_object)
    out.grandchild = draken_array_grandchild_unwrap(<PyObject*>vector_object)


cdef object _optional(int64_t value):
    return None if value == kUnknown else value


cdef class NativeManifest:
    """One relation's manifest: native file rows and per-column cells, plus the
    sketch vectors. Built by `decode_manifest_parquet`."""

    cdef CNativeManifest* _manifest
    cdef readonly tuple columns
    cdef readonly tuple physical   # each column's DrakenType, in load-time order
    cdef readonly dict sketches

    def __dealloc__(self):
        del self._manifest

    def __init__(self):
        raise TypeError("a NativeManifest is built by decode_manifest_parquet")

    def __len__(self):
        return self._manifest.file_count()

    @property
    def stats_are_authoritative(self):
        return self._manifest.stats_are_authoritative()

    def record_count(self):
        """Live rows (physical minus deleted), None when any file's count is unknown."""
        return _optional(self._manifest.record_count())

    def total_size(self):
        return self._manifest.total_size()

    def resident_bytes(self):
        """The memory this manifest holds resident (file rows, cells, the
        sketch vectors it views) - what a cache of decoded manifests budgets by."""
        return self._manifest.resident_bytes()

    @property
    def bounds_are_ordinal(self):
        return self._manifest.bounds_are_ordinal()

    cdef inline size_t _position(self, size_t position) except? 0:
        if position >= self._manifest.column_count():
            raise IndexError(f"no column at load-time position {position}")
        return position

    # --- estimates (manifest_estimates.hpp); columns by load-time position ---

    def row_group_count(self):
        return _optional(row_group_count(self._manifest[0]))

    def has_deletes(self):
        return has_deletes(self._manifest[0])

    def ordinal_bounds(self, size_t position):
        cdef int64_t lo = 0, hi = 0
        if not ordinal_bounds(self._manifest[0], self._position(position), lo, hi):
            return None
        return lo, hi

    def length_bounds(self, size_t position):
        cdef int64_t lo = 0, hi = 0
        if not length_bounds(self._manifest[0], self._position(position), lo, hi):
            return None
        return lo, hi

    def char_class_totals(self, size_t position):
        """(the 8 class byte totals, non-null rows), or None without char-class
        statistics for the column."""
        cdef int64_t totals[8]
        cdef int64_t non_null_rows = 0
        if not char_class_stats(self._manifest[0], self._position(position), totals, non_null_rows):
            return None
        return tuple(totals[k] for k in range(8)), non_null_rows

    def distogram(self, size_t position):
        """The column's per-file histograms folded into one Distogram, or None."""
        from opteryx.third_party.maki_nage.distogram import load_counts_i64
        from opteryx.third_party.maki_nage.distogram import merge

        cdef vector[int64_t] counts
        cdef vector[HistogramPart] parts
        histogram_parts(self._manifest[0], self._position(position), counts, parts)
        if parts.empty():
            return None
        cdef int64_t[::1] view = <int64_t[:counts.size()]> counts.data()
        cdef size_t k
        combined = None
        for k in range(parts.size()):
            dgram = load_counts_i64(view[parts[k].begin:parts[k].end], parts[k].lo, parts[k].hi)
            combined = dgram if combined is None else merge(combined, dgram)
        return combined

    def estimate_cardinality(self, size_t position):
        cdef double estimate = -1.0
        cdef int64_t count = estimate_cardinality(self._manifest[0], self._position(position), estimate)
        if estimate >= 0.0:
            return int(estimate)
        return _optional(count)

    def estimate_range_cardinality(self, size_t position, bint identity_category):
        return _optional(estimate_range_cardinality(self._manifest[0], self._position(position), identity_category))

    def total_null_count(self, size_t position):
        return _optional(total_null_count(self._manifest[0], self._position(position)))

    def null_fraction(self, size_t position):
        cdef double fraction = 0.0
        if not null_fraction(self._manifest[0], self._position(position), fraction):
            return None
        return fraction

    def total_uncompressed_size(self, size_t position):
        return _optional(total_uncompressed_size(self._manifest[0], self._position(position)))

    def value_range(self, size_t position, bint identity_category):
        """The column's numeric (min, max), or None."""
        cdef End lo, hi
        if not value_range(self._manifest[0], self._position(position), identity_category, lo, hi):
            return None
        return _number(lo), _number(hi)

    def sketch_row_widths(self, size_t row):
        """How many column slices file `row` has in each sketch (min-k,
        histogram, char-class); None for a sketch the file has no row in."""
        return (
            _row_width(self._manifest.min_k, self._manifest.file(row).vector_row),
            _row_width(self._manifest.histogram, self._manifest.file(row).vector_row),
            _row_width(self._manifest.char_class, self._manifest.file(row).vector_row),
        )

    def position_of(self, str column):
        """The column's load-time position, or None."""
        cdef string key = column.encode("utf-8")
        cdef unordered_map[string, size_t].const_iterator found = self._manifest.positions().find(key)
        if found == self._manifest.positions().end():
            return None
        return deref(found).second

    def file_paths(self):
        """Every file's path, in row order."""
        cdef size_t row
        return [self._manifest.file(row).path.decode("utf-8") for row in range(self._manifest.file_count())]

    def find_file(self, str path):
        """The row of the file at `path`, or None."""
        cdef int64_t row = self._manifest.find_file(path.encode("utf-8"))
        return None if row < 0 else row

    # --- writing (manifest_encode.hpp) --------------------------------------

    def to_parquet(self, list field_ids=None):
        """The manifest parquet for these rows - the shared manifest format, the
        catalog's full column set, bounds in the ORDINAL dialect - as bytes.
        `field_ids` (one per column; None for a column with no id) keys each
        row's lists; omitted, they are keyed by load-time position (core's own
        manifests). A catalog's manifest carries the catalog's ids."""
        from rugo import parquet as rugo_parquet

        cdef vector[int64_t] ids
        if field_ids is not None:
            for field_id in field_ids:
                ids.push_back(-1 if field_id is None else <int64_t?>field_id)
        return rugo_parquet.write_parquet(self._morsel(False, ids), compression="zstd", bloom_filters=True)

    def show_morsel(self):
        """SHOW MANIFEST's rows: the manifest's columns (manifest_io._MANIFEST_COLUMNS),
        with min_values / max_values rendered as TEXT - one row's bounds span
        every column's type, which no typed ARRAY holds (manifest_io._bound_as_text)."""
        cdef vector[int64_t] positions
        return self._morsel(True, positions)

    cdef _morsel(self, bint bounds_as_text, const vector[int64_t]& field_ids):
        from draken.morsels.morsel import Morsel

        cdef EncodedManifest e = encode_manifest(self._manifest[0], field_ids)
        cdef uint32_t n = e.rows
        # every encoded buffer is adopted, used or not - the bridge owns them
        min_values = _list_vector(e.min_values, n)
        max_values = _list_vector(e.max_values, n)
        delete_paths = _adopt(draken_vector_own_string(e.delete_file_path.slots, e.delete_file_path.arena,
                                                       e.delete_file_path.arena_len, e.delete_file_path_validity, n,
                                                       DRAKEN_VARCHAR))
        deleted = _scalar_vector(e.deleted_record_count, n)
        element_min_values = _list_vector(e.element_min_values, n)
        element_max_values = _list_vector(e.element_max_values, n)
        element_min_k_hashes = _list_vector(e.element_min_k_hashes, n)
        if bounds_as_text:
            min_values, max_values = self._text_bounds()
        morsel = Morsel()
        # the order of manifest_io._MANIFEST_COLUMNS
        morsel.append_vector("file_path", _strings(e.file_path, n))
        morsel.append_vector("file_format", _strings(e.file_format, n))
        morsel.append_vector("record_count", _scalar_vector(e.record_count, n))
        morsel.append_vector("file_size_in_bytes", _scalar_vector(e.file_size, n))
        morsel.append_vector("uncompressed_size_in_bytes", _scalar_vector(e.uncompressed_size, n))
        morsel.append_vector("column_uncompressed_sizes_in_bytes", _list_vector(e.column_sizes, n))
        morsel.append_vector("null_counts", _list_vector(e.null_counts, n))
        morsel.append_vector("min_k_hashes", _list_vector(e.min_k, n))
        morsel.append_vector("histogram_counts", _list_vector(e.histogram_counts, n))
        morsel.append_vector("histogram_bins", _scalar_vector(e.histogram_bins, n))
        morsel.append_vector("min_values", min_values)
        morsel.append_vector("max_values", max_values)
        morsel.append_vector("field_ids", _list_vector(e.field_ids, n))
        morsel.append_vector("min_lengths", _list_vector(e.min_lengths, n))
        morsel.append_vector("max_lengths", _list_vector(e.max_lengths, n))
        morsel.append_vector("char_class_counts", _list_vector(e.char_class_counts, n))
        morsel.append_vector("char_total_bytes", _list_vector(e.char_total_bytes, n))
        morsel.append_vector("distinct_counts", _list_vector(e.distinct_counts, n))
        if not bounds_as_text:
            # the catalog's ARRAY element statistics and merge-on-read columns;
            # SHOW MANIFEST's schema has none of them
            morsel.append_vector("element_min_values", element_min_values)
            morsel.append_vector("element_max_values", element_max_values)
            morsel.append_vector("element_min_k_hashes", element_min_k_hashes)
            morsel.append_vector("delete_file_path", delete_paths)
            morsel.append_vector("deleted_record_count", deleted)
        return morsel

    cdef tuple _text_bounds(self):
        """min_values / max_values as ARRAY<VARCHAR>: per file, each column's
        bound as text (the ordinal key in the ordinal dialect, the decoded value
        otherwise), or an empty list for a file with no bound at all."""
        from draken import draken_native as dn
        from opteryx.models.manifest_io import _bound_as_text

        cdef size_t row, position
        cdef size_t columns = self._manifest.column_count()
        cdef cbool ordinal = self._manifest.bounds_are_ordinal()
        cdef Bounds* b
        cdef End end
        lows = []
        highs = []
        for row in range(self._manifest.file_count()):
            low_row = []
            high_row = []
            for position in range(columns):
                b = &self._manifest.cell(row, position).bounds
                if ordinal:
                    low_row.append(None if b.min_ordinal == kNoBound else str(b.min_ordinal))
                    high_row.append(None if b.max_ordinal == kNoBound else str(b.max_ordinal))
                else:
                    low_row.append(_bound_as_text(_end_value(_decoded(b[0], True))))
                    high_row.append(_bound_as_text(_end_value(_decoded(b[0], False))))
            if all(value is None for value in low_row) and all(value is None for value in high_row):
                low_row = []
                high_row = []
            lows.append(low_row)
            highs.append(high_row)
        varchar = dn.DrakenType.VARCHAR.value
        return (
            dn.vector_array_from_sequence(lows, element_type=varchar, nesting_depth=1),
            dn.vector_array_from_sequence(highs, element_type=varchar, nesting_depth=1),
        )

    # --- file facts, as batches in row order --------------------------------

    def file_formats(self):
        cdef size_t row
        return [self._manifest.file(row).format.decode("utf-8") for row in range(self._manifest.file_count())]

    def record_counts(self):
        """Each file's PHYSICAL row count; None where unknown."""
        cdef size_t row
        return [_optional(self._manifest.file(row).record_count) for row in range(self._manifest.file_count())]

    def file_sizes(self):
        cdef size_t row
        return [self._manifest.file(row).file_size for row in range(self._manifest.file_count())]

    def uncompressed_sizes(self):
        cdef size_t row
        return [_optional(self._manifest.file(row).uncompressed_size) for row in range(self._manifest.file_count())]

    def deleted_record_counts(self):
        cdef size_t row
        return [self._manifest.file(row).deleted_record_count for row in range(self._manifest.file_count())]

    def delete_positions(self):
        """{path: file-local deleted row ordinals} for every file carrying
        merge-on-read deletes. A file whose deletes were never resolved raises:
        serving it would resurrect deleted rows."""
        cdef size_t row
        cdef ManifestFile* file
        out = {}
        for row in range(self._manifest.file_count()):
            file = &self._manifest.file(row)
            if file.deleted_record_count == 0:
                continue
            path = file.path.decode("utf-8")
            if not file.delete_positions_resolved:
                raise ValueError(
                    f"{path} reports {file.deleted_record_count} deleted rows but no delete vector "
                    "was resolved at binding; refusing to scan and serve deleted rows."
                )
            out[path] = tuple(file.delete_positions)
        return out

    def unresolved_deletes(self):
        """[{file_path, delete_file_path, deleted_record_count}] for each file
        whose merge-on-read deletes are counted but not yet resolved - what a
        catalog's sidecar reader takes."""
        cdef size_t row
        cdef ManifestFile* file
        out = []
        for row in range(self._manifest.file_count()):
            file = &self._manifest.file(row)
            if file.deleted_record_count == 0 or file.delete_positions_resolved:
                continue
            out.append({
                "file_path": file.path.decode("utf-8"),
                "delete_file_path": file.delete_file_path.decode("utf-8"),
                "deleted_record_count": file.deleted_record_count,
            })
        return out

    def resolve_deletes(self, dict positions):
        """Attach resolved delete positions ({file_path: row ordinals}) to every
        file with unresolved deletes. PRODUCER ONLY: called on a freshly decoded
        manifest before anything else holds it. A file left without a vector
        raises - scanning it would resurrect deleted rows."""
        cdef size_t row
        cdef ManifestFile* file
        for row in range(self._manifest.file_count()):
            file = &self._manifest.file(row)
            if file.deleted_record_count == 0 or file.delete_positions_resolved:
                continue
            path = file.path.decode("utf-8")
            vector = positions.get(path)
            if vector is None:
                raise ValueError(
                    f"{path} reports {file.deleted_record_count} deleted rows but its delete "
                    "sidecar holds no vector for it; refusing to scan and serve deleted rows."
                )
            file.delete_positions.clear()
            for ordinal in vector:
                file.delete_positions.push_back(<int64_t?>ordinal)
            file.delete_positions_resolved = True

    def with_paths(self, list paths):
        """A copy of this manifest whose files live at `paths` (one per file,
        in order) - the same rows, statistics and sketches."""
        cdef size_t row
        if len(paths) != self._manifest.file_count():
            raise ValueError("one path per file")
        cdef NativeManifest out = self.subset(list(range(self._manifest.file_count())))
        for row in range(out._manifest.file_count()):
            out._manifest.file(row).path = (<str?>paths[row]).encode("utf-8")
        return out

    def subset(self, list rows):
        """A manifest of the files at `rows` (indexes into this one, in the
        order given), sharing the sketch vectors. This one is untouched."""
        cdef vector[size_t] at
        for row in rows:
            at.push_back(<size_t?>row)
        cdef NativeManifest out = NativeManifest.__new__(NativeManifest)
        out._manifest = _new_subset(self._manifest[0], at)
        out.columns = self.columns
        out.physical = self.physical
        out.sketches = self.sketches
        return out

    # --- answers read straight off the statistics ----------------------------

    def has_null_counts(self):
        """Whether any file records a null count for any column."""
        return has_null_counts(self._manifest[0])

    def extremes(self, size_t position):
        """(min, max) of the column across every file (footer bounds first),
        as Python values - for the statistics-only MIN/MAX answer, whose gate
        admits only types whose bound IS the value. (None, None) when nothing
        bounds it or two files' bounds never compared."""
        cdef End lo, hi
        if not extreme_ends(self._manifest[0], self._position(position), lo, hi):
            return None, None
        return _end_value(lo), _end_value(hi)

    def key_ranges(self, size_t position):
        """[(row, low, high)] for the files whose MANIFEST bounds the column
        (real bounds only), for compaction planning's overlap reasoning."""
        cdef size_t row
        cdef End lo, hi
        out = []
        for row in range(self._manifest.file_count()):
            if file_key_range(self._manifest[0], row, self._position(position), lo, hi):
                out.append((row, _end_value(lo), _end_value(hi)))
        return out

    # --- pruning (manifest_prune.hpp) ---------------------------------------    # --- pruning (manifest_prune.hpp) ---------------------------------------

    def prune_files(self, ExprArena arena not None, ColumnTable columns not None, list predicate_ids, dict live_types):
        """The rows of the files `predicate_ids` (placed expressions of `arena`)
        cannot rule out, in order. `live_types` maps each column name still in
        the relation's schema to its type id."""
        cdef _PruneCall call = _PruneCall(self, arena, columns, predicate_ids, live_types)
        return list(prune(self._manifest[0], call.inputs, call.cols, call.predicates))

    def zone_map_terms(self, ExprArena arena not None, ColumnTable columns not None, list predicate_ids, dict live_types):
        """`(column, op_code, ordinal)` row-group zone-map terms (ordinal dialect only)."""
        cdef _PruneCall call = _PruneCall(self, arena, columns, predicate_ids, live_types)
        cdef vector[ZoneTerm] terms = zone_terms(self._manifest[0], call.inputs, call.cols, call.predicates)
        cdef size_t k
        out = []
        for k in range(terms.size()):
            # the int128 as a Python int (C arithmetic would overflow the shift)
            ordinal = int(zone_ordinal_hi(terms[k])) * 18446744073709551616 + int(zone_ordinal_lo(terms[k]))
            out.append((terms[k].column.decode("utf-8"), terms[k].op, ordinal))
        return out

    def prune_files_for_topn(self, str column, bint descending, int64_t limit, dict live_types):
        """The rows of the files that can hold a top-`limit` row of `column`."""
        cdef unordered_map[string, uint32_t] live
        cdef PruneColumns cols
        _fill_live_types(live, live_types)
        cols.position = &self._manifest.positions()
        cols.live_types = &live
        cols.types = column_type_table()
        return list(prune_files_for_topn(self._manifest[0], cols, column.encode("utf-8"), descending, limit))

    def file_row(self, size_t row):
        """File `row` as a dict - for checking the rows against another reader."""
        cdef ManifestFile* f = &self._manifest.file(row)
        return {
            "file_path": f.path.decode("utf-8"),
            "file_format": f.format.decode("utf-8"),
            "record_count": _optional(f.record_count),
            "file_size_in_bytes": f.file_size,
            "uncompressed_size_in_bytes": _optional(f.uncompressed_size),
            "row_group_count": _optional(f.row_group_count),
            "histogram_bins": _optional(f.histogram_bins),
            "deleted_record_count": f.deleted_record_count,
            "delete_file_path": f.delete_file_path.decode("utf-8") or None,
            "distinct_sketch_family": f.distinct_sketch_family,
            "has_footer": f.has_footer,
            "vector_row": f.vector_row,
        }

    def cell(self, size_t row, size_t column):
        """File `row`'s cell for the column at load-time position `column`, as a dict."""
        cdef ManifestCell* c = &self._manifest.cell(row, column)
        return {
            "bounds": _bounds_view(&c.bounds),
            "null_count": _optional(c.null_count),
            "min_length": _optional(c.min_length),
            "max_length": _optional(c.max_length),
            "char_total_bytes": _optional(c.char_total_bytes),
            "uncompressed_size": _optional(c.uncompressed_size),
            "distinct_count": None if c.distinct_count == kUnknown else (c.distinct_count, c.distinct_exact),
            "distinct_floor": _optional(c.distinct_floor),
            "distinct_sketch": tuple(c.distinct_sketch) if c.has_distinct_sketch else None,
            "element_bounds": None if c.element_min == kNoBound else (c.element_min, c.element_max),
            "element_min_k": tuple(c.element_min_k),
            "footer": None if not self._manifest.file(row).has_footer else {
                "bounds": _bounds_view(&c.footer.bounds),
                "null_count": _optional(c.footer.null_count),
                "distinct_count": _optional(c.footer.distinct_count),
                "uncompressed_size": _optional(c.footer.uncompressed_size),
            },
        }


def decode_manifest_parquet(
    bytes data,
    tuple columns,
    tuple physical,
    dict position_of_field_id,
    bint bounds_are_ordinal,
    bint stats_are_authoritative,
):
    """Decode manifest parquet `data` into a NativeManifest over `columns` (the
    load-time schema's column names, in order), whose physical DrakenTypes are
    `physical`. `position_of_field_id` maps the schema's field ids to positions -
    empty when the schema has none, and a manifest's lists are then positional.
    `bounds_are_ordinal` says what min_values/max_values hold (see
    manifest_decode.hpp)."""
    from rugo import parquet as rugo_parquet

    if len(columns) != len(physical):
        raise ValueError("one physical type per column")

    morsels = []
    with rugo_parquet.read_parquet(data) as reader:
        for morsel in reader:
            morsels.append(morsel)
    cdef dict vectors = {}
    cdef uint32_t rows = 0
    if morsels:
        combined = morsels[0] if len(morsels) == 1 else morsels[0].combine(morsels)
        rows = combined.num_rows
        for name_b in combined.column_names:
            name = name_b.decode("utf-8") if type(name_b) is bytes else name_b
            # the native Vector behind the Python Vector - what the bridge reads
            vectors[name] = combined.column(name_b)._nb

    cdef ManifestColumnsIn columns_in
    cdef ManifestSchemaIn schema_in
    cdef NativeManifest out = NativeManifest.__new__(NativeManifest)
    out.columns = columns
    out.physical = physical
    out.sketches = {
        name: combined.column(name.encode("utf-8")) for name in SKETCH_COLUMNS if name in vectors
    } if rows else {}

    for name in columns:
        schema_in.columns.push_back((<str?>name).encode("utf-8"))
    for draken_type in physical:
        schema_in.physical.push_back(<DrakenType><int>draken_type.value)
    for field_id, position in position_of_field_id.items():
        schema_in.position_of_field_id[<int64_t>field_id] = <size_t>position
    schema_in.bounds_are_ordinal = bounds_are_ordinal
    schema_in.stats_are_authoritative = stats_are_authoritative

    if rows:
        columns_in.file_path = _scalar(vectors, "file_path", True)
        columns_in.file_format = _scalar(vectors, "file_format", True)
        columns_in.record_count = _scalar(vectors, "record_count", True)
        columns_in.file_size = _scalar(vectors, "file_size_in_bytes", True)
        columns_in.uncompressed_size = _scalar(vectors, "uncompressed_size_in_bytes", True)
        columns_in.histogram_bins = _scalar(vectors, "histogram_bins", True)
        columns_in.deleted_record_count = _scalar(vectors, "deleted_record_count", False)
        columns_in.delete_file_path = _scalar(vectors, "delete_file_path", False)
        _array(columns_in.column_uncompressed_sizes, vectors, "column_uncompressed_sizes_in_bytes", True)
        _array(columns_in.null_counts, vectors, "null_counts", True)
        _array(columns_in.min_values, vectors, "min_values", True)
        _array(columns_in.max_values, vectors, "max_values", True)
        _array(columns_in.min_lengths, vectors, "min_lengths", True)
        _array(columns_in.max_lengths, vectors, "max_lengths", True)
        _array(columns_in.field_ids, vectors, "field_ids", True)
        _array(columns_in.char_total_bytes, vectors, "char_total_bytes", True)
        _array(columns_in.distinct_counts, vectors, "distinct_counts", False)
        _array(columns_in.element_min_values, vectors, "element_min_values", False)
        _array(columns_in.element_max_values, vectors, "element_max_values", False)
        _nested_array(columns_in.element_min_k_hashes, vectors, "element_min_k_hashes")

    out._manifest = new_decoded_manifest(columns_in, schema_in, rows)
    _bind_sketch_views(out)
    # the decoded rows are copies; the vectors only need to outlive the decode,
    # except the sketches, held above
    return out


cdef extern from *:
    """
    static inline opteryx::planner::estimate_detail::End
    decoded_end_of(const opteryx::planner::Bounds& b, bool is_min) {
        return opteryx::planner::estimate_detail::decoded_end(b, is_min);
    }
    // A heap copy of `m` narrowed to `rows` (a NativeManifest the Python
    // object then owns).
    static inline opteryx::planner::NativeManifest*
    new_subset(const opteryx::planner::NativeManifest& m, const std::vector<size_t>& rows) {
        return new opteryx::planner::NativeManifest(m.subset(rows));
    }
    """
    End _decoded "decoded_end_of"(const Bounds& b, cbool is_min)
    CNativeManifest* _new_subset "new_subset"(const CNativeManifest& m, const vector[size_t]& rows) except +


from decimal import Decimal as _Decimal


cdef _end_value(const End& end):
    """An End as the Python value it stands for (None for an absent end)."""
    cdef string text
    if end.tag == DECODED_INT64:
        return end.i
    if end.tag == DECODED_UINT64:
        return <uint64_t>end.i
    if end.tag == DECODED_DOUBLE:
        return end.d
    if end.tag == DECODED_TEXT or end.tag == DECODED_OTHER:
        text = end.text[0]
        return text.decode("utf-8")
    if end.tag == DECODED_BYTES:
        text = end.text[0]
        return <bytes>text
    if end.tag == DECODED_DECIMAL:
        return _Decimal(end.i).scaleb(-end.scale)
    if end.tag == DECODED_BOOL:
        return end.i != 0
    return None


cdef _number(End& end):
    """A numeric End as the Python number it stands for."""
    if end.tag == DECODED_INT64:
        return end.i
    if end.tag == DECODED_UINT64:
        return <uint64_t>end.i
    return end.d


cdef tuple _bound_end(DecodedTag tag, int64_t as_int, double as_double, const string& as_text):
    """One decoded end as a (kind, value) pair, for the debug views."""
    if tag == DECODED_NONE:
        return None
    if tag == DECODED_INT64:
        return ("int", as_int)
    if tag == DECODED_UINT64:
        return ("int", <uint64_t>as_int)
    if tag == DECODED_DOUBLE:
        return ("float", as_double)
    if tag == DECODED_TEXT:
        return ("text", as_text)
    if tag == DECODED_BYTES:
        return ("bytes", as_text)
    if tag == DECODED_DECIMAL:
        return ("decimal", as_int, as_double)
    if tag == DECODED_BOOL:
        return ("bool", as_int)
    return ("other", as_text)


cdef dict _bounds_view(Bounds* b):
    return {
        "min_ordinal": None if b.min_ordinal == kNoBound else b.min_ordinal,
        "max_ordinal": None if b.max_ordinal == kNoBound else b.max_ordinal,
        "min": _bound_end(b.min_tag, b.min_int, b.min_double, b.min_text),
        "max": _bound_end(b.max_tag, b.max_int, b.max_double, b.max_text),
    }


cdef NodeKinds _KINDS
cdef bint _KINDS_LOADED = False


cdef void _load_kinds() except *:
    """opteryx.expression.NodeType's values, read once - never restated."""
    global _KINDS_LOADED
    if _KINDS_LOADED:
        return
    from opteryx.expression import NodeType

    _KINDS.and_ = NodeType.AND.value
    _KINDS.or_ = NodeType.OR.value
    _KINDS.dnf = NodeType.DNF.value
    _KINDS.cnf = NodeType.CNF.value
    _KINDS.comparison = NodeType.COMPARISON_OPERATOR.value
    _KINDS.binary = NodeType.BINARY_OPERATOR.value
    _KINDS.unary = NodeType.UNARY_OPERATOR.value
    _KINDS.function = NodeType.FUNCTION.value
    _KINDS.identifier = NodeType.IDENTIFIER.value
    _KINDS.nested = NodeType.NESTED.value
    _KINDS.literal = NodeType.LITERAL.value
    _KINDS.between = NodeType.BETWEEN.value
    _KINDS_LOADED = True


cdef void _fill_live_types(unordered_map[string, uint32_t]& out, dict live_types) except *:
    for name, type_id in live_types.items():
        out[(<str?>name).encode("utf-8")] = <uint32_t>type_id


cdef class _PruneCall:
    """One pruning call's native inputs, alive for the call."""

    cdef DeriveInputs inputs
    cdef PruneColumns cols
    cdef vector[int64_t] predicates
    cdef unordered_map[string, uint32_t] live
    cdef ExprArena arena
    cdef ColumnTable columns

    def __cinit__(self, NativeManifest manifest, ExprArena arena, ColumnTable columns, list predicate_ids, dict live_types):
        _load_kinds()
        self.arena = arena
        self.columns = columns
        _fill_live_types(self.live, live_types)
        for expr_id in predicate_ids:
            self.predicates.push_back(<int64_t?>expr_id)
        self.inputs.exprs = arena._table
        self.inputs.columns = &columns._rows
        self.inputs.types = column_type_table()
        self.inputs.kinds = &_KINDS
        self.inputs.live_types = &self.live
        self.cols.position = &manifest._manifest.positions()
        self.cols.live_types = &self.live
        self.cols.types = column_type_table()


cdef extern from *:
    """
    // Slices in a sketch vector's row, or -1 when the row is absent or null.
    static inline int64_t sketch_row_width(const opteryx::planner::NestedArrayView& v, uint32_t row) {
        if (!v.present() || row >= v.n_files() || !opteryx::planner::sketch_bit_valid(v.outer->validity, row)) return -1;
        const int32_t* offsets = static_cast<const int32_t*>(v.outer->data);
        const uint32_t at = v.outer->selection[row];
        return static_cast<int64_t>(offsets[at + 1]) - offsets[at];
    }
    """
    int64_t sketch_row_width(const NestedArrayView& v, uint32_t row)


cdef _row_width(const NestedArrayView& v, uint32_t row):
    cdef int64_t width = sketch_row_width(v, row)
    return None if width < 0 else width


cdef _adopt(PyObject* handle):
    """The Vector a bridge call returned (a NEW reference), as a Python value
    holding its one reference."""
    vector = <object>handle
    Py_DECREF(vector)
    return vector


cdef _strings(EncodedStrings& column, uint32_t rows):
    return _adopt(draken_vector_own_string(column.slots, column.arena, column.arena_len, NULL, rows, DRAKEN_VARCHAR))


cdef _scalar_vector(EncodedScalar& column, uint32_t rows):
    return _adopt(draken_vector_own_raw(column.data, column.validity, rows, DRAKEN_INT64))


cdef _list_vector(EncodedList& column, uint32_t rows):
    """A list column as an ARRAY Vector - two levels deep for the sketches."""
    leaf = _adopt(draken_vector_own_raw(column.leaf, column.leaf_validity, column.leaf_length, column.leaf_type))
    if column.depth == 2:
        middle = _adopt(draken_vector_own_array_child(column.mid_offsets, <PyObject*>leaf, column.mid_validity,
                                                      column.mid_length))
        return _adopt(draken_vector_own_array_child(column.offsets, <PyObject*>middle, NULL, rows))
    return _adopt(draken_vector_own_array_child(column.offsets, <PyObject*>leaf, NULL, rows))


_OP_NAMES = ("Eq", "NotEq", "Gt", "GtEq", "Lt", "LtEq", "Between", None)


cdef _bval(const BVal& v):
    """A bound value as the Python value it stands for (a carried literal as
    its kind only)."""
    if v.kind == BKIND_INT:
        return int(bval_int_hi(v)) * 18446744073709551616 + int(bval_int_lo(v))
    if v.kind == BKIND_FLOAT:
        return v.d
    if v.kind == BKIND_BYTES:
        return <bytes>v.bytes
    if v.kind == BKIND_BOOL:
        return bool(bval_int_lo(v))
    return ("literal",)


cdef tuple _term(const BoundTerm& t):
    upper = _bval(t.upper) if t.op == OP_BETWEEN else None
    return (t.column.decode("utf-8"), _OP_NAMES[<int>t.op], _bval(t.value), upper, t.derived)


def derivation(ExprArena arena not None, ColumnTable columns not None, list predicate_ids, dict live_types):
    """What predicate_bounds.hpp derives from `predicate_ids` - for checking it
    against another reader: {"terms": [(column, op, value, upper, derived)],
    "nulls": [(column, requires_null)], "folds": [(column, lower, [term])]}."""
    _load_kinds()
    cdef unordered_map[string, uint32_t] live
    cdef DeriveInputs inputs
    cdef vector[int64_t] ids
    _fill_live_types(live, live_types)
    for expr_id in predicate_ids:
        ids.push_back(<int64_t?>expr_id)
    inputs.exprs = arena._table
    inputs.columns = &columns._rows
    inputs.types = column_type_table()
    inputs.kinds = &_KINDS
    inputs.live_types = &live
    cdef vector[int64_t] conjuncts = split_conjuncts(inputs, ids)
    cdef vector[BoundTerm] terms = derive_bound_terms(inputs, conjuncts)
    cdef vector[NullTerm] nulls = derive_null_terms(inputs, conjuncts)
    cdef vector[FoldTerm] folds = derive_fold_terms(inputs, conjuncts)
    cdef size_t k, j
    fold_views = []
    for k in range(folds.size()):
        fold_views.append((
            folds[k].column.decode("utf-8"),
            folds[k].lower,
            [_term(folds[k].terms[j]) for j in range(folds[k].terms.size())],
        ))
    return {
        "terms": [_term(terms[k]) for k in range(terms.size())],
        "nulls": [(nulls[k].column.decode("utf-8"), nulls[k].requires_null) for k in range(nulls.size())],
        "folds": fold_views,
    }


cdef NestedArrayView _sketch_view(dict sketches, str name) except *:
    cdef PyObject* handle
    sketch = sketches.get(name)
    if sketch is None:
        return NestedArrayView()
    # a draken Vector wrapper (Morsel.column) or the native Vector itself (the
    # array constructors) - dispatched on the concrete type
    native = sketch if type(sketch) is _NativeVector else sketch._nb
    handle = <PyObject*>native
    # A sketch column in which no file holds a single slice describes nothing:
    # no rows at all, or rows that are all empty lists - which a writer types
    # with no element shape (a flat array, or no type), since there is no
    # element to take one from. Any other shape must be array<array<T>>.
    cdef const DrakenVector* outer = draken_vector_unwrap(handle)
    if outer.length == 0:
        return NestedArrayView()
    if outer.type == DRAKEN_ARRAY and draken_array_child_unwrap(handle).length == 0:
        return NestedArrayView()
    return NestedArrayView(
        outer,
        draken_array_child_unwrap(handle),
        draken_array_grandchild_unwrap(handle),
    )


cdef void _bind_sketch_views(NativeManifest manifest) except *:
    """Point the native manifest at the sketch vectors it holds."""
    manifest._manifest.min_k = _sketch_view(manifest.sketches, "min_k_hashes")
    manifest._manifest.histogram = _sketch_view(manifest.sketches, "histogram_counts")
    manifest._manifest.char_class = _sketch_view(manifest.sketches, "char_class_counts")


cdef class NativeManifestBuilder:
    """Builds a NativeManifest file by file: the native builder the in-process
    producers (footer readers, listings, writers, catalog rows) use (Q8 M-c).
    Columns are addressed by load-time position;
    mapping a producer's own keys to positions is the producer's job."""

    cdef CNativeManifest* _manifest
    cdef tuple _columns
    cdef tuple _physical_types
    cdef vector[DrakenType] _physical
    # sketch rows this builder owns (min-k, histogram, char-class)
    cdef SketchStaging _min_k
    cdef SketchStaging _histogram
    cdef SketchStaging _char_class

    def __cinit__(self, tuple columns, tuple physical, bint bounds_are_ordinal, bint stats_are_authoritative):
        cdef vector[string] names
        if len(columns) != len(physical):
            raise ValueError("one physical type per column")
        for name in columns:
            names.push_back((<str?>name).encode("utf-8"))
        for draken_type in physical:
            self._physical.push_back(<DrakenType><int>draken_type.value)
        self._manifest = new CNativeManifest(names, bounds_are_ordinal, stats_are_authoritative)
        self._columns = columns
        self._physical_types = physical

    def __dealloc__(self):
        del self._manifest

    def add_file(
        self,
        str path,
        str file_format,
        int64_t record_count,
        int64_t file_size,
        int64_t row_group_count=-1,
        int64_t uncompressed_size=-1,
        int64_t histogram_bins=-1,
        int64_t deleted_record_count=0,
        str delete_file_path=None,
        tuple delete_positions=None,
        uint32_t vector_row=0,
    ):
        """A file row; its index. A count of -1 is UNKNOWN (never zero)."""
        cdef ManifestFile file
        file.path = path.encode("utf-8")
        file.format = file_format.encode("utf-8")
        file.record_count = record_count
        file.file_size = file_size
        file.row_group_count = row_group_count
        file.uncompressed_size = uncompressed_size
        file.histogram_bins = histogram_bins
        file.deleted_record_count = deleted_record_count
        if delete_file_path is not None:
            file.delete_file_path = delete_file_path.encode("utf-8")
        if delete_positions is not None:
            file.delete_positions_resolved = True
            for position in delete_positions:
                file.delete_positions.push_back(<int64_t?>position)
        file.vector_row = vector_row
        return self._manifest.add_file(file)

    def set_footer(self, size_t row, FileColumnStats footer not None):
        """The file's parquet footer statistics (manifest_footer.hpp), beside the
        manifest's own. A column the footer does not describe stays unknown."""
        cdef size_t position
        cdef ManifestFile* file = &self._manifest.file(row)
        file.has_footer = True
        for position in range(len(self._columns)):
            index = footer._name_to_idx.get(self._columns[position])
            if index is None:
                continue
            apply_footer_stat(footer._stats[<size_t>index], self._manifest.cell(row, position))

    cdef inline Bounds* _bounds(self, size_t row, size_t position) except NULL:
        return &self._manifest.cell(row, position).bounds

    def set_ordinal_bound(self, size_t row, size_t position, bint is_min, int64_t ordinal):
        set_ordinal_bound(self._bounds(row, position)[0], self._physical[position], is_min, ordinal)

    def set_int_bound(self, size_t row, size_t position, bint is_min, int64_t value):
        set_int_bound(self._bounds(row, position)[0], self._physical[position], is_min, value)

    def set_uint_bound(self, size_t row, size_t position, bint is_min, uint64_t value):
        set_uint_bound(self._bounds(row, position)[0], is_min, value)

    def set_double_bound(self, size_t row, size_t position, bint is_min, double value):
        set_double_bound(self._bounds(row, position)[0], is_min, value)

    def set_text_bound(self, size_t row, size_t position, bint is_min, str value):
        set_bytes_bound(self._bounds(row, position)[0], is_min, True, value.encode("utf-8"))

    def set_bytes_bound(self, size_t row, size_t position, bint is_min, bytes value):
        set_bytes_bound(self._bounds(row, position)[0], is_min, False, value)

    def set_decimal_bound(self, size_t row, size_t position, bint is_min, int64_t unscaled, int32_t scale, double value):
        set_decimal_bound(self._bounds(row, position)[0], is_min, unscaled, scale, value)

    def set_bool_bound(self, size_t row, size_t position, bint is_min, bint value):
        set_bool_bound(self._bounds(row, position)[0], is_min, value)

    def set_counts(
        self,
        size_t row,
        size_t position,
        int64_t null_count=-1,
        int64_t min_length=-1,
        int64_t max_length=-1,
        int64_t char_total_bytes=-1,
        int64_t uncompressed_size=-1,
        int64_t distinct_floor=-1,
    ):
        """The manifest's per-column counts; -1 leaves a count as it is."""
        cdef ManifestCell* cell = &self._manifest.cell(row, position)
        if null_count != -1:
            cell.null_count = null_count
        if min_length != -1:
            cell.min_length = min_length
        if max_length != -1:
            cell.max_length = max_length
        if char_total_bytes != -1:
            cell.char_total_bytes = char_total_bytes
        if uncompressed_size != -1:
            cell.uncompressed_size = uncompressed_size
        if distinct_floor != -1:
            cell.distinct_floor = distinct_floor

    def set_distinct_count(self, size_t row, size_t position, int64_t count, bint exact):
        cdef ManifestCell* cell = &self._manifest.cell(row, position)
        cell.distinct_count = count
        cell.distinct_exact = exact

    def set_distinct_sketch(self, size_t row, size_t position, list hashes, int32_t family):
        """A skene file's own KMV sketch for the column, in hash family `family`."""
        cdef ManifestCell* cell = &self._manifest.cell(row, position)
        cdef ManifestFile* file = &self._manifest.file(row)
        cell.has_distinct_sketch = True
        cell.distinct_sketch.clear()
        for hash_value in hashes:
            cell.distinct_sketch.push_back(<uint64_t?>hash_value)
        file.distinct_sketch_family = family

    def copy_manifest_row(self, size_t row, NativeManifest source not None, size_t source_row):
        """Every column statistic of `source`'s row `source_row` - a decoded
        manifest over the same columns, ANALYZE's - into this file's row. The
        file's footer statistics, if set, are kept."""
        cdef size_t position
        cdef ManifestCell* target
        cdef FooterStats footer
        cdef ManifestFile* file
        if source._manifest.column_count() != len(self._columns):
            raise ValueError("the manifest describes different columns")
        for position in range(len(self._columns)):
            target = &self._manifest.cell(row, position)
            footer = target.footer
            target[0] = source._manifest.cell(source_row, position)
            target.footer = footer
        file = &self._manifest.file(row)
        file.histogram_bins = source._manifest.file(source_row).histogram_bins
        file.uncompressed_size = source._manifest.file(source_row).uncompressed_size

    cdef SketchStaging* _staging(self, str kind) except NULL:
        if kind == "min_k":
            return &self._min_k
        if kind == "histogram":
            return &self._histogram
        if kind == "char_class":
            return &self._char_class
        raise ValueError(f"no sketch kind {kind!r}")

    def start_sketch_rows(self, size_t row):
        """Give file `row` a row of every sketch kind - an empty slice per
        column - unless it has one."""
        cdef size_t columns = len(self._columns)
        self._min_k.row_of(row, columns)
        self._histogram.row_of(row, columns)
        self._char_class.row_of(row, columns)

    def set_sketch(self, size_t row, size_t position, str kind, list values):
        """File `row`'s slice of `kind` (min_k / histogram / char_class) for the
        column at `position`."""
        cdef SketchStaging* staging = self._staging(kind)
        cdef vector[optional[vector[uint64_t]]]* slices = &staging.row_of(row, len(self._columns))
        cdef vector[uint64_t] slice
        for value in values:
            slice.push_back(<uint64_t>(<int64_t?>value) if value < 0 else <uint64_t?>value)
        slices[0][position] = slice

    def carry_analyzed(self, size_t row, NativeManifest analyzed not None, size_t analyzed_row,
                       bint bounds, bint null_counts):
        """What a dataset's ANALYZE manifest (decoded over the same columns)
        adds to a file described from its own footer: per column the string
        lengths, char byte totals and uncompressed sizes - and its bounds and
        null counts only when asked (a skene footer's own outrank them) - and
        the file's uncompressed size and histogram width. ANALYZE's distinct
        counts are NOT carried. The file's sketch-vector row becomes its
        ANALYZE row."""
        cdef size_t columns = len(self._columns)
        cdef size_t position
        cdef ManifestCell* target
        cdef ManifestCell* source
        cdef ManifestFile* file = &self._manifest.file(row)
        cdef ManifestFile* source_file = &analyzed._manifest.file(analyzed_row)
        if analyzed._manifest.column_count() != columns:
            raise ValueError("the ANALYZE manifest describes different columns")
        for position in range(columns):
            target = &self._manifest.cell(row, position)
            source = &analyzed._manifest.cell(analyzed_row, position)
            target.min_length = source.min_length
            target.max_length = source.max_length
            target.char_total_bytes = source.char_total_bytes
            target.uncompressed_size = source.uncompressed_size
            if bounds:
                target.bounds = source.bounds
            if null_counts:
                target.null_count = source.null_count
        file.uncompressed_size = source_file.uncompressed_size
        file.histogram_bins = source_file.histogram_bins
        file.vector_row = source_file.vector_row

    def add_file_from(self, NativeManifest source not None, size_t source_row, list positions):
        """File `source_row` of `source` as a new file row - its cells and its
        sketch rows - with column k taking `source`'s column positions[k]
        (None: nothing recorded). Its row."""
        cdef vector[int64_t] at
        for position in positions:
            at.push_back(-1 if position is None else <int64_t?>position)
        cdef size_t added = self._manifest.add_file_cells_from(source._manifest[0], source_row, at)
        cdef uint32_t vector_row = source._manifest.file(source_row).vector_row
        self._min_k.carry_keyed(source._manifest.min_k, vector_row, added, at)
        self._histogram.carry_keyed(source._manifest.histogram, vector_row, added, at)
        self._char_class.carry_keyed(source._manifest.char_class, vector_row, added, at)
        return added

    def set_skene_footer(self, size_t row, const unsigned char[::1] file not None):
        """The .skene file's footer statistics (skene_stats.hpp) into file
        `row`: its record and row group counts, and per column the union of the
        row groups' ordinal bounds, the summed null count, the NDV and its floor,
        and the file's own KMV sketch. `file` is the file's bytes. Returns
        (any column bounded, any column's null count recorded). Raises
        ValueError with skene's message when the footer cannot be read."""
        cdef SkeneApplied applied
        cdef string failure = read_skene_footer_into(
            self._manifest[0], row, <const void*>&file[0], <size_t>file.shape[0], self._physical, applied
        )
        if not failure.empty():
            raise ValueError(failure.decode("utf-8", "replace"))
        return applied.any_bounds, applied.any_nulls

    def set_file_counts(self, size_t row, record_count, row_group_count):
        """File `row`'s record and row group counts; None is UNKNOWN."""
        cdef ManifestFile* file = &self._manifest.file(row)
        file.record_count = kUnknown if record_count is None else <int64_t?>record_count
        file.row_group_count = kUnknown if row_group_count is None else <int64_t?>row_group_count

    def set_bounds_are_ordinal(self, bint ordinal):
        """The dialect the manifest's bounds are in, for a producer that learns
        it while building (a skene dataset: ordinal when any file bounds anything)."""
        self._manifest.set_bounds_are_ordinal(ordinal)

    def set_delete_positions(self, size_t row, tuple positions):
        """File `row`'s merge-on-read deletes, resolved: file-local row ordinals."""
        cdef ManifestFile* file = &self._manifest.file(row)
        file.delete_positions.clear()
        for position in positions:
            file.delete_positions.push_back(<int64_t?>position)
        file.delete_positions_resolved = True

    def relocate_file(self, size_t row, str path, int64_t file_size):
        """File `row` now lives at `path` and is `file_size` bytes (its file was
        rewritten - the statistics describe the same rows)."""
        cdef ManifestFile* file = &self._manifest.file(row)
        file.path = path.encode("utf-8")
        file.file_size = file_size

    def carry_statistics(self, size_t row, NativeManifest source not None, size_t source_row):
        """A prior manifest's statistics of one file into this file's row: the
        value statistics (bounds, null counts, lengths, char bytes) of every
        column, and each sketch row that is exactly this schema's width. Sizes,
        distinct counts and footer statistics are the producer's own to set."""
        cdef size_t columns = len(self._columns)
        cdef size_t position
        cdef ManifestCell* target
        cdef ManifestCell* prior
        if source._manifest.column_count() != columns:
            raise ValueError("the manifest describes different columns")
        for position in range(columns):
            target = &self._manifest.cell(row, position)
            prior = &source._manifest.cell(source_row, position)
            target.bounds = prior.bounds
            target.null_count = prior.null_count
            target.min_length = prior.min_length
            target.max_length = prior.max_length
            target.char_total_bytes = prior.char_total_bytes
        cdef uint32_t vector_row = source._manifest.file(source_row).vector_row
        self._min_k.carry(source._manifest.min_k, vector_row, row, columns)
        self._histogram.carry(source._manifest.histogram, vector_row, row, columns)
        self._char_class.carry(source._manifest.char_class, vector_row, row, columns)

    def clear_statistics(self, size_t row, size_t position):
        """Forget the column's value statistics and sketch slices in file `row`;
        whether a sketch slice it dropped held anything."""
        cdef ManifestCell* cell = &self._manifest.cell(row, position)
        cdef Bounds empty
        cell.bounds = empty
        cell.null_count = kUnknown
        cell.min_length = kUnknown
        cell.max_length = kUnknown
        cell.char_total_bytes = kUnknown
        cdef bint k = self._min_k.clear(row, position)
        cdef bint h = self._histogram.clear(row, position)
        cdef bint c = self._char_class.clear(row, position)
        return k or h or c

    def has_statistics(self):
        """Whether any file holds a value statistic or a non-empty sketch slice."""
        cdef size_t row, position
        cdef ManifestCell* cell
        if self._min_k.any_values() or self._histogram.any_values() or self._char_class.any_values():
            return True
        for row in range(self._manifest.file_count()):
            for position in range(len(self._columns)):
                cell = &self._manifest.cell(row, position)
                if (cell.bounds.min_ordinal != kNoBound or cell.bounds.max_ordinal != kNoBound
                        or cell.null_count != kUnknown or cell.min_length != kUnknown
                        or cell.max_length != kUnknown or cell.char_total_bytes != kUnknown):
                    return True
        return False

    def build(self, dict sketches):
        """The NativeManifest. Its sketches are either the ones this builder
        was given (`set_sketch` / `carry_statistics`), or `sketches`
        (SKETCH_COLUMNS name -> draken vector) - never both. The builder is
        spent: it holds an empty manifest after."""
        cdef size_t rows = self._manifest.file_count()
        cdef shared_ptr[const OwnedNested] min_k = self._min_k.build(rows, DRAKEN_UINT64)
        cdef shared_ptr[const OwnedNested] histogram = self._histogram.build(rows, DRAKEN_INT64)
        cdef shared_ptr[const OwnedNested] char_class = self._char_class.build(rows, DRAKEN_INT64)
        cdef bint staged_rows = min_k.get() != NULL or histogram.get() != NULL or char_class.get() != NULL
        if staged_rows and sketches:
            raise ValueError("a manifest's sketches are the builder's or the vectors given, never both")
        cdef NativeManifest out = NativeManifest.__new__(NativeManifest)
        out._manifest = self._manifest
        self._manifest = new CNativeManifest(vector[string](), False, False)
        out.columns = self._columns
        out.physical = self._physical_types
        out.sketches = sketches
        cdef size_t row
        if staged_rows:
            # owned sketches are laid out one outer row per file, in file order
            for row in range(rows):
                out._manifest.file(row).vector_row = <uint32_t>row
            out._manifest.own_sketches(min_k, histogram, char_class)
        else:
            _bind_sketch_views(out)
        return out



cdef class FileStats:
    """One data file's statistics, fed row group by row group by the writer
    that writes it (file_stats.hpp): the catalog manifest's full statistic set -
    per column the KMV sketch, null count, byte size, ordinal min / max and
    histogram, string byte classes and lengths, ARRAY element statistics. The
    columns are the first row group's; every later row group must carry the
    same ones."""

    cdef FileStatsAccumulator* _accumulator
    cdef tuple _columns
    cdef tuple _physical

    def __cinit__(self):
        self._accumulator = NULL

    def __dealloc__(self):
        del self._accumulator

    def add_row_group(self, morsel):
        cdef vector[DrakenType] physical
        cdef size_t position
        cdef PyObject* handle
        names = tuple(name.decode("utf-8") if type(name) is bytes else name for name in morsel.column_names)
        vectors = [morsel._cxx_column(name) for name in morsel.column_names]
        if self._accumulator == NULL:
            self._columns = names
            self._physical = tuple(vector.type for vector in vectors)
            for draken_type in self._physical:
                physical.push_back(<DrakenType><int>draken_type.value)
            self._accumulator = new FileStatsAccumulator(physical)
        elif names != self._columns:
            raise ValueError(f"a row group of columns {names} in a file of columns {self._columns}")
        for position in range(len(vectors)):
            vector = vectors[position]
            # a draken Vector wrapper or the native Vector itself
            native = vector if type(vector) is _NativeVector else vector._nb
            handle = <PyObject*>native
            self._accumulator.add(position, draken_owner_unwrap(handle)[0])

    @property
    def uncompressed_size(self):
        """The in-memory byte footprint of every row group added so far - the
        manifest's size unit (Morsel.nbytes' accounting, column by column)."""
        return 0 if self._accumulator == NULL else self._accumulator.uncompressed_size()

    def file_row(self, str path, str file_format, int64_t record_count, int64_t file_size,
                 int64_t row_group_count, int64_t uncompressed_size):
        """The finished file as a native file row: a NativeManifest of one file
        over the columns written, bounds in the ordinal dialect."""
        if self._accumulator == NULL:
            raise ValueError(f"no row group was written to '{path}'")
        cdef NativeManifestBuilder builder = NativeManifestBuilder(self._columns, self._physical, True, True)
        cdef size_t row = builder.add_file(path, file_format, record_count, file_size, row_group_count, uncompressed_size)
        cdef vector[vector[uint64_t]] min_k
        cdef vector[vector[int64_t]] histogram
        cdef vector[vector[int64_t]] char_class
        self._accumulator.write(builder._manifest[0], row, min_k, histogram, char_class)
        stage_u64(builder._min_k, row, min_k)
        stage_i64(builder._histogram, row, histogram)
        stage_i64(builder._char_class, row, char_class)
        return builder.build({})


def concat_rows(list batches):
    """Native file rows (NativeManifests over the same columns) as one batch, in
    order, each file's sketch rows with it. An empty list is an empty batch over
    no columns."""
    if not batches:
        return NativeManifestBuilder((), (), True, True).build({})
    cdef NativeManifest first = batches[0]
    cdef NativeManifest batch
    cdef size_t row
    identity = list(range(len(first.columns)))
    cdef NativeManifestBuilder builder = NativeManifestBuilder(first.columns, first.physical, True, True)
    for batch in batches:
        if batch.columns != first.columns:
            raise ValueError(f"file rows over columns {batch.columns} and {first.columns} are not one batch")
        for row in range(len(batch)):
            builder.add_file_from(batch, row, identity)
    return builder.build({})



def aggregate_skene_blobs(list row_groups, list positions, list sketches):
    """What skene_stats.hpp aggregates from per-row-group statistics blobs in
    the shape skene.read_metadata() emits (`row_groups[i]["column_statistics"]`,
    one blob or None per slot; `sketches` one {hash_family, hashes} or None per
    slot; `positions` each slot's column or None) - for checking the native
    rules against hand-built footers. Returns (lower, upper, nulls, distincts,
    sketches, family, floors), each keyed by column position."""
    cdef SkeneFileMetadata meta
    cdef SkeneRowGroupSummary summary
    cdef SkeneRowGroupColumnStatistics slot
    cdef SkeneColumnSketch sketch
    cdef vector[int64_t] at
    cdef vector[DrakenType] physical
    cdef size_t k
    for group in row_groups:
        summary.column_statistics.clear()
        for blob in group["column_statistics"]:
            slot.present = blob is not None
            if blob is not None:
                slot.statistics.flags = blob["flags"] & ~(kStatNdv | kStatNdvExact)
                slot.statistics.min_ordinal = blob["min_ordinal"]
                slot.statistics.max_ordinal = blob["max_ordinal"]
                slot.statistics.null_count = blob["null_count"]
                # None is skene's "not tracked": the flag, not the number, says so
                if blob["ndv"] is not None:
                    slot.statistics.flags |= kStatNdv
                    slot.statistics.ndv = blob["ndv"]
                    if blob["ndv_exact"]:
                        slot.statistics.flags |= kStatNdvExact
            summary.column_statistics.push_back(slot)
        meta.row_groups.push_back(summary)
    for entry in sketches:
        sketch.hashes.clear()
        sketch.hash_family = 0
        if entry is not None:
            sketch.hash_family = entry["hash_family"]
            for value in entry["hashes"]:
                sketch.hashes.push_back(<uint64_t?>value)
        meta.sketches.push_back(sketch)
    width = 1 + max([p for p in positions if p is not None], default=-1)
    for position in positions:
        at.push_back(-1 if position is None else <int64_t?>position)
    for k in range(<size_t>width):
        physical.push_back(DRAKEN_INT64)
    cdef NativeManifestBuilder builder = NativeManifestBuilder(
        tuple(f"c{k}" for k in range(width)), tuple(_INT64 for _ in range(width)), True, True
    )
    cdef size_t row = builder.add_file("f", "SKENE", 0, 0)
    apply_skene_footer(builder._manifest[0], row, meta, physical, at)
    cdef NativeManifest out = builder.build({})
    lower, upper, nulls, distincts, file_sketches, floors = {}, {}, {}, {}, {}, {}
    for k in range(<size_t>width):
        cell = out.cell(0, k)
        if cell["bounds"]["min_ordinal"] is not None:
            lower[k] = cell["bounds"]["min_ordinal"]
            upper[k] = cell["bounds"]["max_ordinal"]
        if cell["null_count"] is not None:
            nulls[k] = cell["null_count"]
        if cell["distinct_count"] is not None:
            distincts[k] = cell["distinct_count"]
        if cell["distinct_sketch"] is not None:
            file_sketches[k] = list(cell["distinct_sketch"])
        if cell["distinct_floor"] is not None:
            floors[k] = cell["distinct_floor"]
    families = {entry["hash_family"] for entry in sketches if entry is not None}
    return lower, upper, nulls, distincts, file_sketches, (families.pop() if families else None), floors
