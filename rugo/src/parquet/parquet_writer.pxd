# Cython extern declarations for the rugo parquet writer core
# (_parquet_writer.hpp). Header-only, namespace rugo_pq_write.

from libc.stdint cimport uint8_t, uint32_t, int32_t, int64_t
from libc.stddef cimport size_t
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "_parquet_writer.hpp" namespace "rugo_pq_write":

    cdef enum PType:
        PT_BOOLEAN
        PT_INT32
        PT_INT64
        PT_FLOAT
        PT_DOUBLE
        PT_BYTE_ARRAY
        PT_FLBA

    cdef enum LogicalKindPq:
        LK_NONE
        LK_DATE
        LK_TIMESTAMP
        LK_DECIMAL
        LK_TIME
        LK_INTERVAL

    cdef enum:
        CODEC_UNCOMPRESSED
        CODEC_ZSTD
        TU_MILLIS
        TU_MICROS
        TU_NANOS

    cdef struct StrSlice:
        const uint8_t* ptr
        uint32_t len

    cdef cppclass ColumnInput:
        ColumnInput() except +
        string name
        PType type
        bint is_utf8
        const uint8_t* validity
        const int32_t* i32
        const int64_t* i64
        const double* f64
        const uint8_t* boolean
        const StrSlice* strs
        const uint8_t* dec_raw
        LogicalKindPq logical
        int dec_width
        int dec_scale
        int dec_precision
        int ts_unit
        bint ts_utc
        bint bloom
        const uint32_t* codes
        uint32_t dict_count
        bint dict_enabled
        bint is_unsigned
        int int_bit_width
        bint is_array
        int array_depth
        PType elem_type
        bint elem_is_utf8
        const uint8_t* rep_levels
        const uint8_t* def_levels
        size_t num_levels
        size_t num_elements
        const uint32_t* row_level_offsets
        const uint32_t* row_element_offsets

    cdef cppclass ColumnStats:
        bint has_minmax
        vector[uint8_t] min_bytes
        vector[uint8_t] max_bytes
        int64_t null_count

    vector[uint8_t] WriteParquet(const vector[ColumnInput]& cols,
                                 size_t num_rows, int codec,
                                 int zstd_level,
                                 vector[ColumnStats]* out_stats,
                                 size_t max_rows_per_rg,
                                 size_t max_page_bytes) except + nogil

    cdef cppclass StreamingParquetWriter:
        StreamingParquetWriter(int codec, int zstd_level,
                               size_t max_page_bytes) except +
        void add_row_group(const vector[ColumnInput]& rg_cols,
                           size_t rg_rows) except + nogil
        vector[uint8_t] take_pending() except + nogil
        vector[uint8_t] finish() except + nogil
