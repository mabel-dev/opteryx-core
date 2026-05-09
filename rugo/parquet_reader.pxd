# parquet_reader.pxd
from libc.stdint cimport uint8_t, int32_t, int64_t, uint32_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map


cdef extern from "metadata.hpp":
    cdef cppclass MetadataParseOptions:
        bint schema_only
        bint include_statistics
        long long max_row_groups
        MetadataParseOptions() except +

    cdef cppclass ColumnStats:
        string name
        string physical_type
        string logical_type

        # Sizes & counts
        int64_t num_values
        int64_t total_uncompressed_size
        int64_t total_compressed_size

        # Offsets
        int64_t data_page_offset
        int64_t index_page_offset
        int64_t dictionary_page_offset

        # Statistics
        bint has_min
        bint has_max
        string min
        string max
        int64_t null_count
        int64_t distinct_count

        # Bloom filter
        int64_t bloom_offset
        int64_t bloom_length

        # Encodings & codec
        vector[int32_t] encodings
        int32_t codec

        # Key/value metadata
        unordered_map[string, string] key_value_metadata

        # Schema-derived level info (Dremel encoding)
        int32_t repetition_type
        int32_t max_definition_level
        int32_t max_repetition_level

        # FIXED_LEN_BYTE_ARRAY width in bytes (0 for other physical types)
        int32_t type_length

    cdef cppclass RowGroupStats:
        long long num_rows
        long long total_byte_size
        vector[ColumnStats] columns

    cdef cppclass SchemaElement:
        string name
        string full_name
        string physical_type
        string logical_type
        int num_children
        int type_length
        int scale
        int precision
        int repetition_type
        vector[SchemaElement] children

    cdef cppclass SchemaField:
        string name
        string physical_type
        string logical_type
        bint nullable

    cdef cppclass FileStats:
        long long num_rows
        vector[RowGroupStats] row_groups
        vector[SchemaElement] schema
        vector[SchemaField] schema_columns

    FileStats ReadParquetMetadataC(const char* path)
    FileStats ReadParquetMetadataFromBuffer(const uint8_t* buf, size_t size) except +
    FileStats ReadParquetMetadataC(const char* path, const MetadataParseOptions& options)
    FileStats ReadParquetMetadata(const string& path, const MetadataParseOptions& options)
    FileStats ReadParquetMetadata(const string& path)
    FileStats ReadParquetMetadataFromBuffer(const uint8_t* buf, size_t size, const MetadataParseOptions& options) except +
    bint TestBloomFilter(const string& file_path, long long bloom_offset, long long bloom_length, const string& value)

    # Helper functions
    const char* EncodingToString(int32_t enc)
    const char* CompressionCodecToString(int32_t codec)

    cdef cppclass AggColumnStat:
        string name
        string physical_type
        string logical_type
        string min_bytes
        string max_bytes
        int64_t null_count
        bint has_min
        bint has_max
        bint null_count_complete

    vector[AggColumnStat] AggregateColumnStats(const FileStats& fs)


cdef extern from "decode.hpp":
    cdef cppclass DecodedColumn:
        vector[uint8_t] valid_bits
        int32_t num_rows
        int32_t max_rep_level
        int32_t max_def_level
        vector[int32_t] rep_levels
        vector[int32_t] def_levels
        vector[int32_t] int32_values
        vector[int64_t] int64_values
        vector[string] string_values
        vector[int32_t] dict_indices
        vector[int32_t] dict_int32_values
        vector[int64_t] dict_int64_values
        vector[float] dict_float32_values
        vector[double] dict_float64_values
        vector[uint8_t] boolean_values
        vector[float] float32_values
        vector[double] float64_values
        string type
        int32_t pages_skipped
        int32_t pages_decoded
        bint success
        # Zero-copy output pointers (set by caller before decode)
        int64_t* ext_int64
        double*  ext_float64
        int32_t* ext_int32
        float*   ext_float32
        int32_t  ext_written
        # Flat arena for byte_array dict values (no per-entry std::string alloc)
        vector[uint8_t]  string_dict_arena
        vector[uint32_t] string_dict_offsets
        vector[int32_t]  string_dict_lens
        uint8_t code_width
        bint dict_ordered
        # Packed dictionary codes (for nullable dict columns)
        vector[uint8_t] dict_codes_array  # Full-width packed code array (code_width bytes/row)
        # RLE skip-dense outputs (non-nullable dict columns only)
        vector[int64_t]  rle_int64_values
        vector[double]   rle_float64_values
        vector[int32_t]  rle_run_lengths
        size_t           rle_total_length
        vector[uint8_t]  rle_str_arena
        vector[uint32_t] rle_str_offsets
        vector[int32_t]  rle_str_lens

    cdef cppclass DecodedTable:
        vector[vector[DecodedColumn]] row_groups  # [row_group][column]
        vector[string] column_names
        bint success

    bint CanDecode(const string& path)
    bint CanDecode(const uint8_t* data, size_t size)

    # New memory-based functions
    DecodedColumn DecodeColumnFromChunk(const uint8_t* data, size_t size, const ColumnStats* col) nogil
    DecodedColumn DecodeColumnFromChunk(const uint8_t* data, size_t size, const ColumnStats* col, const uint8_t* row_mask) nogil
    DecodedColumn DecodeColumnFromChunk(const uint8_t* data, size_t size, const ColumnStats* col, int64_t* ext_int64, double* ext_float64, int32_t* ext_int32, float* ext_float32) nogil
    DecodedColumn DecodeColumnFromChunk(const uint8_t* data, size_t size, const ColumnStats* col, int64_t* ext_int64, double* ext_float64, int32_t* ext_int32, float* ext_float32, const uint8_t* row_mask) nogil
    DecodedColumn DecodeColumnFromMemory(const uint8_t* data, size_t size, const string& column_name, const RowGroupStats& row_group, int row_group_index) nogil
    DecodedColumn DecodeColumnFromMemory(const uint8_t* data, size_t size, const string& column_name, const RowGroupStats& row_group, int row_group_index, int64_t* ext_int64, double* ext_float64, int32_t* ext_int32, float* ext_float32) nogil
    DecodedTable ReadParquet(const uint8_t* data, size_t size, const vector[string]& column_names) nogil
    DecodedTable ReadParquet(const uint8_t* data, size_t size) nogil

cdef extern from "filesystem.hpp" namespace "rugo":
    cdef cppclass ParquetFooterResult:
        vector[uint8_t] envelope
        int64_t bytes_fetched

    ParquetFooterResult FetchParquetFooter(const string& path, int64_t file_size) except +
    ParquetFooterResult FetchParquetFooter(const string& path) except +


cdef extern from "type_widening.hpp" namespace "parquet_simd":
    # SIMD-accelerated type widening functions
    void widen_int32_to_int64(const int32_t* src, int64_t* dst, size_t count) nogil
    void widen_float32_to_float64(const float* src, double* dst, size_t count) nogil
