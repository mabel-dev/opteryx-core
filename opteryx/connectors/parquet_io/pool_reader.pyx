# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True

"""Pure C++ parquet IO pipeline with lock-free queues.

Replaces pool_reader.py with a Cython bridge to the C++ ParquetIOPipeline.

Architecture:
- C++ threads: read column chunk bytes + decode (DecodeColumnFromChunk)
- C++ results: MorselRef with vector<DecodedColumn>
- Cython bridge: convert DecodedColumn → Draken vectors → MemoryPool
- Result queue: lock-free moodycamel (zero-copy, no mutexes)
"""

from libc.stdint cimport uint8_t, int32_t, int64_t, uint32_t, uint64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map


cdef extern from "../../../../third_party/mabel/rugo/parquet/metadata.hpp":
    cdef cppclass ColumnStats:
        string name
        string physical_type
        string logical_type
        int64_t num_values
        int64_t total_uncompressed_size
        int64_t total_compressed_size
        int64_t data_page_offset
        int64_t dictionary_page_offset
        int32_t codec
        int32_t max_definition_level
        int32_t max_repetition_level


cdef extern from "../../../../third_party/mabel/rugo/parquet/decode.hpp":
    # Forward declare for use in MorselRef
    cdef cppclass DecodedColumn:
        int32_t num_rows
        string type
        bint success


cdef extern from "../../../../third_party/mabel/rugo/parquet/io_pipeline.hpp" namespace "rugo":
    cdef cppclass MorselRef:
        string path
        int rg_idx
        vector[string] column_names
        vector[DecodedColumn] decoded_columns
        int64_t bytes_fetched
        uint64_t read_ns
        uint64_t decode_ns
        string error
        bint success

    cdef cppclass ParquetIOPipeline:
        ParquetIOPipeline(int read_workers, int decode_workers, size_t queue_capacity) except +
        void submit_row_group(const string& path, int rg_idx,
                             const vector[string]& column_names,
                             const vector[ColumnStats]& column_stats) nogil
        bint try_get_result(MorselRef& out) nogil
        void wait_shutdown() nogil
        int pending_work_count() nogil


cdef class CppIOPipeline:
    """Cython wrapper for C++ ParquetIOPipeline."""
    cdef ParquetIOPipeline* pipeline

    def __cinit__(self, int read_workers=16, int decode_workers=4, size_t queue_capacity=256):
        """Initialize the C++ pipeline."""
        self.pipeline = new ParquetIOPipeline(read_workers, decode_workers, queue_capacity)

    def __dealloc__(self):
        """Clean up C++ pipeline."""
        if self.pipeline:
            del self.pipeline
            self.pipeline = NULL

    def submit_work(self, str path, int rg_idx, list column_names, list column_stats_dicts):
        """Submit a row group for processing."""
        cdef vector[string] col_names_vec
        cdef vector[ColumnStats] col_stats_vec
        cdef ColumnStats stats
        cdef string path_str

        # Prepare everything with GIL before entering nogil block
        path_enc = path.encode('utf-8')
        path_str = path_enc

        # Prepare column names
        for col_name in column_names:
            col_name_bytes = col_name.encode('utf-8')
            col_names_vec.push_back(col_name_bytes)

        # Prepare column stats
        for stats_dict in column_stats_dicts:
            stats.name = stats_dict['name'].encode('utf-8')
            stats.physical_type = stats_dict['physical_type'].encode('utf-8')
            stats.logical_type = stats_dict.get('logical_type', '').encode('utf-8')
            stats.num_values = stats_dict.get('num_values', -1)
            stats.total_uncompressed_size = stats_dict.get('total_uncompressed_size', -1)
            stats.total_compressed_size = stats_dict.get('total_compressed_size', -1)
            stats.data_page_offset = stats_dict.get('data_page_offset', -1)
            stats.dictionary_page_offset = stats_dict.get('dictionary_page_offset', -1)
            stats.codec = stats_dict.get('codec', -1)
            stats.max_definition_level = stats_dict.get('max_definition_level', 0)
            stats.max_repetition_level = stats_dict.get('max_repetition_level', 0)
            col_stats_vec.push_back(stats)

        # Submit to pipeline (without GIL)
        with nogil:
            self.pipeline.submit_row_group(path_str, rg_idx, col_names_vec, col_stats_vec)

    def get_result(self):
        """Try to get a decoded row group result (non-blocking)."""
        cdef MorselRef result
        cdef bint got_result

        with nogil:
            got_result = self.pipeline.try_get_result(result)

        if not got_result:
            return None

        if not result.success:
            return {
                'success': False,
                'error': result.error.decode('utf-8'),
                'path': result.path.decode('utf-8'),
                'rg_idx': result.rg_idx,
            }

        # Convert decoded columns to Python/Draken vectors
        # NOTE: This is a placeholder — full implementation needs type dispatch
        vectors = {}
        for i in range(result.decoded_columns.size()):
            col = result.decoded_columns[i]
            col_name = result.column_names[i].decode('utf-8')
            # For now, just store the decoded column metadata
            # Real implementation: convert to DrakenVector based on type
            vectors[col_name] = {
                'type': col.type.decode('utf-8'),
                'num_rows': col.num_rows,
                'success': col.success,
            }

        return {
            'success': result.success,
            'path': result.path.decode('utf-8'),
            'rg_idx': result.rg_idx,
            'column_names': [name.decode('utf-8') for name in result.column_names],
            'vectors': vectors,
            'bytes_fetched': result.bytes_fetched,
            'read_ns': result.read_ns,
            'decode_ns': result.decode_ns,
        }

    def close(self):
        """Shutdown the pipeline."""
        with nogil:
            self.pipeline.wait_shutdown()
