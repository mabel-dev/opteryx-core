# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True

"""
Pure C++ Parquet IO pipeline with lock-free queues.

No Python in hot path. All operations are typed Cython/C++.
- C++ threads: read column chunk bytes + decode (DecodeColumnFromChunk)
- Lock-free queue: moodycamel carries result descriptors
- MemoryPool: zero-copy storage for decoded columns
- GIL: Released during I/O and decode operations
"""

from libc.stdint cimport uint8_t, int32_t, int64_t, uint32_t, uint64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
import struct
import json
import time

from opteryx.compiled.structures.memory_pool cimport MemoryPool
from opteryx.compiled.structures.column_deserializer cimport deserialize_row_group


# C++ metadata structures
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


# C++ decoded column structure
cdef extern from "../../../../third_party/mabel/rugo/parquet/decode.hpp":
    cdef cppclass DecodedColumn:
        vector[uint8_t] valid_bits
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
        int32_t num_rows
        int32_t pages_skipped
        int32_t pages_decoded
        int32_t max_rep_level
        int32_t max_def_level
        vector[int32_t] rep_levels
        vector[int32_t] def_levels
        bint success


# C++ IO pipeline
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
    """Cython wrapper for C++ ParquetIOPipeline - pure compiled code, no GIL in hot path."""
    cdef ParquetIOPipeline* pipeline
    cdef MemoryPool pool

    def __cinit__(self, int read_workers=16, int decode_workers=4, size_t queue_capacity=256,
                  int64_t pool_size=256*1024*1024):
        """Initialize the C++ pipeline with a MemoryPool for decoded column storage.

        Args:
            read_workers: Number of read worker threads
            decode_workers: Number of decode worker threads
            queue_capacity: Size of result queue
            pool_size: Size of MemoryPool for storing decoded column bytes (default 256MB)
        """
        self.pipeline = new ParquetIOPipeline(read_workers, decode_workers, queue_capacity)
        self.pool = MemoryPool(pool_size, name="parquet-io", auto_resize=False)

    def __dealloc__(self):
        """Clean up C++ pipeline and memory pool."""
        if self.pipeline:
            del self.pipeline
            self.pipeline = NULL

    def submit_work(self, str path, int rg_idx, list column_names, list column_stats_dicts):
        """Submit a row group for processing (nogil)."""
        cdef vector[string] col_names_vec
        cdef vector[ColumnStats] col_stats_vec
        cdef string path_str

        # Map codec names to integers (must match rugo::CompressionCodec enum)
        codec_map = {
            'UNCOMPRESSED': 0,
            'SNAPPY': 1,
            'GZIP': 2,
            'LZO': 3,
            'BROTLI': 4,
            'LZ4': 5,
            'ZSTD': 6,
        }

        # Prepare everything with GIL before entering nogil block
        path_enc = path.encode('utf-8')
        path_str = path_enc

        # Prepare column names
        for col_name in column_names:
            col_name_bytes = col_name.encode('utf-8')
            col_names_vec.push_back(col_name_bytes)

        # Prepare column stats - build list of dicts for C++ conversion
        stats_list = []
        for stats_dict in column_stats_dicts:
            codec_name = stats_dict.get('compression_codec', 'UNCOMPRESSED')
            stats_list.append({
                'name': stats_dict['name'],
                'physical_type': stats_dict['physical_type'],
                'logical_type': stats_dict.get('logical_type', ''),
                'num_values': stats_dict.get('num_values', -1),
                'total_uncompressed_size': stats_dict.get('total_uncompressed_size', -1),
                'total_compressed_size': stats_dict.get('total_compressed_size', -1),
                'data_page_offset': stats_dict.get('data_page_offset', -1),
                'dictionary_page_offset': stats_dict.get('dictionary_page_offset', -1),
                'codec': codec_map.get(codec_name, 0),
                'max_definition_level': stats_dict.get('max_definition_level', 0),
                'max_repetition_level': stats_dict.get('max_repetition_level', 0),
            })

        # Build C++ ColumnStats vector
        for s_dict in stats_list:
            cdef ColumnStats stats
            stats.name = s_dict['name'].encode('utf-8')
            stats.physical_type = s_dict['physical_type'].encode('utf-8')
            stats.logical_type = s_dict['logical_type'].encode('utf-8')
            stats.num_values = s_dict['num_values']
            stats.total_uncompressed_size = s_dict['total_uncompressed_size']
            stats.total_compressed_size = s_dict['total_compressed_size']
            stats.data_page_offset = s_dict['data_page_offset']
            stats.dictionary_page_offset = s_dict['dictionary_page_offset']
            stats.codec = s_dict['codec']
            stats.max_definition_level = s_dict['max_definition_level']
            stats.max_repetition_level = s_dict['max_repetition_level']
            col_stats_vec.push_back(stats)

        # Submit to pipeline (without GIL)
        with nogil:
            self.pipeline.submit_row_group(path_str, rg_idx, col_names_vec, col_stats_vec)

    cdef bytes _serialize_decoded_column(self, DecodedColumn& col, str col_name):
        """Serialize a DecodedColumn to bytes for storage in MemoryPool."""
        col_type = col.type.decode('utf-8')
        type_bytes = col_type.encode('utf-8')

        # Serialize null bitmap
        null_bitmap = bytes(col.valid_bits)

        # Serialize data based on type
        if col_type == "int64":
            data = bytes(col.int64_values)
        elif col_type == "int32":
            data = bytes(col.int32_values)
        elif col_type == "float64":
            data = bytes(col.float64_values)
        elif col_type == "float32":
            data = bytes(col.float32_values)
        elif col_type == "boolean":
            data = bytes(col.boolean_values)
        elif col_type == "string":
            # String encoding: serialize dict_indices + string_values
            if col.dict_indices.size() > 0:
                # Dictionary-encoded: indices + dictionary
                indices_bytes = bytes(col.dict_indices)
                dict_values = json.dumps([s.decode('utf-8') if isinstance(s, bytes) else s
                                         for s in col.string_values]).encode('utf-8')
                data = struct.pack('<I', 1) + struct.pack('<Q', len(indices_bytes)) + indices_bytes + \
                       struct.pack('<Q', len(dict_values)) + dict_values
            else:
                # Plain strings: serialize as JSON
                string_values = json.dumps([s.decode('utf-8') if isinstance(s, bytes) else s
                                           for s in col.string_values]).encode('utf-8')
                data = struct.pack('<I', 0) + string_values
        else:
            raise ValueError(f"Unsupported column type: {col_type}")

        # Pack: type_len, type, num_rows, null_bitmap_len, null_bitmap, data_len, data
        parts = []
        parts.append(struct.pack('<I', len(type_bytes)))
        parts.append(type_bytes)
        parts.append(struct.pack('<q', col.num_rows))
        parts.append(struct.pack('<Q', len(null_bitmap)))
        parts.append(null_bitmap)
        parts.append(struct.pack('<Q', len(data)))
        parts.append(data)

        return b''.join(parts)

    def get_result(self):
        """Try to get a decoded row group result, serialize to MemoryPool + descriptors (nogil)."""
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

        # Serialize each decoded column to MemoryPool, create descriptors
        row_group_descriptors = {}
        for i in range(result.decoded_columns.size()):
            col = result.decoded_columns[i]
            col_name = result.column_names[i].decode('utf-8')

            if not col.success:
                return {
                    'success': False,
                    'error': f'Column {col_name} decode failed',
                    'path': result.path.decode('utf-8'),
                    'rg_idx': result.rg_idx,
                }

            # Serialize column to bytes
            col_bytes = self._serialize_decoded_column(col, col_name)

            # Store in MemoryPool
            ref_id = self.pool.commit(col_bytes)
            if ref_id == -1:
                return {
                    'success': False,
                    'error': f'MemoryPool exhausted storing column {col_name}',
                    'path': result.path.decode('utf-8'),
                    'rg_idx': result.rg_idx,
                }

            # Create descriptor dict
            row_group_descriptors[col_name] = {
                'column_name': col_name,
                'column_type': col.type.decode('utf-8'),
                'num_rows': col.num_rows,
                'null_count': sum(1 for b in col.valid_bits if not b),
                'ref_id': ref_id,
                'data_offset': 0,
                'data_length': len(col_bytes),
                'metadata': {
                    'pages_skipped': col.pages_skipped,
                    'pages_decoded': col.pages_decoded,
                }
            }

        return {
            'success': True,
            'path': result.path.decode('utf-8'),
            'rg_idx': result.rg_idx,
            'column_descriptors': row_group_descriptors,
            'bytes_fetched': result.bytes_fetched,
            'read_ns': result.read_ns,
            'decode_ns': result.decode_ns,
        }

    def close(self):
        """Shutdown the pipeline (nogil)."""
        with nogil:
            self.pipeline.wait_shutdown()


def iter_row_groups_ipc(
    filesystem,
    paths,
    column_names,
    cache=None,
    read_workers=16,
    decode_workers=4,
    predicates=None,
    file_sizes=None,
    connector=None,
    query_id=None,
    prefetched_footers=None,
    footer_bytes_cache=None,
):
    """
    Pure Cython parquet IO pipeline - C++ read/decode with lock-free transport.

    No Python in hot path. All I/O and decode operations happen in C++ threads
    without GIL contention. Results are transported via lock-free moodycamel queue
    and materialized as Draken vectors on consumer side.
    """
    from opteryx.connectors.parquet_io.reader import fetch_footer
    from opteryx.connectors.parquet_io.predicates import row_group_may_satisfy

    # Initialize C++ pipeline
    cdef CppIOPipeline pipeline = CppIOPipeline(
        read_workers=read_workers,
        decode_workers=decode_workers,
        queue_capacity=256,
        pool_size=256*1024*1024
    )

    try:
        # Fetch footers and build work items
        work_items = []
        for path in paths:
            # Use prefetched footer if available, otherwise fetch
            if prefetched_footers and path in prefetched_footers:
                meta = prefetched_footers[path]
            else:
                meta = fetch_footer(
                    filesystem,
                    path,
                    cache=cache,
                    connector=connector,
                    footer_bytes_cache=footer_bytes_cache,
                )

            # Filter row groups by predicates if provided
            for rg_idx, rg_meta in enumerate(meta.get("row_groups", [])):
                if predicates and not row_group_may_satisfy(rg_meta, predicates):
                    continue
                work_items.append((path, rg_idx))

        if not work_items:
            return

        # Submit all work to C++ pipeline
        import os
        for path, rg_idx in work_items:
            if rg_idx == 0 and not os.path.exists(path):
                import sys
                print(f"DEBUG: Path does not exist: {path}", file=sys.stderr)
            # Get column stats from metadata
            if prefetched_footers and path in prefetched_footers:
                meta = prefetched_footers[path]
            else:
                meta = fetch_footer(
                    filesystem,
                    path,
                    cache=cache,
                    connector=connector,
                    footer_bytes_cache=footer_bytes_cache,
                )
            rg_meta = meta["row_groups"][rg_idx]

            # Build column_stats list
            column_stats_dicts = []
            for col_name in column_names:
                for col_meta in rg_meta["columns"]:
                    if col_meta["name"] == col_name:
                        column_stats_dicts.append(col_meta)
                        break

            pipeline.submit_work(path, rg_idx, column_names, column_stats_dicts)

        # Consume results as they arrive
        results_received = 0
        while results_received < len(work_items):
            result = pipeline.get_result()
            if result is None:
                time.sleep(0.0001)  # Back-off poll
                continue

            if not result['success']:
                raise RuntimeError(f"C++ Pipeline error: {result.get('error', 'unknown')}")

            # Deserialize columns from MemoryPool (Cython operation)
            row_group_descriptors = result['column_descriptors']
            columns_data = deserialize_row_group(row_group_descriptors, pipeline.pool)

            # Extract Draken vectors
            row_group = {}
            for col_name, col_data in columns_data.items():
                row_group[col_name] = col_data['vector']

            # Add metadata
            row_group['__path__'] = result['path']
            row_group['__row_group__'] = result['rg_idx']
            row_group['__parquet_scan_strategy__'] = 'cpp-pipeline'

            results_received += 1
            yield row_group

    finally:
        pipeline.close()
