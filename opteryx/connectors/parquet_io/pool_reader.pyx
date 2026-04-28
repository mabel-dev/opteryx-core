# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True

"""
C++ Parquet IO pipeline with lock-free queues.

No Python in the hot path. All IO, decode, and IPC-serialize run in C++:
- Local files: POSIX pread()
- HTTP / HTTPS: libcurl range reads
- GCS gs://: rewritten to https://storage.googleapis.com/... then libcurl
- MemoryPool: zero-copy IPC handoff to the query engine
"""

from libc.stdint cimport uint8_t, int32_t, int64_t, uint32_t, uint64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
import time

from opteryx.compiled.structures.memory_pool cimport MemoryPool
from opteryx.compiled.structures.column_deserializer cimport deserialize_row_group


cdef extern from "metadata.hpp":
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
        vector[int32_t] encodings


cdef extern from "io_pipeline.hpp" namespace "rugo":
    cdef cppclass MorselRef:
        string path
        int rg_idx
        vector[string] column_names
        vector[vector[uint8_t]] column_ipc_bytes
        int64_t bytes_fetched
        uint64_t read_ns
        uint64_t decode_ns
        string error
        bint success

    cdef cppclass ParquetIOPipeline:
        ParquetIOPipeline(int decode_workers, size_t queue_capacity) except +
        void submit_row_group(const string& path, int rg_idx,
                             const vector[string]& column_names,
                             const vector[ColumnStats]& column_stats) nogil
        bint try_get_result(MorselRef& out) nogil
        void wait_shutdown() nogil
        int pending_work_count() nogil


cdef class CppIOPipeline:
    cdef ParquetIOPipeline* pipeline
    cdef MemoryPool pool

    def __cinit__(self, int decode_workers=4, size_t queue_capacity=256,
                  int64_t pool_size=256*1024*1024):
        self.pipeline = new ParquetIOPipeline(decode_workers, queue_capacity)
        self.pool = MemoryPool(pool_size, name="parquet-io", auto_resize=False)

    def __dealloc__(self):
        if self.pipeline:
            del self.pipeline
            self.pipeline = NULL

    def submit_work(self, str path, int rg_idx, list column_names, list column_stats_dicts):
        """Submit a row group for C++ read+decode+serialize (absolute file offsets)."""
        cdef vector[string] col_names_vec
        cdef vector[ColumnStats] col_stats_vec
        cdef string path_str

        codec_map = {
            'UNCOMPRESSED': 0, 'SNAPPY': 1, 'GZIP': 2, 'LZO': 3,
            'BROTLI': 4, 'LZ4': 5, 'ZSTD': 6,
        }
        encoding_map = {
            'PLAIN': 0, 'RLE': 3, 'PLAIN_DICTIONARY': 2,
            'RLE_DICTIONARY': 8, 'BYTE_STREAM_SPLIT': 9,
        }

        path_str = path.encode('utf-8')

        for col_name in column_names:
            col_names_vec.push_back(col_name.encode('utf-8'))

        cdef ColumnStats stats
        for s_dict in column_stats_dicts:
            codec_name = s_dict.get('compression_codec', 'UNCOMPRESSED')
            encoding_codes = [encoding_map[e] for e in s_dict.get('encodings', [])
                              if e in encoding_map]

            stats.name = s_dict['name'].encode('utf-8')
            stats.physical_type = s_dict['physical_type'].encode('utf-8')
            stats.logical_type = s_dict.get('logical_type', '').encode('utf-8')
            stats.num_values = s_dict.get('num_values', -1)
            stats.total_uncompressed_size = s_dict.get('total_uncompressed_size', -1)
            stats.total_compressed_size = s_dict.get('total_compressed_size', -1)
            stats.data_page_offset = s_dict.get('data_page_offset', -1)
            stats.dictionary_page_offset = s_dict.get('dictionary_page_offset', -1)
            stats.codec = codec_map.get(codec_name, 0)
            stats.max_definition_level = s_dict.get('max_definition_level', 0)
            stats.max_repetition_level = s_dict.get('max_repetition_level', 0)
            stats.encodings.clear()
            for enc_code in encoding_codes:
                stats.encodings.push_back(enc_code)
            col_stats_vec.push_back(stats)

        with nogil:
            self.pipeline.submit_row_group(path_str, rg_idx, col_names_vec, col_stats_vec)

    def get_result(self):
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

        cdef dict ref_ids = {}
        cdef int64_t ref_id
        cdef str col_name

        for i in range(result.column_ipc_bytes.size()):
            col_name = result.column_names[i].decode('utf-8')
            ref_id = self.pool.commit(bytes(result.column_ipc_bytes[i]))
            if ref_id == -1:
                return {
                    'success': False,
                    'error': f'MemoryPool exhausted storing column {col_name}',
                    'path': result.path.decode('utf-8'),
                    'rg_idx': result.rg_idx,
                }
            ref_ids[col_name] = ref_id

        return {
            'success': True,
            'path': result.path.decode('utf-8'),
            'rg_idx': result.rg_idx,
            'ref_ids': ref_ids,
            'bytes_fetched': result.bytes_fetched,
            'read_ns': result.read_ns,
            'decode_ns': result.decode_ns,
        }

    def close(self):
        with nogil:
            self.pipeline.wait_shutdown()


def iter_row_groups_ipc(
    filesystem,
    paths,
    column_names,
    decode_workers=4,
    predicates=None,
    file_sizes=None,
    connector=None,
    query_id=None,
    prefetched_footers=None,
    footer_bytes_cache=None,
    **kwargs,
):
    """
    C++ Parquet IO pipeline: read + decode + serialize all in C++, no Python in hot path.
    Supports local files (POSIX), HTTP/HTTPS (libcurl), and GCS gs:// (rewritten to
    signed HTTPS URLs at submission time so C++ libcurl needs no auth headers).
    """
    from opteryx.connectors.parquet_io.reader import fetch_footer
    from opteryx.connectors.parquet_io.predicates import row_group_may_satisfy

    # Planning-time URL signer: converts gs:// paths to signed HTTPS URLs so the
    # C++ pipeline can fetch them via libcurl without any Authorization header.
    # Only present on GCS filesystems; local/HTTP paths pass through unchanged.
    # Each unique path is signed once; a reverse map translates C++ result paths
    # back to original paths for Python-side consumers (fetch_columns, telemetry).
    sign_url = getattr(filesystem, "rewrite_to_signed_url", None)
    cdef dict orig_to_cpp = {}
    cdef dict cpp_to_orig = {}
    if sign_url:
        for path in paths:
            if path not in orig_to_cpp:
                cpp_path = sign_url(path)
                orig_to_cpp[path] = cpp_path
                cpp_to_orig[cpp_path] = path

    cdef CppIOPipeline pipeline = CppIOPipeline(
        decode_workers=decode_workers,
        queue_capacity=256,
        pool_size=256*1024*1024,
    )

    try:
        work_items = []
        for path in paths:
            if prefetched_footers and path in prefetched_footers:
                meta = prefetched_footers[path]
            else:
                meta = fetch_footer(
                    filesystem, path,
                    connector=connector, footer_bytes_cache=footer_bytes_cache,
                )
            for rg_idx, rg_meta in enumerate(meta.get("row_groups", [])):
                if predicates and not row_group_may_satisfy(rg_meta, predicates):
                    continue
                work_items.append((path, rg_idx))

        if not work_items:
            return

        for path, rg_idx in work_items:
            if prefetched_footers and path in prefetched_footers:
                meta = prefetched_footers[path]
            else:
                meta = fetch_footer(
                    filesystem, path,
                    connector=connector, footer_bytes_cache=footer_bytes_cache,
                )
            rg_meta = meta["row_groups"][rg_idx]

            column_stats_dicts = []
            for col_name in column_names:
                for col_meta in rg_meta["columns"]:
                    if col_meta["name"] == col_name:
                        column_stats_dicts.append(col_meta)
                        break

            # Submit the signed URL to C++; Python path stays unchanged.
            pipeline.submit_work(
                orig_to_cpp.get(path, path), rg_idx, column_names, column_stats_dicts
            )

        results_received = 0
        while results_received < len(work_items):
            result = pipeline.get_result()
            if result is None:
                time.sleep(0.0001)
                continue

            if not result['success']:
                raise RuntimeError(f"Parquet pipeline error: {result.get('error', 'unknown')}")

            row_group = deserialize_row_group(result['ref_ids'], pipeline.pool)
            # Translate signed URL back to the original path for Python consumers.
            row_group['__path__'] = cpp_to_orig.get(result['path'], result['path'])
            row_group['__row_group__'] = result['rg_idx']
            row_group['__parquet_scan_strategy__'] = 'cpp-pipeline'

            results_received += 1
            yield row_group

    finally:
        pipeline.close()
