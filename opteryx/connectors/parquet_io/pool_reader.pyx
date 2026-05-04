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
from libcpp.unordered_map cimport unordered_map
import struct as _struct
import time

from opteryx.compiled.structures.memory_pool cimport MemoryPool
from opteryx.compiled.structures.column_deserializer cimport deserialize_row_group
from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache as _FooterBytesCache
from opteryx.tracing.event_recorder import record_event as _record_event
from opteryx import config as _cfg
from rugo.parquet_reader import read_metadata_from_bytes as _read_metadata_from_bytes
from rugo.parquet_reader import decode_value as _decode_value_c
from rugo.parquet_reader cimport ReadParquetMetadataFromBuffer, FileStats, RowGroupStats, ColumnStats

_PARQUET_MAGIC = b"PAR1"
_PARQUET_FOOTER_SUFFIX = 8
_FOOTER_PREFETCH = 65536


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
        uint64_t spin_iterations() nogil
        uint64_t enqueue_count() nogil
        size_t queue_high_watermark() nogil


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

    cdef submit_work_native(self, str cpp_path, int rg_idx, list column_names, RowGroupStats* rg):
        """Submit a row group using C++ ColumnStats directly — no Python dict round-trip."""
        cdef vector[string] col_names_vec
        cdef vector[ColumnStats] col_stats_vec
        cdef string path_str = cpp_path.encode('utf-8')
        cdef string cpp_col_name
        cdef size_t i

        for col_name in column_names:
            cpp_col_name = col_name.encode('utf-8')
            col_names_vec.push_back(cpp_col_name)
            for i in range(rg.columns.size()):
                if rg.columns[i].name == cpp_col_name:
                    col_stats_vec.push_back(rg.columns[i])
                    break

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
        cdef const uint8_t* col_ptr
        cdef Py_ssize_t col_len
        cdef const uint8_t[::1] col_view

        for i in range(result.column_ipc_bytes.size()):
            col_name = result.column_names[i].decode('utf-8')
            # Zero-copy handoff: commit() uses the buffer protocol, so a typed
            # memoryview over the C++ vector storage is enough. Avoids a Python
            # bytes() copy of the entire column IPC payload.
            col_len = result.column_ipc_bytes[i].size()
            if col_len == 0:
                ref_id = self.pool.commit(b"")
            else:
                col_ptr = result.column_ipc_bytes[i].data()
                col_view = <const uint8_t[:col_len]>col_ptr
                ref_id = self.pool.commit(col_view)
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

    def diagnostics(self):
        return {
            "spin_iterations": self.pipeline.spin_iterations(),
            "enqueue_count": self.pipeline.enqueue_count(),
            "queue_high_watermark": self.pipeline.queue_high_watermark(),
        }


cdef tuple _read_footer_payload(
    object filesystem,
    str path,
    object file_size_in,
    object connector,
    object footer_cache,
):
    cdef int64_t file_size, prefetch_size, prefetch_offset
    cdef int64_t footer_length, total_footer_payload, footer_offset, bytes_fetched
    cdef uint64_t start_ns = time.monotonic_ns()
    cdef bytes tail_bytes, footer_bytes_data, envelope

    if _cfg.OPTERYX_TRACE:
        _record_event("download_start", file_id=path, component="footer", connector=connector)

    if file_size_in is None or file_size_in <= 0:
        file_info = filesystem.get_file_info(path)
        file_size = file_info.size
    else:
        file_size = file_size_in

    if file_size < _PARQUET_FOOTER_SUFFIX:
        raise ValueError(
            f"File {path!r} is too small to be a valid Parquet file ({file_size} B)"
        )

    if footer_cache is not None:
        cached = footer_cache.get(path)
        if cached is not None:
            return cached, 0, 0

    prefetch_size = min(_FOOTER_PREFETCH, file_size)
    prefetch_offset = file_size - prefetch_size
    (tail_bytes,) = filesystem.read_ranges(path, [(prefetch_offset, prefetch_size)])

    if tail_bytes[-4:] != _PARQUET_MAGIC:
        raise ValueError(
            f"File {path!r} does not end with Parquet magic bytes "
            f"(got {tail_bytes[-4:]!r}, expected {_PARQUET_MAGIC!r})"
        )

    (footer_length,) = _struct.unpack_from("<I", tail_bytes, len(tail_bytes) - _PARQUET_FOOTER_SUFFIX)
    if footer_length == 0 or footer_length > file_size - _PARQUET_FOOTER_SUFFIX:
        raise ValueError(
            f"Invalid footer length {footer_length} in {path!r} (file_size={file_size})"
        )

    total_footer_payload = footer_length + _PARQUET_FOOTER_SUFFIX
    if total_footer_payload <= prefetch_size:
        footer_start = len(tail_bytes) - total_footer_payload
        footer_bytes_data = tail_bytes[footer_start : footer_start + footer_length]
        bytes_fetched = prefetch_size
    else:
        footer_offset = file_size - _PARQUET_FOOTER_SUFFIX - footer_length
        (footer_bytes_data,) = filesystem.read_ranges(path, [(footer_offset, footer_length)])
        bytes_fetched = prefetch_size + footer_length

    if _cfg.OPTERYX_TRACE:
        _record_event(
            "download_complete",
            file_id=path, component="footer", bytes_received=bytes_fetched, connector=connector,
        )

    envelope = _PARQUET_MAGIC + footer_bytes_data + _struct.pack("<I", footer_length) + _PARQUET_MAGIC

    if footer_cache is not None:
        footer_cache.put(path, envelope)

    return envelope, bytes_fetched, time.monotonic_ns() - start_ns


cdef dict _parse_footer_envelope(str path, bytes envelope, int64_t footer_bytes):
    cdef dict meta
    try:
        meta = _read_metadata_from_bytes(envelope)
    except Exception as exc:
        raise RuntimeError(f"Failed to parse Parquet footer from {path!r}: {exc}") from exc
    meta["__footer_bytes__"] = footer_bytes
    return meta


cpdef dict fetch_footer(
    object filesystem,
    str path,
    object file_size = None,
    object connector = None,
    object footer_bytes_cache = None,
):
    cdef bytes envelope
    cdef int64_t footer_bytes
    envelope, footer_bytes, _ = _read_footer_payload(
        filesystem, path, file_size, connector, footer_bytes_cache
    )
    return _parse_footer_envelope(path, envelope, footer_bytes)


cdef bint _rg_passes_predicates_native(RowGroupStats& rg, list predicates):
    """Evaluate AND-combined predicates against RowGroupStats min/max without materialising a Python dict."""
    cdef size_t i
    cdef string col_str
    cdef object min_val, max_val, value, col_name, op

    for pred in predicates:
        col_name, op, value = pred
        col_str = col_name.encode('utf-8') if isinstance(col_name, str) else col_name
        for i in range(rg.columns.size()):
            if rg.columns[i].name != col_str:
                continue
            min_val = _decode_value_c(
                rg.columns[i].physical_type, rg.columns[i].logical_type,
                rg.columns[i].min, False,
            ) if rg.columns[i].has_min else None
            max_val = _decode_value_c(
                rg.columns[i].physical_type, rg.columns[i].logical_type,
                rg.columns[i].max, False,
            ) if rg.columns[i].has_max else None
            if min_val is None or max_val is None:
                break
            try:
                if op == "Eq":
                    if value < min_val or value > max_val:
                        return False
                elif op == "NotEq":
                    if min_val == max_val == value:
                        return False
                elif op == "Gt":
                    if max_val <= value:
                        return False
                elif op == "GtEq":
                    if max_val < value:
                        return False
                elif op == "Lt":
                    if min_val >= value:
                        return False
                elif op == "LtEq":
                    if min_val > value:
                        return False
            except TypeError:
                pass
            break
    return True


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
        queue_capacity=1024,
        pool_size=256*1024*1024,
    )

    cdef uint64_t t_phase1_ns = 0
    cdef uint64_t t_phase2_ns = 0
    cdef uint64_t t_footer_ns = 0
    cdef uint64_t t_submit_ns = 0
    cdef uint64_t t_consume_ns = 0
    cdef uint64_t t_get_result_ns = 0
    cdef uint64_t t_sleep_ns = 0
    cdef uint64_t n_sleep_ticks = 0
    cdef uint64_t _t0, _t1, _ts
    cdef unordered_map[string, FileStats] local_footers_native
    cdef string path_bytes_cpp
    cdef const uint8_t* footer_buf_ptr
    cdef size_t footer_buf_size
    cdef RowGroupStats* rg_ptr
    cdef size_t rg_i

    try:
        _t0 = time.monotonic_ns()
        work_items = []
        for path in paths:
            if prefetched_footers and path in prefetched_footers:
                # Prefetched dict path — predicate check uses Python dict API
                meta = prefetched_footers[path]
                for rg_idx, rg_meta in enumerate(meta.get("row_groups", [])):
                    if predicates and not row_group_may_satisfy(rg_meta, predicates):
                        continue
                    work_items.append((path, rg_idx))
            else:
                path_bytes_cpp = path.encode('utf-8')
                if local_footers_native.count(path_bytes_cpp) == 0:
                    _ts = time.monotonic_ns()
                    envelope, _, _ = _read_footer_payload(
                        filesystem, path, None, connector, footer_bytes_cache
                    )
                    t_footer_ns += time.monotonic_ns() - _ts
                    footer_buf_ptr = <const uint8_t*>envelope
                    footer_buf_size = len(envelope)
                    local_footers_native[path_bytes_cpp] = ReadParquetMetadataFromBuffer(
                        footer_buf_ptr, footer_buf_size
                    )
                for rg_i in range(local_footers_native[path_bytes_cpp].row_groups.size()):
                    if predicates and not _rg_passes_predicates_native(
                        local_footers_native[path_bytes_cpp].row_groups[rg_i], predicates
                    ):
                        continue
                    work_items.append((path, rg_i))
        t_phase1_ns = time.monotonic_ns() - _t0

        if not work_items:
            return

        _t0 = time.monotonic_ns()
        for path, rg_idx in work_items:
            _ts = time.monotonic_ns()
            if prefetched_footers and path in prefetched_footers:
                # Prefetched dict path — column stats come from Python dict
                meta = prefetched_footers[path]
                rg_meta = meta["row_groups"][rg_idx]
                column_stats_dicts = []
                for col_name in column_names:
                    for col_meta in rg_meta["columns"]:
                        if col_meta["name"] == col_name:
                            column_stats_dicts.append(col_meta)
                            break
                pipeline.submit_work(
                    orig_to_cpp.get(path, path), rg_idx, column_names, column_stats_dicts
                )
            else:
                # Native path — column stats come directly from C++ FileStats
                path_bytes_cpp = path.encode('utf-8')
                rg_ptr = &local_footers_native[path_bytes_cpp].row_groups[rg_idx]
                pipeline.submit_work_native(
                    orig_to_cpp.get(path, path), rg_idx, column_names, rg_ptr
                )
            t_submit_ns += time.monotonic_ns() - _ts
        t_phase2_ns = time.monotonic_ns() - _t0

        _t0 = time.monotonic_ns()
        results_received = 0
        while results_received < len(work_items):
            _ts = time.monotonic_ns()
            result = pipeline.get_result()
            t_get_result_ns += time.monotonic_ns() - _ts
            if result is None:
                _ts = time.monotonic_ns()
                time.sleep(0.0001)
                t_sleep_ns += time.monotonic_ns() - _ts
                n_sleep_ticks += 1
                continue

            if not result['success']:
                raise RuntimeError(f"Parquet pipeline error: {result.get('error', 'unknown')}")

            row_group = deserialize_row_group(result['ref_ids'], pipeline.pool)

            # Defensive: detect columns decoded to 0 rows in a non-empty row group.
            # List columns (max_repetition_level > 0) have more values than rows and
            # are intentionally excluded — only a zero-length column in a non-empty
            # row group indicates a C++ decoder bug (silent data loss).
            col_lengths = {
                k: len(v)
                for k, v in row_group.items()
                if not (isinstance(k, str) and k.startswith('__'))
                   and hasattr(v, '__len__')
            }
            if col_lengths:
                lengths = list(col_lengths.values())
                max_len = max(lengths)
                if max_len > 0 and any(l == 0 for l in lengths):
                    raise RuntimeError(
                        f"C++ decoder produced zero-length column(s) in non-empty row group for "
                        f"path={result['path']!r} rg={result['rg_idx']}: {col_lengths}"
                    )

            # Translate signed URL back to the original path for Python consumers.
            row_group['__path__'] = cpp_to_orig.get(result['path'], result['path'])
            row_group['__row_group__'] = result['rg_idx']
            row_group['__parquet_scan_strategy__'] = 'cpp-pipeline'

            results_received += 1
            yield row_group
        t_consume_ns = time.monotonic_ns() - _t0

    finally:
        import os, sys
        if os.environ.get("OPTERYX_IO_DIAG"):
            diag = pipeline.diagnostics()
            sys.stderr.write(
                "\n[io_diag] paths=%d rgs=%d  phase1=%.1fms phase2=%.1fms consume=%.1fms\n"
                "         footer_total=%.1fms  submit_total=%.3fms\n"
                "         get_result_total=%.1fms  sleep_total=%.1fms ticks=%d\n"
                "         queue: enqueues=%d high_watermark=%d spin_iters=%d\n"
                % (
                    len(set(p for p, _ in work_items)) if work_items else 0,
                    len(work_items),
                    t_phase1_ns / 1e6, t_phase2_ns / 1e6, t_consume_ns / 1e6,
                    t_footer_ns / 1e6, t_submit_ns / 1e6,
                    t_get_result_ns / 1e6, t_sleep_ns / 1e6, n_sleep_ticks,
                    diag["enqueue_count"], diag["queue_high_watermark"],
                    diag["spin_iterations"],
                )
            )
        pipeline.close()
