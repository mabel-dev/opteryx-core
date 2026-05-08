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


cdef class CppIOPipeline:
    # C attributes declared in pool_reader.pxd; only method bodies here.

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
            stats.type_length = s_dict.get('type_length') or 0
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

    def wait_result(self):
        """Block (GIL released) until a result is available or pipeline is drained."""
        cdef MorselRef result
        cdef bint got_result

        with nogil:
            got_result = self.pipeline.wait_and_get_result(result)

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
    cdef int64_t footer_start, n, lp, env_size, off
    cdef uint32_t footer_length_u32
    cdef uint64_t start_ns = time.monotonic_ns()
    cdef const uint8_t[::1] tail_view, footer_view
    cdef uint8_t[::1] env_view
    cdef bytearray env_buf
    cdef bytes envelope

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
    # Buffer-protocol assignment: works for bytes (HTTP/GCS) and memoryview
    # (local filesystem zero-copy path) without branching.
    tail_view = filesystem.read_ranges(path, [(prefetch_offset, prefetch_size)])[0]

    n = tail_view.shape[0]
    if not (tail_view[n - 4] == 0x50   # 'P'
        and tail_view[n - 3] == 0x41   # 'A'
        and tail_view[n - 2] == 0x52   # 'R'
        and tail_view[n - 1] == 0x31): # '1'
        raise ValueError(
            f"File {path!r} does not end with Parquet magic bytes "
            f"(got {bytes(tail_view[n - 4:n])!r}, expected {_PARQUET_MAGIC!r})"
        )

    # 4-byte little-endian read of footer length: typed C arithmetic, no struct.
    lp = n - _PARQUET_FOOTER_SUFFIX
    footer_length_u32 = (
        <uint32_t>tail_view[lp]
        | (<uint32_t>tail_view[lp + 1] << 8)
        | (<uint32_t>tail_view[lp + 2] << 16)
        | (<uint32_t>tail_view[lp + 3] << 24)
    )
    footer_length = <int64_t>footer_length_u32
    if footer_length == 0 or footer_length > file_size - _PARQUET_FOOTER_SUFFIX:
        raise ValueError(
            f"Invalid footer length {footer_length} in {path!r} (file_size={file_size})"
        )

    total_footer_payload = footer_length + _PARQUET_FOOTER_SUFFIX
    if total_footer_payload <= prefetch_size:
        footer_start = n - total_footer_payload
        footer_view = tail_view[footer_start : footer_start + footer_length]
        bytes_fetched = prefetch_size
    else:
        footer_offset = file_size - _PARQUET_FOOTER_SUFFIX - footer_length
        footer_view = filesystem.read_ranges(path, [(footer_offset, footer_length)])[0]
        bytes_fetched = prefetch_size + footer_length

    if _cfg.OPTERYX_TRACE:
        _record_event(
            "download_complete",
            file_id=path, component="footer", bytes_received=bytes_fetched, connector=connector,
        )

    # Assemble envelope: MAGIC + footer + length(LE) + MAGIC, in one contiguous
    # bytearray with a single memcpy of the footer payload. _parse_footer_envelope
    # and the cache contract take `bytes`, so we materialise once at the end.
    env_size = 4 + footer_length + 4 + 4
    env_buf = bytearray(env_size)
    env_view = env_buf

    env_view[0] = 0x50
    env_view[1] = 0x41
    env_view[2] = 0x52
    env_view[3] = 0x31

    env_view[4 : 4 + footer_length] = footer_view

    off = 4 + footer_length
    env_view[off]     = <uint8_t>( footer_length_u32        & 0xff)
    env_view[off + 1] = <uint8_t>((footer_length_u32 >>  8) & 0xff)
    env_view[off + 2] = <uint8_t>((footer_length_u32 >> 16) & 0xff)
    env_view[off + 3] = <uint8_t>((footer_length_u32 >> 24) & 0xff)

    env_view[off + 4] = 0x50
    env_view[off + 5] = 0x41
    env_view[off + 6] = 0x52
    env_view[off + 7] = 0x31

    envelope = bytes(env_buf)

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
            result = pipeline.wait_result()
            t_get_result_ns += time.monotonic_ns() - _ts
            if result is None:
                # Pipeline drained before all work completed — should not happen.
                raise RuntimeError(
                    f"Parquet pipeline drained with {len(work_items) - results_received} "
                    f"result(s) missing"
                )

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
                "         wait_result_total=%.1fms\n"
                "         queue: enqueues=%d high_watermark=%d spin_iters=%d\n"
                % (
                    len(set(p for p, _ in work_items)) if work_items else 0,
                    len(work_items),
                    t_phase1_ns / 1e6, t_phase2_ns / 1e6, t_consume_ns / 1e6,
                    t_footer_ns / 1e6, t_submit_ns / 1e6,
                    t_get_result_ns / 1e6,
                    diag["enqueue_count"], diag["queue_high_watermark"],
                    diag["spin_iterations"],
                )
            )
        pipeline.close()
