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
C++ Parquet IO pipeline with lock-free queues.

No Python in the hot path. All IO, decode, and IPC-serialize run in C++:
- Local files: POSIX pread()
- HTTP / HTTPS: libcurl range reads
- GCS gs://: rewritten to https://storage.googleapis.com/... then libcurl
- MemoryPool: zero-copy IPC handoff to the query engine
"""

from libc.stdint cimport uint8_t, int32_t, int64_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
import time

from opteryx.compiled.structures.memory_pool cimport MemoryPool
from opteryx.compiled.structures.column_deserializer cimport deserialize_row_group
from opteryx.compiled.structures.footer_cache cimport ParquetFooterBytesCache

# WP-6b direct path: wrap worker-built Draken buffers into Vectors (consumer-side,
# GIL held). DirectKind 1=int64 2=float32 3=float64 4=bool 5=decimal128.
from draken.core.buffers cimport (
    DrakenType, DRAKEN_INT64, DRAKEN_FLOAT32, DRAKEN_FLOAT64, DRAKEN_BOOL, DRAKEN_DECIMAL128
)
from draken.vectors.vector cimport Vector, from_decoded as _vector_from_decoded
from rugo.parquet_reader import decode_value as _decode_value_c, _make_scan_row_group
from rugo.parquet_reader cimport ReadParquetMetadataFromBuffer, FileStats, RowGroupStats, ColumnStats, AggColumnStat, AggregateColumnStats
from rugo.parquet_reader cimport EncodingToString, CompressionCodecToString
from rugo.parquet_reader cimport ParquetFooterResult, FetchParquetFooter

_PARQUET_MAGIC = b"PAR1"
_PARQUET_FOOTER_SUFFIX = 8
_FOOTER_PREFETCH = 65536


cdef inline tuple _split_columns(MorselRef* result):
    """Split a result's columns into (pool ref_ids, direct Vectors).

    Pool-path columns (direct_kind == 0) return their MemoryPool ref_id for the
    deserializer. Direct-path columns (WP-6b: non-nullable fixed-width) are
    wrapped here into a Draken Vector via ownership transfer — morsel_take_direct
    hands the draken_alloc'd buffer to the Vector and nulls the MorselRef slot so
    its destructor won't double-free.
    """
    cdef dict ref_ids = {}
    cdef dict direct = {}
    cdef bytes col_name
    cdef int dk
    cdef DrakenType dtype
    cdef uint32_t dlen
    cdef void* dptr
    cdef uint8_t* dval
    cdef Vector vec
    cdef size_t i
    for i in range(result.columns.size()):
        col_name = result.column_names[i]   # bytes — names are bytes throughout
        dk = result.columns[i].direct_kind
        if dk == 0:
            ref_ids[col_name] = result.columns[i].ref_id
            continue
        if dk == 1:
            dtype = DRAKEN_INT64
        elif dk == 2:
            dtype = DRAKEN_FLOAT32
        elif dk == 3:
            dtype = DRAKEN_FLOAT64
        elif dk == 4:
            dtype = DRAKEN_BOOL
        else:  # dk == 5
            dtype = DRAKEN_DECIMAL128
        dlen = result.columns[i].length
        # Take both data + validity (nulls the slots so the dtor won't free them).
        dptr = morsel_take_direct(result[0], i, &dval)
        vec = _vector_from_decoded(dptr, dval, dlen, dtype)
        if dk == 5 and dlen > 0:
            vec._nb.set_decimal_descriptor(
                result.columns[i].dec_precision, result.columns[i].dec_scale)
        direct[col_name] = vec
    return ref_ids, direct


cdef class CppIOPipeline:
    # C attributes declared in pool_reader.pxd; only method bodies here.

    def __cinit__(self, int decode_workers=4, size_t queue_capacity=256,
                  int64_t pool_size=256*1024*1024):
        self.pipeline = new ParquetIOPipeline(decode_workers, queue_capacity)
        self.pool = MemoryPool(pool_size, name="parquet-io", auto_resize=False)
        self.committed_bytes = 0
        # Workers serialize decoded columns directly into this pool's reserved
        # memory — no intermediate heap buffer, no consumer-side commit() copy.
        wire_pool_sink(self.pipeline, self.pool._pool)

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

    cdef submit_work_native_masked(self, str cpp_path, int rg_idx, list column_names, RowGroupStats* rg, bytes row_mask):
        """Submit a row group with a per-row mask using C++ ColumnStats directly."""
        cdef vector[string] col_names_vec
        cdef vector[ColumnStats] col_stats_vec
        cdef vector[uint8_t] mask_vec
        cdef string path_str = cpp_path.encode('utf-8')
        cdef string cpp_col_name
        cdef Py_ssize_t i
        cdef Py_ssize_t num_rows = <Py_ssize_t>rg.num_rows
        cdef Py_ssize_t packed_len = (num_rows + 7) >> 3
        cdef const uint8_t* mask_ptr = <const uint8_t*>row_mask

        for col_name in column_names:
            cpp_col_name = col_name.encode('utf-8')
            col_names_vec.push_back(cpp_col_name)
            for i in range(rg.columns.size()):
                if rg.columns[i].name == cpp_col_name:
                    col_stats_vec.push_back(rg.columns[i])
                    break

        if num_rows > 0 and len(row_mask) < packed_len:
            raise ValueError(
                f"row mask for row group {rg_idx} is too short: "
                f"expected at least {packed_len} packed bytes for {num_rows} rows, "
                f"got {len(row_mask)}"
            )

        # Pass-1 stores a bit-packed bitmap; the native parquet decoder expects
        # one byte per logical row, so expand it here before handing off.
        mask_vec.resize(num_rows)
        for i in range(num_rows):
            mask_vec[i] = (mask_ptr[i >> 3] >> (i & 7)) & 1

        with nogil:
            self.pipeline.submit_row_group(path_str, rg_idx, col_names_vec, col_stats_vec, mask_vec)

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

        # Pool columns → ref_ids (deserialized later); direct columns (WP-6b) →
        # Vectors wrapped here via ownership transfer.
        cdef dict ref_ids
        cdef dict direct
        ref_ids, direct = _split_columns(&result)

        return {
            'success': True,
            'path': result.path.decode('utf-8'),
            'rg_idx': result.rg_idx,
            'ref_ids': ref_ids,
            'direct': direct,
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

        # Pool columns → ref_ids (deserialized later); direct columns (WP-6b) →
        # Vectors wrapped here via ownership transfer.
        cdef dict ref_ids
        cdef dict direct
        ref_ids, direct = _split_columns(&result)

        return {
            'success': True,
            'path': result.path.decode('utf-8'),
            'rg_idx': result.rg_idx,
            'ref_ids': ref_ids,
            'direct': direct,
            'bytes_fetched': result.bytes_fetched,
            'read_ns': result.read_ns,
            'decode_ns': result.decode_ns,
        }

    def close(self):
        with nogil:
            self.pipeline.wait_shutdown()

    def diagnostics(self):
        cdef int i
        cdef int n_buckets = self.pipeline.http_latency_bucket_count()
        # Histogram as [(upper_bound_ms, count), ...]; bound 0 = overflow bucket.
        cdef list latency_histogram = [
            (self.pipeline.http_latency_bucket_bound_ms(i), self.pipeline.http_latency_bucket(i))
            for i in range(n_buckets)
        ]
        return {
            "spin_iterations": self.pipeline.spin_iterations(),
            "enqueue_count": self.pipeline.enqueue_count(),
            "queue_high_watermark": self.pipeline.queue_high_watermark(),
            "http_request_count": self.pipeline.http_request_count(),
            "http_fetch_ops": self.pipeline.http_fetch_ops(),
            "http_latency_histogram_ms": latency_histogram,
            "worker_blocked_ns": self.pipeline.worker_blocked_ns(),
            "ipc_bytes_serialized": self.pipeline.ipc_bytes_serialized(),
            "ipc_bytes_committed": self.committed_bytes,
        }


cdef tuple _read_footer_payload(
    str path,
    int64_t file_size,
    ParquetFooterBytesCache footer_cache,
):
    """Fetch the Parquet footer envelope. Pure C++ IO, typed cache.

    Returns (envelope_bytes, bytes_fetched). Pass file_size=-1 to auto-detect
    via stat() (local) or HEAD (http/gcs). Pass footer_cache=None to bypass
    the cache.
    """
    cdef ParquetFooterResult result
    cdef size_t env_sz
    cdef const uint8_t* env_ptr
    cdef bytes envelope

    if footer_cache is not None:
        cached = footer_cache.get(path)
        if cached is not None:
            return cached, 0

    result = FetchParquetFooter(path.encode("utf-8"), file_size)

    env_sz = result.envelope.size()
    env_ptr = result.envelope.data()
    envelope = env_ptr[:env_sz]

    if footer_cache is not None:
        footer_cache.put(path, envelope)

    return envelope, result.bytes_fetched


cpdef dict fetch_column_chunk_info(
    str path,
    int rg_idx,
    list column_names,
    ParquetFooterBytesCache footer_bytes_cache = None,
):
    """Return decode metadata for the requested columns in one row group.

    Returns {col_name: col_info_dict} containing only the fields needed to
    read and decode column chunks (offsets, sizes, codec, encodings, levels,
    type info).  Only requested columns are decoded — no Python objects created
    for the rest of the schema.
    """
    cdef bytes envelope
    cdef const uint8_t* buf_ptr
    cdef size_t buf_size
    cdef FileStats fs
    cdef size_t rg_count, col_count, col_i
    cdef str col_name

    envelope, _ = _read_footer_payload(path, -1, footer_bytes_cache)
    buf_ptr = <const uint8_t*>envelope
    buf_size = <size_t>len(envelope)
    fs = ReadParquetMetadataFromBuffer(buf_ptr, buf_size)

    rg_count = fs.row_groups.size()
    if rg_idx < 0 or <size_t>rg_idx >= rg_count:
        raise IndexError(
            f"Row group {rg_idx} out of range [0, {rg_count})"
        )

    requested = set(column_names)
    cdef dict result = {}
    col_count = fs.row_groups[rg_idx].columns.size()
    for col_i in range(col_count):
        col = fs.row_groups[rg_idx].columns[col_i]
        col_name = col.name.decode("utf-8")
        # Use display name (top-level) for lookup — matches schema_columns naming.
        dot = col_name.find(".")
        display = col_name[:dot] if dot >= 0 else col_name
        if display not in requested:
            continue
        encodings_list = [
            EncodingToString(enc).decode("utf-8")
            for enc in col.encodings
        ]
        codec_str = CompressionCodecToString(col.codec).decode("utf-8") if col.codec >= 0 else None
        result[display] = {
            "name":                  display,
            "physical_type":         col.physical_type.decode("utf-8"),
            "logical_type":          col.logical_type.decode("utf-8") if col.logical_type.size() > 0 else "",
            "data_page_offset":      col.data_page_offset      if col.data_page_offset >= 0      else None,
            "dictionary_page_offset":col.dictionary_page_offset if col.dictionary_page_offset >= 0 else None,
            "total_compressed_size": col.total_compressed_size  if col.total_compressed_size >= 0  else None,
            "compression_codec":     codec_str,
            "encodings":             encodings_list,
            "max_definition_level":  col.max_definition_level  if col.max_definition_level >= 0  else None,
            "max_repetition_level":  col.max_repetition_level  if col.max_repetition_level >= 0  else None,
            "type_length":           col.type_length            if col.type_length > 0             else None,
            "num_values":            col.num_values             if col.num_values >= 0             else None,
        }

    return result


cpdef tuple fetch_column_stats(
    str path,
    int64_t file_size = -1,
    ParquetFooterBytesCache footer_bytes_cache = None,
):
    """Fast planning-phase stats: aggregate column min/max/null_count in C++.

    Returns (num_rows, footer_bytes, col_stats_dict) where:
      col_stats_dict = {name: (min_val, max_val, null_count_or_None)}

    Pass file_size=-1 to auto-detect via stat()/HEAD. Binary min/max
    aggregation runs in C++; decode_value is called once per column.
    """
    cdef bytes envelope
    cdef int64_t footer_bytes
    cdef const uint8_t* buf_ptr
    cdef size_t buf_size
    cdef FileStats fs
    cdef vector[AggColumnStat] agg_stats
    cdef str col_name, log_type
    cdef bint prefer_text

    envelope, footer_bytes = _read_footer_payload(path, file_size, footer_bytes_cache)

    buf_ptr = <const uint8_t*>envelope
    buf_size = <size_t>len(envelope)
    fs = ReadParquetMetadataFromBuffer(buf_ptr, buf_size)
    agg_stats = AggregateColumnStats(fs)

    cdef dict col_stats = {}
    for agg in agg_stats:
        col_name = agg.name.decode("utf-8")
        log_type = agg.logical_type.decode("utf-8") if agg.logical_type.size() > 0 else ""
        prefer_text = log_type == "json" or log_type.startswith("array<")
        min_val = _decode_value_c(agg.physical_type, agg.logical_type, agg.min_bytes, prefer_text) if agg.has_min else None
        max_val = _decode_value_c(agg.physical_type, agg.logical_type, agg.max_bytes, prefer_text) if agg.has_max else None
        null_count = agg.null_count if agg.null_count_complete else None
        col_stats[col_name] = (min_val, max_val, null_count)

    return fs.num_rows, footer_bytes, col_stats


cdef bint _rg_passes_predicates_native(RowGroupStats& rg, list predicates):
    """Evaluate AND-combined predicates against RowGroupStats min/max without materialising a Python dict."""
    cdef size_t i
    cdef string col_str
    cdef object min_val, max_val, value, col_name, op

    for pred in predicates:
        col_name, op, value = pred
        col_str = col_name.encode('utf-8')   # predicates always carry str names from the binder
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
                elif op == "InList":
                    # Prune only when no candidate value falls in [min, max].
                    # Mirrors predicates._can_prune_rowgroup; an empty list
                    # matches nothing so any() is False -> prune.
                    if not any(min_val <= v <= max_val for v in value):
                        return False
                elif op == "NotInList":
                    # Prune only when the whole group is a single excluded value.
                    if min_val == max_val and min_val in value:
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

    # Created after footer collection so the IPC pool can be sized from the
    # data (see "Footer-derived pool sizing" below) rather than a fixed constant.
    cdef CppIOPipeline pipeline = None

    cdef uint64_t t_phase1_ns = 0
    cdef uint64_t t_phase2_ns = 0
    cdef uint64_t t_footer_ns = 0
    cdef uint64_t t_submit_ns = 0
    cdef uint64_t t_consume_ns = 0
    cdef uint64_t t_get_result_ns = 0
    cdef uint64_t t_deserialize_ns = 0
    cdef uint64_t _t0, _t1, _ts
    cdef unordered_map[string, FileStats] local_footers_native
    cdef string path_bytes_cpp
    cdef const uint8_t* footer_buf_ptr
    cdef size_t footer_buf_size
    cdef RowGroupStats* rg_ptr
    cdef RowGroupStats* rgp
    cdef size_t rg_i
    cdef size_t ci
    cdef int64_t max_rg_bytes = 0
    cdef int64_t rg_bytes
    cdef int64_t est_rg
    cdef int64_t dyn_pool_size
    cdef int in_flight_limit
    cdef int next_to_submit
    cdef int n_items

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
                    envelope, _ = _read_footer_payload(orig_to_cpp.get(path, path), -1, footer_bytes_cache)
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

        # ── Footer-derived pool sizing (#2) ──────────────────────────────────
        # Size the IPC pool from the largest projected row group rather than a
        # fixed constant. total_uncompressed_size is the decoded-but-encoded
        # column-chunk size; a 2x factor pads for dict-code widening (RLE → u32
        # codes) and IPC framing. The historical 256MB stays as a floor so small
        # reads behave exactly as before. Combined with the in-flight bound
        # below, peak pool occupancy is in_flight_limit + 1 row groups (the
        # in-flight window plus the one dequeued-but-not-yet-deserialized
        # result whose pool bytes are still held while the window is refilled),
        # so memory scales with per-row-group size × concurrency — never with
        # total file or dataset size.
        # proj_set for prefetched-footer path (str); proj_set_bytes for C++ native path (bytes).
        # Column names from the operator are str (schema); C++ footer returns bytes.
        proj_set = set(column_names)
        proj_set_bytes = {name.encode('utf-8') for name in column_names}
        max_rg_bytes = 0
        for path, rg_idx in work_items:
            rg_bytes = 0
            if prefetched_footers and path in prefetched_footers:
                rg_meta = prefetched_footers[path]["row_groups"][rg_idx]
                for col_meta in rg_meta["columns"]:
                    if col_meta["name"] in proj_set:
                        cm_sz = col_meta.get("total_uncompressed_size", 0)
                        if cm_sz and cm_sz > 0:
                            rg_bytes += cm_sz
            else:
                path_bytes_cpp = path.encode('utf-8')
                rgp = &local_footers_native[path_bytes_cpp].row_groups[rg_idx]
                for ci in range(rgp.columns.size()):
                    if rgp.columns[ci].total_uncompressed_size > 0 and \
                            bytes(rgp.columns[ci].name) in proj_set_bytes:
                        rg_bytes += rgp.columns[ci].total_uncompressed_size
            if rg_bytes > max_rg_bytes:
                max_rg_bytes = rg_bytes

        in_flight_limit = decode_workers + 2
        est_rg = max_rg_bytes * 2
        dyn_pool_size = est_rg * (in_flight_limit + 1)
        if dyn_pool_size < 256*1024*1024:
            dyn_pool_size = 256*1024*1024

        pipeline = CppIOPipeline(
            decode_workers=decode_workers,
            queue_capacity=1024,
            pool_size=dyn_pool_size,
        )

        # ── Bounded submission + consume (#1) ────────────────────────────────
        # At most in_flight_limit row groups are submitted-but-not-yet-received
        # at any instant. Each loop iteration does exactly one of two things:
        # receive a result from the C++ queue, or deserialize the previously
        # received (pending) result. The window top-up runs at the head of
        # every iteration, so after a result is received — and before this
        # thread spends time deserializing it and the downstream operators
        # consume the yielded morsel — replacement work is already submitted
        # and the C++ workers stay fed. Peak pool occupancy is bounded by
        # in_flight_limit + 1 row groups (window + the pending result), which
        # the pool sizing above accounts for.
        _t0 = time.monotonic_ns()
        n_items = len(work_items)
        next_to_submit = 0
        results_received = 0
        pending_result = None
        while results_received < n_items or pending_result is not None:
            # Top up the in-flight window.
            while next_to_submit < n_items and (next_to_submit - results_received) < in_flight_limit:
                _ts = time.monotonic_ns()
                path, rg_idx = work_items[next_to_submit]
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
                next_to_submit += 1

            if pending_result is None:
                # Receive phase: block for the next result, count it against
                # the window, then loop back so the top-up above refills the
                # window before any Python-side consumption happens.
                _ts = time.monotonic_ns()
                result = pipeline.wait_result()
                t_get_result_ns += time.monotonic_ns() - _ts
                if result is None:
                    # Pipeline drained before all work completed — should not happen.
                    raise RuntimeError(
                        f"Parquet pipeline drained with {n_items - results_received} "
                        f"result(s) missing"
                    )

                if not result['success']:
                    raise RuntimeError(f"Parquet pipeline error: {result.get('error', 'unknown')}")

                results_received += 1
                pending_result = result
                continue

            # Consume phase: deserialize the pending result and yield it.
            result = pending_result
            pending_result = None

            _ts = time.monotonic_ns()
            row_group = deserialize_row_group(result['ref_ids'], pipeline.pool)
            # WP-6b: direct columns are already Vectors (wrapped in wait_result);
            # merge them in. Keyed by column-name bytes, same as the pool columns.
            row_group.update(result['direct'])
            t_deserialize_ns += time.monotonic_ns() - _ts

            # Defensive: detect columns decoded to 0 rows in a non-empty row group.
            # List columns (max_repetition_level > 0) have more values than rows and
            # are intentionally excluded — only a zero-length column in a non-empty
            # row group indicates a C++ decoder bug (silent data loss).
            col_lengths = {
                k: len(v)
                for k, v in row_group.items()
                if not (isinstance(k, str) and k.startswith('__'))
                   and getattr(v, '__len__', None) is not None
            }
            if col_lengths:
                lengths = list(col_lengths.values())
                max_len = max(lengths)
                if max_len > 0 and any(l == 0 for l in lengths):
                    raise RuntimeError(
                        f"C++ decoder produced zero-length column(s) in non-empty row group for "
                        f"path={result['path']!r} rg={result['rg_idx']}: {col_lengths}"
                    )

            # Build typed metadata object; remove all __*__ keys from dict.
            # Yield (ScanRowGroup, {col: Vector}) to separate metadata from data.
            path_str = cpp_to_orig.get(result['path'], result['path'])
            telemetry_dict = {
                '__bytes_fetched__': result.get('bytes_fetched', 0),
                '__time_read_ranges_ns__': result.get('read_ns', 0),
                '__time_decode_columns_ns__': result.get('decode_ns', 0),
            }
            scan_rg = _make_scan_row_group(path_str, result['rg_idx'], 'cpp-pipeline', telemetry_dict)
            # row_group is now pure {col: Vector}; clean for the operator.
            yield (scan_rg, row_group)
        t_consume_ns = time.monotonic_ns() - _t0

    finally:
        import os, sys
        # pipeline is None if we returned before footer-sized creation
        # (no work items) or failed during sizing.
        if pipeline is not None:
            if os.environ.get("OPTERYX_IO_DIAG"):
                diag = pipeline.diagnostics()
                sys.stderr.write(
                    "\n[io_diag] paths=%d rgs=%d  phase1=%.1fms phase2=%.1fms consume=%.1fms\n"
                    "         footer_total=%.1fms  submit_total=%.3fms\n"
                    "         wait_result_total=%.1fms deserialize_total=%.1fms\n"
                    "         queue: enqueues=%d high_watermark=%d spin_iters=%d\n"
                    "         http: requests=%d fetch_ops=%d  worker_blocked=%.1fms\n"
                    "         handoff: serialized=%d committed=%d bytes\n"
                    % (
                        len(set(p for p, _ in work_items)) if work_items else 0,
                        len(work_items),
                        t_phase1_ns / 1e6, t_phase2_ns / 1e6, t_consume_ns / 1e6,
                        t_footer_ns / 1e6, t_submit_ns / 1e6,
                        t_get_result_ns / 1e6, t_deserialize_ns / 1e6,
                        diag["enqueue_count"], diag["queue_high_watermark"],
                        diag["spin_iterations"],
                        diag["http_request_count"], diag["http_fetch_ops"],
                        diag["worker_blocked_ns"] / 1e6,
                        diag["ipc_bytes_serialized"], diag["ipc_bytes_committed"],
                    )
                )
            diag_json_path = os.environ.get("OPTERYX_IO_DIAG_JSON")
            if diag_json_path:
                # Machine-readable diag for the dev benchmark harness: one JSON
                # object per scan, appended as a line (JSONL). Dev tooling only.
                import json
                record = pipeline.diagnostics()
                record.update({
                    "paths": len(set(p for p, _ in work_items)) if work_items else 0,
                    "row_groups": len(work_items),
                    "phase1_ns": t_phase1_ns,
                    "footer_ns": t_footer_ns,
                    "submit_ns": t_submit_ns,
                    "consume_ns": t_consume_ns,
                    "wait_result_ns": t_get_result_ns,
                    "deserialize_ns": t_deserialize_ns,
                })
                with open(diag_json_path, "a") as diag_file:
                    diag_file.write(json.dumps(record) + "\n")
            pipeline.close()


def iter_pass2_row_groups_ipc(
    filesystem,
    work_items,
    column_names,
    decode_workers=None,
    file_sizes=None,
    connector=None,
    query_id=None,
    prefetched_footers=None,
    footer_bytes_cache=None,
):
    """
    C++ Parquet IO pipeline for pass-2 late materialization.

    Decodes pass-2 columns in parallel, applying a per-row-group mask so only
    surviving rows (from pass-1 predicate evaluation) are decoded and serialized.

    work_items: list of (path, rg_idx, mask_bytes) triples.
    column_names: pass-2 (projection-only) column names.

    Yields row_group dicts with __path__, __row_group__, __parquet_scan_strategy__,
    and __bytes_fetched__ in completion order.
    """
    if not work_items:
        return

    # Pass-2 decode is CPU-bound (decompress + materialize), so size the worker
    # pool to the host: use all but two cores, with a floor of two. Mirrors the
    # thread_pool_manager convention (os.cpu_count() or 4) with a floor of 2.
    if decode_workers is None:
        import os
        decode_workers = max(2, (os.cpu_count() or 4) - 2)

    # Planning-time URL signer: converts gs:// paths to signed HTTPS URLs.
    sign_url = getattr(filesystem, "rewrite_to_signed_url", None)
    cdef dict orig_to_cpp = {}
    cdef dict cpp_to_orig = {}
    if sign_url:
        for path, _rg, _mask in work_items:
            if path not in orig_to_cpp:
                cpp_path = sign_url(path)
                orig_to_cpp[path] = cpp_path
                cpp_to_orig[cpp_path] = path

    cdef CppIOPipeline pipeline = CppIOPipeline(
        decode_workers=decode_workers,
        queue_capacity=1024,
        pool_size=256*1024*1024,
    )

    cdef unordered_map[string, FileStats] local_footers_native
    cdef string path_bytes_cpp
    cdef const uint8_t* footer_buf_ptr
    cdef size_t footer_buf_size
    cdef RowGroupStats* rg_ptr

    try:
        # Load footers for all paths needed (footer cache hits expected — pass 1 already fetched them).
        # TODO: factor out footer-loading logic shared with iter_row_groups_ipc.
        for path, rg_idx, mask_bytes in work_items:
            path_bytes_cpp = path.encode('utf-8')
            if local_footers_native.count(path_bytes_cpp) == 0:
                envelope, _ = _read_footer_payload(orig_to_cpp.get(path, path), -1, footer_bytes_cache)
                footer_buf_ptr = <const uint8_t*>envelope
                footer_buf_size = len(envelope)
                local_footers_native[path_bytes_cpp] = ReadParquetMetadataFromBuffer(
                    footer_buf_ptr, footer_buf_size
                )

        # Submit all pass-2 work items with their masks.
        for path, rg_idx, mask_bytes in work_items:
            path_bytes_cpp = path.encode('utf-8')
            rg_ptr = &local_footers_native[path_bytes_cpp].row_groups[rg_idx]
            pipeline.submit_work_native_masked(
                orig_to_cpp.get(path, path), rg_idx, column_names, rg_ptr, bytes(mask_bytes)
            )

        # Consume results in completion order.
        results_received = 0
        while results_received < len(work_items):
            result = pipeline.wait_result()
            if result is None:
                raise RuntimeError(
                    f"Parquet pass-2 pipeline drained with {len(work_items) - results_received} "
                    f"result(s) missing"
                )

            if not result['success']:
                raise RuntimeError(f"Parquet pass-2 pipeline error: {result.get('error', 'unknown')}")

            row_group = deserialize_row_group(result['ref_ids'], pipeline.pool)
            row_group.update(result['direct'])  # WP-6b direct columns

            # Build typed metadata object; remove all __*__ keys from dict.
            # Yield (ScanRowGroup, {col: Vector}) to separate metadata from data.
            path_str = cpp_to_orig.get(result['path'], result['path'])
            telemetry_dict = {
                '__bytes_fetched__': result['bytes_fetched'],
                '__time_read_ranges_ns__': result.get('read_ns', 0),
                '__time_decode_columns_ns__': result.get('decode_ns', 0),
            }
            scan_rg = _make_scan_row_group(path_str, result['rg_idx'], 'cpp-pipeline-pass2', telemetry_dict)
            # row_group is now pure {col: Vector}; clean for the operator.
            results_received += 1
            yield (scan_rg, row_group)

    finally:
        pipeline.close()
