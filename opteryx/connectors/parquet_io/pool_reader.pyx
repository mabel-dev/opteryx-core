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
import struct

# Native mutex for thread-safe concurrent pull (M4 / no-GIL target). The lock is
# the real cross-thread synchronisation once the GIL is gone; today it is held
# only over GIL-release-free bookkeeping (cursor advance + accounting), never
# across the nogil decode wait or work submission, so no GIL/mutex inversion.
cdef extern from "<mutex>" namespace "std" nogil:
    cppclass cpp_mutex "std::mutex":
        cpp_mutex()
        void lock()
        void unlock()

from opteryx.compiled.structures.memory_pool cimport MemoryPool
from opteryx.compiled.structures.column_deserializer cimport deserialize_row_group
from opteryx.compiled.structures.footer_cache cimport ParquetFooterBytesCache, ParquetParsedFooterCache
from opteryx.compiled.structures.column_stats cimport FileColumnStats, file_column_stats_from_agg

# WP-6b direct path: wrap worker-built Draken buffers into Vectors (consumer-side,
# GIL held). DirectKind 1=int64 2=float32 3=float64 4=bool 5=decimal128.
from draken.core.buffers cimport (
    DrakenType, DrakenVector,
    DRAKEN_INT64, DRAKEN_FLOAT32, DRAKEN_FLOAT64, DRAKEN_BOOL, DRAKEN_DECIMAL128, DRAKEN_VARCHAR
)
from draken.vectors.vector cimport Vector, from_decoded as _vector_from_decoded
from cpython.ref cimport PyObject

# All-NULL column factory for schema-evolution NULL-fill (a projected column
# absent from a file). NULL-typed: no storage, absorbed by type promotion.
from draken.draken_native import vector_null_from_length

# Stage 4b: wrap worker-built direct string slots into a VARCHAR Vector. Mirrors
# column_deserializer._wrap_raw_pyobj — draken_vector_own_string copies the
# slots+arena into a self-owned block and frees the inputs, so the worker buffers
# must have been taken (nulled) on the MorselRef first to avoid a double free.
cdef extern from "core/string_slot.h" nogil:
    ctypedef struct DrakenStringSlot:
        pass

cdef extern from *:
    """static inline void _pr_decref(PyObject* op) { Py_DECREF(op); }"""
    void _pr_decref(PyObject* op)

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)
    int draken_vector_mark_dict_sorted(PyObject* obj)
    PyObject* draken_vector_own_string(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint8_t* validity, uint32_t length, DrakenType vec_type)
    PyObject* draken_vector_own_string_dict(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint32_t* codes, uint32_t data_length,
        uint8_t* validity, uint32_t length, DrakenType vec_type)
    PyObject* draken_vector_own_dict_i64(
        void* data, uint32_t data_length,
        uint32_t* codes, uint32_t length, uint8_t* validity)
    PyObject* draken_vector_own_dict_f64(
        void* data, uint32_t data_length,
        uint32_t* codes, uint32_t length, uint8_t* validity)
    PyObject* draken_vector_own_dict_f32(
        void* data, uint32_t data_length,
        uint32_t* codes, uint32_t length, uint8_t* validity)

from rugo.parquet import decode_value as _decode_value_c, _make_scan_row_group
from rugo.parquet_reader cimport ReadParquetMetadataFromBuffer, FileStats, RowGroupStats, ColumnStats, AggColumnStat, AggregateColumnStats
from rugo.parquet_reader cimport TestBloomFilter
from rugo.parquet_reader cimport EncodingToString, CompressionCodecToString
from rugo.parquet_reader cimport ParquetFooterResult, FetchParquetFooter, FetchParquetFootersMany

_PARQUET_MAGIC = b"PAR1"
_PARQUET_FOOTER_SUFFIX = 8
_FOOTER_PREFETCH = 65536

# Process-global cache for parsed Parquet FileStats (~64 MB, 512 entries).
# Avoids repeated Thrift deserialization when the same files are scanned
# across queries. Keyed by canonical file path (never signed URLs).
# Typed so Cython dispatches cdef methods (try_get / put_fs) statically.
cdef ParquetParsedFooterCache _PARSED_FOOTER_CACHE = ParquetParsedFooterCache()


cdef inline Vector _wrap_string_direct(MorselRef* result, size_t i):
    """Stage 4b: wrap direct DK_VARCHAR column i into a dense VARCHAR Vector.

    Takes the worker's slots (data) + validity via morsel_take_direct and the arena
    via morsel_take_string — nulling ALL of them on the MorselRef — then hands them
    to draken_vector_own_string, which copies into a self-owned block and frees the
    slots+arena (and owns the validity). Because every buffer was taken, the
    MorselRef destructor frees nothing for this column: no double free.
    """
    cdef uint32_t dlen = result.columns[i].length
    cdef size_t arena_len = result.columns[i].arena_len
    cdef void* slots_ptr
    cdef uint8_t* dval
    cdef void* arena_ptr
    cdef void* codes_ptr
    cdef PyObject* raw
    cdef Vector vec
    slots_ptr = morsel_take_direct(result[0], i, &dval)
    morsel_take_string(result[0], i, &arena_ptr, &codes_ptr)
    raw = draken_vector_own_string(
        <DrakenStringSlot*>slots_ptr, <uint8_t*>arena_ptr, arena_len,
        dval, dlen, DRAKEN_VARCHAR)
    if raw == NULL:
        raise MemoryError("draken_vector_own_string failed")
    vec = Vector.__new__(Vector)
    vec._nb = <object>raw          # Cython incref → refcount 2
    _pr_decref(raw)                # balance the NEW ref → refcount 1
    vec._dv = draken_vector_unwrap(raw)
    return vec


cdef inline Vector _wrap_string_dict_direct(MorselRef* result, size_t i):
    """Stage 4b: wrap direct DK_VARCHAR_DICT column i into a dict-shape VARCHAR
    Vector. Like the plain wrap but also hands the codes selection to
    draken_vector_own_string_dict, which owns all four buffers; morsel_take_direct
    + morsel_take_string null slots/validity/arena/codes so the destructor frees
    none of them (no double free)."""
    cdef uint32_t dlen = result.columns[i].length
    cdef size_t arena_len = result.columns[i].arena_len
    cdef uint32_t data_length = result.columns[i].data_length
    cdef void* slots_ptr
    cdef uint8_t* dval
    cdef void* arena_ptr
    cdef void* codes_ptr
    cdef PyObject* raw
    cdef Vector vec
    slots_ptr = morsel_take_direct(result[0], i, &dval)
    morsel_take_string(result[0], i, &arena_ptr, &codes_ptr)
    raw = draken_vector_own_string_dict(
        <DrakenStringSlot*>slots_ptr, <uint8_t*>arena_ptr, arena_len,
        <uint32_t*>codes_ptr, data_length,
        dval, dlen, DRAKEN_VARCHAR)
    if raw == NULL:
        raise MemoryError("draken_vector_own_string_dict failed")
    vec = Vector.__new__(Vector)
    vec._nb = <object>raw
    _pr_decref(raw)
    vec._dv = draken_vector_unwrap(raw)
    if result.columns[i].dict_sorted:
        draken_vector_mark_dict_sorted(raw)
    return vec


cdef inline Vector _wrap_num_dict_direct(MorselRef* result, size_t i, int dk):
    """Wrap a numeric §11 compressed (Dict-shaped) column into a Vector. `data`
    (dictionary) + validity come via morsel_take_direct; the codes selection via
    morsel_take_string (arena is NULL for numeric dicts). The draken own_dict_*
    entry owns all three buffers; both takes null the MorselRef slots so the
    destructor frees nothing. dk: 8=int64, 9=float64, 10=float32."""
    cdef uint32_t dlen = result.columns[i].length
    cdef uint32_t data_length = result.columns[i].data_length
    cdef void* data_ptr
    cdef uint8_t* dval
    cdef void* arena_ptr
    cdef void* codes_ptr
    cdef PyObject* raw
    cdef Vector vec
    data_ptr = morsel_take_direct(result[0], i, &dval)
    morsel_take_string(result[0], i, &arena_ptr, &codes_ptr)
    if dk == 9:
        raw = draken_vector_own_dict_f64(data_ptr, data_length, <uint32_t*>codes_ptr, dlen, dval)
    elif dk == 10:
        raw = draken_vector_own_dict_f32(data_ptr, data_length, <uint32_t*>codes_ptr, dlen, dval)
    else:
        raw = draken_vector_own_dict_i64(data_ptr, data_length, <uint32_t*>codes_ptr, dlen, dval)
    if raw == NULL:
        raise MemoryError("draken_vector_own_dict_* failed")
    vec = Vector.__new__(Vector)
    vec._nb = <object>raw
    _pr_decref(raw)
    vec._dv = draken_vector_unwrap(raw)
    if result.columns[i].dict_sorted:
        draken_vector_mark_dict_sorted(raw)
    return vec


cdef inline Vector _wrap_direct(MorselRef* result, size_t i):
    """Wrap direct column i into a Draken Vector via ownership transfer.
    morsel_take_direct hands the draken_alloc'd buffer + validity to the Vector and
    nulls the MorselRef slots so the destructor won't double-free. Fixed-width kinds
    1..5; DK_VARCHAR (6) routes to the string wrap.
    """
    cdef int dk = result.columns[i].direct_kind
    cdef uint32_t dlen = result.columns[i].length
    cdef DrakenType dtype
    cdef void* dptr
    cdef uint8_t* dval
    cdef Vector vec
    if dk == 6:
        return _wrap_string_direct(result, i)
    if dk == 7:
        return _wrap_string_dict_direct(result, i)
    if dk == 8 or dk == 9 or dk == 10:
        return _wrap_num_dict_direct(result, i, dk)
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
    dptr = morsel_take_direct(result[0], i, &dval)
    vec = _vector_from_decoded(dptr, dval, dlen, dtype)
    if dk == 5 and dlen > 0:
        vec._nb.set_decimal_descriptor(
            result.columns[i].dec_precision, result.columns[i].dec_scale)
    return vec


cdef inline tuple _split_columns(MorselRef* result):
    """Split a result's columns into (pool ref_ids, direct Vectors).

    Pool-path columns (direct_kind == 0) return their MemoryPool ref_id for the
    deserializer. Direct-path columns are wrapped here via _wrap_direct.
    """
    cdef dict ref_ids = {}
    cdef dict direct = {}
    cdef bytes col_name
    cdef size_t i
    for i in range(result.columns.size()):
        col_name = result.column_names[i]   # bytes — names are bytes throughout
        if result.columns[i].direct_kind == 0:
            ref_ids[col_name] = result.columns[i].ref_id
            continue
        direct[col_name] = _wrap_direct(result, i)
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

    def add_int_needles(self, str column, list needles):
        """Phase 2: register pushed int equality/IN needles for a column so the
        worker can skip data-page decode when the dictionary is disjoint."""
        cdef vector[int64_t] v
        cdef int64_t x
        for x in needles:
            v.push_back(x)
        self.pipeline.add_int_needles(column.encode('utf-8'), v)

    def add_str_pred(self, str column, int kind, list patterns):
        """Phase 2: register a pushed string decode-skip predicate. kind: 1=membership
        (=/IN), 2=starts-with, 3=ends-with, 4=contains. `patterns` are bytes."""
        cdef vector[string] v
        cdef bytes b
        for p in patterns:
            b = p if isinstance(p, bytes) else str(p).encode('utf-8')
            v.push_back(<string>b)
        self.pipeline.add_str_pred(column.encode('utf-8'), kind, v)

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

        # A projected column absent from this file (schema evolution) has no
        # ColumnStats — push the name only when a stat is found, keeping names and
        # stats strictly parallel. The consumer (next_vectors) realigns the decoded
        # columns back to the full projection, filling missing ones with NULLs.
        for col_name in column_names:
            cpp_col_name = col_name.encode('utf-8')
            for i in range(rg.columns.size()):
                if rg.columns[i].name == cpp_col_name:
                    col_names_vec.push_back(cpp_col_name)
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

        # Schema evolution: keep names and stats strictly parallel (push the name
        # only when its stat is found) — the consumer realigns + NULL-fills.
        for col_name in column_names:
            cpp_col_name = col_name.encode('utf-8')
            for i in range(rg.columns.size()):
                if rg.columns[i].name == cpp_col_name:
                    col_names_vec.push_back(cpp_col_name)
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

    def cancel(self):
        """WP-8: signal early cancellation so queued-but-unstarted decode tasks
        bail before doing IO/decode. Non-blocking; the wait for in-flight tasks
        happens in close(). Idempotent and safe to call before close()."""
        with nogil:
            self.pipeline.cancel()

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
            "http_retries": self.pipeline.http_retries(),
            "http_latency_histogram_ms": latency_histogram,
            "worker_blocked_ns": self.pipeline.worker_blocked_ns(),
            "ipc_bytes_serialized": self.pipeline.ipc_bytes_serialized(),
            "ipc_bytes_committed": self.committed_bytes,
            "cancelled_skips": self.pipeline.cancelled_skips(),
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


cdef list _fetch_footers_many(list urls, list sizes):
    """Concurrently fetch footer envelopes for many remote files (one get_many
    batch in C++). `urls` are the already-rewritten/signed fetch URLs; `sizes`
    are the known file sizes (no per-file HEAD). Returns envelopes (bytes) in
    input order.
    """
    cdef vector[string] cpp_paths
    cdef vector[int64_t] cpp_sizes
    cdef Py_ssize_t i, n = len(urls)
    cdef str url
    cpp_paths.reserve(n)
    cpp_sizes.reserve(n)
    for i in range(n):
        url = urls[i]
        cpp_paths.push_back(url.encode("utf-8"))
        cpp_sizes.push_back(<int64_t>sizes[i])

    cdef vector[ParquetFooterResult] results
    with nogil:
        results = FetchParquetFootersMany(cpp_paths, cpp_sizes)

    cdef list out = []
    cdef size_t k
    cdef const uint8_t* env_ptr
    cdef size_t env_sz
    for k in range(results.size()):
        env_sz = results[k].envelope.size()
        env_ptr = results[k].envelope.data()
        out.append(env_ptr[:env_sz])
    return out


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

    if not _PARSED_FOOTER_CACHE.try_get(path, &fs):
        envelope, _ = _read_footer_payload(path, -1, footer_bytes_cache)
        buf_ptr = <const uint8_t*>envelope
        buf_size = <size_t>len(envelope)
        fs = ReadParquetMetadataFromBuffer(buf_ptr, buf_size)
        _PARSED_FOOTER_CACHE.put_fs(path, fs)

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

    Returns (num_rows, footer_bytes, FileColumnStats) where FileColumnStats
    holds decoded values lazily — nothing is decoded until get_min/get_max
    is called.

    Pass file_size=-1 to auto-detect via stat()/HEAD.
    """
    cdef bytes envelope
    cdef int64_t footer_bytes
    cdef const uint8_t* buf_ptr
    cdef size_t buf_size
    cdef FileStats fs
    cdef vector[AggColumnStat] agg_stats

    if not _PARSED_FOOTER_CACHE.try_get(path, &fs):
        envelope, footer_bytes = _read_footer_payload(path, file_size, footer_bytes_cache)
        buf_ptr = <const uint8_t*>envelope
        buf_size = <size_t>len(envelope)
        fs = ReadParquetMetadataFromBuffer(buf_ptr, buf_size)
        _PARSED_FOOTER_CACHE.put_fs(path, fs)
    else:
        footer_bytes = 0
    agg_stats = AggregateColumnStats(fs)

    return fs.num_rows, footer_bytes, file_column_stats_from_agg(agg_stats)


cdef inline bint _is_local_path(str path):
    """True for filesystem paths the C++ bloom probe (ifstream) can open."""
    return not (path.startswith("gs://") or path.startswith("s3://")
                or path.startswith("http://") or path.startswith("https://")
                or path.startswith("file://"))


cdef bint _bloom_value_bytes(const string& physical_type, object value, string* out):
    """Encode a predicate literal into the exact PLAIN bytes the writer hashed
    into the bloom filter for this physical type. Returns False (=> skip the
    bloom probe) for any type/value we can't encode byte-identically — never
    guess, a wrong encoding would prune live rows.

    Matches _parquet_writer.hpp bloom_hashes: int32=4 LE, int64=8 LE,
    byte_array=raw value bytes (no length prefix)."""
    cdef bytes b
    # bool is an int subclass but boolean columns are never bloom-filtered.
    if isinstance(value, bool):
        return False
    if physical_type == b"byte_array":
        if isinstance(value, str):
            b = (<str>value).encode("utf-8")
        elif isinstance(value, (bytes, bytearray)):
            b = bytes(value)
        else:
            return False
    elif physical_type == b"int64":
        if not isinstance(value, int):
            return False
        try:
            b = struct.pack("<q", value)
        except (struct.error, OverflowError):
            return False
    elif physical_type == b"int32":
        if not isinstance(value, int):
            return False
        try:
            b = struct.pack("<i", value)
        except (struct.error, OverflowError):
            return False
    else:
        # int96/float/double/fixed_len_byte_array(decimal): not wired yet.
        return False
    out[0] = <string>b
    return True


cdef bint _bloom_excludes(str cpp_path, ColumnStats* col, object op, object value):
    """True only when the column's bloom filter PROVES the predicate cannot
    match this row group (safe to prune). Eq -> the value is absent; InList ->
    every candidate is absent. Any probe error or unencodable value fails OPEN
    (returns False = keep the row group)."""
    cdef string path_b = cpp_path.encode("utf-8")
    cdef string vbytes
    if op == "Eq":
        if not _bloom_value_bytes(col.physical_type, value, &vbytes):
            return False
        try:
            return not TestBloomFilter(path_b, col.bloom_offset, col.bloom_length, vbytes)
        except Exception:
            return False
    elif op == "InList":
        # Prune only if NONE of the candidates may be present.
        for v in value:
            if not _bloom_value_bytes(col.physical_type, v, &vbytes):
                return False  # can't prove this one absent -> can't prune
            try:
                if TestBloomFilter(path_b, col.bloom_offset, col.bloom_length, vbytes):
                    return False  # a candidate may be present
            except Exception:
                return False
        return True
    return False


cdef bint _rg_passes_predicates_native(RowGroupStats& rg, list predicates, str cpp_path):
    """Evaluate AND-combined predicates against RowGroupStats min/max (and bloom
    filters, for Eq/InList on local files) without materialising a Python dict.
    `cpp_path` is the local file path for bloom probing, or None to skip it."""
    cdef size_t i
    cdef string col_str
    cdef object min_val, max_val, value, col_name, op

    for pred in predicates:
        col_name, op, value = pred
        col_str = col_name.encode('utf-8')   # predicates always carry str names from the binder
        for i in range(rg.columns.size()):
            if rg.columns[i].name != col_str:
                continue
            # Bloom membership pruning runs first: it can exclude on equality
            # even when min/max stats are absent, and is independent of them.
            if (cpp_path is not None and rg.columns[i].bloom_offset >= 0
                    and (op == "Eq" or op == "InList")):
                if _bloom_excludes(cpp_path, &rg.columns[i], op, value):
                    return False
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


cdef class IpcRowGroupSource:
    """Native single-pass row-group driver: owns the C++ pipeline lifecycle and
    feeds decoded row groups one at a time WITHOUT a Python generator frame.

    ``open_ipc_source`` does the once-per-scan planning (footer fetch, work-item
    pruning, pool sizing); ``next_vectors`` submits the bounded in-flight window
    and blocks (GIL released) for the next decoded row group, returning a tuple
    ``(vectors, bytes_fetched, read_ns, decode_ns, path, rg_idx)`` in submission
    order. All ``MorselRef`` / C++ handling stays inside this module.

    Thread-safe concurrent pull (M4 / no-GIL target): a native ``std::mutex``
    guards the cursor (``next_to_submit`` / ``results_received``). The claim
    (advance the submit window + take one result slot) runs under the lock; the
    actual work submission and the blocking decode wait run OUTSIDE the lock — so
    the lock is never held across a GIL-releasing region (no GIL/mutex inversion
    while a GIL still exists) and N threads decode/assemble in parallel. Per-row
    telemetry is RETURNED, never stored on the instance, so concurrent callers
    can't clobber each other's values. The bounded window keeps peak pool
    occupancy ~ ``in_flight_limit`` row groups plus the few in-flight per thread.
    """

    cdef CppIOPipeline pipeline
    cdef list work_items                 # [(path, rg_idx)]
    cdef list column_names               # str, submission order
    cdef list column_names_bytes         # bytes, for name-keyed callers
    cdef list column_null_fillers        # per-column callable n->Vector (schema-evolution NULL-fill); None = untyped
    cdef unordered_map[string, FileStats]* footer_map
    cdef object prefetched_footers
    cdef dict orig_to_cpp
    cdef dict cpp_to_orig
    cdef int in_flight_limit
    cdef int n_items
    cdef int next_to_submit
    cdef int results_received
    cdef bint _closed
    cdef list masks                      # pass-2: bit-packed survival mask per work item (else None)
    cdef cpp_mutex* _mtx                  # guards the cursor under concurrent pull

    def __cinit__(self):
        self.footer_map = NULL
        self._closed = False
        self.pipeline = None
        self.column_null_fillers = None
        self.next_to_submit = 0
        self.results_received = 0
        self.n_items = 0
        self.in_flight_limit = 0
        self.masks = None
        self._mtx = new cpp_mutex()

    def __dealloc__(self):
        if self.footer_map != NULL:
            del self.footer_map
            self.footer_map = NULL
        if self._mtx != NULL:
            del self._mtx
            self._mtx = NULL

    cdef void _submit_one(self, int idx):
        """Submit one work item to the C++ pipeline. Called OUTSIDE the cursor
        lock — the pipeline's work queue is itself thread-safe and the footer map
        is read-only after open(), so disjoint claimed ranges submit safely in
        parallel."""
        cdef object path = self.work_items[idx][0]
        cdef int rg_idx = self.work_items[idx][1]
        cdef str cpp_path = self.orig_to_cpp.get(path, path)
        cdef string path_bytes_cpp
        cdef RowGroupStats* rg_ptr
        cdef list column_stats_dicts
        cdef list present_names
        cdef object meta, rg_meta, col_meta, col_name
        if self.masks is not None:
            # Pass-2 late materialization: decode only surviving rows (the
            # bit-packed mask from pass-1) — always the native-footer path.
            path_bytes_cpp = path.encode('utf-8')
            rg_ptr = &self.footer_map[0][path_bytes_cpp].row_groups[rg_idx]
            self.pipeline.submit_work_native_masked(
                cpp_path, rg_idx, self.column_names, rg_ptr, self.masks[idx])
            return
        if self.prefetched_footers and path in self.prefetched_footers:
            meta = self.prefetched_footers[path]
            rg_meta = meta["row_groups"][rg_idx]
            # Keep names and stats parallel: a projected column absent from this
            # file contributes neither (schema evolution). next_vectors realigns.
            column_stats_dicts = []
            present_names = []
            for col_name in self.column_names:
                for col_meta in rg_meta["columns"]:
                    if col_meta["name"] == col_name:
                        column_stats_dicts.append(col_meta)
                        present_names.append(col_name)
                        break
            self.pipeline.submit_work(cpp_path, rg_idx, present_names, column_stats_dicts)
        else:
            path_bytes_cpp = path.encode('utf-8')
            rg_ptr = &self.footer_map[0][path_bytes_cpp].row_groups[rg_idx]
            self.pipeline.submit_work_native(cpp_path, rg_idx, self.column_names, rg_ptr)

    cpdef tuple next_vectors(self):
        """Block for the next decoded row group; return
        ``(vectors, bytes_fetched, read_ns, decode_ns, path, rg_idx)`` (vectors in
        submission order) or None on exhaustion. Direct (WP-6b) columns are wrapped
        zero-copy; pool-path columns are deserialized and slotted positionally."""
        cdef MorselRef result
        cdef bint got
        cdef list vectors
        cdef dict ref_ids
        cdef dict pool_dict
        cdef dict present
        cdef list aligned
        cdef Py_ssize_t i, ncols, max_len, this_len, nreq, k
        cdef int submit_start, submit_end, idx
        cdef int64_t fill_rows
        cdef bytes req_name
        cdef str cpp_path, path_str

        # ── Claim phase (under the cursor lock): advance the submit window to keep
        # it full, then take one result slot. No GIL-releasing call runs here. ──
        self._mtx.lock()
        submit_start = self.next_to_submit
        submit_end = submit_start
        while submit_end < self.n_items and \
                (submit_end - self.results_received) < self.in_flight_limit:
            submit_end += 1
        self.next_to_submit = submit_end
        if self.results_received >= self.n_items:
            self._mtx.unlock()
            return None
        self.results_received += 1
        self._mtx.unlock()

        # ── Submit the claimed range OUTSIDE the lock (queue is thread-safe). ──
        for idx in range(submit_start, submit_end):
            self._submit_one(idx)

        # ── Blocking wait OUTSIDE the lock; the C++ queue hands each concurrent
        # caller a distinct completed result. ──
        with nogil:
            got = self.pipeline.pipeline.wait_and_get_result(result)
        if not got:
            raise RuntimeError("Parquet pipeline drained with result(s) missing")
        if not result.success:
            raise RuntimeError(f"Parquet pipeline error: {result.error.decode('utf-8')}")

        cpp_path = result.path.decode('utf-8')
        path_str = self.cpp_to_orig.get(cpp_path, cpp_path)

        # Phase 2 empty row group: a pushed-conjunct equality column's dictionary
        # lacked every needle → zero surviving rows. Skip all assembly; the
        # consumer records telemetry and moves on (vectors=None sentinel).
        if result.empty_filtered:
            return (None, result.bytes_fetched, result.read_ns,
                    result.decode_ns, path_str, result.rg_idx, result.empty_rows)

        ncols = result.columns.size()
        vectors = [None] * ncols
        ref_ids = None
        for i in range(ncols):
            if result.columns[i].direct_kind == 0:
                if ref_ids is None:
                    ref_ids = {}
                ref_ids[result.column_names[i]] = result.columns[i].ref_id
                continue
            vectors[i] = _wrap_direct(&result, i)

        # Pool-path columns (strings/dict/list) are deserialized here and slotted
        # positionally; fixed-width + date/timestamp arrive direct via _wrap_direct.
        if ref_ids is not None:
            pool_dict = deserialize_row_group(ref_ids, self.pipeline.pool)
            for i in range(ncols):
                if vectors[i] is None:
                    vectors[i] = pool_dict[result.column_names[i]]

        # Defensive: a zero-length column in an otherwise non-empty row group is
        # a C++ decoder bug (silent data loss).
        max_len = 0
        for i in range(ncols):
            this_len = len(vectors[i])
            if this_len > max_len:
                max_len = this_len
        if max_len > 0:
            for i in range(ncols):
                if len(vectors[i]) == 0:
                    raise RuntimeError(
                        f"C++ decoder produced zero-length column in non-empty row "
                        f"group for path={path_str!r} rg={result.rg_idx}"
                    )

        # ── Schema-evolution realignment ────────────────────────────────────
        # The decoded columns cover only the projected columns PRESENT in this
        # file, in projection order. Realign them to the full projection,
        # inserting an all-NULL column for any column the file lacks, so the
        # caller's positional pairing (identity names ↔ vectors) stays correct.
        # NULL-typed columns are absorbed by type promotion downstream and are
        # skipped by the DATE/TIMESTAMP/DECIMAL coercion (type != INT64).
        nreq = len(self.column_names_bytes)
        if ncols != nreq:
            present = {}
            for i in range(ncols):
                present[result.column_names[i]] = vectors[i]
            fill_rows = max_len
            if fill_rows == 0:
                fill_rows = self._rg_num_rows(path_str, result.rg_idx)
            aligned = [None] * nreq
            for k in range(nreq):
                req_name = self.column_names_bytes[k]
                if req_name in present:
                    aligned[k] = present[req_name]
                elif self.column_null_fillers is not None:
                    # Typed all-NULL column of the column's physical type, so it
                    # concatenates cleanly with the same column from files that
                    # do carry it (small-morsel combine).
                    aligned[k] = Vector(self.column_null_fillers[k](<uint32_t>fill_rows))
                else:
                    # No type info supplied (non-schema-driven callers): a
                    # NULL-typed all-null column. Correct for the scan; absorbed
                    # by type promotion where the consumer is type-uniform.
                    aligned[k] = Vector(vector_null_from_length(<uint32_t>fill_rows))
            vectors = aligned

        return (vectors, result.bytes_fetched, result.read_ns,
                result.decode_ns, path_str, result.rg_idx)

    cdef int64_t _rg_num_rows(self, str path_str, int rg_idx):
        """Row-group logical row count, for NULL-filling a row group whose every
        projected column is absent from the file. Reads the already-parsed footer
        (native footer_map keyed by original path, or the prefetched footer dict)."""
        cdef string key = path_str.encode('utf-8')
        if self.footer_map != NULL and self.footer_map[0].count(key) > 0:
            return self.footer_map[0][key].row_groups[rg_idx].num_rows
        if self.prefetched_footers and path_str in self.prefetched_footers:
            return self.prefetched_footers[path_str]["row_groups"][rg_idx]["num_rows"]
        return 0

    cpdef void close(self):
        """Cancel outstanding work, drain the pool, free the footer map. Safe to
        call more than once and on a source that produced no work."""
        if self._closed:
            return
        self._closed = True
        if self.pipeline is not None:
            self.pipeline.cancel()
            self.pipeline.close()
        if self.footer_map != NULL:
            del self.footer_map
            self.footer_map = NULL


def _flatten_dict_skip_predicates(predicates):
    """Flatten pushed (col, op, value) triples into dictionary decode-skip inputs:
    ``(int_needles{col:[int]}, str_preds{col:(kind,[bytes])})``. kind: 1=membership
    (=/IN), 2=starts-with, 3=ends-with, 4=contains. One predicate per column for
    strings (first wins) — a single conjunct is sound for skipping."""
    int_needles = {}
    str_preds = {}
    _kind = {"_STARTS_WITH": 2, "_ENDS_WITH": 3, "InStr": 4}
    for pred in predicates:
        p_col, p_op, p_val = pred
        if p_op == "Eq":
            if type(p_val) is int:
                int_needles.setdefault(p_col, []).append(p_val)
            else:
                b = p_val if isinstance(p_val, bytes) else (p_val.encode("utf-8") if isinstance(p_val, str) else None)
                if b is not None and p_col not in str_preds:
                    str_preds[p_col] = (1, [b])
        elif p_op == "InList":
            ints = [vv for vv in (p_val or []) if type(vv) is int]
            if ints:
                int_needles.setdefault(p_col, []).extend(ints)
            elif p_col not in str_preds:
                strs = []
                for vv in (p_val or []):
                    if isinstance(vv, bytes):
                        strs.append(vv)
                    elif isinstance(vv, str):
                        strs.append(vv.encode("utf-8"))
                if strs:
                    str_preds[p_col] = (1, strs)
        elif p_op in _kind:
            b = p_val if isinstance(p_val, bytes) else (p_val.encode("utf-8") if isinstance(p_val, str) else None)
            if b is not None and p_col not in str_preds:
                str_preds[p_col] = (_kind[p_op], [b])
    return int_needles, str_preds


cpdef IpcRowGroupSource open_ipc_source(
    filesystem,
    paths,
    column_names,
    int decode_workers=4,
    predicates=None,
    file_sizes=None,
    connector=None,
    query_id=None,
    prefetched_footers=None,
    footer_bytes_cache=None,
    null_fillers=None,
):
    """Plan a single-pass scan: fetch footers, prune row groups, size the pool,
    and create the C++ pipeline. Returns a started IpcRowGroupSource; the caller
    drives it with next_vectors() and releases it with close(). A source with
    n_items == 0 is returned for empty/fully-pruned scans (close() still safe)."""
    from opteryx.connectors.parquet_io.predicates import row_group_may_satisfy

    cdef IpcRowGroupSource src = IpcRowGroupSource()
    src.column_names = list(column_names)
    src.column_names_bytes = [c.encode('utf-8') for c in src.column_names]
    src.column_null_fillers = list(null_fillers) if null_fillers is not None else None
    src.prefetched_footers = prefetched_footers
    src.footer_map = new unordered_map[string, FileStats]()

    # Signed-URL rewrite (GCS): C++ libcurl fetches need no auth header. A reverse
    # map translates C++ result paths back to originals for telemetry.
    sign_url = getattr(filesystem, "rewrite_to_signed_url", None)
    cdef dict orig_to_cpp = {}
    cdef dict cpp_to_orig = {}
    if sign_url:
        for path in paths:
            if path not in orig_to_cpp:
                cpp_path = sign_url(path)
                orig_to_cpp[path] = cpp_path
                cpp_to_orig[cpp_path] = path
    src.orig_to_cpp = orig_to_cpp
    src.cpp_to_orig = cpp_to_orig

    cdef string path_bytes_cpp
    cdef const uint8_t* footer_buf_ptr
    cdef size_t footer_buf_size
    cdef RowGroupStats* rgp
    cdef size_t rg_i, ci
    cdef int64_t max_rg_bytes = 0
    cdef int64_t rg_bytes
    cdef int64_t est_rg, dyn_pool_size
    cdef int in_flight_limit

    work_items = []

    # ── Batch footer prefetch: one concurrent get_many for remote/uncached files.
    batch_orig = []
    batch_urls = []
    batch_sizes = []
    for path in paths:
        if prefetched_footers and path in prefetched_footers:
            continue
        fetch_url = orig_to_cpp.get(path, path)
        if not (fetch_url.startswith("gs://")
                or fetch_url.startswith("http://")
                or fetch_url.startswith("https://")):
            continue
        if _PARSED_FOOTER_CACHE.try_get(path, &src.footer_map[0][path.encode('utf-8')]):
            continue
        if footer_bytes_cache is not None and footer_bytes_cache.get(fetch_url) is not None:
            continue
        batch_orig.append(path)
        batch_urls.append(fetch_url)
        batch_sizes.append(file_sizes.get(path, -1) if file_sizes else -1)
    if batch_urls:
        envelopes = _fetch_footers_many(batch_urls, batch_sizes)
        for bi in range(len(batch_orig)):
            envelope = envelopes[bi]
            if footer_bytes_cache is not None:
                footer_bytes_cache.put(batch_urls[bi], envelope)
            footer_buf_ptr = <const uint8_t*>envelope
            footer_buf_size = len(envelope)
            src.footer_map[0][batch_orig[bi].encode('utf-8')] = ReadParquetMetadataFromBuffer(
                footer_buf_ptr, footer_buf_size
            )
            _PARSED_FOOTER_CACHE.put_fs(batch_orig[bi], src.footer_map[0][batch_orig[bi].encode('utf-8')])

    for path in paths:
        if prefetched_footers and path in prefetched_footers:
            meta = prefetched_footers[path]
            for rg_idx, rg_meta in enumerate(meta.get("row_groups", [])):
                if predicates and not row_group_may_satisfy(rg_meta, predicates):
                    continue
                work_items.append((path, rg_idx))
        else:
            path_bytes_cpp = path.encode('utf-8')
            if src.footer_map[0].count(path_bytes_cpp) == 0:
                if not _PARSED_FOOTER_CACHE.try_get(path, &src.footer_map[0][path_bytes_cpp]):
                    envelope, _ = _read_footer_payload(
                        orig_to_cpp.get(path, path),
                        file_sizes.get(path, -1) if file_sizes else -1,
                        footer_bytes_cache,
                    )
                    footer_buf_ptr = <const uint8_t*>envelope
                    footer_buf_size = len(envelope)
                    src.footer_map[0][path_bytes_cpp] = ReadParquetMetadataFromBuffer(
                        footer_buf_ptr, footer_buf_size
                    )
                    _PARSED_FOOTER_CACHE.put_fs(path, src.footer_map[0][path_bytes_cpp])
            # Local file path for bloom probing (None for remote — the C++
            # bloom probe opens the file via ifstream and can't read URLs).
            cpp_path_str = orig_to_cpp.get(path, path)
            bloom_path = cpp_path_str if _is_local_path(cpp_path_str) else None
            for rg_i in range(src.footer_map[0][path_bytes_cpp].row_groups.size()):
                if predicates and not _rg_passes_predicates_native(
                    src.footer_map[0][path_bytes_cpp].row_groups[rg_i], predicates, bloom_path
                ):
                    continue
                work_items.append((path, rg_i))

    src.work_items = work_items
    src.n_items = len(work_items)
    if src.n_items == 0:
        return src

    # ── Footer-derived pool sizing from the largest projected row group. ──
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
            rgp = &src.footer_map[0][path_bytes_cpp].row_groups[rg_idx]
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
    src.in_flight_limit = in_flight_limit
    src.pipeline = CppIOPipeline(
        decode_workers=decode_workers,
        queue_capacity=1024,
        pool_size=dyn_pool_size,
    )
    # Phase 2: pushed per-value predicates → worker dictionary decode-skip. Same
    # conjunct assumption as min/max row-group pruning above.
    if predicates:
        int_needles, str_preds = _flatten_dict_skip_predicates(predicates)
        for cname, needles in int_needles.items():
            if needles and cname not in str_preds:
                src.pipeline.add_int_needles(cname, needles)
        for cname in str_preds:
            kind, pats = str_preds[cname]
            if pats:
                src.pipeline.add_str_pred(cname, kind, pats)
    return src


cpdef IpcRowGroupSource open_pass2_source(
    filesystem,
    work_items,
    column_names,
    int decode_workers=0,
    file_sizes=None,
    connector=None,
    query_id=None,
    footer_bytes_cache=None,
    null_fillers=None,
):
    """Pass-2 late-materialization driver: decode only the surviving rows of the
    pre-determined ``work_items`` (``(path, rg_idx, mask_bytes)`` from pass-1).

    Returns an IpcRowGroupSource configured with explicit work items + per-row-group
    masks — no footer pruning (pass-1 already chose the row groups). The masked
    submit + the thread-safe next_vectors() consume are shared with the single-pass
    driver. Pass-1 already fetched the footers, so these are cache hits."""
    if decode_workers <= 0:
        import os
        decode_workers = max(2, (os.cpu_count() or 4) - 2)

    cdef IpcRowGroupSource src = IpcRowGroupSource()
    src.column_names = list(column_names)
    src.column_names_bytes = [c.encode('utf-8') for c in src.column_names]
    src.column_null_fillers = list(null_fillers) if null_fillers is not None else None
    src.prefetched_footers = None
    src.footer_map = new unordered_map[string, FileStats]()

    sign_url = getattr(filesystem, "rewrite_to_signed_url", None)
    cdef dict orig_to_cpp = {}
    cdef dict cpp_to_orig = {}
    if sign_url:
        for path, _rg, _mask in work_items:
            if path not in orig_to_cpp:
                cpp_path = sign_url(path)
                orig_to_cpp[path] = cpp_path
                cpp_to_orig[cpp_path] = path
    src.orig_to_cpp = orig_to_cpp
    src.cpp_to_orig = cpp_to_orig

    cdef string path_bytes_cpp
    cdef const uint8_t* footer_buf_ptr
    cdef size_t footer_buf_size
    cdef list wi = []
    cdef list masks = []
    for path, rg_idx, mask_bytes in work_items:
        wi.append((path, rg_idx))
        masks.append(bytes(mask_bytes))
        path_bytes_cpp = path.encode('utf-8')
        if src.footer_map[0].count(path_bytes_cpp) == 0:
            if not _PARSED_FOOTER_CACHE.try_get(path, &src.footer_map[0][path_bytes_cpp]):
                envelope, _ = _read_footer_payload(
                    orig_to_cpp.get(path, path),
                    file_sizes.get(path, -1) if file_sizes else -1,
                    footer_bytes_cache,
                )
                footer_buf_ptr = <const uint8_t*>envelope
                footer_buf_size = len(envelope)
                src.footer_map[0][path_bytes_cpp] = ReadParquetMetadataFromBuffer(
                    footer_buf_ptr, footer_buf_size
                )
                _PARSED_FOOTER_CACHE.put_fs(path, src.footer_map[0][path_bytes_cpp])
    src.work_items = wi
    src.masks = masks
    src.n_items = len(wi)
    if src.n_items == 0:
        return src
    src.in_flight_limit = decode_workers + 2
    src.pipeline = CppIOPipeline(
        decode_workers=decode_workers,
        queue_capacity=1024,
        pool_size=256*1024*1024,
    )
    return src


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

    Thin generator wrapper over IpcRowGroupSource for callers that want the
    (ScanRowGroup, {col: Vector}) contract (latmat pass-1, tests). The single-pass
    scan operator drives IpcRowGroupSource directly with no generator frame.
    """
    cdef IpcRowGroupSource src = open_ipc_source(
        filesystem, paths, column_names,
        decode_workers=decode_workers, predicates=predicates,
        file_sizes=file_sizes, connector=connector, query_id=query_id,
        prefetched_footers=prefetched_footers, footer_bytes_cache=footer_bytes_cache,
    )
    cdef list names = src.column_names_bytes
    cdef list vectors
    cdef tuple pulled
    cdef Py_ssize_t i, n
    if src.n_items == 0:
        src.close()
        return
    try:
        while True:
            pulled = src.next_vectors()
            if pulled is None:
                break
            vectors = pulled[0]
            if vectors is None:
                # Phase 2 empty row group (dictionary-membership skip): no columns.
                continue
            n = len(vectors)
            row_group = {names[i]: vectors[i] for i in range(n)}
            telemetry_dict = {
                '__bytes_fetched__': pulled[1],
                '__time_read_ranges_ns__': pulled[2],
                '__time_decode_columns_ns__': pulled[3],
            }
            scan_rg = _make_scan_row_group(pulled[4], pulled[5], 'cpp-pipeline', telemetry_dict)
            yield (scan_rg, row_group)
    finally:
        import os
        diag_json_path = os.environ.get("OPTERYX_IO_DIAG_JSON")
        if diag_json_path and src.pipeline is not None:
            # Machine-readable diag for the dev benchmark harness (one JSON line
            # per scan). The split native driver no longer measures the Python
            # per-phase timers, so those fields are reported as 0; the C++ counters
            # (the load-bearing ones) are preserved.
            import json
            record = src.pipeline.diagnostics()
            record.update({
                "paths": len(set(p for p, _ in src.work_items)) if src.work_items else 0,
                "row_groups": src.n_items,
            })
            with open(diag_json_path, "a") as diag_file:
                diag_file.write(json.dumps(record) + "\n")
        src.close()


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
        # WP-8: cancel before close so unconsumed pass-2 row groups bail in the
        # workers rather than decode during wait_shutdown (e.g. on early
        # abandonment). Harmless flag flip on normal exhaustion.
        pipeline.cancel()
        pipeline.close()
