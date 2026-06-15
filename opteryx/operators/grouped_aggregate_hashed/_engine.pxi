# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# GroupHashEngine — core grouped aggregation engine.
# CarcharIndex maps uint64 hash → int64 group slot.
# No Python in the ingest inner loop.

# Probe-prefetch distance (rows ahead) for the carchar ingest loop. Tunable.
DEF _AGG_PROBE_PREFETCH = 16

cdef extern from "time.h":
    ctypedef long time_t
    ctypedef int clockid_t

    cdef struct timespec:
        time_t tv_sec
        long tv_nsec

    int clock_gettime(clockid_t clk_id, timespec* tp) nogil

cdef extern from "time.h":
    clockid_t CLOCK_MONOTONIC

from libc.stdint cimport int64_t, uint64_t, uint32_t, uint8_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector

from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenType, DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY
from draken.core.buffers cimport DrakenVector

cdef extern from "core/buffers.h" nogil:
    int draken_is_compressed(const DrakenVector* v)


cdef extern from "carchar_index.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharIndex:
        CarcharIndex(size_t initial_capacity, double load_factor) except +
        void reserve(size_t expected_entries) except +
        size_t size() const
        bint lookup_fast(uint64_t key, int64_t& payload_ref_out) const
        size_t insert_new(uint64_t key, int64_t payload_ref) except +
        # Single-probe combined find/insert.  Returns True if new_id was newly
        # inserted; False if an existing payload was returned in payload_out.
        # Declared noexcept nogil so the hot keying loop runs off-GIL. The ONLY
        # throw path is the internal resize on a miss; callers MUST reserve()
        # worst-case capacity before probing so that resize is unreachable here.
        bint find_or_insert_id(uint64_t key, int64_t new_id, int64_t& payload_out) noexcept nogil
        void prefetch(uint64_t key) noexcept nogil


cdef extern from "parvi.hpp" namespace "opteryx::parvi":
    cdef struct ParviResult:
        size_t slot
        bint   found

    cdef cppclass ParviMap:
        ParviMap() except +
        size_t size() const
        bint   full() const
        bint   lookup_fast(uint64_t key, int64_t& payload_ref_out) const
        ParviResult insert_new(uint64_t key, int64_t payload_ref)
        void   drain_into(CarcharIndex& target) const

    const size_t kCapacity


cdef double _CARCHAR_LOAD_FACTOR = 0.70
# Holds 0.70 * 4096 = 2867 groups before the first resize — covers the common
# medium-cardinality GROUP BY cohort with zero resizes at ~70KB init cost,
# without taxing tiny or huge aggregations. (Swept empirically; the real fix for
# high-cardinality is NDV-based reserve(), which a constant cannot provide.)
cdef size_t _INITIAL_INDEX_CAPACITY = 4096
cdef size_t _PARVI_CAPACITY = 16  # must match opteryx::parvi::kCapacity


cdef inline long long _now_ns() noexcept nogil:
    cdef timespec ts
    if clock_gettime(CLOCK_MONOTONIC, &ts) != 0:
        return 0
    return <long long>ts.tv_sec * 1000000000 + <long long>ts.tv_nsec


cdef class GroupHashEngine:
    """
    Core engine for hashed grouped aggregation.

    ingest() is called once per input morsel.
    finalize_morsels() is a Python generator that yields result morsels.

    Design invariant: zero Python object creation inside the ingest loop.
    All state is in C++ vectors. Collectors hold typed C++ arrays.
    """

    cdef CarcharIndex* _index
    cdef ParviMap* _parvi
    cdef bint _use_parvi                      # chosen at init — parvi is the active map
    cdef bint _promoted_from_parvi            # telemetry: parvi overflowed → carchar
    cdef int64_t _parvi_final_size            # telemetry: groups held in parvi at promotion
    cdef KeyStore _key_store
    cdef list _collectors           # list[BaseCollector] — not accessed inside the loop
    cdef list _key_kinds            # list[int] — resolved on first morsel
    cdef int64_t _num_groups
    cdef vector[uint32_t] _state_indices_buf   # reused per morsel, length == n_rows
    cdef vector[int64_t] _new_row_scratch     # indices of rows that introduced new groups
    cdef vector[int64_t] _code_state          # compressed path: group id per code (-1 = unprobed)
    cdef list _group_columns                  # list[bytes] — init only
    cdef bint _resolved                       # True after first morsel type resolution
    cdef bint _telemetry_enabled
    cdef long long _time_resolve_ns
    cdef long long _time_hash_ns
    cdef long long _time_lookup_ns
    cdef long long _time_store_keys_ns
    cdef long long _time_grow_ns
    cdef long long _time_accumulate_ns
    cdef long long _time_finalize_ns
    cdef long long _time_reconstruct_ns
    cdef long long _time_reconstruct_single_fixed_ns
    cdef long long _time_reconstruct_single_string_ns
    cdef long long _time_reconstruct_multi_ns
    cdef long long _time_build_morsel_ns
    cdef long long _time_slice_output_ns

    def __cinit__(
        self,
        list group_columns,
        list collectors,
        bint telemetry_enabled=False,
        bint use_parvi=False,
    ):
        self._group_columns = group_columns
        self._collectors = collectors
        self._num_groups = 0
        self._resolved = False
        self._telemetry_enabled = telemetry_enabled
        self._use_parvi = use_parvi
        self._promoted_from_parvi = False
        self._parvi_final_size = 0
        self._parvi = NULL
        self._time_resolve_ns = 0
        self._time_hash_ns = 0
        self._time_lookup_ns = 0
        self._time_store_keys_ns = 0
        self._time_grow_ns = 0
        self._time_accumulate_ns = 0
        self._time_finalize_ns = 0
        self._time_reconstruct_ns = 0
        self._time_reconstruct_single_fixed_ns = 0
        self._time_reconstruct_single_string_ns = 0
        self._time_reconstruct_multi_ns = 0
        self._time_build_morsel_ns = 0
        self._time_slice_output_ns = 0
        # key_kinds and KeyStore are initialized after first-morsel resolution
        self._index = NULL
        self._key_kinds = [KEY_MULTI_FIXED_INT] * len(group_columns)

    def __dealloc__(self):
        if self._index != NULL:
            del self._index
            self._index = NULL
        if self._parvi != NULL:
            del self._parvi
            self._parvi = NULL

    cpdef void set_telemetry_enabled(self, bint enabled):
        self._telemetry_enabled = enabled

    cdef void _promote_parvi_to_carchar(self):
        """One-shot: promote the small parvi map to a carchar index.

        Called when parvi.insert_new() signals overflow. Slot ids assigned in
        parvi are dense and monotonic in insertion order — identical to the
        convention num_groups++ used by the ingest loop — so no remap of the
        collectors, KeyStore or _num_groups is needed. After this call the
        carchar index holds every live entry from parvi, _use_parvi is False,
        and _parvi has been freed.
        """
        self._index = new CarcharIndex(_INITIAL_INDEX_CAPACITY, _CARCHAR_LOAD_FACTOR)
        if self._telemetry_enabled:
            self._parvi_final_size = <int64_t>self._parvi.size()
        self._parvi.drain_into(self._index[0])
        del self._parvi
        self._parvi = NULL
        self._use_parvi = False
        self._promoted_from_parvi = True

    cdef void _resolve_on_first_morsel(self, Morsel morsel):
        """Called once on first non-empty morsel to fix collector types and key kinds."""
        cdef long long start_ns
        if self._telemetry_enabled:
            start_ns = _now_ns()
        resolve_deferred_collectors(
            self._collectors, morsel, self._group_columns, self._key_kinds
        )
        self._key_store = KeyStore(self._group_columns, self._key_kinds)
        # Set the actual DrakenType for each string key column so the key store
        # reconstructs VARCHAR as str (via vector_from_string_sequence) and
        # VARBINARY as bytes (via vector_from_bytes_sequence).
        cdef Py_ssize_t _ki
        cdef DrakenType _col_type
        cdef int _col_type_int
        for _ki in range(<Py_ssize_t>len(self._group_columns)):
            if self._key_kinds[_ki] == KEY_MULTI_ENCODED_STRING:
                _col_type_int = morsel.column(self._group_columns[_ki])._nb.type.value
                _col_type = <DrakenType>_col_type_int
                self._key_store.set_string_col_type(_ki, _col_type)
            elif self._key_kinds[_ki] == KEY_MULTI_FIXED_TIMESTAMP64:
                # Capture the timestamp unit ("s"/"ms"/"us"/"ns") so the key
                # store can reconstruct the group key as TIMESTAMP, not int64.
                self._key_store.set_timestamp_col_unit(
                    _ki, morsel.column(self._group_columns[_ki])._nb.logical_type_unit)
            elif self._key_kinds[_ki] == KEY_MULTI_FIXED_DECIMAL128:
                # Capture (precision, scale) so the key store reconstructs the group
                # key as DECIMAL128(p, s), not a raw int128.
                self._key_store.set_decimal_col_descriptor(
                    _ki,
                    morsel.column(self._group_columns[_ki])._nb.logical_type_precision,
                    morsel.column(self._group_columns[_ki])._nb.logical_type_scale)
        if self._use_parvi:
            # Small-map fast path: no heap hash table, single SIMD group.
            # Carchar is allocated lazily on overflow (see _promote_parvi_to_carchar).
            self._parvi = new ParviMap()
        else:
            self._index = new CarcharIndex(_INITIAL_INDEX_CAPACITY, _CARCHAR_LOAD_FACTOR)
        self._resolved = True

        if self._telemetry_enabled:
            self._time_resolve_ns += _now_ns() - start_ns

    cpdef void ingest(self, Morsel morsel):
        """Route one input Morsel to the appropriate ingest path."""
        cdef Py_ssize_t n_rows = morsel.num_rows
        if n_rows == 0:
            return
        if not self._resolved:
            self._resolve_on_first_morsel(morsel)
        self._do_ingest(morsel)

    cdef void _do_ingest(self, Morsel morsel) except *:
        """
        Process one input Morsel directly into the global hash table.
        No Python in the inner loop.
        """
        cdef long long start_ns
        cdef long long phase_start
        cdef Py_ssize_t n_rows = morsel.num_rows
        if n_rows == 0:
            return

        if self._telemetry_enabled:
            start_ns = _now_ns()

        if not self._resolved:
            self._resolve_on_first_morsel(morsel)

        # Ensure scratch buffers are large enough.
        if <Py_ssize_t>self._state_indices_buf.size() < n_rows:
            self._state_indices_buf.resize(n_rows)

        cdef uint32_t* si_buf = self._state_indices_buf.data()
        cdef int64_t state_idx
        cdef Py_ssize_t i
        cdef int64_t num_groups = self._num_groups
        cdef uint64_t h
        cdef int cache_slot
        cdef uint64_t cache_keys[8]
        cdef int64_t cache_vals[8]
        cdef uint8_t cache_used[8]
        cdef object hv
        cdef DrakenVector* huv
        cdef const uint64_t* khashes
        cdef const uint32_t* codes
        cdef bint compressed
        cdef Py_ssize_t k, c
        cdef int64_t* code_state
        cdef bint _is_new
        cdef ParviResult pr
        cdef bint _hot_is_new

        self._new_row_scratch.clear()
        # Reserve worst-case scratch (every row introduces a new group) so the
        # push_back inside the nogil keying loop never reallocates.
        self._new_row_scratch.reserve(<size_t>n_rows)
        if self._telemetry_enabled:
            phase_start = _now_ns()
        # Shape-preserving keying hash. Compressed (single dict/constant key) →
        # data holds k distinct hashes addressed by per-row codes; dense/multi →
        # data holds n hashes with identity selection. Read uniformly as
        # khashes[codes[i]] for any shape. hv kept alive for the whole ingest.
        hv = morsel.hash_keys(self._group_columns)
        huv = (<Vector>hv).unified()
        khashes = <const uint64_t*>huv.data
        codes = huv.selection
        if self._telemetry_enabled:
            self._time_hash_ns += _now_ns() - phase_start

        # Compressed fast path: probe each of the k distinct hashes ONCE (on its
        # first-occurrence row), not once per row. Collapses the random hash-table
        # probing from n to k. Only when carchar is the active map — parvi keeps
        # its per-row loop (its 8-slot cache already absorbs repeats at tiny k).
        compressed = (not self._use_parvi) and (draken_is_compressed(huv) != 0)
        # Reserve worst-case index capacity (every row a new group) BEFORE probing
        # so find_or_insert_id's internal resize — its only throw path — cannot
        # fire inside the nogil keying loop below. Idempotent: no-ops once capacity
        # is sufficient, so steady state is a single hoisted resize, not per-row.
        # Parvi (low-cardinality) has fixed capacity and promotes under the GIL, so
        # it needs no reserve here.
        if not self._use_parvi:
            self._index.reserve(<size_t>(num_groups + n_rows))
        if self._telemetry_enabled:
            phase_start = _now_ns()

        if compressed:
            k = <Py_ssize_t>huv.data_length
            if <Py_ssize_t>self._code_state.size() < k:
                self._code_state.resize(k)
            code_state = self._code_state.data()
            # Capacity reserved above; probe loop is pure C++ on raw pointers and
            # the noexcept-nogil index — run it off-GIL (CLAUDE.md §2).
            with nogil:
                for c in range(k):
                    code_state[c] = -1          # unprobed sentinel (group ids are >= 0)
                for i in range(n_rows):
                    c = <Py_ssize_t>codes[i]
                    state_idx = code_state[c]
                    if state_idx == -1:
                        # First occurrence of this code in the morsel → probe once.
                        _is_new = self._index.find_or_insert_id(khashes[c], num_groups, state_idx)
                        if _is_new:
                            self._new_row_scratch.push_back(i)   # first occurrence row
                            num_groups += 1
                        code_state[c] = state_idx
                    si_buf[i] = <uint32_t>state_idx
        else:
            # i is shared across parvi → carchar handoff so an overflow mid-morsel
            # resumes at the row after the one that triggered promotion.
            i = 0
            if self._use_parvi:
                # Tiny direct-mapped cache for repeated hashes within this morsel.
                # This targets very low-cardinality GROUP BY workloads (e.g. status/category).
                for cache_slot in range(8):
                    cache_used[cache_slot] = 0

                while i < n_rows:
                    h = khashes[codes[i]]
                    cache_slot = <int>(h & 7)
                    if cache_used[cache_slot] and cache_keys[cache_slot] == h:
                        state_idx = cache_vals[cache_slot]
                        si_buf[i] = <uint32_t>state_idx
                        i += 1
                        continue

                    # Single-probe path: insert_new returns existing slot on hit,
                    # new slot on insert, and kCapacity on overflow.
                    pr = self._parvi.insert_new(h, num_groups)
                    if pr.found:
                        state_idx = num_groups
                        self._new_row_scratch.push_back(i)
                        num_groups += 1
                    elif pr.slot == _PARVI_CAPACITY:
                        state_idx = num_groups
                        # Parvi overflow: drain into carchar and continue seamlessly.
                        self._promote_parvi_to_carchar()
                        self._index.insert_new(h, state_idx)
                        self._new_row_scratch.push_back(i)
                        num_groups += 1
                    else:
                        state_idx = <int64_t>pr.slot

                    cache_keys[cache_slot] = h
                    cache_vals[cache_slot] = state_idx
                    cache_used[cache_slot] = 1
                    si_buf[i] = <uint32_t>state_idx
                    i += 1
                    if not self._use_parvi:
                        break  # promoted — finish the morsel on the carchar path
            if not self._use_parvi:
                # Software-pipelined probe: prefetch the control line a probe
                # _AGG_PROBE_PREFETCH rows ahead would touch, so its cache miss
                # overlaps the dependent find_or_insert_id for the current row
                # (memory-level parallelism on the latency-bound probe). All hashes
                # are precomputed in khashes. ~10% on high-cardinality (all-miss)
                # GROUP BY; negligible when the table is cache-resident.
                # Capacity reserved above; pure C++ on raw pointers + the
                # noexcept-nogil index — run the probe off-GIL (CLAUDE.md §2).
                with nogil:
                    while i < n_rows:
                        if i + _AGG_PROBE_PREFETCH < n_rows:
                            self._index.prefetch(khashes[codes[i + _AGG_PROBE_PREFETCH]])
                        _hot_is_new = self._index.find_or_insert_id(khashes[codes[i]], num_groups, state_idx)
                        if _hot_is_new:
                            self._new_row_scratch.push_back(i)
                            num_groups += 1
                        si_buf[i] = <uint32_t>state_idx
                        i += 1
        if self._telemetry_enabled:
            self._time_lookup_ns += _now_ns() - phase_start

        self._num_groups = num_groups

        # ---- Post-loop (once per morsel, not per row) ----

        cdef Py_ssize_t n_new = <Py_ssize_t>self._new_row_scratch.size()
        if n_new > 0:
            if self._telemetry_enabled:
                phase_start = _now_ns()
            # Store group keys for new groups
            self._key_store.store_new_rows(
                morsel,
                self._new_row_scratch.data(),
                n_new,
            )
            if self._telemetry_enabled:
                self._time_store_keys_ns += _now_ns() - phase_start

            if self._telemetry_enabled:
                phase_start = _now_ns()
            # Grow all collectors to the new group count
            for collector in self._collectors:
                (<BaseCollector>collector).grow(num_groups)
            if self._telemetry_enabled:
                self._time_grow_ns += _now_ns() - phase_start

        if self._telemetry_enabled:
            phase_start = _now_ns()
        # Accumulate each collector over this morsel's rows
        for collector in self._collectors:
            (<BaseCollector>collector).accumulate(morsel, si_buf, n_rows)
        if self._telemetry_enabled:
            self._time_accumulate_ns += _now_ns() - phase_start

        if self._telemetry_enabled:
            self._time_resolve_ns += _now_ns() - start_ns

    def finalize_morsels(self, Py_ssize_t chunk_size=65536, filter_fn=None):
        """
        Generator. Yields result Morsels in chunks.
        Called once after all input morsels have been ingested.

        Args:
            chunk_size: Size of output chunks (default 65536)
            filter_fn: Optional callable that takes a Morsel and returns filtered Morsel.
                       If provided, filter is applied once to the complete result before chunking.
                       This avoids reconstructing groups that don't pass the filter.
        """
        from draken.morsels.morsel import Morsel as _Morsel

        cdef long long start_ns
        cdef long long phase_start
        cdef int64_t num_groups = self._num_groups

        if self._telemetry_enabled:
            start_ns = _now_ns()

        if num_groups == 0 or not self._resolved:
            return

        cdef long long t0
        cdef long long t1
        cdef long long _recon_elapsed

        # Reconstruct key columns — timed by path for split telemetry
        cdef list key_names = []
        cdef list key_vecs = []
        if self._telemetry_enabled:
            t0 = _now_ns()
            self._key_store.reconstruct_vectors(num_groups, key_names, key_vecs)
            _recon_elapsed = _now_ns() - t0
            self._time_reconstruct_ns += _recon_elapsed
            if self._key_store._n_cols == 1:
                if self._key_store._key_kinds[0] == KEY_MULTI_ENCODED_STRING:
                    self._time_reconstruct_single_string_ns += _recon_elapsed
                else:
                    self._time_reconstruct_single_fixed_ns += _recon_elapsed
            else:
                self._time_reconstruct_multi_ns += _recon_elapsed
        else:
            self._key_store.reconstruct_vectors(num_groups, key_names, key_vecs)

        # Finalize each aggregate collector
        cdef list agg_names = []
        cdef list agg_vecs = []
        if self._telemetry_enabled:
            t0 = _now_ns()
        for collector in self._collectors:
            c = <BaseCollector>collector
            agg_names.append(
                c.result_name.decode("utf-8") if isinstance(c.result_name, bytes) else c.result_name
            )
            agg_vecs.append(c.finalize(num_groups))
        if self._telemetry_enabled:
            self._time_finalize_ns += _now_ns() - t0

        # Build one full-size Morsel, then apply filter if provided, then slice into chunks
        if self._telemetry_enabled:
            t0 = _now_ns()
        all_names = agg_names + key_names
        all_vecs = agg_vecs + key_vecs
        full_morsel = _Morsel.from_vectors(all_names, all_vecs)
        if self._telemetry_enabled:
            self._time_build_morsel_ns += _now_ns() - t0

        # Apply filter early (before chunking) if provided, to avoid creating chunks for filtered-out groups
        if filter_fn is not None:
            full_morsel = filter_fn(full_morsel)
            if full_morsel is None or full_morsel.num_rows == 0:
                return

        cdef Py_ssize_t start = 0
        cdef Py_ssize_t length
        cdef Py_ssize_t result_rows = full_morsel.num_rows
        while start < result_rows:
            if self._telemetry_enabled:
                t1 = _now_ns()
            length = min(chunk_size, result_rows - start)
            yield full_morsel.slice(start, length)
            if self._telemetry_enabled:
                self._time_slice_output_ns += _now_ns() - t1
            start += length

    cpdef dict telemetry(self):
        return {
            "time_resolve_ns": self._time_resolve_ns,
            "time_hash_ns": self._time_hash_ns,
            "time_lookup_ns": self._time_lookup_ns,
            "time_store_keys_ns": self._time_store_keys_ns,
            "time_grow_ns": self._time_grow_ns,
            "time_accumulate_ns": self._time_accumulate_ns,
            "time_finalize_ns": self._time_finalize_ns,
            "time_reconstruct_ns": self._time_reconstruct_ns,
            "time_reconstruct_single_fixed_ns": self._time_reconstruct_single_fixed_ns,
            "time_reconstruct_single_string_ns": self._time_reconstruct_single_string_ns,
            "time_reconstruct_multi_ns": self._time_reconstruct_multi_ns,
            "time_build_morsel_ns": self._time_build_morsel_ns,
            "time_slice_output_ns": self._time_slice_output_ns,
            # Hash-map variant telemetry. Only meaningful when telemetry is on;
            # otherwise _parvi_final_size is never updated (stays 0) and the
            # bools still reflect the final map state since they are kept in
            # sync on every transition.
            "used_parvi": self._parvi != NULL or self._promoted_from_parvi,
            "promoted_from_parvi": self._promoted_from_parvi,
            "parvi_final_size": self._parvi_final_size,
        }
