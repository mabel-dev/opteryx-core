# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# GroupHashEngine — core grouped aggregation engine.
# CarcharIndex maps uint64 hash → int64 group slot.
# No Python in the ingest inner loop.

cdef extern from "time.h":
    ctypedef long time_t
    ctypedef int clockid_t

    cdef struct timespec:
        time_t tv_sec
        long tv_nsec

    int clock_gettime(clockid_t clk_id, timespec* tp) nogil

cdef extern from "time.h":
    clockid_t CLOCK_MONOTONIC

from libc.stdint cimport int64_t, uint64_t, uint8_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector

from draken.morsels.morsel cimport Morsel


cdef extern from "carchar_index.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharIndex:
        CarcharIndex(size_t initial_capacity, double load_factor) except +
        void reserve(size_t expected_entries)
        size_t size() const
        bint lookup_fast(uint64_t key, int64_t& payload_ref_out) const
        size_t insert_new(uint64_t key, int64_t payload_ref) except +
        # Single-probe combined find/insert.  Returns True if new_id was newly
        # inserted; False if an existing payload was returned in payload_out.
        bint find_or_insert_id(uint64_t key, int64_t new_id, int64_t& payload_out) except +


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
cdef size_t _INITIAL_INDEX_CAPACITY = 256
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
    cdef vector[int64_t] _state_indices_buf   # reused per morsel, length == n_rows
    cdef vector[int64_t] _new_row_scratch     # indices of rows that introduced new groups
    cdef list _group_columns                  # list[bytes] — init only
    cdef bint _resolved                       # True after first morsel type resolution
    cdef bint _telemetry_enabled
    cdef bint _use_partial_agg                # True when all keys are dict/RLE and cardinality is low
    cdef bint _allow_partial_agg              # False for local engines to prevent recursion
    cdef list _original_collectors            # resolved collector instances before merge-swap
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
        bint allow_partial_agg=True,
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
        self._use_partial_agg = False
        self._allow_partial_agg = allow_partial_agg
        self._original_collectors = None
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

    cdef void _resolve_on_first_morsel(self, object morsel):
        """Called once on first non-empty morsel to fix collector types and key kinds."""
        cdef long long start_ns
        if self._telemetry_enabled:
            start_ns = _now_ns()
        resolve_deferred_collectors(
            self._collectors, morsel, self._group_columns, self._key_kinds
        )
        self._key_store = KeyStore(self._group_columns, self._key_kinds)
        if self._use_parvi:
            # Small-map fast path: no heap hash table, single SIMD group.
            # Carchar is allocated lazily on overflow (see _promote_parvi_to_carchar).
            self._parvi = new ParviMap()
        else:
            self._index = new CarcharIndex(_INITIAL_INDEX_CAPACITY, _CARCHAR_LOAD_FACTOR)
        self._resolved = True

        if self._allow_partial_agg:
            self._try_enable_partial_agg(morsel)

        if self._telemetry_enabled:
            self._time_resolve_ns += _now_ns() - start_ns

    cdef void _try_enable_partial_agg(self, object morsel):
        """Gate: enable partial aggregation when all group keys are dict/RLE encoded
        and the estimated group count is ≤ 50% of morsel row count.

        When enabled, swaps _collectors for merge-mode collectors (reading from
        result_name) and stores the originals in _original_collectors for local
        engine construction each morsel.

        Design intent: for low-cardinality dict/RLE-encoded string group keys, a
        per-morsel local aggregate collapses N rows to K groups (K << N), so the
        global hash table sees K probes per morsel instead of N.  Both the local
        table (bounded by dict/RLE cardinality) and the global table (bounded by K
        total distinct groups) stay cache-resident, eliminating the DRAM-speed
        probes that dominate high-cardinality aggregation.

        This gate does NOT fire on integer group keys (KEY_MULTI_FIXED_INT) because:
          - ClickBench-style queries group on dense integers (UserID, WatchID, RegionID)
            where every row is a near-unique group and partial agg gives no fold.
          - Integer-keyed GROUP BY hits the irreducible cache-miss floor regardless
            of strategy; the right fix there is radix partitioning (out of scope here).

        The optimization targets real-world GROUP BY country/category/status patterns
        where a handful of dict entries appear millions of times per morsel.
        AVG and non-mergeable aggregates (COUNT DISTINCT, MEDIAN) are excluded;
        see _clone_as_merge() on each collector for the merge semantics.
        """
        # All group key columns must be encoded strings.
        for k in self._key_kinds:
            if k != KEY_MULTI_ENCODED_STRING:
                return

        # All collectors must be mergeable (non-None _clone_as_merge).
        cdef list merge_collectors = []
        cdef BaseCollector c, mc
        cdef StringVector svec
        for c in self._collectors:
            mc = (<BaseCollector>c)._clone_as_merge()
            if mc is None:
                return
            merge_collectors.append(mc)

        # Estimate combined group cardinality using dampened product of dict/RLE sizes.
        cdef double estimated = 1.0
        cdef bint first_col = True
        cdef Py_ssize_t card
        for col_name in self._group_columns:
            vec = morsel.column(col_name)
            if not isinstance(vec, StringVector):
                return
            svec = <StringVector>vec
            if svec.unified().selection != NULL:
                card = svec.c_dict_size()
            else:
                return
            if first_col:
                estimated = <double>card
                first_col = False
            else:
                estimated *= <double>card * 0.6

        if estimated > <double>morsel.num_rows * 0.5:
            return

        # Gate passed: store originals, swap in merge collectors.
        self._original_collectors = self._collectors
        self._collectors = merge_collectors
        self._use_partial_agg = True

    cdef void _ingest_with_partial_agg(self, object morsel) except *:
        """Route one raw morsel through a fresh local engine, then merge the
        partial result (one row per local group) into the global hash table."""
        cdef list local_collectors = []
        cdef BaseCollector c
        for c in self._original_collectors:
            local_collectors.append((<BaseCollector>c)._clone_empty())

        local_engine = GroupHashEngine(
            self._group_columns,
            local_collectors,
            False,   # no telemetry on local engines
            False,   # start with carchar; parvi (16 slots) would immediately overflow
            False,   # allow_partial_agg=False — prevent recursion
        )
        local_engine.ingest(morsel)
        for partial_morsel in local_engine.finalize_morsels():
            self._ingest_direct(partial_morsel)

    cdef void _ingest_direct(self, object morsel) except *:
        """Ingest a partial morsel directly into the global hash table,
        bypassing the partial-agg routing check."""
        self._do_ingest(morsel)

    cdef void _ingest_dict_single_string_fast(
        self, object morsel, StringVector svec, Py_ssize_t n_rows
    ) except *:
        """Fast ingest path for a single dict-encoded string GROUP BY key.

        Computes the K dict-value hashes once, looks them up in the index to
        build a code→state mapping, then walks the N codes filling the
        state-indices buffer.  Avoids the per-row hash-and-probe loop
        entirely.  Assumes carchar (non-parvi) mode and a non-NULL index.
        """
        cdef Py_ssize_t dict_size = svec.c_dict_size()
        cdef DrakenVector* _suv = svec.unified()
        cdef const uint8_t* codes = <const uint8_t*>_suv.selection
        cdef uint8_t code_width = _suv.sel_width
        cdef const uint8_t* row_nulls = _suv.validity
        cdef const int64_t* counts = svec.c_dict_code_counts_ptr()
        cdef int64_t* si_buf = self._state_indices_buf.data()
        cdef int64_t num_groups = self._num_groups
        cdef int64_t state_idx
        cdef uint64_t h
        cdef Py_ssize_t i, di
        cdef uint32_t code
        cdef int64_t null_state_idx = -1
        cdef Py_ssize_t first_null_row = -1
        cdef uint64_t null_marker = mix_hash(0, NULL_HASH)
        cdef int64_t* code_to_state = NULL
        cdef uint8_t* dict_is_new = NULL
        cdef Py_ssize_t* first_row_per_dict = NULL
        cdef Py_ssize_t alloc_k = dict_size if dict_size > 0 else 1
        cdef bint _is_new

        code_to_state = <int64_t*>malloc(<size_t>alloc_k * sizeof(int64_t))
        dict_is_new = <uint8_t*>malloc(<size_t>alloc_k * sizeof(uint8_t))
        first_row_per_dict = <Py_ssize_t*>malloc(<size_t>alloc_k * sizeof(Py_ssize_t))
        if code_to_state == NULL or dict_is_new == NULL or first_row_per_dict == NULL:
            if code_to_state != NULL: free(code_to_state)
            if dict_is_new != NULL: free(dict_is_new)
            if first_row_per_dict != NULL: free(first_row_per_dict)
            raise MemoryError()

        try:
            # Phase 1: per dict entry — compute hash, insert/lookup, build map.
            # Skip entries that are unreferenced (count == 0) to avoid
            # allocating empty groups.  Sentinel state_idx = -2 means
            # "this dict entry is itself null; rows pointing here go to
            # the global null group".  Sentinel state_idx = -3 means
            # "this dict entry has zero references; should never be looked
            # up via si_buf".
            for di in range(dict_size):
                first_row_per_dict[di] = -1
                dict_is_new[di] = 0
                if counts[di] <= 0:
                    code_to_state[di] = -3
                    continue
                if svec.c_dict_value_is_null(di):
                    code_to_state[di] = -2
                    continue
                h = svec.c_dict_value_hash(di)
                _is_new = self._index.find_or_insert_id(h, num_groups, state_idx)
                if _is_new:
                    num_groups += 1
                    dict_is_new[di] = 1
                code_to_state[di] = state_idx

            # Phase 2: walk N rows, fill si_buf, lazily allocate the null group.
            for i in range(n_rows):
                if row_nulls != NULL and not (
                    (row_nulls[i >> 3] >> (i & 7)) & 1
                ):
                    if null_state_idx < 0:
                        _is_new = self._index.find_or_insert_id(null_marker, num_groups, null_state_idx)
                        if _is_new:
                            num_groups += 1
                            first_null_row = i
                    si_buf[i] = null_state_idx
                    continue

                if code_width == 1:
                    code = (<const uint8_t*>codes)[i]
                elif code_width == 2:
                    code = (<const uint16_t*>codes)[i]
                else:
                    code = (<const uint32_t*>codes)[i]

                if <Py_ssize_t>code >= dict_size:
                    raise ValueError(
                        f"dictionary index out of bounds at row {i}: {code}"
                    )

                state_idx = code_to_state[<Py_ssize_t>code]
                if state_idx == -3:
                    # Should be unreachable: a valid row referencing a code
                    # implies count[code] > 0.  Fail fast if violated.
                    raise ValueError(
                        f"dict-fast-path invariant violated at row {i}: "
                        f"code {code} was unreferenced but a valid row uses it"
                    )
                if state_idx == -2:
                    # Dict entry is null → goes to the global null group.
                    if null_state_idx < 0:
                        _is_new = self._index.find_or_insert_id(null_marker, num_groups, null_state_idx)
                        if _is_new:
                            num_groups += 1
                            first_null_row = i
                    si_buf[i] = null_state_idx
                    continue

                si_buf[i] = state_idx
                if dict_is_new[<Py_ssize_t>code] and first_row_per_dict[<Py_ssize_t>code] < 0:
                    first_row_per_dict[<Py_ssize_t>code] = i

            # Phase 3: collect first-row indices for newly-allocated states so
            # KeyStore.store_new_rows can read each new group's key value.
            for di in range(dict_size):
                if dict_is_new[di] and first_row_per_dict[di] >= 0:
                    self._new_row_scratch.push_back(first_row_per_dict[di])
            if first_null_row >= 0:
                self._new_row_scratch.push_back(first_null_row)
        finally:
            free(code_to_state)
            free(dict_is_new)
            free(first_row_per_dict)

        self._num_groups = num_groups

        # Mirror the post-loop work the regular ingest path does.
        cdef Py_ssize_t n_new = <Py_ssize_t>self._new_row_scratch.size()
        cdef long long phase_start
        if n_new > 0:
            if self._telemetry_enabled:
                phase_start = _now_ns()
            self._key_store.store_new_rows(
                morsel,
                self._new_row_scratch.data(),
                n_new,
            )
            if self._telemetry_enabled:
                self._time_store_keys_ns += _now_ns() - phase_start

            if self._telemetry_enabled:
                phase_start = _now_ns()
            for collector in self._collectors:
                (<BaseCollector>collector).grow(num_groups)
            if self._telemetry_enabled:
                self._time_grow_ns += _now_ns() - phase_start

        if self._telemetry_enabled:
            phase_start = _now_ns()
        for collector in self._collectors:
            (<BaseCollector>collector).accumulate(morsel, si_buf, n_rows)
        if self._telemetry_enabled:
            self._time_accumulate_ns += _now_ns() - phase_start

    cpdef void ingest(self, object morsel):
        """Route one input Morsel to the appropriate ingest path."""
        cdef Py_ssize_t n_rows = morsel.num_rows
        if n_rows == 0:
            return
        if not self._resolved:
            self._resolve_on_first_morsel(morsel)
        if self._use_partial_agg:
            self._ingest_with_partial_agg(morsel)
        else:
            self._do_ingest(morsel)

    cdef void _do_ingest(self, object morsel) except *:
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
        self._new_row_scratch.clear()

        # Single dict-encoded string-key fast path: skip morsel.hash entirely
        # and use the dict codes as group ids.  K hash-table lookups instead
        # of N.  Only viable in carchar mode (parvi has its own small-map
        # invariants we don't replicate here).
        cdef Vector raw_vec
        cdef StringVector svec_key
        if (
            not self._use_parvi
            and self._index != NULL
            and len(self._group_columns) == 1
            and self._key_kinds[0] == KEY_MULTI_ENCODED_STRING
        ):
            raw_vec = <Vector>morsel.column(self._group_columns[0])
            if isinstance(raw_vec, StringVector):
                svec_key = <StringVector>raw_vec
                # Cardinality gate: at K ~ N the existing path's batched
                # hash + cached lookups beat our K-independent path.  Only
                # take the fast path when K is meaningfully smaller than N.
                if (
                    svec_key.unified().selection != NULL
                    and svec_key.c_dict_size() <= (n_rows >> 2)
                ):
                    if self._telemetry_enabled:
                        phase_start = _now_ns()
                    self._ingest_dict_single_string_fast(morsel, svec_key, n_rows)
                    if self._telemetry_enabled:
                        self._time_lookup_ns += _now_ns() - phase_start
                        self._time_resolve_ns += _now_ns() - start_ns
                    return

        if self._telemetry_enabled:
            phase_start = _now_ns()
        # Compute group hashes for the group-by columns
        cdef uint64_t[::1] hashes = morsel.hash(self._group_columns)
        if self._telemetry_enabled:
            self._time_hash_ns += _now_ns() - phase_start

        cdef int64_t* si_buf = self._state_indices_buf.data()
        cdef int64_t state_idx
        cdef Py_ssize_t i
        cdef int64_t num_groups = self._num_groups
        cdef uint64_t h
        cdef int cache_slot
        cdef uint64_t cache_keys[8]
        cdef int64_t cache_vals[8]
        cdef uint8_t cache_used[8]

        cdef ParviResult pr
        if self._telemetry_enabled:
            phase_start = _now_ns()
        # i is shared across parvi → carchar handoff so an overflow mid-morsel
        # resumes at the row after the one that triggered promotion.
        i = 0
        if self._use_parvi:
            # Tiny direct-mapped cache for repeated hashes within this morsel.
            # This targets very low-cardinality GROUP BY workloads (e.g. status/category).
            for cache_slot in range(8):
                cache_used[cache_slot] = 0

            while i < n_rows:
                h = hashes[i]
                cache_slot = <int>(h & 7)
                if cache_used[cache_slot] and cache_keys[cache_slot] == h:
                    state_idx = cache_vals[cache_slot]
                    si_buf[i] = state_idx
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
                si_buf[i] = state_idx
                i += 1
                if not self._use_parvi:
                    break  # promoted — finish the morsel on the carchar path
        cdef bint _hot_is_new
        if not self._use_parvi:
            while i < n_rows:
                _hot_is_new = self._index.find_or_insert_id(hashes[i], num_groups, state_idx)
                if _hot_is_new:
                    self._new_row_scratch.push_back(i)
                    num_groups += 1
                si_buf[i] = state_idx
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
