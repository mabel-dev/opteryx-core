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

from libc.stdint cimport int64_t, uint64_t, uint32_t, uint8_t, int32_t
from libc.stddef cimport size_t
from libcpp.vector cimport vector
from libcpp.string cimport string

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
        size_t size() noexcept nogil
        bint   full() noexcept nogil
        bint   lookup_fast(uint64_t key, int64_t& payload_ref_out) noexcept nogil
        # Pure noexcept C++ (fixed-capacity SIMD map; insert_new never allocates,
        # drain_into copies into a pre-reserved carchar) → safe to call from the
        # nogil ingest span. drain_into's internal carchar inserts cannot resize
        # because the engine reserves the carchar to hold kCapacity before drain.
        ParviResult insert_new(uint64_t key, int64_t payload_ref) noexcept nogil
        void   drain_into(CarcharIndex& target) noexcept nogil

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
    # One keying hash per group, appended in group-id order as groups are created
    # (both ingest paths). Lets the nogil merge read other's per-group hashes
    # directly — no reconstruct_vectors, no rehash. Only maintained on the nogil-
    # merge-eligible (single-col mergeable) path; the GIL merge ignores it.
    cdef vector[uint64_t] _group_hashes
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
    # S-B.3c nogil ingest_cxx state — populated once under the GIL at resolve.
    # The nogil span reads group/value column views straight off the CxxMorsel
    # by these positional indices and drives the collectors through borrowed
    # pointers, so it never touches the Python `self._collectors` list.
    cdef vector[int32_t] _group_col_idxs      # group cols' positions in the morsel
    cdef vector[int32_t] _collector_col_idxs  # each collector's source col (-1 = COUNT*)
    cdef vector[PyObject*] _collector_ptrs    # borrowed refs to collectors (list keeps them alive)
    cdef bint _all_nogil                      # gate: single-col key + every collector nogil-capable
    cdef public long long nogil_ingest_morsels  # morsels taken through the GIL-released span
    cdef public long long gil_ingest_morsels    # morsels that fell to the labelled GIL ingest

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
        self._all_nogil = False
        self.nogil_ingest_morsels = 0
        self.gil_ingest_morsels = 0

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
        and _parvi has been freed. The carchar is pre-allocated at resolve, so
        promotion is a drain into the existing index (no allocation here).
        """
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
        # reconstructs the group key with the right type (VARCHAR vs VARBINARY)
        # directly from the stored native string slots via from_decoded.
        cdef Py_ssize_t _ki
        cdef DrakenType _col_type
        cdef int _col_type_int
        for _ki in range(<Py_ssize_t>len(self._group_columns)):
            if self._key_kinds[_ki] == KEY_MULTI_ENCODED_STRING:
                _col_type_int = morsel._cxx_column(self._group_columns[_ki])._nb.type.value
                _col_type = <DrakenType>_col_type_int
                self._key_store.set_string_col_type(_ki, _col_type)
            elif self._key_kinds[_ki] == KEY_MULTI_FIXED_TIMESTAMP64:
                # Capture the timestamp unit ("s"/"ms"/"us"/"ns") so the key
                # store can reconstruct the group key as TIMESTAMP, not int64.
                self._key_store.set_timestamp_col_unit(
                    _ki, morsel._cxx_column(self._group_columns[_ki])._nb.logical_type_unit)
            elif self._key_kinds[_ki] == KEY_MULTI_FIXED_DECIMAL128:
                # Capture (precision, scale) so the key store reconstructs the group
                # key as DECIMAL128(p, s), not a raw int128.
                self._key_store.set_decimal_col_descriptor(
                    _ki,
                    morsel._cxx_column(self._group_columns[_ki])._nb.logical_type_precision,
                    morsel._cxx_column(self._group_columns[_ki])._nb.logical_type_scale)
        # Always allocate the carchar index. When parvi is the active small-map
        # fast path, the carchar is its promotion target — pre-allocated here
        # (under the GIL) and sized well above kCapacity, so promotion-on-overflow
        # inside the nogil span is a pure `drain_into` with no allocation/resize.
        self._index = new CarcharIndex(_INITIAL_INDEX_CAPACITY, _CARCHAR_LOAD_FACTOR)
        if self._use_parvi:
            self._parvi = new ParviMap()
        self._resolved = True

        # ---- S-B.3c nogil ingest_cxx wiring (under the GIL, once) ----------
        # Resolve group columns to their positional indices in the CxxMorsel
        # (the same index space m.columns[i] the nogil span reads) by matching
        # the canonical name vector, and snapshot collector pointers + source
        # column indices into C arrays so the nogil span never touches Python.
        cdef const CxxMorsel* cxm = NULL
        cdef string gtarget
        cdef object _gname
        cdef size_t _cj
        cdef int _found_idx
        cdef BaseCollector _bc
        self._group_col_idxs.clear()
        self._collector_ptrs.clear()
        self._collector_col_idxs.clear()
        cdef bint _all_nogil = (len(self._group_columns) >= 1)
        if morsel._cxx is not None:
            cxm = cxx_morsel_raw_ptr(<PyObject*>morsel._cxx)
        if cxm == NULL:
            _all_nogil = False
        else:
            for _ki in range(<Py_ssize_t>len(self._group_columns)):
                _gname = self._group_columns[_ki]
                if isinstance(_gname, str):
                    _gname = _gname.encode("utf-8")
                gtarget = <string>(<bytes>_gname)
                _found_idx = -1
                for _cj in range(cxm.names.size()):
                    if cxm.names[_cj] == gtarget:
                        _found_idx = <int>_cj
                        break
                if _found_idx < 0:
                    _all_nogil = False
                    break
                self._group_col_idxs.push_back(<int32_t>_found_idx)
        for collector in self._collectors:
            _bc = <BaseCollector>collector
            self._collector_ptrs.push_back(<PyObject*>collector)
            self._collector_col_idxs.push_back(<int32_t>_bc._col_idx)
            if not _bc._nogil_capable:
                _all_nogil = False
        self._all_nogil = _all_nogil

        if self._telemetry_enabled:
            self._time_resolve_ns += _now_ns() - start_ns

    cpdef void ingest(self, Morsel morsel):
        """Route one input Morsel to the appropriate ingest path."""
        cdef Py_ssize_t n_rows = morsel.num_rows
        if n_rows == 0:
            return
        if not self._resolved:
            self._resolve_on_first_morsel(morsel)

        # ---- grouped-agg ingest: one dispatch, the GIL split made VISIBLE ----
        # No silent fallback. Morsels that can run GIL-free take the nogil span
        # (keying+store+grow+accumulate, parvi included); the only morsels that
        # provably cannot are the two genuine DUAL INTERFACES — a collector that
        # calls into Python (median / count-distinct / approx / array_agg; its
        # real API is accumulate_gil), and a non-Cxx synthetic source (architect-
        # agreed Python path). Those take the labelled _ingest_gil, tallied in
        # gil_ingest_morsels so the split is reported honestly (nogil vs gil),
        # never hidden behind a silent `else`. parvi is NOT here — it now runs in
        # the nogil span; only the two permanent dual interfaces remain on GIL.
        cdef const CxxMorsel* cxm
        cdef CxxMorsel* hashm
        cdef const DrakenVector* huv
        cdef bint compressed
        cdef Py_ssize_t kdist
        cdef ErrCtx err
        cdef int rc

        if (not self._all_nogil) or (morsel._cxx is None):
            self.gil_ingest_morsels += 1
            self._ingest_gil(morsel)
            return

        cxm = cxx_morsel_raw_ptr(<PyObject*>morsel._cxx)
        if cxm == NULL:
            raise RuntimeError("grouped-agg ingest: Cxx morsel handle resolved to NULL")
        hashm = cxx_hash_c(
            cxm, self._group_col_idxs.data(), <uint32_t>self._group_col_idxs.size()
        )
        if hashm == NULL:
            raise MemoryError("cxx_hash_c failed in grouped-aggregate ingest")
        huv = &hashm.columns[0].view
        # parvi and the compressed fast path are mutually exclusive (parvi keys
        # row-by-row); only consider compressed once carchar is the active map.
        compressed = (not self._use_parvi) and (draken_is_compressed(huv) != 0)
        # Hoist every allocating/throwing prep under the GIL so the span below is
        # alloc-free at the hash-table and scratch level.
        if <Py_ssize_t>self._state_indices_buf.size() < n_rows:
            self._state_indices_buf.resize(n_rows)
        self._new_row_scratch.clear()
        self._new_row_scratch.reserve(<size_t>n_rows)
        self._index.reserve(<size_t>(self._num_groups + n_rows))
        if compressed:
            kdist = <Py_ssize_t>huv.data_length
            if <Py_ssize_t>self._code_state.size() < kdist:
                self._code_state.resize(kdist)
        err.code = 0
        err.msg = NULL
        self.nogil_ingest_morsels += 1
        with nogil:
            rc = self._ingest_cxx_span(cxm, huv, compressed, n_rows, &err)
        cxx_morsel_delete(hashm)
        if rc != 0:
            raise MemoryError("grouped-aggregate nogil ingest failed (alloc)")

    cdef int _ingest_cxx_span(
        self,
        const CxxMorsel* m,
        const DrakenVector* huv,
        bint compressed,
        Py_ssize_t n_rows,
        ErrCtx* err,
    ) noexcept nogil:
        """GIL-released single-column grouped-aggregate ingest span.

        Mirrors _ingest_gil's carchar keying → store → grow → accumulate, but
        reads the keying hash (huv), key view and value views straight off the
        CxxMorsel and drives the collectors through borrowed pointers — zero
        Python, GIL released. All allocating prep (hash, scratch resizes, index
        reserve) is done by the caller under the GIL, so the only allocations
        reachable here are the collectors'/key-store's own nogil realloc paths,
        which report failure via False → err. Returns 0 on success."""
        cdef uint32_t* si_buf = self._state_indices_buf.data()
        cdef const uint64_t* khashes = <const uint64_t*>huv.data
        cdef const uint32_t* codes = huv.selection
        cdef int64_t num_groups = self._num_groups
        cdef int64_t state_idx = 0
        cdef Py_ssize_t i, c, kdist
        cdef int64_t* code_state
        cdef bint _is_new
        cdef bint _hot_is_new
        # parvi (low-cardinality) keying state — the SIMD map and its 8-slot
        # repeat cache, ported into the nogil span so low-card GROUP BY is also
        # GIL-free. On overflow it drains into the pre-reserved carchar (nogil)
        # and the morsel finishes on the carchar probe loop.
        cdef uint64_t h
        cdef int cache_slot
        cdef uint64_t cache_keys[8]
        cdef int64_t cache_vals[8]
        cdef uint8_t cache_used[8]
        cdef ParviResult pr

        self._new_row_scratch.clear()

        if self._use_parvi:
            i = 0
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
                pr = self._parvi.insert_new(h, num_groups)
                if pr.found:
                    state_idx = num_groups
                    self._new_row_scratch.push_back(i)
                    self._group_hashes.push_back(h)
                    num_groups += 1
                elif pr.slot == _PARVI_CAPACITY:
                    # Overflow → promote: drain the 16 live entries into the
                    # pre-reserved carchar (nogil, no resize), flip to carchar,
                    # and insert the overflow key as the next new group.
                    if self._telemetry_enabled:
                        self._parvi_final_size = <int64_t>self._parvi.size()
                    self._parvi.drain_into(self._index[0])
                    self._use_parvi = False
                    self._promoted_from_parvi = True
                    state_idx = num_groups
                    _hot_is_new = self._index.find_or_insert_id(h, num_groups, state_idx)
                    self._new_row_scratch.push_back(i)
                    self._group_hashes.push_back(h)
                    num_groups += 1
                else:
                    state_idx = <int64_t>pr.slot
                cache_keys[cache_slot] = h
                cache_vals[cache_slot] = state_idx
                cache_used[cache_slot] = 1
                si_buf[i] = <uint32_t>state_idx
                i += 1
                if not self._use_parvi:
                    break
            # Mid-morsel promotion → finish the remaining rows on carchar.
            if not self._use_parvi:
                while i < n_rows:
                    if i + _AGG_PROBE_PREFETCH < n_rows:
                        self._index.prefetch(khashes[codes[i + _AGG_PROBE_PREFETCH]])
                    _hot_is_new = self._index.find_or_insert_id(khashes[codes[i]], num_groups, state_idx)
                    if _hot_is_new:
                        self._new_row_scratch.push_back(i)
                        self._group_hashes.push_back(khashes[codes[i]])
                        num_groups += 1
                    si_buf[i] = <uint32_t>state_idx
                    i += 1
        elif compressed:
            kdist = <Py_ssize_t>huv.data_length
            code_state = self._code_state.data()
            for c in range(kdist):
                code_state[c] = -1
            for i in range(n_rows):
                c = <Py_ssize_t>codes[i]
                state_idx = code_state[c]
                if state_idx == -1:
                    _is_new = self._index.find_or_insert_id(khashes[c], num_groups, state_idx)
                    if _is_new:
                        self._new_row_scratch.push_back(i)
                        self._group_hashes.push_back(khashes[c])
                        num_groups += 1
                    code_state[c] = state_idx
                si_buf[i] = <uint32_t>state_idx
        else:
            i = 0
            while i < n_rows:
                if i + _AGG_PROBE_PREFETCH < n_rows:
                    self._index.prefetch(khashes[codes[i + _AGG_PROBE_PREFETCH]])
                _hot_is_new = self._index.find_or_insert_id(khashes[codes[i]], num_groups, state_idx)
                if _hot_is_new:
                    self._new_row_scratch.push_back(i)
                    self._group_hashes.push_back(khashes[codes[i]])
                    num_groups += 1
                si_buf[i] = <uint32_t>state_idx
                i += 1

        self._num_groups = num_groups

        cdef Py_ssize_t n_new = <Py_ssize_t>self._new_row_scratch.size()
        cdef const DrakenVector* key_view
        cdef Py_ssize_t jc
        cdef int32_t cidx
        # Collectors are driven through borrowed casts of the stored PyObject*:
        # the inline `(<BaseCollector>ptr).method()` form takes no owned reference
        # (an assignment to a `cdef BaseCollector` would incref → need the GIL).
        if n_new > 0:
            if self._group_col_idxs.size() == 1:
                key_view = &m.columns[self._group_col_idxs[0]].view
                if not self._key_store.store_new_rows_single_view(
                    key_view, self._new_row_scratch.data(), n_new
                ):
                    err.code = 2
                    err.msg = "key store alloc failed"
                    return 2
            else:
                if not self._key_store.store_new_rows_multi_view(
                    m,
                    self._group_col_idxs.data(),
                    <Py_ssize_t>self._group_col_idxs.size(),
                    self._new_row_scratch.data(),
                    n_new,
                ):
                    err.code = 2
                    err.msg = "key store alloc failed"
                    return 2
            for jc in range(<Py_ssize_t>self._collector_ptrs.size()):
                if not (<BaseCollector>self._collector_ptrs[jc]).grow_nogil(num_groups):
                    err.code = 3
                    err.msg = "collector grow alloc failed"
                    return 3

        for jc in range(<Py_ssize_t>self._collector_ptrs.size()):
            cidx = self._collector_col_idxs[jc]
            if cidx >= 0:
                (<BaseCollector>self._collector_ptrs[jc]).accumulate(&m.columns[cidx].view, si_buf, n_rows)
            else:
                (<BaseCollector>self._collector_ptrs[jc]).accumulate(NULL, si_buf, n_rows)

        return 0

    cdef void _ingest_gil(self, Morsel morsel) except *:
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
                            self._group_hashes.push_back(khashes[c])
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
                        self._group_hashes.push_back(h)
                        num_groups += 1
                    elif pr.slot == _PARVI_CAPACITY:
                        state_idx = num_groups
                        # Parvi overflow: drain into carchar and continue seamlessly.
                        self._promote_parvi_to_carchar()
                        self._index.insert_new(h, state_idx)
                        self._new_row_scratch.push_back(i)
                        self._group_hashes.push_back(h)
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
                            self._group_hashes.push_back(khashes[codes[i]])
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
        # Accumulate each collector over this morsel's rows. nogil-capable
        # collectors take a pre-resolved value view (resolved here under the GIL on
        # this fallback path; ingest_cxx resolves it from the CxxMorsel* nogil).
        # `_vtmp` holds the transient Vector alive so its unified() DrakenVector*
        # stays valid for the accumulate call. Complex collectors stay on the GIL
        # accumulate_gil(morsel) path.
        cdef BaseCollector bc
        cdef Vector _vtmp
        for collector in self._collectors:
            bc = <BaseCollector>collector
            if bc._nogil_capable:
                if bc._col_idx >= 0:
                    _vtmp = morsel._get_column(bc._col_idx)
                    bc.accumulate(_vtmp.unified(), si_buf, n_rows)
                else:
                    bc.accumulate(NULL, si_buf, n_rows)
            else:
                bc.accumulate_gil(morsel, si_buf, n_rows)
        if self._telemetry_enabled:
            self._time_accumulate_ns += _now_ns() - phase_start

        if self._telemetry_enabled:
            self._time_resolve_ns += _now_ns() - start_ns

    cpdef bint is_mergeable(self):
        """True iff every collector supports partition-parallel merge
        (COUNT / SUM / MIN / MAX / AVG over int & float). DECIMAL/string/bool/
        interval MIN-MAX, decimal SUM/AVG, COUNT DISTINCT and MEDIAN report
        is_mergeable()==False, so an engine containing any of them must stay
        serial. Collectors are concrete (resolved) by the time merge() runs."""
        cdef Py_ssize_t i
        for i in range(len(self._collectors)):
            if not (<BaseCollector>self._collectors[i]).is_mergeable():
                return False
        return True

    cpdef void merge(self, GroupHashEngine other) except *:
        """WP-7: combine another engine's per-group partial accumulators into this
        one. Both engines must come from the same plan over disjoint input
        partitions (same group columns, positionally-matching collectors). After
        merge, this engine's finalize() yields the exact total — identical to a
        single engine over all rows.

        Algorithm (per-group state transplant):
          1. Reconstruct `other`'s group keys into a one-row-per-group morsel and
             hash them with the SAME hash_keys used at ingest — deterministic per
             key value, so an identical key collides with this engine's matching
             group. (Reconstruct consumes other's KeyStore; other is spent.)
          2. find_or_insert each hash into this engine's index, building an
             other_group -> self_group slot map and the list of keys new to self.
          3. store_new_rows() copies the new group keys into this KeyStore; grow
             collectors to the new group count.
          4. merge_group_state() transplants each collector's per-group state via
             the slot map.
        """
        from draken.morsels.morsel import Morsel as _Morsel

        cdef Py_ssize_t n_collectors = len(self._collectors)
        if n_collectors != <Py_ssize_t>len(other._collectors):
            raise ValueError(
                f"cannot merge engines with {n_collectors} vs "
                f"{len(other._collectors)} collectors"
            )

        # Empty other — nothing to combine (even if it held a non-mergeable
        # collector, there is no partial state to merge).
        if not other._resolved or other._num_groups == 0:
            return

        # other is resolved → its collectors are concrete. Refuse loud if any is
        # non-mergeable rather than silently producing a wrong answer.
        if not other.is_mergeable():
            raise NotImplementedError(
                "grouped engine contains a non-mergeable collector "
                "(COUNT DISTINCT / MEDIAN / decimal / string / bool / interval)"
            )

        cdef Py_ssize_t i
        cdef Py_ssize_t j
        cdef int64_t n_other = other._num_groups

        # This engine never ingested (e.g. an empty partition) — adopt other's
        # resolved state wholesale via a clean ownership move, then return.
        if not self._resolved:
            self._index = other._index
            other._index = NULL
            self._parvi = other._parvi
            other._parvi = NULL
            self._use_parvi = other._use_parvi
            self._promoted_from_parvi = other._promoted_from_parvi
            self._parvi_final_size = other._parvi_final_size
            self._key_store = other._key_store
            other._key_store = None
            self._collectors = other._collectors
            other._collectors = []
            self._key_kinds = other._key_kinds
            self._num_groups = n_other
            self._resolved = True
            # Adopt other's per-group hashes too so the merged engine remains
            # eligible for a subsequent nogil merge as the base.
            self._group_hashes = other._group_hashes
            other._group_hashes.clear()
            # Adopt the nogil-ingest wiring (collector ptrs / col idxs / group col
            # idxs / _all_nogil) so the adopted collectors are reachable via the
            # borrowed-pointer nogil paths on this engine.
            self._group_col_idxs = other._group_col_idxs
            self._collector_ptrs = other._collector_ptrs
            self._collector_col_idxs = other._collector_col_idxs
            self._all_nogil = other._all_nogil
            other._num_groups = 0
            other._resolved = False
            return

        if not self.is_mergeable():
            raise NotImplementedError(
                "grouped engine contains a non-mergeable collector "
                "(COUNT DISTINCT / MEDIAN / decimal / string / bool / interval)"
            )

        # The <Type>other cast inside merge_group_state is UNCHECKED — verify the
        # collector types line up first (once per collector, not per group) so a
        # misconfigured plan fails loud instead of reading the wrong struct layout.
        for i in range(n_collectors):
            if type(self._collectors[i]) is not type(other._collectors[i]):
                raise ValueError(
                    f"collector type mismatch at position {i}: "
                    f"{type(self._collectors[i]).__name__} vs "
                    f"{type(other._collectors[i]).__name__}"
                )

        cdef int64_t num_groups = self._num_groups
        cdef BaseCollector sc
        cdef BaseCollector oc
        cdef int64_t g

        # Zero group columns (GROUP BY all-constant literal) — a single implicit
        # group; there is no key material to hash or store. Transplant slot 0.
        if <Py_ssize_t>len(self._group_columns) == 0:
            if num_groups == 0:
                num_groups = 1
                for j in range(n_collectors):
                    (<BaseCollector>self._collectors[j]).grow(num_groups)
                self._num_groups = num_groups
            for j in range(n_collectors):
                sc = <BaseCollector>self._collectors[j]
                oc = <BaseCollector>other._collectors[j]
                for g in range(n_other):
                    sc.merge_group_state(oc, g, 0)
            return

        # ---- 1. Reconstruct other's keys (consumes other's KeyStore) ----
        cdef list key_names = []
        cdef list key_vecs = []
        other._key_store.reconstruct_vectors(n_other, key_names, key_vecs)
        # Address the keys morsel by this engine's group columns exactly as the
        # ingest path does, so hash_keys() and store_new_rows() agree on columns.
        keys_morsel = _Morsel.from_vectors(self._group_columns, key_vecs)

        cdef object hv = keys_morsel.hash_keys(self._group_columns)
        cdef DrakenVector* huv = (<Vector>hv).unified()
        cdef const uint64_t* khashes = <const uint64_t*>huv.data
        cdef const uint32_t* codes = huv.selection

        # ---- 2. Map each other-group to a self-group slot ----
        cdef vector[int64_t] self_slot
        self_slot.resize(<size_t>n_other)
        cdef vector[int64_t] new_rows
        new_rows.reserve(<size_t>n_other)

        cdef uint64_t h
        cdef int64_t slot
        cdef bint is_new
        cdef ParviResult pr

        if not self._use_parvi:
            # Reserve worst-case (every other-group a new self-group) so
            # find_or_insert_id's internal resize cannot fire below.
            self._index.reserve(<size_t>(num_groups + n_other))

        # Keep _group_hashes in lockstep on this GIL merge path too, so a base
        # combined via the GIL dual interface stays composable as `other` in a
        # later nogil merge (whose size invariant would otherwise fail loud).
        self._group_hashes.reserve(<size_t>(num_groups + n_other))
        for g in range(n_other):
            h = khashes[codes[g]]
            if self._use_parvi:
                pr = self._parvi.insert_new(h, num_groups)
                if pr.found:
                    slot = num_groups
                    new_rows.push_back(g)
                    self._group_hashes.push_back(h)
                    num_groups += 1
                elif pr.slot == _PARVI_CAPACITY:
                    slot = num_groups
                    self._promote_parvi_to_carchar()
                    self._index.insert_new(h, slot)
                    new_rows.push_back(g)
                    self._group_hashes.push_back(h)
                    num_groups += 1
                    # Promoted mid-loop: reserve the worst-case carchar tail.
                    self._index.reserve(<size_t>(num_groups + (n_other - g)))
                else:
                    slot = <int64_t>pr.slot
            else:
                is_new = self._index.find_or_insert_id(h, num_groups, slot)
                if is_new:
                    new_rows.push_back(g)
                    self._group_hashes.push_back(h)
                    num_groups += 1
            self_slot[g] = slot

        # ---- 3. Store new group keys; grow collectors ----
        cdef Py_ssize_t n_new = <Py_ssize_t>new_rows.size()
        if n_new > 0:
            self._key_store.store_new_rows(keys_morsel, new_rows.data(), n_new)
        for j in range(n_collectors):
            (<BaseCollector>self._collectors[j]).grow(num_groups)
        self._num_groups = num_groups

        # ---- 4. Transplant per-group collector state ----
        for j in range(n_collectors):
            sc = <BaseCollector>self._collectors[j]
            oc = <BaseCollector>other._collectors[j]
            for g in range(n_other):
                sc.merge_group_state(oc, g, self_slot[g])

    cpdef bint is_resolved(self):
        """True once the engine has seen its first non-empty morsel (collectors
        concrete, key store built). A fresh base is not resolved; the GIL-free
        merge requires a resolved base, so the first partial into an empty base
        must go through the adopt-wholesale merge()."""
        return self._resolved

    cpdef bint is_mergeable_nogil(self):
        """True iff this engine can be combined via the GIL-free merge_nogil path:
        a single group column AND every collector has a real nogil transplant.
        Multi-column / non-nogil collectors fall back to the GIL merge — a genuine
        dual interface (capability), not a perf gate."""
        if <Py_ssize_t>len(self._group_columns) != 1:
            return False
        cdef Py_ssize_t i
        for i in range(len(self._collectors)):
            if not (<BaseCollector>self._collectors[i]).is_mergeable_nogil():
                return False
        return True

    # Scratch reused across merge_nogil calls on the SAME base engine (the merge
    # target). Sized under the GIL before each nogil span; never shared between
    # concurrently-merging engines (each radix bin has its own base engine).
    cdef vector[int64_t] _merge_self_slot
    cdef vector[int64_t] _merge_new_rows

    cpdef int merge_nogil_driver(self, GroupHashEngine other) except -1:
        """GIL-side driver for the GIL-free per-group merge.

        Validates the dual-interface preconditions, hoists every allocating /
        throwing step (type checks, index reserve, scratch + group-hash reserve)
        under the GIL, then runs the pure-C++ transplant with the GIL RELEASED.
        Returns 1 if the nogil merge ran, 0 if there was nothing to merge.
        Raises (NotImplementedError / ValueError / MemoryError) on a misuse or
        an allocation failure — never silently degrades.

        This is the method a parallel scheduler / the dev prototype submits per
        radix bin; B independent bins each call base.merge_nogil_driver(worker_b)
        with no shared state, so the only serialization left is whatever the
        Python call layer adds — the transplant itself is GIL-free."""
        cdef Py_ssize_t n_collectors = len(self._collectors)
        if n_collectors != <Py_ssize_t>len(other._collectors):
            raise ValueError(
                f"cannot merge engines with {n_collectors} vs "
                f"{len(other._collectors)} collectors"
            )
        if not other._resolved or other._num_groups == 0:
            return 0
        if not self._resolved:
            raise NotImplementedError(
                "merge_nogil_driver requires a resolved base engine; use merge() "
                "for the adopt-empty-base case"
            )
        if (not self.is_mergeable_nogil()) or (not other.is_mergeable_nogil()):
            raise NotImplementedError(
                "merge_nogil_driver requires single-column key + all-nogil "
                "collectors; use merge() for the GIL dual interface"
            )
        if self._use_parvi:
            raise NotImplementedError(
                "merge_nogil_driver requires the base engine on the carchar map "
                "(parvi base not supported on the nogil merge path)"
            )
        # other must carry one stored hash per group (the nogil-merge ingest
        # invariant). If it ingested via a path that did not record them, refuse
        # loud rather than read a short vector.
        if <int64_t>other._group_hashes.size() != other._num_groups:
            raise NotImplementedError(
                "merge_nogil_driver: other engine has no per-group hashes "
                "(was it ingested off the nogil-merge path?)"
            )
        # The <Type>other cast in merge_group_state_nogil is UNCHECKED — verify
        # collector types line up once here, under the GIL, before the span.
        cdef Py_ssize_t i
        for i in range(n_collectors):
            if type(self._collectors[i]) is not type(other._collectors[i]):
                raise ValueError(
                    f"collector type mismatch at position {i}: "
                    f"{type(self._collectors[i]).__name__} vs "
                    f"{type(other._collectors[i]).__name__}"
                )

        cdef int64_t n_other = other._num_groups
        # Hoist all allocation under the GIL: index worst-case reserve, the slot
        # map + new-row scratch, and the group-hash tail. After this the nogil
        # span touches only the collectors'/key-store's own nogil realloc paths
        # (which report failure via False), so no std::bad_alloc can escape the
        # noexcept-nogil body.
        self._index.reserve(<size_t>(self._num_groups + n_other))
        self._merge_self_slot.resize(<size_t>n_other)
        self._merge_new_rows.clear()
        self._merge_new_rows.reserve(<size_t>n_other)
        self._group_hashes.reserve(<size_t>(self._num_groups + n_other))

        cdef ErrCtx err
        err.code = 0
        err.msg = NULL
        cdef int rc
        with nogil:
            rc = self._merge_nogil(other, &err)
        if rc != 0:
            raise MemoryError("merge_nogil failed (alloc)")
        return 1

    cdef int _merge_nogil(self, GroupHashEngine other, ErrCtx* err) noexcept nogil:
        """GIL-free per-group transplant. Reads other's stored per-group hashes
        and final-form group keys directly — no reconstruct, no rehash, no Python.
        All allocating prep is hoisted by merge_nogil_driver under the GIL, so the
        only allocations reachable here are nogil realloc paths reporting via
        False. Returns 0 on success, non-zero (+ err set) on alloc failure."""
        cdef int64_t n_other = other._num_groups
        cdef int64_t num_groups = self._num_groups
        cdef int64_t* self_slot = self._merge_self_slot.data()
        cdef const uint64_t* o_hashes = other._group_hashes.data()
        cdef int64_t g
        cdef int64_t slot
        cdef uint64_t h
        cdef bint is_new

        self._merge_new_rows.clear()

        # ---- 1. Map each other-group to a self-group slot (carchar probe) ----
        for g in range(n_other):
            h = o_hashes[g]
            is_new = self._index.find_or_insert_id(h, num_groups, slot)
            if is_new:
                self._merge_new_rows.push_back(g)
                self._group_hashes.push_back(h)
                num_groups += 1
            self_slot[g] = slot

        cdef Py_ssize_t n_new = <Py_ssize_t>self._merge_new_rows.size()

        # ---- 2. Copy new group keys (keystore → keystore, nogil) ----
        if n_new > 0:
            if not self._key_store.append_groups_from_single(
                other._key_store, self._merge_new_rows.data(), n_new
            ):
                err.code = 2
                err.msg = "merge key copy alloc failed"
                return 2
            # ---- 3. Grow collectors to the new group count ----
            for g in range(<Py_ssize_t>self._collector_ptrs.size()):
                if not (<BaseCollector>self._collector_ptrs[g]).grow_nogil(num_groups):
                    err.code = 3
                    err.msg = "merge collector grow alloc failed"
                    return 3

        self._num_groups = num_groups

        # ---- 4. Transplant per-group collector state ----
        cdef Py_ssize_t jc
        cdef Py_ssize_t n_collectors = <Py_ssize_t>self._collector_ptrs.size()
        for jc in range(n_collectors):
            for g in range(n_other):
                (<BaseCollector>self._collector_ptrs[jc]).merge_group_state_nogil(
                    <BaseCollector>other._collector_ptrs[jc], g, self_slot[g]
                )
        return 0

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
            "nogil_ingest_morsels": self.nogil_ingest_morsels,
            "gil_ingest_morsels": self.gil_ingest_morsels,
        }
