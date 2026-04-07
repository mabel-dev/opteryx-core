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

from opteryx.compiled.draken.morsels.morsel cimport Morsel


cdef extern from "carchar_index.hpp" namespace "opteryx::carchar":
    cdef cppclass CarcharIndex:
        CarcharIndex(size_t initial_capacity, double load_factor) except +
        void reserve(size_t expected_entries)
        size_t size() const
        bint lookup_fast(uint64_t key, int64_t& payload_ref_out) const
        size_t insert_new(uint64_t key, int64_t payload_ref) except +


cdef double _CARCHAR_LOAD_FACTOR = 0.70
cdef size_t _INITIAL_INDEX_CAPACITY = 256


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
    cdef KeyStore _key_store
    cdef list _collectors           # list[BaseCollector] — not accessed inside the loop
    cdef list _key_kinds            # list[int] — resolved on first morsel
    cdef int64_t _num_groups
    cdef vector[int64_t] _state_indices_buf   # reused per morsel, length == n_rows
    cdef vector[int64_t] _new_row_scratch     # indices of rows that introduced new groups
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
    ):
        self._group_columns = group_columns
        self._collectors = collectors
        self._num_groups = 0
        self._resolved = False
        self._telemetry_enabled = telemetry_enabled
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

    cpdef void set_telemetry_enabled(self, bint enabled):
        self._telemetry_enabled = enabled

    cdef void _resolve_on_first_morsel(self, object morsel):
        """Called once on first non-empty morsel to fix collector types and key kinds."""
        cdef long long start_ns
        if self._telemetry_enabled:
            start_ns = _now_ns()
        resolve_deferred_collectors(
            self._collectors, morsel, self._group_columns, self._key_kinds
        )
        self._key_store = KeyStore(self._group_columns, self._key_kinds)
        self._index = new CarcharIndex(_INITIAL_INDEX_CAPACITY, _CARCHAR_LOAD_FACTOR)
        self._resolved = True
        if self._telemetry_enabled:
            self._time_resolve_ns += _now_ns() - start_ns

    cpdef void ingest(self, object morsel):
        """
        Process one input Morsel. No Python in the inner loop.
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

        if self._telemetry_enabled:
            phase_start = _now_ns()
        # Compute group hashes for the group-by columns
        cdef uint64_t[::1] hashes = morsel.hash(self._group_columns)
        if self._telemetry_enabled:
            self._time_hash_ns += _now_ns() - phase_start

        # Ensure scratch buffers are large enough
        if <Py_ssize_t>self._state_indices_buf.size() < n_rows:
            self._state_indices_buf.resize(n_rows)

        self._new_row_scratch.clear()

        cdef int64_t* si_buf = self._state_indices_buf.data()
        cdef int64_t state_idx
        cdef Py_ssize_t i
        cdef int64_t num_groups = self._num_groups

        if self._telemetry_enabled:
            phase_start = _now_ns()
        for i in range(n_rows):
            if not self._index.lookup_fast(hashes[i], state_idx):
                # New group
                state_idx = num_groups
                self._index.insert_new(hashes[i], state_idx)
                self._new_row_scratch.push_back(i)
                num_groups += 1
            si_buf[i] = state_idx
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

    def finalize_morsels(self, Py_ssize_t chunk_size=65536):
        """
        Generator. Yields result Morsels in chunks.
        Called once after all input morsels have been ingested.
        """
        from opteryx.compiled.draken.morsels.morsel import Morsel as _Morsel

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

        # Build one full-size Morsel, then slice into chunks
        if self._telemetry_enabled:
            t0 = _now_ns()
        all_names = agg_names + key_names
        all_vecs = agg_vecs + key_vecs
        full_morsel = _Morsel.from_vectors(all_names, all_vecs)
        if self._telemetry_enabled:
            self._time_build_morsel_ns += _now_ns() - t0

        cdef Py_ssize_t start = 0
        cdef Py_ssize_t length
        while start < num_groups:
            if self._telemetry_enabled:
                t1 = _now_ns()
            length = min(chunk_size, num_groups - start)
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
        }
