# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# KeyStore — per-group byte-encoded key storage and reconstruction.
#
# store_new_rows()        — hot path; no Python/isinstance in inner loop.
#                           Multi-column path pre-computes dispatch codes and
#                           raw C pointers once per morsel to avoid isinstance().
#
# reconstruct_vectors()   — finalize path; writes directly into owned Draken
#                           buffers.  No Python list/object churn, no pyarrow.
#   Fixed columns  -> Int64Vector backed by alloc_fixed_buffer().
#   String columns -> StringVectorBuilder.append_bytes() (no Python str objects).

from libc.string cimport memset
from libc.stdint cimport int64_t, uint8_t

from libcpp.string cimport string
from libcpp.vector cimport vector

from opteryx.compiled.draken.core.buffers cimport DrakenFixedBuffer, DrakenVarBuffer, DrakenType
from opteryx.compiled.draken.core.fixed_vector cimport alloc_fixed_buffer, free_fixed_buffer
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector


# ---------------------------------------------------------------------------
# Dispatch codes for multi-column store_new_rows (replaces isinstance in loop)
# ---------------------------------------------------------------------------
cdef int _DISPATCH_INT64   = 0
cdef int _DISPATCH_BOOL    = 1
cdef int _DISPATCH_FLOAT64 = 2
cdef int _DISPATCH_STRING  = 3


# ---------------------------------------------------------------------------
# Null-bitmap helper (re-declared here for .pxi locality)
# ---------------------------------------------------------------------------
cdef inline bint _ks_bitmap_is_valid(uint8_t* bitmap, Py_ssize_t index) noexcept nogil:
    if bitmap == NULL:
        return True
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


# ---------------------------------------------------------------------------
# Module-level reconstruction helpers for single-column paths.
# Defined before KeyStore so there is no forward-reference issue.
# ---------------------------------------------------------------------------

cdef Int64Vector _recon_single_fixed(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    int64_t num_groups,
    Py_ssize_t nb_size,
) except *:
    """
    Decode a single fixed-width key column directly into an Int64Vector.

    The output vector owns its data (allocated via alloc_fixed_buffer).
    A null bitmap is malloc'd only when at least one null group is encountered;
    ownership is transferred to iv.ptr.null_bitmap so free_fixed_buffer frees it.
    """
    cdef Int64Vector iv = Int64Vector(<size_t>num_groups)
    cdef int64_t* i64_data = <int64_t*>iv.ptr.data
    cdef uint8_t* nbits = NULL
    cdef bint any_null = False
    cdef int64_t val, valid_flag
    cdef Py_ssize_t gi

    for gi in range(num_groups):
        _decode_single_fixed_key_record(payload_bytes, payload_offsets, gi, &val, &valid_flag)
        i64_data[gi] = val
        if not valid_flag:
            if not any_null:
                nbits = <uint8_t*>malloc(nb_size)
                if nbits == NULL:
                    raise MemoryError()
                memset(nbits, 0xFF, nb_size)
                any_null = True
            nbits[gi >> 3] &= ~(1 << (gi & 7))

    if any_null:
        # free_fixed_buffer() will call free(iv.ptr.null_bitmap) in __dealloc__
        iv.ptr.null_bitmap = nbits

    return iv


cdef StringVector _recon_single_string(
    const vector[uint8_t]& payload_bytes,
    const vector[int64_t]& payload_offsets,
    int64_t num_groups,
) except *:
    """
    Decode a single string key column directly into a StringVector.

    Two-pass strategy (both passes read from the compact offset+byte store):
      Pass 1 — sum byte lengths for exact builder capacity.
      Pass 2 — fill builder with raw bytes or null entries.

    StringVectorBuilder.append_bytes() takes a raw (char*, len) pair — no
    Python str object is created at any point.
    """
    cdef Py_ssize_t gi
    cdef int64_t valid_flag
    cdef string enc_str
    cdef Py_ssize_t total_bytes = 0

    # Pass 1: measure total byte content
    for gi in range(num_groups):
        _decode_single_encoded_key_record(payload_bytes, payload_offsets, gi, enc_str, &valid_flag)
        if valid_flag:
            total_bytes += <Py_ssize_t>enc_str.size()

    # Pass 2: build the vector with exact capacity (resizable=True as a safety net)
    cdef StringVectorBuilder builder = StringVectorBuilder(
        <Py_ssize_t>num_groups, total_bytes, True,
    )
    for gi in range(num_groups):
        _decode_single_encoded_key_record(payload_bytes, payload_offsets, gi, enc_str, &valid_flag)
        if valid_flag:
            builder.append_bytes(enc_str.data(), <Py_ssize_t>enc_str.size())
        else:
            builder.append_null()

    return builder.finish()


# ---------------------------------------------------------------------------
# KeyStore
# ---------------------------------------------------------------------------

cdef class KeyStore:
    """
    Stores the group-key values for new groups in a compact byte representation.

    store_new_rows() is called once per morsel (not per row) after the ingest
    loop has identified which rows introduce new groups.  It extracts key data
    directly from Draken C-level buffers — no to_pylist(), no Python objects.
    Multi-column keys use pre-computed dispatch codes to eliminate isinstance()
    from the per-row inner loop.

    reconstruct_vectors() is called once during finalize.  It writes directly
    into owned Draken buffers — no Python list/object churn, no pyarrow.
    """

    cdef list _group_columns          # list[bytes|str] — read at init only
    cdef vector[int64_t] _key_kinds   # KEY_MULTI_FIXED_* or KEY_MULTI_ENCODED_STRING per column
    cdef Py_ssize_t _n_cols
    cdef vector[uint8_t] _bytes       # flat concatenated encoded key bytes
    cdef vector[int64_t] _offsets     # one int64 offset per group (+ sentinel)

    def __cinit__(self, list group_columns, list key_kinds):
        self._group_columns = group_columns
        self._n_cols = len(group_columns)
        cdef Py_ssize_t i
        for i in range(len(key_kinds)):
            self._key_kinds.push_back(<int64_t>key_kinds[i])
        # First offset sentinel: byte stream starts at 0
        self._offsets.push_back(0)

    # ------------------------------------------------------------------
    # store_new_rows — hot path, called once per morsel
    # ------------------------------------------------------------------

    cdef void store_new_rows(
        self,
        object morsel,            # Morsel
        const int64_t* row_indices,
        Py_ssize_t n_new,
    ) except *:
        """
        Encode group keys for new groups and append to the byte store.

        Single-column path: specialised per vector type; no isinstance in loop.
        Multi-column path: dispatch codes + raw C pointers pre-computed once per
        morsel; the inner per-row loop contains only integer comparisons.
        """
        if n_new == 0:
            return

        cdef Py_ssize_t col_idx, ri, row_idx
        cdef int64_t key_kind
        cdef list vecs
        cdef Vector vec
        cdef Int64Vector iv
        cdef Float64Vector fv
        cdef StringVector sv
        cdef BoolVector bv
        cdef int64_t* i64_data
        cdef uint8_t* bool_data
        cdef uint8_t* raw_bool_data
        cdef uint8_t* nulls
        cdef DrakenVarBuffer* vbuf
        cdef int64_t int_val
        cdef int64_t valid_flag
        cdef const char* str_ptr
        cdef Py_ssize_t str_len
        cdef int64_t const_i64
        cdef uint8_t const_bool
        cdef uint8_t* bool_nulls

        # Accumulators for multi-column record encoding
        cdef vector[int64_t] fixed_values
        cdef vector[int64_t] fixed_valids
        cdef vector[string]  encoded_values
        cdef vector[int64_t] encoded_valids
        cdef string enc_str

        # Multi-column pre-computed dispatch
        cdef vector[int]    col_dispatch
        cdef vector[size_t] col_null_ptrs
        cdef vector[size_t] col_dense_ptrs
        cdef vector[size_t] col_varbuf_ptrs
        cdef vector[bint]   col_has_const
        cdef vector[int64_t] col_const_vals
        cdef int disp

        if self._n_cols == 1:
            # ----------------------------------------------------------------
            # Single-column fast paths — statically dispatched
            # ----------------------------------------------------------------
            key_kind = self._key_kinds[0]
            vec = morsel.column(self._group_columns[0])
            nulls = vec.null_bitmap_ptr()

            if key_kind == KEY_MULTI_ENCODED_STRING:
                sv = <StringVector>vec
                vbuf = sv.ptr
                for ri in range(n_new):
                    row_idx = row_indices[ri]
                    valid_flag = 1 if _ks_bitmap_is_valid(nulls, row_idx) else 0
                    if valid_flag and vbuf != NULL:
                        str_ptr = <const char*>(vbuf.data + vbuf.offsets[row_idx])
                        str_len  = vbuf.offsets[row_idx + 1] - vbuf.offsets[row_idx]
                    else:
                        str_ptr = NULL
                        str_len  = 0
                    _append_single_encoded_key_record(
                        self._bytes, self._offsets, str_ptr, str_len, valid_flag,
                    )

            elif isinstance(vec, Int64Vector):
                iv = <Int64Vector>vec
                if iv._has_const:
                    const_i64 = iv._const_value
                    for ri in range(n_new):
                        row_idx   = row_indices[ri]
                        valid_flag = 1 if _ks_bitmap_is_valid(nulls, row_idx) else 0
                        int_val   = const_i64 if valid_flag else 0
                        _append_single_fixed_key_record(
                            self._bytes, self._offsets, int_val, valid_flag,
                        )
                else:
                    i64_data = <int64_t*>iv.dense_ptr()
                    for ri in range(n_new):
                        row_idx   = row_indices[ri]
                        valid_flag = 1 if _ks_bitmap_is_valid(nulls, row_idx) else 0
                        int_val   = i64_data[row_idx] if valid_flag else 0
                        _append_single_fixed_key_record(
                            self._bytes, self._offsets, int_val, valid_flag,
                        )

            elif isinstance(vec, BoolVector):
                bv = <BoolVector>vec
                # BoolVector.null_bitmap_ptr() returns NULL (base impl); use ptr.null_bitmap
                bool_nulls = bv.ptr.null_bitmap
                if bv._has_const:
                    const_bool = bv._const_value
                    for ri in range(n_new):
                        row_idx   = row_indices[ri]
                        valid_flag = 1 if _ks_bitmap_is_valid(bool_nulls, row_idx) else 0
                        int_val   = <int64_t>const_bool if valid_flag else 0
                        _append_single_fixed_key_record(
                            self._bytes, self._offsets, int_val, valid_flag,
                        )
                else:
                    bool_data = <uint8_t*>bv.ptr.data
                    for ri in range(n_new):
                        row_idx   = row_indices[ri]
                        valid_flag = 1 if _ks_bitmap_is_valid(bool_nulls, row_idx) else 0
                        int_val   = <int64_t>((bool_data[row_idx >> 3] >> (row_idx & 7)) & 1) if valid_flag else 0
                        _append_single_fixed_key_record(
                            self._bytes, self._offsets, int_val, valid_flag,
                        )

            else:
                # Float64Vector and other fixed-width types — store as raw int64 bits
                fv = <Float64Vector>vec
                i64_data = <int64_t*>fv.dense_ptr()
                for ri in range(n_new):
                    row_idx   = row_indices[ri]
                    valid_flag = 1 if _ks_bitmap_is_valid(nulls, row_idx) else 0
                    int_val   = i64_data[row_idx] if valid_flag else 0
                    _append_single_fixed_key_record(
                        self._bytes, self._offsets, int_val, valid_flag,
                    )

        else:
            # ----------------------------------------------------------------
            # Multi-column path
            # ----------------------------------------------------------------
            # Pre-fetch all column vectors once (Python call, outside inner loop).
            vecs = [morsel.column(self._group_columns[col_idx]) for col_idx in range(self._n_cols)]

            # Pre-compute per-column dispatch codes + raw C pointers.
            # This runs once per morsel and eliminates isinstance() from the
            # inner row loop, replacing it with cheap integer comparisons.
            col_dispatch.resize(self._n_cols)
            col_null_ptrs.resize(self._n_cols, 0)
            col_dense_ptrs.resize(self._n_cols, 0)
            col_varbuf_ptrs.resize(self._n_cols, 0)
            col_has_const.resize(self._n_cols, False)
            col_const_vals.resize(self._n_cols, 0)

            for col_idx in range(self._n_cols):
                key_kind = self._key_kinds[col_idx]
                vec = vecs[col_idx]

                if key_kind == KEY_MULTI_ENCODED_STRING:
                    col_dispatch[col_idx]   = _DISPATCH_STRING
                    sv = <StringVector>vec
                    col_null_ptrs[col_idx]  = <size_t>sv.null_bitmap_ptr()
                    col_varbuf_ptrs[col_idx] = <size_t>sv.ptr

                elif isinstance(vec, Int64Vector):
                    col_dispatch[col_idx] = _DISPATCH_INT64
                    iv = <Int64Vector>vec
                    col_null_ptrs[col_idx] = <size_t>iv.null_bitmap_ptr()
                    if iv._has_const:
                        col_has_const[col_idx] = True
                        col_const_vals[col_idx] = iv._const_value
                    else:
                        col_dense_ptrs[col_idx] = <size_t>iv.dense_ptr()

                elif isinstance(vec, BoolVector):
                    col_dispatch[col_idx] = _DISPATCH_BOOL
                    bv = <BoolVector>vec
                    # BoolVector: null bitmap lives at ptr.null_bitmap, not null_bitmap_ptr()
                    col_null_ptrs[col_idx] = <size_t>bv.ptr.null_bitmap
                    if bv._has_const:
                        col_has_const[col_idx] = True
                        col_const_vals[col_idx] = <int64_t>bv._const_value
                    else:
                        col_dense_ptrs[col_idx] = <size_t>bv.ptr.data

                else:
                    # Float64Vector and other fixed-width types
                    col_dispatch[col_idx] = _DISPATCH_FLOAT64
                    fv = <Float64Vector>vec
                    col_null_ptrs[col_idx] = <size_t>fv.null_bitmap_ptr()
                    col_dense_ptrs[col_idx] = <size_t>fv.dense_ptr()

            # Inner loop: static dispatch only (no isinstance, no Python calls)
            for ri in range(n_new):
                row_idx = row_indices[ri]
                fixed_values.clear()
                fixed_valids.clear()
                encoded_values.clear()
                encoded_valids.clear()

                for col_idx in range(self._n_cols):
                    disp       = col_dispatch[col_idx]
                    nulls      = <uint8_t*>col_null_ptrs[col_idx]
                    valid_flag = 1 if _ks_bitmap_is_valid(nulls, row_idx) else 0

                    if disp == _DISPATCH_STRING:
                        vbuf = <DrakenVarBuffer*>col_varbuf_ptrs[col_idx]
                        if valid_flag and vbuf != NULL:
                            enc_str.assign(
                                <const char*>(vbuf.data + vbuf.offsets[row_idx]),
                                vbuf.offsets[row_idx + 1] - vbuf.offsets[row_idx],
                            )
                        else:
                            enc_str.clear()
                        encoded_values.push_back(enc_str)
                        encoded_valids.push_back(valid_flag)

                    elif disp == _DISPATCH_INT64:
                        if col_has_const[col_idx]:
                            int_val = col_const_vals[col_idx] if valid_flag else 0
                        else:
                            i64_data = <int64_t*>col_dense_ptrs[col_idx]
                            int_val  = i64_data[row_idx] if valid_flag else 0
                        fixed_values.push_back(int_val)
                        fixed_valids.push_back(valid_flag)

                    elif disp == _DISPATCH_BOOL:
                        if col_has_const[col_idx]:
                            int_val = col_const_vals[col_idx] if valid_flag else 0
                        else:
                            raw_bool_data = <uint8_t*>col_dense_ptrs[col_idx]
                            int_val = <int64_t>((raw_bool_data[row_idx >> 3] >> (row_idx & 7)) & 1) if valid_flag else 0
                        fixed_values.push_back(int_val)
                        fixed_valids.push_back(valid_flag)

                    else:  # _DISPATCH_FLOAT64 and other fixed-width
                        i64_data = <int64_t*>col_dense_ptrs[col_idx]
                        int_val  = i64_data[row_idx] if valid_flag else 0
                        fixed_values.push_back(int_val)
                        fixed_valids.push_back(valid_flag)

                _append_multi_key_record(
                    self._bytes, self._offsets,
                    fixed_values, fixed_valids,
                    encoded_values, encoded_valids,
                )

    # ------------------------------------------------------------------
    # reconstruct_vectors — finalize path, called once
    # ------------------------------------------------------------------

    cdef void reconstruct_vectors(
        self,
        int64_t num_groups,
        list out_names,
        list out_vecs,
    ) except *:
        """
        Decode stored group keys directly into Draken Vectors.

        Fixed columns  → Int64Vector with owned malloc'd buffer + optional null
                         bitmap; zero Python int objects allocated.
        String columns → StringVector built via StringVectorBuilder.append_bytes();
                         zero Python str objects; no pyarrow conversion.

        Single-column paths delegate to module-level helpers (_recon_single_fixed /
        _recon_single_string) which are specialised and tightly bounded.

        Multi-column path pre-allocates one vector per column, then fills all of
        them in a single decode loop.
        """
        cdef Py_ssize_t gi, col_idx, fidx, eidx
        cdef Py_ssize_t n_fixed = 0, n_encoded = 0
        cdef int64_t key_kind
        cdef int64_t val, valid_flag
        cdef string enc_str
        cdef Py_ssize_t nb_size = (num_groups + 7) // 8
        cdef object col_name

        # ---- Single-column fast paths ----
        if self._n_cols == 1:
            key_kind = self._key_kinds[0]
            col_name = self._group_columns[0]
            out_names.append(col_name.decode("utf-8") if isinstance(col_name, bytes) else col_name)

            if key_kind == KEY_MULTI_ENCODED_STRING:
                out_vecs.append(_recon_single_string(self._bytes, self._offsets, num_groups))
            else:
                out_vecs.append(_recon_single_fixed(self._bytes, self._offsets, num_groups, nb_size))
            return

        # ---- Multi-column path ----
        cdef vector[int64_t] fixed_values
        cdef vector[int64_t] fixed_valids
        cdef vector[string]  encoded_values
        cdef vector[int64_t] encoded_valids

        # Count column kinds
        for col_idx in range(self._n_cols):
            if self._key_kinds[col_idx] == KEY_MULTI_ENCODED_STRING:
                n_encoded += 1
            else:
                n_fixed += 1

        fixed_values.resize(n_fixed)
        fixed_valids.resize(n_fixed)
        encoded_values.resize(n_encoded)
        encoded_valids.resize(n_encoded)

        # Allocate one output vector per column up-front.
        # Fixed  → Int64Vector (owns its data buffer via alloc_fixed_buffer).
        # String → StringVectorBuilder (resizable, estimated capacity).
        cdef list fixed_iv_list      = []   # Int64Vector per fixed column
        cdef list str_builder_list   = []   # StringVectorBuilder per string column
        # Raw data/bitmap pointers stored as size_t for efficient inner-loop access
        cdef vector[size_t] fixed_data_ptrs    # int64_t* data per fixed column
        cdef vector[size_t] fixed_bitmap_ptrs  # uint8_t* null bitmap per fixed column
        cdef vector[bint]   fixed_any_null     # tracks whether each fixed col has a null

        cdef Int64Vector _alloc_iv
        cdef uint8_t*    _alloc_nbits
        for col_idx in range(self._n_cols):
            if self._key_kinds[col_idx] == KEY_MULTI_ENCODED_STRING:
                # Estimate ~8 bytes/string; builder reallocs if needed (resizable=True)
                b = StringVectorBuilder(
                    <Py_ssize_t>num_groups,
                    max(<Py_ssize_t>num_groups * 8, 64),
                    True,
                )
                str_builder_list.append(b)
            else:
                _alloc_iv = Int64Vector(<size_t>num_groups)
                fixed_iv_list.append(_alloc_iv)
                fixed_data_ptrs.push_back(<size_t><int64_t*>_alloc_iv.ptr.data)
                # Bitmap: all-valid (0xFF) until a null is found
                _alloc_nbits = <uint8_t*>malloc(nb_size)
                if _alloc_nbits == NULL:
                    raise MemoryError()
                memset(_alloc_nbits, 0xFF, nb_size)
                fixed_bitmap_ptrs.push_back(<size_t>_alloc_nbits)
                fixed_any_null.push_back(False)

        # Decode all groups, writing each value directly into the pre-allocated buffers
        cdef StringVectorBuilder _sv_builder
        cdef Int64Vector         _fixed_iv
        cdef int64_t*            _i64_ptr
        cdef uint8_t*            _nbits_ptr

        for gi in range(num_groups):
            _decode_multi_key_record(
                self._bytes, self._offsets, gi,
                fixed_values, fixed_valids,
                encoded_values, encoded_valids,
            )
            fidx = 0
            eidx = 0
            for col_idx in range(self._n_cols):
                if self._key_kinds[col_idx] == KEY_MULTI_ENCODED_STRING:
                    _sv_builder = <StringVectorBuilder>str_builder_list[eidx]
                    if encoded_valids[eidx]:
                        _sv_builder.append_bytes(
                            encoded_values[eidx].data(),
                            <Py_ssize_t>encoded_values[eidx].size(),
                        )
                    else:
                        _sv_builder.append_null()
                    eidx += 1
                else:
                    _i64_ptr      = <int64_t*>fixed_data_ptrs[fidx]
                    _i64_ptr[gi]  = fixed_values[fidx]
                    if not fixed_valids[fidx]:
                        _nbits_ptr = <uint8_t*>fixed_bitmap_ptrs[fidx]
                        _nbits_ptr[gi >> 3] &= ~(1 << (gi & 7))
                        fixed_any_null[fidx] = True
                    fidx += 1

        # Finalise: attach null bitmaps to fixed vectors; call finish() on builders
        fidx = 0
        eidx = 0
        for col_idx in range(self._n_cols):
            col_name = self._group_columns[col_idx]
            out_names.append(col_name.decode("utf-8") if isinstance(col_name, bytes) else col_name)

            if self._key_kinds[col_idx] == KEY_MULTI_ENCODED_STRING:
                _sv_builder = <StringVectorBuilder>str_builder_list[eidx]
                out_vecs.append(_sv_builder.finish())
                eidx += 1
            else:
                _fixed_iv   = <Int64Vector>fixed_iv_list[fidx]
                _nbits_ptr  = <uint8_t*>fixed_bitmap_ptrs[fidx]
                if fixed_any_null[fidx]:
                    # Transfer ownership: free_fixed_buffer() will free this pointer
                    _fixed_iv.ptr.null_bitmap = _nbits_ptr
                else:
                    free(_nbits_ptr)
                out_vecs.append(_fixed_iv)
                fidx += 1
