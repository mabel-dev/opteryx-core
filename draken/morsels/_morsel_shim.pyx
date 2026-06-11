# cython: language_level=3
# Cython shim for draken.morsels.morsel — E.24 vtable bridge.

from libc.stdint cimport int32_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, calloc, free
from libc.string cimport memcpy, memset
from libc.stddef cimport size_t

from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF
from libcpp.vector cimport vector

from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector, from_decoded
from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_ARRAY, DRAKEN_INT64

# C-level Py_DECREF: Cython 3 made cpython.ref.Py_DECREF take `object`, which
# would re-INCREF; we need the raw PyObject* form for the C++ column store.
cdef extern from *:
    """static inline void _morsel_decref(PyObject* op) { Py_XDECREF(op); }"""
    void _morsel_decref(PyObject* op)

# Native take over a raw int32 index buffer — no per-row PyObject boxing.
# PyObject* in/out keeps `object` out of the .pyx (CLAUDE.md §3); the returned
# handle is a NEW reference (balanced below in _take_native).
cdef extern from "core/draken_bridge.h":
    PyObject* draken_vector_take_buffer(PyObject* vec_obj,
                                        const int32_t* indices, uint32_t n)
    PyObject* draken_vector_take_with_null_buffer(PyObject* vec_obj,
                                        const int32_t* indices, uint32_t n)


cdef inline Vector _take_native(Vector col, const int32_t* idx, uint32_t n):
    """Gather rows from `col` at the typed indices, fully native (no boxed list)."""
    cdef PyObject* raw = draken_vector_take_buffer(<PyObject*>col._nb, idx, n)
    if raw == NULL:
        raise RuntimeError("draken_vector_take_buffer failed")
    cdef object nb_obj = <object>raw   # Cython incref → refcount = 2
    _morsel_decref(raw)                # balance the NEW ref → refcount = 1
    return Vector(nb_obj)


cdef inline Vector _take_native_with_null(Vector col, const int32_t* idx, uint32_t n):
    """Gather rows from `col`; index < 0 yields a NULL output row. Native."""
    cdef PyObject* raw = draken_vector_take_with_null_buffer(<PyObject*>col._nb, idx, n)
    if raw == NULL:
        raise RuntimeError("draken_vector_take_with_null_buffer failed")
    cdef object nb_obj = <object>raw
    _morsel_decref(raw)
    return Vector(nb_obj)

# C++ hash functions for the cdef c_hash method. draken_hash dispatches per
# DrakenType; simd_mix_hash mixes per-column hashes into a running buffer.
# draken_hash lives in the global namespace (static inline in ops/hash.h).
cdef extern from "ops/hash.h" nogil:
    void draken_hash(const DrakenVector& v, uint64_t* out, uint32_t n)

cdef extern from "simd_hash.h" nogil:
    void simd_mix_hash(uint64_t* dest, const uint64_t* values, size_t count)

# draken allocator (mimalloc) — no GIL required. Used for the per-column hash
# scratch buffer so the multi-column hash loop stays fully nogil.
cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil


cdef inline object _unwrap(object v):
    """Return the raw nanobind VectorOwner from either a Vector or a raw nb vector."""
    if isinstance(v, Vector):
        return (<Vector>v)._nb
    return v


cdef inline Vector _wrap(object v):
    if isinstance(v, Vector):
        return <Vector>v
    return Vector(v)


cdef Morsel _make_morsel():
    cdef Morsel m = Morsel.__new__(Morsel)
    m._col_names = []
    m._zero_col_num_rows = 0
    return m


cdef class Morsel:
    def __cinit__(self):
        self._col_names = []
        # self._columns is a C++ vector[PyObject*] — default-constructed empty.
        self._zero_col_num_rows = 0

    def __dealloc__(self):
        # Release the strong reference held on each column Vector.
        cdef Py_ssize_t i
        for i in range(<Py_ssize_t>self._columns.size()):
            _morsel_decref(self._columns[i])
        self._columns.clear()

    # ---- C++ column-store accessors -------------------------------------
    cdef Py_ssize_t _num_columns(self) noexcept:
        return <Py_ssize_t>self._columns.size()

    cdef Vector _get_column(self, Py_ssize_t i):
        return <Vector>(<object>self._columns[i])

    cdef void _set_column(self, Py_ssize_t i, Vector v):
        Py_INCREF(v)
        _morsel_decref(self._columns[i])
        self._columns[i] = <PyObject*>v

    cdef void _append_column(self, Vector v):
        Py_INCREF(v)
        self._columns.push_back(<PyObject*>v)

    cdef void _clear_columns(self):
        cdef Py_ssize_t i
        for i in range(<Py_ssize_t>self._columns.size()):
            _morsel_decref(self._columns[i])
        self._columns.clear()

    def __len__(self):
        return self._num_columns()

    def __getitem__(self, int idx):
        return self._get_column(idx)

    @property
    def num_rows(self):
        if self._columns.size() == 0:
            return self._zero_col_num_rows
        return self._get_column(0).length

    @property
    def ptr(self):
        # evaluation.pyx accesses morsel.ptr.num_rows — satisfy via self
        return self

    @property
    def nbytes(self):
        return self.num_rows * len(self._col_names) * 8

    @property
    def num_columns(self):
        return self._num_columns()

    @property
    def column_names(self):
        return self._col_names

    @property
    def column_types(self):
        return [self._get_column(i).type for i in range(self._num_columns())]

    def hash(self, col_names=None, columns=None):
        # Returns uint64_t[::1] row hashes for the given columns via the C++ path.
        # col_names: list of column names (str or bytes), or None for all columns.
        # columns=:  alias for col_names, accepted for call-site compatibility.
        from array import array as _array
        cdef Py_ssize_t n = self.num_rows
        if n == 0:
            return _array('Q', b'')

        if col_names is None:
            col_names = columns  # may still be None → all columns

        cdef int32_t n_cols = 0
        cdef int32_t* col_indices = self._resolve_columns_to_indices(col_names, &n_cols)

        # Resolve column pointers up front (GIL held here) so c_hash runs
        # fully nogil — no Python list indexing inside the hash loop.
        cdef const DrakenVector** dvs
        try:
            dvs = self._columns_to_pointers(col_indices, n_cols)
        finally:
            free(col_indices)

        # calloc zeroes the buffer — required for multi-column simd_mix_hash.
        cdef uint64_t* buf = <uint64_t*>calloc(<size_t>n, sizeof(uint64_t))
        if buf == NULL:
            draken_free(dvs)
            raise MemoryError()

        cdef bint needs_gil
        try:
            needs_gil = self.c_hash(buf, dvs, n_cols, n)
        finally:
            draken_free(dvs)

        if needs_gil:
            free(buf)
            raise NotImplementedError("Morsel.hash: DRAKEN_ARRAY columns cannot be hashed via this path")

        cdef bytes raw = (<const char*>buf)[:n * sizeof(uint64_t)]
        free(buf)
        return _array('Q', raw)

    def hash_keys(self, col_names=None):
        """Shape-preserving keying hash — the entry point for group-by / join /
        distinct.  Returns an INT64 Vector.

        Single column: the column's shaped hash (dict→dict, dense→dense), so a
        compressed key yields k distinct hashes the consumer probes once each.
        Multiple columns: the per-column hashes are mixed into a dense hash
        vector (a composite key has no shared dict structure).

        Null keys are baked to NULL_HASH so they collide; per-row null semantics
        are read from the source key columns by consumers that need them.
        """
        cdef Py_ssize_t n = self.num_rows
        cdef int32_t n_cols = 0
        cdef int32_t* col_indices = self._resolve_columns_to_indices(col_names, &n_cols)

        # Single column → shape-preserving hash vector.
        if n_cols == 1:
            col_idx = col_indices[0]
            free(col_indices)
            return Vector(self._get_column(col_idx)._nb.hash_shaped())

        # Multi-column → dense mixed hash. Resolve pointers (GIL held) so the
        # mix loop runs fully nogil, then wrap the owned buffer as a Vector.
        cdef const DrakenVector** dvs
        try:
            dvs = self._columns_to_pointers(col_indices, n_cols)
        finally:
            free(col_indices)

        if n == 0:
            draken_free(dvs)
            return from_decoded(NULL, NULL, 0, DRAKEN_INT64)

        cdef uint64_t* buf = <uint64_t*>draken_malloc(<size_t>n * sizeof(uint64_t))
        if buf == NULL:
            draken_free(dvs)
            raise MemoryError()
        memset(buf, 0, <size_t>n * sizeof(uint64_t))  # zeroed: required by simd_mix_hash

        cdef bint needs_gil
        try:
            needs_gil = self.c_hash(buf, dvs, n_cols, n)
        finally:
            draken_free(dvs)

        if needs_gil:
            draken_free(buf)
            raise NotImplementedError(
                "Morsel.hash_keys: DRAKEN_ARRAY columns cannot be hashed via this path")

        # buf is draken_malloc'd; from_decoded assumes ownership (freed on GC).
        return from_decoded(<void*>buf, NULL, <uint32_t>n, DRAKEN_INT64)

    def __str__(self):
        """Format morsel as an ASCII table for display."""
        if self.num_rows == 0:
            if self.num_columns == 0:
                return ""
            col_names = [name.decode('utf-8') if isinstance(name, bytes) else name
                         for name in self._col_names]
            return " | ".join(col_names)

        col_names = [name.decode('utf-8') if isinstance(name, bytes) else name
                     for name in self._col_names]
        col_data = []
        for col_idx in range(self.num_columns):
            vec = self._get_column(col_idx)
            try:
                py_list = vec.to_pylist()
                col_data.append(py_list)
            except Exception:
                col_data.append([None] * self.num_rows)

        max_rows = min(self.num_rows, 5)
        lines = []

        if col_names:
            lines.append(" | ".join(col_names))
            lines.append("-" * (sum(len(n) for n in col_names) + 3 * (len(col_names) - 1)))

        for row_idx in range(max_rows):
            row = []
            for col_idx in range(self.num_columns):
                val = col_data[col_idx][row_idx]
                val_str = str(val)[:30] if val is not None else "NULL"
                row.append(val_str)
            lines.append(" | ".join(row))

        if self.num_rows > 5:
            remaining = self.num_rows - 5
            lines.append("... %d more rows ..." % remaining)

        return "\n".join(lines) if lines else ""

    def _column_index_from_name(self, name):
        if isinstance(name, str):
            name = name.encode("utf-8")
        for i, n in enumerate(self._col_names):
            if n == name:
                return i
        raise KeyError(f"_column_index_from_name: column not found: {name!r}")

    def _ensure_name_map(self):
        return {name: i for i, name in enumerate(self._col_names)}

    cdef int32_t* _resolve_columns_to_indices(self, object columns,
                                              int32_t* n_cols_out) except NULL:
        """Resolve column names → freshly-malloc'd int32 index array.

        columns=None → indices [0, _num_columns()).
        Caller owns the returned buffer (free()). Raises KeyError on missing
        name; MemoryError on alloc failure.
        """
        cdef Py_ssize_t avail = self._num_columns()
        cdef Py_ssize_t i, j
        cdef int32_t n_cols
        cdef int32_t* result
        cdef bytes name_bytes
        cdef bint found

        if columns is None:
            n_cols = <int32_t>avail
            result = <int32_t*>malloc((<size_t>n_cols if n_cols > 0 else 1) * sizeof(int32_t))
            if result == NULL:
                raise MemoryError()
            for i in range(n_cols):
                result[i] = <int32_t>i
        else:
            n_cols = <int32_t>len(columns)
            result = <int32_t*>malloc((<size_t>n_cols if n_cols > 0 else 1) * sizeof(int32_t))
            if result == NULL:
                raise MemoryError()
            for i in range(n_cols):
                name = columns[i]
                if isinstance(name, str):
                    name_bytes = (<str>name).encode("utf-8")
                elif isinstance(name, bytes):
                    name_bytes = name
                else:
                    free(result)
                    raise TypeError(
                        "Morsel._resolve_columns_to_indices: column name must be "
                        "str or bytes; got %s" % type(name).__name__
                    )
                found = False
                for j in range(avail):
                    if self._col_names[j] == name_bytes:
                        result[i] = <int32_t>j
                        found = True
                        break
                if not found:
                    free(result)
                    raise KeyError(
                        "Morsel._resolve_columns_to_indices: column not found: %r"
                        % name_bytes
                    )
        n_cols_out[0] = n_cols
        return result

    cdef const DrakenVector** _columns_to_pointers(self, int32_t* col_indices,
                                                   int32_t n_cols) except NULL:
        """Build a draken_malloc'd array of DrakenVector* for the given indices.

        Touches the Python column list, so must be called under the GIL — once,
        before any nogil hot path. The morsel keeps the Vector objects alive, so
        the returned pointers stay valid for the morsel's lifetime. Caller owns
        the buffer (draken_free).
        """
        cdef const DrakenVector** result = <const DrakenVector**>draken_malloc(
            (<size_t>n_cols if n_cols > 0 else 1) * sizeof(DrakenVector*))
        if result == NULL:
            raise MemoryError()
        cdef int32_t i
        for i in range(n_cols):
            result[i] = self._get_column(col_indices[i]).unified()
        return result

    cdef bint c_hash(self, uint64_t* hashes_ptr, const DrakenVector** dvs,
                     int32_t n_cols, Py_ssize_t n) nogil:
        """Hash + mix specified columns into hashes_ptr.

        hashes_ptr: caller-allocated n uint64s, pre-zeroed.
        dvs: pre-resolved DrakenVector* array (n_cols entries). No Python access
        happens here — the column pointers are resolved by the caller under the
        GIL via _columns_to_pointers, so this method runs fully nogil.

        Single-column shortcut: writes column hash directly into hashes_ptr,
        no mix (saves one allocation + one pass).
        Multi-column: hashes each column into tmp, then simd_mix_hash into
        hashes_ptr.

        Returns 0 on success, 1 if any column needs GIL fallback (e.g.
        DRAKEN_ARRAY, which can't hash nogil).
        """
        if n == 0 or n_cols == 0:
            return 0

        cdef Py_ssize_t c
        cdef const DrakenVector* dv
        cdef uint64_t* tmp = NULL
        cdef bint needs_gil = 0

        # Single-column shortcut: hash directly into the output, no mix.
        if n_cols == 1:
            dv = dvs[0]
            if dv.type == DRAKEN_ARRAY:
                return 1
            draken_hash(dv[0], hashes_ptr, <uint32_t>n)
            return 0

        # Multi-column: scratch buffer for per-column hashes (mimalloc, no GIL).
        tmp = <uint64_t*>draken_malloc(<size_t>n * sizeof(uint64_t))
        if tmp == NULL:
            return 1

        for c in range(n_cols):
            dv = dvs[c]
            if dv.type == DRAKEN_ARRAY:
                needs_gil = 1
                break
            draken_hash(dv[0], tmp, <uint32_t>n)
            simd_mix_hash(hashes_ptr, tmp, <size_t>n)

        draken_free(tmp)
        return needs_gil

    def append(self, vec):
        cdef Vector wrapped = _wrap(vec)
        self._append_column(wrapped)

    cdef void _empty_inplace(self):
        """Zero all columns to 0 rows, in place."""
        cdef Py_ssize_t n = self._num_columns()
        cdef Py_ssize_t i
        if n == 0:
            self._zero_col_num_rows = 0
            return
        for i in range(n):
            # n==0 take: the kernels never deref the index pointer when n==0.
            self._set_column(i, _take_native(self._get_column(i), NULL, 0))

    cdef void _take_inplace(self, int32_t[::1] indices):
        """Filter all columns to the given row indices, in place. Native gather
        straight off the typed memoryview — no boxed index list."""
        cdef Py_ssize_t n = self._num_columns()
        cdef Py_ssize_t i
        cdef uint32_t ni = <uint32_t>indices.shape[0]
        if n == 0:
            self._zero_col_num_rows = indices.shape[0]
            return
        cdef const int32_t* idx = &indices[0] if ni > 0 else NULL
        for i in range(n):
            self._set_column(i, _take_native(self._get_column(i), idx, ni))

    def append_vector(self, name, vec):
        if isinstance(name, str):
            name = name.encode("utf-8")
        cdef Vector wrapped = _wrap(vec)
        self._col_names.append(name)
        self._append_column(wrapped)

    def column(self, name, fallback=None):
        if isinstance(name, str):
            name = name.encode("utf-8")
        for i, n in enumerate(self._col_names):
            if n == name:
                return self._get_column(i)
        if fallback is not None:
            if isinstance(fallback, str):
                fallback = fallback.encode("utf-8")
            for i, n in enumerate(self._col_names):
                if n == fallback:
                    return self._get_column(i)
        raise KeyError(f"column not found: {name!r}")

    def select(self, col_names):
        cdef Morsel result = _make_morsel()
        for name in col_names:
            if isinstance(name, str):
                name = name.encode("utf-8")
            for i, n in enumerate(self._col_names):
                if n == name:
                    result._col_names.append(n)
                    result._append_column(self._get_column(i))
                    break
        if result._num_columns() == 0:
            result._zero_col_num_rows = self.num_rows
        return result

    def rename(self, new_names):
        cdef Morsel result = _make_morsel()
        for i, name in enumerate(new_names):
            if isinstance(name, str):
                name = name.encode("utf-8")
            result._col_names.append(name)
            result._append_column(self._get_column(i))
        if result._num_columns() == 0:
            result._zero_col_num_rows = self.num_rows
        return result

    def filter_mask(self, mask):
        """Keep rows where the BoolVector `mask` is valid AND true.

        Fully native: each column is gathered by the C++ `mask` kernel, which
        derives the surviving-row indices from the mask bitmap (unified
        row_is_valid/row_bool) and runs the typed take. No Python per-row loop,
        no to_pylist, no boxed index list.
        """
        cdef Morsel result = _make_morsel()
        result._col_names = list(self._col_names)
        cdef Py_ssize_t n = self._num_columns()
        cdef object mask_nb = (<Vector>mask)._nb if isinstance(mask, Vector) else mask
        cdef Py_ssize_t i
        cdef object nb_masked
        if n == 0:
            # No columns to gather — just carry the surviving row count.
            result._zero_col_num_rows = mask_nb.count_true()
            return result
        for i in range(n):
            nb_masked = self._get_column(i)._nb.mask(mask_nb)
            result._append_column(Vector(nb_masked))
        return result

    def take(self, indices):
        cdef Morsel result = _make_morsel()
        result._col_names = list(self._col_names)
        cdef Py_ssize_t n = self._num_columns()
        cdef Py_ssize_t i
        # Materialise the index iterable into a typed int32 buffer ONCE, then
        # gather every column natively (no per-column re-boxing of a list).
        idx_list = list(indices)
        cdef uint32_t ni = <uint32_t>len(idx_list)
        if n == 0:
            result._zero_col_num_rows = ni
            return result
        cdef int32_t* idx = <int32_t*>malloc(<size_t>(ni if ni > 0 else 1) * sizeof(int32_t))
        if idx == NULL:
            raise MemoryError()
        try:
            for i in range(ni):
                idx[i] = <int32_t>idx_list[i]
            for i in range(n):
                result._append_column(_take_native(self._get_column(i), idx, ni))
        finally:
            free(idx)
        return result

    def slice(self, Py_ssize_t offset=0, Py_ssize_t length=0, Py_ssize_t start=-1):
        cdef Py_ssize_t real_start = start if start >= 0 else offset
        cdef Morsel result = _make_morsel()
        result._col_names = list(self._col_names)
        cdef Py_ssize_t n = self._num_columns()
        cdef Py_ssize_t i
        if n == 0:
            result._zero_col_num_rows = length
            return result
        for i in range(n):
            nb_sliced = self._get_column(i)._nb.slice(<uint32_t>real_start, <uint32_t>length)
            result._append_column(Vector(nb_sliced))
        return result

    def copy(self, columns=None, mask=None):
        cdef Morsel result = _make_morsel()
        cdef Py_ssize_t n = self._num_columns()
        cdef Py_ssize_t i
        if columns is not None:
            col_set = []
            for c in columns:
                if isinstance(c, str):
                    c = c.encode("utf-8")
                col_set.append(c)
            for i in range(n):
                if self._col_names[i] in col_set:
                    if mask is not None:
                        nb_v = self._get_column(i)._nb.take(mask)
                        result._append_column(Vector(nb_v))
                    else:
                        result._append_column(self._get_column(i))
                    result._col_names.append(self._col_names[i])
            if result._num_columns() == 0:
                result._zero_col_num_rows = len(mask) if mask is not None else self.num_rows
        elif mask is not None:
            result._col_names = list(self._col_names)
            for i in range(n):
                nb_v = self._get_column(i)._nb.take(mask)
                result._append_column(Vector(nb_v))
            if result._num_columns() == 0:
                result._zero_col_num_rows = len(mask)
        else:
            result._col_names = list(self._col_names)
            for i in range(n):
                result._append_column(self._get_column(i))
            if result._num_columns() == 0:
                result._zero_col_num_rows = self._zero_col_num_rows
        return result

    @classmethod
    def combine(cls, morsels):
        """Vertical concatenation of multiple morsels with the same schema.

        Native, buffer-level: each column's N nanobind vectors are concatenated
        by `vector_concat` directly on the underlying buffers — no per-row Python
        objects, no UTF-8 decode, type preserved exactly.
        """
        if not morsels:
            return _make_morsel()
        first = <Morsel>morsels[0]
        if len(morsels) == 1:
            return first
        from draken.draken_native import vector_concat as _nb_concat
        cdef Morsel result = _make_morsel()
        result._col_names = list(first._col_names)
        cdef Py_ssize_t ncols = first._num_columns()
        cdef Py_ssize_t nmorsels = len(morsels)
        for col_idx in range(ncols):
            col_parts = [
                (<Morsel>morsels[mi])._get_column(col_idx)._nb
                for mi in range(nmorsels)
            ]
            nb_v = _nb_concat(col_parts)
            result._append_column(Vector(nb_v))
        return result

    @classmethod
    def from_vectors(cls, col_names, col_vecs):
        cdef Morsel result = cls.__new__(cls)
        cdef Vector wrapped
        result._col_names = []
        result._zero_col_num_rows = 0
        for name, vec in zip(col_names, col_vecs):
            if isinstance(name, str):
                name = name.encode("utf-8")
            wrapped = _wrap(vec)
            result._col_names.append(name)
            result._append_column(wrapped)
        return result


cpdef Morsel align_tables(Morsel left_morsel, Morsel right_morsel,
                           int32_t[::1] left_view, int32_t[::1] right_view):
    """Materialise a join result by taking rows from left and right at the given indices.

    left_view:  int32 indices into left_morsel — negative values produce null rows
                (used by RIGHT/FULL outer-join callers for unmatched right rows).
    right_view: int32 indices into right_morsel — negative values produce null rows
                (used by LEFT/FULL outer-join callers for unmatched left rows).

    Returns a new Morsel with all left_morsel columns followed by all right_morsel
    columns. Number of rows equals len(left_view).
    """
    cdef Py_ssize_t n = left_view.shape[0]
    cdef Morsel result = _make_morsel()
    cdef Py_ssize_t i, j
    cdef int ncols_left, ncols_right
    cdef object nb_taken, nb_new
    cdef bint left_has_neg, right_has_neg

    # Left columns: may contain -1 for unmatched right rows (RIGHT/FULL outer).
    left_has_neg = False
    for i in range(n):
        if left_view[i] < 0:
            left_has_neg = True
            break

    ncols_left = left_morsel._num_columns()
    cdef const int32_t* lidx = &left_view[0] if n > 0 else NULL
    if not left_has_neg:
        for j in range(ncols_left):
            result._col_names.append(left_morsel._col_names[j])
            result._append_column(_take_native(left_morsel._get_column(j), lidx, <uint32_t>n))
    else:
        for j in range(ncols_left):
            result._col_names.append(left_morsel._col_names[j])
            result._append_column(
                _take_native_with_null(left_morsel._get_column(j), lidx, <uint32_t>n))

    # Right columns: may contain -1 for unmatched left rows (LEFT/FULL outer).
    right_has_neg = False
    for i in range(n):
        if right_view[i] < 0:
            right_has_neg = True
            break

    ncols_right = right_morsel._num_columns()

    cdef const int32_t* ridx
    if not right_has_neg:
        # Fast path: no null rows — native gather off the memoryview.
        ridx = &right_view[0] if n > 0 else NULL
        for j in range(ncols_right):
            result._col_names.append(right_morsel._col_names[j])
            result._append_column(_take_native(right_morsel._get_column(j), ridx, <uint32_t>n))
    else:
        # Unmatched-row path: native gather where right_view[i] < 0 → NULL row.
        # No to_pylist round-trip, no per-type reconstruction; ALL types
        # supported (incl. ARRAY / INTERVAL / VARBINARY).
        ridx = &right_view[0] if n > 0 else NULL
        for j in range(ncols_right):
            result._col_names.append(right_morsel._col_names[j])
            result._append_column(
                _take_native_with_null(right_morsel._get_column(j), ridx, <uint32_t>n))

    return result
