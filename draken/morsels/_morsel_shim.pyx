# cython: language_level=3
# Cython shim for draken.morsels.morsel — E.24 vtable bridge.

from libc.stdint cimport int32_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, calloc, free
from libc.string cimport memcpy, memset
from libc.stddef cimport size_t

from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF
from libcpp.vector cimport vector

from libcpp.memory cimport shared_ptr

from draken.morsels.morsel cimport Morsel
from draken.morsels.cxx_morsel cimport (
    CxxMorsel, cxx_morsel_raw_ptr, cxx_morsel_nbytes,
    cxx_morsel_shallow_copy, cxx_morsel_to_handle, cxx_morsel_delete,
    cxx_select_c, cxx_take_c,
)

# Typed-index fast path: producers (morsel_sort, join align) hand `take`/`filter`
# an array('i') (int32). Gathering straight off its buffer via the nogil C-ABI
# (cxx_take_c) skips the Python-list round-trip the nanobind `.take(list)` path
# forces (measured ~7x on the take call for 1M rows). `array` is the Python type;
# explicit isinstance/typecode dispatch keeps CLAUDE.md §9 (no try/except flow).
from array import array as _ArrayType
from draken.vectors.vector cimport Vector, from_decoded
from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_ARRAY, DRAKEN_INT64
from draken.core.buffers cimport draken_vector_nbytes
from draken.core.buffers cimport draken_vector_projected_arena_bytes

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


cdef size_t _vector_nbytes_with_array_children(Vector v):
    """draken_vector_nbytes(v._dv) (offsets, for a DRAKEN_ARRAY column) plus,
    recursively, the owned child subtree reached via the nanobind array_child
    accessor. Used for materialized (PyObject-backed) morsels' Morsel.nbytes,
    where there is no VectorOwner/child_owner reachable from Cython directly
    (unlike the Cxx-backed path, which delegates to cxx_morsel_nbytes and its
    child_owner walk). Guards on array_child_type first, same pattern as
    Vector._to_json, since array_child raises for a non-array/childless vector.
    """
    cdef size_t total = draken_vector_nbytes(v._dv)
    cdef Vector child
    if v._dv != NULL and v._dv.type == DRAKEN_ARRAY and v._nb.array_child_type is not None:
        child = Vector(v._nb.array_child)
        total += _vector_nbytes_with_array_children(child)
    return total

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


# ---- S-B.1a boundary bridges (Morsel <-> shared_ptr[CxxMorsel]) --------------
# These are the conversions the carrier flip (S-B.1b) uses at the scan/cursor and
# during the gil-wrapped transition. The chain itself will carry shared_ptr[CxxMorsel]
# directly (no PyObject); these cross the boundary only. Round-trip is byte-identical
# because every step shallow-copies — the per-column shared_ptr<VectorOwner> owners
# (and thus the bytes) are shared, never duplicated.

cdef shared_ptr[CxxMorsel] morsel_to_cxx(Morsel m):
    """Boundary IN: any Morsel -> an owned heap shared_ptr<CxxMorsel> (shares
    column owners). Cxx-backed: shallow-copy the cached substrate directly.
    PyObject-backed: build a transient CxxMorsel from the columns first (the
    shallow copy captures the owners, so the transient handle can be released)."""
    if m._cxx_ptr is not NULL:
        return shared_ptr[CxxMorsel](cxx_morsel_shallow_copy(m._cxx_ptr))
    cdef object handle = m._get_cxx()   # nanobind CxxMorsel built from PyObject cols
    cdef const CxxMorsel* p = cxx_morsel_raw_ptr(<PyObject*>handle)
    return shared_ptr[CxxMorsel](cxx_morsel_shallow_copy(p))


cdef Morsel cxx_to_morsel(shared_ptr[CxxMorsel] sp):
    """Boundary OUT: shared_ptr<CxxMorsel> -> a PyObject Morsel (the shim build)."""
    cdef PyObject* raw = cxx_morsel_to_handle(sp.get())   # NEW ref
    cdef object handle = <object>raw                       # Cython incref -> 2
    _morsel_decref(raw)                                    # balance -> 1
    return Morsel.from_cxx(handle)


cdef object _cxx_owned_to_handle(CxxMorsel* out):
    """Wrap an OWNED CxxMorsel* (from a cxx_*_c transform) into a nanobind handle.
    The shared_ptr adopts `out` (freed on release, same idiom as cxx_select_sp);
    cxx_morsel_to_handle shallow-copies into the handle."""
    cdef shared_ptr[CxxMorsel] sp = shared_ptr[CxxMorsel](out)
    cdef PyObject* raw = cxx_morsel_to_handle(sp.get())   # NEW ref
    cdef object handle = <object>raw                       # Cython incref -> 2
    _morsel_decref(raw)                                    # balance -> 1
    return handle


cdef shared_ptr[CxxMorsel] cxx_morsel_from_vectors_sp(list vectors, list names):
    """Build an owned heap shared_ptr<CxxMorsel> DIRECTLY from column vector handles +
    identity names, with NO intermediate Python Morsel object. Byte-identical to
    morsel_to_cxx(Morsel.from_cxx_vectors(names, vectors)) — same nanobind assembly +
    shallow-copy, sharing the per-column owners (bytes never duplicated) — but skips
    constructing the transient Morsel wrapper.

    S-B.2 prerequisite: lets a scan / column-producing operator emit the native carrier
    directly instead of building a Morsel and re-encoding it (parquet_read.pyx:930). It
    is GIL-held (the vector handles are PyObjects; the assembly is the existing C++
    builder) — that is fine, since next_morsel is pulled outside the chain's nogil push
    span. Additive + behaviour-neutral: nothing calls it yet."""
    from draken.draken_native import cxx_morsel_from_vectors
    cdef Py_ssize_t n = len(vectors)
    cdef Py_ssize_t i
    cdef list handles = [_unwrap(vectors[i]) for i in range(n)]
    cdef object handle = cxx_morsel_from_vectors(handles, names)
    cdef const CxxMorsel* p = cxx_morsel_raw_ptr(<PyObject*>handle)
    return shared_ptr[CxxMorsel](cxx_morsel_shallow_copy(p))


cpdef object _build_from_vectors_for_test(list vectors, list names):
    """Test seam: exercise cxx_morsel_from_vectors_sp from Python (returns a Morsel via
    the OUT bridge) so the cdef builder can be differentially verified against
    Morsel.from_cxx_vectors. Not used in production."""
    return cxx_to_morsel(cxx_morsel_from_vectors_sp(vectors, names))


cdef shared_ptr[CxxMorsel] cxx_select_sp(shared_ptr[CxxMorsel] m, list names):
    """Select/reorder columns of a CxxMorsel by identity name (bytes), returning a new
    owned CxxMorsel that SHARES the column owners (no data copy). Names not present are
    skipped; an all-missing select carries the row count. S-B.2 prereq #3 — lets the
    scan apply its output projection without leaving the substrate. GIL-held only to
    marshal the name byte-buffers; the select itself is the nogil container op."""
    cdef Py_ssize_t n = len(names)
    cdef Py_ssize_t i
    cdef const char** ptrs = <const char**>draken_malloc(<size_t>n * sizeof(char*))
    cdef uint32_t* lens = <uint32_t*>draken_malloc(<size_t>n * sizeof(uint32_t))
    cdef bytes b
    cdef list keep = []   # keep the byte buffers alive across the call
    cdef bint freed = False
    try:
        for i in range(n):
            b = names[i] if isinstance(names[i], bytes) else (<str>names[i]).encode("utf-8")
            keep.append(b)
            ptrs[i] = <const char*>b
            lens[i] = <uint32_t>len(b)
        return shared_ptr[CxxMorsel](cxx_select_c(m.get(), ptrs, lens, <uint32_t>n))
    finally:
        draken_free(ptrs)
        draken_free(lens)


cpdef object _select_for_test(object morsel, list names):
    """Test seam: exercise cxx_select_sp from Python (Morsel in/out via the bridges)."""
    return cxx_to_morsel(cxx_select_sp(morsel_to_cxx(<Morsel>morsel), names))


cdef class Morsel:
    def __cinit__(self):
        self._col_names = []
        # self._columns is a C++ vector[PyObject*] — default-constructed empty.
        self._zero_col_num_rows = 0
        self._cxx = None  # S1: Cxx-backed when set (see _ensure_pyobject)
        self._cxx_ptr = NULL

    # ---- CxxMorsel substrate seams -------------------------------------------
    cpdef void materialize(self) except *:
        """Cursor-boundary shim: build PyObject columns from `_cxx` and clear it.
        Byte-identical to from_vectors (wraps the exact VectorOwners). Idempotent.
        The CURSOR is the only sanctioned caller — this is the single point where
        the C++ substrate becomes Python for the user."""
        if self._cxx is None:
            return
        cxx = self._cxx
        # _col_names is already cached (set in _set_cxx); clearing keeps it.
        self._set_cxx(None)
        cdef Vector wrapped
        for vec in cxx.to_vectors():
            wrapped = _wrap(vec)
            self._append_column(wrapped)

    cdef void _ensure_pyobject(self) except *:
        """Engine-internal guard: PyObject column access is only valid on a
        PyObject-backed morsel. A Cxx-backed morsel reaching here means an
        operator/path read columns without being converted to the CxxMorsel
        substrate — FAIL LOUD (no silent materialization)."""
        if self._cxx is not None:
            raise RuntimeError(
                "Morsel: engine-internal PyObject column access on a Cxx-backed "
                "morsel. This operator/path is NOT converted to the CxxMorsel "
                "substrate. Only the cursor may materialize (Morsel.materialize)."
            )

    cdef void _set_cxx(self, object handle):
        """Single sanctioned write to the Cxx carrier: keep the nanobind handle,
        the C-level read pointer, and the cached names in lock-step. Pass None to
        clear. The one `names()` call here (when set) is the ONLY metadata cross
        of the binding per morsel — all later name resolution uses `_col_names`."""
        self._cxx = handle
        if handle is not None:
            self._cxx_ptr = cxx_morsel_raw_ptr(<PyObject*>handle)
            self._col_names = list(handle.names())
        else:
            self._cxx_ptr = NULL

    cdef const DrakenVector* _col_view(self, Py_ssize_t i) noexcept nogil:
        # Pure C++ struct access — no PyObject, no nanobind, GIL-releasable.
        return &self._cxx_ptr.columns[<size_t>i].view

    cpdef object _get_cxx(self):
        """Return the CxxMorsel handle for converted (C++) operators: `_cxx` if
        set (zero work), else build a transient one from the PyObject columns."""
        if self._cxx is not None:
            return self._cxx
        from draken.draken_native import cxx_morsel_from_vectors
        cdef Py_ssize_t n = self._num_columns()
        cdef Py_ssize_t i
        handles = [self._get_column(i)._nb for i in range(n)]
        # `_zero_col_num_rows` is the ONLY carrier of the row count when there are no
        # columns (`select([])`/`rename([])` on a PyObject-backed Morsel set it); it
        # must cross the boundary or the morsel arrives in C++ as zero rows.
        return cxx_morsel_from_vectors(handles, list(self._col_names),
                                       self._zero_col_num_rows)

    cpdef object _cxx_column(self, identity, name=None):
        """Read one column for a CONVERTED operator: from the CxxMorsel substrate
        when Cxx-backed (no whole-morsel materialization), else the normal column
        (preserving the identity→name fallback). Returns a Cython Vector."""
        if isinstance(identity, str):
            identity = identity.encode("utf-8")
        if self._cxx is not None:
            return Vector(self._cxx.column(identity))
        if name is not None:
            return self.column(identity, name)
        return self.column(identity)

    def __dealloc__(self):
        # Release the strong reference held on each column Vector.
        cdef Py_ssize_t i
        for i in range(<Py_ssize_t>self._columns.size()):
            _morsel_decref(self._columns[i])
        self._columns.clear()

    # ---- C++ column-store accessors -------------------------------------
    cdef Py_ssize_t _num_columns(self) noexcept:
        # Cheap: answer from the Cxx backing without materializing.
        if self._cxx is not None:
            return <Py_ssize_t>self._cxx.num_columns
        return <Py_ssize_t>self._columns.size()

    cdef Vector _get_column(self, Py_ssize_t i):
        if self._cxx is not None:
            # Cxx-backed: hand back a Vector over the i-th substrate column. The
            # underlying owner is kept alive by the CxxMorsel for the morsel's
            # lifetime, so the borrowed DrakenVector* stays valid. No materialize.
            return <Vector>self._cxx_column(self._col_names[i])
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
        # Row count — consistent with row-access __getitem__. Production
        # callers (insert row tallies, cross-join/writer emptiness checks)
        # all treat len(morsel) as the number of rows. Use num_columns for
        # the column count.
        return self.num_rows

    def __getitem__(self, Py_ssize_t idx):
        """Row access: return row ``idx`` as a tuple of per-column values.

        Negative indices follow Python convention. An out-of-range index
        raises ``IndexError`` — no silent ``(None, ...)`` padding (CLAUDE.md
        §1: fail fast, never silently degrade). For column access use
        ``column(name)``.
        """
        cdef Py_ssize_t n = self.num_rows
        cdef Py_ssize_t i = idx
        if i < 0:
            i += n
        if i < 0 or i >= n:
            raise IndexError("Morsel row index out of range")
        cdef Py_ssize_t ncols = self._num_columns()
        cdef Py_ssize_t c
        return tuple([self._get_column(c)[i] for c in range(ncols)])

    @property
    def num_rows(self):
        # Cheap: answer from the Cxx backing without materializing.
        if self._cxx is not None:
            return self._cxx.num_rows
        if self._columns.size() == 0:
            return self._zero_col_num_rows
        return self._get_column(0).length

    @property
    def ptr(self):
        # evaluation.pyx accesses morsel.ptr.num_rows — satisfy via self
        return self

    @property
    def nbytes(self):
        """Approximate in-memory footprint (bytes): the real owned payload of every
        column vector — offsets/fixed-width data, the string arena for the string
        family, and validity bitmaps — summed via the native draken_vector_nbytes
        helper. Replaces the old flat rows×cols×8 estimate, which grossly
        undercounted variable-length string columns (a wide, string-heavy result
        buffered against this figure could blow far past an intended memory cap).
        DRAKEN_ARRAY columns additionally recurse into their owned child subtree
        (offsets alone would still miss the actual element data) — the Cxx-backed
        path via cxx_morsel_nbytes's child_owner walk, the materialized-PyObject
        path via the array_child accessor (_vector_nbytes_with_array_children),
        since there is no VectorOwner reachable from a bare nanobind Vector at
        this layer. See buffers.h for the exact per-type accounting."""
        cdef Py_ssize_t n = self._num_columns()
        cdef Py_ssize_t i
        cdef size_t total = 0
        cdef Vector v
        if self._cxx is not None:
            # Cxx-backed: delegate to the C++ substrate twin (cxx_morsel.h),
            # which recurses into DRAKEN_ARRAY child_owner subtrees.
            total = cxx_morsel_nbytes(self._cxx_ptr)
        else:
            for i in range(n):
                v = self._get_column(i)
                total += _vector_nbytes_with_array_children(v)
        return total

    @property
    def num_columns(self):
        return self._num_columns()

    @property
    def column_names(self):
        # Metadata: answer from the Cxx backing without materializing.
        if self._cxx is not None:
            return list(self._col_names)
        return self._col_names

    @property
    def column_types(self):
        return [self._get_column(i).type for i in range(self._num_columns())]

    @property
    def schema(self):
        """Return {column_name_str: DrakenType} for each column."""
        names = self.column_names
        types = self.column_types
        return {
            (n.decode("utf-8") if isinstance(n, bytes) else n): t
            for n, t in zip(names, types)
        }

    def __iter__(self):
        """Iterate rows as named tuples. Field names are the column names."""
        import collections
        import keyword
        import re
        names = self.column_names
        str_names = [
            (n.decode("utf-8") if isinstance(n, bytes) else n)
            for n in names
        ]
        # Sanitise: namedtuple field names must be valid, non-keyword identifiers
        safe = []
        for i, n in enumerate(str_names):
            candidate = re.sub(r"\W", "_", n)
            if not candidate or candidate[0].isdigit():
                candidate = f"_{candidate}"
            if keyword.iskeyword(candidate):
                candidate = f"{candidate}_"
            safe.append(candidate or f"col{i}")
        Row = collections.namedtuple("Row", safe, rename=True)
        columns = [self._get_column(i).to_pylist() for i in range(self._num_columns())]
        for row_idx in range(self.num_rows):
            yield Row(*[col[row_idx] for col in columns])

    def to_arrow(self):
        """Return a pyarrow.Table. Requires pyarrow (not a rugo dependency)."""
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError("to_arrow() requires pyarrow: pip install pyarrow")
        self._ensure_pyobject()
        names = self.column_names
        str_names = [
            (n.decode("utf-8") if isinstance(n, bytes) else n)
            for n in names
        ]
        arrays = [
            self._get_column(i).to_arrow()
            for i in range(self._num_columns())
        ]
        return pa.table(dict(zip(str_names, arrays)))

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
            if self._cxx is not None:
                return Vector((<Vector>self._cxx_column(
                    self._col_names[col_idx]))._nb.hash_shaped())
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

    def _render_table(self, colorize):
        """Shared implementation for __str__ and to_string."""
        self._ensure_pyobject()

        cdef int nc = self.num_columns
        cdef int nr = self.num_rows

        col_names = [name.decode('utf-8') if isinstance(name, bytes) else str(name)
                     for name in self._col_names]
        # `.type_name`, NOT `.type.name`: the latter is the PHYSICAL tag, so an
        # IPv4 column rendered as "UINT32" and a DECIMAL(10,2) as bare "DECIMAL"
        # — naming a different type than the column actually has.
        col_types = [self._get_column(i).type_name for i in range(nc)]

        if nc == 0:
            return "Morsel{%d rows, 0 columns}" % nr

        # --- Column widths: max of header, type label, and data ---
        # max_display_width() scans unique slots directly in C++ — no to_pylist.
        MAX_COL = 30
        col_w = []
        for i in range(nc):
            vec = self._get_column(i)
            data_w = vec._nb.max_display_width()
            w = max(len(col_names[i]), len(col_types[i]), data_w)
            col_w.append(min(w, MAX_COL))

        # Row index width
        idx_w = max(len(str(nr)), 1) + 1  # +1 for padding

        def _cell(val, width, col_type):
            if val is None:
                s = "null"
                if colorize:
                    return "\033[38;2;64;75;108m\033[3m" + s.ljust(width) + "\033[0m"
                return s.ljust(width)
            if isinstance(val, bytes):
                try:
                    s = val.decode("utf-8")
                except UnicodeDecodeError:
                    s = val.hex()
            else:
                s = str(val)
            if len(s) > width:
                s = s[:width - 1] + "\u2026"
            s = s.ljust(width)
            if not colorize:
                return s
            # Type-based colouring for the terminal palette.
            # bool must come before int (bool is a subclass of int).
            import datetime as _dt
            import decimal as _dec
            if isinstance(val, bool):
                return "\033[38;2;139;233;253m\033[3m" + s + "\033[0m"
            if isinstance(val, int):
                return "\033[38;2;189;147;249m" + s + "\033[0m"
            if isinstance(val, (float, _dec.Decimal)):
                return "\033[38;2;255;121;198m" + s + "\033[0m"
            if isinstance(val, str):
                return "\033[38;2;255;171;82m" + s + "\033[0m"
            if isinstance(val, _dt.datetime):
                return "\033[38;2;80;250;123m" + s + "\033[0m"
            if isinstance(val, _dt.date):
                return "\033[38;2;80;250;123m" + s + "\033[0m"
            if isinstance(val, _dt.time):
                return "\033[38;2;26;185;67m" + s + "\033[0m"
            if isinstance(val, bytes):
                return "\033[38;2;196;160;0m" + s + "\033[0m"
            return s

        RESET  = "\033[0m"
        PUNCT  = "\033[38;5;102m"
        HEAD   = "\033[1m"
        TYPE_C = "\033[38;2;98;114;164m"
        IDX_C  = "\033[38;2;98;114;164m"

        def _p(s):
            return (PUNCT + s + RESET) if colorize else s

        lines = []

        # Top border
        lines.append(
            _p("\u250c" + ("\u2500" * idx_w) + "\u252c\u2500" +
               "\u2500\u252c\u2500".join("\u2500" * w for w in col_w) + "\u2500\u2510")
        )
        # Header: column names
        if colorize:
            name_cells = " \u2502 ".join(
                HEAD + n.center(w)[:w] + RESET for n, w in zip(col_names, col_w))
            lines.append(_p("\u2502") + (" " * idx_w) + _p("\u2502 ") + name_cells + _p(" \u2502"))
        else:
            lines.append(
                "\u2502" + (" " * idx_w) + "\u2502 " +
                " \u2502 ".join(n.center(w)[:w] for n, w in zip(col_names, col_w)) + " \u2502"
            )
        # Header: type names
        if colorize:
            type_cells = " \u2502 ".join(
                TYPE_C + t.center(w)[:w] + RESET for t, w in zip(col_types, col_w))
            lines.append(_p("\u2502") + (" " * idx_w) + _p("\u2502 ") + type_cells + _p(" \u2502"))
        else:
            lines.append(
                "\u2502" + (" " * idx_w) + "\u2502 " +
                " \u2502 ".join(t.center(w)[:w] for t, w in zip(col_types, col_w)) + " \u2502"
            )
        # Header separator: \u255e\u2550\u256a\u2550\u2561
        lines.append(
            _p("\u255e" + ("\u2550" * idx_w) + "\u256a\u2550" +
               "\u2550\u256a\u2550".join("\u2550" * w for w in col_w) + "\u2550\u2561")
        )

        if nr == 0:
            lines.append(
                _p("\u2514" + ("\u2500" * idx_w) + "\u2534\u2500" +
                   "\u2500\u2534\u2500".join("\u2500" * w for w in col_w) + "\u2500\u2518")
            )
            lines.append("0 rows x %d columns" % nc)
            return "\n".join(lines)

        LIMIT = 5
        top_and_tail = nr > (2 * LIMIT)
        show_indices = (list(range(LIMIT)) + list(range(nr - LIMIT, nr))) if top_and_tail \
                       else list(range(min(nr, LIMIT * 2)))

        # Materialise only the rows we will show
        col_data = [self._get_column(i).to_pylist() for i in range(nc)]

        for display_pos, row_idx in enumerate(show_indices):
            if top_and_tail and display_pos == LIMIT:
                skipped = nr - 2 * LIMIT
                gap = _p("\u2502") + ".".center(idx_w) + _p("\u2502 ")
                mid = ("%d more rows" % skipped).center(sum(col_w) + 3 * (nc - 1))
                lines.append(gap + mid + _p(" \u2502"))
            formatted = [_cell(col_data[ci][row_idx], col_w[ci], col_types[ci])
                         for ci in range(nc)]
            idx_str = (IDX_C + str(row_idx + 1).rjust(idx_w - 1) + RESET) if colorize \
                      else str(row_idx + 1).rjust(idx_w - 1)
            lines.append(
                _p("\u2502") + idx_str + _p(" \u2502 ") +
                _p(" \u2502 ").join(formatted) + _p(" \u2502")
            )

        # Bottom border
        lines.append(
            _p("\u2514" + ("\u2500" * idx_w) + "\u2534\u2500" +
               "\u2500\u2534\u2500".join("\u2500" * w for w in col_w) + "\u2500\u2518")
        )
        lines.append("%d rows x %d columns" % (nr, nc))
        return "\n".join(lines)

    def to_string(self, colorize=False):
        """Render the morsel as a table string.

        colorize=False  plain text (default; same as __str__).
        colorize=True   ANSI colour codes for the terminal palette.
        """
        return self._render_table(colorize)

    def __str__(self):
        return self._render_table(colorize=False)

    def _column_index_from_name(self, name):
        if isinstance(name, str):
            name = name.encode("utf-8")
        # Metadata lookup — answer from the substrate names when Cxx-backed,
        # without materializing the PyObject column store.
        cdef list names_src = self._col_names
        for i, n in enumerate(names_src):
            if n == name:
                return i
        raise KeyError(f"_column_index_from_name: column not found: {name!r}")

    def _ensure_name_map(self):
        # Metadata only — answer from the substrate names when Cxx-backed.
        names_src = self._col_names
        return {name: i for i, name in enumerate(names_src)}

    cdef int32_t* _resolve_columns_to_indices(self, object columns,
                                              int32_t* n_cols_out) except NULL:
        """Resolve column names → freshly-malloc'd int32 index array.

        columns=None → indices [0, _num_columns()).
        Caller owns the returned buffer (free()). Raises KeyError on missing
        name; MemoryError on alloc failure.

        Cxx-aware: when Cxx-backed, resolves names against the substrate
        (`_cxx.names()`) without materializing the PyObject column store.
        """
        cdef list names_src
        if self._cxx is not None:
            names_src = self._col_names
        else:
            names_src = self._col_names
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
                    if names_src[j] == name_bytes:
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
        if self._cxx_ptr is not NULL:
            # Cxx-backed: read the DrakenVector* straight out of the C++ substrate
            # (columns[idx].view) — no PyObject, no nanobind, no name resolution.
            # The pointer is valid while the morsel stays Cxx-backed.
            for i in range(n_cols):
                result[i] = self._col_view(col_indices[i])
            return result
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
        self._ensure_pyobject()
        self._append_column(wrapped)

    cdef void _empty_inplace(self):
        """Zero all columns to 0 rows, in place."""
        if self._cxx is not None:
            # Cxx-native: 0-row slice replaces the substrate in place.
            self._set_cxx(self._cxx.slice(0, 0))
            return
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
        cdef uint32_t ni2
        cdef CxxMorsel* out
        if self._cxx_ptr is not NULL:
            # Cxx-native: gather via the nogil C-ABI straight off the typed
            # memoryview — no Python-list round-trip through nanobind `.take`.
            ni2 = <uint32_t>indices.shape[0]
            with nogil:
                out = cxx_take_c(self._cxx_ptr, &indices[0] if ni2 > 0 else NULL, ni2)
            self._set_cxx(_cxx_owned_to_handle(out))
            return
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
        if self._cxx is not None:
            # Cxx-native: rebuild the substrate with the extra column appended.
            from draken.draken_native import cxx_morsel_from_vectors
            names = list(self._col_names)
            handles = [(<Vector>self._cxx_column(nm))._nb for nm in names]
            names.append(name)
            handles.append(wrapped._nb)
            self._set_cxx(cxx_morsel_from_vectors(handles, names))
            return
        self._col_names.append(name)
        self._append_column(wrapped)

    def column(self, name, fallback=None):
        self._ensure_pyobject()
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
        if self._cxx is not None:
            # Cxx-native: stay Cxx-backed (no materialization). Result columns share
            # the same owners; lazy materialization deferred to a real PyObject access.
            names = [n if isinstance(n, bytes) else n.encode("utf-8") for n in col_names]
            return Morsel.from_cxx(self._cxx.select(names))
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
        if self._cxx is not None:
            names = [n if isinstance(n, bytes) else n.encode("utf-8") for n in new_names]
            return Morsel.from_cxx(self._cxx.rename(names))
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
        cdef object mask_nb = (<Vector>mask)._nb if isinstance(mask, Vector) else mask
        cdef Py_ssize_t i
        cdef object nb_masked
        if self._cxx is not None:
            # Cxx-native: mask the whole substrate in ONE nanobind crossing
            # (cxx_mask derives surviving indices once, type-takes every column),
            # staying Cxx-backed. No per-column crossing, no materialization.
            # Zero-column morsels carry count_true, matching the PyObject path.
            return Morsel.from_cxx(self._cxx.mask(mask_nb))
        cdef Morsel result = _make_morsel()
        result._col_names = list(self._col_names)
        cdef Py_ssize_t n = self._num_columns()
        if n == 0:
            # No columns to gather — just carry the surviving row count.
            result._zero_col_num_rows = mask_nb.count_true()
            return result
        for i in range(n):
            nb_masked = self._get_column(i)._nb.mask(mask_nb)
            result._append_column(Vector(nb_masked))
        return result

    def take(self, indices):
        cdef int32_t[::1] iv
        cdef uint32_t nfast
        cdef CxxMorsel* out
        if self._cxx_ptr is not NULL and isinstance(indices, _ArrayType) \
                and (<object>indices).typecode == "i":
            # Typed fast path: gather via the nogil C-ABI straight off the int32
            # buffer (morsel_sort / join align output) — skips the nanobind
            # `.take(list)` round-trip. See the _ArrayType import note above.
            iv = indices
            nfast = <uint32_t>iv.shape[0]
            with nogil:
                out = cxx_take_c(self._cxx_ptr, &iv[0] if nfast > 0 else NULL, nfast)
            return cxx_to_morsel(shared_ptr[CxxMorsel](out))
        if self._cxx is not None:
            # Cxx-native: stay Cxx-backed (no materialization).
            return Morsel.from_cxx(self._cxx.take(list(indices)))
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

    def partition_by_hash(self, col_names, int n_bins):
        """Partition rows into `n_bins` sub-morsels by hash(col_names) bucket.

        The routing hash is the existing keying hash (the nogil `c_hash` path):
        for a single string key it is the 64-bit str_hash_seed (XXH3 for long
        values, folded slot words for short) run through the keying mix — NOT the
        slot's dead hash32 field (E37); for multi-column / fixed-width keys it mixes
        the per-column hashes. Identical keys always route to the same bin (per
        key value); NULL keys collide to one bin. `n_bins` MUST be a power of two
        (bucket = hash & (n_bins - 1)).

        Returns `n_bins` Morsels (some may be empty). The bins are a ROW-DISJOINT
        partition of self — every input row lands in exactly one bin and every
        instance of a group key lands in the same bin — so per-bin grouped
        aggregation needs NO cross-bin merge.
        """
        if n_bins < 1 or (n_bins & (n_bins - 1)) != 0:
            raise ValueError("partition_by_hash: n_bins must be a power of two")

        cdef bint is_cxx = self._cxx is not None
        cdef Py_ssize_t n = self.num_rows
        cdef Py_ssize_t ncols = self._num_columns()
        cdef uint64_t mask = <uint64_t>(n_bins - 1)
        cdef Py_ssize_t b, i
        cdef list result = []
        cdef Morsel sub

        if n == 0:
            for b in range(n_bins):
                if is_cxx:
                    sub = Morsel.from_cxx(self._cxx.take([]))
                else:
                    sub = _make_morsel()
                    sub._col_names = list(self._col_names)
                    sub._zero_col_num_rows = 0
                result.append(sub)
            return result

        # 1) per-row routing hashes — reuse the nogil c_hash path (slot hash for
        #    strings, mixed hash for multi-column).
        cdef int32_t n_cols = 0
        cdef int32_t* col_indices = self._resolve_columns_to_indices(col_names, &n_cols)
        cdef const DrakenVector** dvs
        try:
            dvs = self._columns_to_pointers(col_indices, n_cols)
        finally:
            free(col_indices)
        cdef uint64_t* hashes = <uint64_t*>calloc(<size_t>n, sizeof(uint64_t))
        if hashes == NULL:
            draken_free(dvs)
            raise MemoryError()
        cdef bint needs_gil
        try:
            needs_gil = self.c_hash(hashes, dvs, n_cols, n)
        finally:
            draken_free(dvs)
        if needs_gil:
            free(hashes)
            raise NotImplementedError(
                "partition_by_hash: column type cannot be hashed via this path"
            )

        # 2) counting-partition row indices into contiguous per-bin ranges (nogil).
        cdef int32_t* counts = <int32_t*>calloc(<size_t>n_bins, sizeof(int32_t))
        cdef int32_t* offsets = <int32_t*>malloc(<size_t>n_bins * sizeof(int32_t))
        cdef int32_t* cursor = <int32_t*>malloc(<size_t>n_bins * sizeof(int32_t))
        cdef int32_t* idx = <int32_t*>malloc(<size_t>n * sizeof(int32_t))
        if counts == NULL or offsets == NULL or cursor == NULL or idx == NULL:
            free(hashes); free(counts); free(offsets); free(cursor); free(idx)
            raise MemoryError()
        cdef uint64_t bucket
        with nogil:
            for i in range(n):
                counts[hashes[i] & mask] += 1
            offsets[0] = 0
            for b in range(1, n_bins):
                offsets[b] = offsets[b - 1] + counts[b - 1]
            for b in range(n_bins):
                cursor[b] = offsets[b]
            for i in range(n):
                bucket = hashes[i] & mask
                idx[cursor[bucket]] = <int32_t>i
                cursor[bucket] += 1
        free(hashes)
        free(cursor)

        # 3) gather each bin's contiguous index slice into a sub-morsel. The
        #    routing is representation-agnostic: a Cxx-backed morsel stays Cxx
        #    (native take, no materialization); a PyObject morsel takes natively.
        cdef int32_t start, blen
        cdef list bin_idx
        try:
            for b in range(n_bins):
                start = offsets[b]
                blen = counts[b]
                if is_cxx:
                    bin_idx = [idx[start + i] for i in range(blen)]
                    result.append(Morsel.from_cxx(self._cxx.take(bin_idx)))
                    continue
                sub = _make_morsel()
                sub._col_names = list(self._col_names)
                if ncols == 0:
                    sub._zero_col_num_rows = blen
                else:
                    for i in range(ncols):
                        sub._append_column(
                            _take_native(self._get_column(i), idx + start, <uint32_t>blen)
                        )
                result.append(sub)
        finally:
            free(counts); free(offsets); free(idx)
        return result

    def slice(self, Py_ssize_t offset=0, Py_ssize_t length=0, Py_ssize_t start=-1):
        cdef Py_ssize_t real_start = start if start >= 0 else offset
        if self._cxx is not None:
            # Cxx-native: stay Cxx-backed (no materialization).
            return Morsel.from_cxx(self._cxx.slice(<uint32_t>real_start, <uint32_t>length))
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
        cdef int32_t[::1] iv
        cdef uint32_t nfast
        cdef CxxMorsel* out
        cdef const CxxMorsel* p
        if self._cxx is not None:
            # Cxx-native: project (preserving morsel column order) and/or gather
            # by index off the substrate — no materialization. CxxMorsel is
            # immutable, so a no-arg copy shares the handle (mutators rebind).
            cxx_names = list(self._col_names)
            cxx = self._cxx
            if columns is not None:
                col_set = set(
                    c.encode("utf-8") if isinstance(c, str) else c for c in columns)
                cxx = cxx.select([nm for nm in cxx_names if nm in col_set])
            if mask is not None:
                if isinstance(mask, _ArrayType) and (<object>mask).typecode == "i":
                    # Typed fast path: gather off the int32 buffer via the nogil
                    # C-ABI — no Python-list round-trip through nanobind `.take`.
                    iv = mask
                    nfast = <uint32_t>iv.shape[0]
                    p = cxx_morsel_raw_ptr(<PyObject*>cxx)
                    with nogil:
                        out = cxx_take_c(p, &iv[0] if nfast > 0 else NULL, nfast)
                    cxx = _cxx_owned_to_handle(out)
                else:
                    cxx = cxx.take(list(mask))
            return Morsel.from_cxx(cxx)
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

        Raises ValueError if the morsels don't all share the same column names
        in the same order. Callers guarantee schema uniformity; every column
        read below is positional off the FIRST morsel's names, so a silent
        mismatch would concat the wrong columns together rather than fail.
        """
        if not morsels:
            return _make_morsel()
        first = <Morsel>morsels[0]
        if len(morsels) == 1:
            return first
        from draken.draken_native import vector_concat as _nb_concat
        # Column reads go through `_cxx_column`, which is representation-agnostic
        # (substrate when Cxx-backed, PyObject otherwise) — no materialization.
        cdef list names = list(first.column_names)
        cdef Py_ssize_t ncols = len(names)
        cdef Py_ssize_t nmorsels = len(morsels)
        cdef Py_ssize_t mi
        cdef list other_names
        for mi in range(1, nmorsels):
            other_names = list((<Morsel>morsels[mi]).column_names)
            if other_names != names:
                raise ValueError(
                    "Morsel.combine: all morsels must share one schema — "
                    f"morsel 0 has columns {names!r}, morsel {mi} has {other_names!r}"
                )
        cdef list out_vecs = []
        for col_idx in range(ncols):
            col_parts = [
                (<Vector>(<Morsel>morsels[mi])._cxx_column(names[col_idx]))._nb
                for mi in range(nmorsels)
            ]
            # The column name goes with the parts: a type mismatch here is only
            # actionable if the error says WHICH column disagreed.
            col_name = names[col_idx]
            out_vecs.append(_nb_concat(
                col_parts,
                col_name.decode("utf-8", "replace") if isinstance(col_name, bytes) else col_name,
            ))
        if first._cxx is not None:
            return cls.from_cxx_vectors(names, out_vecs)
        cdef Morsel result = _make_morsel()
        result._col_names = list(names)
        for col_idx in range(ncols):
            result._append_column(Vector(out_vecs[col_idx]))
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

    @classmethod
    def from_cxx(cls, cxx):
        """S1: build a Cxx-backed Morsel carrying a draken_native.CxxMorsel handle.
        `_columns`/`_col_names` stay empty until a PyObject accessor calls
        `_ensure_pyobject`, which materializes them byte-identically and clears `_cxx`."""
        cdef Morsel result = cls.__new__(cls)
        result._set_cxx(cxx)
        return result

    @classmethod
    def from_cxx_vectors(cls, col_names, col_vecs):
        """S1: same (names, vectors) signature as from_vectors, but produces a
        Cxx-backed Morsel — the column handles are folded into a CxxMorsel and the
        PyObject Vectors materialize lazily. Used by the scan under CXX_MORSEL_SCAN."""
        from draken.draken_native import cxx_morsel_from_vectors
        names = [n if isinstance(n, bytes) else n.encode("utf-8") for n in col_names]
        handles = [_unwrap(v) for v in col_vecs]
        return cls.from_cxx(cxx_morsel_from_vectors(handles, names))


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
    # Column reads go through `_cxx_column` (representation-agnostic) so the join
    # result is built directly off the substrate when inputs are Cxx-backed — no
    # whole-morsel materialization. Output preserves the Cxx representation when
    # either input carries it.
    cdef Py_ssize_t n = left_view.shape[0]
    cdef list left_names = list(left_morsel.column_names)
    cdef list right_names = list(right_morsel.column_names)
    cdef list out_names = []
    cdef list out_vecs = []
    cdef Py_ssize_t i, j
    cdef int ncols_left, ncols_right
    cdef bint left_has_neg, right_has_neg

    # Left columns: may contain -1 for unmatched right rows (RIGHT/FULL outer).
    left_has_neg = False
    for i in range(n):
        if left_view[i] < 0:
            left_has_neg = True
            break

    ncols_left = left_morsel._num_columns()
    cdef const int32_t* lidx = &left_view[0] if n > 0 else NULL
    for j in range(ncols_left):
        out_names.append(left_names[j])
        if not left_has_neg:
            out_vecs.append(_take_native(
                <Vector>left_morsel._cxx_column(left_names[j]), lidx, <uint32_t>n))
        else:
            out_vecs.append(_take_native_with_null(
                <Vector>left_morsel._cxx_column(left_names[j]), lidx, <uint32_t>n))

    # Right columns: may contain -1 for unmatched left rows (LEFT/FULL outer).
    right_has_neg = False
    for i in range(n):
        if right_view[i] < 0:
            right_has_neg = True
            break

    ncols_right = right_morsel._num_columns()
    cdef const int32_t* ridx = &right_view[0] if n > 0 else NULL
    for j in range(ncols_right):
        out_names.append(right_names[j])
        if not right_has_neg:
            out_vecs.append(_take_native(
                <Vector>right_morsel._cxx_column(right_names[j]), ridx, <uint32_t>n))
        else:
            # Unmatched-row path: native gather where right_view[i] < 0 → NULL row.
            # ALL types supported (incl. ARRAY / INTERVAL / VARBINARY).
            out_vecs.append(_take_native_with_null(
                <Vector>right_morsel._cxx_column(right_names[j]), ridx, <uint32_t>n))

    if left_morsel._cxx is not None or right_morsel._cxx is not None:
        return Morsel.from_cxx_vectors(out_names, out_vecs)
    cdef Morsel result = _make_morsel()
    result._col_names = out_names
    for j in range(len(out_vecs)):
        result._append_column(<Vector>out_vecs[j])
    return result


# =============================================================================
# MorselBatcher — the ONE place morsel batching policy lives.
# =============================================================================
# Every write sink and the cursor used to hand-roll this: hold a pending list,
# sum rows, flush at a threshold. Five copies, all of them row-only, and the
# byte axis missing from every one — which is how an OPTIMIZE pass over wide
# string rows piled 262144 rows into one Morsel.combine and got
# `concat: total arena bytes exceed 4 GB` out of concat_string. The row
# threshold cannot see payload width, so no value of it is safe.
#
# Two axes, both enforced here:
#
#   rows  — no emitted morsel exceeds `max_rows`. Oversized input is SPLIT
#           (slice), and batches are packed tight, so the single-row-group
#           guarantee the write path's bounds pruning depends on actually
#           holds. The sinks did not hold it before: they checked the
#           threshold BEFORE appending, so one oversized morsel went straight
#           through and produced a multi-row-group file with no bounds.
#
#   bytes — no COMBINED batch exceeds `max_arena_bytes` of projected string
#           arena in any single column. A lone morsel over the budget is
#           emitted as-is and that is provably safe: its arena already exists
#           with uint32 offsets, so it is under 4 GB by construction, and
#           Morsel.combine short-circuits a single-element list to the morsel
#           itself with no concat at all. Concat can only overflow when it
#           merges two or more, which is exactly what this budget bounds.
#
# Byte accounting is EXACT — per column, view-aware, matching concat_string's
# own counting pass byte for byte (see draken_vector_projected_arena_bytes).
#
# The cheap measure was tried first and is UNSAFE. Morsel.nbytes reports OWNED
# payload, sized by data_length; concat materializes every LOGICAL row, so a
# dict or constant string column is copied at its row count, not its unique
# count. Measured: a 5000-row dict column over 50 distinct 1 KB values owns
# 46 KB and hands concat 5 MB — a 100x UNDER-count, and dict-encoded strings
# are what the parquet scan produces by default. A budget built on that number
# admits a batch concat then refuses, which is the exact failure this class
# exists to prevent. Owned bytes are the right answer for memory pressure and
# the wrong answer for "how much will concat copy"; they are not a conservative
# bound in either direction.
#
# The exact walk is O(rows) per STRING column (zero for every other type) and
# runs once per pushed morsel — the same pass concat itself does, against a
# batch that is about to be encoded to parquet.
#
# Per column, not summed across columns: the 4 GB ceiling is per arena, and a
# summed budget over-splits a wide table by roughly its column count.

# The arena ceiling. FIXED, not tunable: DrakenStringSlot.ext.arena_offset is a
# uint32, so 4 GB is a hard property of the format, not a workload preference.
# 1 GiB leaves the slot table, validity and per-column headroom inside that,
# and keeps a flushed file a sane size. Changing this is a format-level
# decision, not configuration.
MORSEL_MAX_ARENA_BYTES = 1 << 30


cdef class MorselBatcher:
    """Coalesce and split a morsel stream into batches bounded by rows AND bytes.

    Streaming, not list-in/list-out: a batcher that takes the whole stream up
    front forces every caller to hold it in memory first, which is the failure
    this class exists to prevent. `push` returns the batches that became ready
    (usually none), `finish` returns the tail. Morsels are held by REFERENCE
    until emit; exactly one Morsel.combine runs per emitted batch, never an
    incremental concat per arrival (that re-copies the growing buffer every
    time and is quadratic in morsel count).

    Zero-row morsels are dropped. A caller that needs the engine's courtesy
    empty-schema morsel (the cursor does) must handle it outside the batcher:
    that is result semantics, not batching.
    """

    cdef Py_ssize_t max_rows
    cdef uint64_t max_arena_bytes
    cdef list _pending
    cdef Py_ssize_t _pending_rows
    # Per-column projected arena bytes of everything in `_pending`. Per column,
    # not summed: the 4 GB ceiling is per arena, and a summed budget over-splits
    # a wide table by roughly its column count.
    cdef vector[uint64_t] _pending_bytes

    def __init__(self, Py_ssize_t max_rows, max_arena_bytes=None):
        if max_rows <= 0:
            raise ValueError("MorselBatcher: max_rows must be positive")
        cdef uint64_t budget = (
            MORSEL_MAX_ARENA_BYTES if max_arena_bytes is None else <uint64_t>max_arena_bytes
        )
        if budget == 0 or budget > <uint64_t>MORSEL_MAX_ARENA_BYTES:
            raise ValueError(
                "MorselBatcher: max_arena_bytes must be in (0, "
                f"{MORSEL_MAX_ARENA_BYTES}] — the ceiling is the uint32 arena offset"
            )
        self.max_rows = max_rows
        self.max_arena_bytes = budget
        self._pending = []
        self._pending_rows = 0

    @property
    def pending_rows(self):
        return self._pending_rows

    def push(self, morsel):
        """Accept one morsel; return the list of batches that became ready."""
        cdef list ready = []
        cdef Py_ssize_t n, offset, space, take
        if morsel is None:
            return ready
        n = morsel.num_rows
        if n == 0:
            return ready
        offset = 0
        while offset < n:
            space = self.max_rows - self._pending_rows
            take = n - offset
            if take > space:
                take = space
            if offset == 0 and take == n:
                self._append(morsel, take, ready)
            else:
                self._append(morsel.slice(offset, take), take, ready)
            offset += take
        return ready

    def finish(self):
        """Emit whatever is buffered. Returns a list (empty if nothing pending)."""
        if not self._pending:
            return []
        return [self._emit()]

    @classmethod
    def defrag(cls, morsels, Py_ssize_t max_rows, max_arena_bytes=None):
        """One-shot form: a list of morsels in, a list of bounded batches out."""
        cdef MorselBatcher batcher = cls(max_rows, max_arena_bytes)
        cdef list out = []
        for morsel in morsels:
            out.extend(batcher.push(morsel))
        out.extend(batcher.finish())
        return out

    # ---- internals ----------------------------------------------------------

    cdef _append(self, object piece, Py_ssize_t rows, list ready):
        cdef vector[uint64_t] piece_bytes = self._measure(piece)
        if self._pending != [] and self._exceeds(piece_bytes):
            # Flush what we have; `piece` starts the next batch. It is never
            # split on the byte axis — see the class docstring.
            ready.append(self._emit())
        self._pending.append(piece)
        self._pending_rows += rows
        self._add(piece_bytes)
        if self._pending_rows >= self.max_rows:
            ready.append(self._emit())

    cdef object _emit(self):
        cdef object merged = Morsel.combine(self._pending)
        self._pending = []
        self._pending_rows = 0
        self._pending_bytes.clear()
        return merged

    cdef vector[uint64_t] _measure(self, object piece) except *:
        """Exact projected arena bytes, one entry per column."""
        cdef vector[uint64_t] out
        cdef list names = list(piece.column_names)
        cdef Py_ssize_t ncols = len(names)
        cdef Py_ssize_t i
        cdef Vector col
        out.reserve(ncols)
        for i in range(ncols):
            col = <Vector>(<Morsel>piece)._cxx_column(names[i])
            out.push_back(draken_vector_projected_arena_bytes(col._dv))
        return out

    cdef bint _exceeds(self, vector[uint64_t]& piece_bytes) except -1:
        cdef Py_ssize_t i
        if piece_bytes.size() != self._pending_bytes.size():
            # Schema disagreement — Morsel.combine is the one that must report
            # it, with the column names in the message.
            return False
        for i in range(<Py_ssize_t>piece_bytes.size()):
            if self._pending_bytes[i] + piece_bytes[i] > self.max_arena_bytes:
                return True
        return False

    cdef void _add(self, vector[uint64_t]& piece_bytes) except *:
        cdef Py_ssize_t i
        if self._pending_bytes.size() == 0:
            self._pending_bytes = piece_bytes
            return
        if piece_bytes.size() != self._pending_bytes.size():
            return
        for i in range(<Py_ssize_t>piece_bytes.size()):
            self._pending_bytes[i] = self._pending_bytes[i] + piece_bytes[i]
