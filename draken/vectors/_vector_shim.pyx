# cython: language_level=3
# Cython shim for draken.vectors.vector — provides __pyx_vtable__ for cimport consumers.
# E.24: _nb holds the nanobind handle; _dv is a borrowed pointer into it.

from cpython.object cimport PyObject
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, uint8_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libcpp.string cimport string
from libcpp.vector cimport vector

from draken.core.buffers cimport DrakenVector, DrakenType
from draken.core.buffers cimport (
    DRAKEN_ARRAY, DRAKEN_NULL, DRAKEN_VECTOR_FP16,
    DRAKEN_TIME32, DRAKEN_TIME64, DRAKEN_DECIMAL, DRAKEN_DECIMAL128,
)

# Shared native value renderer (draken/interop/value_format.hpp) — the SAME
# per-value formatting rugo's write_jsonl uses, so Vector._to_json() output
# matches the /download JSON format. Resolved via -I draken.
# Draken's logical-type vocabulary — imported, never copied (see CLAUDE.md §14).
cdef extern from "logical_type.h":
    cdef enum class LogicalKind(uint8_t):
        NONE
        TIMESTAMP
        TIME
        DECIMAL
        VECTOR
        IPV4

    cdef enum class TimestampUnit(uint8_t):
        SECONDS
        MILLISECONDS
        MICROSECONDS
        NANOSECONDS

    # The SINGLE source of the physical+descriptor -> SQL name mapping. Called
    # rather than reimplemented: opteryx's ColumnType delegates to the same
    # function, so a name cannot differ between the two surfaces.
    string type_display_name_parts(DrakenType physical, LogicalKind kind,
                                   TimestampUnit unit, uint8_t precision,
                                   uint8_t scale, uint32_t dimension)

cdef extern from "interop/value_format.hpp" namespace "rugo_text" nogil:
    # One descriptor per column carries every logical-type field the renderer
    # needs — including the KIND, which is the only thing that distinguishes an
    # IPv4 column from the plain UINT32 it is physically identical to.
    cdef struct LogicalDesc:
        LogicalKind kind
        int unit
        int scale
        int dim

    # One nesting level below a column's own ARRAY-ness — see ColumnDesc and
    # render_json_value in value_format.hpp for how these chain for
    # ARRAY<ARRAY<...>>.
    cdef struct ArrayLevel:
        const DrakenVector* vec
        LogicalDesc desc

    cdef struct ColumnDesc:
        LogicalDesc column
        vector[ArrayLevel] levels

    void render_json_column(string& out, const DrakenVector* dv,
                            const ColumnDesc& desc, size_t nrows)


def type_display_name(physical, kind=None, unit=None, precision=0, scale=0,
                      dimension=0):
    """SQL type name for a physical tag plus optional descriptor parts.

    The module-level entry point onto draken's single source of that mapping, for
    callers that hold a type description rather than a vector — opteryx's
    ColumnType delegates here so there is one table of type names, not two either
    side of the module boundary.

    `physical` is a DrakenType; `kind` a LogicalKind (or None for no descriptor);
    `unit` the temporal unit as 's'/'ms'/'us'/'ns'. Returns '' for a physical tag
    with no name, which the caller should treat as an error rather than print.
    """
    cdef LogicalKind k = LogicalKind.NONE
    cdef TimestampUnit u = TimestampUnit.MICROSECONDS
    if kind is not None and int(kind.value) != 0:
        k = <LogicalKind><int>kind.value
    if unit is not None:
        u = <TimestampUnit><int>_unit_code(unit)
    cdef string out = type_display_name_parts(
        <DrakenType>physical.value, k, u,
        <uint8_t>(precision or 0), <uint8_t>(scale or 0),
        <uint32_t>(dimension or 0))
    return out.decode("utf-8")


cdef inline int _unit_code(object u):
    # Temporal unit → code understood by the renderer (s=0, ms=1, us=2, ns=3).
    if u == "s": return 0
    if u == "ms": return 1
    if u == "ns": return 3
    return 2  # us / default


cdef inline void _fill_logical_desc(LogicalDesc* d, object nb) except *:
    # Pack one nanobind Vector handle's logical type into `d`. No descriptor
    # leaves `d` at its C++ defaults (kind NONE, no parameters).
    cdef object kind = nb.logical_type_kind
    cdef object unit
    cdef object scale
    cdef object dim
    if kind is None:
        return
    d.kind = <LogicalKind><int>kind.value
    unit = nb.logical_type_unit
    if unit is not None:
        d.unit = _unit_code(unit)
    scale = nb.logical_type_scale
    if scale is not None:
        d.scale = <int>scale
    dim = nb.logical_type_dimension
    if dim is not None:
        d.dim = <int>dim

# Native row-hash kernel (header-only static inline in ops/hash.h). Used by
# c_hash_single to fill a caller buffer with zero Python object creation.
cdef extern from "ops/hash.h" nogil:
    void draken_hash(const DrakenVector& v, uint64_t* out, uint32_t n)
    void draken_hash_distinct(const DrakenVector& v, uint64_t* out)

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)
    PyObject* draken_vector_own_raw(void* data, uint8_t* validity, uint32_t length, DrakenType dtype)
    PyObject* draken_vector_own_dict_i64(void* data, uint32_t data_length,
                                          uint32_t* codes, uint32_t length,
                                          uint8_t* validity)
    PyObject* draken_vector_own_dict(void* data, uint32_t data_length,
                                      uint32_t* codes, uint32_t length,
                                      uint8_t* validity, DrakenType dtype)

cdef extern from *:
    """static inline void _vec_shim_decref(PyObject* op) { Py_DECREF(op); }"""
    void _vec_shim_decref(PyObject* op)

# Arrow C Data Interface export — defined in draken/interop/draken_to_arrow.h.
# Fills ArrowArray + ArrowSchema from a DrakenVector; returns true on success.
# Caller passes &arr and &schema to pa.Array._import_from_c().
cdef extern from "interop/draken_to_arrow.h" nogil:
    ctypedef struct ArrowArray:
        pass
    ctypedef struct ArrowSchema:
        pass
    bint draken_export_to_arrow(
        const DrakenVector* dv,
        ArrowArray*         out_array,
        ArrowSchema*        out_schema,
    ) except +


# Types draken_export_to_arrow() (interop/draken_to_arrow.h) NEVER supports,
# regardless of shape -- see its trailing "DECIMAL, DECIMAL128, TIME32, TIME64,
# ARRAY, FP16 -- not supported here" branch. to_arrow() falls back to
# pa.array(to_pylist()) for these, which is fine when real values are present
# (pyarrow infers the type from them) but breaks when the list is empty or
# all-NULL -- pyarrow has nothing to infer from and silently produces
# pa.null(), erasing the declared type. build_arrow_type_for() below resolves
# the pyarrow type explicitly from the vector's own descriptor instead.
def build_arrow_type_for(nb_desc, pa):
    """Explicit pyarrow type for `nb_desc`'s (a draken_native Vector) declared
    DrakenType -- used by to_arrow()'s fallback so type information survives
    even with zero (or all-NULL) rows. Also used for DECIMAL/DECIMAL128 even
    when non-empty: pyarrow's own inference sizes precision off the digits
    actually present in the Decimal values, not the column's declared
    precision/scale, which is wrong and can vary morsel to morsel.

    Covers every DrakenType, not just the 5 gap types above, because an
    ARRAY's child can be any physical type (recursion below).
    """
    from draken.draken_native import DrakenType as _DT
    dt = nb_desc.type
    if dt == _DT.INT8:          return pa.int8()
    if dt == _DT.INT16:         return pa.int16()
    if dt == _DT.INT32:         return pa.int32()
    if dt == _DT.INT64:         return pa.int64()
    if dt == _DT.UINT8:         return pa.uint8()
    if dt == _DT.UINT16:        return pa.uint16()
    if dt == _DT.UINT32:        return pa.uint32()
    if dt == _DT.UINT64:        return pa.uint64()
    if dt == _DT.FLOAT32:       return pa.float32()
    if dt == _DT.FLOAT64:       return pa.float64()
    if dt == _DT.BOOL:          return pa.bool_()
    if dt == _DT.DATE32:        return pa.date32()
    if dt == _DT.TIMESTAMP64:   return pa.timestamp("us")
    if dt in (_DT.VARCHAR, _DT.NVARCHAR, _DT.VARIANT):
        return pa.string()
    if dt == _DT.VARBINARY:     return pa.binary()
    if dt == _DT.INTERVAL:      return pa.month_day_nano_interval()
    if dt == _DT.NULL:          return pa.null()
    if dt in (_DT.DECIMAL, _DT.DECIMAL128):
        precision = nb_desc.logical_type_precision
        scale = nb_desc.logical_type_scale
        if precision is None or scale is None:
            raise RuntimeError(f"{dt}: vector missing precision/scale descriptor")
        return pa.decimal128(precision, scale)
    if dt == _DT.TIME32:
        unit = nb_desc.logical_type_unit
        if unit is None:
            raise RuntimeError("TIME32: vector missing unit descriptor")
        return pa.time32(unit)
    if dt == _DT.TIME64:
        unit = nb_desc.logical_type_unit
        if unit is None:
            raise RuntimeError("TIME64: vector missing unit descriptor")
        return pa.time64(unit)
    if dt == _DT.VECTOR_FP16:
        # to_pylist() decodes fp16 rows to plain Python float lists (double
        # precision), never pa.float16() elements -- match that shape.
        return pa.list_(pa.float64())
    if dt == _DT.ARRAY:
        child_dt = nb_desc.array_child_type
        if child_dt is None:
            raise RuntimeError("ARRAY: vector missing child type descriptor")
        return pa.list_(build_arrow_type_for(nb_desc.array_child, pa))
    raise RuntimeError(f"build_arrow_type_for: unmapped DrakenType {dt}")


cdef class Vector:
    def __cinit__(self, object nb_vector=None):
        if nb_vector is None or isinstance(nb_vector, int):
            # int form: BoolVector(n) compat — subclass __cinit__ sets _nb/_dv
            self._nb = None
            self._dv = NULL
        else:
            self._nb = nb_vector
            self._dv = draken_vector_unwrap(<PyObject*>nb_vector)

    @property
    def length(self):
        return self._nb.length

    @property
    def type(self):
        return self._nb.type

    @property
    def type_name(self):
        """The SQL type name, descriptor included — 'IPV4', 'DECIMAL(10, 2)'.

        NOT the same as `type.name`, which is the PHYSICAL tag: an IPv4 column is
        physically UINT32 and a DECIMAL(10,2) is physically DECIMAL, so naming a
        column from `.type` alone reports a different type than the column has.
        Computed by draken's type_display_name_parts, the one place that mapping
        lives.
        """
        cdef object kind = self._nb.logical_type_kind
        cdef LogicalKind k = LogicalKind.NONE
        cdef TimestampUnit u = TimestampUnit.MICROSECONDS
        cdef uint8_t precision = 0
        cdef uint8_t scale = 0
        cdef uint32_t dimension = 0
        cdef object v
        if kind is not None:
            k = <LogicalKind><int>kind.value
            v = self._nb.logical_type_unit
            if v is not None:
                u = <TimestampUnit><int>_unit_code(v)
            v = self._nb.logical_type_precision
            if v is not None:
                precision = <uint8_t>v
            v = self._nb.logical_type_scale
            if v is not None:
                scale = <uint8_t>v
            v = self._nb.logical_type_dimension
            if v is not None:
                dimension = <uint32_t>v
        cdef string out = type_display_name_parts(
            <DrakenType>self._nb.type.value, k, u, precision, scale, dimension)
        if out.size() == 0:
            raise NotImplementedError(
                f"no display name for physical type {self._nb.type!r}")
        return out.decode("utf-8")

    @property
    def data_length(self):
        return self._nb.data_length

    @property
    def is_dense(self):
        return self._nb.data_length == self._nb.length

    @property
    def is_compressed(self):
        return self._nb.data_length < self._nb.length

    @property
    def is_constant(self):
        return self._nb.data_length == 1

    @property
    def is_dict(self):
        return 1 < self._nb.data_length < self._nb.length

    def __len__(self):
        return self._nb.length

    def __repr__(self):
        cdef int n = self._nb.length
        cdef list vals = self._nb.to_pylist()
        cdef str type_name = str(self._nb.type).split(".")[-1]
        if n <= 10:
            return f"Vector<{type_name}>[{', '.join(repr(v) for v in vals)}]"
        preview = vals[:5]
        tail = vals[n - 2:]
        return (
            f"Vector<{type_name}>[{', '.join(repr(v) for v in preview)}, "
            f"... ({n - 7} more) ..., "
            f"{', '.join(repr(v) for v in tail)}]"
        )

    def __getitem__(self, int idx):
        return self._nb[idx]

    def take(self, indices):
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.take(list(indices)))

    def to_float64_vector(self):
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.to_float64())

    def _compare_scalar(self, value, int op):
        from draken.vectors.bool_vector import BoolVector
        return BoolVector(self._nb.compare_scalar(value, op))

    def _compare_vector(self, other, int op):
        from draken.vectors.bool_vector import BoolVector
        cdef object other_nb = other._nb if isinstance(other, Vector) else other
        return BoolVector(self._nb.compare_vector(other_nb, op))

    def equals_vector(self, other):
        return self._compare_vector(other, 0)

    def not_equals_vector(self, other):
        return self._compare_vector(other, 1)

    def greater_than_vector(self, other):
        return self._compare_vector(other, 2)

    def greater_than_or_equals_vector(self, other):
        return self._compare_vector(other, 3)

    def less_than_vector(self, other):
        return self._compare_vector(other, 4)

    def less_than_or_equals_vector(self, other):
        return self._compare_vector(other, 5)

    def _compare_float64_vector(self, other, int op):
        from draken.vectors.bool_vector import BoolVector
        cdef object other_nb = other._nb if isinstance(other, Vector) else other
        return BoolVector(self._nb.compare_vector(other_nb, op))

    def _compare_vector_op(self, other, int op):
        from draken.vectors.bool_vector import BoolVector
        cdef object other_nb = other._nb if isinstance(other, Vector) else other
        return BoolVector(self._nb.compare_vector(other_nb, op))

    def between(self, lower, upper, lower_inclusive=True, upper_inclusive=True):
        from draken.vectors.bool_vector import BoolVector
        return BoolVector(self._nb.between(lower, upper, lower_inclusive, upper_inclusive))

    def in_list(self, values):
        from draken.vectors.bool_vector import BoolVector
        return BoolVector(self._nb.in_list(values))

    def hash(self):
        return self._nb.hash()

    def hash_shaped(self):
        # Shape-preserving keying hash: returns an INT64 Vector that mirrors this
        # vector's shape (dict→dict, dense→dense). See draken_hash_shaped.
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.hash_shaped())

    def unique(self):
        # First-occurrence-index permutation (INT32 Vector), only supported
        # on a hash_shaped() output. See draken_native.cpp's "unique" binding.
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.unique())

    def sum(self):
        return self._nb.sum()

    def min(self):
        return self._nb.min()

    def max(self):
        return self._nb.max()

    def null_count(self):
        # Native null-row count (validity-bitmap popcount) -- see this
        # class's own is_null() below for the slow, still-unfixed shim this
        # deliberately does NOT go through.
        return self._nb.null_count()

    def ordinal_min_max(self):
        # min/max of THIS vector treated as ordinalize()'s own INT64 output --
        # correctly excludes the ORDINAL_NULL sentinel ordinalize() bakes into
        # the data for null rows (its output has no validity bitmap of its
        # own). Do not call .min()/.max() directly on ordinalize() output if
        # the column can contain nulls -- see draken_native.cpp's
        # ordinal_min_max binding for the full contract.
        return self._nb.ordinal_min_max()

    def histogram_bucket(self, vmin, vmax, n_bins=32):
        # Bucket THIS ordinalize()'d INT64 vector into n_bins equi-width bins
        # given vmin/vmax from ordinal_min_max(). Excludes ORDINAL_NULL rows.
        return self._nb.histogram_bucket(vmin, vmax, n_bins)

    def char_class_stats(self):
        # Per-column byte-class histogram/total-bytes/length-range for a
        # VARCHAR/NVARCHAR/VARBINARY vector -- see draken_native.cpp's
        # char_class_stats binding for the class table and contract.
        return self._nb.char_class_stats()

    def is_null_at(self, idx):
        return self._nb.is_null_at(idx)

    def compare_at(self, i, j):
        return self._nb.compare_at(i, j)

    def null_bitmap(self):
        if self._dv == NULL:
            return None
        if self._dv.validity == NULL:
            return None
        cdef Py_ssize_t n_bytes = (self._dv.length + 7) // 8
        return bytes((<uint8_t*>self._dv.validity)[:n_bytes])

    def is_null(self):
        cdef list vals = self._nb.to_pylist()
        cdef Py_ssize_t n = len(vals)
        cdef bytearray result = bytearray(n)
        cdef Py_ssize_t i
        for i in range(n):
            if vals[i] is None:
                result[i] = 1
        return result

    def to_pylist(self):
        return self._nb.to_pylist()

    def _to_json(self):
        """Serialize this column's values to a JSON array as one ``bytes``.

        Returns ``b"[v0,v1,…]"`` rendered natively, with no per-value Python
        object creation. The per-value formatting is the SAME code rugo's
        ``write_jsonl`` uses (``render_json_scalar``), so the output matches the
        ``/download`` JSON format: timestamps as RFC-3339 ``+00:00``, decimals
        scaled by the column's ``logical_type_scale``, NaN/Inf and nulls as
        ``null``. Honours the ``data[selection[i]]`` indirection, so dense,
        dict, constant and sliced columns all render correctly. ARRAY columns
        nest to whatever depth the data actually has (ARRAY<ARRAY<T>> etc.) —
        see ColumnDesc/render_json_value in value_format.hpp.
        """
        cdef const DrakenVector* dv = self._dv
        if dv == NULL:
            return b"[]"
        cdef ColumnDesc desc      # C++ defaults: kind NONE, no levels
        cdef list child_vecs = []   # keep each level's Vector alive: desc.levels borrows its _dv
        cdef Vector cur = self
        cdef Vector cv
        cdef ArrayLevel lvl
        cdef string out
        # The logical type lives on the nanobind descriptor, not the
        # DrakenVector ABI (see rugo _text_render.pxi for the same reads).
        _fill_logical_desc(&desc.column, self._nb)
        # Walk the ARRAY nesting chain to whatever depth the data actually
        # has, one level per iteration — mirrors row_array_to_pylist's
        # descent through VectorOwner::child_owner in draken_native.cpp.
        while cur._dv != NULL and cur._dv.type == DRAKEN_ARRAY and cur._nb.array_child_type is not None:
            cv = Vector(cur._nb.array_child)
            child_vecs.append(cv)
            lvl.vec = cv._dv
            lvl.desc.kind = LogicalKind.NONE
            lvl.desc.unit = 0
            lvl.desc.scale = 0
            lvl.desc.dim = 0
            _fill_logical_desc(&lvl.desc, cv._nb)
            desc.levels.push_back(lvl)
            cur = cv
        render_json_column(out, dv, desc, <size_t>dv.length)
        return PyBytes_FromStringAndSize(out.data(), out.size())

    def to_arrow(self):
        """Convert this Vector to a pyarrow.Array via the Arrow C Data Interface.

        Dense numeric/bool/string/interval types are translated in C++ without
        going through Python object boxing. Dict, constant, DECIMAL, DECIMAL128,
        TIME32/64, ARRAY and VECTOR_FP16 fall back to to_pylist(); of those,
        DECIMAL, DECIMAL128, TIME32/64, ARRAY and VECTOR_FP16 use an explicit
        pyarrow type from the vector's own descriptor (build_arrow_type_for)
        rather than value inference, so an empty or all-NULL result keeps its
        declared type (plain dict/constant-encoded ordinary types still rely on
        inference — they always carry real values whenever they reach this
        fallback at all).
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError("to_arrow() requires pyarrow: pip install pyarrow")
        cdef const DrakenVector* dv = self._dv
        if dv == NULL:
            return pa.array([], type=pa.null())
        cdef ArrowArray  arr
        cdef ArrowSchema schema
        if draken_export_to_arrow(dv, &arr, &schema):
            return pa.Array._import_from_c(<size_t>&arr, <size_t>&schema)
        # Fallback: types not supported by the C++ exporter (dict, constant,
        # DECIMAL, DECIMAL128, TIME32/64, ARRAY, VECTOR_FP16).
        #
        # DECIMAL, DECIMAL128, TIME32/64, ARRAY and VECTOR_FP16 land here
        # regardless of shape -- every dense/dict/constant vector of these
        # types, not just dict/constant-encoded ones. For those, resolve the
        # pyarrow type explicitly from the vector's own descriptor
        # (build_arrow_type_for) instead of letting pa.array() infer it from
        # to_pylist() -- inference silently collapses to pa.null() when the
        # list is empty or all-NULL, and for DECIMAL/DECIMAL128 is wrong even
        # when non-empty (see build_arrow_type_for's docstring). Plain
        # dict/constant-encoded columns of ordinary types (VARCHAR, INT64, ...)
        # keep relying on inference here -- their to_pylist() carries real
        # values whenever they reach this fallback at all.
        if dv.type == DRAKEN_TIME32 or dv.type == DRAKEN_TIME64 \
                or dv.type == DRAKEN_DECIMAL or dv.type == DRAKEN_DECIMAL128 \
                or dv.type == DRAKEN_ARRAY or dv.type == DRAKEN_VECTOR_FP16:
            return pa.array(self._nb.to_pylist(), type=build_arrow_type_for(self._nb, pa))
        return pa.array(self._nb.to_pylist())

    def materialize(self):
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.materialize())

    def ordinalize(self):
        # Renamed from `compress` (2026-07-30) to disambiguate from the two
        # OTHER things called "compress" elsewhere in draken: the native
        # Vector.compress() (dict-encode for most types / drop-nulls for
        # ARRAY-FP16-NULL) and the never-implemented `compress(mask)` in
        # README.md. Converts values to monotonic int64 ORDINAL keys -- used
        # by opteryx_catalog's manifest builder (via rugo's Morsel -> this
        # shim Vector) to compute per-column min/max/histogram bins over
        # Tb-scale data.
        #
        # Native kernel (draken/ops/ordinalize.h), not a Python loop: this
        # was a to_pylist()-boxing shim per the same "no Python on the
        # execution path" rule as the rest of draken (.claude/CLAUDE.md §2)
        # -- and it never supported strings at all (see ordinalize.h's
        # VARCHAR/NVARCHAR/VARBINARY/VARIANT kernel, which this delegate
        # now covers along with every other type in the dispatch table).
        #
        # Returns a shape-preserving native INT64 Vector (dict-compressed
        # input stays compressed -- only the data_length distinct values are
        # ordinalized, not every row), matching sort.pyx's expectation of a
        # comparable int64 key per logical position via .to_pylist()/.data.
        # Same wrapping convention as hash_shaped() above.
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.ordinalize())

    cdef DrakenVector* unified(self) noexcept:
        return <DrakenVector*>self._dv

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        return self._dv.validity

    cdef bint c_hash_single(self, uint64_t* out, int32_t n) except -1 nogil:
        # Hash directly into the caller buffer via the C++ kernel — fully nogil,
        # zero Python object creation. Unsupported key types (array/null/fp16)
        # fail loudly rather than degrading to a per-row Python hash path.
        cdef DrakenVector* dv = <DrakenVector*>self._dv
        if (dv.type == DRAKEN_ARRAY or dv.type == DRAKEN_NULL
                or dv.type == DRAKEN_VECTOR_FP16):
            with gil:
                raise TypeError(
                    "c_hash_single: unsupported key vector type %d "
                    "(array/null/fp16 cannot be row-hashed)" % <int>dv.type)
        draken_hash(dv[0], out, <uint32_t>n)
        return 0

    cdef bint c_hash_distinct(self, uint64_t* out) except -1 nogil:
        # Hash the data_length DISTINCT values of this vector into out[0..data_length).
        # Reuses the same per-type kernel as c_hash_single via a dense view, so a
        # value's hash is byte-identical whether it arrives dense or dict-shaped —
        # the cross-shape invariant COUNT(DISTINCT) relies on to dedup across
        # morsels. Caller reads data_length from the DrakenVector. Same type guard
        # as c_hash_single: array/null/fp16 cannot be row-hashed.
        cdef DrakenVector* dv = <DrakenVector*>self._dv
        if (dv.type == DRAKEN_ARRAY or dv.type == DRAKEN_NULL
                or dv.type == DRAKEN_VECTOR_FP16):
            with gil:
                raise TypeError(
                    "c_hash_distinct: unsupported key vector type %d "
                    "(array/null/fp16 cannot be row-hashed)" % <int>dv.type)
        draken_hash_distinct(dv[0], out)
        return 0

    @property
    def array_child(self):
        """Return the child Vector of a DRAKEN_ARRAY vector as a Python Vector."""
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.array_child)


cdef Vector dict_int64_from_decoded(void* dict_vals, uint32_t data_length,
                                     uint32_t* codes, uint32_t length,
                                     uint8_t* validity):
    """Create a dict-encoded int64 Vector from hand-allocated (draken_malloc) buffers.

    dict_vals:    draken_malloc'd int64_t[data_length] unique values (dictionary).
    data_length:  number of unique values.
    codes:        draken_malloc'd uint32_t[length] per-row codes.
    length:       logical row count.
    validity:     draken_malloc'd null bitmap (1-bit-per-row), or NULL.
    All non-NULL buffers MUST be draken_malloc'd; ownership is transferred on call.
    """
    cdef PyObject* raw = draken_vector_own_dict_i64(dict_vals, data_length, codes, length, validity)
    if raw == NULL:
        raise MemoryError("draken_vector_own_dict_i64 failed")
    cdef Vector result = Vector.__new__(Vector)
    result._nb = <object>raw
    _vec_shim_decref(raw)
    result._dv = draken_vector_unwrap(raw)
    return result


cdef Vector dict_from_decoded(void* dict_vals, uint32_t data_length,
                              uint32_t* codes, uint32_t length,
                              uint8_t* validity, DrakenType dtype):
    """Create a dict-encoded Vector of `dtype` from hand-allocated (draken_malloc)
    buffers. Generic analogue of dict_int64_from_decoded (E33: added for
    DRAKEN_UINT8/16/32/64, which have no fixed elem size the way int64/float64/
    float32 do — the caller's dict_vals buffer must already be laid out at
    dtype's native elem size). All non-NULL buffers MUST be draken_malloc'd;
    ownership is transferred on call.
    """
    cdef PyObject* raw = draken_vector_own_dict(dict_vals, data_length, codes, length, validity, dtype)
    if raw == NULL:
        raise MemoryError("draken_vector_own_dict failed")
    cdef Vector result = Vector.__new__(Vector)
    result._nb = <object>raw
    _vec_shim_decref(raw)
    result._dv = draken_vector_unwrap(raw)
    return result


cdef Vector from_decoded(void* data, uint8_t* validity, uint32_t length, DrakenType dtype):
    """Create a dense Vector wrapping hand-allocated (draken_malloc) buffers.

    data and validity MUST have been allocated with draken_malloc; ownership
    is transferred to the new Vector on success (draken_free'd on GC).
    validity may be NULL (all-valid normalization invariant).
    Analogous to from_decoded in _bool_vector_shim.pyx.
    """
    cdef PyObject* raw = draken_vector_own_raw(data, validity, length, dtype)
    if raw == NULL:
        raise MemoryError("draken_vector_own_raw failed")
    cdef Vector result = Vector.__new__(Vector)
    result._nb = <object>raw   # Cython incref → refcount = 2
    _vec_shim_decref(raw)      # balance the NEW ref → refcount = 1
    result._dv = draken_vector_unwrap(raw)
    return result
