# cython: language_level=3
# Cython shim for draken.vectors.vector — provides __pyx_vtable__ for cimport consumers.
# E.24: _nb holds the nanobind handle; _dv is a borrowed pointer into it.

from cpython.object cimport PyObject
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, uint8_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libcpp.string cimport string

from draken.core.buffers cimport DrakenVector, DrakenType
from draken.core.buffers cimport DRAKEN_ARRAY, DRAKEN_NULL, DRAKEN_VECTOR_FP16

# Shared native value renderer (draken/interop/value_format.hpp) — the SAME
# per-value formatting rugo's write_jsonl uses, so Vector._to_json() output
# matches the /download JSON format. Resolved via -I draken.
cdef extern from "interop/value_format.hpp" namespace "rugo_text" nogil:
    void render_json_column(string& out, const DrakenVector* dv,
                            const DrakenVector* child, int unit, int scale,
                            int cunit, int cscale, size_t nrows)


cdef inline int _unit_code(object u):
    # Temporal unit → code understood by the renderer (s=0, ms=1, us=2, ns=3).
    if u == "s": return 0
    if u == "ms": return 1
    if u == "ns": return 3
    return 2  # us / default

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

    def sum(self):
        return self._nb.sum()

    def min(self):
        return self._nb.min()

    def max(self):
        return self._nb.max()

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
        dict, constant and sliced columns all render correctly.
        """
        cdef const DrakenVector* dv = self._dv
        if dv == NULL:
            return b"[]"
        cdef const DrakenVector* child = NULL
        cdef int unit = 0, scale = 0, cunit = 0, cscale = 0
        cdef Vector cv
        cdef string out
        # unit/scale live on the nanobind logical descriptor, not the
        # DrakenVector ABI (see rugo _jsonl_writer.pyx for the same reads).
        u = self._nb.logical_type_unit
        if u is not None:
            unit = _unit_code(u)
        sc = self._nb.logical_type_scale
        if sc is not None:
            scale = <int>sc
        if dv.type == DRAKEN_ARRAY and self._nb.array_child_type is not None:
            cv = Vector(self._nb.array_child)
            child = cv._dv
            cu = cv._nb.logical_type_unit
            if cu is not None:
                cunit = _unit_code(cu)
            csc = cv._nb.logical_type_scale
            if csc is not None:
                cscale = <int>csc
        render_json_column(out, dv, child, unit, scale, cunit, cscale, <size_t>dv.length)
        return PyBytes_FromStringAndSize(out.data(), out.size())

    def to_arrow(self):
        """Convert this Vector to a pyarrow.Array via the Arrow C Data Interface.

        Dense numeric/bool/string/interval types are translated in C++ without
        going through Python object boxing.  Dict, constant, TIME, DECIMAL128,
        and ARRAY fall back to to_pylist().
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
        # TIME32/64, DECIMAL128, ARRAY).
        return pa.array(self._nb.to_pylist())

    def materialize(self):
        from draken.vectors.vector import Vector as _V
        return _V(self._nb.materialize())

    def compress(self):
        # sort.pyx expects int64_t[::1] memoryview — sortable int64 keys.
        # For E.24 shim: convert to int64 sort keys via to_pylist().
        # Keys only need to be order-preserving (monotonic), not the exact
        # stored representation — so temporal values map to an epoch int.
        import struct
        import datetime as _dt
        from array import array as _array
        vals = self._nb.to_pylist()
        type_name = self._nb.type.name
        keys = []
        if type_name in ("FLOAT32", "FLOAT64"):
            for v in vals:
                if v is None:
                    keys.append(-0x8000000000000000)
                else:
                    # IEEE 754 bit cast to sortable int64
                    bits = struct.unpack('Q', struct.pack('d', float(v)))[0]
                    if bits & 0x8000000000000000:
                        bits ^= 0xFFFFFFFFFFFFFFFF
                    keys.append(bits & 0x7FFFFFFFFFFFFFFF)
        else:
            for v in vals:
                if v is None:
                    keys.append(-0x8000000000000000)
                elif isinstance(v, bool):
                    keys.append(1 if v else 0)
                elif isinstance(v, _dt.datetime):
                    # microseconds since epoch — monotonic, fits int64
                    keys.append(int(v.timestamp() * 1_000_000))
                elif isinstance(v, _dt.date):
                    keys.append(v.toordinal())
                elif isinstance(v, _dt.timedelta):
                    keys.append(int(v.total_seconds() * 1_000_000))
                else:
                    keys.append(int(v))
        return _array('q', keys)

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
