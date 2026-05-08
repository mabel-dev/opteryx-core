# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
ArrayVector: Native list/array vector for Draken using DrakenArrayBuffer storage.

Runtime operations avoid PyArrow entirely; only the conversion helpers interact
with Arrow buffers for zero-copy interop. Offsets/null buffers live in C memory
and a flattened child Vector stores nested values.
"""

from cpython.buffer cimport PyBUF_READ
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING
from cpython.exc cimport PyErr_Occurred
from cpython.long cimport PyLong_AsSsize_t
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.memoryview cimport PyMemoryView_FromMemory
from cpython.ref cimport Py_DECREF
from cpython.sequence cimport PySequence_Fast
from cpython.sequence cimport PySequence_Fast_GET_ITEM, PySequence_Fast_GET_SIZE

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, int8_t, intptr_t, uint8_t, uint64_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset

from draken.core.buffers cimport (
    DrakenArrayBuffer,
    DRAKEN_ARRAY,
    DRAKEN_NON_NATIVE,
    DRAKEN_STRING,
)
from draken.vectors.string_vector cimport StringVector
from draken.interop.arrow cimport arrow_type_to_draken, vector_from_arrow
from draken.interop.vector_sequence cimport vector_from_sequence
from draken.vectors.vector cimport (
    MIX_HASH_CONSTANT,
    NULL_HASH,
    Vector,
    mix_hash,
    simd_mix_hash,
    simd_popcount,
)

DEF ARRAY_HASH_CHUNK = 64


cdef inline DrakenArrayBuffer* _alloc_array_buffer() except *:
    cdef DrakenArrayBuffer* buf = <DrakenArrayBuffer*> malloc(sizeof(DrakenArrayBuffer))
    if buf == NULL:
        raise MemoryError()
    buf.offsets = NULL
    buf.values = NULL
    buf.null_bitmap = NULL
    buf.length = 0
    buf.value_type = DRAKEN_NON_NATIVE
    return buf


cdef inline bint _row_is_null(DrakenArrayBuffer* ptr, Py_ssize_t idx) nogil:
    if ptr == NULL or ptr.null_bitmap == NULL:
        return False
    return ((ptr.null_bitmap[idx >> 3] >> (idx & 7)) & 1) == 0


cdef class ArrayVector(Vector):

    def __cinit__(self):
        self.ptr = NULL
        self._child = None
        self.owns_offsets = False
        self.owns_null_bitmap = False
        self._arrow_parent = None
        self._arrow_offsets_buf = None
        self._arrow_null_buf = None
        self._arrow_child_array = None
        self._child_arrow_type = None
        self._child_decode_utf8 = False

    def __dealloc__(self):
        if self.ptr != NULL:
            if self.owns_offsets and self.ptr.offsets != NULL:
                free(self.ptr.offsets)
            if self.owns_null_bitmap and self.ptr.null_bitmap != NULL:
                free(self.ptr.null_bitmap)
            free(self.ptr)
            self.ptr = NULL

    @property
    def length(self):
        return 0 if self.ptr == NULL else <Py_ssize_t> self.ptr.length

    def __len__(self):
        return self.length

    @property
    def dtype(self):
        return DRAKEN_ARRAY

    @property
    def itemsize(self):
        return 0

    @property
    def child_dtype(self):
        if self.ptr == NULL:
            return DRAKEN_NON_NATIVE
        return self.ptr.value_type

    def __getitem__(self, Py_ssize_t i):
        if self.ptr == NULL:
            raise IndexError("ArrayVector is not initialized")
        if i < 0 or i >= <Py_ssize_t> self.ptr.length:
            raise IndexError("Index out of bounds")
        if _row_is_null(self.ptr, i):
            return None
        return self._materialize_row(i)

    def to_arrow(self):
        if self.ptr == NULL:
            import pyarrow as pa

            return pa.array([], type=pa.list_(pa.null()))

        if self._child is None:
            raise ValueError("ArrayVector child vector is not initialized")

        import pyarrow as pa

        child_arrow = (<Vector> self._child).to_arrow()

        # When a native decoder signals UTF-8 children, cast binary -> utf8.
        if self._child_decode_utf8 and child_arrow.type == pa.binary():
            try:
                child_arrow = child_arrow.cast(pa.utf8())
            except Exception:
                pass

        # When we originated from an Arrow array we know the desired child type;
        # e.g. string children should stay UTF8 lists instead of degrading to binary.
        if self._child_arrow_type is not None and child_arrow.type != self._child_arrow_type:
            try:
                child_arrow = child_arrow.cast(self._child_arrow_type)
            except Exception:
                # Casting should normally succeed (binary -> string, etc.).
                # If it fails we fall back to the raw child to avoid losing data.
                pass
        cdef intptr_t offs_addr = <intptr_t> self.ptr.offsets
        cdef intptr_t nb_addr = <intptr_t> self.ptr.null_bitmap
        cdef Py_ssize_t nb_size = (self.ptr.length + 7) // 8
        if nb_size == 0:
            nb_size = 1

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pa.foreign_buffer(nb_addr, nb_size, base=self))
        else:
            buffers.append(None)
        buffers.append(
            pa.foreign_buffer(
                offs_addr,
                (<Py_ssize_t> self.ptr.length + 1) * sizeof(int32_t),
                base=self,
            )
        )

        list_type = pa.list_(child_arrow.type)
        return pa.Array.from_buffers(
            list_type,
            self.length,
            buffers,
            children=[child_arrow],
        )

    def take(self, indices):
        if self.ptr == NULL:
            raise ValueError("ArrayVector is not initialized")
        if self._child is None:
            raise ValueError("ArrayVector child vector is not initialized")

        cdef object seq = PySequence_Fast(indices, "indices must be a finite sequence")
        if seq is None:
            raise TypeError("indices must be a sequence of integers")

        cdef Py_ssize_t n = PySequence_Fast_GET_SIZE(seq)
        cdef Py_ssize_t* normalized = NULL
        cdef int32_t* new_offsets = NULL
        cdef uint8_t* new_null = NULL
        cdef int32_t* child_idx = NULL
        cdef Py_ssize_t total = 0
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t idx = 0
        cdef Py_ssize_t start = 0
        cdef Py_ssize_t end = 0
        cdef Py_ssize_t pos = 0
        cdef Py_ssize_t nb_size = 0
        cdef Py_ssize_t child_cap = 0
        cdef int32_t[:] idx_view
        cdef Vector child_vec
        cdef object child_result
        cdef ArrayVector result

        try:
            if n > 0:
                normalized = <Py_ssize_t*> PyMem_Malloc(n * sizeof(Py_ssize_t))
                if normalized == NULL:
                    raise MemoryError()

            new_offsets = <int32_t*> malloc((n + 1) * sizeof(int32_t))
            if new_offsets == NULL:
                raise MemoryError()
            new_offsets[0] = 0

            for i in range(n):
                idx = PyLong_AsSsize_t(<object> PySequence_Fast_GET_ITEM(seq, i))
                if idx == -1 and PyErr_Occurred():
                    raise TypeError("indices must be integers")
                if idx < 0:
                    idx += self.length
                if idx < 0 or idx >= self.length:
                    raise IndexError("Index out of bounds")
                normalized[i] = idx
                start = self.ptr.offsets[idx]
                end = self.ptr.offsets[idx + 1]
                total += end - start
                if total > 0x7FFFFFFF:
                    raise OverflowError("ArrayVector.take exceeds 32-bit capacity")
                new_offsets[i + 1] = <int32_t> total

            if self.ptr.null_bitmap != NULL and n > 0:
                nb_size = (n + 7) >> 3
                if nb_size == 0:
                    nb_size = 1
                new_null = <uint8_t*> malloc(nb_size)
                if new_null == NULL:
                    raise MemoryError()
                memset(new_null, 0xFF, nb_size)
                for i in range(n):
                    if _row_is_null(self.ptr, normalized[i]):
                        new_null[i >> 3] &= ~(1 << (i & 7))

            child_cap = total if total > 0 else 1
            child_idx = <int32_t*> PyMem_Malloc(child_cap * sizeof(int32_t))
            if child_idx == NULL:
                raise MemoryError()

            for i in range(n):
                start = self.ptr.offsets[normalized[i]]
                end = self.ptr.offsets[normalized[i] + 1]
                while start < end:
                    child_idx[pos] = <int32_t> start
                    pos += 1
                    start += 1

            if total == 0:
                # avoid creating a zero-length memoryview
                import array as pyarray
                child_result = (<Vector>self._child).take(pyarray.array('i'))
            else:
                idx_view = <int32_t[:total]> child_idx
                child_vec = <Vector> self._child
                child_result = child_vec.take(idx_view)

            result = ArrayVector()
            result.ptr = _alloc_array_buffer()
            result.ptr.length = <size_t> n
            result.ptr.value_type = self.ptr.value_type
            result.ptr.offsets = new_offsets
            result.ptr.null_bitmap = new_null
            result.owns_offsets = True
            result.owns_null_bitmap = new_null != NULL
            result._child = child_result
            result._child_arrow_type = self._child_arrow_type
            new_offsets = NULL
            new_null = NULL
            return result
        finally:
            if child_idx != NULL:
                PyMem_Free(child_idx)
            if normalized != NULL:
                PyMem_Free(normalized)
            if seq is not None:
                Py_DECREF(seq)
            if new_offsets != NULL:
                free(new_offsets)
            if new_null != NULL:
                free(new_null)

    def is_null(self):
        if self.ptr == NULL or self.ptr.length == 0:
            return []
        cdef Py_ssize_t n = <Py_ssize_t> self.ptr.length
        cdef Py_ssize_t i
        cdef list out = []
        for i in range(n):
            out.append(_row_is_null(self.ptr, i))
        return out

    cpdef object null_bitmap(self):
        if self.ptr == NULL or self.ptr.null_bitmap == NULL:
            return None
        cdef Py_ssize_t nb_size = (self.ptr.length + 7) // 8
        if nb_size == 0:
            nb_size = 1
        return PyMemoryView_FromMemory(<char*> self.ptr.null_bitmap, nb_size, PyBUF_READ)

    @property
    def null_count(self):
        if self.ptr == NULL or self.ptr.null_bitmap == NULL:
            return 0
        cdef Py_ssize_t n = <Py_ssize_t> self.ptr.length
        return n - <Py_ssize_t>simd_popcount(self.ptr.null_bitmap, (<size_t>n + 7) >> 3)

    def to_pylist(self):
        if self.ptr == NULL:
            return []
        cdef Py_ssize_t i, n = <Py_ssize_t> self.ptr.length
        cdef list out = []
        for i in range(n):
            if _row_is_null(self.ptr, i):
                out.append(None)
            else:
                out.append(self._materialize_row(i))
        return out

    cdef object _materialize_row(self, Py_ssize_t idx):
        if self._child is None:
            raise ValueError("ArrayVector child vector is not initialized")
        cdef DrakenArrayBuffer* ptr = self.ptr
        cdef Py_ssize_t start = ptr.offsets[idx]
        cdef Py_ssize_t end = ptr.offsets[idx + 1]
        cdef Py_ssize_t count = end - start
        if count <= 0:
            return []
        cdef object child = self._child
        cdef object getitem = child.__getitem__
        cdef list values = [None] * count
        cdef Py_ssize_t j
        for j in range(count):
            values[j] = getitem(start + j)
        return values

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0
    ) except *:
        """Hash array rows into output buffer.

        Hashes each row by:
        1. Hashing the offset pair (structure of the array row)
        2. XORing with hashes of child elements in that range

        This avoids materializing entire Python list objects per row.
        """
        if self.ptr == NULL:
            return

        cdef Py_ssize_t n = <Py_ssize_t> self.ptr.length
        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("ArrayVector.hash_into: output buffer too small")

        if self._child is None:
            raise ValueError("ArrayVector child vector is not initialized")

        cdef DrakenArrayBuffer* ptr = self.ptr
        cdef Py_ssize_t total_children = ptr.offsets[n] if n > 0 else 0
        cdef object child = self._child
        cdef uint64_t[::1] child_hashes
        cdef Py_ssize_t i, j
        cdef int32_t start, end
        cdef uint64_t offset_hash, row_hash

        # Hash all child elements once, then reuse for each array row
        if total_children > 0:
            child_hashes = child.hash()
        else:
            from array import array
            child_hashes = array("Q")

        for i in range(n):
            # Null rows
            if _row_is_null(ptr, i):
                out_buf[offset + i] = NULL_HASH
                continue

            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            # Hash the row length (structure), not absolute offsets — identical
            # content at different positions must hash equally.
            offset_hash = mix_hash(<uint64_t>(end - start), MIX_HASH_CONSTANT)

            # XOR together child hashes for elements in this row's range
            row_hash = offset_hash
            for j in range(start, end):
                row_hash = mix_hash(row_hash, child_hashes[j])

            out_buf[offset + i] = mix_hash(out_buf[offset + i], row_hash)

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        """ArrayVector cannot implement nogil hashing because it stores child as Python object.

        Return 1 to indicate fallback to Python hash_into is required.
        Per CLAUDE.md: "Fail Fast, Fail Clean" - we don't attempt impossible optimizations.
        """
        return 1

    cpdef object min(self):
        """Min is not defined for array vectors."""
        raise NotImplementedError("min() is not supported for ArrayVector")

    cpdef object max(self):
        """Max is not defined for array vectors."""
        raise NotImplementedError("max() is not supported for ArrayVector")

    cpdef object sum(self):
        """Sum is not defined for array vectors."""
        raise NotImplementedError("sum() is not supported for ArrayVector")

    def __str__(self):
        if self.ptr == NULL:
            return "<ArrayVector uninitialized>"
        preview = self.to_pylist()[:10]
        return f"<ArrayVector len={self.length} values={preview}>"


cdef ArrayVector from_arrow(object array):
    cdef ArrayVector vec = ArrayVector()
    cdef intptr_t offsets_addr
    cdef intptr_t null_addr = 0
    vec.ptr = _alloc_array_buffer()
    vec.ptr.length = <size_t> len(array)

    bufs = array.buffers()
    vec._arrow_parent = array
    vec._arrow_null_buf = bufs[0]
    vec._arrow_offsets_buf = bufs[1]

    if bufs[1] is None:
        raise ValueError("List arrays require an offsets buffer")

    # Detect LargeList (64-bit offsets) and fail fast. We only support ListType (32-bit).
    array_type = array.type
    type_name = str(array_type)
    if type_name.startswith("large_list"):
        raise NotImplementedError(
            f"LargeList arrays (64-bit offsets) are not supported. "
            f"Got type: {type_name}. Use list_ (32-bit offsets) instead."
        )

    # Handle offsets with array.offset
    # This assumes ListType (int32 offsets) - LargeList check above prevents silent data corruption.
    cdef Py_ssize_t offset = array.offset
    offsets_addr = <intptr_t> bufs[1].address + offset * 4
    vec.ptr.offsets = <int32_t*> offsets_addr

    # Variables for null bitmap handling
    cdef Py_ssize_t n_bytes
    cdef bytes new_bitmap
    cdef uint8_t* dst_bitmap
    cdef uint8_t* src_bitmap
    cdef int bit_offset
    cdef Py_ssize_t byte_offset
    cdef int shift_down
    cdef int shift_up
    cdef uint8_t val
    cdef Py_ssize_t i

    if bufs[0] is not None:
        null_addr = <intptr_t> bufs[0].address
        if offset % 8 == 0:
            vec.ptr.null_bitmap = <uint8_t*> (null_addr + (offset >> 3))
        else:
            # Unaligned offset: copy and shift
            n_bytes = (vec.ptr.length + 7) // 8
            new_bitmap = PyBytes_FromStringAndSize(NULL, n_bytes)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap)

            byte_offset = offset >> 3
            bit_offset = offset & 7
            src_bitmap = <uint8_t*> null_addr + byte_offset

            shift_down = bit_offset
            shift_up = 8 - bit_offset

            for i in range(n_bytes):
                val = src_bitmap[i] >> shift_down
                val |= (src_bitmap[i+1] << shift_up)
                dst_bitmap[i] = val

            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap # Keep alive
    else:
        vec.ptr.null_bitmap = NULL

    child_array = array.values
    vec._arrow_child_array = child_array
    vec._child_arrow_type = array.type.value_type
    vec._child = vector_from_arrow(child_array)
    vec.ptr.value_type = arrow_type_to_draken(array.type.value_type)

    return vec

cdef ArrayVector from_sequence(object data):
    """
    Create ArrayVector from a Python sequence of lists.

    Args:
        data: Python sequence where each element is a list (or None for null rows)

    Returns:
        ArrayVector with nested list structure

    Example:
        >>> vec = from_sequence([[1, 2, 3], [4, 5], None, [6]])
        >>> vec.to_arrow()  # Converts to PyArrow ListArray
    """
    cdef Py_ssize_t n = len(data)
    cdef Py_ssize_t i
    cdef Py_ssize_t offset = 0
    cdef Py_ssize_t nb
    cdef object row
    cdef bint has_nulls = False

    # Single pass: check for nulls and determine total flat length.
    for i in range(n):
        if data[i] is None:
            has_nulls = True
        else:
            offset += len(data[i])
    cdef Py_ssize_t total_flat = offset

    # Allocate offsets array.
    cdef int32_t* offsets_buf = <int32_t*> malloc((n + 1) * sizeof(int32_t))
    if offsets_buf == NULL:
        raise MemoryError()

    # Allocate null bitmap if needed (Arrow convention: 1=valid, 0=null).
    cdef uint8_t* null_bm = NULL
    if has_nulls:
        nb = (n + 7) >> 3
        null_bm = <uint8_t*> malloc(nb)
        if null_bm == NULL:
            free(offsets_buf)
            raise MemoryError()
        memset(null_bm, 0xFF, nb)

    # Second pass: populate offsets, null bitmap, and flat items list.
    flat_items = []
    offset = 0
    for i in range(n):
        offsets_buf[i] = <int32_t> offset
        row = data[i]
        if row is None:
            if null_bm != NULL:
                null_bm[i >> 3] &= ~(<uint8_t>(1 << (i & 7)))
        else:
            flat_items.extend(row)
            offset += len(row)
    offsets_buf[n] = <int32_t> offset

    # Build the child vector via the interop layer (handles all element types).
    cdef object child_vec = vector_from_sequence(flat_items)

    # Assemble ArrayVector directly — same layout as array_vector_from_parts
    # but accepts any Vector child, not just StringVector.
    cdef ArrayVector vec = ArrayVector.__new__(ArrayVector)
    cdef DrakenArrayBuffer* buf = _alloc_array_buffer()

    cdef int32_t* offs_copy = <int32_t*> malloc((n + 1) * sizeof(int32_t))
    if offs_copy == NULL:
        free(offsets_buf)
        if null_bm != NULL:
            free(null_bm)
        raise MemoryError()
    memcpy(offs_copy, offsets_buf, (n + 1) * sizeof(int32_t))
    free(offsets_buf)

    buf.offsets = offs_copy
    buf.length = <size_t> n
    buf.value_type = child_vec.dtype if total_flat > 0 else DRAKEN_STRING
    buf.values = <void*> <object> child_vec

    if null_bm != NULL:
        buf.null_bitmap = null_bm
        vec.owns_null_bitmap = True
    else:
        buf.null_bitmap = NULL
        vec.owns_null_bitmap = False

    vec.ptr = buf
    vec._child = child_vec
    vec.owns_offsets = True
    return vec


cdef ArrayVector array_vector_from_parts(
        StringVector flat_child,
        int32_t* offsets,
        uint8_t* list_null_bitmap,
        Py_ssize_t num_rows):
    """
    Construct an ArrayVector directly from pre-decoded components.

    Intended for use by native decoders (e.g. rugo) that have already decoded
    a Parquet LIST column into its constituent parts without going through PyArrow.

    Args:
        flat_child:       Southern StringVector containing all concatenated list elements.
        offsets:          (num_rows + 1) int32 offsets array; will be copied.
        list_null_bitmap: Arrow validity bitmap for the outer list rows (1=valid, 0=null);
                          pass NULL if all rows are non-null.
        num_rows:         Number of outer (list) rows.

    Returns:
        ArrayVector that owns its offsets and null_bitmap buffers, holding
        a strong Python reference to flat_child.
    """
    cdef ArrayVector vec = ArrayVector.__new__(ArrayVector)
    cdef DrakenArrayBuffer* buf = _alloc_array_buffer()
    cdef Py_ssize_t nb
    cdef int32_t* offs_copy
    cdef uint8_t* bm_copy

    offs_copy = <int32_t*> malloc((num_rows + 1) * sizeof(int32_t))
    if offs_copy == NULL:
        raise MemoryError()
    memcpy(offs_copy, offsets, (num_rows + 1) * sizeof(int32_t))

    buf.offsets = offs_copy
    buf.length = <size_t> num_rows
    buf.value_type = DRAKEN_STRING
    buf.values = <void*> flat_child

    if list_null_bitmap != NULL:
        nb = (num_rows + 7) >> 3
        bm_copy = <uint8_t*> malloc(nb)
        if bm_copy == NULL:
            raise MemoryError()
        memcpy(bm_copy, list_null_bitmap, nb)
        buf.null_bitmap = bm_copy
        vec.owns_null_bitmap = True
    else:
        buf.null_bitmap = NULL
        vec.owns_null_bitmap = False

    vec.ptr = buf
    vec._child = flat_child
    vec.owns_offsets = True
    return vec
