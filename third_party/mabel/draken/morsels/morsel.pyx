# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

"""
Morsel: Batch data container for columnar processing in Draken.

This module provides the Morsel class which represents a batch of columnar data
similar to Arrow's RecordBatch but optimized for Draken's internal processing.
Morsels contain multiple Vector columns and provide efficient batch operations
for analytical workloads.

The module includes:
- Morsel class for managing collections of Vector columns
- DrakenTypeInt helper for debugging type information
- Integration with Draken's core buffer management system
"""

from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Calloc
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from libc.stddef cimport size_t
from libc.stdlib cimport malloc
from libc.string cimport memcpy, memset, strlen
from libc.stdint cimport int32_t, int64_t, uint8_t
from libc.stdint cimport uint64_t

from opteryx.draken.core.buffers cimport (
    DrakenFixedBuffer,
    DrakenMorsel,
    DrakenType,
    DrakenVarBuffer,
    DRAKEN_ARRAY,
    DRAKEN_BOOL,
    DRAKEN_CONSTANT,
    DRAKEN_DATE32,
    DRAKEN_DICTIONARY,
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_INT8,
    DRAKEN_INTERVAL,
    DRAKEN_NON_NATIVE,
    DRAKEN_STRING,
    DRAKEN_TIME32,
    DRAKEN_TIME64,
    DRAKEN_TIMESTAMP64,
)
from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.date32_vector cimport Date32Vector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.integer_vector cimport IntegerVector
from opteryx.draken.vectors.interval_vector cimport IntervalVector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.time_vector cimport TimeVector
from opteryx.draken.vectors.timestamp_vector cimport TimestampVector
from opteryx.draken.interop.arrow cimport vector_from_arrow
from opteryx.draken.interop.arrow cimport vector_from_sequence

# Python helper: int subclass for DrakenType enum debugging
cdef class DrakenTypeInt(int):
    def __repr__(self):
        return f"{self._enum_name()}({int(self)})"

    def __str__(self):
        return self._enum_name()

    def _enum_name(self):
        mapping = {
            1: "DRAKEN_INT8",
            2: "DRAKEN_INT16",
            3: "DRAKEN_INT32",
            4: "DRAKEN_INT64",
            20: "DRAKEN_FLOAT32",
            21: "DRAKEN_FLOAT64",
            30: "DRAKEN_DATE32",
            40: "DRAKEN_TIMESTAMP64",
            43: "DRAKEN_INTERVAL",
            50: "DRAKEN_BOOL",
            60: "DRAKEN_STRING",
            61: "DRAKEN_DICTIONARY",
            62: "DRAKEN_CONSTANT",
            80: "DRAKEN_ARRAY",
            100: "DRAKEN_NON_NATIVE",
        }
        return mapping.get(int(self), f"UNKNOWN({int(self)})")


cdef inline bint _bitmap_get(uint8_t* bitmap, Py_ssize_t index):
    return ((bitmap[index >> 3] >> (index & 7)) & 1) != 0


cdef inline void _bitmap_set(uint8_t* bitmap, Py_ssize_t index):
    bitmap[index >> 3] |= <uint8_t>(1 << (index & 7))


cdef inline void _bitmap_clear(uint8_t* bitmap, Py_ssize_t index):
    bitmap[index >> 3] &= <uint8_t>(~(1 << (index & 7)))


cdef void _copy_bits(
    uint8_t* src,
    Py_ssize_t src_offset,
    uint8_t* dst,
    Py_ssize_t dst_offset,
    Py_ssize_t length,
):
    cdef Py_ssize_t i
    cdef Py_ssize_t src_index
    cdef Py_ssize_t dst_index
    for i in range(length):
        src_index = src_offset + i
        dst_index = dst_offset + i
        if _bitmap_get(src, src_index):
            _bitmap_set(dst, dst_index)
        else:
            _bitmap_clear(dst, dst_index)


cdef uint8_t* _merge_null_bitmaps(
    uint8_t* left_bitmap,
    Py_ssize_t left_offset,
    Py_ssize_t left_rows,
    uint8_t* right_bitmap,
    Py_ssize_t right_offset,
    Py_ssize_t right_rows,
) except *:
    cdef Py_ssize_t total_rows = left_rows + right_rows
    cdef Py_ssize_t null_bytes
    cdef Py_ssize_t i
    cdef uint8_t* merged
    cdef uint8_t trailing_mask

    if total_rows == 0:
        return NULL

    if left_bitmap == NULL and right_bitmap == NULL:
        return NULL

    null_bytes = (total_rows + 7) // 8
    merged = <uint8_t*> malloc(null_bytes)
    if merged == NULL:
        raise MemoryError()

    memset(merged, 0xFF, null_bytes)

    if left_bitmap != NULL:
        for i in range(left_rows):
            if not _bitmap_get(left_bitmap, left_offset + i):
                _bitmap_clear(merged, i)

    if right_bitmap != NULL:
        for i in range(right_rows):
            if not _bitmap_get(right_bitmap, right_offset + i):
                _bitmap_clear(merged, left_rows + i)

    if (total_rows & 7) != 0:
        trailing_mask = <uint8_t>((1 << (total_rows & 7)) - 1)
        merged[null_bytes - 1] &= trailing_mask

    return merged


cdef void _concat_fixed_buffers(
    DrakenFixedBuffer* out_ptr,
    DrakenFixedBuffer* left_ptr,
    DrakenFixedBuffer* right_ptr,
    Py_ssize_t left_rows,
    Py_ssize_t right_rows,
    Py_ssize_t left_null_offset=0,
    Py_ssize_t right_null_offset=0,
) except *:
    cdef Py_ssize_t left_bytes = left_rows * <Py_ssize_t>left_ptr.itemsize
    cdef Py_ssize_t right_bytes = right_rows * <Py_ssize_t>left_ptr.itemsize
    cdef uint8_t* merged_nulls

    if left_bytes > 0 and left_ptr.data != NULL:
        memcpy(out_ptr.data, left_ptr.data, left_bytes)
    if right_bytes > 0 and right_ptr.data != NULL:
        memcpy(<char*>out_ptr.data + left_bytes, right_ptr.data, right_bytes)

    merged_nulls = _merge_null_bitmaps(
        left_ptr.null_bitmap,
        left_null_offset,
        left_rows,
        right_ptr.null_bitmap,
        right_null_offset,
        right_rows,
    )
    out_ptr.null_bitmap = merged_nulls


cdef void _concat_bool_buffers(
    DrakenFixedBuffer* out_ptr,
    DrakenFixedBuffer* left_ptr,
    DrakenFixedBuffer* right_ptr,
    Py_ssize_t left_rows,
    Py_ssize_t right_rows,
) except *:
    cdef Py_ssize_t total_rows = left_rows + right_rows
    cdef Py_ssize_t packed_bytes = (total_rows + 7) // 8
    cdef uint8_t* out_bits = <uint8_t*> out_ptr.data

    if packed_bytes > 0:
        memset(out_bits, 0, packed_bytes)
        if left_rows > 0:
            _copy_bits(<uint8_t*>left_ptr.data, 0, out_bits, 0, left_rows)
        if right_rows > 0:
            _copy_bits(<uint8_t*>right_ptr.data, 0, out_bits, left_rows, right_rows)

    out_ptr.null_bitmap = _merge_null_bitmaps(
        left_ptr.null_bitmap,
        0,
        left_rows,
        right_ptr.null_bitmap,
        0,
        right_rows,
    )


cdef void _concat_string_buffers(
    StringVector out_vec,
    StringVector left_vec,
    StringVector right_vec,
    Py_ssize_t left_rows,
    Py_ssize_t right_rows,
) except *:
    cdef DrakenVarBuffer* out_ptr = out_vec.ptr
    cdef DrakenVarBuffer* left_ptr = left_vec.ptr
    cdef DrakenVarBuffer* right_ptr = right_vec.ptr
    cdef Py_ssize_t i
    cdef Py_ssize_t left_bytes = <Py_ssize_t>left_ptr.offsets[left_rows]
    cdef Py_ssize_t right_bytes = <Py_ssize_t>right_ptr.offsets[right_rows]

    if left_bytes > 0 and left_ptr.data != NULL:
        memcpy(out_ptr.data, left_ptr.data, left_bytes)
    if right_bytes > 0 and right_ptr.data != NULL:
        memcpy(out_ptr.data + left_bytes, right_ptr.data, right_bytes)

    out_ptr.offsets[0] = 0
    for i in range(1, left_rows + 1):
        out_ptr.offsets[i] = left_ptr.offsets[i]
    for i in range(1, right_rows + 1):
        out_ptr.offsets[left_rows + i] = <int32_t>(left_bytes + right_ptr.offsets[i])

    out_ptr.null_bitmap = _merge_null_bitmaps(
        left_ptr.null_bitmap,
        0,
        left_rows,
        right_ptr.null_bitmap,
        0,
        right_rows,
    )


cdef uint8_t* _allocate_valid_bitmap(Py_ssize_t total_rows) except NULL:
    cdef Py_ssize_t null_bytes
    cdef uint8_t* bitmap
    cdef uint8_t trailing_mask

    if total_rows <= 0:
        return NULL

    null_bytes = (total_rows + 7) // 8
    bitmap = <uint8_t*> malloc(null_bytes)
    if bitmap == NULL:
        raise MemoryError()
    memset(bitmap, 0xFF, null_bytes)

    if (total_rows & 7) != 0:
        trailing_mask = <uint8_t>((1 << (total_rows & 7)) - 1)
        bitmap[null_bytes - 1] &= trailing_mask

    return bitmap


cdef class Morsel:

    cdef void _empty_inplace(self)

    def __cinit__(self):
        self.ptr = <DrakenMorsel*> NULL
        self._columns = []
        self._encoded_names = []
        self._name_to_index = None

    def __dealloc__(self):
        if self.ptr is not NULL:
            PyMem_Free(self.ptr.column_names)
            PyMem_Free(self.ptr.column_types)
            PyMem_Free(self.ptr.columns)
            PyMem_Free(self.ptr)

    cdef inline void _rebuild_name_to_index(self):
        """Refresh the cached mapping from encoded column name -> index."""
        cdef dict mapping = {}
        cdef Py_ssize_t i, n
        if self.ptr is NULL:
            self._name_to_index = mapping
            return
        n = self.ptr.num_columns
        for i in range(n):
            mapping[self._encoded_names[i]] = i
        self._name_to_index = mapping

    cdef inline dict _ensure_name_map(self):
        if self._name_to_index is None:
            self._rebuild_name_to_index()
        return self._name_to_index

    cdef inline Py_ssize_t _column_index_from_name(self, object column):
        """Resolve column identifier (str/bytes/int) to a numeric index."""
        if isinstance(column, int):
            if column < 0 or column >= self.ptr.num_columns:
                raise IndexError(f"Column index {column} out of range")
            return <Py_ssize_t>column

        cdef bytes key
        if isinstance(column, str):
            key = column.encode("utf-8")
        else:
            key = column

        cdef dict mapping = self._ensure_name_map()
        cdef object idx = mapping.get(key)
        if idx is None:
            raise KeyError(f"Column '{column}' not found")
        return <Py_ssize_t>idx

    @staticmethod
    def from_arrow(object table):
        cdef int i, n = table.num_columns
        cdef Morsel self = Morsel()
        cdef Vector vec
        cdef bytes encoded_name

        self._columns = [None] * n
        self._encoded_names = [None] * n
        self.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        self.ptr.num_columns = n
        self.ptr.num_rows = table.num_rows
        self.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n)
        self.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n)
        self.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n)

        for i in range(n):
            col = table.column(i)
            # if hasattr(col, "num_chunks") and col.num_chunks > 1:
            #     col = col.combine_chunks()
            vec = Vector.from_arrow(col)
            self._columns[i] = vec

            name = table.schema.field(i).name
            encoded_name = name.encode("utf-8")
            self._encoded_names[i] = encoded_name

            self.ptr.columns[i] = <void*>vec
            self.ptr.column_types[i] = vec.dtype
            self.ptr.column_names[i] = <const char*>encoded_name

        self._rebuild_name_to_index()

        return self

    @staticmethod
    def from_vectors(list names, list vectors):
        """Construct a Morsel directly from lists of column names and Draken Vector objects.

        Unlike ``from_arrow`` this method does not touch PyArrow at all; it is
        intended for use by native decoders (e.g. rugo) that produce Draken
        vectors directly.

        Args:
            names:   List of column name strings (or bytes).
            vectors: List of Draken Vector instances in the same order.

        Returns:
            A fully initialised Morsel.
        """
        cdef int i, n = len(vectors)
        if len(names) != n:
            raise ValueError("names and vectors must have the same length")
        if n == 0:
            raise ValueError("at least one column required")

        cdef Morsel self = Morsel()
        cdef Vector vec
        cdef bytes encoded_name

        self._columns = [None] * n
        self._encoded_names = [None] * n
        self.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        self.ptr.num_columns = n
        self.ptr.num_rows = (<Vector>vectors[0]).length
        self.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n)
        self.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n)
        self.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n)

        for i in range(n):
            vec = vectors[i]
            self._columns[i] = vec

            if isinstance(names[i], str):
                encoded_name = names[i].encode("utf-8")
            else:
                encoded_name = names[i]
            self._encoded_names[i] = encoded_name

            self.ptr.columns[i] = <void*>vec
            self.ptr.column_types[i] = vec.dtype
            self.ptr.column_names[i] = <const char*>encoded_name

        self._rebuild_name_to_index()
        return self

    @staticmethod
    def combine(list morsels):
        cdef Py_ssize_t i
        cdef Py_ssize_t j
        cdef Py_ssize_t n_morsels
        cdef Py_ssize_t n_columns
        cdef Py_ssize_t total_rows = 0
        cdef Py_ssize_t row_offset
        cdef Py_ssize_t byte_offset
        cdef Py_ssize_t string_offset
        cdef Py_ssize_t current_rows
        cdef Py_ssize_t current_bytes
        cdef Py_ssize_t total_string_bytes
        cdef object morsel_obj
        cdef Morsel morsel
        cdef Morsel first
        cdef Vector current_vec
        cdef Vector new_vec
        cdef list values
        cdef uint8_t* null_bitmap
        cdef Int64Vector out_i64
        cdef Float64Vector out_f64
        cdef BoolVector out_bool
        cdef IntegerVector out_int
        cdef Date32Vector out_date32
        cdef TimeVector out_time
        cdef TimestampVector out_ts
        cdef IntervalVector out_interval
        cdef StringVector out_str
        cdef Int64Vector src_i64
        cdef Float64Vector src_f64
        cdef BoolVector src_bool
        cdef IntegerVector src_int
        cdef Date32Vector src_date32
        cdef TimeVector src_time
        cdef TimestampVector src_ts
        cdef IntervalVector src_interval
        cdef StringVector src_str

        if morsels is None:
            raise ValueError("morsels must not be None")

        morsels = [m for m in morsels if m is not None and m.num_rows > 0]
        n_morsels = len(morsels)
        if n_morsels == 0:
            raise ValueError("at least one non-empty morsel is required")

        first = <Morsel> morsels[0]
        if n_morsels == 1:
            return first.copy()

        n_columns = first.ptr.num_columns
        for morsel_obj in morsels:
            morsel = <Morsel> morsel_obj
            if morsel.ptr.num_columns != n_columns:
                raise ValueError(
                    f"Cannot combine morsel with {morsel.ptr.num_columns} columns "
                    f"into schema with {n_columns} columns"
                )
            for i in range(n_columns):
                if first._encoded_names[i] != morsel._encoded_names[i]:
                    raise ValueError(
                        f"Cannot combine morsels with different schemas: "
                        f"column {i} differs ({first._encoded_names[i]!r} != {morsel._encoded_names[i]!r})"
                    )
            total_rows += morsel.ptr.num_rows

        cdef Morsel self = Morsel()
        self._columns = [None] * n_columns
        self._encoded_names = list(first._encoded_names)
        self.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        self.ptr.num_columns = n_columns
        self.ptr.num_rows = total_rows
        self.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n_columns)
        self.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n_columns)
        self.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n_columns)

        for i in range(n_columns):
            self.ptr.column_names[i] = <const char*> self._encoded_names[i]
            self.ptr.column_types[i] = first.ptr.column_types[i]
            current_vec = <Vector> first.ptr.columns[i]

            if isinstance(current_vec, Int64Vector):
                out_i64 = Int64Vector(<size_t> total_rows)
                row_offset = 0
                byte_offset = 0
                null_bitmap = NULL
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_i64 = <Int64Vector> morsel.ptr.columns[i]
                    current_rows = morsel.ptr.num_rows
                    current_bytes = current_rows * <Py_ssize_t> src_i64.ptr.itemsize
                    if current_bytes > 0 and src_i64.ptr.data != NULL:
                        memcpy(<char*> out_i64.ptr.data + byte_offset, src_i64.ptr.data, current_bytes)
                    if src_i64.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_i64.ptr.null_bitmap, 0, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                    byte_offset += current_bytes
                out_i64.ptr.null_bitmap = null_bitmap
                new_vec = <Vector> out_i64

            elif isinstance(current_vec, Float64Vector):
                out_f64 = Float64Vector(<size_t> total_rows)
                row_offset = 0
                byte_offset = 0
                null_bitmap = NULL
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_f64 = <Float64Vector> morsel.ptr.columns[i]
                    current_rows = morsel.ptr.num_rows
                    current_bytes = current_rows * <Py_ssize_t> src_f64.ptr.itemsize
                    if current_bytes > 0 and src_f64.ptr.data != NULL:
                        memcpy(<char*> out_f64.ptr.data + byte_offset, src_f64.ptr.data, current_bytes)
                    if src_f64.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_f64.ptr.null_bitmap, 0, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                    byte_offset += current_bytes
                out_f64.ptr.null_bitmap = null_bitmap
                new_vec = <Vector> out_f64

            elif isinstance(current_vec, BoolVector):
                out_bool = BoolVector(<size_t> total_rows)
                row_offset = 0
                null_bitmap = NULL
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_bool = <BoolVector> morsel.ptr.columns[i]
                    current_rows = morsel.ptr.num_rows
                    if current_rows > 0 and src_bool.ptr.data != NULL:
                        _copy_bits(<uint8_t*> src_bool.ptr.data, 0, <uint8_t*> out_bool.ptr.data, row_offset, current_rows)
                    if src_bool.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_bool.ptr.null_bitmap, 0, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                out_bool.ptr.null_bitmap = null_bitmap
                new_vec = <Vector> out_bool

            elif isinstance(current_vec, IntegerVector):
                out_int = IntegerVector((<IntegerVector> current_vec).ptr.type, <size_t> total_rows)
                row_offset = 0
                byte_offset = 0
                null_bitmap = NULL
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_int = <IntegerVector> morsel.ptr.columns[i]
                    if src_int.ptr.type != out_int.ptr.type:
                        values = []
                        for morsel_obj in morsels:
                            values.extend((<Morsel> morsel_obj).column(first._encoded_names[i]).to_pylist())
                        new_vec = <Vector> vector_from_sequence(values, self.ptr.column_types[i])
                        break
                    current_rows = morsel.ptr.num_rows
                    current_bytes = current_rows * <Py_ssize_t> src_int.ptr.itemsize
                    if current_bytes > 0 and src_int.ptr.data != NULL:
                        memcpy(<char*> out_int.ptr.data + byte_offset, src_int.ptr.data, current_bytes)
                    if src_int.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_int.ptr.null_bitmap, 0, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                    byte_offset += current_bytes
                else:
                    out_int.ptr.null_bitmap = null_bitmap
                    new_vec = <Vector> out_int

            elif isinstance(current_vec, Date32Vector):
                out_date32 = Date32Vector(<size_t> total_rows)
                row_offset = 0
                byte_offset = 0
                null_bitmap = NULL
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_date32 = <Date32Vector> morsel.ptr.columns[i]
                    current_rows = morsel.ptr.num_rows
                    current_bytes = current_rows * <Py_ssize_t> src_date32.ptr.itemsize
                    if current_bytes > 0 and src_date32.ptr.data != NULL:
                        memcpy(<char*> out_date32.ptr.data + byte_offset, src_date32.ptr.data, current_bytes)
                    if src_date32.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_date32.ptr.null_bitmap, 0, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                    byte_offset += current_bytes
                out_date32.ptr.null_bitmap = null_bitmap
                new_vec = <Vector> out_date32

            elif isinstance(current_vec, TimeVector):
                out_time = TimeVector(<size_t> total_rows, (<TimeVector> current_vec).is_time64)
                row_offset = 0
                byte_offset = 0
                null_bitmap = NULL
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_time = <TimeVector> morsel.ptr.columns[i]
                    if src_time.is_time64 != out_time.is_time64:
                        values = []
                        for morsel_obj in morsels:
                            values.extend((<Morsel> morsel_obj).column(first._encoded_names[i]).to_pylist())
                        new_vec = <Vector> vector_from_sequence(values, self.ptr.column_types[i])
                        break
                    current_rows = morsel.ptr.num_rows
                    current_bytes = current_rows * <Py_ssize_t> src_time.ptr.itemsize
                    if current_bytes > 0 and src_time.ptr.data != NULL:
                        memcpy(<char*> out_time.ptr.data + byte_offset, src_time.ptr.data, current_bytes)
                    if src_time.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_time.ptr.null_bitmap, 0, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                    byte_offset += current_bytes
                else:
                    out_time.ptr.null_bitmap = null_bitmap
                    new_vec = <Vector> out_time

            elif isinstance(current_vec, TimestampVector):
                out_ts = TimestampVector(<size_t> total_rows)
                out_ts.timestamp_unit = (<TimestampVector> current_vec).timestamp_unit
                out_ts.null_bit_offset = 0
                row_offset = 0
                byte_offset = 0
                null_bitmap = NULL
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_ts = <TimestampVector> morsel.ptr.columns[i]
                    current_rows = morsel.ptr.num_rows
                    current_bytes = current_rows * <Py_ssize_t> src_ts.ptr.itemsize
                    if current_bytes > 0 and src_ts.ptr.data != NULL:
                        memcpy(<char*> out_ts.ptr.data + byte_offset, src_ts.ptr.data, current_bytes)
                    if src_ts.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_ts.ptr.null_bitmap, src_ts.null_bit_offset, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                    byte_offset += current_bytes
                out_ts.ptr.null_bitmap = null_bitmap
                new_vec = <Vector> out_ts

            elif isinstance(current_vec, IntervalVector):
                out_interval = IntervalVector(<size_t> total_rows)
                row_offset = 0
                byte_offset = 0
                null_bitmap = NULL
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_interval = <IntervalVector> morsel.ptr.columns[i]
                    current_rows = morsel.ptr.num_rows
                    current_bytes = current_rows * <Py_ssize_t> src_interval.ptr.itemsize
                    if current_bytes > 0 and src_interval.ptr.data != NULL:
                        memcpy(<char*> out_interval.ptr.data + byte_offset, src_interval.ptr.data, current_bytes)
                    if src_interval.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_interval.ptr.null_bitmap, 0, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                    byte_offset += current_bytes
                out_interval.ptr.null_bitmap = null_bitmap
                new_vec = <Vector> out_interval

            elif isinstance(current_vec, StringVector):
                total_string_bytes = 0
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_str = <StringVector> morsel.ptr.columns[i]
                    total_string_bytes += src_str.ptr.offsets[morsel.ptr.num_rows]
                out_str = StringVector(<size_t> total_rows, <size_t> total_string_bytes)
                row_offset = 0
                string_offset = 0
                null_bitmap = NULL
                out_str.ptr.offsets[0] = 0
                for morsel_obj in morsels:
                    morsel = <Morsel> morsel_obj
                    src_str = <StringVector> morsel.ptr.columns[i]
                    current_rows = morsel.ptr.num_rows
                    current_bytes = src_str.ptr.offsets[current_rows]
                    if current_bytes > 0 and src_str.ptr.data != NULL:
                        memcpy(out_str.ptr.data + string_offset, src_str.ptr.data, current_bytes)
                    for j in range(1, current_rows + 1):
                        out_str.ptr.offsets[row_offset + j] = <int32_t> (
                            string_offset + src_str.ptr.offsets[j]
                        )
                    if src_str.ptr.null_bitmap != NULL:
                        if null_bitmap == NULL:
                            null_bitmap = _allocate_valid_bitmap(total_rows)
                        _copy_bits(src_str.ptr.null_bitmap, 0, null_bitmap, row_offset, current_rows)
                    row_offset += current_rows
                    string_offset += current_bytes
                out_str.ptr.null_bitmap = null_bitmap
                new_vec = <Vector> out_str

            else:
                values = []
                for morsel_obj in morsels:
                    values.extend((<Morsel> morsel_obj).column(first._encoded_names[i]).to_pylist())
                new_vec = <Vector> vector_from_sequence(values, self.ptr.column_types[i])

            self._columns[i] = new_vec
            self.ptr.columns[i] = <void*> new_vec
            self.ptr.column_types[i] = new_vec.dtype

        self._rebuild_name_to_index()
        return self

    @staticmethod
    def iter_from_arrow(object table, batch_size=None):
        """Yield ``Morsel`` instances from an Arrow table without forcing ``combine_chunks``."""
        import pyarrow as pa
        cdef Py_ssize_t start
        cdef Py_ssize_t length

        if not isinstance(table, pa.Table):
            raise TypeError("iter_from_arrow expects a pyarrow.Table")

        if table.num_rows == 0:
            return

        if batch_size is not None:
            if not isinstance(batch_size, int):
                raise TypeError("batch_size must be an integer or None")
            if batch_size <= 0:
                raise ValueError("batch_size must be a positive integer when provided")

            start = 0
            while start < table.num_rows:
                length = table.num_rows - start
                if length > batch_size:
                    length = batch_size
                slice = table.slice(start, length)
                yield Morsel.from_arrow(slice)
                start += length
            return

        # Build chunk boundaries from all columns so we never split an Arrow chunk.
        cdef Py_ssize_t total_rows = table.num_rows
        cdef Py_ssize_t previous = 0
        cdef Py_ssize_t boundary
        cdef set breakpoints = set()
        cdef object column
        cdef object chunk
        cdef Py_ssize_t chunk_length
        cdef Py_ssize_t slice_length

        for column in table.columns:
            boundary = 0
            for chunk in column.chunks:
                chunk_length = len(chunk)
                boundary += chunk_length
                breakpoints.add(boundary)

        if not breakpoints:
            breakpoints.add(total_rows)

        chunk_count = 0
        for boundary in sorted(breakpoints):
            if boundary <= previous:
                continue
            if boundary > total_rows:
                boundary = total_rows
            slice_length = boundary - previous
            if slice_length <= 0:
                previous = boundary
                continue
            yield Morsel.from_arrow(table.slice(previous, slice_length))
            previous = boundary

    cpdef Vector column(self, bytes name):
        cdef dict mapping = self._ensure_name_map()
        cdef object idx = mapping.get(name)
        if idx is None:
            raise KeyError(f"Column '{name}' not found")
        return <Vector>self.ptr.columns[<Py_ssize_t>idx]

    @property
    def shape(self) -> tuple:
        """Return (num_rows, num_columns) tuple."""
        return (self.ptr.num_rows, self.ptr.num_columns)

    @property
    def num_rows(self) -> int:
        """Return the number of rows."""
        return self.ptr.num_rows

    def __len__(self) -> int:
        """Return the number of rows (for len() compatibility)."""
        return self.ptr.num_rows

    @property
    def num_columns(self) -> int:
        """Return the number of columns."""
        return self.ptr.num_columns

    @property
    def nbytes(self):
        """
        Return the approximate number of bytes used by this morsel.

        Strategy:
        - Prefer `Vector.nbytes` when exposed by the vector implementation.
        - Fall back to converting the vector to an Arrow array and using
          `array.nbytes` when available.
        - If neither is available, attempt a fixed-width approximation using
          the Arrow type's `bit_width` when possible.
        This keeps the property safe (never raises) and conservative.
        """
        cdef Py_ssize_t i
        cdef object vec
        cdef object arr
        cdef object nb
        cdef uint64_t total = 0

        for i in range(self.ptr.num_columns):
            try:
                vec = <Vector>self.ptr.columns[i]
            except Exception:
                continue

            # Prefer vector-level reporting
            try:
                nb = getattr(vec, "nbytes", None)
                if nb is not None:
                    total += <uint64_t>nb
                    continue
            except Exception:
                nb = None

            # Fall back to Arrow array size
            try:
                arr = vec.to_arrow()
                nb = getattr(arr, "nbytes", None)
                if nb is not None:
                    total += <uint64_t>nb
                    continue

                # Try a naive fixed-width estimate
                try:
                    bit_width = arr.type.bit_width
                    itemsize = bit_width // 8
                    total += <uint64_t>(itemsize * len(arr))
                    continue
                except Exception:
                    # Unknown/variable-width: best-effort zero contribution
                    continue
            except Exception:
                # If all else fails, ignore this column
                continue

        return total


    @property
    def column_names(self) -> list:
        """Return the list of column names."""
        cdef list names = []
        cdef size_t i
        cdef const char* cstr
        for i in range(self.ptr.num_columns):
            cstr = self.ptr.column_names[i]
            names.append(<str> PyBytes_FromStringAndSize(cstr, strlen(cstr)))
        return names

    @property
    def column_types(self) -> list:
        """Return the list of column types"""
        cdef list types = []
        cdef size_t i
        for i in range(self.ptr.num_columns):
            types.append(DrakenTypeInt(self.ptr.column_types[i]))
        return types

    def __getitem__(self, Py_ssize_t i) -> tuple:
        out = []
        for c in self._columns:
            try:
                out.append(c[i])
            except Exception:
                out.append(None)
        return tuple(out)

    def slice(self, Py_ssize_t offset, Py_ssize_t length):
        """
        Return a new Morsel representing rows [offset: offset+length).
        This is implemented as a small, zero-copy (where underlying vectors
        support take) or minimal-copy operation where necessary by leveraging
        each Vector's take() method.
        """
        cdef Morsel result
        cdef int i, n_columns = self.ptr.num_columns
        cdef Py_ssize_t start = offset
        cdef Py_ssize_t ln = length
        cdef Vector vec
        cdef object new_vec
        
        if ln <= 0 or start >= self.ptr.num_rows:
            # return an empty morsel of the same schema
            result = self._full_copy()
            result._empty_inplace()
            return result

        # clamp length to available rows
        if start + ln > self.ptr.num_rows:
            ln = self.ptr.num_rows - start

        # Build an indices buffer for take (C array -> memoryview)
        cdef int32_t* indices_ptr = <int32_t*> PyMem_Malloc(ln * sizeof(int32_t))
        if indices_ptr == NULL:
            raise MemoryError()
        cdef int32_t[::1] indices_view
        try:
            for i in range(ln):
                indices_ptr[i] = <int32_t>(start + i)
            indices_view = <int32_t[:ln]>indices_ptr

            # Build the new morsel
            result = Morsel()
            result._columns = [None] * n_columns
            result._encoded_names = [None] * n_columns
            result.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
            if result.ptr == NULL:
                raise MemoryError()
            result.ptr.num_columns = n_columns
            result.ptr.num_rows = ln
            result.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n_columns)
            result.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n_columns)
            result.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n_columns)

            for i in range(n_columns):
                vec = <Vector> self.ptr.columns[i]
                # Attempt to use vector.take(indices_view) when available
                try:
                    new_vec = vec.take(indices_view)
                except (AttributeError, TypeError):
                    # Fallback: try passing a python list of indices to take (some vector
                    # implementations accept python lists). Avoid converting to Arrow.
                    try:
                        py_indices = [<int>(start + j) for j in range(ln)]
                        new_vec = vec.take(py_indices)
                    except Exception:
                        raise NotImplementedError(
                            f"{type(vec).__name__} does not implement take(); "
                            "add a native take() method to this vector type"
                        )

                result._columns[i] = new_vec
                result._encoded_names[i] = self._encoded_names[i]
                result.ptr.columns[i] = <void*> new_vec
                result.ptr.column_types[i] = new_vec.dtype
                result.ptr.column_names[i] = <const char*> self.ptr.column_names[i]

            result._rebuild_name_to_index()
            return result
        finally:
            PyMem_Free(indices_ptr)

    def __repr__(self) -> str:
        return f"<Morsel: {self.ptr.num_rows} rows x {self.ptr.num_columns} columns>"

    def copy(self, columns=None, mask=None) -> Morsel:
        """
        Create a copy of this Morsel, optionally filtering columns and rows.

        Args:
            columns: List of column names to include (None = all columns)
            mask: Boolean mask or list of row indices (None = all rows)

        Returns:
            Morsel: New copied Morsel with optional filtering
        """
        cdef Morsel result

        # If no filtering, do a simple full copy
        if columns is None and mask is None:
            return self._full_copy()

        # Apply column filtering first if specified
        if columns is not None:
            result = self._full_copy()
            result._select_inplace(columns)
        else:
            result = self._full_copy()

        # Apply row filtering (mask) if specified
        if mask is not None:
            if result._looks_like_boolean_mask(mask):
                result._filter_mask_inplace(mask)
            else:
                result._take_inplace(mask)

        return result

    def filter_mask(self, mask) -> Morsel:
        """
        Filter rows using a boolean mask (True keeps row, False/None drops row).

        Args:
            mask: BoolVector or boolean-like sequence with length == num_rows.

        Returns:
            Morsel: Self (for method chaining)
        """
        self._filter_mask_inplace(mask)
        return self

    cpdef void append(self, Morsel other):
        """Append another morsel's rows to this morsel in-place."""
        cdef Py_ssize_t i
        cdef Py_ssize_t n_columns
        cdef Py_ssize_t left_rows
        cdef Py_ssize_t right_rows
        cdef Py_ssize_t total_rows
        cdef Vector left_vec
        cdef Vector right_vec
        cdef Vector new_vec
        cdef list values

        cdef Int64Vector out_i64
        cdef Float64Vector out_f64
        cdef BoolVector out_bool
        cdef IntegerVector out_int
        cdef Date32Vector out_date32
        cdef TimeVector out_time
        cdef TimestampVector out_ts
        cdef IntervalVector out_interval
        cdef StringVector out_str
        cdef Py_ssize_t total_string_bytes

        if other is None:
            return
        if self.ptr is NULL or other.ptr is NULL:
            raise ValueError("Cannot append uninitialized morsels")

        n_columns = self.ptr.num_columns
        if n_columns != other.ptr.num_columns:
            raise ValueError(
                f"Cannot append morsel with {other.ptr.num_columns} columns "
                f"to morsel with {n_columns} columns"
            )

        for i in range(n_columns):
            if self._encoded_names[i] != other._encoded_names[i]:
                raise ValueError(
                    f"Cannot append morsels with different schemas: "
                    f"column {i} differs ({self._encoded_names[i]!r} != {other._encoded_names[i]!r})"
                )

        right_rows = other.ptr.num_rows
        if right_rows == 0:
            return

        left_rows = self.ptr.num_rows
        total_rows = left_rows + right_rows

        for i in range(n_columns):
            left_vec = <Vector> self.ptr.columns[i]
            right_vec = <Vector> other.ptr.columns[i]

            if isinstance(left_vec, Int64Vector) and isinstance(right_vec, Int64Vector):
                out_i64 = Int64Vector(<size_t>total_rows)
                _concat_fixed_buffers(
                    out_i64.ptr,
                    (<Int64Vector>left_vec).ptr,
                    (<Int64Vector>right_vec).ptr,
                    left_rows,
                    right_rows,
                )
                new_vec = <Vector>out_i64

            elif isinstance(left_vec, Float64Vector) and isinstance(right_vec, Float64Vector):
                out_f64 = Float64Vector(<size_t>total_rows)
                _concat_fixed_buffers(
                    out_f64.ptr,
                    (<Float64Vector>left_vec).ptr,
                    (<Float64Vector>right_vec).ptr,
                    left_rows,
                    right_rows,
                )
                new_vec = <Vector>out_f64

            elif isinstance(left_vec, BoolVector) and isinstance(right_vec, BoolVector):
                out_bool = BoolVector(<size_t>total_rows)
                _concat_bool_buffers(
                    out_bool.ptr,
                    (<BoolVector>left_vec).ptr,
                    (<BoolVector>right_vec).ptr,
                    left_rows,
                    right_rows,
                )
                new_vec = <Vector>out_bool

            elif isinstance(left_vec, IntegerVector) and isinstance(right_vec, IntegerVector):
                if (<IntegerVector>left_vec).ptr.type != (<IntegerVector>right_vec).ptr.type:
                    values = left_vec.to_pylist()
                    values.extend(right_vec.to_pylist())
                    new_vec = <Vector>vector_from_sequence(values, self.ptr.column_types[i])
                else:
                    out_int = IntegerVector((<IntegerVector>left_vec).ptr.type, <size_t>total_rows)
                    _concat_fixed_buffers(
                        out_int.ptr,
                        (<IntegerVector>left_vec).ptr,
                        (<IntegerVector>right_vec).ptr,
                        left_rows,
                        right_rows,
                    )
                    new_vec = <Vector>out_int

            elif isinstance(left_vec, Date32Vector) and isinstance(right_vec, Date32Vector):
                out_date32 = Date32Vector(<size_t>total_rows)
                _concat_fixed_buffers(
                    out_date32.ptr,
                    (<Date32Vector>left_vec).ptr,
                    (<Date32Vector>right_vec).ptr,
                    left_rows,
                    right_rows,
                )
                new_vec = <Vector>out_date32

            elif isinstance(left_vec, TimeVector) and isinstance(right_vec, TimeVector):
                if (<TimeVector>left_vec).is_time64 != (<TimeVector>right_vec).is_time64:
                    values = left_vec.to_pylist()
                    values.extend(right_vec.to_pylist())
                    new_vec = <Vector>vector_from_sequence(values, self.ptr.column_types[i])
                else:
                    out_time = TimeVector(<size_t>total_rows, (<TimeVector>left_vec).is_time64)
                    _concat_fixed_buffers(
                        out_time.ptr,
                        (<TimeVector>left_vec).ptr,
                        (<TimeVector>right_vec).ptr,
                        left_rows,
                        right_rows,
                    )
                    new_vec = <Vector>out_time

            elif isinstance(left_vec, TimestampVector) and isinstance(right_vec, TimestampVector):
                out_ts = TimestampVector(<size_t>total_rows)
                out_ts.timestamp_unit = (<TimestampVector>left_vec).timestamp_unit
                out_ts.null_bit_offset = 0
                _concat_fixed_buffers(
                    out_ts.ptr,
                    (<TimestampVector>left_vec).ptr,
                    (<TimestampVector>right_vec).ptr,
                    left_rows,
                    right_rows,
                    (<TimestampVector>left_vec).null_bit_offset,
                    (<TimestampVector>right_vec).null_bit_offset,
                )
                new_vec = <Vector>out_ts

            elif isinstance(left_vec, IntervalVector) and isinstance(right_vec, IntervalVector):
                out_interval = IntervalVector(<size_t>total_rows)
                _concat_fixed_buffers(
                    out_interval.ptr,
                    (<IntervalVector>left_vec).ptr,
                    (<IntervalVector>right_vec).ptr,
                    left_rows,
                    right_rows,
                )
                new_vec = <Vector>out_interval

            elif isinstance(left_vec, StringVector) and isinstance(right_vec, StringVector):
                total_string_bytes = (
                    (<StringVector>left_vec).ptr.offsets[left_rows]
                    + (<StringVector>right_vec).ptr.offsets[right_rows]
                )
                out_str = StringVector(<size_t>total_rows, <size_t>total_string_bytes)
                _concat_string_buffers(
                    out_str,
                    <StringVector>left_vec,
                    <StringVector>right_vec,
                    left_rows,
                    right_rows,
                )
                new_vec = <Vector>out_str

            else:
                # Generic fallback keeps API coverage for less common vector
                # classes. This path avoids Morsel<->Arrow conversion, but may
                # still use Arrow internally in vector_from_sequence for some
                # dtypes.
                values = left_vec.to_pylist()
                values.extend(right_vec.to_pylist())
                new_vec = <Vector>vector_from_sequence(values, self.ptr.column_types[i])

            self._columns[i] = new_vec
            self.ptr.columns[i] = <void*> new_vec
            self.ptr.column_types[i] = new_vec.dtype

        self.ptr.num_rows = total_rows

    cdef bint _looks_like_boolean_mask(self, object mask):
        cdef object dtype
        cdef object item

        if isinstance(mask, BoolVector):
            return True

        # Arrow boolean arrays/chunked arrays
        try:
            import pyarrow as pa
            if hasattr(mask, "type") and pa.types.is_boolean(mask.type):
                return True
        except Exception:
            pass

        # NumPy/pandas boolean arrays/series
        try:
            dtype = getattr(mask, "dtype", None)
            if dtype is not None and str(dtype) in ("bool", "bool_"):
                return True
        except Exception:
            pass

        if isinstance(mask, (list, tuple)):
            if len(mask) == 0:
                return False
            for item in mask:
                if item is None:
                    continue
                if not isinstance(item, bool):
                    return False
            return True

        return False

    cdef void _filter_mask_inplace(self, object mask):
        cdef Py_ssize_t i
        cdef Py_ssize_t n_rows = self.ptr.num_rows
        cdef Py_ssize_t selected = 0
        cdef int32_t* indices_ptr = NULL
        cdef int32_t[::1] indices_view
        cdef BoolVector bool_mask
        cdef uint8_t* data_bits
        cdef uint8_t* valid_bits
        cdef object py_mask
        cdef object item
        cdef bint keep

        if n_rows == 0:
            self._empty_inplace()
            return

        indices_ptr = <int32_t*> PyMem_Malloc(n_rows * sizeof(int32_t))
        if indices_ptr == NULL:
            raise MemoryError()

        try:
            if isinstance(mask, BoolVector):
                bool_mask = <BoolVector> mask
                if bool_mask.length != n_rows:
                    raise ValueError(
                        f"Boolean mask length {bool_mask.length} does not match morsel row count {n_rows}"
                    )

                data_bits = <uint8_t*> bool_mask.ptr.data
                valid_bits = bool_mask.ptr.null_bitmap

                for i in range(n_rows):
                    if valid_bits != NULL and ((valid_bits[i >> 3] >> (i & 7)) & 1) == 0:
                        continue
                    if ((data_bits[i >> 3] >> (i & 7)) & 1) != 0:
                        indices_ptr[selected] = <int32_t> i
                        selected += 1

                if selected == 0:
                    self._empty_inplace()
                    return

                indices_view = <int32_t[:selected]> indices_ptr
                self._take_inplace(indices_view)
                return

            if hasattr(mask, "to_pylist"):
                py_mask = mask.to_pylist()
            elif hasattr(mask, "tolist"):
                py_mask = mask.tolist()
            else:
                py_mask = mask

            if not hasattr(py_mask, "__len__"):
                raise TypeError("filter_mask expects a boolean sequence")
            if not hasattr(py_mask, "__getitem__"):
                py_mask = list(py_mask)

            if len(py_mask) != n_rows:
                raise ValueError(
                    f"Boolean mask length {len(py_mask)} does not match morsel row count {n_rows}"
                )

            for i in range(n_rows):
                item = py_mask[i]
                if hasattr(item, "as_py"):
                    item = item.as_py()
                elif hasattr(item, "item"):
                    try:
                        item = item.item()
                    except Exception:
                        pass
                if item is None:
                    continue
                if isinstance(item, bool):
                    keep = <bint> item
                    if keep:
                        indices_ptr[selected] = <int32_t> i
                        selected += 1
                    continue
                raise TypeError("filter_mask expects booleans or nulls")

            indices_view = <int32_t[:selected]> indices_ptr
            self._take_inplace(indices_view)
        finally:
            PyMem_Free(indices_ptr)

    def empty(self) -> Morsel:
        """
        Make this morsel empty in-place while preserving schema (column names
        and types). Useful for operators that need an empty morsel with the
        same shape metadata.

        Returns:
            Morsel: self
        """
        self._empty_inplace()
        return self

    cdef Morsel _full_copy(self):
        """Create a complete copy of this Morsel."""
        cdef int i, n_columns = self.ptr.num_columns
        cdef Morsel result = Morsel()
        cdef Vector vec

        # Initialize result morsel
        result._columns = [None] * n_columns
        result._encoded_names = [None] * n_columns
        result.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        result.ptr.num_columns = n_columns
        result.ptr.num_rows = self.ptr.num_rows
        result.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n_columns)
        result.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n_columns)
        result.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n_columns)

        # Copy all columns (vectors are referenced, not deep-copied for performance)
        for i in range(n_columns):
            vec = <Vector>self.ptr.columns[i]
            result._columns[i] = vec
            result._encoded_names[i] = self._encoded_names[i]
            result.ptr.columns[i] = <void*>vec
            result.ptr.column_types[i] = self.ptr.column_types[i]
            result.ptr.column_names[i] = self.ptr.column_names[i]

        result._rebuild_name_to_index()
        return result

    def take(self, indices) -> Morsel:
        """
        Take rows by indices (IN-PLACE operation - modifies this Morsel).

        Args:
            indices: List or array of row indices to select

        Returns:
            Morsel: Self (for method chaining)
        """
        self._take_inplace(indices)
        return self

    cdef void _take_inplace(self, indices):
        """Internal in-place take implementation."""
        cdef int32_t[::1] indices_view
        cdef int i, n_indices, n_columns = self.ptr.num_columns
        cdef Vector src_vec, dst_vec
        cdef int32_t* indices_ptr = NULL
        cdef int64_t[::1] input_view_64
        cdef int32_t[::1] input_view_32
        cdef bint free_indices = False
        cdef bint indices_ready = False

        # Try fast path for int32 memoryview (e.g. Int32Buffer)
        if not indices_ready:
            try:
                input_view_32 = indices
                n_indices = input_view_32.shape[0]
                if n_indices == 0:
                    self._empty_inplace()
                    return
                indices_view = input_view_32
                indices_ready = True
            except (TypeError, ValueError):
                pass

        # Try fast path for int64 memoryview (e.g. from numpy)
        if not indices_ready:
            try:
                input_view_64 = indices
                n_indices = input_view_64.shape[0]
                if n_indices == 0:
                    self._empty_inplace()
                    return
                
                indices_ptr = <int32_t*>PyMem_Malloc(n_indices * sizeof(int32_t))
                if indices_ptr == NULL:
                    raise MemoryError()
                free_indices = True
                
                # Fast copy/cast loop
                for i in range(n_indices):
                    indices_ptr[i] = <int32_t>input_view_64[i]
                    
                indices_view = <int32_t[:n_indices]>indices_ptr
                indices_ready = True
                
            except (TypeError, ValueError):
                pass

        if not indices_ready:
            # Fallback to existing logic
            if not hasattr(indices, '__len__'):
                indices = [indices]

            if hasattr(indices, 'to_pylist'):
                indices = indices.to_pylist()
            elif hasattr(indices, 'tolist'):  # Handle numpy arrays if passed in
                indices = indices.tolist()

            # Convert to C array
            n_indices = len(indices)
            # Fast-path: empty selection -> replace each column with an empty
            # vector of the same concrete class and set rowcount to 0.
            if n_indices == 0:
                self._empty_inplace()
                return

            indices_ptr = <int32_t*>PyMem_Malloc(n_indices * sizeof(int32_t))
            if indices_ptr == NULL:
                raise MemoryError()
            free_indices = True

            for i in range(n_indices):
                indices_ptr[i] = <int32_t>indices[i]

            # Create memoryview from C array
            indices_view = <int32_t[:n_indices]>indices_ptr

        try:
            # Take from each column using vector's native take method
            for i in range(n_columns):
                src_vec = <Vector>self.ptr.columns[i]

                # All vector types should now have take method
                dst_vec = src_vec.take(indices_view)

                # Replace the vector in-place
                self._columns[i] = dst_vec
                self.ptr.columns[i] = <void*>dst_vec

            # Update row count
            self.ptr.num_rows = n_indices

        finally:
            if free_indices and indices_ptr != NULL:
                PyMem_Free(indices_ptr)

    cdef void _empty_inplace(self):
        """Replace each column with a zero-length vector of the same class.

        This preserves column types and names while ensuring a valid internal
        layout that converts cleanly to Arrow (offset arrays, null bitmaps,
        etc.).
        """
        cdef int i, n_columns = self.ptr.num_columns
        cdef object empty_indices
        cdef Vector src_vec
        cdef Vector dst_vec

        try:
            from array import array as pyarray

            empty_indices = pyarray("i")
        except Exception:
            empty_indices = []

        for i in range(n_columns):
            src_vec = <Vector>self.ptr.columns[i]
            try:
                dst_vec = src_vec.take(empty_indices)
                if dst_vec is not None:
                    self._columns[i] = dst_vec
                    self.ptr.columns[i] = <void*>dst_vec
                    continue
            except Exception:
                pass

            dst_vec = self._empty_vector_like(i, src_vec)

            self._columns[i] = dst_vec
            self.ptr.columns[i] = <void*>dst_vec

        # Ensure num_rows is zero
        self.ptr.num_rows = 0

    cdef Vector _empty_vector_like(self, Py_ssize_t column_index, Vector src_vec):
        """Create an empty vector that preserves the source vector's type."""
        cdef DrakenType expected = self.ptr.column_types[column_index]
        cdef Vector candidate

        # First try to instantiate the vector class directly. Prefer this
        # path and fail fast if the concrete vector cannot build an empty
        # instance without help.
        try:
            candidate = src_vec.__class__(<size_t>0)
            if candidate is not None and self._vector_dtype_matches(candidate, expected):
                return candidate
        except Exception:
            candidate = None

        # Fall back to an empty Arrow round-trip. This preserves the source
        # logical type for vectors that cannot be instantiated with a bare
        # size argument, including Arrow-backed non-native vectors.
        try:
            import pyarrow as pa

            candidate = vector_from_arrow(pa.array([], type=src_vec.to_arrow().type))
            if candidate is not None and self._vector_dtype_matches(candidate, expected):
                return candidate
        except Exception:
            candidate = None

        cdef int expected_code = <int>expected
        raise RuntimeError(
            f"Unable to create empty vector for column {int(column_index)} "
            f"(DrakenType {expected_code})"
        )

    cdef bint _vector_dtype_matches(self, Vector vector, DrakenType expected):
        """Best-effort check that a vector reports the requested dtype."""

        try:
            return vector.dtype == expected
        except Exception:
            # If the vector does not expose dtype, accept it. Downstream
            # consumers (hashing/to_arrow) will validate behavior.
            return True

    def select(self, columns) -> Morsel:
        """
        Select columns by name (IN-PLACE operation - modifies this Morsel).

        Args:
            columns: List of column names to select, or single column name

        Returns:
            Morsel: Self (for method chaining)
        """
        self._select_inplace(columns)
        return self

    cdef void _select_inplace(self, columns):
        """Internal in-place select implementation."""
        cdef int j, n_selected
        cdef list column_indices = []
        cdef Vector vec

        # Normalize columns to list
        if isinstance(columns, str):
            columns = [columns]
        elif isinstance(columns, bytes):
            columns = [columns]

        # Find column indices efficiently using cache
        for col in columns:
            column_indices.append(self._column_index_from_name(col))

        n_selected = len(column_indices)

        # Reallocate arrays for selected columns
        cdef void** new_columns = <void**>PyMem_Malloc(sizeof(void*) * n_selected)
        cdef const char** new_column_names = <const char**>PyMem_Malloc(sizeof(const char*) * n_selected)
        cdef DrakenType* new_column_types = <DrakenType*>PyMem_Malloc(sizeof(DrakenType) * n_selected)
        cdef list new_column_list = [None] * n_selected
        cdef list new_encoded_names = [None] * n_selected

        # Copy selected columns
        for j, i in enumerate(column_indices):
            vec = <Vector>self.ptr.columns[i]
            new_column_list[j] = vec
            new_encoded_names[j] = self._encoded_names[i]
            new_columns[j] = <void*>vec
            new_column_types[j] = self.ptr.column_types[i]
            new_column_names[j] = self.ptr.column_names[i]

        # Free old arrays and replace with new ones
        PyMem_Free(self.ptr.columns)
        PyMem_Free(self.ptr.column_names)
        PyMem_Free(self.ptr.column_types)

        self.ptr.columns = new_columns
        self.ptr.column_names = new_column_names
        self.ptr.column_types = new_column_types
        self.ptr.num_columns = n_selected
        self._columns = new_column_list
        self._encoded_names = new_encoded_names
        self._rebuild_name_to_index()

    cpdef void append_vector(self, object name, Vector vector):
        """Append a single vector as a new column in-place."""
        cdef Py_ssize_t i
        cdef Py_ssize_t n_columns
        cdef Py_ssize_t vector_length
        cdef bytes encoded_name
        cdef void** new_columns = NULL
        cdef const char** new_column_names = NULL
        cdef DrakenType* new_column_types = NULL
        cdef list new_column_list
        cdef list new_encoded_names
        cdef Vector existing_vec

        if isinstance(name, str):
            encoded_name = name.encode("utf-8")
        elif isinstance(name, bytes):
            encoded_name = <bytes>name
        else:
            raise TypeError("column name must be str or bytes")

        vector_length = len(vector)

        if self.ptr is NULL:
            self.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
            if self.ptr == NULL:
                raise MemoryError()

            self.ptr.num_columns = 1
            self.ptr.num_rows = vector_length
            self.ptr.columns = <void**> PyMem_Malloc(sizeof(void*))
            self.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*))
            self.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType))
            if (
                self.ptr.columns == NULL
                or self.ptr.column_names == NULL
                or self.ptr.column_types == NULL
            ):
                if self.ptr.columns != NULL:
                    PyMem_Free(self.ptr.columns)
                if self.ptr.column_names != NULL:
                    PyMem_Free(self.ptr.column_names)
                if self.ptr.column_types != NULL:
                    PyMem_Free(self.ptr.column_types)
                PyMem_Free(self.ptr)
                self.ptr = NULL
                raise MemoryError()

            self._columns = [vector]
            self._encoded_names = [encoded_name]
            self.ptr.columns[0] = <void*>vector
            self.ptr.column_names[0] = <const char*>encoded_name
            self.ptr.column_types[0] = vector.dtype
            self._rebuild_name_to_index()
            return

        n_columns = self.ptr.num_columns
        if self.ptr.num_rows != vector_length:
            raise ValueError(
                f"Cannot append vector of length {vector_length} to morsel with {self.ptr.num_rows} rows"
            )

        new_columns = <void**> PyMem_Malloc(sizeof(void*) * (n_columns + 1))
        new_column_names = <const char**> PyMem_Malloc(sizeof(const char*) * (n_columns + 1))
        new_column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * (n_columns + 1))
        if new_columns == NULL or new_column_names == NULL or new_column_types == NULL:
            if new_columns != NULL:
                PyMem_Free(new_columns)
            if new_column_names != NULL:
                PyMem_Free(new_column_names)
            if new_column_types != NULL:
                PyMem_Free(new_column_types)
            raise MemoryError()

        new_column_list = [None] * (n_columns + 1)
        new_encoded_names = [None] * (n_columns + 1)

        for i in range(n_columns):
            existing_vec = <Vector>self.ptr.columns[i]
            new_column_list[i] = existing_vec
            new_encoded_names[i] = self._encoded_names[i]
            new_columns[i] = <void*>existing_vec
            new_column_names[i] = self.ptr.column_names[i]
            new_column_types[i] = self.ptr.column_types[i]

        new_column_list[n_columns] = vector
        new_encoded_names[n_columns] = encoded_name
        new_columns[n_columns] = <void*>vector
        new_column_names[n_columns] = <const char*>encoded_name
        new_column_types[n_columns] = vector.dtype

        PyMem_Free(self.ptr.columns)
        PyMem_Free(self.ptr.column_names)
        PyMem_Free(self.ptr.column_types)

        self.ptr.columns = new_columns
        self.ptr.column_names = new_column_names
        self.ptr.column_types = new_column_types
        self.ptr.num_columns = n_columns + 1
        self._columns = new_column_list
        self._encoded_names = new_encoded_names
        self._rebuild_name_to_index()

    def rename(self, names) -> Morsel:
        """
        Rename columns (IN-PLACE operation - modifies this Morsel).

        Args:
            names: List of new column names or dict mapping old->new names

        Returns:
            Morsel: Self (for method chaining)
        """
        cdef int i, n_columns = self.ptr.num_columns
        cdef list new_names = []
        cdef bytes encoded_name

        # Handle different name formats
        if isinstance(names, dict):
            # Dict mapping old->new names
            for i in range(n_columns):
                old_name = self.ptr.column_names[i].decode('utf-8')
                new_names.append(names.get(old_name, old_name))
        else:
            # List of new names
            if len(names) != n_columns:
                raise ValueError(f"Expected {n_columns} names, got {len(names)}")
            new_names = list(names)

        # Update column names in-place
        for i in range(n_columns):
            encoded_name = new_names[i].encode('utf-8')
            self._encoded_names[i] = encoded_name
            self.ptr.column_names[i] = <const char*>encoded_name

        self._rebuild_name_to_index()
        return self

    def to_arrow(self):
        """
        Convert Morsel to Arrow Table with high-performance implementation.

        Returns:
            pyarrow.Table: Table with same data and column names
        """
        import pyarrow as pa

        # Get column names as strings
        column_names = []
        cdef int i
        for i in range(self.ptr.num_columns):
            column_names.append(self.ptr.column_names[i].decode('utf-8'))

        # Get arrow columns from vectors using their native to_arrow methods
        arrow_columns = []
        cdef Vector vec
        for i in range(self.ptr.num_columns):
            vec = <Vector>self.ptr.columns[i]
            arrow_columns.append(vec.to_arrow())

        return pa.table(arrow_columns, names=column_names)

    cpdef uint64_t[::1] hash(self, columns=None):
        """Return per-row hash values, optionally restricted to selected columns."""
        cdef Py_ssize_t row_count = self.ptr.num_rows
        cdef Py_ssize_t idx
        cdef list column_indices
        cdef Py_ssize_t n_selected
        cdef Py_ssize_t alloc_rows
        cdef uint64_t* out_buf
        cdef Vector vec

        if columns is None:
            column_indices = list(range(self.ptr.num_columns))
        else:
            if isinstance(columns, (str, bytes, int)):
                columns = [columns]

            column_indices = []
            for col in columns:
                column_indices.append(self._column_index_from_name(col))

        n_selected = len(column_indices)

        if row_count == 0:
            from array import array

            return array("Q")

        if n_selected == 0:
            alloc_rows = row_count if row_count > 0 else 1
            out_buf = <uint64_t*> PyMem_Calloc(alloc_rows, sizeof(uint64_t))
            if out_buf == NULL:
                raise MemoryError()
            return <uint64_t[:row_count]> out_buf

        if n_selected == 1:
            vec = <Vector>self.ptr.columns[column_indices[0]]
            return vec.hash()

        alloc_rows = row_count if row_count > 0 else 1
        out_buf = <uint64_t*> PyMem_Calloc(alloc_rows, sizeof(uint64_t))
        if out_buf == NULL:
            raise MemoryError()

        cdef uint64_t[::1] out_view = <uint64_t[:row_count]> out_buf

        for idx in column_indices:
            vec = <Vector> self.ptr.columns[idx]
            vec.hash_into(out_view, 0)

        return <uint64_t[:row_count]> out_buf
