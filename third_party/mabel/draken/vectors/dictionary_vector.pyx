# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from cpython.buffer cimport PyBUF_READ
from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_AsStringAndSize, PyBytes_FromStringAndSize
from cpython.memoryview cimport PyMemoryView_FromMemory
from libc.stddef cimport size_t
from libc.stdint cimport (
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    intptr_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
)
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memcmp, memset
from libcpp.string cimport string

from opteryx.draken.core.buffers cimport (
    DRAKEN_BOOL,
    DRAKEN_DICTIONARY,
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DictAccessor,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_INT8,
    DRAKEN_STRING,
    DrakenType,
    DrakenDictionaryBuffer,
    DrakenVarBuffer,
)
from opteryx.draken.core.var_vector cimport alloc_var_buffer, free_var_buffer
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.vector cimport NULL_HASH, Vector, mix_hash
from opteryx.compiled.structures.relation_statistics cimport to_int


cdef extern from *:
    """
    #define XXH_INLINE_ALL
    #include "xxhash.h"
    """
    uint64_t XXH3_64bits(const void* input, size_t length) nogil


cdef extern from "re2/stringpiece.h" namespace "re2":
    cdef cppclass StringPiece:
        StringPiece() except +
        StringPiece(const char* data, size_t length) except +
        size_t size() const


cdef extern from "re2/re2.h" namespace "re2":
    cdef cppclass RE2:
        RE2(const string& pattern) except +
        bint ok() const

        enum Anchor:
            UNANCHORED
            ANCHOR_START
            ANCHOR_BOTH

        bint Match(
            const StringPiece& text,
            size_t startpos,
            size_t endpos,
            Anchor re_anchor,
            StringPiece* submatch,
            int nsubmatch,
        ) const


cdef inline uint8_t _code_width_for_dict_size(Py_ssize_t dict_size):
    if dict_size <= 256:
        return 1
    if dict_size <= 65536:
        return 2
    return 4


cdef inline bint _is_string_dict_type(int dict_type) noexcept nogil:
    return dict_type == DRAKEN_STRING


cdef inline int32_t _dict_itemsize_for_type(int dict_type) noexcept:
    if dict_type == DRAKEN_INT8 or dict_type == DRAKEN_BOOL:
        return 1
    if dict_type == DRAKEN_INT16:
        return 2
    if dict_type == DRAKEN_INT32 or dict_type == DRAKEN_FLOAT32:
        return 4
    if dict_type == DRAKEN_INT64 or dict_type == DRAKEN_FLOAT64:
        return 8
    return -1


cdef inline uint32_t _read_code(const DrakenDictionaryBuffer* ptr, Py_ssize_t i) noexcept nogil:
    if ptr.code_width == 1:
        return (<uint8_t*>ptr.codes)[i]
    if ptr.code_width == 2:
        return (<uint16_t*>ptr.codes)[i]
    return (<uint32_t*>ptr.codes)[i]


cdef inline void _write_code(DrakenDictionaryBuffer* ptr, Py_ssize_t i, uint32_t code) noexcept nogil:
    if ptr.code_width == 1:
        (<uint8_t*>ptr.codes)[i] = <uint8_t>code
    elif ptr.code_width == 2:
        (<uint16_t*>ptr.codes)[i] = <uint16_t>code
    else:
        (<uint32_t*>ptr.codes)[i] = <uint32_t>code


cdef inline object _coerce_literal_bytes(object literal):
    if literal is None:
        return None
    if hasattr(literal, "as_py"):
        try:
            literal = literal.as_py()
        except Exception:
            return None
    if isinstance(literal, bytes):
        return literal
    if isinstance(literal, str):
        try:
            return literal.encode("utf8")
        except Exception:
            return None
    return None


cdef inline void _set_true_bit(uint8_t* bits, Py_ssize_t i) noexcept nogil:
    bits[i >> 3] |= <uint8_t>(1 << (i & 7))


cdef inline uint8_t _ascii_lower(uint8_t b) noexcept nogil:
    if b >= 65 and b <= 90:
        return b + 32
    return b


cdef inline bint _byte_equals(uint8_t left, uint8_t right, bint ignore_case) noexcept nogil:
    if ignore_case:
        return _ascii_lower(left) == _ascii_lower(right)
    return left == right


cdef bint _sql_like_match(
    const uint8_t* text,
    Py_ssize_t text_len,
    const uint8_t* pattern,
    Py_ssize_t pattern_len,
    bint ignore_case,
) noexcept nogil:
    cdef Py_ssize_t ti = 0
    cdef Py_ssize_t pi = 0
    cdef Py_ssize_t last_pct = -1
    cdef Py_ssize_t last_match = 0
    cdef uint8_t pc

    while ti < text_len:
        if pi < pattern_len:
            pc = pattern[pi]

            # Escape sequence: treat next character as literal.
            if pc == 92 and (pi + 1) < pattern_len:
                if _byte_equals(text[ti], pattern[pi + 1], ignore_case):
                    ti += 1
                    pi += 2
                    continue
            elif pc == 95:  # "_"
                ti += 1
                pi += 1
                continue
            elif pc == 37:  # "%"
                last_pct = pi
                pi += 1
                last_match = ti
                continue
            elif _byte_equals(text[ti], pc, ignore_case):
                ti += 1
                pi += 1
                continue

        if last_pct != -1:
            last_match += 1
            ti = last_match
            pi = last_pct + 1
            continue
        return False

    while pi < pattern_len and pattern[pi] == 37:
        pi += 1

    return pi == pattern_len


cdef void _copy_bitmap_shifted(
    uint8_t* src,
    uint8_t* dst,
    Py_ssize_t offset,
    Py_ssize_t length,
) noexcept nogil:
    cdef Py_ssize_t i
    cdef int shift = offset & 7
    cdef Py_ssize_t byte_offset = offset >> 3
    cdef Py_ssize_t num_bytes = (length + 7) // 8

    if num_bytes == 0:
        return

    if shift == 0:
        memcpy(dst, src + byte_offset, num_bytes)
        return

    for i in range(num_bytes - 1):
        dst[i] = (src[byte_offset + i] >> shift) | (src[byte_offset + i + 1] << (8 - shift))

    i = num_bytes - 1
    if ((offset + length - 1) >> 3) > (byte_offset + i):
        dst[i] = (src[byte_offset + i] >> shift) | (src[byte_offset + i + 1] << (8 - shift))
    else:
        dst[i] = src[byte_offset + i] >> shift


cdef class DictionaryVector(Vector):
    def __cinit__(
        self,
        size_t length=0,
        size_t dict_length=0,
        size_t dict_bytes_cap=0,
        uint8_t code_width=1,
        bint ordered=False,
        int dict_value_type=DRAKEN_STRING,
        bint wrap=False,
    ):
        cdef size_t code_bytes

        if wrap:
            self.ptr = NULL
            self.owns_data = False
            self.owns_dictionary_values = False
            self._dict_owner_ref = None
            self._accessor.codes = NULL
            self._accessor.code_width = 0
            self._accessor.row_nulls = NULL
            self._accessor.length = 0
            self._accessor.dict_values = NULL
            self._accessor.value_type = DRAKEN_STRING
            return

        self.ptr = <DrakenDictionaryBuffer*>malloc(sizeof(DrakenDictionaryBuffer))
        if self.ptr == NULL:
            raise MemoryError()

        if code_width not in (1, 2, 4):
            free(self.ptr)
            self.ptr = NULL
            raise ValueError("DictionaryVector code_width must be one of 1, 2, or 4")

        code_bytes = length * <size_t>code_width
        if code_bytes > 0:
            self.ptr.codes = <uint8_t*>malloc(code_bytes)
            if self.ptr.codes == NULL:
                free(self.ptr)
                self.ptr = NULL
                raise MemoryError()
            memset(self.ptr.codes, 0, code_bytes)
        else:
            self.ptr.codes = NULL

        if not _is_string_dict_type(dict_value_type) and _dict_itemsize_for_type(dict_value_type) <= 0:
            if self.ptr.codes != NULL:
                free(self.ptr.codes)
            free(self.ptr)
            self.ptr = NULL
            raise ValueError("Unsupported DictionaryVector child type")

        self.ptr.dictionary_values = alloc_var_buffer(<DrakenType>dict_value_type, dict_length, dict_bytes_cap)
        if self.ptr.dictionary_values.offsets != NULL:
            self.ptr.dictionary_values.offsets[0] = 0

        self.ptr.code_width = code_width
        self.ptr.null_bitmap = NULL
        self.ptr.length = length
        self.ptr.ordered = 1 if ordered else 0
        self.ptr.type = DRAKEN_DICTIONARY
        self.owns_data = True
        self.owns_dictionary_values = True
        self._dict_owner_ref = None
        self._accessor.codes = NULL
        self._accessor.code_width = 0
        self._accessor.row_nulls = NULL
        self._accessor.length = 0
        self._accessor.dict_values = NULL
        self._accessor.value_type = DRAKEN_STRING

    def __dealloc__(self):
        if self.ptr == NULL:
            return
        if self.owns_data:
            if self.ptr.codes != NULL:
                free(self.ptr.codes)
            if self.owns_dictionary_values and self.ptr.dictionary_values != NULL:
                free_var_buffer(self.ptr.dictionary_values, True)
            if self.ptr.null_bitmap != NULL:
                free(self.ptr.null_bitmap)
        free(self.ptr)
        self.ptr = NULL

    @property
    def length(self):
        return self.ptr.length

    def __len__(self):
        return self.ptr.length

    @property
    def dtype(self):
        return DRAKEN_DICTIONARY

    cdef DictAccessor* dict_accessor(self) noexcept:
        if self.ptr == NULL:
            return NULL
        self._accessor.codes = self.ptr.codes
        self._accessor.code_width = self.ptr.code_width
        self._accessor.row_nulls = self.ptr.null_bitmap
        self._accessor.length = self.ptr.length
        self._accessor.dict_values = self.ptr.dictionary_values
        if self.ptr.dictionary_values != NULL:
            self._accessor.value_type = self.ptr.dictionary_values.type
        else:
            self._accessor.value_type = DRAKEN_STRING
        return &self._accessor

    cdef void* dense_ptr(self) noexcept:
        return NULL

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        if self.ptr == NULL:
            return NULL
        return self.ptr.null_bitmap

    @property
    def code_width(self):
        return self.ptr.code_width

    @property
    def dictionary_size(self):
        if self.ptr.dictionary_values == NULL:
            return 0
        return self.ptr.dictionary_values.length

    @property
    def dictionary_value_type(self):
        if self.ptr.dictionary_values == NULL:
            return DRAKEN_STRING
        return self.ptr.dictionary_values.type

    @property
    def ordered(self):
        return bool(self.ptr.ordered)

    @property
    def null_count(self):
        cdef Py_ssize_t i, n = self.ptr.length
        cdef Py_ssize_t count = 0
        cdef uint8_t byte
        if self.ptr.null_bitmap == NULL:
            return 0
        for i in range(n):
            byte = self.ptr.null_bitmap[i >> 3]
            if ((byte >> (i & 7)) & 1) == 0:
                count += 1
        return count

    cpdef object null_bitmap(self):
        cdef Py_ssize_t nb_size
        if self.ptr.null_bitmap == NULL:
            return None
        nb_size = (self.ptr.length + 7) // 8
        if nb_size == 0:
            nb_size = 1
        return PyMemoryView_FromMemory(<char*>self.ptr.null_bitmap, nb_size, PyBUF_READ)

    def __getitem__(self, Py_ssize_t i):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf
        cdef uint8_t byte
        cdef uint32_t code
        cdef int32_t start, end

        if i < 0 or i >= <Py_ssize_t>ptr.length:
            raise IndexError("Index out of range")

        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            if ((byte >> (i & 7)) & 1) == 0:
                return None

        code = _read_code(ptr, i)
        dict_buf = ptr.dictionary_values
        if dict_buf == NULL or code >= dict_buf.length:
            raise IndexError("Dictionary code out of range")

        if dict_buf.null_bitmap != NULL:
            byte = dict_buf.null_bitmap[code >> 3]
            if ((byte >> (code & 7)) & 1) == 0:
                return None

        if _is_string_dict_type(dict_buf.type):
            start = dict_buf.offsets[code]
            end = dict_buf.offsets[code + 1]
            return PyBytes_FromStringAndSize(<char*>dict_buf.data + start, end - start)

        if dict_buf.type == DRAKEN_INT8:
            return (<int8_t*>dict_buf.data)[code]
        if dict_buf.type == DRAKEN_INT16:
            return (<int16_t*>dict_buf.data)[code]
        if dict_buf.type == DRAKEN_INT32:
            return (<int32_t*>dict_buf.data)[code]
        if dict_buf.type == DRAKEN_INT64:
            return (<int64_t*>dict_buf.data)[code]
        if dict_buf.type == DRAKEN_FLOAT32:
            return (<float*>dict_buf.data)[code]
        if dict_buf.type == DRAKEN_FLOAT64:
            return (<double*>dict_buf.data)[code]
        if dict_buf.type == DRAKEN_BOOL:
            return (<uint8_t*>dict_buf.data)[code] != 0

        raise TypeError("Unsupported dictionary value type")

    cpdef list to_pylist(self):
        cdef Py_ssize_t i, n = self.ptr.length
        cdef list out = []
        for i in range(n):
            out.append(self[i])
        return out

    cdef BoolVector _equals_numeric(self, object literal, bint invert):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef int dict_type
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n
        cdef Py_ssize_t i
        cdef uint8_t byte
        cdef uint32_t code
        cdef uint8_t* dict_matches = NULL
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef object lit_obj = literal
        cdef int64_t lit_i64 = 0
        cdef double lit_f64 = 0
        cdef bint lit_bool = False
        cdef bint parse_ok = True

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if dict_buf == NULL or dict_buf.length == 0:
            if invert:
                for i in range(n):
                    if ptr.null_bitmap != NULL:
                        byte = ptr.null_bitmap[i >> 3]
                        if ((byte >> (i & 7)) & 1) == 0:
                            continue
                    _set_true_bit(out_bits, i)
            return out

        if hasattr(lit_obj, "as_py"):
            try:
                lit_obj = lit_obj.as_py()
            except Exception:
                parse_ok = False

        dict_type = dict_buf.type
        if parse_ok and lit_obj is None:
            parse_ok = False

        if parse_ok:
            try:
                if dict_type == DRAKEN_FLOAT32 or dict_type == DRAKEN_FLOAT64:
                    lit_f64 = float(lit_obj)
                elif dict_type == DRAKEN_BOOL:
                    lit_bool = bool(lit_obj)
                else:
                    lit_i64 = int(lit_obj)
            except Exception:
                parse_ok = False

        if not parse_ok:
            if invert:
                for i in range(n):
                    if ptr.null_bitmap != NULL:
                        byte = ptr.null_bitmap[i >> 3]
                        if ((byte >> (i & 7)) & 1) == 0:
                            continue
                    _set_true_bit(out_bits, i)
            return out

        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue

                if dict_type == DRAKEN_INT8:
                    if (<int8_t*>dict_buf.data)[i] == <int8_t>lit_i64:
                        dict_matches[i] = 1
                elif dict_type == DRAKEN_INT16:
                    if (<int16_t*>dict_buf.data)[i] == <int16_t>lit_i64:
                        dict_matches[i] = 1
                elif dict_type == DRAKEN_INT32:
                    if (<int32_t*>dict_buf.data)[i] == <int32_t>lit_i64:
                        dict_matches[i] = 1
                elif dict_type == DRAKEN_INT64:
                    if (<int64_t*>dict_buf.data)[i] == <int64_t>lit_i64:
                        dict_matches[i] = 1
                elif dict_type == DRAKEN_FLOAT32:
                    if (<float*>dict_buf.data)[i] == <float>lit_f64:
                        dict_matches[i] = 1
                elif dict_type == DRAKEN_FLOAT64:
                    if (<double*>dict_buf.data)[i] == <double>lit_f64:
                        dict_matches[i] = 1
                elif dict_type == DRAKEN_BOOL:
                    if ((<uint8_t*>dict_buf.data)[i] != 0) == lit_bool:
                        dict_matches[i] = 1

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                code = _read_code(ptr, i)
                if code < dict_n and dict_matches[code] != 0:
                    if not invert:
                        _set_true_bit(out_bits, i)
                elif invert:
                    _set_true_bit(out_bits, i)
        finally:
            free(dict_matches)

        return out

    cdef BoolVector _in_list_numeric(self, object literals):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef int dict_type
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n
        cdef Py_ssize_t i
        cdef Py_ssize_t j
        cdef uint8_t byte
        cdef uint32_t code
        cdef uint8_t* dict_matches = NULL
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef bint include_null = False
        cdef object literal
        cdef object lit_obj
        cdef bint parse_ok
        cdef list int_literals = []
        cdef list float_literals = []
        cdef list bool_literals = []
        cdef int64_t* int_vals = NULL
        cdef double* float_vals = NULL
        cdef uint8_t* bool_vals = NULL
        cdef Py_ssize_t int_count = 0
        cdef Py_ssize_t float_count = 0
        cdef Py_ssize_t bool_count = 0

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if literals is None:
            return out

        dict_type = dict_buf.type if dict_buf != NULL else DRAKEN_INT64
        for literal in literals:
            if literal is None:
                include_null = True
                continue
            lit_obj = literal
            parse_ok = True
            if hasattr(lit_obj, "as_py"):
                try:
                    lit_obj = lit_obj.as_py()
                except Exception:
                    parse_ok = False
            if not parse_ok:
                continue
            try:
                if dict_type == DRAKEN_FLOAT32 or dict_type == DRAKEN_FLOAT64:
                    float_literals.append(float(lit_obj))
                elif dict_type == DRAKEN_BOOL:
                    bool_literals.append(1 if bool(lit_obj) else 0)
                else:
                    int_literals.append(int(lit_obj))
            except Exception:
                continue

        if dict_buf == NULL or dict_buf.length == 0:
            if include_null and ptr.null_bitmap != NULL:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        _set_true_bit(out_bits, i)
            return out

        int_count = len(int_literals)
        float_count = len(float_literals)
        bool_count = len(bool_literals)

        if int_count > 0:
            int_vals = <int64_t*>malloc(<size_t>int_count * sizeof(int64_t))
            if int_vals == NULL:
                raise MemoryError()
            for i in range(int_count):
                int_vals[i] = <int64_t>int_literals[i]
        if float_count > 0:
            float_vals = <double*>malloc(<size_t>float_count * sizeof(double))
            if float_vals == NULL:
                if int_vals != NULL:
                    free(int_vals)
                raise MemoryError()
            for i in range(float_count):
                float_vals[i] = <double>float_literals[i]
        if bool_count > 0:
            bool_vals = <uint8_t*>malloc(<size_t>bool_count)
            if bool_vals == NULL:
                if int_vals != NULL:
                    free(int_vals)
                if float_vals != NULL:
                    free(float_vals)
                raise MemoryError()
            for i in range(bool_count):
                bool_vals[i] = <uint8_t>bool_literals[i]

        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            if int_vals != NULL:
                free(int_vals)
            if float_vals != NULL:
                free(float_vals)
            if bool_vals != NULL:
                free(bool_vals)
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue

                if dict_type == DRAKEN_INT8:
                    for j in range(int_count):
                        if (<int8_t*>dict_buf.data)[i] == <int8_t>int_vals[j]:
                            dict_matches[i] = 1
                            break
                elif dict_type == DRAKEN_INT16:
                    for j in range(int_count):
                        if (<int16_t*>dict_buf.data)[i] == <int16_t>int_vals[j]:
                            dict_matches[i] = 1
                            break
                elif dict_type == DRAKEN_INT32:
                    for j in range(int_count):
                        if (<int32_t*>dict_buf.data)[i] == <int32_t>int_vals[j]:
                            dict_matches[i] = 1
                            break
                elif dict_type == DRAKEN_INT64:
                    for j in range(int_count):
                        if (<int64_t*>dict_buf.data)[i] == <int64_t>int_vals[j]:
                            dict_matches[i] = 1
                            break
                elif dict_type == DRAKEN_FLOAT32:
                    for j in range(float_count):
                        if (<float*>dict_buf.data)[i] == <float>float_vals[j]:
                            dict_matches[i] = 1
                            break
                elif dict_type == DRAKEN_FLOAT64:
                    for j in range(float_count):
                        if (<double*>dict_buf.data)[i] == <double>float_vals[j]:
                            dict_matches[i] = 1
                            break
                elif dict_type == DRAKEN_BOOL:
                    for j in range(bool_count):
                        if ((<uint8_t*>dict_buf.data)[i] != 0) == (bool_vals[j] != 0):
                            dict_matches[i] = 1
                            break

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        if include_null:
                            _set_true_bit(out_bits, i)
                        continue
                code = _read_code(ptr, i)
                if code < dict_n and dict_matches[code] != 0:
                    _set_true_bit(out_bits, i)
        finally:
            if dict_matches != NULL:
                free(dict_matches)
            if int_vals != NULL:
                free(int_vals)
            if float_vals != NULL:
                free(float_vals)
            if bool_vals != NULL:
                free(bool_vals)

        return out

    cdef BoolVector _compare_numeric(self, object literal, int op):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef int dict_type
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n
        cdef Py_ssize_t i
        cdef uint8_t byte
        cdef uint32_t code
        cdef uint8_t* dict_matches = NULL
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef object lit_obj = literal
        cdef int64_t lit_i64 = 0
        cdef double lit_f64 = 0
        cdef bint lit_bool = False
        cdef bint parse_ok = True
        cdef bint match = False
        cdef int8_t val_i8
        cdef int16_t val_i16
        cdef int32_t val_i32
        cdef int64_t val_i64
        cdef float val_f32
        cdef double val_f64
        cdef bint val_bool

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if dict_buf == NULL or dict_buf.length == 0:
            return out

        dict_type = dict_buf.type
        if _is_string_dict_type(dict_type):
            raise TypeError("Dictionary comparison kernels require numeric dictionary values")

        if hasattr(lit_obj, "as_py"):
            try:
                lit_obj = lit_obj.as_py()
            except Exception:
                parse_ok = False

        if parse_ok and lit_obj is None:
            parse_ok = False

        if parse_ok:
            try:
                if dict_type == DRAKEN_FLOAT32 or dict_type == DRAKEN_FLOAT64:
                    lit_f64 = float(lit_obj)
                elif dict_type == DRAKEN_BOOL:
                    lit_bool = bool(lit_obj)
                else:
                    lit_i64 = int(lit_obj)
            except Exception:
                parse_ok = False

        if not parse_ok:
            return out

        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue

                match = False
                if dict_type == DRAKEN_INT8:
                    val_i8 = (<int8_t*>dict_buf.data)[i]
                    if op == 0:
                        match = val_i8 < <int8_t>lit_i64
                    elif op == 1:
                        match = val_i8 > <int8_t>lit_i64
                    elif op == 2:
                        match = val_i8 <= <int8_t>lit_i64
                    elif op == 3:
                        match = val_i8 >= <int8_t>lit_i64
                elif dict_type == DRAKEN_INT16:
                    val_i16 = (<int16_t*>dict_buf.data)[i]
                    if op == 0:
                        match = val_i16 < <int16_t>lit_i64
                    elif op == 1:
                        match = val_i16 > <int16_t>lit_i64
                    elif op == 2:
                        match = val_i16 <= <int16_t>lit_i64
                    elif op == 3:
                        match = val_i16 >= <int16_t>lit_i64
                elif dict_type == DRAKEN_INT32:
                    val_i32 = (<int32_t*>dict_buf.data)[i]
                    if op == 0:
                        match = val_i32 < <int32_t>lit_i64
                    elif op == 1:
                        match = val_i32 > <int32_t>lit_i64
                    elif op == 2:
                        match = val_i32 <= <int32_t>lit_i64
                    elif op == 3:
                        match = val_i32 >= <int32_t>lit_i64
                elif dict_type == DRAKEN_INT64:
                    val_i64 = (<int64_t*>dict_buf.data)[i]
                    if op == 0:
                        match = val_i64 < <int64_t>lit_i64
                    elif op == 1:
                        match = val_i64 > <int64_t>lit_i64
                    elif op == 2:
                        match = val_i64 <= <int64_t>lit_i64
                    elif op == 3:
                        match = val_i64 >= <int64_t>lit_i64
                elif dict_type == DRAKEN_FLOAT32:
                    val_f32 = (<float*>dict_buf.data)[i]
                    if op == 0:
                        match = val_f32 < <float>lit_f64
                    elif op == 1:
                        match = val_f32 > <float>lit_f64
                    elif op == 2:
                        match = val_f32 <= <float>lit_f64
                    elif op == 3:
                        match = val_f32 >= <float>lit_f64
                elif dict_type == DRAKEN_FLOAT64:
                    val_f64 = (<double*>dict_buf.data)[i]
                    if op == 0:
                        match = val_f64 < <double>lit_f64
                    elif op == 1:
                        match = val_f64 > <double>lit_f64
                    elif op == 2:
                        match = val_f64 <= <double>lit_f64
                    elif op == 3:
                        match = val_f64 >= <double>lit_f64
                elif dict_type == DRAKEN_BOOL:
                    val_bool = (<uint8_t*>dict_buf.data)[i] != 0
                    if op == 0:
                        match = val_bool < lit_bool
                    elif op == 1:
                        match = val_bool > lit_bool
                    elif op == 2:
                        match = val_bool <= lit_bool
                    elif op == 3:
                        match = val_bool >= lit_bool

                if match:
                    dict_matches[i] = 1

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                code = _read_code(ptr, i)
                if code < dict_n and dict_matches[code] != 0:
                    _set_true_bit(out_bits, i)
        finally:
            free(dict_matches)

        return out

    cpdef BoolVector is_null_boolvector(self):
        """Return a BoolVector where True = SQL NULL position.

        Handles both null-bitmap nulls (ptr.null_bitmap) and NaN-encoded nulls
        in float32/float64 dictionary values. No Arrow round-trip.
        """
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n = 0
        cdef Py_ssize_t i
        cdef uint32_t code
        cdef uint8_t byte
        cdef uint8_t* dict_null = NULL
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef int dict_type = DRAKEN_STRING
        cdef float fval
        cdef double dval

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if dict_buf != NULL:
            dict_n = dict_buf.length
            dict_type = dict_buf.type

        if dict_n > 0:
            dict_null = <uint8_t*>malloc(dict_n)
            if dict_null == NULL:
                raise MemoryError()
            memset(dict_null, 0, dict_n)
            try:
                for i in range(dict_n):
                    # Dict-level null bitmap (proper Arrow nulls in the dictionary)
                    if dict_buf.null_bitmap != NULL:
                        byte = dict_buf.null_bitmap[i >> 3]
                        if ((byte >> (i & 7)) & 1) == 0:
                            dict_null[i] = 1
                            continue
                    # NaN-encoded null — float32/float64 only (IEEE 754: NaN != NaN)
                    if dict_type == DRAKEN_FLOAT32:
                        fval = (<float*>dict_buf.data)[i]
                        if fval != fval:
                            dict_null[i] = 1
                    elif dict_type == DRAKEN_FLOAT64:
                        dval = (<double*>dict_buf.data)[i]
                        if dval != dval:
                            dict_null[i] = 1

                for i in range(n):
                    # Row-level null bitmap (proper Arrow nulls on the indices)
                    if ptr.null_bitmap != NULL:
                        byte = ptr.null_bitmap[i >> 3]
                        if ((byte >> (i & 7)) & 1) == 0:
                            _set_true_bit(out_bits, i)
                            continue
                    # Dict-level null (NaN or bitmap)
                    code = _read_code(ptr, i)
                    if code < <uint32_t>dict_n and dict_null[code] != 0:
                        _set_true_bit(out_bits, i)
            finally:
                free(dict_null)
        else:
            # Empty dictionary — only row-level bitmap nulls possible
            if ptr.null_bitmap != NULL:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        _set_true_bit(out_bits, i)

        return out

    cpdef BoolVector less_than(self, object literal):
        return self._compare_numeric(literal, 0)

    cpdef BoolVector greater_than(self, object literal):
        return self._compare_numeric(literal, 1)

    cpdef BoolVector less_than_or_equals(self, object literal):
        return self._compare_numeric(literal, 2)

    cpdef BoolVector greater_than_or_equals(self, object literal):
        return self._compare_numeric(literal, 3)

    cpdef BoolVector equals(self, object literal):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n = 0
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef uint8_t byte
        cdef uint32_t code
        cdef object literal_bytes_obj = _coerce_literal_bytes(literal)
        cdef bytes literal_bytes
        cdef const char* lit_ptr
        cdef Py_ssize_t lit_len
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef uint8_t* dict_matches = NULL

        if dict_buf != NULL and not _is_string_dict_type(dict_buf.type):
            return self._equals_numeric(literal, False)

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if literal_bytes_obj is None or dict_buf == NULL or dict_buf.length == 0:
            return out

        literal_bytes = literal_bytes_obj
        lit_ptr = PyBytes_AS_STRING(literal_bytes)
        lit_len = len(literal_bytes)
        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                start = dict_buf.offsets[i]
                end = dict_buf.offsets[i + 1]
                if (end - start) != lit_len:
                    continue
                if memcmp(<const void*>(dict_buf.data + start), <const void*>lit_ptr, <size_t>lit_len) == 0:
                    dict_matches[i] = 1

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                code = _read_code(ptr, i)
                if code < dict_n and dict_matches[code] != 0:
                    _set_true_bit(out_bits, i)
        finally:
            free(dict_matches)

        return out

    cpdef BoolVector not_equals(self, object literal):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n = 0
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef uint8_t byte
        cdef uint32_t code
        cdef object literal_bytes_obj = _coerce_literal_bytes(literal)
        cdef bytes literal_bytes
        cdef const char* lit_ptr
        cdef Py_ssize_t lit_len
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef uint8_t* dict_matches = NULL

        if dict_buf != NULL and not _is_string_dict_type(dict_buf.type):
            return self._equals_numeric(literal, True)

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if ptr.length == 0:
            return out

        # SQL null semantics here are two-valued for filtering: null row -> False.
        if literal_bytes_obj is None or dict_buf == NULL or dict_buf.length == 0:
            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                _set_true_bit(out_bits, i)
            return out

        literal_bytes = literal_bytes_obj
        lit_ptr = PyBytes_AS_STRING(literal_bytes)
        lit_len = len(literal_bytes)
        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                start = dict_buf.offsets[i]
                end = dict_buf.offsets[i + 1]
                if (end - start) != lit_len:
                    continue
                if memcmp(<const void*>(dict_buf.data + start), <const void*>lit_ptr, <size_t>lit_len) == 0:
                    dict_matches[i] = 1

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                code = _read_code(ptr, i)
                if not (code < dict_n and dict_matches[code] != 0):
                    _set_true_bit(out_bits, i)
        finally:
            free(dict_matches)

        return out

    cpdef BoolVector in_list(self, object literals):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n = 0
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef uint8_t byte
        cdef uint32_t code
        cdef object literal
        cdef object literal_bytes_obj
        cdef list byte_literals = []
        cdef bint include_null = False
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef uint8_t* dict_matches = NULL
        cdef bytes lit_b
        cdef const char* lit_ptr
        cdef Py_ssize_t lit_len

        if dict_buf != NULL and not _is_string_dict_type(dict_buf.type):
            return self._in_list_numeric(literals)

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if literals is None:
            return out

        for literal in literals:
            if literal is None:
                include_null = True
                continue
            literal_bytes_obj = _coerce_literal_bytes(literal)
            if literal_bytes_obj is not None:
                byte_literals.append(literal_bytes_obj)

        if dict_buf == NULL or dict_buf.length == 0:
            if include_null and ptr.null_bitmap != NULL:
                for i in range(n):
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        _set_true_bit(out_bits, i)
            return out

        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        try:
            if len(byte_literals) > 0:
                for i in range(dict_n):
                    if dict_buf.null_bitmap != NULL:
                        byte = dict_buf.null_bitmap[i >> 3]
                        if ((byte >> (i & 7)) & 1) == 0:
                            continue
                    start = dict_buf.offsets[i]
                    end = dict_buf.offsets[i + 1]
                    for lit_b in byte_literals:
                        lit_ptr = PyBytes_AS_STRING(lit_b)
                        lit_len = len(lit_b)
                        if (end - start) != lit_len:
                            continue
                        if memcmp(<const void*>(dict_buf.data + start), <const void*>lit_ptr, <size_t>lit_len) == 0:
                            dict_matches[i] = 1
                            break

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        if include_null:
                            _set_true_bit(out_bits, i)
                        continue
                code = _read_code(ptr, i)
                if code < dict_n and dict_matches[code] != 0:
                    _set_true_bit(out_bits, i)
        finally:
            free(dict_matches)

        return out

    cpdef BoolVector like(self, object pattern, bint ignore_case=False):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n = 0
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef uint8_t byte
        cdef uint32_t code
        cdef object pattern_bytes_obj = _coerce_literal_bytes(pattern)
        cdef bytes pattern_bytes
        cdef const uint8_t* pat_ptr
        cdef Py_ssize_t pat_len
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef uint8_t* dict_matches = NULL

        if dict_buf != NULL and not _is_string_dict_type(dict_buf.type):
            raise TypeError("Dictionary LIKE kernels require string dictionary values")

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if pattern_bytes_obj is None or dict_buf == NULL or dict_buf.length == 0:
            return out

        pattern_bytes = pattern_bytes_obj
        pat_ptr = <const uint8_t*>PyBytes_AS_STRING(pattern_bytes)
        pat_len = len(pattern_bytes)
        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                start = dict_buf.offsets[i]
                end = dict_buf.offsets[i + 1]
                if _sql_like_match(
                    dict_buf.data + start,
                    end - start,
                    pat_ptr,
                    pat_len,
                    ignore_case,
                ):
                    dict_matches[i] = 1

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                code = _read_code(ptr, i)
                if code < dict_n and dict_matches[code] != 0:
                    _set_true_bit(out_bits, i)
        finally:
            free(dict_matches)

        return out

    cpdef BoolVector rlike(self, object pattern):
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n = 0
        cdef Py_ssize_t i
        cdef int32_t start, end
        cdef uint8_t byte
        cdef uint32_t code
        cdef object pattern_bytes_obj = _coerce_literal_bytes(pattern)
        cdef object regex = None
        cdef object match_obj
        cdef object value_bytes
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef uint8_t* dict_matches = NULL

        if dict_buf != NULL and not _is_string_dict_type(dict_buf.type):
            raise TypeError("Dictionary RLIKE kernels require string dictionary values")

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if pattern_bytes_obj is None or dict_buf == NULL or dict_buf.length == 0:
            return out

        import re

        try:
            regex = re.compile(pattern_bytes_obj)
        except Exception as err:
            raise ValueError("Invalid regular expression") from err

        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                start = dict_buf.offsets[i]
                end = dict_buf.offsets[i + 1]
                value_bytes = PyBytes_FromStringAndSize(<char*>(dict_buf.data + start), end - start)
                match_obj = regex.search(value_bytes)
                if match_obj is not None:
                    dict_matches[i] = 1

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                code = _read_code(ptr, i)
                if code < dict_n and dict_matches[code] != 0:
                    _set_true_bit(out_bits, i)
        finally:
            if dict_matches != NULL:
                free(dict_matches)

        return out

    cpdef BoolVector contains(self, object substr, bint ignore_case=False):
        """Return mask: 1 if element contains substr, else 0. Propagates NULLs."""
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t nbytes = (n + 7) >> 3
        cdef Py_ssize_t dict_n = 0
        cdef Py_ssize_t i, j, p, q
        cdef int32_t start, end
        cdef Py_ssize_t str_len
        cdef uint8_t byte
        cdef uint32_t code
        cdef bint found
        cdef object substr_bytes_obj = _coerce_literal_bytes(substr)
        cdef bytes substr_bytes
        cdef const uint8_t* ndl_ptr
        cdef Py_ssize_t ndl_len
        cdef uint8_t* ndl_lower = NULL
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
        cdef uint8_t* dict_matches = NULL

        if dict_buf != NULL and not _is_string_dict_type(dict_buf.type):
            raise TypeError("Dictionary CONTAINS kernels require string dictionary values")

        if nbytes > 0:
            memset(out_bits, 0, nbytes)
        out.ptr.null_bitmap = NULL

        if substr_bytes_obj is None or dict_buf == NULL or dict_buf.length == 0:
            return out

        substr_bytes = substr_bytes_obj
        ndl_ptr = <const uint8_t*>PyBytes_AS_STRING(substr_bytes)
        ndl_len = len(substr_bytes)

        dict_n = dict_buf.length
        dict_matches = <uint8_t*>malloc(dict_n)
        if dict_matches == NULL:
            raise MemoryError()
        memset(dict_matches, 0, dict_n)

        if ignore_case and ndl_len > 0:
            ndl_lower = <uint8_t*>malloc(<size_t>ndl_len)
            if ndl_lower == NULL:
                free(dict_matches)
                raise MemoryError()
            for j in range(ndl_len):
                ndl_lower[j] = _ascii_lower(ndl_ptr[j])

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                start = dict_buf.offsets[i]
                end = dict_buf.offsets[i + 1]
                str_len = end - start
                if ndl_len == 0:
                    dict_matches[i] = 1
                    continue
                if ndl_len > str_len:
                    continue
                found = False
                for p in range(str_len - ndl_len + 1):
                    if ignore_case:
                        q = 0
                        while q < ndl_len and _ascii_lower(dict_buf.data[start + p + q]) == ndl_lower[q]:
                            q += 1
                        if q == ndl_len:
                            found = True
                            break
                    else:
                        if dict_buf.data[start + p] == ndl_ptr[0]:
                            q = 1
                            while q < ndl_len and dict_buf.data[start + p + q] == ndl_ptr[q]:
                                q += 1
                            if q == ndl_len:
                                found = True
                                break
                if found:
                    dict_matches[i] = 1

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        continue
                code = _read_code(ptr, i)
                if code < dict_n and dict_matches[code] != 0:
                    _set_true_bit(out_bits, i)
        finally:
            free(dict_matches)
            if ndl_lower != NULL:
                free(ndl_lower)

        return out

    cpdef DictionaryVector take(self, int32_t[::1] indices):
        cdef DrakenDictionaryBuffer* src = self.ptr
        cdef DrakenVarBuffer* src_dict = src.dictionary_values
        cdef Py_ssize_t n = indices.shape[0]
        cdef Py_ssize_t i
        cdef int32_t src_idx
        cdef size_t code_bytes = 0
        cdef uint8_t* out_null = NULL
        cdef Py_ssize_t out_nb_size
        cdef DictionaryVector out = DictionaryVector(wrap=True)
        cdef DrakenDictionaryBuffer* dst = out.ptr

        out.ptr = <DrakenDictionaryBuffer*>malloc(sizeof(DrakenDictionaryBuffer))
        if out.ptr == NULL:
            raise MemoryError()
        memset(out.ptr, 0, sizeof(DrakenDictionaryBuffer))
        out.owns_data = True
        out.owns_dictionary_values = False
        out._dict_owner_ref = self
        dst = out.ptr

        dst.type = DRAKEN_DICTIONARY
        dst.length = <size_t>n
        dst.code_width = src.code_width
        dst.ordered = src.ordered
        dst.dictionary_values = src_dict

        code_bytes = <size_t>n * <size_t>src.code_width
        if code_bytes > 0:
            dst.codes = <uint8_t*>malloc(code_bytes)
            if dst.codes == NULL:
                raise MemoryError()
            memset(dst.codes, 0, code_bytes)
        else:
            dst.codes = NULL

        if n > 0:
            out_nb_size = (n + 7) >> 3
            out_null = <uint8_t*>malloc(out_nb_size)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, out_nb_size)
            dst.null_bitmap = out_null

        for i in range(n):
            src_idx = indices[i]
            if src_idx < 0 or src_idx >= <int32_t>src.length:
                raise IndexError("take index out of bounds")
            if src.null_bitmap != NULL and ((src.null_bitmap[src_idx >> 3] >> (src_idx & 7)) & 1) == 0:
                _write_code(dst, i, 0)
                continue
            _write_code(dst, i, _read_code(src, src_idx))
            if out_null != NULL:
                out_null[i >> 3] |= (1 << (i & 7))

        return out

    def to_arrow(self):
        import pyarrow as pa

        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t dict_n = 0
        cdef Py_ssize_t dict_bytes = 0
        cdef object dict_null = None
        cdef object idx_null = None
        cdef int dict_type = DRAKEN_STRING
        cdef int32_t itemsize = 0

        if dict_buf != NULL:
            dict_n = dict_buf.length
            dict_type = dict_buf.type
            if dict_n > 0:
                if _is_string_dict_type(dict_type):
                    dict_bytes = dict_buf.offsets[dict_n]
                else:
                    itemsize = _dict_itemsize_for_type(dict_type)
                    if itemsize <= 0:
                        raise TypeError(f"Unsupported dictionary child type {dict_type}")
                    dict_bytes = dict_n * itemsize

        if dict_buf == NULL or dict_n == 0:
            if dict_type == DRAKEN_INT8:
                dictionary_values = pa.array([], type=pa.int8())
            elif dict_type == DRAKEN_INT16:
                dictionary_values = pa.array([], type=pa.int16())
            elif dict_type == DRAKEN_INT32:
                dictionary_values = pa.array([], type=pa.int32())
            elif dict_type == DRAKEN_INT64:
                dictionary_values = pa.array([], type=pa.int64())
            elif dict_type == DRAKEN_FLOAT32:
                dictionary_values = pa.array([], type=pa.float32())
            elif dict_type == DRAKEN_FLOAT64:
                dictionary_values = pa.array([], type=pa.float64())
            elif dict_type == DRAKEN_BOOL:
                dictionary_values = pa.array([], type=pa.bool_())
            else:
                dictionary_values = pa.array([], type=pa.binary())
        elif _is_string_dict_type(dict_type):
            offs_buf = pa.foreign_buffer(<intptr_t>dict_buf.offsets, (dict_n + 1) * sizeof(int32_t), base=self)
            if dict_bytes <= 0 or dict_buf.data == NULL:
                data_buf = pa.py_buffer(b"")
            else:
                data_buf = pa.foreign_buffer(<intptr_t>dict_buf.data, dict_bytes, base=self)
            if dict_buf.null_bitmap != NULL:
                dict_null = pa.foreign_buffer(<intptr_t>dict_buf.null_bitmap, (dict_n + 7) // 8, base=self)
            dictionary_values = pa.Array.from_buffers(pa.binary(), dict_n, [dict_null, offs_buf, data_buf])
        else:
            if dict_type == DRAKEN_INT8:
                value_type = pa.int8()
            elif dict_type == DRAKEN_INT16:
                value_type = pa.int16()
            elif dict_type == DRAKEN_INT32:
                value_type = pa.int32()
            elif dict_type == DRAKEN_INT64:
                value_type = pa.int64()
            elif dict_type == DRAKEN_FLOAT32:
                value_type = pa.float32()
            elif dict_type == DRAKEN_FLOAT64:
                value_type = pa.float64()
            elif dict_type == DRAKEN_BOOL:
                value_type = pa.bool_()
            else:
                raise TypeError(f"Unsupported dictionary child type {dict_type}")

            if dict_buf.null_bitmap != NULL:
                dict_null = pa.foreign_buffer(<intptr_t>dict_buf.null_bitmap, (dict_n + 7) // 8, base=self)
            if dict_type == DRAKEN_BOOL:
                # bool payload is bit-packed in Arrow; dictionary bool is not currently supported here.
                raise TypeError("Dictionary bool export is not supported")
            if dict_bytes <= 0 or dict_buf.data == NULL:
                data_buf = pa.py_buffer(b"")
            else:
                data_buf = pa.foreign_buffer(<intptr_t>dict_buf.data, dict_bytes, base=self)
            dictionary_values = pa.Array.from_buffers(value_type, dict_n, [dict_null, data_buf])

        if ptr.code_width == 1:
            index_type = pa.uint8()
        elif ptr.code_width == 2:
            index_type = pa.uint16()
        else:
            index_type = pa.uint32()

        if ptr.null_bitmap != NULL:
            idx_null = pa.foreign_buffer(<intptr_t>ptr.null_bitmap, (n + 7) // 8, base=self)
        if n == 0 or ptr.codes == NULL:
            idx_data = pa.py_buffer(b"")
        else:
            idx_data = pa.foreign_buffer(<intptr_t>ptr.codes, n * ptr.code_width, base=self)

        indices = pa.Array.from_buffers(index_type, n, [idx_null, idx_data])
        return pa.DictionaryArray.from_arrays(indices, dictionary_values, ordered=bool(ptr.ordered))

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t dict_n
        cdef Py_ssize_t i
        cdef uint8_t byte
        cdef uint32_t code
        cdef int32_t start, end
        cdef uint64_t* dict_hashes = NULL
        cdef uint64_t value_hash
        cdef int dict_type
        cdef int32_t itemsize

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("DictionaryVector.hash_into: output buffer too small")
        if dict_buf == NULL:
            raise ValueError("DictionaryVector has no dictionary values buffer")

        dict_n = dict_buf.length
        dict_type = dict_buf.type
        itemsize = _dict_itemsize_for_type(dict_type)
        dict_hashes = <uint64_t*>malloc(<size_t>dict_n * sizeof(uint64_t)) if dict_n > 0 else NULL
        if dict_n > 0 and dict_hashes == NULL:
            raise MemoryError()

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        dict_hashes[i] = NULL_HASH
                        continue
                if _is_string_dict_type(dict_type):
                    start = dict_buf.offsets[i]
                    end = dict_buf.offsets[i + 1]
                    dict_hashes[i] = XXH3_64bits(<const void*>(dict_buf.data + start), <size_t>(end - start))
                elif dict_type == DRAKEN_INT64:
                    dict_hashes[i] = <uint64_t>(<int64_t*>dict_buf.data)[i]
                elif dict_type == DRAKEN_FLOAT64:
                    dict_hashes[i] = (<uint64_t*>dict_buf.data)[i]
                elif itemsize > 0:
                    dict_hashes[i] = XXH3_64bits(
                        <const void*>(dict_buf.data + (i * itemsize)),
                        <size_t>itemsize,
                    )
                else:
                    raise TypeError("Unsupported dictionary value type for hashing")

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        out_buf[offset + i] = mix_hash(out_buf[offset + i], NULL_HASH)
                        continue
                code = _read_code(ptr, i)
                if code >= dict_n:
                    raise ValueError("Dictionary code out of bounds during hashing")
                value_hash = dict_hashes[code]
                out_buf[offset + i] = mix_hash(out_buf[offset + i], value_hash)
        finally:
            if dict_hashes != NULL:
                free(dict_hashes)

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        cdef DrakenDictionaryBuffer* ptr = self.ptr
        cdef DrakenVarBuffer* dict_buf = ptr.dictionary_values
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t dict_n
        cdef Py_ssize_t i
        cdef uint8_t byte
        cdef uint32_t code
        cdef int32_t start, end
        cdef int64_t* dict_compressed = NULL
        cdef int64_t null_value = to_int(None)
        cdef object dict_item
        cdef int dict_type

        if n == 0:
            return
        if offset < 0 or offset + n > out_buf.shape[0]:
            raise ValueError("DictionaryVector.compress_into: output buffer too small")
        if dict_buf == NULL:
            raise ValueError("DictionaryVector has no dictionary values buffer")

        dict_n = dict_buf.length
        dict_type = dict_buf.type
        dict_compressed = <int64_t*>malloc(<size_t>dict_n * sizeof(int64_t)) if dict_n > 0 else NULL
        if dict_n > 0 and dict_compressed == NULL:
            raise MemoryError()

        try:
            for i in range(dict_n):
                if dict_buf.null_bitmap != NULL:
                    byte = dict_buf.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        dict_compressed[i] = null_value
                        continue
                if _is_string_dict_type(dict_type):
                    start = dict_buf.offsets[i]
                    end = dict_buf.offsets[i + 1]
                    dict_item = PyBytes_FromStringAndSize(<char*>dict_buf.data + start, end - start)
                    dict_compressed[i] = to_int(dict_item)
                elif dict_type == DRAKEN_INT8:
                    dict_compressed[i] = <int64_t>(<int8_t*>dict_buf.data)[i]
                elif dict_type == DRAKEN_INT16:
                    dict_compressed[i] = <int64_t>(<int16_t*>dict_buf.data)[i]
                elif dict_type == DRAKEN_INT32:
                    dict_compressed[i] = <int64_t>(<int32_t*>dict_buf.data)[i]
                elif dict_type == DRAKEN_INT64:
                    dict_compressed[i] = <int64_t>(<int64_t*>dict_buf.data)[i]
                elif dict_type == DRAKEN_FLOAT32:
                    dict_compressed[i] = to_int((<float*>dict_buf.data)[i])
                elif dict_type == DRAKEN_FLOAT64:
                    dict_compressed[i] = to_int((<double*>dict_buf.data)[i])
                elif dict_type == DRAKEN_BOOL:
                    dict_compressed[i] = 1 if ((<uint8_t*>dict_buf.data)[i] != 0) else 0
                else:
                    raise TypeError("Unsupported dictionary value type for compression")

            for i in range(n):
                if ptr.null_bitmap != NULL:
                    byte = ptr.null_bitmap[i >> 3]
                    if ((byte >> (i & 7)) & 1) == 0:
                        out_buf[offset + i] = null_value
                        continue
                code = _read_code(ptr, i)
                if code >= dict_n:
                    raise ValueError("Dictionary code out of bounds during compression")
                out_buf[offset + i] = dict_compressed[code]
        finally:
            if dict_compressed != NULL:
                free(dict_compressed)


cdef DictionaryVector from_arrow(object array):
    import pyarrow as pa

    if not pa.types.is_dictionary(array.type):
        raise TypeError("from_arrow expects a pyarrow DictionaryArray")

    cdef object indices = array.indices
    cdef object dict_values = array.dictionary
    cdef object idx_type = indices.type
    cdef object value_type = array.type.value_type
    cdef int dict_value_type = DRAKEN_STRING
    cdef int32_t dict_itemsize = 0

    # Keep unsupported index widths on the generic ArrowVector fallback path.
    if not (
        idx_type.equals(pa.int8())
        or idx_type.equals(pa.int16())
        or idx_type.equals(pa.int32())
        or idx_type.equals(pa.uint8())
        or idx_type.equals(pa.uint16())
        or idx_type.equals(pa.uint32())
    ):
        raise TypeError(f"Unsupported dictionary index type: {idx_type}")

    if pa.types.is_string(value_type) or pa.types.is_binary(value_type):
        dict_value_type = DRAKEN_STRING
    elif value_type.equals(pa.int8()):
        dict_value_type = DRAKEN_INT8
        dict_itemsize = 1
    elif value_type.equals(pa.int16()):
        dict_value_type = DRAKEN_INT16
        dict_itemsize = 2
    elif value_type.equals(pa.int32()):
        dict_value_type = DRAKEN_INT32
        dict_itemsize = 4
    elif value_type.equals(pa.int64()):
        dict_value_type = DRAKEN_INT64
        dict_itemsize = 8
    elif value_type.equals(pa.float32()):
        dict_value_type = DRAKEN_FLOAT32
        dict_itemsize = 4
    elif value_type.equals(pa.float64()):
        dict_value_type = DRAKEN_FLOAT64
        dict_itemsize = 8
    else:
        raise TypeError(f"Unsupported dictionary value type: {value_type}")

    cdef Py_ssize_t n = len(array)
    cdef Py_ssize_t dict_n = len(dict_values)
    cdef uint8_t code_width = _code_width_for_dict_size(dict_n)
    cdef Py_ssize_t i
    cdef Py_ssize_t idx_offset = indices.offset
    cdef Py_ssize_t dict_offset = dict_values.offset
    cdef Py_ssize_t dict_nb_size
    cdef Py_ssize_t row_nb_size
    cdef int32_t* src_offsets = NULL
    cdef int32_t* dst_offsets
    cdef int32_t dict_data_start = 0
    cdef int32_t dict_data_bytes = 0
    cdef int32_t code
    cdef uint8_t* src_bitmap
    cdef object dict_bufs
    cdef object idx_bufs
    cdef uint8_t* row_null_bitmap = NULL
    cdef intptr_t dict_offs_addr = 0
    cdef intptr_t dict_data_addr = 0
    cdef intptr_t dict_null_addr = 0
    cdef intptr_t idx_data_addr = 0
    cdef intptr_t idx_null_addr = 0

    dict_bufs = dict_values.buffers()
    idx_bufs = indices.buffers()

    if dict_n > 0:
        if dict_value_type == DRAKEN_STRING:
            dict_offs_addr = <intptr_t>dict_bufs[1].address
            src_offsets = <int32_t*>dict_offs_addr
            dict_data_start = src_offsets[dict_offset]
            dict_data_bytes = src_offsets[dict_offset + dict_n] - dict_data_start
            if dict_data_bytes < 0:
                raise ValueError("Invalid dictionary offsets")
        else:
            dict_data_start = <int32_t>(dict_offset * dict_itemsize)
            dict_data_bytes = <int32_t>(dict_n * dict_itemsize)
    else:
        dict_data_bytes = 0

    cdef DictionaryVector vec = DictionaryVector(
        <size_t>n,
        <size_t>dict_n,
        <size_t>dict_data_bytes,
        code_width,
        bool(array.type.ordered),
        dict_value_type,
    )
    cdef DrakenDictionaryBuffer* ptr = vec.ptr
    cdef DrakenVarBuffer* dict_ptr = ptr.dictionary_values

    if dict_n > 0:
        dst_offsets = dict_ptr.offsets
        if dict_value_type == DRAKEN_STRING:
            src_offsets = (<int32_t*>dict_offs_addr) + dict_offset
            for i in range(dict_n + 1):
                dst_offsets[i] = src_offsets[i] - src_offsets[0]

            if dict_data_bytes > 0:
                dict_data_addr = <intptr_t>dict_bufs[2].address
                memcpy(
                    dict_ptr.data,
                    (<uint8_t*>dict_data_addr) + dict_data_start,
                    <size_t>dict_data_bytes,
                )
        else:
            for i in range(dict_n + 1):
                dst_offsets[i] = <int32_t>(i * dict_itemsize)
            if dict_data_bytes > 0:
                dict_data_addr = <intptr_t>dict_bufs[1].address
                memcpy(
                    dict_ptr.data,
                    (<uint8_t*>dict_data_addr) + dict_data_start,
                    <size_t>dict_data_bytes,
                )

        if dict_bufs[0] is not None:
            dict_nb_size = (dict_n + 7) >> 3
            dict_ptr.null_bitmap = <uint8_t*>malloc(dict_nb_size)
            if dict_ptr.null_bitmap == NULL:
                raise MemoryError()
            dict_null_addr = <intptr_t>dict_bufs[0].address
            src_bitmap = <uint8_t*>dict_null_addr
            _copy_bitmap_shifted(src_bitmap, dict_ptr.null_bitmap, dict_offset, dict_n)

    if indices.null_count > 0 and n > 0:
        row_nb_size = (n + 7) >> 3
        row_null_bitmap = <uint8_t*>malloc(row_nb_size)
        if row_null_bitmap == NULL:
            raise MemoryError()
        memset(row_null_bitmap, 0, row_nb_size)
        ptr.null_bitmap = row_null_bitmap

    cdef int8_t* p_i8 = NULL
    cdef int16_t* p_i16 = NULL
    cdef int32_t* p_i32 = NULL
    cdef uint8_t* p_u8 = NULL
    cdef uint16_t* p_u16 = NULL
    cdef uint32_t* p_u32 = NULL
    cdef uint8_t* src_row_bitmap = NULL

    if idx_bufs[1] is not None and n > 0:
        idx_data_addr = <intptr_t>idx_bufs[1].address
        if idx_type.equals(pa.int8()):
            p_i8 = <int8_t*>idx_data_addr
        elif idx_type.equals(pa.int16()):
            p_i16 = <int16_t*>idx_data_addr
        elif idx_type.equals(pa.int32()):
            p_i32 = <int32_t*>idx_data_addr
        elif idx_type.equals(pa.uint8()):
            p_u8 = <uint8_t*>idx_data_addr
        elif idx_type.equals(pa.uint16()):
            p_u16 = <uint16_t*>idx_data_addr
        else:
            p_u32 = <uint32_t*>idx_data_addr

    if idx_bufs[0] is not None:
        idx_null_addr = <intptr_t>idx_bufs[0].address
        src_row_bitmap = <uint8_t*>idx_null_addr

    for i in range(n):
        if src_row_bitmap != NULL and ((src_row_bitmap[(idx_offset + i) >> 3] >> ((idx_offset + i) & 7)) & 1) == 0:
            _write_code(ptr, i, 0)
            continue

        if p_i8 != NULL:
            code = p_i8[idx_offset + i]
        elif p_i16 != NULL:
            code = p_i16[idx_offset + i]
        elif p_i32 != NULL:
            code = p_i32[idx_offset + i]
        elif p_u8 != NULL:
            code = p_u8[idx_offset + i]
        elif p_u16 != NULL:
            code = p_u16[idx_offset + i]
        else:
            code = <int32_t>p_u32[idx_offset + i]

        if code < 0 or code >= dict_n:
            raise ValueError(f"Dictionary index out of bounds at row {i}: {code} (dict_size={dict_n})")

        _write_code(ptr, i, <uint32_t>code)
        if row_null_bitmap != NULL:
            row_null_bitmap[i >> 3] |= (1 << (i & 7))

    return vec
