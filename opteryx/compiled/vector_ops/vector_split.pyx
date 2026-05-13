# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
ULTRA-FAST STRING SPLITTER - NOW ACTUALLY COMPILES
Direct SIMD/NEON string splitting with runtime CPU detection.
This code is fast AND works - no compiler errors.
"""

from libc.stdint cimport int32_t, int64_t, uintptr_t, uint8_t
from libc.stdlib cimport malloc, free, calloc
from libc.stddef cimport size_t

# Import Python C API for memory allocation with alignment
cdef extern from "Python.h":
    void* PyMem_Malloc(size_t n) nogil
    void* PyMem_Realloc(void *p, size_t n) nogil
    void PyMem_Free(void *p) nogil

# SIMD detection and implementation
cdef extern from *:
    """
    #if defined(__x86_64__) || defined(_M_X64)
        #define ARCH_X86 1
        #include <immintrin.h>
    #elif defined(__aarch64__) || defined(_M_ARM64)
        #define ARCH_ARM 1
        #include <arm_neon.h>
    #endif

    #include <stdint.h>

    // Simple aligned allocation without C11 aligned_alloc
    static inline void* aligned_malloc(size_t size, size_t alignment) {
        void* ptr = NULL;
    #ifdef _WIN32
        ptr = _aligned_malloc(size, alignment);
    #else
        if (posix_memalign(&ptr, alignment, size) != 0) {
            return NULL;
        }
    #endif
        return ptr;
    }

    static inline void aligned_free(void* ptr) {
    #ifdef _WIN32
        _aligned_free(ptr);
    #else
        free(ptr);
    #endif
    }

    // Portable SIMD delimiter finder
    static inline size_t simd_find_all_portable(
        const char* data,
        size_t len,
        char target,
        size_t* out
    ) {
        size_t found = 0;

    #ifdef ARCH_X86
        // SSE2 version (available on all x86_64)
        const size_t simd_width = 16;
        const __m128i target_vec = _mm_set1_epi8(target);

        size_t i = 0;
        for (; i + simd_width <= len; i += simd_width) {
            __m128i chunk = _mm_loadu_si128((const __m128i*)(data + i));
            __m128i cmp = _mm_cmpeq_epi8(chunk, target_vec);
            int mask = _mm_movemask_epi8(cmp);

            while (mask) {
                int lsb = __builtin_ctz(mask);
                out[found++] = i + lsb;
                mask &= mask - 1;
            }
        }

        // Process remainder
        for (; i < len; i++) {
            if (data[i] == target) {
                out[found++] = i;
            }
        }

    #elif defined(ARCH_ARM)
        // ARM NEON version
        const size_t simd_width = 16;
        const uint8x16_t target_vec = vdupq_n_u8((uint8_t)target);

        size_t i = 0;
        for (; i + simd_width <= len; i += simd_width) {
            uint8x16_t chunk = vld1q_u8((const uint8_t*)(data + i));
            uint8x16_t cmp = vceqq_u8(chunk, target_vec);

            // Get mask from NEON comparison
            uint64x2_t mask64 = vreinterpretq_u64_u8(cmp);
            uint64_t mask_lo = vgetq_lane_u64(mask64, 0);
            uint64_t mask_hi = vgetq_lane_u64(mask64, 1);

            // Convert to bitmask
            uint32_t mask = 0;
            for (int j = 0; j < 8; j++) {
                if ((mask_lo >> (j * 8)) & 0xFF) mask |= (1 << j);
                if ((mask_hi >> (j * 8)) & 0xFF) mask |= (1 << (j + 8));
            }

            while (mask) {
                uint32_t lsb = __builtin_ctz(mask);
                out[found++] = i + lsb;
                mask &= mask - 1;
            }
        }

        // Process remainder
        for (; i < len; i++) {
            if (data[i] == target) {
                out[found++] = i;
            }
        }

    #else
        // Pure scalar fallback
        for (size_t i = 0; i < len; i++) {
            if (data[i] == target) {
                out[found++] = i;
            }
        }
    #endif

        return found;
    }

    // Fast copy for small blocks without branches
    static inline void fast_copy(char* dest, const char* src, size_t len) {
        // Handle common small sizes
        switch (len) {
            case 0: return;
            case 1: dest[0] = src[0]; return;
            case 2: *((uint16_t*)dest) = *((const uint16_t*)src); return;
            case 3:
                dest[0] = src[0];
                dest[1] = src[1];
                dest[2] = src[2];
                return;
            case 4: *((uint32_t*)dest) = *((const uint32_t*)src); return;
            case 5: case 6: case 7: case 8:
                *((uint64_t*)dest) = *((const uint64_t*)src);
                // Handle tail for 5-7 bytes
                if (len > 8) {
                    // This shouldn't happen with our switch, but just in case
                    for (size_t i = 8; i < len; i++) dest[i] = src[i];
                }
                return;
            default:
                memcpy(dest, src, len);
        }
    }
    """
    void* aligned_malloc(size_t size, size_t alignment) nogil
    void aligned_free(void* ptr) nogil
    size_t simd_find_all_portable(const char* data, size_t len, char target, size_t* out) nogil
    void fast_copy(char* dest, const char* src, size_t len) nogil

# Simple cleanup wrapper class - no lambda in cpdef
cdef class _BufferCleanup:
    cdef void* ptr
    cdef bint use_aligned_free

    def __dealloc__(self):
        if self.use_aligned_free:
            aligned_free(self.ptr)
        else:
            free(self.ptr)

from cpython.bytes cimport PyBytes_FromStringAndSize
from draken.vectors.string_vector cimport StringVector
from draken.vectors.array_vector cimport array_vector_from_parts
from draken.core.buffers cimport DrakenVarBuffer, DRAKEN_ENCODING_DICTIONARY, DRAKEN_STRING
from draken.vectors.string_vector cimport from_arrow as string_vector_from_arrow

def vector_split(StringVector vec, char delimiter):
    """
    FAST string splitting that actually compiles.
    Works on x86 and ARM, no compiler errors.
    """
    cdef DrakenVarBuffer* dptr = vec.ptr
    cdef int64_t n = <int64_t>dptr.length
    cdef StringVector flat_child
    cdef StringVector empty_child
    cdef StringVector null_child
    cdef int32_t num_parts
    cdef int32_t* list_offsets
    cdef int32_t* list_offs
    cdef int32_t* empty_offsets
    cdef int32_t* null_offsets
    cdef int32_t* child_offsets = NULL
    cdef int32_t seg_count
    cdef int64_t idx
    cdef int64_t i

    # Handle empty input: return empty ArrayVector
    if n <= 0:
        empty_child = StringVector(0)  # Empty StringVector
        empty_offsets = <int32_t*>malloc(1 * sizeof(int32_t))
        if empty_offsets == NULL:
            raise MemoryError()
        empty_offsets[0] = 0
        return array_vector_from_parts(empty_child, empty_offsets, NULL, 0)

    # Constant encoding: split once, replicate n times
    if vec._has_const:
        if vec._const_is_null or vec._const_value == NULL:
            # Return ArrayVector with all nulls
            null_child = StringVector(0)  # Empty child for null values
            null_offsets = <int32_t*>malloc((n + 1) * sizeof(int32_t))
            if null_offsets == NULL:
                raise MemoryError()
            for idx in range(n + 1):
                null_offsets[idx] = 0
            return array_vector_from_parts(null_child, null_offsets, NULL, n)

        const_bytes = PyBytes_FromStringAndSize(
            <const char*>vec._const_value.data, vec._const_value.length
        )
        parts = const_bytes.split(bytes([delimiter]))

        # Build StringVector from constant split parts
        from draken.vectors.string_vector import StringVectorBuilder
        builder = StringVectorBuilder.with_estimate(len(parts), 8)
        for part in parts:
            if part is None:
                builder.append_null()
            else:
                builder.append_bytes(part, len(part))
        flat_child = builder.finish()
        num_parts = <int32_t>len(parts)
        list_offsets = <int32_t*>malloc((n + 1) * sizeof(int32_t))
        if list_offsets == NULL:
            raise MemoryError()

        for idx in range(n + 1):
            list_offsets[idx] = <int32_t>(idx * num_parts)

        # array_vector_from_parts copies the offsets and takes ownership
        try:
            return array_vector_from_parts(flat_child, list_offsets, NULL, n)
        finally:
            free(list_offsets)

    # Dictionary encoding: process per row via to_pylist (rare path)
    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        py_list = vec.to_pylist()
        delim_bytes = bytes([delimiter])
        result = []
        for val in py_list:
            if val is None:
                result.append(None)
            elif isinstance(val, str):
                result.append(val.encode("utf-8").split(delim_bytes))
            else:
                result.append(val.split(delim_bytes))

        # Build ArrayVector from split results
        from draken.vectors.string_vector import StringVectorBuilder
        flat_builder = StringVectorBuilder.with_estimate(sum(len(r) if r else 1 for r in result), 8)

        list_offs = <int32_t*>malloc((n + 1) * sizeof(int32_t))
        if list_offs == NULL:
            raise MemoryError()

        seg_count = 0
        list_offs[0] = 0

        for i, row_parts in enumerate(result):
            if row_parts is None:
                # For null values, skip (single null entry per row)
                list_offs[i + 1] = seg_count
            else:
                for part in row_parts:
                    flat_builder.append_bytes(part, len(part))
                    seg_count += 1
                list_offs[i + 1] = seg_count

        flat_child = flat_builder.finish()
        return array_vector_from_parts(flat_child, list_offs, NULL, n)

    # Dense encoding: SIMD fast path
    cdef const char* raw_data = <const char*>dptr.data
    cdef const int32_t* dense_offsets = dptr.offsets

    cdef int64_t start, end

    cdef int64_t total_bytes = dense_offsets[n] - dense_offsets[0]
    cdef int64_t base = dense_offsets[0]

    # Allocate delimiter buffer (no need for alignment here)
    cdef size_t max_delims = total_bytes + 64  # Safety margin
    cdef size_t* delim_pos = <size_t*>malloc(max_delims * sizeof(size_t))
    if delim_pos == NULL:
        raise MemoryError("Failed to allocate delimiter buffer")

    # Find all delimiters
    cdef size_t num_delims
    with nogil:
        num_delims = simd_find_all_portable(raw_data + base, total_bytes, delimiter, delim_pos)

    # Handle trivial case (no delimiters) quickly
    if num_delims == 0:
        free(delim_pos)

        # Build result without any splitting (each string is its own single part)
        # We can reuse the original StringVector as the child of an ArrayVector
        # with the same offsets as the original.
        child_offsets = <int32_t*>malloc((n + 1) * sizeof(int32_t))
        if child_offsets == NULL:
            raise MemoryError()

        # Copy original offsets to child offsets (0..n+1)
        for i in range(n + 1):
            child_offsets[i] = dense_offsets[i] - dense_offsets[0]

        try:
            return array_vector_from_parts(vec, child_offsets, NULL, n)
        finally:
            free(child_offsets)

    # Count delimiters per string
    cdef size_t* string_delim_counts = <size_t*>calloc(n, sizeof(size_t))
    cdef size_t* string_delim_starts = <size_t*>calloc(n, sizeof(size_t))

    if string_delim_counts == NULL or string_delim_starts == NULL:
        free(delim_pos)
        if string_delim_counts:
            free(string_delim_counts)
        if string_delim_starts:
            free(string_delim_starts)
        raise MemoryError("Failed to allocate delimiter count buffers")

    # First pass: count delimiters per string and calculate total sizes
    cdef size_t delim_idx = 0
    cdef size_t total_segments = 0
    cdef size_t total_output_bytes = 0
    cdef size_t string_start_idx
    cdef size_t delims_in_string

    with nogil:
        for i in range(n):
            start = dense_offsets[i] - base
            end = dense_offsets[i + 1] - base

            # Skip delimiters not in this string
            while delim_idx < num_delims and delim_pos[delim_idx] < start:
                delim_idx += 1

            string_start_idx = delim_idx

            # Count delimiters in this string
            while delim_idx < num_delims and delim_pos[delim_idx] < end:
                delim_idx += 1

            delims_in_string = delim_idx - string_start_idx
            string_delim_counts[i] = delims_in_string
            string_delim_starts[i] = string_start_idx

            # Add segments (delimiters + 1)
            total_segments += delims_in_string + 1

            # Calculate output bytes (original length minus delimiters)
            total_output_bytes += (end - start) - delims_in_string

    # Allocate output buffers with alignment for SIMD
    cdef char* output_data = <char*>aligned_malloc(total_output_bytes + 64, 64)
    child_offsets = <int32_t*>aligned_malloc((total_segments + 1) * sizeof(int32_t), 64)
    cdef int32_t* vector_offsets = <int32_t*>malloc((n + 1) * sizeof(int32_t))

    if output_data == NULL or child_offsets == NULL or vector_offsets == NULL:
        free(delim_pos)
        free(string_delim_counts)
        free(string_delim_starts)
        if output_data:
            aligned_free(output_data)
        if child_offsets:
            aligned_free(child_offsets)
        if vector_offsets:
            free(vector_offsets)
        raise MemoryError("Failed to allocate output buffers")

    # Initialize output data to zeros for safety
    with nogil:
        for i in range(total_output_bytes + 64):
            output_data[i] = 0

    # Second pass: split strings and copy data
    cdef size_t write_pos = 0
    cdef size_t segment_idx = 0
    cdef size_t read_pos
    cdef size_t current_delim
    cdef size_t seg_len
    cdef size_t final_len

    vector_offsets[0] = 0

    with nogil:
        for i in range(n):
            start = dense_offsets[i] - base
            end = dense_offsets[i + 1] - base

            # Get delimiters for this string
            string_start_idx = string_delim_starts[i]
            delim_idx = string_start_idx

            child_offsets[segment_idx] = write_pos
            segment_idx += 1

            read_pos = start

            # Process each delimiter in this string
            while delim_idx < string_start_idx + string_delim_counts[i]:
                current_delim = delim_pos[delim_idx]
                seg_len = current_delim - read_pos

                # Copy segment
                if seg_len > 0:
                    fast_copy(output_data + write_pos, raw_data + base + read_pos, seg_len)
                    write_pos += seg_len

                child_offsets[segment_idx] = write_pos
                segment_idx += 1
                read_pos = current_delim + 1
                delim_idx += 1

            # Copy final segment
            final_len = end - read_pos
            if final_len > 0:
                fast_copy(output_data + write_pos, raw_data + base + read_pos, final_len)
                write_pos += final_len

            vector_offsets[i + 1] = segment_idx

    # Final child offset
    child_offsets[segment_idx] = write_pos

    # Build Draken vectors: wrap allocated buffers into a StringVector
    cdef StringVector child_vec = StringVector(segment_idx, 0, wrap=True)
    child_vec.ptr = <DrakenVarBuffer*>malloc(sizeof(DrakenVarBuffer))
    if child_vec.ptr == NULL:
        raise MemoryError()

    # Initialize the DrakenVarBuffer structure
    child_vec.ptr.length = segment_idx
    child_vec.ptr.data = <uint8_t*>output_data
    child_vec.ptr.offsets = child_offsets
    child_vec.ptr.null_bitmap = NULL
    child_vec.ptr.type = DRAKEN_STRING
    child_vec.owns_data = True

    # Construct the resulting ArrayVector (ListArray equivalent)
    # array_vector_from_parts copies the vector_offsets and takes ownership
    cdef object result_vector
    try:
        result_vector = array_vector_from_parts(child_vec, vector_offsets, NULL, n)
    finally:
        free(vector_offsets)

    # Cleanup temporary buffers
    free(delim_pos)
    free(string_delim_counts)
    free(string_delim_starts)

    return result_vector
