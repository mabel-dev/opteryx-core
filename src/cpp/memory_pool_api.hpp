#ifndef OPTERYX_MEMORY_POOL_API_HPP
#define OPTERYX_MEMORY_POOL_API_HPP

/*
 * memory_pool_api.hpp
 *
 * Lightweight C ABI for interacting with the Cython-owned MemoryPool from
 * native C++ code. The C++ Parquet IO pipeline expects a pointer to a
 * `MemoryPoolApi` structure which provides function pointers to perform the
 * minimal operations required by the zero-copy transport:
 *
 *  - reserve_for_write_ptr(ctx, size, out_ptr, out_capacity) -> ref_id
 *      Reserve a segment for direct write into the pool. On success returns a
 *      positive ref_id and sets *out_ptr (address) and *out_capacity (bytes).
 *      On failure returns -1 (no reservation) and leaves out_ptr/out_capacity
 *      unspecified.
 *
 *  - finalize_commit(ctx, ref_id, actual_length)
 *      Finalize a previously reserved segment by setting the actual length
 *      written and releasing the write-latch so readers can access it.
 *
 *  - commit_bytes(ctx, data, length) -> ref_id
 *      Commit a contiguous byte buffer into the pool (copy). Returns ref_id or
 *      -1 on failure.
 *
 *  - read_latched(ctx, ref_id, out_len) -> void*
 *      Return a pointer to the latched data and set *out_len to its length.
 *      Ownership remains with the MemoryPool; the caller MUST call `unlatch`
 *      when done reading and eventually `release` to free the segment.
 *
 *  - unlatch(ctx, ref_id)
 *      Decrement the latch count acquired by read_latched / reserve_for_write_ptr.
 *
 *  - release(ctx, ref_id)
 *      Release (free) the committed segment identified by ref_id. After this
 *      call the ref_id is no longer valid.
 *
 * Concurrency contract:
 *  - These functions are expected to be thread-safe; the underlying MemoryPool
 *    implementation provides its own internal locking/latching semantics.
 *  - The `ctx` pointer is an opaque handle owned by the caller (Cython). The
 *    C++ pipeline MUST NOT free or mutate ctx except by calling the provided
 *    callbacks.
 *
 * Error handling:
 *  - Functions that return int64_t use -1 to indicate failure where documented.
 *  - Callbacks MUST avoid throwing exceptions across the C ABI boundary. If
 *    the underlying implementation may throw, it must catch and translate into
 *    error return values or set a thread-local error indicator if necessary.
 *
 * Example usage (conceptual):
 *
 *   MemoryPoolApi api = { .ctx = py_mem_ctx,
 *                         .reserve_for_write_ptr = reserve_fn,
 *                         .finalize_commit = finalize_fn,
 *                         ... };
 *
 *   uintptr_t ptr = 0;
 *   int64_t cap = 0;
 *   int64_t ref = api.reserve_for_write_ptr(api.ctx, estimated_size, &ptr, &cap);
 *   if (ref != -1) {
 *       // write into (void*)ptr up to cap bytes
 *       api.finalize_commit(api.ctx, ref, actual_bytes_written);
 *   } else {
 *       // fallback to copying bytes via commit_bytes
 *   }
 */

#include <cstdint>   // int64_t, uintptr_t
#include <cstddef>   // size_t
#include <cstdbool>  // bool

#ifdef __cplusplus
extern "C" {
#endif

/* Function pointer types for MemoryPool operations. Use C-style function
 * pointers for a stable ABI between Cython and C++ code.
 */

/* Reserve a segment for direct write.
 * - ctx: opaque context
 * - size: requested reservation size in bytes
 * - out_ptr: pointer to uintptr_t to receive the address of writable memory
 * - out_capacity: pointer to int64_t to receive the reserved capacity (>= size)
 *
 * Returns:
 *  - ref_id (positive) on success
 *  - -1 on failure (no reservation available)
 */
typedef int64_t (*mp_reserve_for_write_ptr_fn)(
    void* ctx,
    int64_t size,
    uintptr_t* out_ptr,
    int64_t* out_capacity
);

/* Finalize a prior reservation:
 * - ctx: opaque context
 * - ref_id: reference id returned by reserve_for_write_ptr
 * - actual_length: number of bytes actually written by the producer
 *
 * Returns 0 on success, non-zero on error (optional; implementations can
 * choose to make this void and throw on severe errors).
 */
typedef int (*mp_finalize_commit_fn)(
    void* ctx,
    int64_t ref_id,
    int64_t actual_length
);

/* Commit a bytes buffer into the pool (copy).
 * - ctx: opaque context
 * - data: pointer to source data
 * - length: length in bytes
 *
 * Returns:
 *  - ref_id (positive) on success
 *  - -1 on failure
 */
typedef int64_t (*mp_commit_bytes_fn)(
    void* ctx,
    const void* data,
    int64_t length
);

/* Read a latched pointer to a committed ref. The returned pointer is owned by
 * the MemoryPool; callers MUST call unlatch(ref_id) when done reading.
 *
 * - ctx: opaque context
 * - ref_id: reference id to read
 * - out_len: pointer to int64_t to receive the data length
 *
 * Returns:
 *  - pointer to data (non-null) on success (may be a pointer to internal pool)
 *  - NULL on failure (invalid ref_id)
 *
 * Note: The returned pointer is valid only until unlatch/release is called.
 */
typedef void* (*mp_read_latched_fn)(
    void* ctx,
    int64_t ref_id,
    int64_t* out_len
);

/* Unlatch a previously latched ref_id (decrement read latch count).
 * - ctx: opaque context
 * - ref_id: reference id to unlatch
 *
 * Returns 0 on success, non-zero on failure (optional).
 */
typedef int (*mp_unlatch_fn)(
    void* ctx,
    int64_t ref_id
);

/* Release (free) a previously committed ref.
 * - ctx: opaque context
 * - ref_id: reference id to release
 *
 * Returns 0 on success, non-zero on failure (optional).
 */
typedef int (*mp_release_fn)(
    void* ctx,
    int64_t ref_id
);

/* MemoryPoolApi: the vtable handed to native code. C++ pipeline receives a
 * pointer to this structure and invokes callbacks using the provided ctx.
 *
 * The caller must ensure the MemoryPoolApi instance and the ctx remain valid
 * for the lifetime of the pipeline that uses them.
 */
typedef struct MemoryPoolApi {
    void* ctx;

    mp_reserve_for_write_ptr_fn reserve_for_write_ptr;
    mp_finalize_commit_fn finalize_commit;
    mp_commit_bytes_fn commit_bytes;
    mp_read_latched_fn read_latched;
    mp_unlatch_fn unlatch;
    mp_release_fn release;

    /* Optional: version or flags field for future evolution */
    int32_t version;
    int32_t reserved;
} MemoryPoolApi;

/* Small helpers for common sentinel values */
static inline int64_t mp_refid_failure() { return -1; }

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* OPTERYX_MEMORY_POOL_API_HPP */