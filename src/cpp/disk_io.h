#ifndef DISK_READER_H
#define DISK_READER_H

#include <cstddef>
#include <cstdint>

/**
 * Fast disk reader with platform-specific I/O optimizations
 * 
 * @param path File path to read
 * @param dst Destination buffer (must be pre-allocated)
 * @param out_len Output parameter for bytes read
 * @param sequential Hint for sequential access pattern
 * @param willneed Hint that data will be needed soon (prefetch)
 * @param drop_after Drop page cache after reading
 * @return 0 on success, negative errno on failure
 */
int read_all_pread(const char* path, uint8_t* dst, size_t* out_len,
                   bool sequential, bool willneed, bool drop_after);

/**
 * Read a slice/range of a file using pread
 * 
 * @param path File path to read from
 * @param offset Byte offset to start reading from
 * @param length Number of bytes to read
 * @param dst Destination buffer (must be pre-allocated to at least length bytes)
 * @param out_len Output parameter for bytes actually read
 * @param sequential Hint for sequential access pattern
 * @param willneed Hint that data will be needed soon (prefetch)
 * @param drop_after Drop page cache after reading
 * @return 0 on success, negative errno on failure
 */
int read_slice_pread(const char* path, size_t offset, size_t length, uint8_t* dst, size_t* out_len,
                     bool sequential, bool willneed, bool drop_after);

int read_all_mmap(const char* path, uint8_t** dst, size_t* out_len);

/**
 * Read a slice/range of a file using mmap
 * 
 * @param path File path to read from
 * @param offset Byte offset to start mapping from
 * @param length Number of bytes to map
 * @param dst Output pointer to mapped memory
 * @param out_len Output parameter for bytes actually mapped
 * @return 0 on success, negative errno on failure
 */
int read_slice_mmap(const char* path, size_t offset, size_t length, uint8_t** dst, size_t* out_len);

int unmap_memory_c(unsigned char* addr, size_t size);

/**
 * Per-range descriptor for read_ranges_pread.
 * Caller fills offset, length, dst (pre-allocated to >= length bytes).
 * Callee fills out_len (bytes actually read; may be < length on EOF) and rc
 * (0 on success, negative errno on failure, -ECANCELED if not run due to
 * fail-fast on a sibling range).
 */
struct read_range_t {
    size_t   offset;
    size_t   length;
    uint8_t* dst;
    size_t   out_len;
    int      rc;
};

/**
 * Read N ranges from one file with a single open/close.
 *
 * Opinionated for Parquet column-fetch: caller is expected to have already
 * coalesced adjacent ranges. Workers share one fd via pread(); cap is
 * min(count, 8). For count == 1 the caller's thread does the pread inline
 * (no thread spawn).
 *
 * Fail-fast: if any pread fails, in-flight workers drain and remaining
 * ranges are marked rc == -ECANCELED. The function's return value is the
 * first negative errno seen (or 0 on full success).
 *
 * NOT thread-safe with respect to the same `ranges` array being passed to
 * concurrent calls. Independent calls on independent arrays are fine.
 *
 * @param path File path
 * @param ranges Array of read_range_t (length `count`); modified in place
 * @param count Number of ranges
 * @return 0 on success, otherwise the first negative errno encountered
 */
int read_ranges_pread(const char* path, read_range_t* ranges, size_t count);
#endif // DISK_READER_H