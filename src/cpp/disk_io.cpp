/*
 * Ultra-fast disk reader with platform-specific optimizations
 */

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

#include "disk_io.h"

size_t get_optimal_chunk_size(size_t file_size) {
    // For very small files, read in one chunk
    if (file_size <= (2 << 20)) {  // 2MB
        return file_size;
    }
    // For medium files, use 16MB chunks
    else if (file_size <= (128 << 20)) {  // 128MB
        return 16 << 20;
    }
    // For large files, use larger chunks but limit to 64MB
    else {
        return 64 << 20;
    }
}

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

int read_all_pread(const char* path, uint8_t* dst, size_t* out_len,
                   bool sequential, bool willneed, bool drop_after) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t size = static_cast<size_t>(st.st_size);
    
    const size_t CHUNK = get_optimal_chunk_size(size);
    
    // For files smaller than chunk size, read in one go
    if (size <= CHUNK) {
        if (sequential) posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
        if (willneed) posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
        
        ssize_t n = read(fd, dst, size);  // Use read instead of pread for single read
        if (n < 0 || static_cast<size_t>(n) != size) {
            int e = (n < 0) ? -errno : -EIO;
            close(fd);
            return e;
        }
    } else {
        // For larger files, use pread with fewer, larger chunks
        if (sequential) posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
        if (willneed) posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
        
        size_t off = 0;
        while (off < size) {
            size_t to_read = std::min(CHUNK, size - off);
            ssize_t n = pread(fd, dst + off, to_read, static_cast<off_t>(off));
            if (n <= 0) { 
                int e = (n == 0) ? -EIO : -errno; 
                close(fd); 
                return e; 
            }
            off += static_cast<size_t>(n);
        }
    }

    if (drop_after) posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    close(fd);
    *out_len = size;
    return 0;
}

#elif defined(__APPLE__)
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

int read_all_pread(const char* path, uint8_t* dst, size_t* out_len,
                   bool sequential, bool willneed, bool drop_after) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t size = static_cast<size_t>(st.st_size);
    
    const size_t CHUNK = get_optimal_chunk_size(size);
    
    if (sequential) fcntl(fd, F_RDAHEAD, 1);
    if (drop_after) fcntl(fd, F_NOCACHE, 1);

    // Try to read in larger chunks for better performance
    if (size <= CHUNK) {
        ssize_t n = read(fd, dst, size);
        if (n < 0 || static_cast<size_t>(n) != size) {
            int e = (n < 0) ? -errno : -EIO;
            close(fd);
            return e;
        }
    } else {
        size_t off = 0;
        while (off < size) {
            size_t to_read = std::min(CHUNK, size - off);
            ssize_t n = pread(fd, dst + off, to_read, static_cast<off_t>(off));
            if (n <= 0) { 
                int e = (n == 0) ? -EIO : -errno; 
                close(fd); 
                return e; 
            }
            off += static_cast<size_t>(n);
        }
    }

    close(fd);
    *out_len = size;
    return 0;
}

#else
// Windows optimized version
#include <windows.h>

int read_all_pread(const char* path, uint8_t* dst, size_t* out_len,
                   bool sequential, bool willneed, bool drop_after) {
    HANDLE hFile = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, 
                              NULL, OPEN_EXISTING, 
                              FILE_ATTRIBUTE_NORMAL | 
                              (sequential ? FILE_FLAG_SEQUENTIAL_SCAN : FILE_FLAG_RANDOM_ACCESS), 
                              NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        return -1;
    }

    DWORD sizeHigh = 0;
    DWORD sizeLow = GetFileSize(hFile, &sizeHigh);
    size_t size = (static_cast<size_t>(sizeHigh) << 32) | sizeLow;

    DWORD bytesRead = 0;
    BOOL success = ReadFile(hFile, dst, static_cast<DWORD>(size), &bytesRead, NULL);
    
    CloseHandle(hFile);

    if (!success || bytesRead != size) {
        return -1;
    }

    *out_len = bytesRead;
    return 0;
}
#endif

// Ultra-fast mmap version - often the fastest for file reading
int read_all_mmap(const char* path, uint8_t** dst, size_t* out_len) {
#ifdef __linux__
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t size = static_cast<size_t>(st.st_size);
    
    // Handle empty files - mmap doesn't work with size 0
    if (size == 0) {
        close(fd);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    void* mapped = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (mapped == MAP_FAILED) {
        return -errno;
    }

    *dst = static_cast<uint8_t*>(mapped);
    *out_len = size;
    
    // Caller must call munmap(*dst, *out_len) when done!
    return 0;
#elif defined(__APPLE__)
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st{};
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t size = static_cast<size_t>(st.st_size);
    
    // Handle empty files - mmap doesn't work with size 0
    if (size == 0) {
        close(fd);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    void* mapped = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (mapped == MAP_FAILED) {
        return -errno;
    }

    // On macOS, advise sequential access
    madvise(mapped, size, MADV_SEQUENTIAL);
    
    *dst = static_cast<uint8_t*>(mapped);
    *out_len = size;
    return 0;
#else
    // Windows mmap
    HANDLE hFile = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, 
                              NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) return -1;

    DWORD sizeHigh = 0;
    DWORD sizeLow = GetFileSize(hFile, &sizeHigh);
    size_t size = (static_cast<size_t>(sizeHigh) << 32) | sizeLow;

    // Handle empty files
    if (size == 0) {
        CloseHandle(hFile);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }

    HANDLE hMapping = CreateFileMappingA(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
    if (!hMapping) {
        CloseHandle(hFile);
        return -1;
    }

    void* mapped = MapViewOfFile(hMapping, FILE_MAP_READ, 0, 0, size);
    CloseHandle(hMapping);
    CloseHandle(hFile);

    if (!mapped) return -1;

    *dst = static_cast<uint8_t*>(mapped);
    *out_len = size;
    return 0;
#endif
}

// Slice/range read implementations - for blob store range requests

#ifdef __linux__

int read_slice_pread(const char* path, size_t offset, size_t length, uint8_t* dst, size_t* out_len,
                     bool sequential, bool willneed, bool drop_after) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t file_size = static_cast<size_t>(st.st_size);
    
    // Validate offset and length
    if (offset >= file_size) {
        close(fd);
        *out_len = 0;
        return 0;
    }
    
    size_t to_read = (offset + length > file_size) ? (file_size - offset) : length;
    
    if (sequential) posix_fadvise(fd, static_cast<off_t>(offset), static_cast<off_t>(to_read), POSIX_FADV_SEQUENTIAL);
    if (willneed) posix_fadvise(fd, static_cast<off_t>(offset), static_cast<off_t>(to_read), POSIX_FADV_WILLNEED);
    
    ssize_t n = pread(fd, dst, to_read, static_cast<off_t>(offset));
    if (n < 0) {
        int e = -errno;
        close(fd);
        return e;
    }
    
    if (drop_after) posix_fadvise(fd, static_cast<off_t>(offset), static_cast<off_t>(to_read), POSIX_FADV_DONTNEED);
    close(fd);
    
    *out_len = static_cast<size_t>(n);
    return 0;
}

#elif defined(__APPLE__)

int read_slice_pread(const char* path, size_t offset, size_t length, uint8_t* dst, size_t* out_len,
                     bool sequential, bool willneed, bool drop_after) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t file_size = static_cast<size_t>(st.st_size);
    
    // Validate offset and length
    if (offset >= file_size) {
        close(fd);
        *out_len = 0;
        return 0;
    }
    
    size_t to_read = (offset + length > file_size) ? (file_size - offset) : length;
    
    if (sequential) fcntl(fd, F_RDAHEAD, 1);
    if (drop_after) fcntl(fd, F_NOCACHE, 1);
    
    ssize_t n = pread(fd, dst, to_read, static_cast<off_t>(offset));
    if (n < 0) {
        int e = -errno;
        close(fd);
        return e;
    }
    
    close(fd);
    *out_len = static_cast<size_t>(n);
    return 0;
}

#else
// Windows slice read

int read_slice_pread(const char* path, size_t offset, size_t length, uint8_t* dst, size_t* out_len,
                     bool sequential, bool willneed, bool drop_after) {
    HANDLE hFile = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, 
                              NULL, OPEN_EXISTING, 
                              FILE_ATTRIBUTE_NORMAL | 
                              (sequential ? FILE_FLAG_SEQUENTIAL_SCAN : FILE_FLAG_RANDOM_ACCESS), 
                              NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        return -1;
    }

    DWORD sizeHigh = 0;
    DWORD sizeLow = GetFileSize(hFile, &sizeHigh);
    size_t file_size = (static_cast<size_t>(sizeHigh) << 32) | sizeLow;

    // Validate offset and length
    if (offset >= file_size) {
        CloseHandle(hFile);
        *out_len = 0;
        return 0;
    }
    
    size_t to_read = (offset + length > file_size) ? (file_size - offset) : length;
    
    // Seek to offset
    LARGE_INTEGER li;
    li.QuadPart = static_cast<LONGLONG>(offset);
    if (!SetFilePointerEx(hFile, li, NULL, FILE_BEGIN)) {
        CloseHandle(hFile);
        return -1;
    }
    
    DWORD bytesRead = 0;
    BOOL success = ReadFile(hFile, dst, static_cast<DWORD>(to_read), &bytesRead, NULL);
    
    CloseHandle(hFile);

    if (!success) {
        return -1;
    }

    *out_len = bytesRead;
    return 0;
}
#endif

// Slice mmap implementations

#ifdef __linux__

int read_slice_mmap(const char* path, size_t offset, size_t length, uint8_t** dst, size_t* out_len) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t file_size = static_cast<size_t>(st.st_size);
    
    // Validate offset
    if (offset >= file_size) {
        close(fd);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    size_t to_map = (offset + length > file_size) ? (file_size - offset) : length;
    
    // Handle empty reads
    if (to_map == 0) {
        close(fd);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    // Map from the beginning and return pointer to offset within mapping
    void* mapped = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (mapped == MAP_FAILED) {
        return -errno;
    }

    *dst = static_cast<uint8_t*>(mapped) + offset;
    *out_len = to_map;
    
    // Note: Caller must unmap the entire mapped region (subtract offset from ptr, add to size)
    return 0;
}

#elif defined(__APPLE__)

int read_slice_mmap(const char* path, size_t offset, size_t length, uint8_t** dst, size_t* out_len) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st {};
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t file_size = static_cast<size_t>(st.st_size);
    
    // Validate offset
    if (offset >= file_size) {
        close(fd);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    size_t to_map = (offset + length > file_size) ? (file_size - offset) : length;
    
    // Handle empty reads
    if (to_map == 0) {
        close(fd);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    // Map from the beginning and return pointer to offset within mapping
    void* mapped = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (mapped == MAP_FAILED) {
        return -errno;
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);
    
    *dst = static_cast<uint8_t*>(mapped) + offset;
    *out_len = to_map;
    
    // Note: Caller must unmap the entire mapped region (subtract offset from ptr, add to size)
    return 0;
}

#else
// Windows slice mmap

int read_slice_mmap(const char* path, size_t offset, size_t length, uint8_t** dst, size_t* out_len) {
    HANDLE hFile = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, 
                              NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) return -1;

    DWORD sizeHigh = 0;
    DWORD sizeLow = GetFileSize(hFile, &sizeHigh);
    size_t file_size = (static_cast<size_t>(sizeHigh) << 32) | sizeLow;

    // Validate offset
    if (offset >= file_size) {
        CloseHandle(hFile);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    size_t to_map = (offset + length > file_size) ? (file_size - offset) : length;
    
    // Handle empty reads
    if (to_map == 0) {
        CloseHandle(hFile);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }

    HANDLE hMapping = CreateFileMappingA(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
    if (!hMapping) {
        CloseHandle(hFile);
        return -1;
    }

    void* mapped = MapViewOfFile(hMapping, FILE_MAP_READ, 0, 0, file_size);
    CloseHandle(hMapping);
    CloseHandle(hFile);

    if (!mapped) return -1;

    *dst = static_cast<uint8_t*>(mapped) + offset;
    *out_len = to_map;
    
    // Note: Caller must unmap the entire mapped region (subtract offset from ptr, add to size)
    return 0;
}
#endif

int unmap_memory_c(unsigned char* addr, size_t size) {
#ifdef __linux__
    return munmap(addr, size) == 0 ? 0 : -errno;
#elif defined(__APPLE__)
    return munmap(addr, size) == 0 ? 0 : -errno;
#else
    return UnmapViewOfFile(addr) ? 0 : -1;
#endif
}

// ---------------------------------------------------------------------------
// Batched range read.
//
// Opens the file once, fans pread() across workers sharing one fd. Caller
// supplies pre-allocated destination buffers; we never allocate on the hot
// path. Fail-fast on the first error.
// ---------------------------------------------------------------------------

namespace {

constexpr size_t kMaxRangeWorkers = 8;

#if defined(__linux__) || defined(__APPLE__)

// Run one range against an open fd (clamped against file_size).
// Returns 0 on success or a negative errno.
inline int run_one_range(int fd, size_t file_size, read_range_t& r) {
    if (r.offset >= file_size) {
        r.out_len = 0;
        r.rc = 0;
        return 0;
    }
    size_t to_read = (r.offset + r.length > file_size)
                       ? (file_size - r.offset)
                       : r.length;
    size_t done = 0;
    while (done < to_read) {
        ssize_t n = pread(fd, r.dst + done, to_read - done,
                          static_cast<off_t>(r.offset + done));
        if (n < 0) {
            if (errno == EINTR) continue;
            r.rc = -errno;
            r.out_len = done;
            return r.rc;
        }
        if (n == 0) {
            // Short read: file ended earlier than fstat said. Accept it.
            break;
        }
        done += static_cast<size_t>(n);
    }
    r.out_len = done;
    r.rc = 0;
    return 0;
}

int read_ranges_pread_posix(const char* path, read_range_t* ranges, size_t count) {
    if (count == 0) return 0;

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        int e = -errno;
        for (size_t i = 0; i < count; ++i) { ranges[i].out_len = 0; ranges[i].rc = e; }
        return e;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        int e = -errno;
        close(fd);
        for (size_t i = 0; i < count; ++i) { ranges[i].out_len = 0; ranges[i].rc = e; }
        return e;
    }
    size_t file_size = static_cast<size_t>(st.st_size);

    // Single-range fast path: no thread spawn.
    if (count == 1) {
        int rc = run_one_range(fd, file_size, ranges[0]);
        close(fd);
        return rc;
    }

    // Initialise output fields; -ECANCELED for ranges that don't end up running.
    for (size_t i = 0; i < count; ++i) {
        ranges[i].out_len = 0;
        ranges[i].rc = -ECANCELED;
    }

    std::atomic<size_t> next_idx{0};
    std::atomic<int> first_err{0};

    size_t worker_count = std::min<size_t>(count, kMaxRangeWorkers);

    auto worker = [&]() {
        while (true) {
            if (first_err.load(std::memory_order_relaxed) != 0) return;
            size_t i = next_idx.fetch_add(1, std::memory_order_relaxed);
            if (i >= count) return;

            int rc = run_one_range(fd, file_size, ranges[i]);
            if (rc != 0) {
                int expected = 0;
                first_err.compare_exchange_strong(expected, rc,
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed);
                return;
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(worker_count);
    for (size_t w = 0; w < worker_count; ++w) {
        threads.emplace_back(worker);
    }
    for (auto& t : threads) t.join();

    close(fd);
    return first_err.load(std::memory_order_relaxed);
}

#else  // Windows: simple, correct, sequential. Windows is not the perf target.

int read_ranges_pread_win(const char* path, read_range_t* ranges, size_t count) {
    if (count == 0) return 0;

    HANDLE hFile = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ,
                               NULL, OPEN_EXISTING,
                               FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
                               NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        int e = -1;
        for (size_t i = 0; i < count; ++i) { ranges[i].out_len = 0; ranges[i].rc = e; }
        return e;
    }

    DWORD sizeHigh = 0;
    DWORD sizeLow = GetFileSize(hFile, &sizeHigh);
    size_t file_size = (static_cast<size_t>(sizeHigh) << 32) | sizeLow;

    int first_err = 0;
    for (size_t i = 0; i < count; ++i) {
        read_range_t& r = ranges[i];
        if (first_err != 0) { r.out_len = 0; r.rc = -ECANCELED; continue; }
        if (r.offset >= file_size) { r.out_len = 0; r.rc = 0; continue; }
        size_t to_read = (r.offset + r.length > file_size)
                           ? (file_size - r.offset) : r.length;
        OVERLAPPED ov = {};
        ov.Offset = static_cast<DWORD>(r.offset & 0xFFFFFFFFu);
        ov.OffsetHigh = static_cast<DWORD>(r.offset >> 32);
        DWORD got = 0;
        BOOL ok = ReadFile(hFile, r.dst, static_cast<DWORD>(to_read), &got, &ov);
        if (!ok && GetLastError() != ERROR_HANDLE_EOF) {
            r.rc = -1;
            r.out_len = got;
            first_err = -1;
        } else {
            r.rc = 0;
            r.out_len = got;
        }
    }

    CloseHandle(hFile);
    return first_err;
}

#endif

}  // namespace

// Public symbol — the only definition outside the anonymous namespace.
int read_ranges_pread(const char* path, read_range_t* ranges, size_t count) {
#if defined(__linux__) || defined(__APPLE__)
    return read_ranges_pread_posix(path, ranges, count);
#else
    return read_ranges_pread_win(path, ranges, count);
#endif
}