// IPC deserialiser implementation. See header for design notes.

#include "ipc_deserialize.hpp"
#include "memory_pool.hpp"

#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace opteryx {

namespace {

// Little-endian unaligned uint32_t read. The IPC format is little-endian on
// disk (writer is x86/ARM little-endian); we replicate the byte-order rather
// than reinterpret_casting because the payload is not guaranteed to be
// 4-byte-aligned (the preceding null_bitmap is variable-length).
static inline const uint8_t* read_u32(const uint8_t* p, uint32_t& out) noexcept {
    out = (static_cast<uint32_t>(p[0]))            |
          (static_cast<uint32_t>(p[1]) <<  8)      |
          (static_cast<uint32_t>(p[2]) << 16)      |
          (static_cast<uint32_t>(p[3]) << 24);
    return p + 4;
}

// Allocate + memcpy. Returns nullptr on OOM. nbytes == 0 returns nullptr without
// allocating, matching the existing Cython behaviour (a NULL null_bitmap is
// interpreted as "non-nullable").
static inline uint8_t* malloc_copy(const uint8_t* src, size_t nbytes) noexcept {
    if (nbytes == 0) return nullptr;
    uint8_t* dst = static_cast<uint8_t*>(std::malloc(nbytes));
    if (dst == nullptr) return nullptr;
    std::memcpy(dst, src, nbytes);
    return dst;
}

static inline bool bounds_ok(const uint8_t* p, const uint8_t* end, uint32_t need) noexcept {
    return static_cast<size_t>(end - p) >= static_cast<size_t>(need);
}

// Helper: copy the null bitmap (if present) into out.null_bitmap.
// On OOM, frees the previously-allocated data buffer and writes status=kStatusOom.
// Returns true on success.
static inline bool copy_nulls(const uint8_t* null_bitmap, uint32_t null_bitmap_len,
                              DecodedFixedColumn& out) noexcept {
    if (null_bitmap_len == 0) {
        out.null_bitmap = nullptr;
        return true;
    }
    out.null_bitmap = malloc_copy(null_bitmap, null_bitmap_len);
    if (out.null_bitmap == nullptr) {
        std::free(out.data);
        out.data = nullptr;
        out.status = kStatusOom;
        return false;
    }
    return true;
}

// Plain copy of a contiguous data buffer (int64, float32, float64, bool).
// Reads the leading uint32 data_len from `p`, then memcpys `data_len` bytes
// into a fresh malloc'd buffer. Bounds-checks the declared length.
static bool copy_contig(const uint8_t* p, const uint8_t* end,
                        DecodedFixedColumn& out) noexcept {
    if (!bounds_ok(p, end, 4)) { out.status = kStatusTruncated; return false; }
    uint32_t data_len;
    p = read_u32(p, data_len);
    if (!bounds_ok(p, end, data_len)) { out.status = kStatusTruncated; return false; }

    // data_len may legitimately be zero (an empty column). Leave out.data as
    // nullptr in that case; the destination Vector's Cython constructor
    // accepts a NULL data pointer for zero-length buffers.
    if (data_len > 0) {
        out.data = std::malloc(data_len);
        if (out.data == nullptr) { out.status = kStatusOom; return false; }
        std::memcpy(out.data, p, data_len);
    }
    return true;
}

// int32 → int64 widening. Source is data_len / 4 int32 values; destination is
// the same count as int64 values. The source pointer is not aligned (the
// preceding null bitmap is variable-length), so use memcpy-into-scratch
// rather than reinterpret_cast — the compiler emits scalar sign-extension
// loads on ARM and x86. A future NEON/AVX2 path can replace this loop.
static bool widen_int32_to_int64(const uint8_t* p, const uint8_t* end,
                                 DecodedFixedColumn& out) noexcept {
    if (!bounds_ok(p, end, 4)) { out.status = kStatusTruncated; return false; }
    uint32_t data_len;
    p = read_u32(p, data_len);
    if (!bounds_ok(p, end, data_len)) { out.status = kStatusTruncated; return false; }

    const uint32_t n = data_len >> 2;
    const size_t dst_bytes = static_cast<size_t>(n) * sizeof(int64_t);

    if (dst_bytes > 0) {
        out.data = std::malloc(dst_bytes);
        if (out.data == nullptr) { out.status = kStatusOom; return false; }

        int64_t* dst = static_cast<int64_t*>(out.data);
        int32_t scratch;
        for (uint32_t i = 0; i < n; ++i) {
            std::memcpy(&scratch, p + (i * sizeof(int32_t)), sizeof(int32_t));
            dst[i] = static_cast<int64_t>(scratch);
        }
    }
    return true;
}

}  // anonymous namespace

void deserialize_fixed_column(const uint8_t* data, int64_t length, DecodedFixedColumn& out) noexcept {
    // Pre-initialise so error paths return a clean state. Buffers stay null
    // until the per-type parser succeeds.
    out.kind = IpcKind::Int64;
    out.num_rows = 0;
    out.data = nullptr;
    out.null_bitmap = nullptr;
    out.status = kStatusOk;
    out.tag = 0;

    if (data == nullptr || length <= 0) {
        out.status = kStatusTruncated;
        return;
    }

    const uint8_t* p   = data;
    const uint8_t* end = p + static_cast<size_t>(length);

    // Minimum header: tag(1) + num_rows(4) + null_bitmap_len(4) = 9 bytes.
    if (!bounds_ok(p, end, 9)) {
        out.status = kStatusTruncated;
        return;
    }

    out.tag = *p++;
    uint32_t num_rows;
    p = read_u32(p, num_rows);
    uint32_t null_bitmap_len;
    p = read_u32(p, null_bitmap_len);

    if (!bounds_ok(p, end, null_bitmap_len)) {
        out.status = kStatusTruncated;
        return;
    }
    const uint8_t* null_bitmap = p;
    p += null_bitmap_len;

    out.num_rows = num_rows;

    switch (out.tag) {
        case kTagInt64:
            if (!copy_contig(p, end, out)) return;
            if (!copy_nulls(null_bitmap, null_bitmap_len, out)) return;
            out.kind = IpcKind::Int64;
            break;

        case kTagInt32:
            // Widened destination is int64.
            if (!widen_int32_to_int64(p, end, out)) return;
            if (!copy_nulls(null_bitmap, null_bitmap_len, out)) return;
            out.kind = IpcKind::Int64;
            break;

        case kTagFloat32:
            if (!copy_contig(p, end, out)) return;
            if (!copy_nulls(null_bitmap, null_bitmap_len, out)) return;
            out.kind = IpcKind::Float32;
            break;

        case kTagFloat64:
            if (!copy_contig(p, end, out)) return;
            if (!copy_nulls(null_bitmap, null_bitmap_len, out)) return;
            out.kind = IpcKind::Float64;
            break;

        case kTagBool:
            // BoolVector stores bit-packed data; data_len is the byte count of
            // the packed payload. num_rows is the logical row count.
            if (!copy_contig(p, end, out)) return;
            if (!copy_nulls(null_bitmap, null_bitmap_len, out)) return;
            out.kind = IpcKind::Bool;
            break;

        case kTagStrDict:
        case kTagStrPlain:
        case kTagInt64Dict:
        case kTagFloat32Dict:
        case kTagFloat64Dict:
            out.status = kStatusNotHandled;
            return;

        default:
            out.status = kStatusUnknownTag;
            return;
    }

    out.status = kStatusOk;
}

void deserialize_row_group_fixed(
    MemoryPool& pool,
    const int64_t* ref_ids,
    size_t n_cols,
    DecodedFixedColumn* out
) noexcept {
    for (size_t i = 0; i < n_cols; ++i) {
        // Pre-initialise to a clean state. deserialize_fixed_column will overwrite
        // these fields on the fixed-width path; on early-exit error paths the
        // Cython caller sees a well-formed (status != Ok, pointers nullptr) slot.
        out[i].kind = IpcKind::Int64;
        out[i].num_rows = 0;
        out[i].data = nullptr;
        out[i].null_bitmap = nullptr;
        out[i].status = kStatusOk;
        out[i].tag = 0;

        ReadResult r{nullptr, 0};
        try {
            r = pool.read(ref_ids[i], /*latch=*/true);
        } catch (...) {
            // Pool lookup failed (bad ref_id, etc). Surface as truncated so
            // the Cython side raises ValueError rather than hanging on an
            // un-acquired latch.
            out[i].status = kStatusTruncated;
            continue;  // No latch to release.
        }

        // Parse + malloc + memcpy (or set kStatusNotHandled for dict/string).
        // Tag byte is captured inside deserialize_fixed_column regardless of
        // status, so the Cython fallback path knows which dict/string parser
        // to invoke.
        deserialize_fixed_column(
            static_cast<const uint8_t*>(r.ptr),
            r.length,
            out[i]
        );

        // Always unlatch — the destination buffer (if any) is in malloc'd
        // memory now, independent of the pool segment.
        try {
            pool.unlatch(ref_ids[i]);
        } catch (...) {
            // Pool state error during unlatch is unrecoverable from here.
            // Leave out[i] as it stands; the Cython caller will free any
            // malloc'd buffers if it raises on a non-Ok status.
        }
    }
}

}  // namespace opteryx
