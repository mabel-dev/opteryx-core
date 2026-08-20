#include "encoding.h"

#include <cstdio>
#include <cstring>

#include "lz4.h"
#include "zstd.h"

namespace skene {
namespace {

Status fail(Code code, const char* message) { return Status(code, message); }

inline uint64_t width_mask(uint8_t width) {
    // Shifting by 64 is undefined, so the full-width case is spelled out rather
    // than left to (1 << 64) - 1.
    return width >= 64 ? ~uint64_t{0} : ((uint64_t{1} << width) - 1u);
}

inline uint64_t packed_bytes(uint32_t count, uint8_t width) {
    return (static_cast<uint64_t>(count) * width + 7u) / 8u;
}

// LSB-first bit packing.
//
// The accumulator is 128-bit on purpose: `width` reaches 64 and up to 7 bits can
// still be pending from the previous value, so a 64-bit accumulator would drop
// the top bits of a full-width delta — silently, and only on data wide enough to
// need it.
void pack(const uint64_t* values, uint32_t count, uint8_t width, uint8_t* out) {
    if (width == 0) return;
    const uint64_t mask = width_mask(width);
    __uint128_t accumulator = 0;
    int pending = 0;
    size_t at = 0;
    for (uint32_t i = 0; i < count; ++i) {
        accumulator |= static_cast<__uint128_t>(values[i] & mask) << pending;
        pending += width;
        while (pending >= 8) {
            out[at++] = static_cast<uint8_t>(accumulator);
            accumulator >>= 8;
            pending -= 8;
        }
    }
    if (pending > 0) out[at] = static_cast<uint8_t>(accumulator);
}

// Inverse of pack(). `available` bounds the read so a corrupt width or count
// cannot walk off the section.
Status unpack(const uint8_t* in, uint64_t available, uint32_t count, uint8_t width,
              uint64_t* out) {
    if (width == 0) {
        std::memset(out, 0, static_cast<size_t>(count) * sizeof(uint64_t));
        return Status::ok();
    }
    if (packed_bytes(count, width) > available)
        return fail(Code::kMalformed,
                    "bit-packed body is shorter than its declared count and width");

    const uint64_t mask = width_mask(width);
    __uint128_t accumulator = 0;
    int pending = 0;
    size_t at = 0;
    for (uint32_t i = 0; i < count; ++i) {
        while (pending < width) {
            accumulator |= static_cast<__uint128_t>(in[at++]) << pending;
            pending += 8;
        }
        out[i] = static_cast<uint64_t>(accumulator) & mask;
        accumulator >>= width;
        pending -= width;
    }
    return Status::ok();
}

// Inverse of pack() specialised for SELECTION CODES, which the format bounds at
// 32 bits (bitpack_decode_codes rejects a wider declared width before calling
// this). That bound is what makes the direct form possible, and it matters:
// selection decode is per-ROW work on the widest side of a dict column, so it
// dominated the read of every repeat-heavy column (measured on TPC-H lineitem,
// a 2-distinct BOOL-ish column cost 8ms against 2.4ms for a 32MB dense one).
//
// Three things the generic unpack() does that this does not:
//   - no uint64 scratch vector, allocated and zeroed once per column per file,
//     then narrowed to uint32 in a second pass — this writes uint32 directly;
//   - no byte-at-a-time refill loop with a 128-bit accumulator — each value is
//     ONE unaligned 8-byte load, one shift, one mask, no inner loop and no
//     carried dependency between iterations, so it pipelines;
//   - no per-value bounds branch in the body: the safe prefix is computed once.
//
// The 8-byte load is correct because width <= 32 and the bit offset within a
// byte is <= 7, so a value never spans more than 39 bits — always inside the
// 64-bit window. It reads little-endian, which is unconditional here: the
// format IS little-endian and reader.cpp rejects any other file before a
// section is ever interpreted, so there is no big-endian path to be wrong on.
Status unpack32(const uint8_t* in, uint64_t available, uint32_t count, uint8_t width,
                uint32_t* out) {
    if (width == 0) {
        std::memset(out, 0, static_cast<size_t>(count) * sizeof(uint32_t));
        return Status::ok();
    }
    const uint64_t nbytes = packed_bytes(count, width);
    if (nbytes > available)
        return fail(Code::kMalformed,
                    "bit-packed body is shorter than its declared count and width");

    const uint64_t mask = width_mask(width);

    // Largest i whose 8-byte window stays inside the body: (i*width)/8 + 8 <= nbytes.
    uint32_t fast_count = 0;
    if (nbytes >= 8) {
        const uint64_t max_byte = nbytes - 8u;
        const uint64_t max_i = (max_byte * 8u) / width;   // (i*width)>>3 <= max_byte
        fast_count = max_i >= count ? count : static_cast<uint32_t>(max_i);
    }

    uint32_t i = 0;
    for (; i < fast_count; ++i) {
        const uint64_t bit = static_cast<uint64_t>(i) * width;
        uint64_t chunk;
        std::memcpy(&chunk, in + (bit >> 3), sizeof(chunk));
        out[i] = static_cast<uint32_t>((chunk >> (bit & 7u)) & mask);
    }
    // Tail: the last few values, read bit by bit so no load can pass the end.
    for (; i < count; ++i) {
        const uint64_t bit = static_cast<uint64_t>(i) * width;
        uint64_t value = 0;
        for (uint8_t b = 0; b < width; ++b) {
            const uint64_t at = bit + b;
            const uint64_t byte_index = at >> 3;
            if (byte_index >= nbytes) break;
            value |= static_cast<uint64_t>((in[byte_index] >> (at & 7u)) & 1u) << b;
        }
        out[i] = static_cast<uint32_t>(value & mask);
    }
    return Status::ok();
}

// Reads one value of `item_bytes` as an unsigned integer. Signed types are read
// through their unsigned twin: see the wrapping-difference note in format.h.
inline uint64_t load(const void* data, size_t index, size_t item_bytes) {
    if (item_bytes == 4) return static_cast<const uint32_t*>(data)[index];
    return static_cast<const uint64_t*>(data)[index];
}

inline void store(void* data, size_t index, size_t item_bytes, uint64_t value) {
    if (item_bytes == 4)
        static_cast<uint32_t*>(data)[index] = static_cast<uint32_t>(value);
    else
        static_cast<uint64_t*>(data)[index] = value;
}

}  // namespace

uint8_t bits_required(uint64_t max_value) {
    uint8_t bits = 0;
    while (max_value > 0) { ++bits; max_value >>= 1; }
    return bits;
}

// ─── kBitpack ───────────────────────────────────────────────────────────────

bool bitpack_encode_codes(const uint32_t* codes, uint32_t count,
                          uint32_t data_length, std::vector<uint8_t>* out) {
    // The width comes from data_length, not from a scan: every code is already
    // proven < data_length by the writer's classification pass, so scanning for
    // the maximum would re-derive a bound we hold.
    const uint8_t width = bits_required(data_length > 0 ? data_length - 1u : 0u);

    const uint64_t plain = static_cast<uint64_t>(count) * sizeof(uint32_t);
    const uint64_t encoded = sizeof(BitpackHeader) + packed_bytes(count, width);
    if (encoded >= plain) return false;

    out->assign(static_cast<size_t>(encoded), 0);

    BitpackHeader header{};
    header.count     = count;
    header.bit_width = width;
    std::memcpy(out->data(), &header, sizeof(header));

    if (width > 0) {
        std::vector<uint64_t> widened(count);
        for (uint32_t i = 0; i < count; ++i) widened[i] = codes[i];
        pack(widened.data(), count, width, out->data() + sizeof(BitpackHeader));
    }
    return true;
}

bool bitpack_encode_u32(const uint32_t* values, uint32_t count,
                        std::vector<uint8_t>* out) {
    uint32_t maximum = 0;
    for (uint32_t i = 0; i < count; ++i)
        if (values[i] > maximum) maximum = values[i];
    const uint8_t width = bits_required(maximum);

    const uint64_t plain = static_cast<uint64_t>(count) * sizeof(uint32_t);
    const uint64_t encoded = sizeof(BitpackHeader) + packed_bytes(count, width);
    if (encoded >= plain) return false;

    out->assign(static_cast<size_t>(encoded), 0);

    BitpackHeader header{};
    header.count     = count;
    header.bit_width = width;
    std::memcpy(out->data(), &header, sizeof(header));

    if (width > 0) {
        std::vector<uint64_t> widened(count);
        for (uint32_t i = 0; i < count; ++i) widened[i] = values[i];
        pack(widened.data(), count, width, out->data() + sizeof(BitpackHeader));
    }
    return true;
}

Status bitpack_decode_codes(const uint8_t* stored, uint64_t stored_bytes,
                            uint32_t count, uint32_t* out) {
    if (stored_bytes < sizeof(BitpackHeader))
        return fail(Code::kMalformed, "bit-packed section is too short for its header");

    BitpackHeader header;
    std::memcpy(&header, stored, sizeof(header));

    if (header.count != count) {
        char message[192];
        std::snprintf(message, sizeof(message),
                      "bit-packed section holds %u values but the column declares %u",
                      header.count, count);
        return fail(Code::kMalformed, message);
    }
    if (header.bit_width > 32)
        return fail(Code::kMalformed,
                    "bit-packed selection declares a width above 32 bits");

    return unpack32(stored + sizeof(BitpackHeader),
                    stored_bytes - sizeof(BitpackHeader),
                    count, header.bit_width, out);
}

// ─── kDeltaBitpack ──────────────────────────────────────────────────────────

bool type_supports_delta(DrakenType type) {
    switch (type) {
        case DRAKEN_INT32:  case DRAKEN_UINT32:
        case DRAKEN_DATE32: case DRAKEN_TIME32:
        case DRAKEN_INT64:  case DRAKEN_UINT64:
        case DRAKEN_TIMESTAMP64: case DRAKEN_TIME64:
        case DRAKEN_DECIMAL:
            return true;
        default:
            return false;
    }
}

bool delta_bitpack_encode(const void* data, uint32_t count, size_t item_bytes,
                          std::vector<uint8_t>* out) {
    if (count == 0 || (item_bytes != 4 && item_bytes != 8)) return false;

    const uint32_t deltas = count - 1u;
    std::vector<uint64_t> differences(deltas > 0 ? deltas : 1u);

    // Wrapping unsigned subtraction. For an ascending SIGNED array this is the
    // true step magnitude regardless of sign (-5 -> 3 gives 8), and it cannot
    // overflow the way signed subtraction would across a wide range.
    uint64_t maximum = 0;
    uint64_t previous = load(data, 0, item_bytes);
    for (uint32_t i = 1; i < count; ++i) {
        const uint64_t current = load(data, i, item_bytes);
        uint64_t difference = current - previous;
        if (item_bytes == 4) difference &= 0xFFFFFFFFu;
        differences[i - 1u] = difference;
        if (difference > maximum) maximum = difference;
        previous = current;
    }

    const uint8_t width = bits_required(maximum);
    const uint64_t plain = static_cast<uint64_t>(count) * item_bytes;
    const uint64_t encoded = sizeof(DeltaBitpackHeader) + item_bytes
                           + packed_bytes(deltas, width);
    if (encoded >= plain) return false;

    out->assign(static_cast<size_t>(encoded), 0);

    DeltaBitpackHeader header{};
    header.count      = count;
    header.item_bytes = static_cast<uint8_t>(item_bytes);
    header.bit_width  = width;
    std::memcpy(out->data(), &header, sizeof(header));

    const uint64_t first = load(data, 0, item_bytes);
    std::memcpy(out->data() + sizeof(DeltaBitpackHeader), &first, item_bytes);

    if (deltas > 0 && width > 0)
        pack(differences.data(), deltas, width,
             out->data() + sizeof(DeltaBitpackHeader) + item_bytes);
    return true;
}

Status delta_bitpack_decode(const uint8_t* stored, uint64_t stored_bytes,
                            uint32_t count, size_t item_bytes, void* out) {
    if (stored_bytes < sizeof(DeltaBitpackHeader))
        return fail(Code::kMalformed, "delta section is too short for its header");

    DeltaBitpackHeader header;
    std::memcpy(&header, stored, sizeof(header));

    if (header.count != count) {
        char message[192];
        std::snprintf(message, sizeof(message),
                      "delta section holds %u values but the column declares %u",
                      header.count, count);
        return fail(Code::kMalformed, message);
    }
    if (header.item_bytes != item_bytes) {
        char message[192];
        std::snprintf(message, sizeof(message),
                      "delta section declares %u-byte items but the column's type "
                      "is %zu-byte", header.item_bytes, item_bytes);
        return fail(Code::kMalformed, message);
    }
    if (header.bit_width > 64)
        return fail(Code::kMalformed, "delta section declares a width above 64 bits");
    if (count == 0) return Status::ok();
    if (stored_bytes < sizeof(DeltaBitpackHeader) + item_bytes)
        return fail(Code::kMalformed, "delta section is too short to hold its first value");

    uint64_t value = 0;
    std::memcpy(&value, stored + sizeof(DeltaBitpackHeader), item_bytes);
    store(out, 0, item_bytes, value);

    const uint32_t deltas = count - 1u;
    if (deltas == 0) return Status::ok();

    std::vector<uint64_t> differences(deltas);
    SKENE_RETURN_IF_ERROR(
        unpack(stored + sizeof(DeltaBitpackHeader) + item_bytes,
               stored_bytes - sizeof(DeltaBitpackHeader) - item_bytes,
               deltas, header.bit_width, differences.data()));

    for (uint32_t i = 1; i <= deltas; ++i) {
        value += differences[i - 1u];
        if (item_bytes == 4) value &= 0xFFFFFFFFu;
        store(out, i, item_bytes, value);
    }
    return Status::ok();
}

// ─── kZstd ──────────────────────────────────────────────────────────────────

bool zstd_encode(const void* plain, size_t plain_bytes, int level,
                 std::vector<uint8_t>* out) {
    if (plain_bytes == 0) return false;

    const size_t bound = ZSTD_compressBound(plain_bytes);
    out->resize(bound);
    const size_t produced =
        ZSTD_compress(out->data(), bound, plain, plain_bytes, level);
    if (ZSTD_isError(produced) || produced >= plain_bytes) {
        out->clear();
        return false;   // "not smaller" is a normal answer, not a failure
    }
    out->resize(produced);
    return true;
}

Status zstd_decode(const uint8_t* stored, uint64_t stored_bytes,
                   uint64_t plain_bytes, uint8_t* out) {
    const size_t produced =
        ZSTD_decompress(out, static_cast<size_t>(plain_bytes), stored,
                        static_cast<size_t>(stored_bytes));
    if (ZSTD_isError(produced)) {
        char message[192];
        std::snprintf(message, sizeof(message), "zstd section failed to decode: %s",
                      ZSTD_getErrorName(produced));
        return fail(Code::kMalformed, message);
    }
    // A frame that decodes to a different size than the directory declares is a
    // contradiction: the section's shape is decided by the directory, and a body
    // disagreeing with it must be rejected rather than reshaped.
    if (produced != plain_bytes) {
        char message[192];
        std::snprintf(message, sizeof(message),
                      "zstd section decodes to %zu bytes but the directory "
                      "declares %llu", produced,
                      static_cast<unsigned long long>(plain_bytes));
        return fail(Code::kMalformed, message);
    }
    return Status::ok();
}

// ─── kLz4 ───────────────────────────────────────────────────────────────────

bool lz4_encode(const void* plain, size_t plain_bytes, std::vector<uint8_t>* out) {
    if (plain_bytes == 0) return false;
    // LZ4's block API is int-sized throughout. A section past that ceiling is
    // stored plain — the same answer as "not smaller", and for the same reason:
    // there is no correct compressed form to emit, so there is nothing to lose
    // by storing the bytes as they are.
    if (plain_bytes > static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) return false;

    const int source_bytes = static_cast<int>(plain_bytes);
    const int bound = LZ4_compressBound(source_bytes);
    if (bound <= 0) return false;
    out->resize(static_cast<size_t>(bound));
    const int produced =
        LZ4_compress_default(static_cast<const char*>(plain),
                             reinterpret_cast<char*>(out->data()),
                             source_bytes, bound);
    if (produced <= 0 || static_cast<size_t>(produced) >= plain_bytes) {
        out->clear();
        return false;   // "not smaller" is a normal answer, not a failure
    }
    out->resize(static_cast<size_t>(produced));
    return true;
}

Status lz4_decode(const uint8_t* stored, uint64_t stored_bytes,
                  uint64_t plain_bytes, uint8_t* out) {
    // Both lengths come from the section directory, which is file content and
    // therefore untrusted. LZ4 takes them as `int`, so a value that does not fit
    // would be truncated into a smaller — and plausible — size: the decoder
    // would then write within a capacity it was never given. Reject rather than
    // narrow.
    if (stored_bytes > static_cast<uint64_t>(INT_MAX)
            || plain_bytes > static_cast<uint64_t>(INT_MAX)) {
        char message[192];
        std::snprintf(message, sizeof(message),
                      "lz4 section declares %llu stored / %llu plain bytes, past "
                      "the codec's 2GB block ceiling",
                      static_cast<unsigned long long>(stored_bytes),
                      static_cast<unsigned long long>(plain_bytes));
        return fail(Code::kMalformed, message);
    }
    // An LZ4 block never decodes to nothing: the writer only emits this encoding
    // for a body it compressed, and a zero-length one was never a candidate.
    if (plain_bytes == 0)
        return fail(Code::kMalformed, "lz4 section declares zero plain bytes");

    const int produced =
        LZ4_decompress_safe(reinterpret_cast<const char*>(stored),
                            reinterpret_cast<char*>(out),
                            static_cast<int>(stored_bytes),
                            static_cast<int>(plain_bytes));
    if (produced < 0) {
        char message[192];
        std::snprintf(message, sizeof(message),
                      "lz4 section failed to decode (error %d)", produced);
        return fail(Code::kMalformed, message);
    }
    // A block that decodes SHORT is as wrong as one that overruns: the
    // directory decides the section's shape, and the tail of the destination
    // buffer would otherwise keep whatever was there before.
    if (static_cast<uint64_t>(produced) != plain_bytes) {
        char message[192];
        std::snprintf(message, sizeof(message),
                      "lz4 section decodes to %d bytes but the directory "
                      "declares %llu", produced,
                      static_cast<unsigned long long>(plain_bytes));
        return fail(Code::kMalformed, message);
    }
    return Status::ok();
}

}  // namespace skene
