#pragma once
// rugo/src/orc/orc_rle.hpp — ORC's run-length encodings, decoded from a fully
// decompressed stream held in memory (docs/ORC_READER_DESIGN.md §4.1):
//
//   ByteRle   — runs of one byte, or literal bytes                  (BYTE, boolean bits)
//   BoolRle   — ByteRle over bits packed MSB-first                   (PRESENT, BOOLEAN)
//   IntRle    — RLE v1 (DIRECT / DICTIONARY) and v2 (DIRECT_V2 / DICTIONARY_V2); v2 has
//               four sub-encodings: Short Repeat, Direct, Patched Base, Delta
//
// Each decoder is a pull reader: read(out, count) yields exactly `count` values or
// throws. A stream that ends early, a run that over-reads its bytes, or a header that
// cannot be valid throws std::runtime_error; nothing here reads past the buffer.

#include <cstdint>
#include <cstring>
#include <string>

#include "orc_proto.hpp"

namespace rugo::orc {

// ── byte RLE ────────────────────────────────────────────────────────────────

class ByteRle {
public:
    ByteRle(const uint8_t* p, size_t n) : p_(p), end_(p + n) {}

    void read(uint8_t* out, size_t count) {
        while (count > 0) {
            if (left_ == 0) refill();
            const size_t take = left_ < count ? left_ : count;
            if (repeat_) {
                std::memset(out, value_, take);
            } else {
                std::memcpy(out, p_, take);
                p_ += take;
            }
            left_ -= take;
            out += take;
            count -= take;
        }
    }

    // Pass over the next `count` values.
    void skip(size_t count) {
        while (count > 0) {
            if (left_ == 0) refill();
            const size_t take = left_ < count ? left_ : count;
            if (!repeat_) p_ += take;
            left_ -= take;
            count -= take;
        }
    }

private:
    void refill() {
        if (p_ == end_) fail("a byte run-length stream ends before its values do");
        const int8_t header = static_cast<int8_t>(*p_++);
        if (header >= 0) {
            if (p_ == end_) fail("a byte run-length run is missing its value");
            repeat_ = true;
            value_ = *p_++;
            left_ = static_cast<size_t>(header) + 3;
        } else {
            repeat_ = false;
            left_ = static_cast<size_t>(-static_cast<int>(header));
            if (left_ > static_cast<size_t>(end_ - p_)) fail("a byte run-length literal runs past its stream");
        }
    }
    const uint8_t* p_;
    const uint8_t* end_;
    size_t left_ = 0;
    bool repeat_ = false;
    uint8_t value_ = 0;
};

// ── boolean RLE ─────────────────────────────────────────────────────────────

// A bit-packed (MSB first) byte RLE stream, read in pieces: each read continues at the
// bit after the last one read, so a stripe's PRESENT or BOOLEAN stream can be decoded a
// row range at a time.
class BitReader {
public:
    BitReader(const uint8_t* p, size_t n) : bytes_(p, n) {}

    // The next `count` bits as an LSB-first bitmap at `out` ((count + 7) / 8 bytes are
    // written; the unused high bits of the last are cleared). Returns the number set.
    size_t read_bitmap(size_t count, uint8_t* out) {
        if (left_ != 0) return read_bitmap_unaligned(count, out);
        const size_t full = count / 8;
        const unsigned tail = static_cast<unsigned>(count % 8);
        bytes_.read(out, full + (tail != 0));
        size_t nonnull = 0;
        size_t at = 0;
        for (; full - at >= sizeof(uint64_t); at += sizeof(uint64_t)) {
            uint64_t bits;
            std::memcpy(&bits, out + at, sizeof(bits));
            nonnull += static_cast<size_t>(__builtin_popcountll(bits));
            bits = ((bits & 0x5555555555555555ULL) << 1) | ((bits >> 1) & 0x5555555555555555ULL);
            bits = ((bits & 0x3333333333333333ULL) << 2) | ((bits >> 2) & 0x3333333333333333ULL);
            bits = ((bits & 0x0f0f0f0f0f0f0f0fULL) << 4) | ((bits >> 4) & 0x0f0f0f0f0f0f0f0fULL);
            std::memcpy(out + at, &bits, sizeof(bits));
        }
        for (; at < full + (tail != 0); ++at) {
            const unsigned raw = out[at];
            unsigned bits = raw;
            bits = ((bits & 0x55) << 1) | ((bits >> 1) & 0x55);
            bits = ((bits & 0x33) << 2) | ((bits >> 2) & 0x33);
            bits = ((bits & 0x0f) << 4) | ((bits >> 4) & 0x0f);
            if (at == full && tail != 0) {
                bits &= (1u << tail) - 1;
                // The byte's low 8 - tail bits belong to the next read.
                cur_ = static_cast<uint8_t>(raw << tail);
                left_ = 8 - tail;
            }
            out[at] = static_cast<uint8_t>(bits);
            nonnull += static_cast<size_t>(__builtin_popcount(bits));
        }
        return nonnull;
    }

    // The next `count` bits, one byte each (0/1).
    void read_bools(size_t count, uint8_t* out) {
        for (size_t i = 0; i < count; ++i) out[i] = next_bit();
    }

    // Position at a row index entry's place: `run_bytes` bytes into the current byte run, then
    // `bits` bits into the next byte. Only on a fresh reader.
    void seek(uint64_t run_bytes, uint64_t bits) {
        if (bits > 7) fail("a row index bit offset is larger than 7 (the file is corrupt)");
        bytes_.skip(static_cast<size_t>(run_bytes));
        if (bits != 0) {
            bytes_.read(&cur_, 1);
            cur_ = static_cast<uint8_t>(cur_ << bits);
            left_ = 8 - static_cast<unsigned>(bits);
        }
    }

private:
    uint8_t next_bit() {
        if (left_ == 0) {
            bytes_.read(&cur_, 1);
            left_ = 8;
        }
        const uint8_t bit = cur_ >> 7;
        cur_ = static_cast<uint8_t>(cur_ << 1);
        left_ -= 1;
        return bit;
    }

    size_t read_bitmap_unaligned(size_t count, uint8_t* out) {
        std::memset(out, 0, (count + 7) / 8);
        size_t nonnull = 0;
        for (size_t i = 0; i < count; ++i) {
            const uint8_t bit = next_bit();
            out[i >> 3] |= static_cast<uint8_t>(bit << (i & 7));
            nonnull += bit;
        }
        return nonnull;
    }

    ByteRle bytes_;
    uint8_t cur_ = 0;      // the current byte's unread bits, at the top
    unsigned left_ = 0;    // how many
};

// `count` booleans from a bit-packed (MSB first) byte RLE stream, one byte each (0/1).
inline void read_bools(const uint8_t* p, size_t n, size_t count, uint8_t* out) {
    BitReader(p, n).read_bools(count, out);
}

// Decode ORC's MSB-first PRESENT bits into an LSB-first validity bitmap.
// The unused high bits of the final byte are cleared and excluded from the count.
inline size_t read_present(const uint8_t* p, size_t n, size_t count, uint8_t* out) {
    return BitReader(p, n).read_bitmap(count, out);
}

// ── integer RLE ─────────────────────────────────────────────────────────────

namespace detail {

// Width of an RLE v2 encoded-width code (the spec's 5-bit table).
inline unsigned decode_width(unsigned code) {
    if (code <= 23) return code + 1;
    switch (code) {
        case 24: return 26;
        case 25: return 28;
        case 26: return 30;
        case 27: return 32;
        case 28: return 40;
        case 29: return 48;
        case 30: return 56;
        default: return 64;
    }
}

// The fixed bit width a patch entry is stored at (the spec's closestFixedBits).
inline unsigned closest_fixed_bits(unsigned n) {
    if (n == 0) return 1;
    if (n <= 24) return n;
    if (n <= 26) return 26;
    if (n <= 28) return 28;
    if (n <= 30) return 30;
    if (n <= 32) return 32;
    if (n <= 40) return 40;
    if (n <= 48) return 48;
    if (n <= 56) return 56;
    return 64;
}

// Read exactly N bytes, even at the end of a packed run. The shifts make this
// independent of host byte order and permit unaligned inputs.
template <unsigned N>
inline uint64_t load_be(const uint8_t* p) {
    static_assert(N >= 1 && N <= 8, "packed values must fit in a 64-bit load");
    uint64_t v = 0;
    for (unsigned i = 0; i < N; ++i) v = (v << 8) | p[i];
    return v;
}

template <unsigned Width, unsigned I>
inline void unpack_one(const uint8_t* src, uint64_t* out) {
    constexpr unsigned bit = I * Width;
    constexpr unsigned offset = bit % 8;
    constexpr unsigned bytes = (offset + Width + 7) / 8;
    constexpr unsigned shift = bytes * 8 - offset - Width;
    constexpr uint64_t mask = ~uint64_t{0} >> (64 - Width);
    out[I] = (load_be<bytes>(src + bit / 8) >> shift) & mask;
}

template <unsigned Width>
inline void unpack_width(const uint8_t* src, size_t count, uint64_t* out) {
    while (count >= 8) {
        unpack_one<Width, 0>(src, out);
        unpack_one<Width, 1>(src, out);
        unpack_one<Width, 2>(src, out);
        unpack_one<Width, 3>(src, out);
        unpack_one<Width, 4>(src, out);
        unpack_one<Width, 5>(src, out);
        unpack_one<Width, 6>(src, out);
        unpack_one<Width, 7>(src, out);
        src += Width;
        out += 8;
        count -= 8;
    }
    // The final group can contain fewer than eight values and must not read
    // beyond its byte-aligned payload.
    uint64_t acc = 0;
    unsigned have = 0;
    for (size_t i = 0; i < count; ++i) {
        uint64_t v = 0;
        unsigned need = Width;
        while (need > 0) {
            if (have == 0) {
                acc = *src++;
                have = 8;
            }
            const unsigned take = need < have ? need : have;
            v = (v << take) | ((acc >> (have - take)) & ((uint64_t{1} << take) - 1));
            have -= take;
            need -= take;
        }
        out[i] = v;
    }
}

}  // namespace detail

class IntRle {
public:
    IntRle(const uint8_t* p, size_t n, bool v2, bool is_signed)
        : p_(p), end_(p + n), v2_(v2), signed_(is_signed) {}

    void read(int64_t* out, size_t count) {
        read_runs(count, [&](const int64_t* values, size_t n) {
            std::memcpy(out, values, n * sizeof(int64_t));
            out += n;
        });
    }

    // Pass over the next `count` values.
    void skip(size_t count) {
        read_runs(count, [](const int64_t*, size_t) {});
    }

    // Values remain valid only for the duration of the callback; a later run
    // reuses the decoder's bounded scratch buffer.
    template <typename Consumer>
    void read_runs(size_t count, Consumer&& consume) {
        while (count > 0) {
            if (pos_ == len_) refill();
            const size_t take = (len_ - pos_) < count ? (len_ - pos_) : count;
            consume(buf_ + pos_, take);
            pos_ += take;
            count -= take;
        }
    }

private:
    static constexpr size_t kMaxRun = 512;

    void refill() {
        if (p_ == end_) fail("an integer run-length stream ends before its values do");
        pos_ = len_ = 0;
        if (v2_) next_v2(); else next_v1();
    }

    uint64_t varint() {
        uint64_t v = 0;
        for (unsigned shift = 0; shift < 70; shift += 7) {
            if (p_ == end_) fail("an integer run-length varint runs past its stream");
            const uint8_t b = *p_++;
            v |= static_cast<uint64_t>(b & 0x7f) << shift;
            if ((b & 0x80) == 0) return v;
        }
        fail("an integer run-length varint is longer than 10 bytes");
    }

    int64_t read_value() {
        const uint64_t v = varint();
        return signed_ ? unzigzag(v) : static_cast<int64_t>(v);
    }

    // ── v1 ──
    void next_v1() {
        const int8_t header = static_cast<int8_t>(*p_++);
        if (header >= 0) {
            const size_t run = static_cast<size_t>(header) + 3;
            if (p_ == end_) fail("an integer run-length run is missing its delta");
            const int8_t delta = static_cast<int8_t>(*p_++);
            int64_t v = read_value();
            for (size_t i = 0; i < run; ++i) {
                buf_[i] = v;
                v = static_cast<int64_t>(static_cast<uint64_t>(v) + static_cast<uint64_t>(static_cast<int64_t>(delta)));
            }
            len_ = run;
        } else {
            const size_t lit = static_cast<size_t>(-static_cast<int>(header));
            for (size_t i = 0; i < lit; ++i) buf_[i] = read_value();
            len_ = lit;
        }
    }

    // ── v2 ──

    // `count` big-endian packed values of `width` bits each, byte-aligned at the end.
    void unpack(size_t count, unsigned width, uint64_t* out) {
        const size_t total_bits = count * width;
        const size_t bytes = (total_bits + 7) / 8;
        if (bytes > static_cast<size_t>(end_ - p_)) fail("an integer run-length run extends past its stream");
        switch (width) {
#define ORC_UNPACK_WIDTH(w) case w: detail::unpack_width<w>(p_, count, out); break
            ORC_UNPACK_WIDTH(1);  ORC_UNPACK_WIDTH(2);  ORC_UNPACK_WIDTH(3);  ORC_UNPACK_WIDTH(4);
            ORC_UNPACK_WIDTH(5);  ORC_UNPACK_WIDTH(6);  ORC_UNPACK_WIDTH(7);  ORC_UNPACK_WIDTH(8);
            ORC_UNPACK_WIDTH(9);  ORC_UNPACK_WIDTH(10); ORC_UNPACK_WIDTH(11); ORC_UNPACK_WIDTH(12);
            ORC_UNPACK_WIDTH(13); ORC_UNPACK_WIDTH(14); ORC_UNPACK_WIDTH(15); ORC_UNPACK_WIDTH(16);
            ORC_UNPACK_WIDTH(17); ORC_UNPACK_WIDTH(18); ORC_UNPACK_WIDTH(19); ORC_UNPACK_WIDTH(20);
            ORC_UNPACK_WIDTH(21); ORC_UNPACK_WIDTH(22); ORC_UNPACK_WIDTH(23); ORC_UNPACK_WIDTH(24);
            ORC_UNPACK_WIDTH(26); ORC_UNPACK_WIDTH(28); ORC_UNPACK_WIDTH(30); ORC_UNPACK_WIDTH(32);
            ORC_UNPACK_WIDTH(40); ORC_UNPACK_WIDTH(48); ORC_UNPACK_WIDTH(56); ORC_UNPACK_WIDTH(64);
#undef ORC_UNPACK_WIDTH
            default: fail("an integer run-length run has an invalid bit width");
        }
        p_ += bytes;
    }

    void next_v2() {
        const uint8_t b0 = *p_++;
        const unsigned enc = b0 >> 6;
        switch (enc) {
            case 0: short_repeat(b0); break;
            case 1: direct(b0); break;
            case 2: patched_base(b0); break;
            default: delta(b0); break;
        }
    }

    uint8_t byte() {
        if (p_ == end_) fail("an integer run-length header runs past its stream");
        return *p_++;
    }

    void short_repeat(uint8_t b0) {
        const unsigned width = ((b0 >> 3) & 7) + 1;
        const size_t count = (b0 & 7) + 3;
        if (width > static_cast<size_t>(end_ - p_)) fail("a short-repeat run extends past its stream");
        uint64_t v = 0;
        for (unsigned i = 0; i < width; ++i) v = (v << 8) | *p_++;
        const int64_t value = signed_ ? unzigzag(v) : static_cast<int64_t>(v);
        for (size_t i = 0; i < count; ++i) buf_[i] = value;
        len_ = count;
    }

    void direct(uint8_t b0) {
        const unsigned width = detail::decode_width((b0 >> 1) & 0x1f);
        const size_t count = ((static_cast<size_t>(b0 & 1) << 8) | byte()) + 1;
        uint64_t tmp[kMaxRun];
        unpack(count, width, tmp);
        for (size_t i = 0; i < count; ++i)
            buf_[i] = signed_ ? unzigzag(tmp[i]) : static_cast<int64_t>(tmp[i]);
        len_ = count;
    }

    void patched_base(uint8_t b0) {
        const unsigned width = detail::decode_width((b0 >> 1) & 0x1f);
        const size_t count = ((static_cast<size_t>(b0 & 1) << 8) | byte()) + 1;
        const uint8_t b2 = byte();
        const uint8_t b3 = byte();
        const unsigned base_bytes = (b2 >> 5) + 1;
        const unsigned patch_width = detail::decode_width(b2 & 0x1f);
        const unsigned gap_width = (b3 >> 5) + 1;
        const size_t patch_count = b3 & 0x1f;

        if (base_bytes > static_cast<size_t>(end_ - p_)) fail("a patched-base run extends past its stream");
        uint64_t base_raw = 0;
        for (unsigned i = 0; i < base_bytes; ++i) base_raw = (base_raw << 8) | *p_++;
        const uint64_t sign_bit = uint64_t{1} << (base_bytes * 8 - 1);
        int64_t base;
        if (base_raw & sign_bit) base = -static_cast<int64_t>(base_raw & ~sign_bit);
        else base = static_cast<int64_t>(base_raw);

        uint64_t data[kMaxRun];
        unpack(count, width, data);

        const unsigned entry_width = detail::closest_fixed_bits(patch_width + gap_width);
        uint64_t patches[32];
        unpack(patch_count, entry_width, patches);

        const uint64_t patch_mask = patch_width >= 64 ? ~uint64_t{0} : ((uint64_t{1} << patch_width) - 1);
        size_t patch_at = 0;
        size_t next_index = 0;
        uint64_t next_patch = 0;
        bool have_patch = false;
        // Position the first patch: a gap of 255 with a zero patch only advances the index.
        auto load_patch = [&]() {
            size_t gap = 0;
            while (patch_at < patch_count) {
                const uint64_t entry = patches[patch_at++];
                const uint64_t g = patch_width >= 64 ? 0 : (entry >> patch_width);
                const uint64_t pv = entry & patch_mask;
                gap += static_cast<size_t>(g);
                if (g == 255 && pv == 0) continue;
                next_index += gap;
                next_patch = pv;
                have_patch = true;
                return;
            }
            have_patch = false;
        };
        load_patch();
        for (size_t i = 0; i < count; ++i) {
            uint64_t v = data[i];
            if (have_patch && i == next_index) {
                v |= next_patch << width;
                load_patch();
            }
            buf_[i] = static_cast<int64_t>(static_cast<uint64_t>(base) + v);
        }
        len_ = count;
    }

    void delta(uint8_t b0) {
        const unsigned width_code = (b0 >> 1) & 0x1f;
        const unsigned width = width_code == 0 ? 0 : detail::decode_width(width_code);
        const size_t count = ((static_cast<size_t>(b0 & 1) << 8) | byte()) + 1;
        const uint64_t base_raw = varint();
        const int64_t base = signed_ ? unzigzag(base_raw) : static_cast<int64_t>(base_raw);
        const int64_t delta_base = unzigzag(varint());
        buf_[0] = base;
        if (count == 1) {
            len_ = 1;
            return;
        }
        if (width == 0) {
            int64_t v = base;
            for (size_t i = 1; i < count; ++i) {
                v = static_cast<int64_t>(static_cast<uint64_t>(v) + static_cast<uint64_t>(delta_base));
                buf_[i] = v;
            }
        } else {
            int64_t v = static_cast<int64_t>(static_cast<uint64_t>(base) + static_cast<uint64_t>(delta_base));
            buf_[1] = v;
            uint64_t deltas[kMaxRun];
            unpack(count - 2, width, deltas);
            for (size_t i = 2; i < count; ++i) {
                const uint64_t d = deltas[i - 2];
                v = delta_base < 0 ? static_cast<int64_t>(static_cast<uint64_t>(v) - d)
                                   : static_cast<int64_t>(static_cast<uint64_t>(v) + d);
                buf_[i] = v;
            }
        }
        len_ = count;
    }

    const uint8_t* p_;
    const uint8_t* end_;
    bool v2_;
    bool signed_;
    int64_t buf_[kMaxRun];
    size_t pos_ = 0;
    size_t len_ = 0;
};

}  // namespace rugo::orc
