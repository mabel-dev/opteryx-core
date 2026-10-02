#pragma once
// rugo/src/compression/stream_decompress.hpp — transparent, STREAMING decompression
// of whole-file compressed text inputs (JSONL, CSV).
//
// Header-only and pure C++: compiled into rugo_native (Python: bind-time schema
// inference, READ_CSV) and into opteryx.operators._operators (NativeJsonlScanSource),
// each with its own copy — the same arrangement as rugo's JSONL core.
//
// Codecs:
//   gzip  (.gz)          — miniz tinfl raw inflate; multi-member streams; CRC32 and
//                          ISIZE verified per member.
//   zstd  (.zst / .zstd) — ZSTD_decompressStream; multi-frame and skippable frames;
//                          the frame's content checksum (when present) is verified.
//   lz4   (.lz4)         — the LZ4 FRAME format (what the lz4 CLI writes); linked and
//                          independent blocks; content checksum (XXH32) verified.
// Recognised but NOT supported — always a clear error, never parsed as text:
//   bzip2, xz, zip, snappy-framed.
//
// Detection: magic bytes are authoritative. An extension that claims a codec the
// bytes do not carry is an error (a truncated/mislabelled file), never a fallback
// to plain text. Nothing here is ever "best effort": a corrupt or truncated stream
// throws std::runtime_error.
//
// Memory: decoders read from a caller-owned compressed buffer and write into
// caller-owned output; their own state is bounded (zstd window, 32KB..1MB inflate
// ring, one lz4 block + 64KB history). LineChunker cuts the decompressed stream into
// newline-aligned chunks using EXACTLY the rule the uncompressed JSONL path uses —
// the first '\n' at or after `target` bytes — so the bind-time schema chunk and the
// scan's first chunk hold the same bytes.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

#include "zstd.h"
#include "miniz_tinfl.h"
#include "lz4.h"

#if defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

namespace rugo::compression {

enum class Codec : uint8_t { NONE, GZIP, ZSTD, LZ4, BZIP2, XZ, ZIP, SNAPPY };

inline const char* codec_name(Codec c) {
    switch (c) {
        case Codec::NONE:   return "none";
        case Codec::GZIP:   return "gzip";
        case Codec::ZSTD:   return "zstd";
        case Codec::LZ4:    return "lz4";
        case Codec::BZIP2:  return "bzip2";
        case Codec::XZ:     return "xz";
        case Codec::ZIP:    return "zip";
        case Codec::SNAPPY: return "snappy";
    }
    return "unknown";
}

inline bool codec_supported(Codec c) {
    return c == Codec::NONE || c == Codec::GZIP || c == Codec::ZSTD || c == Codec::LZ4;
}

inline uint32_t read_le32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

// The codec the leading bytes carry, or NONE. Every signature here is impossible as
// the start of JSON text, and implausible as the start of CSV (each opens with a
// control byte or an invalid UTF-8 sequence; bzip2 additionally needs its block
// magic, so a CSV header that happens to start "BZh" is not mistaken for one).
inline Codec detect_magic(const uint8_t* p, size_t n) {
    if (n >= 2 && p[0] == 0x1F && p[1] == 0x8B) return Codec::GZIP;
    if (n >= 4) {
        const uint32_t m = read_le32(p);
        if (m == 0xFD2FB528u) return Codec::ZSTD;
        if ((m & 0xFFFFFFF0u) == 0x184D2A50u) return Codec::ZSTD;   // skippable frame
        if (m == 0x184D2204u) return Codec::LZ4;
        if (m == 0x04034B50u) return Codec::ZIP;                     // "PK\3\4"
    }
    if (n >= 10 && p[0] == 'B' && p[1] == 'Z' && p[2] == 'h' && p[3] >= '1' && p[3] <= '9' &&
        p[4] == 0x31 && p[5] == 0x41 && p[6] == 0x59 && p[7] == 0x26 && p[8] == 0x53 &&
        p[9] == 0x59)
        return Codec::BZIP2;
    if (n >= 6 && std::memcmp(p, "\xFD" "7zXZ\x00", 6) == 0) return Codec::XZ;
    if (n >= 10 && std::memcmp(p, "\xFF\x06\x00\x00sNaPpY", 10) == 0) return Codec::SNAPPY;
    return Codec::NONE;
}

// The codec a path's final extension claims, or NONE.
inline Codec codec_from_extension(const std::string& path) {
    const size_t dot = path.find_last_of('.');
    const size_t slash = path.find_last_of("/\\");
    if (dot == std::string::npos || (slash != std::string::npos && dot < slash)) return Codec::NONE;
    std::string ext = path.substr(dot + 1);
    for (char& ch : ext) ch = static_cast<char>((ch >= 'A' && ch <= 'Z') ? ch + 32 : ch);
    if (ext == "gz" || ext == "gzip") return Codec::GZIP;
    if (ext == "zst" || ext == "zstd") return Codec::ZSTD;
    if (ext == "lz4") return Codec::LZ4;
    if (ext == "bz2" || ext == "bzip2") return Codec::BZIP2;
    if (ext == "xz") return Codec::XZ;
    if (ext == "zip") return Codec::ZIP;
    if (ext == "sz" || ext == "snappy") return Codec::SNAPPY;
    return Codec::NONE;
}

// The codec to decode `path`'s bytes with. Throws, naming the file and the codec,
// for an unsupported codec, or for an extension that claims a codec the bytes do
// not carry. A zero-byte file is plain (an empty relation) whatever its extension.
inline Codec resolve_codec(const std::string& path, const uint8_t* p, size_t n) {
    if (n == 0) return Codec::NONE;
    const Codec magic = detect_magic(p, n);
    const Codec ext = codec_from_extension(path);
    const Codec codec = magic != Codec::NONE ? magic : ext;
    const std::string what = path.empty() ? std::string("the input") : "'" + path + "'";
    if (!codec_supported(codec)) {
        throw std::runtime_error(
            what + " is " + codec_name(codec) +
            "-compressed, which is not supported; supported compression is gzip (.gz), "
            "zstd (.zst) and lz4 (.lz4). Decompress the file first.");
    }
    if (ext != Codec::NONE && magic == Codec::NONE) {
        throw std::runtime_error(
            what + " has a " + codec_name(ext) + " file extension but does not "
            "start with " + codec_name(ext) + " data; the file is corrupt or mislabelled.");
    }
    return codec;
}

// ---------------------------------------------------------------------------
// CRC-32 (IEEE, the gzip polynomial). ARMv8 CRC32 instructions where the target has
// them (Apple silicon does; baseline armv8-a does not), else slicing-by-8.

namespace detail {

struct Crc32Tables {
    uint32_t t[8][256];
    Crc32Tables() {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            t[0][i] = c;
        }
        for (uint32_t i = 0; i < 256; ++i)
            for (int s = 1; s < 8; ++s) t[s][i] = (t[s - 1][i] >> 8) ^ t[0][t[s - 1][i] & 0xFF];
    }
};

inline const Crc32Tables& crc_tables() {
    static const Crc32Tables tables;
    return tables;
}

}  // namespace detail

inline uint32_t crc32_update(uint32_t crc, const uint8_t* p, size_t n) {
    crc = ~crc;
#if defined(__ARM_FEATURE_CRC32)
    while (n >= 8) {
        uint64_t v;
        std::memcpy(&v, p, 8);
        crc = __crc32d(crc, v);
        p += 8;
        n -= 8;
    }
    while (n--) crc = __crc32b(crc, *p++);
#else
    const auto& T = detail::crc_tables().t;
    while (n >= 8) {
        const uint32_t a = read_le32(p) ^ crc;
        const uint32_t b = read_le32(p + 4);
        crc = T[7][a & 0xFF] ^ T[6][(a >> 8) & 0xFF] ^ T[5][(a >> 16) & 0xFF] ^ T[4][a >> 24] ^
              T[3][b & 0xFF] ^ T[2][(b >> 8) & 0xFF] ^ T[1][(b >> 16) & 0xFF] ^ T[0][b >> 24];
        p += 8;
        n -= 8;
    }
    while (n--) crc = T[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);
#endif
    return ~crc;
}

// XXH32, for the lz4 frame's checksums. Streaming state.
class Xxh32 {
public:
    explicit Xxh32(uint32_t seed = 0) {
        v_[0] = seed + P1 + P2;
        v_[1] = seed + P2;
        v_[2] = seed;
        v_[3] = seed - P1;
        seed_ = seed;
    }
    void update(const uint8_t* p, size_t n) {
        total_ += n;
        if (buf_len_ + n < 16) {
            std::memcpy(buf_ + buf_len_, p, n);
            buf_len_ += n;
            return;
        }
        if (buf_len_) {
            const size_t fill = 16 - buf_len_;
            std::memcpy(buf_ + buf_len_, p, fill);
            round4(buf_);
            p += fill;
            n -= fill;
            buf_len_ = 0;
        }
        while (n >= 16) {
            round4(p);
            p += 16;
            n -= 16;
        }
        std::memcpy(buf_, p, n);
        buf_len_ = n;
    }
    uint32_t digest() const {
        uint32_t h = total_ >= 16 ? rotl(v_[0], 1) + rotl(v_[1], 7) + rotl(v_[2], 12) + rotl(v_[3], 18)
                                  : seed_ + P5;
        h += static_cast<uint32_t>(total_);
        const uint8_t* p = buf_;
        size_t n = buf_len_;
        while (n >= 4) {
            h += read_le32(p) * P3;
            h = rotl(h, 17) * P4;
            p += 4;
            n -= 4;
        }
        while (n--) {
            h += (*p++) * P5;
            h = rotl(h, 11) * P1;
        }
        h ^= h >> 15;
        h *= P2;
        h ^= h >> 13;
        h *= P3;
        h ^= h >> 16;
        return h;
    }
    static uint32_t of(const uint8_t* p, size_t n, uint32_t seed = 0) {
        Xxh32 x(seed);
        x.update(p, n);
        return x.digest();
    }

private:
    static constexpr uint32_t P1 = 2654435761u, P2 = 2246822519u, P3 = 3266489917u,
                              P4 = 668265263u, P5 = 374761393u;
    static uint32_t rotl(uint32_t x, int r) { return (x << r) | (x >> (32 - r)); }
    static uint32_t round(uint32_t acc, uint32_t in) { return rotl(acc + in * P2, 13) * P1; }
    void round4(const uint8_t* p) {
        v_[0] = round(v_[0], read_le32(p));
        v_[1] = round(v_[1], read_le32(p + 4));
        v_[2] = round(v_[2], read_le32(p + 8));
        v_[3] = round(v_[3], read_le32(p + 12));
    }
    uint32_t v_[4];
    uint32_t seed_;
    uint64_t total_ = 0;
    uint8_t buf_[16];
    size_t buf_len_ = 0;
};

// ---------------------------------------------------------------------------
// Decoders. `read` writes up to `cap` decompressed bytes into `dst` and returns the
// count; 0 means the stream is complete (and has been fully validated). Corrupt or
// truncated input throws std::runtime_error.

class StreamDecoder {
public:
    virtual ~StreamDecoder() = default;
    virtual size_t read(uint8_t* dst, size_t cap) = 0;
};

class ZstdDecoder final : public StreamDecoder {
public:
    ZstdDecoder(const uint8_t* src, size_t len) : in_{src, len, 0} {
        dctx_ = ZSTD_createDCtx();
        if (dctx_ == nullptr) throw std::runtime_error("zstd: out of memory");
    }
    ~ZstdDecoder() override { ZSTD_freeDCtx(dctx_); }

    size_t read(uint8_t* dst, size_t cap) override {
        ZSTD_outBuffer out{dst, cap, 0};
        while (out.pos < out.size) {
            if (in_.pos >= in_.size) {
                // Input exhausted: complete only if the last frame ended cleanly.
                if (last_ != 0) throw std::runtime_error("zstd: the compressed data is truncated");
                break;
            }
            const size_t before = out.pos;
            const size_t before_in = in_.pos;
            last_ = ZSTD_decompressStream(dctx_, &out, &in_);
            if (ZSTD_isError(last_))
                throw std::runtime_error(std::string("zstd: ") + ZSTD_getErrorName(last_));
            if (out.pos == before && in_.pos == before_in && last_ != 0) {
                // No progress with input left and output room: a frame the
                // decoder cannot advance (should not happen with valid input).
                throw std::runtime_error("zstd: the compressed data is corrupt");
            }
        }
        return out.pos;
    }

private:
    ZSTD_inBuffer in_;
    ZSTD_DCtx* dctx_ = nullptr;
    size_t last_ = 0;   // 0 = at a frame boundary
};

class GzipDecoder final : public StreamDecoder {
public:
    GzipDecoder(const uint8_t* src, size_t len)
        : src_(src), len_(len), ring_(new uint8_t[kRing]) {
        begin_member();
    }

    size_t read(uint8_t* dst, size_t cap) override {
        size_t written = 0;
        while (written < cap) {
            if (pend_len_ > 0) {
                const size_t n = std::min(pend_len_, cap - written);
                std::memcpy(dst + written, ring_.get() + pend_off_, n);
                pend_off_ += n;
                pend_len_ -= n;
                written += n;
                continue;
            }
            if (done_) break;
            if (member_done_) {
                finish_member();
                continue;
            }
            // tinfl's wrapping mode requires (next - start) + avail to be the ring's
            // power-of-two size, so it always gets the whole remainder of the ring and
            // whatever does not fit `dst` is served from the ring on the next pass.
            size_t in_avail = len_ - pos_;
            size_t out_avail = kRing - ring_pos_;
            uint8_t* out_next = ring_.get() + ring_pos_;
            const tinfl_status st = tinfl_decompress(&inflator_, src_ + pos_, &in_avail,
                                                     ring_.get(), out_next, &out_avail, 0);
            pos_ += in_avail;
            if (out_avail) {
                crc_ = crc32_update(crc_, out_next, out_avail);
                isize_ += static_cast<uint32_t>(out_avail);
                pend_off_ = ring_pos_;
                pend_len_ = out_avail;
                ring_pos_ = (ring_pos_ + out_avail) & (kRing - 1);
            }
            if (st == TINFL_STATUS_DONE) {
                member_done_ = true;
            } else if (st == TINFL_STATUS_FAILED_CANNOT_MAKE_PROGRESS ||
                       st == TINFL_STATUS_NEEDS_MORE_INPUT) {
                throw std::runtime_error("gzip: the compressed data is truncated");
            } else if (st < TINFL_STATUS_DONE) {
                throw std::runtime_error("gzip: the compressed data is corrupt (invalid deflate stream)");
            }
        }
        return written;
    }

private:
    static constexpr size_t kRing = 1u << 20;   // power of two >= TINFL_LZ_DICT_SIZE

    void need(size_t n) const {
        if (len_ - pos_ < n) throw std::runtime_error("gzip: the compressed data is truncated");
    }

    void begin_member() {
        need(10);
        const uint8_t* h = src_ + pos_;
        if (h[0] != 0x1F || h[1] != 0x8B)
            throw std::runtime_error("gzip: not a gzip member (bad magic)");
        if (h[2] != 8) throw std::runtime_error("gzip: unsupported compression method");
        const uint8_t flg = h[3];
        if (flg & 0xE0) throw std::runtime_error("gzip: reserved header flags set (corrupt header)");
        pos_ += 10;
        if (flg & 0x04) {   // FEXTRA
            need(2);
            const size_t xlen = static_cast<size_t>(src_[pos_]) | (static_cast<size_t>(src_[pos_ + 1]) << 8);
            pos_ += 2;
            need(xlen);
            pos_ += xlen;
        }
        for (const uint8_t bit : {uint8_t(0x08), uint8_t(0x10)}) {   // FNAME, FCOMMENT
            if (flg & bit) {
                const void* z = std::memchr(src_ + pos_, 0, len_ - pos_);
                if (z == nullptr) throw std::runtime_error("gzip: the compressed data is truncated");
                pos_ = static_cast<size_t>(static_cast<const uint8_t*>(z) - src_) + 1;
            }
        }
        if (flg & 0x02) {   // FHCRC
            need(2);
            pos_ += 2;
        }
        tinfl_init(&inflator_);
        crc_ = 0;
        isize_ = 0;
        ring_pos_ = 0;
        member_done_ = false;
    }

    void finish_member() {
        need(8);
        const uint32_t crc = read_le32(src_ + pos_);
        const uint32_t isize = read_le32(src_ + pos_ + 4);
        pos_ += 8;
        if (crc != crc_) throw std::runtime_error("gzip: CRC32 mismatch (the compressed data is corrupt)");
        if (isize != isize_) throw std::runtime_error("gzip: length mismatch (the compressed data is corrupt)");
        // Another member follows, or only zero padding (which gzip(1) tolerates).
        if (pos_ < len_) {
            if (src_[pos_] == 0x1F) {
                begin_member();
                return;
            }
            for (size_t i = pos_; i < len_; ++i)
                if (src_[i] != 0)
                    throw std::runtime_error("gzip: unexpected trailing data after the last member");
        }
        done_ = true;
    }

    const uint8_t* src_;
    size_t len_;
    size_t pos_ = 0;
    std::unique_ptr<uint8_t[]> ring_;
    size_t ring_pos_ = 0;
    size_t pend_off_ = 0;     // decoded bytes in the ring not yet handed out
    size_t pend_len_ = 0;
    tinfl_decompressor inflator_;
    uint32_t crc_ = 0;
    uint32_t isize_ = 0;
    bool member_done_ = false;
    bool done_ = false;
};

class Lz4FrameDecoder final : public StreamDecoder {
public:
    Lz4FrameDecoder(const uint8_t* src, size_t len) : src_(src), len_(len) { next_frame(); }

    size_t read(uint8_t* dst, size_t cap) override {
        size_t written = 0;
        while (written < cap) {
            if (pend_len_ > 0) {
                const size_t n = std::min(pend_len_, cap - written);
                std::memcpy(dst + written, pend_, n);
                pend_ += n;
                pend_len_ -= n;
                written += n;
                continue;
            }
            if (done_) break;
            next_block();
        }
        return written;
    }

private:
    static constexpr size_t kHist = 64 * 1024;

    void need(size_t n) const {
        if (len_ - pos_ < n) throw std::runtime_error("lz4: the compressed data is truncated");
    }

    // Parse frame headers, skipping skippable frames. Sets done_ at clean EOF.
    void next_frame() {
        for (;;) {
            if (pos_ == len_) {
                done_ = true;
                return;
            }
            need(4);
            const uint32_t magic = read_le32(src_ + pos_);
            if ((magic & 0xFFFFFFF0u) == 0x184D2A50u) {
                need(8);
                const uint32_t size = read_le32(src_ + pos_ + 4);
                pos_ += 8;
                need(size);
                pos_ += size;
                continue;
            }
            if (magic != 0x184D2204u)
                throw std::runtime_error("lz4: not an lz4 frame (bad magic; legacy lz4 format is not supported)");
            pos_ += 4;
            need(2);
            const uint8_t flg = src_[pos_];
            const uint8_t bd = src_[pos_ + 1];
            if ((flg >> 6) != 1) throw std::runtime_error("lz4: unsupported frame version");
            independent_ = (flg >> 5) & 1;
            block_checksum_ = (flg >> 4) & 1;
            const bool content_size = (flg >> 3) & 1;
            content_checksum_ = (flg >> 2) & 1;
            const bool dict_id = flg & 1;
            const int bsid = (bd >> 4) & 7;
            if (bsid < 4) throw std::runtime_error("lz4: invalid block size in frame header");
            block_max_ = size_t(1) << (8 + 2 * bsid);
            const size_t hdr = 2 + (content_size ? 8 : 0) + (dict_id ? 4 : 0);
            need(hdr + 1);
            const uint8_t hc = static_cast<uint8_t>((Xxh32::of(src_ + pos_, hdr) >> 8) & 0xFF);
            if (hc != src_[pos_ + hdr]) throw std::runtime_error("lz4: frame header checksum mismatch (corrupt)");
            if (dict_id) throw std::runtime_error("lz4: frames that need an external dictionary are not supported");
            pos_ += hdr + 1;
            buf_.reset(new uint8_t[kHist + block_max_]);
            hist_len_ = 0;
            last_total_ = 0;
            content_hash_ = Xxh32(0);
            return;
        }
    }

    void next_block() {
        // Linked blocks reference up to 64KB of prior output: keep the tail of what
        // the previous block left at the buffer's front.
        if (independent_) {
            hist_len_ = 0;
        } else if (last_total_ > 0) {
            const size_t keep = std::min(last_total_, kHist);
            std::memmove(buf_.get(), buf_.get() + last_total_ - keep, keep);
            hist_len_ = keep;
        }
        last_total_ = 0;
        need(4);
        const uint32_t word = read_le32(src_ + pos_);
        pos_ += 4;
        if (word == 0) {   // EndMark
            if (content_checksum_) {
                need(4);
                if (read_le32(src_ + pos_) != content_hash_.digest())
                    throw std::runtime_error("lz4: content checksum mismatch (the compressed data is corrupt)");
                pos_ += 4;
            }
            next_frame();
            return;
        }
        const bool raw = word & 0x80000000u;
        const size_t size = word & 0x7FFFFFFFu;
        if (size > block_max_) throw std::runtime_error("lz4: block larger than the frame's block size (corrupt)");
        need(size + (block_checksum_ ? 4 : 0));
        const uint8_t* block = src_ + pos_;
        if (block_checksum_ && Xxh32::of(block, size) != read_le32(block + size))
            throw std::runtime_error("lz4: block checksum mismatch (the compressed data is corrupt)");
        uint8_t* out = buf_.get() + hist_len_;
        int produced;
        if (raw) {
            std::memcpy(out, block, size);
            produced = static_cast<int>(size);
        } else if (independent_ || hist_len_ == 0) {
            produced = LZ4_decompress_safe(reinterpret_cast<const char*>(block),
                                           reinterpret_cast<char*>(out), static_cast<int>(size),
                                           static_cast<int>(block_max_));
        } else {
            produced = LZ4_decompress_safe_usingDict(
                reinterpret_cast<const char*>(block), reinterpret_cast<char*>(out),
                static_cast<int>(size), static_cast<int>(block_max_),
                reinterpret_cast<const char*>(buf_.get()), static_cast<int>(hist_len_));
        }
        if (produced < 0) throw std::runtime_error("lz4: the compressed data is corrupt");
        pos_ += size + (block_checksum_ ? 4 : 0);
        if (content_checksum_) content_hash_.update(out, static_cast<size_t>(produced));
        // Hand this block out from the buffer; the history slide for the NEXT linked
        // block happens once these bytes are drained (start of next_block).
        pend_ = out;
        pend_len_ = static_cast<size_t>(produced);
        last_total_ = hist_len_ + static_cast<size_t>(produced);
    }

    const uint8_t* src_;
    size_t len_;
    size_t pos_ = 0;
    bool independent_ = true, block_checksum_ = false, content_checksum_ = false;
    size_t block_max_ = 0;
    std::unique_ptr<uint8_t[]> buf_;
    size_t hist_len_ = 0;
    Xxh32 content_hash_{0};
    const uint8_t* pend_ = nullptr;
    size_t pend_len_ = 0;
    size_t last_total_ = 0;   // history + last block's bytes, for the next slide
    bool done_ = false;
};

inline std::unique_ptr<StreamDecoder> make_decoder(Codec codec, const uint8_t* src, size_t len) {
    switch (codec) {
        case Codec::GZIP: return std::make_unique<GzipDecoder>(src, len);
        case Codec::ZSTD: return std::make_unique<ZstdDecoder>(src, len);
        case Codec::LZ4:  return std::make_unique<Lz4FrameDecoder>(src, len);
        default: break;
    }
    throw std::runtime_error(std::string("no decoder for ") + codec_name(codec));
}

// ---------------------------------------------------------------------------
// An owned, uninitialised byte buffer (no zero-fill on growth: chunks are ~128MB).

struct ByteBuffer {
    std::unique_ptr<uint8_t[]> data;
    size_t len = 0;
    size_t cap = 0;

    void reserve(size_t want) {
        if (want <= cap) return;
        std::unique_ptr<uint8_t[]> grown(new uint8_t[want]);
        if (len) std::memcpy(grown.get(), data.get(), len);
        data = std::move(grown);
        cap = want;
    }
};

// Cuts a decoder's output into newline-aligned chunks: each chunk is at least
// `target` bytes and ends at the first '\n' at or after offset `target` (inclusive),
// or at end of stream. Identical to the uncompressed path's cut rule.
class LineChunker {
public:
    explicit LineChunker(std::unique_ptr<StreamDecoder> decoder) : dec_(std::move(decoder)) {}

    // The next chunk into `out` (replacing its contents). False at end of stream.
    bool next(size_t target, ByteBuffer& out) {
        if (target == 0) target = 1;
        out.len = 0;
        out.reserve(std::max(target + kStep, carry_.len));
        if (carry_.len) {
            std::memcpy(out.data.get(), carry_.data.get(), carry_.len);
            out.len = carry_.len;
            carry_.len = 0;
        }
        size_t scan = target;
        for (;;) {
            if (out.len > scan) {
                const void* nl = std::memchr(out.data.get() + scan, '\n', out.len - scan);
                if (nl != nullptr) {
                    const size_t end = static_cast<size_t>(static_cast<const uint8_t*>(nl) - out.data.get()) + 1;
                    const size_t rest = out.len - end;
                    if (rest) {
                        carry_.reserve(rest);
                        std::memcpy(carry_.data.get(), out.data.get() + end, rest);
                        carry_.len = rest;
                    }
                    out.len = end;
                    return true;
                }
                scan = out.len;
            }
            if (eof_) return out.len > 0;
            if (out.cap - out.len < kStep / 4) out.reserve(out.cap + std::max(out.cap / 2, kStep));
            const size_t got = dec_->read(out.data.get() + out.len, out.cap - out.len);
            if (got == 0) eof_ = true;
            out.len += got;
        }
    }

private:
    static constexpr size_t kStep = 4u << 20;
    std::unique_ptr<StreamDecoder> dec_;
    ByteBuffer carry_;
    bool eof_ = false;
};

}  // namespace rugo::compression
