#include "avro_container.hpp"

#include <cstring>
#include <memory>
#include <stdexcept>

#include "avro_varint.hpp"
#include "compression/stream_decompress.hpp"  // crc32_update
#include "miniz_tinfl.h"
#include "snappy.h"
#include "zstd.h"

namespace rugo::avro {

namespace {

[[noreturn]] void corrupt(const std::string& msg) { throw std::runtime_error("Avro container: " + msg); }

// Strict UTF-8 (RFC 3629): no overlongs, no surrogates, nothing past U+10FFFF — the
// same rule Python's decoder applies, so a key that passes here decodes there.
bool utf8_strict(const std::string& s) {
    const auto* p = reinterpret_cast<const uint8_t*>(s.data());
    const uint8_t* end = p + s.size();
    while (p < end) {
        const uint8_t b = *p;
        if (b < 0x80) { ++p; continue; }
        size_t n;
        uint32_t cp, min;
        if ((b & 0xE0) == 0xC0) { n = 2; cp = b & 0x1F; min = 0x80; }
        else if ((b & 0xF0) == 0xE0) { n = 3; cp = b & 0x0F; min = 0x800; }
        else if ((b & 0xF8) == 0xF0) { n = 4; cp = b & 0x07; min = 0x10000; }
        else return false;
        if (static_cast<size_t>(end - p) < n) return false;
        for (size_t i = 1; i < n; ++i) {
            if ((p[i] & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (p[i] & 0x3F);
        }
        if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) return false;
        p += n;
    }
    return true;
}

// A header value for an error message: printable ASCII as is, every other byte \xNN.
std::string printable(const std::string& s) {
    static const char* hex = "0123456789abcdef";
    std::string o;
    for (unsigned char ch : s) {
        if (ch >= 0x20 && ch < 0x7F && ch != '\\') { o.push_back(static_cast<char>(ch)); continue; }
        o += "\\x";
        o.push_back(hex[ch >> 4]);
        o.push_back(hex[ch & 15]);
    }
    return o;
}

std::string read_string(Cursor& c) {
    const uint64_t n = read_length(c);
    std::string s(reinterpret_cast<const char*>(c.p), static_cast<size_t>(n));
    c.p += n;
    return s;
}

}  // namespace

Header read_header(const uint8_t* data, size_t size) {
    static constexpr uint8_t kMagic[4] = {'O', 'b', 'j', 1};
    if (size < 4 || std::memcmp(data, kMagic, 4) != 0) corrupt("not an Avro object container file (bad magic)");
    Header h;
    Cursor c{data + 4, data + size};
    bool have_schema = false;
    std::string codec = "null";
    for (;;) {
        int64_t count = read_long(c);
        if (count == 0) break;
        if (count < 0) {
            count = -count;
            read_long(c);  // the block's byte size; not needed
        }
        for (int64_t i = 0; i < count; ++i) {
            std::string k = read_string(c);
            std::string v = read_string(c);
            if (!utf8_strict(k)) corrupt("a metadata key is not valid UTF-8");
            if (k == "avro.schema") { h.schema_json = v; have_schema = true; }
            else if (k == "avro.codec") codec = v;
            h.metadata.emplace_back(std::move(k), std::move(v));
        }
    }
    if (!have_schema) corrupt("the header has no 'avro.schema'");
    if (!utf8_strict(h.schema_json)) corrupt("'avro.schema' is not valid UTF-8");
    if (codec == "null") h.codec = BlockCodec::Null;
    else if (codec == "deflate") h.codec = BlockCodec::Deflate;
    else if (codec == "snappy") h.codec = BlockCodec::Snappy;
    else if (codec == "zstandard") h.codec = BlockCodec::Zstandard;
    else corrupt("codec '" + printable(codec) + "' is not supported (null, deflate, snappy and zstandard are)");
    need(c, 16);
    std::memcpy(h.sync, c.p, 16);
    c.p += 16;
    h.first_block = static_cast<size_t>(c.p - data);
    return h;
}

BlockReader::BlockReader(const uint8_t* data, size_t size, const Header& h)
    : data_(data), size_(size), pos_(h.first_block), sync_(h.sync) {}

bool BlockReader::next(Block& out) {
    if (pos_ == size_) return false;
    Cursor c{data_ + pos_, data_ + size_};
    const int64_t count = read_long(c);
    const int64_t bytes = read_long(c);
    if (count < 0 || bytes < 0) corrupt("block " + std::to_string(index_) + " has a negative count or size");
    if (static_cast<uint64_t>(c.end - c.p) < static_cast<uint64_t>(bytes) + 16)
        corrupt("block " + std::to_string(index_) + " runs past the end of the file");
    out.count = count;
    out.bytes = c.p;
    out.size = static_cast<size_t>(bytes);
    c.p += bytes;
    if (std::memcmp(c.p, sync_, 16) != 0)
        corrupt("block " + std::to_string(index_) + " is not followed by the file's sync marker");
    c.p += 16;
    pos_ = static_cast<size_t>(c.p - data_);
    ++index_;
    return true;
}

namespace {

void inflate_raw(const uint8_t* src, size_t n, draken::AppendBuffer<uint8_t>& out) {
    tinfl_decompressor d;
    tinfl_init(&d);
    out.clear();
    size_t in_pos = 0;
    size_t cap = n * 4 > 4096 ? n * 4 : 4096;
    out.reserve(cap);
    for (;;) {
        size_t in_avail = n - in_pos;
        size_t out_avail = out.capacity() - out.size();
        const tinfl_status st = tinfl_decompress(
            &d, src + in_pos, &in_avail, out.data(), out.data() + out.size(), &out_avail,
            TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF);
        in_pos += in_avail;
        out.resize_uninit(out.size() + out_avail);
        if (st == TINFL_STATUS_DONE) break;
        if (st != TINFL_STATUS_HAS_MORE_OUTPUT) corrupt("deflate: the compressed block is corrupt");
        out.reserve(out.capacity() * 2);
    }
    // D10 (a): fastavro and PyIceberg write `zlib.compress(data)[2:-1]` — the raw
    // stream followed by 3 leftover bytes of the zlib adler32. Up to the 4 bytes of
    // that trailer are accepted after the end-of-stream marker; more is corrupt.
    if (n - in_pos > 4) corrupt("deflate: trailing bytes after the compressed block");
}

void unzstd(const uint8_t* src, size_t n, draken::AppendBuffer<uint8_t>& out) {
    std::unique_ptr<ZSTD_DCtx, size_t (*)(ZSTD_DCtx*)> dctx(ZSTD_createDCtx(), ZSTD_freeDCtx);
    if (!dctx) throw std::bad_alloc();
    out.clear();
    const unsigned long long known = ZSTD_getFrameContentSize(src, n);
    size_t cap = (known != ZSTD_CONTENTSIZE_UNKNOWN && known != ZSTD_CONTENTSIZE_ERROR)
                     ? static_cast<size_t>(known) : (n * 4 > 4096 ? n * 4 : 4096);
    out.reserve(cap ? cap : 1);
    ZSTD_inBuffer in{src, n, 0};
    size_t last = 0;
    for (;;) {
        ZSTD_outBuffer ob{out.data(), out.capacity(), out.size()};
        last = ZSTD_decompressStream(dctx.get(), &ob, &in);
        if (ZSTD_isError(last)) corrupt(std::string("zstandard: ") + ZSTD_getErrorName(last));
        out.resize_uninit(ob.pos);
        if (in.pos == in.size && last == 0) break;
        if (in.pos == in.size && ob.pos < ob.size) corrupt("zstandard: the compressed block is truncated");
        if (ob.pos == ob.size) out.reserve(out.capacity() * 2);
    }
}

void unsnappy(const uint8_t* src, size_t n, draken::AppendBuffer<uint8_t>& out) {
    if (n < 4) corrupt("snappy: the block is shorter than its CRC trailer");
    const size_t body = n - 4;
    size_t len = 0;
    if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(src), body, &len))
        corrupt("snappy: the compressed block is corrupt");
    out.clear();
    out.resize_uninit(len);
    if (!snappy::RawUncompress(reinterpret_cast<const char*>(src), body, reinterpret_cast<char*>(out.data())))
        corrupt("snappy: the compressed block is corrupt");
    const uint8_t* t = src + body;
    const uint32_t want = (uint32_t(t[0]) << 24) | (uint32_t(t[1]) << 16) | (uint32_t(t[2]) << 8) | t[3];
    if (rugo::compression::crc32_update(0, out.data(), len) != want)
        corrupt("snappy: CRC32 mismatch (the block is corrupt)");
}

}  // namespace

std::pair<const uint8_t*, size_t> block_payload(const Block& b, BlockCodec codec,
                                                draken::AppendBuffer<uint8_t>& scratch) {
    switch (codec) {
        case BlockCodec::Null:      return {b.bytes, b.size};
        case BlockCodec::Deflate:   inflate_raw(b.bytes, b.size, scratch); break;
        case BlockCodec::Snappy:    unsnappy(b.bytes, b.size, scratch); break;
        case BlockCodec::Zstandard: unzstd(b.bytes, b.size, scratch); break;
    }
    return {scratch.data(), scratch.size()};
}

}  // namespace rugo::avro
