#include "orc_reader.hpp"

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>

#include "base64/_base64.h"        // mabel bintob64 (exported by draken_native)
#include "core/alloc.h"
#include "core/append_buffer.h"
#include "core/string_slot.h"
#include "interop/value_format.hpp"  // the shared JSON value rendering (parquet, avro, orc)
#include "libdeflate.h"
#include "lz4.h"
#include "ops/float_ops.h"           // fp_canon: -0.0 / NaN canon, as the other renderers
#include "orc_rle.hpp"
#include "snappy.h"
#include "zstd.h"

namespace rugo::orc {

namespace {

using rugo::avro::OutKind;

constexpr size_t kTailRead = 16 * 1024;
constexpr size_t kDefaultBlock = 256 * 1024;
// Seconds from 1970-01-01 to ORC's 2015-01-01 timestamp epoch.
constexpr int64_t kOrcEpochOffset = 1420070400;

constexpr size_t pad8(size_t bytes) { return (bytes + 7) & ~size_t(7); }

bool is_utc_name(const std::string& zone) {
    static const char* names[] = {"UTC", "GMT", "Etc/UTC", "Etc/GMT", "Etc/Universal", "Universal",
                                  "Zulu", "Etc/Zulu", "Etc/UCT", "UCT", "Z"};
    for (const char* n : names)
        if (zone == n) return true;
    return false;
}

// A stream's decompressed bytes: a view into the file when uncompressed, else owned.
struct Bytes {
    const uint8_t* p = nullptr;
    size_t n = 0;
    std::vector<uint8_t> owned;
};

// One decompressed chunk appended to `out` (at most `limit` bytes).
void inflate_chunk(Compression c, const uint8_t* src, size_t n, size_t limit, std::vector<uint8_t>& out,
                   libdeflate_decompressor*& deflater) {
    const size_t base = out.size();
    out.resize(base + limit);
    uint8_t* dst = out.data() + base;
    size_t produced = 0;
    switch (c) {
        case Compression::Zlib: {
            if (deflater == nullptr) {
                deflater = libdeflate_alloc_decompressor();
                if (deflater == nullptr) throw std::bad_alloc();
            }
            const libdeflate_result r = libdeflate_deflate_decompress(deflater, src, n, dst, limit, &produced);
            if (r != LIBDEFLATE_SUCCESS) fail("a zlib chunk is corrupt or larger than the compression block size");
            break;
        }
        case Compression::Snappy: {
            size_t len = 0;
            if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(src), n, &len))
                fail("a snappy chunk is corrupt");
            if (len > limit) fail("a snappy chunk is larger than the compression block size");
            if (!snappy::RawUncompress(reinterpret_cast<const char*>(src), n, reinterpret_cast<char*>(dst)))
                fail("a snappy chunk is corrupt");
            produced = len;
            break;
        }
        case Compression::Lz4: {
            const int got = LZ4_decompress_safe(reinterpret_cast<const char*>(src), reinterpret_cast<char*>(dst),
                                                static_cast<int>(n), static_cast<int>(limit));
            if (got < 0) fail("an lz4 chunk is corrupt or larger than the compression block size");
            produced = static_cast<size_t>(got);
            break;
        }
        case Compression::Zstd: {
            const size_t got = ZSTD_decompress(dst, limit, src, n);
            if (ZSTD_isError(got)) fail(std::string("a zstd chunk is corrupt: ") + ZSTD_getErrorName(got));
            produced = got;
            break;
        }
        default:
            fail(std::string("compression ") + compression_name(c) + " is not supported");
    }
    out.resize(base + produced);
}

// A decompressed stream (design doc §3.2): `n` bytes at `p` are a sequence of chunks,
// each led by a 3-byte little-endian header `length * 2 + isOriginal`.
Bytes decompress(Compression c, size_t block_size, const uint8_t* p, size_t n) {
    Bytes out;
    if (c == Compression::None) {
        out.p = p;
        out.n = n;
        return out;
    }
    if (c == Compression::Lzo) fail("compression lzo is not supported");
    const size_t limit = block_size != 0 ? block_size : kDefaultBlock;
    libdeflate_decompressor* deflater = nullptr;
    try {
        size_t at = 0;
        while (at < n) {
            if (n - at < 3) fail("a compression chunk header runs past its stream");
            const uint32_t header = p[at] | (static_cast<uint32_t>(p[at + 1]) << 8) | (static_cast<uint32_t>(p[at + 2]) << 16);
            at += 3;
            const size_t len = header >> 1;
            if (len > n - at) fail("a compression chunk extends past its stream");
            if (header & 1) {
                if (len > limit) fail("an uncompressed chunk is larger than the compression block size");
                out.owned.insert(out.owned.end(), p + at, p + at + len);
            } else {
                inflate_chunk(c, p + at, len, limit, out.owned, deflater);
            }
            at += len;
        }
    } catch (...) {
        if (deflater != nullptr) libdeflate_free_decompressor(deflater);
        throw;
    }
    if (deflater != nullptr) libdeflate_free_decompressor(deflater);
    out.p = out.owned.data();
    out.n = out.owned.size();
    return out;
}

size_t width_of(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: return 1;
        case DRAKEN_INT16: return 2;
        case DRAKEN_INT32: case DRAKEN_FLOAT32: case DRAKEN_DATE32: return 4;
        case DRAKEN_INT64: case DRAKEN_FLOAT64: case DRAKEN_TIMESTAMP64: case DRAKEN_TIME64: case DRAKEN_DECIMAL: return 8;
        case DRAKEN_DECIMAL128: case DRAKEN_UUID: return 16;
        default: return 0;
    }
}

bool is_int_type(DrakenType t) {
    return t == DRAKEN_INT8 || t == DRAKEN_INT16 || t == DRAKEN_INT32 || t == DRAKEN_INT64;
}

const char* type_name(DrakenType t) {
    switch (t) {
        case DRAKEN_BOOL: return "BOOLEAN";
        case DRAKEN_INT8: return "INT8";
        case DRAKEN_INT16: return "INT16";
        case DRAKEN_INT32: return "INT32";
        case DRAKEN_INT64: return "INT64";
        case DRAKEN_FLOAT32: return "FLOAT32";
        case DRAKEN_FLOAT64: return "FLOAT64";
        case DRAKEN_VARCHAR: return "VARCHAR";
        case DRAKEN_VARBINARY: return "VARBINARY";
        case DRAKEN_DATE32: return "DATE";
        case DRAKEN_TIMESTAMP64: return "TIMESTAMP";
        case DRAKEN_TIME64: return "TIME";
        case DRAKEN_UUID: return "UUID";
        case DRAKEN_DECIMAL: case DRAKEN_DECIMAL128: return "DECIMAL";
        case DRAKEN_NVARCHAR: return "NVARCHAR";
        case DRAKEN_ARRAY: return "ARRAY";
        default: return "another type";
    }
}

// Describe one primitive column (design doc §3.3).
FileColumn describe_leaf(const Type& t, const std::string& name, uint32_t type_id) {
    FileColumn fc;
    fc.name = name;
    fc.kind = t.kind;
    fc.kind_name = kind_name(t.kind);
    fc.type_id = type_id;
    if (const std::string* id = t.attribute("iceberg.id")) {
        try {
            fc.iceberg_id = std::stoll(*id);
        } catch (const std::exception&) {
            fc.refusal = "its iceberg.id attribute '" + *id + "' is not an integer";
            return fc;
        }
    }
    const std::string* unit = t.attribute("iceberg.timestamp-unit");
    const std::string* binary = t.attribute("iceberg.binary-type");
    const std::string* long_type = t.attribute("iceberg.long-type");
    switch (t.kind) {
        case Kind::Boolean: fc.type = DRAKEN_BOOL; break;
        case Kind::Byte: fc.type = DRAKEN_INT8; break;
        case Kind::Short: fc.type = DRAKEN_INT16; break;
        case Kind::Int: fc.type = DRAKEN_INT32; break;
        case Kind::Long:
            fc.type = DRAKEN_INT64;
            if (long_type != nullptr) {
                if (*long_type == "TIME") fc.type = DRAKEN_TIME64;
                else fc.refusal = "its iceberg.long-type '" + *long_type + "' is not supported";
            }
            break;
        case Kind::Float: fc.type = DRAKEN_FLOAT32; break;
        case Kind::Double: fc.type = DRAKEN_FLOAT64; break;
        case Kind::String: case Kind::Varchar: case Kind::Char: fc.type = DRAKEN_VARCHAR; break;
        case Kind::Binary:
            fc.type = DRAKEN_VARBINARY;
            if (binary != nullptr) {
                if (*binary == "UUID") {
                    fc.type = DRAKEN_UUID;   // 16 bytes, network order; every value is checked at decode
                } else if (*binary == "FIXED") {
                    const std::string* length = t.attribute("iceberg.length");
                    try {
                        if (length == nullptr) throw std::invalid_argument("absent");
                        const long long n = std::stoll(*length);
                        if (n <= 0 || n > UINT32_MAX) throw std::invalid_argument("range");
                        fc.fixed_length = static_cast<uint32_t>(n);
                    } catch (const std::exception&) {
                        fc.refusal = "its iceberg.binary-type FIXED has no valid iceberg.length";
                    }
                } else {
                    fc.refusal = "its iceberg.binary-type '" + *binary + "' is not supported";
                }
            }
            break;
        case Kind::Date: fc.type = DRAKEN_DATE32; break;
        case Kind::Timestamp: case Kind::TimestampInstant:
            fc.type = DRAKEN_TIMESTAMP64;
            if (unit != nullptr && *unit != "MICROS")
                fc.refusal = "its iceberg.timestamp-unit '" + *unit + "' is not supported (microseconds only)";
            break;
        case Kind::Decimal:
            if (t.precision == 0 || t.precision > 38)
                fc.refusal = "its decimal precision " + std::to_string(t.precision) + " is outside 1..38";
            fc.type = t.precision > 18 ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
            fc.precision = static_cast<uint8_t>(t.precision);
            fc.scale = static_cast<uint8_t>(t.scale);
            break;
        default:
            fc.refusal = std::string(kind_name(t.kind)) + " columns are not supported";
    }
    fc.readable = fc.refusal.empty();
    return fc;
}

constexpr unsigned kMaxNesting = 64;

// Why a nested subtree cannot be read; empty when it can. A child's type id must come after its
// parent's (ORC numbers types in pre-order), which also rules out a cyclic, corrupt file.
std::string subtree_refusal(const std::vector<Type>& types, uint32_t id, unsigned depth) {
    if (depth > kMaxNesting) return "it is nested more than " + std::to_string(kMaxNesting) + " levels deep";
    const Type& t = types[id];
    auto child = [&](uint32_t c) -> std::string {
        if (c >= types.size() || c <= id) fail("a column refers to a type that does not exist (the file is corrupt)");
        return subtree_refusal(types, c, depth + 1);
    };
    switch (t.kind) {
        case Kind::Struct: {
            if (t.subtypes.size() != t.field_names.size()) fail("a struct's field names and types differ in number");
            for (uint32_t c : t.subtypes) {
                std::string why = child(c);
                if (!why.empty()) return why;
            }
            return std::string();
        }
        case Kind::List:
            if (t.subtypes.size() != 1) fail("a list does not have exactly one element type (the file is corrupt)");
            return child(t.subtypes[0]);
        case Kind::Map: {
            if (t.subtypes.size() != 2) fail("a map does not have exactly a key and a value type (the file is corrupt)");
            if (t.subtypes[0] >= types.size()) fail("a column refers to a type that does not exist (the file is corrupt)");
            const Kind key = types[t.subtypes[0]].kind;
            if (key != Kind::String && key != Kind::Varchar && key != Kind::Char)
                return std::string("its MAP has ") + kind_name(key) + " keys, and a JSON object's keys are strings";
            for (uint32_t c : t.subtypes) {
                std::string why = child(c);
                if (!why.empty()) return why;
            }
            return std::string();
        }
        case Kind::Union:
            return "UNION columns are not supported";
        default: {
            const FileColumn leaf = describe_leaf(t, std::string(), id);
            return leaf.readable ? std::string() : leaf.refusal;
        }
    }
}

// Describe a top-level column of the file (design doc §3.3; docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1).
FileColumn describe(const std::vector<Type>& types, const std::string& name, uint32_t type_id) {
    const Type& t = types[type_id];
    if (t.kind != Kind::Struct && t.kind != Kind::List && t.kind != Kind::Map) return describe_leaf(t, name, type_id);
    FileColumn fc;
    fc.name = name;
    fc.kind = t.kind;
    fc.kind_name = kind_name(t.kind);
    fc.type_id = type_id;
    fc.nested = true;
    if (const std::string* id = t.attribute("iceberg.id")) {
        try {
            fc.iceberg_id = std::stoll(*id);
        } catch (const std::exception&) {
            fc.refusal = "its iceberg.id attribute '" + *id + "' is not an integer";
            return fc;
        }
    }
    fc.refusal = subtree_refusal(types, type_id, 0);
    fc.type = DRAKEN_NVARCHAR;
    if (fc.refusal.empty() && t.kind == Kind::List) {
        // A LIST of a plain scalar (or of lists of one) is an ARRAY; every other nested column is one
        // JSON value.
        uint32_t bottom = type_id;
        uint32_t depth = 0;
        while (types[bottom].kind == Kind::List) {
            bottom = types[bottom].subtypes[0];
            ++depth;
        }
        const FileColumn elem = describe_leaf(types[bottom], std::string(), bottom);
        switch (elem.type) {
            case DRAKEN_BOOL: case DRAKEN_INT32: case DRAKEN_INT64: case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
            case DRAKEN_VARCHAR: case DRAKEN_VARBINARY:
                if (elem.fixed_length == 0 && elem.readable) {
                    fc.type = DRAKEN_ARRAY;
                    fc.child_type = elem.type;
                    fc.array_depth = static_cast<uint8_t>(depth);
                }
                break;
            default: break;
        }
    }
    fc.readable = fc.refusal.empty();
    return fc;
}

// Why a file column cannot be read as the reader's type; empty when it can.
std::string compatibility(const FileColumn& f, const ReaderColumn& r) {
    const DrakenType ft = f.type;
    const DrakenType rt = r.type;
    bool ok = false;
    switch (ft) {
        case DRAKEN_BOOL: ok = rt == DRAKEN_BOOL; break;
        case DRAKEN_INT8: ok = is_int_type(rt); break;
        case DRAKEN_INT16: ok = rt == DRAKEN_INT16 || rt == DRAKEN_INT32 || rt == DRAKEN_INT64; break;
        case DRAKEN_INT32: ok = rt == DRAKEN_INT32 || rt == DRAKEN_INT64; break;
        case DRAKEN_INT64: ok = rt == DRAKEN_INT64; break;
        case DRAKEN_FLOAT32: ok = rt == DRAKEN_FLOAT32 || rt == DRAKEN_FLOAT64; break;
        case DRAKEN_FLOAT64: ok = rt == DRAKEN_FLOAT64; break;
        case DRAKEN_VARCHAR: ok = rt == DRAKEN_VARCHAR; break;
        case DRAKEN_VARBINARY: ok = rt == DRAKEN_VARBINARY; break;
        case DRAKEN_UUID: ok = rt == DRAKEN_UUID; break;
        case DRAKEN_DATE32: ok = rt == DRAKEN_DATE32; break;
        case DRAKEN_TIMESTAMP64: ok = rt == DRAKEN_TIMESTAMP64; break;
        case DRAKEN_TIME64: ok = rt == DRAKEN_TIME64; break;
        case DRAKEN_NVARCHAR: ok = rt == DRAKEN_NVARCHAR; break;
        case DRAKEN_ARRAY: ok = rt == DRAKEN_ARRAY && r.child_type == f.child_type && r.array_depth == f.array_depth; break;
        case DRAKEN_DECIMAL: case DRAKEN_DECIMAL128:
            if (rt == DRAKEN_DECIMAL || rt == DRAKEN_DECIMAL128) {
                if (r.scale != f.scale)
                    return "the file's decimal scale " + std::to_string(f.scale) + " cannot be read as scale " +
                           std::to_string(r.scale);
                if (r.precision < f.precision)
                    return "the file's decimal precision " + std::to_string(f.precision) +
                           " cannot be read as precision " + std::to_string(r.precision);
                ok = true;
            }
            break;
        default: break;
    }
    if (ok) return std::string();
    return std::string("the file's ") + kind_name(f.kind) + " cannot be read as " + type_name(rt);
}

struct ColPlan {
    ReaderColumn rc;
    int file_col = -1;   // index into file_columns; -1 = absent from the file (NULL constant)
};

// ── decoding helpers ────────────────────────────────────────────────────────

// Validity bitmap (LSB first, bit set = valid) and the flags it came from.
struct Presence {
    std::vector<uint8_t> flags;   // one byte per row; empty when every row is present
    size_t nonnull = 0;
    uint8_t* validity = nullptr;  // draken_malloc'd; null when there are no nulls
};

template <typename T>
void read_ints_into(IntRle& reader, T* out, size_t count, const uint8_t* flags) {
    if (flags == nullptr) {
        if constexpr (std::is_same_v<T, int64_t>) {
            reader.read(out, count);
        } else {
            reader.read_runs(count, [&](const int64_t* values, size_t n) {
                for (size_t i = 0; i < n; ++i) out[i] = static_cast<T>(values[i]);
                out += n;
            });
        }
    } else {
        size_t row = 0;
        reader.read_runs(count, [&](const int64_t* values, size_t n) {
            for (size_t i = 0; i < n; ++i) {
                while (!flags[row]) ++row;
                out[row++] = static_cast<T>(values[i]);
            }
        });
    }
}

template <typename T>
void read_bytes_into(ByteRle& reader, T* out, size_t count, const uint8_t* validity) {
    if constexpr (std::is_same_v<T, int8_t>) {
        if (validity == nullptr) {
            reader.read(reinterpret_cast<uint8_t*>(out), count);
            return;
        }
    }
    uint8_t values[512];
    size_t row = 0;
    size_t bitmap_byte = 0;
    unsigned bits = 0;
    while (count != 0) {
        const size_t n = std::min(count, sizeof(values));
        reader.read(values, n);
        if (validity == nullptr) {
            for (size_t i = 0; i < n; ++i) out[row++] = static_cast<int8_t>(values[i]);
        } else {
            for (size_t i = 0; i < n; ++i) {
                while (bits == 0) bits = validity[bitmap_byte++];
                const size_t at = (bitmap_byte - 1) * 8 + static_cast<unsigned>(__builtin_ctz(bits));
                out[at] = static_cast<int8_t>(values[i]);
                bits &= bits - 1;
            }
        }
        count -= n;
    }
}

int64_t timestamp_micros(int64_t seconds, int64_t nanos, const std::string& where) {
    const uint64_t raw = static_cast<uint64_t>(nanos);
    uint64_t value = raw >> 3;
    const unsigned zeros = static_cast<unsigned>(raw & 7);
    if (zeros != 0) {
        uint64_t scale = 1;
        for (unsigned z = 0; z <= zeros; ++z) scale *= 10;
        value *= scale;
    }
    if (value > 999999999) fail(where + "a timestamp's nanoseconds are out of range");
    int64_t epoch = seconds + kOrcEpochOffset;
    // Seconds are truncated toward zero; match the Java reader's pre-1970 adjustment.
    if (epoch < 0 && value > 999999) epoch -= 1;
    return epoch * 1000000 + static_cast<int64_t>(value / 1000);
}

uint8_t* alloc_bytes(size_t n) {
    auto* p = static_cast<uint8_t*>(draken_malloc(n ? n : 1));
    if (p == nullptr) throw std::bad_alloc();
    return p;
}

uint8_t* alloc_zero(size_t n) {
    auto* p = static_cast<uint8_t*>(draken_calloc(n ? n : 1, 1));
    if (p == nullptr) throw std::bad_alloc();
    return p;
}

uint32_t* zero_codes(uint32_t n) {
    auto* z = static_cast<uint32_t*>(draken_calloc(n ? n : 1, sizeof(uint32_t)));
    if (z == nullptr) throw std::bad_alloc();
    return z;
}

// A NULL constant column of `rows` rows (a reader column the file does not hold): one
// value, every position 0, every row NULL (CLAUDE.md §11).
void null_constant(const ReaderColumn& rc, uint32_t rows, Column& out) {
    out.type = rc.type;
    out.length = rows;
    out.precision = rc.precision;
    out.scale = rc.scale;
    out.validity = alloc_zero(pad8((static_cast<size_t>(rows) + 7) / 8));
    out.codes = zero_codes(rows);
    if (rc.type == DRAKEN_VARCHAR || rc.type == DRAKEN_VARBINARY) {
        out.kind = OutKind::ConstString;
        auto* slot = static_cast<DrakenStringSlot*>(draken_calloc(1, sizeof(DrakenStringSlot)));
        if (slot == nullptr) throw std::bad_alloc();
        out.slots = slot;
        out.arena = nullptr;
        out.arena_len = 0;
    } else {
        out.kind = OutKind::ConstRaw;
        out.data = alloc_zero(16);
    }
}

void build_slot(draken::AppendBuffer<DrakenStringSlot>& slots, draken::AppendBuffer<uint8_t>& arena, size_t at,
                const uint8_t* src, uint64_t len) {
    if (len <= STR_INLINE_MAX) {
        str_init_inline(&slots[at], src, static_cast<uint32_t>(len));
        return;
    }
    const size_t off = arena.size();
    if (off + len > UINT32_MAX) fail("a stripe's string bytes exceed 4 GiB");
    arena.append(src, static_cast<size_t>(len));
    draken_build_string_slot(&slots[at], src, static_cast<uint32_t>(len), static_cast<uint32_t>(off));
}

unsigned __int128 pow10_128(unsigned n) {
    unsigned __int128 v = 1;
    for (unsigned i = 0; i < n; ++i) v *= 10;
    return v;
}

__int128 read_varint128(const uint8_t*& p, const uint8_t* end) {
    unsigned __int128 v = 0;
    unsigned shift = 0;
    for (;;) {
        if (p == end) fail("a decimal value runs past its stream");
        if (shift > 126) fail("a decimal value is wider than 128 bits");
        const uint8_t b = *p++;
        v |= static_cast<unsigned __int128>(b & 0x7f) << shift;
        shift += 7;
        if ((b & 0x80) == 0) break;
    }
    return static_cast<__int128>(v >> 1) ^ -static_cast<__int128>(v & 1);
}

}  // namespace

// ── OrcFile ─────────────────────────────────────────────────────────────────

using StreamMap = std::map<std::pair<uint32_t, uint32_t>, std::pair<uint64_t, uint64_t>>;
struct NestedNode;
struct DeclCtx;

struct OrcFile::Impl {
    const uint8_t* data = nullptr;
    size_t size = 0;
    PostScript ps;
    Footer footer;
    std::vector<FileColumn> columns;
    std::vector<ColPlan> plan;
    std::vector<std::string> out_names;

    Bytes stream(uint64_t offset, uint64_t length) const {
        if (offset > size || length > size - offset) fail("a stream extends past the end of the file");
        return decompress(ps.compression, static_cast<size_t>(ps.block_size), data + offset, static_cast<size_t>(length));
    }

    void decode_column(const StripeFooter& sf, const StreamMap& streams, const ColPlan& cp, uint32_t rows,
                       Column& out) const;
    // One primitive column of `rows` entries, read as `rc`.
    void decode_leaf(const StripeFooter& sf, const StreamMap& streams, const FileColumn& fc,
                     const ReaderColumn& rc, uint32_t rows, Column& out) const;
    // A STRUCT / LIST / MAP column: its subtree decoded, then rendered (JSON text, or an ARRAY).
    void decode_nested(const StripeFooter& sf, const StreamMap& streams, const FileColumn& fc,
                       const ReaderColumn& rc, uint32_t rows, Column& out) const;
    NestedNode build_node(const StripeFooter& sf, const StreamMap& streams, uint32_t id, const std::string& name,
                          const std::string& key, size_t n, const DeclCtx* decl = nullptr, int di = -1) const;
};

OrcFile::OrcFile(const uint8_t* data, size_t size) : impl_(new Impl) {
    Impl& m = *impl_;
    m.data = data;
    m.size = size;
    if (size < 4 || std::memcmp(data, "ORC", 3) != 0) fail("the file does not start with the magic \"ORC\" (not an ORC file)");
    const size_t ps_len = data[size - 1];
    if (ps_len == 0 || ps_len + 1 > size) fail("the postscript length is out of range (the file is corrupt)");
    m.ps = parse_postscript({data + size - 1 - ps_len, ps_len});
    if (m.ps.compression == Compression::Lzo) fail("compression lzo is not supported");
    const uint64_t tail = 1 + ps_len + m.ps.footer_length;
    if (tail > size) fail("the footer extends before the start of the file (the file is corrupt)");
    const uint8_t* footer_at = data + size - tail;
    Bytes footer_bytes = decompress(m.ps.compression, static_cast<size_t>(m.ps.block_size), footer_at,
                                    static_cast<size_t>(m.ps.footer_length));
    m.footer = parse_footer({footer_bytes.p, footer_bytes.n});
    if (m.footer.encrypted) fail("the file is encrypted, which this reader does not support");
    if (m.footer.types.empty() || m.footer.types[0].kind != Kind::Struct)
        fail("the file's root type is not a struct (the file is corrupt)");
    const uint64_t content_end = size - tail - m.ps.metadata_length;
    uint64_t total_rows = 0;
    for (const StripeInfo& s : m.footer.stripes) {
        const uint64_t end = s.offset + s.index_length + s.data_length + s.footer_length;
        if (end < s.offset || end > content_end) fail("a stripe extends past the file's content (the file is corrupt)");
        if (s.rows > UINT32_MAX) fail("a stripe has more than 4 billion rows");
        total_rows += s.rows;
    }
    if (total_rows != m.footer.num_rows) fail("the stripes' row counts do not add up to the file's (the file is corrupt)");

    const Type& root = m.footer.types[0];
    if (root.subtypes.size() != root.field_names.size()) fail("the root struct's field names and types differ in number");
    for (size_t i = 0; i < root.subtypes.size(); ++i) {
        const uint32_t id = root.subtypes[i];
        if (id >= m.footer.types.size()) fail("a column refers to a type that does not exist (the file is corrupt)");
        m.columns.push_back(describe(m.footer.types, root.field_names[i], id));
    }
}

OrcFile::~OrcFile() = default;

uint64_t OrcFile::num_rows() const { return impl_->footer.num_rows; }
size_t OrcFile::stripe_count() const { return impl_->footer.stripes.size(); }
uint32_t OrcFile::stripe_rows(size_t stripe) const { return static_cast<uint32_t>(impl_->footer.stripes.at(stripe).rows); }
Compression OrcFile::compression() const { return impl_->ps.compression; }
std::string OrcFile::compression_name() const { return rugo::orc::compression_name(impl_->ps.compression); }
const std::vector<FileColumn>& OrcFile::file_columns() const { return impl_->columns; }

void OrcFile::bind_all() {
    std::vector<ReaderColumn> reader;
    std::vector<std::string> decode;
    for (const FileColumn& f : impl_->columns) {
        if (!f.readable) fail("column '" + f.name + "': " + f.refusal);
        ReaderColumn rc;
        rc.name = f.name;
        rc.type = f.type;
        rc.precision = f.precision;
        rc.scale = f.scale;
        rc.child_type = f.child_type;
        rc.array_depth = f.array_depth;
        reader.push_back(rc);
        decode.push_back(f.name);
    }
    bind(reader, decode, false);
}

void OrcFile::bind(const std::vector<ReaderColumn>& reader, const std::vector<std::string>& decode, bool match_by_id) {
    Impl& m = *impl_;
    m.plan.clear();
    m.out_names = decode;
    if (match_by_id && !decode.empty()) {
        bool any_id = false;
        for (const FileColumn& f : m.columns) any_id = any_id || f.iceberg_id >= 0;
        if (!any_id && !m.columns.empty())
            fail("none of the file's columns carries an `iceberg.id` attribute, so its columns cannot be matched to the table's");
    }
    for (const std::string& name : decode) {
        const ReaderColumn* rc = nullptr;
        for (const ReaderColumn& c : reader)
            if (c.name == name) rc = &c;
        if (rc == nullptr) fail("column '" + name + "' is not in the reader schema");
        ColPlan cp;
        cp.rc = *rc;
        for (size_t i = 0; i < m.columns.size(); ++i) {
            const FileColumn& f = m.columns[i];
            const bool hit = match_by_id ? (rc->field_id >= 0 && f.iceberg_id == rc->field_id) : f.name == rc->name;
            if (!hit) continue;
            if (cp.file_col >= 0) fail("column '" + name + "' matches two of the file's columns");
            cp.file_col = static_cast<int>(i);
        }
        if (cp.file_col < 0) {
            if (!rc->nullable) fail("column '" + name + "' is required but the file does not have it");
            if (rc->type == DRAKEN_ARRAY) fail("column '" + name + "' is an ARRAY the file does not have, which cannot be a NULL constant");
        } else {
            const FileColumn& f = m.columns[static_cast<size_t>(cp.file_col)];
            if (!f.readable) fail("column '" + name + "': " + f.refusal);
            const std::string why = compatibility(f, *rc);
            if (!why.empty()) fail("column '" + name + "': " + why);

        }
        m.plan.push_back(std::move(cp));
    }
}

Batch OrcFile::decode_stripe(size_t stripe) const {
    const Impl& m = *impl_;
    const StripeInfo& si = m.footer.stripes.at(stripe);
    Batch batch;
    batch.rows = static_cast<uint32_t>(si.rows);
    if (m.plan.empty()) return batch;

    // The stripe footer names every stream; offsets are the running sum of their lengths,
    // index streams first, so a column that is not asked for is never touched.
    const uint64_t footer_at = si.offset + si.index_length + si.data_length;
    const Bytes footer_bytes = m.stream(footer_at, si.footer_length);
    const StripeFooter sf = parse_stripe_footer({footer_bytes.p, footer_bytes.n});
    StreamMap streams;
    uint64_t at = si.offset;
    for (const StreamInfo& s : sf.streams) {
        streams[{s.column, s.kind}] = {at, s.length};
        at += s.length;
    }
    if (at > footer_at) fail("a stripe's streams extend past its data (the file is corrupt)");

    batch.columns.resize(m.plan.size());
    for (size_t i = 0; i < m.plan.size(); ++i) m.decode_column(sf, streams, m.plan[i], batch.rows, batch.columns[i]);
    return batch;
}

void OrcFile::Impl::decode_column(const StripeFooter& sf, const StreamMap& streams, const ColPlan& cp, uint32_t rows,
                                  Column& out) const {
    if (cp.file_col < 0) {
        null_constant(cp.rc, rows, out);
        return;
    }
    const FileColumn& fc = columns[static_cast<size_t>(cp.file_col)];
    if (fc.nested) decode_nested(sf, streams, fc, cp.rc, rows, out);
    else decode_leaf(sf, streams, fc, cp.rc, rows, out);
}

void OrcFile::Impl::decode_leaf(const StripeFooter& sf, const StreamMap& streams, const FileColumn& fc,
                                const ReaderColumn& rc, uint32_t rows, Column& out) const {
    const uint32_t id = fc.type_id;
    const std::string where = "column '" + fc.name + "': ";
    if (id >= sf.columns.size()) fail(where + "the stripe has no encoding for it (the file is corrupt)");
    const ColumnEncoding enc = sf.columns[id];
    if (enc.kind > 3) fail(where + "it has column encoding " + std::to_string(enc.kind) + ", which this reader does not know");
    const bool v2 = enc.kind >= 2;
    const bool dictionary = enc.kind == 1 || enc.kind == 3;

    auto find = [&](StreamKind k) -> const std::pair<uint64_t, uint64_t>* {
        auto it = streams.find({id, static_cast<uint32_t>(k)});
        return it == streams.end() ? nullptr : &it->second;
    };
    auto need = [&](StreamKind k, const char* what) -> Bytes {
        const auto* s = find(k);
        if (s == nullptr) fail(where + "its " + what + " stream is missing");
        return stream(s->first, s->second);
    };

    // PRESENT: one boolean per row; absent when every row has a value.
    Presence pres;
    pres.nonnull = rows;
    if (const auto* s = find(StreamKind::Present)) {
        const Bytes pb = stream(s->first, s->second);
        pres.flags.resize(rows);
        read_bools(pb.p, pb.n, rows, pres.flags.data());
        pres.nonnull = 0;
        for (uint8_t f : pres.flags) pres.nonnull += f;
        if (pres.nonnull != rows) {
            pres.validity = alloc_zero(pad8((static_cast<size_t>(rows) + 7) / 8));
            for (uint32_t r = 0; r < rows; ++r)
                if (pres.flags[r]) pres.validity[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
        }
    }
    struct Guard {
        uint8_t* p;
        ~Guard() { draken_free(p); }
    } validity_guard{pres.validity};
    auto take_validity = [&]() {
        uint8_t* v = pres.validity;
        pres.validity = nullptr;
        validity_guard.p = nullptr;
        return v;
    };
    const size_t nn = pres.nonnull;
    const bool all_valid = pres.nonnull == rows;

    out.type = rc.type;
    out.length = rows;
    out.precision = rc.precision;
    out.scale = rc.scale;

    // Nullable output retains zeroes at null rows. All-valid output is entirely overwritten.
    auto fixed_out = [&](size_t width) {
        draken::AppendBuffer<uint8_t> buf;
        const size_t bytes = static_cast<size_t>(rows) * width;
        if (all_valid) buf.resize_uninit(bytes);
        else buf.resize_fill(bytes, 0);
        return buf;
    };
    const uint8_t* flags = all_valid ? nullptr : pres.flags.data();

    switch (fc.kind) {
        case Kind::Boolean: {
            const Bytes b = need(StreamKind::Data, "DATA");
            std::vector<uint8_t> vals(nn);
            read_bools(b.p, b.n, nn, vals.data());
            auto* bits = alloc_zero(pad8((static_cast<size_t>(rows) + 7) / 8));
            size_t k = 0;
            for (uint32_t r = 0; r < rows; ++r) {
                if (!all_valid && !pres.flags[r]) continue;
                if (vals[k++]) bits[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
            }
            out.kind = OutKind::Raw;
            out.data = bits;
            break;
        }
        case Kind::Byte: {
            const Bytes b = need(StreamKind::Data, "DATA");
            const size_t width = width_of(rc.type);
            draken::AppendBuffer<uint8_t> buf = fixed_out(width);
            ByteRle reader(b.p, b.n);
            switch (width) {
                case 1: read_bytes_into(reader, reinterpret_cast<int8_t*>(buf.data()), nn, pres.validity); break;
                case 2: read_bytes_into(reader, reinterpret_cast<int16_t*>(buf.data()), nn, pres.validity); break;
                case 4: read_bytes_into(reader, reinterpret_cast<int32_t*>(buf.data()), nn, pres.validity); break;
                default: read_bytes_into(reader, reinterpret_cast<int64_t*>(buf.data()), nn, pres.validity); break;
            }
            out.kind = OutKind::Raw;
            out.data = buf.release();
            break;
        }
        case Kind::Short: case Kind::Int: case Kind::Long: case Kind::Date: {
            const Bytes b = need(StreamKind::Data, "DATA");
            const size_t width = width_of(rc.type);
            draken::AppendBuffer<uint8_t> buf = fixed_out(width);
            IntRle reader(b.p, b.n, v2, true);
            switch (width) {
                case 2: read_ints_into(reader, reinterpret_cast<int16_t*>(buf.data()), nn, flags); break;
                case 4: read_ints_into(reader, reinterpret_cast<int32_t*>(buf.data()), nn, flags); break;
                default: read_ints_into(reader, reinterpret_cast<int64_t*>(buf.data()), nn, flags); break;
            }
            out.kind = fc.type == DRAKEN_TIME64 ? OutKind::Time64 : OutKind::Raw;
            out.data = buf.release();
            break;
        }
        case Kind::Float: case Kind::Double: {
            const Bytes b = need(StreamKind::Data, "DATA");
            const size_t w = fc.kind == Kind::Float ? 4 : 8;
            if (b.n < nn * w) fail(where + "its DATA stream is shorter than its values");
            draken::AppendBuffer<uint8_t> buf = fixed_out(width_of(rc.type));
            uint8_t* d = buf.data();
            if (all_valid && w == width_of(rc.type)) {
                if (rows != 0) std::memcpy(d, b.p, static_cast<size_t>(rows) * w);
            } else {
                size_t k = 0;
                for (uint32_t r = 0; r < rows; ++r) {
                    if (!all_valid && !pres.flags[r]) continue;
                    const uint8_t* src = b.p + (k++) * w;
                    if (fc.kind == Kind::Float) {
                        float f;
                        std::memcpy(&f, src, 4);
                        if (rc.type == DRAKEN_FLOAT32) reinterpret_cast<float*>(d)[r] = f;
                        else reinterpret_cast<double*>(d)[r] = static_cast<double>(f);
                    } else {
                        double f;
                        std::memcpy(&f, src, 8);
                        reinterpret_cast<double*>(d)[r] = f;
                    }
                }
            }
            out.kind = OutKind::Raw;
            out.data = buf.release();
            break;
        }
        case Kind::String: case Kind::Varchar: case Kind::Char: case Kind::Binary: {
            if (fc.type == DRAKEN_UUID) {
                // A BINARY column marked iceberg.binary-type=UUID: exactly 16 bytes per value, in
                // network order, into a 16-byte lane. Anything else is not a uuid - fail, naming it.
                if (dictionary) fail(where + "a uuid column with a dictionary encoding is not supported");
                const Bytes lens_b = need(StreamKind::Length, "LENGTH");
                std::vector<int64_t> lens(nn);
                IntRle(lens_b.p, lens_b.n, v2, false).read(lens.data(), nn);
                for (int64_t l : lens)
                    if (l != 16) fail(where + "a uuid value is " + std::to_string(l) + " bytes, not 16");
                const Bytes data_b = need(StreamKind::Data, "DATA");
                if (data_b.n < nn * 16) fail(where + "its DATA stream is shorter than its uuid values");
                draken::AppendBuffer<uint8_t> buf = fixed_out(16);
                const uint8_t* src = data_b.p;
                for (uint32_t r = 0; r < rows; ++r) {
                    if (!all_valid && !pres.flags[r]) continue;
                    std::memcpy(buf.data() + static_cast<size_t>(r) * 16, src, 16);
                    src += 16;
                }
                out.kind = OutKind::Raw;
                out.data = buf.release();
                break;
            }
            if (!dictionary) {
                const Bytes lens_b = need(StreamKind::Length, "LENGTH");
                std::vector<int64_t> lens(nn);
                IntRle(lens_b.p, lens_b.n, v2, false).read(lens.data(), nn);
                if (fc.fixed_length != 0)
                    for (int64_t l : lens)
                        if (l != static_cast<int64_t>(fc.fixed_length))
                            fail(where + "a fixed(" + std::to_string(fc.fixed_length) + ") value is " + std::to_string(l) + " bytes");
                const Bytes data_b = need(StreamKind::Data, "DATA");
                uint64_t total = 0;
                for (int64_t l : lens) {
                    if (l < 0) fail(where + "a string has a negative length");
                    total += static_cast<uint64_t>(l);
                }
                if (total > data_b.n) fail(where + "its DATA stream is shorter than its string lengths");
                draken::AppendBuffer<DrakenStringSlot> slots;
                draken::AppendBuffer<uint8_t> arena;
                slots.resize_uninit(rows);
                arena.reserve(static_cast<size_t>(total));
                const uint8_t* src = data_b.p;
                size_t k = 0;
                for (uint32_t r = 0; r < rows; ++r) {
                    if (!all_valid && !pres.flags[r]) {
                        str_init_null(&slots[r]);
                        continue;
                    }
                    const uint64_t len = static_cast<uint64_t>(lens[k++]);
                    build_slot(slots, arena, r, src, len);
                    src += len;
                }
                out.kind = OutKind::String;
                out.arena_len = arena.size();
                out.slots = slots.release();
                out.arena = arena.release();
            } else {
                const uint32_t dsize = enc.dictionary_size;
                const Bytes idx_b = need(StreamKind::Data, "DATA");
                std::vector<int64_t> idx(nn);
                IntRle(idx_b.p, idx_b.n, v2, false).read(idx.data(), nn);
                if (dsize == 0) {
                    if (nn != 0) fail(where + "it has values but an empty dictionary");
                    draken::AppendBuffer<DrakenStringSlot> slots;
                    slots.resize_uninit(rows);
                    for (uint32_t r = 0; r < rows; ++r) str_init_null(&slots[r]);
                    out.kind = OutKind::String;
                    out.slots = slots.release();
                    break;
                }
                const Bytes lens_b = need(StreamKind::Length, "LENGTH");
                std::vector<int64_t> lens(dsize);
                IntRle(lens_b.p, lens_b.n, v2, false).read(lens.data(), dsize);
                if (fc.fixed_length != 0)
                    for (int64_t l : lens)
                        if (l != static_cast<int64_t>(fc.fixed_length))
                            fail(where + "a fixed(" + std::to_string(fc.fixed_length) + ") value is " + std::to_string(l) + " bytes");
                const Bytes dict_b = need(StreamKind::DictionaryData, "DICTIONARY_DATA");
                uint64_t total = 0;
                for (int64_t l : lens) {
                    if (l < 0) fail(where + "a dictionary entry has a negative length");
                    total += static_cast<uint64_t>(l);
                }
                if (total > dict_b.n) fail(where + "its DICTIONARY_DATA stream is shorter than its entry lengths");
                draken::AppendBuffer<DrakenStringSlot> dslots;
                draken::AppendBuffer<uint8_t> darena;
                dslots.resize_uninit(dsize);
                const uint8_t* src = dict_b.p;
                for (uint32_t e = 0; e < dsize; ++e) {
                    const uint64_t len = static_cast<uint64_t>(lens[e]);
                    build_slot(dslots, darena, e, src, len);
                    src += len;
                }
                draken::AppendBuffer<uint32_t> codes;
                codes.resize_fill(rows, 0);
                size_t k = 0;
                for (uint32_t r = 0; r < rows; ++r) {
                    if (!all_valid && !pres.flags[r]) continue;
                    const int64_t c = idx[k++];
                    if (c < 0 || static_cast<uint64_t>(c) >= dsize) fail(where + "a dictionary index is out of range");
                    codes[r] = static_cast<uint32_t>(c);
                }
                out.kind = OutKind::Dict;
                out.dict_len = dsize;
                out.arena_len = darena.size();
                out.slots = dslots.release();
                out.arena = darena.release();
                out.codes = codes.release();
            }
            break;
        }
        case Kind::Timestamp: case Kind::TimestampInstant: {
            if (fc.kind == Kind::Timestamp && !is_utc_name(sf.writer_timezone))
                fail(where + "it is a TIMESTAMP written in time zone '" + sf.writer_timezone +
                     "'; only UTC writers are supported (a TIMESTAMP is wall-clock time in its writer's zone)");
            const Bytes data_b = need(StreamKind::Data, "DATA");
            const Bytes nano_b = need(StreamKind::Secondary, "SECONDARY");
            draken::AppendBuffer<uint8_t> buf = fixed_out(sizeof(int64_t));
            auto* values = reinterpret_cast<int64_t*>(buf.data());
            IntRle seconds(data_b.p, data_b.n, v2, true);
            IntRle nanos(nano_b.p, nano_b.n, v2, false);
            size_t row = 0;
            seconds.read_runs(nn, [&](const int64_t* secs, size_t n) {
                nanos.read_runs(n, [&](const int64_t* fractions, size_t count) {
                    if (all_valid) {
                        for (size_t i = 0; i < count; ++i, ++row)
                            values[row] = timestamp_micros(secs[i], fractions[i], where);
                    } else {
                        for (size_t i = 0; i < count; ++i) {
                            while (!flags[row]) ++row;
                            values[row++] = timestamp_micros(secs[i], fractions[i], where);
                        }
                    }
                    secs += count;
                });
            });
            out.kind = OutKind::Timestamp;
            out.data = buf.release();
            break;
        }
        case Kind::Decimal: {
            const Bytes data_b = need(StreamKind::Data, "DATA");
            const Bytes scale_b = need(StreamKind::Secondary, "SECONDARY");
            std::vector<int64_t> scales(nn);
            IntRle(scale_b.p, scale_b.n, v2, true).read(scales.data(), nn);
            const size_t w = width_of(rc.type);
            draken::AppendBuffer<uint8_t> buf = fixed_out(w);
            const unsigned __int128 limit = pow10_128(rc.precision);
            const uint8_t* p = data_b.p;
            const uint8_t* end = data_b.p + data_b.n;
            size_t k = 0;
            for (uint32_t r = 0; r < rows; ++r) {
                if (!all_valid && !pres.flags[r]) continue;
                __int128 v = read_varint128(p, end);
                const int64_t s = scales[k++];
                if (s < 0 || s > 38) fail(where + "a decimal value has scale " + std::to_string(s));
                const int target = rc.scale;
                if (s < target) {
                    v *= static_cast<__int128>(pow10_128(static_cast<unsigned>(target - s)));
                } else if (s > target) {
                    const __int128 div = static_cast<__int128>(pow10_128(static_cast<unsigned>(s - target)));
                    if (v % div != 0) fail(where + "a decimal value cannot be rescaled to scale " + std::to_string(target) + " exactly");
                    v /= div;
                }
                const unsigned __int128 mag = v < 0 ? static_cast<unsigned __int128>(-v) : static_cast<unsigned __int128>(v);
                if (mag >= limit) fail(where + "a decimal value exceeds precision " + std::to_string(rc.precision));
                if (w == 16) std::memcpy(buf.data() + static_cast<size_t>(r) * 16, &v, 16);
                else *reinterpret_cast<int64_t*>(buf.data() + static_cast<size_t>(r) * 8) = static_cast<int64_t>(v);
            }
            out.kind = w == 16 ? OutKind::Decimal128 : OutKind::Decimal64;
            out.data = buf.release();
            break;
        }
        default:
            fail(where + std::string(kind_name(fc.kind)) + " columns are not supported");
    }
    out.validity = take_validity();
}

// ── nested columns ──────────────────────────────────────────────────────────
//
// A STRUCT / LIST / MAP column is decoded top-down into a tree of NestedNodes and then rendered a
// row at a time. In ORC a child holds an entry only for each NON-NULL entry of its parent (a
// struct's children are as long as the struct has non-null rows; a list's or map's child is as long
// as the sum of the non-null lengths), so every node knows its own entry count `n` before it is
// read, and a child's cursor simply follows the order the parent consumes it in.

struct NestedNode {
    uint32_t id = 0;
    Kind kind = Kind::Long;
    std::string key;               // the member name in the parent struct
    size_t n = 0;                  // entries at this level
    std::vector<uint8_t> flags;    // one per entry; empty = every entry is present
    std::vector<int64_t> lens;     // LIST / MAP: one length per present entry
    std::vector<NestedNode> kids;
    FileColumn fc;                 // a primitive: how it decoded
    Column leaf;                   // a primitive: its n entries, nulls placed
    size_t pos = 0;                // render cursor: the next entry
    size_t vpos = 0;               // render cursor: the next present entry (LIST / MAP lengths)
    bool missing = false;          // a member the table declares and this file lacks: JSON null, no entries
};

// The shape a TABLE declares for a nested column (pre-order) and where each node's subtree ends. The
// column is rendered with the table's member names, each file's fields matched by `iceberg.id`
// (docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.3, option (c)).
struct DeclCtx {
    const std::vector<DeclaredNode>& nodes;
    std::vector<int> end;

    explicit DeclCtx(const std::vector<DeclaredNode>& n) : nodes(n), end(n.size(), 0) { end_of(0); }

    int end_of(int i) {
        int j = i + 1;
        for (int c = 0; c < nodes[static_cast<size_t>(i)].n_children; ++c) j = end_of(j);
        end[static_cast<size_t>(i)] = j;
        return j;
    }
};

namespace {

bool is_container(Kind k) { return k == Kind::Struct || k == Kind::List || k == Kind::Map; }

bool entry_present(const NestedNode& nd, size_t e) {
    if (!is_container(nd.kind)) {
        const uint8_t* v = nd.leaf.validity;
        return v == nullptr || ((v[e >> 3] >> (e & 7)) & 1) != 0;
    }
    return nd.flags.empty() || nd.flags[e] != 0;
}

void render_string_bytes(std::string& o, const NestedNode& nd, size_t e) {
    const Column& c = nd.leaf;
    const DrakenStringSlot* slot = c.kind == OutKind::Dict ? &c.slots[c.codes[e]] : &c.slots[e];
    const uint8_t* p = str_data(slot, c.arena);
    const size_t n = str_length(slot);
    if (nd.fc.type == DRAKEN_VARCHAR) {
        rugo_text::json_string(o, reinterpret_cast<const char*>(p), n);
        return;
    }
    // BINARY: quoted base64. b64_encoded_size counts bintob64's trailing NUL, which the closing quote overwrites.
    const size_t sz = b64_encoded_size(n);
    const size_t at = o.size();
    o.resize(at + sz + 1);
    o[at] = '"';
    bintob64(&o[at + 1], p, n);
    o[at + sz] = '"';
}

// One present primitive entry, as the shared renderer writes it.
void render_leaf(std::string& o, const NestedNode& nd, size_t e) {
    const Column& c = nd.leaf;
    const uint8_t* d = static_cast<const uint8_t*>(c.data);
    switch (nd.fc.type) {
        case DRAKEN_BOOL: o.append(((d[e >> 3] >> (e & 7)) & 1) ? "true" : "false"); return;
        case DRAKEN_INT8: rugo_text::fmt_int64(o, reinterpret_cast<const int8_t*>(d)[e]); return;
        case DRAKEN_INT16: rugo_text::fmt_int64(o, reinterpret_cast<const int16_t*>(d)[e]); return;
        case DRAKEN_INT32: rugo_text::fmt_int64(o, reinterpret_cast<const int32_t*>(d)[e]); return;
        case DRAKEN_INT64: rugo_text::fmt_int64(o, reinterpret_cast<const int64_t*>(d)[e]); return;
        case DRAKEN_DATE32: rugo_text::fmt_date_quoted(o, reinterpret_cast<const int32_t*>(d)[e]); return;
        case DRAKEN_TIME64: rugo_text::fmt_time_quoted(o, reinterpret_cast<const int64_t*>(d)[e], rugo_text::U_US); return;
        case DRAKEN_TIMESTAMP64:
            rugo_text::fmt_timestamp_quoted(o, reinterpret_cast<const int64_t*>(d)[e], rugo_text::U_US);
            return;
        case DRAKEN_FLOAT32: {
            const float v = draken::ops::fp_canon(reinterpret_cast<const float*>(d)[e]);
            if (rugo_text::double_is_nan_or_inf(static_cast<double>(v))) o.append("null");
            else rugo_text::fmt_float(o, v);
            return;
        }
        case DRAKEN_FLOAT64: {
            const double v = draken::ops::fp_canon(reinterpret_cast<const double*>(d)[e]);
            if (rugo_text::double_is_nan_or_inf(v)) o.append("null");
            else rugo_text::fmt_double(o, v);
            return;
        }
        case DRAKEN_DECIMAL:
            rugo_text::fmt_decimal(o, static_cast<__int128>(reinterpret_cast<const int64_t*>(d)[e]), nd.fc.scale);
            return;
        case DRAKEN_DECIMAL128: {
            __int128 v;
            std::memcpy(&v, d + e * 16, 16);
            rugo_text::fmt_decimal(o, v, nd.fc.scale);
            return;
        }
        case DRAKEN_UUID: rugo_text::fmt_uuid_quoted(o, d + e * 16); return;
        case DRAKEN_VARCHAR: case DRAKEN_VARBINARY: render_string_bytes(o, nd, e); return;
        default: fail(std::string("a nested ") + type_name(nd.fc.type) + " value cannot be rendered");
    }
}

void render_entry(std::string& o, NestedNode& nd);

// Entry `e` of `nd`, which is present. Consumes the children it owns, in order.
void render_value(std::string& o, NestedNode& nd, size_t e) {
    switch (nd.kind) {
        case Kind::Struct: {
            o.push_back('{');
            for (size_t i = 0; i < nd.kids.size(); ++i) {
                if (i != 0) o.push_back(',');
                rugo_text::json_string(o, nd.kids[i].key.data(), nd.kids[i].key.size());
                o.push_back(':');
                if (nd.kids[i].missing) o.append("null");
                else render_entry(o, nd.kids[i]);
            }
            o.push_back('}');
            return;
        }
        case Kind::List: {
            const int64_t len = nd.lens[nd.vpos++];
            o.push_back('[');
            for (int64_t j = 0; j < len; ++j) {
                if (j != 0) o.push_back(',');
                render_entry(o, nd.kids[0]);
            }
            o.push_back(']');
            return;
        }
        case Kind::Map: {
            const int64_t len = nd.lens[nd.vpos++];
            o.push_back('{');
            for (int64_t j = 0; j < len; ++j) {
                if (j != 0) o.push_back(',');
                if (!entry_present(nd.kids[0], nd.kids[0].pos)) fail("a map has a null key");
                render_entry(o, nd.kids[0]);
                o.push_back(':');
                render_entry(o, nd.kids[1]);
            }
            o.push_back('}');
            return;
        }
        default: render_leaf(o, nd, e); return;
    }
}

void render_entry(std::string& o, NestedNode& nd) {
    const size_t e = nd.pos++;
    if (!entry_present(nd, e)) {
        o.append("null");
        return;
    }
    render_value(o, nd, e);
}

// Replace a dictionary-coded string leaf by the dense string column an ARRAY child must be.
void densify_strings(Column& c) {
    if (c.kind != OutKind::Dict) return;
    draken::AppendBuffer<DrakenStringSlot> slots;
    draken::AppendBuffer<uint8_t> arena;
    slots.resize_uninit(c.length);
    for (uint32_t r = 0; r < c.length; ++r) {
        const DrakenStringSlot* from = &c.slots[c.codes[r]];
        const bool null = c.validity != nullptr && ((c.validity[r >> 3] >> (r & 7)) & 1) == 0;
        if (null) str_init_null(&slots[r]);
        else build_slot(slots, arena, r, str_data(from, c.arena), str_length(from));
    }
    draken_free(c.slots);
    draken_free(c.arena);
    draken_free(c.codes);
    c.kind = OutKind::String;
    c.slots = slots.release();
    c.arena_len = arena.size();
    c.arena = arena.release();
    c.codes = nullptr;
    c.dict_len = 0;
}


// The validity bitmap of a node's entries (null when every entry is present).
uint8_t* validity_of(const NestedNode& nd) {
    if (nd.flags.empty()) return nullptr;
    uint8_t* v = alloc_zero(pad8((nd.n + 7) / 8));
    for (size_t r = 0; r < nd.n; ++r)
        if (nd.flags[r]) v[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
    return v;
}

// A LIST node as an Array column: its offsets and validity, and its element column moved in (a
// scalar leaf), or, for a list of lists, the element list as a whole Array column of its own.
// Takes `validity` (this node's, possibly null).
void fill_array(NestedNode& list, Column& out, uint8_t* validity) {
    out.validity = validity;   // `out` owns it from here, whatever happens next
    draken::AppendBuffer<uint8_t> off_buf;
    off_buf.resize_fill((list.n + 1) * sizeof(int32_t), 0);
    int32_t* offsets = reinterpret_cast<int32_t*>(off_buf.data());
    size_t k = 0;
    int64_t at = 0;
    for (size_t r = 0; r < list.n; ++r) {
        offsets[r] = static_cast<int32_t>(at);
        if (list.flags.empty() || list.flags[r]) at += list.lens[k++];
    }
    if (at > INT32_MAX) fail("a column has more than 2 billion list elements in a stripe");
    offsets[list.n] = static_cast<int32_t>(at);
    out.kind = OutKind::Array;
    out.type = DRAKEN_ARRAY;
    out.length = static_cast<uint32_t>(list.n);
    out.offsets = reinterpret_cast<int32_t*>(off_buf.release());
    NestedNode& elem = list.kids[0];
    if (elem.kind == Kind::List) {
        out.child_type = DRAKEN_ARRAY;
        out.child_length = static_cast<uint32_t>(elem.n);
        out.child_col = std::make_unique<Column>();
        fill_array(elem, *out.child_col, validity_of(elem));
        return;
    }
    if (elem.fc.type == DRAKEN_VARCHAR || elem.fc.type == DRAKEN_VARBINARY) densify_strings(elem.leaf);
    Column& child = elem.leaf;
    out.child_type = elem.fc.type;
    out.child_length = child.length;
    out.data = child.data != nullptr ? child.data : alloc_zero(16);
    out.slots = child.slots;
    out.arena = child.arena;
    out.arena_len = child.arena_len;
    out.child_validity = child.validity;
    child.disown();
}

}  // namespace

NestedNode OrcFile::Impl::build_node(const StripeFooter& sf, const StreamMap& streams, uint32_t id,
                                     const std::string& name, const std::string& key, size_t n,
                                     const DeclCtx* decl, int di) const {
    NestedNode nd;
    nd.id = id;
    nd.key = key;
    nd.n = n;
    const Type& t = footer.types[id];
    nd.kind = t.kind;
    const std::string where = "column '" + name + "': ";
    if (n > UINT32_MAX) fail(where + "has more than 4 billion nested values in a stripe");
    if (id >= sf.columns.size()) fail(where + "the stripe has no encoding for it (the file is corrupt)");
    const ColumnEncoding enc = sf.columns[id];
    if (enc.kind > 3) fail(where + "it has column encoding " + std::to_string(enc.kind) + ", which this reader does not know");
    const bool v2 = enc.kind >= 2;

    if (di >= 0) {
        // This file node must be the table's field `di`: the same field id, the same kind of thing.
        const DeclaredNode& d = decl->nodes[static_cast<size_t>(di)];
        const std::string* idattr = t.attribute("iceberg.id");
        if (idattr == nullptr)
            fail(where + "the file carries no iceberg.id for it, so it cannot be matched to the table's field '" +
                 d.name + "' (id " + std::to_string(d.field_id) + ")");
        long long have_id = -1;
        try {
            have_id = std::stoll(*idattr);
        } catch (const std::exception&) {
            fail(where + "its iceberg.id attribute '" + *idattr + "' is not an integer");
        }
        if (have_id != d.field_id)
            fail(where + "the table declares '" + d.name + "' as field id " + std::to_string(d.field_id) +
                 " but the file has id " + std::to_string(have_id) + " there");
        const int have = t.kind == Kind::Struct ? 1 : t.kind == Kind::List ? 2 : t.kind == Kind::Map ? 3 : 0;
        if (have != d.kind) {
            static const char* words[] = {"a scalar", "a struct", "a list", "a map"};
            fail(where + "the table declares '" + d.name + "' (id " + std::to_string(d.field_id) + ") as " +
                 words[d.kind] + " but the file has " + words[have]);
        }
    }

    if (!is_container(t.kind)) {
        nd.fc = describe_leaf(t, name, id);
        if (!nd.fc.readable) fail(where + nd.fc.refusal);
        ReaderColumn rc;
        rc.name = name;
        rc.type = nd.fc.type;
        rc.precision = nd.fc.precision;
        rc.scale = nd.fc.scale;
        decode_leaf(sf, streams, nd.fc, rc, static_cast<uint32_t>(n), nd.leaf);
        return nd;
    }

    // PRESENT: one flag per entry; absent when every entry is present.
    size_t present = n;
    auto it = streams.find({id, static_cast<uint32_t>(StreamKind::Present)});
    if (it != streams.end()) {
        const Bytes pb = stream(it->second.first, it->second.second);
        nd.flags.resize(n);
        read_bools(pb.p, pb.n, n, nd.flags.data());
        present = 0;
        for (uint8_t f : nd.flags) present += f;
        if (present == n) nd.flags.clear();
    }

    if (t.kind == Kind::Struct) {
        if (di < 0) {
            for (size_t i = 0; i < t.subtypes.size(); ++i)
                nd.kids.push_back(build_node(sf, streams, t.subtypes[i], name + "." + t.field_names[i], t.field_names[i], present));
            return nd;
        }
        // The table's members, in the table's order and under the table's names, each matched to the
        // file's field of the same id. A member the file lacks (added after it was written) is JSON null;
        // a field the file has and the table no longer does is never decoded.
        int ci = di + 1;
        for (int k = 0; k < decl->nodes[static_cast<size_t>(di)].n_children; ++k) {
            const DeclaredNode& dc = decl->nodes[static_cast<size_t>(ci)];
            const uint32_t none = UINT32_MAX;
            uint32_t hit = none;
            for (uint32_t sub : t.subtypes) {
                const std::string* idattr = footer.types[sub].attribute("iceberg.id");
                if (idattr == nullptr)
                    fail(where + "the file carries no iceberg.id on one of its fields, so its fields cannot be matched to the table's");
                if (std::to_string(dc.field_id) == *idattr) { hit = sub; break; }
            }
            if (hit == none) {
                NestedNode m;
                m.missing = true;
                m.key = dc.name;
                nd.kids.push_back(std::move(m));
            } else {
                nd.kids.push_back(build_node(sf, streams, hit, name + "." + dc.name, dc.name, present, decl, ci));
            }
            ci = decl->end[static_cast<size_t>(ci)];
        }
        return nd;
    }

    // LIST / MAP: one length per present entry; the children hold that many entries in all.
    auto lit = streams.find({id, static_cast<uint32_t>(StreamKind::Length)});
    if (lit == streams.end()) fail(where + "its LENGTH stream is missing");
    const Bytes lens_b = stream(lit->second.first, lit->second.second);
    nd.lens.resize(present);
    IntRle(lens_b.p, lens_b.n, v2, false).read(nd.lens.data(), present);
    uint64_t total = 0;
    for (int64_t l : nd.lens) {
        if (l < 0) fail(where + "a list or map has a negative length");
        total += static_cast<uint64_t>(l);
        if (total > UINT32_MAX) fail(where + "has more than 4 billion nested values in a stripe");
    }
    if (t.kind == Kind::List) {
        nd.kids.push_back(build_node(sf, streams, t.subtypes[0], name + "[]", std::string(), total, decl,
                                     di >= 0 ? di + 1 : -1));
    } else {
        nd.kids.push_back(build_node(sf, streams, t.subtypes[0], name + ".key", "key", total, decl,
                                     di >= 0 ? di + 1 : -1));
        nd.kids.push_back(build_node(sf, streams, t.subtypes[1], name + ".value", "value", total, decl,
                                     di >= 0 ? decl->end[static_cast<size_t>(di) + 1] : -1));
    }
    return nd;
}

void OrcFile::Impl::decode_nested(const StripeFooter& sf, const StreamMap& streams, const FileColumn& fc,
                                  const ReaderColumn& rc, uint32_t rows, Column& out) const {
    // A declared shape applies to a column that renders as JSON; an ARRAY (a list of a plain scalar) has no
    // names to drift.
    std::unique_ptr<DeclCtx> decl;
    if (!rc.declared.empty() && fc.type == DRAKEN_NVARCHAR) decl = std::make_unique<DeclCtx>(rc.declared);
    NestedNode top = build_node(sf, streams, fc.type_id, fc.name, std::string(), rows, decl.get(), decl ? 0 : -1);
    out.type = rc.type;
    out.length = rows;

    // The column's own nulls.
    uint8_t* validity = nullptr;
    struct Guard {
        uint8_t*& p;
        ~Guard() { draken_free(p); }
    } validity_guard{validity};
    if (!top.flags.empty()) {
        validity = alloc_zero(pad8((static_cast<size_t>(rows) + 7) / 8));
        for (uint32_t r = 0; r < rows; ++r)
            if (top.flags[r]) validity[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
    }

    if (fc.type == DRAKEN_ARRAY) {
        // A LIST of a plain scalar, or of lists of one: the offsets, and the element column as decoded.
        out.type = DRAKEN_ARRAY;
        uint8_t* owned = validity;
        validity = nullptr;   // fill_array's column owns it now
        fill_array(top, out, owned);
        return;
    }

    // Everything else is one JSON value per row.
    std::string all;
    std::vector<uint64_t> ends(rows);
    for (uint32_t r = 0; r < rows; ++r) {
        if (top.flags.empty() || top.flags[r]) render_value(all, top, r);
        ends[r] = all.size();
    }
    draken::AppendBuffer<DrakenStringSlot> slots;
    draken::AppendBuffer<uint8_t> arena;
    slots.resize_uninit(rows);
    uint64_t from = 0;
    for (uint32_t r = 0; r < rows; ++r) {
        if (!top.flags.empty() && !top.flags[r]) {
            str_init_null(&slots[r]);
        } else {
            build_slot(slots, arena, r, reinterpret_cast<const uint8_t*>(all.data()) + from, ends[r] - from);
        }
        from = ends[r];
    }
    out.kind = OutKind::String;
    out.arena_len = arena.size();
    out.slots = slots.release();
    out.arena = arena.release();
    out.validity = validity;
    validity = nullptr;
}

}  // namespace rugo::orc
