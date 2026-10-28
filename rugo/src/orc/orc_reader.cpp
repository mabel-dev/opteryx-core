#include "orc_reader.hpp"

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <new>
#include <optional>
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
#include "orc_timestamp.hpp"
#include "snappy.h"
#include "zstd.h"

namespace rugo::orc {

namespace {

using rugo::avro::OutKind;

constexpr size_t kTailRead = 16 * 1024;
constexpr size_t kDefaultBlock = 256 * 1024;

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

// Where a file's streams come from: the file's bytes and how its streams are compressed.
struct StreamSource {
    const uint8_t* data = nullptr;
    size_t size = 0;
    Compression compression = Compression::None;
    size_t block_size = 0;

    Bytes get(uint64_t offset, uint64_t length) const {
        if (offset > size || length > size - offset) fail("a stream extends past the end of the file");
        return decompress(compression, block_size, data + offset, static_cast<size_t>(length));
    }
};

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

// Validity bitmap (LSB first, bit set = valid); null means every row is present.
struct Presence {
    size_t nonnull = 0;
    uint8_t* validity = nullptr;  // draken_malloc'd
};

inline bool present_at(const uint8_t* validity, size_t row) {
    return validity == nullptr || (validity[row >> 3] & (1u << (row & 7))) != 0;
}

struct PresentCursor {
    const uint8_t* validity;
    size_t byte = 0;
    unsigned bits = 0;

    size_t next() {
        while (bits == 0) bits = validity[byte++];
        const size_t row = (byte - 1) * 8 + static_cast<unsigned>(__builtin_ctz(bits));
        bits &= bits - 1;
        return row;
    }
};

template <typename T>
void read_ints_into(IntRle& reader, T* out, size_t count, const uint8_t* validity) {
    if (validity == nullptr) {
        if constexpr (std::is_same_v<T, int64_t>) {
            reader.read(out, count);
        } else {
            reader.read_runs(count, [&](const int64_t* values, size_t n) {
                for (size_t i = 0; i < n; ++i) out[i] = static_cast<T>(values[i]);
                out += n;
            });
        }
    } else {
        PresentCursor cursor{validity};
        reader.read_runs(count, [&](const int64_t* values, size_t n) {
            for (size_t i = 0; i < n; ++i) out[cursor.next()] = static_cast<T>(values[i]);
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
    PresentCursor cursor{validity};
    while (count != 0) {
        const size_t n = std::min(count, sizeof(values));
        reader.read(values, n);
        if (validity == nullptr) {
            for (size_t i = 0; i < n; ++i) out[row++] = static_cast<int8_t>(values[i]);
        } else {
            for (size_t i = 0; i < n; ++i) out[cursor.next()] = static_cast<int8_t>(values[i]);
        }
        count -= n;
    }
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
    StreamSource src;
    Footer footer;
    std::vector<FileColumn> columns;
    std::vector<ColPlan> plan;
    std::vector<std::string> out_names;

    Bytes stream(uint64_t offset, uint64_t length) const { return src.get(offset, length); }

    // A STRUCT / LIST / MAP column's subtree, `n` entries of node `id`, decoded whole for the stripe.
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
    m.src = StreamSource{data, size, m.ps.compression, static_cast<size_t>(m.ps.block_size)};
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

// ── column cursors ──────────────────────────────────────────────────────────
//
// A stripe's column is decoded a row range at a time, in order: each read() continues where the
// last one ended, so a stripe becomes morsels of a bounded size (design doc §6.3). A cursor owns its
// column's decompressed streams for the stripe.

namespace {

// One primitive column of a stripe, read as `type` (with `precision` / `scale`). Its streams are
// opened by seek(): from the stripe's first row, or from a row group's first row by the column's
// row index (design doc §6.5). A dictionary is read whole at construction.
struct LeafCursor {
    LeafCursor(const StreamSource& src, const StripeFooter& sf, const StreamMap& streams, const FileColumn& f,
               DrakenType type, uint8_t precision, uint8_t scale);
    ~LeafCursor() {
        draken_free(dslots);
        draken_free(darena);
    }
    LeafCursor(const LeafCursor&) = delete;
    LeafCursor& operator=(const LeafCursor&) = delete;

    // Open every stream at `at`'s row group (null = the stripe's first row), decompressing only as
    // far as `until`'s row group needs (null = the end of the stripe).
    void seek(const RowIndexEntry* at, const RowIndexEntry* until);

    // The next `rows` entries.
    void read(uint32_t rows, Column& out);

    // A stream of the stripe: where it is in the file; absent when the stripe has none.
    struct Range {
        uint64_t offset = 0;
        uint64_t length = 0;
        bool exists = false;
    };

    const StreamSource& src;
    const FileColumn& fc;
    DrakenType type;
    uint8_t precision;
    uint8_t scale;
    std::string where;
    bool v2 = false;
    bool dictionary = false;
    Range present_r, data_r, length_r, secondary_r;   // length_r: a direct string's LENGTH

    Bytes present_b, data_b, length_b, secondary_b;   // the open windows of the streams
    std::optional<BitReader> present;
    std::optional<BitReader> bools;         // BOOLEAN DATA
    std::optional<ByteRle> bytes;           // BYTE DATA
    std::optional<IntRle> ints;             // integer DATA; seconds; dictionary indexes
    std::optional<IntRle> lengths;          // string LENGTH
    std::optional<IntRle> secondary;        // timestamp nanos; decimal scales
    const uint8_t* raw = nullptr;           // the unread rest of a raw DATA stream
    const uint8_t* raw_end = nullptr;

    // A dictionary-encoded string column: the stripe's dictionary, decoded once. Each read emits
    // a dictionary of only the entries its rows use (remap: entry -> code in the read; kNone = unused).
    static constexpr uint32_t kNone = UINT32_MAX;
    uint32_t dsize = 0;
    DrakenStringSlot* dslots = nullptr;
    uint8_t* darena = nullptr;
    std::vector<uint32_t> remap;
    std::vector<uint32_t> touched;

    std::vector<int64_t> scratch;

private:
    // Walks one row index entry's positions in stream order. A null entry yields zeroes.
    struct Positions {
        const RowIndexEntry* e;
        size_t i = 0;
        uint64_t take(const std::string& where) {
            if (e == nullptr) return 0;
            if (i >= e->positions.size()) fail(where + "its row index has fewer positions than its streams need (the file is corrupt)");
            return e->positions[i++];
        }
        void finish(const std::string& where) const {
            if (e != nullptr && i != e->positions.size())
                fail(where + "its row index has more positions than its streams need (the file is corrupt)");
        }
    };
    // Open `r` at `at`'s place: the window of decompressed bytes and where in it the place is.
    Bytes open(const Range& r, Positions& at, Positions& until, size_t& start);
};

LeafCursor::LeafCursor(const StreamSource& s, const StripeFooter& sf, const StreamMap& streams, const FileColumn& f,
                       DrakenType t, uint8_t p, uint8_t sc)
    : src(s), fc(f), type(t), precision(p), scale(sc), where("column '" + f.name + "': ") {
    const uint32_t id = fc.type_id;
    if (id >= sf.columns.size()) fail(where + "the stripe has no encoding for it (the file is corrupt)");
    const ColumnEncoding enc = sf.columns[id];
    if (enc.kind > 3) fail(where + "it has column encoding " + std::to_string(enc.kind) + ", which this reader does not know");
    v2 = enc.kind >= 2;
    dictionary = enc.kind == 1 || enc.kind == 3;

    auto find = [&](StreamKind k) -> Range {
        auto it = streams.find({id, static_cast<uint32_t>(k)});
        if (it == streams.end()) return Range{};
        return Range{it->second.first, it->second.second, true};
    };
    auto need = [&](StreamKind k, const char* what) -> Range {
        const Range r = find(k);
        if (!r.exists) fail(where + "its " + what + " stream is missing");
        return r;
    };

    // PRESENT: one boolean per row; absent when every row has a value.
    present_r = find(StreamKind::Present);

    switch (fc.kind) {
        case Kind::Boolean: case Kind::Byte: case Kind::Short: case Kind::Int: case Kind::Long: case Kind::Date:
        case Kind::Float: case Kind::Double:
            data_r = need(StreamKind::Data, "DATA");
            break;
        case Kind::String: case Kind::Varchar: case Kind::Char: case Kind::Binary: {
            if (fc.type == DRAKEN_UUID && dictionary) fail(where + "a uuid column with a dictionary encoding is not supported");
            data_r = need(StreamKind::Data, "DATA");
            if (!dictionary) {
                length_r = need(StreamKind::Length, "LENGTH");
                break;
            }
            dsize = enc.dictionary_size;
            if (dsize == 0) break;
            const Range lens_r = need(StreamKind::Length, "LENGTH");
            const Bytes lens_b = src.get(lens_r.offset, lens_r.length);
            std::vector<int64_t> lens(dsize);
            IntRle(lens_b.p, lens_b.n, v2, false).read(lens.data(), dsize);
            if (fc.fixed_length != 0)
                for (int64_t l : lens)
                    if (l != static_cast<int64_t>(fc.fixed_length))
                        fail(where + "a fixed(" + std::to_string(fc.fixed_length) + ") value is " + std::to_string(l) + " bytes");
            const Range dict_r = need(StreamKind::DictionaryData, "DICTIONARY_DATA");
            const Bytes dict_b = src.get(dict_r.offset, dict_r.length);
            uint64_t total = 0;
            for (int64_t l : lens) {
                if (l < 0) fail(where + "a dictionary entry has a negative length");
                total += static_cast<uint64_t>(l);
            }
            if (total > dict_b.n) fail(where + "its DICTIONARY_DATA stream is shorter than its entry lengths");
            draken::AppendBuffer<DrakenStringSlot> ds;
            draken::AppendBuffer<uint8_t> da;
            ds.resize_uninit(dsize);
            const uint8_t* from = dict_b.p;
            for (uint32_t e = 0; e < dsize; ++e) {
                const uint64_t len = static_cast<uint64_t>(lens[e]);
                build_slot(ds, da, e, from, len);
                from += len;
            }
            dslots = ds.release();
            darena = da.release();
            remap.assign(dsize, kNone);
            break;
        }
        case Kind::Timestamp: case Kind::TimestampInstant:
            if (fc.kind == Kind::Timestamp && !is_utc_name(sf.writer_timezone))
                fail(where + "it is a TIMESTAMP written in time zone '" + sf.writer_timezone +
                     "'; only UTC writers are supported (a TIMESTAMP is wall-clock time in its writer's zone)");
            data_r = need(StreamKind::Data, "DATA");
            secondary_r = need(StreamKind::Secondary, "SECONDARY");
            break;
        case Kind::Decimal:
            data_r = need(StreamKind::Data, "DATA");
            secondary_r = need(StreamKind::Secondary, "SECONDARY");
            break;
        default:
            fail(where + std::string(kind_name(fc.kind)) + " columns are not supported");
    }
}

// The end of the compressed chunk `count` chunks on from the one at `chunk` (or the stream's end).
uint64_t chunks_end(const StreamSource& src, uint64_t offset, uint64_t length, uint64_t chunk, unsigned count) {
    uint64_t at = chunk;
    for (unsigned i = 0; i < count && at < length; ++i) {
        if (length - at < 3) fail("a compression chunk header runs past its stream");
        const uint8_t* h = src.data + offset + at;
        const uint64_t len = (h[0] | (static_cast<uint32_t>(h[1]) << 8) | (static_cast<uint32_t>(h[2]) << 16)) >> 1;
        at += 3 + len;
    }
    return at < length ? at : length;
}

Bytes LeafCursor::open(const Range& r, Positions& at, Positions& until, size_t& start) {
    if (r.offset > src.size || r.length > src.size - r.offset) fail("a stream extends past the end of the file");
    if (src.compression == Compression::None) {
        start = static_cast<size_t>(at.take(where));
        if (until.e != nullptr) until.take(where);
        if (start > r.length) fail(where + "a row index position is past its stream (the file is corrupt)");
        return src.get(r.offset, r.length);
    }
    const uint64_t chunk = at.take(where);
    start = static_cast<size_t>(at.take(where));
    uint64_t end = r.length;
    if (until.e != nullptr) {
        // The next group's values begin in its chunk; a run that starts there may run on into the
        // next ones, so the window takes three chunks from it.
        const uint64_t next = until.take(where);
        until.take(where);
        if (next < chunk || next > r.length) fail(where + "a row index position is past its stream (the file is corrupt)");
        end = chunks_end(src, r.offset, r.length, next, 3);
    }
    if (chunk > r.length) fail(where + "a row index position is past its stream (the file is corrupt)");
    Bytes b = src.get(r.offset + chunk, end - chunk);
    if (start > b.n) fail(where + "a row index position is past its chunk (the file is corrupt)");
    return b;
}

void LeafCursor::seek(const RowIndexEntry* at_entry, const RowIndexEntry* until_entry) {
    Positions at{at_entry};
    Positions until{until_entry};
    size_t start = 0;
    // A run-length stream: the window, then `run` values into the run at the place.
    auto int_rle = [&](const Range& r, Bytes& b, std::optional<IntRle>& reader, bool is_signed) {
        b = open(r, at, until, start);
        reader.emplace(b.p + start, b.n - start, v2, is_signed);
        const uint64_t run = at.take(where);
        if (until.e != nullptr) until.take(where);
        reader->skip(static_cast<size_t>(run));
    };
    auto bits = [&](const Range& r, Bytes& b, std::optional<BitReader>& reader) {
        b = open(r, at, until, start);
        reader.emplace(b.p + start, b.n - start);
        const uint64_t run = at.take(where);
        const uint64_t bit = at.take(where);
        if (until.e != nullptr) {
            until.take(where);
            until.take(where);
        }
        reader->seek(run, bit);
    };
    auto raw_data = [&]() {
        data_b = open(data_r, at, until, start);
        raw = data_b.p + start;
        raw_end = data_b.p + data_b.n;
    };

    if (present_r.exists) bits(present_r, present_b, present);
    switch (fc.kind) {
        case Kind::Boolean: bits(data_r, data_b, bools); break;
        case Kind::Byte: {
            data_b = open(data_r, at, until, start);
            bytes.emplace(data_b.p + start, data_b.n - start);
            const uint64_t run = at.take(where);
            if (until.e != nullptr) until.take(where);
            bytes->skip(static_cast<size_t>(run));
            break;
        }
        case Kind::Short: case Kind::Int: case Kind::Long: case Kind::Date: int_rle(data_r, data_b, ints, true); break;
        case Kind::Float: case Kind::Double: raw_data(); break;
        case Kind::String: case Kind::Varchar: case Kind::Char: case Kind::Binary:
            if (dictionary) {
                int_rle(data_r, data_b, ints, false);
            } else {
                raw_data();
                int_rle(length_r, length_b, lengths, false);
            }
            break;
        case Kind::Timestamp: case Kind::TimestampInstant:
            int_rle(data_r, data_b, ints, true);
            int_rle(secondary_r, secondary_b, secondary, false);
            break;
        case Kind::Decimal:
            raw_data();
            int_rle(secondary_r, secondary_b, secondary, true);
            break;
        default: fail(where + std::string(kind_name(fc.kind)) + " columns are not supported");
    }
    at.finish(where);
}

void LeafCursor::read(uint32_t rows, Column& out) {
    // PRESENT: this range's validity; null when every row has a value.
    Presence pres;
    pres.nonnull = rows;
    if (present) {
        draken::AppendBuffer<uint8_t> bitmap;
        bitmap.resize_fill(pad8((static_cast<size_t>(rows) + 7) / 8), 0);
        pres.nonnull = present->read_bitmap(rows, bitmap.data());
        if (pres.nonnull != rows) pres.validity = bitmap.release();
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

    out.type = type;
    out.length = rows;
    out.precision = precision;
    out.scale = scale;

    // Nullable output retains zeroes at null rows. All-valid output is entirely overwritten.
    auto fixed_out = [&](size_t width) {
        draken::AppendBuffer<uint8_t> buf;
        const size_t n = static_cast<size_t>(rows) * width;
        if (all_valid) buf.resize_uninit(n);
        else buf.resize_fill(n, 0);
        return buf;
    };
    auto raw_left = [&]() { return static_cast<size_t>(raw_end - raw); };
    const uint8_t* validity = pres.validity;

    switch (fc.kind) {
        case Kind::Boolean: {
            std::vector<uint8_t> vals(nn);
            bools->read_bools(nn, vals.data());
            auto* bits = alloc_zero(pad8((static_cast<size_t>(rows) + 7) / 8));
            size_t k = 0;
            for (uint32_t r = 0; r < rows; ++r) {
                if (!present_at(validity, r)) continue;
                if (vals[k++]) bits[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
            }
            out.kind = OutKind::Raw;
            out.data = bits;
            break;
        }
        case Kind::Byte: {
            const size_t width = width_of(type);
            draken::AppendBuffer<uint8_t> buf = fixed_out(width);
            switch (width) {
                case 1: read_bytes_into(*bytes, reinterpret_cast<int8_t*>(buf.data()), nn, validity); break;
                case 2: read_bytes_into(*bytes, reinterpret_cast<int16_t*>(buf.data()), nn, validity); break;
                case 4: read_bytes_into(*bytes, reinterpret_cast<int32_t*>(buf.data()), nn, validity); break;
                default: read_bytes_into(*bytes, reinterpret_cast<int64_t*>(buf.data()), nn, validity); break;
            }
            out.kind = OutKind::Raw;
            out.data = buf.release();
            break;
        }
        case Kind::Short: case Kind::Int: case Kind::Long: case Kind::Date: {
            const size_t width = width_of(type);
            draken::AppendBuffer<uint8_t> buf = fixed_out(width);
            switch (width) {
                case 2: read_ints_into(*ints, reinterpret_cast<int16_t*>(buf.data()), nn, validity); break;
                case 4: read_ints_into(*ints, reinterpret_cast<int32_t*>(buf.data()), nn, validity); break;
                default: read_ints_into(*ints, reinterpret_cast<int64_t*>(buf.data()), nn, validity); break;
            }
            out.kind = fc.type == DRAKEN_TIME64 ? OutKind::Time64 : OutKind::Raw;
            out.data = buf.release();
            break;
        }
        case Kind::Float: case Kind::Double: {
            const size_t w = fc.kind == Kind::Float ? 4 : 8;
            if (raw_left() < nn * w) fail(where + "its DATA stream is shorter than its values");
            draken::AppendBuffer<uint8_t> buf = fixed_out(width_of(type));
            uint8_t* d = buf.data();
            if (all_valid && w == width_of(type)) {
                if (rows != 0) std::memcpy(d, raw, static_cast<size_t>(rows) * w);
            } else {
                size_t k = 0;
                for (uint32_t r = 0; r < rows; ++r) {
                    if (!present_at(validity, r)) continue;
                    const uint8_t* from = raw + (k++) * w;
                    if (fc.kind == Kind::Float) {
                        float f;
                        std::memcpy(&f, from, 4);
                        if (type == DRAKEN_FLOAT32) reinterpret_cast<float*>(d)[r] = f;
                        else reinterpret_cast<double*>(d)[r] = static_cast<double>(f);
                    } else {
                        double f;
                        std::memcpy(&f, from, 8);
                        reinterpret_cast<double*>(d)[r] = f;
                    }
                }
            }
            raw += nn * w;
            out.kind = OutKind::Raw;
            out.data = buf.release();
            break;
        }
        case Kind::String: case Kind::Varchar: case Kind::Char: case Kind::Binary: {
            if (fc.type == DRAKEN_UUID) {
                // A BINARY column marked iceberg.binary-type=UUID: exactly 16 bytes per value, in
                // network order, into a 16-byte lane. Anything else is not a uuid - fail, naming it.
                scratch.resize(nn);
                lengths->read(scratch.data(), nn);
                for (size_t i = 0; i < nn; ++i)
                    if (scratch[i] != 16) fail(where + "a uuid value is " + std::to_string(scratch[i]) + " bytes, not 16");
                if (raw_left() < nn * 16) fail(where + "its DATA stream is shorter than its uuid values");
                draken::AppendBuffer<uint8_t> buf = fixed_out(16);
                for (uint32_t r = 0; r < rows; ++r) {
                    if (!present_at(validity, r)) continue;
                    std::memcpy(buf.data() + static_cast<size_t>(r) * 16, raw, 16);
                    raw += 16;
                }
                out.kind = OutKind::Raw;
                out.data = buf.release();
                break;
            }
            if (!dictionary) {
                scratch.resize(nn);
                lengths->read(scratch.data(), nn);
                if (fc.fixed_length != 0)
                    for (size_t i = 0; i < nn; ++i)
                        if (scratch[i] != static_cast<int64_t>(fc.fixed_length))
                            fail(where + "a fixed(" + std::to_string(fc.fixed_length) + ") value is " + std::to_string(scratch[i]) + " bytes");
                uint64_t total = 0;
                for (size_t i = 0; i < nn; ++i) {
                    if (scratch[i] < 0) fail(where + "a string has a negative length");
                    total += static_cast<uint64_t>(scratch[i]);
                }
                if (total > raw_left()) fail(where + "its DATA stream is shorter than its string lengths");
                draken::AppendBuffer<DrakenStringSlot> slots;
                draken::AppendBuffer<uint8_t> arena;
                slots.resize_uninit(rows);
                arena.reserve(static_cast<size_t>(total));
                size_t k = 0;
                for (uint32_t r = 0; r < rows; ++r) {
                    if (!present_at(validity, r)) {
                        str_init_null(&slots[r]);
                        continue;
                    }
                    const uint64_t len = static_cast<uint64_t>(scratch[k++]);
                    build_slot(slots, arena, r, raw, len);
                    raw += len;
                }
                out.kind = OutKind::String;
                out.arena_len = arena.size();
                out.slots = slots.release();
                out.arena = arena.release();
                break;
            }
            scratch.resize(nn);
            ints->read(scratch.data(), nn);
            if (dsize == 0 && nn != 0) fail(where + "it has values but an empty dictionary");
            // The entries this range uses, in first-use order, become its dictionary.
            draken::AppendBuffer<uint32_t> codes;
            codes.resize_fill(rows, 0);
            touched.clear();
            size_t k = 0;
            for (uint32_t r = 0; r < rows; ++r) {
                if (!present_at(validity, r)) continue;
                const int64_t c = scratch[k++];
                if (c < 0 || static_cast<uint64_t>(c) >= dsize) fail(where + "a dictionary index is out of range");
                uint32_t& code = remap[static_cast<size_t>(c)];
                if (code == kNone) {
                    code = static_cast<uint32_t>(touched.size());
                    touched.push_back(static_cast<uint32_t>(c));
                }
                codes[r] = code;
            }
            if (touched.empty()) {   // every row of the range is null
                draken::AppendBuffer<DrakenStringSlot> slots;
                slots.resize_uninit(rows);
                for (uint32_t r = 0; r < rows; ++r) str_init_null(&slots[r]);
                out.kind = OutKind::String;
                out.slots = slots.release();
                break;
            }
            draken::AppendBuffer<DrakenStringSlot> cslots;
            draken::AppendBuffer<uint8_t> carena;
            cslots.resize_uninit(touched.size());
            for (size_t j = 0; j < touched.size(); ++j) {
                const uint32_t e = touched[j];
                const DrakenStringSlot* from = &dslots[e];
                build_slot(cslots, carena, j, str_data(from, darena), str_length(from));
                remap[e] = kNone;
            }
            out.kind = OutKind::Dict;
            out.dict_len = static_cast<uint32_t>(touched.size());
            out.arena_len = carena.size();
            out.slots = cslots.release();
            out.arena = carena.release();
            out.codes = codes.release();
            break;
        }
        case Kind::Timestamp: case Kind::TimestampInstant: {
            draken::AppendBuffer<uint8_t> buf = fixed_out(sizeof(int64_t));
            auto* values = reinterpret_cast<int64_t*>(buf.data());
            size_t row = 0;
            PresentCursor cursor{validity};
            IntRle& nanos = *secondary;
            ints->read_runs(nn, [&](const int64_t* secs, size_t n) {
                nanos.read_runs(n, [&](const int64_t* fractions, size_t count) {
                    const size_t first = row;
                    uint64_t offsets = 0;
                    if (all_valid) {
                        for (size_t i = 0; i < count; ++i, ++row) {
                            offsets |= timestamp_seconds_offset(secs[i]);
                            values[row] = timestamp_micros_in_window(secs[i], fractions[i], where);
                        }
                    } else {
                        for (size_t i = 0; i < count; ++i) {
                            row = cursor.next();
                            offsets |= timestamp_seconds_offset(secs[i]);
                            values[row++] = timestamp_micros_in_window(secs[i], fractions[i], where);
                        }
                    }
                    if (!timestamp_offsets_in_window(offsets)) [[unlikely]]
                        timestamp_micros_run_checked(values, first, validity, secs, fractions, count, where);
                    secs += count;
                });
            });
            out.kind = OutKind::Timestamp;
            out.data = buf.release();
            break;
        }
        case Kind::Decimal: {
            scratch.resize(nn);
            secondary->read(scratch.data(), nn);
            const size_t w = width_of(type);
            draken::AppendBuffer<uint8_t> buf = fixed_out(w);
            const unsigned __int128 limit = pow10_128(precision);
            const uint8_t* p = raw;
            size_t k = 0;
            for (uint32_t r = 0; r < rows; ++r) {
                if (!present_at(validity, r)) continue;
                __int128 v = read_varint128(p, raw_end);
                const int64_t s = scratch[k++];
                if (s < 0 || s > 38) fail(where + "a decimal value has scale " + std::to_string(s));
                const int target = scale;
                if (s < target) {
                    v *= static_cast<__int128>(pow10_128(static_cast<unsigned>(target - s)));
                } else if (s > target) {
                    const __int128 div = static_cast<__int128>(pow10_128(static_cast<unsigned>(s - target)));
                    if (v % div != 0) fail(where + "a decimal value cannot be rescaled to scale " + std::to_string(target) + " exactly");
                    v /= div;
                }
                const unsigned __int128 mag = v < 0 ? static_cast<unsigned __int128>(-v) : static_cast<unsigned __int128>(v);
                if (mag >= limit) fail(where + "a decimal value exceeds precision " + std::to_string(precision));
                if (w == 16) std::memcpy(buf.data() + static_cast<size_t>(r) * 16, &v, 16);
                else *reinterpret_cast<int64_t*>(buf.data() + static_cast<size_t>(r) * 8) = static_cast<int64_t>(v);
            }
            raw = p;
            out.kind = w == 16 ? OutKind::Decimal128 : OutKind::Decimal64;
            out.data = buf.release();
            break;
        }
        default:
            fail(where + std::string(kind_name(fc.kind)) + " columns are not supported");
    }
    out.validity = take_validity();
}

}  // namespace

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
    std::vector<uint8_t> validity; // LSB-first bitmap; empty = every entry is present
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
    return present_at(nd.validity.empty() ? nullptr : nd.validity.data(), e);
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
    if (nd.validity.empty()) return nullptr;
    uint8_t* v = alloc_zero(pad8(nd.validity.size()));
    std::memcpy(v, nd.validity.data(), nd.validity.size());
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
        if (present_at(list.validity.empty() ? nullptr : list.validity.data(), r)) at += list.lens[k++];
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
        LeafCursor leaf(src, sf, streams, nd.fc, nd.fc.type, nd.fc.precision, nd.fc.scale);
        leaf.seek(nullptr, nullptr);
        leaf.read(static_cast<uint32_t>(n), nd.leaf);
        return nd;
    }

    // PRESENT: one bit per entry; absent when every entry is present.
    size_t present = n;
    auto it = streams.find({id, static_cast<uint32_t>(StreamKind::Present)});
    if (it != streams.end()) {
        const Bytes pb = stream(it->second.first, it->second.second);
        nd.validity.resize((n + 7) / 8);
        present = read_present(pb.p, pb.n, n, nd.validity.data());
        if (present == n) nd.validity.clear();
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

// ── stripe cursor ───────────────────────────────────────────────────────────

namespace {

// `n` bits of LSB-first `src` from bit `from`, into `dst` (zeroed) from bit 0.
void copy_bits(const uint8_t* src, size_t from, size_t n, uint8_t* dst) {
    if ((from & 7) == 0) {
        if (n == 0) return;
        std::memcpy(dst, src + from / 8, (n + 7) / 8);
        if ((n & 7) != 0) dst[n / 8] &= static_cast<uint8_t>((1u << (n & 7)) - 1);
        return;
    }
    for (size_t i = 0; i < n; ++i)
        if ((src[(from + i) >> 3] >> ((from + i) & 7)) & 1) dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
}

// Entries [from, from + n) of a validity bitmap; null stays null (every entry present).
uint8_t* slice_validity(const uint8_t* v, size_t from, size_t n) {
    if (v == nullptr) return nullptr;
    uint8_t* out = alloc_zero(pad8((n + 7) / 8));
    copy_bits(v, from, n, out);
    return out;
}

// Rows [from, from + n) of an Array column (fill_array's shape), copied.
void slice_array(const Column& src, size_t from, size_t n, Column& out) {
    out.kind = OutKind::Array;
    out.type = DRAKEN_ARRAY;
    out.length = static_cast<uint32_t>(n);
    out.validity = slice_validity(src.validity, from, n);
    draken::AppendBuffer<uint8_t> off_buf;
    off_buf.resize_uninit((n + 1) * sizeof(int32_t));
    int32_t* offsets = reinterpret_cast<int32_t*>(off_buf.data());
    const int32_t base = src.offsets[from];
    for (size_t i = 0; i <= n; ++i) offsets[i] = src.offsets[from + i] - base;
    out.offsets = reinterpret_cast<int32_t*>(off_buf.release());
    const size_t cn = static_cast<size_t>(offsets[n]);
    out.child_type = src.child_type;
    out.child_length = static_cast<uint32_t>(cn);
    if (src.child_type == DRAKEN_ARRAY) {
        out.child_col = std::make_unique<Column>();
        slice_array(*src.child_col, static_cast<size_t>(base), cn, *out.child_col);
        return;
    }
    out.child_validity = slice_validity(src.child_validity, static_cast<size_t>(base), cn);
    if (src.child_type == DRAKEN_VARCHAR || src.child_type == DRAKEN_VARBINARY) {
        draken::AppendBuffer<DrakenStringSlot> slots;
        draken::AppendBuffer<uint8_t> arena;
        slots.resize_uninit(cn);
        for (size_t i = 0; i < cn; ++i) {
            const size_t e = static_cast<size_t>(base) + i;
            if (!present_at(src.child_validity, e)) {
                str_init_null(&slots[i]);
                continue;
            }
            const DrakenStringSlot* slot = &src.slots[e];
            build_slot(slots, arena, i, str_data(slot, src.arena), str_length(slot));
        }
        out.data = alloc_zero(16);
        out.arena_len = arena.size();
        out.slots = slots.release();
        out.arena = arena.release();
        return;
    }
    const uint8_t* d = static_cast<const uint8_t*>(src.data);
    if (src.child_type == DRAKEN_BOOL) {
        uint8_t* bits = alloc_zero(pad8((cn + 7) / 8));
        copy_bits(d, static_cast<size_t>(base), cn, bits);
        out.data = bits;
        return;
    }
    const size_t w = width_of(src.child_type);
    uint8_t* values = alloc_bytes(cn * w);
    if (cn != 0) std::memcpy(values, d + static_cast<size_t>(base) * w, cn * w);
    out.data = values;
}

// Rows [from, from + rows) of a JSON-rendered nested column. The node's render cursors carry on
// from the previous range, so the ranges must come in order.
void render_json(NestedNode& top, size_t from, uint32_t rows, DrakenType type, Column& out) {
    const uint8_t* top_validity = top.validity.empty() ? nullptr : top.validity.data();
    out.type = type;
    out.length = rows;
    std::string all;
    std::vector<uint64_t> ends(rows);
    for (uint32_t r = 0; r < rows; ++r) {
        if (present_at(top_validity, from + r)) render_value(all, top, from + r);
        ends[r] = all.size();
    }
    draken::AppendBuffer<DrakenStringSlot> slots;
    draken::AppendBuffer<uint8_t> arena;
    slots.resize_uninit(rows);
    uint64_t at = 0;
    for (uint32_t r = 0; r < rows; ++r) {
        if (!present_at(top_validity, from + r)) str_init_null(&slots[r]);
        else build_slot(slots, arena, r, reinterpret_cast<const uint8_t*>(all.data()) + at, ends[r] - at);
        at = ends[r];
    }
    out.kind = OutKind::String;
    out.arena_len = arena.size();
    out.slots = slots.release();
    out.arena = arena.release();
    out.validity = slice_validity(top_validity, from, rows);
}

}  // namespace

// One bound column of the stripe: a primitive's cursor, a nested column decoded whole (rendered
// or sliced a range at a time), or none (a NULL constant).
struct StripeCursor::Col {
    const ReaderColumn* rc = nullptr;
    uint32_t type_id = 0;                 // a primitive: its type id (the column of its ROW_INDEX)
    std::unique_ptr<LeafCursor> leaf;
    std::vector<RowIndexEntry> index;     // a primitive: the stripe's row index, once read
    std::unique_ptr<NestedNode> json;
    uint32_t json_at = 0;                 // the next row the JSON renderer stands at
    std::unique_ptr<Column> array;
};

// What the cursor keeps of the stripe: where its streams are, and its row groups.
struct StripeCursor::Stripe {
    const StreamSource* src = nullptr;
    StreamMap streams;
    uint32_t stride = 0;     // rows per row group; 0 = the file has no row index
    int indexed = -1;        // -1 = not yet looked; 0 / 1
};

StripeCursor::StripeCursor(const OrcFile& file, size_t stripe, const std::vector<uint32_t>* columns)
    : stripe_(new Stripe) {
    const OrcFile::Impl& m = *file.impl_;
    const StripeInfo& si = m.footer.stripes.at(stripe);
    rows_ = static_cast<uint32_t>(si.rows);
    stripe_->src = &m.src;
    stripe_->stride = m.footer.row_index_stride;
    std::vector<uint32_t> all;
    if (columns == nullptr) {
        for (uint32_t i = 0; i < m.plan.size(); ++i) all.push_back(i);
        columns = &all;
    }
    if (columns->empty()) return;

    // The stripe footer names every stream; offsets are the running sum of their lengths,
    // index streams first, so a column that is not asked for is never touched.
    const uint64_t footer_at = si.offset + si.index_length + si.data_length;
    const Bytes footer_bytes = m.stream(footer_at, si.footer_length);
    const StripeFooter sf = parse_stripe_footer({footer_bytes.p, footer_bytes.n});
    StreamMap& streams = stripe_->streams;
    uint64_t at = si.offset;
    for (const StreamInfo& s : sf.streams) {
        streams[{s.column, s.kind}] = {at, s.length};
        at += s.length;
    }
    if (at > footer_at) fail("a stripe's streams extend past its data (the file is corrupt)");

    cols_.reserve(columns->size());
    for (const uint32_t i : *columns) {
        if (i >= m.plan.size()) fail("a stripe cursor was asked for a column that is not bound");
        const ColPlan& cp = m.plan[i];
        auto c = std::make_unique<Col>();
        c->rc = &cp.rc;
        if (cp.file_col >= 0) {
            const FileColumn& fc = m.columns[static_cast<size_t>(cp.file_col)];
            if (!fc.nested) {
                c->type_id = fc.type_id;
                c->leaf = std::make_unique<LeafCursor>(m.src, sf, streams, fc, cp.rc.type, cp.rc.precision, cp.rc.scale);
            } else {
                // A declared shape applies to a column that renders as JSON; an ARRAY (a list of a plain
                // scalar) has no names to drift.
                std::unique_ptr<DeclCtx> decl;
                if (!cp.rc.declared.empty() && fc.type == DRAKEN_NVARCHAR) decl = std::make_unique<DeclCtx>(cp.rc.declared);
                NestedNode top = m.build_node(sf, streams, fc.type_id, fc.name, std::string(), rows_, decl.get(), decl ? 0 : -1);
                if (fc.type == DRAKEN_ARRAY) {
                    // A LIST of a plain scalar, or of lists of one: the offsets, and the element column as decoded.
                    c->array = std::make_unique<Column>();
                    fill_array(top, *c->array, validity_of(top));
                } else {
                    c->json = std::make_unique<NestedNode>(std::move(top));
                }
            }
        }
        cols_.push_back(std::move(c));
    }
}

StripeCursor::~StripeCursor() = default;

uint32_t StripeCursor::stride() const { return stripe_->stride; }

bool StripeCursor::indexed() {
    Stripe& st = *stripe_;
    if (st.indexed >= 0) return st.indexed == 1;
    st.indexed = 0;
    if (st.stride == 0) return false;
    const size_t groups = (static_cast<size_t>(rows_) + st.stride - 1) / st.stride;
    for (auto& c : cols_) {
        if (!c->leaf) continue;
        auto it = st.streams.find({c->type_id, static_cast<uint32_t>(StreamKind::RowIndex)});
        if (it == st.streams.end()) return false;
        const Bytes b = st.src->get(it->second.first, it->second.second);
        c->index = parse_row_index({b.p, b.n});
        if (c->index.size() != groups)
            fail(c->leaf->where + "its row index has " + std::to_string(c->index.size()) + " row groups, not " +
                 std::to_string(groups) + " (the file is corrupt)");
    }
    st.indexed = 1;
    return true;
}

Batch StripeCursor::next(uint32_t n) { return read(at_, n, rows_); }

Batch StripeCursor::read(uint32_t first, uint32_t n, uint32_t run_end) {
    if (first < at_ || n > run_end - first || run_end > rows_ || first > run_end)
        fail("a stripe was read out of order or past its last row");
    const bool contiguous = opened_ && first == at_ && first + n <= window_end_;
    if (!contiguous) {
        if (!opened_ && first == 0 && run_end == rows_) {
            for (auto& c : cols_)
                if (c->leaf) c->leaf->seek(nullptr, nullptr);
            window_end_ = rows_;
        } else if (indexed() && first % stripe_->stride == 0) {
            // Open every primitive at the row group `first` starts, as far as the run needs.
            const uint32_t stride = stripe_->stride;
            const size_t group = first / stride;
            const size_t until = (static_cast<size_t>(run_end) + stride - 1) / stride;
            for (auto& c : cols_) {
                if (!c->leaf) continue;
                c->leaf->seek(&c->index[group], until < c->index.size() ? &c->index[until] : nullptr);
            }
            window_end_ = until < (static_cast<size_t>(rows_) + stride - 1) / stride ? static_cast<uint32_t>(until * stride) : rows_;
        } else if (!opened_ || window_end_ == rows_) {
            // No row index to seek by: read on from where the streams stand, dropping the rows between.
            if (!opened_) {
                for (auto& c : cols_)
                    if (c->leaf) c->leaf->seek(nullptr, nullptr);
                window_end_ = rows_;
                at_ = 0;
            }
            if (first > at_) {
                for (auto& c : cols_) {
                    if (!c->leaf) continue;
                    Column dropped;
                    c->leaf->read(first - at_, dropped);
                }
            }
        } else {
            fail("a stripe cursor read starts inside a row group it has not opened");
        }
        opened_ = true;
    }

    Batch batch;
    batch.rows = n;
    batch.columns.resize(cols_.size());
    for (size_t i = 0; i < cols_.size(); ++i) {
        Col& c = *cols_[i];
        Column& out = batch.columns[i];
        if (c.leaf) {
            c.leaf->read(n, out);
        } else if (c.json) {
            if (first > c.json_at) {   // the renderer's cursors only move forward, row by row
                Column dropped;
                render_json(*c.json, c.json_at, first - c.json_at, c.rc->type, dropped);
            }
            render_json(*c.json, first, n, c.rc->type, out);
            c.json_at = first + n;
        } else if (c.array && first == 0 && n == rows_) {
            out = std::move(*c.array);
        } else if (c.array) {
            slice_array(*c.array, first, n, out);
        } else {
            null_constant(*c.rc, n, out);
        }
    }
    at_ = first + n;
    return batch;
}

Batch OrcFile::decode_stripe(size_t stripe) const {
    StripeCursor cursor(*this, stripe);
    return cursor.next(cursor.rows());
}

}  // namespace rugo::orc
