#pragma once
// rugo/src/orc/orc_proto.hpp — the protobuf subset an ORC file's metadata needs, and the
// ORC metadata messages decoded with it. Pure C++, header-only, no protobuf library
// (docs/ORC_READER_DESIGN.md §3.1).
//
// Every read is bounds-checked: a truncated or corrupt message throws
// std::runtime_error, never reads past its buffer. Unknown fields are skipped, so a
// newer writer's extra fields cost nothing.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace rugo::orc {

[[noreturn]] inline void fail(const std::string& msg) { throw std::runtime_error("ORC: " + msg); }

// ── protobuf wire reader ────────────────────────────────────────────────────

struct Slice {
    const uint8_t* p = nullptr;
    size_t n = 0;
};

class ProtoReader {
public:
    ProtoReader(const uint8_t* p, size_t n) : p_(p), end_(p + n) {}
    explicit ProtoReader(Slice s) : ProtoReader(s.p, s.n) {}

    // The next field's number and wire type; false at the end of the message.
    bool next(uint32_t& field, uint32_t& wire) {
        if (p_ == end_) return false;
        const uint64_t key = varint();
        field = static_cast<uint32_t>(key >> 3);
        wire = static_cast<uint32_t>(key & 7);
        return true;
    }

    uint64_t varint() {
        uint64_t v = 0;
        for (unsigned shift = 0; shift < 70; shift += 7) {
            if (p_ == end_) fail("a protobuf varint runs past the end of its message");
            const uint8_t b = *p_++;
            v |= static_cast<uint64_t>(b & 0x7f) << shift;
            if ((b & 0x80) == 0) return v;
        }
        fail("a protobuf varint is longer than 10 bytes");
    }

    Slice bytes() {
        const uint64_t len = varint();
        if (len > static_cast<uint64_t>(end_ - p_)) fail("a protobuf field runs past the end of its message");
        Slice s{p_, static_cast<size_t>(len)};
        p_ += len;
        return s;
    }

    std::string str() {
        const Slice s = bytes();
        return std::string(reinterpret_cast<const char*>(s.p), s.n);
    }

    void skip(uint32_t wire) {
        switch (wire) {
            case 0: varint(); break;
            case 1: advance(8); break;
            case 2: bytes(); break;
            case 5: advance(4); break;
            default: fail("a protobuf field has an unsupported wire type " + std::to_string(wire));
        }
    }

    // A repeated scalar field that may arrive packed (one length-delimited run) or one
    // varint at a time; appends every value to `out`.
    void repeated_varint(uint32_t wire, std::vector<uint32_t>& out) {
        if (wire == 2) {
            ProtoReader inner(bytes());
            while (!inner.done()) out.push_back(static_cast<uint32_t>(inner.varint()));
        } else if (wire == 0) {
            out.push_back(static_cast<uint32_t>(varint()));
        } else {
            fail("a repeated integer field has wire type " + std::to_string(wire));
        }
    }

    // As repeated_varint, for 64-bit values.
    void repeated_varint64(uint32_t wire, std::vector<uint64_t>& out) {
        if (wire == 2) {
            ProtoReader inner(bytes());
            while (!inner.done()) out.push_back(inner.varint());
        } else if (wire == 0) {
            out.push_back(varint());
        } else {
            fail("a repeated integer field has wire type " + std::to_string(wire));
        }
    }

    bool done() const { return p_ == end_; }

private:
    void advance(size_t n) {
        if (n > static_cast<size_t>(end_ - p_)) fail("a protobuf field runs past the end of its message");
        p_ += n;
    }
    const uint8_t* p_;
    const uint8_t* end_;
};

// ── ORC metadata ────────────────────────────────────────────────────────────

enum class Compression : uint8_t { None = 0, Zlib = 1, Snappy = 2, Lzo = 3, Lz4 = 4, Zstd = 5 };

inline const char* compression_name(Compression c) {
    switch (c) {
        case Compression::None:   return "none";
        case Compression::Zlib:   return "zlib";
        case Compression::Snappy: return "snappy";
        case Compression::Lzo:    return "lzo";
        case Compression::Lz4:    return "lz4";
        case Compression::Zstd:   return "zstd";
    }
    return "unknown";
}

enum class Kind : uint8_t {
    Boolean = 0, Byte, Short, Int, Long, Float, Double, String, Binary, Timestamp,
    List, Map, Struct, Union, Decimal, Date, Varchar, Char, TimestampInstant,
};

inline const char* kind_name(Kind k) {
    static const char* names[] = {"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "STRING",
                                  "BINARY", "TIMESTAMP", "LIST", "MAP", "STRUCT", "UNION", "DECIMAL",
                                  "DATE", "VARCHAR", "CHAR", "TIMESTAMP_INSTANT"};
    return static_cast<size_t>(k) < sizeof(names) / sizeof(names[0]) ? names[static_cast<size_t>(k)] : "unknown";
}

struct Type {
    Kind kind = Kind::Struct;
    std::vector<uint32_t> subtypes;
    std::vector<std::string> field_names;
    uint32_t precision = 0;
    uint32_t scale = 0;
    std::vector<std::pair<std::string, std::string>> attributes;

    const std::string* attribute(const char* key) const {
        for (const auto& kv : attributes)
            if (kv.first == key) return &kv.second;
        return nullptr;
    }
};

struct StripeInfo {
    uint64_t offset = 0;
    uint64_t index_length = 0;
    uint64_t data_length = 0;
    uint64_t footer_length = 0;
    uint64_t rows = 0;
};

struct PostScript {
    uint64_t footer_length = 0;
    uint64_t metadata_length = 0;
    Compression compression = Compression::None;
    uint64_t block_size = 0;
};

struct Footer {
    uint64_t content_length = 0;
    uint64_t num_rows = 0;
    uint32_t row_index_stride = 0;
    bool encrypted = false;
    std::vector<StripeInfo> stripes;
    std::vector<Type> types;
};

enum class StreamKind : uint32_t {
    Present = 0, Data = 1, Length = 2, DictionaryData = 3, DictionaryCount = 4, Secondary = 5,
    RowIndex = 6, BloomFilter = 7, BloomFilterUtf8 = 8,
};

struct StreamInfo {
    uint32_t kind = 0;
    uint32_t column = 0;
    uint64_t length = 0;
};

// ColumnEncoding kinds: DIRECT 0 (RLE v1), DICTIONARY 1 (RLE v1), DIRECT_V2 2, DICTIONARY_V2 3.
struct ColumnEncoding {
    uint32_t kind = 0;
    uint32_t dictionary_size = 0;
};

struct StripeFooter {
    std::vector<StreamInfo> streams;
    std::vector<ColumnEncoding> columns;
    std::string writer_timezone;
};

// Protobuf sint and ORC signed run-length values are zigzag encoded.
inline int64_t unzigzag(uint64_t v) { return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1); }

// What a ColumnStatistics says about an integer or date column (docs/ORC_READER_DESIGN.md §6.7):
// how many non-null values, and their range from intStatistics or dateStatistics. Nothing else
// is read.
struct ColumnStats {
    bool has_count = false;
    uint64_t count = 0;
    bool has_range = false;
    int64_t min = 0;
    int64_t max = 0;
};

inline ColumnStats parse_column_stats(Slice s) {
    ColumnStats cs;
    ProtoReader r(s);
    uint32_t field, wire;
    while (r.next(field, wire)) {
        if (field == 1 && wire == 0) {
            cs.has_count = true;
            cs.count = r.varint();
        } else if ((field == 2 || field == 7) && wire == 2) {
            // IntegerStatistics {sint64 minimum = 1; sint64 maximum = 2}; DateStatistics {sint32 ...}.
            ProtoReader ir(r.bytes());
            bool has_min = false, has_max = false;
            uint32_t f, w;
            while (ir.next(f, w)) {
                if (f == 1 && w == 0) {
                    cs.min = unzigzag(ir.varint());
                    has_min = true;
                } else if (f == 2 && w == 0) {
                    cs.max = unzigzag(ir.varint());
                    has_max = true;
                } else {
                    ir.skip(w);
                }
            }
            cs.has_range = has_min && has_max;
        } else {
            r.skip(wire);
        }
    }
    return cs;
}

// One row group of a column's ROW_INDEX stream: where each of the column's streams stands at
// the group's first row (docs/ORC_READER_DESIGN.md §6.5). Per stream, in the column's stream
// order: the compressed chunk's offset in the stream and the offset in its decompressed bytes
// (one byte offset when the file is uncompressed), then the run-length decoder's position. And
// the group's statistics (§6.7).
struct RowIndexEntry {
    std::vector<uint64_t> positions;
    ColumnStats stats;
};

inline std::vector<RowIndexEntry> parse_row_index(Slice s) {
    std::vector<RowIndexEntry> entries;
    ProtoReader r(s);
    uint32_t field, wire;
    while (r.next(field, wire)) {
        if (field != 1) {
            r.skip(wire);
            continue;
        }
        RowIndexEntry e;
        ProtoReader er(r.bytes());
        uint32_t f, w;
        while (er.next(f, w)) {
            if (f == 1) er.repeated_varint64(w, e.positions);
            else if (f == 2 && w == 2) e.stats = parse_column_stats(er.bytes());
            else er.skip(w);
        }
        entries.push_back(std::move(e));
    }
    return entries;
}

// The file's Metadata section: per stripe, per type id, the stripe's column statistics (§6.7).
inline std::vector<std::vector<ColumnStats>> parse_metadata(Slice s) {
    std::vector<std::vector<ColumnStats>> stripes;
    ProtoReader r(s);
    uint32_t field, wire;
    while (r.next(field, wire)) {
        if (field != 1 || wire != 2) {
            r.skip(wire);
            continue;
        }
        std::vector<ColumnStats> columns;
        ProtoReader sr(r.bytes());
        uint32_t f, w;
        while (sr.next(f, w)) {
            if (f == 1 && w == 2) columns.push_back(parse_column_stats(sr.bytes()));
            else sr.skip(w);
        }
        stripes.push_back(std::move(columns));
    }
    return stripes;
}

inline PostScript parse_postscript(Slice s) {
    PostScript ps;
    bool magic = false;
    ProtoReader r(s);
    uint32_t field, wire;
    while (r.next(field, wire)) {
        switch (field) {
            case 1: ps.footer_length = r.varint(); break;
            case 2: ps.compression = static_cast<Compression>(r.varint()); break;
            case 3: ps.block_size = r.varint(); break;
            case 5: ps.metadata_length = r.varint(); break;
            case 8000: magic = r.str() == "ORC"; break;
            default: r.skip(wire);
        }
    }
    if (!magic) fail("the postscript does not end with the magic \"ORC\" (not an ORC file, or corrupt)");
    if (static_cast<uint8_t>(ps.compression) > static_cast<uint8_t>(Compression::Zstd))
        fail("the file declares compression kind " + std::to_string(static_cast<unsigned>(ps.compression)) +
             ", which this reader does not know");
    return ps;
}

inline Type parse_type(Slice s) {
    Type t;
    ProtoReader r(s);
    uint32_t field, wire;
    while (r.next(field, wire)) {
        switch (field) {
            case 1: {
                const uint64_t k = r.varint();
                if (k > static_cast<uint64_t>(Kind::TimestampInstant))
                    fail("a column has type kind " + std::to_string(k) + ", which this reader does not know");
                t.kind = static_cast<Kind>(k);
                break;
            }
            case 2: r.repeated_varint(wire, t.subtypes); break;
            case 3: t.field_names.push_back(r.str()); break;
            case 5: t.precision = static_cast<uint32_t>(r.varint()); break;
            case 6: t.scale = static_cast<uint32_t>(r.varint()); break;
            case 7: {
                ProtoReader pair(r.bytes());
                std::string key, value;
                uint32_t f, w;
                while (pair.next(f, w)) {
                    if (f == 1) key = pair.str();
                    else if (f == 2) value = pair.str();
                    else pair.skip(w);
                }
                t.attributes.emplace_back(std::move(key), std::move(value));
                break;
            }
            default: r.skip(wire);
        }
    }
    return t;
}

inline StripeInfo parse_stripe_info(Slice s) {
    StripeInfo si;
    ProtoReader r(s);
    uint32_t field, wire;
    while (r.next(field, wire)) {
        switch (field) {
            case 1: si.offset = r.varint(); break;
            case 2: si.index_length = r.varint(); break;
            case 3: si.data_length = r.varint(); break;
            case 4: si.footer_length = r.varint(); break;
            case 5: si.rows = r.varint(); break;
            default: r.skip(wire);
        }
    }
    return si;
}

inline Footer parse_footer(Slice s) {
    Footer f;
    ProtoReader r(s);
    uint32_t field, wire;
    while (r.next(field, wire)) {
        switch (field) {
            case 2: f.content_length = r.varint(); break;
            case 3: f.stripes.push_back(parse_stripe_info(r.bytes())); break;
            case 4: f.types.push_back(parse_type(r.bytes())); break;
            case 6: f.num_rows = r.varint(); break;
            case 8: f.row_index_stride = static_cast<uint32_t>(r.varint()); break;
            case 10: r.skip(wire); f.encrypted = true; break;
            default: r.skip(wire);
        }
    }
    return f;
}

inline StripeFooter parse_stripe_footer(Slice s) {
    StripeFooter sf;
    ProtoReader r(s);
    uint32_t field, wire;
    while (r.next(field, wire)) {
        switch (field) {
            case 1: {
                ProtoReader sr(r.bytes());
                StreamInfo si;
                uint32_t f, w;
                while (sr.next(f, w)) {
                    if (f == 1) si.kind = static_cast<uint32_t>(sr.varint());
                    else if (f == 2) si.column = static_cast<uint32_t>(sr.varint());
                    else if (f == 3) si.length = sr.varint();
                    else sr.skip(w);
                }
                sf.streams.push_back(si);
                break;
            }
            case 2: {
                ProtoReader cr(r.bytes());
                ColumnEncoding ce;
                uint32_t f, w;
                while (cr.next(f, w)) {
                    if (f == 1) ce.kind = static_cast<uint32_t>(cr.varint());
                    else if (f == 2) ce.dictionary_size = static_cast<uint32_t>(cr.varint());
                    else cr.skip(w);
                }
                sf.columns.push_back(ce);
                break;
            }
            case 3: sf.writer_timezone = r.str(); break;
            case 4: fail("the stripe is encrypted, which this reader does not support");
            default: r.skip(wire);
        }
    }
    return sf;
}

}  // namespace rugo::orc
