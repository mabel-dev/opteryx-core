// Compression is a PURE SIZE OPTIMIZATION — swept across the type system.
//
// test_compression covers the codec's own behaviour on a couple of shapes.
// This file covers the OTHER direction: that the property holds for every type
// and every selection shape the format can hold, at the sizes where the writer's
// compression gate is off (empty, one row) as well as where it fires.
//
// The assertion is always the same and is deliberately not "the file reads
// back": it is "the file reads back IDENTICALLY to the same morsel written with
// no codec at all". A codec that quietly reshaped a column — dropped a validity
// bitmap, re-derived a dictionary, lost a logical descriptor — would satisfy a
// self-consistency check and fail this one.
//
// Every case runs against EVERY codec. A codec added later that only inherited
// the sweep written for the first would be a codec whose own edges are untested.

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "core/interval_slot.h"

#include "build_vectors.h"
#include "harness.h"
#include "skene/format.h"
#include "skene/reader.h"
#include "skene/writer.h"

using namespace skene;
using namespace skene_test;

namespace {

struct Codec {
    const char*  name;
    SectionCodec codec;
    int          level;
};

const Codec kCodecs[] = {
    {"zstd-3", SectionCodec::kZstd, 3},
    {"lz4",    SectionCodec::kLz4,  0},
};

// The case currently under test, so a failure names it rather than leaving a
// line number in a sweep of hundreds.
std::string g_case;

void fail_case(int line, const char* what, const std::string& detail) {
    skene_test::report(__FILE__, line, what, g_case + ": " + detail);
}

std::vector<uint8_t> write(const CxxMorsel& m, SectionCodec codec, int level,
                           bool acceleration) {
    WriteOptions options;
    options.read_acceleration = acceleration;
    options.codec = codec;
    options.zstd_level = level;
    std::vector<uint8_t> bytes;
    Status st = write_morsel(m, options, &bytes);
    if (!st.is_ok()) fail_case(__LINE__, "write failed", st.message());
    return bytes;
}

// ─── Comparison ─────────────────────────────────────────────────────────────

void compare_vectors(const DrakenVector& a, const DrakenVector& b,
                     const std::string& where);

// The physical bytes behind `data`, by type family. Comparing these rather than
// element-by-element catches a codec that decoded to the right VALUES through a
// buffer of the wrong size — which is the failure mode a length mismatch would
// otherwise hide behind a selection that never reads the tail.
void compare_data(const DrakenVector& a, const DrakenVector& b,
                  const std::string& where) {
    if (a.data == nullptr || b.data == nullptr) {
        ++skene_test::g_checks;
        if ((a.data == nullptr) != (b.data == nullptr))
            fail_case(__LINE__, "one side has no data buffer", where);
        return;
    }

    if (draken_type_is_string_storage(a.type)) {
        const DrakenStringArena* x = static_cast<const DrakenStringArena*>(a.data);
        const DrakenStringArena* y = static_cast<const DrakenStringArena*>(b.data);
        CHECK_EQ(x->length, y->length);
        CHECK_EQ(x->arena_used, y->arena_used);
        CHECK_EQ(x->payloads_elided, y->payloads_elided);
        CHECK_EQ(static_cast<int>(x->type), static_cast<int>(y->type));
        ++skene_test::g_checks;
        if (std::memcmp(x->slots, y->slots,
                        static_cast<size_t>(x->length) * sizeof(DrakenStringSlot)) != 0)
            fail_case(__LINE__, "string slots differ", where);
        if (x->arena_used > 0) {
            ++skene_test::g_checks;
            if (std::memcmp(x->arena, y->arena,
                            static_cast<size_t>(x->arena_used)) != 0)
                fail_case(__LINE__, "string arena bytes differ", where);
        }
        return;
    }

    size_t bytes = 0;
    if (a.type == DRAKEN_BOOL) {
        bytes = (static_cast<size_t>(a.data_length) + 7u) / 8u;
    } else if (a.type == DRAKEN_ARRAY) {
        // int32 offsets, one past the end.
        bytes = (static_cast<size_t>(a.data_length) + 1u) * sizeof(int32_t);
    } else if (a.type == DRAKEN_NULL) {
        return;  // self-describing: every row null, no data to compare
    } else {
        bytes = static_cast<size_t>(a.data_length)
              * draken_type_fixed_itemsize(a.type);
    }
    if (bytes == 0) return;
    ++skene_test::g_checks;
    if (std::memcmp(a.data, b.data, bytes) != 0)
        fail_case(__LINE__, "data bytes differ", where);
}

void compare_vectors(const DrakenVector& a, const DrakenVector& b,
                     const std::string& where) {
    CHECK_EQ(static_cast<int>(a.type), static_cast<int>(b.type));
    CHECK_EQ(a.length, b.length);
    CHECK_EQ(a.data_length, b.data_length);
    CHECK_EQ(a.flags, b.flags);

    ++skene_test::g_checks;
    if ((a.validity == nullptr) != (b.validity == nullptr))
        fail_case(__LINE__, "validity presence differs", where);
    if (a.validity != nullptr && b.validity != nullptr && a.length > 0) {
        // The bits ABOVE `length` in the final byte are padding and carry no
        // meaning, so they are masked out rather than required to match.
        const uint32_t whole = a.length / 8u;
        ++skene_test::g_checks;
        if (whole > 0 && std::memcmp(a.validity, b.validity, whole) != 0)
            fail_case(__LINE__, "validity bitmap differs", where);
        const uint32_t remainder = a.length % 8u;
        if (remainder != 0) {
            const uint8_t mask = static_cast<uint8_t>((1u << remainder) - 1u);
            ++skene_test::g_checks;
            if ((a.validity[whole] & mask) != (b.validity[whole] & mask))
                fail_case(__LINE__, "validity tail bits differ", where);
        }
    }

    // Uniform access is data[selection[i]], so the codes are as load-bearing as
    // the values — a dictionary re-derived rather than restored shows up here.
    for (uint32_t i = 0; i < a.length && i < b.length; ++i) {
        ++skene_test::g_checks;
        if (a.selection[i] != b.selection[i]) {
            fail_case(__LINE__, "selection code differs",
                      where + " at row " + std::to_string(i));
            break;
        }
    }

    compare_data(a, b, where);
}

void compare_morsels(const CxxMorsel& a, const CxxMorsel& b) {
    CHECK_EQ(a.num_columns(), b.num_columns());
    CHECK_EQ(a.num_rows(), b.num_rows());
    for (size_t c = 0; c < a.num_columns() && c < b.num_columns(); ++c) {
        ++skene_test::g_checks;
        if (a.names[c] != b.names[c])
            fail_case(__LINE__, "column name differs", a.names[c]);
        compare_vectors(a.columns[c].view, b.columns[c].view, a.names[c]);

        // Logical descriptors are INTERNED, so identity is the check — two equal
        // copies would mean the reader rebuilt one instead of resolving it.
        ++skene_test::g_checks;
        if (a.columns[c].own->logical_type != b.columns[c].own->logical_type)
            fail_case(__LINE__, "logical descriptor differs", a.names[c]);

        // ARRAY children ride along and must survive the same way.
        const VectorOwner* ac = a.columns[c].own->child_owner.get();
        const VectorOwner* bc = b.columns[c].own->child_owner.get();
        ++skene_test::g_checks;
        if ((ac == nullptr) != (bc == nullptr)) {
            fail_case(__LINE__, "array child presence differs", a.names[c]);
        } else if (ac != nullptr && bc != nullptr) {
            compare_vectors(ac->vec, bc->vec, a.names[c] + ".child");
        }
    }
}

// The whole point of the file, applied to one morsel: every codec must produce a
// file that reads back exactly as the uncompressed one does.
void check_every_codec(const CxxMorsel& in, const std::string& label) {
    for (bool acceleration : {false, true}) {
        g_case = label + (acceleration ? " [accelerated]" : " [plain]");

        const auto raw = write(in, SectionCodec::kNone, 0, acceleration);
        CxxMorsel raw_out;
        ++skene_test::g_checks;
        Status st = read_morsel(raw.data(), raw.size(), 0, ReadOptions(), &raw_out);
        if (!st.is_ok()) {
            fail_case(__LINE__, "uncompressed file did not read", st.message());
            continue;
        }

        for (const Codec& codec : kCodecs) {
            g_case = label + (acceleration ? " [accelerated] " : " [plain] ")
                   + codec.name;
            const auto packed = write(in, codec.codec, codec.level, acceleration);

            CxxMorsel packed_out;
            ++skene_test::g_checks;
            Status read = read_morsel(packed.data(), packed.size(), 0, ReadOptions(),
                                      &packed_out);
            if (!read.is_ok()) {
                fail_case(__LINE__, "compressed file did not read", read.message());
                continue;
            }
            compare_morsels(raw_out, packed_out);

            // A codec must never make a file bigger: each section falls back
            // independently when the compressed form is not smaller.
            ++skene_test::g_checks;
            if (packed.size() > raw.size())
                fail_case(__LINE__, "the codec made the file bigger",
                          std::to_string(raw.size()) + " -> " +
                          std::to_string(packed.size()));
        }
    }
}

// ─── Fixtures ───────────────────────────────────────────────────────────────

// Row counts: nothing, one, and enough that a section clears kCompressMinBytes
// (10240) so the codec actually engages. The first two are the edges where the
// writer declines and the round trip must still be exact.
const uint32_t kRowCounts[] = {0, 1, 4000};

template <typename T>
std::vector<T> ramp(uint32_t rows) {
    std::vector<T> values(rows);
    // Low cardinality, so there is real redundancy to compress, but not constant
    // — a constant column takes a different path entirely.
    for (uint32_t i = 0; i < rows; ++i) values[i] = static_cast<T>(i % 37);
    return values;
}

std::vector<bool> some_nulls(uint32_t rows) {
    std::vector<bool> valid(rows, true);
    for (uint32_t i = 0; i < rows; ++i) valid[i] = (i % 5) != 0;
    return valid;
}

std::vector<uint32_t> codes_for(uint32_t rows, uint32_t distinct) {
    std::vector<uint32_t> codes(rows);
    for (uint32_t i = 0; i < rows; ++i) codes[i] = i % distinct;
    return codes;
}

// One flat fixed-width type, in all three shapes, with and without nulls.
template <typename T>
void sweep_fixed(const char* name, DrakenType type,
                 const LogicalType* logical = nullptr) {
    for (uint32_t rows : kRowCounts) {
        const std::string label =
            std::string(name) + " x" + std::to_string(rows);

        check_every_codec(
            morsel_of({{"v", dense_column<T>(ramp<T>(rows), type, {}, logical)}}),
            label + " dense");

        if (rows > 0)
            check_every_codec(
                morsel_of({{"v", dense_column<T>(ramp<T>(rows), type,
                                                 some_nulls(rows), logical)}}),
                label + " dense+nulls");

        // A dictionary's codes are its own buffer, and restoring them verbatim
        // is the property the format exists for. At zero rows the dictionary is
        // EMPTY rather than holding one unreferenced value: a data_length of 1
        // over a length of 0 is a vector the reader rejects outright, which is a
        // property of the zero-row edge and not of any codec.
        const uint32_t distinct = rows == 0 ? 0u : (rows < 8u ? rows : 8u);
        std::vector<T> values(distinct);
        for (uint32_t i = 0; i < distinct; ++i) values[i] = static_cast<T>(i * 3 + 1);
        auto dict = dict_column<T>(values, codes_for(rows, distinct), type);
        dict.own->logical_type = logical;
        check_every_codec(morsel_of({{"v", std::move(dict)}}), label + " dict");

        if (rows > 0)
            check_every_codec(
                morsel_of({{"v", constant_column<T>(static_cast<T>(9), rows, type,
                                                    logical)}}),
                label + " constant");
    }
}

std::vector<std::string> text(uint32_t rows) {
    static const char* fragments[] = {
        "carefully regular accounts sleep against the",
        "short",
        "furiously bold requests wake quickly among the",
        "",
    };
    std::vector<std::string> values;
    values.reserve(rows);
    for (uint32_t i = 0; i < rows; ++i) {
        std::string s = fragments[i % 4];
        if (!s.empty()) s += " " + std::to_string(i % 53);
        values.push_back(s);
    }
    return values;
}

void sweep_strings() {
    const DrakenType types[] = {DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY,
                                DRAKEN_VARIANT};
    const char* names[] = {"VARCHAR", "NVARCHAR", "VARBINARY", "VARIANT"};
    for (size_t t = 0; t < 4; ++t) {
        for (uint32_t rows : kRowCounts) {
            const std::string label =
                std::string(names[t]) + " x" + std::to_string(rows);
            check_every_codec(
                morsel_of({{"s", string_column(text(rows), types[t])}}),
                label);
            if (rows > 0)
                check_every_codec(
                    morsel_of({{"s", string_column(text(rows), types[t],
                                                   some_nulls(rows))}}),
                    label + "+nulls");
        }
    }

    // Length-only: no arena at all, and every long slot stamped with the elided
    // trap offset. Losing `payloads_elided` here turns that trap into a 4GB
    // out-of-bounds read, so it has to survive a compressed round trip too.
    for (uint32_t rows : kRowCounts)
        check_every_codec(
            morsel_of({{"s", string_column(text(rows), DRAKEN_VARCHAR, {},
                                           /*elide=*/true)}}),
            "VARCHAR length-only x" + std::to_string(rows));
}

void sweep_bool() {
    for (uint32_t rows : kRowCounts) {
        std::vector<bool> bits(rows);
        for (uint32_t i = 0; i < rows; ++i) bits[i] = (i % 3) == 0;
        const std::string label = "BOOL x" + std::to_string(rows);
        check_every_codec(morsel_of({{"b", bool_column(bits)}}), label);
        if (rows > 0)
            check_every_codec(
                morsel_of({{"b", bool_column(bits, some_nulls(rows))}}),
                label + "+nulls");
    }
}

void sweep_array() {
    for (uint32_t rows : kRowCounts) {
        std::vector<std::vector<int64_t>> values;
        values.reserve(rows);
        for (uint32_t i = 0; i < rows; ++i) {
            std::vector<int64_t> row;
            for (uint32_t j = 0; j < (i % 4); ++j)
                row.push_back(static_cast<int64_t>(i + j));
            values.push_back(std::move(row));
        }
        check_every_codec(morsel_of({{"a", array_column(values)}}),
                          "ARRAY x" + std::to_string(rows));
    }
}

void sweep_parameterized() {
    // These types carry a MANDATORY logical descriptor; the writer refuses them
    // without one, so each is swept with its own. (INTERVAL deliberately is not
    // here — it is parameterized in SQL but not in draken, and requires none.)
    LogicalType ts;
    ts.kind = LogicalKind::TIMESTAMP;
    ts.unit = TimestampUnit::MILLISECONDS;
    sweep_fixed<int64_t>("TIMESTAMP64", DRAKEN_TIMESTAMP64,
                         logical_type_intern(ts));

    LogicalType t32;
    t32.kind = LogicalKind::TIME;
    t32.unit = TimestampUnit::MILLISECONDS;
    sweep_fixed<int32_t>("TIME32", DRAKEN_TIME32, logical_type_intern(t32));

    LogicalType t64;
    t64.kind = LogicalKind::TIME;
    t64.unit = TimestampUnit::MICROSECONDS;
    sweep_fixed<int64_t>("TIME64", DRAKEN_TIME64, logical_type_intern(t64));

    LogicalType dec;
    dec.kind = LogicalKind::DECIMAL;
    dec.precision = 18;
    dec.scale = 2;
    sweep_fixed<int64_t>("DECIMAL", DRAKEN_DECIMAL, logical_type_intern(dec));

    // IPV4 REFINES an otherwise-unparameterized physical type rather than
    // completing one, so it is the case where a lost descriptor is a rendering
    // regression rather than an unreadable column — silent, and worth pinning.
    LogicalType ip;
    ip.kind = LogicalKind::IPV4;
    sweep_fixed<uint32_t>("IPV4", DRAKEN_UINT32, logical_type_intern(ip));

}

// The 16-byte composites. Neither can go through the flat sweep: their values
// are structs, so a `static_cast<T>(i)` ramp does not exist for them.
template <typename T>
void sweep_composite(const char* name, DrakenType type, T (*make)(uint32_t),
                     const LogicalType* logical) {
    for (uint32_t rows : kRowCounts) {
        const std::string label = std::string(name) + " x" + std::to_string(rows);

        std::vector<T> values(rows);
        for (uint32_t i = 0; i < rows; ++i) values[i] = make(i % 37u);
        check_every_codec(
            morsel_of({{"v", dense_column<T>(values, type, {}, logical)}}),
            label + " dense");

        if (rows > 0)
            check_every_codec(
                morsel_of({{"v", dense_column<T>(values, type, some_nulls(rows),
                                                 logical)}}),
                label + " dense+nulls");

        const uint32_t distinct = rows == 0 ? 0u : (rows < 8u ? rows : 8u);
        std::vector<T> pool(distinct);
        for (uint32_t i = 0; i < distinct; ++i) pool[i] = make(i);
        auto dict = dict_column<T>(pool, codes_for(rows, distinct), type);
        dict.own->logical_type = logical;
        check_every_codec(morsel_of({{"v", std::move(dict)}}), label + " dict");

        if (rows > 0)
            check_every_codec(
                morsel_of({{"v", constant_column<T>(make(3), rows, type,
                                                    logical)}}),
                label + " constant");
    }
}

// int128 unscaled value, as two 64-bit halves — the layout draken stores.
struct Decimal128Slot { uint64_t low; int64_t high; };
static_assert(sizeof(Decimal128Slot) == 16, "DECIMAL128 slot must be 16 bytes");

void sweep_composites() {
    // INTERVAL is parameterized in SQL but NOT in draken: it needs no logical
    // descriptor, unlike DECIMAL128 next to it, which is uninterpretable
    // without its precision and scale.
    sweep_composite<DrakenIntervalSlot>(
        "INTERVAL", DRAKEN_INTERVAL,
        [](uint32_t i) { return DrakenIntervalSlot{static_cast<int64_t>(i),
                                                   static_cast<int64_t>(i) * 1000}; },
        nullptr);

    LogicalType dec128;
    dec128.kind = LogicalKind::DECIMAL;
    dec128.precision = 38;
    dec128.scale = 4;
    sweep_composite<Decimal128Slot>(
        "DECIMAL128", DRAKEN_DECIMAL128,
        [](uint32_t i) { return Decimal128Slot{static_cast<uint64_t>(i) * 7u,
                                               static_cast<int64_t>(i)}; },
        logical_type_intern(dec128));
}

void sweep_fp16() {
    // VECTOR_FP16's data buffer is rows * dimension halves while length and
    // data_length count ROWS, so it cannot go through the flat sweep.
    LogicalType vec;
    vec.kind = LogicalKind::VECTOR;
    vec.dimension = 8;
    const LogicalType* logical = logical_type_intern(vec);

    for (uint32_t rows : kRowCounts) {
        std::vector<uint16_t> halves(static_cast<size_t>(rows) * 8u);
        for (size_t i = 0; i < halves.size(); ++i)
            halves[i] = static_cast<uint16_t>(i % 61);
        check_every_codec(
            morsel_of({{"v", fp16_column(halves, rows, logical)}}),
            "VECTOR_FP16 x" + std::to_string(rows));

        const uint32_t distinct = rows == 0 ? 0u : (rows < 6u ? rows : 6u);
        std::vector<uint16_t> distinct_halves(static_cast<size_t>(distinct) * 8u);
        for (size_t i = 0; i < distinct_halves.size(); ++i)
            distinct_halves[i] = static_cast<uint16_t>(i * 7u % 65535u);
        check_every_codec(
            morsel_of({{"v", fp16_dict_column(distinct_halves, distinct,
                                              codes_for(rows, distinct),
                                              logical)}}),
            "VECTOR_FP16 dict x" + std::to_string(rows));
    }
}

void sweep_null_type() {
    // DRAKEN_NULL is self-describing: every row is null, there is no data and no
    // validity. It has no sections to compress, which is exactly why it belongs
    // here — a codec must not invent one.
    for (uint32_t rows : kRowCounts) {
        CxxColumn column;
        // Dense at zero rows, constant above it. A constant vector always
        // declares data_length 1, and the reader refuses that over a length of
        // 0 — the zero-row edge of the constant shape, not a codec question.
        column.view = rows == 0
            ? draken_vector_from_dense(nullptr, 0, DRAKEN_NULL, nullptr)
            : draken_vector_from_constant(nullptr, rows, DRAKEN_NULL, nullptr);
        column.own = std::make_shared<VectorOwner>(
            column.view, OwnedBuffer<void>(nullptr), OwnedBuffer<uint8_t>(nullptr));
        check_every_codec(morsel_of({{"n", std::move(column)}}),
                          "NULL x" + std::to_string(rows));
    }
}

void sweep_multi_column() {
    // Mixed columns in one file: the case where a per-section decision could
    // pick differently either side of a column boundary.
    const uint32_t rows = 4000;
    check_every_codec(
        morsel_of({
            {"n", dense_column<int64_t>(ramp<int64_t>(rows), DRAKEN_INT64)},
            {"s", string_column(text(rows))},
            {"b", bool_column(std::vector<bool>(rows, true))},
            {"d", dict_column<int32_t>({1, 2, 3, 4}, codes_for(rows, 4),
                                       DRAKEN_INT32)},
        }),
        "mixed columns");
}

}  // namespace

int main() {
    sweep_fixed<int8_t>("INT8", DRAKEN_INT8);
    sweep_fixed<int16_t>("INT16", DRAKEN_INT16);
    sweep_fixed<int32_t>("INT32", DRAKEN_INT32);
    sweep_fixed<int64_t>("INT64", DRAKEN_INT64);
    sweep_fixed<uint8_t>("UINT8", DRAKEN_UINT8);
    sweep_fixed<uint16_t>("UINT16", DRAKEN_UINT16);
    sweep_fixed<uint32_t>("UINT32", DRAKEN_UINT32);
    sweep_fixed<uint64_t>("UINT64", DRAKEN_UINT64);
    sweep_fixed<float>("FLOAT32", DRAKEN_FLOAT32);
    sweep_fixed<double>("FLOAT64", DRAKEN_FLOAT64);
    sweep_fixed<int32_t>("DATE32", DRAKEN_DATE32);
    sweep_parameterized();
    sweep_fp16();
    sweep_composites();
    sweep_bool();
    sweep_strings();
    sweep_array();
    sweep_null_type();
    sweep_multi_column();
    return skene_test::summary("test_codec_roundtrip");
}
