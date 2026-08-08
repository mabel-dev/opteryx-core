// Round-trip: write a morsel, read it back, and assert it is the SAME morsel —
// same values, same encoding shape, same flags, same logical types.
//
// Value equality alone would pass on a format that silently re-dictionaries a
// column or drops an IPv4 descriptor, which are exactly the failures this format
// exists to prevent. So every test asserts shape and type as well as values.

#include <cstring>
#include <string>
#include <vector>

#include "build_vectors.h"
#include "harness.h"
#include "skene/file_io.h"
#include "skene/reader.h"
#include "skene/writer.h"

#include "core/interval_slot.h"
#include "core/ipv4.h"

using namespace skene;
using namespace skene_test;

static std::vector<uint8_t> round_trip(const CxxMorsel& in, CxxMorsel* out,
                                       const ReadOptions& options = ReadOptions()) {
    std::vector<uint8_t> bytes;
    Status write = write_morsel(in, WriteOptions::for_spill(), &bytes);
    if (!write.is_ok()) {
        std::fprintf(stderr, "  write failed: %s\n", write.message().c_str());
        ++skene_test::g_failures;
        return bytes;
    }
    Status read = read_morsel(bytes.data(), bytes.size(), options, out);
    if (!read.is_ok()) {
        std::fprintf(stderr, "  read failed: %s\n", read.message().c_str());
        ++skene_test::g_failures;
    }
    return bytes;
}

static bool row_is_valid(const DrakenVector& v, uint32_t row) {
    if (v.validity == nullptr) return true;
    return (v.validity[row >> 3] & (1u << (row & 7u))) != 0;
}

// The vector-level contract: same values via the uniform access path, AND the
// same encoding shape it went in with.
static void check_vector_identical(const DrakenVector& a, const DrakenVector& b) {
    CHECK_EQ(a.length, b.length);
    CHECK_EQ(a.data_length, b.data_length);
    CHECK_EQ(static_cast<int>(a.type), static_cast<int>(b.type));
    CHECK_EQ(a.flags, b.flags);

    // NULLNESS is the contract, not the presence of a bitmap. The writer drops
    // an all-ones bitmap because an absent section already MEANS all-valid, so
    // a vector can legitimately come back with no bitmap where it went in with
    // a redundant one. What may never differ is which rows are null.
    for (uint32_t i = 0; i < a.length && i < b.length; ++i)
        CHECK_EQ(row_is_valid(a, i), row_is_valid(b, i));
    if (b.validity != nullptr) CHECK(a.validity != nullptr);

    // Shape must be PRESERVED, not merely equivalent: a dict that comes back
    // dense gives the same answers today and loses every dict fast path forever.
    CHECK_EQ(draken_is_dense(&a), draken_is_dense(&b));
    CHECK_EQ(draken_is_constant(&a), draken_is_constant(&b));
    CHECK_EQ(draken_is_dict(&a), draken_is_dict(&b));

    for (uint32_t i = 0; i < a.length && i < b.length; ++i)
        CHECK_EQ(a.selection[i], b.selection[i]);
}

template <typename T>
static void check_values(const DrakenVector& v, const std::vector<T>& expect) {
    const T* data = static_cast<const T*>(v.data);
    CHECK_EQ(v.length, static_cast<uint32_t>(expect.size()));
    for (uint32_t i = 0; i < v.length && i < expect.size(); ++i)
        CHECK_EQ(data[v.selection[i]], expect[i]);
}

static std::string row_string(const DrakenVector& v, uint32_t row) {
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot* slot = &sa->slots[v.selection[row]];
    return std::string(reinterpret_cast<const char*>(str_data(slot, sa->arena)),
                       str_length(slot));
}

// ─── Families ───────────────────────────────────────────────────────────────

static void test_fixed_width() {
    const std::vector<int64_t> values = {-9223372036854775807LL, -1, 0, 1, 4242};
    auto in = morsel_of({{"n", dense_column<int64_t>(values, DRAKEN_INT64)}});

    CxxMorsel out;
    round_trip(in, &out);
    CHECK_EQ(out.num_columns(), size_t{1});
    CHECK_EQ(out.num_rows(), uint32_t{5});
    CHECK(out.names[0] == "n");
    check_vector_identical(in.columns[0].view, out.columns[0].view);
    check_values(out.columns[0].view, values);
}

static void test_bool() {
    std::vector<bool> bits(20, false);
    bits[0] = bits[7] = bits[8] = bits[19] = true;
    auto in = morsel_of({{"b", bool_column(bits)}});

    CxxMorsel out;
    round_trip(in, &out);
    check_vector_identical(in.columns[0].view, out.columns[0].view);

    const uint8_t* packed = static_cast<const uint8_t*>(out.columns[0].view.data);
    for (uint32_t i = 0; i < bits.size(); ++i)
        CHECK_EQ(((packed[i >> 3] >> (i & 7u)) & 1u) != 0, bits[i]);
}

// INTERVAL is a 16-byte two-component slot, and the components are stored
// SEPARATELY on purpose: months are calendar months (28-31 days) and the
// sub-month field is microseconds, so a round trip that normalized them into one
// number would come back a different interval. Value equality per component is
// therefore the contract, not equality of any total.
static void test_interval_round_trips() {
    const std::vector<DrakenIntervalSlot> values = {
        {1, 0},                  // whole months, no sub-month part
        {-3, 123456789},         // mixed signs across the two components
        {0, -1},                 // sub-month only, negative
        {14, 2592000000000LL},   // a sub-month part equal to one normalized month
    };
    auto in = morsel_of({{"iv", dense_column<DrakenIntervalSlot>(
                                    values, DRAKEN_INTERVAL,
                                    {true, false, true, true})}});
    CxxMorsel out;
    round_trip(in, &out);

    const DrakenVector& v = out.columns[0].view;
    check_vector_identical(in.columns[0].view, v);
    CHECK_EQ(static_cast<int>(v.type), static_cast<int>(DRAKEN_INTERVAL));

    // check_values cannot serve: DrakenIntervalSlot has no operator== and
    // CHECK_EQ would need to stringify it. Compare component-wise instead.
    const DrakenIntervalSlot* got = static_cast<const DrakenIntervalSlot*>(v.data);
    for (uint32_t i = 0; i < values.size(); ++i) {
        CHECK_EQ(got[v.selection[i]].months, values[i].months);
        CHECK_EQ(got[v.selection[i]].us, values[i].us);
    }
}

// The dict shape is what Parquet re-derives rather than restores, and an
// interval dictionary is the realistic case — a column of "1 month" and
// "1 year" repeated is how intervals actually arrive.
static void test_interval_dict_shape_survives() {
    const std::vector<DrakenIntervalSlot> distinct = {{2, 500}, {-7, 0}};
    const std::vector<uint32_t> codes = {0, 1, 1, 0, 1};
    auto in = morsel_of({{"iv", dict_column<DrakenIntervalSlot>(
                                    distinct, codes, DRAKEN_INTERVAL)}});
    CxxMorsel out;
    round_trip(in, &out);

    const DrakenVector& v = out.columns[0].view;
    check_vector_identical(in.columns[0].view, v);
    CHECK(draken_is_dict(&v));
    CHECK_EQ(v.data_length, uint32_t{2});

    const DrakenIntervalSlot* got = static_cast<const DrakenIntervalSlot*>(v.data);
    for (uint32_t i = 0; i < codes.size(); ++i) {
        CHECK_EQ(got[v.selection[i]].months, distinct[codes[i]].months);
        CHECK_EQ(got[v.selection[i]].us, distinct[codes[i]].us);
    }
}

// INTERVAL has a defined order (value_order.cpp's compare_interval) and an
// ordinalization, so read acceleration must actually engage on it — sort,
// deduplicate, and produce min/max — and the values must still come back in
// their original ROW order through the permutation. A type that silently fell
// back to kAsWritten would pass every test above and lose every pruning path.
static void test_interval_value_ordering_engages() {
    const std::vector<DrakenIntervalSlot> distinct = {
        {5, 0}, {-2, 999}, {0, 0}, {1, -500}};
    std::vector<DrakenIntervalSlot> values;
    for (int i = 0; i < 400; ++i) values.push_back(distinct[i % 4]);

    auto in = morsel_of({{"iv", dense_column<DrakenIntervalSlot>(values, DRAKEN_INTERVAL)}});

    std::vector<uint8_t> bytes;
    CHECK(write_morsel(in, WriteOptions::for_storage(), &bytes).is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(static_cast<int>(meta.columns[0].value_order),
             static_cast<int>(ValueOrder::kAscending));
    CHECK(meta.columns[0].has_statistics);
    CHECK((meta.columns[0].statistics.flags & kStatMin) != 0);
    CHECK((meta.columns[0].statistics.flags & kStatMax) != 0);
    CHECK(meta.columns[0].statistics.min_ordinal < meta.columns[0].statistics.max_ordinal);

    CxxMorsel out;
    CHECK(read_morsel(bytes.data(), bytes.size(), &out).is_ok());
    const DrakenVector& v = out.columns[0].view;
    CHECK_EQ(v.data_length, uint32_t{4});  // deduplicated to the distinct count
    const DrakenIntervalSlot* got = static_cast<const DrakenIntervalSlot*>(v.data);
    for (uint32_t i = 0; i < values.size(); ++i) {
        CHECK_EQ(got[v.selection[i]].months, values[i].months);
        CHECK_EQ(got[v.selection[i]].us, values[i].us);
    }
}

// A bitmap saying "nothing is null" states what an absent section already means,
// so the writer drops it. Producers hand these over constantly — every column of
// every TPC-H table arrives with one — and they were pure redundancy on disk.
static void test_all_valid_bitmap_is_dropped() {
    // The test helper normalises all-valid to nullptr, so the redundant bitmap
    // has to be built deliberately: allocate one via a real null, then set every
    // bit. That is exactly the shape a producer hands over.
    auto in = morsel_of({{"n", dense_column<int64_t>({1, 2, 3, 4, 5}, DRAKEN_INT64,
                                                     {true, true, false, true, true})}});
    CHECK(in.columns[0].view.validity != nullptr);
    const_cast<uint8_t*>(in.columns[0].view.validity)[0] = 0xFF;

    CxxMorsel out;
    round_trip(in, &out);
    const DrakenVector& v = out.columns[0].view;

    CHECK(v.validity == nullptr);                    // and came back without it
    for (uint32_t i = 0; i < 5; ++i) CHECK(row_is_valid(v, i));
    check_values<int64_t>(v, {1, 2, 3, 4, 5});
}

// The trailing byte holds padding bits above the row count. They mean nothing,
// so they must not decide whether the bitmap counts as all-valid — with 5 rows
// the top three bits are arbitrary and the drop must happen regardless.
static void test_all_valid_ignores_padding_bits() {
    auto in = morsel_of({{"n", dense_column<int64_t>({1, 2, 3, 4, 5}, DRAKEN_INT64,
                                                     {true, true, false, true, true})}});
    const_cast<uint8_t*>(in.columns[0].view.validity)[0] = 0x1F;  // padding cleared

    CxxMorsel out;
    round_trip(in, &out);
    CHECK(out.columns[0].view.validity == nullptr);

    // One real null in the same byte must still be carried.
    auto with_null = morsel_of({{"n", dense_column<int64_t>({1, 2, 3, 4, 5}, DRAKEN_INT64,
                                                            {true, true, false, true, true})}});
    CxxMorsel kept;
    round_trip(with_null, &kept);
    CHECK(kept.columns[0].view.validity != nullptr);
    CHECK(!row_is_valid(kept.columns[0].view, 2));
}

static void test_nulls() {
    auto in = morsel_of({{"n", dense_column<int64_t>({1, 2, 3, 4, 5}, DRAKEN_INT64,
                                                     {true, false, true, false, true})}});
    CxxMorsel out;
    round_trip(in, &out);
    check_vector_identical(in.columns[0].view, out.columns[0].view);

    const DrakenVector& v = out.columns[0].view;
    CHECK(v.validity != nullptr);
    CHECK(row_is_valid(v, 0));
    CHECK(!row_is_valid(v, 1));
    CHECK(row_is_valid(v, 2));
    CHECK(!row_is_valid(v, 3));
    CHECK(row_is_valid(v, 4));
}

static void test_strings() {
    const std::vector<std::string> values = {
        "short",
        "a considerably longer string that lives in the arena",
        "",
        "also quite long, definitely past twelve bytes",
        "exactly12chr",  // the inline boundary
    };
    auto in = morsel_of({{"s", string_column(values)}});

    CxxMorsel out;
    round_trip(in, &out);
    check_vector_identical(in.columns[0].view, out.columns[0].view);

    for (uint32_t i = 0; i < values.size(); ++i)
        CHECK(row_string(out.columns[0].view, i) == values[i]);

    // The rebuilt arena's pointers must land inside its own fresh block, not
    // dangle into the source buffer.
    const DrakenStringArena* sa =
        static_cast<const DrakenStringArena*>(out.columns[0].view.data);
    CHECK(sa->slots != nullptr);
    CHECK_EQ(sa->owns_buffers, uint8_t{0});
    CHECK_EQ(sa->payloads_elided, uint8_t{0});
    CHECK(reinterpret_cast<const uint8_t*>(sa->slots) >
          reinterpret_cast<const uint8_t*>(sa));
}

static void test_length_only_column() {
    // The dangerous one. A length-only column has a NULL arena and long slots
    // stamped with the 0xFFFFFFFF trap; if payloads_elided is lost the trap
    // becomes a ~4 GB out-of-bounds read.
    const std::vector<std::string> values = {
        "tiny", "a long value whose bytes were never materialized", "x"};
    auto in = morsel_of({{"len_only",
                          string_column(values, DRAKEN_VARCHAR, {}, /*elide=*/true)}});

    CxxMorsel out;
    round_trip(in, &out);

    const DrakenStringArena* sa =
        static_cast<const DrakenStringArena*>(out.columns[0].view.data);
    CHECK_EQ(sa->payloads_elided, uint8_t{1});
    CHECK_EQ(sa->arena_used, size_t{0});
    CHECK(sa->arena == nullptr);

    // Lengths survive — that is the whole point of the column — and the trap
    // value is still in place rather than normalised to something dereferenceable.
    for (uint32_t i = 0; i < values.size(); ++i) {
        const DrakenStringSlot* slot = &sa->slots[out.columns[0].view.selection[i]];
        CHECK_EQ(str_length(slot), static_cast<uint32_t>(values[i].size()));
        if (values[i].size() > STR_INLINE_MAX)
            CHECK_EQ(slot->ext.arena_offset, STR_ELIDED_PAYLOAD_OFFSET);
    }
}

static void test_variant_and_varbinary() {
    // Both are German-string storage; VARIANT holds JSON text in exactly the
    // same layout, so it must round-trip as an arena even though it can never be
    // a sort key.
    auto in = morsel_of({
        {"j", string_column({"{\"a\":1}", "{\"b\":[1,2,3,4,5,6,7,8]}"}, DRAKEN_VARIANT)},
        {"raw", string_column({std::string("\x00\x01\x02", 3), "binary payload here"},
                              DRAKEN_VARBINARY)},
    });

    CxxMorsel out;
    round_trip(in, &out);
    CHECK_EQ(out.num_columns(), size_t{2});
    CHECK_EQ(static_cast<int>(out.columns[0].view.type), static_cast<int>(DRAKEN_VARIANT));
    CHECK_EQ(static_cast<int>(out.columns[1].view.type), static_cast<int>(DRAKEN_VARBINARY));
    CHECK(row_string(out.columns[0].view, 0) == "{\"a\":1}");
    CHECK(row_string(out.columns[1].view, 1) == "binary payload here");
}

// ─── Encoding shapes ────────────────────────────────────────────────────────

static void test_dict_shape_is_restored_not_rederived() {
    const std::vector<uint32_t> codes = {2, 0, 1, 1, 0, 2, 2, 0};
    auto in = morsel_of({{"d", dict_column<int64_t>({100, 200, 300}, codes, DRAKEN_INT64)}});

    CxxMorsel out;
    round_trip(in, &out);

    const DrakenVector& v = out.columns[0].view;
    CHECK(draken_is_dict(&v));       // still a dict, not flattened to dense
    CHECK_EQ(v.data_length, uint32_t{3});
    check_vector_identical(in.columns[0].view, v);

    const std::vector<int64_t> expect = {300, 100, 200, 200, 100, 300, 300, 100};
    check_values(v, expect);
}

static void test_constant_shape_is_restored() {
    auto in = morsel_of({{"k", constant_column<int64_t>(42, 1000, DRAKEN_INT64)}});

    CxxMorsel out;
    round_trip(in, &out);

    const DrakenVector& v = out.columns[0].view;
    CHECK(draken_is_constant(&v));
    CHECK_EQ(v.data_length, uint32_t{1});
    CHECK_EQ(v.length, uint32_t{1000});
    for (uint32_t i = 0; i < 1000; ++i)
        CHECK_EQ(static_cast<const int64_t*>(v.data)[v.selection[i]], int64_t{42});
    // The selection must be the shared global, not a materialized array.
    CHECK(out.columns[0].own->codes_buf == nullptr);
}

static void test_permutation_selection_survives() {
    // data_length == length AND a real permutation — the shape value ordering
    // will produce constantly, and the one a shape heuristic misreads as
    // identity, silently reordering every row.
    const std::vector<uint32_t> perm = {3, 1, 0, 2};
    auto in = morsel_of({{"p", dict_column<int64_t>({7, 8, 9, 10}, perm, DRAKEN_INT64)}});

    CxxMorsel out;
    round_trip(in, &out);

    const DrakenVector& v = out.columns[0].view;
    CHECK_EQ(v.data_length, v.length);
    for (uint32_t i = 0; i < perm.size(); ++i) CHECK_EQ(v.selection[i], perm[i]);
    check_values(out.columns[0].view, std::vector<int64_t>{10, 8, 7, 9});
}

static void test_flags_survive_verbatim() {
    auto column = dense_column<int64_t>({5, 6, 7}, DRAKEN_INT64);
    // The sort operator sets these on its own output; losing them costs every
    // downstream sorted-input fast path.
    column.view.flags |= DRAKEN_ROW_SORTED;
    column.own->vec.flags = column.view.flags;

    auto in = morsel_of({{"sorted", std::move(column)}});
    CxxMorsel out;
    round_trip(in, &out);

    CHECK(draken_vector_is_row_sorted(&out.columns[0].view));
    CHECK_EQ(in.columns[0].view.flags, out.columns[0].view.flags);
}

// ─── Logical types — the reason this format exists ──────────────────────────

static void test_ipv4_survives_typed_and_renders() {
    LogicalType lt;
    lt.kind = LogicalKind::IPV4;
    const LogicalType* interned = logical_type_intern(lt);

    const std::vector<uint32_t> addresses = {
        0xC0A80101u,  // 192.168.1.1
        0x08080808u,  // 8.8.8.8
        0x00000000u,  // 0.0.0.0
        0xFFFFFFFFu,  // 255.255.255.255
    };
    auto in = morsel_of({{"ip", dense_column<uint32_t>(addresses, DRAKEN_UINT32,
                                                       {}, interned)}});
    CxxMorsel out;
    round_trip(in, &out);

    // Still UINT32 physically, still IPV4 logically. Parquet drops the second
    // half and hands back a bare unsigned integer.
    const VectorOwner* owner = out.columns[0].own.get();
    CHECK_EQ(static_cast<int>(out.columns[0].view.type), static_cast<int>(DRAKEN_UINT32));
    CHECK(owner->logical_type != nullptr);
    CHECK(owner->logical_type->kind == LogicalKind::IPV4);

    CHECK(*owner->logical_type == *interned);

    // CROSS-TRANSLATION-UNIT LINKAGE GUARD.
    //
    // `interned` was produced by logical_type_intern in THIS translation unit;
    // owner->logical_type by the same function in reader_v1.cpp. Pointer
    // equality across those two TUs is exactly the identity guarantee
    // draken/logical_type.h states, and it holds only because the function has
    // EXTERNAL linkage — plain `inline`, never `static inline`.
    //
    // It was `static inline` until 2026-08-04, which gave every TU its own copy
    // of the function and its own function-local registry, silently falsifying
    // the guarantee. Nothing compared these pointers at the time, so it was
    // latent. Re-adding `static` fails right here, which is the point: this is
    // the cheapest cross-TU guard the header has.
    CHECK(owner->logical_type == interned);

    // Interning is also stable across reads: the same file read twice yields
    // the same descriptor pointer, not two equal copies.
    CxxMorsel again;
    {
        std::vector<uint8_t> bytes;
        CHECK(write_morsel(in, WriteOptions::for_spill(), &bytes).is_ok());
        CHECK(read_morsel(bytes.data(), bytes.size(), &again).is_ok());
    }
    CHECK(again.columns[0].own->logical_type == owner->logical_type);

    // And it renders dotted-decimal, which is what a consumer actually sees.
    const char* expect[] = {"192.168.1.1", "8.8.8.8", "0.0.0.0", "255.255.255.255"};
    const uint32_t* values = static_cast<const uint32_t*>(out.columns[0].view.data);
    for (uint32_t i = 0; i < addresses.size(); ++i) {
        char text[draken::ipv4::MAX_TEXT_LENGTH + 1];
        const uint32_t n = draken::ipv4::format(
            values[out.columns[0].view.selection[i]], text);
        text[n] = '\0';
        CHECK(std::string(text) == expect[i]);
    }
}

static void test_timestamp_and_decimal_descriptors() {
    LogicalType ts;
    ts.kind = LogicalKind::TIMESTAMP;
    ts.unit = TimestampUnit::MILLISECONDS;
    ts.offset_minutes = -330;  // -05:30, a half-hour offset
    const LogicalType* ts_interned = logical_type_intern(ts);

    LogicalType dec;
    dec.kind = LogicalKind::DECIMAL;
    dec.precision = 18;
    dec.scale = 6;
    const LogicalType* dec_interned = logical_type_intern(dec);

    auto in = morsel_of({
        {"t", dense_column<int64_t>({1700000000000LL}, DRAKEN_TIMESTAMP64, {}, ts_interned)},
        {"d", dense_column<int64_t>({123456789LL}, DRAKEN_DECIMAL, {}, dec_interned)},
    });

    CxxMorsel out;
    round_trip(in, &out);

    const LogicalType* t = out.columns[0].own->logical_type;
    CHECK(t != nullptr);
    CHECK(t->unit == TimestampUnit::MILLISECONDS);
    CHECK_EQ(t->offset_minutes, int16_t{-330});

    const LogicalType* d = out.columns[1].own->logical_type;
    CHECK(d != nullptr);
    CHECK_EQ(d->precision, uint8_t{18});
    CHECK_EQ(d->scale, uint8_t{6});
}

// VECTOR_FP16 is the type whose descriptor is LOAD-BEARING rather than
// refining: `dimension` is the item width, so losing it does not degrade the
// column the way losing IPV4 does — it makes the bytes unreadable. Every
// assertion here is about that width surviving.
static void test_fp16_vector_survives_with_dimension() {
    LogicalType lt;
    lt.kind = LogicalKind::VECTOR;
    lt.dimension = 4u;
    const LogicalType* interned = logical_type_intern(lt);

    // 3 rows of dimension 4 — 12 halves in a buffer whose row count is 3.
    const std::vector<uint16_t> halves = {
        0x3C00, 0x4000, 0x4200, 0x4400,   // 1, 2, 3, 4
        0x0000, 0xBC00, 0x3555, 0x7BFF,   // 0, -1, ~1/3, max finite
        0x0001, 0x8000, 0x3C00, 0xC000,   // min subnormal, -0, 1, -2
    };
    auto in = morsel_of({{"emb", fp16_column(halves, 3u, interned)}});

    CxxMorsel out;
    round_trip(in, &out);

    const DrakenVector& v = out.columns[0].view;
    check_vector_identical(in.columns[0].view, v);
    CHECK_EQ(static_cast<int>(v.type), static_cast<int>(DRAKEN_VECTOR_FP16));
    CHECK_EQ(v.length, uint32_t{3});

    const VectorOwner* owner = out.columns[0].own.get();
    CHECK(owner->logical_type != nullptr);
    if (owner->logical_type != nullptr) {
        CHECK(owner->logical_type->kind == LogicalKind::VECTOR);
        CHECK_EQ(owner->logical_type->dimension, uint32_t{4});
        CHECK(owner->logical_type == interned);  // interned, not copied
    }

    // Bit-exact, not approximately equal: these are stored halves, and this
    // format memcpys them. -0 must not come back as 0, and the subnormal must
    // not come back flushed.
    const uint16_t* got = static_cast<const uint16_t*>(v.data);
    for (uint32_t row = 0; row < 3; ++row)
        for (uint32_t d = 0; d < 4; ++d)
            CHECK_EQ(got[v.selection[row] * 4u + d], halves[row * 4u + d]);
}

// Two shapes an embedding column really takes: repeated vectors (dict) and
// missing ones (null rows). Both must survive with the width intact.
static void test_fp16_vector_dict_and_null_shapes() {
    LogicalType lt;
    lt.kind = LogicalKind::VECTOR;
    lt.dimension = 3u;
    const LogicalType* interned = logical_type_intern(lt);

    const std::vector<uint16_t> halves = {0x3C00, 0x4000, 0x4200,
                                          0xBC00, 0xC000, 0xC200};
    const std::vector<uint32_t> codes = {0, 1, 1, 0, 1};
    auto in = morsel_of({{"emb", fp16_dict_column(halves, 2u, codes, interned)}});

    CxxMorsel out;
    round_trip(in, &out);
    const DrakenVector& v = out.columns[0].view;
    check_vector_identical(in.columns[0].view, v);
    CHECK(draken_is_dict(&v));
    CHECK_EQ(v.data_length, uint32_t{2});
    const uint16_t* got = static_cast<const uint16_t*>(v.data);
    for (uint32_t row = 0; row < codes.size(); ++row)
        for (uint32_t d = 0; d < 3; ++d)
            CHECK_EQ(got[v.selection[row] * 3u + d], halves[codes[row] * 3u + d]);

    // A null row still occupies its slot in the data buffer — the width is a
    // property of the column, not of the rows that happen to be present.
    auto with_null = morsel_of({{"emb", fp16_column(halves, 2u, interned,
                                                    {true, false})}});
    CxxMorsel out2;
    round_trip(with_null, &out2);
    const DrakenVector& v2 = out2.columns[0].view;
    CHECK_EQ(v2.length, uint32_t{2});
    CHECK(!row_is_valid(v2, 1));
    const uint16_t* got2 = static_cast<const uint16_t*>(v2.data);
    for (uint32_t d = 0; d < 3; ++d)
        CHECK_EQ(got2[v2.selection[0] * 3u + d], halves[d]);
}

// The other half of the contract: without the descriptor there is no width, so
// the write must FAIL rather than emit a column no reader can size. This is the
// case IPV4 deliberately does not share — a bare UINT32 is still a column.
static void test_fp16_vector_without_descriptor_is_refused() {
    const std::vector<uint16_t> halves = {0x3C00, 0x4000, 0x4200, 0x4400};
    auto in = morsel_of({{"emb", fp16_column(halves, 2u, nullptr)}});

    std::vector<uint8_t> bytes;
    Status write = write_morsel(in, WriteOptions::for_spill(), &bytes);
    CHECK(!write.is_ok());
    CHECK(write.code() == Code::kMalformed);
}

// ─── ARRAY ──────────────────────────────────────────────────────────────────

static void test_array_round_trip() {
    const std::vector<std::vector<int64_t>> rows = {{1, 2, 3}, {}, {4}, {5, 6}};
    auto in = morsel_of({{"arr", array_column(rows)}});

    CxxMorsel out;
    round_trip(in, &out);

    const VectorOwner* owner = out.columns[0].own.get();
    CHECK_EQ(static_cast<int>(out.columns[0].view.type), static_cast<int>(DRAKEN_ARRAY));
    CHECK(owner->child_owner != nullptr);
    CHECK_EQ(owner->child_owner->vec.length, uint32_t{6});

    const int32_t* offsets = static_cast<const int32_t*>(out.columns[0].view.data);
    const DrakenVector& child = owner->child_owner->vec;
    for (uint32_t row = 0; row < rows.size(); ++row) {
        const int32_t begin = offsets[out.columns[0].view.selection[row]];
        const int32_t end   = offsets[out.columns[0].view.selection[row] + 1];
        CHECK_EQ(static_cast<size_t>(end - begin), rows[row].size());
        for (int32_t j = begin; j < end; ++j)
            CHECK_EQ(static_cast<const int64_t*>(child.data)[child.selection[j]],
                     rows[row][j - begin]);
    }
}

// ─── Multi-column, selection, metadata ──────────────────────────────────────

static void test_column_selection() {
    auto in = morsel_of({
        {"a", dense_column<int64_t>({1, 2, 3}, DRAKEN_INT64)},
        {"b", string_column({"x", "y", "z"})},
        {"c", dense_column<int64_t>({7, 8, 9}, DRAKEN_INT64)},
    });

    std::vector<uint8_t> bytes;
    CHECK(write_morsel(in, WriteOptions::for_spill(), &bytes).is_ok());

    ReadOptions options;
    options.columns = {"c", "a"};  // order follows the REQUEST, not the file
    CxxMorsel out;
    CHECK(read_morsel(bytes.data(), bytes.size(), options, &out).is_ok());

    CHECK_EQ(out.num_columns(), size_t{2});
    CHECK(out.names[0] == "c");
    CHECK(out.names[1] == "a");
    check_values(out.columns[0].view, std::vector<int64_t>{7, 8, 9});
    check_values(out.columns[1].view, std::vector<int64_t>{1, 2, 3});

    // Asking for a column that is not there is an error, not a quietly shorter
    // result — that would hide the caller's bug.
    ReadOptions missing;
    missing.columns = {"nope"};
    CxxMorsel ignored;
    CHECK(!read_morsel(bytes.data(), bytes.size(), missing, &ignored).is_ok());
}

static void test_metadata_and_column_extent() {
    auto in = morsel_of({
        {"a", dense_column<int64_t>({1, 2, 3}, DRAKEN_INT64)},
        {"b", string_column({"alpha", "a longer value past twelve", "gamma"})},
    });

    std::vector<uint8_t> bytes;
    CHECK(write_morsel(in, WriteOptions::for_spill(), &bytes).is_ok());

    FileMetadata meta;
    CHECK(read_metadata(bytes.data(), bytes.size(), &meta).is_ok());
    CHECK_EQ(meta.version, uint16_t{1});
    CHECK_EQ(meta.row_count, uint64_t{3});
    CHECK_EQ(meta.columns.size(), size_t{2});
    CHECK(meta.columns[1].name == "b");

    // The headline affordance: one range request per column. The extent must
    // cover every one of that column's sections and stay inside the file.
    for (const ColumnMetadata& column : meta.columns) {
        CHECK(column.byte_bytes > 0);
        CHECK(column.byte_offset >= kFileHeadBytes);
        CHECK(column.byte_offset + column.byte_bytes <= bytes.size());
    }
    // Columns are laid out contiguously and in order, so extents must not overlap.
    CHECK(meta.columns[0].byte_offset + meta.columns[0].byte_bytes
          <= meta.columns[1].byte_offset);

    // The footer can be located from the tail alone — the two-request remote path.
    uint64_t footer_offset = 0, footer_bytes = 0;
    CHECK(footer_extent(bytes.data() + bytes.size() - kFileTailBytes,
                        kFileTailBytes, bytes.size(),
                        &footer_offset, &footer_bytes).is_ok());
    CHECK_EQ(footer_offset + footer_bytes, bytes.size() - kFileTailBytes);
}

static void test_empty_and_zero_column_morsels() {
    CxxMorsel empty;  // no columns, no rows
    CxxMorsel out;
    round_trip(empty, &out);
    CHECK_EQ(out.num_columns(), size_t{0});
    CHECK_EQ(out.num_rows(), uint32_t{0});

    auto zero_rows = morsel_of({{"a", dense_column<int64_t>({}, DRAKEN_INT64)}});
    CxxMorsel out2;
    round_trip(zero_rows, &out2);
    CHECK_EQ(out2.num_columns(), size_t{1});
    CHECK_EQ(out2.num_rows(), uint32_t{0});
}

static void test_file_on_disk_round_trips() {
    auto in = morsel_of({
        {"n",  dense_column<int64_t>({3, 1, 2}, DRAKEN_INT64)},
        {"s",  string_column({"a", "a longer value past twelve bytes", "c"})},
    });

    std::vector<uint8_t> bytes;
    CHECK(write_morsel(in, WriteOptions::for_spill(), &bytes).is_ok());

    const std::string path = "./skene-test-roundtrip.skene";
    CHECK(write_file(path, bytes).is_ok());

    std::vector<uint8_t> loaded;
    CHECK(read_file(path, &loaded).is_ok());
    CHECK_EQ(loaded.size(), bytes.size());
    CHECK_EQ(std::memcmp(loaded.data(), bytes.data(), bytes.size()), 0);

    CxxMorsel out;
    CHECK(read_morsel(loaded.data(), loaded.size(), &out).is_ok());
    CHECK_EQ(out.num_columns(), size_t{2});
    check_values(out.columns[0].view, std::vector<int64_t>{3, 1, 2});

    // write_file renames into place, so no ".skene-partial" may survive a
    // successful write — a reader that found one would see a truncated object.
    std::vector<uint8_t> leftover;
    CHECK(!read_file(path + ".skene-partial", &leftover).is_ok());

    // A missing file, and a file too small to be one, both fail loud rather
    // than producing an empty morsel.
    CHECK(!read_file("./definitely-not-here.skene", &leftover).is_ok());
    CHECK(write_file(path, std::vector<uint8_t>{1, 2, 3}).is_ok());
    Status st = read_file(path, &leftover);
    CHECK(!st.is_ok());
    CHECK(st.code() == Code::kTruncated);

    std::remove(path.c_str());
}

int main() {
    test_file_on_disk_round_trips();
    test_fixed_width();
    test_bool();
    test_interval_round_trips();
    test_interval_dict_shape_survives();
    test_interval_value_ordering_engages();
    test_nulls();
    test_all_valid_bitmap_is_dropped();
    test_all_valid_ignores_padding_bits();
    test_strings();
    test_length_only_column();
    test_variant_and_varbinary();
    test_dict_shape_is_restored_not_rederived();
    test_constant_shape_is_restored();
    test_permutation_selection_survives();
    test_flags_survive_verbatim();
    test_ipv4_survives_typed_and_renders();
    test_timestamp_and_decimal_descriptors();
    test_fp16_vector_survives_with_dimension();
    test_fp16_vector_dict_and_null_shapes();
    test_fp16_vector_without_descriptor_is_refused();
    test_array_round_trip();
    test_column_selection();
    test_metadata_and_column_extent();
    test_empty_and_zero_column_morsels();
    return skene_test::summary("test_roundtrip");
}
