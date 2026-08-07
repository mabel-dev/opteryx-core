// Section body encodings (FORMAT.md §7.7).
//
// An encoding is a pure size optimization: decoding MUST reproduce the plain
// bytes exactly. So every test here is a round trip, and the interesting cases
// are the boundaries where a bit packer typically goes wrong — widths that are
// not byte multiples, width 0, width at the full type size, and counts that do
// not divide evenly into bytes.

#include <cstring>
#include <random>
#include <vector>

#include "encoding.h"
#include "harness.h"

using namespace skene;
using namespace skene_test;

// ─── Width computation ──────────────────────────────────────────────────────

static void test_bits_required() {
    CHECK_EQ(bits_required(0), uint8_t{0});
    CHECK_EQ(bits_required(1), uint8_t{1});
    CHECK_EQ(bits_required(2), uint8_t{2});
    CHECK_EQ(bits_required(3), uint8_t{2});
    CHECK_EQ(bits_required(255), uint8_t{8});
    CHECK_EQ(bits_required(256), uint8_t{9});
    CHECK_EQ(bits_required(0xFFFFFFFFull), uint8_t{32});
    CHECK_EQ(bits_required(~uint64_t{0}), uint8_t{64});
}

// ─── Bit packing ────────────────────────────────────────────────────────────

static void round_trip_codes(const std::vector<uint32_t>& codes, uint32_t data_length,
                             bool expect_packed) {
    std::vector<uint8_t> encoded;
    const bool packed = bitpack_encode_codes(codes.data(),
                                             static_cast<uint32_t>(codes.size()),
                                             data_length, &encoded);
    ++skene_test::g_checks;
    if (packed != expect_packed) {
        skene_test::report(__FILE__, __LINE__, "packing decision",
                           "expected packed=" + std::to_string(expect_packed) +
                           " got " + std::to_string(packed));
        return;
    }
    if (!packed) return;

    std::vector<uint32_t> decoded(codes.size(), 0xDEADBEEF);
    Status st = bitpack_decode_codes(encoded.data(), encoded.size(),
                                     static_cast<uint32_t>(codes.size()),
                                     decoded.data());
    CHECK(st.is_ok());
    CHECK(decoded == codes);
}

static void test_bitpack_widths() {
    // Width 1 — the extreme that a naive packer gets wrong by writing whole bytes.
    round_trip_codes({0, 1, 1, 0, 1, 0, 0, 1, 1}, 2, true);

    // Width 2, count not a byte multiple: 5 values * 2 bits = 10 bits, so the
    // final partial byte must still be written and read back.
    round_trip_codes({0, 1, 2, 3, 2}, 4, true);

    // Widths 3..9 straddle the byte boundary in both directions, which is where
    // an off-by-one in the accumulator shows up.
    for (uint32_t distinct = 5; distinct <= 400; distinct = distinct * 2 + 1) {
        std::vector<uint32_t> codes;
        for (uint32_t i = 0; i < 37; ++i) codes.push_back(i % distinct);
        round_trip_codes(codes, distinct, true);
    }

    // Width 0 — every code is necessarily 0, so no payload bits at all. The
    // decoder must produce zeros rather than reading a body that is not there.
    round_trip_codes({0, 0, 0, 0}, 1, true);

    // A single row DECLINES: one code is 4 plain bytes and the header alone is
    // 8, so packing could only make it bigger. Four such rows do pay (16 -> 8),
    // which is the case just above.
    round_trip_codes({0}, 1, false);
}

static void test_bitpack_declines_when_not_smaller() {
    // 2^32 distinct values needs the full 32 bits, so packing cannot beat plain
    // once the 8-byte header is counted. "Not worth it" must be a normal answer,
    // not a failure.
    std::vector<uint32_t> codes = {0, 1, 2, 3};
    std::vector<uint8_t> encoded;
    CHECK(!bitpack_encode_codes(codes.data(), 4, 0xFFFFFFFFu, &encoded));

    // And a tiny column where the header outweighs the saving.
    CHECK(!bitpack_encode_codes(codes.data(), 1, 4u, &encoded));
}

static void test_bitpack_size_is_actually_smaller() {
    // The point of the encoding. 1000 rows over 16 distinct values is 4 bits per
    // row: 500 bytes plus a header, against 4000 plain.
    std::vector<uint32_t> codes(1000);
    for (size_t i = 0; i < codes.size(); ++i) codes[i] = i % 16;

    std::vector<uint8_t> encoded;
    CHECK(bitpack_encode_codes(codes.data(), 1000, 16, &encoded));
    CHECK_EQ(encoded.size(), size_t{8 + 500});

    std::vector<uint32_t> decoded(1000);
    CHECK(bitpack_decode_codes(encoded.data(), encoded.size(), 1000,
                               decoded.data()).is_ok());
    CHECK(decoded == codes);
}

static void test_bitpack_rejects_corrupt_bodies() {
    std::vector<uint32_t> codes(100);
    for (size_t i = 0; i < codes.size(); ++i) codes[i] = i % 7;
    std::vector<uint8_t> encoded;
    CHECK(bitpack_encode_codes(codes.data(), 100, 7, &encoded));

    std::vector<uint32_t> decoded(100);

    // A count that disagrees with the column is a contradiction, not a hint.
    CHECK(!bitpack_decode_codes(encoded.data(), encoded.size(), 99,
                                decoded.data()).is_ok());

    // A truncated body must be refused rather than read past its end.
    CHECK(!bitpack_decode_codes(encoded.data(), sizeof(BitpackHeader) + 1, 100,
                                decoded.data()).is_ok());
    CHECK(!bitpack_decode_codes(encoded.data(), 3, 100, decoded.data()).is_ok());

    // A width beyond what a uint32 can hold cannot be honoured.
    std::vector<uint8_t> bad = encoded;
    bad[4] = 33;
    CHECK(!bitpack_decode_codes(bad.data(), bad.size(), 100, decoded.data()).is_ok());
}

// ─── Delta + bit packing ────────────────────────────────────────────────────

template <typename T>
static void round_trip_delta(const std::vector<T>& ascending, bool expect_encoded) {
    const size_t item_bytes = sizeof(T);
    std::vector<uint8_t> encoded;
    const bool did = delta_bitpack_encode(ascending.data(),
                                          static_cast<uint32_t>(ascending.size()),
                                          item_bytes, &encoded);
    ++skene_test::g_checks;
    if (did != expect_encoded) {
        skene_test::report(__FILE__, __LINE__, "delta encoding decision",
                           "expected " + std::to_string(expect_encoded) +
                           " got " + std::to_string(did));
        return;
    }
    if (!did) return;

    std::vector<T> decoded(ascending.size(), T{0});
    Status st = delta_bitpack_decode(encoded.data(), encoded.size(),
                                     static_cast<uint32_t>(ascending.size()),
                                     item_bytes, decoded.data());
    CHECK(st.is_ok());
    CHECK(decoded == ascending);
}

static void test_delta_round_trips() {
    // Dense ascending run — deltas of 1, so a single bit each.
    std::vector<int64_t> run(500);
    for (size_t i = 0; i < run.size(); ++i) run[i] = 1000 + static_cast<int64_t>(i);
    round_trip_delta(run, true);

    // Timestamps a second apart: constant large-ish deltas.
    std::vector<int64_t> stamps(200);
    for (size_t i = 0; i < stamps.size(); ++i)
        stamps[i] = 1700000000000000LL + static_cast<int64_t>(i) * 1000000LL;
    round_trip_delta(stamps, true);

    // 32-bit dates.
    std::vector<int32_t> dates(300);
    for (size_t i = 0; i < dates.size(); ++i) dates[i] = 19000 + static_cast<int32_t>(i) / 3;
    round_trip_delta(dates, true);

    // Repeats give delta 0 — width must handle a run of zeros between steps.
    round_trip_delta(std::vector<int64_t>{5, 5, 5, 9, 9, 100}, true);
}

static void test_delta_crossing_zero_does_not_overflow() {
    // THE case signed subtraction gets wrong: an ascending run that crosses
    // zero. Reconstruction adds the stored difference back, and if either
    // direction were done in signed arithmetic the negative half would come back
    // wrong. The wrapping unsigned difference is exact in both directions.
    std::vector<int64_t> crossing = {-1000000, -999999, -1, 0, 1, 999999, 1000000};
    round_trip_delta(crossing, true);

    std::vector<int32_t> crossing32(100);
    for (size_t i = 0; i < crossing32.size(); ++i)
        crossing32[i] = -50000 + static_cast<int32_t>(i) * 1000;
    round_trip_delta(crossing32, true);

    // Unsigned values above the signed range: the top half of the uint64 domain
    // must not be reinterpreted as negative anywhere in the round trip.
    std::vector<uint64_t> high(100);
    for (size_t i = 0; i < high.size(); ++i)
        high[i] = (uint64_t{1} << 63) + i * 7u;
    round_trip_delta(high, true);
}

static void test_delta_edge_counts() {
    round_trip_delta(std::vector<int64_t>{42}, false);        // one value: header costs more
    round_trip_delta(std::vector<int64_t>{}, false);          // empty
    round_trip_delta(std::vector<int64_t>{1, 2}, false);      // too few to amortise the header

    // All-identical: every delta is 0, so width 0 and no payload at all.
    std::vector<int64_t> same(100, 7);
    round_trip_delta(same, true);
}

static void test_delta_declines_when_it_cannot_pay() {
    // A full 64-bit width can never win: the packed differences alone cost the
    // same 8 bytes per value as plain, and the header and first value are pure
    // overhead on top. The encoder must decline rather than emit something
    // larger than what it replaced.
    std::vector<uint64_t> full_width = {0, 1, uint64_t{1} << 63, ~uint64_t{0}};
    round_trip_delta(full_width, false);

    // Spanning the whole signed range needs 64-bit differences for the same
    // reason — correct to handle, not worth encoding.
    std::vector<int64_t> extremes = {
        INT64_MIN, INT64_MIN + 1, -5, 0, 3, INT64_MAX - 1, INT64_MAX};
    round_trip_delta(extremes, false);

    // A width that is merely large is a near-miss rather than a clear loss, and
    // the decision is made on measured size either way — never on a guess about
    // which side of the line the data falls.
    std::vector<int32_t> wide32 = {INT32_MIN, -1, 0, 1, INT32_MAX};
    round_trip_delta(wide32, false);
}

static void test_delta_rejects_corrupt_bodies() {
    std::vector<int64_t> run(100);
    for (size_t i = 0; i < run.size(); ++i) run[i] = static_cast<int64_t>(i) * 3;
    std::vector<uint8_t> encoded;
    CHECK(delta_bitpack_encode(run.data(), 100, 8, &encoded));

    std::vector<int64_t> decoded(100);
    CHECK(!delta_bitpack_decode(encoded.data(), encoded.size(), 99, 8,
                                decoded.data()).is_ok());
    // An item width that disagrees with the column's type would reinterpret
    // every value at the wrong stride.
    CHECK(!delta_bitpack_decode(encoded.data(), encoded.size(), 100, 4,
                                decoded.data()).is_ok());
    CHECK(!delta_bitpack_decode(encoded.data(), 4, 100, 8, decoded.data()).is_ok());
    CHECK(!delta_bitpack_decode(encoded.data(), sizeof(DeltaBitpackHeader) + 2, 100, 8,
                                decoded.data()).is_ok());
}

static void test_delta_type_eligibility() {
    CHECK(type_supports_delta(DRAKEN_INT32));
    CHECK(type_supports_delta(DRAKEN_INT64));
    CHECK(type_supports_delta(DRAKEN_UINT64));
    CHECK(type_supports_delta(DRAKEN_TIMESTAMP64));
    CHECK(type_supports_delta(DRAKEN_DATE32));
    CHECK(type_supports_delta(DRAKEN_DECIMAL));

    // Floats: a difference between bit patterns is not a number.
    CHECK(!type_supports_delta(DRAKEN_FLOAT32));
    CHECK(!type_supports_delta(DRAKEN_FLOAT64));
    // 16-byte composites and the narrow widths, where the header cannot pay.
    CHECK(!type_supports_delta(DRAKEN_DECIMAL128));
    CHECK(!type_supports_delta(DRAKEN_INTERVAL));
    CHECK(!type_supports_delta(DRAKEN_INT8));
    // Non-flat families.
    CHECK(!type_supports_delta(DRAKEN_BOOL));
    CHECK(!type_supports_delta(DRAKEN_VARCHAR));
    CHECK(!type_supports_delta(DRAKEN_ARRAY));
}

// ─── Randomised ─────────────────────────────────────────────────────────────

static void test_random_round_trips() {
    // Fixed seed: a codec test that fails only sometimes is worse than one that
    // does not exist, because it trains people to re-run.
    std::mt19937_64 rng(0x5CE7E5EEDull);

    for (int trial = 0; trial < 200; ++trial) {
        const uint32_t distinct = 1 + (rng() % 5000);
        const uint32_t count    = 1 + (rng() % 500);
        std::vector<uint32_t> codes(count);
        for (uint32_t i = 0; i < count; ++i) codes[i] = rng() % distinct;

        std::vector<uint8_t> encoded;
        if (!bitpack_encode_codes(codes.data(), count, distinct, &encoded)) continue;
        std::vector<uint32_t> decoded(count, 0);
        CHECK(bitpack_decode_codes(encoded.data(), encoded.size(), count,
                                   decoded.data()).is_ok());
        if (decoded != codes) {
            skene_test::report(__FILE__, __LINE__, "random bitpack round trip",
                               "trial " + std::to_string(trial));
            return;
        }
    }
    ++skene_test::g_checks;

    for (int trial = 0; trial < 200; ++trial) {
        const uint32_t count = 2 + (rng() % 300);
        std::vector<int64_t> values(count);
        int64_t at = static_cast<int64_t>(rng()) >> 2;
        for (uint32_t i = 0; i < count; ++i) {
            values[i] = at;
            at += static_cast<int64_t>(rng() % 100000);   // ascending
        }
        std::vector<uint8_t> encoded;
        if (!delta_bitpack_encode(values.data(), count, 8, &encoded)) continue;
        std::vector<int64_t> decoded(count, 0);
        CHECK(delta_bitpack_decode(encoded.data(), encoded.size(), count, 8,
                                   decoded.data()).is_ok());
        if (decoded != values) {
            skene_test::report(__FILE__, __LINE__, "random delta round trip",
                               "trial " + std::to_string(trial));
            return;
        }
    }
    ++skene_test::g_checks;
}

int main() {
    test_bits_required();
    test_bitpack_widths();
    test_bitpack_declines_when_not_smaller();
    test_bitpack_size_is_actually_smaller();
    test_bitpack_rejects_corrupt_bodies();
    test_delta_round_trips();
    test_delta_crossing_zero_does_not_overflow();
    test_delta_edge_counts();
    test_delta_declines_when_it_cannot_pay();
    test_delta_rejects_corrupt_bodies();
    test_delta_type_eligibility();
    test_random_round_trips();
    return skene_test::summary("test_encoding");
}
