#include "bloom.h"

#include <cmath>
#include <cstring>

#include "core/string_slot.h"

#define XXH_INLINE_ALL
#include "xxhash.h"

namespace skene {
namespace {

constexpr uint32_t kBytesPerBlock = 32;
constexpr uint32_t kWordsPerBlock = 8;

// The Parquet SBBF salts. Reproduced exactly, not chosen — a filter built with
// different salts is a different filter, and the point of matching the spec is
// that this and rugo's copy can collapse into one later.
constexpr uint32_t kSalts[kWordsPerBlock] = {
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

// Bits per distinct value needed to actually DELIVER `fpp` from a split-block
// filter — measured, not derived (bench/bloom_sizing.cpp).
//
// The classic Bloom formula, -ln(p)/ln(2)^2, is wrong here and wrong by more the
// looser the target gets: a split-block filter confines every one of a key's bits
// to a single 32-byte block, trading accuracy for one cache line per probe, and
// that penalty grows as bits-per-key falls. Sized by the classic formula, a
// requested 5% delivers 7.6% and a requested 10% delivers 15.5%. A filter that
// silently misses its stated rate is worse than one that costs more, because
// every caller downstream is reasoning with the stated number.
//
// These are the measured bits-per-key at which THIS filter reaches the rate.
double bits_per_key_for(double fpp) {
    struct Point { double fpp, bits; };
    // Ascending by fpp. Calibrated on int64 keys, 1k-300k distinct.
    static constexpr Point kCurve[] = {
        {0.001, 17.4}, {0.005, 12.9}, {0.01, 10.7},
        {0.02,  9.0},  {0.05,  7.3},  {0.10,  6.1}, {0.25, 4.5},
    };
    const size_t n = sizeof(kCurve) / sizeof(kCurve[0]);
    if (fpp <= kCurve[0].fpp)     return kCurve[0].bits;
    if (fpp >= kCurve[n - 1].fpp) return kCurve[n - 1].bits;
    for (size_t i = 1; i < n; ++i) {
        if (fpp > kCurve[i].fpp) continue;
        // Linear in log(fpp), which is where the relationship is nearly straight.
        const double t = (std::log(fpp) - std::log(kCurve[i - 1].fpp))
                       / (std::log(kCurve[i].fpp) - std::log(kCurve[i - 1].fpp));
        return kCurve[i - 1].bits + t * (kCurve[i].bits - kCurve[i - 1].bits);
    }
    return kCurve[n - 1].bits;
}

// Block count. NOT rounded to a power of two.
//
// The canonical block index, ((hash >> 32) * blocks) >> 32, is a multiply-shift
// that maps uniformly into [0, blocks) for ANY block count — the power-of-two
// constraint is Parquet's spec requirement, not an arithmetic one, and honouring
// it here cost 1.05x to 1.72x depending on where a column's NDV happened to fall.
// That is a large, erratic tax on every file for nothing.
uint32_t block_count_for(uint64_t distinct, double fpp) {
    if (distinct == 0) distinct = 1;
    if (!(fpp > 0.0 && fpp < 1.0)) fpp = kDefaultFalsePositiveRate;
    const double bits = static_cast<double>(distinct) * bits_per_key_for(fpp);
    uint32_t blocks = static_cast<uint32_t>(
        std::ceil(bits / static_cast<double>(kBytesPerBlock * 8)));
    if (blocks < 1) blocks = 1;
    return blocks;
}

inline uint32_t block_index(uint64_t hash, uint32_t blocks) {
    return static_cast<uint32_t>(((hash >> 32) * blocks) >> 32);
}

void set_bits(uint8_t* bitset, uint32_t blocks, uint64_t hash) {
    uint32_t* block = reinterpret_cast<uint32_t*>(
        bitset + static_cast<size_t>(block_index(hash, blocks)) * kBytesPerBlock);
    const uint32_t key = static_cast<uint32_t>(hash);
    for (uint32_t i = 0; i < kWordsPerBlock; ++i)
        block[i] |= uint32_t{1} << ((key * kSalts[i]) >> 27);
}

bool test_bits(const uint8_t* bitset, uint32_t blocks, uint64_t hash) {
    const uint32_t* block = reinterpret_cast<const uint32_t*>(
        bitset + static_cast<size_t>(block_index(hash, blocks)) * kBytesPerBlock);
    const uint32_t key = static_cast<uint32_t>(hash);
    for (uint32_t i = 0; i < kWordsPerBlock; ++i) {
        const uint32_t mask = uint32_t{1} << ((key * kSalts[i]) >> 27);
        if ((block[i] & mask) == 0) return false;
    }
    return true;
}

// The bytes that identify one value. Fixed-width types hash their raw bytes;
// the string family hashes its CONTENT, so two slots holding the same bytes at
// different arena offsets hash alike — matching how deduplication defines "the
// same value".
bool value_bytes_at(const DrakenVector& vector, uint32_t index,
                    const uint8_t** out, uint32_t* out_length) {
    if (draken_type_is_string_storage(vector.type)) {
        const DrakenStringArena* arena =
            static_cast<const DrakenStringArena*>(vector.data);
        if (arena == nullptr || arena->payloads_elided) return false;
        const DrakenStringSlot* slot = &arena->slots[index];
        *out = str_data(slot, arena->arena);
        *out_length = str_length(slot);
        return true;
    }
    if (vector.type == DRAKEN_BOOL) {
        // Two possible values: a filter over them cannot exclude anything a
        // min/max does not already, so it is not worth the bytes.
        return false;
    }
    const size_t width = draken_type_fixed_itemsize(vector.type);
    if (width == 0) return false;   // ARRAY / NULL / FP16: no flat bytes
    *out = static_cast<const uint8_t*>(vector.data) + index * width;
    *out_length = static_cast<uint32_t>(width);
    return true;
}

#pragma pack(push, 1)
struct BloomHeader {
    uint32_t num_blocks;
    uint32_t reserved;
};
#pragma pack(pop)

static_assert(sizeof(BloomHeader) == 8u, "BloomHeader layout drift");

}  // namespace

bool bloom_hash_value(const void* value_bytes, uint32_t value_length,
                      uint64_t* out_hash) {
    if (value_bytes == nullptr && value_length > 0) return false;
    *out_hash = static_cast<uint64_t>(XXH64(value_bytes, value_length, 0));
    return true;
}

bool bloom_build(const DrakenVector& vector, double false_positive_rate,
                 std::vector<uint8_t>* out) {
    // A probe of the first value decides eligibility, so an unsupported type
    // costs nothing beyond that check.
    const uint8_t* probe = nullptr;
    uint32_t probe_length = 0;
    if (vector.data_length == 0
            || !value_bytes_at(vector, 0, &probe, &probe_length))
        return false;

    const uint32_t blocks = block_count_for(vector.data_length, false_positive_rate);
    out->assign(sizeof(BloomHeader)
                    + static_cast<size_t>(blocks) * kBytesPerBlock, 0);

    BloomHeader header{};
    header.num_blocks = blocks;
    std::memcpy(out->data(), &header, sizeof(header));

    uint8_t* bitset = out->data() + sizeof(BloomHeader);
    for (uint32_t i = 0; i < vector.data_length; ++i) {
        const uint8_t* bytes = nullptr;
        uint32_t length = 0;
        if (!value_bytes_at(vector, i, &bytes, &length)) { out->clear(); return false; }
        uint64_t hash = 0;
        if (!bloom_hash_value(bytes, length, &hash)) { out->clear(); return false; }
        set_bits(bitset, blocks, hash);
    }
    return true;
}

Status bloom_probe(const uint8_t* stored, uint64_t stored_bytes,
                   const void* value_bytes, uint32_t value_length,
                   bool* out_may_contain) {
    if (stored_bytes < sizeof(BloomHeader))
        return Status(Code::kMalformed, "bloom filter is too small to hold its header");

    BloomHeader header;
    std::memcpy(&header, stored, sizeof(header));

    // Any positive block count is valid — the multiply-shift block index does not
    // require a power of two. Zero is not: it would make block selection divide
    // into an empty bitset.
    if (header.num_blocks == 0)
        return Status(Code::kMalformed,
                      "bloom filter declares zero blocks, so it has no bitset to "
                      "probe");

    const uint64_t needed = sizeof(BloomHeader)
                          + static_cast<uint64_t>(header.num_blocks) * kBytesPerBlock;
    if (stored_bytes != needed)
        return Status(Code::kMalformed,
                      "bloom filter length disagrees with its declared block count");

    uint64_t hash = 0;
    if (!bloom_hash_value(value_bytes, value_length, &hash))
        return Status(Code::kMalformed, "bloom probe: value bytes are null");

    *out_may_contain = test_bits(stored + sizeof(BloomHeader), header.num_blocks, hash);
    return Status::ok();
}

}  // namespace skene
