#pragma once
// Split Block Bloom Filter (SBBF) WRITER — byte-for-byte compatible with the
// rugo reader (rugo/src/parquet/bloom_filter.cpp) and the canonical parquet
// format (PyArrow / DuckDB).
//
// Format (parquet spec): the bitset is a sequence of 32-byte blocks, each 8
// little-endian uint32 words. A value's XXH64 hash (seed 0, over the value's
// PLAIN-encoded bytes) selects a block and sets one bit per word.
//
// Block selection: canonical `((hash >> 32) * num_blocks) >> 32`. The rugo
// reader uses a top-bits shift for power-of-2 block counts, which is IDENTICAL
// to this for powers of two — so we ALWAYS size to a power-of-2 block count,
// keeping the filter probe-compatible with both the reader and PyArrow.
//
// See docs/PARQUET_WRITER_DESIGN.md.

#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>

#define XXH_INLINE_ALL
#include "xxhash.h"

namespace rugo_pq_write {

constexpr uint32_t kBloomBytesPerBlock = 32;
constexpr uint32_t kBloomWordsPerBlock = 8;
constexpr uint32_t kBloomSalts[kBloomWordsPerBlock] = {
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

struct BloomFilter {
  std::vector<uint8_t> bitset; // num_blocks * 32 bytes, zero-initialized
  uint32_t num_blocks = 0;
};

inline uint32_t bloom_next_pow2(uint32_t n) {
  uint32_t p = 1;
  while (p < n)
    p <<= 1;
  return p;
}

// Power-of-2 block count for `ndv` distinct values at false-positive prob `fpp`.
// bits = -ndv * ln(fpp) / ln(2)^2; bytes = bits/8; blocks = bytes/32, rounded up
// to a power of two (so block selection matches the reader and PyArrow).
inline uint32_t bloom_num_blocks(size_t ndv, double fpp) {
  if (ndv == 0)
    ndv = 1;
  const double ln2 = 0.6931471805599453;
  double bits = -(double)ndv * std::log(fpp) / (ln2 * ln2);
  uint32_t blocks = (uint32_t)std::ceil(bits / (double)(kBloomBytesPerBlock * 8));
  if (blocks < 1)
    blocks = 1;
  return bloom_next_pow2(blocks);
}

inline uint64_t bloom_hash(const uint8_t *data, size_t len) {
  return (uint64_t)XXH64(data, len, 0);
}

// Set the 8 bits for a precomputed hash. Targets are little-endian, so the
// uint32 word view aliases the bitset bytes directly.
inline void bloom_insert_hash(BloomFilter &bf, uint64_t h) {
  uint32_t low = (uint32_t)(h & 0xFFFFFFFFULL);
  uint32_t block = (uint32_t)(((h >> 32) * (uint64_t)bf.num_blocks) >> 32);
  uint32_t *words =
      (uint32_t *)(bf.bitset.data() + (size_t)block * kBloomBytesPerBlock);
  for (uint32_t i = 0; i < kBloomWordsPerBlock; ++i) {
    uint32_t bit = (low * kBloomSalts[i]) >> 27; // 0..31
    words[i] |= (1u << bit);
  }
}

// Build a filter from precomputed hashes (caller dedups for NDV sizing).
inline BloomFilter bloom_build(const std::vector<uint64_t> &hashes, size_t ndv,
                               double fpp) {
  BloomFilter bf;
  bf.num_blocks = bloom_num_blocks(ndv, fpp);
  bf.bitset.assign((size_t)bf.num_blocks * kBloomBytesPerBlock, 0);
  for (uint64_t h : hashes)
    bloom_insert_hash(bf, h);
  return bf;
}

} // namespace rugo_pq_write
