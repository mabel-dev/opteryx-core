#pragma once
// decode_encodings.hpp
// Parquet encoding/decoding primitives:
//   - RLE/bit-packed level skipping & index decoding
//   - DELTA_BINARY_PACKED (int/long, template defined here)
//   - DELTA_BYTE_ARRAY (byte-array / string columns)
//
// Non-template implementations live in decode_encodings.cpp.

#include "decode_primitives.hpp"
#include <algorithm>
#include <cstdint>
#include <type_traits>
#include <vector>

#include "core/append_buffer.h"
#include <string>

// ---------------------------------------------------------------------------
// Definition levels / repetition levels
// ---------------------------------------------------------------------------

// Skip RLE/bit-packed encoded levels (repetition or definition).
// Returns the number of bytes to skip.
// Per the Parquet spec, for Data Page V1 levels are RLE-encoded with a 4-byte
// length prefix *only* when max_level > 0; when max_level == 0 nothing is
// written at all.
size_t SkipRLEBitPackedLevels(const uint8_t *data, size_t max_size,
                               int max_level);

// ---------------------------------------------------------------------------
// Dictionary index decoder (RLE / bit-packed hybrid)
// ---------------------------------------------------------------------------

// Decode RLE/bit-packed dictionary indices from `data`.
// Returns the number of indices decoded, or -1 on error.
// The caller must prepend a 4-byte little-endian length prefix to the raw
// on-disk data before calling this function (see DecodeColumnFromChunk).
int32_t DecodeRLEBitPackedIndices(const uint8_t *data, size_t data_size,
                                  int32_t num_values, int bit_width,
                                  draken::AppendBuffer<int32_t> &indices);

// Variant that also returns the number of bytes consumed (including the 4-byte prefix).
// On success, updates `bytes_consumed` with the total bytes read.
// Returns the number of indices decoded, or -1 on error.
int32_t DecodeRLEBitPackedIndicesWithConsumption(const uint8_t *data, size_t data_size,
                                                 int32_t num_values, int bit_width,
                                                 draken::AppendBuffer<int32_t> &indices,
                                                 size_t &bytes_consumed);

// Variant with no 4-byte length prefix: data points directly at the RLE stream.
int32_t DecodeRLEBitPackedIndicesNoPrefix(const uint8_t *data, size_t data_size,
                                          int32_t num_values, int bit_width,
                                          draken::AppendBuffer<int32_t> &indices);

// Width-targeted variant of DecodeRLEBitPackedIndicesNoPrefix: decodes the
// `num_values` codes straight into `dst` at `code_width` bytes per value (1, 2
// or 4, little-endian, value i at dst + i * code_width) — the packed per-row
// code array a nullable dictionary column carries — with no int32 index buffer
// and no per-value bounds check. `*max_code` receives the largest code written
// (0 when num_values == 0), so the caller bounds-checks once against the
// dictionary size; a code wider than code_width is truncated in `dst` but still
// reported whole in `*max_code`. Returns num_values on success, -1 on a short or
// malformed stream or an unsupported width.
int32_t DecodeRLEBitPackedCodesPacked(const uint8_t *data, size_t data_size,
                                      int32_t num_values, int bit_width,
                                      uint8_t *dst, uint8_t code_width,
                                      uint32_t *max_code);

// Per-value verdicts straight from the hybrid code stream
// (docs/PARQUET_RUN_DOMAIN_PREDICATE_DESIGN.md §11): vpass[i] = entry_pass[code_i]
// for i in [0, num_values), where entry_pass holds one 0/1 byte per dictionary
// entry. An RLE run is ONE lookup and a fill of its length — its codes are never
// materialised; a bit-packed group is unpacked (per-arch unpackers) and looked
// up per code. `*runs_filled` and `*codes_unpacked` accumulate how many values
// took each path (telemetry). Returns num_values on success; -1 on a short or
// malformed stream; -2 when a code is at or above `num_entries`, with that code
// in `*bad_code`. On failure vpass is partially written and must not be read.
int32_t DecodeRLEBitPackedVerdicts(const uint8_t *data, size_t data_size,
                                   int32_t num_values, int bit_width,
                                   const uint8_t *entry_pass, uint32_t num_entries,
                                   uint8_t *vpass, uint32_t *bad_code,
                                   long long *runs_filled, long long *codes_unpacked);

// Selective variant of DecodeRLEBitPackedIndicesNoPrefix: walks the same stream
// of `num_values` codes but APPENDS only the codes whose `sel` byte is non-zero
// (sel has one 0/1 byte per value). An RLE run appends its code once per
// selected value; a bit-packed group of 8 with no selected value is skipped
// without being unpacked. `out` is cleared first. Returns the number of values
// WALKED (== num_values on success), or -1 on error; out.size() is the number
// selected.
int32_t DecodeRLEBitPackedIndicesSelected(const uint8_t *data, size_t data_size,
                                          int32_t num_values, int bit_width,
                                          const uint8_t *sel,
                                          draken::AppendBuffer<int32_t> &out);

// Header-only probe: is this level stream a SINGLE RLE run of `expect_value`
// that covers at least `num_values` entries?  Used to recognise the
// all-present definition-level stream that every nullable-but-null-free
// column carries, without expanding one int32 per row to discover it.
// `data`/`data_size` span the raw RLE stream (no 4-byte prefix — the caller
// skips it for V1).  Reads the leading varint header plus the run value only;
// it never touches the rest of the stream and never allocates.
// Returns false for anything it cannot prove, including a bit-packed leading
// run, a run shorter than the page, or a truncated stream — "don't know" is
// always reported as false, so a false answer only ever costs the full decode.
bool LevelStreamIsSingleRunOf(const uint8_t *data, size_t data_size,
                              int32_t num_values, int bit_width,
                              int32_t expect_value);

// Skip-dense variant: decode RLE/bit-packed dict indices directly into run-level SoA arrays
// with no dense intermediate allocation.  RLE segments emit one (code, count) pair per
// segment; bit-packed segments decode values and merge consecutive equals into runs.
// Callers use the run_codes/run_counts output to resolve dict values directly (one dict
// lookup per run instead of per row).
// Returns total values decoded, or -1 on error.
int32_t DecodeRLEBitPackedIndicesToRuns(const uint8_t *data, size_t data_size,
                                        int32_t num_values, int bit_width,
                                        draken::AppendBuffer<int32_t> &run_codes,
                                        draken::AppendBuffer<int32_t> &run_counts);

// ---------------------------------------------------------------------------
// DELTA_BINARY_PACKED (encoding id 4) -- template; must live in header
// ---------------------------------------------------------------------------

// Spec shape (as parquet-cpp enforces): block size a multiple of 128 and
// fitting int32, miniblocks dividing it into multiples of 32 values. Bounds
// values_per_miniblock so a hostile header cannot overflow the size math.
static inline bool delta_header_valid(uint64_t block_size, uint64_t num_miniblocks) {
  return block_size % 128u == 0 && block_size <= INT32_MAX &&
         block_size % num_miniblocks == 0 && (block_size / num_miniblocks) % 32u == 0;
}

// 32 LSB-first W-bit values from `src` (4*W bytes) into `out`. W is a template
// parameter so every shift and mask is a compile-time constant and the loop
// unrolls. Each value is one unaligned 8-byte load (plus a ninth byte only for
// W >= 58), so `src` needs kUnpack32Slack readable bytes past its 4*W.
static constexpr size_t kUnpack32Slack = 9;

template <unsigned W>
static inline void Unpack32(const uint8_t *__restrict__ src, uint64_t *__restrict__ out) {
  constexpr uint64_t mask = W == 64 ? ~0ULL : ((1ULL << W) - 1ULL);
#pragma GCC unroll 32
  for (unsigned i = 0; i < 32; ++i) {
    const unsigned bit = i * W;
    const unsigned off = bit & 7u;
    uint64_t v = ReadLE64U(src + (bit >> 3)) >> off;
    if (W + off > 64) v |= (uint64_t)src[(bit >> 3) + 8] << (64 - off);
    out[i] = v & mask;
  }
}

// Static dispatch of Unpack32 over the miniblock width (1..64).
static inline void Unpack32Width(const uint8_t *src, unsigned w, uint64_t *out) {
  switch (w) {
#define RUGO_U32(W) case W: Unpack32<W>(src, out); return;
    RUGO_U32(1)  RUGO_U32(2)  RUGO_U32(3)  RUGO_U32(4)  RUGO_U32(5)  RUGO_U32(6)  RUGO_U32(7)  RUGO_U32(8)
    RUGO_U32(9)  RUGO_U32(10) RUGO_U32(11) RUGO_U32(12) RUGO_U32(13) RUGO_U32(14) RUGO_U32(15) RUGO_U32(16)
    RUGO_U32(17) RUGO_U32(18) RUGO_U32(19) RUGO_U32(20) RUGO_U32(21) RUGO_U32(22) RUGO_U32(23) RUGO_U32(24)
    RUGO_U32(25) RUGO_U32(26) RUGO_U32(27) RUGO_U32(28) RUGO_U32(29) RUGO_U32(30) RUGO_U32(31) RUGO_U32(32)
    RUGO_U32(33) RUGO_U32(34) RUGO_U32(35) RUGO_U32(36) RUGO_U32(37) RUGO_U32(38) RUGO_U32(39) RUGO_U32(40)
    RUGO_U32(41) RUGO_U32(42) RUGO_U32(43) RUGO_U32(44) RUGO_U32(45) RUGO_U32(46) RUGO_U32(47) RUGO_U32(48)
    RUGO_U32(49) RUGO_U32(50) RUGO_U32(51) RUGO_U32(52) RUGO_U32(53) RUGO_U32(54) RUGO_U32(55) RUGO_U32(56)
    RUGO_U32(57) RUGO_U32(58) RUGO_U32(59) RUGO_U32(60) RUGO_U32(61) RUGO_U32(62) RUGO_U32(63) RUGO_U32(64)
#undef RUGO_U32
  default: break;  // callers reject w > 64 and route w == 0 separately
  }
}

// Decode DELTA_BINARY_PACKED for int32 or int64 columns into a caller-owned
// buffer with capacity >= num_values (the hot path: a pre-sized ext_* buffer or
// a pre-reserved result slot). Returns the number of values decoded, or -1 on
// error. Deltas are summed in the column's unsigned width, so they wrap exactly
// as the writer produced them.
template <typename T>
static int32_t DecodeDeltaBinaryPacked(const uint8_t *data, size_t data_size,
                                       int32_t num_values,
                                       T *out) {
  using U = std::make_unsigned_t<T>;
  if (num_values == 0) return 0;  // all-null page: the header still carries a first value
  if (data_size < 4) return -1;

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;

  // Header
  uint64_t block_size = ReadUnsignedVarint(ptr, end);
  if (block_size == 0 || ptr >= end) return -1;

  uint64_t num_miniblocks = ReadUnsignedVarint(ptr, end);
  if (num_miniblocks == 0 || ptr >= end) return -1;

  /* total_value_count */ ReadUnsignedVarint(ptr, end);
  if (ptr >= end) return -1;

  int64_t first_value = ReadZigZagVarint(ptr, end);
  if (ptr > end) return -1;

  out[0] = static_cast<T>(first_value);
  if (num_values == 1) return 1;

  int32_t decoded = 1;
  if (!delta_header_valid(block_size, num_miniblocks)) return -1;
  const uint32_t values_per_miniblock = static_cast<uint32_t>(block_size / num_miniblocks);
  const uint32_t groups_per_miniblock = values_per_miniblock / 32u;

  U last = static_cast<U>(out[0]);
  if (num_miniblocks > static_cast<uint64_t>(end - ptr)) return decoded;  // widths cannot fit
  std::vector<uint8_t> bit_widths(num_miniblocks);
  uint64_t deltas[32];
  while (decoded < num_values && ptr < end) {
    int64_t min_delta = ReadZigZagVarint(ptr, end);
    if (ptr > end) break;
    const U md = static_cast<U>(min_delta);

    if (num_miniblocks > static_cast<uint64_t>(end - ptr)) break;
    std::memcpy(bit_widths.data(), ptr, num_miniblocks);
    ptr += num_miniblocks;

    for (uint64_t mb = 0; mb < num_miniblocks && decoded < num_values; mb++) {
      const uint8_t bit_width = bit_widths[mb];
      // Corrupt input ends the decode outright: a `break` here only left this
      // block, and the outer loop then re-read the miniblock's data bytes as the
      // next block's header - zero bytes "decoding" as a run of the first value.
      // Returning short lets the caller's count check raise.
      if (bit_width > 64) return decoded;

      if (bit_width == 0) {
        // All deltas in this miniblock equal min_delta; no data bytes needed.
        const uint32_t take = std::min<uint32_t>(values_per_miniblock, (uint32_t)(num_values - decoded));
        for (uint32_t i = 0; i < take; i++) { last += md; out[decoded++] = static_cast<T>(last); }
        continue;
      }
      const uint64_t bytes_needed = static_cast<uint64_t>(values_per_miniblock) * bit_width / 8;
      if (bytes_needed > static_cast<uint64_t>(end - ptr)) return decoded;

      const size_t group_bytes = 4u * bit_width;
      for (uint32_t g = 0; g < groups_per_miniblock && decoded < num_values; g++) {
        const uint8_t *src = ptr + (size_t)g * group_bytes;
        if ((size_t)(end - src) >= group_bytes + kUnpack32Slack) {
          Unpack32Width(src, bit_width, deltas);
        } else {
          uint8_t padded[4 * 64 + kUnpack32Slack] = {0};  // page tail: never read past `end`
          std::memcpy(padded, src, group_bytes);
          Unpack32Width(padded, bit_width, deltas);
        }
        const uint32_t take = std::min<uint32_t>(32u, (uint32_t)(num_values - decoded));
        for (uint32_t i = 0; i < take; i++) {
          last += md + static_cast<U>(deltas[i]);
          out[decoded++] = static_cast<T>(last);
        }
      }
      ptr += bytes_needed;
    }
  }

  return decoded;
}

// std::vector-output form for the non-hot callers (DELTA_BYTE_ARRAY prefix and
// suffix lengths, DELTA_LENGTH_BYTE_ARRAY lengths): one algorithm, delegated.
template <typename T>
static int32_t DecodeDeltaBinaryPacked(const uint8_t *data, size_t data_size,
                                       int32_t num_values,
                                       std::vector<T> &values) {
  values.clear();
  if (num_values <= 0) return num_values == 0 ? 0 : -1;
  values.resize((size_t)num_values);
  const int32_t decoded = DecodeDeltaBinaryPacked<T>(data, data_size, num_values, values.data());
  values.resize(decoded > 0 ? (size_t)decoded : 0u);
  return decoded;
}

// ---------------------------------------------------------------------------
// DELTA_BYTE_ARRAY (encoding id 6)
// ---------------------------------------------------------------------------

// Decode DELTA_BYTE_ARRAY for byte-array (string) columns.
// Returns the number of values decoded, or -1 on error.
int32_t DecodeDeltaByteArray(const uint8_t *data, size_t data_size,
                             int32_t num_values,
                             std::vector<std::string> &values);

// ---------------------------------------------------------------------------
// Value encodings decoded into PLAIN layout
// ---------------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY (6), BYTE_STREAM_SPLIT (9) and ALP (10) decode into
// the page's PLAIN byte layout, so the walks read them through the PLAIN path
// unchanged. `num_values` is the page's PRESENT value count. Each returns an
// empty string on success, else the reason the page cannot be decoded.

// BYTE_STREAM_SPLIT: `width` streams of `num_values` bytes (stream j holds byte
// j of every value) -> `num_values` contiguous `width`-byte values at `dst`.
std::string DecodeByteStreamSplit(const uint8_t *data, size_t data_size,
                                  int32_t num_values, size_t width, uint8_t *dst);

// ALP (parquet-format AlpEncoding.md): FLOAT when `width` is 4, DOUBLE when 8.
// Writes `num_values` values, bit-exact, at `dst`.
std::string DecodeAlp(const uint8_t *data, size_t data_size, int32_t num_values,
                      size_t width, uint8_t *dst);

// DELTA_LENGTH_BYTE_ARRAY: a DELTA_BINARY_PACKED stream of lengths, then every
// value's bytes back to back -> PLAIN BYTE_ARRAY (4-byte LE length + bytes per
// value) in `out`, which is cleared first.
std::string DecodeDeltaLengthByteArray(const uint8_t *data, size_t data_size,
                                       int32_t num_values,
                                       draken::AppendBuffer<uint8_t> &out);
