#pragma once
// decode_encodings.hpp
// Parquet encoding/decoding primitives:
//   - RLE/bit-packed level skipping & index decoding
//   - DELTA_BINARY_PACKED (int/long, template defined here)
//   - DELTA_BYTE_ARRAY (byte-array / string columns)
//
// Non-template implementations live in decode_encodings.cpp.

#include "decode_primitives.hpp"
#include <cstdint>
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

// Decode DELTA_BINARY_PACKED for int32 or int64 columns.
// Returns the number of values decoded, or -1 on error.
template <typename T>
static int32_t DecodeDeltaBinaryPacked(const uint8_t *data, size_t data_size,
                                       int32_t num_values,
                                       std::vector<T> &values) {
  if (data_size < 4) return -1;

  const uint8_t *ptr = data;
  const uint8_t *end = data + data_size;

  values.clear();
  values.reserve(num_values);

  // Header
  uint64_t block_size = ReadUnsignedVarint(ptr, end);
  if (block_size == 0 || ptr >= end) return -1;

  uint64_t num_miniblocks = ReadUnsignedVarint(ptr, end);
  if (num_miniblocks == 0 || ptr >= end) return -1;

  /* total_value_count */ ReadUnsignedVarint(ptr, end);
  if (ptr >= end) return -1;

  int64_t first_value = ReadZigZagVarint(ptr, end);
  if (ptr > end) return -1;

  values.push_back(static_cast<T>(first_value));
  if (num_values == 1) return 1;

  int32_t decoded = 1;
  if (!delta_header_valid(block_size, num_miniblocks)) return -1;
  uint32_t values_per_miniblock = static_cast<uint32_t>(block_size / num_miniblocks);

  while (decoded < num_values && ptr < end) {
    int64_t min_delta = ReadZigZagVarint(ptr, end);
    if (ptr > end) break;

    std::vector<uint8_t> bit_widths;
    bit_widths.reserve(num_miniblocks);
    for (uint64_t mb = 0; mb < num_miniblocks && ptr < end; mb++) {
      bit_widths.push_back(*ptr++);
    }
    if (bit_widths.size() != num_miniblocks) break;

    for (uint64_t mb = 0; mb < num_miniblocks && decoded < num_values; mb++) {
      uint8_t bit_width = bit_widths[mb];
      if (bit_width > 64) break;  // corrupt: ReadPackedLSB holds widths <= 64

      if (bit_width == 0) {
        // All deltas in this miniblock equal min_delta; no data bytes needed.
        for (uint32_t i = 0; i < values_per_miniblock && decoded < num_values; i++) {
          T last = values.back();
          values.push_back(last + static_cast<T>(min_delta));
          decoded++;
        }
      } else {
        const uint64_t bytes_needed =
            (static_cast<uint64_t>(values_per_miniblock) * bit_width + 7) / 8;
        if (bytes_needed > static_cast<uint64_t>(end - ptr)) break;

        for (uint32_t i = 0; i < values_per_miniblock && decoded < num_values; i++) {
          const uint64_t delta = ReadPackedLSB(ptr, (size_t)bytes_needed, i, bit_width);

          T last = values.back();
          values.push_back(last + static_cast<T>(min_delta + (int64_t)delta));
          decoded++;
        }
        ptr += bytes_needed;
      }
    }
  }

  return decoded;
}

// Pointer-output variant: decodes directly into a caller-owned buffer with
// capacity >= num_values, instead of a std::vector. Same algorithm as the
// vector overload above (kept in sync manually — no shared body because the
// running "last value" source differs: out[decoded-1] vs values.back()).
// Used on hot paths where the destination (a pre-sized ext_* buffer or a
// pre-reserved result vector slot) is already known, so decoding into a
// temporary vector and then std::copy-ing it over is a wasted allocation +
// extra full pass.
// Returns the number of values decoded, or -1 on error.
template <typename T>
static int32_t DecodeDeltaBinaryPacked(const uint8_t *data, size_t data_size,
                                       int32_t num_values,
                                       T *out) {
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
  uint32_t values_per_miniblock = static_cast<uint32_t>(block_size / num_miniblocks);

  while (decoded < num_values && ptr < end) {
    int64_t min_delta = ReadZigZagVarint(ptr, end);
    if (ptr > end) break;

    std::vector<uint8_t> bit_widths;
    bit_widths.reserve(num_miniblocks);
    for (uint64_t mb = 0; mb < num_miniblocks && ptr < end; mb++) {
      bit_widths.push_back(*ptr++);
    }
    if (bit_widths.size() != num_miniblocks) break;

    for (uint64_t mb = 0; mb < num_miniblocks && decoded < num_values; mb++) {
      uint8_t bit_width = bit_widths[mb];
      if (bit_width > 64) break;  // corrupt: ReadPackedLSB holds widths <= 64

      if (bit_width == 0) {
        // All deltas in this miniblock equal min_delta; no data bytes needed.
        for (uint32_t i = 0; i < values_per_miniblock && decoded < num_values; i++) {
          T last = out[decoded - 1];
          out[decoded] = last + static_cast<T>(min_delta);
          decoded++;
        }
      } else {
        const uint64_t bytes_needed =
            (static_cast<uint64_t>(values_per_miniblock) * bit_width + 7) / 8;
        if (bytes_needed > static_cast<uint64_t>(end - ptr)) break;

        for (uint32_t i = 0; i < values_per_miniblock && decoded < num_values; i++) {
          const uint64_t delta = ReadPackedLSB(ptr, (size_t)bytes_needed, i, bit_width);

          T last = out[decoded - 1];
          out[decoded] = last + static_cast<T>(min_delta + (int64_t)delta);
          decoded++;
        }
        ptr += bytes_needed;
      }
    }
  }

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
