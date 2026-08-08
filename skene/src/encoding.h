#pragma once
// Internal: section body encodings (FORMAT.md §7.7).
//
// Every encoding here is a pure size optimization. Decoding one MUST reproduce
// the plain bytes exactly — the format's correctness never depends on which
// encoding was chosen, only its size does. That is why the writer is free to
// pick per section and the reader never has to agree in advance.
//
// Only encodings this file can BOTH encode and decode may be declared in
// format.h: an unrecognised encoding on a required section is fatal, so shipping
// a declared-but-undecodable encoding would mean released readers rejecting
// valid files.

#include <cstdint>
#include <vector>

#include "skene/format.h"
#include "skene/status.h"

#include "core/buffers.h"

namespace skene {

// ─── Bit packing ────────────────────────────────────────────────────────────
//
// LSB-first, `bit_width` bits per value, no padding between values. Chosen over
// a byte-oriented varint because every array this format packs has a bound known
// in advance — selection codes are < data_length — so a fixed width beats
// per-value length bytes across the whole low-cardinality range, which is
// precisely where codes dominate file size.

// Bits needed to represent every value in [0, max_value]. 0 when max_value == 0.
uint8_t bits_required(uint64_t max_value);

// ─── Selection codes: kBitpack ──────────────────────────────────────────────

// Packs `count` codes, each known to be < `data_length`.
//
// Returns false (with `out` untouched) when packing would not be smaller than
// plain — the writer then emits kPlain. Never a failure: "not worth it" is a
// normal answer.
bool bitpack_encode_codes(const uint32_t* codes, uint32_t count,
                          uint32_t data_length, std::vector<uint8_t>* out);

// Decodes into `out`, which must hold `count` uint32.
Status bitpack_decode_codes(const uint8_t* stored, uint64_t stored_bytes,
                            uint32_t count, uint32_t* out);

// ─── Ascending integer data: kDeltaBitpack ──────────────────────────────────

// True when `type` is a flat 4- or 8-byte integer-family type whose ascending
// order makes first-order differences meaningful.
//
// Excludes floats (delta over float bit patterns is meaningless), INTERVAL and
// DECIMAL128 (16-byte composites), and the 1/2-byte widths, where an 8-byte
// header plus a first value costs more than it could ever save.
bool type_supports_delta(DrakenType type);

// Encodes an ASCENDING array. The caller guarantees ascending order — this is
// only ever called on a value-ordered column, where ascending is established by
// construction rather than assumed.
//
// Returns false when it would not be smaller than plain.
bool delta_bitpack_encode(const void* data, uint32_t count, size_t item_bytes,
                          std::vector<uint8_t>* out);

// Decodes into `out`, which must hold `count * item_bytes` bytes.
Status delta_bitpack_decode(const uint8_t* stored, uint64_t stored_bytes,
                            uint32_t count, size_t item_bytes, void* out);

// ─── General-purpose compression: kZstd ─────────────────────────────────────
//
// Applied per SECTION so each extent stays independently fetchable and
// decodable; compressing the file as a unit would be smaller but would make
// reading one column mean reading all of them.
//
// Measured on real data (TPC-H, see BENCHMARKS.md): the bit-packing and delta
// encodings already extract almost everything from numeric columns, but a
// string arena of near-unique text still holds most of its redundancy — and
// comment-style columns dominate real tables. Without this skene is ~3x larger
// than zstd Parquet on TPC-H; with it, roughly at parity.

// Returns false when compression would not be smaller — the writer then emits
// the uncompressed form. Never a failure: "not worth it" is a normal answer.
bool zstd_encode(const void* plain, size_t plain_bytes, int level,
                 std::vector<uint8_t>* out);

// Decodes into `out`, which must hold exactly `plain_bytes`.
Status zstd_decode(const uint8_t* stored, uint64_t stored_bytes,
                   uint64_t plain_bytes, uint8_t* out);

// ─── General-purpose compression: kLz4 ──────────────────────────────────────
//
// The read-first codec. Same per-section application as kZstd, different trade:
// measured on a ClickBench row group (dev/skene_codec_bench.cpp), 4.49x at
// 8931 MB/s decode against zstd-9's 7.34x at 3477 MB/s. 8931 MB/s is the rate
// the reader's own uncompressed path runs at on the same file, so LZ4's decode
// is roughly free relative to work already being done.
//
// LZ4 BLOCK format, not the frame format: a block carries no header, so it
// cannot self-describe its decoded size — the section directory's `plain_bytes`
// supplies it. That is deliberate (a frame header per section would be bytes
// spent restating what the directory already stores), and it is why the decode
// side below must treat `plain_bytes` as an exact requirement rather than a
// capacity hint.

// Returns false when compression would not be smaller — the writer then emits
// the uncompressed form. Never a failure: "not worth it" is a normal answer.
// Also returns false above LZ4's block-size ceiling, which is the same answer
// for the same reason: the section is stored plain.
bool lz4_encode(const void* plain, size_t plain_bytes, std::vector<uint8_t>* out);

// Decodes into `out`, which must hold exactly `plain_bytes`.
//
// EXACTLY is load-bearing on the upper side as well as the lower: `plain_bytes`
// is handed to LZ4 as the destination CAPACITY, and LZ4 is entitled to write
// anywhere within a capacity it is given — its final copy is a 16-byte wildcopy
// that can run past the decoded length while staying inside the declared bound.
// A caller that declares more than it allocated therefore gets a buffer overrun
// even on a body that decodes correctly. Every caller passes the section
// directory's `plain_bytes` and allocates to the same value.
Status lz4_decode(const uint8_t* stored, uint64_t stored_bytes,
                  uint64_t plain_bytes, uint8_t* out);

}  // namespace skene
