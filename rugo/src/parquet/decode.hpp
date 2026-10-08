#pragma once
#include "compression.hpp"
#include "metadata.hpp"
#include "core/append_buffer.h"  // draken::AppendBuffer — every decoded buffer
#include <cstdint>
#include <string>
#include <vector>

// Per-value predicate pushed to the decoder: one conjunct of the scan's WHERE,
// of the form `column <op> literal`. Used two ways:
//   * Dictionary decode-skip (Phase 2): evaluated against a dict-encoded
//     column's dictionary BEFORE decoding its data pages. If no unique value
//     satisfies it, the whole row group yields zero rows (for a pushed conjunct)
//     and the data pages are skipped.
//   * Page search (docs/PARQUET_PAGE_SEARCH_DESIGN.md), kinds 4/5 only, and only
//     when the caller also passes a PageSearchOut: the conjunct is evaluated on
//     the raw page bytes and only passing rows are emitted.
// Either way it only ever drops rows the conjunct proves dead; the scan's own
// filter still evaluates the full predicate over whatever survives.
struct ValuePredicate {
  // -1 none, 0 int membership (=/IN), 1 str membership (=/IN),
  // 2 str starts-with, 3 str ends-with, 4 str contains (LIKE '%x%'),
  // 5 str not-contains (NOT LIKE '%x%': a value passes when it contains none of
  // the patterns).
  int kind = -1;
  const std::vector<int64_t>*     int_vals = nullptr;  // kind 0
  const std::vector<std::string>* str_vals = nullptr;  // kinds 1..5 (operands/patterns)
};

// Page search result (docs/PARQUET_PAGE_SEARCH_DESIGN.md §3). A caller arms the
// search on ONE column by passing a non-null PageSearchOut together with a kind
// 4/5 ValuePredicate; the decoder then emits only the rows that pass (and that a
// row_mask, if any, selects) and records them here, so the caller can decode the
// row group's other columns under the same rows.
struct PageSearchOut {
  // One byte per row of the column chunk, 1 = emitted. Valid only when `applied`.
  std::vector<uint8_t> row_mask;
  // False when the decoder declined the search (not a scalar byte_array column,
  // not kind 4/5, an empty pattern): the column was decoded exactly as it would
  // have been without it and `row_mask` is meaningless.
  bool applied = false;
};

// PageIndex page-jump plan (page_index.hpp). Built by the IO pipeline from the
// column's OffsetIndex once the row group's page-pruned row_mask is final: one
// entry per DATA page in file order. A page marked `pruned` has NO surviving
// row under the mask, so the decoder must advance PAST it using these offsets
// and never read its bytes — on the remote path they were not fetched (the
// buffer holds a hole there), so header-walking into it would parse zeros as a
// STOP and silently truncate the column. Offsets are CHUNK-RELATIVE (the same
// frame as ColumnStats::data_page_offset after the caller's base subtraction)
// and point at the page HEADER; `page_sizes` covers header + payload.
//
// Invariants the decoder enforces (fail loud, never guess):
//   * a plan is only ever supplied together with a row_mask;
//   * the column has max_repetition_level == 0 (page rows == page values);
//   * every non-pruned data page the decoder reaches starts exactly at the
//     plan's offset for that page index.
struct PageJumpPlan {
  std::vector<int64_t> page_offsets;
  std::vector<int32_t> page_sizes;
  std::vector<int32_t> page_rows;
  std::vector<uint8_t> pruned;   // 1 = jump over without reading
  size_t size() const { return page_offsets.size(); }
};

// Scalar (non-owning) fields of DecodedColumn, factored into a base class so a
// single whole-subobject default-assignment resets ALL of them completely — see
// DecodedColumn::reset(). Making this a BASE (not a member) keeps every existing
// field access (`col.num_rows`, `col.success`, …) source-compatible, including
// the flat member list mirrored in parquet_reader.pxd (Cython resolves the
// members through the base transparently). If you add a scalar, add it HERE and
// it is reset for free; do NOT add scalars directly to DecodedColumn.
// Length-only string stubs (DecodedColumn::append_string_stub). kStringStubInline MUST equal
// draken's STR_INLINE_MAX (core/string_slot.h): the longest value a slot stores inline.
// io_pipeline.hpp, which includes that header, static_asserts the equality.
constexpr size_t kStringStubInline = 12;
constexpr size_t kStringStubPrefix = 4;

struct DecodedColumnMeta {
  // E33: set from the column's Parquet IntType logical-type annotation.
  // int_bit_width is the DECLARED width (8/16/32/64) and is_unsigned its
  // signedness — note both are independent of `type`/physical width: a declared
  // 8- or 16-bit column (signed OR unsigned) still arrives over the wire as
  // physical "int32" (Parquet has no int8/int16 physical storage), so
  // int_bit_width can be narrower than what `type` implies.
  //
  // 0 == not an integer column, or an integer carrying no IntType annotation.
  // The latter is NOT the same as "unknown width": a bare physical int32 or
  // int64 is exactly a 32- or 64-bit signed column (that is what PyArrow and
  // parquet-mr emit), so consumers resolve absent-width from `type`.
  bool is_unsigned = false;
  int32_t int_bit_width = 0;
  // DECIMAL logical type. Set when the column carries a "decimal(P,S)" logical
  // annotation, so the Cython layer can materialize a real DECIMAL/DECIMAL128
  // vector regardless of the physical tier (`type` int32/int64 for width<=8,
  // int128 for width 9..16) — the physical type alone can't distinguish a
  // decimal from a plain int, and precision/scale live nowhere else on the
  // decoded column.
  bool is_decimal = false;
  uint8_t decimal_precision = 0;
  uint8_t decimal_scale = 0;
  // Draken logical descriptor KIND for kinds parquet has no logical type to
  // express (today only IPV4 = 5), copied verbatim from ColumnStats — which
  // recovers it from the file's key-value metadata (metadata.cpp's
  // ApplyDrakenLogicalKV). 0 = the file says nothing, which means "don't know",
  // never "no descriptor". This decoder does not interpret it: the bits it
  // produces are identical either way; only the vector materializer acts on it.
  int32_t draken_logical_kind = 0;
  int32_t num_rows = 0;  // total rows including nulls (= sum of page_values)
  int32_t pages_skipped = 0;  // pages skipped due to row_mask (no selected rows in page)
  int32_t pages_decoded = 0;  // pages that passed the row_mask check and were decompressed/decoded
  int32_t max_rep_level = 0;  // from ColumnStats (needed by Cython for list offset reconstruction)
  int32_t max_def_level = 0;  // from ColumnStats (needed by Cython for list offset reconstruction)
  bool success = false;
  uint8_t code_width = 0;                     // bytes per code (1, 2, 4) for dict_indices
  bool dict_ordered = false;                  // dictionary page is_sorted flag
  // Zero-copy output pointers (optional). When non-null, numeric decode writes
  // directly into the caller-supplied buffer, bypassing the internal std::vector<T>.
  // Only used when max_definition_level == 0 (guaranteed non-nullable column).
  int64_t* ext_int64   = nullptr;
  double*  ext_float64 = nullptr;
  int32_t* ext_int32   = nullptr;
  float*   ext_float32 = nullptr;
  int32_t  ext_written = 0;   // elements written to the active ext_* buffer
  size_t   rle_total_length = 0;   // sum of rle_run_lengths (= num logical rows)
  int32_t  rle_last_code = -1;     // ⚠ DEFAULT -1 (page-boundary merge sentinel), NOT 0
  // Phase 2 dictionary-membership skip: set when a pushed equality/IN needle set
  // was supplied for this int dict column and NONE of the needles appears in the
  // dictionary. The data pages are NOT decoded; the dictionary survives and
  // num_rows is the logical row count. The consumer builds a Dict vector with
  // arbitrary (zero) codes — every row is a guaranteed non-match for the
  // equality, so the codes never surface (0 rows survive the conjunct).
  bool dict_all_filtered = false;
  // Set when the decoder was asked for a length-only column and stubbed at least one
  // value: string_arena then holds only the bytes a length-only consumer reads (each
  // value's full bytes when it is inline in a slot, otherwise just its 4-byte
  // prefix) — see append_string_stub. Such a column may ONLY be consumed by the
  // direct string builders, which take lengths from string_lens; anything that
  // reads `len` bytes at string_offsets[i] (the IPC serializer) would overread.
  bool payloads_stubbed = false;
};

// Structure to hold decoded column data.
// Reuse contract: a DecodedColumn may be reused across column decodes via
// reset() (retains vector capacity — the whole point). reset() MUST clear every
// owning container below AND slice-assign the scalar base; the completeness test
// test_decoded_column_reset_is_complete enforces this. 32 owning containers
// (29 std::vector + `type` + `logical_type` + `error_message`) + scalars (in DecodedColumnMeta).
struct DecodedColumn : DecodedColumnMeta {
  draken::AppendBuffer<uint8_t> valid_bits;       // Arrow-style validity bitmap: 1=valid, 0=null; empty=all-valid
  draken::AppendBuffer<int32_t> int32_values;
  draken::AppendBuffer<int64_t> int64_values;
  draken::AppendBuffer<__int128> int128_values;   // FIXED_LEN_BYTE_ARRAY DECIMAL with width 9..16
                                         //   (precision > 18 → DECIMAL128). type == "int128".
  // Flat arena for dense (non-dict) byte_array values — one entry per PRESENT
  // value, in stream order. Mirrors the string_dict_* triple below; replaces the
  // old std::vector<std::string> string_values (one heap allocation per value).
  // Dense byte_array values: value k is string_arena[string_offsets[k] ..
  // + string_lens[k]). NOT necessarily packed — a PLAIN page may be appended
  // whole (length prefixes and all) with offsets pointing past each prefix — so
  // offsets + lens are the only authority for a value's extent.
  draken::AppendBuffer<uint8_t> string_arena;
  draken::AppendBuffer<uint32_t> string_offsets;  // byte start offset per value
  draken::AppendBuffer<int32_t> string_lens;     // byte length per value
  draken::AppendBuffer<int32_t> dict_indices;      // non-empty → dict codes; per-row indices
  draken::AppendBuffer<int32_t> dict_int32_values; // compact dictionary payload for int32 columns
  draken::AppendBuffer<int64_t> dict_int64_values; // compact dictionary payload for int64 columns
  draken::AppendBuffer<__int128> dict_int128_values; // compact dictionary payload for int128 (DECIMAL128) columns
  draken::AppendBuffer<float> dict_float32_values; // compact dictionary payload for float32 columns
  draken::AppendBuffer<double> dict_float64_values; // compact dictionary payload for float64 columns
  draken::AppendBuffer<uint8_t> boolean_values;   // for boolean (using uint8_t instead of bool)
  draken::AppendBuffer<float> float32_values;     // for float32
  draken::AppendBuffer<double> float64_values;    // for float64
  std::string type; // "int32", "int64", "string", "boolean", "float32", "float64"
  // The column's LOGICAL type string as metadata.cpp built it ("varchar",
  // "decimal(P,S)", "array<byte_array>", "array<varchar>", …), copied verbatim
  // from ColumnStats. The decoder does not interpret it — the bits it produces
  // are identical either way. It is here because the physical type alone cannot
  // tell a VARCHAR apart from opaque BINARY: parquet stores both as BYTE_ARRAY
  // and distinguishes them only by the String annotation, and a LIST column
  // carries its LEAF's annotation here as "array<...>". Only the vector
  // materializer acts on it, via the one predicate in parquet_reader.pxi.
  // Empty = the file says nothing, which means "don't know", never "not a string".
  std::string logical_type;
  // Raw level vectors (populated when max_rep > 0 or max_def > 0, respectively).
  // Used by the Cython binding for list column offset/null-bitmap reconstruction.
  draken::AppendBuffer<int32_t> rep_levels;  // one entry per logical value (all pages)
  draken::AppendBuffer<int32_t> def_levels;  // one entry per logical value (all pages)
  // Per-nesting-depth definition-level thresholds, copied verbatim from
  // ColumnStats — see the comment there for the derivation. Size is
  // max_rep_level + 1 for a list column (index 0 unused, depth k at [k]); empty
  // for a non-list column. Consumers walking rep/def levels MUST read these
  // rather than assuming the all-OPTIONAL constants 2k-1 / 2k; a column whose
  // element, LIST group or an intermediate level is REQUIRED shifts them.
  std::vector<int32_t> list_def_thresholds;
  // Specific, actionable failure reason (e.g. a decompression error). Empty when
  // the column decoded, or when it was an "unsupported shape" honest rejection
  // (those stay message-less so the caller reports its generic decode failure).
  // A non-empty message means a genuine error the caller should surface verbatim.
  std::string error_message;

  // Arena for byte_array dict strings. Entry k's bytes are
  // string_dict_arena[string_dict_offsets[k] .. + string_dict_lens[k]).
  // The arena is NOT packed: it is the decompressed dictionary page itself
  // (zero-copy — each entry's 4-byte PLAIN length prefix stays in place between
  // entries), with any re-interned values appended after it. offsets + lens are
  // the ONLY authority for an entry's extent; never derive a length from the
  // distance between two offsets or from the arena size.
  draken::AppendBuffer<uint8_t> string_dict_arena;
  draken::AppendBuffer<uint32_t> string_dict_offsets;  // byte start offset per entry
  draken::AppendBuffer<int32_t> string_dict_lens;     // byte length per entry

  // Packed dictionary codes for nullable dict columns
  draken::AppendBuffer<uint8_t> dict_codes_array;      // Full-width packed code array (code_width bytes per row)
                                              // One code per row (nulls filled with 0); empty = not used

  // ── RLE skip-dense outputs ─────────────────────────────────────────────────
  // Populated instead of dict_indices for non-nullable dict columns when
  // max_definition_level == 0.  C++ resolves dict codes to actual values (one
  // lookup per run rather than per row), eliminating the O(N) dict_indices
  // allocation and the subsequent Cython O(N) scan.
  //
  // int32 dict → int64 column and float32 dict → float64 column are both
  // widened in C++ to avoid extra Cython type-switching complexity.
  //
  // Exactly one of {rle_int64_values, rle_float64_values, rle_str_lens} is
  // non-empty for a given column; rle_run_lengths is shared across all types.
  draken::AppendBuffer<int64_t> rle_int64_values;    // int32 and int64 dict columns
  draken::AppendBuffer<double> rle_float64_values;  // float32 and float64 dict columns
  draken::AppendBuffer<int32_t> rle_run_lengths;     // shared repeat counts [num_runs]
  // String RLE (byte_array dict columns):
  draken::AppendBuffer<uint8_t> rle_str_arena;       // packed bytes for all run string values
  draken::AppendBuffer<uint32_t> rle_str_offsets;     // byte offset per run in arena [num_runs]
  draken::AppendBuffer<int32_t> rle_str_lens;        // byte length per run value [num_runs]

  // Append one dense byte_array value to the string arena triple.
  void append_string(const void* p, size_t len) {
    string_offsets.push_back(static_cast<uint32_t>(string_arena.size()));
    string_lens.push_back(static_cast<int32_t>(len));
    const uint8_t* b = static_cast<const uint8_t*>(p);
    string_arena.append(b, len);
  }

  // Length-only counterpart of append_string: the planner proved no read of this
  // column ever dereferences a long value's payload, so only what the slot builder
  // consumes is kept. A value of at most kStringStubInline bytes is stored whole
  // (it lives inline in its slot); a longer one keeps just its first 4 bytes (the
  // slot's lex-order prefix). string_lens records the TRUE length, so offsets +
  // lens remain the authority for a value's extent and `len` bytes must never be
  // read back at string_offsets[i] for a stubbed value. Sets payloads_stubbed.
  void append_string_stub(const void* p, size_t len) {
    string_offsets.push_back(static_cast<uint32_t>(string_arena.size()));
    string_lens.push_back(static_cast<int32_t>(len));
    const uint8_t* b = static_cast<const uint8_t*>(p);
    string_arena.append(b, len <= kStringStubInline ? len : kStringStubPrefix);
    payloads_stubbed = true;
  }

  // Reset to the default-constructed state WITHOUT releasing vector capacity, so
  // the buffers are reused by the next decode. clear() retains capacity for the
  // trivially-destructible containers. The base slice-assign resets all 18 scalars
  // completely (incl. rle_last_code == -1). Keep this in sync with the member
  // list above — test_decoded_column_reset_is_complete (in
  // decoded_column_reset_test.cpp) is the runtime guard, and the static_assert
  // directly below this struct is the compile-time member-addition tripwire.
  void reset() {
    valid_bits.clear();          int32_values.clear();        int64_values.clear();
    int128_values.clear();       dict_indices.clear();
    string_arena.clear();        string_offsets.clear();       string_lens.clear();
    dict_int32_values.clear();   dict_int64_values.clear();    dict_int128_values.clear();
    dict_float32_values.clear();
    dict_float64_values.clear(); boolean_values.clear();       float32_values.clear();
    float64_values.clear();      rep_levels.clear();           def_levels.clear();
    list_def_thresholds.clear();
    string_dict_arena.clear();   string_dict_offsets.clear();  string_dict_lens.clear();
    dict_codes_array.clear();    rle_int64_values.clear();     rle_float64_values.clear();
    rle_run_lengths.clear();     rle_str_arena.clear();        rle_str_offsets.clear();
    rle_str_lens.clear();
    type.clear();                error_message.clear();       logical_type.clear();
    static_cast<DecodedColumnMeta&>(*this) = DecodedColumnMeta{};  // resets all meta scalars
  }
};

// Member-addition tripwire for the reuse contract above. A member added to
// DecodedColumn but omitted from reset() leaks stale data from the previous
// column into the next decode — a silent wrong answer, not a crash — so a new
// member must not be able to land quietly. This breaks the BUILD the moment the
// member list changes, naming the two obligations that come with it.
//
// It cannot live inside the class body: DecodedColumn is incomplete there.
//
// The sizes are DERIVED, never hardcoded: sizeof(std::string) is 24 on libc++
// (ARM dev) and 32 on libstdc++ (x86 prod), so a literal byte count would pin
// this to one toolchain and break the other's wheel build. Every std::vector<T>
// (and every draken::AppendBuffer<T>) instantiation is the same size regardless
// of T, so counting one of each is exact.
//
// These two counts are the SINGLE SOURCE OF TRUTH for the member list, shared
// with the test: decoded_column_reset_test.cpp asserts its own coverage lists
// against them. That is what makes the guard a chain rather than two independent
// numbers — bumping a count here to clear this assert breaks the test file until
// the new member is actually added to its coverage list, and therefore actually
// filled, reset and checked.
// kDecodedColumnVectorMembers counts every owning element container (the test's
// X-list); all but list_def_thresholds (a std::vector copied from ColumnStats)
// are draken::AppendBuffers.
constexpr int kDecodedColumnVectorMembers = 29;
constexpr int kDecodedColumnStdVectorMembers = 1;  // list_def_thresholds
constexpr int kDecodedColumnStringMembers = 3;  // type, logical_type, error_message

static_assert(sizeof(DecodedColumn) ==
                  sizeof(DecodedColumnMeta)
                  + kDecodedColumnStdVectorMembers * sizeof(std::vector<int32_t>)
                  + (kDecodedColumnVectorMembers - kDecodedColumnStdVectorMembers)
                        * sizeof(draken::AppendBuffer<int32_t>)
                  + kDecodedColumnStringMembers * sizeof(std::string),
              "DecodedColumn gained or lost a member: clear it in reset() AND cover it in "
              "decoded_column_reset_test.cpp (bump the count above and the X-list there).");

// Companion tripwire for the scalar base. Scalars are reset for free by the
// slice-assign in reset(), so this is not a correctness guard — it exists so a
// new scalar's DEFAULT gets covered by the test (rle_last_code == -1 is the
// reason that matters). LP64 layout, both targets.
static_assert(sizeof(DecodedColumnMeta) == 96,
              "DecodedColumnMeta gained or lost a scalar: assert its default in "
              "decoded_column_reset_test.cpp.");

// Structure to hold a decoded table
struct DecodedTable {
  std::vector<std::vector<DecodedColumn>> row_groups; // [row_group][column]
  std::vector<std::string> column_names;
  bool success = false;
  // Specific, actionable failure reason when success == false and the failure is
  // a genuine error (a decompression error, corruption, or a metadata read
  // failure). Empty when the table decoded, or when success==false is only an
  // honest per-column non-result (e.g. an absent column) the API tolerates.
  std::string error;
};

// Check if a parquet file can be decoded with our limited decoder
// Returns true only if:
// - All columns are uncompressed
// - All columns use PLAIN encoding
// - All columns are int32, int64, or string types
bool CanDecode(const std::string &path);

// Check if parquet data in memory can be decoded
bool CanDecode(const uint8_t* data, size_t size);

// NEW PRIMARY API: Read parquet data from memory view with column selection.
// Designed to be called serially; Opteryx achieves parallelism at the
// inter-file level by running multiple decode calls concurrently.
DecodedTable ReadParquet(const uint8_t* data, size_t size,
                         const std::vector<std::string>& column_names);

// Overload that decodes all columns when none are specified
DecodedTable ReadParquet(const uint8_t* data, size_t size);

// Overload with a row-group skip mask. `row_group_mask[rg] == 0` skips decoding
// that row group entirely (it is emitted empty) — used for predicate pushdown,
// where the caller has already pruned via footer statistics. An empty mask
// decodes every row group. The mask is sized to the file's row-group count;
// out-of-range / short masks treat missing entries as "decode".
DecodedTable ReadParquet(const uint8_t* data, size_t size,
                         const std::vector<std::string>& column_names,
                         const std::vector<uint8_t>& row_group_mask);

// Decode a single column chunk from an isolated range-read buffer.
// Offsets in target_col must be relative to the start of the buffer
// (i.e. subtract base_offset before calling).
// prefer_dict: when true AND the column is dictionary-encoded int32/int64/float,
// keep the dictionary + per-row codes instead of resolving codes to values (the
// rle skip-dense path) or materialising dense. The caller then builds a §11
// "compressed" (Dict-shaped) DrakenVector. No-op on plain pages / non-numeric dict.
// skip_pred (Phase 2): if non-null and no dictionary value satisfies it, the data
// pages are not decoded and dict_all_filtered is set. This is supported UNDER a
// row_mask as well (a page-pruning mask; the pipeline declines it for a
// caller-supplied pass-2 mask — see io_pipeline.hpp): the dictionary decides
// exactly as it does unmasked, and num_rows then reports the mask's survivor
// count, not the chunk's. A LIST column under a mask is the one shape the skip
// declines — its mask is per logical row while num_values counts slots.
// prefer_dict is likewise armed under a mask: a masked dict column comes back
// Dict-shaped with its codes compacted to the survivors.
// jump: PageIndex page-jump plan (see PageJumpPlan) — only with a row_mask.
// search: page search (see PageSearchOut) — armed only together with a kind 4/5
// skip_pred. The column then emits only the rows that pass the conjunct (ANDed
// with row_mask when one is given) and search->row_mask says which.
// In-place (buffer-reusing) primary: decodes into caller-owned `out`, resetting
// it at entry. Hoist one `out` above a per-row-group column loop and pass it each
// column to reuse its vector capacity across columns. `out` is a function-local
// per worker invocation — no cross-thread sharing.
// Cross-query chunk cache request (docs/C7_PAGE_CACHE_DESIGN.md; the cache is
// draken/core/chunk_cache.h). `data` handed to DecodeColumnFromChunk must be
// the chunk itself (chunk-relative offsets in `target_col`), as the IO pipeline
// passes it; page bytes are cached by their offset within that buffer.
// nullptr = no cache (standalone rugo, the Python reader, the patcher).
struct ChunkCacheRequest {
  const char* path;
  size_t      path_len;
  int64_t     chunk_offset;  // absolute file offset of the chunk's first byte
  bool        admit;         // false: use hits, never fill (compaction)
  bool        remote;        // fetched over the network: a refill pays a re-fetch
};

void DecodeColumnFromChunk(DecodedColumn& out, const uint8_t* data, size_t size,
                           const ColumnStats* target_col,
                           int64_t* ext_int64   = nullptr,
                           double*  ext_float64 = nullptr,
                           int32_t* ext_int32   = nullptr,
                           float*   ext_float32 = nullptr,
                           const uint8_t* row_mask = nullptr,
                           bool prefer_dict = false,
                           const ValuePredicate* skip_pred = nullptr,
                           const PageJumpPlan* jump = nullptr,
                           PageSearchOut* search = nullptr,
                           bool length_only = false,
                           const ChunkCacheRequest* cache = nullptr);

// In-place convenience: mask-only (matches the 4-arg by-value convenience below).
inline void DecodeColumnFromChunk(DecodedColumn& out, const uint8_t* data, size_t size,
                                  const ColumnStats* target_col,
                                  const uint8_t* row_mask,
                                  bool prefer_dict = false,
                                  const ValuePredicate* skip_pred = nullptr,
                                  const PageJumpPlan* jump = nullptr,
                                  PageSearchOut* search = nullptr,
                                  bool length_only = false,
                                  const ChunkCacheRequest* cache = nullptr) {
  DecodeColumnFromChunk(out, data, size, target_col,
                        nullptr, nullptr, nullptr, nullptr,
                        row_mask, prefer_dict, skip_pred, jump, search, length_only, cache);
}

// By-value overload (thin shim over the in-place primary — see decode_column.cpp).
DecodedColumn DecodeColumnFromChunk(const uint8_t* data, size_t size,
                                    const ColumnStats* target_col,
                                    int64_t* ext_int64   = nullptr,
                                    double*  ext_float64 = nullptr,
                                    int32_t* ext_int32   = nullptr,
                                    float*   ext_float32 = nullptr,
                                    const uint8_t* row_mask = nullptr,
                                    bool prefer_dict = false,
                                    const ValuePredicate* skip_pred = nullptr,
                                    const PageJumpPlan* jump = nullptr);

// Convenience overload: no ext_* zero-copy buffers, only a row_mask.
// Matches the 4-argument Cython binding DecodeColumnFromChunk(data, size, col, mask).
inline DecodedColumn DecodeColumnFromChunk(const uint8_t* data, size_t size,
                                           const ColumnStats* target_col,
                                           const uint8_t* row_mask,
                                           bool prefer_dict = false,
                                           const ValuePredicate* skip_pred = nullptr,
                                           const PageJumpPlan* jump = nullptr) {
  return DecodeColumnFromChunk(data, size, target_col,
                               nullptr, nullptr, nullptr, nullptr,
                               row_mask, prefer_dict, skip_pred, jump);
}

// Decode a specific column from memory buffer for a specific row group.
// Pass non-null ext_* pointer (pre-allocated, capacity >= row_group.num_rows)
// to decode directly into a caller-supplied buffer and skip the internal
// std::vector<T> entirely.  Only valid when max_definition_level == 0.
DecodedColumn DecodeColumnFromMemory(const uint8_t* data, size_t size, 
                                   const std::string &column_name,
                                   const RowGroupStats &row_group, 
                                   int row_group_index,
                                   int64_t* ext_int64   = nullptr,
                                   double*  ext_float64 = nullptr,
                                   int32_t* ext_int32   = nullptr,
                                   float*   ext_float32 = nullptr);

// Decode every requested column for exactly ONE row group. Same underlying
// DecodeColumnFromMemory primitive ReadParquet uses internally, but scoped to
// a single row group so a streaming caller can consume and release it before
// decoding the next one. ReadParquet decodes and retains every row group of
// the file in one DecodedTable before returning anything (see its comment
// above) — unbounded memory on a large file; this is the primitive a real
// streaming reader is built on. Throws std::runtime_error on a genuine
// per-column decode failure, with the same "row group N, column 'X': reason"
// message ReadParquet raises for the equivalent failure.
std::vector<DecodedColumn> DecodeRowGroupColumns(
    const uint8_t* data, size_t size,
    const std::vector<std::string>& column_names,
    const RowGroupStats& row_group, int row_group_index);

// Legacy file-based functions (kept for backward compatibility)
DecodedColumn DecodeColumn(const std::string &path, const std::string &column_name, 
                           const RowGroupStats &row_group, int row_group_index);

DecodedColumn DecodeColumn(const std::string &path, const std::string &column_name);
