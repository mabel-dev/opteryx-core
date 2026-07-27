#pragma once
#include "metadata.hpp"
#include <cstdint>
#include <string>
#include <vector>

// Dictionary-membership decode-skip predicate (Phase 2). Evaluated against a
// dict-encoded column's dictionary BEFORE decoding its data pages: if no unique
// value satisfies it, the whole row group yields zero rows (for a pushed
// conjunct) and the data pages are skipped. Valid for any per-value predicate;
// kinds below cover what the planner pushes today.
struct DictSkipPredicate {
  // -1 none, 0 int membership (=/IN), 1 str membership (=/IN),
  // 2 str starts-with, 3 str ends-with, 4 str contains.
  int kind = -1;
  const std::vector<int64_t>*     int_vals = nullptr;  // kind 0
  const std::vector<std::string>* str_vals = nullptr;  // kinds 1..4 (operands/patterns)
};

// Scalar (non-owning) fields of DecodedColumn, factored into a base class so a
// single whole-subobject default-assignment resets ALL of them completely — see
// DecodedColumn::reset(). Making this a BASE (not a member) keeps every existing
// field access (`col.num_rows`, `col.success`, …) source-compatible, including
// the flat member list mirrored in parquet_reader.pxd (Cython resolves the
// members through the base transparently). If you add a scalar, add it HERE and
// it is reset for free; do NOT add scalars directly to DecodedColumn.
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
};

// Structure to hold decoded column data.
// Reuse contract: a DecodedColumn may be reused across column decodes via
// reset() (retains vector capacity — the whole point). reset() MUST clear every
// owning container below AND slice-assign the scalar base; the completeness test
// test_decoded_column_reset_is_complete enforces this. 28 owning containers
// (26 std::vector + `type` + `error_message`) + scalars (in DecodedColumnMeta).
struct DecodedColumn : DecodedColumnMeta {
  std::vector<uint8_t> valid_bits;       // Arrow-style validity bitmap: 1=valid, 0=null; empty=all-valid
  std::vector<int32_t> int32_values;
  std::vector<int64_t> int64_values;
  std::vector<__int128> int128_values;   // FIXED_LEN_BYTE_ARRAY DECIMAL with width 9..16
                                         //   (precision > 18 → DECIMAL128). type == "int128".
  std::vector<std::string> string_values; // For byte_array: either flat values (dict_indices empty)
                                           //   or the compact dictionary (dict_indices non-empty)
  std::vector<int32_t> dict_indices;      // non-empty → string_values is the dict; per-row indices
  std::vector<int32_t> dict_int32_values; // compact dictionary payload for int32 columns
  std::vector<int64_t> dict_int64_values; // compact dictionary payload for int64 columns
  std::vector<__int128> dict_int128_values; // compact dictionary payload for int128 (DECIMAL128) columns
  std::vector<float> dict_float32_values; // compact dictionary payload for float32 columns
  std::vector<double> dict_float64_values; // compact dictionary payload for float64 columns
  std::vector<uint8_t> boolean_values;   // for boolean (using uint8_t instead of bool)
  std::vector<float> float32_values;     // for float32
  std::vector<double> float64_values;    // for float64
  std::string type; // "int32", "int64", "string", "boolean", "float32", "float64"
  // Raw level vectors (populated when max_rep > 0 or max_def > 0, respectively).
  // Used by the Cython binding for list column offset/null-bitmap reconstruction.
  std::vector<int32_t> rep_levels;  // one entry per logical value (all pages)
  std::vector<int32_t> def_levels;  // one entry per logical value (all pages)
  // Specific, actionable failure reason (e.g. a decompression error). Empty when
  // the column decoded, or when it was an "unsupported shape" honest rejection
  // (those stay message-less so the caller reports its generic decode failure).
  // A non-empty message means a genuine error the caller should surface verbatim.
  std::string error_message;

  // Flat arena for byte_array dict strings — eliminates one heap allocation per
  // unique dictionary value (replaces the old std::vector<std::string> dict_string).
  std::vector<uint8_t>  string_dict_arena;    // packed bytes for all dict entries
  std::vector<uint32_t> string_dict_offsets;  // byte start offset per entry
  std::vector<int32_t>  string_dict_lens;     // byte length per entry

  // Packed dictionary codes for nullable dict columns
  std::vector<uint8_t> dict_codes_array;      // Full-width packed code array (code_width bytes per row)
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
  std::vector<int64_t>  rle_int64_values;    // int32 and int64 dict columns
  std::vector<double>   rle_float64_values;  // float32 and float64 dict columns
  std::vector<int32_t>  rle_run_lengths;     // shared repeat counts [num_runs]
  // String RLE (byte_array dict columns):
  std::vector<uint8_t>  rle_str_arena;       // packed bytes for all run string values
  std::vector<uint32_t> rle_str_offsets;     // byte offset per run in arena [num_runs]
  std::vector<int32_t>  rle_str_lens;        // byte length per run value [num_runs]

  // Reset to the default-constructed state WITHOUT releasing vector capacity, so
  // the buffers are reused by the next decode. clear() retains capacity for the
  // trivially-destructible containers; for `string_values` the inner std::strings
  // are freed (outer array retained). The base slice-assign resets all 18 scalars
  // completely (incl. rle_last_code == -1). Keep this in sync with the member
  // list above — test_decoded_column_reset_is_complete is the guard.
  void reset() {
    valid_bits.clear();          int32_values.clear();        int64_values.clear();
    int128_values.clear();       string_values.clear();       dict_indices.clear();
    dict_int32_values.clear();   dict_int64_values.clear();    dict_int128_values.clear();
    dict_float32_values.clear();
    dict_float64_values.clear(); boolean_values.clear();       float32_values.clear();
    float64_values.clear();      rep_levels.clear();           def_levels.clear();
    string_dict_arena.clear();   string_dict_offsets.clear();  string_dict_lens.clear();
    dict_codes_array.clear();    rle_int64_values.clear();     rle_float64_values.clear();
    rle_run_lengths.clear();     rle_str_arena.clear();        rle_str_offsets.clear();
    rle_str_lens.clear();
    type.clear();                error_message.clear();
    static_cast<DecodedColumnMeta&>(*this) = DecodedColumnMeta{};  // resets all meta scalars
  }
};

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
// pages are not decoded and dict_all_filtered is set.
// In-place (buffer-reusing) primary: decodes into caller-owned `out`, resetting
// it at entry. Hoist one `out` above a per-row-group column loop and pass it each
// column to reuse its vector capacity across columns. `out` is a function-local
// per worker invocation — no cross-thread sharing.
void DecodeColumnFromChunk(DecodedColumn& out, const uint8_t* data, size_t size,
                           const ColumnStats* target_col,
                           int64_t* ext_int64   = nullptr,
                           double*  ext_float64 = nullptr,
                           int32_t* ext_int32   = nullptr,
                           float*   ext_float32 = nullptr,
                           const uint8_t* row_mask = nullptr,
                           bool prefer_dict = false,
                           const DictSkipPredicate* skip_pred = nullptr);

// In-place convenience: mask-only (matches the 4-arg by-value convenience below).
inline void DecodeColumnFromChunk(DecodedColumn& out, const uint8_t* data, size_t size,
                                  const ColumnStats* target_col,
                                  const uint8_t* row_mask,
                                  bool prefer_dict = false,
                                  const DictSkipPredicate* skip_pred = nullptr) {
  DecodeColumnFromChunk(out, data, size, target_col,
                        nullptr, nullptr, nullptr, nullptr,
                        row_mask, prefer_dict, skip_pred);
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
                                    const DictSkipPredicate* skip_pred = nullptr);

// Convenience overload: no ext_* zero-copy buffers, only a row_mask.
// Matches the 4-argument Cython binding DecodeColumnFromChunk(data, size, col, mask).
inline DecodedColumn DecodeColumnFromChunk(const uint8_t* data, size_t size,
                                           const ColumnStats* target_col,
                                           const uint8_t* row_mask,
                                           bool prefer_dict = false,
                                           const DictSkipPredicate* skip_pred = nullptr) {
  return DecodeColumnFromChunk(data, size, target_col,
                               nullptr, nullptr, nullptr, nullptr,
                               row_mask, prefer_dict, skip_pred);
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

// Legacy file-based functions (kept for backward compatibility)
DecodedColumn DecodeColumn(const std::string &path, const std::string &column_name, 
                           const RowGroupStats &row_group, int row_group_index);

DecodedColumn DecodeColumn(const std::string &path, const std::string &column_name);
