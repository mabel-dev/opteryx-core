#pragma once
// skene/patch.h — change a file's COLUMNS without decoding its data.
//
// Drops and renames columns, producing a new file. The columns that survive are
// copied section-for-section, byte-for-byte: nothing is decoded, no checksum is
// recomputed over data, and no encoding decision is revisited. Only the footers
// are rebuilt.
//
// That is possible because skene keeps every column's bytes in sections located
// by ABSOLUTE offsets in a footer, separate from the bytes themselves. A rename
// touches no data section at all — names live only in footers — so the whole
// DATA+INDEX region comes out bit-identical. A drop omits exactly the dropped
// column's sections and copies the rest, so the result is a compaction that
// happens to lose a column, not a file with dead bytes in it.
//
// The cost tracks the file's SIZE, not the number of values in it.
//
// This never modifies its input. The caller writes the result somewhere new and
// points a snapshot at it, so anything still referring to the original keeps
// reading the shape it was written under.
//
// WHY THIS EXISTS WITH NO CALLER
// ------------------------------
// No connector writes skene tables today, so no SQL statement reaches this. It
// is here because the engine's column DDL (ALTER TABLE ... DROP/RENAME COLUMN)
// is a capability of the ENGINE, not of one file format, and skene's
// metadata/data split supports it exactly as parquet's footer does. Building it
// only when a writer appears would mean discovering then whether the format
// could carry it — after the format had frozen.
//
// NOT YET SUPPORTED, and refused rather than approximated:
//   - retyping a column    (needs the values re-encoded, since skene stores the
//                           exact DrakenType and so the exact item width)
//   - dropping or renaming an ARRAY child; only top-level columns are named

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "skene/status.h"

namespace skene {

// A column to ADD, described by a DONOR file: a complete .skene file holding
// exactly one column of exactly one row, written by skene's own writer.
//
// The donor carries the new column's name, DrakenType, logical descriptor and
// the value every existing row is filled with. Taking all of that from a file
// the writer produced means this code needs no copy of draken's type mapping —
// there is no second version of it to drift.
//
// Skene makes the rest almost free, because a constant column is a first-class
// shape rather than a special case: `selection_kind == CONSTANT` stores ONE
// value and no selection section, and the reader attaches the shared zero
// selection so every row reads `data[0]`. A one-row donor and an N-row constant
// column therefore have the SAME data section — they differ only in `length`.
// The one thing that scales with N is the validity bitmap, and only when the
// fill is NULL, where it is `ceil(N/8)` zero bytes.
//
// Whether the fill is NULL is read from the donor's own row, not passed
// separately: a flag that disagreed with the donor would be a second source of
// truth for the same fact.
using DonorFile = std::vector<uint8_t>;

// Produce a new file from `file`, with `drop` removed, `rename` applied, and a
// column appended per entry in `add`.
//
// `rename` is (old_name, new_name). All three operate on TOP-LEVEL column
// names, matched exactly.
//
// A name in `drop` or `rename` that the file does not have is an error, as is
// an `add` whose name is already in use: the caller believes something about
// this file that is not true, and quietly doing nothing would leave it
// believing it.
Status patch_columns(const void* file, size_t file_bytes,
                     const std::vector<std::string>& drop,
                     const std::vector<std::pair<std::string, std::string>>& rename,
                     const std::vector<DonorFile>& add,
                     std::vector<uint8_t>* out);

// Drop/rename only.
inline Status patch_columns(const void* file, size_t file_bytes,
                            const std::vector<std::string>& drop,
                            const std::vector<std::pair<std::string, std::string>>& rename,
                            std::vector<uint8_t>* out) {
    return patch_columns(file, file_bytes, drop, rename, {}, out);
}

}  // namespace skene
