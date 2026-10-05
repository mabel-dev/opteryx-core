#ifndef _JSONL_INTERPRETER_HPP_
#define _JSONL_INTERPRETER_HPP_

#include <vector>
#include <string>
#include <cstdint>
#include <optional>

#include "markers.hpp"
#include "parse_context.hpp"

namespace rugo::_jsonl {

// A column to capture from each record, matched by exact bytes (length + first-byte fast
// reject, then memcmp — no hashing). The wanted set is the output columns ∪ predicate
// columns. pred_idx is the index into MapProjection::predicates when this column carries an
// inline-evaluated predicate, else -1. `out` is its capture slot: < MapProjection::ncols for
// an output column (its index in the output), beyond for a predicate-only column.
struct WantedColumn {
    const char* name;
    uint32_t    len;
    uint8_t     first;     // name[0], for fast reject
    int         pred_idx;  // predicate index, or -1
    uint32_t    out = 0;   // capture slot

    // Optional ONE-LEVEL nested sub-key (nested_column.hpp): when `sub_len` is non-zero,
    // the wanted value is not this key's value but the value of `sub` INSIDE it
    // (`commit->>'collection'`). The container's bytes are then never materialised — the
    // same marker walk that bounds the container also matches the sub-key inside it and
    // emits a span for the sub-value alone, tagged with `slot`.
    //
    // Matched at nesting depth 1 ONLY: `commit->>'collection'` must never match
    // `commit.record.collection`. Depth is guarded structurally by find_nested_field.
    const char* sub     = nullptr;
    uint32_t    sub_len = 0;
    uint8_t     sub_first = 0;  // sub[0], for fast reject
    uint8_t     slot = 0;       // FieldSpan::slot for this column's spans (0 = top-level)

    // Several wanted columns can share one top-level key (`commit->>'a'`, `commit->>'b'`,
    // and `commit` itself). The key is matched once; `next` chains to the next wanted
    // column with the same key (index into MapProjection::columns, -1 = end), so one pass
    // over the value serves all of them.
    int next = -1;
};

// Projection + predicate pushdown for build_columns. MINIMAL EXTENT: only fields whose key
// matches a wanted column are captured, and once every wanted column is resolved nothing
// more is materialised for the record; with `early_exit` its tail is not even parsed (see
// build_columns).
// Predicate columns are evaluated INLINE the moment their value is captured — a failing row
// stops materialising right there. Ordinals count every field, so captured spans carry true
// positions.
//
// A key that occurs more than once in a record: its FIRST occurrence resolves the column
// (and, for a nested column, its first container decides — as yyjson's object lookup, and so
// draken's `->>`, does); later occurrences are ignored.
struct MapProjection {
    const std::vector<WantedColumn>* columns;
    const std::vector<Predicate>*    predicates;
    const std::vector<uint32_t>*     pred_slot;  // per predicate: the capture slot of its column
    size_t                           ncols;      // output columns (capture slots [0, ncols))
    // Per output column: 1 = copy each captured value's bytes into ColumnMap::arena while
    // they are hot (see ColumnMap); 0 = reference the source buffer. head_copy_columns.
    const std::vector<uint8_t>*      copy_bytes;
    // Early exit (build_columns): stop reading each record once every wanted column is
    // resolved, and only check the rest of its line's brackets and strings. Set when the
    // read has a projection; an unprojected read wants every key, so it has no tail to skip.
    bool                             early_exit;
};

// The column-major document map: for each output column, one span per surviving row —
// the column's value in that row, or an ABSENT span (span_absent) when the record does not
// carry it. Rows are records that are well-formed lines and pass every predicate, in input
// order. Column builders index it directly: no per-row key search.
//
// A COPIED column (copied[c] = 1) holds its values' bytes in arena[c], copied when each
// record closed — while the parse window was still in cache — and its spans' value_start
// index the arena (a string's body is followed by its closing quote, as in the source, so
// every reader of a span sees the same bytes either way). The column builders then read
// one dense array instead of striding the cold source buffer. An uncopied column's spans
// index the source buffer. Which columns are copied changes no value, only where the
// builders read it (head_copy_columns).
struct ColumnMap {
    size_t rows = 0;
    std::vector<std::vector<FieldSpan>> cols;
    std::vector<std::vector<uint8_t>> arena;   // per column: the copied values' bytes
    std::vector<uint8_t> copied;               // per column: 1 = spans index arena[c]

    // First malformed input, as RecordSet::malformed* (only consulted with fail_on_error).
    bool     malformed = false;
    uint32_t malformed_pos = 0;
    uint32_t malformed_count = 0;

    size_t num_records() const { return rows; }

    // The bytes column `c`'s spans index: its arena when copied, else `source`.
    const uint8_t* base(size_t c, const uint8_t* source) const {
        if (!copied[c]) return source;
        return arena[c].empty() ? reinterpret_cast<const uint8_t*>("") : arena[c].data();
    }
};

// The span a ColumnMap holds for a row that does not carry the column.
inline FieldSpan absent_span() {
    FieldSpan f{};
    f.type = static_cast<uint8_t>(ValueType::Unknown);
    return f;
}
inline bool span_absent(const FieldSpan& f) {
    return f.type == static_cast<uint8_t>(ValueType::Unknown);
}

// A view over one record's fields inside a RecordSet's flat span arena. Cheap to copy
// (pointer + length); supports range-for and indexing so consumers read it like the old
// per-record vector.
struct RecordView {
    const FieldSpan* ptr = nullptr;
    uint32_t         n   = 0;
    const FieldSpan* begin() const { return ptr; }
    const FieldSpan* end()   const { return ptr + n; }
    size_t           size()  const { return n; }
    bool             empty() const { return n == 0; }
    const FieldSpan& operator[](size_t i) const { return ptr[i]; }
};

// Flat-arena document map: every field of every record lives in one contiguous `spans`
// buffer, with per-record ranges [offsets[r], offsets[r+1]). Replaces the old
// std::vector<std::vector<FieldSpan>> (one malloc per record) — the build allocates two
// growing buffers instead of N+1, which dominates map-build cost on narrow rows.
struct RecordSet {
    std::vector<FieldSpan> spans;     // all fields of all records, contiguous
    std::vector<uint32_t>  offsets;   // size = num_records + 1; starts {0}

    // First malformed input detected while building this set (dropped/abandoned line,
    // unterminated container, a raw unescaped control character inside a string, a
    // record left open with no closing brace at buffer/chunk end, ...). `malformed_pos`
    // is the absolute byte offset of the FIRST such occurrence, valid iff `malformed` is
    // true. Only ever consulted when ParseContext.fail_on_error is true — otherwise the
    // record producing it was already silently skipped, matching pre-existing lenient
    // behaviour. `malformed_count` is the total across every occurrence (not just the
    // first), so a caller running with fail_on_error=false can still report how many
    // rows it silently dropped instead of the drop being invisible.
    bool     malformed = false;
    uint32_t malformed_pos = 0;
    uint32_t malformed_count = 0;

    size_t     num_records() const { return offsets.empty() ? 0 : offsets.size() - 1; }
    size_t     size()        const { return num_records(); }
    RecordView operator[](size_t r) const {
        return RecordView{ spans.data() + offsets[r], offsets[r + 1] - offsets[r] };
    }
    // Append another set's records (offsets rebased onto this set's span arena).
    void append(const RecordSet& other) {
        const uint32_t base = static_cast<uint32_t>(spans.size());
        spans.insert(spans.end(), other.spans.begin(), other.spans.end());
        if (offsets.empty()) offsets.push_back(0);
        for (size_t i = 1; i < other.offsets.size(); ++i)
            offsets.push_back(base + other.offsets[i]);
        malformed_count += other.malformed_count;
        if (other.malformed && (!malformed || other.malformed_pos < malformed_pos)) {
            malformed = true;
            malformed_pos = other.malformed_pos;
        }
    }
};

// Build the column-major document map of the byte range [range_start, buffer_length) of
// `buffer`: the projection's output columns, for every surviving row.
//
// The range is processed in line-aligned windows of ~256 KB: each window is indexed by
// scan_structural_index (structural_scan.hpp — positions only, in-string structure
// masked out) into one reused buffer that stays cache-resident, and the window's index is
// walked before the next is scanned. Nothing proportional to the input is materialised
// except the output spans.
//
// Value shape is coarse (string / array / object / scalar) and read only from the
// structural delimiter — no value parsing. Container values ([…], {…}) are bounded by
// bracket depth over the masked index, so interior commas/brackets — including those
// inside strings, which the index never holds — do not truncate them. Key identity is
// never hashed; it is matched by exact bytes only for the wanted set, capturing only those
// fields and stopping each record's materialisation once they are found (minimal extent).
// Every predicate is evaluated when the record closes; a record that fails one is not a row.
//
// range_start must be the start of a line (0, or one past a newline). Every line in the
// range is judged on its own: one that is not exactly one object is rejected whole (see
// MapBuilder's line discipline), so a range can be cut at any newline.
//
// EARLY EXIT (proj.early_exit, unfiltered ranges only — not with `lines`): the range is
// read a line at a time. Each line is indexed 64 bytes at a time (starting from what the
// previous line needed) and parsed as it grows; once every wanted column is resolved — or
// an inline predicate failed — the rest of the line is NOT indexed or parsed. It is read
// once by check_line_tail (structural_scan.hpp), which finds the line's end and accepts
// the record iff, outside strings, its brackets close to depth 0 exactly once, at the
// line's last non-whitespace byte, which is '}', and the line does not end inside a
// string. A line that fails that check, or that ends before its record is finished, is
// indexed whole and judged by the full rules. So past the wanted columns a record's MEMBER
// GRAMMAR AND SCALAR TOKENS ARE NOT VALIDATED (a missing ':' or a bare word there is
// accepted): whether such a line is accepted depends on the projection (ruled 2026-10-05).
// Everything before the last wanted column, every line that is not one object, a record
// cut inside a string, unbalanced brackets and a second value on the line are still
// rejected, whatever the projection.
//
// `lines` (the raw prefilter's survivors, prefilter_lines): when given, ONLY these lines —
// ascending, each a whole line of the range — are input, and the bytes between them are
// never scanned. Each line is begun at its own start, so the skipped bytes are never
// judged by the line discipline, and the range ends with the last line.
ColumnMap build_columns(
    const uint8_t* buffer,
    size_t buffer_length,
    const MapProjection& proj,
    size_t range_start = 0,
    const std::vector<LineSpan>* lines = nullptr
);

// The row-major map of EVERY field of every record in [0, buffer_length), by the same
// parser and line discipline as build_columns. Only for the head sample — column discovery
// (discover_column_names) and the literal check (check_predicate_literals) — never the bulk
// read.
RecordSet build_map(const uint8_t* buffer, size_t buffer_length);

// Which output columns build_columns copies into ColumnMap::arena: per column, 1 when its
// first non-null value in the head sample (context.infer_sample_size records) is a string
// or scalar — short values whose cold, strided re-reads dominated the column builders —
// and 0 for a container, or a column DECLARED VARIANT / ARRAY<T>, or a `->` column: large
// values the builders already read sequentially, and which copying would only move twice.
// A nested `->>` column is copied (its sub-values are short). Decided from the head of the
// whole buffer, so every range of a threaded read decides the same.
std::vector<uint8_t> head_copy_columns(const uint8_t* buffer, size_t buffer_length,
                                       const std::vector<std::string>& columns,
                                       const ParseContext& context);

// Collect the union of keys across the RecordSet's first `sample_records` records, in
// first-seen order (for column-name discovery at the Cython edge, so RecordSet's internals
// stay opaque to Cython).
//
// NDJSON is not required to be homogeneous: a key absent from record 0 but present in
// record 3 is a real column, and typing the relation off record 0 alone silently drops it.
// `sample_records` is ParseContext.infer_sample_size — the SAME window that bounds per-column
// type inference (see column_builder.cpp's "first non-null value inside the sample window"),
// so a column discovered inside the window is always typed from inside it too. Any record
// past the window cannot introduce a column.
//
// First-seen order, not sorted: record 0's keys keep their document order, and each later
// record appends only what is new. Column order is therefore stable for the common
// homogeneous case and deterministic for the heterogeneous one.
std::vector<std::string> sample_record_keys(
    const RecordSet& rs, const uint8_t* buffer, size_t sample_records);

// The relation's column names for one read: the keys of the first
// `context.infer_sample_size` records of the INPUT (sample_record_keys over a head-only,
// unprojected, unfiltered map), narrowed to `context.projected_columns` in projection order
// when a projection is given.
//
// Discovery must not run over the records that SURVIVED predicates (or the raw prefilter):
// that makes the column set depend on which rows matched — a key absent from the matching
// rows vanished from the result instead of coming back all-null, and columns=None with
// predicates returned fewer columns than without. The head is parsed on its own (growing
// by line count until it holds `infer_sample_size` records or covers the buffer), so the
// cost is the sample window, not the input.
std::vector<std::string> discover_column_names(
    const uint8_t* buffer, size_t buffer_length, const ParseContext& context);

// Fail loud, BEFORE any row is filtered, on a predicate literal that cannot be compared
// with its column (predicate_literal.hpp): against a DECLARED column's type, and against
// every non-null value of the column in the head sample (the same records the reader
// infers column types from). Throws std::invalid_argument naming the column, its type and
// the literal. A value past the sample window whose JSON kind conflicts is still caught
// when the predicate is evaluated on it (evaluate_predicate throws).
void check_predicate_literals(
    const uint8_t* buffer, size_t buffer_length, const ParseContext& context);

}  // namespace rugo::_jsonl

#endif  // _JSONL_INTERPRETER_HPP_
