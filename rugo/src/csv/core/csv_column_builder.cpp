#include <Python.h>

#include "csv_column_builder.hpp"
#include "../../declared_parse.hpp"   // explicit_schema strict per-value parse (shared with JSONL)
#include "../../predicate_literal.hpp" // predicate literal vs column type contract (shared with JSONL)
#include "csv_scan.hpp"

#include <algorithm>
#include <cstring>
#include <thread>
#include <future>
#include <exception>
#include <stdexcept>

#include "draken_bridge.h"
#include "string_slot.h"
#include "alloc.h"
#include "BS_thread_pool.hpp"

namespace rugo::_csv {

// ---------------------------------------------------------------------------
// unescape_csv_field
// ---------------------------------------------------------------------------

uint32_t unescape_csv_field(
    const uint8_t* src,
    uint32_t       len,
    uint8_t*       out) noexcept
{
    uint32_t out_len = 0;
    uint32_t i = 0;
    while (i < len) {
        const uint8_t c = src[i];
        if (c == '"' && i + 1 < len && src[i + 1] == '"') {
            out[out_len++] = '"';
            i += 2;
        } else {
            out[out_len++] = c;
            ++i;
        }
    }
    return out_len;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

namespace {

// How a predicate compares, fixed per predicate once its column's type is known
// (build_columns_streaming). The literal's kind was checked against that type
// first (predicate_literal.hpp), so each mode only ever sees a literal it can take.
enum PredMode : uint8_t {
    PRED_STRING  = 0,   // string-family column: byte-wise compare, never numeric
    PRED_NUMERIC = 1,   // numeric column: numeric compare, never byte-wise
    PRED_BOOL    = 2,   // BOOL column (declared): true/false, false < true
    PRED_OTHER   = 3,   // type outside the contract (DATE, DECIMAL, IPV4, ...):
                        // the reader's original compare — numeric when both the
                        // literal and the field parse as numbers, else byte-wise
};

// One pushed predicate, with its literal pre-parsed ONCE (never per row).
struct PredEval {
    uint8_t     op;
    uint8_t     mode;
    bool        is_int;    // literal parsed as int64
    bool        is_float;  // literal parsed as float64 (and not int64)
    bool        bool_val;
    int64_t     i64;
    double      f64;
    std::string str;
};

template <typename T>
static inline bool apply_op(uint8_t op, T a, T b) {
    switch (op) {
        case 0: return a == b;
        case 1: return a != b;
        case 2: return a <  b;
        case 3: return a <= b;
        case 4: return a >  b;
        case 5: return a >= b;
    }
    return false;
}

// Numeric compare of a field against p's literal. Returns false (no match) when
// the field is not a number: under PRED_NUMERIC such a field is a type mismatch
// that commit_row reports (or, with ignore_errors, stores as NULL — which matches
// nothing either).
static bool eval_numeric(const PredEval& p, const uint8_t* fptr, uint32_t flen) {
    if (flen == 0) return false;
    if (p.is_int) {
        int64_t fv;
        if (rugo::_jsonl::fast_parse_int64(fptr, 0, flen - 1, fv))
            return apply_op<int64_t>(p.op, fv, p.i64);
    }
    const double cmp = p.is_int ? static_cast<double>(p.i64) : p.f64;
    double fv;
    if (rugo::_jsonl::fast_parse_float64(fptr, 0, flen - 1, fv))
        return apply_op<double>(p.op, fv, cmp);
    return false;
}

static bool eval_string(const PredEval& p, const uint8_t* fptr, uint32_t flen) {
    const int cmp = std::memcmp(fptr, p.str.data(),
                                std::min(static_cast<size_t>(flen), p.str.size()));
    const int cmp2 = (cmp != 0) ? cmp :
        (flen < p.str.size() ? -1 : flen > p.str.size() ? 1 : 0);
    return apply_op<int>(p.op, cmp2, 0);
}

// Shared predicate evaluation: returns true if the field passes p.
static bool eval_predicate(const PredEval& p, const uint8_t* fptr, uint32_t flen, bool is_null)
{
    // SQL 3VL: a comparison against NULL is UNKNOWN, which a filter drops --
    // for every operator, != included.
    if (is_null) return false;

    switch (p.mode) {
        case PRED_STRING:
            return eval_string(p, fptr, flen);
        case PRED_NUMERIC:
            return eval_numeric(p, fptr, flen);
        case PRED_BOOL: {
            bool fv;
            // The same strict parser the declared BOOL column is built with.
            if (!rugo::detail::strict_bool(fptr, flen, &fv)) return false;
            return apply_op<int>(p.op, fv ? 1 : 0, p.bool_val ? 1 : 0);
        }
        default: {  // PRED_OTHER
            if (p.is_int || p.is_float) {
                double fv;
                if (flen > 0 && rugo::_jsonl::fast_parse_float64(fptr, 0, flen - 1, fv))
                    return eval_numeric(p, fptr, flen);
            }
            return eval_string(p, fptr, flen);
        }
    }
}

// Build a draken_malloc'd validity bitmap. Returns nullptr if all rows valid.
static uint8_t* build_validity(const std::vector<uint8_t>& null_bm, uint32_t n) {
    if (n == 0 || null_bm.empty()) return nullptr;
    const size_t nb = (static_cast<size_t>(n) + 7) >> 3;
    bool has_nulls = false;
    for (size_t b = 0; b < nb && !has_nulls; ++b) {
        uint8_t mask = 0xFF;
        if (b == nb - 1 && (n & 7)) mask = static_cast<uint8_t>((1u << (n & 7)) - 1);
        if ((null_bm[b] & mask) != mask) has_nulls = true;
    }
    if (!has_nulls) return nullptr;
    const size_t alloc = std::max(static_cast<size_t>(8), (nb + 7u) & ~7u);
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(alloc));
    std::memset(v, 0xFF, alloc);
    std::memcpy(v, null_bm.data(), nb);
    if (n & 7) v[nb - 1] &= static_cast<uint8_t>((1u << (n & 7)) - 1);
    return v;
}

// bit_copy: copy n_bits from src[0..] (LSB first) into dst starting at dst_bit_offset.
static void bit_copy(uint8_t* dst, size_t dst_bit_offset,
                     const uint8_t* src, uint32_t n_bits) noexcept
{
    if (!src || n_bits == 0) return;
    const size_t   db0  = dst_bit_offset >> 3;
    const uint32_t dbit = static_cast<uint32_t>(dst_bit_offset & 7);
    if (dbit == 0) {
        const size_t bytes = (n_bits + 7) >> 3;
        std::memcpy(dst + db0, src, bytes);
        const uint32_t tail = n_bits & 7;
        if (tail) dst[db0 + bytes - 1] &= static_cast<uint8_t>((1u << tail) - 1);
    } else {
        for (uint32_t i = 0; i < n_bits; ++i) {
            const uint8_t bit = (src[i >> 3] >> (i & 7)) & 1u;
            const size_t  d   = dst_bit_offset + i;
            if (bit) dst[d >> 3] |=  static_cast<uint8_t>(1u << (d & 7));
            else     dst[d >> 3] &= static_cast<uint8_t>(~(1u << (d & 7)));
        }
    }
}

// ---------------------------------------------------------------------------
// sniff_csv_column_types — scalar FSM, up to ctx.sniff_sample_size non-null
// values per column. Returns one DrakenType per entry in proj_ordinals.
// ---------------------------------------------------------------------------

static std::vector<DrakenType> sniff_csv_column_types(
    const uint8_t*               body,
    size_t                       body_len,
    const std::vector<uint32_t>& proj_ordinals,
    const std::vector<uint8_t>&  declared,   // 1 == caller declared this column's type
    const CsvParseContext&       ctx)
{
    const size_t np = proj_ordinals.size();
    std::vector<DrakenType> types(np, DRAKEN_INT64);
    std::vector<uint32_t>   seen(np, 0);

    // A declared column is not sniffed at all: its type is stated, so sampling it
    // is work whose answer is thrown away. Marking it VARCHAR-with-a-full-sample
    // makes it "decided" for the early-exit test below, and the caller overwrites
    // the entry with the declared type.
    for (size_t i = 0; i < np; ++i) {
        if (declared[i]) {
            types[i] = DRAKEN_VARCHAR;
            seen[i]  = ctx.sniff_sample_size;
        }
    }

    if (np == 0 || body_len == 0) return types;

    std::vector<uint8_t> scratch;

    auto widen = [](DrakenType cur, const uint8_t* ptr, uint32_t len) -> DrakenType {
        if (cur == DRAKEN_VARCHAR) return DRAKEN_VARCHAR;
        if (cur == DRAKEN_INT64) {
            if (len == 0) return DRAKEN_VARCHAR;
            int64_t v;
            if (rugo::_jsonl::fast_parse_int64(ptr, 0, len - 1, v)) return DRAKEN_INT64;
            cur = DRAKEN_FLOAT64;
        }
        if (len == 0) return DRAKEN_VARCHAR;
        double v;
        return rugo::_jsonl::fast_parse_float64(ptr, 0, len - 1, v) ? DRAKEN_FLOAT64 : DRAKEN_VARCHAR;
    };

    enum class S { FIELD_START, UNQUOTED, QUOTED, DQ_PENDING };
    S        state            = S::FIELD_START;
    uint32_t field_start      = 0;
    bool     was_quoted       = false;
    bool     has_escape       = false;
    uint32_t quote_close      = 0;
    bool     cr_ended         = false;
    uint32_t current_col      = 0;
    size_t   req_idx          = 0;
    bool     done             = false;

    auto process_field = [&](uint32_t value_end) {
        while (req_idx < np && proj_ordinals[req_idx] < current_col) ++req_idx;

        if (req_idx < np && proj_ordinals[req_idx] == current_col
                && !declared[req_idx] && seen[req_idx] < ctx.sniff_sample_size) {
            uint32_t raw_len = (value_end > field_start) ? (value_end - field_start) : 0u;
            const bool is_null = (raw_len == 0 && !was_quoted);
            if (!is_null) {
                const uint8_t* ptr = body + field_start;
                uint32_t       len = raw_len;
                if (has_escape && raw_len > 0) {
                    if (scratch.size() < raw_len) scratch.resize(raw_len);
                    len = unescape_csv_field(ptr, raw_len, scratch.data());
                    ptr = scratch.data();
                }
                types[req_idx] = widen(types[req_idx], ptr, len);
                ++seen[req_idx];
            }
            ++req_idx;
        }

        ++current_col;
        was_quoted = false;
        has_escape = false;
        state = S::FIELD_START;
    };

    auto end_row = [&]() {
        // skip remaining requested columns (missing = null, doesn't affect type)
        while (req_idx < np && proj_ordinals[req_idx] <= current_col) ++req_idx;
        current_col = 0;
        req_idx     = 0;
        state       = S::FIELD_START;
        done = true;
        for (size_t i = 0; i < np; ++i)
            if (types[i] != DRAKEN_VARCHAR && seen[i] < ctx.sniff_sample_size) { done = false; break; }
    };

    for (size_t i = 0; i < body_len && !done; ++i) {
        const uint8_t c = body[i];

        if (cr_ended) {
            if (c == '\n') { end_row(); field_start = static_cast<uint32_t>(i + 1); }
            cr_ended = false;
            continue;
        }

        switch (state) {
            case S::FIELD_START:
                if (c == '"') {
                    was_quoted = true;
                    field_start = static_cast<uint32_t>(i + 1);
                    state = S::QUOTED;
                } else if (c == ctx.delimiter) {
                    process_field(static_cast<uint32_t>(i));
                    field_start = static_cast<uint32_t>(i + 1);
                } else if (c == '\n') {
                    process_field(static_cast<uint32_t>(i));
                    end_row();
                    field_start = static_cast<uint32_t>(i + 1);
                } else if (c == '\r') {
                    if (i + 1 < body_len && body[i + 1] == '\n') {
                        process_field(static_cast<uint32_t>(i));
                        cr_ended = true;
                    }
                } else {
                    state = S::UNQUOTED;
                }
                break;

            case S::UNQUOTED:
                if (c == ctx.delimiter) {
                    process_field(static_cast<uint32_t>(i));
                    field_start = static_cast<uint32_t>(i + 1);
                } else if (c == '\n') {
                    process_field(static_cast<uint32_t>(i));
                    end_row();
                    field_start = static_cast<uint32_t>(i + 1);
                } else if (c == '\r') {
                    if (i + 1 < body_len && body[i + 1] == '\n') {
                        process_field(static_cast<uint32_t>(i));
                        cr_ended = true;
                    }
                }
                break;

            case S::QUOTED:
                if (c == '"') { quote_close = static_cast<uint32_t>(i); state = S::DQ_PENDING; }
                break;

            case S::DQ_PENDING:
                if (c == '"') { has_escape = true; state = S::QUOTED; }
                else if (c == ctx.delimiter) {
                    process_field(quote_close);
                    field_start = static_cast<uint32_t>(i + 1);
                } else if (c == '\n') {
                    process_field(quote_close);
                    end_row();
                    field_start = static_cast<uint32_t>(i + 1);
                } else if (c == '\r') {
                    if (i + 1 < body_len && body[i + 1] == '\n') {
                        process_field(quote_close);
                        cr_ended = true;
                    } else {
                        state = S::UNQUOTED;
                    }
                } else {
                    state = S::UNQUOTED;
                }
                break;
        }
    }

    return types;
}

// ---------------------------------------------------------------------------
// Per-thread columnar output buffer.
// Uses growable std::vector internals; draken_malloc'd on finalize.
// ---------------------------------------------------------------------------

struct ColBuf {
    DrakenType type;
    uint32_t   n = 0;

    // Set when the column came from ctx.explicit_schema rather than the sniffer.
    // `raw` then holds one fixed-width element per row (BOOL: one 0/1 BYTE per
    // row, bit-packed later in finalize_col_buf — a per-thread bit stream cannot
    // be concatenated at byte granularity, and rows land here one at a time).
    bool                 declared_col = false;
    rugo::DeclaredType   declared;
    std::string          declared_name;   // as the caller spelled it, for errors
    size_t               elem = 0;        // bytes per element in `raw`
    std::vector<uint8_t> raw;

    std::vector<int64_t> i64;
    std::vector<double>  f64;

    std::vector<DrakenStringSlot> slots;
    std::vector<uint8_t>          arena;

    std::vector<uint8_t> null_bm;   // packed LSB-first; grown lazily in commit_row

    // Unescape target for this column's "" fields; grown to the field's raw
    // length on demand. Stable for the row: one field per column per row, and
    // commit_row consumes the pending view before the next row can regrow it.
    std::vector<uint8_t> esc_scratch;

    explicit ColBuf(DrakenType t) : type(t) {}

    ColBuf(DrakenType t, const rugo::DeclaredType& dt, const std::string& spelling)
        : type(t), declared_col(true), declared(dt), declared_name(spelling) {
        elem = (dt.type == DRAKEN_BOOL) ? 1u : rugo::declared_elem_size(dt.type);
    }
};

// Pending field view for one projected column in the current row.
struct FieldPend {
    const uint8_t* ptr;
    uint32_t       len;
    bool           is_null;
};

// ---------------------------------------------------------------------------
// stream_build_range — single streaming pass for one thread's byte range.
// ---------------------------------------------------------------------------

static void stream_build_range(
    const uint8_t*               body,
    size_t                       range_start,
    size_t                       range_end,
    const CsvParseContext&       ctx,
    const std::vector<uint32_t>& req_ords,      // sorted
    const std::vector<int>&      proj_idx_map,  // req_ords[i] → ColBuf index (-1 if not proj)
    const std::vector<std::vector<int>>& preds_for_req,  // req_ords[i] → EVERY predicate on that column
    const std::vector<PredEval>& preds,
    const std::vector<std::string>& proj_col_names,  // ColBuf index -> projected column name
    std::vector<ColBuf>&         bufs)
{
    const size_t n_req  = req_ords.size();
    const size_t n_proj = bufs.size();

    if (range_end <= range_start || n_proj == 0) return;

    const uint8_t* chunk     = body + range_start;
    const size_t   chunk_len = range_end - range_start;

    // Shared scratch for predicate-only escaped fields (consumed immediately)
    std::vector<uint8_t> pred_scratch;

    // Per-row pending views (one per projected column)
    std::vector<FieldPend> pending(n_proj, {nullptr, 0, true});
    bool row_pred_ok = true;

    // FSM state
    enum class S { FIELD_START, UNQUOTED, QUOTED, DQ_PENDING };
    S        state            = S::FIELD_START;
    uint32_t field_start      = 0;
    bool     was_quoted       = false;
    bool     has_escape       = false;
    uint32_t quote_close      = 0;
    bool     cr_ended         = false;
    uint32_t current_col      = 0;
    size_t   req_idx          = 0;

    // commit_row: write pending field views to output buffers.
    auto commit_row = [&]() {
        for (size_t pi = 0; pi < n_proj; ++pi) {
            ColBuf& buf = bufs[pi];
            const FieldPend& fp = pending[pi];

            const size_t bm_byte = buf.n >> 3;
            const size_t bm_bit  = buf.n & 7;
            if (buf.null_bm.size() <= bm_byte)
                buf.null_bm.resize(bm_byte + 1, 0xFF);
            if (fp.is_null)
                buf.null_bm[bm_byte] &= static_cast<uint8_t>(~(1u << bm_bit));

            // A DECLARED column is parsed strictly, ahead of every sniffed arm.
            // ctx.ignore_errors is deliberately NOT consulted: it softens a GUESS
            // made from a sample window, and a declared type is not a guess.
            if (buf.declared_col) {
                if (rugo::declared_is_string(buf.declared.type)) {
                    DrakenStringSlot slot;
                    if (fp.is_null || fp.len == 0) {
                        str_init_null(&slot);
                    } else if (fp.len > STR_INLINE_MAX) {
                        const uint32_t off = static_cast<uint32_t>(buf.arena.size());
                        buf.arena.insert(buf.arena.end(), fp.ptr, fp.ptr + fp.len);
                        draken_build_string_slot(&slot, fp.ptr, fp.len, off);
                    } else {
                        draken_build_string_slot(&slot, fp.ptr, fp.len, 0);
                    }
                    buf.slots.push_back(slot);
                } else {
                    const size_t at = buf.raw.size();
                    buf.raw.resize(at + buf.elem, 0u);
                    if (!fp.is_null) {
                        // index 0 into a one-element window: BOOL writes bit 0 of
                        // that byte, which finalize_col_buf packs down later.
                        if (fp.len == 0 ||
                            !rugo::declared_parse_into(buf.declared, fp.ptr, fp.len,
                                                       buf.raw.data() + at, 0)) {
                            throw std::runtime_error(
                                "CSV column '" + proj_col_names[pi] + "' is declared " +
                                buf.declared_name + " in explicit_schema, but the value '" +
                                std::string(reinterpret_cast<const char*>(fp.ptr), fp.len) +
                                "' is not a valid " + buf.declared_name +
                                ". A declared type is parsed strictly; ignore_errors does "
                                "not apply to it.");
                        }
                    }
                }
                ++buf.n;
                continue;
            }

            if (buf.type == DRAKEN_INT64) {
                int64_t v = 0;
                if (!fp.is_null && fp.len > 0 &&
                    !rugo::_jsonl::fast_parse_int64(fp.ptr, 0, fp.len - 1, v)) {
                    if (!ctx.ignore_errors) {
                        throw std::runtime_error(
                            "CSV column '" + proj_col_names[pi] + "' was inferred as INTEGER "
                            "from its first " + std::to_string(ctx.sniff_sample_size) +
                            " sampled value(s), but a later value '" +
                            std::string(reinterpret_cast<const char*>(fp.ptr), fp.len) +
                            "' does not parse as an integer. Pass ignore_errors=>true to treat "
                            "such values as NULL instead of failing.");
                    }
                    buf.null_bm[bm_byte] &= static_cast<uint8_t>(~(1u << bm_bit));
                    v = 0;
                }
                buf.i64.push_back(v);
            } else if (buf.type == DRAKEN_FLOAT64) {
                double v = 0.0;
                if (!fp.is_null && fp.len > 0 &&
                    !rugo::_jsonl::fast_parse_float64(fp.ptr, 0, fp.len - 1, v)) {
                    if (!ctx.ignore_errors) {
                        throw std::runtime_error(
                            "CSV column '" + proj_col_names[pi] + "' was inferred as FLOAT "
                            "from its first " + std::to_string(ctx.sniff_sample_size) +
                            " sampled value(s), but a later value '" +
                            std::string(reinterpret_cast<const char*>(fp.ptr), fp.len) +
                            "' does not parse as a number. Pass ignore_errors=>true to treat "
                            "such values as NULL instead of failing.");
                    }
                    buf.null_bm[bm_byte] &= static_cast<uint8_t>(~(1u << bm_bit));
                    v = 0.0;
                }
                buf.f64.push_back(v);
            } else {
                // VARCHAR
                DrakenStringSlot slot;
                if (fp.is_null || fp.len == 0) {
                    str_init_null(&slot);
                } else if (fp.len > STR_INLINE_MAX) {
                    const uint32_t off = static_cast<uint32_t>(buf.arena.size());
                    buf.arena.insert(buf.arena.end(), fp.ptr, fp.ptr + fp.len);
                    draken_build_string_slot(&slot, fp.ptr, fp.len, off);
                } else {
                    draken_build_string_slot(&slot, fp.ptr, fp.len, 0);
                }
                buf.slots.push_back(slot);
            }

            ++buf.n;
        }
    };

    // emit_field: called when a field boundary is found.
    auto emit_field = [&](uint32_t value_end) {
        while (req_idx < n_req && req_ords[req_idx] < current_col) ++req_idx;

        if (req_idx < n_req && req_ords[req_idx] == current_col) {
            const int pi = proj_idx_map[req_idx];

            // Get raw field bytes
            uint32_t raw_len = (value_end > field_start) ? (value_end - field_start) : 0u;
            const bool is_null = (raw_len == 0 && !was_quoted);

            const uint8_t* fptr = nullptr;
            uint32_t       flen = 0;

            if (!is_null && raw_len > 0) {
                fptr = chunk + field_start;
                flen = raw_len;
                if (has_escape) {
                    // Unescape into stable scratch (proj col scratch or shared pred scratch)
                    std::vector<uint8_t>& sc = (pi >= 0) ? bufs[pi].esc_scratch : pred_scratch;
                    if (sc.size() < raw_len) sc.resize(raw_len);
                    flen = unescape_csv_field(fptr, raw_len, sc.data());
                    fptr = sc.data();
                }
            }

            // Predicate evaluation (short-circuit once failed). A column may carry
            // several predicates (`a > 2 AND a < 5`); every one of them must pass.
            for (const int pd : preds_for_req[req_idx]) {
                if (!row_pred_ok) break;
                row_pred_ok = eval_predicate(preds[pd], fptr, flen, is_null);
            }

            // Store pending view for projected columns
            if (pi >= 0) pending[pi] = {fptr, flen, is_null};

            ++req_idx;
        }

        ++current_col;
        was_quoted = false;
        has_escape = false;
        state = S::FIELD_START;
    };

    // end_row: handle missing trailing fields then commit or discard.
    auto end_row = [&]() {
        // Fill missing trailing requested columns with null
        while (req_idx < n_req) {
            const int pi = proj_idx_map[req_idx];
            // Missing field is NULL: UNKNOWN under every comparison operator.
            if (!preds_for_req[req_idx].empty()) row_pred_ok = false;
            if (pi >= 0) pending[pi] = {nullptr, 0, true};
            ++req_idx;
        }

        if (row_pred_ok) commit_row();

        // Reset for next row
        current_col = 0;
        req_idx     = 0;
        row_pred_ok = true;
        for (auto& p : pending) p = {nullptr, 0, true};
        state = S::FIELD_START;
    };

    // Drive the SIMD structural scan
    scan_structural_csv(chunk, chunk_len, ctx, [&](uint32_t pos, CsvMarkerType type) {
        if (cr_ended) {
            if (type == CsvMarkerType::NEWLINE) {
                end_row();
                field_start = pos + 1;
            }
            cr_ended = false;
            return;
        }

        switch (state) {
            case S::FIELD_START:
                switch (type) {
                    case CsvMarkerType::QUOTE:
                        // Non-structural bytes emit no marker, so FIELD_START
                        // also covers "inside an unquoted field, no marker yet".
                        // Only a quote at the field's first byte opens a quoted
                        // field; anywhere else it is an ordinary byte.
                        if (pos == field_start) {
                            was_quoted  = true;
                            field_start = pos + 1;
                            state = S::QUOTED;
                        } else {
                            state = S::UNQUOTED;
                        }
                        break;
                    case CsvMarkerType::DELIMITER:
                        emit_field(pos);
                        field_start = pos + 1;
                        break;
                    case CsvMarkerType::NEWLINE:
                        emit_field(pos);
                        end_row();
                        field_start = pos + 1;
                        break;
                    case CsvMarkerType::CR:
                        if (pos + 1 < static_cast<uint32_t>(chunk_len) && chunk[pos + 1] == '\n') {
                            emit_field(pos);
                            cr_ended = true;
                        }
                        break;
                    default: break;
                }
                break;

            case S::UNQUOTED:
                switch (type) {
                    case CsvMarkerType::DELIMITER:
                        emit_field(pos);
                        field_start = pos + 1;
                        break;
                    case CsvMarkerType::NEWLINE:
                        emit_field(pos);
                        end_row();
                        field_start = pos + 1;
                        break;
                    case CsvMarkerType::CR:
                        if (pos + 1 < static_cast<uint32_t>(chunk_len) && chunk[pos + 1] == '\n') {
                            emit_field(pos);
                            cr_ended = true;
                        }
                        break;
                    default: break;
                }
                break;

            case S::QUOTED:
                switch (type) {
                    case CsvMarkerType::QUOTE:
                        quote_close = pos;
                        state = S::DQ_PENDING;
                        break;
                    default: break;
                }
                break;

            case S::DQ_PENDING:
                switch (type) {
                    case CsvMarkerType::QUOTE:
                        has_escape = true;
                        state = S::QUOTED;
                        break;
                    case CsvMarkerType::DELIMITER:
                        emit_field(quote_close);
                        field_start = pos + 1;
                        break;
                    case CsvMarkerType::NEWLINE:
                        emit_field(quote_close);
                        end_row();
                        field_start = pos + 1;
                        break;
                    case CsvMarkerType::CR:
                        if (pos + 1 < static_cast<uint32_t>(chunk_len) && chunk[pos + 1] == '\n') {
                            emit_field(quote_close);
                            cr_ended = true;
                        } else {
                            state = S::UNQUOTED;
                        }
                        break;
                    default:
                        state = S::UNQUOTED;
                        break;
                }
                break;
        }
    });

    // Handle final partial row (file doesn't end with \n)
    const bool in_partial_row = (state != S::FIELD_START) || (current_col > 0);
    if (in_partial_row) {
        switch (state) {
            case S::DQ_PENDING:
                emit_field(quote_close);
                break;
            default:
                emit_field(static_cast<uint32_t>(chunk_len));
                break;
        }
        end_row();
    }
}

// ---------------------------------------------------------------------------
// finalize_col_buf — merge per-thread ColBufs into one ParsedCsvColumn.
// ---------------------------------------------------------------------------

static ParsedCsvColumn finalize_col_buf(
    std::vector<ColBuf>& thread_bufs,
    DrakenType           type)
{
    ParsedCsvColumn pc;
    pc.type = type;

    // A declared column carries its logical descriptor onto the output column;
    // every ColBuf for one column was constructed from the same declaration, so
    // the first is representative.
    const bool declared_col = !thread_bufs.empty() && thread_bufs[0].declared_col;
    if (declared_col) {
        pc.logical_kind   = thread_bufs[0].declared.logical_kind;
        pc.unit           = thread_bufs[0].declared.unit;
        pc.offset_minutes = thread_bufs[0].declared.offset_minutes;
        pc.precision      = thread_bufs[0].declared.precision;
        pc.scale          = thread_bufs[0].declared.scale;
    }
    const bool string_col = declared_col
        ? rugo::declared_is_string(thread_bufs[0].declared.type)
        : (type == DRAKEN_VARCHAR);

    uint32_t total = 0;
    for (const auto& b : thread_bufs) total += b.n;
    pc.length = total;

    if (total == 0) {
        if (string_col) {
            pc.is_string = true;
            pc.slots = static_cast<DrakenStringSlot*>(draken_malloc(0));
        }
        return pc;
    }

    // Build merged validity bitmap
    {
        bool any_nulls = false;
        for (const auto& b : thread_bufs) {
            if (b.null_bm.empty()) continue;
            const size_t nb = (static_cast<size_t>(b.n) + 7) >> 3;
            for (size_t i = 0; i < nb && i < b.null_bm.size() && !any_nulls; ++i) {
                uint8_t mask = 0xFF;
                if (i == nb - 1 && (b.n & 7)) mask = static_cast<uint8_t>((1u << (b.n & 7)) - 1);
                if ((b.null_bm[i] & mask) != mask) any_nulls = true;
            }
        }
        if (any_nulls) {
            const size_t nb    = (static_cast<size_t>(total) + 7) >> 3;
            const size_t alloc = std::max(static_cast<size_t>(8), (nb + 7u) & ~7u);
            uint8_t* v = static_cast<uint8_t*>(draken_malloc(alloc));
            std::memset(v, 0xFF, alloc);
            size_t bit_off = 0;
            for (const auto& b : thread_bufs) {
                if (b.n > 0 && !b.null_bm.empty())
                    bit_copy(v, bit_off, b.null_bm.data(), b.n);
                bit_off += b.n;
            }
            if (total & 7) v[nb - 1] &= static_cast<uint8_t>((1u << (total & 7)) - 1);
            pc.validity = v;
        }
    }

    if (declared_col && !string_col) {
        if (type == DRAKEN_BOOL) {
            // Per-thread streams hold one 0/1 BYTE per row; pack them into the
            // 1-bit-per-row bitmap draken expects. Concatenating bit streams at
            // byte granularity would silently misalign every thread but the first.
            const size_t nb    = (static_cast<size_t>(total) + 7) >> 3;
            const size_t alloc = std::max(static_cast<size_t>(8), (nb + 7u) & ~7u);
            uint8_t* data = static_cast<uint8_t*>(draken_malloc(alloc));
            std::memset(data, 0, alloc);
            uint32_t row = 0;
            for (const auto& b : thread_bufs)
                for (uint32_t i = 0; i < b.n; ++i, ++row)
                    if (b.raw[i]) data[row >> 3] |= static_cast<uint8_t>(1u << (row & 7));
            pc.data = data;
            return pc;
        }
        const size_t es = thread_bufs[0].elem;
        uint8_t* data = static_cast<uint8_t*>(
            draken_malloc(std::max<size_t>(static_cast<size_t>(total) * es, 1)));
        size_t off = 0;
        for (auto& b : thread_bufs) {
            if (b.n == 0) continue;
            std::memcpy(data + off, b.raw.data(), static_cast<size_t>(b.n) * es);
            off += static_cast<size_t>(b.n) * es;
        }
        pc.data = data;
        return pc;
    }

    if (type == DRAKEN_INT64) {
        int64_t* data = static_cast<int64_t*>(draken_malloc(static_cast<size_t>(total) * 8));
        size_t off = 0;
        for (auto& b : thread_bufs) {
            std::memcpy(data + off, b.i64.data(), b.n * 8);
            off += b.n;
        }
        pc.data = data;
        return pc;
    }

    if (type == DRAKEN_FLOAT64) {
        double* data = static_cast<double*>(draken_malloc(static_cast<size_t>(total) * 8));
        size_t off = 0;
        for (auto& b : thread_bufs) {
            std::memcpy(data + off, b.f64.data(), b.n * 8);
            off += b.n;
        }
        pc.data = data;
        return pc;
    }

    // VARCHAR — concatenate slots + arenas; rebase external arena offsets
    pc.is_string = true;
    size_t total_arena = 0;
    for (const auto& b : thread_bufs) total_arena += b.arena.size();

    pc.slots = static_cast<DrakenStringSlot*>(
        draken_malloc(static_cast<size_t>(total) * sizeof(DrakenStringSlot)));
    pc.arena     = total_arena ? static_cast<uint8_t*>(draken_malloc(total_arena)) : nullptr;
    pc.arena_len = total_arena;

    uint32_t slot_off  = 0;
    size_t   arena_base = 0;
    for (auto& b : thread_bufs) {
        if (b.arena.size() && pc.arena)
            std::memcpy(static_cast<uint8_t*>(pc.arena) + arena_base,
                        b.arena.data(), b.arena.size());

        std::memcpy(pc.slots + slot_off, b.slots.data(),
                    static_cast<size_t>(b.n) * sizeof(DrakenStringSlot));

        if (arena_base > 0) {
            DrakenStringSlot* dst = pc.slots + slot_off;
            for (uint32_t i = 0; i < b.n; ++i)
                if (!str_is_inline(dst + i))
                    dst[i].ext.arena_offset += static_cast<uint32_t>(arena_base);
        }

        slot_off   += b.n;
        arena_base += b.arena.size();
    }
    return pc;
}

}  // namespace

// ---------------------------------------------------------------------------
// build_columns_streaming — public entry point
// ---------------------------------------------------------------------------

StreamResult build_columns_streaming(
    const uint8_t*               buffer,
    size_t                       length,
    size_t                       header_offset,
    const std::vector<std::string>& column_names,
    uint32_t                     num_cols,
    const std::vector<uint32_t>& request_ordinals,
    const std::vector<size_t>&   proj_indices,
    const CsvParseContext&       ctx,
    size_t                       max_threads)
{
    StreamResult result;
    result.num_rows = 0;

    // A header-only buffer (empty body) still runs through: every requested column
    // comes back typed with zero rows, and every predicate is still type-checked.
    if (length < header_offset || request_ordinals.empty()) return result;

    const uint8_t* body     = buffer + header_offset;
    const size_t   body_len = length - header_offset;
    const size_t   n_req    = request_ordinals.size();
    const size_t   n_proj   = proj_indices.size();

    // Build per-req-ordinal metadata maps
    std::vector<int> proj_idx_map(n_req, -1);
    for (size_t i = 0; i < proj_indices.size(); ++i)
        proj_idx_map[proj_indices[i]] = static_cast<int>(i);

    // Map each predicate to its req_ord index. Every predicate column is in
    // request_ordinals (the Cython edge refuses a predicate on an unknown column),
    // so a predicate that finds none here is an internal inconsistency.
    const size_t n_pred = ctx.predicates.size();
    std::vector<size_t>           pred_req(n_pred);
    std::vector<std::vector<int>> preds_for_req(n_req);
    for (size_t pi = 0; pi < n_pred; ++pi) {
        size_t found = n_req;
        for (size_t ri = 0; ri < n_req && found == n_req; ++ri)
            if (column_names[request_ordinals[ri]] == ctx.predicates[pi].column) found = ri;
        if (found == n_req)
            throw std::invalid_argument(
                "predicate on column '" + ctx.predicates[pi].column +
                "': no such column in this CSV");
        pred_req[pi] = found;
        preds_for_req[found].push_back(static_cast<int>(pi));
    }

    // Resolve ctx.explicit_schema against EVERY requested column (projected and
    // predicate-only) BEFORE sniffing: a declared column is not sniffed and not
    // widened, it is parsed as stated.
    std::vector<uint8_t>            req_declared(n_req, 0);
    std::vector<rugo::DeclaredType> req_declared_types(n_req);
    std::vector<std::string>        req_declared_names(n_req);
    for (size_t r = 0; r < n_req; ++r) {
        const std::string& name = column_names[request_ordinals[r]];
        const auto it = ctx.explicit_schema.find(name);
        if (it == ctx.explicit_schema.end()) continue;
        if (!rugo::parse_declared_type(it->second, &req_declared_types[r])) {
            // The Cython edge validates every declared name eagerly through this
            // same parser; this is a backstop for a non-Python caller.
            throw std::runtime_error(
                "explicit_schema: unsupported type '" + it->second + "' for column '" +
                name + "'; supported types are " +
                std::string(rugo::declared_type_vocabulary()));
        }
        if (rugo::declared_is_structured(req_declared_types[r].type)) {
            // Same backstop: ARRAY<T>/VARIANT are read out of JSON structure a CSV
            // field does not have (declared_type.hpp). Refused, never approximated.
            throw std::runtime_error(
                "explicit_schema: type '" + it->second + "' for column '" +
                name + "' is JSONL-only; a CSV field has no "
                "JSON structure to read an ARRAY or VARIANT from");
        }
        req_declared[r]       = 1;
        req_declared_names[r] = it->second;
    }

    // Sniff every requested column — a predicate-only column included, because its
    // predicate's literal is checked against the type below.
    std::vector<DrakenType> req_types =
        sniff_csv_column_types(body, body_len, request_ordinals, req_declared, ctx);
    for (size_t r = 0; r < n_req; ++r)
        if (req_declared[r]) req_types[r] = req_declared_types[r].type;

    // Projected-column views of the above, in ColBuf order.
    std::vector<uint8_t>            is_declared(n_proj, 0);
    std::vector<rugo::DeclaredType> declared_types(n_proj);
    std::vector<std::string>        declared_names(n_proj);
    std::vector<DrakenType>         col_types(n_proj);
    for (size_t i = 0; i < n_proj; ++i) {
        const size_t r    = proj_indices[i];
        is_declared[i]    = req_declared[r];
        declared_types[i] = req_declared_types[r];
        declared_names[i] = req_declared_names[r];
        col_types[i]      = req_types[r];
    }

    // Check each predicate's literal against its column's type (fail loud, before a
    // single row is filtered — see predicate_literal.hpp), pick its compare mode, and
    // pre-parse the literal ONCE.
    std::vector<PredEval> preds(n_pred);
    for (size_t i = 0; i < n_pred; ++i) {
        const CsvPredicate& cp = ctx.predicates[i];
        const size_t r = pred_req[i];
        const DrakenType t = req_types[r];
        const uint8_t lk = req_declared[r] ? req_declared_types[r].logical_kind : rugo::LK_NONE;
        if (!rugo::literal_fits_type(t, lk, cp.kind)) {
            const std::string type_name = req_declared[r] ? req_declared_names[r]
                : t == DRAKEN_INT64 ? "INT64" : t == DRAKEN_FLOAT64 ? "FLOAT64" : "VARCHAR";
            throw std::invalid_argument(
                rugo::literal_mismatch_message(cp.column, type_name, cp.kind, cp.value));
        }
        PredEval& p = preds[i];
        p.op       = cp.op;
        p.str      = cp.value;
        p.is_int   = false;
        p.is_float = false;
        p.bool_val = false;
        p.i64      = 0;
        p.f64      = 0.0;
        if (!rugo::literal_contract_covers(t, lk))  p.mode = PRED_OTHER;
        else if (rugo::declared_is_string(t))       p.mode = PRED_STRING;
        else if (t == DRAKEN_BOOL)                  p.mode = PRED_BOOL;
        else                                        p.mode = PRED_NUMERIC;

        if (p.mode == PRED_BOOL) {
            p.bool_val = (cp.value == "true");   // the Cython edge spells a bool true/false
        } else if (p.mode == PRED_NUMERIC || p.mode == PRED_OTHER) {
            const uint8_t* pv = reinterpret_cast<const uint8_t*>(p.str.data());
            const uint32_t pe = p.str.empty() ? 0 : static_cast<uint32_t>(p.str.size() - 1);
            if (!p.str.empty()) {
                p.is_int   = rugo::_jsonl::fast_parse_int64(pv, 0, pe, p.i64);
                p.is_float = !p.is_int && rugo::_jsonl::fast_parse_float64(pv, 0, pe, p.f64);
            }
            if (p.mode == PRED_NUMERIC && !p.is_int && !p.is_float)
                // An int/float literal always renders to a parseable number; this is a
                // backstop for a non-Python caller that set `kind` inconsistently.
                throw std::invalid_argument(
                    "predicate on column '" + cp.column + "': literal '" + cp.value +
                    "' is marked numeric but does not parse as a number");
        }
    }

    // Find safe row-boundary splits for threading
    size_t hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 1;
    size_t nt = max_threads ? std::min(max_threads, hw) : hw;

    const std::vector<uint32_t> splits = find_safe_splits_parallel(body, body_len, ctx, nt);

    struct Range { size_t start; size_t end; };
    std::vector<Range> ranges;

    if (splits.size() < 2 || nt <= 1) {
        ranges.push_back({0, body_len});
        nt = 1;
    } else {
        nt = std::min(nt, splits.size());
        size_t prev_end = 0;
        for (size_t t = 0; t < nt; ++t) {
            const size_t split_idx = ((t + 1) * splits.size() / nt) - 1;
            const size_t split_pos = splits[split_idx];
            ranges.push_back({prev_end, split_pos + 1});
            prev_end = split_pos + 1;
        }
        if (prev_end < body_len)
            ranges.back().end = body_len;
    }

    // Allocate per-thread ColBufs: [thread][proj_col]
    std::vector<std::vector<ColBuf>> thread_bufs(nt);
    for (size_t t = 0; t < nt; ++t) {
        thread_bufs[t].reserve(n_proj);
        for (size_t c = 0; c < n_proj; ++c) {
            if (is_declared[c])
                thread_bufs[t].emplace_back(col_types[c], declared_types[c], declared_names[c]);
            else
                thread_bufs[t].emplace_back(col_types[c]);
        }
    }

    // ColBuf index -> projected column name, for type-mismatch error messages.
    std::vector<std::string> proj_col_names(n_proj);
    for (size_t i = 0; i < n_proj; ++i)
        proj_col_names[i] = column_names[request_ordinals[proj_indices[i]]];

    auto run_thread = [&](size_t t) {
        stream_build_range(
            body, ranges[t].start, ranges[t].end,
            ctx, request_ordinals, proj_idx_map, preds_for_req, preds,
            proj_col_names, thread_bufs[t]);
    };

    if (nt <= 1) {
        run_thread(0);
    } else {
        BS::thread_pool<> pool(nt);
        std::vector<std::future<void>> futs;
        futs.reserve(nt);
        for (size_t t = 0; t < nt; ++t)
            futs.push_back(pool.submit_task([&, t]() { run_thread(t); }));
        // Drain every future before propagating -- a thread_build_range task
        // can throw on a type mismatch (see commit_row); if we rethrow after
        // only the first future without waiting on the rest, still-running
        // pool threads keep referencing this frame's locals (ctx, thread_bufs,
        // ...) by capture after it unwinds -- a use-after-free, not just a
        // missed result.
        std::exception_ptr first_exc;
        for (auto& f : futs) {
            try {
                f.get();
            } catch (...) {
                if (!first_exc) first_exc = std::current_exception();
            }
        }
        if (first_exc) std::rethrow_exception(first_exc);
    }

    // Count survivors and finalize
    if (n_proj > 0) {
        for (size_t t = 0; t < nt; ++t)
            result.num_rows += thread_bufs[t][0].n;
    }

    result.columns.resize(n_proj);
    for (size_t c = 0; c < n_proj; ++c) {
        std::vector<ColBuf> col_bufs;
        col_bufs.reserve(nt);
        for (size_t t = 0; t < nt; ++t)
            col_bufs.push_back(std::move(thread_bufs[t][c]));
        result.columns[c] = finalize_col_buf(col_bufs, col_types[c]);
    }

    return result;
}

// ---------------------------------------------------------------------------
// wrap_csv_column — GIL required
// ---------------------------------------------------------------------------

PyObject* wrap_csv_column(ParsedCsvColumn& pc) {
    if (pc.is_string)
        return draken_vector_own_string(
            pc.slots, pc.arena, pc.arena_len,
            pc.validity, pc.length, pc.type,
            /*keyhash=*/nullptr);   // E37: csv producer = task #5
    // A declared IPV4/TIMESTAMP/DECIMAL column carries a logical-type descriptor,
    // which lives on the Vector's owner rather than in the frozen DrakenVector, so
    // it must be attached at construction. own_raw_logical is own_raw when the
    // kind is NONE — every sniffed column.
    if (pc.logical_kind != 0)
        return draken_vector_own_raw_logical(pc.data, pc.validity, pc.length, pc.type,
                                             pc.logical_kind, pc.unit, pc.offset_minutes,
                                             pc.precision, pc.scale, /*dimension=*/0u);
    return draken_vector_own_raw(pc.data, pc.validity, pc.length, pc.type);
}

}  // namespace rugo::_csv
