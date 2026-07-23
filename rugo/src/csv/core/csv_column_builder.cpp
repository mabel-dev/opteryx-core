#include <Python.h>

#include "csv_column_builder.hpp"
#include "csv_scan.hpp"

#include <algorithm>
#include <cstring>
#include <thread>
#include <future>

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
    uint16_t       len,
    uint8_t*       out) noexcept
{
    uint32_t out_len = 0;
    uint32_t i = 0;
    while (i < static_cast<uint32_t>(len)) {
        const uint8_t c = src[i];
        if (c == '\\' && i + 1 < static_cast<uint32_t>(len)) {
            out[out_len++] = src[i + 1];
            i += 2;
        } else if (c == '"' && i + 1 < static_cast<uint32_t>(len) && src[i + 1] == '"') {
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

// Shared predicate evaluation: returns true if the field passes predicate[pred_i].
// pred_i64/pred_f64/pred_is_int/pred_is_float are pre-parsed values.
static bool eval_predicate(
    uint8_t             op,
    bool                pred_is_int,
    bool                pred_is_float,
    int64_t             pred_i64,
    double              pred_f64,
    const std::string&  pred_str,
    const uint8_t*      fptr,
    uint32_t            flen,
    bool                is_null)
{
    if (is_null) return (op == 1 /* NE */);

    if (pred_is_int) {
        int64_t fv;
        if (flen > 0 && rugo::_jsonl::fast_parse_int64(fptr, 0, flen - 1, fv)) {
            switch (op) {
                case 0: return fv == pred_i64;
                case 1: return fv != pred_i64;
                case 2: return fv <  pred_i64;
                case 3: return fv <= pred_i64;
                case 4: return fv >  pred_i64;
                case 5: return fv >= pred_i64;
            }
        }
    }
    if (pred_is_int || pred_is_float) {
        const double cmp = pred_is_int ? static_cast<double>(pred_i64) : pred_f64;
        double fv;
        if (flen > 0 && rugo::_jsonl::fast_parse_float64(fptr, 0, flen - 1, fv)) {
            switch (op) {
                case 0: return fv == cmp;
                case 1: return fv != cmp;
                case 2: return fv <  cmp;
                case 3: return fv <= cmp;
                case 4: return fv >  cmp;
                case 5: return fv >= cmp;
            }
        }
    }
    // String comparison
    const int cmp = std::memcmp(fptr, pred_str.data(),
                                std::min(static_cast<size_t>(flen), pred_str.size()));
    const int cmp2 = (cmp != 0) ? cmp :
        (flen < pred_str.size() ? -1 : flen > pred_str.size() ? 1 : 0);
    switch (op) {
        case 0: return cmp2 == 0;
        case 1: return cmp2 != 0;
        case 2: return cmp2 <  0;
        case 3: return cmp2 <= 0;
        case 4: return cmp2 >  0;
        case 5: return cmp2 >= 0;
    }
    return false;
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
// sniff_csv_column_types — scalar FSM, up to SNIFF_LIMIT non-null per column.
// Returns one DrakenType per entry in proj_ordinals.
// ---------------------------------------------------------------------------

static constexpr uint32_t SNIFF_LIMIT = 128;

static std::vector<DrakenType> sniff_csv_column_types(
    const uint8_t*               body,
    size_t                       body_len,
    const std::vector<uint32_t>& proj_ordinals,
    const CsvParseContext&       ctx)
{
    const size_t np = proj_ordinals.size();
    std::vector<DrakenType> types(np, DRAKEN_INT64);
    std::vector<uint32_t>   seen(np, 0);

    if (np == 0 || body_len == 0) return types;

    uint8_t scratch[UINT16_MAX];

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

    enum class S { FIELD_START, UNQUOTED, QUOTED, ESCAPE_IN_QUOTED, DQ_PENDING };
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

        if (req_idx < np && proj_ordinals[req_idx] == current_col && seen[req_idx] < SNIFF_LIMIT) {
            uint32_t raw_len = (value_end > field_start) ? (value_end - field_start) : 0u;
            if (raw_len > UINT16_MAX) raw_len = UINT16_MAX;
            const bool is_null = (raw_len == 0 && !was_quoted);
            if (!is_null) {
                const uint8_t* ptr = body + field_start;
                uint32_t       len = raw_len;
                if (has_escape && raw_len > 0) {
                    len = unescape_csv_field(ptr, static_cast<uint16_t>(raw_len), scratch);
                    ptr = scratch;
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
            if (types[i] != DRAKEN_VARCHAR && seen[i] < SNIFF_LIMIT) { done = false; break; }
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
                if (c == '\\') { has_escape = true; state = S::ESCAPE_IN_QUOTED; }
                else if (c == '"') { quote_close = static_cast<uint32_t>(i); state = S::DQ_PENDING; }
                break;

            case S::ESCAPE_IN_QUOTED:
                state = S::QUOTED;
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

    std::vector<int64_t> i64;
    std::vector<double>  f64;

    std::vector<DrakenStringSlot> slots;
    std::vector<uint8_t>          arena;

    std::vector<uint8_t> null_bm;   // packed LSB-first; grown lazily in commit_row

    std::vector<uint8_t> esc_scratch;  // UINT16_MAX; stable per-row for this column

    explicit ColBuf(DrakenType t) : type(t) {
        esc_scratch.resize(UINT16_MAX);
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
    const std::vector<int>&      pred_idx_map,  // req_ords[i] → ctx.predicates index (-1 if none)
    const std::vector<int64_t>&  pred_i64,
    const std::vector<double>&   pred_f64,
    const std::vector<bool>&     pred_is_int,
    const std::vector<bool>&     pred_is_float,
    const std::vector<std::string>& pred_values,
    std::vector<ColBuf>&         bufs)
{
    const size_t n_req  = req_ords.size();
    const size_t n_proj = bufs.size();

    if (range_end <= range_start || n_proj == 0) return;

    const uint8_t* chunk     = body + range_start;
    const size_t   chunk_len = range_end - range_start;

    // Shared scratch for predicate-only escaped fields
    std::vector<uint8_t> pred_scratch(UINT16_MAX);

    // Per-row pending views (one per projected column)
    std::vector<FieldPend> pending(n_proj, {nullptr, 0, true});
    bool row_pred_ok = true;

    // FSM state
    enum class S { FIELD_START, UNQUOTED, QUOTED, ESCAPE_IN_QUOTED, DQ_PENDING };
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

            if (buf.type == DRAKEN_INT64) {
                int64_t v = 0;
                if (!fp.is_null && fp.len > 0)
                    rugo::_jsonl::fast_parse_int64(fp.ptr, 0, fp.len - 1, v);
                buf.i64.push_back(v);
            } else if (buf.type == DRAKEN_FLOAT64) {
                double v = 0.0;
                if (!fp.is_null && fp.len > 0)
                    rugo::_jsonl::fast_parse_float64(fp.ptr, 0, fp.len - 1, v);
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
            const int pd = pred_idx_map[req_idx];

            // Get raw field bytes
            uint32_t raw_len = (value_end > field_start) ? (value_end - field_start) : 0u;
            if (raw_len > UINT16_MAX) raw_len = UINT16_MAX;
            const bool is_null = (raw_len == 0 && !was_quoted);

            const uint8_t* fptr = nullptr;
            uint32_t       flen = 0;

            if (!is_null && raw_len > 0) {
                fptr = chunk + field_start;
                flen = raw_len;
                if (has_escape) {
                    // Unescape into stable scratch (proj col scratch or shared pred scratch)
                    uint8_t* sc = (pi >= 0) ? bufs[pi].esc_scratch.data() : pred_scratch.data();
                    flen = unescape_csv_field(fptr, static_cast<uint16_t>(raw_len), sc);
                    fptr = sc;
                }
            }

            // Predicate evaluation (short-circuit once failed)
            if (pd >= 0 && row_pred_ok) {
                row_pred_ok = eval_predicate(
                    ctx.predicates[pd].op,
                    pred_is_int[pd], pred_is_float[pd],
                    pred_i64[pd], pred_f64[pd], pred_values[pd],
                    fptr, flen, is_null);
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
            const int pd = pred_idx_map[req_idx];
            const int pi = proj_idx_map[req_idx];
            if (pd >= 0 && row_pred_ok)
                row_pred_ok = (ctx.predicates[pd].op == 1 /* NE */);
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
                        was_quoted  = true;
                        field_start = pos + 1;
                        state = S::QUOTED;
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
                    case CsvMarkerType::BACKSLASH:
                        state = S::UNQUOTED;
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
                    case CsvMarkerType::BACKSLASH:
                        has_escape = true;
                        state = S::ESCAPE_IN_QUOTED;
                        break;
                    case CsvMarkerType::QUOTE:
                        quote_close = pos;
                        state = S::DQ_PENDING;
                        break;
                    default: break;
                }
                break;

            case S::ESCAPE_IN_QUOTED:
                state = S::QUOTED;
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

    uint32_t total = 0;
    for (const auto& b : thread_bufs) total += b.n;
    pc.length = total;

    if (total == 0) {
        if (type == DRAKEN_VARCHAR) {
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

    if (length <= header_offset || request_ordinals.empty()) return result;

    const uint8_t* body     = buffer + header_offset;
    const size_t   body_len = length - header_offset;
    const size_t   n_req    = request_ordinals.size();
    const size_t   n_proj   = proj_indices.size();

    // Build per-req-ordinal metadata maps
    std::vector<int> proj_idx_map(n_req, -1);
    std::vector<int> pred_idx_map(n_req, -1);

    for (size_t i = 0; i < proj_indices.size(); ++i)
        proj_idx_map[proj_indices[i]] = static_cast<int>(i);

    // Map predicate column names → req_ord indices
    for (size_t pi = 0; pi < ctx.predicates.size(); ++pi) {
        for (size_t ci = 0; ci < column_names.size(); ++ci) {
            if (column_names[ci] == ctx.predicates[pi].column) {
                for (size_t ri = 0; ri < n_req; ++ri) {
                    if (request_ordinals[ri] == static_cast<uint32_t>(ci)) {
                        pred_idx_map[ri] = static_cast<int>(pi);
                        break;
                    }
                }
                break;
            }
        }
    }

    // Sniff column types from first SNIFF_LIMIT non-null values per projected column
    std::vector<uint32_t> proj_ordinals(n_proj);
    for (size_t i = 0; i < n_proj; ++i)
        proj_ordinals[i] = request_ordinals[proj_indices[i]];

    const std::vector<DrakenType> col_types =
        sniff_csv_column_types(body, body_len, proj_ordinals, ctx);

    // Pre-parse predicate comparison values
    const size_t             n_pred = ctx.predicates.size();
    std::vector<int64_t>     pred_i64(n_pred);
    std::vector<double>      pred_f64(n_pred);
    std::vector<bool>        pred_is_int(n_pred, false);
    std::vector<bool>        pred_is_float(n_pred, false);
    std::vector<std::string> pred_values(n_pred);
    for (size_t i = 0; i < n_pred; ++i) {
        pred_values[i] = ctx.predicates[i].value;
        const uint8_t* pv = reinterpret_cast<const uint8_t*>(pred_values[i].data());
        const uint32_t pe = pred_values[i].size()
            ? static_cast<uint32_t>(pred_values[i].size() - 1) : 0;
        pred_is_int[i]   = rugo::_jsonl::fast_parse_int64(pv, 0, pe, pred_i64[i]);
        pred_is_float[i] = !pred_is_int[i] && pred_values[i].size() > 0 &&
                           rugo::_jsonl::fast_parse_float64(pv, 0, pe, pred_f64[i]);
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
        for (size_t c = 0; c < n_proj; ++c)
            thread_bufs[t].emplace_back(col_types[c]);
    }

    auto run_thread = [&](size_t t) {
        stream_build_range(
            body, ranges[t].start, ranges[t].end,
            ctx, request_ordinals, proj_idx_map, pred_idx_map,
            pred_i64, pred_f64, pred_is_int, pred_is_float, pred_values,
            thread_bufs[t]);
    };

    if (nt <= 1) {
        run_thread(0);
    } else {
        BS::thread_pool<> pool(nt);
        std::vector<std::future<void>> futs;
        futs.reserve(nt);
        for (size_t t = 0; t < nt; ++t)
            futs.push_back(pool.submit_task([&, t]() { run_thread(t); }));
        for (auto& f : futs) f.get();
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
    return draken_vector_own_raw(pc.data, pc.validity, pc.length, pc.type);
}

}  // namespace rugo::_csv
