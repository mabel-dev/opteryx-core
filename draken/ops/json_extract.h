#pragma once
// draken/ops/json_extract.h — JSON field extraction core.
//
// Provides:
//   draken::ops::dotpath_to_jsonptr(path, len) → std::string  (via json_path.h)
//   draken::ops::tokenize_jsonptr(ptr, len)    → JsonPtrPath  (via json_path.h)
//   draken::ops::JsonNav                                       (bind-time-resolved path)
//   draken::ops::JDocGuard                                     (RAII for yyjson_doc*)
//   draken::ops::ReadPool                                      (per-column parse arena)
//   draken::ops::extract_rows(...)                             (the shared row loop)
//
// extract_rows is the SINGLE implementation of `->`, `->>` and top-level map
// access. Two consumers finalize its component buffers differently:
//   * the C-ABI kernel (draken/ops/kernels/extraction.cpp) → vecresult_from_string_buffers
//     — the execution path: `->` and `->>` (mode 0) reach it from the VM.
//   * the nanobind binding (opteryx/compiled/nanobind/vector_json.cpp) → draken_vector_own_string
//     — NOT on the execution path since the VM went C-ABI direct. It is now
//     reached only by draken/tests/native/test_vector_json.py and the jsonbench
//     harness, and it is the sole remaining caller of mode 1 (top-level key via
//     yyjson_obj_getn); no SQL syntax reaches that mode.
// Neither owns a copy of the loop.
//
// The TU that includes this header must have "third_party/yyjson/src" on its
// include path and must link yyjson.c (or the pre-built yyjson.o).

#include <cstdlib>   // free() — yyjson_val_write allocates with libc malloc
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "yyjson.h"

#include "ops/json_path.h"
#include "ops/string_result.h"

namespace draken::ops {

// ---------------------------------------------------------------------------
// Read flags — chosen once here so every consumer of extract_rows parses
// identically and they cannot drift apart.
//
// YYJSON_READ_STOP_WHEN_DONE: a row's bytes are ONE document. What follows the
//   closing brace is not ours to validate, and stopping at the end of the first
//   value skips the trailing-content scan.
//
// YYJSON_READ_NUMBER_AS_RAW: numbers keep their original source bytes instead of
//   being parsed into int64/double. This is the one flag here with a VISIBLE
//   effect, and it is deliberate:
//
//     `->` and `->>` return the JSON *text* of the selected value, so for a number
//     the source token already IS the answer. Parsing it to a double and printing
//     it back is a lossy round-trip we pay for twice — number parsing on read,
//     shortest-representation formatting on write. Keeping the raw token skips
//     both and cannot drift from the source.
//
//     Rendering therefore changes for numbers that are not written canonically:
//     `{"a":1e3}` -> 'a' now yields `1e3` where it used to yield `1000.0`, and
//     `{"a":1.10}` yields `1.10` where it used to yield `1.1`. Integers — the
//     overwhelming majority of real JSON numbers — are byte-identical either way.
//
//   Dropping the flag from this line reverts that rendering exactly, and changes
//   nothing else.
static constexpr yyjson_read_flag kJsonReadFlags =
    YYJSON_READ_STOP_WHEN_DONE | YYJSON_READ_NUMBER_AS_RAW;

// ---------------------------------------------------------------------------
// RAII guard — frees yyjson_doc* on scope exit
// ---------------------------------------------------------------------------
struct JDocGuard {
    yyjson_doc* doc;
    explicit JDocGuard(yyjson_doc* d) noexcept : doc(d) {}
    ~JDocGuard() noexcept { if (doc) yyjson_doc_free(doc); }
    JDocGuard(const JDocGuard&)            = delete;
    JDocGuard& operator=(const JDocGuard&) = delete;
    JDocGuard(JDocGuard&& o) noexcept : doc(o.doc) { o.doc = nullptr; }
};

// ---------------------------------------------------------------------------
// ReadPool — one parse arena for the whole column.
//
// yyjson's default allocator mallocs (and then frees) the document's value array
// on every read. At one document per row that is two allocator round-trips per
// row, for a buffer whose lifetime ends before the next row starts.
//
// Instead: size a single block from the LONGEST document in the column and re-init
// a pool allocator over it per row — an O(1) pointer reset that reclaims whatever
// the previous row used. yyjson_doc_free against a pool allocator is likewise a
// no-op. No parsing decision changes; only where the memory comes from.
//
// Above kMaxPoolBytes the pool is declined and yyjson's default allocator is used.
// That is an allocation-strategy choice, not a behaviour switch — both arms parse
// the same bytes into the same values. Without the cap a column holding a single
// pathological multi-megabyte document would reserve ~13x its size up front.
// ---------------------------------------------------------------------------
class ReadPool {
  public:
    static constexpr size_t kMaxPoolBytes = 16u * 1024u * 1024u;

    // `max_doc_len` must be >= the longest document handed to read().
    explicit ReadPool(size_t max_doc_len) {
        const size_t need = yyjson_read_max_memory_usage(max_doc_len, kJsonReadFlags);
        if (need == 0u || need > kMaxPoolBytes) return;   // decline; use libc malloc
        buf_.resize(need);
        enabled_ = true;
    }

    yyjson_doc* read(const char* data, size_t len, yyjson_read_err* err) noexcept {
        if (!enabled_)
            return yyjson_read_opts(const_cast<char*>(data), len, kJsonReadFlags,
                                    nullptr, err);
        yyjson_alc alc;
        yyjson_alc_pool_init(&alc, buf_.data(), buf_.size());
        return yyjson_read_opts(const_cast<char*>(data), len, kJsonReadFlags, &alc, err);
    }

  private:
    std::vector<char> buf_;
    bool              enabled_ = false;
};

// Longest PHYSICAL value in a string-family vector — the pool only has to hold the
// biggest document that can actually be parsed, and a dict-shaped column's repeated
// rows all point back into the same `data_length` values.
static inline size_t max_slot_length(const DrakenVector* dv) noexcept {
    const auto* sa = static_cast<const DrakenStringArena*>(dv->data);
    size_t max_len = 0u;
    const uint32_t k = dv->data_length;
    for (uint32_t j = 0u; j < k; ++j) {
        const uint32_t l = str_length(&sa->slots[j]);
        if (l > max_len) max_len = l;
    }
    return max_len;
}

// ---------------------------------------------------------------------------
// JsonNav — a navigation path with every path-shaped decision already made at
// bind time (splitting, un-escaping, index parsing — see json_path.h).
//
// mode 0: walk the pointer tokens (`->`, `->>`).
// mode 1: one verbatim top-level object key (`vector_map_access`) — never
//         un-escaped, never treated as an array index.
// ---------------------------------------------------------------------------
struct JsonNav {
    const JsonPtrToken* tokens  = nullptr;
    uint32_t            ntokens = 0u;
    const char*         blob    = nullptr;
    int                 mode    = 0;
};

// Walk `nav` from `root`. nullptr on any miss — absent key, index past the end, or
// a token applied to a scalar.
//
// This is the per-row replacement for yyjson_ptr_getn. Same result; the splitting,
// un-escaping and index parsing already happened once at bind time, leaving only
// the container lookups.
static inline yyjson_val* nav_tokens(yyjson_val* root, const JsonNav& nav) noexcept {
    if (nav.ntokens == 0u) return root;

    if (nav.mode == 1) {
        if (!root || !yyjson_is_obj(root)) return nullptr;
        const JsonPtrToken& t = nav.tokens[0];
        return yyjson_obj_getn(root, nav.blob + t.off, t.len);
    }

    yyjson_val* cur = root;
    for (uint32_t i = 0u; i < nav.ntokens; ++i) {
        if (!cur) return nullptr;
        const JsonPtrToken& t = nav.tokens[i];
        if (yyjson_is_obj(cur)) {
            cur = yyjson_obj_getn(cur, nav.blob + t.off, t.len);
        } else if (yyjson_is_arr(cur)) {
            if (t.index == kJsonPtrNotIndex) return nullptr;
            cur = yyjson_arr_get(cur, static_cast<size_t>(t.index));
        } else {
            return nullptr;   // a token applied to a scalar
        }
    }
    return cur;
}

// ---------------------------------------------------------------------------
// extract_rows_multi — the core loop, for N paths over ONE parse.
//
//   dv         — VARCHAR/NVARCHAR/VARBINARY/VARIANT vector; one JSON document per row.
//   navs       — npaths bind-time-resolved navigation paths (see JsonNav).
//   text_modes — npaths flags; false (`->`) emits JSON text tagged VARIANT,
//                true (`->>`) emits text with JSON strings unquoted (raw UTF-8)
//                and everything else serialized, tagged NVARCHAR. Per path, so a
//                `->` and a `->>` on the same column still share one parse.
//   out        — caller-provided array of npaths StringRows, filled on success.
//
// Parsing is what this costs — navigation and emit are noise beside it — so N
// paths over one document parse once, not N times. That is the whole reason this
// signature is plural; a single-path extraction is just npaths == 1 (extract_rows
// below), which keeps ONE row loop in the codebase rather than two that can drift.
//
// Null TVL: null input row / missing key / JSON null → null output row.
// Invalid JSON in any row → std::runtime_error (fail fast; never a silent null).
//
// Throws on error, having freed every buffer it allocated (including partially
// built outputs for the other paths). Callers running under the C ABI wrap this
// so the throw becomes an error sentinel.
// ---------------------------------------------------------------------------
static inline void extract_rows_multi(
    const DrakenVector* dv,
    const JsonNav*      navs,
    const bool*         text_modes,
    size_t              npaths,
    StringRows*         out,
    const char*         fn_name)
{
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    for (size_t k = 0u; k < npaths; ++k) {
        out[k] = StringRows{};
        out[k].length = n;
        out[k].type   = text_modes[k] ? DRAKEN_NVARCHAR : DRAKEN_VARIANT;
    }

    // RAII: release EVERY path's allocations if any row throws.
    struct Guard {
        StringRows* o; size_t n; bool released = false;
        ~Guard() { if (!released && o) for (size_t k = 0u; k < n; ++k) sr_free(o[k]); }
    } guard{out, npaths};

    for (size_t k = 0u; k < npaths; ++k) out[k].slots = sr_alloc_slots(n);

    std::vector<bool> any_null(npaths, false);

    // Long-form bytes accumulate per path, then move into draken_malloc'd buffers.
    // NOT pre-reserved: `->>` on a short scalar (the overwhelmingly common shape —
    // an id, an enum-ish string, a timestamp) is <= STR_INLINE_MAX and never touches
    // the arena at all, so a rows*32 reservation per path is megabytes of untouched
    // memory per morsel per path. Multiplied by npaths and by every worker thread it
    // cost more in allocator traffic than the geometric growth it was avoiding.
    std::vector<std::vector<uint8_t>> arena_bufs(npaths);

    // One parse arena reused by every row (see ReadPool).
    ReadPool pool(max_slot_length(dv));

    for (uint32_t i = 0u; i < n; ++i) {
        if (!sr_row_is_valid(dv, i)) {
            for (size_t k = 0u; k < npaths; ++k) {
                sr_mark_null(out[k], i);
                any_null[k] = true;
            }
            continue;
        }

        const DrakenStringSlot* src_slot = &sa->slots[dv->selection[i]];
        const uint8_t* json_bytes = str_data(src_slot, sa->arena);
        const uint32_t json_len   = str_length(src_slot);

        yyjson_read_err parse_err;
        yyjson_doc* raw_doc = pool.read(reinterpret_cast<const char*>(json_bytes),
                                        static_cast<size_t>(json_len), &parse_err);
        if (!raw_doc)
            throw std::runtime_error(
                std::string(fn_name) + ": invalid JSON at row " + std::to_string(i) +
                ": " + (parse_err.msg ? parse_err.msg : "unknown error"));

        JDocGuard doc_guard(raw_doc);
        yyjson_val* root = yyjson_doc_get_root(raw_doc);

        for (size_t k = 0u; k < npaths; ++k) {
            yyjson_val* val = nav_tokens(root, navs[k]);

            if (!val || yyjson_is_null(val)) {
                sr_mark_null(out[k], i);
                any_null[k] = true;
                continue;
            }

            // `->>` on a JSON string yields the raw (unquoted, unescaped) UTF-8
            // content; everything else — and all of `->` — is serialized as JSON
            // text. yyjson_val_write allocates with libc malloc; the guard frees it
            // even if the arena insert below throws.
            struct MallocGuard {
                char* p;
                ~MallocGuard() { if (p) std::free(p); }
            } mg{nullptr};

            const char* out_str = nullptr;
            size_t      out_len = 0u;
            if (yyjson_is_raw(val)) {
                // A number still in its source bytes (kJsonReadFlags). Those bytes
                // are already the JSON text of the value, for `->` and `->>` alike
                // — no write call, no allocation, no re-formatting.
                out_str = yyjson_get_raw(val);
                out_len = yyjson_get_len(val);
            } else if (text_modes[k] && yyjson_is_str(val)) {
                out_str = yyjson_get_str(val);
                out_len = yyjson_get_len(val);
            } else {
                mg.p = yyjson_val_write(val, 0u, &out_len);
                if (!mg.p)
                    throw std::runtime_error(
                        std::string(fn_name) + ": yyjson_val_write failed at row " +
                        std::to_string(i));
                out_str = mg.p;
            }

            if (out_len <= STR_INLINE_MAX) {
                str_init_inline(&out[k].slots[i],
                                reinterpret_cast<const uint8_t*>(out_str),
                                static_cast<uint32_t>(out_len));
            } else {
                std::vector<uint8_t>& ab = arena_bufs[k];
                const uint32_t off = static_cast<uint32_t>(ab.size());
                ab.insert(ab.end(),
                          reinterpret_cast<const uint8_t*>(out_str),
                          reinterpret_cast<const uint8_t*>(out_str) + out_len);
                // insert() may have reallocated — re-derive the pointer for the hash.
                draken_build_string_slot(&out[k].slots[i], ab.data() + off,
                                         static_cast<uint32_t>(out_len), off);
            }
        }
    }

    for (size_t k = 0u; k < npaths; ++k) {
        if (!any_null[k] && out[k].validity) {
            draken_free(out[k].validity);
            out[k].validity = nullptr;
        }
        out[k].arena_len = arena_bufs[k].size();
        if (out[k].arena_len > 0u) {
            out[k].arena = static_cast<uint8_t*>(draken_malloc(out[k].arena_len));
            if (!out[k].arena) throw std::bad_alloc();
            std::memcpy(out[k].arena, arena_bufs[k].data(), out[k].arena_len);
        }
    }

    guard.released = true;
}

// Single-path extraction — npaths == 1 over the loop above, so there is exactly
// one row loop in the codebase.
static inline StringRows extract_rows(
    const DrakenVector* dv,
    const JsonNav&      nav,
    bool                text_mode,
    const char*         fn_name)
{
    StringRows out;
    const bool tm = text_mode;
    extract_rows_multi(dv, &nav, &tm, 1u, &out, fn_name);
    return out;
}

} // namespace draken::ops
