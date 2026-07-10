#pragma once
// draken/ops/json_extract.h — JSON field extraction core.
//
// Provides:
//   draken::ops::dotpath_to_jsonptr(path, len) → std::string  (via json_path.h)
//   draken::ops::JDocGuard                                     (RAII for yyjson_doc*)
//   draken::ops::ExtractRows                                   (owned component buffers)
//   draken::ops::extract_rows(...)                             (the shared row loop)
//
// extract_rows is the SINGLE implementation of `->`, `->>` and top-level map
// access. Two consumers finalize its component buffers differently:
//   * the C-ABI kernel (draken/ops/kernels/extraction.cpp) → vecresult_from_string_buffers
//   * the nanobind binding (opteryx/compiled/nanobind/vector_json.cpp) → draken_vector_own_string
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
// extract_rows — the core loop.
//
//   dv        — VARCHAR/NVARCHAR/VARBINARY/VARIANT vector; one JSON document per row.
//   nav       — RFC 6901 pointer (mode 0, pre-converted) or a raw top-level key (mode 1).
//   mode      — 0: yyjson_ptr_getn (full pointer);  1: yyjson_obj_getn (top-level key).
//   text_mode — false (`->`):  emit JSON text, tagged VARIANT.
//               true  (`->>`): emit text; JSON strings unquoted (raw UTF-8),
//                              everything else serialized. Tagged NVARCHAR.
//
// Null TVL: null input row / missing key / JSON null → null output row.
// Invalid JSON in any row → std::runtime_error (fail fast; never a silent null).
//
// Throws on error, having freed every buffer it allocated. Callers running under
// the C ABI wrap this in DRAKEN_KERNEL_TRY, which converts the throw into an
// error sentinel.
// ---------------------------------------------------------------------------
static inline StringRows extract_rows(
    const DrakenVector* dv,
    const char* nav,
    size_t      nav_len,
    int         mode,
    bool        text_mode,
    const char* fn_name)
{
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    StringRows out;
    out.length = n;
    out.type   = text_mode ? DRAKEN_NVARCHAR : DRAKEN_VARIANT;
    out.slots  = sr_alloc_slots(n);

    // RAII: release every allocation if any row throws.
    struct Guard {
        StringRows* o; bool released = false;
        ~Guard() { if (!released && o) sr_free(*o); }
    } guard{&out};

    bool any_null = false;

    // Long-form bytes accumulate here, then move into a draken_malloc'd buffer.
    std::vector<uint8_t> arena_buf;
    arena_buf.reserve(static_cast<size_t>(n) * 32u);

    for (uint32_t i = 0u; i < n; ++i) {
        if (!sr_row_is_valid(dv, i)) {
            sr_mark_null(out, i);
            any_null = true;
            continue;
        }

        const DrakenStringSlot* src_slot = &sa->slots[dv->selection[i]];
        const uint8_t* json_bytes = str_data(src_slot, sa->arena);
        const uint32_t json_len   = str_length(src_slot);

        yyjson_read_err parse_err;
        yyjson_doc* raw_doc = yyjson_read_opts(
            const_cast<char*>(reinterpret_cast<const char*>(json_bytes)),
            static_cast<size_t>(json_len), 0u, nullptr, &parse_err);
        if (!raw_doc)
            throw std::runtime_error(
                std::string(fn_name) + ": invalid JSON at row " + std::to_string(i) +
                ": " + (parse_err.msg ? parse_err.msg : "unknown error"));

        JDocGuard doc_guard(raw_doc);

        yyjson_val* val = nullptr;
        if (mode == 0) {
            val = (nav_len == 0u)
                ? yyjson_doc_get_root(raw_doc)
                : yyjson_ptr_getn(yyjson_doc_get_root(raw_doc), nav, nav_len);
        } else {
            yyjson_val* root = yyjson_doc_get_root(raw_doc);
            if (root && yyjson_is_obj(root)) val = yyjson_obj_getn(root, nav, nav_len);
        }

        if (!val || yyjson_is_null(val)) {
            sr_mark_null(out, i);
            any_null = true;
            continue;
        }

        // `->>` on a JSON string yields the raw (unquoted, unescaped) UTF-8
        // content; everything else — and all of `->` — is serialized as JSON text.
        // yyjson_val_write allocates with libc malloc; the guard frees it even if
        // the arena insert below throws.
        struct MallocGuard {
            char* p;
            ~MallocGuard() { if (p) std::free(p); }
        } mg{nullptr};

        const char* out_str = nullptr;
        size_t      out_len = 0u;
        if (text_mode && yyjson_is_str(val)) {
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
            str_init_inline(&out.slots[i],
                            reinterpret_cast<const uint8_t*>(out_str),
                            static_cast<uint32_t>(out_len));
        } else {
            const uint32_t off = static_cast<uint32_t>(arena_buf.size());
            arena_buf.insert(arena_buf.end(),
                             reinterpret_cast<const uint8_t*>(out_str),
                             reinterpret_cast<const uint8_t*>(out_str) + out_len);
            // insert() may have reallocated — re-derive the pointer for the hash.
            draken_build_string_slot(&out.slots[i], arena_buf.data() + off,
                                     static_cast<uint32_t>(out_len), off);
        }
    }

    if (!any_null && out.validity) {
        draken_free(out.validity);
        out.validity = nullptr;
    }

    out.arena_len = arena_buf.size();
    if (out.arena_len > 0u) {
        out.arena = static_cast<uint8_t*>(draken_malloc(out.arena_len));
        if (!out.arena) throw std::bad_alloc();
        std::memcpy(out.arena, arena_buf.data(), out.arena_len);
    }

    guard.released = true;
    return out;
}

} // namespace draken::ops
