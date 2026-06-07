// opteryx/compiled/nanobind/vector_json.cpp — Milestone E.18, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, two functions:
//
//   vector_json_extract(docs, path)   — full JSONPath / JSON Pointer extraction.
//   vector_map_access(docs, key)      — simpler top-level object key access.
//
// Both inputs:
//   docs  — VARCHAR DrakenVector (JSON text, one document per row)
//   path/key — Python bytes (UTF-8; the path/key is scalar, same for all rows)
//
// Both outputs:
//   DRAKEN_VARCHAR DrakenVector — JSON-text representation of the extracted value.
//   Chains compose: JSON_EXTRACT(JSON_EXTRACT(x,'$.a'),'$.b') is valid.
//
// Null TVL:
//   Null input row               → null output row.
//   JSON null value (`null`)     → null output row  (SQL convention).
//   Missing key / out-of-range   → null output row.
//   Invalid JSON in a row        → RuntimeError (fail fast, no silent null).
//
// Path pre-processing:
//   The path/key bytes are converted to RFC 6901 JSON Pointer format ONCE
//   before the row loop, so per-row cost is only parse + navigate + serialise.
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_json_extract.pyx  (deleted)
//   opteryx/compiled/vector_ops/vector_map_access.pyx    (deleted)

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstdlib>   // free() — yyjson_val_write allocates with libc malloc
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"
#include "ops/json_extract.h"   // JDocGuard, dotpath_to_jsonptr

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static inline bool is_valid_at(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return true;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

// Unwrap a VARCHAR-family or VARIANT DrakenVector.  Raises TypeError on non-Vector or
// non-string type.  VARIANT is accepted because vector_json_extract returns VARIANT,
// making it composable: json_extract(json_extract(x, '$.a'), '$.b').
static const DrakenVector* unwrap_str_vec(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    const bool ok =
        dv->type == DRAKEN_VARCHAR   ||
        dv->type == DRAKEN_NVARCHAR  ||
        dv->type == DRAKEN_VARBINARY ||
        dv->type == DRAKEN_VARIANT;
    if (!ok)
        throw nb::type_error(
            (std::string(fn) + ": expected a VARCHAR/NVARCHAR/VARBINARY DrakenVector").c_str());
    return dv;
}

// Emit a null slot into the arena/slots at logical row i.
static inline void emit_null(DrakenStringSlot* slots, uint32_t code,
                              uint8_t*& validity, uint32_t i, uint32_t n) {
    // Mark invalid in the bitmap.
    if (!validity) {
        const uint32_t nb_bytes = (n + 7u) >> 3;
        validity = static_cast<uint8_t*>(draken_malloc(nb_bytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0xFFu, nb_bytes);
    }
    validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
    // Zero the slot so it's in a defined state.
    std::memset(&slots[code], 0, sizeof(DrakenStringSlot));
}

// ---------------------------------------------------------------------------
// Core extraction loop
// ---------------------------------------------------------------------------
//
// mode == 0  →  use yyjson_ptr_get   (full JSON Pointer, pre-converted)
// mode == 1  →  use yyjson_obj_get   (top-level key, no pointer syntax)

static nb::object impl_extract(
    nb::object  docs_obj,
    const char* nav_arg,   // RFC 6901 pointer (mode 0) or raw key (mode 1)
    size_t      nav_len,
    int         mode,
    bool        text_mode, // false: `->` → VARIANT (JSON text); true: `->>` → NVARCHAR text
    const char* fn_name)
{
    // `->`  (text_mode=false): emit the value as JSON text, tagged VARIANT.
    // `->>` (text_mode=true):  emit text — JSON strings unquoted (raw UTF-8),
    //                          other values serialized; tagged NVARCHAR.
    const DrakenType out_type = text_mode ? DRAKEN_NVARCHAR : DRAKEN_VARIANT;
    const DrakenVector* dv = unwrap_str_vec(docs_obj, fn_name);
    const DrakenStringArena* sa =
        static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    // Allocate output slots (one per logical row, dense layout).
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    uint8_t* validity = nullptr;   // allocated lazily on first null
    bool     any_null = false;

    // Growing arena: we accumulate extern-slot bytes here, then copy to a
    // draken_malloc'd buffer at the end.  Offsets stored in slots reference
    // positions within this buffer and remain valid as it grows.
    std::vector<uint8_t> arena_buf;
    arena_buf.reserve(static_cast<size_t>(n) * 32u);

    // RAII: free slots on any early exception.
    struct SlotsGuard {
        DrakenStringSlot* p; bool released;
        ~SlotsGuard() { if (!released && p) draken_free(p); }
    } sg{slots, false};

    for (uint32_t i = 0u; i < n; ++i) {
        const uint32_t code = dv->selection[i];

        if (!is_valid_at(dv, i)) {
            emit_null(slots, i, validity, i, n);
            any_null = true;
            continue;
        }

        const DrakenStringSlot* src_slot = &sa->slots[code];
        const uint8_t* json_bytes = str_data(src_slot, sa->arena);
        const uint32_t json_len   = str_length(src_slot);

        // Parse the JSON document.
        yyjson_read_err parse_err;
        yyjson_doc* raw_doc = yyjson_read_opts(
            const_cast<char*>(reinterpret_cast<const char*>(json_bytes)),
            static_cast<size_t>(json_len),
            0u, nullptr, &parse_err);

        if (!raw_doc) {
            draken_free(slots);
            sg.released = true;
            if (validity) draken_free(validity);
            throw std::runtime_error(
                std::string(fn_name) + ": invalid JSON at row " +
                std::to_string(i) + ": " +
                (parse_err.msg ? parse_err.msg : "unknown error"));
        }

        draken::ops::JDocGuard doc_guard(raw_doc);

        // Navigate to the target value.
        yyjson_val* val = nullptr;
        if (mode == 0) {
            // Full JSON Pointer via yyjson_ptr_get.
            if (nav_len == 0u) {
                val = yyjson_doc_get_root(raw_doc);
            } else {
                val = yyjson_ptr_getn(
                    yyjson_doc_get_root(raw_doc), nav_arg, nav_len);
            }
        } else {
            // Top-level object key via yyjson_obj_get.
            yyjson_val* root = yyjson_doc_get_root(raw_doc);
            if (root && yyjson_is_obj(root)) {
                val = yyjson_obj_getn(root, nav_arg, nav_len);
            }
        }

        // Missing key or JSON null → SQL NULL.
        if (!val || yyjson_is_null(val)) {
            emit_null(slots, i, validity, i, n);
            any_null = true;
            continue;
        }

        // Produce the output bytes. `->>` on a JSON string yields the raw
        // (unquoted, unescaped) UTF-8 content; everything else — and all of
        // `->` — is serialized as JSON text. The bytes are copied into the slot
        // below within this iteration, before the parsed doc is freed.
        const char* out_str = nullptr;
        size_t      out_len = 0u;
        char*       malloced = nullptr;   // set only when we serialized
        if (text_mode && yyjson_is_str(val)) {
            out_str = yyjson_get_str(val);
            out_len = yyjson_get_len(val);
        } else {
            malloced = yyjson_val_write(val, 0u, &out_len);
            if (!malloced) {
                draken_free(slots);
                sg.released = true;
                if (validity) draken_free(validity);
                throw std::runtime_error(
                    std::string(fn_name) + ": yyjson_val_write failed at row " +
                    std::to_string(i));
            }
            out_str = malloced;
        }

        // Build the output slot (copies the bytes into our own storage).
        if (out_len <= STR_INLINE_MAX) {
            str_init_inline(&slots[i],
                reinterpret_cast<const uint8_t*>(out_str),
                static_cast<uint32_t>(out_len));
        } else {
            const uint32_t off = static_cast<uint32_t>(arena_buf.size());
            arena_buf.insert(arena_buf.end(),
                reinterpret_cast<const uint8_t*>(out_str),
                reinterpret_cast<const uint8_t*>(out_str) + out_len);
            // arena_buf may have reallocated; re-derive data pointer for hash.
            draken_build_string_slot(
                &slots[i],
                arena_buf.data() + off,
                static_cast<uint32_t>(out_len),
                off);
        }

        if (malloced) std::free(malloced);   // yyjson_val_write uses libc malloc
    }

    // Discard unused validity bitmap.
    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    // Copy growing arena to a draken_malloc'd buffer.
    const size_t arena_size = arena_buf.size();
    uint8_t* final_arena    = nullptr;
    if (arena_size > 0u) {
        final_arena = static_cast<uint8_t*>(draken_malloc(arena_size));
        if (!final_arena) {
            draken_free(slots);
            sg.released = true;
            if (validity) draken_free(validity);
            throw std::bad_alloc();
        }
        std::memcpy(final_arena, arena_buf.data(), arena_size);
    }

    sg.released = true;  // ownership now transfers to draken_vector_own_string

    PyObject* out = draken_vector_own_string(
        slots, final_arena, arena_size, validity, n, out_type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_json_extract — full JSONPath / JSON Pointer
// ---------------------------------------------------------------------------

static nb::object impl_json_extract(nb::object docs, nb::bytes path_bytes, bool text_mode) {
    const char* raw = path_bytes.c_str();
    const size_t raw_len = path_bytes.size();

    // Convert dot-notation to RFC 6901 JSON Pointer once, before the row loop.
    std::string json_ptr = draken::ops::dotpath_to_jsonptr(raw, raw_len);

    return impl_extract(docs, json_ptr.c_str(), json_ptr.size(),
                        0 /* PtrGet */, text_mode,
                        text_mode ? "vector_json_extract_text" : "vector_json_extract");
}

// ---------------------------------------------------------------------------
// vector_map_access — simpler top-level object key
// ---------------------------------------------------------------------------

static nb::object impl_map_access(nb::object docs, nb::bytes key_bytes) {
    return impl_extract(docs, key_bytes.c_str(), key_bytes.size(),
                        1 /* ObjGet */, false /* VARIANT */, "vector_map_access");
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_json, m) {

    m.def("vector_json_extract",
        [](nb::object docs, nb::bytes path) -> nb::object {
            return impl_json_extract(docs, path, /*text_mode=*/false);
        },
        nb::arg("docs"), nb::arg("path"),
        "`->`: extract a field from each JSON document, as a JSON value.\n"
        "path: bytes — dot-notation (\"$.a.b[0]\"), JSON Pointer (\"/a/b/0\"),\n"
        "      or bare top-level key (\"name\").\n"
        "Output: DRAKEN_VARIANT (JSON-text of extracted value).\n"
        "Null TVL: null input row / missing key / JSON null → null output.\n"
        "Invalid JSON → RuntimeError (fail fast).");

    m.def("vector_json_extract_text",
        [](nb::object docs, nb::bytes path) -> nb::object {
            return impl_json_extract(docs, path, /*text_mode=*/true);
        },
        nb::arg("docs"), nb::arg("path"),
        "`->>`: extract a field as TEXT. JSON strings are returned unquoted\n"
        "(raw UTF-8); other values (number/bool/object/array) are serialized to\n"
        "their JSON text. Output: DRAKEN_NVARCHAR.\n"
        "Null TVL: null input row / missing key / JSON null → null output.\n"
        "Invalid JSON → RuntimeError (fail fast).");

    m.def("vector_map_access",
        [](nb::object docs, nb::bytes key) -> nb::object {
            return impl_map_access(docs, key);
        },
        nb::arg("docs"), nb::arg("key"),
        "Top-level object key access on each JSON document in a VARCHAR vector.\n"
        "key: bytes — simple field name (no path syntax).\n"
        "Output: DRAKEN_VARIANT (JSON-text of extracted value).\n"
        "Null TVL: null input row / missing key / JSON null → null output.\n"
        "Invalid JSON → RuntimeError (fail fast).");
}
