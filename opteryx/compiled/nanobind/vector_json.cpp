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
#include <stdexcept>
#include <string>

#include "core/buffers.h"
#include "core/draken_bridge.h"
#include "ops/json_extract.h"   // extract_rows, dotpath_to_jsonptr

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Core extraction loop
// ---------------------------------------------------------------------------
//
// mode == 0  →  use yyjson_ptr_get   (full JSON Pointer, pre-converted)
// mode == 1  →  use yyjson_obj_get   (top-level key, no pointer syntax)

static nb::object impl_extract(
    nb::object  docs_obj,
    const draken::ops::JsonPtrPath& path,  // resolved before the row loop
    int         mode,
    bool        text_mode, // false: `->` → VARIANT (JSON text); true: `->>` → NVARCHAR text
    const char* fn_name)
{
    // The row loop lives in draken/ops/json_extract.h — the SAME code the C-ABI
    // kernel (draken/ops/kernels/extraction.cpp) runs. This wrapper only unwraps
    // the Python operand and re-wraps the produced buffers as a Vector.
    //
    // The kernel gets its resolved path from extraction_ctx (built once per bind);
    // here there is no bind step, so the equivalent resolution happens once per
    // call — still outside the row loop, which is the property that matters.
    draken::ops::JsonNav nav;
    nav.tokens  = path.tokens.data();
    nav.ntokens = static_cast<uint32_t>(path.tokens.size());
    nav.blob    = path.blob.data();
    nav.mode    = mode;

    const DrakenVector* dv = unwrap_str_vec(docs_obj, fn_name);
    draken::ops::StringRows rows =
        draken::ops::extract_rows(dv, nav, text_mode, fn_name);

    PyObject* out = draken_vector_own_string(
        rows.slots, rows.arena, rows.arena_len, rows.validity, rows.length, rows.type);
    if (!out) {
        draken::ops::sr_free(rows);
        throw nb::python_error();
    }
    return nb::steal<nb::object>(out);
}


// ---------------------------------------------------------------------------
// vector_json_extract — full JSONPath / JSON Pointer
// ---------------------------------------------------------------------------

static nb::object impl_json_extract(nb::object docs, nb::bytes path_bytes, bool text_mode) {
    const char* raw = path_bytes.c_str();
    const size_t raw_len = path_bytes.size();

    // Resolve the path once, before the row loop: dot-notation → RFC 6901 pointer
    // → tokens with escapes applied and array indices parsed.
    const std::string json_ptr = draken::ops::dotpath_to_jsonptr(raw, raw_len);
    const draken::ops::JsonPtrPath path =
        draken::ops::tokenize_jsonptr(json_ptr.data(), json_ptr.size());

    return impl_extract(docs, path, 0 /* token walk */, text_mode,
                        text_mode ? "vector_json_extract_text" : "vector_json_extract");
}

// ---------------------------------------------------------------------------
// vector_map_access — simpler top-level object key
// ---------------------------------------------------------------------------

static nb::object impl_map_access(nb::object docs, nb::bytes key_bytes) {
    // Verbatim top-level key: no pointer syntax, no escape resolution, never an
    // array index.
    const draken::ops::JsonPtrPath path =
        draken::ops::single_key_path(key_bytes.c_str(), key_bytes.size());
    return impl_extract(docs, path, 1 /* ObjGet */, false /* VARIANT */, "vector_map_access");
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

void register_vector_json(nb::module_ &m) {

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
