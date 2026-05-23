#pragma once
// draken/ops/json_extract.h — JSON field extraction helpers for vector_json.cpp.
//
// Provides:
//   draken::ops::dotpath_to_jsonptr(path, len) → std::string  (RFC 6901 pointer)
//   draken::ops::JDocGuard                                     (RAII for yyjson_doc*)
//
// The TU that includes this header must have "third_party/yyjson/src" on its
// include path and must link yyjson.c (or the pre-built yyjson.o).

#include <string>
#include <stdexcept>
#include "yyjson.h"

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
// dotpath_to_jsonptr — convert dot-notation path to RFC 6901 JSON Pointer
//
// Accepted input formats:
//   "$.a.b[0].c"  →  "/a/b/0/c"   (dollar-dot prefix)
//   "/a/b"        →  "/a/b"        (already a JSON Pointer — pass through)
//   "name"        →  "/name"       (bare key — prepend slash)
//   ""            →  ""            (empty — root pointer for yyjson_doc_get_root)
//
// RFC 6901 escaping applied to key tokens:
//   '~' → "~0",  '/' → "~1"
//
// The result is a std::string suitable for yyjson_ptr_get / yyjson_doc_ptr_get.
// ---------------------------------------------------------------------------
static inline std::string dotpath_to_jsonptr(const char* path, size_t len) {
    if (len == 0) return std::string{};

    // Already a JSON Pointer — pass through.
    if (path[0] == '/') return std::string(path, len);

    std::string result;
    result.reserve(len + 4);

    const char* p   = path;
    const char* end = path + len;

    // Skip leading "$" or "$."
    if (p < end && p[0] == '$') {
        ++p;
        if (p < end && p[0] == '.') ++p;
    }

    // If nothing remains after stripping "$", return empty (root).
    if (p >= end) return std::string{};

    while (p < end) {
        if (p[0] == '.') { ++p; continue; }

        if (p[0] == '[') {
            // Array index token: [42]
            ++p; // skip '['
            const char* idx_start = p;
            while (p < end && p[0] != ']') ++p;
            if (p >= end)
                throw std::invalid_argument("JSON path: unmatched '['");
            result += '/';
            result.append(idx_start, static_cast<size_t>(p - idx_start));
            ++p; // skip ']'
            if (p < end && p[0] == '.') ++p; // optional trailing dot
            continue;
        }

        // Regular key token — stop at '.' or '['.
        result += '/';
        while (p < end && p[0] != '.' && p[0] != '[') {
            if      (p[0] == '~') result += "~0";
            else if (p[0] == '/') result += "~1";
            else                  result += p[0];
            ++p;
        }
    }

    return result;
}

} // namespace draken::ops
