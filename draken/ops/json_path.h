#pragma once
// draken/ops/json_path.h — JSON navigation paths, resolved at BIND time.
//
// Two stages, both of which run once per plan and never per row:
//   dotpath_to_jsonptr  — dot-notation ("$.a.b[0]") → RFC 6901 pointer ("/a/b/0")
//   tokenize_jsonptr    — RFC 6901 pointer → unescaped tokens + parsed array indices
//
// The second stage exists because yyjson_ptr_getn re-scans the pointer string on
// EVERY row: splitting on '/', un-escaping ~0/~1, and re-parsing numeric tokens as
// array indices. None of that depends on the document, so none of it belongs in the
// row loop — tokenize once at bind time and the loop is left with nothing but
// container lookups (see nav_tokens in json_extract.h).
//
// Split out of json_extract.h so translation units that only need to resolve a path
// at bind time (kernel_registry.cpp, which stores the tokens in extraction_ctx) do
// not have to pull in yyjson.

#include <string>
#include <stdexcept>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace draken::ops {

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

// ---------------------------------------------------------------------------
// Pre-tokenized JSON Pointer
//
// One token per '/'-separated reference token, with RFC 6901 escapes already
// resolved and any array index already parsed. `off`/`len` address the companion
// byte blob; `index` is the array subscript, or kJsonPtrNotIndex when the token is
// not a valid RFC 6901 array index.
//
// Trivially copyable and self-contained (offsets, not pointers) so the whole thing
// can be memcpy'd into the tail of a malloc'd extraction_ctx block.
// ---------------------------------------------------------------------------
static constexpr uint32_t kJsonPtrNotIndex = 0xFFFFFFFFu;

struct JsonPtrToken {
    uint32_t off;    // byte offset into the blob
    uint32_t len;    // token length in bytes, escapes resolved
    uint32_t index;  // parsed array index, or kJsonPtrNotIndex
};

struct JsonPtrPath {
    std::vector<JsonPtrToken> tokens;
    std::string               blob;
};

// Is `s` a valid RFC 6901 array index? The grammar is "0" | [1-9][0-9]* — leading
// zeros are NOT valid, which is what keeps {"01": …} reachable as an object key.
// Anything else (including "-", the past-the-end token) is not an index; on an
// array container that yields a miss, exactly as yyjson_ptr does.
static inline uint32_t jsonptr_parse_index(const char* s, size_t len) noexcept {
    if (len == 0u || len > 10u) return kJsonPtrNotIndex;
    if (s[0] == '0') return (len == 1u) ? 0u : kJsonPtrNotIndex;
    uint64_t v = 0u;
    for (size_t i = 0u; i < len; ++i) {
        const char c = s[i];
        if (c < '0' || c > '9') return kJsonPtrNotIndex;
        v = v * 10u + static_cast<uint64_t>(c - '0');
        if (v >= kJsonPtrNotIndex) return kJsonPtrNotIndex;
    }
    return static_cast<uint32_t>(v);
}

// Split an RFC 6901 pointer into tokens. `ptr` must already be a pointer (the
// output of dotpath_to_jsonptr): empty means "the whole document", otherwise it
// starts with '/'. An empty pointer yields zero tokens.
//
// Escapes are resolved here, once: "~1" → '/', "~0" → '~'. Per RFC 6901 the order
// matters — "~01" is the two characters '~' and '1', not '/' — which a
// single-pass left-to-right scan gets right for free.
static inline JsonPtrPath tokenize_jsonptr(const char* ptr, size_t len) {
    JsonPtrPath out;
    if (len == 0u) return out;
    if (ptr[0] != '/')
        throw std::invalid_argument("JSON pointer: must be empty or start with '/'");

    out.blob.reserve(len);

    size_t i = 1u;  // skip the leading '/'
    while (true) {
        const uint32_t tok_off = static_cast<uint32_t>(out.blob.size());
        while (i < len && ptr[i] != '/') {
            if (ptr[i] == '~' && i + 1u < len) {
                if      (ptr[i + 1u] == '0') { out.blob += '~'; i += 2u; continue; }
                else if (ptr[i + 1u] == '1') { out.blob += '/'; i += 2u; continue; }
            }
            out.blob += ptr[i];
            ++i;
        }
        const uint32_t tok_len = static_cast<uint32_t>(out.blob.size()) - tok_off;
        out.tokens.push_back(JsonPtrToken{
            tok_off, tok_len,
            jsonptr_parse_index(out.blob.data() + tok_off, tok_len)});
        if (i >= len) break;
        ++i;  // step over the '/' and start the next token
    }
    return out;
}

// A single verbatim key — the top-level-object-key navigation mode
// (`vector_map_access`), where the bytes are used exactly as given: no pointer
// syntax, no escape resolution, never an array index.
static inline JsonPtrPath single_key_path(const char* key, size_t len) {
    JsonPtrPath out;
    out.blob.assign(key, len);
    out.tokens.push_back(JsonPtrToken{0u, static_cast<uint32_t>(len), kJsonPtrNotIndex});
    return out;
}

} // namespace draken::ops
