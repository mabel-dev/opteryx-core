#include "jsonl_reader.hpp"
#include "volnitsky.h"     // SPIKE: raw prefilter
#include <cstring>

namespace rugo::_jsonl {

PrefilterResult volnitsky_prefilter(
    const uint8_t* buffer, size_t length,
    const uint8_t* needle, size_t needle_len) {
    PrefilterResult r;
    if (length < needle_len || needle_len < 2) return r;
    VolnitskyTable* t = volnitsky_alloc();
    volnitsky_build(t, needle, needle_len);
    r.candidates.reserve(length / 16);

    // Single whole-buffer Volnitsky pass: the bigram table skips ~needle_len-1 bytes across
    // every non-matching window, so a rare needle leaps over whole records. On a hit, copy
    // the enclosing line and jump past it (handles dedup of multiple hits in one record).
    size_t last_end = 0; bool any = false;
    for (size_t p = needle_len - 1; p < length; ) {
        const uint16_t h = (static_cast<uint16_t>(buffer[p - 1]) << 8) | buffer[p];
        const uint16_t k = t->entries[h];
        if (!k) { p += needle_len - 1; continue; }
        const size_t hs = p - k;
        if (hs + needle_len <= length && std::memcmp(buffer + hs, needle, needle_len) == 0
            && (!any || hs >= last_end)) {
            size_t ls = hs; while (ls > 0 && buffer[ls - 1] != '\n') --ls;
            size_t le = hs; while (le < length && buffer[le] != '\n') ++le;
            r.candidates.insert(r.candidates.end(), buffer + ls, buffer + le);
            r.candidates.push_back('\n');
            ++r.matched_records;
            last_end = le; any = true;
            p = (le + 1 > needle_len - 1) ? le + 1 : needle_len - 1;  // skip past the matched line
            continue;
        }
        p += 1;
    }
    volnitsky_free(t);
    return r;
}

}  // namespace rugo::_jsonl

// ---------------------------------------------------------------------------------
// Shared by the Cython read_jsonl edge and the engine's native JSONL scan Source
// (src/cpp/engine/native_jsonl_scan_source.hpp): one implementation of the prefilter
// gate and of the malformed-record message, so the two readers cannot drift.
// ---------------------------------------------------------------------------------

#include "predicate_literal.hpp"   // LITERAL_STRING

namespace rugo::_jsonl {

namespace {

constexpr uint8_t kOpEq = 0;   // parse_context.hpp Predicate::op

// Find `needle` in [hay, hay + n); SIZE_MAX if absent.
size_t find_bytes(const uint8_t* hay, size_t n, const uint8_t* needle, size_t m) {
    if (m == 0) return 0;
    if (m > n) return SIZE_MAX;
    for (size_t i = 0; i + m <= n; ++i) {
        const void* p = std::memchr(hay + i, needle[0], n - m - i + 1);
        if (p == nullptr) return SIZE_MAX;
        i = static_cast<size_t>(static_cast<const uint8_t*>(p) - hay);
        if (std::memcmp(hay + i, needle, m) == 0) return i;
    }
    return SIZE_MAX;
}

// One code point from UTF-8 under Python's errors='replace' (Unicode "maximal subpart"
// practice, which CPython follows): an ill-formed sequence becomes ONE U+FFFD covering the
// bytes consumed before the first byte that cannot continue it, and that byte is re-read.
uint32_t decode_one(const uint8_t* s, size_t n, size_t& i) {
    const uint8_t b0 = s[i];
    if (b0 < 0x80) { ++i; return b0; }
    size_t need;
    uint32_t cp;
    uint8_t lo = 0x80, hi = 0xBF;   // bounds on the SECOND byte
    if (b0 >= 0xC2 && b0 <= 0xDF)      { need = 1; cp = b0 & 0x1F; }
    else if (b0 >= 0xE0 && b0 <= 0xEF) { need = 2; cp = b0 & 0x0F;
        if (b0 == 0xE0) lo = 0xA0; else if (b0 == 0xED) hi = 0x9F; }
    else if (b0 >= 0xF0 && b0 <= 0xF4) { need = 3; cp = b0 & 0x07;
        if (b0 == 0xF0) lo = 0x90; else if (b0 == 0xF4) hi = 0x8F; }
    else { ++i; return 0xFFFD; }
    size_t j = i + 1;
    for (size_t k = 0; k < need; ++k, ++j) {
        if (j >= n) { i = j; return 0xFFFD; }
        const uint8_t b = s[j];
        const uint8_t l = (k == 0) ? lo : 0x80, h = (k == 0) ? hi : 0xBF;
        if (b < l || b > h) { i = j; return 0xFFFD; }
        cp = (cp << 6) | (b & 0x3F);
    }
    i = j;
    return cp;
}

// str.isprintable() for a non-ASCII code point. Python prints every code point outside
// categories Cc/Cf/Cs/Co/Cn/Zl/Zp/Zs; the ranges below are the assigned members of those
// categories that real data carries (C1 controls, the space separators, the format
// characters, private use). An UNASSIGNED code point (Cn) is printed raw here where repr()
// would escape it — the one place this can differ, and only in an error message.
bool printable_non_ascii(uint32_t c) {
    if (c <= 0xA0) return false;                       // C1 controls + NBSP
    if (c == 0xAD) return false;                       // soft hyphen
    if (c >= 0x600 && c <= 0x605) return false;
    if (c == 0x61C || c == 0x6DD || c == 0x70F || c == 0x180E) return false;
    if (c == 0x1680) return false;
    if (c >= 0x2000 && c <= 0x200F) return false;
    if (c >= 0x2028 && c <= 0x202F) return false;
    if (c >= 0x205F && c <= 0x206F) return false;
    if (c == 0x3000) return false;
    if (c >= 0xD800 && c <= 0xF8FF) return false;      // surrogates + private use
    if (c == 0xFEFF) return false;
    if (c >= 0xFFF9 && c <= 0xFFFB) return false;
    if (c == 0xFFFE || c == 0xFFFF) return false;
    if (c >= 0xF0000) return false;                    // supplementary private use
    return true;
}

void append_utf8(std::string& out, uint32_t c) {
    if (c < 0x80) { out.push_back(static_cast<char>(c)); }
    else if (c < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (c >> 6)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    } else if (c < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (c >> 12)));
        out.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (c >> 18)));
        out.push_back(static_cast<char>(0x80 | ((c >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    }
}

void append_hex_escape(std::string& out, uint32_t c) {
    static const char* hex = "0123456789abcdef";
    int digits;
    if (c < 0x100) { out += "\\x"; digits = 2; }
    else if (c < 0x10000) { out += "\\u"; digits = 4; }
    else { out += "\\U"; digits = 8; }
    for (int d = digits - 1; d >= 0; --d) out.push_back(hex[(c >> (4 * d)) & 0xF]);
}

}  // namespace

std::string py_str_repr(const uint8_t* bytes, size_t length) {
    std::vector<uint32_t> cps;
    cps.reserve(length);
    bool has_single = false, has_double = false;
    for (size_t i = 0; i < length;) {
        const uint32_t c = decode_one(bytes, length, i);
        has_single |= (c == '\'');
        has_double |= (c == '"');
        cps.push_back(c);
    }
    const char quote = (has_single && !has_double) ? '"' : '\'';
    std::string out;
    out.reserve(length + 2);
    out.push_back(quote);
    for (const uint32_t c : cps) {
        if (c == static_cast<uint32_t>(quote) || c == '\\') { out.push_back('\\'); out.push_back(static_cast<char>(c)); }
        else if (c == '\t') out += "\\t";
        else if (c == '\n') out += "\\n";
        else if (c == '\r') out += "\\r";
        else if (c < 0x20 || c == 0x7F) append_hex_escape(out, c);
        else if (c < 0x80) out.push_back(static_cast<char>(c));
        else if (printable_non_ascii(c)) append_utf8(out, c);
        else append_hex_escape(out, c);
    }
    out.push_back(quote);
    return out;
}

std::string malformed_error_message(const uint8_t* buffer, size_t length, uint32_t offset) {
    const size_t limit = static_cast<size_t>(offset) < length ? static_cast<size_t>(offset) : length;
    size_t line = 1;
    for (size_t i = 0; i < limit; ++i)
        if (buffer[i] == '\n') ++line;
    size_t snippet_end = offset;
    while (snippet_end < length && buffer[snippet_end] != '\n') ++snippet_end;
    const size_t start = static_cast<size_t>(offset) < length ? static_cast<size_t>(offset) : length;
    size_t snippet_len = snippet_end > start ? snippet_end - start : 0;
    if (snippet_len > 200) snippet_len = 200;
    return "Malformed JSONL at line " + std::to_string(line) + " (byte offset " +
           std::to_string(offset) + "): " + py_str_repr(buffer + start, snippet_len);
}

bool maybe_prefilter(const uint8_t* buffer, size_t length, const ParseContext& context,
                     std::vector<uint8_t>& out) {
    if (context.predicates.size() != 1) return false;
    const Predicate& pred = context.predicates[0];
    // `==` only: IN / NOT IN carry their members in `members`, not one value.
    if (pred.op != kOpEq || !pred.members.empty()) return false;
    // A string literal only: its needle is the quoted value, which only a JSON string
    // carries. A non-string literal is a type mismatch against a string column, and
    // prefiltering on it would drop every record before evaluate_predicate could raise.
    if (pred.kind != LITERAL_STRING) return false;

    // Probe the first record: only prefilter when the column is stored as a quoted
    // (string) value. A bare numeric/bool value isn't quoted, so a quoted needle would
    // false-negative. Bounded to 4KB — real JSONL lines are far shorter than that.
    const size_t first_window = length < 4096 ? length : 4096;
    const void* nl = std::memchr(buffer, '\n', first_window);
    const size_t first_len = nl ? static_cast<size_t>(static_cast<const uint8_t*>(nl) - buffer)
                                : first_window;
    std::string key;
    key.reserve(pred.column.size() + 3);
    key.push_back('"');
    key += pred.column;
    key += "\":";
    const size_t ki = find_bytes(buffer, first_len,
                                 reinterpret_cast<const uint8_t*>(key.data()), key.size());
    if (ki == SIZE_MAX) return false;            // key absent / non-compact formatting
    const size_t vpos = ki + key.size();
    if (vpos >= first_len || buffer[vpos] != '"') return false;   // bare value: numeric hazard

    std::string needle;
    needle.reserve(pred.value.size() + 2);
    needle.push_back('"');
    needle += pred.value;
    needle.push_back('"');
    if (needle.size() < 8) return false;         // short/low-entropy value: won't pay off
    const uint8_t* ndl = reinterpret_cast<const uint8_t*>(needle.data());

    // Selectivity sample on the first ~1MB: if the needle already hits >30% of sampled
    // rows there is little to skip — run the normal path instead of a full prefilter.
    const size_t sample_len = length < 1000000 ? length : 1000000;
    const PrefilterResult sr = volnitsky_prefilter(buffer, sample_len, ndl, needle.size());
    size_t sample_lines = 0;
    for (size_t i = 0; i < sample_len; ++i) sample_lines += (buffer[i] == '\n');
    if (sample_lines > 0 && sr.matched_records * 10 > sample_lines * 3) return false;

    PrefilterResult r = volnitsky_prefilter(buffer, length, ndl, needle.size());
    out = std::move(r.candidates);
    return true;
}

}  // namespace rugo::_jsonl
