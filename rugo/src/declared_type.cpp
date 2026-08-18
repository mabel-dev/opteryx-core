// rugo/src/declared_type.cpp — see declared_type.hpp for the contract.
//
// The grammar mirrors opteryx/types/logical_type.py::try_parse_column_type. Read
// the two together when changing either.

#include "declared_type.hpp"

#include <cstdlib>

namespace rugo {

namespace {

inline bool is_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

std::string trim_upper(const std::string& s) {
    size_t a = 0;
    size_t b = s.size();
    while (a < b && is_space(s[a])) ++a;
    while (b > a && is_space(s[b - 1])) --b;
    std::string out;
    out.reserve(b - a);
    for (size_t i = a; i < b; ++i) {
        const char c = s[i];
        out.push_back((c >= 'a' && c <= 'z') ? static_cast<char>(c - 32) : c);
    }
    return out;
}

// Non-negative integer from an already-trimmed, all-digit string. Returns -1 on
// anything else — so "DECIMAL(x, 2)" is a rejected type name rather than a
// silently-zero precision.
int parse_uint_param(const std::string& s) {
    if (s.empty() || s.size() > 3) return -1;
    int v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return -1;
        v = v * 10 + (c - '0');
    }
    return v;
}

// Plain (unparameterized) canonical names. This is the inverse of draken's
// type_display_name_parts for every tag whose name carries no parameters, and it
// is the same table opteryx's _NAME_OF holds — note DATE32's canonical name is
// "DATE", not "DATE32".
bool plain_name(const std::string& u, DrakenType* out) {
    if (u == "INT8")      { *out = DRAKEN_INT8;      return true; }
    if (u == "INT16")     { *out = DRAKEN_INT16;     return true; }
    if (u == "INT32")     { *out = DRAKEN_INT32;     return true; }
    if (u == "INT64")     { *out = DRAKEN_INT64;     return true; }
    if (u == "UINT8")     { *out = DRAKEN_UINT8;     return true; }
    if (u == "UINT16")    { *out = DRAKEN_UINT16;    return true; }
    if (u == "UINT32")    { *out = DRAKEN_UINT32;    return true; }
    if (u == "UINT64")    { *out = DRAKEN_UINT64;    return true; }
    if (u == "FLOAT32")   { *out = DRAKEN_FLOAT32;   return true; }
    if (u == "FLOAT64")   { *out = DRAKEN_FLOAT64;   return true; }
    if (u == "BOOL")      { *out = DRAKEN_BOOL;      return true; }
    if (u == "DATE")      { *out = DRAKEN_DATE32;    return true; }
    if (u == "VARCHAR")   { *out = DRAKEN_VARCHAR;   return true; }
    return false;
}

// SQL-spelling aliases. Deliberately the SAME set opteryx accepts on the read
// side (_SQL_NAME_ALIASES), minus the ones whose target this reader cannot
// produce. Widening this table widens what a stored schema may say, so it tracks
// opteryx's rather than growing its own entries.
//
// It is also what keeps the four ORIGINAL rugo names working: "double" ->
// FLOAT64, "boolean" -> BOOL and "string" -> VARCHAR land here, and "int64" is
// canonical already.
bool alias_name(const std::string& u, DrakenType* out) {
    if (u == "INTEGER" || u == "INT" || u == "BIGINT") { *out = DRAKEN_INT64;   return true; }
    if (u == "TINYINT")                                { *out = DRAKEN_INT8;    return true; }
    if (u == "SMALLINT")                               { *out = DRAKEN_INT16;   return true; }
    if (u == "DOUBLE" || u == "FLOAT")                 { *out = DRAKEN_FLOAT64; return true; }
    if (u == "REAL")                                   { *out = DRAKEN_FLOAT32; return true; }
    if (u == "STRING" || u == "TEXT")                  { *out = DRAKEN_VARCHAR; return true; }
    if (u == "BOOLEAN")                                { *out = DRAKEN_BOOL;    return true; }
    return false;
}

// TIMESTAMP[unit] / the bare TIMESTAMP. The bare name means MICROSECONDS, which
// is what opteryx's parser does and what every schema persisted before the unit
// was serialized says — re-reading those must not change their meaning.
bool timestamp_unit(const std::string& u, uint8_t* out) {
    if (u == "S")  { *out = 0; return true; }
    if (u == "MS") { *out = 1; return true; }
    if (u == "US") { *out = 2; return true; }
    if (u == "NS") { *out = 3; return true; }
    return false;
}

}  // namespace

bool parse_declared_type(const std::string& name, DeclaredType* out) {
    const std::string u = trim_upper(name);
    if (u.empty()) return false;

    // DECIMAL(p, s) — the tier follows the precision exactly as opteryx's
    // DECIMAL() constructor chooses it: an 18-digit value fits the int64 tier,
    // anything wider needs DECIMAL128.
    if (u.size() > 8 && u.compare(0, 8, "DECIMAL(") == 0 && u.back() == ')') {
        const std::string params = u.substr(8, u.size() - 9);
        const size_t comma = params.find(',');
        if (comma == std::string::npos) return false;
        std::string p_s = params.substr(0, comma);
        std::string s_s = params.substr(comma + 1);
        // trim_upper on the whole string already handled case; params keep spaces
        size_t a = 0, b = p_s.size();
        while (a < b && is_space(p_s[a])) ++a;
        while (b > a && is_space(p_s[b - 1])) --b;
        p_s = p_s.substr(a, b - a);
        a = 0; b = s_s.size();
        while (a < b && is_space(s_s[a])) ++a;
        while (b > a && is_space(s_s[b - 1])) --b;
        s_s = s_s.substr(a, b - a);

        const int p = parse_uint_param(p_s);
        const int s = parse_uint_param(s_s);
        if (p < 1 || p > 38 || s < 0 || s > p) return false;
        out->type = (p > 18) ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL;
        out->logical_kind = LK_DECIMAL;
        out->precision = static_cast<uint8_t>(p);
        out->scale = static_cast<uint8_t>(s);
        return true;
    }

    // TIMESTAMP[unit]
    if (u.size() > 10 && u.compare(0, 10, "TIMESTAMP[") == 0 && u.back() == ']') {
        uint8_t unit = 2;
        if (!timestamp_unit(u.substr(10, u.size() - 11), &unit)) return false;
        out->type = DRAKEN_TIMESTAMP64;
        out->logical_kind = LK_TIMESTAMP;
        out->unit = unit;
        return true;
    }
    if (u == "TIMESTAMP") {
        out->type = DRAKEN_TIMESTAMP64;
        out->logical_kind = LK_TIMESTAMP;
        out->unit = 2;   // microseconds — the canonical default
        return true;
    }

    // Canonical, NOT an alias: IPV4 cannot go through the plain-name table,
    // which maps UINT32 to the name "UINT32". Resolving it there would produce a
    // bare unsigned column and drop the descriptor — the exact defect that made
    // a declared-IPV4 column store an INT64 in the first place.
    if (u == "IPV4") {
        out->type = DRAKEN_UINT32;
        out->logical_kind = LK_IPV4;
        return true;
    }

    DrakenType phys;
    if (plain_name(u, &phys) || alias_name(u, &phys)) {
        out->type = phys;
        out->logical_kind = LK_NONE;
        return true;
    }
    return false;
}

const char* declared_type_vocabulary() {
    return "INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64, FLOAT32, "
           "FLOAT64, BOOL, VARCHAR, DATE, TIMESTAMP[s|ms|us|ns], DECIMAL(p, s), "
           "IPV4 (aliases: INTEGER/INT/BIGINT, TINYINT, SMALLINT, DOUBLE/FLOAT, "
           "REAL, STRING/TEXT, BOOLEAN)";
}

}  // namespace rugo
