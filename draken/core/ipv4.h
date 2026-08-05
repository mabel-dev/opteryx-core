#pragma once
// draken/core/ipv4.h — canonical IPv4 <-> uint32 conversions.
//
// An IPv4 column is DRAKEN_UINT32 carrying LogicalKind::IPV4 (see
// draken/logical_type.h). The 32 bits ARE the address: octet A occupies bits
// 31..24, D occupies bits 7..0, so 192.168.1.1 is 0xC0A80101 (3232235777).
// That mapping makes unsigned integer ordering identical to IPv4 address
// ordering, which is why sort/group/join/compare need no IPv4 awareness at all.
//
// This header is the ONE place that mapping is written down. The value
// renderer (interop/value_format.hpp), the cast kernels, and the CIDR
// containment and IP_TRUNC kernels all route through here so a change to
// parsing strictness cannot land in one of them and not the others.
//
// Pure C++, header-only, no Python, no allocation.

#include <cstdint>

namespace draken {
namespace ipv4 {

// Maximum characters in a rendered address: "255.255.255.255".
constexpr uint32_t MAX_TEXT_LENGTH = 15u;

// Maximum characters in a CIDR: "255.255.255.255/32" — the address plus "/nn".
constexpr uint32_t MAX_CIDR_TEXT_LENGTH = MAX_TEXT_LENGTH + 3u;

// ---------------------------------------------------------------------------
// Parse dotted-decimal text -> uint32. Returns true on success.
//
// Deliberately STRICT — exactly four decimal octets, each 0..255, separated by
// single dots, no leading/trailing space, no leading zeros beyond a bare "0",
// no partial forms ("10.1" as shorthand for 10.0.0.1), no trailing junk.
// inet_aton()-style shorthand and octal-by-leading-zero are a documented
// source of security bugs (an ACL and a parser disagreeing on what "010.1"
// means), so this refuses them rather than picking a convention.
//
// The caller decides what a rejection means: the CAST kernel raises, so an
// unparseable address fails loud instead of silently becoming NULL or 0.
// ---------------------------------------------------------------------------
inline bool parse(const uint8_t* text, uint32_t length, uint32_t* out) noexcept {
    uint32_t result = 0u;
    uint32_t i = 0u;
    for (int octet = 0; octet < 4; ++octet) {
        uint32_t value = 0u;
        uint32_t digits = 0u;
        while (i < length) {
            const uint8_t c = text[i];
            if (c < '0' || c > '9') break;
            value = value * 10u + static_cast<uint32_t>(c - '0');
            ++digits;
            ++i;
            if (value > 255u) return false;   // bail before overflow, not after
        }
        if (digits == 0u || digits > 3u) return false;
        // Reject leading zeros ("01", "000") — see the octal note above. A
        // single "0" is the only zero-led form allowed.
        if (digits > 1u && text[i - digits] == '0') return false;
        result = (result << 8) | value;
        if (octet < 3) {
            if (i >= length || text[i] != '.') return false;
            ++i;
        }
    }
    if (i != length) return false;            // trailing junk
    *out = result;
    return true;
}

// ---------------------------------------------------------------------------
// Exact rendered length of `value`, WITHOUT rendering it.
//
// Must agree with format() for every input — it exists so a caller that needs
// to size a buffer before filling it (the VARCHAR cast arena) does not have to
// render every value twice. Kept adjacent to format() for the same reason the
// parse/render pair lives together: the two must move as one. The three dots
// are unconditional; each octet is 1, 2 or 3 digits with no leading zeros, so
// its width is a pair of threshold tests rather than a divide chain.
// ---------------------------------------------------------------------------
inline uint32_t text_length(uint32_t value) noexcept {
    uint32_t total = 3u;   // "a.b.c.d" — three separators, always
    for (int shift = 24; shift >= 0; shift -= 8) {
        const uint32_t octet = (value >> shift) & 0xFFu;
        total += 1u + (octet >= 10u ? 1u : 0u) + (octet >= 100u ? 1u : 0u);
    }
    return total;
}

// ---------------------------------------------------------------------------
// Render uint32 -> dotted-decimal. Writes at most MAX_TEXT_LENGTH bytes to
// `dst` (no null terminator) and returns the number written.
//
// Branch-light per octet: the two- and three-digit cases share a two-digit
// lookup so the common case is a couple of table loads rather than a divide
// chain. `dst` must have room for MAX_TEXT_LENGTH bytes.
// ---------------------------------------------------------------------------
inline uint32_t format(uint32_t value, char* dst) noexcept {
    static constexpr char kTwoDigits[201] =
        "0001020304050607080910111213141516171819"
        "2021222324252627282930313233343536373839"
        "4041424344454647484950515253545556575859"
        "6061626364656667686970717273747576777879"
        "8081828384858687888990919293949596979899";
    char* p = dst;
    for (int shift = 24; shift >= 0; shift -= 8) {
        const uint32_t octet = (value >> shift) & 0xFFu;
        if (octet >= 100u) {
            *p++ = static_cast<char>('0' + octet / 100u);
            const uint32_t rest = octet % 100u;
            *p++ = kTwoDigits[2u * rest];
            *p++ = kTwoDigits[2u * rest + 1u];
        } else if (octet >= 10u) {
            *p++ = kTwoDigits[2u * octet];
            *p++ = kTwoDigits[2u * octet + 1u];
        } else {
            *p++ = static_cast<char>('0' + octet);
        }
        if (shift > 0) *p++ = '.';
    }
    return static_cast<uint32_t>(p - dst);
}

// ---------------------------------------------------------------------------
// Netmask for a prefix length (0..32). Caller must have validated the range.
//
// prefix == 0 is special-cased because `0xFFFFFFFFu << 32` is undefined
// behaviour on a 32-bit type — on x86 the shift count is taken mod 32, so the
// UB silently yields 0xFFFFFFFF (the /32 mask) and a /0 predicate that should
// match everything would match almost nothing.
// ---------------------------------------------------------------------------
inline uint32_t netmask(uint32_t prefix) noexcept {
    return prefix == 0u ? 0u : (0xFFFFFFFFu << (32u - prefix));
}

// ---------------------------------------------------------------------------
// Last (highest) address of the network `base`/`prefix`. `base` must already be
// masked — parse_cidr returns it that way.
//
// Because the 32 bits ARE the address, a network is exactly the closed unsigned
// interval [base, broadcast]: `(ip & netmask) == base` and `base <= ip <=
// broadcast` select the same set. That equivalence is what lets the planner
// rewrite a literal-CIDR containment into a range predicate that storage can
// prune on, so the two forms must derive their bounds from this one function.
// ---------------------------------------------------------------------------
inline uint32_t broadcast(uint32_t base, uint32_t prefix) noexcept {
    return base | ~netmask(prefix);
}

// ---------------------------------------------------------------------------
// Parse "A.B.C.D/prefix" -> base address (already masked) + prefix length.
// Returns true on success. A bare address with no '/' is rejected: silently
// treating it as /32 would make a typo'd CIDR quietly become a single-host
// match instead of an error.
// ---------------------------------------------------------------------------
inline bool parse_cidr(const uint8_t* text, uint32_t length,
                       uint32_t* out_base, uint32_t* out_prefix) noexcept {
    uint32_t slash = 0u;
    while (slash < length && text[slash] != '/') ++slash;
    if (slash == length) return false;                  // no '/'

    uint32_t address = 0u;
    if (!parse(text, slash, &address)) return false;

    const uint32_t digits = length - slash - 1u;
    if (digits == 0u || digits > 2u) return false;      // "" or ">99"
    uint32_t prefix = 0u;
    for (uint32_t k = slash + 1u; k < length; ++k) {
        const uint8_t c = text[k];
        if (c < '0' || c > '9') return false;
        prefix = prefix * 10u + static_cast<uint32_t>(c - '0');
    }
    if (prefix > 32u) return false;

    *out_base = address & netmask(prefix);
    *out_prefix = prefix;
    return true;
}

}  // namespace ipv4
}  // namespace draken
