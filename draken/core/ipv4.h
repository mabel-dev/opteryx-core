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
#include <cstring>

#if defined(__aarch64__)
#include <arm_neon.h>
#define DRAKEN_IPV4_NEON 1
#elif defined(__SSE2__) || defined(_M_X64)
#include <emmintrin.h>
#define DRAKEN_IPV4_SSE2 1
#endif

// Whether a 16-byte load may be taken straight off the caller's bytes. The
// loads below never cross a 4 KiB page boundary (see parse), so they cannot
// fault — but ASan does not model pages, it models allocations, and reading
// past a heap string into its redzone is exactly what it exists to report. So
// under a sanitizer the same code takes the zero-padded copy instead: SAME
// answers, one memcpy slower, and a clean sanitizer run.
#if defined(__SANITIZE_ADDRESS__)
#define DRAKEN_IPV4_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(memory_sanitizer)
#define DRAKEN_IPV4_SANITIZED 1
#endif
#endif

namespace draken {
namespace ipv4 {

// Maximum characters in a rendered address: "255.255.255.255".
constexpr uint32_t MAX_TEXT_LENGTH = 15u;

// Minimum characters in a rendered address: "0.0.0.0".
constexpr uint32_t MIN_TEXT_LENGTH = 7u;

// Maximum characters in a CIDR: "255.255.255.255/32" — the address plus "/nn".
constexpr uint32_t MAX_CIDR_TEXT_LENGTH = MAX_TEXT_LENGTH + 3u;

// Bytes format() must be allowed to TOUCH — which is more than the bytes it
// writes. It renders each octet with one 4-byte store (three digits plus the
// separator, whatever the octet's real width), so rendering the 15-byte
// "255.255.255.255" stores through byte 15 even though it reports 15. A caller
// that hands format() a 15-byte buffer would take a one-byte overrun; buffers
// are sized by this constant, never by MAX_TEXT_LENGTH.
constexpr uint32_t FORMAT_SCRATCH_BYTES = 16u;

namespace detail {

// Decimal text of every octet value, followed by '.', padded to four bytes:
// {'7','.',0,0} / {'4','2','.',0} / {'2','5','5','.'}. One 4-byte store then
// renders an octet AND its separator with no divide, no remainder and no
// width-dependent branch — the width only advances the cursor.
struct OctetQuad { char b[4]; };

struct OctetTables {
    OctetQuad quad[256];
    uint8_t   width[256];
};

inline constexpr OctetTables kOctets = [] {
    OctetTables t{};
    for (int v = 0; v < 256; ++v) {
        if (v >= 100) {
            t.quad[v].b[0] = static_cast<char>('0' + v / 100);
            t.quad[v].b[1] = static_cast<char>('0' + (v / 10) % 10);
            t.quad[v].b[2] = static_cast<char>('0' + v % 10);
            t.quad[v].b[3] = '.';
            t.width[v] = 3u;
        } else if (v >= 10) {
            t.quad[v].b[0] = static_cast<char>('0' + v / 10);
            t.quad[v].b[1] = static_cast<char>('0' + v % 10);
            t.quad[v].b[2] = '.';
            t.width[v] = 2u;
        } else {
            t.quad[v].b[0] = static_cast<char>('0' + v);
            t.quad[v].b[1] = '.';
            t.width[v] = 1u;
        }
    }
    return t;
}();

// Digit weights indexed by the octet's digit COUNT, so a 1-, 2- or 3-digit
// octet is the same three multiply-adds with two of the weights zeroed. The
// bytes a shorter octet does not own are read anyway (they are the separator or
// the next octet) and multiplied by zero — that is why this is branchless.
inline constexpr uint32_t kWeight0[4] = {0u, 1u, 10u, 100u};
inline constexpr uint32_t kWeight1[4] = {0u, 0u,  1u,  10u};
inline constexpr uint32_t kWeight2[4] = {0u, 0u,  0u,   1u};

// Shared tail of the vector paths: `b` holds the text (at least 16 readable
// bytes), `length` its true length, p1/p2/p3 the three dot positions. The
// character set and the dot COUNT have already been checked by the caller, so
// what is left is per-octet: width 1..3, no leading zero, value <= 255.
inline bool octets_to_u32(const uint8_t* b, uint32_t length,
                          uint32_t p1, uint32_t p2, uint32_t p3,
                          uint32_t* out) noexcept {
    const uint32_t l0 = p1;
    const uint32_t l1 = p2 - p1 - 1u;
    const uint32_t l2 = p3 - p2 - 1u;
    const uint32_t l3 = length - p3 - 1u;
    // Each width must be 1..3. Subtracting one turns a zero width (two adjacent
    // dots, a leading dot, a trailing dot) into a huge unsigned, so one unsigned
    // compare per octet covers both ends; the results are OR'd rather than
    // branched on. Validating the widths FIRST is what bounds the last octet's
    // start at byte 12 — that is the invariant the 4-byte reads below rely on to
    // stay inside the sixteen readable bytes.
    if (((l0 - 1u > 2u) | (l1 - 1u > 2u) | (l2 - 1u > 2u) | (l3 - 1u > 2u))) return false;

    const uint32_t start[4] = {0u, p1 + 1u, p2 + 1u, p3 + 1u};
    const uint32_t width[4] = {l0, l1, l2, l3};

    uint32_t result = 0u;
    uint32_t bad = 0u;
    for (int k = 0; k < 4; ++k) {
        const uint32_t w = width[k];
        uint32_t chunk;
        std::memcpy(&chunk, b + start[k], 4);
        const uint32_t d0 = ((chunk      ) & 0xFFu) - '0';
        const uint32_t d1 = ((chunk >>  8) & 0xFFu) - '0';
        const uint32_t d2 = ((chunk >> 16) & 0xFFu) - '0';
        const uint32_t value = d0 * kWeight0[w] + d1 * kWeight1[w] + d2 * kWeight2[w];
        bad |= static_cast<uint32_t>(value > 255u);
        // Reject leading zeros ("01", "000") — see the octal note on parse().
        bad |= static_cast<uint32_t>(w > 1u) & static_cast<uint32_t>(d0 == 0u);
        result = (result << 8) | (value & 0xFFu);
    }
    if (bad) return false;
    *out = result;
    return true;
}

// Scalar four-octet parse — the portable path, and the definition of correct
// that the vector paths are differential-tested against. Unrolled to three
// explicit digit slots rather than a digit loop: an octet is 1..3 digits, so
// the loop only ever bought an unpredictable branch per character.
inline bool parse_scalar(const uint8_t* t, uint32_t length, uint32_t* out) noexcept {
    uint32_t result = 0u;
    uint32_t i = 0u;
    for (int octet = 0; octet < 4; ++octet) {
        const uint32_t d0 = static_cast<uint32_t>(t[i]) - '0';
        if (d0 > 9u) return false;
        uint32_t value = d0;
        uint32_t digits = 1u;
        if (i + 1u < length) {
            const uint32_t d1 = static_cast<uint32_t>(t[i + 1u]) - '0';
            if (d1 <= 9u) {
                if (d0 == 0u) return false;               // leading zero
                value = value * 10u + d1;
                digits = 2u;
                if (i + 2u < length) {
                    const uint32_t d2 = static_cast<uint32_t>(t[i + 2u]) - '0';
                    if (d2 <= 9u) {
                        value = value * 10u + d2;
                        digits = 3u;
                        if (value > 255u) return false;
                        // a fourth digit is an overlong octet, not a separator
                        if (i + 3u < length
                            && static_cast<uint32_t>(t[i + 3u]) - '0' <= 9u) return false;
                    }
                }
            }
        }
        i += digits;
        result = (result << 8) | value;
        if (octet < 3) {
            if (i >= length || t[i] != '.') return false;
            ++i;
        }
    }
    if (i != length) return false;            // trailing junk
    *out = result;
    return true;
}

}  // namespace detail

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
//
// SHAPE OF THE FAST PATH: every accepted address is 7..15 bytes, so one 16-byte
// vector covers the whole input. The vector classifies all sixteen bytes as
// digit / dot / neither at once and yields the three dot positions; the octets
// then convert branchlessly (detail::octets_to_u32). The scalar path is the
// same predicate written the slow way and stays the reference implementation —
// the two must agree byte for byte on EVERY input, accepted or rejected, which
// is what draken/tests/native/test_ipv4_exhaustive.cpp pins.
// ---------------------------------------------------------------------------
inline bool parse(const uint8_t* text, uint32_t length, uint32_t* out) noexcept {
    // Outside [7, 15] nothing can parse, and the vector load below is sized for
    // 15. Unsigned wrap folds both bounds into one compare.
    if (length - MIN_TEXT_LENGTH > MAX_TEXT_LENGTH - MIN_TEXT_LENGTH) return false;

#if defined(DRAKEN_IPV4_NEON) || defined(DRAKEN_IPV4_SSE2)
    // A 16-byte load that starts inside a page cannot fault as long as it does
    // not reach the next one, so the bytes are read where they lie and the
    // zero-padded copy is paid only by the roughly 1-in-256 string that starts
    // within 16 bytes of a page end. Bytes past `length` ARE read but never
    // trusted: the masks below drop them, and octets_to_u32 only reads what the
    // validated octet widths cover.
    uint8_t pad[16];
    const uint8_t* b = text;
#if defined(DRAKEN_IPV4_SANITIZED)
    const bool copy = true;     // ASan sees allocations, not pages — see above
#else
    const bool copy = (reinterpret_cast<uintptr_t>(text) & 4095u) > 4096u - 16u;
#endif
    if (copy) {
        std::memset(pad, 0, sizeof(pad));
        std::memcpy(pad, text, length);
        b = pad;
    }
#else
    // No vector unit: the reference implementation IS the implementation, and it
    // reads only the bytes it was given, so there is nothing to pad.
    return detail::parse_scalar(text, length, out);
#endif

#if defined(DRAKEN_IPV4_NEON)
    const uint8x16_t v = vld1q_u8(b);
    const uint8x16_t is_digit = vcleq_u8(vsubq_u8(v, vdupq_n_u8('0')), vdupq_n_u8(9));
    const uint8x16_t is_dot   = vceqq_u8(v, vdupq_n_u8('.'));
    // vshrn by 4 packs the 16 lane predicates into 16 NIBBLES of one uint64 —
    // the AArch64 stand-in for a movemask, four bits per byte rather than one.
    const uint64_t ok_bits  = vget_lane_u64(vreinterpret_u64_u8(
        vshrn_n_u16(vreinterpretq_u16_u8(vorrq_u8(is_digit, is_dot)), 4)), 0);
    const uint64_t dot_bits = vget_lane_u64(vreinterpret_u64_u8(
        vshrn_n_u16(vreinterpretq_u16_u8(is_dot), 4)), 0);
    const uint64_t live = (1ULL << (4u * length)) - 1ULL;   // length <= 15, so no UB
    if ((ok_bits & live) != live) return false;            // a non-address byte
    const uint64_t dots = dot_bits & live;
    if (__builtin_popcountll(dots) != 12) return false;    // exactly three dots
    const uint32_t p1 = static_cast<uint32_t>(__builtin_ctzll(dots)) >> 2;
    const uint64_t r2 = dots & ~(0xFULL << (p1 << 2));
    const uint32_t p2 = static_cast<uint32_t>(__builtin_ctzll(r2)) >> 2;
    const uint64_t r3 = r2 & ~(0xFULL << (p2 << 2));
    const uint32_t p3 = static_cast<uint32_t>(__builtin_ctzll(r3)) >> 2;
    return detail::octets_to_u32(b, length, p1, p2, p3, out);

#elif defined(DRAKEN_IPV4_SSE2)
    const __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(b));
    const __m128i flip = _mm_set1_epi8(static_cast<char>(0x80));
    const __m128i off  = _mm_sub_epi8(v, _mm_set1_epi8('0'));
    // SSE2 has no unsigned byte compare; biasing both sides by 0x80 turns the
    // signed compare into the unsigned one, which is what (c - '0') <= 9 needs.
    const __m128i is_digit = _mm_cmplt_epi8(_mm_xor_si128(off, flip),
                                            _mm_xor_si128(_mm_set1_epi8(10), flip));
    const __m128i is_dot   = _mm_cmpeq_epi8(v, _mm_set1_epi8('.'));
    const uint32_t ok_bits  = static_cast<uint32_t>(
        _mm_movemask_epi8(_mm_or_si128(is_digit, is_dot)));
    const uint32_t dot_bits = static_cast<uint32_t>(_mm_movemask_epi8(is_dot));
    const uint32_t live = (1u << length) - 1u;             // length <= 15
    if ((ok_bits & live) != live) return false;            // a non-address byte
    const uint32_t dots = dot_bits & live;
    if (__builtin_popcount(dots) != 3) return false;       // exactly three dots
    const uint32_t p1 = static_cast<uint32_t>(__builtin_ctz(dots));
    const uint32_t r2 = dots & (dots - 1u);
    const uint32_t p2 = static_cast<uint32_t>(__builtin_ctz(r2));
    const uint32_t p3 = static_cast<uint32_t>(__builtin_ctz(r2 & (r2 - 1u)));
    return detail::octets_to_u32(b, length, p1, p2, p3, out);
#endif
}

// ---------------------------------------------------------------------------
// Exact rendered length of `value`, WITHOUT rendering it.
//
// Must agree with format() for every input — it exists so a caller that needs
// to size a buffer before filling it (the VARCHAR cast arena) does not have to
// render every value twice. Kept adjacent to format() for the same reason the
// parse/render pair lives together: the two must move as one.
//
// The width rule is written TWICE — as two threshold tests here, as a table
// lookup in format() — because the two spellings win on different targets
// (measured 2026-08-22: on x86-64/gcc the thresholds are 0.73 ns and the table
// 2.02 ns here, while inside format() the ranking inverts). Two spellings of one
// rule is a drift risk, so it is pinned by an exhaustive test over all 2^32
// values (draken/tests/native/test_ipv4_exhaustive.cpp), not by inspection.
// ---------------------------------------------------------------------------
inline uint32_t octet_width(uint32_t octet) noexcept {
    return 1u + (octet >= 10u ? 1u : 0u) + (octet >= 100u ? 1u : 0u);
}

inline uint32_t text_length(uint32_t value) noexcept {
    return 3u + octet_width((value >> 24) & 0xFFu)
              + octet_width((value >> 16) & 0xFFu)
              + octet_width((value >>  8) & 0xFFu)
              + octet_width( value        & 0xFFu);
}

// ---------------------------------------------------------------------------
// Render uint32 -> dotted-decimal. Returns the number of bytes written (no null
// terminator), which is at most MAX_TEXT_LENGTH.
//
// ⛔ `dst` must have FORMAT_SCRATCH_BYTES (16) bytes of room, NOT
// MAX_TEXT_LENGTH. Each octet is one 4-byte store of "ddd." from a table, and
// the cursor then advances by the octet's real width — so the last octet of
// "255.255.255.255" stores through byte 15 and the return value is still 15.
// Sizing a buffer by MAX_TEXT_LENGTH is a one-byte stack overrun.
// ---------------------------------------------------------------------------
inline uint32_t format(uint32_t value, char* dst) noexcept {
    const uint32_t o0 = (value >> 24) & 0xFFu;
    const uint32_t o1 = (value >> 16) & 0xFFu;
    const uint32_t o2 = (value >>  8) & 0xFFu;
    const uint32_t o3 =  value        & 0xFFu;
    char* p = dst;
    std::memcpy(p, detail::kOctets.quad[o0].b, 4); p += detail::kOctets.width[o0] + 1u;
    std::memcpy(p, detail::kOctets.quad[o1].b, 4); p += detail::kOctets.width[o1] + 1u;
    std::memcpy(p, detail::kOctets.quad[o2].b, 4); p += detail::kOctets.width[o2] + 1u;
    std::memcpy(p, detail::kOctets.quad[o3].b, 4); p += detail::kOctets.width[o3];
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
    if (length > MAX_CIDR_TEXT_LENGTH) return false;
    // The address half is bounded, so the separator can only sit in a short
    // window — but scanning the whole (already capped) string is the same cost
    // and keeps the failure "no '/'" rather than "'/' in the wrong place".
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
