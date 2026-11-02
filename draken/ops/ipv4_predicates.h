#pragma once
// draken/ops/ipv4_predicates.h — IPv4 CIDR containment over a UINT32 column.
//
//   ipv4_in_cidr(addr, netmask, base_ip, dst) → packed DRAKEN_BOOL bitmap in dst,
//                                               returns the result validity
//
// The predicate is `(address & netmask) == base`: one AND and one compare per
// row, no text parsing. That is the whole reason addresses are stored as uint32
// (see core/ipv4.h for the bit order), and it is why sort/group/join need no
// IPv4 awareness at all.
//
// CALLER
//   The registered C-ABI kernel (ops/kernels/binary_op_other.cpp). It was a header
//   so a nanobind twin for the Python evaluator could share it; that twin is gone
//   with the evaluator.
//
// SIMD / §11 NOTE
//   `Identity` is a COMPILE-TIME parameter, not a runtime branch, and it selects
//   exactly one thing: whether a row reads `data[i]` or `data[selection[i]]`.
//   Everything downstream of the accessor — the compare, the packing, the null
//   handling — is a single shared body, so a dense vector and a dictionary
//   vector cannot produce different answers; there is no second implementation
//   to drift. This is the same shape specialisation int64_compare.h uses, and it
//   is here for the same reason: the gather through `selection` is what blocks
//   vectorisation, so hoisting it out of the dense case is the whole win.
//
//   Specialising on encoding shape is an architect-approved exception to §11's
//   default (agreed 2026-08-04, alongside making literal-CIDR predicates prune
//   at the scan). It is NOT a precedent for shape dispatch elsewhere.
//
//   The 8-way unrolled byte-pack removes the read-modify-write dependency
//   between adjacent rows, which is what lets the compiler auto-vectorise to
//   NEON (ARM dev) and AVX2 (x86 prod) without target-specific intrinsics —
//   RISC-V gets the same treatment for free.
//
// NULL SEMANTICS
//   A NULL address gives NULL (three-valued logic, like every other comparison):
//   the result validity is a copy of the address's. FALSE here was a wrong answer
//   in a projection (`CAST(NULL AS IPV4) <<= '10.0.0.0/8'` read FALSE), and made
//   NOT(addr <<= net) select NULL addresses. The data bit of a null row is also
//   cleared (branchlessly, ANDed with the validity byte) so it is canonical 0.
//   `validity` is indexed by LOGICAL row in both shapes (buffers.h: 1 bit per
//   logical row), so the same AND and copy are correct for a dictionary vector.
//
// BIT-BOUNDARY CORRECTNESS
//   `dst` must be zero-initialised by the caller (cmp_alloc_bool_buf does this).
//   Whole bytes are assigned; the ragged tail accumulates via OR only, so it
//   starts at 0 and nothing past ceil(n/8) is ever written.

#include <cstdint>

#include "core/buffers.h"
#include "core/ipv4.h"
#include "ops/int64_compare.h"  // cmp_copy_validity

namespace draken {
namespace ops {

// Core loop. `Identity` true  → dense: row i reads data[i] (vectorisable).
//            `Identity` false → gather: row i reads data[selection[i]].
template <bool Identity>
static inline void ipv4_in_cidr_kernel(
    const uint32_t* data,
    const uint32_t* selection,
    uint32_t        netmask,
    uint32_t        base_ip,
    const uint8_t*  src_null,
    uint8_t*        dst,
    uint32_t        n) noexcept
{
    const uint32_t whole_bytes = n >> 3;

    auto hit = [&](uint32_t pos) -> unsigned {
        uint32_t value;
        if constexpr (Identity) value = data[pos];
        else                    value = data[selection[pos]];
        return static_cast<unsigned>((value & netmask) == base_ip);
    };

    auto pack = [&](uint32_t base) -> uint8_t {
        return static_cast<uint8_t>(
            (hit(base + 0u) << 0) | (hit(base + 1u) << 1) |
            (hit(base + 2u) << 2) | (hit(base + 3u) << 3) |
            (hit(base + 4u) << 4) | (hit(base + 5u) << 5) |
            (hit(base + 6u) << 6) | (hit(base + 7u) << 7));
    };

    if (src_null == nullptr) {
        for (uint32_t b = 0u; b < whole_bytes; ++b) dst[b] = pack(b << 3);
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (hit(i)) dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        // Branchless null handling — see NULL SEMANTICS above.
        for (uint32_t b = 0u; b < whole_bytes; ++b)
            dst[b] = static_cast<uint8_t>(pack(b << 3) & src_null[b]);
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if (((src_null[i >> 3] >> (i & 7)) & 1u) && hit(i))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
}

// Shape dispatch. `dst` must hold ceil(n/8) zeroed bytes. Returns the result
// validity (owned by the caller, nullptr = all valid).
//
// The dense claim is checked, not assumed: DRAKEN_SEL_IDENTITY is a layout HINT
// ("0 = don't know", buffers.h), so a missing flag costs a gather and never a
// wrong answer. `data_length >= length` is verified too — the identity path
// reads data[0 .. n-1] directly, and a vector that claimed identity while
// carrying a shorter data buffer would read out of bounds rather than merely
// answer slowly.
inline uint8_t* ipv4_in_cidr(const DrakenVector* addr,
                             uint32_t            netmask,
                             uint32_t            base_ip,
                             uint8_t*            dst)
{
    const uint32_t  n    = addr->length;
    const uint32_t* data = static_cast<const uint32_t*>(addr->data);

    const bool dense = ((addr->flags & DRAKEN_SEL_IDENTITY) != 0u)
                    && (addr->data_length >= n);

    if (dense)
        ipv4_in_cidr_kernel<true>(data, nullptr, netmask, base_ip,
                                  addr->validity, dst, n);
    else
        ipv4_in_cidr_kernel<false>(data, addr->selection, netmask, base_ip,
                                   addr->validity, dst, n);
    return addr->validity != nullptr ? cmp_copy_validity(addr->validity, n) : nullptr;
}

}  // namespace ops
}  // namespace draken
