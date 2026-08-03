#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/result_helpers.h"
#include "core/ipv4.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "ops/int64_compare.h"   // cmp_alloc_bool_buf
#include <cstring>

/**
 * Binary bitwise and string concatenation operations for Phase 9a.
 *
 * Bitwise operations work on INTEGER types only.
 * String concatenation coerces both operands to VARCHAR and concatenates.
 */

extern "C" {

// Phase 9a: Bitwise and string operations are stubbed; implementations deferred to 9f

VecResult draken_bitwise_or(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise operations not yet implemented");
    });
}

VecResult draken_bitwise_and(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise operations not yet implemented");
    });
}

VecResult draken_bitwise_xor(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise operations not yet implemented");
    });
}

VecResult draken_bitwise_shift_left(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise shift operations not yet implemented");
    });
}

VecResult draken_bitwise_shift_right(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise shift operations not yet implemented");
    });
}

VecResult draken_string_concat(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("String concatenation not yet implemented");
    });
}

// IPv4 CIDR containment over a UINT32 address column, backing `<<=` and `>>=`.
//
// ORDER-AGNOSTIC by design: `addr <<= cidr` and `cidr >>= addr` are the same
// predicate with the operands swapped, and the operand TYPES are unambiguous
// (one side is UINT32, the other a string). Discriminating on type rather than
// position means one kernel serves both spellings, so there is no second
// implementation to drift out of step — and no ctx allocation just to carry a
// direction flag.
//
// Per row: one load, one AND, one compare. No text parsing — that is the whole
// point of storing addresses as uint32.
// SIGNATURE: func_fn_t (ctx, args[], nargs), NOT the (ctx, left, right) binary
// shape used by the kernels above it in this file. The executor reaches this via
// BC_FUNCTION | BC_INSTR_C_NATIVE, which calls through func_fn_t; giving it the
// binary shape means the kernel receives the args ARRAY as `left` and the
// integer `nargs` as `right`, and segfaults on the first dereference. The two
// conventions are not interchangeable and nothing at the call site checks.
VecResult draken_ipv4_in_cidr(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2u || args == nullptr)
            return draken_error_sentinel("draken_ipv4_in_cidr: expects exactly 2 operands");
        const DrakenVector* left  = args[0];
        const DrakenVector* right = args[1];
        if (left == nullptr || right == nullptr)
            return draken_error_sentinel("draken_ipv4_in_cidr: NULL operand");

        const DrakenVector* addr;
        const DrakenVector* cidr;
        if (left->type == DRAKEN_UINT32) {
            addr = left;  cidr = right;
        } else if (right->type == DRAKEN_UINT32) {
            addr = right; cidr = left;
        } else {
            return draken_error_sentinel(
                "draken_ipv4_in_cidr: one operand must be a UINT32 (IPv4) column");
        }
        if (cidr->type != DRAKEN_VARCHAR && cidr->type != DRAKEN_NVARCHAR
                && cidr->type != DRAKEN_VARBINARY)
            return draken_error_sentinel(
                "draken_ipv4_in_cidr: the other operand must be a CIDR string");
        if (cidr->length == 0u)
            return draken_error_sentinel("draken_ipv4_in_cidr: CIDR operand is empty");

        // The CIDR is a scalar: only row 0 is read. A constant-shape vector maps
        // every logical row to the same slot, so this is correct for a literal.
        const uint32_t cidr_slot = cidr->selection[0];
        if (cidr->validity != nullptr
                && !((cidr->validity[cidr_slot >> 3] >> (cidr_slot & 7)) & 1u))
            return draken_error_sentinel("draken_ipv4_in_cidr: CIDR operand is NULL");

        const DrakenStringArena* ca =
            static_cast<const DrakenStringArena*>(cidr->data);
        const DrakenStringSlot* cslot = &ca->slots[cidr_slot];

        uint32_t base_ip = 0u;
        uint32_t prefix  = 0u;
        if (!draken::ipv4::parse_cidr(str_data(cslot, ca->arena),
                                      str_length(cslot), &base_ip, &prefix))
            return draken_error_sentinel(
                "draken_ipv4_in_cidr: invalid CIDR (expected A.B.C.D/prefix, prefix 0..32)");
        const uint32_t netmask = draken::ipv4::netmask(prefix);

        const uint32_t n = addr->length;
        uint8_t* dst = draken::ops::cmp_alloc_bool_buf(n);  // zero-initialised
        if (dst == nullptr)
            return draken_error_sentinel("draken_ipv4_in_cidr: bool buffer alloc failed");

        const uint32_t* codes = addr->selection;
        const uint32_t* data  = static_cast<const uint32_t*>(addr->data);
        const uint8_t*  nulls = addr->validity;
        for (uint32_t i = 0u; i < n; ++i) {
            // A NULL address is contained by nothing → false, not NULL. Matches
            // the behaviour the string-based predicate has always had.
            if (nulls != nullptr && !((nulls[i >> 3] >> (i & 7)) & 1u)) continue;
            if ((data[codes[i]] & netmask) == base_ip)
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }

        VecResult r;
        r.data           = dst;
        r.validity       = nullptr;
        r.selection      = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.type           = DRAKEN_BOOL;
        r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
        return r;
    });
}


// IP_TRUNC(ip, prefix) — apply a network mask, i.e. the NETWORK ADDRESS of `ip`
// within a /prefix network. `IP_TRUNC('192.168.1.1', 24)` -> `192.168.1.0`.
//
// The operation is AND-with-netmask, NOT xor and not a shift. Named after
// BigQuery's NET.IP_TRUNC, which is the only established convention that takes
// the prefix as an ARGUMENT — our addresses carry no prefix length of their own
// (unlike a Postgres `inet`), so Postgres's network()/masklen() signatures do
// not port. "MASK" was rejected: it is ambiguous between the mask VALUE
// (255.255.255.0) and applying it, and collides with data-redaction MASK().
//
// func_fn_t shape (ctx, args[], nargs) — see draken_ipv4_in_cidr above for why
// that matters. Result is UINT32 with no descriptor; the IPV4 descriptor is
// re-attached from the bound output type by the projection.
VecResult draken_ip_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2u || args == nullptr)
            return draken_error_sentinel("IP_TRUNC: expects exactly 2 arguments (address, prefix)");
        const DrakenVector* addr = args[0];
        const DrakenVector* pfx  = args[1];
        if (addr == nullptr || pfx == nullptr)
            return draken_error_sentinel("IP_TRUNC: NULL operand");
        if (addr->type != DRAKEN_UINT32)
            return draken_error_sentinel(
                "IP_TRUNC: first argument must be an IPv4 (UINT32) column");
        if (pfx->length == 0u)
            return draken_error_sentinel("IP_TRUNC: prefix argument is empty");

        // The prefix is a scalar read from row 0. Reading it per row would let a
        // single expression mask different rows to different network sizes, which
        // is not what the function means and would silently produce a column whose
        // values are not comparable with each other.
        const uint32_t pslot = pfx->selection[0];
        if (pfx->validity != nullptr && !((pfx->validity[pslot >> 3] >> (pslot & 7)) & 1u))
            return draken_error_sentinel("IP_TRUNC: prefix must not be NULL");
        int64_t prefix_raw;
        switch (pfx->type) {
            case DRAKEN_INT8:   prefix_raw = static_cast<const int8_t*>(pfx->data)[pslot];   break;
            case DRAKEN_INT16:  prefix_raw = static_cast<const int16_t*>(pfx->data)[pslot];  break;
            case DRAKEN_INT32:  prefix_raw = static_cast<const int32_t*>(pfx->data)[pslot];  break;
            case DRAKEN_INT64:  prefix_raw = static_cast<const int64_t*>(pfx->data)[pslot];  break;
            case DRAKEN_UINT8:  prefix_raw = static_cast<const uint8_t*>(pfx->data)[pslot];  break;
            case DRAKEN_UINT16: prefix_raw = static_cast<const uint16_t*>(pfx->data)[pslot]; break;
            case DRAKEN_UINT32: prefix_raw = static_cast<const uint32_t*>(pfx->data)[pslot]; break;
            default:
                return draken_error_sentinel("IP_TRUNC: prefix must be an integer");
        }
        // Range-checked rather than clamped: IP_TRUNC(ip, 33) is a mistake in the
        // query, and silently treating it as /32 would return plausible-looking
        // rows that answer a different question.
        if (prefix_raw < 0 || prefix_raw > 32)
            return draken_error_sentinel_fmt(
                "IP_TRUNC: prefix must be 0..32, got %lld", (long long)prefix_raw);
        const uint32_t netmask =
            draken::ipv4::netmask(static_cast<uint32_t>(prefix_raw));

        // Compression-aware: mask each PHYSICAL slot once and keep the input's
        // shape, so a dictionary-encoded address column does K masks, not N.
        const uint32_t k = addr->data_length;
        uint32_t* out = static_cast<uint32_t*>(
            draken_malloc((k > 0u ? k : 1u) * sizeof(uint32_t)));
        if (!out) return draken_error_sentinel("IP_TRUNC: allocation failed");
        const uint32_t* src = static_cast<const uint32_t*>(addr->data);
        for (uint32_t j = 0u; j < k; ++j) out[j] = src[j] & netmask;

        VecResult r;
        r.data = out; r.type = DRAKEN_UINT32;
        r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        // Masking never introduces or removes nulls, so validity carries over 1:1.
        kernel_preserve_shape(r, addr);
        return r;
    });
}

}  // extern "C"
