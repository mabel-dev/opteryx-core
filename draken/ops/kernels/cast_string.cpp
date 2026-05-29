#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/result_helpers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include <cstring>

/**
 * Cast kernels: string → numeric/bool (Phase 9c).
 *
 * Compute extracted from opteryx/compiled/nanobind/vector_casts.cpp. Parse
 * failures return an error sentinel with a descriptive message; the nanobind
 * shim re-raises it as ValueError to preserve the existing Python contract.
 */

extern "C" {

VecResult draken_cast_string_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->int64: expected string, got %d", v->type);

        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t n = v->length;
        uint8_t* val = kernel_copy_validity(v);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) { draken_free(val); return draken_error_sentinel("Allocation failed"); }

        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) { out[i] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
            const uint8_t* sdata = str_data(slot, sa->arena);
            const uint32_t slen  = str_length(slot);

            int64_t value = 0;
            int64_t sign = 1;
            uint32_t k = 0;
            if (slen > 0 && sdata[0] == '-') { sign = -1; k = 1; }
            for (; k < slen; ++k) {
                const uint8_t c = sdata[k];
                if (c < '0' || c > '9') {
                    draken_free(out);
                    draken_free(val);
                    return draken_error_sentinel("Invalid digit in integer literal");
                }
                value = value * 10 + (c - '0');
            }
            out[i] = sign * value;
        }

        VecResult r;
        r.data = out; r.validity = val; r.selection = draken_identity_sel(n);
        r.owns_selection = false; r.data_length = n; r.length = n;
        r.type = DRAKEN_INT64; r.flags = 0;
        return r;
    });
}

VecResult draken_cast_string_to_bool(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR && v->type != DRAKEN_VARBINARY)
            return draken_error_sentinel_fmt("cast string->bool: expected string, got %d", v->type);

        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t n = v->length;
        uint8_t* val = kernel_copy_validity(v);
        const size_t nbytes = (n > 0u ? (n + 7u) / 8u : 1u);
        uint8_t* out = static_cast<uint8_t*>(draken_malloc(nbytes));
        if (!out) { draken_free(val); return draken_error_sentinel("Allocation failed"); }
        std::memset(out, 0, nbytes);

        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) continue;
            const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
            const uint8_t* s = str_data(slot, sa->arena);
            const uint32_t slen = str_length(slot);

            bool truth;
            if (slen == 4 && (s[0]|32u)=='t' && (s[1]|32u)=='r' && (s[2]|32u)=='u' && (s[3]|32u)=='e') {
                truth = true;
            } else if (slen == 5 && (s[0]|32u)=='f' && (s[1]|32u)=='a' && (s[2]|32u)=='l' && (s[3]|32u)=='s' && (s[4]|32u)=='e') {
                truth = false;
            } else if (slen == 1 && s[0]=='1') { truth = true;
            } else if (slen == 1 && s[0]=='0') { truth = false;
            } else if (slen == 3 && (s[0]|32u)=='y' && (s[1]|32u)=='e' && (s[2]|32u)=='s') { truth = true;
            } else if (slen == 2 && (s[0]|32u)=='n' && (s[1]|32u)=='o') { truth = false;
            } else if (slen == 2 && (s[0]|32u)=='o' && (s[1]|32u)=='n') { truth = true;
            } else if (slen == 3 && (s[0]|32u)=='o' && (s[1]|32u)=='f' && (s[2]|32u)=='f') { truth = false;
            } else {
                draken_free(out);
                draken_free(val);
                return draken_error_sentinel(
                    "Cannot cast string to BOOL: expected true/false/1/0/yes/no/on/off");
            }
            if (truth) out[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
        }

        VecResult r;
        r.data = out; r.validity = val; r.selection = draken_identity_sel(n);
        r.owns_selection = false; r.data_length = n; r.length = n;
        r.type = DRAKEN_BOOL; r.flags = 0;
        return r;
    });
}

// STRING → FLOAT64: no extracted compute yet (no nanobind source). Remains a stub.
VecResult draken_cast_string_to_float64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("cast string->float64 not yet implemented"); });
}

}  // extern "C"
