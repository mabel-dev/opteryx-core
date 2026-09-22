// draken/ops/kernels/function_json_validate.cpp — SQL:2016 `IS [NOT] JSON`.
//
// Eight C-ABI kernels: four shapes (VALUE/SCALAR/ARRAY/OBJECT) x two polarities.
// The polarity is baked into the kernel rather than carried in a ctx so the
// per-row inner loop has no bind-time load and the plan compiler needs no
// context allocator — the same shape `draken_is_empty`/`draken_is_not_empty`
// take, which is what these lower through.
//
// NULLS. `IS [NOT] JSON` is a TOTAL predicate: the result carries NO validity
// mask and every row gets a definite boolean. A NULL operand is not a
// well-formed document, so `NULL IS JSON` is FALSE and `NULL IS NOT JSON` is
// TRUE. This follows the `IS TRUE`/`IS FALSE` family (evaluation.pyx
// _bv_truth_test_native: "Result is always null-free"), ruled by the architect
// 2026-09-22. It is deliberately NOT the `draken_string_empty` /
// `draken_json_path_exists` contract, which propagate the operand's mask.
//
// The validator is draken/ops/json_validate.h — a single non-allocating pass
// over the bytes. yyjson is not involved: nothing here builds a document.

#include <cstdint>
#include <cstring>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"  // draken_identity_sel
#include "ops/json_validate.h"
#include "ops/vec_result.h"
#include "ops/kernels/error_handling.h"

namespace {

inline bool jk_row_valid(const DrakenVector* v, uint32_t row) noexcept {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7)) & 1u) != 0u;
}

// VARIANT is admitted alongside the string family: it is string-arena backed
// JSON text with the same slot layout, so `(doc -> 'a') IS JSON` composes.
inline bool jk_is_json_text(DrakenType t) noexcept {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY ||
           t == DRAKEN_VARIANT;
}

// Shared body for all eight kernels. `want` is the answer a well-formed
// document of the asked-for shape produces: true for IS JSON, false for
// IS NOT JSON.
VecResult jk_is_json(const DrakenVector* const* args, uint32_t nargs, uint8_t shape,
                     bool want, const char* name) {
    if (!args || nargs != 1u || !args[0])
        return draken_error_sentinel_fmt("%s: expected 1 argument", name);

    const DrakenVector* v = args[0];
    if (!jk_is_json_text(v->type))
        return draken_error_sentinel_fmt(
            "%s: operand must be JSON text (VARCHAR, NVARCHAR, VARBINARY or VARIANT)",
            name);

    const uint32_t n = v->length;
    const size_t nb = (static_cast<size_t>(n) + 7u) / 8u;
    const size_t nb_alloc = nb > 0u ? nb : 1u;

    auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb_alloc);

    const auto* sa = static_cast<const DrakenStringArena*>(v->data);

    // Hoisted out of the row loop: the validator's only state, reused across the
    // whole column so a batch of N documents does no per-row setup.
    draken::ops::JsonDepthStack stack;

    for (uint32_t i = 0u; i < n; ++i) {
        // A NULL row has no document, so it is not well-formed — `matched` stays
        // false and the polarity below turns that into the definite answer.
        bool matched = false;
        if (jk_row_valid(v, i)) {
            const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
            matched = draken::ops::json_is_wellformed(str_data(slot, sa->arena),
                                                      str_length(slot), shape, stack);
        }
        if (matched == want) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    VecResult r{};
    r.data = out;
    r.validity = nullptr;  // total predicate — never null
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

}  // namespace

// C linkage: kernel_registry.cpp declares every kernel inside an `extern "C"`
// block and looks them up by unmangled name.
extern "C" {

#define DRAKEN_IS_JSON_KERNEL(fn_name, shape, want)                                  \
    VecResult fn_name(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) { \
        return jk_is_json(args, nargs, (shape), (want), #fn_name);                    \
    }

DRAKEN_IS_JSON_KERNEL(draken_is_json_value, draken::ops::JSON_SHAPE_VALUE, true)
DRAKEN_IS_JSON_KERNEL(draken_is_not_json_value, draken::ops::JSON_SHAPE_VALUE, false)
DRAKEN_IS_JSON_KERNEL(draken_is_json_scalar, draken::ops::JSON_SHAPE_SCALAR, true)
DRAKEN_IS_JSON_KERNEL(draken_is_not_json_scalar, draken::ops::JSON_SHAPE_SCALAR, false)
DRAKEN_IS_JSON_KERNEL(draken_is_json_array, draken::ops::JSON_SHAPE_ARRAY, true)
DRAKEN_IS_JSON_KERNEL(draken_is_not_json_array, draken::ops::JSON_SHAPE_ARRAY, false)
DRAKEN_IS_JSON_KERNEL(draken_is_json_object, draken::ops::JSON_SHAPE_OBJECT, true)
DRAKEN_IS_JSON_KERNEL(draken_is_not_json_object, draken::ops::JSON_SHAPE_OBJECT, false)

#undef DRAKEN_IS_JSON_KERNEL

}  // extern "C"
