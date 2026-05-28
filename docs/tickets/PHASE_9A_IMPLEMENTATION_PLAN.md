# Phase 9a Implementation Plan — Handoff & Continuation Guide

**Status**: Headers + inventory complete. Implementation in progress.

**Date started**: 2026-05-28

---

## What's Been Completed (Session 1)

### Headers & Design
- ✅ `draken/ops/kernels/kernel_context.h` — 6 context struct types
- ✅ `draken/ops/kernels/cast_kernels.h` — BC_CAST C ABI signatures
- ✅ `draken/ops/kernels/binary_op_kernels.h` — BC_BINARY_OP C ABI signatures
- ✅ `draken/ops/kernels/extraction_kernels.h` — BC_EXTRACTION C ABI signatures
- ✅ `draken/ops/kernels/function_kernels.h` — BC_FUNCTION C ABI signatures (~60+)
- ✅ `draken/ops/kernels/error_handling.h` — Error sentinel pattern
- ✅ `draken/ops/kernels/c_kernel_abi.h` — Central include

### Documentation
- ✅ `docs/tickets/KERNEL_INVENTORY.md` — Full ~90+ kernel enumeration with flags

### Architecture Decisions (Locked)
- ✅ **Full enumeration**: All ~90+ BC_FUNCTION kernels listed
- ✅ **Aggregates out of scope**: Not part of 9a
- ✅ **C-native dispatch**: `cast_to_*` helpers need C impl, not Python wrappers
- ✅ **Decomposed arithmetic**: Separate `draken_add/sub/mul/div/mod`, not unified
- ✅ **Reuse error handling**: Thread-local error sentinel pattern
- ✅ **Individual arithmetic functions**: Not a unified dispatcher

---

## What Remains (Session 2+)

### Phase 1: Error Handling Infrastructure
**File**: `draken/ops/kernels/error_handling.cpp` ✅ COMPLETE

Implemented:
- ✅ `char* draken_error_message_slot()` — thread-local buffer accessor
- ✅ `void draken_error_message_clear()` — reset error state
- ✅ `VecResult draken_error_sentinel(const char* msg)` — return error + set message
- ✅ `VecResult draken_error_sentinel_fmt(const char* fmt, ...)` — printf-style error
- ✅ `bool draken_has_error()` — check if error is set
- ✅ `const char* draken_get_error_message()` — read error

**Done**: Thread-local error infrastructure complete. Unblocks all other phases.

---

### Phase 2: Dispatch Helpers (C implementations)
**Location**: `draken/ops/kernels/cast_dispatch.cpp` ✅ COMPLETE

Replaced Python row-loop closures with C implementations:

| Function | Signature | What it does | Notes |
|---|---|---|---|
| `draken_cast_to_float64` | `(void* ctx, const DrakenVector* v)` | Dispatch to appropriate `*_to_float64` kernel | Check input type, call native cast |
| `draken_cast_to_int64` | `(void* ctx, const DrakenVector* v)` | Dispatch to appropriate `*_to_int64` kernel | Check input type, call native cast |
| `draken_cast_to_varchar` | `(void* ctx, const DrakenVector* v)` | Dispatch to appropriate `*_to_string` kernel | Handle ARRAY → JSON string |
| `draken_cast_to_bool` | `(void* ctx, const DrakenVector* v)` | Dispatch to appropriate `*_to_bool` kernel | Check input type, call native cast |
| `draken_cast_to_date` | `(void* ctx, const DrakenVector* v)` | Dispatch to date conversions or row-loop | May need C++ impl for row-loop path |
| `draken_cast_identity` | `(void* ctx, const DrakenVector* v)` | Return input unchanged | No-op cast |

**Pattern**:
```cpp
extern "C" VecResult draken_cast_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        DrakenType input_type = v->type;
        switch (input_type) {
            case DRAKEN_INT64:
                return draken_cast_int64_to_float64(nullptr, v);
            case DRAKEN_FLOAT64:
                return draken_cast_identity(nullptr, v);  // Already FLOAT64
            case DRAKEN_BOOL:
                return draken_cast_bool_to_float64(nullptr, v);
            case DRAKEN_VARCHAR:
                return draken_cast_string_to_float64(nullptr, v);
            // ... other cases
            default:
                return draken_error_sentinel_fmt("Cannot cast %d to FLOAT64", input_type);
        }
    });
}
```

**Dependency**: Needs error_handling.cpp done first.

---

### Phase 3: Arithmetic Kernel Wrappers
**Location**: `draken/ops/kernels/binary_op_arithmetic.cpp` ✅ COMPLETE

Implemented individual arithmetic operations:

| Function | What it does | Implementation |
|---|---|---|
| `draken_add(ctx, left, right)` | Addition | Call `draken_arithmetic(left, right, BOP_PLUS)` from draken's existing op table |
| `draken_subtract(ctx, left, right)` | Subtraction | Call `draken_arithmetic(left, right, BOP_MINUS)` |
| `draken_multiply(ctx, left, right)` | Multiplication | Call `draken_arithmetic(left, right, BOP_MULTIPLY)` |
| `draken_divide(ctx, left, right)` | Division | Call `draken_arithmetic(left, right, BOP_DIVIDE)` |
| `draken_modulo(ctx, left, right)` | Modulo | Call `draken_arithmetic(left, right, BOP_MODULO)` |
| `draken_binary_arith(ctx, left, right)` | Dispatcher | Read `ctx->op_code`, dispatch to one of the above |

**Note**: These are thin wrappers around the existing C++ `draken_arithmetic` machinery. The DV fast path already proves it's callable.

**Pattern**:
```cpp
extern "C" VecResult draken_add(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        // Call existing draken arithmetic machinery
        return draken_arithmetic_result(left, right, BOP_PLUS);
    });
}

extern "C" VecResult draken_binary_arith(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        auto* ctx_typed = static_cast<const binary_op_ctx*>(ctx);
        switch (ctx_typed->op_code) {
            case BOP_PLUS: return draken_add(nullptr, left, right);
            case BOP_MINUS: return draken_subtract(nullptr, left, right);
            // ...
            default: return draken_error_sentinel("Invalid op_code");
        }
    });
}
```

**Dependency**: Needs to understand `draken_arithmetic` C++ API in draken/ops/hash.h.

---

### Phase 4: Bitwise & String Concat Wrappers
**Location**: `draken/ops/kernels/binary_op_other.cpp`

Implement thin wrappers around existing nanobind C++ kernels:

| Function | Wrapped kernel | Location |
|---|---|---|
| `draken_bitwise_or` | `vector_bitwise_or` | nanobind/vector_bitwise.cpp |
| `draken_bitwise_and` | `vector_bitwise_and` | nanobind/vector_bitwise.cpp |
| `draken_bitwise_xor` | `vector_bitwise_xor` | nanobind/vector_bitwise.cpp |
| `draken_bitwise_shift_left` | `vector_bitwise_shift_left` | nanobind/vector_bitwise.cpp |
| `draken_bitwise_shift_right` | `vector_bitwise_shift_right` | nanobind/vector_bitwise.cpp |
| `draken_string_concat` | `vector_concat` | nanobind/vector_selection_concat.cpp |
| `draken_ip_in_cidr` | `vector_ip_in_cidr` | nanobind/vector_misc.cpp |

**Pattern**: Call the underlying C++ kernel implementation directly (not via nanobind).

**Dependency**: Need to understand how to call C++ kernels from extern "C" context.

---

### Phase 5: Temporal Operator Wrappers
**Location**: `draken/ops/kernels/binary_op_temporal.cpp`

Implement temporal operations:

| Function | What it does | Implementation |
|---|---|---|
| `draken_temporal_interval_op` | DATE/TIMESTAMP ± INTERVAL | Wrap `_date_interval_op_draken` or native |
| `draken_date_minus_date` | DATE - DATE | Wrap `_date_minus_date_draken` or native |
| `draken_interval_interval_op` | INTERVAL ± INTERVAL | Dispatch based on interval types |

**Dependency**: Inspect `opteryx/expression/evaluator/arithmetic.pyx` for the Draken implementations.

---

### Phase 6: Extraction Kernel Wrappers
**Location**: `draken/ops/kernels/extraction.cpp`

Implement thin wrappers:

| Function | Wrapped kernel | Location |
|---|---|---|
| `draken_map_access_string` | `vector_map_access_string` | nanobind/vector_special.cpp |
| `draken_array_map_access` | `vector_array_map_access` | draken/draken_native.cpp |
| `draken_json_extract` | `vector_json_extract` | nanobind/vector_json.cpp |
| `draken_pointer_extract` | TBD | TBD |

**Pattern**: Same as bitwise — call C++ directly.

---

### Phase 7: Cast Kernel Wrappers
**Location**: Multiple files or one central file

Implement thin wrappers for each cast pair. ~20-25 functions:

**Group 1** (`draken/ops/kernels/cast_numeric.cpp`):
- `draken_cast_int64_to_float64` → wraps nanobind/vector_casts.cpp
- `draken_cast_int64_to_string` → wraps nanobind/vector_casts.cpp
- ... (all int64→X and X→int64 casts)

**Group 2** (`draken/ops/kernels/cast_string.cpp`):
- `draken_cast_string_to_float64` → wraps draken_native or nanobind
- `draken_cast_string_to_int64` → wraps nanobind/vector_casts.cpp
- ... (all string conversions)

**Group 3** (`draken/ops/kernels/cast_temporal.cpp`):
- `draken_cast_int64_to_timestamp` → wraps nanobind/vector_casts.cpp
- `draken_cast_date32_to_timestamp` → wraps nanobind/vector_temporal_convert.cpp
- ... (temporal conversions)

**Pattern**:
```cpp
extern "C" VecResult draken_cast_int64_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        // Call the C++ nanobind kernel (or native C++)
        return call_nanobind_kernel_vector_cast_int64_to_float64(v);
    });
}
```

**Dependency**: Need to expose C++ implementations as callable functions (not just nanobind).

---

### Phase 8: Function Kernel Wrappers
**Location**: `draken/ops/kernels/function_*.cpp` (grouped by category)

Implement ~60+ function wrappers. Group by category to manage file size:

- `function_arithmetic.cpp` — ABS, SIGN, CEIL, FLOOR, ROUND, SQRT, POWER, LOG, TRUNC, RANDOM, etc.
- `function_string.cpp` — LENGTH, SUBSTRING, TRIM, LOWERCASE, UPPERCASE, INITCAP, REVERSE, REPLACE, POSITION, CONTAINS, STARTS_WITH, ENDS_WITH, REGEX_REPLACE, LEVENSHTEIN, SOUNDEX, etc.
- `function_temporal.cpp` — UNIXTIME, DATE_TRUNC, DATE_FORMAT, DATE_PART, DATE_DIFF, TIME_DIFF, FLOOR_TEMPORAL, DATE conversions, etc.
- `function_boolean.cpp` — COALESCE, IIF, NULLIF, ALLOP_*, ANYOP_*, IN_LIST, BOOL_AND_CHAIN, etc.
- `function_array.cpp` — ARRAY_CONCAT, CONTAINS_ANY, CONTAINS_ALL, ARRAY_REDUCE, SPLIT, etc.
- `function_hash.cpp` — MD5, SHA1, SHA256, SHA512, BASE64_*, HEX_*, BASE85_*, etc.
- `function_similarity.cpp` — COSINE_SIMILARITY, COSINE_DISTANCE, EMBED, etc.
- `function_json.cpp` — JSON_EXTRACT, JSONB_OBJECT_KEYS, etc.
- `function_utility.cpp` — MAP_ACCESS, EXTRACT, IF_NULL, GREATEST, LEAST, HUMANIZE, CONCAT_WS, etc.

**Pattern** (for nanobind-backed functions):
```cpp
extern "C" VecResult vector_abs(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("abs expects 1 argument");
        // Call the C++ nanobind kernel
        return call_nanobind_function_vector_abs(args[0]);
    });
}
```

**Special handling**:
- **Python row-loops** (`to_ascii`, `to_char`, `left_pad`, `right_pad`, `regex_replace`, `match_against`, `cosine_similarity_text`, `embed`, `if_null`, `array_contains*`, `jsonb_object_keys`, `humanize`): These need full C++ implementation or a wrapper that calls Python (deferred to 9f if Python not allowed).
- **Closures** (`_iif_kernel`, `_coalesce_kernel`, `_concat_ws_kernel`, `_sort_kernel`, `_greatest_kernel`, `_least_kernel`): Replace with direct C implementations (no closure overhead).

**Dependency**: Need to understand how to call existing C++ nanobind kernels from extern "C".

---

### Phase 9: C ABI Parity Test
**Location**: `tests/c_abi_test.cpp` (gtest or doctest)

Create a C/C++ unit test that:
1. For each kernel category (cast, binary_op, extraction, function):
   - Build hand-crafted `DrakenVector` inputs
   - Call the C ABI function with those inputs
   - Compare result with the nanobind binding's output
   - Assert byte-for-byte parity

2. Error path tests:
   - Call a kernel that throws (e.g., invalid JSON path)
   - Verify return value has data == nullptr
   - Verify error message is set in thread-local slot

3. Context struct tests:
   - For parameterized kernels (cast with unit, binary op with code):
     - Verify context is read correctly
     - Verify dispatcher selects the right sub-kernel

**Example test**:
```cpp
TEST(CastKernels, Int64ToFloat64Parity) {
    // Build a hand-crafted INT64 DrakenVector
    DrakenVector int64_vec = build_int64_vector({1, 2, 3, 4, 5});

    // Call C ABI kernel
    VecResult c_result = draken_cast_int64_to_float64(nullptr, &int64_vec);
    
    // Call nanobind kernel (via Python C API or direct C++)
    VecResult nb_result = call_nanobind_vector_cast_int64_to_float64(&int64_vec);
    
    // Compare
    assert_vec_result_equal(c_result, nb_result);
    
    // Cleanup
    draken_free(c_result.data);
    draken_free(c_result.validity);
}
```

---

## File Structure Summary

```
draken/ops/kernels/
├── kernel_context.h              ✅ DONE
├── c_kernel_abi.h                ✅ DONE
├── error_handling.h              ✅ DONE
├── extraction_kernels.h          ✅ DONE
├── cast_kernels.h                ✅ DONE
├── binary_op_kernels.h           ✅ DONE
├── function_kernels.h            ✅ DONE
├── error_handling.cpp            📝 TODO (Phase 1)
├── cast_dispatch.cpp             📝 TODO (Phase 2)
├── binary_op_arithmetic.cpp      📝 TODO (Phase 3)
├── binary_op_other.cpp           📝 TODO (Phase 4)
├── binary_op_temporal.cpp        📝 TODO (Phase 5)
├── extraction.cpp                📝 TODO (Phase 6)
├── cast_numeric.cpp              📝 TODO (Phase 7a)
├── cast_string.cpp               📝 TODO (Phase 7b)
├── cast_temporal.cpp             📝 TODO (Phase 7c)
├── function_arithmetic.cpp       📝 TODO (Phase 8a)
├── function_string.cpp           📝 TODO (Phase 8b)
├── function_temporal.cpp         📝 TODO (Phase 8c)
├── function_boolean.cpp          📝 TODO (Phase 8d)
├── function_array.cpp            📝 TODO (Phase 8e)
├── function_hash.cpp             📝 TODO (Phase 8f)
├── function_similarity.cpp       📝 TODO (Phase 8g)
├── function_json.cpp             📝 TODO (Phase 8h)
└── function_utility.cpp          📝 TODO (Phase 8i)

tests/
├── c_abi_test.cpp                📝 TODO (Phase 9)
```

---

## Dependencies & Blocking

- **Phase 1** (error handling) blocks all others
- **Phase 2** (dispatch helpers) can run in parallel with Phase 3-9
- **Phase 3-6** (arithmetic, bitwise, extraction) mostly independent
- **Phase 7** (cast wrappers) depends on understanding which kernels are C++ vs nanobind
- **Phase 8** (function wrappers) depends on same as Phase 7
- **Phase 9** (test) is last, validates all above

---

## Key Technical Decisions Needed (If Not Already Made)

1. **How to call C++ nanobind kernels from extern "C"**:
   - Option A: Extract C++ function pointers from nanobind (if possible)
   - Option B: Refactor existing C++ kernels to expose both nanobind + extern "C" entry points
   - Option C: Write new C++ implementations that don't depend on nanobind at all

   **Recommendation**: Option B — refactor existing nanobind kernels to have extern "C" entry points with thin nanobind wrappers calling them.

2. **Python row-loop handling**:
   - Functions flagged in inventory (to_ascii, embed, etc.) currently use Python row-loops.
   - Phase 9a must either:
     - Write full C++ implementations for these
     - OR defer to Phase 9f and mark as "fallback" for now
   - **Recommendation**: Defer to 9f; mark with TODO comments in 9a.

3. **Closure decomposition**:
   - Closures like `_iif_kernel`, `_greatest_kernel` need to become direct C functions.
   - **Recommendation**: Implement directly; no closure overhead.

---

## Verification Checklist

Before declaring 9a complete:

- [ ] `make c` compiles cleanly (no warnings)
- [ ] `make q` passes 100/100 (behavior unchanged; nanobind wrappers work)
- [ ] C ABI parity test passes (C kernels == nanobind behavior)
- [ ] No Python objects leak into compiled paths (grep confirms)
- [ ] Thread-local error slots work (tested under concurrency)
- [ ] All ~90 functions have C ABI entry points (inventory match)
- [ ] `make clickbench` non-regressing (sanity check)

---

## Estimated Effort Breakdown

- **Phase 1** (error handling): 1-2 hours
- **Phase 2** (dispatch helpers): 2-3 hours
- **Phase 3** (arithmetic): 1-2 hours
- **Phase 4-6** (bitwise, string, extraction, temporal): 3-4 hours combined
- **Phase 7** (cast wrappers): 4-6 hours (many pairs)
- **Phase 8** (function wrappers): 8-12 hours (60+ functions)
- **Phase 9** (testing): 3-4 hours
- **Integration & fixes**: 2-3 hours

**Total**: ~25-35 hours. Estimate matches design ticket's 3-5 days.

---

## Context Management Notes

This plan is self-contained and can be resumed from any phase:

- If context fills during Phase 3, write `.cpp` files for Phase 3, then:
  1. Commit changes to git (user can do this manually)
  2. Reference this plan in the next session
  3. Continue from Phase 4

- Key files to preserve between sessions:
  - All `.h` files in `draken/ops/kernels/` (headers, read-only)
  - `docs/tickets/KERNEL_INVENTORY.md` (reference)
  - This plan file itself

- Progress tracking: Update this file's TODO status as phases complete.

---

## Notes for Next Session

- Architect's locked decisions (see KERNEL_INVENTORY.md):
  - ✅ Full enumeration
  - ✅ Aggregates out of scope
  - ✅ C-native (no Python wrappers)
  - ✅ Decomposed arithmetic (not unified)
  - ✅ Thread-local error pattern

- All headers are written; focus on implementation.
- Start with Phase 1 (error handling) to unblock others.
- If stuck on how to call C++ kernels, check `draken/ops/hash.h` for `draken_arithmetic` pattern.
