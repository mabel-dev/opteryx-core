# Phase 9b Session 1 — IN PROGRESS (Foundation Complete)

**Date**: 2026-05-28

**Status**: Phase 9b foundation 100% complete; full integration in next session

---

## Session 1 Accomplishment

Implemented all foundation infrastructure for Phase 9b (C kernel ABI integration into bytecode executor):

### 1. BytecodeInstr Structure Extension

**File**: `opteryx/compiled/expression/compiled_expression.pxd`

Added C-native kernel fields:
```c
void* kernel_fn          // C function pointer for C kernels
void* ctx_ptr            // context struct pointer (op_code, unit, etc.)
```

Added execution flag:
```c
BC_INSTR_C_NATIVE = 0x1000  // Distinguishes C native from legacy Python callables
```

**Why**: Allows bytecode executor to dispatch to C function pointers instead of PyObject_Call for BC_FUNCTION, BC_EXTRACTION, BC_CAST, BC_BINARY_OP instructions.

### 2. Kernel Registry Header & Implementation

**Files**:
- `draken/ops/kernels/kernel_registry.h` (130 LOC)
- `draken/ops/kernels/kernel_registry.cpp` (450 LOC)

**Provides**:
- `kernel_registry_lookup(name, out_fn, out_ctx)`: Lookup kernel by name (uppercase string)
- `kernel_alloc_cast_timestamp_ctx(unit)`: Allocate context for INT64→TIMESTAMP with unit
- `kernel_alloc_binary_op_ctx(op_code)`: Allocate context for binary ops with op dispatch
- `kernel_alloc_extraction_ctx(sub_op_code)`: Allocate context for extraction with sub-op dispatch
- `kernel_free_context(ctx)`: Free allocated context

**Registry Table**:
Maps 90+ kernel names to C function pointers:
- Phase 8a: 13 arithmetic functions (ABS, SIGN, CEIL, etc.)
- Phase 8b: 24 string functions (LENGTH, SUBSTRING, TRIM, LOWER, etc.)
- Phase 8c: 10 temporal functions (UNIXTIME, DATE_TRUNC, DATE_FORMAT, etc.)
- Phase 8d: 15 boolean functions (COALESCE, IIF, NULLIF, ALLOP_*, ANYOP_*, etc.)
- Phase 8e: 8 array functions (ARRAY_CONCAT, CONTAINS_*, ARRAY_REDUCE, etc.)
- Phase 8f: 10 hash functions (MD5, SHA*, BASE64_*, HEX_*, BASE85_*)
- Phase 8g: 5 similarity functions (COSINE_SIMILARITY, COSINE_DISTANCE, etc.)
- Phase 8h: 1 JSON function (JSONB_OBJECT_KEYS)
- Phase 8i: 11 utility functions (GREATEST, LEAST, CONCAT_WS, IF_NULL, etc.)

**Why**: Central lookup table allows bytecode builder to resolve kernel names to C function pointers at bind time.

### 3. Cython Wrapper for Kernel Registry

**File**: `draken/ops/kernels/_kernel_registry.pyx` (130 LOC)

**Public Functions**:
- `lookup_kernel(name: str) -> (fn, ctx)`: Look up kernel by name, return opaque pointers
- `alloc_cast_timestamp_ctx(unit: int) -> ptr`: Allocate timestamp context
- `alloc_binary_op_ctx(op_code: int) -> ptr`: Allocate binary op context
- `alloc_extraction_ctx(sub_op_code: int) -> ptr`: Allocate extraction context
- `free_context(ptr)`: Free context

**Why**: Bridges C kernel registry to Cython/Python bytecode builder layer.

### 4. C Kernel Dispatcher for Executor

**File**: `opteryx/expression/evaluator/_c_kernel_dispatch.pyx` (180 LOC)

**Provides**:
- `dispatch_c_kernel(kernel_fn_ptr, ctx_ptr, dv_stack_list, sp, arity)`: Dispatch to C kernel
- `call_c_kernel_variadic(fn, ctx, dv_stack, sp, arity)`: Variadic C kernel call
- `call_c_kernel_binary(fn, ctx, left, right)`: Optimized binary kernel call
- `call_c_kernel_unary(fn, ctx, vec)`: Optimized unary kernel call
- `VecResultWrapper`: Safe wrapper for VecResult to bridge C ↔ Cython

**Dispatch Pattern**:
1. Convert opaque ints back to DrakenVector* pointers
2. Call appropriate optimized path (unary/binary/variadic)
3. Check VecResult for errors (data == NULL means error)
4. Return wrapped result for executor compatibility

**Why**: Provides efficient C kernel dispatch for evaluator with minimal overhead.

---

## What's Ready for Integration (Next Session)

All foundation pieces exist; Phase 9b needs:

1. **Bytecode Builder Integration**
   - Modify `opteryx/compiled/expression/compiled_expression.pyx:build_bytecode()`
   - Call `kernel_registry_lookup()` for BC_FUNCTION/EXTRACTION/CAST/BINARY_OP instructions
   - Populate `slot.kernel_fn` and `slot.ctx_ptr` instead of `slot.callable_ref`
   - Set `BC_INSTR_C_NATIVE` flag when C kernel found

2. **Executor Integration**
   - Modify `opteryx/expression/evaluator/evaluation.pyx:execute_bytecode()`
   - Add C-native dispatch before legacy Python path for BC_FUNCTION
   - Check `slot.flags & BC_INSTR_C_NATIVE` to decide C vs Python dispatch
   - Call `dispatch_c_kernel()` for C kernels
   - Reuse existing result wrapping code (BC_RESULT_NEEDS_NB_WRAP, etc.)

3. **Testing**
   - Extend Phase 9 C ABI test to verify C→Python bridge
   - Integration tests verifying C kernels produce identical output to Python
   - Performance tests comparing C vs Python dispatch overhead

---

## Code Statistics

**Files Created**: 4 (2 C++, 2 Cython)
- kernel_registry.h: 130 LOC
- kernel_registry.cpp: 450 LOC
- _kernel_registry.pyx: 130 LOC
- _c_kernel_dispatch.pyx: 180 LOC

**Total Phase 9b Foundation**: ~890 LOC

**What It Enables**:
- Zero-copy dispatch from bytecode to C kernels (no Python roundtrip)
- Context struct passing for parameterized kernels
- Error handling via thread-local error buffer
- Opaque pointer passing via 64-bit ints for C↔Cython safety

---

## Design Decisions

1. **Opaque Pointer Encoding**
   - C function pointers and context structs passed as opaque `unsigned long long` ints
   - Reason: DrakenVector* cannot be typed in Cython (C-only type), so all C-level state goes through ints
   - Executor converts back to `void*` for kernel call

2. **Registry Lookup at Bind Time**
   - Kernel resolution happens once during bytecode build, not per-execution
   - Reason: Avoid repeated hash table lookups in hot path

3. **Three Dispatch Paths**
   - Unary (1 arg): Single-item stack allocation
   - Binary (2 args): Optimized DrakenVector* pair access
   - Variadic (N args): Flexible args array on stack
   - Reason: Fast path for common cases, general path for variadic functions

4. **Context Allocation Model**
   - Bytecode builder allocates context via `kernel_alloc_*_ctx()`
   - Held in CompiledBytecode._held_refs for lifetime management
   - Executor receives opaque pointer, never needs to free
   - Reason: Clean separation of allocation (Python) from dispatch (C)

---

## What's NOT Done (Intentional)

These are deferred to full Phase 9b integration session:
- Actual modification of bytecode builder to call lookup_kernel()
- Actual modification of executor BC_FUNCTION handler for C dispatch
- Actual modification of BC_EXTRACTION, BC_CAST, BC_BINARY_OP handlers
- Integration tests and performance validation
- Documentation of binding process in developer guide

---

## Next Steps (Session 2 or later)

1. **Full Integration** (~4-6 hours)
   - Modify bytecode builder to populate kernel_fn/ctx_ptr for all 5 opcodes
   - Modify executor to dispatch C kernels for BC_FUNCTION/EXTRACTION/CAST/BINARY_OP
   - Set BC_INSTR_C_NATIVE flag when C kernel found
   - Reuse existing result wrapping for compatibility

2. **Testing** (~2-3 hours)
   - Unit tests: C kernel dispatch with various arities
   - Integration tests: C kernels vs Python callables produce same results
   - Performance tests: Measure C dispatch overhead vs PyObject_Call
   - Error handling: Verify thread-local error buffer works through dispatch

3. **Validation** (~1-2 hours)
   - Run make q to verify no regressions
   - Run make clickbench to check performance
   - Verify 5+ representative queries use C kernels (profiling)

---

## Estimated Effort

- **This session** (Phase 9b foundation): 4-5 hours, 890 LOC
- **Next session** (Phase 9b integration): 4-6 hours
- **Testing/Validation**: 2-3 hours
- **Total Phase 9b**: 10-14 hours (aligns with 2-3 day estimate from Phase 9 design)

---

**Status**: ✅ Foundation complete. Ready for full integration in Phase 9b Session 2.

