# Zero-Python Phase 9 — C-Kernel-ABI Audit (real vs scaffolding vs wired)

> Architect asked (2026-05-28) to **audit all 9a kernels first** before
> committing to more per-kernel work, after `9a-fn` was found to be
> hollow scaffolding. This is the audit. It states what is real, what is
> a shell, and — critically — what is actually invoked by the executor.
> No implementation is done here. Findings verified against the working
> tree, not from memory.

## TL;DR

> **CORRECTION (2026-05-28, kernel-owner PM).** An earlier version of this
> TL;DR claimed "51 registry kernels are real … parity-tested." That is
> **wrong** and is corrected below. Verified against the kernel bodies and
> `c_abi_test.cpp`: of the ~48 registry kernels the parity test exercises,
> **only ~7 are real** (the five arithmetic ops + `draken_binary_arith`,
> and `draken_cast_identity`). The rest — **all leaf casts, all cast
> dispatchers, bitwise, `string_concat`, `ip_in_cidr`, all extractions** —
> return `draken_error_sentinel("… not yet implemented")`. The parity test
> is green *because it asserts those stubs error* (`assert(is_error(r))`),
> not because they compute. "Registered + compiled + parity-tested" was
> conflated with "real"; registration and a green stub-asserting test do
> not imply working compute.

The Phase 9 C-kernel-ABI registry is **dead infrastructure**: it is
built, registered, and parity-tested, but **invoked nowhere by the
executor**. Separately, the BC_FUNCTION half of it (~100 kernels) is
**hollow scaffolding** that has never compiled.

- **~7 registry kernels are real**: the five arithmetic ops + `draken_binary_arith`,
  and `draken_cast_identity` (a same-type no-op). These have working
  `VecResult` bodies and the parity test asserts correct values for them.
- **~41 registry kernels are stubs** — all leaf casts, cast dispatchers,
  bitwise, `string_concat`, `ip_in_cidr`, extractions: each returns
  `draken_error_sentinel("… not yet implemented")`. The parity test
  asserts `is_error` for these, so green parity ≠ working compute.
- **~100 function kernels are shells** — 100 `_impl` declarations, **0
  definitions**, sources excluded from the build.
- **0 kernels are invoked by the executor.** `kernel_fn` has no call
  site in `evaluation.pyx`. Every opcode runs on a *different* path.

So the per-kernel ABI plan, as it stands, has delivered no live
execution benefit. The engine's actual "no-Python hot path" comes from
the **DV fast paths** (`draken_arithmetic_dv`, `draken_compare_dv`),
which predate Phase 9 and are **not** part of the registry.

## Evidence

### 1. What's compiled (`setup.py` ~L705–720)

In the build: `error_handling`, `kernel_registry`, `cast_numeric`,
`cast_string`, `cast_temporal`, `cast_dispatch`, `extraction`,
`binary_op_arithmetic`, `binary_op_other`, `binary_op_temporal`.

Excluded (verbatim comment): *"Function kernels deferred to Phase 9f;
they require nanobind wrappers not yet ported to extern 'C'."* →
`function_{string,arithmetic,temporal,boolean,array,hash,json,
similarity,utility}.cpp` are **uncompiled**.

### 2. What's registered (`kernel_registry.cpp`)

51 entries are registered, but only ~7 have working bodies. **Real**
(correct `VecResult` bodies, parity test asserts values): `draken_add/
subtract/multiply/divide/modulo`, `draken_binary_arith`, and
`draken_cast_identity`. **Stubs** (`draken_error_sentinel("… not yet
implemented")`, parity test asserts `is_error`): `draken_bitwise_*`,
every `draken_cast_*` leaf and dispatcher except identity (~25),
`draken_json_extract`, `draken_map_access_string`,
`draken_pointer_extract`, `draken_array_map_access`,
`draken_string_concat`, `draken_ip_in_cidr`, temporal interval ops.
**Zero general SQL functions** (`lookup_kernel("draken_length"/"upper"/
"abs"…)` all miss).

### 3. Function kernels are shells (all 9 `function_*.cpp`)

`_impl` declarations vs definitions:

| file | `_impl` decls | `_impl` defs |
|------|---------------|--------------|
| function_string.cpp     | 28 | 0 |
| function_boolean.cpp    | 14 | 0 |
| function_arithmetic.cpp | 13 | 0 |
| function_utility.cpp    | 11 | 0 |
| function_temporal.cpp   | 10 | 0 |
| function_hash.cpp       | 10 | 0 |
| function_array.cpp      | 8  | 0 |
| function_similarity.cpp | 5  | 0 |
| function_json.cpp       | 1  | 0 |
| **total**               | **100** | **0** |

The `vector_<name>(ctx,args,nargs)` public wrappers exist (arity check +
`return vector_<name>_impl(...)`), but **no `_impl` is defined anywhere
in the repo** (`grep` for `VecResult vector_*_impl(...) {` → none).
Adding the sources to the build would fail to link.

### 4. The executor invokes `kernel_fn` nowhere

`grep` for any call through the kernel typedefs
(`<func_fn_t>`, `<cast_fn_t>`, `<binop_fn_t>`, `<extr_fn_t>`,
`<case_fn_t>`, `slot.kernel_fn(`) in `evaluation.pyx` → **no matches**.
The typedefs (L680–684) and `VecResult c_result` (L1351) are declared
but never used to call. The only `_c_native_kernel_call_count` bump
(L1786) is the all-null binop short-circuit, which calls **no** kernel.

How each opcode is **actually** computed today:

| opcode         | live path (not the registry) |
|----------------|------------------------------|
| BC_COMPARE     | `draken_compare_dv` (DV C++ fast path) + Python `draken_compare_int` fallback |
| BC_BINARY_OP   | `draken_arithmetic_dv` (DV C++ fast path) + Python `callable_ref` fallback |
| BC_CAST        | Python `callable_ref` |
| BC_EXTRACTION  | nanobind `vector_map_access_string` / `_vector_json_extract` (Python call) |
| BC_UNARY_OP    | `_unary_op_kernel` (Python) |
| BC_FUNCTION    | Python `callable_ref` |
| BC_CASE        | Python closure `callable_ref` |

`draken_arithmetic_dv` / `draken_compare_dv` are direct `cimport`ed C++
ops (`draken/ops/{arithmetic,compare}_dv`), **independent of the kernel
registry**. They are what actually keep Python out of the arithmetic/
compare hot paths.

### 5. What `make kernel-parity` proves

`c_abi_test.cpp` exercises ~48 registry kernels, but **asserts most of
them error**: only the ~7 real kernels (arithmetic + `cast_identity`)
are checked for correct values; every cast/bitwise/extraction/concat
assertion is `assert(is_error(r))` — i.e. it pins the stub behaviour, it
does not verify compute. It does **not** cover: the hollow
function kernels (can't — they don't compile), executor-level behaviour
(nothing calls these kernels in execution), or null/shape conformance
beyond what nanobind itself does. Green parity ≠ used.

## Cost to actually finish the per-kernel ABI plan

To make the registry the live execution path (the original Phase 9
end-state), remaining work:

1. **Implement ~100 function kernel bodies** (`_impl`), delegating to the
   existing draken ops the Python path already uses. Plus build inclusion
   + registration. (9a-fn, ×9 categories.)
2. **Wire the executor** to call `kernel_fn` for CAST, FUNCTION,
   EXTRACTION (and decide binary/compare vs keeping the DV fast path) —
   the VecResult→dv_stack hand-off, the C++→Cython VecResult→Vector
   trampoline (`draken_vector_own` is C++-only), error-sentinel handling.
   This is essentially **9c, which was attempted and reverted**.
3. **Per-row validity merge** + null/shape conformance for every wired
   kernel (dodged 4× since 9a).
4. **9d/9e/9f** for the nogil end-state.

In short: the bulk of Phase 9's *value* (live C-native dispatch, nogil)
is **undelivered**, and the part that is "done" (the registry) is
**unused**. ~51 real kernels sit behind an ABI nothing calls; ~100 are
shells.

## Options for the architect (surfaced, not decided)

1. **Descope & bank reality.** Declare the DV fast paths (arith/compare)
   the no-Python hot-path win — they already work, value-checked — and
   *remove* the dead registry/ABI scaffolding (or freeze it). Phase 9
   ends with: arith/compare nogil-capable via DV ops; cast/function/
   extraction/case on the working Python path. Smallest surface, no
   hollow code in tree. Matches STEP-BACK §4.4.

2. **Finish the ABI for one real category as a pilot** (e.g. CAST —
   kernels already real & registered): wire the executor to call the
   cast `kernel_fn` for real, with value-checked null/shape gates and a
   counter proof it's live. Decide whether the nogil payoff justifies
   doing the rest. Defers the ~100 function-kernel writes.

3. **Commit to the full plan**: implement the 100 function kernels + wire
   everything + validity merge + 9d/9e/9f. Largest effort; this is the
   original Phase 9 in full, with the thrash risk the STEP-BACK
   documented — only sane behind the value-checked/conformance gate.

4. **Delete the scaffolding, keep the idea parked.** Remove the hollow
   `function_*.cpp` + unused registry plumbing so the tree stops implying
   a capability it doesn't have (CLAUDE.md §1: "No hidden behaviour");
   revisit Phase 9 later with a clean slate.

## Recommendation (mine, to weigh)

The honest state is that Phase 9 mostly didn't land, and what landed
isn't wired. Given CLAUDE.md's "broken but honest" and "no hidden
behaviour": I'd **delete/freeze the dead scaffolding** (option 1 or 4)
so the tree reflects reality, keep the DV fast paths as the genuine
no-Python win, and only resume the per-kernel ABI behind a real
null/shape conformance gate if/when the nogil end-state is prioritised.
Spending the next effort writing 100 kernel bodies for an ABI the
executor doesn't call would repeat the inert-code pattern at larger
scale.

## Pointers

- Registry: `draken/ops/kernels/kernel_registry.cpp` (51 real),
  `function_*.cpp` (100 shells).
- Bind-time sets `kernel_fn` (cast: `compiled_expression.pyx` ~L762;
  function hook ~L604–615, always misses).
- Executor (no kernel_fn call site): `evaluation.pyx`
  BC_* handlers ~L1350–1995; DV fast paths via `draken_{arithmetic,
  compare}_dv` cimports (L664–665).
- VecResult→Vector wrap: `draken_vector_own` (C++-only,
  `draken_bridge.h` L196–207) — needs an extern "C" trampoline to be
  callable from the executor.
