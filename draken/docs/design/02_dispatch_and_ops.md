# Draken Dispatch & Operations (DRAFT)

> Status: DRAFT. This is the heart of the inversion: logic lives in C++, dispatched
> once on the type tag, then run in compile-time-templated typed loops. No Python
> class hierarchy, no Cython vtable, no `getattr`, no `object` in the middle.

## Dispatch model

A vector carries a `DrakenType` tag. Each operation is a C++ free function that
switches on the tag **once** at entry, then calls a templated implementation:

```cpp
void hash(const DrakenVector& v, uint64_t* out) {
    switch (v.type) {
        case DRAKEN_INT64:   hash_impl<int64_t>(v, out);  break;
        case DRAKEN_FLOAT64: hash_impl<double>(v, out);   break;
        /* ... */
        default: throw unsupported_type(v.type);
    }
}
```

- **One dispatch, then a tight typed loop.** Same shape DuckDB uses.
- No `Vector` base class with overridable methods; no per-type Python subclasses.
- An unsupported type throws — there is **no untyped fallback** that boxes values
  (the old `getattr`/`self[i]`/`to_int` crutch does not exist, so it cannot silently
  become the live path).

**Mechanism (resolved): a dispatch table, not a hand-written `switch`.** The
`switch` above illustrates the *semantics*; the real dispatch indexes a table by the
type tag — a per-type ops descriptor, i.e. a manual vtable keyed by `DrakenType`
(not by a class). The indirect call is paid **once per op invocation** (per
column/morsel), amortized over all rows → negligible, and adding a type is "fill a
table row," not "edit every switch."

## Shape handling inside an op

For each typed impl, the uniform path is `data[selection[i]]`. Permitted
performance fast paths (must be result-identical):
- **constant** (`data_length == 1`): compute once, broadcast.
- **dense** (identity selection): read `data[i]` directly → contiguous SIMD.
- **dict**: gather `data[selection[i]]` into scratch, then the same contiguous
  kernel. No separate dict loop that can drift from dense.

Null handling: `validity` is logical-row-indexed; gating chooses nonnull /
branchless / branching by null density (port the existing thresholds, ~70%).

## SIMD strategy (resolved)

Hand-written SIMD where it is core to performance: **integer, boolean, and string**
ops — bit-packing (bool), the byte-pack compare trick (integer), string
hashing/compare — patterns compilers don't reliably vectorise. Other types (float,
temporal, decimal) lean on **compiler auto-vectorisation** unless profiling says
otherwise (float arithmetic in particular auto-vectorises cleanly). Targets: NEON
(ARM dev) and AVX2 (x86 prod).

## Operation catalog (initial)

Per type unless noted. Signatures are illustrative.

| Op | Notes |
|----|-------|
| `hash(v, out)` / `hash_single` | uint64 per row; feeds joins/distinct/group-by. **Pilot op.** |
| `compare_scalar(v, value, op)` → bool mask | eq/ne/gt/ge/lt/le; SIMD kernels (`_*_compare.hpp`). |
| `compare_vector(a, b, op)` → bool mask | same type. |
| `compare_cross(a, b, op)` | dispatch **promotes** to a common type, then mono-typed `compare_vector`; no `object other`, no cross-type kernel family (see Cross-type). |
| `between(v, lo, hi, incl…)` | single pass. |
| `in_list(v, set)` → bool mask | hash to uint64 + native `CarcharSet` bulk probe (`probe_found_32`); all types, **not** Python set membership. |
| `arithmetic(a, b, op)` → vector | **in scope** — add/sub/mul/div/mod/neg; typed batch kernel like compare; cross-type via promote-at-dispatch (result type by SQL rules — int64×float64 → float64, the *defined* lossy-OK semantic). Replaces `arithmetic_kernels`/`get_arithmetic_kernel`. |
| `sum/min/max(v)` | reductions (`_reductions.hpp`); nullable variants return count. |
| `take(v, indices)` | gather → owned result. |
| `materialize(v)` | expand dict/const → dense (owned). |
| `compress_into(v, out)` | typed; no per-row boxing. |
| `to_python(v, i)` / `to_pylist(v)` | **edge only** — the sole place a value becomes a `PyObject*`. |

**Resolved: arithmetic is in scope** (it's a typed batch kernel exactly like compare).
Note the rebuild **maintains function, not the exact interface** — we keep the
*capability* (`get_arithmetic_kernel`'s add/sub/mul/div/…) but are free to redesign the
API and update call sites; the consumer "contract" (`03`) is the set of *functions to
preserve*, not frozen signatures.

| `arithmetic(a, b, op)` | add/sub/mul/div/mod; typed batch kernel; cross-type via promote-at-dispatch (see precision rule). Replaces `arithmetic_kernels`/`get_arithmetic_kernel`. |

<!--
/opus/ RESOLVED: arithmetic IS in scope for the draken core (it's a typed batch kernel
exactly like compare) and is now in the catalog above + the int64 pilot op set in 08.
It uses the same promote-at-dispatch path as compare_cross, governed by the precision
table below (int×float arithmetic → float64 is the defined SQL semantic, lossless for
narrow ints, accepted-by-definition for int64). The draken_old surface
`arithmetic_kernels.get_arithmetic_kernel` is the thing being replaced — flagged in 07
for re-homing so it doesn't fall through the cracks. CLOSED. -->


## Cross-type comparison (resolved: promote at dispatch)

Cross-type ops are **not** handled by a combinatorial family of cross-type kernels
— that surface explodes with every new type (int128, …) and pushes complexity into
the ops. Instead the **dispatch layer promotes** to a common type, then calls the
ordinary mono-typed op. Ops stay simple and single-typed; only the promotion table
grows when a type is added. (Consequently `_int64_float64_compare.hpp` and its kin
are **superseded**, not port targets.)

**Resolved precision rule (asymmetric — compare exact, arithmetic may promote):**
- **Comparison** `int64 × float64` must be **exact** — do **not** promote int64 to
  double (lossy past 2^53, returns the wrong boolean silently → a §1 violation).
  Use the standard exact comparison (range-check the double against int64 bounds,
  then compare against the integer/floor). Bounded cost, exact.
- **Arithmetic** `int64 × float64` → float64 *is* the defined SQL result type, so
  double promotion is the correct semantic, not a silent lie.

This asymmetry is the rule; the promotion layer encodes it as a table (compare:
exact-promote; arithmetic: value-promote), not a per-pair guess.

<!--
/opus/ RESOLVED — the rule, decided here (not per-pair-later). The asymmetry below is
the contract; the promotion layer implements it, ops stay mono-typed.

  | Mixed pair          | COMPARE / IN                | ARITHMETIC                  |
  |---------------------|-----------------------------|-----------------------------|
  | int64 × float64     | EXACT — no double promotion  | float64 (defined SQL semantic) |
  | int32/16/8 × float  | promote to double — LOSSLESS (all fit in 2^53) | float64 |
  | int64-dec × int128-dec | promote int64→int128 (lossless) | int128, see decimal note |
  | decimal × int       | scale-align, integer domain (lossless) | scale-aware |

EXACT int64×float64 compare = the standard branch: range-check the double against
int64 bounds, then compare against floor/ceil — bounded cost, no wrong booleans near
2^53. Arithmetic→double is the *defined* result type, so it is not a silent lie and
needs no special handling. Only the int64×float64 *compare* cell needs the exact path;
the narrow-int×float cells are lossless under double and need nothing.
-->


## in_list / IN — hash-based, always (resolved)

IN / NOT IN hashes values to uint64 and probes the native `CarcharSet`
(`third_party/mabel/carchar/carchar_set.hpp`) for **all** types — fixed-width
included, no raw-key special case. This matches join/distinct (one path).

**Hash-only, no key verification — accepted, quantified, signed-off exception.**
`CarcharSet` compares 64-bit hashes only (no stored-key verify), so a collision can
admit a wrong row in IN/join. Expected wrong rows per query ≈ `N·K / 2^64` (N probe
rows, K set/build keys). At our volumes this is negligible (e.g. N=1e9, K=1e4 →
~5e-7; even N=K=1e8 → ~5e-4). Decision: **accept hash-only**, no verify — it is not a
realistic risk at the data sizes we target. (If a future workload pushes N·K toward
~1e18, revisit: add key-verify for that path.)

<!--
/opus/ CLOSED. Confirmed against carchar_set.hpp (stores uint64 only, no key, no
verify). Architect decision: accept hash-only engine-wide as a named, quantified
exception (body above). The exception is now explicit and owned — exactly what §1's
"never lie about state" requires (we're not pretending it's exact; we've stated the
bound and signed it off). No verify path in v1; revisit only if N·K approaches ~1e18. -->


## Forbidden (the old smells, restated as rules)

- No `<object>self` / `getattr(self, "...")` dynamic dispatch.
- No `object`-typed value in any compiled loop; element boxing only at the Python
  edge functions.
- No shape-discriminant that can skip rows (e.g. `ptr.data == NULL`).
- No Python `in`, Python `set`, or per-row Python calls in hot loops.

## Open questions

- [ ] Templated `<T>` per op vs a few hand-written SIMD specializations — how much
      do we lean on the compiler vs intrinsics (NEON/AVX2)? /JJ/ where it matters, we manually specialize for performance, integer, boolean and string ops are so very core to performance we SIMD these, others we could leave for the compiler
- [ ] Does dispatch live in one big `switch` per op, or a dispatch table / tag-class? /JJ/ I'd prefer a dispatch table
- [ ] in_list raw-key vs hash for fixed-width — decide; and string exactness. /JJ/ hash-based, always
- [ ] Where do cross-type promotions live (int×float, decimal×int)? /JJ/ depends on where this detail is available, dispatch is probably the right place, in op makes ops complex and creates a huge surface if we add another type (e.g. int128)

## Source to port from
`draken_old/vectors/_int64_compare.hpp`, `_int64_reductions.hpp`,
`_integer_compare.hpp` (already templated on `<T,Op>`); the gather/gating logic in
`draken_old/vectors/integer64_vector.pyx`. (`_int64_float64_compare.hpp` is
**superseded** by promote-at-dispatch — do not port the cross-type kernel family.)
