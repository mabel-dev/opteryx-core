# Milestone E.0 — Consumer Rewrite Scoping

> Status: LIVING DOCUMENT. Last updated 2026-05-23 (E.16b).
> E.0: Inventory + sequenced plan (research-only).
> E.1: §2 revised — `cdef public` + nanobind C++ wrapper pattern.
> E.2: §2 revised again — C′ canonical (pure nanobind C++, no .pyx layer). §3 updated.
> E.3: Phase 1 cost datapoint added (math ops: abs/sign/sqrt/round).
> E.4: Phase 1 cost datapoint added (codec + bool ops, pure C′ no Part A).
> E.5: Phase 1 cost datapoint added (array reductions ANY/ALL, with Part A).
> E.6: Phase 1 cost datapoint added (string length/emptiness + array element count, pure C′).
> E.7 REVISED: §2 updated — draken_vector_own_string gets DrakenType type parameter;
>   string type family (VARCHAR/NVARCHAR/VARBINARY) introduced. Phase 1 (E.7) datapoint added.
> E.8: Phase 1 cost datapoint added (hex encode/decode + MD5/SHA digest, first own_string consumers).
> E.9: Phase 1 cost datapoint added (cast cluster: int↔string + string→int + int→timestamp). draken_vector_own_timestamp added to bridge.
> E.10: Phase 1 cost datapoint added (Part A + Part B; bytewise — no UTF-8 decisions). 4 .pyx deleted (vector_starts_ends, vector_contains, vector_contains_all, vector_contains_any). CI variants (ASCII fold only) ported as in-scope since live in production. Array membership ops (contains_any/all on DRAKEN_ARRAY) ported duck-typed via to_pylist(). Callers updated: string_ops, text, json_ops, utility, operations/__init__.
> E.11: Phase 1 cost datapoint added (multi-arg shapes + concat — bytewise, no UTF-8). 3 .pyx deleted (vector_coalesce, vector_iif, vector_concat; vector_concat_array/concat_ws_array were dead code). New shape: variadic nb::args for coalesce + concat; fixed-itemsize derived from DrakenType tag. iif null-in-mask follows SQL: treated as FALSE (→ false branch). concat VARBINARY mixing: VARBINARY wins. Type promotion applied uniformly across all three ops. Callers updated: registrar/logical.pyx, implementations/logical.pyx, binary_operators.pyx, arithmetic.pyx (scalar inputs wrapped via StringVector.from_constant). 39 new tests, 2369/2369 draken native tests green.
> E.12: Phase 1 cost datapoint added (temporal conversion cluster — numeric unit math, no strings). 4 .pyx deleted (vector_date32_to_timestamp, vector_timestamp_to_date32, vector_unixtime, vector_floor_temporal). Unit read via PyObject_GetAttrString("logical_type_unit") — function-level, not per-row. floor_div corrects C++ truncation-toward-zero for pre-epoch timestamps. floor_temporal supports second/minute/hour/day (singular+plural, case-insensitive); month/year deferred. Callers updated: casts.pyx (2 sites), temporal.pyx (3 sites). 64 new tests, 2433/2433 draken native tests green.
> E.13: Phase 1 cost datapoint added (temporal arithmetic cluster — calendar-aware date_diff/date_part/date_trunc/date_format). 4 .pyx deleted (vector_date_diff, vector_date_format, vector_date_part, vector_date_trunc). New Part A: draken/ops/temporal_arith.h (Howard Hinnant proleptic Gregorian + floor_div + date_trunc_batch + date_diff_batch). dayofweek uses +3 offset (0=Monday, epoch=Thursday=3); week trunc uses ((days-4)%7+7)%7 (days-to-Monday). Callers updated: temporal.pyx (4 functions fully replaced). 96 new tests, 2529/2529 draken native tests green.
> E.14: Phase 1 cost datapoint added (mixed-shape cluster — LOG, IN LIST, IP IN CIDR). 3 .pyx deleted (vector_log, vector_in_list, vector_ip_in_cidr). New Part A: draken/ops/float_log.h (IEEE std::log, change-of-base, broadcast). vector_in_list accepts raw Python sequence — CarcharSet built internally from vector type (avoids pre-build at plan time); negate via bm_negate_inplace. boost::math has no log; std::log used instead (IEEE 754 compliant, equivalent). binder.py + predicate_rewriter.py now store raw lists (deferred set-build to execution). 7 callers updated across comparisons/string_ops/temporal_ops/list_ops/fastpath_dictionary/operations/__init__/binary_operators/arithmetic. 27 new tests, 2556/2556 draken native tests green.
> E.15: Phase 1 cost datapoint added (bytewise string misc — levenshtein + position + random_strings). 3 .pyx deleted (vector_levenshtein, vector_position, vector_random_string). Pure C′, no Part A (algo inlined in consumer). levenshtein: two-row rolling DP, O(min(m,n)) space, null TVL. position: BMH, SQL 1-based, 0=not-found, null TVL (upgrade from old sentinel-0 behaviour). random_strings: inline PCG32 seeded non-deterministically, 64-char ASCII alphabet (matches old .pyx). Bytewise on NVARCHAR is approximate (codepoint variants deferred). Callers updated: registrar/text.pyx (levenshtein + position), implementations/arithmetic.pyx (random_strings). 35 new tests, 2591/2591 draken native tests green.
> E.16: Phase 1 cost datapoint added (mixed string/fp16 — replace + cosine_similarity + cosine_distance). 2 .pyx deleted (vector_replace, vector_cosine). New Part A: draken/ops/vector_cosine.h (fp16→float64 widening; dot product + L2 norms; zero-norm→NaN; lazy validity bitmap). replace: left-to-right bytewise scan; empty needle = no-op (PostgreSQL convention; avoids infinite-loop hazard). cosine: logical_type_dimension read via PyObject_GetAttrString (D.8 path, function-level not per-row). distance: 1 - clip(sim, -1, 1) with NaN passthrough. vector_split split out → Phase 15b (requires draken_vector_own_array bridge not yet in draken_bridge.h). Callers updated: registrar/text.pyx (replace), implementations/utility.pyx (cosine_similarity + cosine_distance). 46 new tests, 2637/2637 draken native tests green.
> E.16b: Phase 15b complete — draken_vector_own_array bridge added to draken_bridge.h + draken_native.cpp; vector_split_native.cpp consumer ported; vector_split.pyx deleted. Bridge wraps raw C buffers (parent_offsets + child string slots + child arena + parent validity) with RAII; builds child consolidated block [DrakenStringArena|slots|arena] matching draken_vector_own_string layout; child elements always valid (no per-element null bitmap). Consumer: two-pass bytewise scan (pass 1 counts segments + arena upper bound; pass 2 emits draken_build_string_slot with inline/extern dispatch). All buffer cleanup manual (OwnedBuffer internal to draken_native.cpp, not visible to consumers). Caller updated: implementations/text.pyx redirects single-char-delimiter fast path to new nanobind module. 29 new tests, all green.
> Refer to `07_consumer_contract.md` and `09_delivery.md` for context.

---

## §1 Inventory

### 1.1 Size (verified by fresh grep, 2026-05-23)

Counting unit: **cimport statements** (each `from X cimport a, b` = 1 statement; multi-symbol).
Python import counting: **import statements**.

| Binding class | Count | Bridgeable without rewrite? |
|---|--:|---|
| `cimport draken.core.buffers` — struct ABI | **95** | Yes — `buffers.pxd` already exists; survives rebuild. |
| cimport of per-type **cdef classes** | **274** | **No.** nanobind emits no `.pxd`; every site rewrites. |
| Python `import draken.*` | **~147** | Rewrite as nanobind surface fills in ("function not signature"). |
| **Total** | **~516** | |

Files touched: **110 opteryx + 5 rugo** = 115 (by-cimport count; Python-import overlaps).

### 1.2 Cimport sites by old module (drives rewrite order)

| Old module | cimport statements | Primary symbols imported | New surface |
|---|--:|---|---|
| `draken.core.buffers` | **95** | `DrakenVector`, `DrakenStringArena`, `DrakenStringSlot`, `DrakenFixedBuffer`, `DrakenVarBuffer`, `DrakenArrayBuffer`, `DrakenConstantStringPayload`, `DRAKEN_*` tags, `str_data`, `str_length`, `alloc_var_buffer`, `draken_vector_from_dense` | **SURVIVES** — `buffers.pxd` (frozen). No rewrite needed for struct access; rewrite needed for call sites that construct/return old Cython objects. |
| `draken.vectors.string_vector` | **57** | `StringVector`, `StringVectorBuilder`, `from_packed_dict`, `from_dict_buffers`, `make_string_dict_only`, `_StringVectorCIterator`, `_varbuffer_to_string_arena`, `StringElement` | New vector construction ops + `DrakenStringArena` direct struct manipulation (§2). |
| `draken.vectors.integer64_vector` | **45** | `Integer64Vector`, `from_sequence as int64_from_sequence`, `from_decoded as int64_from_decoded`, `from_packed_dict as int64_from_packed_dict`, `make_int64_dict_only` | `draken_vector_from_dense/constant/dict`, `draken_hash`, arithmetic ops. |
| `draken.vectors.bool_vector` | **38** | `BoolVector`, `bool_vector_from_bits` | `draken_vector_from_dense/constant/dict`, bool ops. |
| `draken.vectors.vector` (base) | **25** | `Vector`, `NULL_HASH`, `mix_hash`, `simd_popcount` | `DrakenVector` struct + `draken_hash`. `NULL_HASH`/`mix_hash`/`simd_popcount` — expose from `ops/hash.h` or `src/cpp/simd_hash.h`. |
| `draken.vectors.array_vector` | **19** | `ArrayVector`, `array_vector_from_parts` | `DrakenArrayBuffer` struct, `draken_vector_from_dense` with `DRAKEN_ARRAY`. |
| `draken.vectors.float64_vector` | **18** | `Float64Vector` | `draken_vector_from_dense/constant/dict`, float ops. |
| `draken.morsels.morsel` | **13** | `Morsel` | nanobind `Morsel` — but compiled hot-path consumers that call `Morsel` at C level need the extraction pattern (§2). |
| `draken.vectors.timestamp_vector` | **10** | `TimestampVector`, `timestamp_dict_from_raw` | `DRAKEN_TIMESTAMP64`, `draken_vector_from_dense/constant/dict`. |
| `draken.vectors.integer{8,16,32}_vector` | **18** (6 each) | `Integer8/16/32Vector` | `DRAKEN_INT8/16/32`, `draken_vector_from_dense/constant/dict`. |
| `draken.vectors.date32_vector` | **6** | `Date32Vector` | `DRAKEN_DATE32`, `draken_vector_from_dense/constant/dict`. |
| `draken.interop.vector_sequence` | **6** | `vector_from_sequence` | `draken.draken_native.vector_from_sequence` Python call at the boundary. |
| `draken.vectors.float32_vector` | **5** | `Float32Vector` | `DRAKEN_FLOAT32`, `draken_vector_from_dense/constant/dict`. |
| `draken.morsels.align` | **5** | `align_tables` | New `align_tables` built on `take` op — **one of the riskier sites** (see §3). |
| `draken.vectors.vector_vector` | **3** | `VectorVector` | `DRAKEN_ARRAY` with nested child? Investigate. |
| `draken.vectors.scalar_constructors` | **3** | `from_scalar` | `vector_from_constant` in nanobind; or `draken_vector_from_constant` + `draken_zero_sel`. |
| `draken.vectors.time_vector` | **1** | `TimeVector` | `DRAKEN_TIME32`/`DRAKEN_TIME64`, `draken_vector_from_dense`. |
| `draken.vectors.null_vector` | **1** | `NullVector` | `DRAKEN_NULL`, `draken_vector_from_dense`. |
| `draken.core.var_vector` | **1** | `alloc_var_buffer` | **Internalize** — the one caller (`vector_cast_int64_to_timestamp`) must be re-homed to use a proper draken constructor. Do not re-expose (per `07`). |

### 1.3 Python `import draken.*` sites by module

| Old module | import sites | Mapped to new surface |
|---|--:|---|
| `draken.interop.vector_sequence` | 24 | `draken.draken_native.vector_from_sequence` |
| `draken.vectors.string_vector` (module ref) | 17 | `draken.draken_native` + direct attribute access |
| `draken.vectors.bool_vector` | 12 | `draken.draken_native.vector_from_bool_sequence` etc. |
| `draken.vectors.string_vector` (class) | 10 | `draken.draken_native.Vector` (type check = `v.type == STRING`) |
| `draken.morsels.morsel` | 13 | `draken.draken_native.Morsel` |
| `draken.vectors.timestamp_vector` | 8 | `draken.draken_native.vector_timestamp_from_sequence` etc. |
| `draken.vectors.date32_vector` | 8 | `draken.draken_native.vector_date32_from_sequence` etc. |
| `draken.vectors.integer64_vector` | 6 | `draken.draken_native.vector_from_sequence` (auto-detect) etc. |
| `draken.vectors.float64_vector` | 5 | `draken.draken_native.vector_float64_from_sequence` etc. |
| `draken.vectors.time_vector` | 3 | `draken.draken_native.vector_time32_from_sequence` etc. |
| `draken.vectors.scalar_constructors` | 6 | `draken.draken_native.vector_from_constant` etc. |
| `draken.vectors.vector` | 2 | `draken.draken_native.Vector` |
| `draken.vectors.null_vector` | 2 | `draken.draken_native.vector_null_from_length` |
| `draken.vectors.interval_vector` | 2 | `draken.draken_native.vector_interval_from_sequence` etc. |
| `draken.vectors.decimal_vector` | 4 | `draken.draken_native.vector_decimal_from_sequence` etc. |
| `draken.vectors.arithmetic_kernels` | 1 | `draken.draken_native.Vector.add/sub/mul/div` |
| `draken.vectors.array_vector` | 1 | `draken.draken_native.vector_array_from_sequence` |
| `draken.vectors.integer32_vector` | 1 | `draken.draken_native.vector_int32_from_sequence` etc. |
| `draken` (top-level `Morsel`) | 1 | `draken.draken_native.Morsel` |

### 1.4 Dead surface (confirmed dropped)

- `draken.interop.arrow` / `from_arrow` — **no consumer**; removed cleanly.
- `to_arrow` — **KEEP** (export only, confirmed in `07`).
- Any symbol from the old `.pyx` files that doesn't appear in the consumer grepped above — **not re-homed**.
- `arithmetic_kernels.get_arithmetic_kernel` dispatch table — collapsed into `Vector.add/sub/mul/div/mod` dispatch on the nanobind handle; one caller (`arithmetic_dispatch.pyx`) replaces the whole indirection.

---

## §2 Binding Pattern for Compiled Consumers (C′ — canonical as of E.2)

> **E.2 revision (2026-05-23):** §2.2 previously documented a two-layer `.pyx`+C++ pattern
> (Layer A = typed `cdef` kernels, Layer B = nanobind wrapper calling the `cdef public`
> symbol). That pattern was the working but vestigial design from E.1. After proving C′ in
> the bitwise pilot (E.2), the `.pyx` layer is dropped entirely. C′ is now canonical.
> The `.pyx`+`cdef` pattern is the new anti-pattern (§2.3, Anti-pattern C).

### 2.1 The central gap: extracting DrakenVector from Python Vector handle

The 274 cdef-class cimport sites work today by calling C-level methods on Cython
`cdef class` objects (e.g., `(<Vector>v).unified()` returns `DrakenVector*`). nanobind
emits no cimportable `.pxd`, so this mechanism is gone.

**C′ replaces the entire consumer with a single pure C++ file.**

### 2.2 Canonical pattern C′: pure nanobind C++ consumer (no `.pyx` layer)

Every compiled consumer is one `.cpp` file that contains both the computation kernel and
the Python entry point. No `.pyx` file. No `cdef`. No Cython compilation step.

```cpp
// opteryx/compiled/nanobind/my_op.cpp — the entire consumer.
#include <Python.h>
#include <nanobind/nanobind.h>
#include "core/buffers.h"
#include "core/draken_bridge.h"  // draken_vector_unwrap, draken_vector_own_raw
#include "ops/int_bitwise.h"     // (or the relevant op header)

namespace nb = nanobind;

NB_MODULE(my_op, m) {
    m.def("my_function", [](nb::object vec) -> nb::object {
        const DrakenVector* dv = draken_vector_unwrap(vec.ptr());
        if (!dv) throw nb::python_error();  // TypeError already set, never segfaults
        VecResult res = draken::ops::some_op(*dv);
        PyObject* out = draken_vector_own_raw(res.data, res.validity, res.length, res.type);
        if (!out) throw nb::python_error();
        return nb::steal<nb::object>(out);
    }, nb::arg("vec"), "docstring");
}
```

**Build (one `.cpp` → one extension):**

```python
Extension(
    "opteryx.compiled.nanobind.my_op",
    sources=[
        "opteryx/compiled/nanobind/my_op.cpp",
        "draken/core/vector_alloc.cpp",         # draken_identity_sel
        "third_party/nanobind/src/nb_combined.cpp",
    ],
    include_dirs=include_dirs + [MIMALLOC_INCLUDE, "third_party/nanobind", ...],
    extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
    extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,  # dynamic_lookup for bridge syms
    extra_objects=[MIMALLOC_OBJ],
    language="c++",
)
```

**RTLD_GLOBAL prerequisite:**

`draken_vector_unwrap` and `draken_vector_own_raw` are implemented in `draken_native.so`.
For consumer extensions to resolve these symbols at import time:

1. `draken/__init__.py` loads `draken_native` with `RTLD_GLOBAL` (done in E.2).
2. Consumer extensions link with `-undefined dynamic_lookup` (macOS) or
   `--allow-shlib-undefined` (Linux).
3. `draken` must be imported before any consumer extension.

**The `.pyx` layer is dropped entirely.** The `cdef public` symbol / forward-declare
dance from E.1 is gone. Zero Cython in each consumer. Zero Cython compile step.

**Constructing output vectors:**

- Op returns a `VecResult` → call `draken_vector_own_raw(res.data, res.validity, res.length, res.type)`.
- Op produces a **string column** (new slots + arena) → call `draken_vector_own_string(slots, arena, arena_len, validity, length, type)`. The `type` parameter must be `DRAKEN_VARCHAR`, `DRAKEN_NVARCHAR`, or `DRAKEN_VARBINARY`; any other value raises `ValueError`. This is the canonical exit-point for all string-producing C++ consumers. Populate slots with `draken_build_string_slot` (in `core/string_slot.h`). All three buffers must be draken_malloc'd; ownership transfers unconditionally on call entry. Storage layout is identical across all three types; the type tag drives op semantics (LENGTH returns codepoints for NVARCHAR, bytes for the others).
- For `draken_vector_own(VecResult)` (C++ RAII, no raw-pointer hand-off): available but
  only from code that can include `draken_native`-internal headers. Prefer `draken_vector_own_raw`
  in consumer extensions.
- `res.selection` from C++ op kernels is always the shared identity pointer (`owns_selection == false`);
  no manual free needed.

**Live example (E.2):** `opteryx/compiled/nanobind/vector_bitwise.cpp` — 6 ops, 1 file, ~100 LOC.

### 2.3 Anti-patterns — do not repeat

Three patterns have been tried and rejected. Each is documented so future agents
don't recreate them.

**Anti-pattern A — `cdef extern` with `object` parameter in `.pyx` (E.0/E.1, DELETED):**
```cython
cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(object vec)   # ← BANNED
```
`nb::object` is a C++ type; it must never appear in Cython `cdef extern`. §3 violation.

**Anti-pattern B — `cpdef object` entry points in `.pyx` (original §2, DELETED):**
```cython
cpdef object my_kernel(object vec, ...):   # ← BANNED: object params/returns in .pyx
```
This was documented as the canonical pattern in the original §2. It puts Python object
handling inside compiled Cython — §3 violation. The whole §2 was rewritten in E.1.

**Anti-pattern C — `.pyx` cdef-kernel + C++ nanobind wrapper (E.1 pattern, DEPRECATED):**
```
vector_sum.pyx     # cdef public int64_t vector_sum_kernel(const DrakenVector*, ...)
vector_sum.cpp     # NB_MODULE: forward-declares vector_sum_kernel, calls it
```
This was the E.1 canonical pattern. It is correct (no §3 violation) but vestigial:
the `.pyx` layer just forwards a typed call to a C++ op — zero logic justifies the
extra Cython compile step, the 79-file `vector_ops/` merger conflict, and the `cdef public`
linkage subtlety. C′ (pure nanobind C++) achieves the same result in one file per consumer.
The E.1 POC (`draken/poc/poc_e1_kernel.pyx` + `poc_e1_nanobind.cpp`) remains as a
historical artifact. Do not use it as a template for Phases 2+.

**`binding_poc.pyx` audit note (pre-approved exception):**
`binding_poc.pyx` (retained, E.0 POC) has `<object>` casts at L128–130 boxing C scalars
into Python ints on the return path. These are §02-acceptable edge-boxing (not parameter
or return-type declarations). Pre-approved; not violations.

### 2.4 `morsels.align` (5 join consumers — highest risk)

`align_tables` today takes two index arrays and materialises rows from both input
morsels. In the new model it is built on `Vector.take` (nanobind) or on `i64_take` /
`str_take` / etc. directly. The 5 join consumers (`nested_loop`, `asof`,
`hashed_inner`, `outer`, `non_equi`) all cimport it — they are a single rewrite unit.
Risk: the join operators are among the most complex files; budget them last in the
consumer rewrite sequence (after simpler vector ops prove the pattern).

---

## §3 Sequenced Rewrite Plan

### 3.1 Dependency ordering principle

`core.buffers` (ABI struct) → base vector extraction pattern → pilot type consumers
(int64) → string consumers (high count + RAII complexity) → bool → float64 →
timestamp/date → array → Python-import-only sites → morsel.align (joins) → tail types.

Each **type** is a unit: C++ kernel gated (per `09` §3) THEN all that type's compiled
consumers rewritten and diff-reviewed. The engine does not compile until all are done.

### 3.2 Phase breakdown

#### Phase 0 — Bridge plumbing + correct POC (prerequisite for all consumer rewrites)

| Item | Effort | Status |
|---|---|---|
| `draken/core/draken_bridge.h` + `draken_vector_unwrap` in `draken_native.cpp` | 2h | ✅ DONE (Milestone B.1/D.1) |
| `draken_vector_own(VecResult)` + `draken_vector_own_raw(...)` wrappers | 2h | ✅ DONE |
| Revise `draken_bridge.h` comment: remove wrong `cdef extern (object vec)` pattern, add C++-only usage note | 0.5h | ✅ DONE (E.1) |
| Revise E.0 scoping doc §2: replace anti-pattern with §3-aligned model; update estimates | 1h | ✅ DONE (E.1) |
| New E.1 POC: `poc_e1_kernel.pyx` (zero object) + `poc_e1_nanobind.cpp` + `run_poc_e1.py` | 3h | ✅ DONE (E.1) |
| Build + run E.1 POC; confirm TypeError raised on non-Vector, all assertions pass | 1h | → next step |
| Update `setup.py` stale `draken/src/*` references to `draken/core/*` (lines 476–478, 524, 536, 804, 831, 851) | 1h | **Flag to architect**: `draken/src/` does not exist |
| **Total Phase 0** | **~10h** | |

#### Phase 1 — core.buffers consumers (95 sites; many are also Phase 2 targets)

These already bind the struct correctly via `buffers.pxd`. The 95 sites need no
structural change — their cimport pattern survives. They do need updating where they
**construct** old Cython-class objects or call methods that no longer exist. This is
caught per-type in Phase 2–6 (the cimport here survives; the per-class usage does not).

Effort: 0h for the cimport itself; consumed by per-type phases below.

#### Phase 1 (E.2) — C′ pilot: 6 bitwise consumer functions ✅ DONE

**Cost datapoint (actual, 2026-05-23):**

| Item | LOC | Notes |
|---|---|---|
| `draken/ops/int_bitwise.h` (kernels + dispatch) | ~240 | 6 ops × 4 types + helpers; all static inline |
| `opteryx/compiled/nanobind/vector_bitwise.cpp` (consumer) | ~100 | 6 functions, 1 NB_MODULE |
| `hash.h` additions (TypeOps slots + OpsTable entries + entry fns) | ~60 | |
| Tests (`test_int_bitwise.py` + `test_bitwise_parity.py`) | ~380 | hypothesis + parametrized |
| Doc update + setup.py + `draken/__init__.py` | ~50 | |

**Per-consumer unit (C′):** 1 `.cpp` file, ~15–20 LOC per function (unwrap + call + wrap).
No `.pyx` file. No Cython compile step. No `cdef public` / forward-declare dance.

**Surprises / flags for Phases 2+:**
- RTLD_GLOBAL mechanism required: `draken/__init__.py` must load `draken_native` with
  `RTLD_GLOBAL` before any consumer extension is imported. See §2.2 — this is now wired.
- Op headers must be self-contained (no hash.h dependency) to avoid simd_hash.cpp link req.
  Achieved by putting dispatch entry fns in `int_bitwise.h` with a switch, not the OpsTable.
- Hash.h OpsTable also wired with bitwise slots (for future hash.h-based consumers) but
  the consumer itself uses `draken::ops::bitwise_*` directly from `int_bitwise.h`.

#### Phase 1 (E.3) — C′: abs / sign / sqrt / round (4 math consumer functions) ✅ DONE

**Cost datapoint (actual, 2026-05-23):**

| Item | LOC | Notes |
|---|---|---|
| Part A0: `dev/vendor_boost_math.py` + vendor run | ~100 | 14 modular repos @ boost-1.86.0 |
| Part A: `draken/ops/float_math.h` (kernels + dispatch) | ~230 | 4 ops × 6 types + helpers; all static inline |
| Part B: `opteryx/compiled/nanobind/vector_math.cpp` (consumer) | ~90 | 5 functions (round + round_digits), 1 NB_MODULE |
| Tests: `test_float_math.py` | ~280 | 63 tests; hypothesis + edge cases; 1951 passing |
| setup.py + deleted .pyx files | ~30 | |

**Surprises / flags for Phases 2+:**
- **boost::math::round is half-AWAY-from-zero**, not half-to-even. The ticket premise was
  incorrect. Used the 2^52 trick (`(x + copysign(2^52, x)) - copysign(2^52, x)`) instead,
  which relies on IEEE 754 FE_TONEAREST (platform default = half-to-even). This is correct,
  efficient (~1 branch), and well-understood. boost stays vendored for future log/exp/trig.
- `sign(NaN)` → null: INT8 cannot represent NaN; NaN float rows are marked null in the
  output validity bitmap. The float_sign kernel allocates validity unconditionally for float
  input (NaN may inject new nulls even when input is all-valid).
- `abs(INT*_MIN)` wraps per C convention (confirmed by architect before implementation).
- `sqrt` on integer types raises for negative values; float types produce NaN (IEEE 754).
- `round` on integer types is identity (no-op); always outputs same int type.
- `round`/`sqrt` on float types always output DRAKEN_FLOAT64 regardless of input width.
- The 4 deleted .pyx files also required deleting the stale generated `vector_ops.pyx` to
  force regeneration (mtime-based skip prevented auto-regen after file removal).
- `draken/core/_boost_math_smoke.cpp` written but NOT wired as a Python extension;
  serves as a standalone compile + correctness check for the vendored slice.

#### Phase 1 (E.4) — C′: base64/85 codec + bool ops (8 consumer functions) ✅ DONE

**Cost datapoint (actual, 2026-05-23) — pure C′, no Part A:**

| Item | LOC | Notes |
|---|---|---|
| `opteryx/compiled/nanobind/vector_codec.cpp` (consumer) | ~265 | 4 functions (b64/b85 encode/decode), 1 NB_MODULE; single-block arena allocation |
| `opteryx/compiled/nanobind/vector_bool_ops.cpp` (consumer) | ~180 | 4 functions (from_int8_mask, from_inverted_bitmap, all_true, and_chain), 1 NB_MODULE |
| Tests: `test_vector_codec.py` + `test_vector_bool_ops.py` | ~370 | 58 tests; round-trip, stdlib parity, null TVL, Kleene 3VL; 58/58 passing |
| setup.py + deleted .pyx files + caller import updates | ~60 | 3 pyx deleted; 4 caller files updated |

**Surprises / flags for Phases 2+:**
- **`b85_decoded_size(L)` undercounts actual decoded bytes** for L not divisible by 5 (integer
  division drops the partial-group tail). Pass 1 allocated no arena space for a slot; pass 2
  produced `actual_len > STR_INLINE_MAX` and branched to the extern path with `out_arena=nullptr`
  → SEGFAULT. Fix: `b85_decoded_size_wrap` replaced with `(L/5)*4 + (L%5 >= 2 ? L%5-1 : 0)`.
  Any future decode consumer using a "max size" function should verify the formula for partial
  groups against the actual codec output at all boundary lengths.
- **Output is always DENSE** (identity-selection). Dict-preserving output for codec would require
  a new `extern "C"` bridge function (Part A) — deferred. Semantics identical; layout differs
  only for highly-repeated inputs.
- **`draken_native` must export bridge symbols** (uppercase `T` in `nm`). Compiling with
  `-fvisibility=hidden` hides them — do NOT use that flag for `draken_native`. Consumer
  extensions are compiled with `-fvisibility=hidden` + `-undefined dynamic_lookup`; symbols
  are resolved from the RTLD_GLOBAL flat namespace at runtime.

#### Phase 1 (E.5) — C′ + Part A: array element-reduction ops (ANY / ALL, 8 consumer functions) ✅ DONE

**Cost datapoint (actual, 2026-05-23):**

| Item | LOC | Notes |
|---|---|---|
| Part A: `draken/ops/array_reductions.h` (kernels + dispatch) | ~344 | 8 ops × INT64 + STRING; ArrScalar carrier; templated any/all kernels; null/empty-row SQL semantics |
| Part A: `draken/core/draken_bridge.h` + `draken_native.cpp` additions | ~40 | `draken_array_child_unwrap` bridge function (new RTLD_GLOBAL symbol) |
| Part B: `opteryx/compiled/nanobind/vector_array_reduce.cpp` (consumer) | ~172 | 8 functions, 1 NB_MODULE; `build_scalar` helper; `ARR_REDUCE_FN` macro; `nb::arg().none()` |
| Part B: Tests (`test_array_reduce.py`) | ~378 | 50 tests; hypothesis; null TVL; vacuous-truth empty rows; bit-boundary crossing |
| Part B: setup.py + deleted .pyx files + `vector_ops.pyx` regen | ~30 | 8 .pyx deleted; regenerated consolidated module |
| Part 0: test helper `pytest.skip` → `raise RuntimeError` (3 files) | ~6 | Fail loud on missing `.so` |

**Time split:**
- Part 0 (test helper fixes): ~0.25h
- Part A (draken ops layer): ~3h (bridge extension + templated kernels + string comparison wiring)
- Part B (consumer layer): ~3h (nanobind module + tests + setup.py + deleted files)
- Part C (doc update): ~0.25h

**Per-consumer unit (C′ + Part A):** same ~15–20 LOC per function in consumer. Part A cost amortises across all 8 ops in one header.

**Surprises / flags for Phases 2+:**
- `draken_bridge.h` required a new bridge symbol (`draken_array_child_unwrap`) for structural array-child access. Decision surfaced to architect — accepted as structural, not op-specific.
- `str_eq_slots` lives in `ops/string_compare.h`, not `core/string_slot.h`. Op headers including string comparison must include `ops/string_compare.h` explicitly.
- Old allop `.pyx` emitted **False** for null rows and **False** for empty rows. E.5 implements correct SQL: null row → **NULL** (TVL), empty row → **True** (vacuous). Tests verify both against the old behaviour delta.
- `vector_array_from_sequence` does not support None *elements within* rows (only None rows). Null-element-within-row tests removed; the kernel handles this path but the factory cannot exercise it via Python.
- 8 deleted anyop/allop `.pyx` files required deleting stale `vector_ops.pyx` to force regeneration (same mtime-skip issue as E.3).
- `nb::arg("literal").none()` required on all 8 functions — nanobind rejects Python `None` passed to `nb::object` without `.none()`.

#### Phase 1 (E.6) — pure C′ batch: string length/emptiness + array element count ✅ DONE

**Cost datapoint (actual, 2026-05-23) — pure C′, no Part A:**

| Item | LOC | Notes |
|---|---|---|
| `opteryx/compiled/nanobind/vector_accessors.cpp` (consumer) | ~165 | 4 functions (str_length, is_empty, is_not_empty, array_length), 1 NB_MODULE |
| `draken/tests/native/test_vector_accessors.py` | ~195 | 32 tests; null TVL; UTF-8 byte vs codepoint; bit-boundary; type errors |
| setup.py + deleted .pyx files + file splits + caller updates | ~80 | 3 .pyx deleted; vector_get_element.pyx split into 2; 3 caller files updated |

**Surprises / flags for Phases 2+:**
- **`vector_get_element.pyx` contained 5 functions**, not 1 (ticket assumed 1). The file holds JSON extraction ops (`vector_json_extract_text`, `vector_json_extract_variant`) out of scope, plus three subscript/access ops. File was split into `vector_json_extract.pyx` (JSON) and `vector_map_access.pyx` (subscript + char-at-index) rather than deleted wholesale.
- **`vector_map_access_string` not ported to C++**: producing a `DrakenStringArena` output requires the single-block allocation pattern used internally in `draken_native.cpp`. No bridge function exists for this — `draken_vector_own_raw` takes `data` and `validity` separately, which would double-free an embedded validity bitmap. Deferred; stays Cython.
- **`vector_length.pyx` was NOT a duplicate** of the pre-rebuild nanobind `vector_length` extension at `src/cpp/vector_length_native.cpp` (module `list_length`). The old extension operates on raw buffer-protocol offsets; the `.pyx` is an ArrayVector→INT64 per-row element count. Separate ops.
- **DRAKEN_ARRAY DrakenVector layout confirmed**: `vec.data = int32_t* offsets` (not `DrakenArrayBuffer*`). Element count at logical row i = `offsets[selection[i]+1] - offsets[selection[i]]`. Old `.pyx` accessed `DrakenArrayBuffer.ptr.offsets` directly (bypassed selection); C++ implementation uses selection correctly.
- **`vector_string_length` null semantics mismatch with SQL standard**: null input row → 0, no output validity. This is the old `.pyx` behaviour (preserved for parity). SQL `LENGTH(NULL)` should return NULL; the discrepancy exists in the old code and is carried over.
- **`vector_json_extract_*.pyx` has banned `hasattr` calls** (CLAUDE.md §8): `hasattr(value, "mini")` and `hasattr(value, "as_list"/"as_dict")`. Inherited verbatim from the pre-split file. Flagged; fixing is out of scope for E.6.
- **pre-existing build failure**: `opteryx/compiled/vector_ops/vector_ops.pyx` and many other `.pyx` files cannot be compiled because draken v1 `.pxd` files (e.g. `draken/vectors/bool_vector.pxd`) were removed with the draken rebuild. `make c` and `make q` fail for this reason (pre-existing, not caused by E.6). The nanobind extension was compiled directly and 2114/2114 draken tests pass.
- **`vector_accessors.cpp` required `third_party/cyan4973` in include path** (for `xxhash.h` via `string_slot.h`). setup.py pattern already includes this via `include_dirs`; no change needed.

**Per-consumer unit (pure C′, no Part A):** ~40 LOC per function in consumer including error handling and result wrapping. Total batch lower than E.4 due to simpler access pattern (no arena building).

#### Phase 1 (E.7 REVISED) — String type family + `draken_vector_own_string` type param + null-TVL fix ✅ DONE

**Scope revision (2026-05-23):** E.7 was revised from a bridge-only fix to incorporate the
"Unicode is opt-in" architectural decision. Full scope: mechanical rename of the old string
tag to DRAKEN_VARCHAR, two new type tags (NVARCHAR=63, VARBINARY=64),
new ingestion factories, per-type LENGTH dispatch, bridge type param, and POC coverage.

**Cost datapoint (actual, 2026-05-23) — type-family revision + foundation fix:**

| Item | LOC | Notes |
|---|---|---|
| Part A: mechanical rename of old string tag to DRAKEN_VARCHAR (buffers.h, buffers.pxd, all consumers, all generated .cpp/.c) | ~bulk sed | 0 grep hits in draken/ opteryx/compiled/ rugo/ |
| Part B: new tags DRAKEN_NVARCHAR=63, DRAKEN_VARBINARY=64 in buffers.h + buffers.pxd + ABI guard label | ~10 | ABI integer unchanged (60 stays DRAKEN_VARCHAR) |
| Part C: ingestion — `vector_from_nvarchar_sequence` + `vector_from_bytes_sequence` in draken_native.cpp | ~80 | NVARCHAR: patch type tag; VARBINARY: full PyBytes_AsStringAndSize path |
| Part C: readback — `row_bytes()`, updated `__getitem__`/`to_pylist`/`child_elem_to_py` for VARBINARY | ~40 | Returns Python `bytes` objects for VARBINARY rows |
| Part D: `vector_accessors.cpp` — NVARCHAR codepoint dispatch + null-TVL fix | ~30 | count_utf8_codepoints; out_validity copy + tail-mask; is_varchar_family check |
| Part D: tests — `test_vector_accessors.py` NVARCHAR + VARBINARY coverage | ~75 | café→4 cp, 日本→2 cp, VARBINARY byte-length, null propagation |
| Part E: `draken_vector_own_string` — add DrakenType type param + validate | ~15 | Raises ValueError on non-string type; sa->type set from param |
| Part E: `draken/ops/hash.h` — NVARCHAR + VARBINARY OpsTable entries (identical to VARCHAR) | ~20 | Identical storage → same op ptrs |
| Part E: `draken/poc/poc_e7_nanobind.cpp` + `run_poc_e7.py` — NVARCHAR + VARBINARY POC tests | ~100 | make_nvarchar_vec, make_bytes_vec, stress_construct_destroy, all three type tags |
| Part A foundation: `draken/core/string_slot.h` — `draken_build_string_slot` helper | ~20 | Static inline; XXH3 hash + delegates to str_init_inline/extern |
| Part A foundation: `draken/core/draken_bridge.h` — `draken_vector_own_string` declaration | ~45 | Full ownership contract; slot-format obligations |
| Part A foundation: `draken/draken_native.cpp` — `draken_vector_own_string` implementation | ~75 | RAII-safe; single consolidated-block layout |

**`draken_vector_own_string` is the canonical exit-point for string-producing C++ consumers.**

All future C++ consumers that produce a new string column MUST use `draken_vector_own_string`
with the appropriate type tag. Call `draken_vector_own_string(slots, arena, arena_len, validity, length, type)`
where `type` is one of `DRAKEN_VARCHAR`, `DRAKEN_NVARCHAR`, or `DRAKEN_VARBINARY`.
Storage is identical across all three; the tag drives op semantics. Ownership is total.

**String type family summary (E.7 REVISED):**
- `DRAKEN_VARCHAR = 60` — default; ASCII semantics; byte-length ops. `vector_from_string_sequence` produces VARCHAR.
- `DRAKEN_NVARCHAR = 63` — opt-in UTF-8; codepoint-length ops (LENGTH counts non-continuation bytes). `vector_from_nvarchar_sequence`.
- `DRAKEN_VARBINARY = 64` — opaque bytes; byte-length ops; to_pylist returns Python `bytes`. `vector_from_bytes_sequence`.

**Surprises / flags for subsequent string-producing consumers:**
- **`draken_vector_own_string` type param**: all callers that pass `type` must use one of the three
  string family constants. Passing e.g. `DRAKEN_INT64` raises `ValueError` at runtime.
- **NVARCHAR and VARBINARY OpsTable entries** are identical to VARCHAR (same storage, same hash/compare/gather ops). `LENGTH` dispatch differs only in `vector_accessors.cpp`, not in the OpsTable.
- **Validity tail-mask is required** when copying input validity to output: set bits beyond
  logical row count `n` to 0 in the last byte. Missing this causes phantom-valid bits.
- **`vector_string_length` null-TVL**: the old behaviour (null → 0, no validity) was a
  regression from SQL standard. E.7 fixes it. Any code that depended on
  `LENGTH(null_col) == 0` (rather than `IS NULL`) will see behavioural change.
- **`draken_build_string_slot` helper** is in `core/string_slot.h` (not the bridge header)
  since it depends on XXH3 which is already included there.

#### Phase 1 (E.8) — C′ with `own_string` bridge: hex encode/decode + MD5/SHA digest ✅ DONE

**Cost datapoint (actual, 2026-05-23) — first real consumers using `draken_vector_own_string`:**

| Item | LOC | Notes |
|---|---|---|
| `vector_hash_codec.cpp` — one NB_MODULE, 6 functions (hex_encode, hex_decode, md5, sha1, sha256, sha512) | ~280 | Two applier templates: `digest_apply` (fixed-length) + `vhex_apply` (variable-length); shared validity copy + null check helpers |
| `test_vector_hash_codec.py` — 49 tests covering all 6 functions | ~260 | Known fixtures (MD5 RFC 1321, SHA-1 test vectors), stdlib parity, null TVL, determinism, TypeError |
| `setup.py` — new Extension block (E.8), single mabel unity-build source | ~40 | |
| Deleted: `vector_hex.pyx`, `vector_md5.pyx`, `vector_sha.pyx`, `_hash_helpers.pyx` | ~200 removed | |
| `vector_ops.pyx` — 4 include lines removed (auto-generated; updated manually) | 4 removed | |
| `hash_encoding.pyx` — import redirected from `vector_ops` to `vector_hash_codec` | 1 | |
| `third_party/crypto/md5.cpp` — bug fix: truncated implementation (missing 4 HH ops, all 16 II ops, state update) | +24 | Pre-existing bug; produced initial-state output for all inputs |

**Surprises / flags for subsequent string-producing consumers:**

- **`_base16.h` uses C99 `restrict` keyword** — invalid in C++. Worked around by defining
  `restrict __restrict__` before the `extern "C"` include block. The `#define restrict` must
  be guarded and undefined after the include to avoid polluting subsequent C++ headers.
- **`_base16.c` is a unity build** — it `#include`s `_base16_dispatch.c`, `_base16_neon.c`,
  `_base16_avx2.c` internally. Only `_base16.c` should appear in the `sources` list;
  adding the sub-files separately causes duplicate symbol link errors.
- **`digest_apply` uses `n * hex_len` arena** — pre-allocated for worst case (all rows
  non-null). Null rows don't write to arena, so `arena_used < n * hex_len` when nulls
  are present. `draken_vector_own_string` receives `arena_used` (actual), not the cap.
- **`draken_build_string_slot` computes XXH3 from the bytes pointer passed** — for
  hash outputs, bytes are written to `arena + off` first, then `draken_build_string_slot`
  is called with that same pointer. This ensures slot.hash32 matches what direct ingestion
  would produce for the same hex string, satisfying the slot-determinism acceptance criterion.
- **md5.cpp was truncated** — the vendored implementation was missing Round 3 items 13–16,
  all of Round 4 (II), and the final `state[n] += x` accumulation. The function was returning
  the MD5 initial-state encoding (`0123456789abcdeffedcba9876543210`) for all inputs.
  The existing Cython `vector_md5` call was presumably shadowed by the system libcrypto at
  runtime (macOS `-undefined dynamic_lookup`), masking the bug. Fixed in this milestone.

#### Phase 1 (E.9) — C′ cast cluster: int↔string + string→int + int→timestamp ✅ DONE

**Cost datapoint (actual, 2026-05-23) — mixed in/out direction, ASCII only, no UTF-8 decisions:**

| Item | LOC | Notes |
|---|---|---|
| `vector_casts.cpp` — one NB_MODULE, 4 functions | ~220 | Two-pass int→string (arena sizing); hand-rolled ASCII loops; zero-transform int→timestamp |
| `test_vector_casts.py` — 43 tests covering all 4 functions | ~225 | Round-trip, boundary values (INT64_MIN/MAX, UINT64_MAX), all units, null TVL, fail-loud |
| `setup.py` — new Extension block | ~30 | |
| Deleted: 4 `.pyx` files | ~230 removed | |
| `casts.pyx` — 2 import lines redirected | 2 | |
| `draken_bridge.h` — new `draken_vector_own_timestamp` declaration | ~25 | Third producer bridge function alongside `own_raw` + `own_string` |
| `draken_native.cpp` — `draken_vector_own_timestamp` implementation | ~55 | Parses unit string → TimestampUnit; "days" scales ×86_400_000_000 → MICROSECONDS; sets mandatory LogicalType via `logical_type_intern` |

**Surprises / flags for subsequent consumers:**

- **`draken_vector_own_raw` cannot produce TIMESTAMP64** — the mandatory `LogicalType` descriptor
  is left null, which is a hard error at readback. Required adding `draken_vector_own_timestamp`
  to the bridge. The "do not grow it" comment means per-op functions, not producer-type functions.
- **int→timestamp is zero-transformation in the new model** — the old `.pyx` multiplied/divided
  to normalize to microseconds. The new draken stores in whatever unit the descriptor says, so
  the int64 values are already the correct physical representation. Only the descriptor needs attaching.
- **Empty string parses to 0** — old `.pyx` `parse_int64` loop never executes on length=0; returns
  `sign * 0 = 0`. Matched in new implementation. Document: `CAST('' AS INT) = 0`.

#### Phase 1 (E.12) — C′ temporal conversion cluster: date32↔timestamp + unixtime + floor ✅ DONE

**Cost datapoint (actual, 2026-05-23) — numeric unit math, no strings, pre-epoch floor correction:**

| Item | LOC | Notes |
|---|---|---|
| `vector_temporal_convert.cpp` — one NB_MODULE, 4 functions | ~260 | floor_div for pre-epoch correctness; PyObject_GetAttrString for unit read; ticks_per_day/second helpers; ci_eq for case-insensitive floor unit names |
| `test_vector_temporal_convert.py` — 64 tests covering all 4 functions | ~370 | Epoch/pre-epoch/null TVL/round-trip/leap-year/cross-unit; ValueError/TypeError fail-loud |
| `setup.py` — new Extension block | ~28 | |
| Deleted: 4 `.pyx` files | ~220 removed | |
| `casts.pyx` — 2 import lines redirected | 2 | |
| `temporal.pyx` — 3 import lines redirected | 3 | |

**Surprises / flags for subsequent consumers:**

- **Unit read via Python attribute, not bridge**: `draken_vector_unwrap` returns `DrakenVector*` with no `logical_type` field. Reading the unit from `TIMESTAMP64` input requires `PyObject_GetAttrString(obj.ptr(), "logical_type_unit")` — one Python API call per function invocation (hoisted before the loop, not per-row). The "do not grow it" constraint on `draken_bridge.h` was respected; no new bridge function was needed.
- **C++ `/` is truncation-toward-zero; Python `//` is floor-toward-−∞**: corrected for pre-epoch (negative) timestamps via `floor_div(a, b) = a/b - (((a^b)<0) & (q*b != a))`. All integer-divide ops (timestamp→date32, unixtime, floor_temporal) use this.
- **`draken_vector_own_raw` works for DATE32** (no mandatory LogicalType, unlike TIMESTAMP64). The `timestamp_to_date32` output wraps via `own_raw` correctly.
- **`floor_temporal` month/year excluded**: non-uniform calendar math; would warrant a new draken op if needed. Only second/minute/hour/day (integer-divisible) are in scope and supported here.
- **`vector_unixtime` OLD code had a null-handling bug**: null rows wrote `0` to data but didn't properly propagate the validity bitmap for `Date32Vector` (the old code called `int64_from_sequence` on a view of raw bytes including the 0-null placeholders, relying on the caller to ignore them). New code copies the validity bitmap directly.

#### Phase 1 (E.13) — C′ temporal arithmetic cluster: date_diff + date_part + date_trunc + date_format ✅ DONE

**Cost datapoint (actual, 2026-05-23) — calendar-aware kernels, first Howard Hinnant consumer, first arena-string temporal producer:**

| Item | LOC | Notes |
|---|---|---|
| `draken/ops/temporal_arith.h` — Part A: Howard Hinnant helpers, floor_div, date_trunc_batch, date_diff_batch | ~225 | Proleptic Gregorian for calendar kinds; integer alignment for sub-day kinds; floor_div for pre-epoch semantics; diff_batch normalises both inputs to microseconds before dispatch |
| `vector_temporal_arith.cpp` — one NB_MODULE, 4 functions | ~640 | Largest consumer so far: complex string output (date_format arena), dual-input null TVL (date_diff), unit-hoisting, ci_eq dispatch, format-string validation whitelist |
| `test_vector_temporal_arith.py` — 96 tests covering all 4 functions | ~650 | Leap-year, pre-epoch, quarter boundaries, week truncation (Monday floor), dayofweek 0=Monday epoch=Thursday=3, null TVL propagation, D.8 unit descriptor, all error paths |
| `setup.py` — new Extension block | ~12 | |
| Deleted: 4 `.pyx` files | ~400 removed | |
| `temporal.pyx` — 4 function bodies replaced | ~30 changed | |

**Surprises / flags for subsequent consumers:**

- **dayofweek offset is +3 not +4**: 0=Monday scheme maps Thursday (epoch) to 3. Formula `(((days+3)%7+7)%7)`. EPOCH_WEEKDAY=4 comment in old code was wrong. Week-trunc formula `((days-4)%7+7)%7` is correct (it computes "days back to Monday" not the weekday index).
- **date_format is the first arena-string temporal producer**: validate format string once at entry (whitelist), then `gmtime_r` per row into a 256-byte stack buffer; inline if ≤12 bytes, else dynamic arena with realloc-on-overflow. Large consumers will follow this arena pattern.
- **date_diff normalises inputs to microseconds**: both inputs may have different timestamp units (start=ms, end=us). Normalisation in date_diff_batch via per-unit multiply/divide avoids overflow for realistic date ranges.
- **date_trunc DATE32→TIMESTAMP64**: DATE32 input is promoted to microsecond ticks before the batch kernel; output is always TIMESTAMP64 in microseconds. Matches old vector_date_trunc.pyx behaviour.
- **format string bytes accepted at the Python edge**: `vector_date_format` NB_MODULE binding accepts both `str` and `bytes` format argument via `PyBytes_Check`, consistent with how callers pass format patterns.

#### Phase 2 — `integer64_vector` (45 sites) + pilot type gate ← **next**

45 cimport sites across ~20 files. Follows Milestone-C per-type gate.

**With C′:** each consumer is one `.cpp` file per logical group. The E.2 cost datapoint
suggests ~15–20 LOC per function for the consumer layer (vs ~30 LOC in the old `.pyx`
plus additional `.cpp` for the wrapper). Estimate reduced from Phase 2's E.1 estimate.

| File cluster | Sites | C++ kernel | C′ consumer .cpp | Total |
|---|---|---|---|---|
| `vector_ops/vector_math.pyx`, `vector_round.pyx`, `vector_abs.pyx` | ~9 | 2h | 2h | 4h |
| `vector_ops/vector_date_part.pyx`, `vector_unixtime.pyx` | ~6 | 1.5h | 1.5h | 3h |
| `operators/grouped_aggregate_hashed/_grouped_agg.pyx` | ~3 | 2h | 1.5h | 3.5h |
| `operators/aggregate/ungrouped_agg.pyx` | ~5 | 2h | 1.5h | 3.5h |
| `compiled/structures/column_deserializer.pyx` | ~4 | 1h | 1.5h | 2.5h |
| Remaining ~18 sites across ~10 files | ~18 | 4h | 3h | 7h |
| **Total Phase 2** | **45** | **12.5h** | **11h** | **~23h** (was 25h E.1, 15h E.0) |

#### Phase 3 — `string_vector` (57 sites) — second pilot

Highest count + RAII complexity (German strings, arena, dict encoding). This is the
riskiest type group. String nanobind bindings are more complex (arena ownership, dict
encoding); budget 2–3h per file for the C++ binding layer.

| File cluster | Sites | Kernel rewrite | Nanobind binding | Total |
|---|---|---|---|---|
| `compiled/io/json_rows.pyx`, `csv_rows.pyx` | ~14 | 5h | 5h | 10h |
| `vector_ops/vector_anyop_like.pyx`, `vector_match_against.pyx`, `vector_contains.pyx`, `vector_split.pyx` | ~12 | 5h | 4h | 9h |
| `compiled/structures/column_deserializer.pyx` | ~6 | 2h | 2h | 4h |
| `vector_ops/vector_rlike.pyx`, `vector_ip_in_cidr.pyx`, `vector_sha.pyx` | ~6 | 3h | 2h | 5h |
| `vector_ops/vector_string_slice.pyx`, `vector_string_length.pyx`, `vector_starts_ends.pyx`, `vector_string_emptiness.pyx` | ~8 | 3h | 3h | 6h |
| Remaining ~11 sites | ~11 | 4h | 2h | 6h |
| **Total Phase 3** | **57** | **22h** | **18h** | **~34h** (was 22h) |

#### Phase 4 — `bool_vector` (38 sites)

38 sites; bit-packed semantics but structurally simpler than strings.
Kernel rewrite: 12h. New nanobind binding layer: ~8h across ~15 files.
Estimate: **~20h** (was 12h).

#### Phase 5 — `float64_vector` (18 sites) + `float32_vector` (5 sites)

23 sites combined. Similar structure to int64.
Kernel rewrite: 8h. New nanobind binding layer: ~4h.
Estimate: **~12h** (was 8h).

#### Phase 6 — `vector` base + `morsel` (25 + 13 = 38 sites)

`Vector` base class is imported by nearly every op; `NULL_HASH`/`mix_hash` must come
from `ops/hash.h`. `Morsel` cimport is in 13 files — mostly for type annotations that
become the nanobind `Morsel`.
Kernel layer: 10h. Nanobind binding additions (interleaved with Phases 2–5): ~5h.
Estimate: **~15h** (was 10h).

*Practical note: Phase 6 work is interleaved with Phases 2–5 since every file that
imports `Vector` is also a typed-vector consumer. Track them together, not separately.*

#### Phase 7 — `timestamp_vector` (10) + `date32_vector` (6) + narrow ints (18)

34 sites combined. Temporal and narrow-int consumers are structurally simple.
Kernel rewrite: 10h. New nanobind binding layer: ~5h across ~10 files.
Estimate: **~15h** (was 10h).

#### Phase 8 — `array_vector` (19 sites)

Array type involves offset buffers + child vector; more structural than scalars.
Kernel rewrite: 8h. New nanobind binding layer: ~3h across ~5 files.
Estimate: **~11h** (was 8h).

#### Phase 9 — `morsels.align` (5 sites — join operators) ← **risk flag**

5 join operators each import `align_tables`. These are the most complex files in the
engine. Rewrite `align_tables` once in C++/Cython using `take`, then update 5 call
sites. `align_tables` itself becomes a C++ function called from the nanobind layer.
Estimate: **~10h** (was 8h; +2h for C++ align_tables binding).

#### Phase 10 — `interop.vector_sequence` (6 cimport + 24 Python import sites)

Cimport sites become Python boundary calls to `draken.draken_native.vector_from_sequence`.
Estimate: **~5h** (was 4h).

#### Phase 11 — Tail (scalar_constructors 3, vector_vector 3, time 1, null 1, decimal 4, interval 2 + var_vector 1)

~15 sites + Python-level only decimal/interval consumers.
Estimate: **~8h** (was 6h; +2h for tail-type nanobind binding additions).

#### Phase 12 — Python import sites (~147 statements across ~40 files)

These are Python-level (not compiled Cython hot paths). Each module maps to a
`draken.draken_native.*` function or the `Vector` class. Unaffected by the pattern change.
Estimate: **8h** (unchanged).

### 3.3 Total estimate

> **C′ revision (E.2, 2026-05-23):** E.1 added ~50% for the new nanobind C++ binding layer.
> C′ drops the `.pyx` kernel layer entirely — one file per consumer, ~15–20 LOC per function.
> The per-consumer cost is lower than E.1 (no Cython compile, no cdef public linkage dance).
> Rough adjustment: ~5–10% reduction from E.1 estimates for Phases 2–11.
> Phase 12 (Python imports) is unaffected.

| Phase | E.0 estimate | E.1 estimate | C′ estimate | Notes |
|---|---|---|---|---|
| Phase 0 (plumbing + POC) | 6h | 10h | — | ✅ DONE |
| Phase 1 (C′ bitwise pilot) | — | — | **✅ DONE** | E.2 cost datapoint: ~240 LOC kernel, ~100 LOC consumer |
| Phase 2 (int64 consumers) | 15h | 25h | **~23h** | E.2 savings: no .pyx compile per consumer |
| Phase 3 (string consumers) | 22h | 34h | **~31h** | RAII complexity unchanged |
| Phase 4 (bool consumers) | 12h | 20h | **~18h** | |
| Phase 5 (float consumers) | 8h | 12h | **~11h** | |
| Phase 6 (vector base + morsel) | 10h | 15h | **~14h** | |
| Phase 7 (temporal + narrow ints) | 10h | 15h | **~14h** | |
| Phase 8 (array) | 8h | 11h | **~10h** | |
| Phase 9 (align/joins) | 8h | 10h | **~10h** | complexity unchanged |
| Phase 10 (vector_sequence) | 4h | 5h | **~5h** | |
| Phase 11 (tail) | 6h | 8h | **~7h** | |
| Phase 12 (Python imports) | 8h | 8h | **~8h** | unchanged |
| **Total** | **~117h** | **~173h** | **~151h** | |

Integration risk (big-bang re-green at E): ~15% contingency → **~175h total** (E.1: ~200h).

### 3.4 Riskiest sites (flag for architect review before each phase)

1. **`morsels.align` + 5 join operators** (Phase 9): most complex files; rewriting
   `align_tables` from scratch is a mini-project. Ensure `take` op is fully tested first.
2. **`csv_rows.pyx` / `json_rows.pyx`** (Phase 3): these are IO hot paths with string
   arena manipulation; the German-string rewrite is invasive.
3. **`_grouped_agg.pyx` / `ungrouped_agg.pyx`** (Phase 2/6): aggregation over hashed
   groups manipulates internal struct layout; needs careful diffing.
4. **`column_deserializer.pyx`** (Phase 2/3): touches both int and string paths; high
   cimport count (14 sites in one file).
5. **`arithmetic_dispatch.pyx`** + `get_arithmetic_kernel` (Phase 2/5): dispatch table
   collapses to `Vector.add/sub/mul/div/mod` — confirm no type promotions are lost.
6. **`var_vector` re-homing** (Phase 11): the single call in
   `vector_cast_int64_to_timestamp` must move to a proper draken constructor.

---

## §4 Standalone POC

The POC is at `draken/poc/binding_poc.pyx` (+ `setup_poc.py`). It proves:
- A Cython `.pyx` can `cdef extern from "core/buffers.h"` and `cdef extern from "ops/hash.h"`.
- `draken_vector_from_dense` constructs a `DrakenVector`.
- `draken_hash` (from `ops/hash.h`) runs on that vector.
- The result is read back from the output buffer.
- The whole thing **compiles and runs** with `python setup_poc.py build_ext --inplace`.

See `draken/poc/README_poc.md` for build + run instructions.

**POC also confirms:**
- `draken_vector_unwrap` mechanism requires Phase 0 plumbing (bridge header + `draken_native.cpp`). The POC uses a manually-constructed `DrakenVector` (no Python `Vector` object) to prove struct-bind + op-call; the extraction layer is a Phase 0 follow-on.
- `hash.h` (the dispatch-table entry point) pulls in `int64_predicates.h → carchar_set.hpp` and `int64_gather.h → std::vector` — the standalone build needs the carchar include path to use `draken_hash` directly. The full draken_native build has this (correct `include_dirs` set). The POC uses `int64_reductions.h` directly (self-contained) as the simpler proof.
- **`setup.py` has stale `draken/src/*` path references** (lines 476–478, 524, 536, 804, 831, 851) — `draken/src/` does not exist; correct paths are `draken/core/*`. These are flagged for fix separately (a separate background task has been created).

---

## §5 ClickBench Baseline

| Platform | Status | Notes |
|---|---|---|
| ARM (Apple Silicon dev) | **Required — can be done by agent** | Check out last-green commit, run `make clickbench`, record. Last green was before "removing python from draken" commit. |
| x86 (GCP Cloud Run prod) | **Requires architect** | Per `09_delivery.md` §8 open item — confirm who runs it. |

Last-green commit: **`7d7c19f2`** ("Remove PyArrow ingestion, make Arrow export-only") —
the commit immediately before the draken gutting began (`5d59ecae` "remove draken v1").
Tag it as `milestone-a-baseline` on both platforms.
The Milestone-E re-green must show `make clickbench ≥ this baseline`.

ARM capture (agent can do): `git stash && git checkout 7d7c19f2 && make clickbench`; record output; `git checkout -`.
