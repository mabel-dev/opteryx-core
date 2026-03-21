# Draken Group-By Layered Architecture Design

## Context

The grouped aggregation stack has grown by accretion and then by attempted
refactors that each added structure without removing complexity.

The current file layout:

```
opteryx/compiled/aggregations/
  aggregate_kernels.pyx / .pxd        ← dead code, only used by dead group_by_draken
  group_by_draken.pyx                  ← dead code, never imported in production
  group_by_draken_kernels/
    00_common.pyx                      ← dead code (included by group_by_draken only)
    10_count_star_int64.pyx            ← dead code
    20_count_distinct_int64.pyx        ← dead code
    30_avg_int64_float64.pyx           ← dead code
    90_factory.pyx                     ← dead code
  kernels/
    key_serialization_zpp.pyx          ← live: key packing/unpacking via zpp_bits
    dictionary_keys.pyx                ← live: vector type readers and dict helpers
    constant_keys.pyx                  ← live: constant-key ingest path
    groupby_finalize_kernels.pyx       ← live: output vector reconstruction
    groupby_telemetry.pyx              ← live: telemetry helpers (91 lines)
  carchar_group_state_engine.pyx       ← live: 4961 lines, the actual engine
```

The `kernels/` subdirectory was introduced to decompose the engine. It succeeded
partially: real code was extracted. But it also created a nested import surface
with confusing names (`dictionary_keys` is not about dictionaries specifically,
`groupby_telemetry` is 91 lines that could live next to what they measure,
`constant_keys` is 327 lines that are only called from one place).

The result is eight source files where four would do, and the engine is still
4961 lines.

This document now reflects the corrected target: collapse the `kernels/`
subdirectory into four flat files with clear single responsibilities, delete all
dead code, and keep the structure honest.

The questions that should be easy to answer:

- what is the coordinator doing?
- what is the state engine doing?
- where does key serialization live?
- where does vector reconstruction live?

## What Went Wrong

The first refactor plan (steps 1–6 below) correctly identified the goals —
separate concerns, make boundaries explicit, keep the engine lean. What it
produced was a `kernels/` subdirectory with five modules, while the engine
stayed at ~5000 lines because extraction was additive rather than subtractive.

Specifically:

- helpers were extracted into `kernels/` modules, but duplicated copies were
  not removed from the engine (e.g. `_bitmap_is_valid`, `_alloc_valid_bitmap`,
  `_read_dictionary_fixed_key` exist in both the engine and in extracted files)
- single-use helpers (`constant_keys.pyx`, `groupby_telemetry.pyx`) were given
  independent modules and build targets despite only having one caller each
- the naming did not match purpose: `dictionary_keys` reads any vector type,
  `groupby_telemetry` is a handful of thin wrappers
- `group_by_draken.pyx` and its five included kernel files were never retired
  after `carchar_group_state_engine.pyx` became the production path

The lesson: decomposition only reduces complexity if you delete what you
extracted. Otherwise you get two surfaces for the same behavior.

## Current Code Reality

The live production path is:

```
DrakenAggregateAndGroupNode   (Python coordinator)
  ↓ ingest() / finalize_morsels()
CarcharGroupStateEngine        (Cython engine, 4961 lines)
  cimports from kernels/:
    key_serialization_zpp      → key packing / unpacking
    dictionary_keys            → reads any vector type regardless of encoding
    constant_keys              → handles constant-key ingest (327 lines, 1 caller)
    groupby_finalize_kernels   → rebuilds output vectors from engine state
    groupby_telemetry          → thin wrappers around self._readings (91 lines, 1 caller)
```

Dead code (never imported in production):

- `aggregate_kernels.pyx` / `.pxd` — only cimported by `00_common.pyx` below
- `group_by_draken.pyx` — only referenced from `setup.py` and one test fixture
- `group_by_draken_kernels/` (5 files) — only included by `group_by_draken.pyx`

## Design Goals

1. **No Python in the hot path.** Python holds the GIL. Holding the GIL blocks
   parallelism. Every call into a Python object, every Python list append, every
   `isinstance` check in a per-row loop is a GIL hold and a missed opportunity
   to parallelize or run concurrent morsels.

2. **No Arrow in the hot path.** PyArrow objects are Python objects. Reading a
   value from a `pyarrow.Array` inside a per-row loop crosses the Python/C
   boundary on every access. Group-by ingest and state updates must operate on
   raw C pointers obtained from Draken native buffers (`DrakenFixedBuffer`,
   `DrakenVarBuffer`, `DictAccessor`, `ConstAccessor`). Arrow is allowed at the
   coordinator boundary only — to convert input morsels into Draken form before
   ingest, and to assemble output morsels after finalize.

3. **No NumPy in the hot path.** NumPy arrays require the GIL for element
   access via Python indexing and carry per-array Python object overhead.
   Memoryviews (`uint8_t[::1]`) are acceptable at boundary points where a
   contiguous buffer is being passed in or out. NumPy must not appear inside
   kernel loops or engine dispatch paths.

4. **Kernels are pure C/Cython with no Python calls.** Kernel functions must be
   `cdef` declared `noexcept nogil` where the loop allows it, and must not call
   into Python objects inside per-row loops. If a kernel needs to signal an
   error it raises a C-level exception — not a Python exception per row.

5. **Prefer C++ over Cython for kernel loops.** Cython with `nogil` is
   acceptable. A `.hpp` / `.cpp` implementation that Cython wraps via `cdef
   extern` is better — it removes Cython overhead entirely and is fully
   optimisable by the compiler. Infrastructure already does this:
   `zpp_key_codec.hpp`, `carchar_index.hpp`. New kernel loops should follow
   the same pattern when performance justifies it.

6. **The engine orchestration has no Python.** The `ingest()` and
   `finalize_morsels()` methods may accept Python objects at their boundary
   (morsels come in as Python objects) but the internal dispatch and per-group
   state updates must not call back into Python. State vectors are C++ STL
   vectors. Indexes are C++ hash maps. The Cython `cdef class` is an
   implementation detail, not a Python interface.

7. **Python lives only in the coordinator.** `DrakenAggregateAndGroupNode` is
   the only Python layer. Expression evaluation, morsel preparation, EOS
   routing, and operator telemetry happen there. Once ingest starts, no Python
   is invoked until finalize returns morsels to the coordinator.

8. Kernel files named `<aggregate>_<primitive_type>.pyx` — one file per
   aggregate × primitive type, containing all encoding variants (plain, dict,
   constant) for that combination.

9. The primitive type is the storage type, not the SQL surface type.
   `date32` is `int32`. `bool` is `int8`. Name for what the CPU sees.

10. The engine dispatches to kernels — it does not contain per-row loops.

11. Delete what was extracted. Decomposition that leaves the original in place
    is duplication, not refactoring.

12. Leave shuffle untouched.

## Target Structure

```
opteryx/compiled/aggregations/
  # Scalar aggregates — unchanged
  approximate_count.pyx
  approximate_median.pyx
  array_agg.pyx
  count_distinct.pyx

  # Infrastructure — no aggregate logic, no state
  key_codec.pyx             # key packing/unpacking via zpp_bits C++ bridge
                            #   (renamed from key_serialization_zpp.pyx)
  vector_readers.pyx        # reads any vector type: plain, dict-encoded, constant
                            #   (renamed from dictionary_keys.pyx)
  group_by_finalize.pyx     # rebuilds output vectors from engine state
                            #   (renamed from groupby_finalize_kernels.pyx)

  # Aggregate kernels — one file per aggregate × primitive type
  #   Each file handles all encodings of that type (plain, dict, constant).
  #   Primitive type = storage type, not SQL surface type.
  #     date32 → int32,  bool → int8,  timestamp64 → int64, etc.
  kernels/
    count_star.pyx          # COUNT(*) — no value column, key-agnostic
    count.pyx               # COUNT(col) — null mask only, type-agnostic
    sum_int64.pyx           # SUM for int64 (plain, dict-int64, constant)
    sum_float64.pyx         # SUM for float32/float64 (plain, dict-float, constant)
    avg_int64.pyx           # AVG for int64 (plain, dict-int64) — separate from SUM: needs per-group count accumulator
    avg_float64.pyx         # AVG for float64 — separate from SUM: needs per-group count accumulator
    min_max_fixed.pyx       # MIN/MAX for all fixed-width primitives (int8/16/32/64, float32/64, and all temporal types)
    min_max_var.pyx         # MIN/MAX for variable-width (string) — DEFERRED to D7:
                            #   requires string arena refactor shared with any_value_var
    count_distinct.pyx       # COUNT(DISTINCT col) — all types reduce to uint64:
                              #   int64 → cast to uint64
                              #   integer → _read_integer_value() → cast to uint64
                              #   string/other → morsel.hash() → uint64
                              #   kernel receives uint64* and a per-group FlatHashSet array
    any_value_fixed.pyx     # ANY_VALUE for fixed-width — stores 64-bit value in carchar state
    any_value_var.pyx       # ANY_VALUE for variable-width — stores arena pointer

  # Engine — owns all state, dispatches to kernels
  group_by_engine.pyx       # CarcharGroupStateEngine: ingest + finalize
                            #   telemetry helpers inlined (was groupby_telemetry.pyx)
                            #   constant-key path inlined (was constant_keys.pyx)
```

### Naming rules

- Kernel file names: `<aggregate>_<primitive_type>.pyx`
- Primitive type is the C storage width the CPU sees. `date32` is `int32`.
  `bool` is `int8`. `float32` aggregates into `float64` but the input is
  still called `float32` or just `float64` if they share a file.
- Infrastructure files are named for what they do, not what they contain.

### Hot-path rules by layer

| Layer | Python | Arrow | NumPy | GIL |
|---|---|---|---|---|
| Kernel per-row loops | ✗ none | ✗ none | ✗ none | released (`nogil`) |
| Kernel entry/exit | ✗ none | ✗ none | memoryview boundary only | acquired to raise exception only |
| Engine ingest dispatch | ✗ none | ✗ none | ✗ none | released where possible |
| Engine finalize | ✗ none | boundary only (output Morsel construction) | ✗ none | held only during Morsel build |
| Coordinator | ✓ allowed | ✓ allowed | ✓ allowed | held throughout |

Arrow and NumPy are allowed at the coordinator boundary to convert incoming
morsels and assemble output morsels. They must not cross into the engine.

### Kernel implementation preference

```
C++ (.hpp/.cpp) via cdef extern   ← best: fully compiler-optimisable, no GIL
Cython cdef nogil                 ← acceptable for straightforward loops
Cython cdef without nogil         ← allowed only at GIL-scope entry/exit
Arrow / NumPy / Python objects    ← not allowed inside kernels or the engine
```

### Encoding variants within one file

Each kernel file covers all three encoding modes for its type. The dispatch
is done once at the top of the ingest call — not per row:

1. **plain** — raw values in a `DrakenFixedBuffer` or `DrakenVarBuffer`
2. **dict-encoded** — codes + dictionary values, accessed via `DictAccessor`
3. **constant** — single value broadcast, accessed via `ConstAccessor`

### What merges back into the engine

`constant_keys.pyx` (327 lines, 1 caller) and `groupby_telemetry.pyx` (91
lines, 1 caller) are dissolved into `group_by_engine.pyx`. They have no reason
to be independent build targets. The constant-key ingest logic is a branch
family inside the engine, not a separate concern.

### What gets deleted

Dead code with no live callers:

- `aggregate_kernels.pyx` / `aggregate_kernels.pxd`
- `group_by_draken.pyx`
- `group_by_draken_kernels/` (entire directory, 5 files)

## Migration Steps

Each step is scoped to be completable in a single implementation request.
Steps must proceed in order — the "Done when" condition for each step is the
entry criterion for the next.

### Step A — Delete dead code ✓ DONE

Remove:
- `opteryx/compiled/aggregations/aggregate_kernels.pyx`
- `opteryx/compiled/aggregations/aggregate_kernels.pxd`
- `opteryx/compiled/aggregations/group_by_draken.pyx`
- `opteryx/compiled/aggregations/group_by_draken_kernels/` (whole directory)

Remove their build entries from `setup.py`. Remove the path references in
`tests/unit/core/test_dictionary_motor_path_guards.py`.

Done when the build passes and no test imports any deleted module.

### Step B — Rename infrastructure files ✓ DONE

Mechanical rename only, no logic changes:

- `kernels/key_serialization_zpp.pyx` → `key_codec.pyx` (move up to aggregations/)
- `kernels/dictionary_keys.pyx` → `vector_readers.pyx` (move up)
- `kernels/groupby_finalize_kernels.pyx` → `group_by_finalize.pyx` (move up)
- `carchar_group_state_engine.pyx` → `group_by_engine.pyx`

Update all `.pxd` files, `setup.py` entries, and `cimport`/`import` statements.

Done when `kernels/` contains only the aggregate kernel files (currently
empty), and no file references old names.

### Step C — Merge single-caller helpers into the engine ✓ DONE

Move `constant_keys.pyx` and `groupby_telemetry.pyx` content directly into
`group_by_engine.pyx`. Remove both files and their `setup.py` build entries.
`kernels/` is now empty.

Done when `kernels/` is empty and the build passes.

### Kernel Implementation Notes (learned from D2)

These apply to every kernel written from D2 onwards:

1. **`seen` / flags arrays are `int64_t*`, not `uint8_t*`.**  
   The engine stores `_seen` and `_multi_seen` as `vector[int64_t]`. Any kernel
   that receives a seen/flags buffer must declare the parameter as `int64_t*`.
   Using `uint8_t*` will produce a Cython type-assignment error at compile time.

2. **Dict kernel variants must be `except *`, not `noexcept nogil`.**  
   `_dict_accessor_read_float_value` and `_dict_accessor_read_int_value` in
   `vector_readers.pxd` are both `except *`. Any kernel that calls them must
   also be declared `except *` (cannot hold the GIL-free guarantee).

3. **`_bitmap_is_valid` in `utils.pxd` takes `const uint8_t*`.**  
   Value null bitmaps travel through the engine as `const uint8_t*`. The helper
   accepts `const`; callers do not need a cast.

### Step D1 — Extract `count_star.pyx` ✓ DONE

COUNT(*) has no value column and is key-agnostic — the simplest kernel and the
template for all later steps. Establishes the kernel file structure and engine
wiring pattern.

1. Write `kernels/count_star.pyx`: `cdef noexcept nogil` update, plain / dict /
   constant encoding modes (all three are identical for COUNT(*): increment count).
2. Wire `group_by_engine.pyx` to call it for COUNT(*) dispatch.
3. Delete the corresponding inline loop from the engine.
4. Build and verify tests pass.

Done when the engine contains no inline COUNT(*) loop.

### Step D2 — Resolve avg/sum merge; extract `sum_int64.pyx` + `sum_float64.pyx` ✓ DONE

**Open Question 4 resolution:** SUM and AVG use separate state arrays and cannot
share kernels. SUM uses `_f64_state`/`_i64_state` (the accumulated value) plus
`_seen` (a `vector[int64_t]` boolean flag). AVG uses `_avg_sums` (double) and
`_avg_counts` (int64). The non-null row count is NOT free alongside SUM — AVG
requires its own independent count accumulator. Step D4 (AVG kernels) is required
and cannot be skipped.

New files:
- `kernels/utils.pxd` — shared inline helpers (`_bitmap_is_valid`, `_read_integer_value`) for all kernel files
- `kernels/sum_float64.pyx` + `.pxd` — 4 functions: plain, dict, multi-plain, multi-dict
- `kernels/sum_int64.pyx` + `.pxd` — 6 functions: i64-plain, i64-dict, integer-plain, and multi-variants for each

Done when the engine contains no inline SUM loops and the avg/sum decision
is recorded.

### Step D3 — Extract `min_max_fixed.pyx` ✓ DONE (`min_max_var.pyx` deferred)

New files:
- `kernels/min_max_fixed.pyx` + `.pxd` — 10 functions covering all fixed-width
  types (float64, int64, generic integer) across single-agg and multi-agg paths,
  with plain and dict-encoded variants. All functions use a `bint is_min` parameter
  with the `if is_min:` branch hoisted outside the per-row loop.

**`min_max_var.pyx` is not extractable without a larger refactor.**
The string MIN/MAX path lives in `_ingest_object_minmax_for_states` and
`_ingest_object_minmax_multi_for_states`, which are `cdef` engine methods rather
than standalone kernel functions. They call `self._store_object_state_bytes`,
`self._compare_bytes`, `self._multi_offset`, and `self._object_state_bytes` —
engine-internal state that cannot be passed to an external `cdef` function
without either passing the whole engine object or redesigning the string state
storage. That redesign belongs to a dedicated step (D8 or alongside D7 string
ANY_VALUE), not here.

**Naming note:** kernel functions are named `minmax_*` (not `min_*`), reflecting
that a single function handles both MIN and MAX via the `bint is_min` parameter.

Done when the engine contains no inline MIN/MAX loops for fixed-width types.

### Step D4 — Extract `avg_int64.pyx` + `avg_float64.pyx` ✓ DONE

AVG was not merged into SUM kernels (resolved in D2: SUM uses `_seen` as a
bool flag; AVG requires an independent per-group row count accumulator —
incompatible state shapes).

New files:
- `kernels/avg_float64.pyx` + `.pxd` — 4 functions: plain accumulate,
  dict-encoded accumulate, multi-agg plain, multi-agg dict
- `kernels/avg_int64.pyx` + `.pxd` — 6 functions: i64 plain, i64 dict,
  integer plain, and multi-agg variants for each

**`TimestampVector` is not supported by AVG.** `AVG(timestamp)` is invalid SQL
(Postgres rejects it). The original fallthrough contained no AVG branch for
`TimestampVector`; none was added here.

Dict-encoded value column support is present in both `_ingest_dictionary_key`
(single-agg) and `_ingest_dictionary_key_multi` (multi-agg) via
`avg_f64_accumulate_from_dict` / `avg_i64_accumulate_from_dict` /
`avg_f64_multi_accumulate_from_dict` / `avg_i64_multi_accumulate_from_dict`.

All 11 engine ingest methods are now wired to kernel dispatch for AVG. No
inline `self._avg_sums[offset]` or `self._multi_avg_sums[offset]`
accumulation loops remain in the engine.

Done when the engine contains no inline AVG loops.

### Step D5 — Extract `count_distinct.pyx` ✓ DONE

All value types reduce to `uint64_t` before insertion into the per-group
`FlatHashSet`, so a single kernel file covers every type:

- `Int64Vector` → cast value to `uint64_t` directly
- `IntegerVector` → `_read_integer_value()` → cast to `uint64_t`
- string / float64 / any other type → `morsel.hash([col])` yields `uint64_t[::1]`

The type-specific divergence happens in the engine dispatch (before the kernel
call), not inside the per-row loop. The kernel signature is uniform:
`uint64_t* value_hashes, FlatHashSet* distinct_sets, int64_t* state_indices, ...`.

New files:
- `kernels/count_distinct.pyx` + `.pxd` — single-agg and multi-agg variants:
  `count_distinct_accumulate` and `count_distinct_multi_accumulate`

**FlatHashSet constraint resolution:** The `list` at the kernel boundary
approach was chosen. The kernel accepts `list distinct_sets` (Python list of
`FlatHashSet` extension objects), pre-resolves each to a raw
`flat_hash_set<uint64_t, IdentityHash>*` pointer (into a malloc'd C array)
before the per-row loop, then runs the per-row loop `nogil` using those raw
C++ pointers. An inline C++ helper (`opteryx_cd::fhs_insert_new`) wraps
`flat_hash_set::insert(v).second` to avoid materialising `pair<iterator,bool>`
in Cython. The engine dispatch handles the three type branches (Int64 direct
cast, IntegerVector expand into a temp `uint64_t[]` buffer, and `morsel.hash()`
for everything else) before calling the kernel.

Done when the engine contains no inline COUNT(DISTINCT) loops.

### Step D6 — Extract `count.pyx`

COUNT(col) is a null check: increment the counter if the validity bit is set.
The primitive is the validity bitmap, not the value type — one file, no type
specialization, `cdef noexcept nogil`. Wire the engine; delete inline
COUNT(col) branches.

Done when the engine contains no inline COUNT(col) loops.

### Step D7 — Extract `any_value_fixed.pyx` + `any_value_var.pyx` + `min_max_var.pyx`

This step covers all aggregates whose variable-width (string) state requires
the engine's arena/object storage — `ANY_VALUE` and `MIN/MAX` on strings share
the same blocker (`_store_object_state_bytes`, `_compare_bytes`) and must be
resolved together.

Confirm or resolve Open Question 3 before writing:
- `any_value_fixed.pyx`: store the 64-bit value inline in carchar state; skip
  row if state is already non-null.
- `any_value_var.pyx`: store arena pointer/offset in carchar state; skip row
  if state already holds a pointer.
- `min_max_var.pyx`: string MIN/MAX — comparison via `_compare_bytes` with
  arena-backed state storage. Covers plain, dict-encoded, and constant string
  encodings. Currently implemented as engine-internal helpers
  `_ingest_object_minmax_for_states` / `_ingest_object_minmax_multi_for_states`.

All three files require the same prerequisite: the string state arena
(`_object_state_bytes`, `_store_object_state_bytes`, `_compare_bytes`) must be
accessible to standalone kernel functions. Either pass the arena as explicit
pointer arguments or extract it into a shared C++ struct — decide at the start
of this step.

Wire the engine; delete inline ANY_VALUE and string MIN/MAX branches.

Done when the engine contains no inline ANY_VALUE or string MIN/MAX loops and
`_ingest_object_minmax_for_states` / `_ingest_object_minmax_multi_for_states`
are removed.

### Step E — Thin the coordinator

`DrakenAggregateAndGroupNode` should only: prepare morsels, evaluate
expressions, call `ingest()` or `finalize_morsels()`, record operator timings.
Remove any mechanics that belong in the engine. The coordinator should be
readable end to end without reading engine internals.

Done when the coordinator contains no per-row logic and no engine-internal
knowledge.

## Why Aggregate × Type

The previous attempt used a generic `dictionary_keys.pyx` containing helpers
for all types, and a generic `groupby_finalize_kernels.pyx` for all output
shapes. Both files work, but neither tells you what aggregate is happening or
what data type is in play. When something is slow, you can't tell which file
to look in.

`sum_int64.pyx` is unambiguous. Anyone debugging a slow `SUM` on an integer
column knows exactly where to look. Adding a new encoding variant for int64
SUM means opening one file. Changing the finalize output shape for int64 SUM
means opening one file.

Kernel files are named for the storage primitive the CPU sees, not the SQL
surface type. `min_max_fixed.pyx` covers `int64`, `float64`, `date32`,
`timestamp64`, and every other fixed-width type — because they all branch the
same way at the instruction level. `count.pyx` operates on the validity bitmap
regardless of value type.

The grouping by aggregate × primitive type also matches how the CPU actually
branches — the same dispatch the engine makes at runtime is reflected in the
file layout.

More importantly, it is the only layout that makes `nogil` practical. A
function that handles `SUM(int64)` for plain, dict, and constant encodings can
be written entirely in C without touching Python objects. A function that
handles "any aggregate, any type" must return Python objects and call Python
methods to dispatch — and that means holding the GIL through the entire loop.

The `<aggregate>_<type>` split is not aesthetic. It is the prerequisite for
removing Python from the hot path.

## Open Questions

1. **RESOLVED: `count.pyx` — type-agnostic, null mask only.**
   COUNT(col) is a null check regardless of value type. The storage primitive
   is the validity bitmap, not the value. One file, no type specialization.

2. **PARTIALLY RESOLVED: `min_max_fixed.pyx` extracted; `min_max_var.pyx` deferred.**
   All fixed-width types (int8/16/32/64, float32/64, date32, time32/64,
   timestamp64) share the same comparison path — `min_max_fixed.pyx` covers all
   of them. `min_max_var.pyx` (variable-width string) cannot be extracted yet:
   the string MIN/MAX path calls engine-internal methods (`_store_object_state_bytes`,
   `_compare_bytes`) and cannot be a standalone `cdef` function without redesigning
   string state storage. This will be resolved alongside D7 (`any_value_var.pyx`),
   which faces the same constraint. Kernel names reflect what the CPU sees.

3. **OPEN: `any_value` may need a fixed/var split.**
   ANY_VALUE stores first-non-null. For fixed-width (≤ 64 bits), the value
   can be stored inline in carchar state. For variable-width (string), the
   state should store a pointer/offset into the arena rather than copying
   bytes. Split into `any_value_fixed.pyx` and `any_value_var.pyx` if the
   storage strategies diverge. Decide during Step D.

4. **RESOLVED: avg/sum kept separate — incompatible state.**
   SUM uses `_seen` as a `vector[int64_t]` boolean flag for NULL-propagation.
   AVG needs an actual per-group row count (`_avg_counts`) to divide by at
   finalize — the non-null row count is not free alongside SUM. Adding the
   count accumulator to SUM would add measurable overhead on SUM-only queries.
   Kernels are kept separate: `sum_*.pyx` takes a `seen` flag array;
   `avg_*.pyx` takes `avg_sums` + `avg_counts` with no `seen` flag.

## References In Code

- `opteryx/operators/draken_aggregate_and_group_node.py`
- `opteryx/compiled/aggregations/carchar_group_state_engine.pyx` → `group_by_engine.pyx`
- `opteryx/compiled/aggregations/kernels/` → aggregate kernel files only
- `docs/draken-aggregate-groupby-design.md`
- `docs/carchar-execution-engine-design.md`
- `docs/draken-group-by-explained-for-humans.md`
