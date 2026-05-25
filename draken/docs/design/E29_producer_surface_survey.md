# E.29 — Survey: zombie `.so` files and the missing producer-side draken surface

> **Status:** Survey complete. No code changes proposed in this doc — this is
> the inventory that feeds the architect call on producer-surface design.
>
> **Trigger:** Architect inspection of `vector_lowercase.pyx`:L44
>     `cdef object builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)`
> revealed §3 violation. Investigation showed the line works at runtime not
> because of correct typing but because a zombie `.so` from the E.24 era
> still satisfies the import. The same pattern is system-wide.
>
> **Headline:** the draken rebuild's "type matrix complete" claim is true
> for **consumer** ops (24 nanobind C′ extensions, all `Vector → Vector`)
> and **false** for **producer** ops (constructors, builders,
> sequence/scalar wrappers, `from_decoded` family, dict constructors). The
> producer surface was never migrated because no consumer-rewrite phase
> exercised it. Every `make q` measurement on this dev machine has been
> against a tree riding zombie `.so` files for producer-side functionality.

---

## 1. Zombie `.so` inventory

`setup.py` post-E.25 registers exactly 3 shim extensions
(`_shim_extensions` at L661–681 has been reduced to the 3 we kept):

```
draken.vectors.vector       ← _vector_shim.pyx
draken.vectors.bool_vector  ← _bool_vector_shim.pyx
draken.morsels.morsel       ← _morsel_shim.pyx
```

On disk **today**, 21 `.so` files in `draken/{vectors,morsels,interop}/`:

```
draken/interop/vector_sequence.cpython-313-darwin.so       ZOMBIE
draken/morsels/align.cpython-313-darwin.so                  ZOMBIE
draken/morsels/morsel.cpython-313-darwin.so                 (current)
draken/vectors/arithmetic_kernels.cpython-313-darwin.so     ZOMBIE
draken/vectors/array_vector.cpython-313-darwin.so           ZOMBIE
draken/vectors/bool_vector.cpython-313-darwin.so            (current)
draken/vectors/date32_vector.cpython-313-darwin.so          ZOMBIE
draken/vectors/decimal_vector.cpython-313-darwin.so         ZOMBIE
draken/vectors/float32_vector.cpython-313-darwin.so         ZOMBIE
draken/vectors/float64_vector.cpython-313-darwin.so         ZOMBIE
draken/vectors/integer16_vector.cpython-313-darwin.so       ZOMBIE
draken/vectors/integer32_vector.cpython-313-darwin.so       ZOMBIE
draken/vectors/integer64_vector.cpython-313-darwin.so       ZOMBIE
draken/vectors/integer8_vector.cpython-313-darwin.so        ZOMBIE
draken/vectors/interval_vector.cpython-313-darwin.so        ZOMBIE
draken/vectors/null_vector.cpython-313-darwin.so            ZOMBIE
draken/vectors/string_vector.cpython-313-darwin.so          ZOMBIE
draken/vectors/time_vector.cpython-313-darwin.so            ZOMBIE
draken/vectors/timestamp_vector.cpython-313-darwin.so       ZOMBIE
draken/vectors/vector.cpython-313-darwin.so                 (current)
draken/vectors/vector_vector.cpython-313-darwin.so          ZOMBIE
```

**18 zombies.** Their exports still resolve at Python import time. Examples
verified:

| Zombie | Exports |
|---|---|
| `draken.vectors.string_vector` | `StringVector`, `StringVectorBuilder` |
| `draken.vectors.integer64_vector` | `Integer64Vector`, `from_decoded`, `from_constant`, `from_dict*` |
| `draken.vectors.float64_vector` | `Float64Vector`, `from_decoded`, `from_constant`, `from_dict*` |
| `draken.vectors.array_vector` | `ArrayVector`, `from_sequence`, `array_vector_from_parts` |
| `draken.interop.vector_sequence` | `vector_from_sequence` |

E.25 reverted the **sources** for these but did not delete the **binaries**.
Any tree where these `.so` files have been built once (i.e., this dev
machine) imports them transparently; a fresh checkout would not. The
state is **machine-local** and silently dependent.

## 2. Already-broken imports

Not every old-draken module has a zombie. Some were truly removed:

| Import | Status |
|---|---|
| `from draken.vectors.scalar_constructors import from_scalar` | **broken** — `.cpp` exists, no `.so`, no `.py` shim |
| `from draken.interop.arrow import vector_from_sequence` | **broken** — `.cpp` exists, no `.so` |

Callers of these are **broken at import** today. Any test or query that
exercises them fails at module-init, not at use. The 0/133 baseline I
recorded from E.25 reflects this — but it's a mixture of "broken-at-import
due to missing module" and "would-work-at-runtime via zombie", and that
mixture is not honest enough to reason from.

## 3. The producer-helper inventory

### 3.1 By symbol — what's used and how often

| Symbol | Total refs | Backing today | Producer category |
|---|--:|---|---|
| `vector_from_sequence` | 73 | zombie `draken.interop.vector_sequence` | sequence constructor |
| `StringVectorBuilder` | 24 | zombie `draken.vectors.string_vector` | arena builder |
| `from_decoded` (per-type) | 24 | zombie `draken.vectors.{integer64,float64,float32,bool}_vector` | raw-buffer → Vector |
| `from_scalar` | 11 | **broken** `draken.vectors.scalar_constructors` | scalar broadcast |
| `bool_vector_from_bits` | 10 | zombie `draken.vectors.bool_vector` (old class) + inline `.cpp` symbol | bitmap → BoolVector |
| `array_vector_from_parts` | 7 | zombie `draken.vectors.array_vector` | nested-array builder |
| `array_from_sequence` (aliased) | 2 | zombie `draken.vectors.array_vector` | sequence → ArrayVector |
| `from_constant` (per-type) | (E.28 gap-8) | zombie per-type | scalar → Constant-shape Vector |
| `int64_from_dict*` / `float64_from_dict*` | (E.28 gap-5/6) | zombie | dict-encoded constructor |
| `string_from_dict_buffers`, `make_string_dict_only` | (E.28 gap-7) | zombie | dict-encoded strings |

Total: **8 distinct producer "shapes"**, surfacing through ~160+ call sites.

### 3.2 Three import paths for `vector_from_sequence`

The same function is imported through three different paths across the
codebase:

| Path | Sample callers | Notes |
|---|---|---|
| `from draken.interop.vector_sequence cimport vector_from_sequence` | `opteryx/operators/_operators.pyx:29` | C-level cimport |
| `from draken.interop.vector_sequence import vector_from_sequence` | `cross_join.pyx`, `exit.pyx`, `unnest_join.pyx` | Python import |
| `from draken.interop.arrow import vector_from_sequence` | `_collectors_distinct.pxi`, `_collectors_approx.pxi`, `_collectors_numeric.pxi` | Python import, **module name violates no-pyarrow rule** |

The `draken.interop.arrow` path is doubly wrong: it imports from a module
called `arrow` (despite CLAUDE.md §4's PyArrow ban being broad-spectrum
on naming in `draken/`), and that module currently doesn't have a `.so`,
so the importers are broken-at-import.

### 3.3 The §3 violation pattern this enables

`StringVectorBuilder` has no `.pxd` in new draken. To call it in Cython
the only typing available is `cdef object`:

```cython
# opteryx/compiled/vector_ops/vector_lowercase.pyx:44
# opteryx/compiled/vector_ops/vector_uppercase.pyx:44
# opteryx/compiled/vector_ops/vector_initcap.pyx:52
# opteryx/compiled/vector_ops/vector_reverse.pyx:29
cdef object builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)
```

This is a §3 violation. It is only forced because the class itself has no
typed surface in new draken. **The typing fix is downstream of the
architecture fix**: until `StringVectorBuilder` has a typed home, the
caller cannot honestly avoid `object`.

The same pattern likely exists for any cdef function returning a
non-cimportable producer result. A broader audit (out of scope of this
survey) would enumerate.

## 4. Per-category producer surface needed

Grouping the 8 shapes into design-relevant categories:

### 4.1 Sequence constructors (most used)
- `vector_from_sequence(py_seq, dtype=...)` — Python list/tuple → Vector
- `array_from_sequence(py_seq)` — Python list of lists → ArrayVector

73 + 2 = **75 call sites**. The single largest producer-helper category.
Used by: every `show_*` operator, function_dataset, cross_join, exit,
unnest_join, every aggregate that finalises a scalar into a 1-row vector,
the collectors in `grouped_aggregate_hashed`.

### 4.2 Scalar constructors
- `from_scalar(value, length, dtype=...)` — broadcast a Python scalar to a
  length-N Constant-shape Vector

11 call sites. Used by null_reader, filter_join, group-by node.

### 4.3 Arena-aware builders (loop-mode construction)
- `StringVectorBuilder.with_estimate(n, avg_len)` + `.append_bytes(...)`,
  `.append_null()`, `.finish()`

24 refs. Used inside per-row loops where the result string length is
data-dependent: `vector_lowercase`, `vector_uppercase`, `vector_initcap`,
`vector_reverse`, plus rugo's parquet/jsonl readers.

### 4.4 Raw-buffer constructors (`from_decoded` family)
- `int64_from_decoded(void* data, uint8_t* nulls, size_t n)`
- `float64_from_decoded(...)`, `float32_from_decoded(...)`, `bool_from_decoded(...)`

24 refs. Used by parquet reader, JSONL reader, fast_float, deserialiser,
buffer reconstruction in joins. **Critical path** — every scan into
the engine goes through here.

### 4.5 Bitmap-to-vector
- `bool_vector_from_bits(uint8_t* bitmap, uint8_t* null_bitmap, uint32_t n)`

10 refs. Used by parquet's null-handling, by the bytecode VM postpass
(was the function E.24 added then E.25 reverted), by some compare
kernels.

### 4.6 Constant-shape constructors (E.28 gap-8)
- `Integer64Vector.from_constant(value, length)`, similar for other types

Used in parquet for column-with-only-the-default-value, in some optimiser
paths. ~10 refs across the codebase.

### 4.7 Dict-encoded constructors (E.28 gap-5/6/7)
- `int64_from_dict(values, codes, ...)`,
  `int64_from_dict_nullable(...)`, `int64_from_packed_dict(...)`
- `float64_from_dict*` variants
- `string_from_dict_buffers(...)`, `make_string_dict_only(...)`

Used by parquet for dict-encoded columns (every parquet file with
RLE_DICTIONARY encoding hits these). **Performance-critical for
parquet/clickbench.**

### 4.8 Nested-array builders
- `array_vector_from_parts(values_vector, offsets, validity)`

7 refs. Used by parquet (LIST columns), JSONL (nested values).

## 5. How this happened

The draken rebuild's design corpus
(`draken/docs/design/00_data_model.md`–`07_consumer_contract.md`) is
written entirely from the consumer side — what a Vector *is*, what
consumers may assume, how dispatch works. The producer side is implicit:
"new draken builds Vectors via some surface, we'll figure it out as
needed."

The consumer-rewrite phases (E.0 and on) then did exactly what the design
described: ported 24 op kernels that take a Vector and return a Vector.
These all wrap their result via `draken_vector_own_raw` /
`draken_vector_own_string` from `draken_bridge.h`, which **internally
calls the producer surface** but doesn't expose a clean Python/Cython
producer API.

Code outside the consumer-rewrite path (operators, rugo, the
case-folding `.pyx`s) continued to use the **old-draken** producer
helpers. Those helpers stayed compiled in `.so` form because nobody
explicitly deleted them. E.25 removed the source registrations in
`setup.py` but the binaries persisted.

The result: a clean consumer layer riding on top of a producer layer that
"works" only because of forgotten binaries.

## 6. Architecture options for the producer surface

Three plausible shapes. Each has been raised informally; this doc commits
none of them — that's the next architect call.

### Option A — Producer classes/functions as nanobind in `draken_native.cpp`

Add to the nanobind module:
- `StringVectorBuilder` (nanobind class with `append_bytes`, `append_null`,
  `finish`)
- `vector_from_sequence(py_seq, dtype=...)` (nanobind free function)
- `from_scalar(value, length, dtype=...)` (nanobind free function)
- `Integer64Vector.from_constant`, etc. — could be classmethods on a
  unified `Vector` class
- `int64_from_decoded`, etc. — nanobind functions taking buffer pointers

Plus Cython shims (mirroring E.24's pattern for `Vector`/`BoolVector`/
`Morsel`) so cimport-using callers like `_operators.pyx` and the
column_deserializer can keep their typed cimports.

Pros: minimal change to the ~160 call sites (they keep their existing
import shape, just point at new locations).

Cons: re-introduces the Cython-vs-nanobind seam for producer types that
E.24 spent so much effort untangling for the consumer types. Means the
`__pyx_vtable__` saga repeats for `StringVectorBuilder` and friends.

### Option B — Pure C bridge API in `draken_bridge.h`

Extend the bridge with producer-side functions:

```c
DrakenStringBuilder* draken_string_builder_new(size_t estimate, size_t avg_len);
void draken_string_builder_append(DrakenStringBuilder*, const uint8_t* bytes, size_t len);
void draken_string_builder_append_null(DrakenStringBuilder*);
DrakenVector*       draken_string_builder_finish(DrakenStringBuilder*, DrakenType);

DrakenVector* draken_vector_from_python_sequence(PyObject* seq, DrakenType dtype);
DrakenVector* draken_vector_from_scalar(PyObject* value, size_t length, DrakenType dtype);
DrakenVector* draken_int64_from_decoded(void* data, uint8_t* nulls, size_t n);
/* … */
```

Callers in `.pyx` use these from `cdef extern` blocks — no Cython class
shape needed, no `__pyx_vtable__`, no nanobind seam. Callers in
`.cpp` (nanobind extensions) use them directly.

Pros: cleanest C′ alignment, no Cython class shape needed at all.
Producer surface is just functions over pointers, type-tagged by
`DrakenType`. Composable with the existing consumer bridge.

Cons: callers that today write idiomatic Python like
`vector_from_sequence([1, 2, 3])` need to either keep doing it via a thin
Python wrapper module, or migrate to a less idiomatic shape. The 75
`vector_from_sequence` call sites are mostly Python-import shapes, so this
is real migration work.

### Option C — Producer code moves into compiled extensions; no Python-visible producer API

Operators that today build a vector from a Python list (the `show_*`
family, `function_dataset`, `exit`) move that construction into a
nanobind C++ extension. The `.pyx` operator caller passes the Python
list to the C++ extension, which calls the existing
`draken_vector_own_*` bridge.

Pros: forces the discipline that no producer construction happens in
Python; everything goes through compiled code. Matches the C′ direction
maximally.

Cons: biggest blast radius. Effectively every operator that emits a
Vector from a Python source needs a new nanobind extension. 30+ call
sites of `vector_from_sequence` alone become 30+ new extension
functions, unless we make one general one.

### Hybrid: B + a thin Python wrapper

The arguably-best compromise:

- C bridge (Option B) is the real surface.
- A small `draken/interop/producer.py` (or similar) wraps the C bridge
  functions and re-exports them as `vector_from_sequence`, `from_scalar`,
  etc. — preserves the call shape callers already use, while making the
  *underlying* implementation honest.
- `StringVectorBuilder` becomes a thin Python class wrapping the C
  builder handle (since builders are stateful and benefit from a Python
  context-manager-like usage shape).

This keeps the 73 call sites of `vector_from_sequence` working with a
single import-line change. The C bridge is the canonical surface; the
Python wrapper is sugar.

## 7. Estimated migration cost (rough, not commit-time)

After producer-surface lands (any option):

- **Zombie cleanup** (E.30 implied): delete 18 `.so` files, add a make
  target / hook that prunes orphan binaries. <1 day.
- **Caller migration** off zombies:
  - `vector_from_sequence` import-path consolidation: 73 sites, mostly
    one-line changes. ~1 day.
  - `from_scalar` migration: 11 sites. <0.5 day.
  - `from_decoded` family: 24 sites; these are `cimport` not Python
    import, so they need a proper `.pxd` or `cdef extern` block. ~1
    day.
  - `StringVectorBuilder` migration: 24 refs across 4 string `.pyx`s
    plus rugo. Loop-shape changes if builder API differs. ~1-2 days.
  - `bool_vector_from_bits`: 10 refs. <0.5 day.
  - `array_vector_from_parts`, `from_constant`, dict constructors: ~30
    refs combined. ~1-2 days.
- **Total caller migration**: ~5-6 days of focused work.
- **Producer surface implementation** itself (Option B + hybrid):
  ~3-5 days, depending on how many helpers and whether the
  `DrakenType`-parameterised functions can share generic implementations.

**Bottom line:** the producer-surface track is ~2 weeks of draken-PM
work. It is **the** blocker before the eval-PM and operator-PM can
genuinely start, because every consumer they'd port talks to producers.

## 8. Recommendation

**Option B + hybrid Python wrapper.** Reasons:

1. It is the only option that keeps producer code out of the Cython
   class-shape problem entirely. No `__pyx_vtable__` second act.
2. The C bridge surface is what the *existing* `draken_vector_own_*`
   functions are already in. We are extending a coherent layer, not
   creating a new one.
3. The thin Python wrapper preserves the 73-site call shape for
   `vector_from_sequence` with one import-line change per site.
4. It is the option most consistent with CLAUDE.md §2 ("Opteryx is not a
   Python application, it is a Cython/C++ application with Python
   orchestration") — producer functions are C, the Python wrapper is
   sugar at the orchestration edge.

The full plan if Option B + hybrid is chosen:

- **E.30**: design ticket — write `draken/docs/design/10_producer_surface.md`
  specifying the bridge functions, signatures, type-dispatch rules,
  ownership semantics. Architect-reviewable, no code.
- **E.31**: implement the C bridge functions for the four highest-traffic
  categories first (sequence, scalar, `from_decoded`, builder).
- **E.32**: thin Python wrapper module(s) — `draken/interop/producer.py`
  or similar. Imports the C functions, exposes idiomatic names.
- **E.33**: migrate callers off zombies, one category at a time. Each
  category is its own micro-ticket.
- **E.34**: delete zombie `.so` files; add a make-target that prunes
  orphans on every build so this can't recur.
- **E.35**: re-baseline `make q` honestly. This is the number that
  goes into the operator-rewrite-PM and eval-PM handover docs.

After E.35, the eval-PM and operator-PM can genuinely start. Until then,
their migrations are riding on a substrate that doesn't exist in any
source file.

## 9. Architect calls and answers (2026-05-25)

### 9.1 Arrow — outbound `to_arrow` only, ideally via Arrow C++

**Architect call (refined):** Arrow has exactly one permissible use:
**outbound `to_arrow` serialisation** — exporting a Draken vector / morsel
into Arrow buffer format for external consumers (tools, federation,
debugging). Even that should use the **Arrow C++ library**, not PyArrow.
There is **no** inbound `from_arrow` capability planned; ingestion is via
rugo (parquet/jsonl/etc.) or via the producer surface directly.

Implications for the current state:
- `draken.interop.arrow.vector_from_sequence` is wrong on **two** counts,
  not one: misnamed (doesn't touch Arrow), and conceptually backwards
  (would be inbound if it did). The 4+ callers in
  `_collectors_{distinct,approx,numeric}.pxi` consolidate onto the
  canonical producer-side `vector_from_sequence` path. The `arrow`
  import path leaves these call sites entirely.
- `draken.interop.arrow` as a module should **not exist** in its current
  form. When `to_arrow` is added in some future ticket, the right shape
  is most likely a method on the Vector/Morsel surface (e.g.
  `vec.to_arrow()`) backed by Arrow C++, not a freestanding `interop.arrow`
  module. The module name is reserved-by-convention, not reserved by
  having a file on disk.
- The `draken.interop.arrow.cpp` file currently in the tree (orphaned
  — no `.so`, no `.py`) goes away as part of the zombie cleanup (E.34).

This stays consistent with §4 of CLAUDE.md: PyArrow is banned in
production; Arrow-the-buffer-format used via the Arrow C++ library is a
distinct thing, scoped to outbound serialisation, no Python dependency.

### 9.2 `from_constant` — free function

**Architect call:** free function. `vector_from_constant(value, length, dtype)`
in the same wrapper module as `vector_from_sequence` and `from_scalar`.
Consistent with the rest of the producer surface.

### 9.3 `bool_vector_from_bits` — collapse into `draken_vector_own_raw`

**Architect call:** confirmed. The bridge already supports this via
`draken_vector_own_raw(buffer, n, DRAKEN_BOOL, validity)`. The E.24
mistake was adding a new function in `draken_native.cpp` when the right
primitive was already there. The 10 callers migrate to the existing
bridge function with the `DRAKEN_BOOL` type tag.

### 9.4 Python imports inside `cdef` method bodies — tyre-fire, dedicated ticket

**Architect call:** "Sounds like a tyre-fire we need to put out before it
spreads."

Agreed. This is not a per-collector micro-ticket folded into the wider
producer-surface migration — it's a focused fix-it pass that goes
*before* the migration, so the migration doesn't entrench the pattern by
moving call shapes around without addressing the underlying violation.

The pattern in `_collectors_{distinct,approx,numeric}.pxi`:

```cython
cdef object some_finalise(self, ...):
    from draken.interop.arrow import vector_from_sequence  # ← inside cdef
    ...
    return vector_from_sequence(vals)
```

is broken on three counts simultaneously:

1. `cdef object` return — §3 violation.
2. Python import inside a cdef body — Python work on a hot path, runtime
   import-system overhead per call, §2 violation.
3. The function is structured as "build a Python list, then call a
   Python-imported constructor" — meaning the *actual work* of producing
   the vector is happening in Python, not in compiled code. Even
   eliminating the imports doesn't fix this; the methods need to do
   producer-side work in typed C, returning a typed `Vector`.

Sequencing decision:
- **E.30a (new):** dedicated tyre-fire ticket. Audit every `.pxi` and
  `.pyx` in `opteryx/` and `rugo/` for Python imports inside `cdef` /
  `cpdef` bodies. Produce an inventory. Categorise: (a) hot-path call
  pattern, (b) called-once-per-query init pattern, (c) genuine
  module-level-deferred import. For (a), surface as fix-required; for
  (b) and (c), surface as fix-recommended (hoist to module level if no
  circular-import reason exists).
- **E.30b:** fix the hot-path violations identified by E.30a. This is
  pre-condition work for E.31 onward, not a parallel track.
- The collector-internals restructure (typed-C producer work instead
  of Python-list-and-call-Python-constructor) folds into E.33 per
  collector once the producer surface is in.

The audit (E.30a) should be cheap — a grep + classification pass, no
code changes. The fix (E.30b) is bounded by what E.30a finds; first
estimate available after the audit lands.

This bumps the project's total sequence by ~1-2 days but reduces the
risk of carrying the pattern forward into the producer-surface
migration. The migration cleans up the import paths; without the
tyre-fire fix, the cleanups would shuffle the violations around rather
than removing them.

### 9.5 Other open questions

- **Builder lifetime:** `StringVectorBuilder` is stateful — should the
  Python wrapper enforce a context-manager shape (`with builder as b:
  ...`), or rely on `__del__` semantics with the C handle freed on
  reference-drop? Context-manager is safer; `__del__` is more idiomatic
  for short-lived builders. Decide before E.31.
- **Dict constructors (E.28 gap-5/6/7):** the existing rugo callers use
  `int64_from_dict`, `int64_from_dict_nullable`, `int64_from_packed_dict`
  as three distinct variants. Could collapse to one
  `vector_from_dict(values, codes, validity, dtype)` if validity-NULL
  signals "no nulls" and the packed-vs-unpacked distinction can be
  encoded in the type tag or a flag. Worth checking before E.31 whether
  rugo actually needs three.

## 10. What this changes about the handover

The handover docs I wrote (`01_draken_state_at_handover.md`,
`opteryx/expression/evaluator/docs/design/00_pm_briefing.md`) implicitly
assumed the eval-PM and operator-PM would migrate cimports against a
producer surface that already existed. That assumption is wrong. Both
handovers need an updated §3 / §4 section that says:

> "Producer-side surface (`StringVectorBuilder`, `vector_from_sequence`,
> `from_scalar`, `from_decoded` family, etc.) is being built by the
> draken-PM as E.30–E.35. Until E.35 closes, do not start consumer-side
> migrations — you'll be migrating to a moving target. Architect will
> notify when the producer surface is stable."

Both PMs should be told to **hold** until E.35.
