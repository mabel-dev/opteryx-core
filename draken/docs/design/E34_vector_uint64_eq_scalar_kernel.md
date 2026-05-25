# E.34 — `vector_uint64_eq_scalar` kernel (the missing draken primitive)

> **Status:** TODO. **Should have existed already.** The draken-PM (me)
> framed `_bool_vector_from_uint64_eq` as an eval-PM migration problem in
> the E.29 survey and the eval-PM briefing. That framing was wrong. It's
> a missing draken-side primitive. The eval-PM agent, lacking the
> primitive, proposed a Python list comprehension over `num_rows` items —
> §2 / §3 violation, exactly the no-false-green pattern. This ticket
> provides the primitive so the eval-PM can migrate honestly.
>
> **Why narrow:** one specific call site needs this (evaluation.pyx:404).
> The pattern is "compare a contiguous uint64 buffer to a scalar, return
> a BoolVector." Solving it generally (Vector-shaped `Morsel.hash()`,
> DRAKEN_UINT64 type, etc.) is a refactor with broader implications.
> This ticket is the narrow primitive that unblocks the eval-PM today.

---

## 1. The gap

```cython
# opteryx/expression/evaluator/evaluation.pyx:403-404
row_hashes_view = morsel.hash(hash_keys)               # array.array('Q'); uint64 buffer
return _bool_vector_from_uint64_eq(row_hashes_view, target_hash)   # OLD HELPER, no replacement
```

`morsel.hash(hash_keys)` mixes multiple columns into a single
per-row uint64 hash (used by the hash-join probe fast-path). The caller
needs a `BoolVector` of "which rows have `hash == target_hash`."

Old draken had `_bool_vector_from_uint64_eq`. New draken does not, and
producer-surface migration paths (`vector_from_bool_sequence` etc.) are
all *Python-level* construction — wrong layer for a per-row hot path.

## 2. What's being delivered

A new C′ nanobind extension:
**`opteryx/compiled/nanobind/vector_hash_eq.cpp`** (or fold into an
existing nearby extension if cleaner — e.g. `vector_hash_codec.cpp`).
Exports one function:

```cpp
// Signature (nanobind-bound):
//   vector_uint64_eq_scalar(buffer: object, length: int, target: int) -> Vector
// where:
//   buffer  — Python buffer-protocol object (array.array('Q'), bytes, memoryview)
//             holding `length` uint64_t values contiguously
//   length  — number of uint64 elements
//   target  — uint64 scalar to compare each element against
// Returns:
//   Vector of type DRAKEN_BOOL, length=length, with row i true iff buffer[i] == target.
//   Validity: all-valid (no nulls). NULL_HASH sentinel from draken_native.cpp's hash()
//   path is *not* a null per the existing convention — it's a real hash value that
//   happens to mean "the input was null"; comparing it to target is well-defined
//   and produces the right answer for the fast-path's intent.
```

Implementation:
- Accept the Python buffer object via nanobind's `nb::buffer` (zero-copy
  pointer extraction).
- Allocate a `(length + 7) / 8`-byte bitmap output via `draken_malloc`.
- Scalar loop: `out_bits[i >> 3] |= (buf[i] == target) << (i & 7)`.
- Wrap result via the existing `draken_vector_own_raw(...,
  DRAKEN_BOOL, ...)` bridge — same pattern other nanobind extensions
  use for bitmap-result construction.
- No validity allocated (all-valid).

Auto-vectorisation should make this fast on the scalar loop alone;
manual SIMD intrinsics are a follow-up if benchmarks warrant.

## 3. The eval-side caller migration (NOT in this ticket — informational)

After this kernel lands, the eval-PM's migration of
`evaluation.pyx:404` becomes a one-line import + call swap:

```cython
# WAS:
from <old-draken-zombie> import _bool_vector_from_uint64_eq
...
return _bool_vector_from_uint64_eq(row_hashes_view, target_hash)

# BECOMES:
from opteryx.compiled.nanobind.vector_hash_eq import vector_uint64_eq_scalar
...
return vector_uint64_eq_scalar(row_hashes_view, len(row_hashes_view), target_hash)
```

No Python loop. No list comprehension. No "TODO: optimise this later"
comment. The eval-PM should not write the Python-loop workaround the
agent proposed in this thread — surface that proposal to the architect
as "we need the kernel" rather than shipping it.

## 4. What is explicitly NOT in scope

- **Changing `Morsel.hash()` to return a Vector.** Tempting; broader
  refactor; backward-incompatible. Future ticket.
- **Adding `DRAKEN_UINT64` as a new type.** No. ABI churn. The buffer
  is treated as opaque bits for equality; signedness is irrelevant for
  `==`.
- **Generalising to other element-wise scalar comparison ops** (lt, gt,
  etc.). The use case is equality-only. If other ops are needed later,
  that's another ticket — don't pre-build.
- **Compiling the existing `int64_compare.h` to handle uint64
  inputs.** Same kernel works at the bit level (equality is
  signedness-blind), but the dispatcher would need to know how to
  accept a raw buffer rather than a Vector. Keeping the new extension
  separate keeps the bridge surface clean.
- **Refactoring `evaluation.pyx`'s caller.** Eval-PM's lane.
- **Adding the bitmap result via a new function in
  `draken_native.cpp`.** Use the existing `draken_vector_own_raw`
  bridge. E.24 added a `bool_vector_from_bits` for this exact use case
  and was reverted — `draken_vector_own_raw` with `DRAKEN_BOOL` already
  does the job (per the memory + E.29 §9.3).

## 5. STOP conditions

- File count > 3: the new `.cpp`, a setup.py Extension entry, and at
  most one test file. Past 3 → drifting.
- You catch yourself adding a function to `draken/draken_native.cpp`.
  **STOP.** This kernel is a C′ extension under
  `opteryx/compiled/nanobind/`, not a nanobind method on Vector or
  Morsel. Same lesson as E.24's `bool_vector_from_bits` revert.
- You catch yourself changing `Morsel.hash()`'s return type. **STOP.**
  Out of scope.
- You introduce a Python loop in the kernel implementation. **STOP.**
  The point of this kernel is to *eliminate* the Python loop.
- `make dt` regresses.

## 6. Acceptance

Run and report verbatim:

1. `ls opteryx/compiled/nanobind/vector_hash_eq.cpp` — file exists (or
   the equivalent if folded into an existing extension).
2. `make draken 2>&1 | tail -5` — builds clean.
3. `python -c "import array; from opteryx.compiled.nanobind.vector_hash_eq import vector_uint64_eq_scalar; buf = array.array('Q', [1,2,3,2,1]); v = vector_uint64_eq_scalar(buf, 5, 2); print([v[i] for i in range(5)])"` — prints `[False, True, False, True, False]`.
4. `make dt 2>&1 | tail -3` — passes.
5. New native test under `draken/tests/` (or wherever C′ extension
   tests live) covering:
   - Empty buffer
   - All-match, no-match, mixed
   - Buffer with the `NULL_HASH` sentinel (treat as a regular value)
   - Large buffer (e.g. 100k elements) — sanity that the loop isn't
     accidentally O(n²)
6. `git diff --stat HEAD` — files changed ≤3 + the test file.

## 7. Reporting back

- §6 acceptance outputs verbatim.
- Confirmation that the implementation contains **no Python loops**,
  **no `nb::cast` to/from Python iterables**, **no `array.array`
  iteration in C++** — only direct buffer-pointer access.
- Confirmation that no `draken_native.cpp` modification was made, no
  bridge function was added, no Morsel surface was touched.
- A one-line note on how the extension reads the input buffer (via
  `nb::buffer_info`, `Py_buffer`, etc.). Whichever pattern is used
  should match what other C′ extensions in
  `opteryx/compiled/nanobind/` already use — don't invent a new
  pattern.

## 8. The lesson

The draken-PM (me) saw `_bool_vector_from_uint64_eq` in the E.29
producer-surface inventory, classified it as "eval-PM's migration
problem," and moved on. The eval-PM agent — given a missing helper and
no draken-side primitive to replace it — proposed a Python list
comprehension, dressed up with a "TODO: long-term fix is...".

The right framing was: every producer-side helper that has a *single
typed scalar-or-buffer input and produces a typed Vector output* is a
draken-side primitive, not a caller migration. The pattern of
caller-side construction in Python is what the entire rebuild was
moving away from.

When this kind of "the agent's fix is to do it in Python" appears
during eval-PM or operator-PM work, the response should be **scope a
draken-side kernel ticket, not accept the Python loop**. Same rule
applies to whatever else surfaces. The eval-PM and operator-PM
briefings will be updated to make this explicit.
