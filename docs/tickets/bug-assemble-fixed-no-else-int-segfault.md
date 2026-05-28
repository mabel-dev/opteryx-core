# Ticket: `assemble_fixed` segfault on CASE WHEN x THEN int_col END (no ELSE, fixed-width result)

> Discovered Phase 7 (2026-05-27) while writing value-checked spot
> tests for the CASE rewrite. Not introduced by Phase 7 — the bind-time
> kernel resolver routes correctly; the kernel itself has a memory /
> state-lifetime bug exposed by the test pattern.
> `make q` does not catch it (shape-only suite).

## Reproduction

A single query in a fresh session works:

```python
session = opteryx.session()
list(session.execute_to_morsels("SELECT CASE WHEN id < 100 THEN 1 END FROM $planets LIMIT 4"))
# → [1, 1, 1, 1]   ✓
```

The same query repeated in the same session works:

```python
session = opteryx.session()
for _ in range(5):
    list(session.execute_to_morsels("SELECT CASE WHEN id < 100 THEN 1 END FROM $planets LIMIT 4"))
# → all [1, 1, 1, 1]   ✓
```

**Two different CASE-no-ELSE-INT queries in the same session crash on
the second** (or sometimes the first):

```python
session = opteryx.session()
list(session.execute_to_morsels("SELECT CASE WHEN id < 100 THEN 1 END FROM $planets LIMIT 4"))
# → [1, 1, 1, 1]
list(session.execute_to_morsels("SELECT CASE WHEN id = 1 THEN id END FROM $planets LIMIT 4"))
# → Fatal Python error: Segmentation fault
```

Control cases (all work):

```sql
SELECT CASE WHEN id < 5 THEN 'x' ELSE 'y' END FROM $planets       -- VARCHAR result
SELECT CASE WHEN id < 5 THEN TRUE ELSE FALSE END FROM $planets    -- BOOL result
SELECT CASE WHEN id < 5 THEN 1 ELSE 0 END FROM $planets           -- INT with ELSE
SELECT CASE WHEN id > 0 THEN 'yes' END FROM $planets              -- STRING no-ELSE
```

## Necessary conditions for the crash

All five must hold:

1. CASE expression with **no ELSE branch**.
2. Result type is **fixed-width** (INT64, FLOAT64, DATE, TIMESTAMP) —
   not VARCHAR/BLOB and not BOOLEAN. `assemble_flat_string` and
   `assemble_bool` are safe.
3. **At least one row matches** some WHEN branch (if all rows are
   unmatched, the `_case_fn` closure short-circuits at
   `case_eval.pyx:319` via `vector_null_from_length(n)` and never calls
   `assemble_fixed`).
4. **Multiple distinct queries** in the same session — single query
   alone or the same query repeated does not crash. Some state leaks
   from one execution into the next.
5. The bind-time path goes through `_ASSEMBLE_FIXED = 1` (verified by
   spying on `build_case_fn`'s `kernel_type` argument — it was `-1`
   meaning runtime fallback, but the runtime fallback also dispatches
   to `assemble_fixed`).

## Likely root cause

The crash is in `assemble_fixed` (or its result lifecycle) at
`opteryx/compiled/vector_ops/case_helpers.pyx:238`. The function:

1. Allocates `out_data` and `out_validity` via `draken_malloc`
   (lines 269–270).
2. Scatters branch parts into `out_data` using
   `src_uv.selection[j]` to find source indices and `rows_per_branch[i]`
   for output positions (lines 289–306).
3. If no nulls observed, **frees** `out_validity` and sets it to NULL
   (lines 326–328).
4. Returns `_vec_from_decoded(out_data, out_validity, n, out_dtype)`.

Three concrete suspects, ranked by likelihood:

### Suspect 1 — `_vec_from_decoded` ownership semantics

If `_vec_from_decoded` does NOT take ownership of `out_data` (treats
it as borrowed), the buffer is freed when the function returns and
the returned Vector points at freed memory. The first query
"succeeds" because nothing else has reused that memory yet; the
second query allocates over the same region, corrupting the first
query's result vector while the executor still holds it via
`anchor[sp]`. When the result is consumed (`to_pylist()`,
`column_names`, etc.) the crash fires.

Verify by reading the `_vec_from_decoded` signature (imported from
`draken/vectors/vector.pyx`). If it owns: rules this out. If it
borrows: this is the bug.

### Suspect 2 — Source-index reads out of bounds for constant vectors

For `THEN 1` (literal), `_compute_compiled` produces a
**constant-encoded** Vector (`data_length == 1`, `length == 4`,
`selection` = global zero of length 4). The loop reads
`src_uv.selection[j]` for `j = 0..rows_i.shape[0]-1`. If the
global zero array isn't sized to accommodate the sub-morsel's row
count, `selection[j]` reads past the end of the buffer.

Verify: log `src_uv.data_length`, `src_uv.length`, and the address
range of `src_uv.selection` from inside `assemble_fixed`. If
`selection` is shorter than `rows_i.shape[0]`, that's the bug.

Per CLAUDE.md §11, the "Constant" shape has `selection` = global
zero vector with `data_length == 1`. If that global zero is sized
once for some fixed N and the actual logical lengths exceed N, we
have a long-standing latent issue.

### Suspect 3 — `out_validity` lifecycle when freed mid-function

Lines 326–328 free `out_validity` and set it to NULL when no nulls
were observed. But it was set during the scatter loop via
`_sel_set_true_bit(out_validity, row_r)` at line 306. So:
- `any_null` starts False.
- Loop runs over parts; each matched row triggers `_sel_set_true_bit`
  (which marks valid).
- After the loop, if `unmatched.shape[0] > 0` AND `else_part is None`,
  line 323-324 sets `any_null = True`.
- Otherwise `any_null` stays False, and lines 326-328 free
  `out_validity`.

For our crash repro: `id < 100 THEN 1 END` — all 4 rows match → no
unmatched → `any_null = False` → out_validity freed. OK fine.

But consider `id = 1 THEN id END` — 1 row matches (id=1), 3 don't
match → `unmatched.shape[0] = 3` → line 323-324 sets
`any_null = True` → out_validity is **not** freed → kept and passed
to `_vec_from_decoded`.

The difference between the two failing queries matches: first query
takes the "free validity" path; second takes the "keep validity"
path. If one path corrupts memory the other reads, that produces
the second-query crash.

## Diagnostic plan

The agent must do the investigation. Suggested order:

1. **Read `_vec_from_decoded` in `draken/vectors/vector.pyx`** and
   the corresponding C function it ultimately calls
   (`draken/core/bitmap_ops.cpp:bool_vector_from_bits` is a
   precedent — it COPIES rather than borrows; verify
   `vec_from_decoded` is the same).
2. **Instrument `assemble_fixed`** with `fprintf(stderr, ...)` calls
   logging `out_data` pointer, `out_validity` pointer, `n`,
   `any_null`, and the source `src_uv.data_length`/`length`/
   `selection` pointer per part. Re-run the two-query repro;
   correlate the crash with the logs.
3. **Run under address sanitizer** if Phase-8b's tree-walker
   deletion hasn't broken ASAN compatibility. `make c` with
   `CFLAGS="-fsanitize=address"` and `LDFLAGS="-fsanitize=address"`
   produces an instrumented build; ASAN will pinpoint
   use-after-free / out-of-bounds reads.
4. **Once you have the smoking gun**, fix in `assemble_fixed` or
   `_vec_from_decoded` (whichever is at fault).

## Fix expectations

The fix is in **one of**:

- `_vec_from_decoded`: ensure it copies (or takes ownership of) the
  input buffers. Adjust `assemble_fixed` to match the ownership
  contract.
- `assemble_fixed`: stop freeing buffers it's about to hand to
  `_vec_from_decoded`. Or stop reading constant-vector selection
  past its actual length.
- The "global zero" selection vector backing constant-encoded
  vectors: ensure it's sized to the maximum logical length any
  caller will use, or use a dedicated constant-shape access path
  (`if data_length == 1: dict_idx = 0` shortcut, no `selection[j]`
  read).

The architect's preference per CLAUDE.md §11 should be respected:
the uniform access pattern is `data[selection[i]]`. If the kernel
needs a constant-shape fast path, it must produce identical results
to the uniform path.

## Scope

**In scope**
- `opteryx/compiled/vector_ops/case_helpers.pyx` — `assemble_fixed`
  (lines 238–330). Fix the bug; do not touch the other two
  assemble kernels unless the same bug applies (read carefully
  before generalising).
- `draken/vectors/vector.pyx` — `from_decoded` /
  `vec_from_decoded` if Suspect 1 is the issue. Surface the change
  here; nb Vector ownership is a draken-layer concern.
- Value-checking tests for the failing patterns; land them where
  `make q` runs.

**Out of scope**
- Audit of other kernels that allocate via `draken_malloc` and
  return via `_vec_from_decoded` (likely the same family —
  `assemble_bool`, `assemble_flat_string`, `_bv_truth_test_native`,
  `_is_null_from_dv`, etc.). **Note any adjacent suspects in PR
  but do not fix here.** Each needs its own ticket.
- The Phase 7 `kernel_type == -1` runtime fallback — that's a
  separate cleanliness issue, not a correctness bug.
- Any change to bind-time `inferred_type` propagation in the
  binder.

## Verification

- `make c` clean **fresh build**.
- `make q` 100/100.
- The repro that previously crashed now runs cleanly:
  ```python
  session = opteryx.session()
  list(session.execute_to_morsels("SELECT CASE WHEN id < 100 THEN 1 END FROM $planets LIMIT 4"))
  list(session.execute_to_morsels("SELECT CASE WHEN id = 1 THEN id END FROM $planets LIMIT 4"))
  ```
- New value-checking tests added to a suite `make q` runs:
  - `SELECT CASE WHEN id < 100 THEN 1 END FROM $planets LIMIT 4` → `[1, 1, 1, 1]`
  - `SELECT CASE WHEN id = 1 THEN id END FROM $planets LIMIT 4` → `[1, None, None, None]`
  - `SELECT CASE WHEN id < 0 THEN id END FROM $planets LIMIT 4` → `[None, None, None, None]`
  - `SELECT CASE WHEN id < 100 THEN id END FROM $planets LIMIT 4` → `[1, 2, 3, 4]`
  - The cross-query repro pattern: run two distinct CASE-no-ELSE-INT
    queries in the same session, both succeed.
- All control cases unchanged:
  - `SELECT CASE WHEN id < 5 THEN 'x' ELSE 'y' END FROM $planets` →
    `['x','x','x','x','x','y','y','y','y']`
  - `SELECT CASE WHEN id < 5 THEN TRUE ELSE FALSE END FROM $planets`
  - `SELECT CASE WHEN id < 5 THEN 1 ELSE 0 END FROM $planets`
- `make clickbench` non-regressing.

## Constraints (from CLAUDE.md)

- **Correctness non-negotiable.** This is a SIGSEGV — the most
  serious failure mode. Fix the actual cause; do not work around
  by special-casing the test queries.
- **Fail fast.** If you discover the bug is in `_vec_from_decoded`
  semantics being unclear (caller-owns vs callee-owns), document
  the ownership contract in a docstring/comment as part of the fix.
- **No `try/except` for control flow** — banned. The repro uses
  `try/except` for diagnostic output only; that's fine in the
  test, banned in production.
- **No `hasattr`** — banned.
- **No silent fallbacks.** If you can't reproduce the crash after
  the fix, make sure the spot tests actually exercise the path
  (not just compile and parse).
- **`make c` clean before claiming completion.**
- **Do not commit.**

## Files (verify before editing)

- `opteryx/compiled/vector_ops/case_helpers.pyx:238–330` —
  `assemble_fixed`. Read end-to-end before changing anything.
- `opteryx/compiled/vector_ops/case_helpers.pyx:39` —
  `from draken.vectors.vector cimport Vector, from_decoded as _vec_from_decoded`.
  Trace `from_decoded` to its definition.
- `draken/vectors/vector.pyx` — `from_decoded` implementation.
- `draken/core/bitmap_ops.cpp:26` — `bool_vector_from_bits` for
  the COPY pattern reference (its docstring explains the ownership
  rationale).
- `opteryx/expression/evaluator/case_eval.pyx:303–337` —
  the `_case_fn` closure that calls `assemble_fixed`. Understand the
  shape of `parts`, `else_part`, `rows_per_branch`, `unmatched` it
  produces.

## Pre-flight reading

1. This ticket end-to-end.
2. `opteryx/compiled/vector_ops/case_helpers.pyx` — entire file
   for the three assemble kernels. Compare their ownership/cleanup
   patterns; the safe two (`assemble_bool`, `assemble_flat_string`)
   may reveal what the broken one does differently.
3. `draken/vectors/vector.pyx` `from_decoded` — what does it own,
   what does it borrow?
4. `draken/core/buffers.h` — the `DrakenVector` struct comment
   on `selection` (the "global zero vector" convention for constant
   shapes).
5. CLAUDE.md §11 — the Vector Model.

## Definition of done

- Diagnosis documented in PR description: which suspect (1, 2, or
  3 above) was the actual cause, with file:line evidence.
- The bug is fixed at root cause, not symptomatically.
- All four repro queries return the documented correct values.
- The two-distinct-queries-same-session pattern no longer crashes.
- Value-checking tests land in a suite `make q` runs.
- All control queries unchanged.
- `make c` clean; `make q` 100/100 with fresh build.
- `make clickbench` non-regressing.

## Side-notes to surface in PR

- The shape-only nature of `make q` let this bug live silently and
  is now responsible for at least two undetected correctness bugs
  (this one + the `COUNT(*) WHERE` zero-column-take bug). The
  test-infrastructure expansion is its own ticket; **flag this
  ticket's findings as additional evidence**.
- Adjacent kernels that may share the same bug shape:
  `assemble_bool`, `assemble_flat_string`, `_bv_truth_test_native`,
  `_is_null_from_dv` (Phase 2 addition), `_bv_op2_native`,
  `_bv_not_native`. **Audit each for the same `draken_malloc` →
  free-or-keep → `_vec_from_decoded` flow** during your diagnosis;
  if you find others affected, flag them. Do not fix them here.
- Other open correctness tickets in the queue:
  - `COUNT(*) FROM x WHERE …` returns 0 (zero-column-take bug)
  - 4 test files import deleted symbols (Phases 3 & 4)
- Phase 8b dead-code cleanup still pending; orthogonal to this
  ticket.
