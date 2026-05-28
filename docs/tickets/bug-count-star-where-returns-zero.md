# Ticket: `COUNT(*)` with `WHERE` returns 0 (zero-column Morsel.take bug)

> Discovered Phase 2 (2026-05-27) while verifying the IS NULL kernel.
> Not introduced by any of the Phase 1–8 zero-Python work — a
> long-standing latent bug exposed by value-checked spot tests.
> `make q` does not catch it (shape-only suite).

## Reproduction

```sql
SELECT COUNT(*) FROM $planets WHERE id > 5      -- returns 0, should be 4
SELECT COUNT(*) FROM $planets WHERE id = 3      -- returns 0, should be 1
SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NULL
                                                -- returns 0, should be 305
```

Works correctly (control cases):

```sql
SELECT COUNT(*) FROM $planets                       -- returns 9    ✓
SELECT COUNT(id) FROM $planets WHERE id > 5         -- returns 4    ✓
SELECT SUM(id) FROM $planets WHERE id > 5           -- returns 30   ✓
SELECT COUNT(*), MAX(id) FROM $planets WHERE id > 5 -- COUNT=4      ✓
```

The diagnostic pattern: `COUNT(*) WHERE …` returns 0; the same query
with **any** other aggregate alongside (e.g. `MAX(id)`) returns the
correct COUNT. That's the bug's fingerprint.

## Root cause

Two collaborating components produce the bug:

1. **Projection pushdown emits zero-column morsels** when the query
   only counts rows (`COUNT(*)`). No columns are referenced, so the
   scan / projection produces morsels with `num_columns == 0` and
   `num_rows == N`.

2. **`Morsel.take(indices)`** in
   `draken/morsels/_morsel_shim.pyx:300` derives the output's
   `num_rows` from a per-column loop:

   ```cython
   def take(self, indices):
       cdef Morsel result = _make_morsel()
       result._col_names = list(self._col_names)
       cdef int n = len(self._columns)
       idx_list = list(indices)
       for i in range(n):
           nb_taken = (<Vector>self._columns[i])._nb.take(idx_list)
           result._nb.append(nb_taken)
           result._columns.append(Vector(nb_taken))
       return result
   ```

   When `n == 0` (zero-column morsel), the loop doesn't run.
   `result._nb` is never appended to. The underlying nanobind Morsel
   has no columns and hence `num_rows = 0` — regardless of how many
   indices were passed.

The filter operator uses `morsel.filter_mask(mask)` (same file, line
295), which is implemented as `self.take([indices where mask is True])`.
So when the filter receives a zero-column morsel and produces a
filtered result, the result is `num_rows = 0`. `CountStarAggregate.apply()`
at `opteryx/operators/aggregate/ungrouped_agg_count.pyx:25` then reads
`morsel.ptr.num_rows == 0` and accumulates nothing.

`SELECT COUNT(*)` without WHERE works because no filter is applied —
the scan emits morsels directly, and the aggregate reads the original
`num_rows`. `COUNT(*), MAX(id)` works because the `MAX(id)` reference
prevents the projection pushdown from eliminating `id`, so morsels
have ≥1 column and the take loop runs.

## Fix

`Morsel.take(indices)` must produce a result whose `num_rows` reflects
`len(indices)`, **regardless of column count**. Two equivalent
approaches:

**A. Explicit zero-column path.** Detect `n == 0` and construct a
zero-column morsel of the right length:

```cython
def take(self, indices):
    cdef Morsel result = _make_morsel()
    result._col_names = list(self._col_names)
    cdef int n = len(self._columns)
    idx_list = list(indices)
    if n == 0:
        # Zero-column morsel: row count comes from the index list, not
        # from any column. Set num_rows directly on the underlying nb
        # Morsel.  Verify the nb Morsel exposes a num_rows setter or a
        # constructor that takes a row count.
        result._nb.set_num_rows(<size_t>len(idx_list))
        return result
    for i in range(n):
        nb_taken = (<Vector>self._columns[i])._nb.take(idx_list)
        result._nb.append(nb_taken)
        result._columns.append(Vector(nb_taken))
    return result
```

If `draken.draken_native.Morsel` doesn't expose a row-count setter,
the cleanest path is to add one (small C++ addition in
`draken/draken_native.cpp` — search for the nb Morsel class). The
setter is meaningful only for zero-column morsels; for any non-empty
morsel `num_rows` is derived from its columns.

**B. Push the responsibility upstream.** Prevent zero-column morsels
from existing in the first place by ensuring projection pushdown
keeps at least one cheap column (e.g. the first scan column) when the
plan has a `COUNT(*)` downstream. This is more invasive (planner
work) and brittle (any future optimisation that drops columns
reintroduces the bug). **Don't do this**; fix it at the morsel layer.

Recommendation: **A**. Surface in PR if the nanobind Morsel API needs
a setter added.

Apply the same fix pattern to `filter_mask` if it has its own loop
elsewhere — verify; today it delegates to `take`, so fixing `take`
fixes both.

## Scope

**In scope**
- `draken/morsels/_morsel_shim.pyx:295–309` — fix `take()` and verify
  `filter_mask()` is correctly downstream of it.
- `draken/draken_native.cpp` — if the nb Morsel class doesn't expose
  a row-count setter that works for zero-column morsels, add one.
  Smallest possible API — single setter, documented as zero-column-only.
- New tests (place under `tests/` next to existing aggregate tests):
  value-checking, not shape-only. **At minimum**:
  - `SELECT COUNT(*) FROM $planets WHERE id > 5` → 4
  - `SELECT COUNT(*) FROM $planets WHERE id = 3` → 1
  - `SELECT COUNT(*) FROM $planets WHERE id < 0` → 0 (the actual zero case)
  - `SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NULL` → 305
  - `SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NOT NULL` → 52
  - Sum-check: those two add to `SELECT COUNT(*) FROM testdata.astronauts` (357).
- Add the value-checking tests to a place `make q` runs (so future
  regressions catch this kind of bug).

**Out of scope**
- Any planner / projection-pushdown change. The morsel-layer fix is
  surgical; the upstream planner is doing what it's allowed to do.
- Broader audit of zero-column morsel handling elsewhere (e.g. JOIN,
  sort). **Surface findings** if you spot adjacent bugs while fixing
  this one, but **do not** take them on here.

## Verification

- `make c` clean **fresh build**.
- `make q` 100/100.
- New value-checking tests all green.
- The repro queries return the right numbers:
  ```
  SELECT COUNT(*) FROM $planets WHERE id > 5      → 4
  SELECT COUNT(*) FROM $planets WHERE id = 3      → 1
  SELECT COUNT(*) FROM $planets WHERE id < 0      → 0
  SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NULL    → 305
  SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NOT NULL → 52
  ```
- The sum-check holds: 305 + 52 = 357 (= `COUNT(*) FROM testdata.astronauts`).
- Existing control cases unchanged:
  ```
  SELECT COUNT(*) FROM $planets                    → 9
  SELECT COUNT(id) FROM $planets WHERE id > 5      → 4
  SELECT SUM(id) FROM $planets WHERE id > 5        → 30
  ```
- `make clickbench` non-regressing.

## Constraints (from CLAUDE.md)

- **Correctness non-negotiable.** This is a value-not-shape bug — a
  shape-only test suite let it through. The fix MUST add
  value-checking tests so a regression is caught next time.
- **Fail fast.** If the nanobind Morsel API needs a new method,
  add it explicitly — don't work around with hacks that pretend a
  zero-column morsel has rows by some other proxy.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **`make c` clean before claiming completion.**
- **Do not commit.**

## Files (verify before editing)

- `draken/morsels/_morsel_shim.pyx:295–309` — `filter_mask` /
  `take`. Verify line numbers against current source.
- `draken/draken_native.cpp` — search for `class Morsel` (or
  `nb::class_<Morsel>`). Confirm the surface. If a num_rows setter
  is needed, add it minimally.
- `opteryx/operators/aggregate/ungrouped_agg_count.pyx:25` —
  `CountStarAggregate.apply()`. **Don't change this** — verify that
  with the morsel-take fix, this reads the correct `num_rows`.
- `opteryx/operators/filter/filter.pyx:194–214` — confirm filter
  reaches `take` via `filter_mask`. Verify no other paths bypass.
- Test location — pick whichever existing aggregate test file the
  team prefers. `tests/integration/sql_battery/test_shapes_basic.py`
  is **shape-only** and missed this; add to a value-checking suite
  or create one.

## Pre-flight reading

1. This ticket end-to-end.
2. `draken/morsels/_morsel_shim.pyx` end to end. It's not large.
3. `opteryx/operators/aggregate/ungrouped_agg_count.pyx` —
   particularly `CountStarAggregate` lines 14–40. Understand what
   `apply` reads.
4. `opteryx/operators/filter/filter.pyx:194–214` — the filter
   dispatch.

## Definition of done

- `Morsel.take(indices)` returns a result whose `num_rows ==
  len(indices)` regardless of `len(self._columns)`. Tested for
  zero-column case explicitly.
- The five new value-checking tests in §Verification land in a suite
  that `make q` runs.
- All six repro queries return the documented correct values.
- All control queries unchanged.
- `make c` clean; `make q` 100/100 with fresh build.
- `make clickbench` non-regressing.

## Side-notes to surface in PR

- The shape-only nature of `make q` let this bug live silently. If
  the test infrastructure has a way to register value-checked
  queries cheaply, expand that mechanism. Out of scope here; flag for
  a separate ticket if non-trivial.
- Two other correctness bugs in the same backlog are still open:
  - `assemble_fixed` segfault on `CASE WHEN x THEN int_col END` (no
    ELSE, INT result) — Phase 7 finding.
  - 4 test files import deleted symbols (`test_map_access_operator.py`,
    `test_draken_comparisons.py`, `test_phase3_array_ops.py`,
    `test_phase1_evaluator.py`) — Phases 3 & 4 findings.
  Same shape: pre-existing, masked by shape-only tests.
- If you discover other zero-column-morsel bugs while fixing this
  one (likely candidates: join, sort, distinct, sub-morsel
  construction in CASE), **note them, do not fix here**. Each gets
  its own ticket.
