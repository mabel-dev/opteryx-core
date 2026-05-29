# Ticket: Shuffle cleanup — CORRECTIVE #3 (close-out: datepart segfault + realistic gate)

> Corrective-2 is **mostly done and verified** — do NOT redo it. This
> ticket closes the two remaining issues: (1) the re-pointed
> `test_datepart_correctness.py` now **segfaults**, and (2) corrective-2's
> "collect-clean dirs" gate was **unachievable** because those dirs are
> full of *pre-existing, unrelated* test rot. This ticket states what's
> already done, fixes the segfault, and sets a gate that can actually be
> met.

## Already done & verified (do NOT touch)

- All 15 Category-1 shuffle/group-state test files deleted (confirmed in
  `git status` as `D`), including `tests/unit/operators/conftest.py`.
- `grep -rn "opteryx.operators.shuffle\|shuffle_node\|opteryx.operators.group_state_store" tests/`
  → **returns nothing**. No dead shuffle imports remain.
- 3 of 4 Category-2 files deleted: `test_projection_constant_morsel.py`,
  `tests/draken/morsels/test_align_tables.py`,
  `tests/draken/vectors/test_vector_encoding.py`.
- Production code untouched. `make q` 137/137; `make et` 41; `make dt`
  unaffected.

## Issue 1 — `test_datepart_correctness.py` segfaults (must fix)

Corrective-2 *re-pointed* (kept) `tests/unit/functions/test_datepart_correctness.py`
by stripping its dead imports. But the file's dead import previously made
it **uncollectable**, which masked a latent crash. Now that it runs, it
**segfaults**:

```
Fatal Python error: Segmentation fault
  tests/unit/functions/test_datepart_correctness.py:94
  in test_datepart_timestamp_arrow_all_supported_units
```

Root: the test passes a **raw PyArrow array**
(`pa.array([BASE_DT], type=pa.timestamp("us"))`) straight into the
engine-internal `date_part()`
(`opteryx.expression.functions.implementations.temporal.date_part`),
which expects a draken `Vector`. The function dereferences the wrong type
and crashes. This is a **stale-API test** (same class as its deleted
siblings — they fed internal kernels objects the engine no longer
accepts).

**Action:** delete `tests/unit/functions/test_datepart_correctness.py`.
It is a stale-API file like the rest of Category 2; salvaging it means
migrating every assertion off the `date_part(pyarrow_array)` internal
calling convention, which is the separate test-API-migration effort, not
this cleanup. Flag the lost coverage (see "Coverage flagged" below).

> Leaving a segfaulting test in the tree is not acceptable as "done" —
> a crashing test is worse than a missing one.

## Issue 2 — realistic gate (corrective-2's gate was unachievable)

`tests/unit/{operators,functions,aggregations}` do **not** collect clean,
and that is **not** this cleanup's fault. The remaining ~33 collection
errors are pre-existing, unrelated rot — none mention shuffle/group_state.
Representative causes (do NOT fix here):

- `tests/helpers.py::execute_and_fetch_all` → `morsel.to_arrow()`
  (`AttributeError: 'Morsel' has no attribute 'to_arrow'`) — breaks every
  test using that helper (incl. `test_agg_count.py`).
- `No module named 'draken.interop.arrow'` (×10),
  `'draken.vectors.string_vector'` (×4),
  `'opteryx.compiled.aggregations'` (×4),
  `'opteryx.operators.{parquet_read,outer_join,distinct,...}_node'`,
  `cannot import name 'date_trunc' / 'vector_iif' / ...`.

This is the standing test-suite API-migration debt (see
`memory/test_suite_api_migration.md`). It is **out of scope** here. Do
not migrate it; do not delete those files. Just don't make it worse.

## Coverage flagged (lost to Category-2 deletions — for a future decision)

These behaviours lost their (already-dead) tests; record for possible
reconstruction against the live API later:
- `test_projection_constant_morsel.py` — projection over constant-encoded morsels.
- `test_align_tables.py` — morsel/table alignment with dict encoding.
- `test_vector_encoding.py` — vector encoding-shape contract (Dense/Constant/Dict/RLE).
- `test_datepart_correctness.py` — DATEPART unit correctness incl. the
  typed-int64 dictionary dispatch path.

## Discovered (separate, do NOT fix here)

`date_part()` **segfaults** on a non-`Vector` argument instead of failing
fast — a §1 (fail-fast, no silent crash) violation, currently only
reachable via the stale test above. Worth a hardening ticket later; out
of scope now.

## Verification — achievable gate

- `git status --short` shows `test_datepart_correctness.py` as deleted
  (`D`), alongside the already-deleted shuffle files. (Paste.)
- No segfault from the functions dir:
  `python -m pytest tests/unit/functions --collect-only -q` completes
  **without a segfault** (it may still report pre-existing ImportError
  collection errors — that's the documented rot, acceptable; a crash is
  not).
- `grep -rn "opteryx.operators.shuffle\|shuffle_node\|opteryx.operators.group_state_store" tests/`
  → nothing. (Paste.)
- `make q` 137/137; `make et` 41; `make dt` unaffected. (Paste.)

## Constraints (CLAUDE.md)

- **Test-only.** No `opteryx/`, `draken/`, `rugo/` changes. Do NOT fix
  the pre-existing rot or the `date_part` robustness bug here — STOP and
  report scope per §8.
- **Honest gate** — a segfaulting test is a hard fail; the close-out
  requires the crash gone and the deletion visible in `git status`.
- **Do not commit.**

## Definition of done

- `test_datepart_correctness.py` deleted; `tests/unit/functions` no
  longer segfaults on collect. (Pasted.)
- No shuffle/group_state imports anywhere in `tests/`. (Pasted grep.)
- Coverage-flag list recorded in the done report.
- `make q` 137/137; `make et` 41; `make dt` unaffected.
- Pre-existing unrelated rot explicitly left alone and named.
