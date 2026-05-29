# Ticket: `COUNT(*)` with `WHERE` still returns 0 — bug moved from `take` to `select`

> Supersedes the root-cause section of
> `bug-count-star-where-returns-zero.md`. That ticket correctly
> identified the *symptom* and one contributing site (`Morsel.take`
> losing `num_rows` for zero-column morsels). `take` was since fixed
> (it now sets `_zero_col_num_rows`), **but the bug still reproduces** —
> it moved to `Morsel.select`, which has the identical zero-column
> `num_rows`-loss defect. Verified against the current build before
> writing this ticket (see Evidence). Do not re-fix `take`; it is
> already correct.

## Status: OPEN — reproduces on current build

## Reproduction (current, value-checked)

```sql
SELECT COUNT(*) FROM $planets WHERE id > 5   -- returns 0, should be 4
SELECT COUNT(*) FROM $planets WHERE id = 3   -- returns 0, should be 1
```

Controls (pass today, must stay passing):

```sql
SELECT COUNT(*) FROM $planets                       -- 9   ✓ (no filter)
SELECT COUNT(id) FROM $planets WHERE id > 5         -- 4   ✓
SELECT COUNT(*), MAX(id) FROM $planets WHERE id > 5 -- 4   ✓ (MAX keeps `id`)
```

Fingerprint unchanged: `COUNT(*) WHERE …` returns 0; add **any** other
aggregate that references a column and the COUNT becomes correct.

## Evidence (run during diagnosis — do not re-derive from scratch)

`make c` clean. With the current build:

```
FAIL COUNT(*) WHERE id>5 -> 0 (exp 4)
FAIL COUNT(*) WHERE id=3 -> 0 (exp 1)
PASS COUNT(*) no where   -> 9
PASS COUNT(id) WHERE id>5 -> 4
PASS COUNT(*),MAX(id) WHERE -> 4
```

Direct probe of the moved defect (zero-column `select` loses rows):

```python
# after-filter morsel: num_rows=4 num_cols=1 names=[b'id']
m.take(array('i', range(4)))  # num_rows=4 num_cols=1   ✓ (take is fixed)
m.select([])                  # num_rows=0 num_cols=0   ✗ should be num_rows=4
```

## Root cause (verified, current incarnation)

The filter operator
(`opteryx/operators/filter/filter.pyx::FilterNode._dispatch_push`, ~L194)
does, after computing the mask:

```cython
filtered = morsel.filter_mask(mask)          # take() — now preserves num_rows ✓
...
if self.post_filter_columns:
    keep = [c for c in filtered.column_names if c in self.post_filter_columns]
    if len(keep) < filtered.num_columns:
        filtered = filtered.select(keep)     # COUNT(*) → keep == []  ✗
if filtered.num_rows > 0:                     # 0 → morsel dropped
    self._emit_cdef(filtered)
```

For `COUNT(*)` nothing downstream needs a column, so `keep == []` and
`filtered = filtered.select([])`. `Morsel.select`
(`draken/morsels/_morsel_shim.pyx:280`) builds its result via
`_make_morsel()` (which sets `_zero_col_num_rows = 0`) and only ever
sets row count *implicitly* by appending columns:

```cython
def select(self, col_names):
    cdef Morsel result = _make_morsel()      # _zero_col_num_rows = 0
    for name in col_names:                    # empty → loop body never runs
        ...
        result._columns.append(...)
    return result                             # 0 columns, num_rows == 0
```

`Morsel.num_rows` returns `_zero_col_num_rows` when there are no columns
(`_morsel_shim.pyx:62`). So `select([])` yields `num_rows == 0`, the
filter drops the morsel at the `num_rows > 0` guard, and
`CountStarAggregate` never sees the 4 surviving rows → `COUNT(*)` == 0.

This is the **same class of bug** the `take` fix addressed (zero-column
morsels must carry an explicit row count), just at a different method.

## Scope

**In scope**
- Fix `Morsel.select` so a zero-column result preserves the source row
  count: `result._zero_col_num_rows = self.num_rows`.
- **Audit the sibling result-producing methods in `_morsel_shim.pyx`
  for the same defect** and fix any that can yield a zero-column morsel
  while a non-zero row count is expected. Candidates to check (verify
  each against its callers; only fix the ones that are actually wrong):
  `rename` (L293), `copy` (L326, both the `columns=` subset path and the
  bare-copy path), `slice` (L322, delegates to `take` — likely already
  OK), and any other method that calls `_make_morsel()` and conditionally
  appends columns. Row-count semantics differ per method
  (select/rename/bare-copy preserve `self.num_rows`; mask/take paths use
  the filtered count) — set the correct one, don't blindly copy
  `self.num_rows`.
- Add value-checked regression tests (see Verification).

**Out of scope**
- `Morsel.take` — already fixed; do not touch.
- The CASE/`assemble_fixed` SIGBUS — resolved separately
  (`bug-assemble-fixed-RESOLVED-real-root-cause.md`).
- Any change to the filter operator's projection logic; the operator is
  correct, the morsel primitive is not.

## Fix sketch

```cython
def select(self, col_names):
    cdef Morsel result = _make_morsel()
    for name in col_names:
        ...
    if not result._columns:
        result._zero_col_num_rows = self.num_rows   # preserve row count
    return result
```

Apply the analogous guard to each sibling method confirmed defective in
the audit.

## Verification — un-dodgeable gate

- `make c` clean.
- **`make dt` green with a NEW draken unit test** asserting
  `Morsel.select([]).num_rows == source.num_rows` (and the same for any
  sibling method fixed in the audit). This pins the fix at the primitive
  level where the bug lives.
- **A value-checked query test that currently returns the wrong answer
  and must go green**, asserting:
  - `SELECT COUNT(*) FROM $planets WHERE id > 5` → `4`
  - `SELECT COUNT(*) FROM $planets WHERE id = 3` → `1`
  Add it to the established value-checked gate (`make et` /
  `tests/test_expression_engine.py`) or, if a more appropriate
  aggregate-level value suite is created, there — but it **must** be a
  value assertion (not shape-only) and the report must paste it passing.
- Controls unchanged: `COUNT(*)` → 9; `COUNT(id) WHERE id>5` → 4;
  `COUNT(*), MAX(id) WHERE id>5` → COUNT 4.
- `make q` 137/137; `make clickbench` non-regressing.

## Constraints (CLAUDE.md)

- **Correctness is the priority** — this returns a silently wrong answer
  (0 instead of 4), worse than a crash. Fix the root cause in the morsel
  primitive, not the filter operator.
- **Fail fast, no silent degradation** — a zero-column morsel with N
  surviving rows must report `num_rows == N`, never 0.
- **`make q` is shape-only and will NOT catch this** — the bug is a
  wrong scalar value with a correct result shape. The value-checked
  tests above are the only real gate.
- Cython must stay typed (no `object` params/returns); `_morsel_shim.pyx`
  is the Python-edge shim — keep edits within its existing style.
- **Do not commit.**

## Files (verify before editing)

- `draken/morsels/_morsel_shim.pyx` — `select` (L280, the fix);
  `_make_morsel` (L34, sets `_zero_col_num_rows = 0`); `num_rows`
  property (L62); `take` (L308, reference for the already-correct
  pattern); `rename`/`copy`/`slice` (audit).
- `opteryx/operators/filter/filter.pyx` — `_dispatch_push` (L194); read
  only, to confirm the `select(keep=[])` call site and the
  `num_rows > 0` drop guard. **Do not change.**
- `opteryx/operators/aggregate/ungrouped_agg_count.pyx` — `CountStar`
  reads `morsel.ptr.num_rows`; read only, to confirm it's the consumer.

## Definition of done

- Both repros return 4 and 1 (pasted). Controls unchanged (pasted).
- `Morsel.select` (and any sibling fixed in the audit) preserves
  `num_rows` for zero-column results.
- `make dt` has the new primitive-level test passing; the value-checked
  query test passes; both pasted.
- `make c` clean; `make q` 137/137; `make clickbench` non-regressing.
