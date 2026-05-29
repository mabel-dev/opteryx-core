# Ticket: assemble_fixed crash — corrective #3 (DRAKEN_NULL part + unmatched scatter)

> Third attempt. `bug-assemble-fixed-no-else-int-segfault.md` has been
> reported complete **twice**; the crash persists both times and the
> required `make et` test was never added (suite stayed 29-green by
> omitting the failing case). This ticket states the root cause and
> makes the gate un-dodgeable: **the two crashing queries must be tests
> in `make et`, and `make et` must be green.** A "done" report without
> the new tests visibly passing is rejected without review.

## The two repros (both currently SIGBUS)

```sql
-- A: NULL-producing branch, fixed-width (INT) result
SELECT CASE WHEN id > 4 THEN NULL ELSE id END FROM $planets LIMIT 6
  → expected [1, 2, 3, 4, None, None]   (currently SIGBUS)

-- B: partial match, no ELSE, fixed-width result
SELECT CASE WHEN id = 1 THEN id END FROM $planets LIMIT 4
  → expected [1, None, None, None]      (currently SIGBUS)
```

Control (works, must stay working):
```sql
SELECT CASE WHEN id < 100 THEN 1 END FROM $planets LIMIT 4   → [1,1,1,1]
SELECT CASE WHEN id<3 THEN 'a' ELSE 'b' END FROM $planets    → ['a','a','b','b']
```

Common factor in the crashes: **fixed-width result with rows that must
become NULL** (unmatched-no-ELSE rows, or a NULL-producing branch).
`assemble_fixed` in `opteryx/compiled/vector_ops/case_helpers.pyx`
mishandles them.

## Root cause (diagnosed — fix these, don't re-investigate from scratch)

### Cause A — DRAKEN_NULL part dereferenced

`THEN NULL` makes `_compute_compiled` produce a **DRAKEN_NULL** part
vector for that branch. A DRAKEN_NULL vector has **`data == NULL`** and,
by the §11 convention, **`validity == NULL` means "all valid"**.

In `assemble_fixed`'s scatter loop:
```cython
if not _sel_is_valid(src_uv.validity, j):   # validity==NULL → returns True (valid!)
    any_null = True
else:
    memcpy(out_data + row_r*itemsize,
           src_uv.data + dict_idx*itemsize,  # src_uv.data == NULL → deref NULL → SIGBUS
           itemsize)
```
For a DRAKEN_NULL part, `_sel_is_valid` wrongly reports every row valid
(NULL validity = all-valid), so the kernel takes the `memcpy` branch and
reads from a NULL `data` pointer.

Also: `template_vec` may be derived from the DRAKEN_NULL part
(`tmpl_uv.type == DRAKEN_NULL`), and `_draken_itemsize(DRAKEN_NULL)`
falls through to `return 1` — a meaningless width for a type with no
data. The template must be derived from a **non-null, fixed-width**
part, not a DRAKEN_NULL one.

**Fix A**: in `assemble_fixed`, treat a DRAKEN_NULL part as "all rows
null for this branch" — never dereference its `data`; mark those output
rows invalid. And derive `out_dtype`/`itemsize` from the first
**non-DRAKEN_NULL** part (or `else_part`); if every part is DRAKEN_NULL,
the whole output is null (use the bind-time-known result type for the
DrakenType, or emit a DRAKEN_NULL/all-null fixed vector of the right
type).

### Cause B — unmatched-no-ELSE scatter (repro B has no DRAKEN_NULL part)

Repro B (`CASE WHEN id=1 THEN id END`) has a normal INT part (id values
for the matched row) and no NULL literal — yet it still crashes. So
there is a **second** defect in the unmatched-rows path: when there are
unmatched rows and no `else_part`, those output rows must be left null
without any out-of-bounds access. Verify:
- `out_validity` sizing/`mask_tail` for `n = branch_id.shape[0]`.
- the matched-row scatter indexing (`rows_per_branch[bid]`, `selection[j]`)
  when a part covers fewer rows than `n`.
- that the unmatched branch (currently just `any_null = True`) doesn't
  leave `out_validity` partially uninitialised for the unmatched rows.

Instrument with `fprintf`/lldb on repro B specifically to pin the exact
line; A and B may share a fix or need two.

## Scope

**In scope**
- Fix `assemble_fixed` (`case_helpers.pyx`) for both causes: DRAKEN_NULL
  parts (no data deref; correct template/dtype) and the unmatched-no-
  ELSE fixed-width scatter.
- Add **both repros A and B as tests in `make et`** (value-checked,
  asserting the expected lists above). These tests **currently crash** —
  they must go green.
- Replace/augment the misleading
  `TestStandingBugs::test_case_when_without_else_returns_null` (which
  only covers the trivial `WHEN FALSE` short-circuit) with these real
  multi-row cases.

**Out of scope**
- The binop path (restored, working).
- COUNT(*)-WHERE bug (separate ticket).
- Phase 9 (paused).

## Verification — un-dodgeable gate

- `make c` clean.
- **`make et` green with the two NEW tests** (A and B) present and
  passing. Paste the `make et` output showing a test count **> 29** and
  the A/B test names passing. Omitting the tests is an automatic
  rejection — the entire failure mode of the prior two attempts was
  "fix not done, test not added, suite stays green."
- Paste the two repro outputs:
  - `CASE WHEN id > 4 THEN NULL ELSE id END FROM $planets LIMIT 6` → `[1,2,3,4,None,None]`
  - `CASE WHEN id = 1 THEN id END FROM $planets LIMIT 4` → `[1,None,None,None]`
- Control cases unchanged (`WHEN id<100 THEN 1 END` → `[1,1,1,1]`;
  string CASE unchanged).
- `make q` 100/100; `make clickbench` non-regressing.

## Constraints (CLAUDE.md)

- **Correctness is the priority — this is a live SIGBUS.** Fix the root
  cause (DRAKEN_NULL deref + unmatched scatter), not a symptom.
- **§11** — a DRAKEN_NULL vector has `data == NULL`; never dereference
  it. `validity == NULL` means all-valid for *normal* vectors, but a
  DRAKEN_NULL-**typed** vector is all-null regardless — handle the type,
  not just the validity pointer.
- **Fail fast** — no silent degradation; unmatched/NULL rows → null
  output, no crash, no wrong value.
- **Broken but honest** — acceptance is the pasted `make et` output with
  A and B passing. Two prior attempts reported done without them.
- **`make c` clean before done.**
- **Do not commit.**

## Files (verify before editing)

- `opteryx/compiled/vector_ops/case_helpers.pyx` — `assemble_fixed`
  (~line 238–330), `_sel_is_valid` (~56), `_draken_itemsize` (~68). The
  agent touched this file last round (9 ins / 4 del) but both repros
  still crash — the change was wrong or incomplete; re-diagnose against
  Causes A and B above.
- `opteryx/expression/evaluator/case_eval.pyx` — `_compute_compiled`
  produces the parts; confirm what a `THEN NULL` branch yields (likely a
  DRAKEN_NULL constant) so `assemble_fixed` handles it.
- `tests/test_expression_engine.py` — add tests A and B; fix the
  overclaiming `TestStandingBugs` test.

## Definition of done

- Repro A → `[1,2,3,4,None,None]`; Repro B → `[1,None,None,None]`. No
  SIGBUS. Pasted.
- `assemble_fixed` handles DRAKEN_NULL parts (no data deref; correct
  output dtype) and the unmatched-no-ELSE fixed-width scatter.
- `make et` contains A and B as passing tests; count > 29; output
  pasted. The trivial `WHEN FALSE` standing-bug test is replaced/
  augmented with the real multi-row cases.
- Control CASE queries unchanged.
- `make c` clean; `make q` 100/100; `make et` green; `make clickbench`
  non-regressing.

## Note (third attempt)

Two prior "done" reports left the crash live and the test absent. For
this attempt, the acceptance check the reviewer will run is literally:
`make et` shows tests A and B passing, and the two repros print the
expected lists. If those aren't in the report, it isn't done — there is
no partial credit on a SIGBUS that's been open since Phase 7.
