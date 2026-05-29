# Ticket: Remove shuffle orphan tests — CORRECTIVE #2 (prior attempt did nothing)

> Supersedes `cleanup-remove-shuffle-operator-orphan-tests.md`. That
> ticket was **reported complete but no work was done**: `git status`
> shows zero deletions, all seven `tests/**/*shuffle*` files are still on
> disk, and `tests/unit/operators/conftest.py` still has
> `import opteryx.operators.shuffle.partitioning` at line 3. A "done"
> report whose deletions don't appear in `git status` is an automatic
> rejection. This corrective also **fixes the original ticket's
> under-scoping** (the dead imports reach beyond the shuffle tests).

## Status: OPEN — nothing done yet

## Verified facts (checked against current tree — do not re-derive)

- **No production code references shuffle / group_state_store / the
  encoding constants.** This is test-only.
- These symbols exist **nowhere importable** in `opteryx/` or `draken/`:
  `ShuffleGroupByOperation`, `ShuffleGroupByOperationV2`, `BinStore`,
  `ShuffleNode`, `AggregationSpec` (the live one is in
  `grouped_aggregate_hashed/_node.pxi`, a *different* import path),
  `opteryx.operators.shuffle.partitioning`, and
  `DRAKEN_ENCODING_CONSTANT/DENSE/DICTIONARY/RLE`.
- `draken.encoding` **does not exist** (`import draken.encoding` →
  ModuleNotFoundError), despite a stale comment in
  `opteryx/expression/evaluator/type_coercion` claiming the constants
  "mirror draken.encoding.*". So the encoding constants have **no live
  home** to re-point to without reconstructing them.

## Category 1 — pure shuffle / group-state-engine tests: DELETE

Every file below imports only dead shuffle/group-state symbols and
cannot be collected. Delete them:

```
tests/unit/operators/test_shuffle_bin_store.py
tests/unit/operators/test_shuffle_group_by.py
tests/unit/operators/test_shuffle_group_by_phase1.py
tests/unit/operators/test_shuffle_merge.py
tests/unit/operators/test_shuffle_node.py
tests/unit/operators/test_shuffle_partitioning.py
tests/unit/operators/test_group_state_store_constant_fastpath.py
tests/unit/operators/test_group_state_store_dictionary_fastpath.py
tests/unit/operators/test_groupby_comprehensive_unit.py
tests/unit/aggregations/test_bloom_groupby_correctness.py
tests/unit/aggregations/test_bloom_groupby_telemetry.py
tests/unit/aggregations/test_group_key_codec_stress.py
tests/unit/aggregations/test_group_key_codec_rewrite.py
tests/unit/aggregations/test_group_key_codec_extensive.py
tests/integration/test_shuffle_groupby_golden.py
```

And fix the conftest:

```
tests/unit/operators/conftest.py
```
Remove the `import opteryx.operators.shuffle.partitioning` and the
autouse partition-kernel-injection fixture that depends on it (it exists
only to feed the removed shuffle operator). If no other fixture in the
file is used by a surviving test, delete the file outright.

> Note `tests/unit/core/test_expression_draken_eval.py:169` references
> the *string* `"feature_groupby_engine_group_state_store"` as a
> telemetry key in an assertion — that is NOT an import and is fine.
> Leave it.

## Category 2 — non-shuffle tests importing a dead encoding constant

These are **not** shuffle tests. They exercise live behaviour
(projection constant-morsel handling, DATEPART correctness, vector
encoding, table alignment) but import `DRAKEN_ENCODING_*` from the dead
`opteryx.operators.group_state_store`:

```
tests/unit/operators/test_projection_constant_morsel.py   DRAKEN_ENCODING_CONSTANT
tests/unit/functions/test_datepart_correctness.py         DRAKEN_ENCODING_DICTIONARY
tests/draken/morsels/test_align_tables.py                 DRAKEN_ENCODING_DICTIONARY
tests/draken/vectors/test_vector_encoding.py              DRAKEN_ENCODING_{CONSTANT,DENSE,DICTIONARY,RLE}
```

Do NOT blindly delete these — that loses real coverage. Per file:

1. Find whether the encoding shape is exposed by a **live** API. The
   §11 model (Dense / Constant / Dict) is authoritative in
   `draken/core/buffers.h`; check whether draken exposes these as
   importable constants/enums anywhere current. `type_coercion`
   duplicates `DRAKEN_ENCODING_CONSTANT` as a private module-level `DEF`
   — that value is known.
2. If a live source exists → **re-point the import** and confirm the
   test passes.
3. If the constant has no live home **and** the test only used it as an
   incidental tag → define the small constant locally in the test (with
   a comment pointing at `buffers.h`) so the behavioural assertions still
   run.
4. Only if the test's *entire* premise is dead (e.g.
   `test_vector_encoding.py` also reads repo paths like
   `opteryx/draken/vectors/vector.pxd` and
   `third_party/mabel/draken/vectors/vector.pyx` that may no longer
   exist — verify) → delete it and **explicitly flag the lost coverage**
   in the done report.

`test_projection_constant_morsel.py` (in `tests/unit/operators`) and
`test_datepart_correctness.py` (in `tests/unit/functions`) **block
collection of their directories**, so they must be resolved (re-pointed
or removed) for the gate below to pass.

## Out of scope

- Production code (none references these symbols).
- The live aggregation operator `grouped_aggregate_hashed/` and its
  tests (`test_grouped_agg_*`, `test_draken_aggregate_*`,
  `test_ungrouped_agg_*`, `test_agg_avg/min/max/sum/count*`) — leave
  untouched; they are the real coverage and must still pass.
- Phase 9 — the *next* ticket.

## Verification — un-dodgeable gate

Paste each:

1. `git status --short` showing the **deletions** (Category 1) and the
   conftest change. (The prior attempt failed precisely here — no
   deletions appeared.)
2. Clean collection of the previously-broken dirs:
   - `python -m pytest tests/unit/operators tests/unit/functions tests/unit/aggregations --collect-only -q`
     → exits 0, no ImportError.
3. `grep -rn "opteryx.operators.shuffle\|shuffle_node\|opteryx.operators.group_state_store" tests/`
   → returns nothing.
4. The surviving operator suite runs and the COUNT(*) gates pass:
   - `python -m pytest tests/unit/operators/test_agg_count.py tests/unit/operators/test_count_star_filtered_projection.py -q`
     → all pass.
5. Any re-pointed Category 2 test passes (paste); any deleted Category 2
   test has its coverage loss flagged.
6. `make q` 137/137; `make et` 40; `make dt` unaffected.

## Constraints (CLAUDE.md)

- **Test-only.** Touch nothing under `opteryx/`, `draken/`, `rugo/`. If
  you must edit production code, STOP and report — scoping missed a
  production reference.
- **No silent coverage loss** — Category 2 deletions must be flagged
  explicitly.
- **Broken but honest** — the gate is the pasted `git status` deletions +
  `--collect-only` clean result. The prior attempt reported done with
  neither; do not repeat that.
- **Do not commit.**

## Definition of done

- Category 1 files deleted (shown in `git status`); conftest fixed.
- Category 2 files re-pointed (preferred) or deleted-with-flag.
- `tests/unit/operators`, `tests/unit/functions`,
  `tests/unit/aggregations` collect cleanly (pasted).
- No `opteryx.operators.shuffle*` / `shuffle_node` / `group_state_store`
  imports remain in `tests/` (pasted grep).
- `test_agg_count.py` + `test_count_star_filtered_projection.py` pass
  (pasted).
- `make q` 137/137; `make et` 40; `make dt` unaffected.
