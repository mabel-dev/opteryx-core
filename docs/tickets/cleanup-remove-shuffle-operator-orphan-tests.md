# Ticket: Remove the unplanned shuffle operator — delete orphaned tests, unbreak `tests/unit/operators`

> Architect decision (2026-05-28): "remove the shuffle operator — it's
> not planned so is noise. then resume phase 9." The shuffle *operator*
> source no longer exists in the tree (it was superseded by
> `opteryx/operators/grouped_aggregate_hashed/`). What remains is a set
> of **orphaned tests** importing dead modules
> (`opteryx.operators.shuffle`, `opteryx.operators.shuffle_node`,
> `opteryx.operators.group_state_store`). One of them is an **autouse
> conftest fixture** that breaks collection of the entire
> `tests/unit/operators/` directory. This ticket removes the noise.

## Status: OPEN

## Why this matters (it's not just tidiness)

`tests/unit/operators/conftest.py` does, at module top:

```python
import opteryx.operators.shuffle.partitioning as partitioning   # module does not exist
```

inside an `autouse=True` fixture file. So **every** test in
`tests/unit/operators/` fails to collect with
`ModuleNotFoundError: No module named 'opteryx.operators.shuffle'` —
including legitimate, wanted tests:

- `test_agg_count.py` (the COUNT(*) value tests — could not serve as a
  gate for `bug-count-star-where-zero-col-select.md` because of this)
- `test_count_star_filtered_projection.py`
- `test_agg_avg/min/max/sum.py`, `test_array_agg.py`,
  `test_distinct_parvi_promotion.py`, `test_join_flaw.py`, etc.

So the breakage actively hid a real bug's natural gate. Removing the
shuffle noise restores a whole directory of value-checked operator tests.

## Verified scope (checked against current tree — do not re-derive)

**Production source: zero shuffle references.** `grep -ri shuffle
opteryx/` (excluding `third_party/`, build artifacts) returns nothing.
The names the orphan tests import moved to the live operator:
`AggregationSpec` is now defined in
`opteryx/operators/grouped_aggregate_hashed/_node.pxi:22`. **This is a
test-only removal — no production code changes.**

### A. Delete — orphaned test files (import dead shuffle/group_state modules)

`tests/unit/operators/`:
- `test_shuffle_bin_store.py`        — `from opteryx.operators.shuffle import BinStore`
- `test_shuffle_group_by.py`         — `from opteryx.operators.shuffle import …`
- `test_shuffle_group_by_phase1.py`  — `from opteryx.operators.shuffle import AggregationSpec, ShuffleGroupByOperation`
- `test_shuffle_merge.py`            — `from opteryx.operators.shuffle import ShuffleMergeOperation, …`
- `test_shuffle_node.py`             — `from opteryx.operators.shuffle_node import ShuffleNode`
- `test_shuffle_partitioning.py`     — `import opteryx.operators.shuffle.partitioning`
- `test_group_state_store_constant_fastpath.py`   — `from opteryx.operators.group_state_store import ShuffleGroupByOperationV2`
- `test_group_state_store_dictionary_fastpath.py` — same
- `test_groupby_comprehensive_unit.py`            — `from opteryx.operators.shuffle_node import ShuffleNode`

`tests/unit/aggregations/`:
- `test_bloom_groupby_correctness.py` — `from opteryx.operators.shuffle import AggregationSpec, ShuffleGroupByOperation`
- `test_bloom_groupby_telemetry.py`   — (verify; references shuffle groupby)
- `test_group_key_codec_stress.py`    — `from opteryx.operators.shuffle import …`
- `test_group_key_codec_rewrite.py`   — same
- `test_group_key_codec_extensive.py` — (verify import before deleting)

**Per-file rule:** confirm each file's *only* opteryx imports are the
dead shuffle/`shuffle_node`/`group_state_store` paths. If a file is
purely dead → delete it. If a file *also* exercises a behaviour that
still exists under a new path (e.g. a group-key codec that moved into
`grouped_aggregate_hashed`), do **not** silently delete that coverage —
note it in the "coverage moved" section of the done report so we can
decide whether to re-point it later. Default action is delete (architect
called shuffle noise), but flag any genuine coverage loss.

### B. Fix — `tests/unit/operators/conftest.py`

Remove the `import opteryx.operators.shuffle.partitioning` and the
autouse partition-kernel-injection fixture that depends on it (it exists
solely to feed the removed shuffle operator). If the conftest has *no
other* fixtures used by surviving tests, delete the file; otherwise keep
only the still-needed fixtures. After this, the directory must collect.

## Out of scope

- Any production code (none references shuffle).
- The live aggregation operator `grouped_aggregate_hashed/` and its
  existing tests (`test_grouped_agg_*`, `test_draken_aggregate_*`,
  `test_ungrouped_agg_*`) — leave untouched; they are the real coverage.
- Phase 9 — resumes in the *next* ticket per architect ("then resume
  phase 9").

## Verification — gate

- `tests/unit/operators/` and `tests/unit/aggregations/` **collect with
  no ImportError** after removal:
  - `python -m pytest tests/unit/operators tests/unit/aggregations --collect-only -q` exits clean.
  - Paste the collection summary (no errors).
- The surviving operator tests **run** (and `test_agg_count.py` +
  `test_count_star_filtered_projection.py` pass — they assert
  COUNT(*)-WHERE correctness, now fixed):
  - `python -m pytest tests/unit/operators -q` — paste pass/fail counts.
  - Pre-existing unrelated failures in surviving files (if any) are NOT
    introduced by this ticket; call them out, do not "fix" them here
    (STOP/report per §8).
- `grep -rn -i "operators.shuffle\|shuffle_node\|group_state_store" tests/`
  returns nothing.
- `make q` 137/137 (unaffected — test-only change).
- `make et` 40 and `make dt` morsel suite unaffected.

## Constraints (CLAUDE.md)

- **Test-only change** — touch nothing under `opteryx/`, `draken/`,
  `rugo/`. If you find yourself editing production code, STOP and report:
  it means a production reference exists that this ticket's scoping
  missed.
- **No silent coverage loss** — flag any deleted test that covered live
  behaviour (§ "coverage moved").
- **Fail fast / honest** — the gate is the pasted `--collect-only` clean
  result plus the operator-suite run. A "done" report without them is
  rejected.
- **Do not commit.**

## Definition of done

- All files in section A deleted; conftest in section B fixed/removed.
- `tests/unit/operators` + `tests/unit/aggregations` collect cleanly
  (pasted); `test_agg_count.py` and `test_count_star_filtered_projection.py`
  pass (pasted).
- No remaining `opteryx.operators.shuffle*` / `shuffle_node` /
  `group_state_store` references in `tests/` (pasted grep).
- `make q` 137/137; `make et`/`make dt` unaffected.
- Any genuine coverage that lived only in deleted files is listed for a
  follow-up decision.
