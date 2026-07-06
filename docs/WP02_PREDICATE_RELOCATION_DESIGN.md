# WP-02 — Native Predicate Relocation (design)

Relocate the per-row residual filter of a predicate-bearing parquet scan out of
the Python bytecode VM (which runs inside the GIL-acquiring scan trampoline) into
a native downstream `ExprFilter` operator, and admit the scan itself to
`NativeParquetScanSource` (composing with WP-01's string-column admission). Row-
group / bloom / min-max **pruning stays at the scan**; only the per-row residual
relocates.

This document is the design contract. It is deliberately written around a single
column-role model so that all cases fall out of one rule — there is no
"exception" path.

---

## 1. The model: column roles are the design

Every column a scan touches has two independent, plan-time-decidable properties:

* **predicate-input?** — referenced by the pushed `WHERE` predicate.
* **emitted?** — present in the projection the downstream engine consumes
  (`scan.columns`).

Their 2×2 gives four roles. This is the whole design surface; nothing else about
a column matters to the scan/filter shape.

| # | predicate-input? | emitted? | role                       | read? | in filter program? | in output? |
|---|------------------|----------|----------------------------|-------|--------------------|------------|
| 1 | no  | yes | projected passthrough           | yes | no  | yes |
| 2 | yes | yes | projected + predicate           | yes | yes | yes |
| 3 | yes | no  | filter-only                     | yes | yes | no  |
| 4 | no  | no  | untouched                       | no  | no  | no  |

"Filtering" is a **row** operation: the predicate yields one survivor mask per
morsel and that mask is applied to every emitted column — a case-1 column is
masked exactly like a case-2 column. The only column-level distinction is *which
set a column belongs to*, not *how it is filtered*.

From the roles derive two sets, once, at plan time:

* **read-set** = emitted ∪ predicate-inputs  (roles 1, 2, 3)
* **emit-set**  = emitted                       (roles 1, 2)

`read-set ∖ emit-set` is exactly role 3 (filter-only). `predicate-inputs ⊆
read-set` always. Role 4 is, by construction, absent from read-set.

This mirrors what the trampoline scan already computes internally
(`required_names = projected ∪ filter_columns`, emit `output_identity_order =
projected`). WP-02 makes that structure the **explicit native plan** instead of
behaviour buried inside one operator.

---

## 2. The canonical plan

One logical shape, correct for all four roles by construction:

```
Scan(read-set)  →  ExprFilter(predicate)  →  Select(emit-set)
```

* **Scan(read-set)** — `NativeParquetScanSource` decodes and emits the read-set
  columns. It carries role-3 columns *through* because they are the filter's
  input; they cannot be dropped before the filter.
* **ExprFilter(predicate)** — the native c-native span evaluator
  (`_dv_filter_span_with_consts_cxx`, `noexcept nogil`, no PyObject touched),
  the same operator standalone `FilterNode` / `HAVING` / NLJ-residual already
  use via `NativePlan.add_expr_filter`. Computes the survivor mask from role-2/3
  columns and applies it to all read-set columns.
* **Select(emit-set)** — projects the surviving morsel down to the emitted
  identities, dropping role-3 columns.

Walk of the roles through this one plan (the mask is computed and applied by the
`ExprFilter`; the `Select` only chooses which surviving columns to forward):

* Role 1 — read, `ExprFilter` applies the mask, kept by Select.
* Role 2 — read, feeds the predicate program, `ExprFilter` applies the mask,
  kept by Select.
* Role 3 — read, feeds the predicate program, `ExprFilter` applies the mask,
  **dropped** by Select. The drop *is* `read-set ⊋ emit-set`; it is not a
  special node.
* Role 4 — never in read-set.

Physical mask mechanism: the `ExprFilter` applies `cxx_mask_c`
(`cxx_mask_with_consts_c` when the plan carries `IDENTIFIER = LITERAL`
const-replacements) — the *same* compaction the production standalone
`FilterNode` already uses. WP-02 introduces no new mask path. Whether that
compaction is selection-view or materializing is an existing property of
`cxx_mask_c`, unchanged here; avoiding the decode+compaction of doomed **role-1**
rows is precisely what the late-materialization follow-on (§9) addresses.

The predicate is applied **exactly once**, downstream, natively. The native scan
performs *no* per-row residual (it has no `_compiled_predicate`); it only prunes
whole row groups. Surviving row groups still contain non-matching rows, which the
`ExprFilter` removes — no double filter, no dropped match.

---

## 3. Degeneracies are algebra, not branches

What is *written* is `Scan → ExprFilter → Select`. What *runs* is whatever
survives two semantics-preserving simplifications — the same "remove an
identity / empty node" any operator earns:

* **predicate-inputs = ∅** (no pushed `WHERE`) → the `ExprFilter` is empty →
  removed. A no-predicate scan is a bare `NativeParquetScanSource` with zero
  filter machinery. **This is the free case.**
* **read-set = emit-set** (no role-3 columns) → the `Select` is the identity
  permutation → removed. Collapses to `Scan → ExprFilter`, layout unchanged —
  the cheap, common form.
* Both hold → plain native scan.

Role 3 therefore costs exactly one `Select` node and is otherwise
indistinguishable from role 2. We explicitly **reject** a "fail-closed when a
filter-only column exists" rule: that would treat the un-collapsed general form
as an exception when it is just `read-set ⊋ emit-set`.

---

## 4. Fail-closed (the only real gate)

The one thing that decides native-vs-trampoline is **lowerability of the
predicate to a c-native span**, and it is a clean boolean, not exception flow:

1. AND-compose the pushed predicate nodes (same compose the trampoline scan uses
   for `_compiled_predicate`).
2. Apply the same plan-time rewrites the filter path already applies
   (`CASE → IF_THEN_ELSE`, `BETWEEN → compares`, decimal-literal rescale) and
   `build_bytecode(lower(...))`.
3. Gate on `bytecode_is_all_c_native(bc)`.
   * **True** → relocate: stash `bc` keyed by `scan.identity`, build the native
     scan with pruning predicates, wire the downstream `ExprFilter` (+ `Select`
     if role-3 columns exist).
   * **False** → return `None`. The scan stays on `StreamingScanSource` with the
     predicate on the existing path (including its bytecode-VM residual). The
     result is correct; only the optimization is declined.

**Why no `try/except` is needed** (§9-compliant). The gate builds the bytecode
from the *exact same AST* the trampoline scan already lowers into
`_compiled_predicate` (`build_bytecode(lower(rewrite(compose(scan.predicates))))`),
and the trampoline does so **unconditionally at execute time with no guard**. A
`build_bytecode`/`lower` raise here would therefore already be a live crash on
the trampoline path for the identical predicate — it cannot be a
merely-latent-in-native case. The compiler runs strictly after the binder has
accepted the predicate, so the AST is well-formed by the time `_compile_scan`
sees it. The only "not lowerable to a native span" signal is the
`bytecode_is_all_c_native` boolean; that is the gate, and it is total. (If a
future predicate shape could genuinely raise in `build_bytecode`, the correct
fix is to make `build_bytecode` classify it as non-c-native, not to wrap the gate
in `try/except`.)

Fail-closed cases: unsupported function in the predicate, or any expression
outside the c-native kernel set. These route to `StreamingScanSource`, unchanged.

---

## 5. Pruning is preserved, unchanged

Pruning already lives in the native planner. `open_native_scan_plan`
(`pool_reader.pyx`) already accepts `predicates` and already runs
`_rg_passes_predicates_native` — row-group min/max **and** bloom membership
pruning — at plan time; it is merely called with `predicates=None` today.

WP-02 passes the **same pruning triples the trampoline path uses**:
`extract_predicate_stats(scan.predicates)` (the `(col, op, value)` form
`_rg_passes_predicates_native` consumes). Because both paths feed identical
triples to identical pruning code, pruning behaviour is identical by
construction: **when a predicate yields prunable triples, both paths prune the
same row groups; when it does not** (e.g. `col1 + col2 > 5`, which
`extract_predicate_stats` cannot reduce to a triple), **both paths prune
nothing.** Either way **bytes-read and row-groups-scanned are identical**
before/after for the same predicate. This is asserted, not assumed (see §8).

Not in scope for the native path this increment: the Phase-2 dictionary
decode-skip (`_flatten_dict_skip_predicates` → `add_int_needles` /
`add_str_pred`) that `open_ipc_source` wires. That is a *decode*-skip (pages are
still fetched; disjoint-dictionary data pages are not decoded), not a *read*-skip
— it does not change bytes-read, and the downstream `ExprFilter` removes those
rows correctly regardless. Wiring it into the native source is a separate,
optional optimization, not a correctness item.

---

## 6. Native wiring (where the code lands)

Scope is unchanged from WP-01 plus the predicate relocation. In-scope files:

* **`opteryx/managers/execution/compiler.py`**
  * `_native_scan_plan(scan)`: remove the blanket `if scan.predicates: return
    None`. Instead classify roles, compute read-set / emit-set, attempt the
    lower-and-gate of §4. On success, build the native scan with
    `predicates=extract_predicate_stats(scan.predicates)` (pruning) and record
    the relocated `bc` + emit-set for the scan identity. On gate failure, return
    `None` (fail-closed). The native scan is planned over **read-set** columns
    (so role-3 columns are decoded and available to the filter).
  * **AND-composition** = the *verbatim* `_compose_predicates` the trampoline
    scan already uses to build `_compiled_predicate`: the planner pushes `WHERE`
    conjuncts as a *list* of nodes (each conjunct may itself contain `OR`), and
    both paths fold that list into one right-leaning `AND` tree before lowering.
    WP-02 lowers the *same composed tree* the trampoline lowers — not a
    re-derivation — so there is no way for the composition to diverge in meaning.
  * `_compile_scan(...)` native branch: after `set_native_scan_source`, if a
    relocated filter exists for this scan:
    1. `add_expr_filter(p, bc, read_set_layout)`.
    2. if `read-set ≠ emit-set`, `add_select(p, indices, emit_ids)` to project
       back to the emitted identities **in `scan.columns` order**.
    The returned `(pipeline, layout)` carries the **emit-set** layout, so every
    downstream consumer sees exactly what the trampoline scan would have emitted.
    `add_select` is an **existing** native operator (already used to compile
    `ProjectionNode`); it takes `(indices, identities)` and both **subsets and
    reorders** — `indices` is the read-set→emit-set positional map (a permutation
    when the predicate columns sit between projected columns), so no new node is
    introduced.
  * The **degeneracy collapses of §3 are performed explicitly by this builder**,
    not by a generic identity-elimination pass: it emits the `ExprFilter` only
    when a relocated predicate exists, and the `Select` only when
    `read-set ≠ emit-set`. A no-predicate scan and a no-role-3 scan simply never
    construct those nodes.
  * The native gate (`native_scan_supported`) is evaluated over the **read-set**
    column list (not the original projection) — role-3 columns must also be
    admissible native kinds. This is a deliberate strict check: if a role-3
    column is a not-yet-admissible kind, the gate returns False and the scan
    fails closed to `StreamingScanSource`.
  * **Lifetime / concurrency of the stashed `bc`:** the `NativePlan` (and the
    relocated bytecode) is built per `execute()` and discarded with it — it is
    not cached across queries. The stash is keyed by `scan.identity` only to
    carry the lowered predicate from `_native_scan_plan` to `_compile_scan`
    within one compile; there is no cross-query shared mutable state.

* **`opteryx/connectors/parquet_io/pool_reader.pyx`**
  * No structural change required — `open_native_scan_plan` already prunes on
    `predicates`. WP-02 simply stops passing `None`. (`native_scan_supported`
    and `open_native_scan_plan` are already called over a caller-supplied column
    list; that list becomes read-set.)

* **`opteryx/operators/parquet_read/parquet_read.pyx`**
  * The bytecode-VM fallback in `_cxx_apply_predicate` is **untouched** — it is
    only reachable from `StreamingScanSource`, i.e. the fail-closed cases. WP-02
    makes it unreachable from *relocated* (native) scans, which is asserted via
    the WP-INSTR worker-purity guard, not by deleting the fallback.

The relocated `ExprFilter` reuses the existing native operator end to end
(`add_expr_filter` → `_expr_filter_tramp` → `_dv_filter_span_with_consts_cxx`);
WP-02 introduces **no new native filter code** and **no new Python** on the
execution path.

**Decoded string type / vector shape (composing with WP-01).** The native scan
emits string columns as `VARCHAR` / `NVARCHAR` / `VARBINARY` `DrakenVector`s in
one of the three unified shapes (dense, constant, or **dict** — parquet
dictionary-encoded columns arrive dict-shaped, they are *not* handed to the
filter as raw dictionary integers). The predicate program reads every column via
the uniform `data[selection[i]]` access mandated by the vector model
(CLAUDE.md §11), so `c_execute_dv_inner` / `cxx_mask_c` consume any of the three
shapes correctly by contract — the relocated filter needs no shape-specialization
and no separate dictionary-aware string path. The parity harness exercises a
dict-encoded string column explicitly to confirm this end to end.

---

## 7. Semantics parity (mandatory)

### Parity is near-by-construction — what actually changes

A sharpening that de-risks this whole section: WP-02 does **not** swap one
predicate evaluator for a different one. Both the trampoline and the relocated
`ExprFilter` run the **same `CompiledBytecode`** through the **same c-native
kernel**:

* Trampoline all-c-native fast path: `filter_morsel_c_native` →
  `_dv_filter_span_cxx` → `c_execute_dv_inner` + `cxx_mask_c`.
* Relocated `ExprFilter`: `_dv_filter_span_with_consts_cxx` →
  `c_execute_dv_inner` + `cxx_mask_c` / `cxx_mask_with_consts_c`.

For exactly the predicates WP-02 relocates (`bytecode_is_all_c_native == True`),
the trampoline was **already** evaluating them with this native kernel — the
Python bytecode VM (`execute_bytecode`) is only the trampoline's fallback for
**non**-c-native predicates, which WP-02 fails closed and leaves on the
trampoline. So the relocation changes *where* the predicate runs (a downstream
native operator vs in-scan under the held GIL), not *how* it is evaluated. The
dominant GIL cost WP-02 removes is therefore the per-morsel **trampoline drive
loop** (`_scan_pull_run` + `cxx_to_morsel`/`morsel_to_cxx` shims), not the VM.

This also means the relocated `ExprFilter` is the *same operator already in
production* for non-pushed `WHERE` (a standalone `FilterNode` compiles to the
identical `add_expr_filter` path). Parity therefore reduces to confirming the
relocation wiring, not validating a new evaluator — but the harness below still
checks it end to end.

### Required parity shapes

Parity (byte-identical survivor sets vs. the trampoline path) is required across:

* numeric comparisons and string comparisons,
* `IN` / `NOT IN`,
* `LIKE`,
* `IS NULL` / `IS NOT NULL`,
* nested `AND` / `OR`,
* cross-type comparisons (operand coercion),
* all-null and all-constant inputs,
* a predicate that prunes all rows,
* a predicate that keeps all rows.

Three-valued logic must match exactly: a filter keeps a row iff the predicate is
`TRUE` (not `FALSE`, not `NULL`). Sequential `ExprFilter`s and a single
AND-composed `ExprFilter` are equivalent under KEEP semantics (`A AND B` is
`TRUE` iff both are `TRUE`); WP-02 AND-composes to mirror the trampoline's single
`_compiled_predicate` exactly.

Parity is the primary deliverable's gate (harness in §8).

---

## 8. Success criteria → checks

* **Correctness** — the parity harness compares relocated-`ExprFilter` results
  against current bytecode-VM results across every shape in §7; must match
  exactly, including all four column roles (esp. a role-3 filter-only column and
  a multi-column predicate).
* **Instrumentation** — for a string-column + predicate `SELECT`:
  `scan_sources[scan] == "NativeParquetScanSource"`, `trampoline_calls ~0`,
  scan-stage `gil_held_ns ~0`; `execute_bytecode` unreachable from the scan path
  for relocated predicates (WP-INSTR worker-purity guard, `whitelist=()`). "~0"
  is the **per-morsel execution loop** target: plan-time predicate lowering and
  scan-plan construction hold the GIL and are expected to — they are one-shot
  setup outside the drive loop and are not counted by the per-morsel
  `trampoline_calls` / execution-time `gil_held_ns` instruments.
* **Pruning** — assert bytes-read and row-groups-scanned are identical to the
  pre-change (trampoline) path for the same predicate.
* **No double / dropped filter** — result set identical to the trampoline path;
  predicate applied exactly once.
* **Regression** — `make q` green. Fail-closed predicates still route to
  `StreamingScanSource` and are correct.
* **Observable** — predicate-bearing scans no longer flatten throughput past
  dop=1 (≈0 scan-stage GIL time on a GIL build).

### Required tests
Parity harness (§7 shapes); regression suite; edge cases — predicate pruning all
rows, keeping all rows, predicate on a string column (composing with WP-01),
predicate referencing multiple columns, a **filter-only (role-3)** predicate
column exercising the `Select`-back path, and an unsupported-function predicate
proving fail-closed to `StreamingScanSource`.

### Required benchmark
Selective and non-selective predicate over a string+numeric parquet table
(ClickBench-style, ≥10M rows). Metric: end-to-end latency + scan-stage
`gil_held_ns` + `trampoline_calls` at dop ∈ {1,4,8}. Success = `gil_held_ns ~0`,
`trampoline_calls ~0`, latency ≤ current, pruning unchanged. Run page-cache-warm
/ in-memory (and throttled-IO if available) as well as warm NVMe — warm NVMe
hides GIL serialization behind fast IO.

---

## 9. Sequenced follow-on: late materialization (NOT this increment)

WP-02 lands the **single-pass** instantiation of the canonical plan: the scan
reads the full read-set, the `ExprFilter` masks, the `Select` narrows. For a
selective predicate over a wide table this decodes role-1 payload rows that do
not survive — the cost two-pass late-materialization exists to avoid.

Late-mat is the **same design, one decode strategy on the same role sets**, not a
new plan:

* pass-1 decode = **predicate-inputs** (roles 2+3) → evaluate predicate →
  survivor mask,
* pass-2 decode = **emit-set − predicate-inputs** (role 1) → decode survivors
  only (`submit_work_native_masked`).

It requires the native source to grow masked pass-2 decode (which `NativeScanPlan`
explicitly does not do yet) and a native combine. Because it reuses the identical
role classification computed in WP-02, it slots on with **zero plan rework** —
that is what makes WP-02 a conscious design rather than a patch.

One re-wiring note for the follow-on (out of scope here, recorded so it is not a
surprise): under late-mat the survivor mask is produced from the pass-1
(predicate-input) columns and consumed by the scan to gate pass-2 decode, so the
`ExprFilter` no longer applies a mask to role-1 columns — it becomes the
mask-*producer* feeding the source, with role-1 columns arriving already
compacted. That is a transformation of the *same* `Scan → ExprFilter → Select`
plan (the node stays, its role narrows), not a different plan.

Optional, cheap diagnostic to land with WP-02 so the single-pass penalty is
*measured* rather than assumed: a telemetry counter for role-3 (filter-only)
decoded bytes — the bytes WP-02 decodes that late-mat would later skip. It makes
the follow-on's payoff visible and turns any regression into a number, not a
hunch. (Diagnostic only; no behaviour.)

Accepted, named consequence of the WP-02 single-pass landing: a query that is
two-pass-eligible on the trampoline today, sent native now, trades late-mat's
decode-skip for zero-Python. Row-group / bloom pruning still fires (bytes-read
unchanged); doomed surviving-row-group payload is decoded until the follow-on
lands. This is surfaced deliberately, not regressed silently, and is the argument
for sequencing late-mat next.

A further, later optimization on the *same* plan: fuse a cheap all-c-native
`ExprFilter` back into the scan loop (operator fusion), recovering the in-scan
nogil-span fast path as a physical transform of `Scan → ExprFilter`, not a
separate code path. The fused predicate **replaces** the `ExprFilter` node (the
node is removed when its evaluation moves into the scan) — there is exactly one
filter point at all times, never a fused filter *and* a surviving `ExprFilter`.

---

## 10. Review checklist

- [ ] parity harness green across all §7 predicate shapes (all four column roles)
- [ ] pruning unaffected — bytes-read / row-groups-scanned assertion
- [ ] role-3 (filter-only) predicate goes native via `Select`-back, correct result
- [ ] degeneracies collapse (no `ExprFilter` when no predicate; no `Select` when
      read-set = emit-set)
- [ ] unsupported predicate fails closed to `StreamingScanSource` with correct results
- [ ] `execute_bytecode` unreachable from scan for relocated predicates (guard)
- [ ] no `with gil` / PyObject in the relocated path
- [ ] no `try/except` control flow in the gate
- [ ] `make q` green
