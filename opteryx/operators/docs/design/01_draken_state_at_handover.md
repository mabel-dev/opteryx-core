# 01 — State of the Draken rebuild at handover

> Status: handover record. Written by the draken-rebuild PM at the point where
> the operator rewrite (`00_operators_and_parallelism.md`) becomes the next
> initiative. Purpose: give the operator-rewrite PM a single document to read
> cold, without needing to reconstruct two weeks of session transcripts.
>
> If this document and the draken design docs disagree, the **draken design
> docs are right** and this document is stale — fix the link, not the truth.

---

## 1. Where the boundary sits

The draken rebuild stops at: **draken is a working columnar vector library
with a C++-first core, a stable nanobind Python surface, a Cython shim layer
(`Vector` / `BoolVector` / `Morsel` cdef classes) for typed cimport from
operator code, and a published ABI/bridge contract.**

The operator rewrite starts at: **everything in `opteryx/operators/` and
`opteryx/expression/evaluator/`, plus the scheduler / pipeline-parallelism
work scoped in `00_operators_and_parallelism.md`.**

`rugo/` and `carchar/` are out of scope for both. Their integration with
draken's morsel/vector ABI is complete and not part of the operator rewrite.

## 2. What is done (and where to read about it)

### 2.1 The design corpus

The draken rebuild's design lives at `draken/docs/design/`:

| Doc | Subject | Status |
|---|---|---|
| `00_data_model.md` | DrakenVector ABI, encoding shapes, selection vectors | Frozen |
| `01_ownership.md` | Memory ownership, RAII, who frees what | Frozen |
| `02_dispatch_and_ops.md` | OpsTable dispatch by DrakenType | Frozen |
| `03_binding.md` | nanobind module shape, Vector/Morsel handles | Frozen |
| `04_testing.md` | Per-type native tests, hypothesis, no parity oracle | Frozen |
| `05_statistics.md` | Per-vector stats surface | Frozen |
| `06_value_encoding.md` | Per-type value layout | Frozen, except where superseded — see §3 |
| `07_consumer_contract.md` | What consumers may assume about a Vector | Frozen |
| `08_implementation_plan.md` | Original phasing | Frozen — historical |
| `09_delivery.md` | Operational delivery plan, milestones A–E | The operational source of truth for status |
| `E0_consumer_rewrite_scoping.md` | Consumer rewrite scoping + cost data | Frozen |
| `E20_evaluator_survey.md` | Evaluator structure, compile blockers, sequencing | Frozen |
| `E23_evaluator_compile_redo.md` | Cleanup ticket for `_impl.so` compile | Done (folded into E.24) |
| `E24_cython_vector_shim.md` | Cython shim for `Vector`/`BoolVector`/`Morsel` | Done — see §4 |

**Read order for a new PM:** `09_delivery.md` → `00_data_model.md` →
`07_consumer_contract.md` → `E20_evaluator_survey.md` → `E24_cython_vector_shim.md`.
The rest is reference-as-needed.

### 2.2 Decisions captured in memory files (architect calls)

A handful of decisions were taken mid-flight and are documented in user-memory
rather than the design corpus. They are binding on the operator rewrite:

- **`draken-rebuild-delivery-plan`** — read the design docs, do not relitigate.
- **`feedback-no-false-green-clean-break`** — no shim/bridging; broken-until-rewritten
  is acceptable. Never fake green.
- **`draken-german-string-format`** — long-form slot is
  `[u32 len][u32 prefix][u32 hash32][u32 offset]`, equality is HASH-ONLY
  (length+prefix+hash, no byte verify). Supersedes doc `06`'s old format.
- **`draken-float-nan-semantics`** — NaN=NaN canonicalised and sorts highest;
  -0.0=0.0 canonicalised. PostgreSQL/DuckDB convention.
- **`draken-string-type-family`** — `DRAKEN_VARCHAR` (default, ASCII, cheap),
  `DRAKEN_NVARCHAR` (opt-in UTF-8), `DRAKEN_VARBINARY` (opaque bytes). Unicode
  is opt-in. Fixed-width variants reserved but not built.
- **`draken-boost-math`** — `boost::math` vendored under `third_party/boost_math/`.
  **Do not use `boost::math::round` — it is half-away-from-zero.** Use the 2^52
  trick for half-to-even.
- **`draken-consumer-edge-pattern`** — Python edge lives in nanobind C++; `.pyx`
  is typed-only (zero `object` params/returns). The two-layer `.pyx` cdef-kernel
  + nanobind glue pattern from E.1 was **dropped** for consumer rewrites — the
  pattern is now pure nanobind C++. Operator rewrite follows the same rule.
- **`feedback-hash-no-parity`** — hash values are disposable. No cross-version
  parity required vs draken_old.

These are not optional. Violating them is what cost the draken rebuild its
worst ticket failures (see §7 landmines).

## 3. The contract you inherit

### 3.1 The ABI

`DrakenVector` — 40-byte struct in `draken/core/buffers.h`, ABI-frozen and
guarded by `draken/core/_abi_guard` at build time. Per CLAUDE.md §11:

```c
typedef struct {
    void*             data;
    const uint32_t*   selection;   // NEVER NULL
    uint32_t          data_length; // unique values in data
    uint32_t          length;      // logical row count
    uint8_t*          validity;    // 1-bit-per-logical-row null mask; NULL = all valid
    DrakenType        type;
} DrakenVector;
```

Uniform access: `data[selection[i]]` for `i in [0, length)`. Three encoding
shapes (Dense / Constant / Dict) differ only in *which* buffer `selection`
points at. Shape-based dispatch is permitted only as a targeted optimisation,
with architect agreement. The default operator MUST be uniform-path-correct.

**This is the only data contract between operators.** No PyArrow, no numpy, no
Python sequences on the hot path.

### 3.2 The nanobind surface (`draken_native.so`)

Exposes `Vector`, `BoolVector` (alias of `Vector` today), and `Morsel` as
nanobind classes. C′ consumer extensions in `opteryx/compiled/nanobind/`
return these as their result type.

### 3.3 The bridge (`draken/core/draken_bridge.h`)

C-callable surface for getting at the underlying `DrakenVector*` from a Python
handle, and for wrapping a freshly-built vector back into a Python handle:

- `draken_vector_unwrap(PyObject*) -> const DrakenVector*` — type-checked,
  fail-fast on wrong type. **Use this**, not `<object>`-typed cimports.
- `draken_vector_own(...)` / `draken_vector_own_raw(...)` /
  `draken_vector_own_string(...)` — wrap a freshly-built vector. The
  `_string` variant takes a `DrakenType` parameter so consumers can produce
  any of VARCHAR / NVARCHAR / VARBINARY.

### 3.4 The Cython shim (E.24)

`draken/vectors/vector.pxd`, `draken/vectors/bool_vector.pxd`,
`draken/morsels/morsel.pxd` declare `cdef class` types that wrap the nanobind
handle plus its unwrapped `DrakenVector*`. The `.so` produced by the shim
shadows the old `.py` re-exports.

**Why this matters for operators:** any `.pyx` that needs to `cimport` a
`Vector`/`BoolVector`/`Morsel` and call `cdef` methods on it can now do so
without the `KeyError: '__pyx_vtable__'` import failure that blocked the
evaluator compile pre-E.24.

**The wrap cost:** when a nanobind handle crosses into a cimport-using
consumer, it gets wrapped once (`cdef Vector v = Vector(nb_vector)`). This
is per-morsel, not per-row. The operator rewrite should be aware of this and
not re-wrap inside loops.

The **strategic direction** (Option B in the architect's "A then B" call) is
that operator-side callers move to `draken_vector_unwrap` directly and the
shim eventually goes away. That migration is part of the operator rewrite —
not a draken-side prerequisite.

### 3.5 The ops layer

`draken/ops/*.h` — typed C++ kernels dispatched by `DrakenType` via OpsTable.
This is where math, comparison, hash, JSON, regex, etc. live. Operators
should NOT add `.pyx` loops to fill perceived op gaps — surface the gap as
a draken-side ticket. (See §7 landmine #4.)

## 4. State of the rebuild at handover

Per `09_delivery.md` plus the post-rebuild deltas:

### 4.1 Done

- Milestone A (scaffolding).
- Type matrix: int64, string (VARCHAR), bool, int8/16/32 + cross-width,
  float32/64, temporals (date32, time, timestamp + logical-type descriptor),
  decimal pt1, null, fp16, interval, array.
- ABI guard at build time; freezing `DrakenVector` to 40 bytes.
- Bridge surface (`draken_vector_unwrap`, `_own`, `_own_raw`, `_own_string`).
- Consumer C′ extensions in `opteryx/compiled/nanobind/`: ~24 modules
  covering bitwise, math, codec, bool_ops, accessors, array_reduce, casts,
  hash_codec, json, misc, selection_concat, special, split_native,
  string_misc{1,2,3}, string_search, temporal_arith, temporal_convert, etc.
- Build isolation (`make draken` / `DRAKEN_BUILD=1`) — draken side builds
  cleanly regardless of opteryx-side Cython breakage (E.22).
- Cython shim layer for `Vector`/`BoolVector`/`Morsel` (E.24).
- Native draken test suite (`make dt`): 2792+ tests, all green.

### 4.2 Not done (and explicitly NOT operator-rewrite prerequisites)

- **UTF-8 cluster** (5 files: `vector_initcap`, `vector_lowercase`,
  `vector_uppercase`, `vector_reverse`, `vector_string_slice`) — pending an
  architect decision on the case-folding library (boost::locale vs ICU vs
  bespoke).
- **Regex cluster** (4 files: `vector_like`, `vector_rlike`,
  `vector_regex_replace`, `anyop_like`) — needs a re2 integration design.
- **Heavy specials:** `vector_match_against`, `vector_dfa_extract`.
- **`rugo/` tree** (3 files) — minor cleanup.
- **Decimal phase 2**, deeper temporal arithmetic, NVARCHAR ops once Unicode
  library is chosen.

These are draken-team work, not operator-team work. They are listed here so
the operator-rewrite PM knows they exist and does NOT try to absorb them.
When the operator rewrite calls into one of these and finds a gap, the
correct response is to **surface the gap, not paper over it with a `.pyx`
loop** (CLAUDE.md §11 + the `draken-consumer-edge-pattern` memory).

### 4.3 In an intermediate state

- **`make q`** — was 0/133 pre-E.24; post-E.24 the baseline is captured in
  the handover record (see §6). Failures are primarily operator-rewrite
  gaps, not draken-side issues. The operator rewrite is what closes them.
- **`make clickbench`** — Milestone E re-green is the destination; no
  pre-handover baseline exists because the engine has not been able to run
  end-to-end since the rebuild started. The clickbench baseline is the
  operator rewrite's responsibility (its Phase 3 gate per the parallelism
  doc, §9).
- **Operator-side `.pyx` files** — the 21 files in `opteryx/operators/` and
  the residual `vector_ops/*.pyx` files imported by the evaluator have NOT
  been C′-ported. Some `.pyx` still use the old Cython-cimport style against
  the draken shim. The shim makes this work today; the operator rewrite
  moves them to `draken_vector_unwrap` natively over time.

## 5. The §12 design decisions (operators-and-parallelism)

`00_operators_and_parallelism.md` §12 lists six unresolved design forks that
must close before Phase 1 operator work begins. They are listed here as a
reminder; they belong to the operator-rewrite PM (or the architect, via the
operator-rewrite PM) to close, not to the draken-rebuild PM.

1. §3.1 — `_push_impl` purity contract (plain cdef / `nogil` signature /
   `with nogil:` block).
2. §4 — LIMIT counter strategy and non-determinism.
3. §5.4 — thread pool: per-session vs per-query.
4. §5.4 — termination primitive: `threading.Event` / C atomic / `volatile`.
5. §6 — coalesce-after-filter strategy: never / threshold / selection vector.
6. §8 — multiplicity declaration: catalog vs operator-`__init__`.

Per the parallelism doc's own Risk #7 and #8, decisions 1 and 5 must close
before Phase 1 operator audits begin — they change what the audit asks of
each operator.

## 6. The `make q` baseline

The post-E.24 `make q` state is captured in a sibling document
(`02_make_q_baseline_at_handover.md`) produced when E.24 lands. It enumerates:

- Pass count.
- Per-failure category: evaluator-residue / operator-rewrite-gap /
  unexpected.
- The (small, expected) tail of failures that are draken-side bugs to fix
  before final handover, vs the (larger, expected) bulk of failures that the
  operator rewrite is going to close incrementally.

The operator-rewrite PM should treat that document as the starting line.

## 7. Landmines — what cost time during the draken rebuild

Captured so they don't bite the next PM.

1. **`object` in `.pyx` violates CLAUDE.md §3.** This was the E.1 termination.
   Every consumer-rewrite ticket must explicitly forbid `object` parameters /
   returns. `<object>` casts at a `def` function's RETURN to box final results
   are allowed (the §02 exception); nowhere else. See the
   `draken-consumer-edge-pattern` memory for the worked example.

2. **The cythonize-batch cascade.** Until E.22 (`make draken` / `DRAKEN_BUILD=1`),
   a single Cython error in any `.pyx` aborted the whole `cythonize()` batch and
   wiped `draken_native.so`. This recurred across four phases (3, 18, 20, 20a)
   before being structurally solved. For operator work, this is no longer an
   issue **so long as draken-side rebuilds use `make draken`**. Full `make c`
   rebuilds still depend on all `.pyx` compiling.

3. **Phase-20a-style drift.** When a ticket gets too big (Phase 20a touched
   57 files), agents pick stubs over migration and acceptance criteria slide.
   Operator-rewrite tickets should have explicit STOP conditions (file count,
   scope creep) — the E.24 ticket is a worked example.

4. **Filling op gaps with `.pyx` loops instead of draken kernels.** Math,
   comparison, hash, string-search, etc. belong in `draken/ops/*.h`,
   dispatched by `DrakenType`. The operator rewrite is downstream of that
   layer; if a needed op is missing, the response is to scope a draken-side
   ticket, not to inline a Python/Cython loop.

5. **Mis-specified tickets.** Three tickets during the draken rebuild had
   spec bugs (allowed `object`, demanded hash parity, missed the
   `__pyx_vtable__` issue). Each was caught and corrected, no false-green
   damage, but the cost was wall-clock. Lesson: when scoping an operator
   ticket, check it against the relevant memory files and the relevant
   sections of `00_operators_and_parallelism.md` and the CLAUDE.md rules
   *explicitly*, not by recollection.

6. **The Cython↔nanobind seam.** Every surprise in the last week of the
   draken rebuild came from this seam (vtable, cdef linkage, batch failure,
   isinstance against shims). The seam is now stable for the cases the
   evaluator hits, but it has not been exercised by every operator yet.
   Expect at least one or two more discoveries during Phase 0 audit.

## 8. Suggested starting point for the operator rewrite

The operators-and-parallelism doc's Phase 0 ("Audit & instrument, no
behaviour change") is the right entry point. It is also the cheapest way to
convert the doc from "spec" to "scoped work" — the audit output is what
sizes Phase 1.

Concretely: pick one operator (Filter is the natural first target — small,
stateless, in the shared-stateless pattern, on the hot path of essentially
every query) and write the Phase 0 audit for it as a worked example before
generalising. That audit will exercise the Cython shim from §3.4 against a
real operator, surface any seam issues, and give the next PM a template.

## 9. What you should NOT do

- Do not modify `draken/` C++ headers, `.h` files, or `draken_native.cpp`
  without surfacing to the architect. The ABI is frozen and the type matrix
  is closed.
- Do not add new `.pyx` files that take `object` parameters on the hot path.
- Do not bypass the dispatch table by hard-coding type checks inline.
- Do not "fix" the residual `vector_ops/*.pyx` files by porting them ad hoc
  unless they are explicitly in scope of a current ticket — the operator
  rewrite is meant to migrate them in-place per operator, not as a separate
  sweep.
- Do not relitigate the §1–§3 design decisions (clean-break, no-shim,
  hash-only equality, NaN/-0.0, string-type-family, Unicode opt-in,
  consumer-edge-pattern). They are settled.

## 10. Who to ask

- **Architect** — design forks, type semantics, dependency policy.
- **Draken-rebuild PM (outgoing)** — questions about the bridge, the shim,
  the ABI guard, why a specific draken-side decision went the way it did,
  draken-side gaps surfaced during operator work.
- **Operator-rewrite PM (incoming, you)** — owns everything inside
  `opteryx/operators/`, `opteryx/expression/evaluator/`, the scheduler, the
  parallelism work, and the consumer-side `.pyx` migrations per operator.

---

**Last updated:** at the point E.24 lands and `make q` baseline is captured.
Past that point, this document is reference; the operator rewrite's own docs
become the operational source of truth.
