# Ticket: Zero-Python Phase 9d — Morsel nogil surface

> Implementation sub-ticket of the locked Phase 9 design
> (`docs/tickets/zero-python-phase-9-c-kernel-abi-design.md` §Post-design).
> Implements Decision 7. **Depends on 9c** (which defines
> `execute_bytecode_c` and the BC_CASE re-entry that needs a nogil
> Morsel take). Unblocks 9e (full nogil annotation).

## Goal

Give `Morsel` a nogil-callable C surface for the operations the
executor performs in the inner loop, so 9e can annotate
`execute_bytecode` (and the BC_CASE re-entry) `nogil` end-to-end.

## Locked decision

**Decision 7**: minimum nogil surface —
- `num_rows` (read-only) — already `morsel.ptr.num_rows`, a C field;
  confirm it's reachable without the GIL.
- column access by index — `morsel._columns[i]` is a Python list
  access (needs GIL). Provide a `cdef DrakenVector* column_dv_c(int idx) nogil`
  that reads from a C array of `DrakenVector*` instead of the Python
  list. Requires the Morsel to maintain a parallel C array of column
  pointers (populate on construction).
- `take(indices)` → a `cdef Morsel take_rows_c(...) nogil` variant, OR
  accept that BC_CASE's sub-morsel take re-acquires the GIL briefly.
  **Surface the choice** — fully-nogil take is more work; a GIL
  reacquire per CASE branch is a small, bounded cost.

## The hard part — `take` for BC_CASE

The executor's hot loop (LOAD_COL, COMPARE, BINARY_OP, etc.) only
needs `num_rows` and column-by-index — both makeable nogil with a C
array of `DrakenVector*`.

BC_CASE is the exception: `_decide_compiled` / `_compute_compiled`
build sub-morsels via `morsel.take(live)`. `take` allocates a new
Morsel, calls `_nb.take()` per column — Python-heavy. Two paths:
- **(a) nogil `take_rows_c`**: a C function that builds the sub-morsel's
  `DrakenVector*` array by calling draken's `draken_take` per column
  (the C kernel, already used by the DV paths). Returns a C-owned
  morsel handle. Most work; keeps CASE fully nogil.
- **(b) GIL reacquire for CASE take**: BC_CASE's C kernel does
  `with gil:` around the sub-morsel construction, runs the branch
  bytecode nogil, releases again. Simple; one GIL transition per CASE
  branch per morsel — bounded, not per-row.

**Recommendation: (b) for the first cut**, with (a) as a follow-up
optimisation if CASE-heavy workloads show the GIL transition matters.
Surface and let the architect decide.

## Scope

**In scope**
- `draken/morsels/_morsel_shim.pyx`:
  - A parallel C array of `DrakenVector*` populated at Morsel
    construction (so column-by-index is nogil).
  - `cdef DrakenVector* column_dv_c(Py_ssize_t idx) nogil` accessor.
  - `cdef uint32_t num_rows_c() nogil` if `ptr.num_rows` isn't already
    cleanly reachable.
  - (If path (a)) a `cdef ... take_rows_c(...) nogil` builder.
- The Morsel `.pxd` (if one exists) to expose these `cdef` methods to
  `evaluation.pyx`'s `cimport`.

**Out of scope**
- The nogil annotation of `execute_bytecode` itself — 9e.
- Any change to the Python-facing Morsel API (`take`, `column`,
  `num_rows` properties stay for Python consumers).

## Verification

- `make c` clean fresh build.
- `make q` 100/100.
- A focused test: call `column_dv_c` / `num_rows_c` from a `cdef nogil`
  context and verify they return correct pointers/counts.
- BC_CASE queries still correct (path (a) or (b)):
  - `SELECT CASE WHEN id < 5 THEN 'small' ELSE 'big' END FROM $planets LIMIT 4`
  - Multi-branch + nested CASE.
- `make clickbench` non-regressing.

## Constraints (CLAUDE.md)

- **No `object` access in nogil methods** — the C array of
  `DrakenVector*` must not require touching the Python `_columns` list
  inside nogil.
- **The C column array must stay in sync** with `_columns` — populate
  both at every construction site. A divergence is a correctness bug
  (stale pointer). Audit all Morsel construction paths.
- **`make c` clean before done.**
- **Do not commit.**

## Pre-flight reading

1. Phase 9 design §Post-design Decision 7.
2. 9c ticket — the `execute_bytecode_c` re-entry that consumes this.
3. `draken/morsels/_morsel_shim.pyx` — all Morsel construction sites
   (`from_vectors`, `take`, `select`, `align_tables`, `_make_morsel`).
   Every one must populate the C column array.
4. `draken/ops/hash.h` `draken_take` — the per-column C take kernel.

## Definition of done

- Morsel exposes `cdef nogil` column-by-index + num_rows accessors
  backed by a C array of `DrakenVector*`.
- The C array is populated at every Morsel construction site.
- BC_CASE sub-morsel path resolved (path (a) nogil take, or (b) bounded
  GIL reacquire — PR states which).
- nogil-context test passes.
- `make c` clean; `make q` 100/100; `make clickbench` non-regressing.
