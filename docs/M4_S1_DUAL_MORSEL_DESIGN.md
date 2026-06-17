# S1 — Dual-Representation Morsel (scan emits CxxMorsel; lazy PyObject shim)

Status: **design, awaiting architect sign-off. No code cut.**
Date: 2026-06-16
Parent: `docs/M4_CPP_MORSEL_DESIGN.md` (S1). Decision locked 2026-06-16: Option A
(dual-representation Morsel, lazy shim); design-doc-first.

Goal: the parquet scan builds a `CxxMorsel` natively (off-GIL assembly, no PyObject),
and the cdef `Morsel` carries it. Operators already converted to C++ read the
`CxxMorsel` directly (nogil); unconverted operators trigger **lazy** materialization
of the PyObject columns. The push protocol is unchanged (still passes `Morsel`), so
conversion stays incremental. The S0 op surface (cxx_hash/take/mask/slice/combine/
select) + seam are the building blocks; they are proven byte-identical to `Morsel`.

---

## 1. The carrier: one authoritative representation at a time

The cdef `Morsel` gains an optional C++ backing. At any instant it is in exactly one
state — never both populated (no dual-write consistency problem):

- **Cxx-backed:** `_cxx` (a `shared_ptr[CxxMorsel]`) is set; `_columns` (the existing
  `vector[PyObject*]`) is empty. Produced by the scan and by converted operators.
- **PyObject-backed:** `_columns` is populated; `_cxx` is null. The current state for
  everything today.

`morsel.pxd` gains:
```cython
from draken.morsels.cxx_morsel cimport CxxMorsel       # new .pxd declaring the C++ type
cdef shared_ptr[CxxMorsel] _cxx                          # null unless Cxx-backed
```

### State transitions
- **`_ensure_pyobject(self)`** — called at the top of every PyObject-facing accessor.
  If `_cxx` is set: materialize `_columns` + `_col_names` from it (one Vector PyObject
  per `CxxColumn` via `nb::cast(own)` — byte-identical to `from_vectors`), then **reset
  `_cxx`** (now PyObject-backed). Idempotent; no-op when already PyObject-backed.
- **`cdef CxxMorsel* _get_cxx(self)`** — for converted operators. If `_cxx` is set,
  return it (nogil, zero work). If PyObject-backed, build a transient `CxxMorsel` from
  `_columns` via the seam (handle→`shared_ptr<VectorOwner>`) and return it **without**
  clearing `_columns` (PyObject stays authoritative; the built CxxMorsel is for this op).

Materialization is the ONLY place PyObject Vectors get created for a Cxx-backed morsel,
and it is byte-identical by construction (it wraps the exact `VectorOwner`s the morsel
holds — the same thing `from_vectors` would have wrapped). Proven by the S0 seam tests.

### Why this removes the GIL hold
For a **contiguous converted segment** (scan → … → agg, all reading `_get_cxx`), the
morsel stays Cxx-backed the whole way; `_ensure_pyobject` never fires. PyObject Vector
creation happens **once, at the segment's end** (the cursor, or the boundary to an
unconverted operator) — not per-operator. That is the win. S1 alone (scan Cxx-backed,
next operator still unconverted → immediate materialization) is perf-neutral; the win
lands when S2 converts the downstream operator to read `_cxx`.

---

## 2. Accessors that call `_ensure_pyobject` (the lazy seams)

Every method that touches `_columns`/`_col_names` as PyObjects:
`__getitem__`, `__len__`, `num_rows` (when columns drive it), `column`, `select`,
`rename`, `take`, `filter_mask`, `slice`, `copy`, `combine`, `from_vectors`-consumers,
`hash`/`hash_keys` (until they read `_cxx`), `__str__`, `column_names`, `column_types`,
`_get_column`/`_set_column`/`_append_column`/`_resolve_columns_to_indices`/
`_columns_to_pointers`/`_take_inplace`/`_empty_inplace`. Centralising on the cdef
`_get_column` + the Python-facing methods covers the surface; audit each call site.

`num_rows` can answer from `_cxx->num_rows()` WITHOUT materializing — keep it cheap so
the driver/telemetry (`drive_scan`, `push`) don't force materialization just to count
rows. Same for `num_columns`.

---

## 3. Scan build path (S1 proper)

`pool_reader.pyx` `_wrap_*` currently build Vector PyObjects via `draken_vector_own_*`.
Add a parallel native path: build a `shared_ptr<VectorOwner>` directly from the same
decoded buffers (the MorselRef handoff is already off-GIL) — a C++ helper that does what
`draken_vector_own_*` does minus the nanobind PyObject wrap. Assemble a `CxxMorsel`
(names + CxxColumns). `parquet_read.pyx` emits it via a new `Morsel.from_cxx(cxx)` (Cxx-
backed carrier) instead of `Morsel.from_vectors(...)`.

**NO FLAG (corrected 2026-06-16).** An earlier draft gated this behind a default-off
`CXX_MORSEL_SCAN` flag "to prove it safely" — but a default-off flag keeps the OLD path
as the default, so a green suite is testing the old path (green-but-fake). "If it's
gated, it's not done." So the scan emits Cxx-backed morsels UNCONDITIONALLY; correctness
is the value-checked q/tpch/clickbench-vs-DuckDB suites on the real default path. The
dual-representation Morsel + lazy materialization is the sanctioned bridge for incremental
op conversion (NOT a fallback); operators not yet reading the CxxMorsel produce PyObject
morsels — that is "not-yet-converted", not a hidden escape hatch.

(Substrate ≠ parallelism: this is the data representation, independent of
`MAX_EXECUTION_WORKERS`. M4's parallel workers are a *consumer* that needs it nogil so
concurrent segments don't serialize on the GIL — worker count never enters its code path.)

---

## 4. Plumbing (new, small)

- `draken/morsels/cxx_morsel.pxd` — cdef-extern declarations of `CxxMorsel`/`CxxColumn`
  for Cython consumers (the morsel module + scan + converted operators).
- C++ helpers (in draken_native or a small TU): `cxx_from_handles(list) -> CxxMorsel`
  (handle→shared_ptr seam, already proven) and `materialize_handles(CxxMorsel&) -> list`
  (CxxColumn→Vector handle, already proven). These ARE the `_cxx_morsel_roundtrip` halves;
  expose them as the two reusable directions.
- `Morsel.from_cxx(cxx)` classmethod/`cdef` factory; `_ensure_pyobject`; `_get_cxx`.

---

## 5. Correctness & invariants

- A Cxx-backed morsel and its materialized PyObject form are byte-identical (same
  VectorOwners). The S0 seam round-trip test already proves this for the column set;
  add a Morsel-level test: `from_cxx(c)` then any PyObject accessor == `from_vectors`.
- **No dual-write:** at most one representation is authoritative; `_ensure_pyobject`
  collapses Cxx→PyObject before any PyObject mutation. `_get_cxx` on a PyObject-backed
  morsel builds a transient (read-only) CxxMorsel; it must NOT be mutated in place.
- `__dealloc__` must release `_cxx` (shared_ptr) AND the existing `_columns` refs.
- ARRAY/LIST columns (scan still pool-path) and any type the native scan build can't
  produce → that column stays PyObject; a morsel is Cxx-backed only if ALL columns are.

---

## 6. Staging (each gated `make q`/tpch/clickbench identical; default off)

- **S1.0 — carrier + lazy seam, inert.** Add `_cxx`, `from_cxx`, `_ensure_pyobject`,
  `_get_cxx`, the `.pxd`, the two C++ helper directions. Nothing builds Cxx-backed
  morsels yet → behaviour-neutral. Morsel-level round-trip test (`from_cxx` ==
  `from_vectors`). Gate.
- **S1.1 — scan emits CxxMorsel** (LANDED: `Morsel.from_cxx_vectors`; native
  `_wrap_*` → shared_ptr<VectorOwner> is a later optimization). NO FLAG — the default
  parquet single-pass path emits Cxx-backed morsels; q/tpch/clickbench (value-checked
  vs DuckDB) are the gate on the real default path.
- **S1.2 — `num_rows`/`num_columns` answer from `_cxx`** without materializing, so the
  driver doesn't force it. Verify `_ensure_pyobject` only fires at the first real
  PyObject column access.
- **Then S2** — convert grouped-agg to read `_get_cxx` (cxx_hash + accumulate nogil),
  so the scan→agg segment never materializes → first measurable M4 win.

## 7. Risks to surface
- `_ensure_pyobject` must cover EVERY PyObject access path, or a missed one reads an
  empty `_columns` on a Cxx-backed morsel → wrong/empty results. Mitigation: route ALL
  PyObject column access through the cdef `_get_column`/`_col_names` accessors and gate
  there; audit `_morsel_shim.pyx` exhaustively; the flag-off default keeps it inert
  until audited.
- Cython holding a C++ `shared_ptr<CxxMorsel>` member in a cdef class with `__cinit__`/
  `__dealloc__` — verify the move/RAII interplay (CxxMorsel is move-only; the morsel
  holds it by shared_ptr so copies are refcounts).
