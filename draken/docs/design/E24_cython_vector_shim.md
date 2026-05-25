# E.24 — Cython shim layer for Vector / BoolVector / Morsel (Option A)

> **Goal:** make `cimport draken.vectors.vector / .bool_vector / draken.morsels.morsel`
> from Cython consumers (the evaluator's bytecode VM in particular) succeed at import
> time — no more `KeyError: '__pyx_vtable__'` — without touching `.h`/`.cpp` sources
> on the draken side, and without doing a 20-file operator rewrite.
>
> **Predecessor:** E.23 (uncovered the blocker). E.23's lower-stakes piece (the
> `vector_dfa_extract.pyx` cimport bug) is still in scope — fold it into this ticket if
> not yet landed.
>
> **Status:** TODO. This is the architect-approved "A" of "A then B" — a tactical
> bridge. B (push all callers through `draken_vector_unwrap`) is a follow-up ticket.

---

## 1. The blocker, restated

Today the layer is:

- `draken_native.so` (nanobind) exposes `Vector`, `BoolVector`, `Morsel`.
- `draken/vectors/vector.py`, `draken/vectors/bool_vector.py`, `draken/morsels/morsel.py`
  are **pure Python** modules that `from draken.draken_native import Vector as ...` and
  re-export.
- `draken/vectors/vector.pxd`, `bool_vector.pxd`, `draken/morsels/morsel.pxd` declare
  these names as Cython `cdef class` with `cdef` methods (`unified()`,
  `null_bitmap_ptr()`, `c_hash_single()`, etc.).

When `_impl.pyx` (or any other Cython consumer that `cimport`s these) is loaded, Cython
verifies that the runtime class has a `__pyx_vtable__` slot. The nanobind class does not.
Import aborts with `KeyError: '__pyx_vtable__'`. `make q` is at 0/133 because of this.

The leftover `.cpp` files in `draken/vectors/` (`vector.cpp`, `bool_vector.cpp`,
`integer64_vector.cpp`, etc.) are draken_old's Cython-generated output. They reference
old include paths (`draken/src/...`) that no longer exist. **They are not directly
reusable.** Don't try to compile them as-is.

## 2. The shape of Option A

The smallest viable shim that lets the existing evaluator cimports work, with no caller
rewrite at this stage:

1. **Author new `.pyx` files** that compile to **real Cython extension modules** at
   `draken.vectors.vector`, `draken.vectors.bool_vector`, `draken.morsels.morsel`. The
   compiled `.so` (with `__pyx_vtable__`) **replaces** the current `.py` re-export at
   the same import path. Pick the new `.pyx` filenames to avoid clashing with the
   stale Cython-generated `.cpp` already sitting in the directories (`_vector_shim.pyx`
   → `_vector_shim.cpp`, etc.) so the build doesn't pick up the dead `.cpp`s.

2. Each shim `cdef class` (`Vector`, `BoolVector`, `Morsel`) stores a single Python
   reference to the underlying nanobind instance plus the unwrapped `DrakenVector*` /
   `DrakenMorsel*` pointer obtained via the existing
   `draken_vector_unwrap` / morsel equivalent from `draken/core/draken_bridge.h`.

   ```cython
   cdef class Vector:
       cdef object _nb           # nanobind handle, keeps memory alive
       cdef const DrakenVector* _dv

       def __cinit__(self, object nb_vector):
           self._nb = nb_vector
           self._dv = draken_vector_unwrap(<PyObject*>nb_vector)

       cdef const DrakenVector* unified(self) noexcept nogil:
           return self._dv
       # ...etc for the cdef methods declared in the existing .pxd
   ```

3. **Update the existing `.pxd`** to match the shim shape exactly. The current `.pxd`s
   declare `cdef class BoolVector(Vector): cdef DrakenFixedBuffer* ptr` — that
   `ptr` attribute will not exist on the shim. Decide the right shape from the C++
   side: every method should be implementable in terms of `_dv` (the unified
   `DrakenVector*`). Drop attributes that aren't needed.

4. **Construction sites:** every call site that previously got a nanobind `Vector`
   back from a C′ extension (e.g. `vector_*` nanobind functions returning `Vector`)
   continues to return the nanobind class. To pass into a cimport-using consumer
   (the bytecode VM in particular), wrap once at the boundary:
   `cdef Vector v = Vector(nb_vector)`. The bytecode VM is the only consumer for
   now; the wrapping cost is one allocation per filter call, not per row.

5. **`isinstance` semantics:** the public Python class people see is still the
   nanobind one (it's what every C′ function returns). The shim is internal — a way
   to give Cython something it can typecheck. Do NOT change the C′ nanobind functions
   to return the shim type; that would propagate the wrapping out across the codebase
   and is what option B is for.

   This implies: `from draken.vectors.vector import Vector` will now resolve to the
   shim (because the `.so` shadows the `.py`). If any Python code does
   `isinstance(x, Vector)` against a runtime nanobind handle, it will break. Audit
   for these sites before landing — likely only a handful. If you find more than ~5,
   STOP and surface.

## 3. Acceptance criteria (hard)

1. `python setup.py build_ext --inplace -j 4` (no `DRAKEN_BUILD`) completes without a
   Cython compile error AND without an `__pyx_vtable__` import error.
2. `opteryx/expression/evaluator/_impl.cpython-313-darwin.so` exists.
3. `python -c "from opteryx.expression.evaluator import _impl"` imports cleanly.
4. `python -c "from draken.vectors.vector import Vector; print(Vector)"` shows the
   Cython shim class (not the nanobind class).
5. `make draken` still works (E.22 must not regress).
6. `make dt` still passes — **2792+ draken tests, all green**. If any fail, the shim
   broke something the tests touch and that has to be the architect's call.
7. Report `make q` state. Pass-count expected to climb meaningfully from 0/133 once
   `_impl` imports; the remaining failures are operator-rewrite gaps (out of scope).

## 4. Non-goals (explicitly out of scope)

- The 20+ operator file rewrite to use `draken_vector_unwrap` directly. That's option
  B, the follow-up ticket.
- Rewriting the bytecode VM (E.20 §5.1 Option B). Still deferred.
- Touching `.h` or `.cpp` files in `draken/`. The C++ side is closed for this ticket.
- "Cleaning up" the leftover `vector.cpp`/`bool_vector.cpp`/etc. in `draken/vectors/`
  that came from draken_old. They are not compiled by the current `setup.py`; they
  are just dead text. Leave them. (A separate housekeeping ticket can delete them
  later — flagging now would balloon the diff.)
- Replacing the nanobind classes. They remain the canonical runtime type.

## 5. Risks & places to STOP

- If implementing the shim requires **changing any C′ nanobind extension's return
  type** (i.e. you find yourself editing `opteryx/compiled/nanobind/*.cpp` to make the
  shim work), you are about to start option B by accident. **Stop and report.**
- If `isinstance(x, Vector)` audits find more than ~5 sites, stop and surface — that
  changes the migration cost picture.
- If `make dt` regresses, stop. The shim must be transparent to draken's own tests
  (which call nanobind directly and shouldn't touch the shim at all).
- If you find that the cdef methods declared on `Vector`/`BoolVector` in the current
  `.pxd` are called from MORE than the bytecode VM (i.e., other Cython sites we
  haven't migrated), surface — the wrapping cost story changes.

## 6. Discipline reminders

- **No `object` parameters/returns in compiled Cython** beyond the documented edge
  pattern. The shim's `__cinit__(self, object nb_vector)` is acceptable because
  `__cinit__` is constructor edge code, but the cdef methods must be typed.
- **No silencing**, no `try/except ImportError`, no fallback `.py` reinstatement.
  The shim either builds and replaces the `.py` or the ticket fails — no halfway
  state.
- **`hasattr` is banned.** Per CLAUDE.md §9.
- **Don't touch git.**
- **No drift.** Files in scope: the three `.pxd` files, three new `.pyx` shim files,
  `setup.py` (adding three Extension entries), the three existing `.py` re-export
  files (delete them so the `.so` is what gets imported), possibly `vector_dfa_extract.pyx`
  (the trivial `uint32_t` cimport fix from E.23). Anything beyond → stop and ask.

## 7. Reporting back

On completion:
- Exact list of files changed/added/deleted (paths + 1-line reason).
- Output of `ls -la draken/vectors/vector*.so draken/vectors/bool_vector*.so draken/morsels/morsel*.so opteryx/expression/evaluator/_impl*.so`.
- Output of `make dt | tail -3`.
- Output of `make q | tail -10` (pass-count + failure shape, NOT fixed in this ticket).
- A note on whether any of the §5 STOP conditions came close to triggering.
