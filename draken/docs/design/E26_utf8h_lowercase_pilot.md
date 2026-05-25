# E.26 — Vendor `utf8.h` + port `vector_lowercase` to C′ nanobind (UTF-8 cluster pilot)

> **Status:** TODO.
>
> **Architect call (2026-05-25):** UTF-8 case-folding library = `sheredom/utf8.h`,
> single-header MIT. Decision recorded; the wider "Unicode library choice"
> design fork is now closed.
>
> **Goal:** vendor `utf8.h` and port **one** UTF-8-cluster file
> (`vector_lowercase`) to a C′ nanobind extension as a worked example for the
> remaining four (`vector_uppercase`, `vector_initcap`, `vector_reverse`,
> `vector_string_slice`). Establish the pattern; do not pre-port the others.
>
> **Predecessors:** E.22 (build isolation makes this safe), E.25 (revert
> baseline). Draken side is post-E.25 clean.

---

## 1. What's being delivered

1. `third_party/utf8h/utf8.h` — single-header from
   <https://github.com/sheredom/utf8.h/blob/main/utf8.h>, vendored verbatim.
   No edits. Pin commit or release tag in a top-of-file comment so we can
   re-vendor reproducibly.
2. `setup.py` — `third_party/utf8h` added to the global `include_dirs`
   alongside `third_party/boost_math`, `third_party/yyjson/src`, etc.
3. `opteryx/compiled/nanobind/vector_string_case.cpp` — new C′ nanobind
   extension exposing `vector_lowercase(Vector) -> Vector`. **Per-type
   semantics** (per `draken-string-type-family` memory):
   - `DRAKEN_VARCHAR` → **ASCII-only fold** (bytewise; `é` stays `é`). No
     `utf8.h` involved on this path; use the existing
     `simd_to_lower` (or equivalent in-arena ASCII fold) so VARCHAR keeps its
     "cheap" cost story.
   - `DRAKEN_NVARCHAR` → **full codepoint-by-codepoint Unicode fold** via
     `utf8.h`'s `utf8lwr` / `utf8lwrcodepoint`. Length-preserving only —
     `utf8.h` does codepoint mapping, not length-changing case folding
     (e.g. German `ß` → `ss`). That limitation is acceptable for v1 and is
     documented in the file's docstring.
   - `DRAKEN_VARBINARY` → **throw** (`std::invalid_argument` or equivalent,
     surfacing as a Python `ValueError`). Case ops on opaque bytes are
     unsupported per the string-family memory.
4. Native draken test under `draken/tests/` exercising all three branches:
   `lower("HELLO")` on VARCHAR → `"hello"`; `lower("ÄÖÜ")` on NVARCHAR →
   `"äöü"`; `lower(<varbinary>)` raises.
5. One-line import update in `opteryx/expression/evaluator/string_ops.pyx`
   (and any other call site) — replace `from opteryx.compiled.vector_ops
   import vector_lowercase` with `from opteryx.compiled.nanobind.vector_string_case
   import vector_lowercase`. **Only if** this is a clean one-line change.
   If the caller pattern is incompatible with the new return type, **STOP**
   and surface — that's eval-PM territory, not part of this ticket.

## 2. What is explicitly NOT in scope

- The other four UTF-8 cluster files. They are the follow-ups, not this
  ticket. After this lands, they get their own tickets, modelled on the
  pattern you establish here.
- Deletion of the old `opteryx/compiled/vector_ops/vector_lowercase.pyx`.
  Leave it for now — the other four UTF-8 files still use the old pattern,
  and removing one prematurely creates a split state. The cleanup is one
  ticket at the end of the cluster.
- Changing any `.cpp` / `.h` in `draken/`. The draken side is closed for
  this ticket. The new extension lives under
  `opteryx/compiled/nanobind/`, not under `draken/`.
- Refactoring the existing `vector_string_misc*.cpp` modules to share the
  new utf8 code. Tempting; not in scope. Each cluster file gets its own
  port; cluster-wide refactoring is a separate ticket at the end.
- `utf8.h` extensions, optimisations, or wrappers. Vendor it verbatim.
- Any change to `opteryx/expression/evaluator/string_ops.pyx` beyond the
  one-line import switch. The evaluator migration is the eval-PM's
  initiative (see `opteryx/expression/evaluator/docs/design/00_pm_briefing.md`).

## 3. STOP conditions

Trip any of these, stop and surface — don't fold the fix into this ticket.

- More than ~6 files touched (vendoring + 1 nanobind extension + 1 test +
  1 setup.py + at most 1 evaluator import-line + 1 design-record line in a
  memory file). If your count is climbing past 6 you've drifted.
- The single-line `string_ops.pyx` import switch turns out to need >1 line
  of change (e.g. the call shape is different, return type isn't the same
  StringVector handle). Stop. Report. This is eval-PM-shaped work.
- `utf8.h`'s `utf8lwr`/`utf8lwrcodepoint` doesn't behave as described in
  its own README. Surface — don't paper over with a hand-rolled fold loop.
- `make draken` regresses. The new extension must build cleanly via
  `DRAKEN_BUILD=1`.
- `make dt` regresses below 2792 passing.
- You feel the urge to extend the `draken_native.cpp` nanobind surface,
  add a new bridge function, or modify `draken/core/draken_bridge.h`. The
  existing bridge (`draken_vector_unwrap` / `draken_vector_own_string`) is
  sufficient — `draken_vector_own_string` takes a `DrakenType` parameter
  exactly so consumers can produce VARCHAR / NVARCHAR / VARBINARY. Use it.

## 4. Discipline reminders (from recent failures)

- **No `object` parameters/returns in compiled Cython.** Per CLAUDE.md §3.
  The new module is `.cpp` not `.pyx`, so this is mostly automatic, but
  the import-line update in `string_ops.pyx` must not introduce one.
- **No fake-green.** If a piece doesn't work, the build is allowed to be
  red. Do not add a compatibility shim, a typedef alias, or a verbatim-C
  struct workaround. (See E.24 for what to NOT do — `draken/docs/design/
  E25_e24_revert_and_redo.md` documents the patterns.)
- **No `try/except` for flow control.** Per CLAUDE.md §9. The VARBINARY
  branch throws cleanly; the caller in `string_ops.pyx` handles the
  exception only if the caller already has an exception path.
- **No cluster-wide refactor.** This ticket is one file. The pattern this
  file establishes is what the next four tickets follow. Do not "while I'm
  here" the cluster.
- **No git commands.** Do not commit, do not push, do not amend.

## 5. Acceptance criteria

Run these and report output verbatim:

1. `ls third_party/utf8h/utf8.h` — file exists.
2. `grep "third_party/utf8h" setup.py` — include path added.
3. `make draken 2>&1 | tail -5` — build succeeds; the new extension
   `opteryx/compiled/nanobind/vector_string_case.cpython-313-darwin.so`
   appears in the "copying ... .so" lines.
4. `ls -la opteryx/compiled/nanobind/vector_string_case*.so` — file exists.
5. `python -c "from opteryx.compiled.nanobind.vector_string_case import
   vector_lowercase; print(vector_lowercase)"` — imports cleanly.
6. `make dt 2>&1 | tail -3` — still ≥2792 passing.
7. Native test output: VARCHAR ASCII fold, NVARCHAR Unicode fold, VARBINARY
   raises. Three assertions, three pass.
8. `git diff --stat HEAD` shows ≤6 files changed in scope (utf8.h is added
   so it doesn't appear in `git diff` of tracked files; verify visually it's
   on disk).

## 6. Reporting back

- The above eight acceptance outputs.
- A brief note on the **pattern established** for the cluster: how the
  per-type dispatch is structured in the .cpp, which `utf8.h` functions
  were used, what the per-type cost story looks like in practice. This
  note is what the next four cluster ports will follow — write it for the
  agent who'll do the next port, not for me.
- A list of any STOP conditions that came close to triggering (with a
  one-line note each on why you didn't stop).
- A confirmation that the architect's
  [[draken-string-type-family]] memory rule was followed: VARCHAR =
  ASCII-only fold, NVARCHAR = Unicode codepoint fold, VARBINARY = throw.

## 7. After this lands

Four follow-up tickets, one per remaining UTF-8 file, each modelled on
this one's pattern: `vector_uppercase`, `vector_initcap`, `vector_reverse`,
`vector_string_slice`. Each is its own ticket — no batch.

Once all five land, a single cleanup ticket deletes the old
`opteryx/compiled/vector_ops/vector_{initcap,lowercase,uppercase,reverse,
string_slice}.pyx` files and their `setup.py` Extension entries.
