# E.33 — Unblock `vector_string_case` and `vector_string_misc2` builds

> **Status:** TODO.
>
> **Goal:** fix two narrow blockers in already-in-flight nanobind
> extensions so `make draken` / `make compile` / `make dt` succeed clean
> from a fresh build, no `.so` skipping.
>
> **Why this exists:** discovered during the post-zombie-sweep verification.
> Both bugs are downstream of other agents' in-flight work on the UTF-8 /
> regex cluster migration; neither relates to E.32 (decimal kernels) or the
> zombie sweep. Surfaced explicitly so they don't sit unfixed silently.
>
> **Predecessors:** E.26 (UTF-8 lowercase pilot — left bug #1 below);
> some regex-cluster migration work in progress (left bug #2 below).

---

## 1. The two bugs

### Bug 1 — `vector_string_case.cpp:402` argument-count mismatch

```cpp
// opteryx/compiled/nanobind/vector_string_case.cpp:402
utf8_int8_t* next = utf8codepointcalcsize(p, &cp);
```

`utf8codepointcalcsize` (declared in `third_party/utf8h/utf8.h:266`) takes
**one argument**: `utf8codepointcalcsize(const utf8_int8_t *str)`. The
caller passes two. Compile fails: `error: no matching function for call
to 'utf8codepointcalcsize'`.

The semantically-correct API for "decode a codepoint and advance the
pointer" is `utf8codepoint(str, &codepoint)` (declared at
`third_party/utf8h/utf8.h:261`). That returns the next pointer AND
writes the codepoint via the out-parameter. The caller's
`utf8_int8_t* next = ...` shape matches what `utf8codepoint` returns.

**Fix:** replace `utf8codepointcalcsize(p, &cp)` with `utf8codepoint(p, &cp)`.

Check the rest of `vector_string_case.cpp` for the same mistake — if
there are other call sites with the same wrong shape, fix them all in
one pass. Read the few nearby lines to make sure the semantics still
make sense (the function is iterating codepoints in some case-folding
loop).

### Bug 2 — `vector_string_misc2` runtime link error

```
ImportError: dlopen ... vector_string_misc2.cpython-313-darwin.so:
  symbol not found in flat namespace
  '__ZN3re23RE213GlobalReplaceEPNSt3__112basic_stringIcNS1_11char_traits...'
```

That's `re2::RE2::GlobalReplace` missing at load. The module was
modified to add a `vector_regex_replace` function using re2 (file
header line 6 documents this and line 316 has the function;
`s_re2_cache` lives at line 320). But `setup.py:1824-1842`'s
`Extension("opteryx.compiled.nanobind.vector_string_misc2", ...)` sources
list does NOT include the re2 `.cc` files. The compile succeeds because
Mac linker is lazy; load fails because the symbol isn't resolved.

**Fix:** add the re2 sources (and any required compile flags) to the
`vector_string_misc2` Extension entry in `setup.py`. The right pattern
to mirror is whatever other nanobind extension already integrates re2
successfully — `grep -n "re2" setup.py` and find an Extension whose
sources include `glob.glob("third_party/re2/re2/*.cc") + [...]`. Use
its exact source/flag set on this extension too.

If after adding the sources the binary balloons unreasonably, surface —
re2 is a large dependency and we may want to factor it into a shared
helper. But for this ticket: smallest viable fix is "add the same re2
source list this extension is using to that one."

## 2. STOP conditions

- File count > 3: `vector_string_case.cpp`, `setup.py`, possibly
  `vector_string_misc2.cpp` if its include set is wrong. Past 3 →
  drifting.
- You start refactoring how the UTF-8 cluster uses `utf8.h`. **STOP.**
  This ticket is two targeted fixes, not a cleanup pass on E.26's
  work.
- You start refactoring the re2 integration to be "cleaner." **STOP.**
  Mirror the existing pattern, fix it later if needed.
- The fix to vector_string_case.cpp turns out to require changing the
  algorithm (not a simple call-site swap). **STOP and surface** —
  that's an E.26 design rework, not a typo fix, and belongs back with
  the UTF-8 agent.
- `make dt` regresses below the post-fix expected count.

## 3. Discipline reminders

- **No git commands.**
- **No `cdef object` / `object` parameters anywhere.** §3.
- **No new draken/ surface.** No new `.pxd`, no new kernel, no
  modification to anything in `draken/`. Both bugs live in
  `opteryx/compiled/nanobind/`.

## 4. Acceptance

Run and report verbatim:

1. `find . -name "*.cpython-313-darwin.so" -delete && make draken 2>&1 | tail -5` — succeeds, all extensions including
   `vector_string_case` and `vector_string_misc2` build and copy.
2. `python -c "from opteryx.compiled.nanobind.vector_string_case import vector_lowercase; print(vector_lowercase)"` — imports cleanly.
3. `python -c "from opteryx.compiled.nanobind.vector_string_misc2 import vector_regex_replace; print(vector_regex_replace)"` — imports cleanly (the symbol is found at load).
4. `make dt 2>&1 | tail -3` — passes including `test_vector_string_case.py` and `test_string_misc2.py` (which were skipped during the zombie-sweep verification). Expected ≥2818 passing (2816 + the 2 previously-skipped test files contributing some count).
5. `git diff --stat HEAD` — files changed ≤3.

## 5. Reporting back

- §4 acceptance outputs verbatim.
- The exact diff applied to `vector_string_case.cpp` (one or two lines).
- The exact diff applied to `setup.py` for the `vector_string_misc2`
  Extension (the source list addition).
- If you found other call sites of `utf8codepointcalcsize` with the
  wrong arg count in `vector_string_case.cpp` or elsewhere, list them.
- Confirmation that no other agent's work was touched and no `draken/`
  side surface was modified.
