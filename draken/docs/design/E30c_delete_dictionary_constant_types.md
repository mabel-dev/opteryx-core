# E.30c — Delete `DRAKEN_DICTIONARY` and `DRAKEN_CONSTANT` from `DrakenType`

> **Status:** TODO. **Critical.**
>
> **Goal:** remove `DRAKEN_DICTIONARY = 61` and `DRAKEN_CONSTANT = 62` from
> the `DrakenType` enum and from every call site that uses them. Replace
> tag-based shape dispatch with layout-based shape inference where any
> caller genuinely needs to know the shape.
>
> **Why this exists:** these two enum values violate CLAUDE.md §11 and
> `draken/docs/design/00_data_model.md` — the most fundamental invariant
> of the unified-format model. Encoding shape (Dense / Constant / Dict)
> is a **layout hint discoverable from the structure of the vector**, not
> a dispatch signal in the type tag. Tag-as-shape was the pattern this
> rebuild was *eradicating*; we shipped it into the frozen ABI anyway.
>
> **Predecessors:** E.29 surfaced producer-side rot riding on the same
> wrong assumption. E.30a/b address Python-imports-in-cdef. This ticket
> addresses the deepest of the three findings — the violation that's in
> the ABI itself.
>
> **The architect is furious about this and rightly so.** Read CLAUDE.md
> §11 twice before starting. Then read `draken/docs/design/00_data_model.md`.
> If anything you do in this ticket reintroduces the tag-as-shape pattern
> under a different name (a `DrakenShape` enum, a `shape` field on
> `DrakenVector`, a "dict-aware" type tag in some other range), **STOP
> immediately and surface.** The point of this ticket is the deletion.
> The point is not to find a different home for the same wrong idea.

---

## 1. The two values to delete

```c
// draken/core/buffers.h:34–72
typedef enum {
    // ...
    DRAKEN_VARCHAR    = 60,
    DRAKEN_DICTIONARY = 61,  ← DELETE
    DRAKEN_CONSTANT   = 62,  ← DELETE
    DRAKEN_NVARCHAR   = 63,
    // ...
} DrakenType;
```

Slot 61 and 62 stay **vacant** with a comment recording why ("reserved,
do not reuse — historical anti-pattern, see E.30c"). Adjacent values
(`DRAKEN_VARCHAR = 60`, `DRAKEN_NVARCHAR = 63`, `DRAKEN_VARBINARY = 64`)
**must not renumber** — they're pinned in the ABI guard.

## 2. The call sites to fix

### 2.1 `draken/core/buffers.pxd:33-34` — Cython declaration

Delete the `DRAKEN_DICTIONARY` and `DRAKEN_CONSTANT` lines from the
`cdef enum DrakenType` block. The Cython side stops knowing about them.

### 2.2 `draken/draken_native.cpp:3232-3233` — nanobind enum exports

Delete:
```cpp
.value("DICTIONARY", DRAKEN_DICTIONARY)
.value("CONSTANT",   DRAKEN_CONSTANT)
```
These export the values to Python. Python callers that used
`DrakenType.DICTIONARY` will now fail at attribute access — that is the
**correct failure**, surface it.

### 2.3 `draken/ops/string_search.h:58-67` — kernel guard

```cpp
// BEFORE
if (v.type != DRAKEN_VARCHAR &&
    v.type != DRAKEN_NVARCHAR &&
    v.type != DRAKEN_VARBINARY &&
    v.type != DRAKEN_DICTIONARY &&     ← DELETE LINE
    v.type != DRAKEN_CONSTANT) {       ← DELETE LINE
    throw ... "(VARCHAR, NVARCHAR, VARBINARY, DICTIONARY, or CONSTANT)";  ← FIX MESSAGE
}
```

```cpp
// AFTER
if (v.type != DRAKEN_VARCHAR &&
    v.type != DRAKEN_NVARCHAR &&
    v.type != DRAKEN_VARBINARY) {
    throw ... "(VARCHAR, NVARCHAR, or VARBINARY)";
}
```

The kernel reads through `selection` uniformly, so a dict-encoded
`DRAKEN_VARCHAR` (which has `type == DRAKEN_VARCHAR` and a dict-shaped
layout) goes through the **same** path. Encoding-shape transparency is
the design.

### 2.4 `draken/storage/morsel_io.cpp` (and `.pyx` source) — serialisation

Find every `if dtype == DRAKEN_DICTIONARY` / `dtype != DRAKEN_DICTIONARY`
branch. These exist in the `.pyx` source that compiles to the `.cpp`.
Each one is the same bug: serialisation is treating an encoding shape
as a type. Migrate each to:

- For "this vector is dict-encoded": check the layout —
  `vec.data_length < vec.length && vec.selection != identity_sel`.
- For "this vector is constant-encoded": check the layout —
  `vec.data_length == 1 && vec.selection == draken_zero_sel(...)`.
- For "what is the logical type of this dict-encoded thing": it's
  whatever `vec.type` says (which, post-deletion, can no longer be
  `DRAKEN_DICTIONARY` — it'll be `DRAKEN_INT64` / `DRAKEN_VARCHAR` /
  whatever the *real* logical type is).

If the serialisation format-on-disk has historically written a
"dictionary" type tag, that is a format-versioning concern — handle by
mapping the legacy on-disk tag to the new (logical type, dict shape)
pair on read. Do not preserve the legacy on disk for new writes.

### 2.5 Audit every other site that branches on type

```
grep -rn "DRAKEN_DICTIONARY\|DRAKEN_CONSTANT" --include="*.h" --include="*.cpp" \
    --include="*.pyx" --include="*.pxi" --include="*.pxd" --include="*.py" .
```

Every hit outside the ones listed above is its own fix. Some categories
to expect:

- **Type-mapping in `draken/interop/arrow.cpp`** (zombie orphan from
  E.24 era — already slated for deletion). Look at it for the **pattern**
  it embodies: `return DrakenType.DRAKEN_DICTIONARY` from a "what type
  is this Arrow column" function. Anywhere this pattern recurs (a type
  function that returns DICTIONARY/CONSTANT instead of the real logical
  type), the *function itself* is wrong, not just the call site. Surface
  these — they need design-level fixes, not just enum-deletion.
- **Test/regression code** that asserts `type == DRAKEN_DICTIONARY`. The
  test was asserting the bug; the assertion is wrong, not the test.
  Migrate to assert the shape via layout AND the type via `vec.type`.
- **Documentation / comments / error messages** referencing
  DICTIONARY/CONSTANT as types. Fix the prose too. Stale comments will
  re-seed this mistake.

### 2.6 `draken/core/_abi_guard.cpp` — pin updates

The ABI guard's `static_assert` block (the file documents one even if
the current implementation is stubbed) must pin the *kept* enum values
(`DRAKEN_VARCHAR = 60`, `DRAKEN_NVARCHAR = 63`, `DRAKEN_VARBINARY = 64`,
etc.). Add an assertion that `DRAKEN_DICTIONARY` and `DRAKEN_CONSTANT`
**do not exist** as identifiers — i.e., the build fails if someone
re-introduces them. Use a sentinel:

```cpp
// Post-E.30c: DRAKEN_DICTIONARY (61) and DRAKEN_CONSTANT (62) are
// permanently retired. Anyone re-adding them must read E.30c first.
#ifdef DRAKEN_DICTIONARY
#error "DRAKEN_DICTIONARY was deleted by E.30c. Encoding shape is layout, not type. Read CLAUDE.md §11."
#endif
#ifdef DRAKEN_CONSTANT
#error "DRAKEN_CONSTANT was deleted by E.30c. Encoding shape is layout, not type. Read CLAUDE.md §11."
#endif
```

This is the forcing function that makes the deletion stick.

## 3. What does NOT replace them

This ticket is the deletion. It is **not** the introduction of:

- A `DrakenShape` enum somewhere else. Layout is the shape.
- A `shape` field on `DrakenVector` struct. The struct already
  describes its shape via the relationship between `selection`,
  `data_length`, and `length`. Adding a field would be the same
  violation in a different costume.
- A new type tag range for "encoding-aware" types (e.g.
  `DRAKEN_DICT_INT64 = 200`). Same violation, same answer: no.
- A method on Vector like `vec.encoding_shape()` returning an enum.
  If a caller genuinely needs the shape (rare — per §11, default is
  shape-blind kernels), it reads it from the layout via small inline
  helpers (see §4 below).

If you find yourself proposing any of the above, **stop and surface**.
The architect is specifically allergic to this pattern and we are
specifically deleting an instance of it.

## 4. Shape-detection helpers (where genuinely needed)

For the small number of call sites that legitimately need to branch on
shape (per §11: "the exception, not the default"), add small inline
helpers in `draken/core/buffers.h`:

```c
static inline bool draken_vector_is_dense(const DrakenVector* v) {
    return v->data_length == v->length &&
           (v->flags & DRAKEN_SEL_IDENTITY) != 0;
}

static inline bool draken_vector_is_constant(const DrakenVector* v) {
    return v->data_length == 1;
    // (zero-sel selection plus data_length==1 is the constant signature)
}

static inline bool draken_vector_is_dict(const DrakenVector* v) {
    return v->data_length < v->length &&
           (v->flags & DRAKEN_SEL_IDENTITY) == 0;
}
```

These read the **layout**. The type tag is irrelevant to them. Use them
sparingly; the §11 default is to not branch on shape at all.

## 5. Out of scope

- The producer-surface design (E.31+). That depends on E.30c being done
  but is its own work.
- The zombie `.so` cleanup (E.34/E.35). Also depends.
- The tyre-fire fix (E.30b). Independent of this ticket; runs in
  parallel.
- Renumbering other enum values. ABI freeze stands.
- Removing `DRAKEN_NON_NATIVE` or other catch-all tags. They aren't
  shape-as-type; they're explicit fallback markers. Leave them.

## 6. STOP conditions

- You add **any** new enum value, field, or method that captures
  encoding shape. **STOP.** Read this ticket again.
- You catch yourself writing "for backward compat, leave DICTIONARY as
  a deprecated alias." **STOP.** No aliases. Delete with prejudice.
  This is the E.24 `DRAKEN_STRING → DRAKEN_VARCHAR` re-alias pattern,
  refusing to die.
- You find an on-disk serialisation format that depends on
  `DRAKEN_DICTIONARY` being a tag. **STOP and surface.** That's a
  format-versioning ticket, not "preserve the violation in the C
  enum."
- `make dt` regresses below 2792. **STOP.** Reverts that broke draken
  itself need investigation; the deletion shouldn't break native tests
  unless the tests were themselves asserting the bug.
- Caller fixes balloon past ~10 files. **STOP and surface.** That's a
  signal that the violation propagated further than expected; surface
  the surface area to the architect before continuing.

## 7. Acceptance

Run and report verbatim:

1. `grep -rn "DRAKEN_DICTIONARY\|DRAKEN_CONSTANT" --include="*.h" --include="*.cpp" --include="*.pyx" --include="*.pxi" --include="*.pxd" --include="*.py" . 2>/dev/null | grep -v __pycache__ | grep -v "/draken/docs/" | grep -v "/draken/draken_old/"`
   — should return only the `#error` sentinels in `_abi_guard.cpp` and
   any prose in design docs. No live code references. No comments
   except the "permanently retired" notes.
2. `make draken 2>&1 | tail -5` — succeeds.
3. `make dt 2>&1 | tail -3` — ≥2792 passing.
4. `python -c "from draken.draken_native import DrakenType; print(hasattr(DrakenType, 'DICTIONARY'))"` — prints `False`. (Note: this uses `hasattr` once for the verification only; CLAUDE.md §9 bans `hasattr` in production code, not in one-shot acceptance checks.)
5. `python -c "from draken.draken_native import DrakenType; print(DrakenType.DICTIONARY)"` — raises `AttributeError`. That's the correct post-state.
6. `git diff --stat HEAD` — files changed should align with §2. If the
   file count is significantly larger than expected, surface what was
   surfaced.

## 8. Reporting back

- The acceptance outputs above.
- A list of every caller file fixed (path + 1-line note on what
  branched-on-type was changed to branch-on-layout).
- The list of **prose** references fixed (docs, comments, error
  messages).
- Any patterns surfaced where the *function itself* was wrong (e.g.
  a type-mapper returning DICTIONARY) — these become follow-up tickets,
  not in-passing fixes.
- An explicit confirmation: **no new enum, no new field, no new method
  was introduced to take the place of DICTIONARY/CONSTANT.** If you
  did add one, even a small one, surface it — it might still be the
  pattern, just wearing a smaller hat.
- A note recording where you stopped if a STOP condition fired.

## 9. The lesson

This violation shipped into the FROZEN ABI under the draken-rebuild
PM's watch. The PM drafted the design doc that explicitly forbids it
("These shape labels are layout hints, not dispatch signals") and then
let it through anyway.

It was caught on day 3 of the post-rebuild work, by the architect
reading `buffers.h` and asking "what the fuck are these." Not by
review of the design corpus. Not by the ABI guard. Not by the type
tests. By a direct read.

If you are the agent executing this ticket: the lesson is not "be
careful." The lesson is **the doc was right and the code was wrong**.
When implementing against the design corpus, the corpus is the
authority. Adding enum values that contradict §11 is the same class
of failure as the E.24 fake-green `DrakenMorsel` C-verbatim struct —
"this compiles, runs, looks like progress, encodes a violation of the
architectural commitment."

We do not pass that test by being slightly more careful next time. We
pass it by **forcing the build to fail** when someone tries to do it
again. Hence §2.6's `#error` sentinels — they are not optional. They
are the only thing that closes this gap durably.
