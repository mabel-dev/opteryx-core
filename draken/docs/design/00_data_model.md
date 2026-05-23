# Draken Data Model (DRAFT)

> Status: DRAFT for review/edit. Anchors the rebuild. The model below is carried
> forward (largely unchanged) from the current draken — it is the *proven* part.
> The orchestration around it is what we are rebuilding C++-first.

<!--
/opus/ FINAL-REVIEW SUMMARY (this doc)
Strengths: the unified vector + "shape is a layout hint, not a correctness switch"
contract is the strongest idea in the whole set, and the fail-safe "don't know is the
default" rule is exactly right. No notes on the core model — it's proven.
-->

> ## RESOLVED — Canonical struct (this section is the single definition)
> `DrakenVector` is **exactly the struct above: 40 bytes on LP64**, with the `flags`
> byte living in the existing tail padding (no size change vs `draken_old`). This is
> the canonical layout; `buffers.h` is the source of truth (CLAUDE.md §11) and a
> Phase-0 `static_assert(sizeof(DrakenVector)==40)` + per-field offset asserts + a
> pinned `DrakenType` underlying-type/values assert guard it in **both** new and
> `draken_old` (see `08`, risk #1).
>
> **Logical-type descriptor (`06`) and value statistics (`05`) are NOT fields on this
> struct.** They are **out-of-band, keyed by column** (owned by the morsel/scan layer),
> for two reasons: (1) it keeps the hot struct lean and cache-friendly; (2) it keeps
> the struct ABI **frozen** through the whole per-type bring-up, so the ~99 compiled
> cimport consumers and mixed old/new morsels stay byte-compatible. Adding either as a
> struct field is forbidden for the duration of the rebuild.


## Purpose

One columnar vector representation, read the same way regardless of how the data
is physically laid out. Operators never branch on "encoding" for correctness.

## Resolved: canonical struct & where metadata lives (ABI-defining)

This struct is **canonical and frozen for the rebuild**; `buffers.h` is its single
source of truth (CLAUDE.md §11). The **only** change vs `draken_old`'s struct is the
`flags` byte, which lands in existing tail padding → `sizeof(DrakenVector) == 40`
holds.

**Logical-type and statistics do NOT live on this struct.** They are **out-of-band**,
associated with a vector/column by identity — *not* a pointer field here. Why this is
non-negotiable:
- keeps the hot struct at 40 bytes and access pointer-cheap;
- keeps the **C ABI frozen** — ~99 compiled opteryx/rugo modules `cimport
  draken.core.buffers` and read this layout directly at compile time, and a mixed
  new/`draken_old` morsel (`04`) is only safe if both share a **byte-identical**
  struct. A stats/logical-type pointer added here would fork the ABI mid-migration
  and turn the "mixed morsel" into a compiled-code segfault.

Therefore: `flags` is the sole addition; logical type (`06`) and stats (`05`) are
out-of-band, keyed by column. (`03` covers how compiled consumers bind this struct.)

## The unified vector

```c
typedef struct {
    void*             data;        // typed payload (cast at the typed layer)
    const uint32_t*   selection;   // never NULL; indices into data
    uint32_t          data_length; // number of unique/physical values in data
    uint32_t          length;      // logical row count
    uint8_t*          validity;    // 1-bit-per-logical-row null mask; NULL = all valid
    DrakenType        type;        // element type tag — single dispatch key
    uint8_t           flags;       // Category-A layout hints (below); 0 = "don't know"
} DrakenVector;

// Category-A layout hint bits (see "Metadata & hints"). Default 0 = conservative.
#define DRAKEN_SEL_IDENTITY     (1u << 0)  // selection[i] == i  (true dense)
#define DRAKEN_SEL_PERMUTATION  (1u << 1)  // bijection, data_length == length
// bits 2..7 reserved for future layout hints
```

**Access is always `data[selection[i]]` for `i in [0, length)`.** Codes in
`selection` are `uint32_t`.

This access model assumes a **fixed-width** element. What `data` physically *is* per
type — fixed-width arrays, German-string slots + arena, array offsets + child vector,
bit-packed bool — and where **parameterized** metadata lives (decimal precision/scale,
timestamp unit, vector dimension, none of which fit the bare `DrakenType` tag) is
defined in **`06_value_encoding.md`**.

`flags` is **free**: on LP64 the struct is 40 bytes with 4 bytes of tail padding
after the 4-byte `type` enum, so a `uint8_t` lands in that padding —
`sizeof(DrakenVector)` is unchanged. Semantics and the fail-safe default rule are
in "Metadata & hints" below.

<!--
/opus/ The byte arithmetic is correct *as written here* (data 8 + selection 8 +
data_length 4 + length 4 + validity 8 + type 4 + flags 1 → 37, pad to 40, flags lands
free in tail padding). But this is only true if NOTHING else is added to the struct.
05 (stats) and 06 (logical type) both push fields toward the vector. If a logical-type
pointer + a stats pointer both land here you're at 56 bytes and the "free flags / 40
unchanged" framing is misleading. Resolve the canonical struct (see top summary) and
then this paragraph is either true or needs rewriting. Recommend logical-type and
stats live OUT of the hot struct (descriptor + side-channel keyed by column), so this
40-byte claim survives — that also keeps the cimport ABI stable for opteryx.
-->


### Three physical shapes — one structure

`selection` is **never NULL**. The shapes differ only in *what it points at*:

| Shape    | `selection` points at            | `data_length` |
|----------|----------------------------------|---------------|
| Dense    | global identity permutation      | `== length`   |
| Constant | global zero vector               | `== 1`        |
| Dict     | owned per-vector uint32 codes    | `< length`    |

Dict is the general form; dense and constant are degenerate dictionaries
(identity codes / all-zero codes). This is why one access pattern covers all three.

### RLE is not a vector shape

Run-length encoding does **not** fit `data[selection[i]]` and is **never** carried
into the engine. The scan layer (rugo) expands RLE into one of the three shapes
above before handing data to execution. No `run_lengths` field exists on
`DrakenVector`.

## Validity (nulls)

- `validity` is a 1-bit-per-**logical-row** mask, Arrow convention (bit set = valid).
- `NULL` means all rows valid (the common case; no allocation).
- An all-null constant uses a full, SIMD-padded zero bitmap (the
  `draken_zero_validity(length)` pattern), **not** a 1-byte sentinel — kernels read
  validity in whole bytes, so it must be a real length-sized buffer.
  (This was a live bug in the old code; the new model bakes the invariant in.)
- **Resolved (constant validity):** use the shared all-zero bitmap; **no dedicated
  "constant is null" flag.** Same principle as the selection array — we don't add a
  shortcut for the identity selection, so we don't add one for a constant's all-zero
  selection or its validity either. Shared buffer, no special case.
- **"All valid" is encoded by `validity == NULL`** — there is no separate
  `ALL_VALID` flag (it would be redundant). **Normalization invariant:** when an op
  produces a result with no nulls, it sets `validity = NULL` rather than allocating
  an all-ones bitmap. So a *present* bitmap always means "nulls may exist," and the
  cheap all-valid check is just `validity == NULL` — no scan, no flag.

## Types

`DrakenType` enum tags the element type (int8/16/32/64, float32/64, bool, date32,
time, timestamp, interval, decimal, string, array, vector(fp16), null). The type
tag is the single dispatch key (see `02_dispatch_and_ops.md`) — there is no
per-type class hierarchy.

`DrakenType` is the **physical** type (storage layout + dispatch). The **logical**
type — the SQL type with its parameters (decimal precision/scale, timestamp unit/tz,
vector dimension) — is carried separately at the vector level, mandatory for
parameterized types; one physical type backs many logical types. See
`06_value_encoding.md`.

ABI note: `length`/`data_length` are `uint32_t` — a deliberate ≤ 4B-rows-per-vector
cap. Indexing/loop counters in code use `Py_ssize_t` (size-typed) and cast at the
boundary; element *values* use fixed-width types (`int64_t`, `uint64_t`, …).

## §11, restated: shape is a layout hint, not a correctness switch

The uniform `data[selection[i]]` access is the **correctness** contract — every
operator must produce identical results for all three shapes.

**Performance-only fast paths are permitted** (this is the post-convergence
relaxation): an operator may detect `data_length == 1` (constant → compute once,
broadcast) or `selection` is identity (dense → contiguous SIMD, no gather) *purely
to go faster*, provided the result is identical to the uniform path. What is
**forbidden** is letting a shape change the *answer*, or hand-specializing in ways
that silently skip rows (the old "ptr.data == NULL discriminant" class of bug).

Dict shape gets contiguous-SIMD by gathering `data[selection[i]]` into scratch,
then running the same kernel — no separate dict code path that can drift.

## Metadata & hints

Two distinct categories of metadata, with **different rules**. Keeping them apart
is what stops "helpful metadata" from becoming "metadata that lies."

### Category A — layout hints (inline, cheap, on the struct)

Describe the *shape of `selection`*. One bit each, packed into a `uint8 flags`
field (room to grow). They never change a result — they only let an op skip work.

- **`SELECTION_IS_IDENTITY`** — `selection[i] == i`, i.e. true dense. Lets ops read
  `data[i]` directly and skip the `selection` stream entirely (big for narrow
  types). **Required for *correct* dense detection**: `data_length == length` is
  *not* sufficient — a permutation also has `data_length == length` — so identity
  cannot be cheaply inferred without this bit (the alternative is an O(n) scan).
- **`SELECTION_IS_PERMUTATION`** — `data_length == length` and `selection` is a
  bijection (every index 0..length-1 exactly once, reordered). Lets
  **order-insensitive whole-column ops** (sum/min/max/count/count-distinct) read
  `data` contiguously and ignore `selection`. Does **not** help positional ops
  (compare/hash) — they still need `selection[i]`.

<!--
/opus/ The permutation fast-path quietly assumes `data` holds *exactly* `length`
live elements and the bijection covers all of them. Spell out the invariant the
maintenance code must guarantee: `data` capacity >= `length` AND every index in
[0,length) is referenced. If `data` is ever over-allocated (capacity > length) or a
permutation is set on a vector whose `data` was sized to something other than `length`,
reading data[0..length) contiguously is wrong. Cheap to state, easy to violate during
take/sort. Worth one sentence in the maintenance contract.
-->


Containment invariant the maintenance code must uphold:
`IDENTITY ⟹ PERMUTATION ⟹ data_length == length`. (Never set identity without
permutation.) Dict (`data_length < length`) and constant (`data_length == 1`,
`length > 1`) are neither.

Explicitly **not** Category-A layout bits (resolved):
- **all-valid** — already encoded by `validity == NULL` (see Validity, above); no flag.
- **sortedness** — *value*-sortedness, a content statistic (Category B,
  `05_statistics.md`): tracked there as an **optional** stat (cleared aggressively on
  any order-affecting transform), never a layout bit.

### Category B — value statistics (NOT inline — see `05_statistics.md`)

min/max location, null_count, is_sorted, ndv, … describe *contents*, cost O(n) to
compute, and must be invalidated on any content/subset change. They live in an
**optional side-channel**, not the hot struct. Deferred to `05_statistics.md`.

### The cardinal rule (applies to every hint and stat)

Each hint has an explicit **"don't know"** state; that state is the **default** and
the value any uncertain transform must fall back to. A hint may assert a fast-fact
**only when certain**. Result: the scheme is **fail-safe** (a missed update loses a
fast-path — slower but correct), never **fail-wrong** (a hint that lies). For the
Category A bits, "don't know" = `0`/false (→ uniform `data[selection[i]]` path).
This is the §1 "never lie about state" principle made structural.

## Open questions (for the architect)

- [ ] Do we keep `data_length`/`length` at `uint32_t`, or widen to 64-bit now?  /JJ/ keep to uint32
- [ ] Is `DrakenType` frozen, or do we add e.g. fixed-size decimal128 as first-class? /JJ/ we can add new types if it makes sense
- [ ] Constant validity: standardize on the shared all-zero bitmap, or model
      "constant is null" as a single flag on the vector (cheaper, but a special case)?
      <!-- /opus/ RESOLVED (see Validity section above, lines ~129-132 + /JJ/): shared
      all-zero validity bitmap, no "constant is null" flag. "No data/no validity" is
      reserved exclusively for the NULL *type* (06); a typed all-null column still
      carries the zero bitmap because its type says values exist. Consistent with the
      shared-selection principle — no per-shape shortcut. CLOSED. --> /JJ/ all-zero selection array for constants

- [ ] Should `selection` be `uint32_t` forever, or allow a wider code type for
      >4B dict rows (interacts with the length cap)? /JJ/ 4B limit is sensible (it needs to fit in memory)
- [ ] Layout hints: ship `SELECTION_IS_IDENTITY` + `SELECTION_IS_PERMUTATION` in a
      `uint8 flags` now? Which transforms are responsible for setting/clearing them
      (scan, take, sort, gather, materialize)? /JJ/ yes
- [ ] Any other Category-A layout bits worth a slot (e.g. `VALIDITY_ALL_VALID`
      cached, `SELECTION_IS_SORTED`)? /JJ/ I think 'sorted' is the same kind of thing as the min/max tracking, not for this level, maybe elsewhere, we'd need to track sorted-ness too, which starts to add a lot of noise. ALL_VALID is encoded into the validity bitmap being `NULL` or do I have that backwards?

      <!-- /opus/ You have it right, not backwards. `validity == NULL` ⇒ all rows
      valid (no nulls, no allocation). A *present* bitmap ⇒ "nulls may exist." So the
      cheap all-valid test is `validity == NULL`, and the normalization invariant
      (lines 76-77) says any op that produces zero nulls must SET validity back to NULL
      rather than leaving an all-ones bitmap behind — otherwise the cheap test silently
      stops working and you scan. No separate ALL_VALID flag needed; adding one would
      be redundant state that can drift out of sync. Agree with dropping it. -->


## Source to port from
`draken_old/src/core/buffers.h`, `draken_old/src/core/vector_alloc.h`
(`draken_identity_sel`, `draken_zero_sel`, `draken_zero_validity`).
