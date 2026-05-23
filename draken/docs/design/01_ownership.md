# Draken Ownership Model (DRAFT)

> Status: DRAFT. The single biggest source of accidental complexity in the old
> code was hand-rolled ownership bookkeeping leaking into the Python/Cython layer
> (`owns_data`, `_owns_selection`, `_owns_dict_data` bints; a `wrap=True`
> construction window; `_arrow_data_buf`/`_arrow_null_buf` Python keep-alive
> objects pinning borrowed memory). C++-first removes all of it.

## Principle

**The vector owns the lifecycle of its buffers, and ownership is expressed in C++
types — not in flags carried by the binding.** A Draken vector never holds a
Python object alive to keep its data valid.

## What a vector owns

A `DrakenVector` references up to three buffers. Each is either **owned** (this
vector frees it) or **shared/borrowed** (it does not):

| Buffer            | Dense        | Constant     | Dict            |
|-------------------|--------------|--------------|-----------------|
| `data`            | owned        | owned (1 elt)| owned (uniques) |
| `selection`       | shared global| shared global| **owned** codes |
| `validity`        | owned or NULL| owned or NULL| owned or NULL   |

> ## RESOLVED — ownership of variable-length & nested auxiliaries (`06` shapes)
> The table above covers fixed-width. The two non-flat physical shapes own their
> auxiliaries as follows (all via the same RAII / `unique_ptr`+stateless-deleter
> mechanism — no new ownership concept):
>
> - **String — arena travels with `data`.** The German-string slots are `data`
>   (owned); the byte **arena** is a separate owned buffer of the **same owner**,
>   allocated and freed together with `data`, exactly like `validity`. Any transform
>   that moves/copies the slots carries the arena. Dict-encoded strings: `data` =
>   unique slots (owned), `selection` = owned codes, arena = owned — three owned
>   buffers, one owner.
> - **Array — parent owns the child; RAII chains.** `data` is `int32 offsets[length+1]`
>   (owned) plus a **child `DrakenVector` owned by the parent** (held by value or
>   `unique_ptr`, so the child's destructor runs when the parent is destroyed). The
>   child owns *its* buffers (which may themselves be dict codes / an arena / a deeper
>   child) — recursion bottoms out naturally because each level owns one level down.
>   No shared/borrowed children; no back-pointers. Freeing the top vector frees the
>   whole subtree in one RAII unwind.
>
> Implication for the allocator: a nested free is N small frees down the subtree, which
> compounds the small-block-churn pressure (see allocator note) — another reason the
> mimalloc choice has to hold up under churn (validated in Phase 2, `08`).


- **Shared globals** (`draken_identity_sel`, `draken_zero_sel`) are process-wide,
  lazy-grown, never freed. Dense/constant point at them → zero allocation, nothing
  to free.
- **Owned** buffers are freed by the C++ owner's destructor (RAII).

In C++ this is a type distinction (an owning buffer handle vs a non-owning `span`
into a shared global), so the destructor does the right thing automatically. No
`bint owns_*` needs to exist anywhere — and definitely not in the binding.

## Resolved model (architect decisions)

- **Per-vector ownership, no pooled arena.** Each vector owns its buffers and frees
  them exactly when it is destroyed. There is **no** per-morsel/per-pipeline bump
  arena coupling lifetimes. ("Per-vector arena lifetime" means *no shared arena* —
  ownership and free are at the vector granularity.)
  - Trade-off (chosen consciously): prompt release + simple, decoupled lifetimes,
    at the cost of **per-buffer alloc/free churn** vs a per-morsel arena's batch-free.
- **`unique_ptr` + custom deleter** per owned buffer; the deleter returns the block
  to the global allocator. RAII frees on vector destruction — no `owns_*` flags.
  <!-- /opus/ Keep the deleter STATELESS (an empty type) so `unique_ptr<T,Del>` stays
  one word. The moment the deleter needs to carry the size class (most slab frees
  need the size to pick the right freelist), it becomes stateful → fat pointer →
  every buffer handle grows. Decide: does the slab recover size from the pointer
  (size-segregated regions, no stored size) or must the deleter carry it? This is a
  real interaction between the allocator choice above and the buffer-handle size. -->

- **Single global allocator.** One memory source → predictable usage, less
  fragmentation than per-subsystem pools; rugo shares it. Because frees are
  per-vector (churny), this allocator must be good at **small-block churn**
  (slab / free-list) — that's where the per-vector decision concentrates pressure.

> ## RESOLVED — allocator = vendored **mimalloc** (`08` Phase 0; sign-off pending clickbench)
> - **Which:** mimalloc — small-block-churn-optimized, zero-dep-friendly, the right fit
>   for the per-vector free pattern this model deliberately chose. Locked pending
>   Phase-2 benchmark validation; fallback if it disappoints is the (rejected)
>   per-morsel arena.
> - **Concurrency (the rugo-shares-it concern):** mimalloc is thread-safe by design
>   (per-thread heaps), so "single global allocator" does **not** mean "single mutex" —
>   resolved structurally. The one pattern to watch is **cross-thread free**: vectors
>   are often allocated on a rugo decode-pool thread and freed later on an execution
>   thread, which is mimalloc's deferred-free (slower) path and is exactly the churn
>   this model maximises. The Phase-2 bench must exercise *cross-thread* alloc/free,
>   not just same-thread, or it will look rosier than production (`08`).

- **`validity` travels with `data`.** One ownership unit: same owner, freed
  together; any transform that moves/copies `data` carries `validity` with it.
  `selection` is separate — usually a non-owned shared global (identity/zero), or
  owned codes for dict.
- **Morsel is a thin, dumb container.** It groups related vectors for convenience
  and owns nothing itself; the vectors own their own memory. (Also resolves the
  Morsel question in `03_binding.md`.)
- **Nested & string ownership (resolved).** The **string arena travels with `data`**
  (same owner, freed together — like validity). For **arrays**, the parent vector
  **owns its child vector**, and RAII chains recursively — a child may itself be
  dict-encoded (owns its codes) or carry its own arena; the parent's destructor frees
  the whole subtree. (Struct/map are string-backed JSON per `06`, so no separate child
  ownership.)

### Allocator (resolved): mimalloc

**mimalloc (locked).** Fits our pattern — high-churn small-block, multi-threaded
(rugo) — and is a one-file MIT vendor. (jemalloc, DuckDB's pick, wins on long-lifetime
fragmentation control, which matters less for batch execution; not chosen.) Still
**validate under clickbench** since per-vector-free bets the allocator absorbs the
churn. **Concurrency: per-thread heaps / sharded free-lists, not a single global
mutex** — "single global allocator" means one *source*, not one lock (mimalloc's
thread-local heaps give this for free). Vendor under §4.

## How data enters Draken (no borrowing, no Python keep-alives)

Preference order (architect's rule: copies only at a true boundary, never as the
base pattern):

1. **Ownership transfer** — a producer (rugo C++, a compute kernel) `malloc`s a
   buffer, fills it, and hands ownership to the vector (the `from_decoded` shape).
   Vector frees it on destruction. No copy, no Python ref.
2. **Draken-allocates / producer-populates** — vector allocates an owned buffer
   and exposes a writable typed view for the producer to fill in place.
3. **Copy at the Python→native boundary** — when the input is a transient Python
   sequence, copy once into an owned buffer; the temporary dies. Acceptable only
   here.

The old "borrow a memoryview / numpy / arrow buffer and pin it via
`_arrow_data_buf`" pattern is **gone**. Compute operators (date_part, joins,
null_filter, fast_float) allocate owned buffers and transfer them.

## Allocator discipline

One allocator family for transferable buffers. The old code mixed `malloc`,
`PyMem_Malloc`, and `std::vector` storage, which made ownership transfer unsafe
(`free()` on `std::vector::data()` is UB; `PyMem` vs `malloc` mismatch). Pick one
(C `malloc`/`free`, or a Draken arena) and require transferable buffers to use it.
Cross-allocator hand-offs (e.g. from a `std::vector`-backed structure like
`CarcharSet`) do **one** explicit copy, documented as such.

## Open questions

- [ ] Smart-pointer strategy: `unique_ptr` with a custom deleter per buffer, a
      small owning `Buffer` struct, or an arena that owns everything for a morsel? /JJ/ `unique_ptr` with custom deleter - I think this means that a morsel is just a dumb wrapper around vectors, rather than being a meaningful construct. It's meant to just be a convenient way to group related vectors together and be very thin, very dumb.
- [ ] Do validity bitmaps get their own ownership, or always travel with `data`?  /JJ/ always travel with `data`
- [ ] Arena lifetime: per-vector, per-morsel, or per-pipeline? (affects how cheaply
      intermediate results allocate) /JJ/ per-vector
- [ ] One global allocator vs per-subsystem — and does rugo share it? /JJ/ I think a global allocator is better, as it avoids fragmentation and makes memory usage more predictable.

## Source to study (and what to delete)
Old ownership machinery to NOT reproduce: `owns_data`/`_owns_*` and `_arrow_*`
fields in `draken_old/vectors/*.pxd`; the `wrap=True` path and `free_fixed_buffer`
in `draken_old/vectors/integer64_vector.pyx`. Keep the *intent* of `from_decoded`
(ownership transfer) — that is the model, just expressed in C++ RAII.
