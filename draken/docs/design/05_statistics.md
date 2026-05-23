# Draken Value Statistics / Zonemaps (DRAFT)

> Status: DRAFT. Category B from `00_data_model.md` — metadata that describes a
> vector's *contents* (not the layout of `selection`). These are powerful
> (zonemap-style skipping is the §12 "read/process less data" win) but they cost
> O(n) to compute and go stale on content changes, so they get stricter rules and
> a different home than the cheap layout bits.

## What goes here

Per-column value statistics, e.g.:
- `min_value_location` / `max_value_location` — index into `data` of the min/max
  (non-null) value. Store the **location, not the value**: type-agnostic (one
  `uint32`, no typed/union field) and it **survives selection-only reorders**
  (data unchanged ⇒ location still valid). `data[min_value_location]` recovers it.
- `null_count`
- `is_sorted` (asc/desc) — *value*-sortedness; enables binary search, merge paths,
  range short-circuit. **v1, optional** — tracked when known, but **cleared
  aggressively** on any order-affecting transform (cheapest to invalidate, costliest
  to maintain). Distinct from any *selection*-ordering layout bit (we are not adding one).
- `ndv` (distinct estimate) — **KMV sketch (default)**, built on the existing
  lightweight fast-hashing path. For COUNT(DISTINCT) and join/agg sizing. Survives
  reorders (order ⊥ cardinality) and KMV sketches **merge** on concat.
- (future) sum, simple histograms

**v1 scope (resolved):** all of the above — `min/max_value_location`, `null_count`,
`is_sorted`, `ndv` — are tracked, and **all are optional** (absent = "not tracked").
None is mandatory; none blocks an op.

## Home: an optional side-channel, NOT the hot struct

> **RESOLVED — stats live OUT-OF-BAND, keyed by column** (the `00` canonical-struct
> decision): NOT a field on the 40-byte hot struct, so the cimport ABI stays frozen
> through bring-up. Nullable here (absent = "don't know", fail-safe) — the opposite of
> the logical-type descriptor (`06`), which shares the same home but is mandatory.

<!--
/opus/ CLOSED. The Category-A/B split and the invalidation table are the clearest part
of the whole set — no changes needed there. Only the home was ambiguous; now pinned to
out-of-band per 00. -->

`DrakenVector` stays lean. Statistics hang off it (or off the morsel column) as an
**optional** structure — a nullable pointer; **absent = "not tracked"**. Reasons:
- They're opt-in (only populated when cheap — see below), so most vectors carry none.
- The set grows (min/max → null_count → ndv → …); inlining bloats the hot struct
  and forces every transform to maintain a growing field set.
- Different lifetime/ownership than the buffers.

## The "not tracked" state is first-class

Mirror the §1 cardinal rule: **absent/`NULL`/sentinel = "don't know"**, and that is
the default. A consumer that finds a stat missing computes it on demand (or skips
the optimization). A stat is present **only when known-correct**. Fail-safe (miss →
do the work), never fail-wrong (a stale stat that lies and skips real rows).

## When to populate (cost discipline)

Populate only when the producer is **already** touching the data:
- **Scan/decode** (rugo): cheapest place — min/max/null_count/sorted often fall out
  of decoding a column chunk for free.
- **An aggregate/sort that already scans**: capture as a by-product.

Never scan *purely* to fill a stat speculatively — that's work that may never pay off.

## Staleness / invalidation (the sharp edge)

| Transform | min/max location | null_count | is_sorted | ndv |
|-----------|------------------|-----------|-----------|-----|
| selection-only reorder (permutation, sort view) | **valid** (data unchanged) | valid | **clear** | **valid** (order ⊥ cardinality) |
| `take` / filter (row subset, same `data`) | clear unless re-derived | clear/recompute | clear | clear (subset ⇒ fewer distinct) |
| new `data` buffer (materialize, cast, compute) | clear | clear | clear | clear |
| append/concat | clear or merge | merge (sum) | clear | **merge** (KMV sketches merge) |

Edge cases: all-null or empty ⇒ no min/max location (sentinel). Min/max are
defined over **non-null** values.

**Invalidation ownership (resolved):** all vector construction/subsetting routes
through a small set of constructors that **clear (or carry/merge) stats by
default** — a transform must *opt in* to preserving a stat, never silently keep a
stale one. Reorders preserve `min/max location`, `null_count`, and `ndv` (order is
orthogonal to all three); only `is_sorted` is cleared. This makes the §1 fail-safe
rule structural: forget to handle a stat in a new transform → it is cleared →
recomputed on demand, never wrong.

## Granularity (resolved): per-vector at execution; chunk/relation stays separate

Execution-level stats live **on the vector**. Since a morsel column *is* a vector
and a `Morsel` is a thin wrapper, "per-vector" and "per-morsel-column" are the same
thing — there is no separate per-morsel stats home. The **scan/chunk and relation**
level (skip a whole row group before it becomes a vector — the biggest win) remains
the existing `relation_statistics` layer: complementary, not a duplicate. Keep the
two distinct; never maintain two copies of the same stat.

> Action: reconcile with `relation_statistics` (imported today by
> `draken_old/vectors/vector.pyx` — a layering smell to fix) before wiring the
> per-vector path.

## Consumers (who reads these)

- Planner / cost-based optimizer (selectivity, join sizing) — relation/chunk level.
- Filter pushdown / zonemap skipping — chunk level (biggest win).
- Execution short-circuits — per-vector (`min/max` for range predicates; `is_sorted`
  for merge/binary-search; `ndv` for COUNT(DISTINCT)).

## Open questions

- [ ] Per-vector stats struct, per-morsel-column, or both? How do they relate to
      `relation_statistics`? /JJ/ morsel columns are vectors, morsels are thin wrappers
- [ ] Which stats are v1: just `min/max_value_location` + `null_count`, or include
      `is_sorted` / `ndv` from the start? /JJ/ min/max loc, null_count, sorted, ndv all tracked, all optional
- [ ] Location (`uint32` into `data`) vs storing the typed value — confirm location
      (type-agnostic, reorder-survivable) is the right call for all types incl. strings. /JJ/ location is type-agnostic, reorder-survivable
- [ ] Who owns invalidation — is it enforced by routing all subsetting through a
      small set of constructors that clear stats by default? /JJ/ invalidation is scoped and enforced by the small set of constructors (ordering doesn't change the ndv)
- [ ] Sketch type for `ndv` (KMV vs HLL) — reuse whatever the existing NDV path uses. /JJ/ KMV is default, we've put work in to lightnight fast hashing
