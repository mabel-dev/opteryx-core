# Mabel — Hash Container Library

Hash containers used by the Opteryx query engine. All containers operate on
`uint64_t` keys (pre-hashed by the engine) and are designed for batch columnar
workloads, not general-purpose use.

Six concrete types across three families:

| Type | Header | Keys | Values | Capacity | Status |
|------|--------|------|--------|----------|--------|
| `CarcharIndex` | `carchar/carchar_index.hpp` | uint64 | int64 payload | Dynamic | Active |
| `CarcharSet` | `carchar/carchar_set.hpp` | uint64 | — | Dynamic | Active |
| `CarcharJoinIndex` | `carchar/carchar_join_index.hpp` | uint64 | row list | Dynamic | Active |
| `ParviMap` | `parvi/parvi.hpp` | uint64 | int64 payload | 16 (fixed) | Active |
| `ParviSet` | `parvi/parvi.hpp` | uint64 | — | 16 (fixed) | Active |
| `PerfectHashSet` | `perfect_hash/perfect_hash_set.hpp` | integer | — | Range (fixed) | Active |
| `PerfectHashMap` | `perfect_hash/perfect_hash_map.hpp` | integer | int64 payload | Range (fixed) | **Unwired** |

`PerfectHashMap` is fully implemented (hpp, pxd, pyx, compiled .so) but has no
active callers. It was wired into the GROUP BY aggregation engine and then
removed after benchmarking showed Carchar outperforming it at realistic
cardinalities — see the PerfectHashMap section below for the reason.

---

## Carchar

Open-addressing hash table. Uses a Swiss Table–style control byte array: each
slot has a 1-byte control value that is either `0x80` (empty) or the top 7 bits
of the key (`key >> 57`). A SIMD compare against the tag filters candidates
before the full 64-bit key check, so most probes cost one vector compare and
one integer compare.

Probing walks the control array in groups of 16 bytes (one SIMD register), and
the instruction used is selected at runtime via an atomic function pointer that
is resolved once on first call (`select_dispatch`). This means one binary serves
AVX2, NEON, RVV, and scalar targets without recompilation.

Capacity is always a power of two. Modulo becomes a bitwise AND. Growth doubles
from the initial capacity at the configured load factor (default 0.70 for the
aggregation index, 0.80 for sets).

**`CarcharIndex`** — maps a hash key to an `int64_t` payload (a group slot id
in the aggregation engine, or an arbitrary reference). The aggregation engine
uses a software-prefetch pattern: it issues `prefetch(key[i + N])` while
processing row `i`, converting a latency-bound sequential probe into a
memory-level-parallel one. This helps ~10% on high-cardinality (all-miss)
workloads; negligible overhead when the table is cache-resident.

**`CarcharSet`** — same structure without the payload array. Used for DISTINCT
and semi-join filtering.

**`CarcharJoinIndex`** — wraps `CarcharIndex` and adds per-key row lists
(one key can match multiple build-side rows). Carries a 256-slot persistent
probe cache across morsel boundaries so repeated probes of the same key in a
probe-heavy join pay zero hash cost after the first hit.

### Excels at
- Unbounded cardinality: grows gracefully to any number of groups.
- Mixed workloads: one table for both low and high cardinality.
- High-cardinality GROUP BY where the working set exceeds L2.
- Join builds where the build side has many distinct keys.

### Performs poorly at
- Very low cardinality: even a 16-slot table allocates on the heap.
- Repeated rehashing: high-cardinality inserts pay multiple doubling passes
  before the table stabilises. The initial capacity hint (`reserve()`) avoids
  this when the NDV estimate is good.

### Design notes
- The tag byte reuses the same bit position as Parvi (top 7 bits) so
  `drain_into` can copy Parvi entries into Carchar without recomputing any tags.
- `kEmpty = 0x80` (high bit set) never collides with a valid tag (tags use bits
  0–6 of the tag byte, i.e. `(key >> 57) & 0x7F`), so empty-slot detection is a
  single bit test.
- Two probe variants are compiled: `probe_find_slot` (linear, wrapping) and
  `probe_find_bucket` (bucket-aligned). The aggregation engine uses the slot
  variant; the join engine uses buckets to keep related rows spatially close.

---

## Parvi

Fixed-capacity inline map/set for at most 16 entries. The entire table is one
16-byte control group — every lookup is exactly one SIMD compare with no loop,
no branch on probe length, and no heap allocation. All storage (`control_`,
`hashes_`, `payload_refs_`) is inline in the object.

The control array is `alignas(64)` to land on a cache line boundary, avoiding
false sharing when multiple Parvi instances coexist (e.g. in worker threads).

When the 16-slot limit is exceeded, `drain_into(CarcharIndex&)` copies the live
entries into a Carchar using the same key/tag scheme, so the promoted map needs
no rehash of the control bytes.

`ParviMap` and `ParviSet` share the same control/probe logic. `ParviSet` omits
the `payload_refs_` array, saving 128 bytes.

The scalar fallback uses a SWAR (SIMD Within A Register) trick to gather the
match bits from 16 bytes into a 16-bit integer without a loop:

```
mask = ((bits & 0x8080808080808080) * 0x8040201008040201) >> 56
```

This multiplies the isolated high bits of 8 bytes so they accumulate into the
top byte, then shifts down. Two such operations (low 8 bytes, high 8 bytes)
produce the full 16-bit match mask.

### Excels at
- Very low cardinality GROUP BY (status codes, booleans, small enums ≤ 16
  distinct values). Zero heap allocation, single cache line working set.
- Queries where the planner can prove the group count is ≤ 16.
- As a warm-up stage before Carchar: if the estimate was correct, no heap
  allocation ever happens; if wrong, drain and continue.

### Performs poorly at
- Any workload that regularly overflows: every overflow pays a `drain_into`
  copy. If the cardinality estimate is consistently wrong, the promotion cost
  outweighs the Parvi benefit. The aggregation engine tracks promotion telemetry.
- Multi-threaded access: no locking. Each engine instance owns one Parvi.

### Design notes
- API is intentionally compatible with `CarcharIndex`/`CarcharSet` for the hot
  path (`lookup_fast`, `insert_new`, `drain_into`) so the engine can swap
  implementations in a single pointer assignment.
- `insert_new` returns `{kCapacity, false}` on overflow (slot == capacity is the
  signal) rather than throwing or silently dropping entries.
- The `alignas(64)` on the control array is a mild over-optimisation for the
  current single-threaded engine, but costs nothing and makes the concurrent
  case safe by default.

---

## PerfectHashSet and PerfectHashMap

Direct-addressed structures for bounded integer keys. The key is mapped to a
slot by `slot = key - min_val` — no hash function, no modulo, no collision
resolution. Lookup and insert are:

```cpp
word = words_[slot >> 6];
mask = 1ULL << (slot & 63);
is_new = !(word & mask);
word |= mask;
```

**`PerfectHashSet`** — bitmap only. Memory cost is `ceil(range / 64) * 8` bytes.

**`PerfectHashMap`** — bitmap plus an `int64_t` payload per slot. Memory cost is
`range * 8` bytes for the payload array plus the bitmap. The payload array must
be allocated at full range size upfront, even if only a small fraction of slots
are ever used.

### Memory footprint by range

| Range | PerfectHashSet | PerfectHashMap |
|-------|----------------|----------------|
| 256 (INT8) | 32 B (1 cache line) | ~2 KB |
| 65 536 (INT16) | 8 KB (L1) | ~512 KB |
| 131 072 (e.g. RegionID) | 16 KB (L1) | ~1 MB (L3) |
| 1 048 576 | 128 KB (L2/L3) | ~8 MB |

This asymmetry is the dominant design consideration. `PerfectHashSet` scales
well because the bitmap is 64× smaller than the payload array. `PerfectHashMap`
at large ranges competes with Carchar's incremental growth, which starts at 32 KB
and only reaches the same size once the cardinality actually warrants it.
Benchmarking on the ClickBench dataset (RegionID, 131K range) showed Carchar
outperforming PerfectHashMap by ~7% for GROUP BY aggregation. The set case was
not benchmarked directly but is expected to hold the advantage given the 16 KB
bitmap fits in L1.

The batch `find_or_insert_32_i{8,16,32,64}` and `probe_{found,not_found}_32_i{8,16,32,64}`
variants take a raw pointer and length rather than a DrakenVector, so they can be
called from a `nogil` Cython block without touching the Python runtime.

Bounds checking varies by width: `i8` and `i16` variants omit it (the type
guarantees the range), `i32` and `i64` variants check and either skip or abort
depending on the call site's semantics.

### Excels at
- DISTINCT filtering on narrow integer columns (INT8, INT16) where the bitmap
  fits in L1. Wired into `DistinctNode` for these cases.
- Semi-join probes against a small known integer domain (e.g. IN-list with
  a bounded integer set compiled at bind time).
- Any probe-heavy path where hash computation is measurably expensive and the
  key domain is genuinely bounded.

### Performs poorly at
- Large ranges with a payload array (`PerfectHashMap`): the `range * 8` byte
  allocation is cold memory. Carchar's incremental growth wins for ranges that
  push the payload out of L2.
- Unknown or unbounded key domains: `min_val`/`max_val` must be known before
  the first row is processed. This requires either type guarantees (INT8/INT16)
  or planner-supplied column statistics.
- Sparse domains: a column with values 0 and 1 000 000 allocates 8 MB for two
  entries. Range check must precede activation.

### Design notes
- The set and map share the same bit-slot arithmetic. The map adds a second
  vector (`payloads_`) aligned with the bit array. This makes the map's hot path
  two independent memory accesses (bitmap word + payload slot) rather than one.
- Copy constructor and assignment are deleted. These structures are constructed
  once with known bounds and never moved.
- `PerfectHashSet` is used as the DISTINCT accelerator. `PerfectHashMap` was
  evaluated for GROUP BY but removed when benchmarking showed no gain over
  Carchar at realistic ClickBench cardinalities.

---

## Selection logic (engine-side)

The aggregation engine (`GroupHashEngine`) selects at plan time via
`HashMapVariantStrategy`:

```
Parvi  ← NDV product ≤ 16, or total input rows ≤ 16
Carchar ← everything else
```

`DistinctNode` selects independently at first-morsel time:

```
PerfectHashSet ← single non-null INT8 or INT16 DISTINCT key (dense encoding only)
Parvi          ← set_variant == "parvi" (planner hint, NDV ≤ 16)
CarcharSet     ← everything else
```

Parvi promotes to Carchar mid-morsel on overflow. PerfectHashSet has no
overflow concept — it covers the full type range by construction.
