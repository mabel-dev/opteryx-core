# Wide-Key Identity (96/128-bit) — Design

Status: PROPOSED 2026-10-31 — awaiting architect rulings (§9). Nothing built.

## 1. Problem

Every keyed operator treats the 64-bit draken row hash as key identity — no key
bytes are compared (`native_key_hash.hpp`, `native_join2.hpp`, GROUP BY's
`CarcharIndex`, DISTINCT, COUNT(DISTINCT), GroupJoin, UNNEST, IN lists). A
collision is a **wrong answer**, not a slow probe.

Birthday bound, n distinct keys in one operator:

| n distinct | P(≥1 collision) @64 | @96 | @128 |
|---|---|---|---|
| 100M | 3e-4 | 6e-14 | 1e-23 |
| 1B | 2.7% | 6e-12 | 1e-21 |
| 2B | **10%** | 3e-11 | 6e-21 |
| 4B | 35% | 1e-10 | 2e-20 |

A join adds cross false-matches ≈ distinct_probe × distinct_build / 2^64 (≈0.2
expected at 2B × 2B). With 2B-row tables arriving, 64-bit identity is no longer
safe for the keys that can collide.

## 2. Which keys can collide — "wide keys"

| Key | 64-bit identity safe? | Why |
|---|---|---|
| Single INT8..INT64/UINT*, DATE, TIMESTAMP, FLOAT*, BOOL | **Yes — exact** | `simd_hash_i64` is `x*C+1` then `x ^ x>>32`, both bijections on 2^64 |
| Short string ≤12 B | No | `raw.lo + raw.hi*C` folds 128→64 linearly (`string_hash.h:47`) |
| Long string | No | XXH3_64 |
| DECIMAL128 out of int64 range | No | `lo ^ hi*C` linear fold (`hash.h:251`) |
| UUID, other 16-byte types | No | 128→64 |
| **Any multi-column key** | No | `simd_mix_hash` folds per-column 64-bit hashes into one 64 (`cxx_morsel_ops.cpp:1201`) |
| **Grouped COUNT(DISTINCT) pair** | No — even for int values | `gb_mix2(gid, vhash)` folds (gid, value) into 64 |

**A key is WIDE iff it is not a single ≤64-bit fixed-width column.** Narrow keys
keep today's code byte-for-byte.

## 3. Answer to the headline question

**Yes — a separate wide variant, selected at plan time, not a slower Carchar/Parvi.**
The narrow instantiation is today's code unchanged (same slot layout, same probe,
same instructions); only wide-key operators instantiate the wide variant. No
per-row branch is added anywhere — one dispatch at operator construction (§3
static-dispatch rule).

Parvi and Medius need **no new table variant** — see §5.3.

## 4. Hash production — the second word `h2`

### 4.1 Contract

For a wide key, draken produces two words per row:
- `h`  — 64-bit, exactly as today's role: bucket, partition (`gb_part`, radix
  `part_of`), tag, KMV, runtime key filter. Its value may change (no hash parity).
- `h2` — 64-bit, **statistically independent of `h`**. If h2 were a function of h
  it adds nothing; independence is the entire correctness argument and must be
  stated at the definition.

Identity = (h, h2). Narrow keys produce only `h`.

### 4.2 Per type

| Type | h | h2 |
|---|---|---|
| Long string | `XXH3_128bits(bytes).low64` → `simd_hash_i64` | `.high64` |
| Short string | (as today) | `mum(raw.lo ^ K1, raw.hi ^ K2)` folded — independent constants |
| DECIMAL128 | (as today) | `mum(lo ^ K3, hi ^ K4)` folded |
| UUID | same as DECIMAL128 pattern | |
| Multi-column | today's `simd_mix_hash` combine | independent combine: `acc2 = fold(mum(acc2 ^ K5, col_word))` where `col_word` = the column's h2 if wide, else its (bijective) h |

`mum` = 64×64→128 multiply, fold = hi ^ lo. Scalar on NEON (no 64-bit vector
multiply, per the NEON mixer finding); x86 uses `mulx`; RVV `vmulhu`+`vmul`.

XXH3_128 costs ≈ XXH3_64 on long inputs (same accumulator loop), so long strings
get h2 nearly free. Short strings / DECIMAL128 / multi-column pay one extra `mum`
per row.

### 4.3 Plumbing

- Draken: an `hash2` entry in the OpsTable for STRING/BINARY, DECIMAL128, UUID
  (filled in the SAME pass as `hash` — one function writing both buffers, not a
  second walk). `cxx_hash_c` gains a wide form returning two uint64 vectors in one
  morsel; shape-preserving (dict keys hash distinct values once, both words).
- Engine: `compute_row_hashes` / `compute_row_hashes_shaped` carry an optional
  `h2` pointer, non-null exactly when the plan marked the key wide.
- Planner: the physical-plan builder sets `wide_identity` per keyed operator from
  the bound key types (§2 rule). Python decides; native instantiates.

## 5. Tables

### 5.1 CarcharIndex (GROUP BY, DISTINCT dir, join index, merges) — packed 96-bit

Today: `Slot { uint64 hash; int64 payload; }` = 16 B, probe `HStride = 2`.

Wide: **same 16-byte slot**, `Slot { uint64 h; uint32 h2_32; uint32 id; }`.
- Identity = 96 bits (64 + top 32 of h2). P(collision) 3e-11 at 2B, 1e-10 at 4B.
- **Zero extra bytes, zero extra cache lines** — the confirm word is on the
  line the probe already loads for the payload.
- Cost: payload narrows to uint32. GROUP BY ids are per-partition dense ids
  (`next_id = hashes.size()`); at 64 partitions this caps a partition at 4.29B
  groups — fails LOUDLY (throw) past it, never wraps.

### 5.2 Probe change (all four ISA variants)

`probe_find_slot_{avx2,neon,rvv,scalar}<HStride>` gain a compile-time
`HiMask` parameter and a `key2` argument:

```
if (hashes[c*HStride] == key &&
    (HiMask == 0 || (hashes[c*HStride + 1] & HiMask) == key2))   // compiled out at HiMask==0
```

The loops already `matches &= matches - 1` and continue on a failed confirm, so a
64-bit-equal / h2-different pair correctly keeps probing and lands on its own slot
(or an empty one). No new control flow.

- CarcharIndex wide: `HStride=2, HiMask=0xFFFFFFFF00000000`, key2 = `h2_32 << 32`.
- CarcharSet wide: slot becomes `{h, h2}` 16 B, `HStride=2, HiMask=~0` — **full
  128-bit identity**, +8 B/slot for wide DISTINCT only.
- Narrow: `HiMask=0` — identical machine code to today.

`resize()` / `items()` / `drain_into` carry the second word.

### 5.3 Parvi / Medius — no table variant; verify through the group lane

These front tiers store only `h` and hand back a group id. Wide GROUP BY verifies a
front-tier hit with `P.h2[gid] == row_h2`, where `P.h2` is a new per-group lane
beside the existing `P.hashes` lane (which the radix merge already needs, §5.5).
- The ids a front tier returns are ≤ 64 / ≤ 512 per partition, so their lane
  entries are hot — the check is an L1 load + compare on the wide path only.
- On mismatch: a genuine 64-bit collision against a front-tier key. The row falls
  through to the CarcharIndex tier (which has 96-bit identity). Requires
  `find_or_insert_group` to skip the front tiers for that row — rare path,
  correctness only.

Unverified exposure if this check were omitted: ≈ 512 × D / 2^64 (5.6e-8 at
D = 2B). Small, but nonzero and unbounded in D — so the design verifies.

### 5.4 Joins — JoinCsr / RadixJoinTable / CarcharJoinIndex

Entries today: `hashes[]` + parallel `rows[]`, sorted by hash within bucket so one
key = one contiguous run.

Wide: add parallel `h2[]` (uint64 → 128-bit identity), **sort by (h, h2)** so a
run is still exactly one key. `find(h, h2, …)` matches both words; the run-end
`upper_bound` compares the pair. Partition and bucket still use `h` only.
Cost: +8 B per build entry (+50% over today's 12 B: 8 hash + 4 row) for wide
joins only. The radix build's sort key widens to 128 bits.

### 5.5 GROUP BY partitions, merge, COUNT(DISTINCT)

- `GBPartData` gains `GBSegVec<uint64_t> h2` (wide only), hinted with the others.
  Radix merge (`dst.index.find_or_insert_id(P.hashes[e], …)` sites) passes `h2`.
- Grouped COUNT(DISTINCT): the pair key becomes `(gb_mix2(gid, vhash),
  gb_mix2'(gid, vhash2-or-vhash))` with an independent second mixer — the pair is
  ALWAYS wide (gid is 32 bits of extra input), even for narrow values.
- Ungrouped COUNT(DISTINCT) (`UCDPartition`): wide iff the value is wide;
  `CarcharSet` wide variant. The existing `insert_raw128` value carriage is
  unchanged.

## 6. Unchanged, by argument

| Consumer | Why no change |
|---|---|
| Runtime key filter | Superset filter — a collision admits an extra row the join then drops |
| KMV / NDV sketches | Estimation — 64 bits ample |
| Partitioning (`gb_part`, radix `part_of`) | Uses `h` only; distribution, not identity |
| Narrow-key operators | Exact already (bijective) |
| IN-list `CarcharSet` (literals) | ≈ k × N / 2^64: k=1,000 literals × 2B rows ≈ 1e-7 — **ruling needed (§9.3)** |

## 7. Costs (to be measured, not assumed)

| Path | Extra memory | Extra work |
|---|---|---|
| Narrow anything | 0 | 0 (identical code) |
| Wide GROUP BY | +8 B/row hash buffer, +8 B/group lane | 1 `mum` (or free for long strings), 1 compare on hit |
| Wide DISTINCT | +8 B/slot | same |
| Wide join | +8 B/build entry, +8 B/probe row | 128-bit sort key on build |

## 8. Verification

- **Collision tests without a knob:** C++ unit tests drive the tables directly with
  crafted (h equal, h2 different) keys — CarcharIndex, CarcharSet, JoinCsr,
  RadixJoinTable, Parvi/Medius fall-through, merge — asserting distinct groups /
  no false join match. No production hash-truncation switch.
- `make q`, `make slt`; `make dt st rt` (native signatures change).
- Bench A/B, interleaved, per arch (Mac + x86):
  - Narrow must be noise: ClickBench int-key queries, TPC-H single-int joins.
  - Wide cost: ClickBench string GROUP BY (Q33/Q34/Q35 family), COUNT(DISTINCT)
    string, TPC-H/TPC-DS composite-key joins.

## 9. Rulings needed

1. **CarcharIndex identity width:** 96-bit packed (§5.1, zero memory, uint32 id
   cap per partition) — *recommended* — vs full 128-bit with a 24-byte slot
   (straddles lines, +50% table).
2. **Join identity width:** full 128 (+8 B/entry) — *recommended* — vs 96 with
   `h2_32` packed beside the uint32 row id (+4 B/entry).
3. **IN lists:** leave 64-bit (≈1e-7 at 1k literals × 2B rows) or move string /
   multi-column IN to the wide CarcharSet.
4. **h2 for short strings:** independent `mum` (as above), or use the raw 16
   slot bytes directly as exact identity (no hash at all, but 128-bit compare
   instead of 96 and a different code path from long strings).
5. Order of landing — suggested: draken `hash2` + plumbing → CarcharIndex/Set
   wide + probe → GROUP BY/DISTINCT → joins → COUNT(DISTINCT).
