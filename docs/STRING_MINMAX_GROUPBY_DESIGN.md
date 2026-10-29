# String MIN/MAX in GROUP BY — Design Proposal

Status: **BUILT and LANDED in main (uncommitted).** 2026-10-29. `make compile`, `q`, `slt`, `dt`, `st` and `rt` pass in main.

- **Parts:** P1 and P4 are kept. The P3 adopt variant replaced copy. P2 was deleted by measurement.
- **Results:** §9.
- **x86:** not yet measured.

Architect decisions (§8): build as proposed, P2 approved, copy merge as the default with adopt measured.

## 1. Problem

The ClickBench REGEXP_REPLACE query (`/* 28 */`) is:

```sql
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k,
       AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer)
FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000
ORDER BY l DESC LIMIT 25
```

The profile was taken on the Mac (samply, 20 warm runs, bench tree with the 2026-10-27 extractor fix). `MIN(Referer)` takes about **48% of busy CPU**:

| Where | Share of busy CPU |
|---|---|
| `pass_c_spec`, MinMaxStr arm: the compare `sv < string_view(cur)` | 18.0% |
| `pass_c_spec`, MinMaxStr arm: `cur.assign(p, len)` (malloc + copy) | 5.7% |
| `pass_c_spec`, MinMaxStr arm: other lines | ~3% |
| `combine_into`, MinMaxStr arm: `std::string` compare and copy | 11.6% |
| `~GBLanes`: freeing the per-group `std::string`s | 10.2% |

The extractor itself is about 7% of busy CPU.

Data facts (`scratch.hits_rugo_262k`):

- 81.0M non-empty Referer rows.
- 19.7M distinct Referer values.
- 3.0M groups.
- The average stored minimum is **161 bytes** long.
- In an earlier probe, 90% of Referer rows arrived dictionary-encoded, about 3.7 rows per distinct value in a batch.

Two ClickBench queries use `MIN(URL)` / `MIN(Title)`, but only 2,056 and 14,140 rows reach their sinks (docs/GROUPBY_DICT_CODE_AGGREGATES_ASSESSMENT.md §3). This proposal does nothing for them.

## 2. How it works today

- **Storage:** `GBLanes::sval` is a `std::vector<std::string>`, one extreme per group. It is resized every batch alongside the segmented lanes, but it is not one of them.
- **Update** (`pass_c_spec`, MinMaxStr arm). For every valid row:
  - read the slot and its bytes;
  - compare them with the group's `std::string`, which is a heap object at a random address;
  - on a win, `assign`, which may free and malloc.
- **Merge** (`combine_into`, MinMaxStr arm): each worker table is folded into the partition's merged table by `std::string` compare and copy.
- **Emit** (`emit_string_lane_column`): one canonical string block is built from the `std::string`s.
- **Teardown:** every `std::string` is freed one by one.

Each of these steps pays per value:

- **malloc/free:** at least once per stored extreme, and again on every win that grows the string.
- **Cold reads:** the extreme's bytes are on the heap, so a compare touches a cold line.
- **Compares:** one full compare per row, even when the row's value was already folded into the same group earlier in the same batch.

## 3. Proposal overview

There are four parts, and each can be measured on its own. Per the ablation rule, build all four, then measure leave-one-out.

| Part | Removes | Needs approval |
|---|---|---|
| P1 Record lane + group-by byte arena | malloc/free per extreme, per-string teardown, `std::vector<std::string>` resize | no — storage only |
| P2 Dictionary fold memo | repeated compares of a (group, value) pair already folded this batch | **yes — §11 shape-specialised path** |
| P3 Merge copies winners into the destination arena | `std::string` copy in `combine_into` | default chosen below; the variant is a decision |
| P4 Prefix-first compare | touching the extreme's bytes when the first 8 bytes decide | no |

## 4. P1 — extreme record lane over a group-by byte arena

Replace `std::vector<std::string> sval` with a fixed-width segmented lane plus one byte arena per `GBLanes`:

```cpp
struct GBStrExt {                 // 24 bytes, trivially copyable
    uint32_t len;                 // bytes in the extreme
    uint32_t cap;                 // bytes reserved at ptr in THIS lanes' arena; 0 = inline
    union {
        uint8_t inl[16];          // len <= 16: the bytes themselves
        struct {
            uint8_t        prefix[8];   // first 8 bytes (P4)
            const uint8_t* ptr;         // GBArena bytes; chunks never move
        } ext;
    };
};
GBSegVec<GBStrExt> sext;          // replaces sval
GBArena            sbytes;        // long extremes' bytes
```

- **Stable pointers:** a `GBArena` never moves a chunk once allocated, so `ptr` stays valid for the arena's lifetime. There is no offset to resolve and no arena id to carry.
- **Overwrite in place:** a new long winner with `len <= cap` is copied over the old bytes.
- **Reallocation:** otherwise, a new range of `max(len, 2*cap)` bytes is taken from the arena. That needs a small `GBArena::alloc(n)` (reserve without copying) next to `put`.
  - The old range becomes garbage.
  - Doubling bounds a group's garbage to less than its final `cap`, so worst case the arena is about 3× the live bytes and typically about 1×.
  - `std::string` reaches the same bound by freeing; this design pays it in held bytes instead.
- **Teardown:** the record lane is trivially destructible, and the arena releases whole chunks back to `GBBlockPool`. The 10.2% `~GBLanes` cost becomes chunk releases.
- **`gb_lanes_swap` (top-k cut):** swaps 24-byte records. The bytes don't move, because `ptr` is absolute.
- **`gb_lanes_resize` / `hint`:** the new lane joins the segmented lanes, so its growth never copies or reallocates (the R1 geometry).
- **Emit:** `emit_string_lane_column` reads records instead of `std::string`s. The output block is still built and copied, as today.
  - The ungrouped sink also calls this emit, holding one `std::string` per spec (`UngroupedAggGlobal::strs`).
  - Either the ungrouped sink moves to one record plus a small arena, or emit takes a (bytes, len) accessor that serves both. **Proposed: the accessor.** The ungrouped path is one value per worker and not hot, so it stays as it is.

## 5. P2 — dictionary fold memo (compressed operand)

When the operand is compressed (`draken_is_compressed(&v)`), rows with the same code carry byte-identical values. Folding a value into a group whose extreme already reflects it cannot change the extreme. So:

```cpp
// per GroupByLocal, grown to the largest data_length seen; never cleared
std::vector<uint64_t> str_memo;   // per code: (epoch, partition, entry) of the last fold
// per row, compressed operand only:
const uint32_t c   = v.selection[i];
const uint64_t tag = (epoch << 38) | (part << 32) | e;   // part < 64, e < 2^32
if (str_memo[c] == tag) continue;   // this value is already folded into this group
str_memo[c] = tag;
fold(e, value(c));                  // compare / assign as P1
```

- **Correct for any code-to-group mapping.** The memo is a cache of "this value went into this group". A code that lands in several groups just misses. A skip happens only on an exact repeat of (batch, group, code).
- **No per-batch reset.** The epoch is bumped once per batch, so there is no O(data_length) clear. That matters because a lazily filtered column can carry `data_length > length`. 26 epoch bits allow 67M batches; on wrap, clear the memo once.
- **`valid`:** this lane's `valid` becomes "folds" rather than "rows".
  - For MinMaxStr it is read only as a `> 0` gate, in `emit_string_lane_column` and in the merge gate and sum.
  - The ungrouped sink's compressed path already relies on exactly this ("c.valid becomes a count of uniques"). I will re-check every reader before building.
- **Precedent:** the ungrouped sink already reduces string MIN/MAX over referenced uniques (`§11 compressed-shape twin of agg2_update_str`), and the 2026-10-18 compression-aware directive covers kernels.
  - This is the grouped version of that, so §11 says stop and ask, hence decision 1 in §8.
  - The uniform path stays the correctness contract, and the memo never changes an answer.
- **Why this is not the rejected per-code pre-aggregation** (GROUPBY_DICT_CODE_AGGREGATES_ASSESSMENT.md §7.2).
  - That assessment covered dictionary-coded GROUP BY **keys** at low group counts, where lane writes were already cache-resident and the ceiling was 1–3% of wall.
  - Here the **operand** is the dictionary, there are 3.0M groups, and each skipped compare is a 161-byte compare against a cold extreme.
  - The compare alone is 18% of CPU.
- **Expected:** for cb28's dictionary rows, compares fall from one per row to one per distinct (group, value) pair per batch, up to 3.7× fewer. This is an estimate from the earlier probe and will be measured.
  - The memo costs one dependent load per row into an array the size of the dictionary.
  - It could lose on operands with few repeats; that is measured in §7, not assumed.

## 6. P3 — merge, and P4 — prefix-first compare

### P3: merge

- **Default: copy.** `combine_into` copies the winning bytes into the destination lane's arena, overwriting in place when the bytes fit. The source table's arena is released when the source is dropped (`list[i] = GBPartData()`), as today. Peak memory stays where it is now.
- **Variant: adopt.** The destination record points straight at the source's bytes, and the source arenas are kept alive until the partition is emitted. This is the R5 pattern the key columns already use.
  - It removes the copy entirely.
  - Peak memory then holds every worker's extremes for the partition until emit. With high-cardinality keys and 16 workers that can be many times the live set.
- **Proposed default: copy.** Adopt is built as a variant arm and kept only if the merge still shows in the profile and peak RSS stays acceptable (decision 2).

### P4: prefix-first compare

- The record keeps the first 8 bytes of a long extreme inline. Compare those against the incoming value's first 8 bytes first. Only on a tie compare the full bytes at `ptr`; only there is the extreme's cold memory touched.
- **Honest expectation for cb28:** close to nothing. Values in one group share the host prefix ("http://y…"), so 8 bytes almost never decide. Other string MIN/MAX shapes may gain.
- It costs 8 bytes per record, which are already in the 24.
- **Kept only if a measured query gains.** Otherwise it is deleted (the slower-is-ditched rule), and the record keeps `prefix` only as part of `inl`.

## 7. Validation and measurement

Correctness: a differential against main.

- **Operand types:** VARCHAR, VARBINARY, NVARCHAR (UTF-8 byte order is codepoint order).
- **Values:** empty strings; values of exactly 16 and 17 bytes (the inline boundary); long values.
- **NULLs:** NULL operands, and all-NULL groups (must emit NULL).
- **Operand shape:** dense, dictionary, constant, and lazily filtered dictionary (`data_length > length`).
- **One code in several groups:** for the P2 memo misses.
- **Plans:**
  - the direct-array path;
  - hash partitions with one and with many batches;
  - the radix-bucket merge;
  - the top-k cut (`gb_lanes_swap`).

Suites:

- `make q`, `make slt`, `make dt`, `make st` and `make rt`, since this touches native structures.
- New slt regressions for the boundary cases above.

Performance:

- **Mac, interleaved A/B, alternating order:** build all four parts, then leave one out at a time.
- **Arms:**
  - cb28;
  - a dense-operand string MIN on hits, e.g. `GROUP BY RegionID, MIN(URL)` read with the dictionary disabled — to be chosen so the operand really is dense;
  - a low-repeat dictionary operand, to find where the P2 memo stops paying;
  - the two `MIN(URL)` ClickBench queries as controls;
  - CB, TPC-H and TPC-DS totals.
- **Peak RSS per arm.** This matters for P1 garbage and for the P3 variant.
- **x86:** the same arms on the x86 box, per the per-architecture ruling.

## 8. Decisions for the architect

1. **P2 dictionary fold memo** — a shape-specialised path in the grouped sink (§11 stop-and-ask). Approve building it, given the precedent in the ungrouped sink and that it never changes an answer? It stays only if measured faster.
2. **P3 merge default** — copy winners (proposed; peak memory as today), or adopt source arenas (no copy, higher peak)? The proposal builds copy and measures adopt as a variant.
3. **Scope** — only `GroupBySink`'s MinMaxStr lane.
   - The ungrouped sink, ARRAY_AGG's string lists (`GBArrayAggState::strs`) and the list-child `cstr` stores keep `std::string`.
   - They share emit through a (bytes, len) accessor.
   - Widen the scope?

What this does not change: the output column format, the expression VM, the scan, or any other aggregate kind.

## 9. Results (2026-10-29, Mac, bench tree)

Method:

- Each (arm, query, round) runs in a fresh process: 1 warm run, then the best of 3 timed runs.
- Arm order rotates every round.
- Figures are medians of per-round paired ratios.
- Built all four parts, then left one out at a time.

### Ablation (5 rounds; ratio = arm without the part ÷ all parts)

| Query | without P2 memo | without P4 prefix | P3 adopt instead of copy |
|---|---|---|---|
| ClickBench REGEXP_REPLACE (`cb28`) | 1.027 | 0.992 | 0.965 |
| same, `k, MIN(Referer)` only | 1.074 | 1.022 | 0.991 |
| `GROUP BY RegionID`, 4 string extremes | **0.889** | 1.138 | 0.999 |
| `GROUP BY SearchPhrase`, MIN(URL), MAX(Title) | 1.007 | 1.006 | 0.929 |
| `GROUP BY UserID`, MIN(SearchPhrase), MAX(URL) | 0.995 | 1.021 | 0.964 |
| `GROUP BY CounterID`, MIN/MAX(Referer) | 1.005 | 1.012 | 0.993 |

- **P2 memo: deleted.** Removing it made `RegionID` 11% faster (0.880–0.915 over all rounds).
  - The probe showed it working as designed: on cb28 it skipped 53.2M of 72.8M dictionary rows.
  - With few, cache-hot groups, though, its per-row lookup into the dictionary-sized table cost more than the compares it saved.
  - Per the no-gating and slower-is-deleted rules, it was removed outright.
- **P3 adopt: replaced copy.** It was faster or equal on every query, and per-query peak RSS did not change (process peaks 5–18 GB, within ±2%).
- **P4 prefix: kept.** Without it, `RegionID` and `CounterID` gain nothing over the base (1.004, 0.999). It is worth 0.88 and 0.93 there, and 0.97–0.99 on `UserID` and `SearchPhrase`.
  - Its one cost is about 1% on the MIN-only cb28 form (1.009 and 1.015 in two runs, half or fewer rounds won), where one group's values share their first 8 bytes.
  - An equality-first rewrite of the tie path measured slower (1.07 on `RegionID`, 1.13 on `CounterID`) and was dropped.

### Final configuration against main's code

P1, P3 adopt and P4. Two runs of 6 rounds each:

| Query | final ÷ base |
|---|---|
| cb28 | 0.803 / 0.821 |
| cb28 MIN-only | 0.799 / 0.802 |
| `RegionID` | 0.892 / 0.892 |
| `SearchPhrase` | 0.617 / 0.600 |
| `UserID` | 0.588 / 0.586 |
| `CounterID` | 0.921 / 0.939 |

Every query is faster in every round.

Suite totals (4 rounds):

| Suite | final ÷ base | per round |
|---|---|---|
| ClickBench | 0.985 | 0.997 / 0.972 / 0.973 / 1.001 |
| TPC-DS SF1 | 0.993 | 1.000 / 1.009 / 0.965 / 1.001 |
| TPC-H SF10 | 1.015 | 0.996 / 1.001 / 1.049 / 1.020 |

The TPC-H shift is uniform across queries, including Q06, Q14 and Q19, which have no GROUP BY. Round 3 was 3–11% slower on every query. So it is not attributable to this change.

Correctness:

- A 21-query differential against main matched for every arm. It covered:
  - 3.0M and 17.6M group outputs;
  - the top-k cut;
  - NULL and all-NULL groups;
  - VARBINARY;
  - the 16/17-byte boundary;
  - empty strings;
  - filtered dictionaries and constant operands;
  - the ungrouped path.
- `make q`, `slt`, `dt`, `st` and `rt` pass.
- A group-by stress build (`OPTERYX_GB_STRESS=1`, which forces every flush and radix merge path) passes `slt` and the small differential.
