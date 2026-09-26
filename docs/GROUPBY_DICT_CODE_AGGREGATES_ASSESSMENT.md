# Dictionary-code GROUP BY for non-COUNT(*) aggregates — assessment

Status: **measured, not recommended.** No engine change. 2026-09-26.

Question: is it worth extending the H20 dictionary-code GROUP BY path
(`src/cpp/engine/native_group_sinks.hpp`, "Pass A … H20") beyond COUNT(*),
and to multi-key GROUP BY where every key is dictionary-shaped?

Answer: **no.** The expensive part of the path (probing once per distinct
key instead of once per row) already runs for every aggregate kind. What an
extension would remove is worth about 1–3% of wall time on the best
candidates. That is below the 2–11% round-to-round spread on this machine.
The time in these queries goes to things key codes do not touch.

## 1. What the code actually does (correction to the brief)

The brief said the dict path applies only when every aggregate is COUNT(*).
That is only true of the **specialised pass C**. The gate that decides the
dict path is:

```
dict_path = single key && compressed && data_length <= 16,384 && data_length*2 <= rows
```

It does **not** check aggregate kinds. When `dict_path` holds:

- Pass A hashes each distinct value once (`compute_row_hashes_shaped`).
- Pass B probes once per distinct value (`dict_part[d]`, `dict_gid[d]`).
  This is the whole H20 win, and it applies to all kinds.
- Pass C:
  - **All-COUNT(*)** (`gb_rows_only`): per row, `grows[dict_gid[codes[i]]] += 1`.
  - **Anything else:** a fill loop writes `mk_hash[i]` and `mk_ent[i]` from
    the code arrays. The generic per-kind pass C then runs unchanged.

So "extending the path" can only mean one thing: pass C loops that index
through codes, which removes the fill loop. The fill loop is therefore the
**ceiling**, and it was timed directly.

## 2. Method

- Build: `make compile`, Python 3.14.5, Apple M5 Pro (18 cores), 16
  execution workers (`resolve_max_execution_workers()`).
- Datasets: `scratch.hits_rugo_262k` (parquet) and `scratch.hits_skene`.
- Temporary counters (since removed) recorded:
  - the dict gate's outcome per morsel;
  - the multi-key shape of each morsel;
  - the fill loop's thread-time.
- Timing:
  - Each arm runs in its own process, because `OPTERYX_GB_DICT` is read once
    per process.
  - Each process runs 1 warm-up plus 3 timed runs and keeps the minimum.
  - 3 rounds, with arm order alternated each round. Tables show the minimum
    over the rounds.
- `*_s` telemetry is thread-seconds. To estimate wall time I divide by 16
  workers. That is an upper bound on the wall saving.

## 3. Which path each candidate takes today

Morsel counts are for 2 executions (warm-up + 1). Parquet and skene agree.

| Query | Keys | Dict gate outcome | Notes |
|---|---|---|---|
| 09 RegionID, COUNT(DISTINCT UserID) | 1 | **100% dict** | already probe-per-distinct |
| 10 RegionID, SUM/COUNT/AVG/COUNT DISTINCT | 1 | **100% dict** | already probe-per-distinct |
| 11 MobilePhoneModel, COUNT(DISTINCT UserID) | 1 | **100% dict** | 11M rows reach the sink after the filter |
| 12 MobilePhone, MobilePhoneModel | 2 | multi-key, **100% of morsels have both keys compressed** | probe only 0.03–0.05 thread-s |
| 13 / 14 SearchPhrase | 1 | **27% dict**; 63% fail the ratio gate, 9% exceed the distinct limit, 1% dense | probe 1.6–2.2 thread-s dominates |
| 15 SearchEngineID, SearchPhrase | 2 | multi-key, 99% both compressed | compound distinct count ≥ SearchPhrase's alone |
| 22 SearchPhrase, MIN(URL) | 1 | 100% dense | **only 2,056 rows reach the sink**; GROUP BY is irrelevant |
| 23 SearchPhrase, MIN(URL), MIN(Title), … | 1 | 100% dense | **only 14,140 rows reach the sink**; GROUP BY is irrelevant |
| 28 CounterID, AVG(length(URL)) | 1 | **100% dict** | |

Key columns do arrive dictionary-shaped from both readers. For Q9–12 and
Q28, nearly every morsel is compressed under both formats.

## 4. Measurements

Wall time in seconds. "on" means `OPTERYX_GB_DICT=1`, "off" means `=0`.

Variants:
- `full`: the ClickBench query as written.
- `rows`: aggregates replaced by COUNT(*), same key and filter.
- `cnt` (Q9): `COUNT(UserID)`. It reads the same column with the cheapest
  non-COUNT(*) kind.
- `noCD` (Q10): the query without its COUNT(DISTINCT).

**Parquet (`hits_rugo_262k`)**

| Variant | on | off | fill (thread-s/run) | fill ≈ wall | ceiling % of full |
|---|---|---|---|---|---|
| Q09 full | 0.2536 | 0.2730 | 0.059 | 3.7 ms | **1.5%** |
| Q09 cnt | 0.0858 | 0.1031 | 0.062 | 3.9 ms | — |
| Q09 rows | 0.0649 | 0.0828 | — | — | — |
| Q10 full | 0.3071 | 0.3318 | 0.068 | 4.2 ms | **1.4%** |
| Q10 noCD | 0.1100 | 0.1311 | 0.063 | 3.9 ms | — |
| Q10 rows | 0.0656 | 0.0838 | — | — | — |
| Q11 full | 0.0984 | 0.0990 | 0.004 | 0.3 ms | **0.3%** |
| Q11 rows | 0.0551 | 0.0561 | — | — | — |
| Q12 full (2 keys) | 0.1132 | 0.1119 | n/a | — | — |
| Q12 rows (2 keys) | 0.0678 | 0.0681 | n/a | — | — |
| Q14 full | 0.2661 | 0.2711 | 0.001 | ~0.1 ms | **~0%** |
| Q14 rows (= Q13) | 0.2033 | 0.1953 | — | — | — |
| Q28 full | 0.3361 | 0.3522 | 0.097 | 6.0 ms | **1.8%** |
| Q28 rows | 0.3074 | 0.3332 | — | — | — |

**Skene (`hits_skene`)**

| Variant | on | off | fill (thread-s/run) | fill ≈ wall | ceiling % of full |
|---|---|---|---|---|---|
| Q09 full | 0.2215 | 0.2384 | 0.048 | 3.0 ms | **1.4%** |
| Q09 cnt | 0.0410 | 0.0632 | 0.048 | 3.0 ms | — |
| Q09 rows | 0.0256 | 0.0513 | — | — | — |
| Q10 full | 0.2690 | 0.2936 | 0.049 | 3.1 ms | **1.2%** |
| Q10 noCD | 0.0651 | 0.0898 | 0.047 | 3.0 ms | — |
| Q10 rows | 0.0260 | 0.0517 | — | — | — |
| Q14 full | 0.2127 | 0.2161 | 0.001 | ~0.1 ms | **~0%** |
| Q14 rows | 0.1558 | 0.1551 | — | — | — |
| Q28 full | 0.1171 | 0.1345 | 0.059 | 3.7 ms | **3.1%** |
| Q28 rows | 0.0827 | 0.1089 | — | — | — |

Round-to-round spread on the parquet full queries was 5–11% (for example,
Q09 full ran 0.2536 / 0.2688 / 0.2807). No ceiling in the tables is above
that noise.

### What the existing path is already worth

| | on | off | saving |
|---|---|---|---|
| COUNT(*)-only, parquet (Q09/Q10 rows) | 65 ms | 83 ms | −22% |
| COUNT(*)-only, skene | 26 ms | 51 ms | −50% |
| Full Q9 / Q10 / Q28 (non-COUNT(*) kinds already covered) | | | −4% to −13% |

The probe telemetry shows where the saving comes from. On Q09 full
(parquet), `probe_s` is 0.107 thread-s with the path on and 0.629 with it
off. Pass C barely moves.

## 5. Where the time actually goes

- **Q9, Q10, Q11 — COUNT(DISTINCT UserID).**
  - Q09 parquet: pass C is 1.63 thread-s for `full` against 0.17 for `cnt`.
  - Q10: `full` minus `noCD` is 197 of 307 ms on parquet and 204 of 269 ms
    on skene.
  - This cost is the per-row insert into the (group, value-hash) pair table
    (`P.cd[s].insert`). It is identical whether the group id comes from a
    code or from `mk_ent`. Key codes cannot reduce it.
- **Q13, Q14, Q15 — SearchPhrase.**
  - Probing dominates: 1.6–2.2 thread-s.
  - The dict gate rejects 73% of morsels, because the distinct count is too
    close to the row count.
  - Where the path does engage, on and off are flat (Q14 rows: 0.2033 on vs
    0.1953 off on parquet; 0.1558 vs 0.1551 on skene).
  - A multi-key version for Q15 would see a compound distinct count at least
    as high as SearchPhrase's alone. It cannot beat the single-key result,
    which is already zero.
- **Q12 — multi-key, where every morsel has both keys compressed.**
  - The whole probe is 0.03–0.05 thread-s, about 2–3 ms of wall.
  - A multi-key dict path has at most that to remove, about 2% of 113 ms.
- **Q22, Q23.** Only thousands of rows reach GROUP BY. Their time is in the
  LIKE filter and the scan.
- **Q28.**
  - `full` minus `rows` is 29 ms on parquet and 34 ms on skene.
  - The fill loop accounts for 6.0 ms and 3.7 ms of that.
  - The rest is `length(URL)` in the projection plus the AVG update. Neither
    is key-code work.

## 6. Design, recorded for completeness (not recommended to build)

If the ceiling ever justified it, this is the shape. It is ordered by the
little value it has.

1. **Pass C row loops templated on an index source (static dispatch).**
   - Dict form: `c = codes[i]; lp[dict_part[c]]; e = dict_gid[c]`.
   - Dense form: `mk_hash[i] >> kGBPartShift; mk_ent[i]`.
   - This deletes the fill loop. Saving: the fill column in §4, 0.3–6 ms.
   - Risk: the per-row index gains one dependent load (`codes → dict_*`).
     The two small arrays stay in L1, but part of the saving could turn into
     a loss. It would have to be measured, not assumed.
2. **Per-code pre-aggregation for SUM / COUNT(col) / AVG / MIN / MAX.**
   - Accumulate into D-sized arrays (D ≤ 16,384, cache-resident), indexed by
     code.
   - Then fold D entries into the lanes, instead of `rows` random lane
     writes.
   - SUM keeps its fail-on-INT64-overflow check. It would sit on the per-code
     accumulator and again on the fold.
   - This only pays when lane writes are cache-hostile. For the candidates,
     groups per partition are few (RegionID ~9k in total), so lane writes are
     already L1/L2-resident. Q10 `noCD` pass C is only 0.53–0.76 thread-s in
     total, and that includes the per-row value read, which does not go away.
3. **COUNT(DISTINCT).** Nothing to gain: pair deduplication is per row by
   definition.
4. **Multi-key where every key is a dictionary column.**
   - Needs a combined per-row code from draken, for example packing codes
     when the product of the key distinct counts fits the 16,384 limit.
   - That breaks the current contract that multi-key hashing is dense per row
     (`native_key_hash.hpp`).
   - This is an ABI/semantics decision for the architect. The measured
     ceiling is about 2% (Q12), and zero for Q15.

## 7. Recommendation

- **Do not extend** the dict-code path by aggregate kind, and **do not add**
  a multi-key dict path. The best-case saving is 0.3–3.1% of wall and sits
  inside measurement noise.
- If ClickBench Q9–11 and Q14 are the target, the evidence points elsewhere.
  These are separate investigations, not started:
  1. **The COUNT(DISTINCT) pair table** (`GBCountDistinct::insert`). It is
     about 45–80% of Q9/Q10/Q11 wall: Q9 `full` minus `cnt`, Q10 `full`
     minus `noCD`, and Q11 `full` minus `rows`, across both formats.
  2. **SearchPhrase probe cost** at moderate per-morsel distinct counts
     (Q13–15). Probing is about 1.6–2.2 thread-s, and the dict gate
     correctly declines most morsels.
