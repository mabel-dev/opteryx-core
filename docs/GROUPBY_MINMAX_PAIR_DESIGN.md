# GROUP BY: MIN and MAX of one column as one record

Status: DESIGN — awaiting architect rulings (D1–D4). Nothing built.
Date: 2026-11-23. Motivating query: JSONBench Q5. The shape is general: MIN(x) and MAX(x) over the same column in one GROUP BY (time ranges or extents per entity).

## Problem

The grouped sink runs `MIN(x)` and `MAX(x)` as two independent `GBKind::MinMaxNum` specs. Each one keeps its own three lanes (`valid`, `mkey`, `i64`) and runs its own full row pass (`pass_c_spec` → `gb_apply_row_kind_rd`, `native_group_sinks.hpp:3729`).

Profile, JSONBench Q5 against Q4 at dup 100M (Mac, same build, samply, 10 s each). Q5 = `MAX - MIN` per `did`; Q4 = `MIN` per `did`. CPU per run:

| | Q5 | Q4 | Q5 extra |
|---|---|---|---|
| aggregate updates (`pass_c_spec`) | 127 ms | 59 ms | +68 |
| lane growth zero-fill (`GBSegArray::resize`) | 31 | 14 | +17 |
| merge fold (`combine_into`) | 21 | 9 | +12 |
| emit | 5 | 2 | +3 |
| scan | 60 | 63 | 0 |
| **total** | **479** | **376** | **+104** |

The cost is memory access, not dispatch:
- The hottest line in Q5 is `:3736`, at 11.1%: the load of the group's `valid[e]` and `mkey[e]`.
- The store at `:3737` is another 5.8%.
- `sort_num_key`'s per-row type switch is only 3.8%.

Rows arrive in time order but group by `did`, so `e` is effectively random. Each spec touches its own `valid`, `mkey` and `i64` cache lines, so a row costs about six random lines for two specs. On x86 (1.25 MB L2 per core) those lanes do not fit in L2. Unmeasured.

The ungrouped sink already keeps both extremes in one cell (`AggCell::min_key`/`max_key`, `agg2_update`, one key compute per row).

## Proposal

Add one kind pair, on the `QuantileShared` precedent:

- **`GBKind::MinMaxPair`** goes on the first of a MIN/MAX pair. It owns one record lane, `GBSegVec<GBMinMaxRec> mm`, where `GBMinMaxRec = { uint64_t min_key; uint64_t max_key; int64_t valid; }` (24 bytes, trivially copyable, like `GBStrExt` in `sext`).
- **`GBKind::MinMaxPartner`** goes on the other spec. It has no lanes, like `QuantileShared`. Emit and top-k read the owner's record in the same partition through `lane_view` and take the other extreme.

Per row: one key compute, one record touched (one cache line), then both compares and one `valid += 1`.

The raw values are not stored, because for every non-float `MinMaxNum` type the raw value is recoverable exactly from the order key:
- signed, temporal, DECIMAL(64) and BOOL: `raw = key ^ SIGN`;
- unsigned: `raw = key`.

`sort_num_key` sign-extends the narrow types before flipping, so the inverse is exact. Floats are excluded (D1).

### Sites the new kinds must cover (all in `native_group_sinks.hpp` unless noted)

The pair must work on every path the sink can take, or the compiler must not form it there. Partial coverage would be a silent wrong answer:

1. `gb_kind_of` (:1710). Selects the kind from the new spec field (D2).
2. `gb_lanes_resize` (:3559), `GBLanes` (:3482), `GBFlatLanes` (:3633), the lane swap (:3522, used by top-k), `lane_view`, and `GBLaneView`.
3. Row updates: `gb_apply_row_kind_rd` (:3729) and `gb_kind_row_resolvable` (:3610). These serve the hash, dictionary and direct-array (`da_sink`) paths, and raw mode.
4. Ticket mode: `ticket_sink` / `ticket_fold` (:7565).
5. Merge: `combine_into` (:6617), which is also the radix scatter.
6. Emit: `emit_lane_column` (:2176). Gathers min or max from the records into an INT64-container column (`key ^ SIGN`), then reuses `emit_fixed_column`.
7. Statistics seed: `queue_seed` (:7253, :7353). Statistics carry both min and max, so the pair takes both directly.
8. Spill: `gb_kind_spillable` (:4251), `gb_spill_lane_mask` (:4274), encode and decode (D3).
9. Top-k fusion: no new code beyond 2 and 6. `topk_select` ranks through `emit_lane_column` + `lane_view` and swaps lanes.
10. Compiler: `_parse_aggregates` (`compiler.py`). Forms the pair and sets the spec field (D2). The sink validates it, failing loudly (like `quantile_owner`'s check at :562).
11. `native_groupjoin.hpp` (:95, :338, :544) has its own lane handling. Either implement it or have the compiler not pair under a group-join (D4).

Unchanged: single MIN, MAX or ANY_VALUE specs (they keep `MinMaxNum`), floats, DECIMAL128/UUID (`MinMaxD128`), strings (`MinMaxStr`), and the ungrouped sink.

## Decisions for the architect

- **D1 — floats.** Recommend leaving FLOAT32/FLOAT64 unpaired (today's two specs). The order key canonicalises −0.0 and maps every NaN to `UINT64_MAX`, so a float record would need the two raw lanes back (40 B instead of 24) and a second record type. The alternative is one 40 B record for every type, with raws always stored: simpler code, about 1.7x the bytes per group on the common (integer/temporal) case.
- **D2 — who forms the pair.** Recommend the **compiler**: add `AggSpec2::minmax_partner` (spec index or -1), mirroring `quantile_owner`, and have the sink fail loudly when the partner is not the opposite direction over the same `col_idx`. The alternative is for the sink to detect pairs at capture by `col_idx`. That is less code but a hidden behaviour, which §1 rules out.
- **D3 — spill format.** Recommend de-interleaving the record into three INT64 spill columns under three new mask bits (10–12; bits 0–9 and 15 are taken), so spill stays one column per lane. The alternative is to refuse spill for paired sinks (`gb_kind_spillable` false), but that turns a spilling MIN+MAX GROUP BY that works today into a refusal, so it is not recommended.
- **D4 — group-join.** Recommend that the compiler **not** pair when the GROUP BY compiles to a group-join (it refuses kinds it doesn't know at construction, `native_groupjoin.hpp:97`). Implementing the pair there is separate work with its own benchmark.

## Expected effect (estimate, not measured)

The pass becomes one key compute and one record line per row instead of two passes and about six lines. Growth zero-fills 24 B per group instead of 48, and the merge folds one record instead of two lane sets.

If the paired pass costs about what Q4's single MIN pass costs, Q5 saves roughly 60–75 of its 479 CPU ms per run (about 12–15%), or about −8 to −12% wall. x86 could gain more, since the lanes it stops touching are the ones that miss L2 there.

Within our suites only JSONBench Q5 has this shape. ClickBench's MIN/MAX pair is ungrouped (Q06), and TPC-H and JOB have no grouped MIN+MAX over one column. The benefit is real for the workload shape but would show up in only one benchmark.

## Verification plan

- **Answers.** Run every MinMaxNum type (INT8–64, UINT8–64, DATE32, TIMESTAMP at all units, TIME, DECIMAL(64), BOOL) with negatives, NULLs, all-NULL groups and a NULL group key. Run each through every path: dictionary, direct-array, hash, raw, ticket, spill (small `GROUPBY_SPILL_BYTES`), statistics seed, top-k fusion (`ORDER BY MIN(x)` / `MAX(x)` `LIMIT k`), and the `OPTERYX_GB_STRESS` build. Compare against the unpaired result, forced by pairing MIN(x) with MAX(x + 0).
- **Performance.** Interleaved A/B on JSONBench Q1–Q5 (10M and dup 100M) and the ClickBench GROUP BY set (no change expected there), on Mac and x86.
- **Suites.** `make q`, `make slt`, and `make dt` + `st` + `rt` (native kind added).
