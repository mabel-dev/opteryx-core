# E37 — Carried key-hash (scan-produced string hash reuse)

Status: **LANDED** for the native parquet scan (Option B, first cut); `make q`
197/197 green. Measured: `GROUP BY URL` 170.3 → 157.8 ms (~7%); key-hash time in
`cxx_hash_c` collapsed 16 → 0.7 ms/query (XXH3 re-seed eliminated). Correctness
verified bit-identical vs pyarrow `value_counts` (402,754 groups, 0 mismatches),
carried path confirmed active (hit counter). Remaining: jsonl/csv + IPC-deserialize
producers, `cxx_take` carry, plan-gating — see §7. Expected to be revisited.

## 1. Problem

A long (>12 byte) string is hashed **twice** on the path from Parquet to a
GROUP BY / JOIN / DISTINCT result, and only the second hash is used:

1. **Build (scan/decode).** `draken_build_string_slot` computes
   `XXH3_64bits(bytes, len)` for every long slot and stores the **truncated**
   lower 32 bits as `hash32` (used only for `str_equals` fast-reject). The
   upper 32 bits are discarded.
2. **Probe (operator).** The hashed GROUP BY / JOIN / DISTINCT operator calls
   `cxx_hash_c` → `draken_hash` → `hash_string`, which re-runs
   `XXH3_64bits(bytes, len)` over the **same arena bytes** to produce the 64-bit
   keying hash.

So we pay the full 64-bit XXH3 at scan, throw away half, and recompute the
identical 64 bits at probe.

### Measured cost (ClickBench `hits`, 1M rows, dense URL column)

| query | warm total | key-hash (`cxx_hash_c`) | hash share |
|---|---|---|---|
| `GROUP BY URL` | 170.3 ms | 15.96 ms | **9.4 %** |
| `JOIN on URL`  | 211.8 ms | 39.2 ms | **18.5 %** |

Raw one-pass XXH3 over the ~1M long URLs = 13.1 ms @ 33.6 GB/s (single core).
The probe hash is on the operator critical path (sequential ingest), so the
share is realizable, not overlapped.

## 2. Why this is worth doing but was NOT done as a slot change

The 16-byte German-string slot (`draken/core/string_slot.h`) is full:
`length(4) + prefix(4) + hash32(4) + arena_offset(4)`. A 64-bit hash does not
fit, and the fields already there earn their keep:

- Measurement across 14 real columns showed `prefix` + `hash32` are
  **complementary**: `prefix` (inside `raw.lo`) individuates front-loaded
  columns (names, vendors, usernames); `hash32` is the only thing that rejects
  shared-head columns whose entropy lives past byte 8 (URL, CVE ids, cvss
  vectors, deep free-text). On no measured column were both idle. So neither is
  dead weight; the slot layout stays.

The reusable value therefore lives **outside** the slot, as a companion buffer.

## 3. Design (Option B)

### 3.1 The carried value is the SEED, not the final hash

`hash_string` computes, per data-element `k`:

```
seed_k = str_hash_seed(slots[k], arena)      // long: XXH3_64bits;  short: raw.lo + raw.hi*MIX
row_hash_i = simd_hash_i64(seed_{selection[i]})   // with NULL_HASH substituted for null rows
```

We carry **`seed_k`** — one `uint64_t` per data-element — in a new owner-held
buffer. The consumer substitutes the `str_hash_seed(...)` call with a
`keyhash_buf[selection[i]]` load and runs the **same** `simd_hash_i64` mix and
the **same** NULL_HASH substitution. The output is therefore **bit-identical**
to `hash_string` by construction — this is pure hoisting of the seed step to
decode, not a new hash function.

### 3.2 Storage — `VectorOwner::keyhash_buf`

```c
OwnedBuffer<uint64_t> keyhash_buf;   // one seed per data-element (data_length entries), or nullptr
```

- Indexed **by data-element** (`keyhash_buf[k]`, addressed via `selection[i]`),
  matching the vector's own `data[selection[i]]` access. So a **dict** vector
  carries `data_length` seeds (8×distinct — negligible); a **dense** vector
  carries `length` seeds (8×row).
- `nullptr` is the universal "not carried → recompute" signal. **Presence ==
  validity.** There is no separate valid flag: any op that does not explicitly
  propagate `keyhash_buf` produces an owner with `nullptr`, which is correct
  (the consumer falls back to `str_hash_seed`).

### 3.3 Producer — the string-column decoders

`draken_build_string_slot` already computes `XXH3_64bits` for every long slot.
`draken_build_string_slot_seed` (draken/ops/string_hash.h) builds the slot AND
emits `str_hash_seed` in one pass — long: reuse the XXH3 (store full 64, still
truncate for `hash32`); short: the `raw.lo + raw.hi*MIX` word-seed. No extra
hashing. `rugo/src/parquet/io_pipeline.hpp` (`build_direct_string_plain` dense +
`build_direct_string_dict`) fills `ColumnOut.keyhash[k]` with these seeds.

**The seed reaches the operator's `VectorOwner::keyhash_buf` via the NATIVE scan
bridge, not the Python wrap.** The engine cutover means a direct (DK_VARCHAR /
DK_VARCHAR_DICT) column becomes a `CxxColumn` in
`native_parquet_scan_source.hpp::build_column` → `emit_dense_string_column` /
`emit_dict_string_column` (native_varchar_pool_decode.hpp), which now take a
`keyhash` param and attach it to `keyhash_buf` (taking ownership; `build_column`
nulls `ColumnOut.keyhash` so `~MorselRef` won't double-free). The Python
`pool_reader.pyx` wrap (`draken_vector_own_string{,_dict}` gained a COPYING
`keyhash` param) is a legacy/fallback path, wired for consistency but not the hot
path. jsonl/csv builders pass nullptr (task #5). `column_deserializer` (IPC/pool
deserialize) passes nullptr — future work.

### 3.4 Consumer — `cxx_hash`

`cxx_hash` holds the column **owners** (not just views), so it can see
`keyhash_buf`:

- **single key** (`hash_shaped_impl`): if `own->keyhash_buf` present and the
  type is a string family, run the local seed→`simd_hash_i64` loop reading
  `keyhash_buf[selection[i]]`; else current path.
- **multi key**: same substitution in the per-column `draken_hash` loop.

### 3.5 Invalidation

| operation | keyhash | why |
|---|---|---|
| transform (`UPPER`, `SUBSTRING`, `CONCAT`, cast, …) | **dropped** (new owner, nullptr) | bytes changed → seed invalid; recompute is correct |
| row-select via **selection narrowing** (mask that keeps `data`+`arena`) | **preserved** | bytes unchanged; addressed through the narrowed `selection` |
| row-select via **materializing take/compaction** (`cxx_take`) | **dropped (first cut)** | bytes unchanged but re-homed; gathering the seeds in lockstep is deferred (§7) |
| projection / pass-through | preserved | column identity unchanged |

Because presence==validity and transforms naturally build fresh owners,
invalidation is **automatic** — the only explicit work is *preserving* it across
the row-select paths we choose to support.

## 4. Correctness gate (non-negotiable)

Oracle parity: for a matrix of {dense, dict, constant} × {no-null, sparse-null,
all-null} × {short, long, mixed} string vectors, the carried-seed path must
produce **byte-identical** `cxx_hash` output to the recompute path. The
`hash_string` recompute path stays as the fallback and the oracle. The carried
path is enabled only after parity is green. A carried hash that differs from the
recompute silently changes GROUP BY / JOIN answers — the exact
`ptr.data==NULL`-class trap the vector contract (§11) forbids.

## 5. Scope of the win

- **GROUP BY** long-string key: eliminates the probe recompute → ~9.4 %.
- **JOIN** long-string key: ~18.5 % (both probe sides + any DISTINCT sink hash
  scan-produced strings); the majority is scan-recompute and recoverable, the
  re-materialized-intermediate slice is not (until take-carry, §7).
- **Dict columns**: already cheap at probe (per-distinct hash); carrying is
  near-free (8×distinct) and keeps the path uniform.

## 6. Why Option B (not A)

Option A carried the hash through *every* intermediate operator (filters,
projections between scan and key op). Option B carries it only while the bytes
are **untransformed**, and drops it on materializing take. This is justified by:
**most filters push into the reader**, so a string column typically arrives at
the key operator straight from scan with the seed intact. Option A's extra
plumbing buys the residual (filters that survive to runtime *and* materialize),
which is the uncommon case. We take the 80 % for 20 % of the work and revisit.

## 7. Known limits / revisit list

1. **`cxx_take` drops the seed.** A runtime filter that materializes between scan
   and the key op forces a recompute. Fix later: gather `keyhash_buf[old_sel]`
   in the take loop (the slot already carries `hash32` through
   `str_clone_with_offset`; the seed extends the same way).
2. **Gating.** ✅ DONE (step 1). The compiler collects every column identity used
   as a GROUP BY / JOIN / DISTINCT key across the whole plan
   (`_Compiler._hash_key_identities`), marks the scan's read columns
   (`hash_key_columns`, parallel to `column_names`), and threads it through
   `open_native_scan_plan` → `NativeScanPlan` → `set_native_scan_source` → the
   scan source, which attaches `keyhash_buf` **only** for keyed columns
   (`wants_keyhash(i)`). Default (nothing keyed) → NO sidecar — `SELECT *` / `LIKE`
   / standalone rugo build nothing. Verified: projection/LIKE = 0 sidecar, GROUP BY
   = sidecar reused, `make q` 197/197.
   Residual: `io_pipeline` (rugo) still *builds* the seed for non-key columns (the
   scan source then drops it) — a small waste (the XXH3 is shared with `hash32`
   anyway) until the flag is threaded into `io_pipeline` to skip the build. Also
   TRANSFORMED keys (`GROUP BY UPPER(x)`) key on the kernel output, not the scan
   column, so they need the directive to reach string kernels = **step 2**.
3. **Drop `hash32`** ✅ DONE (step 3). The equality fast-reject was removed from
   `str_equals` / `sg_eq_slots` / `str_eq_slots` (dead in every hash-bucketed
   caller — reached only after a 64-bit-hash bucket match, so hash32 always
   matched — and negligible on equality filters: length+first4 rejects ~99.9%
   first). With no reader left, `str_init_extern` lost its `hash32` parameter and
   every builder stopped computing the XXH3 for it (~25 call sites, hash32 now
   always 0). The seed builder (`draken_build_string_slot_seed`) still computes its
   XXH3 for the carried seed. The `partition_by_hash` docstring (which wrongly
   claimed it folds hash32 — it uses the 64-bit `str_hash_seed`) and the
   `_key_store.pxi` "hash32 trusted" comments were corrected. Verified: `make q`
   197/197, `test_shapes_basic` 198/198, draken slot tests 215/215, GROUP BY URL
   bit-identical to pyarrow. NOTE: no execution-time change was visible (the avoided
   XXH3 is noise against total query time); this landed for correctness-of-cost and
   simplicity, not a measured speedup.
   Remaining across E37: string-kernel producers for computed keys (step 2), the
   jsonl/csv/IPC scan producers, `cxx_take` carry, and threading the key flag into
   `io_pipeline` so non-key parquet columns skip the seed build too.
3. **WHERE-equality reuse.** The carried seed could accelerate `col = 'literal'`
   equality in a runtime filter (hash the literal once, reject on seed mismatch).
   Moot for now because equality filters mostly push into the reader. Noted, not
   built.

## 8. Memory note

`keyhash_buf` adds `8 × data_length` bytes per carried string column, freed with
the owner (RAII). Dict: 8×distinct (negligible). Dense high-cardinality (URL,
Referer): 8×row. This is the cost weighed against the gating revisit (§7.2);
`draken_vector_nbytes` / `cxx_morsel_nbytes` accounting is updated to include it
so morsel footprint (and the GCS-stream OOM guard) stays honest.
