# NDV (Distinct-Count) Statistics for Production — Design & Decision (WP-8)

**Status:** Awaiting architect decision. Design-only; no engine code proposed here.
**Author:** optimizer review work package WP-8.
**Date:** 2026-06-12.

---

## TL;DR

The number-of-distinct-values (NDV) machinery in Opteryx is **already built, tested,
and working end-to-end**. The only thing missing is that production Parquet has no
`.stats.json` sidecars generated. This is therefore **not a "build NDV" project — it is
a "decide how sidecars get generated and kept fresh" decision.**

Verified today: dropping a sidecar next to a Parquet file makes NDV light up across the
whole planner with **zero engine code change**. For `testdata.tpch_001.nation` the KMV
estimate equals the true distinct count exactly (25 / 5 / 25); for a 105-column,
33 MB ClickBench file the sidecar costs **1.5 s to generate and 30.5 KB on disk (0.09 %
of the Parquet)** and estimates high-cardinality columns within ~4–22 %.

**Recommendation:** adopt **Option A (pipeline-time sidecar generation)** as the primary
path. It is the lowest code, lowest risk, and already proven. Options B–D are
enhancements/fallbacks, not prerequisites.

**Decision needed from the architect:** see [§6](#6-decision-needed).

---

## 1. Current state (what already exists)

| Component | Location | Status |
|---|---|---|
| KMV estimator (K=32) | `opteryx/models/manifest.py::estimate_cardinality` | ✅ works; merges per-file min-k hashes; exact when < K distinct, formula otherwise; returns `None` when absent |
| Sidecar loader | `opteryx/connectors/filesystem_connector.py::_load_sidecar_min_k_hashes` | ✅ reads `<file>.stats.json`; validates `schema_version` + field-id map; **rejects stale → None (fail-safe)** |
| Offline populator | `dev/populate_stats.py` | ✅ single-pass scan, K=32 sketch per column, atomic write; CLI `python dev/populate_stats.py <dir> [--force] [--dry-run]` |
| Sidecar format v1 | `{schema_version, field_ids, min_k_hashes{col→[sorted u64]}}` | ✅ defined, round-trip tested (`tests/unit/dev/test_populate_stats.py`) |
| Catalog NDV | `FileEntry.from_datafile` (Iceberg path) | ✅ carries bounds; can carry NDV if the catalog provides it |

**The gap is purely operational.** Sidecar coverage today is ad-hoc:

```
testdata/tpch_001:     8 parquet,  8 sidecars   (100%)
testdata/tpch_1:      22 parquet, 22 sidecars   (100%)
testdata/clickbench_tiny: 1 parquet, 0 sidecars (0%)  ← the wide, high-cardinality benchmark
testdata/flat:        24 parquet,  0 sidecars   (0%)
```

So the engine already runs with NDV present for some tables and absent for others, and
degrades cleanly to constants when absent. Nothing is broken; the planner is just
flying blind on the tables that lack sidecars.

## 2. Why it matters — what NDV unlocks

Every cost decision the recent WPs wired up consumes NDV. With NDV absent they fall back
to constants and the smarter logic is dormant:

- **Join cardinality** (`estimate_join_cardinality`): `|A||B| / max(NDV)` → without NDV, a fixed 0.1 selectivity.
- **Join-side selection** (WP-7, `_decide_swap`): cardinality-aware rules 2 & 3 only fire when both join-key NDVs are known; else pure row-count fallback.
- **DPccp join ordering** (WP-11): enumerates on row-count cost; NDV would sharpen intermediate-cardinality estimates.
- **Group-by / Distinct output rows** (`estimate_group_by_cardinality`): product of group-key NDVs; without it, `input_rows / 2`.
- **hash-map variant** (parvi vs carchar): NDV-product ≤ 16 → inline 16-slot map; without NDV, always the safe carchar.
- **Predicate selectivity** (WP-5, `selectivity.py`): Eq → `1/NDV`, IN → `n/NDV`; without it, 0.1 / `n·0.1`.

This is why WP-8 is the lever behind WP-5/WP-7/WP-11 actually biting on real data.

## 3. Options

### Option A — Pipeline-time sidecar generation (RECOMMENDED)

Generate `<file>.stats.json` whenever a Parquet file is written, as a step in the
ingest/ETL pipeline (or a scheduled backfill job for existing data). The filesystem
connector already loads them; no engine change.

- **Code:** productionize the existing `dev/populate_stats.py` (move out of `dev/`, add to the write path or a cron backfill). The algorithm is done.
- **Cost (measured):** ~1.5 s and 30.5 KB for a 105-col / 33 MB file (0.09 % size, one-time per file). One extra tiny object per Parquet; one extra small range-read per file at plan time (already implemented, cached by the FS layer).
- **Quality:** exact for ≤32 distinct; ~4–22 % error above (KMV K=32) — adequate for order-of-magnitude cost decisions.
- **Staleness:** sidecar is keyed to `schema_version` + positional field-ids; a rewritten/repartitioned file whose sidecar wasn't regenerated is **rejected → None** (fail-safe, never wrong). Files are immutable in blob stores, so "regenerate on write" keeps it correct by construction.
- **Risk:** low. No hot-path change, no new dependency in the engine (`populate_stats` uses PyArrow, which is allowed in `dev/`/tooling, **not** the engine).
- **Open sub-question:** raise K (32 → 64/128) to cut high-cardinality error? Doubles sidecar size, still tiny. Cheap knob, can defer.

### Option B — Opportunistic runtime collection during scans

Build a KMV sketch in the C++ decode pipeline as columns are materialised, then write
the sidecar back after the query.

- **Cost:** ~sketch-update per decoded value; the populate cost moves into the first query that scans the file.
- **Why it's hard / risky:** decode runs **fully `nogil` in C++** (`pool_reader.pyx`) with no Python in the hot path (§3 contract). This needs (a) a vendored native KMV/HLL in the pipeline, (b) a write-back channel with clear buffer ownership/lifetime, (c) handling partial scans (projection/pushdown means you rarely see all columns or all rows, so the sketch is biased/incomplete). Touches the most performance-sensitive, GIL-sensitive code we have.
- **Verdict:** defer. Only worth it if Option A's "who runs the populator" turns out operationally intractable. The partial-scan bias is a real correctness-of-estimate problem, not just engineering.

### Option C — Footer dictionary-size proxy (zero-cost fallback)

When no sidecar exists, derive a rough NDV from the Parquet dictionary page
(presence + `data_page_offset − dictionary_page_offset`).

- **Cost:** free — already in the footer rugo reads.
- **Quality:** poor. Dictionary size is **compressed bytes**, not cardinality; only present for dictionary-encoded chunks; varies wildly with data. Usable at best as a loose **upper bound**, never a point estimate.
- **Verdict:** optional cheap fallback to seed *something* when sidecars are absent (e.g. for `hash_map_variant` upper-bounding), explicitly marked low-confidence. Not a substitute for A.

### Option D — Catalog-native statistics (Iceberg)

For catalog-backed tables, populate NDV from the catalog's own sketch mechanism
(Iceberg Puffin / Theta sketches) via `FileEntry.from_datafile`.

- **Cost:** zero engine-side if the catalog already computes them; otherwise it's Option A run by the catalog's maintenance jobs.
- **Quality:** good (Theta ≈ KMV).
- **Scope:** **only** catalog tables, not raw `file://`/`gs://` Parquet. Complementary to A, not a replacement.

## 4. Comparison

| | A: pipeline sidecars | B: runtime C++ | C: dict proxy | D: catalog-native |
|---|---|---|---|---|
| Engine code | none (productionize tool) | large, hot-path, nogil | small (reader) | none |
| New runtime cost | ~0 (one small read) | per-value sketch in scan | 0 | 0 |
| Quality | exact ≤32, ~18 % above | same but partial-scan biased | poor (upper bound) | good |
| Staleness safety | fail-safe (schema/field-id reject) | n/a (live) | n/a | catalog-managed |
| Coverage | all FS Parquet | only scanned files | all Parquet | catalog tables only |
| Risk | low | high | low | low |

## 5. Recommendation

1. **Adopt Option A** as the production path: move `populate_stats.py` into the supported
   tooling and run it (a) as a write-side step in the data pipeline and (b) as a one-off
   backfill over existing tables. This lights up every NDV consumer with no engine change.
2. **Optionally add Option C** as an explicitly-low-confidence fallback for sidecar-less
   files, gated so it never overrides a real sidecar.
3. **Keep Option D** for catalog tables where the catalog can carry sketches.
4. **Defer Option B** unless A proves operationally impossible.

This sequencing means the highest-value win (A) needs essentially no engine risk, and the
recent optimizer WPs (5/7/11) start paying off on real data immediately on the tables we
choose to populate.

## 6. Decision needed

1. **Approve Option A** as the primary path, and decide **who owns sidecar generation** —
   write-side pipeline hook vs. scheduled backfill vs. both?
2. **Where does `populate_stats` live** once productionized — stays a `dev/` CLI invoked by
   the pipeline, or promoted to a supported maintenance command?
3. **K value:** keep K=32, or raise to 64/128 to cut high-cardinality error (still < ~120 KB
   per wide file)?
4. **Option C fallback:** want the low-confidence dictionary proxy for sidecar-less files,
   or prefer the planner stay honest (NDV = None → constants) until a sidecar exists?
5. **Option D:** is the Iceberg/catalog path in scope now, or later?

Once 1–5 are answered, the implementation is a small, well-bounded follow-up package
(mostly packaging + a pipeline hook), not an engine change.
