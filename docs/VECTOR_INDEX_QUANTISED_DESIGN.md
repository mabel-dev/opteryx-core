# Vector index — reading fewer bytes (v2 `.vidx`)

**Status:** DESIGN ONLY. Nothing is built. Every ruling below is a proposal for the architect.
**Date:** 2026-10-25
**Amends:** `docs/VECTOR_INDEX_DESIGN.md` §5.2 (index file), §8 (execution), §9 (remote IO). The
rulings in that document stand unless a decision in §7 below asks to change one.
**Trigger:** every exact query against the 7.9M-row tweets index timed out reading the vectors.

---

## 0. Summary

The index file stores one full-fidelity copy of every vector (fp16, 768 B/row at 384 dims) and
an exact search reads all of it. For tweets that is 6.09 GB per query. This design adds a
compact tier and an ordinal-addressable full tier, so that a query reads far fewer bytes without
changing any answer the user can get today.

1. **Compact codes with a provable error bound** (8-bit scalar). A scan of the codes bounds every
   row's true distance from both sides; only rows whose lower bound can still reach the top-k are
   refined against the full fp16 vector. The answer is **exact** and still the draken kernel's
   value. This is filter-and-refine, not ANN.
2. **Full vectors in ordinal order.** A refine is a byte-range read at `ordinal × dims × 2`. A
   WHERE filter's survivors become byte ranges directly, so a filter finally reduces IO.
3. **Explicit approximate search reads codes, not vectors.** `SET nprobe = n` keeps its meaning
   (ruled 2026-10-03) and gets cheaper: the probed clusters' blocks are codes, then the top
   candidates are refined from the full tier.
4. **Fail before the timeout.** The planner already costs index vs exact in seconds
   (`strategies/vector_search_cost.py`). An exact search whose estimate exceeds the query's time
   budget is refused with the byte count and the remedy, instead of running into the timeout.

**What this does not do, stated up front.** A deterministic bound only prunes when the code is
accurate, so the exact path is capped at roughly **2x** fewer bytes than today (8-bit codes are
384 B + 4 B per row against 768 B). That is not enough, by itself, to make a 6 GB exact scan
cheap. The large reductions (10-30x) come from filters (item 2) and from the explicit
approximate path (item 3). Whether the 2x is worth its storage is gated on a measurement (§5, M2).

---

## 1. What is wrong today (verified in code, 2026-10-25)

1. **One tier, 768 B/row, and it does not compress.** `vector_index_file.hpp` body = per cluster
   `uint32 ordinal[n]` then `uint16 vector[n][dims]`. fp16 embeddings do not compress (recorded
   2026-10-04: 258.9 MB file for 258.7 MB of raw vectors).
2. **A WHERE filter does not reduce IO.** `search_index_file`
   (`src/cpp/engine/vector_index_search.hpp:96-105`): for `nprobe == 0` it sets `wanted` to
   *every* block, and `admitted` is only handed to `TopK::offer` at `:135-136`. The survivors are
   skipped while scoring, after the bytes were fetched. This contradicts the "filter first, then
   search" shape in `VECTOR_INDEX_DESIGN.md` §0 and §8. It is a defect against the existing
   design, reported here and fixed by the layout in §3.
3. **Cluster-ordered blocks cannot serve a filter.** Blocks group rows by cluster, so a filter's
   survivors are scattered across all of them, even if the file were read selectively.
4. **There is no cheaper exact route to fall back to, by ruling.** Search without the index must
   embed every row at query time (1.85e-2 s/row measured, 1 thread). On tweets that is not an
   alternative, so the index read is the only path and its size is the query cost.
5. **A timeout is the failure mode.** Nothing estimates the read before it starts.

Not yet known, and measured first (§5, M0): whether the timeouts are bandwidth-bound or caused by
the 128 MB wave barrier (`kWaveBytes`, `vector_index_file.hpp:62`), where a wave must finish
before the next is issued. The 3.8 MB/s recorded on 2026-10-04 was GCS from the office and says
nothing about a Cloud Run worker.

---

## 2. Principles carried over (not re-opened)

- Search is **exact by default**; approximate only by `SET nprobe` (D-9, re-ruled 2026-10-03).
- The distance returned is the **draken kernel's value over the fp16 vectors**, bit for bit as
  today. The compact tier only decides *which rows get the kernel*; it never produces a returned
  distance.
- An index file that does not match its definition fails loud. No fallback, no silent skip.
- Compaction never embeds. A format change must not either (§6).
- The index is derived, billed at logical bytes (D-13), referenced from the manifest, one file per
  data file.

---

## 3. The v2 file

```text
<location>/index/<index_id>/<data-file-stem>-<nonce>.vidx      (version 2)
    SECTION F  full vectors, ORDINAL order, fixed stride:
                 uint16 vector[rows][dims]                      ordinal i at i * dims * 2
    SECTION C  compact codes, CLUSTER order, blocks back to back (as v1 blocks):
                 uint32 ordinal[n]
                 uint8  code[n][dims]                           8-bit scalar, per-dimension scale
                 uint16 err[n]                                  ||v - v_hat||_2, rounded UP, fp16
    FOOTER     dims, rows, clusters, blocks; centroids; per-dimension (min, scale);
               block table (cluster, rows); presence bitmap + rank directory; section offsets
    TAIL       24 bytes: footer length, XXH3-64, version = 2, magic
```

**Section F.** Ordinal order with a fixed stride, so `ordinal → byte offset` is arithmetic and a
refine or a filter's survivors become ranges with no lookup. Rows that are not searchable (NULL or
empty text, deleted at build) keep their slot as a zero vector, which `ann_row_searchable`
already rejects; the presence bitmap records them so a scan can skip the slot without reading it.
v1 stored searchable rows only; v2 trades a few bytes of placeholder for addressability.

**Section C.** Same cluster-ordered block shape as v1, so `ivf_probe` and `blocks_of` carry over.
Each row's code is its normalised fp16 vector quantised to 8 bits against per-dimension
`(min, scale)` kept in the footer. `err` is the 2-norm of the quantisation residual **measured
against the fp16 values the kernel will score**, rounded up when stored.

**Bytes per row at 384 dims:** F 768 + C 384 + 4 (ordinal) + 2 (err) = 1,158 B, against 768 B
today. At rest the index grows **1.5x**; a query that touches only C and a small refine reads
**about 0.5x** of today's bytes. Billing is at logical bytes (D-13), so this is a cost the user
pays and a ruling (§7, D-V1).

### 3.1 The exact path (filter-and-refine)

For query `q` (normalised, fp16 → fp32 weights `w_i = q_i * scale_i`, constant `c0 = Σ q_i * min_i`):

```text
score_hat(row) = c0 + Σ w_i * code_i            ≈ q · v
lower(row)     = 1 - score_hat - err(row) - ε
upper(row)     = 1 - score_hat + err(row) + ε
```

`|q·v − q·v̂| ≤ ‖q‖‖v−v̂‖ = err` (Cauchy-Schwarz, unit `q`), so `lower ≤ kernel distance ≤ upper`
**for every row, always**. ε covers the kernel's fp64 rounding.

1. Scan C. Keep the k-th smallest `upper` seen so far as `τ` (a heap of k uppers).
2. A row whose `lower > τ` cannot be in the top-k: dropped. `lower ≤ τ` (inclusive, so ties at the
   boundary survive): a candidate ordinal.
3. Candidates are sorted, their F slots read as coalesced ranges (the existing
   `kMergeGapBytes` rule), and scored with the **unchanged** draken cosine kernel into the
   existing `TopK`.
4. Result: identical hits and distances to today's exact search. A property test enforces the
   bound (below).

The per-row inner loop is `u8 → f32` convert and FMA over `dims`: about 3 GFLOP for tweets, noise
next to the IO, and NEON/AVX2 via `SIMD_STATIC_SELECT` like the cosine kernel (D-6).

**Why 8 bits and not fewer.** With a deterministic bound the candidate set is the rows within
`2 × err` of the k-th distance. At 8 bits `err` is about 0.007 for 384-d embeddings (estimated,
not measured); at 4 bits it is about 0.11, wider than the spread of the top-k region, so almost
nothing is pruned. Going below 8 bits needs probabilistic bounds (RaBitQ-style), which would make
the "exact" path statistically exact rather than provably exact. That is an architect call and is
not proposed (§7, D-V5).

### 3.2 With a WHERE

Pass 1 yields the survivor mask (already built: `VectorIndexAdmission`). The cost model picks per
file between:

- **F-ranges:** survivors → coalesced ranges over section F → kernel. Bytes = survivor bytes plus
  gaps below `kMergeGapBytes`. Wins when survivors are few or clustered in ordinal order (files
  are append/sort ordered, so date and key filters are usually clustered).
- **C-scan + refine:** the §3.1 path restricted by the mask. Wins when survivors are many and
  scattered. **Honest limit:** scattered survivors at ~1% density average a 77 KB gap, so ranges
  merge into a near-full read; the model must see that, and it is a measurement (§5, M3).

Both arms are exact. The rule is per file, deterministic, native, and shown in EXPLAIN.

### 3.3 The explicit approximate path (`SET nprobe = n`)

Unchanged contract: only the `n` nearest clusters are examined; rows in unprobed clusters cannot
be returned; under a WHERE `nprobe` is ignored (ruled 2026-10-03). New mechanics: the probed
clusters' blocks are C (384 B/row), candidates are ranked by `score_hat`, the best
`k × refine` are read from F and re-scored by the kernel, and the top-k of those is returned.
Returned distances remain the kernel's. At `nprobe=32` of 579 clusters on tweets this reads about
170 MB of C instead of about 430 MB of v1 blocks. `refine` is a new knob and needs the same proof
as `nprobe` (see §5, M4); it is not given an unflagged default.

---

## 4. Planner and refusal

- The cost model gains: bytes of C, bytes of F ranges for the survivor mask, candidate-fraction
  estimate (from M2), request count, measured bandwidth for the storage class.
- **Over budget ⇒ refuse at plan time.** If the cheapest exact arm is estimated above the
  query's time budget, the statement fails with: the file count, bytes, estimate, the budget, and
  `SET nprobe = n` as the remedy. This matches the standing rule that an index-eligible query we
  cannot run properly is refused and never degraded (no fallback). It replaces a timeout with an
  answer in milliseconds. The budget source and threshold are a ruling (D-V3).
- EXPLAIN and telemetry add: arm per file (F-ranges / C-scan / probe), bytes planned vs read,
  candidate count, rows refined. `IndexSearchStats` already carries `bytes_read`, `requests`,
  `rows_scored`.

---

## 5. Measurement plan (baseline before the first edit; nothing below is assumed)

| # | Question | How | Decides |
|---|---|---|---|
| **M0** | Why do exact queries time out? Bandwidth-bound or wave-bound? | Run the existing exact search on tweets from a Cloud Run-shaped worker; record `bytes_read`, `requests`, wall, achieved MB/s, in-flight depth. Compare to a raw parallel range read of the same object. | Whether bytes are the bottleneck at all; whether wave barriers cost more than the 2x. **Do first.** |
| **M1** | Does `err` stay near 0.007 on real embeddings? | Offline script over NVD and tweets fp16 vectors (read with rugo, no numpy in the repo; `dev/`): quantise, compute `err`, check `lower ≤ kernel ≤ upper` on 10^7 random pairs. | The bound is real. A single violation kills the exact claim. |
| **M2** | What fraction of rows survive `lower ≤ τ` for k = 1, 10, 100? | Same script, 100 queries drawn from the corpus and from unrelated text. | Whether the exact path gets near the ideal 0.5x or is eaten by candidates. If survivors exceed ~5%, C-scan buys little. |
| **M3** | Do real filters give clustered survivors? | Survivor-mask gap histogram for typical WHERE clauses on tweets (date, user, language). | F-ranges vs C-scan in the cost model. |
| **M4** | Recall of the C-ranked approximate path at `nprobe` × `refine`. | `dev/vector_index_recall.py` (exists), tie-aware. | Whether `refine` needs a ruled default or must be explicit. |
| **M5** | A/B: v1 exact vs v2 exact vs v2 filtered, GCS cold/warm, Mac and x86, ABBA interleaved. | Only after M0-M4 pass. | Land or delete (slower = deleted). |

M0-M3 are read-only and need no engine change. If M2 shows poor pruning, Section C stays only for
the approximate path and the exact path relies on F-ranges and the refusal; the design degrades
in scope, not in correctness.

---

## 6. Migration: convert, never re-embed

Re-embedding tweets costs 1h42m on the Mac for 7.9M rows (~1,290 rows/s); the vectors do not
change, only their layout. A v1 → v2 **conversion** reads the v1 file (ordinals + fp16 vectors),
writes F in ordinal order, derives C from the same vectors and the existing centroids, and
produces a new object with a new nonce. No model, no embedding identity change. The manifest row
swaps the index file path/sizes in one commit like a refresh. v1 files keep working through the
v1 reader until converted, and `SHOW INDEXES` reports the version. Whether v1 is read after the
cutover or refused is a ruling (D-V4); the standing preference is no silent dual behaviour.

Build for new data: pass 2 (embedding) writes F in ordinal order as it goes and records the
cluster id per row (4 B/row); pass 3 builds C from F (local stage, `VECTOR_INDEX_STAGE_DIR`) so
the embedding work is never repeated. A GCS build still streams into one resumable session,
F then C, so the object is written once and sequentially. Open implementation point: embed
workers may complete out of order, so the F writer must place by ordinal, not by arrival.

Compaction carry (`vector_index_carry.hpp`, D-14) rebuilds F by ordinal mapping and C by
re-clustering; it does not embed. Its row mapping already exists.

---

## 7. Decisions for the architect

| # | Decision | Options | Recommendation |
|---|---|---|---|
| **D-V1** | At-rest growth of 1.5x (billed at logical bytes) for a compact tier | accept / 4-bit codes for approximate only (smaller, no exact bound) / defer | Do M0-M2 first. Accept only if M2 shows pruning; the filter benefit (F-ranges) needs the F layout regardless, and F alone costs +0%. |
| **D-V2** | Ordinal-ordered full section replacing cluster-ordered full vectors | yes / no | **Yes.** It fixes the filter defect (§1.2) and is byte-neutral. Needs the zero-slot rule for unsearchable rows. |
| **D-V3** | Refuse over-budget exact searches at plan time | refuse with the remedy / run and risk the timeout / a session variable to raise the budget | **Refuse.** Fail fast, no silent degrade. The budget source (query timeout vs a config value) is yours. |
| **D-V4** | v1 files after v2 ships | convert and refuse v1 / read both | Convert-then-refuse (no dual behaviour); the converter is the migration. |
| **D-V5** | Sub-8-bit or PQ codes | not now / approximate path only / exact path too | **Not now.** Only the approximate path could use them, and only with the same explicit `SET`. Sub-8-bit with probabilistic bounds changes what "exact" means. |
| **D-V6** | `refine` factor for the approximate path | explicit `SET` / fixed constant | Explicit, with the measured recall curve (M4). No arbitrary default (the "where is 32 from" failure). |

---

## 8. Test plan

- **Bound property test:** for random and adversarial vectors (outlier dimensions, near-duplicate
  rows, zero-variance dimensions), `lower ≤ kernel ≤ upper` for every row. Mutation: shrink
  `err` by 1% and the test must fail.
- **Equivalence:** v2 exact vs v1 exact over the NVD fixture: identical ordinals, identical
  distance bits, for k in {1, 10, 100}, with deletes, with WHERE, with tied distances at the
  boundary.
- **Filter IO:** a selective clustered WHERE must read ≤ survivor bytes + gap slack; asserted on
  `bytes_read`, not on time.
- **Refusal:** the over-budget statement fails at plan time with the numbers; the same statement
  under `SET nprobe` runs.
- **Conversion:** v1 → v2 conversion of a fixture yields the same top-k as the v1 file.
- `make q`, then `make slt`, plus `make dt`/`st`/`rt` since native layouts change.

---

## 9. Not proposed

- Lance, or any second table format (see the 2026-10-25 assessment: the mechanism is a compact
  tier plus a candidate rerank, available inside `.vidx`).
- HNSW or any graph index (deleted; needs the whole file resident).
- Caching 6 GB of vectors in the worker (8 GiB total, shared with execution).
- Dimension reduction or changing the embedding model (changes index identity, rebuilds every
  index).
