# Draken Shuffle Step Design (Rebased on Implemented Capabilities)

## Why This Rewrite Exists

We started from the high-level execution problem (large group-by/sort style workloads), identified shuffle as a core capability, then implemented enabling primitives bottom-up.

This document now walks back up the stack and answers:

1. What tools now exist and are usable.
2. How shuffle should be designed using those tools.
3. Which tools are still missing for a complete operator.

---

## Problem Statement

Shuffle is a reusable execution step that partitions rows deterministically into bins so downstream work can be split safely and predictably.

Primary goals:

1. Deterministic row-to-bin mapping.
2. Memory-bounded operation with spill.
3. Fast spill/replay using Draken-native serialization.
4. Backend-agnostic storage path.

Out of scope for this document:

1. Planner wiring to group-by/sort/join.
2. Distributed cross-node shuffle protocol.

---

## What We Have (Implemented)

## 1) DRKM Morsel Serde (usable now)

Implemented and tested:

- `write_morsel(None, morsel, options)` returns DRKM bytes.
- `read_morsel(payload)` reads from bytes-like buffers.
- codecs available in this path include `none`, `lz4`, `zstd`.

Implication for shuffle:

- Spill format is concrete and buffer-native.
- No Arrow conversion is required in spill hot path.

## 2) Layered KVStore (usable now)

Implemented and configurable:

- up to 3 layers (for example `memory:// -> valkey:// -> gs://`).
- per-layer `max_bytes` threshold routing.
- optional global key prefix (`KVSTORE_KEY_PREFIX`).
- default store/layers from config (`KVSTORE_LOCATION`, `KVSTORE_LAYERS`).
- memory pools can be prewarmed (`KVSTORE_PREWARM_MEMORY_POOLS`).

## 3) Scoped KV write contract (important)

Factory-created KV stores now enforce context on writes:

- required context fields: `query_id`, `operator_id`.
- this is enforced on `set(...)` through `ScopedKeyValueStore` wrapping.

Implication for shuffle:

- Any spill write path must provide `query_id` and `operator_id`.
- Shuffle should use `create_kv_store(...)` and not instantiate raw backend stores directly.

---

## Updated Shuffle Design

## Operator Contract

New physical operator: `ShuffleNode`.

Input:

- Draken morsels.
- partition columns.
- bin settings.

Output (v1):

- replayed morsel stream in bin order.

Parameters:

- `columns`
- `num_bins` (power-of-two)
- `shift_bits` (default `0`)
- `memory_budget_bytes`
- `target_bin_buffer_bytes`
- `spill_enabled`
- `spill_codec_default` (`lz4` default)
- `spill_store` (BinStore adapter)

## Hash and Bin Strategy

For each row hash `h`:

- `bin_id = h & (num_bins - 1)`
- optional repartition pass: `(h >> shift_bits) & (num_bins - 1)`

This preserves the mask/shift model (`2/4/8/16/...`) with no `%`.

## BinStore Contract (revised to match current KV)

Shuffle-facing API:

- `put_chunk(raw_key: bytes|str, payload: bytes|memoryview, *, query_id: str, operator_id: str)`
- `get_chunk(raw_key, *, query_id: str, operator_id: str) -> bytes|memoryview|None`
- `append_manifest(bin_key, chunk_meta, *, query_id: str, operator_id: str)`
- `iter_manifest(bin_key, *, query_id: str, operator_id: str) -> ordered chunk_meta[]`
- `delete_scope(scope_key, *, query_id: str, operator_id: str)`

KV calls under the adapter:

- `set(key, value, query_id=..., operator_id=...)`
- `get(key, query_id=..., operator_id=...)`
- `delete(key, query_id=..., operator_id=...)`

Note:

- Do not embed `query_id` or `operator_id` into raw keys. Scoped KV already namespaces keys with those fields.
- Keep raw keys logical and short.

## Key Convention (raw key layer)

Recommended raw keys:

- chunk: `pass/{pass_id}/bin/{bin_id}/chunk/{chunk_seq}`
- manifest segment: `pass/{pass_id}/bin/{bin_id}/manifest/{segment_seq}`
- manifest index: `pass/{pass_id}/bin/{bin_id}/manifest/index`

Full persisted key is composed by KV prefix + scoped context + raw key.

## Manifest Strategy (v1)

Use append-only manifest segments, not read-modify-write on a single blob.

Write path:

1. write chunk payload
2. write immutable manifest segment entry
3. update small index pointer/list

Read path:

1. load index
2. read referenced segments in order
3. replay chunks in sequence order

This avoids lost updates when multiple flush events happen for a bin.

Output contract to downstream step:

- Shuffle produces one manifest per bin (up to `num_bins` manifests).
- Next step consumes bin manifests and replays associated chunks.

## Spill Payload Contract

Each spilled chunk payload is one full DRKM morsel serialization:

- serialize: `payload = write_morsel(None, morsel, options)`
- deserialize: `morsel = read_morsel(payload)`

No extra outer compression envelope.

---

## Execution Flow

## Input Phase

1. ensure input morsel is Draken (`ensure_draken_morsel`).
2. hash partition columns.
3. derive per-bin row index sets.
4. materialize per-bin fragments.
5. keep fragments in-memory until thresholds trigger spill.
6. spill fragments as DRKM chunks through BinStore with required context.

## End-of-Stream Phase

1. flush remaining in-memory bins.
2. for each bin, replay in-memory chunks + spilled chunks in deterministic order.
3. emit morsels downstream.

---

## Memory and Spill Policy

Controls:

- `shuffle_memory_budget_bytes`
- `shuffle_target_bin_buffer_bytes`
- `shuffle_num_bins_default`
- `shuffle_spill_enabled`
- `shuffle_spill_codec_default`

Policy:

1. spill hottest bin first (largest buffered bytes)
2. continue spilling until under budget

Layer placement is delegated to layered KV thresholds.

---

## Failure and Cleanup

Failure conditions:

- KV write/read failure
- DRKM decode/validation failure

Required behavior:

- fail query with bin/chunk context
- best-effort cleanup by scope
- cleanup uses manifest index traversal (explicit key list), not prefix delete assumptions

---

## Capability Map: Solved vs Missing

## Solved

1. DRKM bytes serde for spill/replay.
2. Layered KV with memory-first routing.
3. Global memory pool lifecycle and prewarm.
4. Enforced write scoping (`query_id`, `operator_id`) for factory-created stores.

## Missing (must be built for ShuffleNode)

1. Draken partition kernel producing `row_indexes_by_bin`.
2. Bin materialization kernel (row-index -> morsel fragment).
3. Concrete `BinStore` adapter implementing manifest/index semantics.
4. `ShuffleNode` physical operator implementation.
5. Planner/logical step registration (`LogicalPlanStepType.Shuffle`) when ready.
6. Shuffle telemetry integration and counters.
7. Integration tests for spill/replay, ordering, and cleanup.

---

## Rollout Plan

## Phase 1: In-memory shuffle

- partition + per-bin buffering
- deterministic replay

## Phase 2: DRKM spill via BinStore

- DRKM chunk write/read
- manifest index and cleanup

## Phase 3: Parallel replay/consume

- bounded bin workers
- deterministic output guarantees retained

---

## Decisions (Locked)

1. Manifest encoding:
- Use `json` for manifest segment/index payloads in v1.
- Rationale: internal preference + existing fast JSON decode path (`simdjson`) is sufficient.

2. `num_bins` policy:
- Use cardinality/volume-aware bin selection with an upper cap of 16 bins.
- Source for `n_rows`:
  use planner/runtime statistics row-count or row-count estimate for the input.
- Formula (log-scaled score, then quantized to power-of-two bins):
  `raw_bins = min(max(ceil(log2(n_rows)) - 16, 1), 16)`
  `num_bins = round_down_to_set(raw_bins, {1, 2, 4, 8, 16})`
- Use this as a v1 heuristic and tune with telemetry later.
- Examples:
  `1k -> 1`, `10k -> 1`, `100k -> 1`, `1m -> 4`, `10m -> 8`, `100m -> 8`, `1b -> 8`

3. Hash seed policy:
- No explicit seed policy for shuffle v1.
- Use existing high-performance Draken morsel hash routines as-is.

4. Replay ordering contract:
- No ordering contract for shuffle output in v1.
- Replay may be opportunistic; SQL semantics do not require deterministic row order without `ORDER BY`.

5. Chunk key uniqueness:
- Chunk keys include a monotonic element (per-bin `chunk_seq`) and may include an additional random suffix.
- Goal: allow safe repeated appends to a bin without key collision.

6. Operator identity source:
- `operator_id` is the existing operator UUID/identity from the plan node.
- Shuffle does not introduce a new operator identity scheme.

7. Telemetry:
- Keep the telemetry set defined in this document as accepted for v1.

8. Delivery approach:
- TDD-first implementation (tests precede feature slices).
