# Draken ARRAY_AGG Design

## Context

`ARRAY_AGG` already exists as a SQL surface in the legacy grouped aggregation path.

Current behavior includes support for:

- `ARRAY_AGG(expr)`
- `ARRAY_AGG(DISTINCT expr)`
- `ARRAY_AGG(expr LIMIT n)`
- `ARRAY_AGG(expr ORDER BY expr [ASC|DESC] LIMIT n)`

The new Draken aggregate/group-by path does not yet support `ARRAY_AGG`.

This document defines the first implementation shape for `ARRAY_AGG` in the Draken/native aggregate stack.

## Goals

1. Add `ARRAY_AGG` to the Draken aggregate/group-by path.
2. Preserve current SQL semantics for the supported `ARRAY_AGG` forms.
3. Implement `ARRAY_AGG` natively in the Draken/carchar grouped path rather than routing through legacy fallback machinery.
4. Keep the first implementation mergeable and structurally compatible with future spill support.
5. Bound memory with an explicit arena budget and fail cleanly if the collector exceeds it.

## Non-Goals

1. Spill-safe list aggregation in v1.
2. `ORDER BY` on a different expression than the aggregated value in v1.
3. Multi-expression `ORDER BY` inside `ARRAY_AGG` in v1.
4. `FILTER` support in v1.

## Why This Is Different

`ARRAY_AGG` is not a scalar aggregate.

Unlike `SUM`, `MIN`, or `COUNT`, the state grows with the number of rows in each group. That makes it structurally different from the current carchar fast-path aggregates, which assume compact scalar state per group.

It also carries aggregate-local modifiers:

- `DISTINCT`
- `ORDER BY`
- `LIMIT`

Those modifiers are already represented on the aggregate expression node:

- `duplicate_treatment`
- `order`
- `limit`

The implementation should preserve those semantics rather than introducing a simplified list collector.

## Current Behavior Reference

The legacy Arrow-based grouped path already treats `ARRAY_AGG` specially.

Observed behavior:

1. Grouping is performed first.
2. `ARRAY_AGG` columns are post-processed after grouping.
3. `ORDER BY` and `LIMIT` are applied at that post-processing stage.

This current implementation is a behavior reference, not the target execution strategy.

## Recommended v1 Architecture

Implement `ARRAY_AGG` as a dedicated carchar-native aggregate with arena-backed variable-width state.

That means:

- supported by `DrakenAggregateNode`
- supported by `DrakenAggregateAndGroupNode`
- admitted into the native grouped aggregate path
- not routed through `GroupStateStore`

The design target is append-friendly collection, bounded allocator ownership, and cheap finalization for the common unordered case.

## Aggregate Surface

The first implementation should support:

- `ARRAY_AGG(expr)`
- `ARRAY_AGG(DISTINCT expr)`
- `ARRAY_AGG(expr LIMIT n)`
- `ARRAY_AGG(DISTINCT expr LIMIT n)`
- `ARRAY_AGG(expr ORDER BY expr)`
- `ARRAY_AGG(expr ORDER BY expr DESC)`
- `ARRAY_AGG(expr ORDER BY expr LIMIT n)`
- `ARRAY_AGG(expr ORDER BY expr DESC LIMIT n)`

The first implementation should reject:

- `ARRAY_AGG(expr ORDER BY other_expr)`
- `ARRAY_AGG(expr ORDER BY expr1, expr2)`
- `ARRAY_AGG(...) FILTER (...)`

## Semantics

### Base `ARRAY_AGG`

- Preserve encounter order.
- Include `NULL` values.

### `DISTINCT`

- Deduplicate by value.
- Preserve first-seen order before any final sort.
- `NULL` collapses to a single `NULL`.
- Apply `DISTINCT` on entry, not as a post-collection pass.
- The executor should support per-group distinct state directly.
- A later optimizer may rewrite `ARRAY_AGG(DISTINCT x)` as pre-dedup on `(group_keys, x)` where valid, but that is not required for correctness.

### `ORDER BY`

- Only support ordering by the same expression being aggregated in v1.
- Sort at finalize time.
- Respect ascending/descending direction.

### `LIMIT`

- Apply after the final ordering semantics are resolved.
- If no `ORDER BY` is present, limit the preserved encounter-order list.
- For unordered forms, enforce limit during collection and stop appending after the limit is reached.
- For ordered forms, do not stop collection early unless a dedicated top-k structure is implemented.

### Combined semantics

Recommended execution order:

1. ingest values
2. apply distinct during ingestion if required
3. finalize ordering if required
4. apply limit

## State Model

Implement `ARRAY_AGG` with two layers of native state:

1. An operator-owned append arena for collected items.
2. A compact per-group header stored in the grouped aggregate state table.

### Arena

The arena is append-only and shared by all groups in the aggregate operator.

Requirements:

- allocate in slabs/pages rather than one eager monolithic allocation
- enforce a hard memory ceiling, for example `1 GiB` per aggregate operator
- fail with a resource error if the ceiling is exceeded
- support reset between queries

The arena should be able to store:

- fixed-width inline values where practical
- offsets/lengths into a value payload region for variable-width values
- block headers for chained collection blocks

### Per-group header

Suggested fields:

- `first_block_index`
- `last_block_index`
- `count`
- `unique_count`
- `distinct_state_index` or `0` when unused
- `flags`

`flags` should at least encode:

- `distinct`
- `ordered`
- `descending`
- `truncated_by_limit`

### Blocked collection layout

Do not store each appended value as an individual linked-list node.

Instead, use a linked chain of small fixed-capacity blocks. Each block contains:

- `next_block_index`
- `used`
- `item[BLOCK_CAPACITY]`

This keeps append O(1) while avoiding one pointer per collected value.

`BLOCK_CAPACITY` should be `8`.

Reasoning:

- smaller than `10`, so tail waste is lower for the many small groups we expect
- larger than `5`, so block metadata overhead does not dominate
- power-of-two sizing keeps allocation and indexing simpler than `5` or `10`

If later profiling shows a better value, this should remain a tunable constant.

### Item layout

For fixed-width values, store the value inline in the block slot.

For variable-width values, store:

- `value_offset`
- `value_length`
- `is_null`

in the block slot, with bytes written to the value arena.

### Distinct state

`DISTINCT` should be handled on entry.

Recommended approach:

- per-group hash state keyed on the aggregated value
- if the value is new for the group, append it
- if the value is already present, skip it

This keeps collection semantics simple and makes unordered `DISTINCT ... LIMIT n` cheap, because collection can stop after `n` unique values.

### Required operations

- `append_value(group_id, value)`
- `append_repeated_value(group_id, value, count)`
- `merge_group_state(target_group, source_group)`
- `finalize_group(group_id) -> array`
- `reset()`

## Ordering Constraint

The key simplification for v1 is:

- only allow `ORDER BY` when the order expression is the same as the aggregated expression

This avoids needing state shaped like:

- `[(sort_key, value), ...]`

and avoids a wider metadata and merge problem in the first implementation.

If later support is needed for `ORDER BY other_expr`, the state should be promoted to paired row tuples rather than retrofitting that into the first version.

## Planner and Operator Integration

### `DrakenAggregateAndGroupNode`

Add `ARRAY_AGG` to:

- supported aggregate set
- normalization logic

Extract aggregate-local options into `AggregationSpec.options`, for example:

- `distinct`
- `limit`
- `order_direction`
- `ordered`

### `DrakenAggregateNode`

Support global `ARRAY_AGG` with the same arena and block-chain machinery, using a single synthetic group.

### Native aggregate kernels

Add a new aggregate code for `array_agg` in the carchar/native aggregate kernels.

The grouped state row should store only the per-group header. The arena and distinct hash tables are operator-owned side structures referenced by those headers.

### Planner note for `DISTINCT`

The base implementation should keep `DISTINCT` semantics inside the aggregate itself.

A later optimization may introduce a pre-group dedup step on `(group_keys, aggregate_expr)` for `ARRAY_AGG(DISTINCT ...)`, but that is optional and should not be baked into the initial executor design.

## Memory Model

`ARRAY_AGG` is variable-memory but should not be unbounded at runtime.

v1 should explicitly implement:

- correctness first
- no spill support
- a hard arena cap
- graceful failure when the cap is exceeded

The preferred model is a slab-backed arena with a configurable ceiling, defaulting to `1 GiB` per aggregate operator.

## Telemetry

Add counters/readings for:

- `array_agg_groups`
- `array_agg_total_elements`
- `array_agg_max_group_length`
- `array_agg_distinct_sets_created`

These will matter before spill exists because memory pressure is the main operational risk.

## Merge Semantics

The state must remain mergeable even before spill is implemented.

Recommended merge behavior:

- unordered non-distinct mode: append source blocks into the target chain
- unordered distinct mode: replay source values through distinct-on-entry append
- ordered mode: defer sorting until finalize in v1

This keeps state combination simple and avoids repeated sorting in intermediate merges.

## Implementation Plan

### Phase 1: Global aggregate

Add the native arena, block-chain collector, and support:

- `ARRAY_AGG(expr)`
- `ARRAY_AGG(DISTINCT expr)`
- `ARRAY_AGG(expr LIMIT n)`

through `DrakenAggregateNode`.

### Phase 2: Grouped native support

Add `array_agg` to:

- `DrakenAggregateAndGroupNode`
- `AggregationSpec.options`
- native aggregate kernels
- grouped finalize path

### Phase 3: Ordered support

Support:

- `ARRAY_AGG(expr ORDER BY expr)`
- `ARRAY_AGG(expr ORDER BY expr DESC)`
- same-expression only

### Phase 4: Hardening

Add:

- telemetry
- arena cap enforcement tests
- larger-group tests
- parity tests for `DISTINCT`, `LIMIT`, and ordered forms

## Acceptance Criteria

1. Parity with current SQL battery coverage for the supported forms.
2. Stable behavior for `DISTINCT`, `LIMIT`, and same-expression `ORDER BY`.
3. Native Draken/carchar execution for grouped `ARRAY_AGG`.
4. Mergeable state shape compatible with future spill work.
5. Graceful failure on arena exhaustion.

## Explicit v1 Decision

The first implementation should be:

- semantically correct
- mergeable
- native to the Draken/carchar aggregate path
- bounded by an arena budget with clear failure semantics

It should not attempt to be:

- spill-safe
- fully general for arbitrary `ORDER BY` expressions
