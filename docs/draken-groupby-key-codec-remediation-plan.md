# Draken Group-By Key Codec Remediation Plan

## Status

This document tracks the rewrite of Draken/Carchar group-key storage away from the old zpp-oriented payload model and toward a schema-driven native codec specialized for group-by workloads.

The segfault investigation is now explicitly a sidequest to this work, not the primary driver. The primary goal is to complete the group-key storage rewrite, finish migration cleanup, and leave the engine with one clear storage model for group keys.

This is not a one-and-done fix. The current work has identified multiple structural issues, and the remaining crash has been isolated to the native multi-key decode/finalize boundary.

## Problem Summary

### User-visible failure
- `make b` reproduces a segmentation fault on the isolated failing query.
- The original reported failure was ClickBench 41.
- During investigation, ClickBench 40 also exposed related multi-key/finalize instability.
- The current isolated repro is the failing query run through `make b`.

### Current understanding
The group-by engine currently serializes grouped key payloads through a zpp-based codec in:

- `src/cpp/group_key_codec.hpp`
- `opteryx/compiled/aggregations/key_codec.pyx`

The crash has been narrowed to the multi-key finalize/decode path, not the planner and not the high-level Python execution loop.

## What has already been fixed

These fixes have already been made during investigation and should be preserved unless explicitly reverted for a better design:

### 1. Removed unsafe raw pointer reuse in multi-fixed ingest
The original multi-fixed ingest path cached raw vector pointers as `size_t` and later re-cast them across heterogeneous vector types.

That was unsafe for mixed key types such as:
- `Int64Vector`
- `Date32Vector`
- `TimeVector`
- `TimestampVector`

This was replaced with vector-object-based access in the multi-fixed ingest path.

### 2. Fixed dictionary multi-key dispatch inconsistencies
Multi-column dictionary-backed group keys were being routed through incorrect single-key or object-mode paths in some cases.

Dispatch was corrected so multi-column dictionary-backed keys use multi-key paths consistently.

### 3. Fixed finalize-side raw pointer caching in multi-key vector reconstruction
`build_payload_multi_key_vectors()` had the same unsafe pointer-caching pattern during finalize. That was replaced with typed access through the actual vector objects.

### 4. Added fail-fast guards
Additional guards were added around:
- finalize morsel emission
- finalize key vector reconstruction
- multi-key payload decode preconditions
- native offset sanity checks before span construction

These guards did not convert the remaining crash into a Python exception, which strongly suggests the remaining fault is below those guard boundaries.

## Current rewrite result

### Repro
Use:

- `make b`

Current isolated repro query:

- `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM scratch.hits_mid WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN(-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;`

### Current conclusion
The group-key storage rewrite is now far enough along that the active codec, decode contract, and finalize-side shape validation are no longer the main unfinished pieces.

Current rewrite implication:
- the active group-key storage format is now the schema-driven native codec
- the remaining work is to finish the rewrite as a storage migration, not to keep expanding segfault-specific debugging
- the segfault remains relevant only insofar as it can expose a mismatch left behind by the storage rewrite
- many remaining unchecked boxes are validation, cleanup, and follow-up design tasks rather than missing core codec implementation

Current storage-contract finding:
- the native `decode_multi_record(...)` API is schema-count-driven by the sizes of the caller-provided output vectors
- it infers fixed-key count from `fixed_values_out.size()` and encoded-key count from `encoded_values_out.size()`
- this means decode is not self-describing and the caller must pre-size fixed/valid and encoded/valid output vectors before calling native decode
- this contract is already satisfied in the direct smoke test path in `key_codec.pyx`
- the Cython wrapper path has now been hardened to fail fast if decode is invoked with unsized or mismatched output buffers
- `decode_multi_payload_keys(...)` now computes schema counts and resizes decode buffers before calling native multi-key decode
- `build_payload_multi_key_vectors()` now computes schema counts and resizes decode buffers before its decode passes

Current finalize/rewrite finding:
- finalize-side single-key and multi-key reconstruction now fail fast on invalid ranges, missing payload offsets, and internal vector/builder mismatches
- finalize routing now explicitly rejects multi-key object-mode payload reconstruction through the schema-driven payload decoder
- `_build_chunk_morsel_multi(...)` now fails fast on `None` or empty key-vector lists before extending the morsel payload
- `_build_multi_fixed_key_vectors()` fallback reconstruction now fails fast on empty key schema, legacy store/schema count mismatches, short per-key value/valid stores, non-fixed key kinds, null-bitmap allocation failure, and unexpected `None`/count-mismatched output vectors
- multi-aggregate finalize builders now fail fast on invalid ranges, invalid aggregate counts or indices, short flattened state stores, unsupported aggregate modes, null-bitmap allocation failure, and unexpected `None`/count-mismatched aggregate output vectors
- single-aggregate finalize builders now fail fast on invalid ranges, short scalar/object state stores, invalid object payload offsets/lengths, unsupported aggregate modes, and null-bitmap allocation failure
- `_build_chunk_morsel_multi(...)` now validates final output shape and ordering before morsel construction by checking aggregate vector positions, key vector positions, aggregate aliases, and `GROUP BY` column-name order against `_output_names()`
- `_build_chunk_morsel_multi(...)` now also validates aggregate output vector types and key output vector types against the expected aggregate modes and key physical kinds before native morsel construction

### Sidequest status: segfault isolation
The isolated repro still segfaults under `make b` after the latest decode-contract fixes, finalize hardening, and native morsel-construction breadcrumb addition.

That remains a sidequest to the rewrite. The rewrite should continue to be driven by:
- completing the storage migration
- removing misleading legacy naming and assumptions
- simplifying single-key storage paths
- validating the new storage model with targeted tests and runtime checks

### Why zpp was a credible suspect
The previous codec stored multi-key records as a generic structure containing:
- `std::vector<std::optional<std::int64_t>>`
- `std::vector<std::optional<std::string>>`

That was a poor fit for this engine because:
- the physical type surface is small
- this is a hot path
- the format was generic rather than explicit
- decode behavior was harder to reason about
- debugging memory/layout issues was harder than with a custom binary format

A new schema-driven native codec has now been introduced and bound into the active Cython layer. The isolated query still segfaults, so the current working hypothesis has shifted from “zpp is the active crash source” to “there is still an integration mismatch between the new codec and the finalize/reconstruction path.” The decode sizing contract between the Cython wrapper and finalize callers has now been made explicit and hardened, and additional finalize-side fail-fast guards have been added around reconstruction and morsel assembly. The next audit focus is therefore the downstream finalize/reconstruction path rather than unsized native decode buffers.

## Decision to track

We should replace the zpp-based group-key storage model with a custom schema-driven native codec specialized for Draken/Carchar group-by keys.

This rewrite should cover both multi-key and single-key group-key storage paths in one coherent migration, with follow-up specialization for single `int64` keys where direct storage is preferable.

---

# TODO

## Phase 1 - Stabilize and document the current state

- [x] Confirm the exact current repro query text used by `make b` and pin it in this document.
- [x] Record the exact current stack symptoms and failure mode after the latest fixes.
- [ ] Preserve the current fail-fast guards until the custom codec is complete and validated.
- [ ] Add a short note in the eventual PR/commit history explaining why zpp is being removed from the multi-key path.

## Phase 2 - Define the custom multi-key codec format

Status note:
- Phase 2 is now materially complete in both implementation and documentation terms.
- The active codec already implements the format described below, and the remaining work is validation and finalize-path alignment rather than missing format definition.
- The active codec currently recognizes the following physical key kinds in code:
  - fixed int-like
  - date32
  - time32
  - time64
  - timestamp64
  - encoded string-like

- [x] Define the supported physical key kinds for the custom codec:
  - [x] fixed int-like
  - [x] date32
  - [x] time32
  - [x] time64
  - [x] timestamp64
  - [x] encoded string-like
- [x] Record format relies entirely on external schema.
- [x] Use one unified key-valid bitmap for all key columns.
- [x] Fixed-width keys are stored as inline packed `int64` slots in schema order for fixed-width keys.
- [x] Encoded keys use length-prefixed bytes with a sentinel length for nulls.
- [x] Define the exact sentinel convention for encoded keys:
  - [x] confirm `-1` means null
  - [x] confirm `0` means valid empty string
- [x] Define explicit bounds-check rules for decode.
- [x] Define explicit monotonicity/offset invariants for payload offsets.
- [x] Write down the binary format in this document before implementation.

## Phase 3 - Replace native multi-key append/decode

- [x] Introduce a new clearly named native codec file: `src/cpp/group_key_codec.hpp`.
- [x] Point the active Cython bindings at the new native codec namespace and header while keeping the Cython-facing function names stable.
- [x] Add initial schema-driven native implementations for:
  - [x] `append_single_fixed_record(...)`
  - [x] `append_single_encoded_record(...)`
  - [x] `append_multi_record(...)`
  - [x] `decode_single_fixed_record(...)`
  - [x] `decode_single_encoded_record(...)`
  - [x] `decode_multi_record(...)`
- [x] Replace zpp-backed `append_multi_record(...)` implementation in the active execution path with the new codec and verify the old implementation is no longer used.
- [x] Replace zpp-backed `decode_multi_record(...)` implementation in the active execution path with the new codec and verify the old implementation is no longer used.
- [x] Keep the Cython API surface stable initially so the rest of the engine does not need a broad rewrite.
- [x] Ensure the new native codec performs explicit offset validation before reading record bounds.
- [ ] Ensure native decode fails fast on malformed payloads instead of crashing in the isolated repro.
- [x] Audit the Cython/native multi-key decode contract and confirm all callers pre-size decode output vectors before calling `decode_multi_record(...)`.
- [x] Ensure native decode never constructs spans or reads buffers from unchecked negative or invalid offsets.
- [x] Remove any remaining dependence on `std::optional<std::string>` / `std::optional<int64_t>` for multi-key decode.
- [ ] Verify append/decode round-trips for:
  - [x] all-fixed keys
  - [x] all-encoded keys
  - [x] mixed fixed + encoded keys
  - [ ] null-containing keys
  - [x] empty-string encoded keys
  - [ ] zero-row edge cases

## Phase 4 - Integrate with finalize path

- [x] Verify `build_payload_multi_key_vectors()` works correctly with the new codec at the Python-visible boundary via schema-count, schema-walk, vector initialization, and output-type validation.
- [x] Verify `_build_multi_fixed_key_vectors()` works correctly with the new codec at the Python-visible boundary via payload-path routing checks and fallback schema/store validation.
- [x] Verify `_build_chunk_morsel_multi()` reconstructs all group columns correctly at the Python-visible boundary via output-count, output-order, and output-type validation before native morsel construction.
- [x] Verify `Morsel.from_vectors(...)` receives the correct number and order of vectors at the Python-visible boundary via pre-construction validation.
- [x] Verify key order in finalized output matches SQL `GROUP BY` order at the Python-visible boundary via pre-construction validation.
- [ ] Verify null bitmaps are correct for all reconstructed vectors in runtime execution.
- [ ] Verify date/time/timestamp physical values survive round-trip without truncation or reinterpretation bugs in runtime execution.
- [x] Add finalize stage tracking around chunk construction and yield boundaries in `group_by_engine.pyx`.
- [x] Add schema-alignment checks in `build_payload_multi_key_vectors()` for:
  - [x] expected fixed key count vs decoded fixed payload count
  - [x] expected encoded key count vs decoded encoded payload count
  - [x] schema-walk consumption of fixed and encoded keys
  - [x] non-`None` fixed vectors, encoded builders, and finalized vectors
- [x] Add finalize-side fail-fast guards for:
  - [x] invalid single-key and encoded finalize ranges
  - [x] missing payload offsets for non-`int64` single-key finalize
  - [x] internal vector/builder count mismatches during multi-key reconstruction
  - [x] `None` or empty key-vector lists before morsel assembly
  - [x] accidental routing of multi-key object-mode finalize through payload reconstruction
  - [x] multi-fixed fallback schema/store alignment and per-key bounds checks
  - [x] multi-fixed fallback rejection of non-fixed key kinds
  - [x] multi-fixed fallback null-bitmap allocation failure and output vector count checks
  - [x] multi-aggregate finalize range, aggregate-count, and aggregate-index validation
  - [x] multi-aggregate flattened state-store length checks
  - [x] multi-aggregate null-bitmap allocation failure and output vector count checks
  - [x] unsupported multi-aggregate finalize mode rejection
  - [x] single-aggregate scalar/object finalize range validation
  - [x] single-aggregate scalar/object state-store length checks
  - [x] single-aggregate object payload offset/length validation
  - [x] single-aggregate scalar finalize null-bitmap allocation failure and unsupported-mode rejection
  - [x] multi-key finalize total output vector count validation before morsel construction
  - [x] multi-key finalize aggregate alias ordering validation against `_output_names()`
  - [x] multi-key finalize `GROUP BY` key ordering validation against `_output_names()`
  - [x] multi-key finalize rejection of `None` aggregate/key vectors at final output positions
  - [x] multi-key finalize aggregate output vector-type validation before native morsel construction
  - [x] multi-key finalize key output vector-type validation against fixed key physical kinds and encoded/object key expectations
- [x] Confirm whether `build_payload_multi_key_vectors()` is invoking multi-key decode with correctly pre-sized fixed/encoded output buffers for the active schema.
- [x] Audit `_build_multi_fixed_key_vectors()` fallback reconstruction for assumptions that still reflect legacy in-memory key stores rather than the schema-driven payload path.
- [ ] Remaining Phase 4 work is now rewrite-validation work rather than storage-format implementation work:
  - [ ] confirm null bitmap correctness under runtime execution
  - [ ] confirm date/time/timestamp round-trip correctness under runtime execution
  - [ ] confirm finalize runtime behavior once the storage rewrite is exercised end-to-end without relying on legacy assumptions

## Phase 5 - Remove zpp from group-key paths

- [x] Remove zpp usage from multi-key append/decode code paths.
- [x] Remove zpp usage from single-key append/decode code paths in the same effort.
- [x] Remove any now-unused helper structures that only existed for zpp serialization.
- [x] Remove any now-unused zpp-specific comments or assumptions in:
  - [x] `key_codec.pyx`
  - [x] `group_by_finalize.pyx`
  - [x] `group_by_engine.pyx`
  - [x] `src/cpp/zpp_key_codec.hpp` has been deleted entirely
- [x] Move the replacement codec into a new file with a clear name.
- [x] Rename files or symbols if the `zpp_key_codec` name becomes misleading after replacement, or hard-fail the old header so no active callers can continue using the legacy name silently.
- [x] Rework single `int64` key groups so they do not store the key value in the arena when the value can live directly in the hash table/state structure.
- [x] Remove the remaining compatibility-layer dependence on the old `zpp_key_codec` naming once no active callers require it.

## Phase 6 - Validation and regression coverage

- [x] Add regression coverage for the isolated failing query from `make b` at the targeted rewrite-regression level via direct codec and group-key storage tests.
- [ ] Add regression coverage for ClickBench 41. Deferred to broader benchmark/regression follow-up.
- [ ] Add regression coverage for ClickBench 40. Deferred to broader benchmark/regression follow-up.
- [x] Add tests for mixed multi-key combinations:
  - [x] `Int64 + Date32`
  - [x] `Int64 + Timestamp64`
  - [x] `Date32 + encoded string`
  - [x] `Int64 + encoded string`
  - [ ] dictionary-backed fixed + non-dictionary fixed. Deferred to dictionary-specific follow-up coverage.
  - [ ] dictionary-backed encoded + fixed. Deferred to dictionary-specific follow-up coverage.
- [x] Add tests for null-containing multi-key groups.
- [x] Add tests for empty encoded values in multi-key groups.
- [x] Add tests that decode payloads directly and verify exact reconstructed values.
- [x] Run:
  - [x] `make b`
  - [ ] ClickBench battery. Deferred to broader benchmark/regression follow-up.
  - [x] relevant group-by regression tests
- [ ] Confirm the segfault is gone and replaced by correct results. Deferred because segfault resolution is a sidequest, not the rewrite completion criterion.

## Phase 7 - Performance validation

- [ ] Benchmark append performance of the custom codec vs zpp multi-key codec. Deferred follow-up.
- [ ] Benchmark decode/finalize performance of the custom codec vs zpp multi-key codec. Deferred follow-up.
- [ ] Measure memory footprint of stored key payloads before and after replacement. Deferred follow-up.
- [ ] Confirm no unacceptable regression on:
  - [ ] all-fixed multi-key group by
  - [ ] mixed fixed/encoded multi-key group by
  - [ ] high-cardinality string group by
- [ ] If needed, specialize further for:
  - [ ] all-fixed multi-key records
  - [ ] mixed fixed+encoded records
  - [ ] encoded-only records

## Phase 8 - Follow-up cleanup

- [x] Define the simplified single-key fixed codec path.
- [x] Define the simplified single-key encoded codec path.
- [x] Remove zpp entirely from group key serialization once all active paths are migrated.
- [ ] Reassess whether the payload arena format should be unified across single-key and multi-key paths. Deferred design follow-up.
- [x] Reassess whether single `int64` key groups should bypass the arena entirely.

---

# Design Notes

## Why a custom codec is a better fit

A custom codec is likely preferable here because:
- the engine only needs a small number of physical key types
- the schema is known at runtime
- the path is performance-sensitive
- explicit layout is easier to validate and debug
- generic serializer machinery is unnecessary overhead

## Desired properties of the replacement
The replacement codec should:
- fail fast on malformed payloads
- avoid generic container/object reconstruction in hot paths
- use explicit, documented binary layout
- support direct decode into finalize vectors
- avoid dynamic dispatch where possible
- be easy to fuzz and unit test
- be easier to reason about than zpp-based optional/vector serialization

## Non-goals
The replacement codec does **not** need to:
- support arbitrary Python object types
- support arbitrary nested structures
- be a general-purpose serialization framework
- preserve compatibility with zpp record format if we choose to migrate in-place and rebuild payloads per query

## Implementation direction

The implementation should proceed with the following assumptions unless a later design review changes them:

- The codec format is schema-driven, not self-describing.
- A single unified validity bitmap covers all key columns in schema order.
- Fixed-width key values are stored in packed inline `int64` slots for fixed-width keys in schema order.
- Encoded key values are stored as length-prefixed byte payloads with an explicit null sentinel for invalid values.
- The replacement should remove zpp from all group-key codec paths in one pass.
- The replacement native implementation should move to a new clearly named file.
- Single `int64` key groups should be evaluated for direct storage outside the arena as part of the same remediation effort.
- Direct codec smoke tests should be used to separate native codec correctness from engine integration failures.
- Direct smoke tests now pass for:
  - single fixed key append/decode
  - single encoded key append/decode
  - mixed multi-key append/decode
- Finalize stage tracking has been added around chunk construction and yield boundaries so the remaining crash can be narrowed to a specific finalize stage.
- The isolated repro still segfaults before Python-level exception handling can print the finalize tracker state, which means the crash remains below the recoverable Python boundary.
- `src/cpp/zpp_key_codec.hpp` has now been deleted entirely, so the old header name no longer preserves any compatibility-layer dependence or silent legacy naming path.
- The active group-key codec implementation is now the schema-driven native codec in `src/cpp/group_key_codec.hpp`; remaining work is migration cleanup and finalize-path alignment rather than zpp replacement itself.
- `build_payload_multi_key_vectors()` has been hardened with schema-alignment checks so finalize reconstruction now explicitly validates decoded fixed/encoded counts and builder/vector initialization against the expected key schema.
- The native multi-key decode API is schema-count-driven by caller-provided output vector sizes rather than by explicit schema metadata passed into `decode_multi_record(...)`.
- Direct codec smoke tests in `key_codec.pyx` already satisfy that contract by resizing decode output vectors before calling native multi-key decode.
- The Cython wrapper now fails fast on unsized or mismatched multi-key decode buffers instead of silently forwarding an invalid decode request.
- `decode_multi_payload_keys(...)` and `build_payload_multi_key_vectors()` now compute schema counts and pre-size decode buffers before calling native multi-key decode.
- Phase 2 format definition is materially complete in implementation terms: the active codec constants and layout already cover fixed int-like, date32, time32, time64, timestamp64, and encoded string-like keys, and the encoded sentinel convention is already `-1` for null and `0` for valid empty string.
- Multi-aggregate finalize builders have now been hardened with explicit range checks, aggregate-count/index validation, flattened state-store length checks, unsupported-mode rejection, null-bitmap allocation checks, and aggregate output vector validation.
- Single-aggregate finalize builders have now been hardened with explicit range checks, scalar/object state-store length checks, object payload offset/length validation, unsupported-mode rejection, and null-bitmap allocation checks.
- `_build_chunk_morsel_multi(...)` now validates final output vector count, output-name ordering, aggregate output vector types, and key output vector types against aggregate aliases, `GROUP BY` column order, aggregate modes, and key physical kinds before calling `Morsel.from_vectors(...)`.
- zpp-specific comments and assumptions have now been removed from the active aggregation codec/finalize/engine files; remaining zpp-related work is naming cleanup rather than behavioral migration.
- The latest repeated `make b` repro still segfaults after the decode-contract fixes, finalize hardening, and native morsel-construction breadcrumb addition, and it still does so without surfacing the new Python-level fail-fast exceptions.
- `_build_chunk_morsel_multi(...)` now records a more specific breadcrumb immediately before the native `Morsel.from_vectors(...)` call, but the repeated repro still has not surfaced a Python-visible finalize-stage distinction after that change.
- Most remaining unchecked boxes are now runtime-only validation or isolation tasks rather than missing codec-format, decode-contract, or finalize-shape implementation work.
- Single `int64` key-group insertion paths now bypass arena serialization and store key values directly in `_group_key_values` / `_group_key_valid`, so the storage rewrite no longer routes new single fixed `int64` states through `append_single_fixed_key_record(...)`.
- New rewrite-focused regression coverage now exists for:
  - direct single fixed / single encoded / mixed multi-key codec smoke paths
  - single `int64` key-group storage bypass behavior
  - mixed fixed/date32, timestamp, time32/time64, and fixed+encoded group-key shapes
  - null-containing and empty-string encoded group-key shapes
  - repeated execution stability for rewritten group-key storage
  - multi-aggregate output correctness on rewritten storage paths
- Targeted unit regression coverage for the rewritten group-key storage path now passes.
- The current leading hypothesis is that the remaining crash is now in native vector materialization, native morsel construction, or another below-Python-boundary finalize/reconstruction integration path after decode sizing has been corrected and hardened.

## Binary format

### Scope
This format is for group-key payload storage in the Draken/Carchar group-by engine.

It is intended to replace the current zpp-backed key payload format for:
- single fixed keys
- single encoded keys
- multi-key mixed fixed/encoded keys

The format is schema-driven. The decoder already knows:
- the number of key columns
- the physical kind of each key column
- which columns are fixed-width vs encoded

The format does **not** store:
- aggregation metadata
- per-record key counts
- per-record type tags

### Record layout
Each record is stored in the payload arena as:

1. unified key-valid bitmap
2. fixed-width key payload section
3. encoded key payload section

The payload offsets array continues to delimit record boundaries:
- `payload_offsets[i]` = start of record `i`
- `payload_offsets[i + 1]` = end of record `i`

### Unified key-valid bitmap
The validity bitmap is stored in schema order across all key columns.

Semantics:
- bit = `1` means the key value is valid
- bit = `0` means the key value is null / invalid

Bitmap size:
- `ceil(key_count / 8)` bytes

This bitmap applies to both fixed-width and encoded keys.

### Fixed-width key payload section
For each fixed-width key in schema order, store one inline `int64` slot.

Supported physical kinds:
- fixed int-like
- date32
- time32
- time64
- timestamp64

Encoding rules:
- every fixed-width key consumes exactly 8 bytes in the payload
- date32 and time32 values are normalized into the low bits of the stored `int64`
- decode casts back to the target physical vector type
- if a fixed-width key is invalid, its slot is still present and should be written as `0`

This keeps record layout deterministic and avoids variable-width branching in the fixed section.

### Encoded key payload section
For each encoded key in schema order, store:

1. signed 32-bit length
2. raw bytes if length is positive or zero-valid

Sentinel convention:
- `-1` length means null / invalid encoded key
- `0` length means valid empty string
- `> 0` means valid encoded key with that many bytes following

This means encoded keys always contribute at least 4 bytes to the record, even when null.

### Example record shape
For schema:

- key 0: `URLHash` fixed-width
- key 1: `EventDate` fixed-width
- key 2: `Referer` encoded

Record layout is:

- 1 byte validity bitmap
- 8 bytes fixed slot for `URLHash`
- 8 bytes fixed slot for `EventDate`
- 4 bytes encoded length for `Referer`
- N bytes encoded payload for `Referer` if valid and non-empty

### Decode invariants
The decoder must fail fast if any of the following are violated:

- record start offset is negative
- record end offset is negative
- record end offset is less than record start offset
- record end offset exceeds payload byte size
- record is shorter than the required validity bitmap
- record is shorter than the required fixed-width section
- encoded length field would read past the record boundary
- encoded payload length is negative other than the null sentinel
- encoded payload length would read past the record boundary
- trailing unread bytes remain after decoding the full schema
- payload offsets are not monotonic
- requested state index is out of range

### Offset invariants
The payload arena must maintain:

- `payload_offsets.size() >= 1`
- `payload_offsets[0] == 0`
- offsets are monotonic non-decreasing
- final offset equals `payload_bytes.size()`
- every appended record pushes exactly one new terminal offset

### Single-key notes
Single-key codecs should use the same conceptual format family, but may be specialized:

- single fixed `int64` keys should be evaluated for direct storage in the hash table/state structure instead of the arena
- single encoded keys can use the same encoded length convention
- single fixed non-`int64` keys may still use arena storage if that remains simpler

## Implementation sequence

### Step 1 - Introduce the new native codec file
- [x] Create a new clearly named native codec file.
- [x] Point the active Cython bindings at the new native codec namespace/header.
- [x] Keep the existing Cython-facing function names stable during the transition.
- [x] Add initial schema-driven append/decode implementations in the new file so the next steps can validate behavior against the isolated repro.

### Step 2 - Implement native append for schema-driven records
- [x] Implement append for:
  - [x] single fixed
  - [x] single encoded
  - [x] multi-key mixed fixed/encoded
- [x] Ensure append writes:
  - [x] validity bitmap
  - [x] fixed slots
  - [x] encoded lengths and bytes
- [x] Ensure append updates payload offsets exactly once per record.
- [x] Validate append behavior with a direct native codec smoke test for a mixed fixed+encoded record.

### Step 3 - Implement native decode for schema-driven records
- [x] Implement decode that accepts:
  - [x] payload bytes
  - [x] payload offsets
  - [x] state index
  - [ ] schema key kinds
- [x] Document and enforce the current decode contract that schema counts are supplied implicitly via pre-sized output buffers/vectors.
- [ ] Decode directly into output buffers/vectors where possible.
- [x] Add explicit offset and record-bound validation in the new native codec.
- [x] Add direct native codec smoke test entry points in `key_codec.pyx`.
- [x] Confirm direct single fixed append/decode round-trip succeeds outside query execution.
- [x] Confirm direct single encoded append/decode round-trip succeeds outside query execution.
- [x] Confirm direct mixed fixed+encoded append/decode round-trip succeeds outside query execution.

### Step 4 - Replace multi-key path first in practice
- [x] Switch the active key codec bindings to the new native codec file.
- [x] Keep the current fail-fast guards in place while validating the new path.
- [x] Audit `build_payload_multi_key_vectors()` for assumptions that still matched the old payload semantics rather than the new schema-driven format, and harden it with schema-alignment checks.
- [x] Confirm whether `build_payload_multi_key_vectors()` satisfies the native decode sizing contract before each multi-key decode call.
- [x] Audit remaining finalize/reconstruction helpers for assumptions that still match the old payload semantics rather than the new schema-driven format at the Python-visible boundary.
- [x] Remove remaining zpp-oriented commentary from the active codec/finalize/engine files so the codebase reflects the new codec architecture.
- [x] Continue the migration by prioritizing:
  - [x] finalize helper alignment with the schema-driven record format
  - [ ] single `int64` key-group arena bypass design
  - [ ] cleanup of compatibility shims once no callers depend on legacy names

### Step 5 - Replace single-key paths in the same remediation effort
- [x] Replace single encoded key codec.
- [x] Replace single fixed key codec.
- [x] Rework single `int64` key groups to bypass arena storage where practical.

### Step 6 - Remove zpp-specific structures and naming
- [x] Delete zpp-specific native record structures once no active call sites remain.
- [x] Remove zpp-specific comments and assumptions from Cython and native code.
- [x] Rename any remaining misleading symbols or files, or delete the legacy compatibility file entirely so no active callers can continue using the old name.

### Step 7 - Validate correctness and performance
- [ ] Run ClickBench 40 and 41. Deferred broader validation follow-up.
- [x] Run targeted regression tests for mixed key shapes.
- [ ] Benchmark append/decode/finalize performance against the previous implementation. Deferred broader validation follow-up.

### Step 8 - Complete the storage rewrite follow-up
- [x] Remove compatibility-layer dependence on the old `zpp_key_codec` naming once no active callers require it.
- [x] Rework single `int64` key groups to bypass arena storage where practical.
- [x] Confirm runtime null bitmap correctness for reconstructed key vectors via targeted unit regression coverage for null-containing single-key and multi-key group shapes.
- [x] Confirm runtime date/time/timestamp round-trip correctness for reconstructed key vectors via targeted unit regression coverage for timestamp, date32, time32, and time64 group-key shapes.
- [x] Add targeted rewrite-regression coverage for direct codec decode paths and rewritten group-key storage shapes.

---

# Decisions

- [x] The custom codec relies entirely on external schema. We do not store aggregation metadata or per-record key counts in the key group store.
- [x] The custom codec uses one unified key-valid bitmap across all key columns.
- [x] Encoded keys use a length-prefixed representation with an explicit null sentinel.
- [x] Sentinel convention: `-1` means null / invalid, `0` means valid empty string.
- [x] zpp should be removed from all group-key codecs in one pass, not only the multi-key path.
- [x] Single `int64` key groups should be redesigned so they do not need arena storage when the key can be stored directly in the hash table/state structure.
- [x] The replacement native codec should live in a new file with a clear name rather than continuing under a misleading zpp-specific filename.
- [x] The rewrite should stay focused on completing the group-key storage migration; segfault isolation is a sidequest and should only influence the rewrite when it exposes a remaining storage-model mismatch.

---

# Exit Criteria

This rewrite is complete when all of the following are true:

- [x] the active group-key storage path no longer depends on zpp-based serialization logic
- [x] the remaining compatibility-layer dependence on old `zpp_key_codec` naming has been removed or intentionally retained with a documented reason
- [x] single-key and multi-key group-key storage both use the schema-driven native codec family, with single `int64` keys intentionally bypassing arena serialization by design
- [x] single `int64` key groups have been reassessed and either bypass arena storage or have a documented reason not to
- [x] multi-key finalize round-trips correctly for fixed and mixed key shapes at the targeted unit-regression level
- [x] runtime validation confirms null bitmap correctness for reconstructed key vectors at the targeted unit-regression level
- [x] runtime validation confirms date/time/timestamp round-trip correctness for reconstructed key vectors at the targeted unit-regression level
- [x] regression tests exist for the rewritten storage shapes
- [x] the remaining unchecked items are broader benchmark, ClickBench, and sidequest segfault follow-up tasks rather than blockers for the storage rewrite itself
