# Type And Cast Catalog Design

**Date:** March 13, 2026  
**Status:** Proposed  
**Goal:** Generate authoritative machine-readable artifacts for Opteryx's type system and cast semantics directly from runtime code and normalization rules.

---

## Executive Summary

Opteryx already generates function signature metadata, but type and cast documentation is still mostly implicit in code:

- the type system is centered on `OrsoTypes`
- type-name normalization is implemented in local helpers
- cast behavior is implemented in dedicated kernels
- planner, binder, and optimizer each add cast-specific rules

This design introduces two separate generated artifacts:

1. `types.json` — canonical reference for the public type system
2. `casts.json` — compatibility and behavior matrix for `CAST` / `TRY_CAST` / `SAFE_CAST`

These artifacts are separate by design:

- `types.json` should be reusable by functions, operators, aggregates, connectors, and schema tooling
- `casts.json` should focus only on conversion behavior and should refer to stable type ids from `types.json`

---

## Why Separate Artifacts

Types and casts are related, but they change at different rates and serve different consumers.

**`types.json` answers:**

- What types exist?
- What is the canonical name?
- What aliases are accepted?
- Is the type numeric, temporal, binary, array-like, or parameterized?
- What parameters does the type accept?

**`casts.json` answers:**

- Can type `A` be cast to type `B`?
- Is the cast supported for `CAST`, `TRY_CAST`, or both?
- Is the cast lossy or strict?
- What optional parameters are accepted?
- Does failure raise or return null?

Keeping them separate avoids turning the type catalog into a huge compatibility matrix, and avoids duplicating type metadata inside every cast rule.

---

## Source Of Truth

### Type Metadata

Primary sources:

- `orso.types.OrsoTypes`
- `opteryx/rugo/converters/orso.py`
- planner type normalization for cast targets in `opteryx/planner/logical_planner/logical_planner_builders.py`
- binder/operator typing rules where type families are used operationally

Observed local normalization surfaces:

- `_normalize_orso_type_aliases()` in `opteryx/rugo/converters/orso.py`
- `_map_parquet_type_to_orso()` in `opteryx/rugo/converters/orso.py`
- `_map_jsonl_type_to_orso()` in `opteryx/rugo/converters/orso.py`
- `_normalize_cast_type()` in `opteryx/planner/logical_planner/logical_planner_builders.py`

### Cast Metadata

Primary sources:

- `opteryx/expression/casts.py`
- `opteryx/planner/logical_planner/logical_planner_builders.py`
- `opteryx/planner/optimizer/strategies/cast_simplification.py`
- tests in `tests/unit/expression/test_casts.py` and `tests/unit/core/test_cast.py`

Observed cast behavior surfaces:

- `cast()` and `try_cast()` kernel factories
- target-specific optimized kernels (`cast_to_int`, `cast_to_double`, `cast_to_varchar`, `cast_to_blob`)
- planner support for `CAST`, `TRY_CAST`, `SAFE_CAST`, and `::`
- literal cast folding rules
- cast simplification rules

---

## Artifact 1: `types.json`

### Purpose

Provide a canonical, generated description of the public type system that documentation and tooling can consume without importing Python internals.

### Proposed Schema

```json
{
  "schema_version": 1,
  "generated_at": "2026-03-13T12:00:00Z",
  "types": [
    {
      "id": "integer",
      "canonical_name": "INTEGER",
      "aliases": ["INT", "INT8", "INT16", "INT32", "INT64"],
      "family": "numeric",
      "category": "scalar",
      "parameterized": false,
      "parameters": [],
      "supports_array_element": true,
      "nullable": true,
      "flags": {
        "is_numeric": true,
        "is_temporal": false,
        "is_binary": false,
        "is_json_like": false
      },
      "accepted_input_spellings": [
        "integer",
        "int",
        "int64"
      ],
      "notes": [
        "Canonical SQL name is INTEGER."
      ]
    }
  ]
}
```

### Required Fields

- `id`: stable lowercase identifier used by other artifacts
- `canonical_name`: preferred public spelling
- `aliases`: accepted alternate spellings
- `family`: `numeric`, `temporal`, `string`, `binary`, `boolean`, `array`, `json`, `other`
- `category`: `scalar`, `collection`, `parameterized`, `internal`
- `parameterized`: whether the type accepts parameters
- `parameters`: parameter definitions such as `length`, `precision`, `scale`, `element_type`
- `flags`: operational facts that other generators can reuse
- `accepted_input_spellings`: all spellings that normalize to the type
- `notes`: short documentation-only guidance

### Optional Fields

- `literal_forms`: if there are stable literal syntaxes worth documenting
- `storage_mappings`: external mappings for parquet/jsonl/connector ingestion
- `examples`: optional, but can be derived later from curated fixtures rather than generated automatically

### Generation Strategy

1. Start with canonical `OrsoTypes` values.
2. Add accepted aliases from local normalization helpers.
3. Derive family/category/flags using `OrsoTypes` predicates where available.
4. Add parameterization metadata from known parse contracts:
   - `VARCHAR(length)`
   - `DECIMAL(precision, scale)`
   - `ARRAY<element_type>`
5. Add connector/storage spelling mappings from parquet/jsonl normalization code.

### Non-Goals

- Do not attempt to encode every internal pyarrow or parquet type.
- Do not treat every ingestion spelling as a public SQL alias unless the planner accepts it.
- Do not infer prose-heavy examples from code.

---

## Artifact 2: `casts.json`

### Purpose

Provide a generated compatibility matrix for cast operations, keyed by type ids from `types.json`.

### Proposed Schema

```json
{
  "schema_version": 1,
  "generated_at": "2026-03-13T12:00:00Z",
  "type_catalog_ref": "types.json",
  "casts": [
    {
      "source_type_id": "varchar",
      "target_type_id": "integer",
      "cast_supported": true,
      "try_cast_supported": true,
      "safe_cast_supported": true,
      "parameters": [],
      "behavior": {
        "on_failure": {
          "cast": "error",
          "try_cast": "null"
        },
        "lossiness": "possible",
        "null_propagation": "preserve_nulls"
      },
      "implementation": {
        "planner_node": "NodeType.CAST",
        "kernel": "cast_to_int",
        "optimized_path": true
      },
      "notes": [
        "String parsing accepts values recognized by INTEGER.parse."
      ]
    }
  ]
}
```

### Required Fields

- `source_type_id`
- `target_type_id`
- `cast_supported`
- `try_cast_supported`
- `safe_cast_supported`
- `parameters`
- `behavior.on_failure`
- `behavior.lossiness`
- `implementation.planner_node`

### Suggested Behavioral Classifications

- `lossiness`: `none`, `possible`, `expected`, `unknown`
- `on_failure.cast`: `error`, `undefined`
- `on_failure.try_cast`: `null`, `undefined`
- `null_propagation`: `preserve_nulls`

### Generation Strategy

Use a hybrid approach:

1. **Static behavior extraction**
   - inspect available kernel factories and target-specific optimized kernels
   - inspect planner normalization for supported target type names
   - inspect whether `TRY_` variants are supported for a target

2. **Generated compatibility probing**
   - generate candidate `(source_type, target_type)` pairs from `types.json`
   - run a lightweight probe set against representative values
   - classify outcomes as supported/unsupported and strict/safe

3. **Manual override table for edge semantics**
   - only for cases where automated probing is insufficient
   - examples: parameterized targets, temporal edge cases, lossy classifications

The important point is that `casts.json` should be mostly generated from real behavior, not maintained as a handwritten matrix.

---

## Public Documentation Outputs

These artifacts should feed at least three generated docs pages:

### 1. Type Reference

Generated from `types.json`

- one section per public type
- aliases
- parameters
- family/category
- accepted spellings
- external normalization notes

### 2. Cast Compatibility Matrix

Generated from `casts.json`

- row = source type
- column = target type
- cell values:
  - `CAST`
  - `TRY_CAST`
  - unsupported
  - lossy / parameterized markers

### 3. Cast Semantics Reference

Generated from `casts.json` plus a small curated template

- `CAST`, `TRY_CAST`, `SAFE_CAST`, and `::`
- error vs null behavior
- parameterized casts
- optimizer notes such as nested cast simplification

---

## Runtime Consumers

The first goal is documentation/reference generation, but these artifacts should also be reusable by runtime-adjacent tooling:

- IDE completion and validation
- linting / static analysis
- function export tooling that needs type ids
- connector compatibility docs
- future `SHOW TYPES` / `SHOW CASTS` introspection surfaces

---

## Proposed Module Layout

```text
opteryx/
  types/
    export.py              # type catalog export
    cast_export.py         # cast matrix export
    metadata.py            # local metadata + overrides only where unavoidable

tools/
  generate_type_catalog.py
  generate_cast_catalog.py

docs/
  type-and-cast-catalog-design.md

opteryx/
  reference/
    types.json
    casts.json
```

Notes:

- Keep generated artifacts out of `docs/` so docs rendering and runtime tooling can share them.
- Keep generation code separate from cast execution kernels.
- Avoid putting these exports under `opteryx/functions/`; they are broader than functions.

---

## Design Principles

1. **Behavior over prose**
   - generate from actual normalization and cast behavior where possible

2. **Separate stable identifiers from display names**
   - `integer` should be the stable id
   - `INTEGER` should be the display/canonical SQL name

3. **Prefer generated facts, curated explanations**
   - compatibility matrix: generated
   - long-form user guidance: templated and lightly curated

4. **Avoid duplicated normalization logic**
   - generators should call shared normalization helpers rather than recreate alias rules

5. **Use explicit overrides sparingly**
   - only for classifications that cannot be reliably discovered from code or probes

---

## Open Questions

1. Should `types.json` include connector/storage-only spellings like parquet physical types, or keep those in a separate ingestion mapping section?
2. Should `casts.json` describe only public SQL cast targets, or every internal target accepted by kernels?
3. Should `SAFE_CAST` remain documented separately from `TRY_CAST`, or be treated as a pure alias in exported metadata?
4. Do we want `SHOW TYPES` and `SHOW CASTS` as explicit SQL surfaces in the same phase, or only file generation first?

Suggested answers:

- include storage spellings, but in a separate field from public aliases
- export only public SQL targets in `casts.json`
- document `SAFE_CAST` as an alias of `TRY_CAST`
- generate files first, then add SQL introspection

---

## Rollout Plan

### Phase 1: Type Catalog

1. Implement `types.json` export.
2. Add tests for canonical names, aliases, and parameterized types.
3. Generate an initial type reference page from the artifact.

### Phase 2: Cast Catalog

1. Implement `casts.json` export using type ids from `types.json`.
2. Add probe-based validation against actual cast kernels.
3. Generate the cast compatibility matrix.

### Phase 3: Tooling Integration

1. Add CLI scripts for regeneration.
2. Add artifact consistency tests to CI.
3. Optionally add `SHOW TYPES` and `SHOW CASTS`.

---

## Testing Strategy

### For `types.json`

- every exported type id resolves to a real `OrsoTypes` target or approved public alias
- aliases normalize to the intended canonical id
- parameter metadata matches parser expectations
- no public type disappears without an explicit fixture change

### For `casts.json`

- exported supported targets match planner normalization rules
- every exported supported cast has a live execution path
- representative probe values verify:
  - valid conversions succeed
  - invalid `CAST` raises
  - invalid `TRY_CAST` returns null
- special cases for `DECIMAL`, `VARCHAR(length)`, `ARRAY<type>`, and temporal targets are covered

---

## Risks And Mitigations

- **Risk:** Type aliases drift across ingestion code and planner code.
  - Mitigation: centralize alias extraction into shared helpers used by generators.

- **Risk:** Cast support inferred from probing misses parameterized or rare edge cases.
  - Mitigation: probe common paths and use a small explicit override table for edge behavior.

- **Risk:** Generated artifacts become runtime dependencies accidentally.
  - Mitigation: treat them as documentation/tooling artifacts first; runtime continues using code metadata directly.

- **Risk:** `types.json` becomes a dumping ground for storage-specific detail.
  - Mitigation: keep public type metadata separate from ingestion/storage mapping sections.

---

## Acceptance Criteria

1. `types.json` is generated from code and checked in as a reproducible artifact.
2. `casts.json` is generated from code plus minimal explicit overrides.
3. Both artifacts use stable type ids and do not duplicate the full type definition in cast rows.
4. Generated docs can produce:
   - a type reference
   - a cast compatibility matrix
   - a cast semantics page
5. Tests verify that exports remain aligned with actual type normalization and cast behavior.

