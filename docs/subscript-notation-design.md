# Map/Iterable Subscript Design (`expr[index]`)

**Date:** March 7, 2026  
**Status:** Phase 1 Implemented, Phase 2 Implemented, Phase 3 Cleanup Implemented  
**Owner:** Query Planner / Expression Engine

---

## Summary

Map/iterable bracket syntax now uses a first-class binary operator (`MapAccess`) instead of being emitted as `GET(...)`.

JSON access remains separate:

1. String-key JSON access is still `Arrow` (`->`)
2. JSON redesign is explicitly deferred

---

## Scope

In scope:

1. Map/iterable bracket access (`expr[index]`)
2. Typing and execution through binary-operator pathways
Out of scope:

1. JSON operator redesign (`->`, `->>`, `@?`, `@>`)
2. Slice notation (`expr[start:end]`)
3. Typed struct-key access via map subscript

Slice note:
The parser does not currently emit slice-aware AST for bracket notation. Supporting
slice syntax would require SQL rewriting or grammar expansion and is deferred.

---

## Phase 1 Outcome (Implemented)

### Planner and AST

1. Bracket syntax now reaches planner construction via `json_access(...)`, because current parser output normalizes bracket expressions into `JsonAccess`.
2. Planner emits:

```python
Node(
    node_type=NodeType.BINARY_OPERATOR,
    value="MapAccess",
    left=<container_expr>,
    right=<key_expr>,
)
```

3. `json_access(...)` now demultiplexes by key type:
4. string key -> `Arrow`
5. non-string key -> `MapAccess`

Implemented in:

1. [`opteryx/planner/logical_planner/logical_planner_builders.py`](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/planner/logical_planner/logical_planner_builders.py)

### Binder and Type Inference

1. Legal operator pairs are now declared in `OPERATOR_MAP`:
2. `ARRAY + INTEGER + MapAccess`
3. `VARCHAR + INTEGER + MapAccess`
4. `BLOB + INTEGER + MapAccess`
5. `determine_type(...)` has a narrow branch for `ARRAY<T>[INTEGER] -> T`

Implemented in:

1. [`opteryx/planner/binder/operator_map.py`](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/planner/binder/operator_map.py)

### Execution

1. Added `MapAccessOp` to binary operator dispatch
2. Runtime supports positional access for:
3. list/array-like
4. `VARCHAR` (subscripting of a string)
5. `BLOB`/bytes-like
6. Subscript key must be integer-like at runtime (string key is rejected)
Implemented in:

1. [`opteryx/managers/expression/binary_operators.py`](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/managers/expression/binary_operators.py)

### Formatter

1. `MapAccess` now formats as `left[right]`

Implemented in:

1. [`opteryx/managers/expression/formatter.py`](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/managers/expression/formatter.py)

### Tests Added

1. `determine_type` tests for:
2. `ARRAY<T>[INTEGER] -> T`
3. `VARCHAR[INTEGER] -> VARCHAR`
4. invalid pairs raise `IncorrectTypeError`
5. `VARCHAR['1']` key-type rejection at bind layer

Implemented in:

1. [`tests/unit/planner/test_map_access_determine_type.py`](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/tests/unit/planner/test_map_access_determine_type.py)
2. [`tests/unit/planner/test_map_access_planning.py`](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/tests/unit/planner/test_map_access_planning.py)
3. [`tests/unit/expression/test_map_access_operator.py`](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/tests/unit/expression/test_map_access_operator.py)
4. [`tests/unit/planner/test_map_access_optimizer.py`](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/tests/unit/planner/test_map_access_optimizer.py)

---

## Current Semantics (After Phase 1)

1. Bracket access is a binary operator, not a function call
2. Key/index in bracket syntax must still be literal (current parser/builder rule)
3. Container types currently supported by `MapAccess`: `ARRAY`, `VARCHAR`, `BLOB`
4. Subscript key type is integer for `MapAccess`; string key is invalid
5. Negative indexes are supported where underlying kernel supports them
6. Out-of-range returns `NULL`
7. Null container rows propagate `NULL`

Important clarification:

1. Subscripting of a string is allowed: `VARCHAR[INTEGER]`
2. Subscripting by a string is not allowed for `MapAccess`
3. Current parser emits `JsonAccess` for bracket syntax; planner now routes non-string keys to `MapAccess` and string keys to `Arrow`

---

## What We Learned

1. A binary-operator value (`MapAccess`) is the right abstraction; no new node kind was needed.
2. `OPERATOR_MAP` legality checks catch most mistakes early and keep behavior consistent.
3. `ARRAY<T>[INTEGER] -> T` needs a targeted `determine_type` branch; generic operator map typing is not enough.
4. Runtime key coercion should be strict; accepting string-like indexes creates mismatches with binder legality.
5. Keeping JSON and map access separate avoids accidental semantic coupling.
6. Predicate-ordering logic needs recursive function detection once access expressions can wrap function calls, for example `SPLIT(name, ' ')[0]`.

---

## Phase 3 Outcome

1. Removed the planner-level `GET` compatibility rewrite.
2. Removed `GET` from the live function registries and catalog.
3. Kept `GET` only as a deprecated name so planning fails cleanly with a deprecation error.
4. Rewrote remaining bracket-style SQL coverage away from `GET(...)` and onto `expr[index]` / `expr['key']`.

Remaining caveat:

1. Parser normalization means bracket syntax still arrives as `JsonAccess` for both JSON-style and iterable-style access.
2. Because of that, "JSON numeric bracket key must always fail" is not fully enforceable at logical-planning time without richer typing or parser annotations.

---

## Risks

1. JSON access edge-cases remain parser-shaped rather than type-shaped.
2. Full engine integration coverage remains noisy because broader execution tests in this checkout are unstable.

Mitigation:

1. Keep JSON separation explicit in docs and code comments.
2. Revisit parser or binder annotations if strict JSON numeric-key rejection becomes mandatory.

---

## Decision

Use `NodeType.BINARY_OPERATOR` `MapAccess` for map/iterable bracket access only, keep JSON access separate, and treat `GET` as removed from the supported function surface.
