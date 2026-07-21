# Plan: VARIANT type + correct `->` / `->>` JSON operator semantics

**Status:** ✅ IMPLEMENTED (2026-06-03). `make q` 137/137, shapes/clickbench/tpch
unregressed. See the implementation notes at the end of this file.
**Architect decisions locked (2026-06-03):**
- Introduce a **new `VARIANT` `OrsoType`** for polymorphic JSON values. Do **not** rename
  `_MISSING_TYPE` (it is the "unresolved/unknown" sentinel used in ~50 guards alongside
  `None`/`0`/`NULL`; renaming would conflate "type unknown" with "deliberately dynamic"
  and mask real type-resolution failures — a §1 hazard).
- **VARIANT gets its own physical draken tag** (NOT logical-over-VARCHAR). Sharing tag 60
  with VARCHAR is a bug — they must be distinguishable at the physical/morsel layer. Add
  `DRAKEN_VARIANT` (new tag number), same German-string storage as the other string-family
  types (holds JSON text), distinct tag. The `->` kernel emits VARIANT-tagged vectors;
  `_DRAKEN_TO_ORSO` gains a VARIANT entry (which resolves the reverse-map ambiguity).
- **VARIANT exposes to Python as `str`** (the JSON-text rendering) even though it is **not**
  a string type semantically (not in `_STRING_TYPES`). `_TYPE_TO_PYTHON[VARIANT] = str`.
- `->` (Arrow) returns **`VARIANT`**.
- `->>` (LongArrow) **always** emits **`NVARCHAR`** (text). Content: JSON strings unquoted
  (`yyjson_get_str`), other JSON values (number/bool/object/array) serialized to their text
  form. JSON is parser-validated UTF-8, so NVARCHAR is safe.
- VARIANT is a valid **left operand for `Arrow`, `LongArrow`, and `MapAccess`** (chaining
  works with no user cast). **Every other use of VARIANT raises "not supported"** — the
  user must extract to a concrete type first. Achieved by adding VARIANT *only* to those
  three extraction-operator rows; any other operator naturally hits the existing
  `OPERATOR_MAP` miss → "Unable to perform … you may need to cast" (fail-fast, §1). No
  VARIANT equality/ordering/arithmetic rows.
- **No `CAST(x AS VARIANT)` and no `CAST(x AS JSON)`** — variants are a pain; not exposed as
  cast targets. (`->`/`->>` already parse string operands on the fly, so neither cast is
  needed.) The concrete-value path is `->>` → NVARCHAR text → `CAST(... AS <type>)`.
- **`@?` (AtQuestion) is in scope** — fix it (currently broken for all string types) in this
  work.
- **`->>` on a non-scalar** (object/array) → its **stringified JSON** as NVARCHAR text
  (`{"b":1}`), not NULL.
- **Missing key → SQL NULL** (both `->` and `->>`). Existence is tested with `@?`, not by
  inspecting the extraction result. (JSON explicit `null` also collapses to SQL NULL.)

---

## 1. Why

PostgreSQL/standard JSON access:
- `json -> key` → **JSON** (a sub-value, still polymorphic) — the VARIANT model.
- `json ->> key` → **text** (the scalar rendered as text; strings **unquoted**).

Opteryx today diverges on **both** type and behavior (verified at runtime):

| Expression | Standard | Opteryx runtime today |
|---|---|---|
| `'{"a":"café"}' -> 'a'` | `"café"` (json) | `"café"` — VARCHAR |
| `'{"a":"café"}' ->> 'a'` | `café` (text, unquoted) | `"café"` — VARCHAR, **still quoted** |
| `'{"a":42}' ->> 'a'` | `42` | `42` — VARCHAR |
| `'{"a":{"b":1}}' ->> 'a'` | `{"b":1}` (text) | `{"b":1}` — VARCHAR |

Two roots:
1. **Behavior:** `->` and `->>` dispatch to the **same kernel** `draken_json_extract`
   (`compiled_expression.pyx` maps both `BC_EXTR_JSON_PTR` and `BC_EXTR_JSON_KEY` to it).
   The kernel always serializes via `yyjson_val_write` (`vector_json.cpp:186`), which keeps
   string quotes. The `PTR`/`KEY` sub-op selects *navigation mode* (RFC-6901 pointer vs raw
   key), not output format — so neither operator unquotes.
2. **Types:** the `OPERATOR_MAP` declares `Arrow`→`_MISSING_TYPE`, `LongArrow`→`BLOB`, but
   the runtime ignores both and returns VARCHAR. The declared result types are already
   inconsistent with execution.

---

## 2. The VARIANT type — representation

**Proposal: VARIANT is a *logical* type over a VARCHAR-shaped physical vector** (JSON text
bytes). The draken vector stays a string (German-string slots + arena, tag
`DRAKEN_VARCHAR`); VARIANT-ness lives in the **bound plan's schema column type**, exactly
as a `CAST` result type drives the schema today. Rationale: JSON sub-values are JSON
*text*; "VARIANT" means "this text's value-type is dynamic," not a new storage shape.

**Consequence / open question (V1):** the morsel→schema reverse map
(`query_session._DRAKEN_TO_ORSO`) can't distinguish VARIANT from VARCHAR (both physical
tag 60). For results whose type is carried by the bound plan this is fine; for a *bare*
VARIANT vector with no plan context it would report VARCHAR. Decide whether that edge
matters. (Alternative: add a `DRAKEN_VARIANT` physical tag — larger, lets the reverse map
and kernels distinguish it; deferred unless V1 proves insufficient.)

---

## 3. Layered change list

Mirrors the NVARCHAR rollout (same touch-points), plus the JSON kernel work.

### 3a. Type system
- `opteryx/types/_orso_types.py`: add `VARIANT = "VARIANT"` enum member; metadata —
  `_TYPE_TO_PYTHON[VARIANT]` (likely `str`, since the physical value is JSON text), and
  decide `is_string()` / classification membership (probably **not** `_STRING_TYPES` — it's
  not a string semantically, even if physically backed by one).
- `opteryx/types/_native_types.py`: `VARIANT → TYPE_OBJECT`.
- `query_session._DRAKEN_TO_ORSO`: see open question V1 (no clean physical tag → likely no
  entry; VARIANT surfaces via plan type, not reverse map).

### 3b. JSON operator kernel — `vector_json.cpp`
- Differentiate output by operator, not just navigation. Two options:
  - (i) two kernels (`draken_json_extract` for `->`, `draken_json_extract_text` for `->>`), or
  - (ii) pass an output-mode flag (json | text) via the kernel context.
- **`->>` text rule** (Postgres-compatible): if the extracted `yyjson_val` is a **string**,
  emit `yyjson_get_str` (unquoted, raw UTF-8 bytes) → NVARCHAR; otherwise (number, bool,
  object, array, null) serialize via `yyjson_val_write` as today → the text rendering.
  Null JSON value → SQL NULL.
- **`->` VARIANT rule:** keep `yyjson_val_write` (JSON text) but the *result* is typed
  VARIANT; physical vector remains VARCHAR-JSON-text.

### 3c. Compiler / execution operand acceptance — `compiled_expression.pyx`
- `EXTRACTION_OPERATOR`: allow `VARIANT` as a valid left operand for `Arrow`/`LongArrow`
  (and `MapAccess`?) so chained `a -> b ->> c` works — i.e. add VARIANT to the operand
  acceptance check alongside the string family.
- The `->>` text result should produce an NVARCHAR vector (reuse the validated re-tag /
  `vector_cast_string_to_nvarchar` machinery, or have the text kernel emit NVARCHAR
  directly since yyjson already guarantees UTF-8).

### 3d. Operator map — `operator_map.py`
Replace the JSON-operator result types (currently `_MISSING_TYPE`/`BLOB`) for **every**
document/key source combination (VARCHAR, NVARCHAR, BLOB, JSONB, STRUCT, **VARIANT**):
- `(*, *, "Arrow")` → `VARIANT`
- `(*, *, "LongArrow")` → `NVARCHAR`
- `(*, *, "AtQuestion")` → `BOOLEAN` (unchanged; but see §5 — AtQuestion is currently
  broken for all string types)
- Add `VARIANT` as a left operand: `(VARIANT, key, "Arrow"|"LongArrow"|"AtQuestion")` for
  chaining; and `(VARIANT, INTEGER, "MapAccess")` → `VARIANT` for array indexing.

### 3e. CAST surface (optional, recommend include)
- `CAST(x AS VARIANT)` / `CAST(x AS JSON)`: parse/validate as JSON, type VARIANT. Mirrors
  `_normalize_cast_type` + a cast path. Lets users explicitly produce VARIANT. (Could be a
  follow-up; the operators don't strictly need it.)

---

## 4. Test plan
- `->>` unquoting: string→`café` (no quotes), number→`42`, bool→`true`, object→`{"b":1}`,
  null→SQL NULL; multibyte string round-trips and `LENGTH(... ->> ...)` counts codepoints.
- `->` returns VARIANT; chained `a -> b -> c` and `a -> b ->> c` resolve.
- Result-schema types: `->` column reports VARIANT, `->>` reports NVARCHAR.
- Regression: `make q`, shapes, clickbench, tpch. **Behavior change risk:** any existing
  query/test depending on `->>` returning the *quoted* form will change — audit the SLT/JSON
  tests and update expectations (this is a correctness fix, so updates are expected).

---

## 5. Resolved decisions (were open questions)
- **VARIANT physical representation:** distinct `DRAKEN_VARIANT` tag (NOT logical-over-
  VARCHAR). Same German-string storage, new tag; reverse map gains a VARIANT entry.
- **`->>` on non-scalars:** stringified JSON as NVARCHAR text (string→unquoted,
  object/array/number/bool→serialized). Never NULL.
- **`@?`:** in scope — fix for all string types this pass.
- **VARIANT in operators:** extraction-only (`->`, `->>`, `[i]`); everything else raises
  the existing not-supported error → user extracts/casts.
- **Missing key / JSON null:** SQL NULL; existence via `@?`.

## 5b. Remaining implementation notes (not architect decisions)
- **Navigation vs output mode:** the existing `PTR`/`KEY` sub-op encodes navigation; the
  output-format axis (VARIANT json-text vs `->>` text/unquote) is new and orthogonal —
  thread it through the kernel (flag or second kernel).
- **`@?` root:** currently raises "Expected str, got Vector" — the handler expects a scalar
  key but receives a Vector; needs the vectorised path wired (diagnose during build).
- **CAST *from* VARIANT to concrete:** not planned; the path is `->>`→NVARCHAR→`CAST`.
  Revisit only if a real need surfaces.
- **VARIANT `to_pylist` → `str`:** the new physical tag must render as the JSON-text string
  at the draken vector surface.

---

## 6. Sequencing (when approved)
1. Add `VARIANT` OrsoType + metadata (3a) — inert until used.
2. `->>` text-unquoting kernel + NVARCHAR result (3b/3c) — the standalone behavior fix.
3. `->` VARIANT typing + chaining + operator-map rework (3b/3c/3d).
4. Optional `CAST AS VARIANT/JSON` (3e).
5. Audit + update JSON/SLT test expectations; full regression.

---

## Implementation notes (2026-06-03)

Landed exactly as specced. Files touched:
- **Physical type:** `draken/core/buffers.h` (`DRAKEN_VARIANT = 65`), `buffers.pxd`,
  `draken/ops/hash.h` (ops-table string-kernel registration so VARIANT flows through
  take/slice/materialize/compress/hash), `draken_native.cpp` (enum export; to_pylist /
  __getitem__ render VARIANT as `str`; `draken_vector_own_string` accepts VARIANT).
- **Logical type:** `opteryx/types/_orso_types.py` (`VARIANT`, python_type=str, parser),
  `_native_types.py`, `query_session._DRAKEN_TO_ORSO` (65→VARIANT).
- **Kernel:** `vector_json.cpp` `impl_extract(text_mode)` — `->` serializes → VARIANT;
  `->>` unquotes JSON strings via `yyjson_get_str`, serializes other values → NVARCHAR.
  New binding `vector_json_extract_text`.
- **Executor:** `evaluation.pyx` routes `BC_EXTR_JSON_KEY` (`->>`) to the text variant;
  `compiled_expression.pyx` accepts VARIANT as an extraction left operand.
- **Operator map:** all Arrow→VARIANT, LongArrow→NVARCHAR; VARIANT extraction-only rows.
- **`@?` fix:** `json_ops.pyx` `_json_at_question` rewritten — coerces the path operand
  (was typed `str`, crashed on the Vector the executor passes), normalises to a JSON
  Pointer, uses `at_pointer(...) is not None` for existence, and returns
  `BoolVector(vector_from_bool_sequence(...))`. The crash was a double bug: it returned a
  raw nanobind INT64 vector, but the executor casts the comparison result to a Cython
  `Vector` and calls `.unified()` → SIGBUS. Now returns a wrapped BOOL vector.
- **Constant folding:** skip folding for VARIANT (and NVARCHAR) — folded scalar literals
  can't carry the type (literal materialisation is VARCHAR-only).

Verified: `->`→VARIANT (`"café"`), `->>`→NVARCHAR unquoted (`café`), `->>` number/object
→ stringified text, chaining `-> -> ->>`, missing key → NULL, `@?` present/missing/null,
`LENGTH(->> )` counts codepoints, VARIANT in a comparison raises (extraction-only).

Still not done (out of scope / future): rugo's JSONL reader now emits VARIANT for object
columns (`parse_objects=True`, the default — see rugo/src/jsonl/core/column_builder.cpp's
parse_typed_column and rugo/README.md), but no other reader (Parquet, CSV) does; VARIANT
literal constant materialisation (hence the no-fold guard); `CAST` to/from VARIANT (by
decision, none).
