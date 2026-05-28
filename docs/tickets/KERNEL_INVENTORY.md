# Kernel Inventory for Phase 9a (C ABI Implementation)

**Status**: First deliverable — surface for architect approval before writing C functions.

This table enumerates all built-in kernels that will receive `extern "C"` C function entries and be registered for bytecode executor dispatch in Phase 9.

---

## BC_EXTRACTION (4 sub-ops)

All are native nanobind kernels with fixed signatures. Map-access opcodes stored in `slot.op_code`.

| Kernel Name | Current Location | Signature Category | Notes |
|---|---|---|---|
| `vector_map_access_string` | `opteryx/compiled/nanobind/vector_special.cpp` | BC_EXTRACTION | 2-arg: (vector, key) |
| `vector_array_map_access` | `draken/draken_native.cpp` | BC_EXTRACTION | 2-arg: (vector, key). Already C++. |
| `vector_json_extract` | `opteryx/compiled/nanobind/vector_json.cpp` | BC_EXTRACTION | 2-arg: (vector, key). Arrow `->` / LongArrow `->>`. |
| `vector_pointer_extract` | TBD — investigate if in scope | BC_EXTRACTION | 2-arg: (vector, key). Path extraction. |

**Sub-opcodes** (`slot.op_code` in executor):
- `BC_EXTR_MAP_STRING` → `vector_map_access_string`
- `BC_EXTR_MAP_ARRAY` → `vector_array_map_access`
- `BC_EXTR_JSON_PTR` → `vector_json_extract` (Arrow `->`)
- `BC_EXTR_JSON_VALUE` → `vector_json_extract` (Arrow `->>`)

---

## BC_CAST (Variadic — depends on source/target pair)

Kernels resolved at bind time by `opteryx/expression/casts.pyx:resolve_cast()`. Mixed sources: nanobind, native C++, and Python closures.

### Numeric Casts
| Kernel Name | Current Location | Signature Category | Context | Notes |
|---|---|---|---|---|
| `vector_cast_int64_to_float64` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_bool_to_float64` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_integer_to_float64` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_string_to_float64` | `draken/draken_native.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_int64_to_string` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_bool_to_string` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_date_to_string` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_timestamp_to_string` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_float64_to_string` | `draken/draken_native.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_string_to_int` (aka `vector_cast_ascii_to_int`) | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_bool_to_int64` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_date32_to_int64` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_timestamp_to_int64` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_integer_to_int64` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_float64_to_int64` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_int64_to_bool` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_float64_to_bool` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_cast_string_to_bool` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | NULL | 1-arg: (vector) |

### Temporal Casts
| Kernel Name | Current Location | Signature Category | Context | Notes |
|---|---|---|---|---|
| `vector_cast_int64_to_timestamp` | `opteryx/compiled/nanobind/vector_casts.cpp` | BC_CAST | `cast_ctx{unit}` if parameterized | 1-arg: (vector). Can be parameterized with unit (ns/us/ms/s/days). |
| `vector_date32_to_timestamp` | `opteryx/compiled/nanobind/vector_temporal_convert.cpp` | BC_CAST | NULL | 1-arg: (vector) |
| `vector_timestamp_to_date32` | `opteryx/compiled/nanobind/vector_temporal_convert.cpp` | BC_CAST | NULL | 1-arg: (vector) |

### Parametrized Casts (Python Closures)
| Kernel Name | Current Location | Signature Category | Context | Notes |
|---|---|---|---|---|
| `_decimal_cast` | `opteryx/expression/casts.pyx:_build_decimal_closure()` | BC_CAST | `cast_ctx{precision, scale}` | Python closure. Row-loop via OrsoTypes.DECIMAL.parse. |
| `_array_cast` | `opteryx/expression/casts.pyx:_build_array_cast()` | BC_CAST | `cast_ctx{element_type}` | Python closure. Row-loop. Calls `vector_array_from_sequence`. |
| `_vector_cast` | `opteryx/expression/casts.pyx:_build_vector_cast()` | BC_CAST | NULL | Python closure. Row-loop via OrsoTypes.VECTOR.parse. |
| `_varchar_cast_with_length` | `opteryx/expression/casts.pyx:_build_varchar_cast_with_length()` | BC_CAST | `cast_ctx{length}` | Python closure. Row-loop with length enforcement (TBD). |

### Passthrough Closures (No-op / Row-loop)
| Name | Signature Category | Context | Notes |
|---|---|---|---|
| `lambda arr: arr` | BC_CAST | NULL | No-op for source == target. |
| `cast_to_double` | BC_CAST | NULL | Dispatch helper with native paths for FLOAT64/INT64/INTEGER/BOOL/STRING. |
| `cast_to_int` | BC_CAST | NULL | Dispatch helper with native paths for INT64/INTEGER/FLOAT64/STRING/BOOL/TIMESTAMP/DATE32. |
| `cast_to_varchar` | BC_CAST | NULL | Dispatch helper with native paths for STRING/FLOAT64/INT64/BOOL/TIMESTAMP/DATE32/ARRAY. Row-loop fallback. |
| `cast_to_boolean` | BC_CAST | NULL | Dispatch helper with native paths for BOOL/INT64/FLOAT64/STRING. Row-loop fallback. |
| `cast_to_date` | BC_CAST | NULL | Dispatch helper. Row-loop via OrsoTypes.DATE.parse. |
| `_build_residual_cast` | BC_CAST | NULL | Residual row-loop for unspecialized pairs. |

---

## BC_BINARY_OP (5 categories)

### Arithmetic Operators (getattr closure → Draken Vector methods)
| Kernel Name | Current Location | Signature Category | Context | Notes |
|---|---|---|---|---|
| `Vector.add` | `draken/draken_native.cpp` (C++ method) | BC_BINARY_OP | `binary_op_ctx{BOP_PLUS}` | Draken Vector method. Called via getattr in closure. C++ backing: `draken_arithmetic`. |
| `Vector.sub` | `draken/draken_native.cpp` | BC_BINARY_OP | `binary_op_ctx{BOP_MINUS}` | Draken Vector method. |
| `Vector.mul` | `draken/draken_native.cpp` | BC_BINARY_OP | `binary_op_ctx{BOP_MULTIPLY}` | Draken Vector method. |
| `Vector.div` | `draken/draken_native.cpp` | BC_BINARY_OP | `binary_op_ctx{BOP_DIVIDE}` | Draken Vector method. |
| `Vector.mod` | `draken/draken_native.cpp` | BC_BINARY_OP | `binary_op_ctx{BOP_MODULO}` | Draken Vector method. |

**Note**: These are NOT direct nanobind kernels; they're draken Vector methods bound via `getattr` in the closure. 9a must expose a unified `draken_binary_arith(ctx, left, right)` wrapper that dispatches internally via `ctx->op_code`.

### Bitwise Operators (nanobind)
| Kernel Name | Current Location | Signature Category | Context | Notes |
|---|---|---|---|---|
| `vector_bitwise_or` | `opteryx/compiled/nanobind/vector_bitwise.cpp` | BC_BINARY_OP | NULL | 2-arg: (left, right) |
| `vector_bitwise_and` | `opteryx/compiled/nanobind/vector_bitwise.cpp` | BC_BINARY_OP | NULL | 2-arg: (left, right) |
| `vector_bitwise_xor` | `opteryx/compiled/nanobind/vector_bitwise.cpp` | BC_BINARY_OP | NULL | 2-arg: (left, right) |
| `vector_bitwise_shift_left` | `opteryx/compiled/nanobind/vector_bitwise.cpp` | BC_BINARY_OP | NULL | 2-arg: (left, right) |
| `vector_bitwise_shift_right` | `opteryx/compiled/nanobind/vector_bitwise.cpp` | BC_BINARY_OP | NULL | 2-arg: (left, right) |

### String Concatenation
| Kernel Name | Current Location | Signature Category | Context | Notes |
|---|---|---|---|---|
| `vector_concat` | `opteryx/compiled/nanobind/vector_selection_concat.cpp` | BC_BINARY_OP | NULL | 2-arg: (left, right). Wrapped in closure for coercion. |

### Temporal Operators (Date/Timestamp ± Interval)
| Kernel Name | Current Location | Signature Category | Context | Notes |
|---|---|---|---|---|
| `_date_interval_op_draken` | `opteryx/expression/evaluator/arithmetic.pyx` | BC_BINARY_OP | `binary_op_ctx{BOP_PLUS \| BOP_MINUS}` | Draken-native path for DATE/TIMESTAMP ± INTERVAL. |
| `_date_minus_date_draken` | `opteryx/expression/evaluator/arithmetic.pyx` | BC_BINARY_OP | `binary_op_ctx{BOP_MINUS}` | Draken-native path for DATE - DATE. |
| INTERVAL ± INTERVAL kernels | `opteryx/expression/intervals.py:INTERVAL_KERNELS` | BC_BINARY_OP | `binary_op_ctx{BOP_PLUS \| BOP_MINUS}` | Map of (left_orso, right_orso, op_str) → kernel. |

### IP-in-CIDR (Bitwise-Or Overload)
| Kernel Name | Current Location | Signature Category | Context | Notes |
|---|---|---|---|---|
| `vector_ip_in_cidr` | `opteryx/compiled/nanobind/vector_misc.cpp` | BC_BINARY_OP | NULL | 2-arg: (left, right). Special case: BOP_BITWISE_OR on VARCHAR. |

---

## BC_FUNCTION (Variadic — ~90+ kernels enumerated)

All functions registered in `opteryx/expression/functions/registrar/` and kernels in `opteryx/compiled/nanobind/*.cpp` and `opteryx/expression/functions/implementations/*`.

**Aggregates are OUT of 9a scope** — handled separately by aggregate operators, not BC_FUNCTION dispatch.

---

### Arithmetic / Numeric Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_abs` | nanobind/vector_math.cpp | nanobind | 1 | Absolute value. |
| `vector_sign` | nanobind/vector_math.cpp | nanobind | 1 | Sign (-1, 0, 1). |
| `vector_ceil` | nanobind/vector_math.cpp | nanobind | 1 | Ceiling. |
| `vector_floor` | nanobind/vector_math.cpp | nanobind | 1 | Floor. |
| `vector_round` / `round1` / `round2` | nanobind/vector_math.cpp | nanobind | 1-2 | Rounding with optional precision. |
| `vector_sqrt` | nanobind/vector_math.cpp | nanobind | 1 | Square root. |
| `vector_power` / `vector_log` | nanobind/vector_math.cpp | nanobind | 2 / 1 | Power, logarithm. |
| `vector_trunc` | nanobind/vector_math.cpp | nanobind | 1 | Truncate. |
| `random_number` / `vector_random` / `vector_random_normal` / `vector_random_strings` | nanobind/vector_misc.cpp | nanobind | 0-1 | Random value generation. |

---

### String / Text Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_length` | nanobind/vector_text.cpp | nanobind | 1 | String length (char count, UTF-8). |
| `vector_string_length` | nanobind/vector_text.cpp | nanobind | 1 | Alias for length. |
| `vector_string_substring` | nanobind/vector_string_slice.cpp | nanobind | 2-3 | Substring extraction. |
| `vector_substring` | nanobind/vector_string_slice.cpp | nanobind | 2-3 | Alias. |
| `vector_string_slice_left` / `vector_string_slice_right` | nanobind/vector_string_slice.cpp | nanobind | 2 | Left/right slice. |
| `vector_trim` / `vector_ltrim` / `vector_rtrim` | nanobind/vector_string_misc.cpp | nanobind | 1-2 | Trim whitespace or specified chars. |
| `vector_lowercase` | draken/ops/kernels/utf8.cpp | nanobind | 1 | Lowercase (UTF-8 aware, Phase E.26). |
| `vector_uppercase` | nanobind/vector_string_case.cpp | nanobind | 1 | Uppercase. |
| `vector_initcap` | nanobind/vector_string_case.cpp | nanobind | 1 | Title case. |
| `vector_reverse` | nanobind/vector_string_misc.cpp | nanobind | 1 | Reverse string. |
| `vector_replace` | nanobind/vector_string_misc.cpp | nanobind | 3 | Replace substring. |
| `vector_position` / `vector_contains` / `vector_starts_with` / `vector_ends_with` | nanobind/vector_string_search.cpp | nanobind | 2 | String search/containment. |
| `vector_ci_starts_with` / `vector_ci_ends_with` | nanobind/vector_string_search.cpp | nanobind | 2 | Case-insensitive search. |
| `vector_regex_replace` | nanobind/vector_string_search.cpp | nanobind | 3 | Regex replace (underlying C++). |
| `vector_levenshtein` | nanobind/vector_string_search.cpp | nanobind | 2 | Levenshtein distance. |
| `vector_soundex` | nanobind/vector_string_misc.cpp | nanobind | 1 | Soundex encoding. |
| `vector_string_is_empty` / `vector_string_is_not_empty` | nanobind/vector_string_misc.cpp | nanobind | 1 | Empty check. |
| `to_ascii` | implementations/text.py (Python row-loop) | cpdef | 1 | Convert to ASCII. **FLAG: Python loop.** |
| `to_char` | implementations/text.py (Python row-loop) | cpdef | 1 | Format as string. **FLAG: Python loop.** |
| `left_pad` / `right_pad` | implementations/text.py (Python row-loop) | cpdef | 3 | Padding. **FLAG: Python loop.** |
| `regex_replace` | implementations/text.py (Python row-loop) | cpdef | 3 | Regex replace (fallback). **FLAG: Python loop.** |
| `match_against` | implementations/text.py (Python row-loop) | cpdef | 2 | Full-text search. **FLAG: Python loop.** |

---

### Date / Time Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_unixtime` / `from_unixtimestamp` / `unixtime` | nanobind/vector_temporal_arith.cpp | nanobind | 1-2 | Unix timestamp conversion. |
| `vector_date_trunc` / `trunc_timestamp` / `trunc_date` | nanobind/vector_temporal_arith.cpp | nanobind | 2 | Temporal truncation. |
| `vector_date_format` / `date_format` | nanobind/vector_temporal_arith.cpp | nanobind | 2 | Format temporal as string. |
| `vector_date_part` / `date_part` | nanobind/vector_temporal_arith.cpp | nanobind | 2 | Extract date component. |
| `vector_date_diff` / `date_diff` / `time_diff` | nanobind/vector_temporal_arith.cpp | nanobind | 2-3 | Temporal difference. |
| `vector_floor_temporal` | nanobind/vector_temporal_arith.cpp | nanobind | 2 | Floor temporal to unit. |
| `date_functions.trunc_date` | implementations/temporal.py (cpdef) | cpdef | 2 | Date truncation. |
| `date_functions.date_part` | implementations/temporal.py (cpdef) | cpdef | 2 | Extract date part. |
| `vector_date32_to_timestamp` / `vector_timestamp_to_date32` | nanobind/vector_temporal_convert.cpp | nanobind | 1 | Temporal type conversion. |

---

### Boolean / Logical Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_coalesce` | nanobind/vector_special.cpp | nanobind | N | Null coalescing (variadic). |
| `vector_iif` | nanobind/vector_special.cpp | nanobind | 3 | IF-THEN-ELSE. |
| `_iif_kernel` | registrar/logical.pyx (closure) | Python closure | 3 | IIF wrapper. **FLAG: Closure.** |
| `vector_nullif` / `null_if` | nanobind/vector_special.cpp / implementations/logical.pyx | nanobind / cpdef | 2 | Return NULL if equal. |
| `_coalesce_kernel` | registrar/logical.pyx (closure) | Python closure | N | Coalesce wrapper. **FLAG: Closure.** |
| `vector_allop_eq` / `vector_allop_neq` | nanobind/vector_bool_ops.cpp | nanobind | 2 | Array comparison (all). |
| `vector_anyop_eq` / `vector_anyop_neq` / `vector_anyop_lt` / `vector_anyop_lte` / `vector_anyop_gt` / `vector_anyop_gte` | nanobind/vector_bool_ops.cpp | nanobind | 2 | Array comparison (any). |
| `vector_in_list` | nanobind/vector_bool_ops.cpp | nanobind | 2 | Membership test. |
| `bool_vector_and_chain` / `bool_vector_all_true` | nanobind/vector_bool_ops.cpp | nanobind | 1-N | Boolean chain ops. |

---

### Array Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_concat` | nanobind/vector_selection_concat.cpp | nanobind | 2 | Concatenate arrays. |
| `vector_contains_any` / `vector_contains_all` | nanobind/vector_bool_ops.cpp | nanobind | 2 | Array membership. |
| `_sort_kernel` | registrar/utility.pyx (closure) | Python closure | 1-2 | Array sort. **FLAG: Closure.** |
| `_greatest_kernel` / `_least_kernel` | registrar/utility.pyx (closure) | Python closure | N | Greatest/least. **FLAG: Closure.** |
| `array_contains` / `array_contains_any` / `array_contains_all` | implementations/utility.pyx (cpdef) | cpdef | 2 | Array containment checks. **FLAG: Python row-loop.** |
| `vector_array_reduce` | nanobind/vector_array_reduce.cpp | nanobind | 2 | Array reduction. |

---

### String Concatenation

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_concat` (overload for strings) | nanobind/vector_selection_concat.cpp | nanobind | 2 | String/array concat. |
| `_concat_ws_kernel` | registrar/text.pyx (closure) | Python closure | N | CONCAT_WS wrapper. **FLAG: Closure.** |

---

### Hashing / Encoding Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_md5` | nanobind/vector_hash_codec.cpp | nanobind | 1 | MD5 hash. |
| `vector_sha1` | nanobind/vector_hash_codec.cpp | nanobind | 1 | SHA1 hash. |
| `vector_sha256` | nanobind/vector_hash_codec.cpp | nanobind | 1 | SHA256 hash. |
| `vector_sha512` | nanobind/vector_hash_codec.cpp | nanobind | 1 | SHA512 hash. |
| `vector_base64_encode` / `vector_base64_decode` | nanobind/vector_codec.cpp | nanobind | 1 | Base64 codec. |
| `vector_hex_encode` / `vector_hex_decode` | nanobind/vector_codec.cpp | nanobind | 1 | Hex codec. |
| `vector_base85_encode` / `vector_base85_decode` | nanobind/vector_codec.cpp | nanobind | 1 | Base85 codec. |

---

### Similarity / Distance Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_cosine_similarity` | nanobind/vector_accessors.cpp | nanobind | 2 | Cosine similarity. |
| `vector_cosine_distance` | nanobind/vector_accessors.cpp | nanobind | 2 | Cosine distance. |
| `_cosine_similarity_text` / `_cosine_distance_text` | implementations/utility.pyx (Python row-loop) | cpdef | 2 | Text similarity. **FLAG: Python loop.** |
| `cosine_similarity` / `cosine_distance` | implementations/utility.pyx (wrapper) | cpdef | 2 | Wrapper for above. **FLAG: Python loop.** |
| `embed` | implementations/utility.pyx (Python row-loop) | cpdef | 1 | Embedding. **FLAG: Python loop.** |

---

### JSON Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_json_extract` | nanobind/vector_json.cpp | nanobind | 2-3 | JSON path extraction (→ / ->>). |

---

### IP / Network Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_ip_in_cidr` | nanobind/vector_misc.cpp | nanobind | 2 | IP address in CIDR block (via BOP_BITWISE_OR). |

---

### Other / Utility Functions

| Kernel Name | Location | Type | Arity | Notes |
|---|---|---|---|---|
| `vector_map_access` / `vector_map_access_string` / `vector_map_access_array` | nanobind/vector_special.cpp / draken_native.cpp | nanobind | 2 | Map/dict/struct access. |
| `vector_array_map_access` | draken/draken_native.cpp | C++ | 2 | Array map access (C++). |
| `vector_split` / `split` | nanobind/vector_split_native.cpp / implementations/text.py | nanobind / cpdef | 2-3 | String split. **cpdef is Python row-loop.** |
| `_concat_ws_kernel` | registrar/text.pyx | Python closure | N | CONCAT_WS. **FLAG: Closure.** |
| `vector_extract` | nanobind/vector_accessors.cpp (or implementations/utility.pyx) | nanobind / cpdef | 2 | Extract (tuple/array element). |
| `cast_to_double` / `cast_to_int` / `cast_to_varchar` / `cast_to_boolean` | opteryx/expression/casts.pyx | Python helper closure | 1 | Cast dispatch helpers. **FLAG: Python dispatch with native sub-paths.** |
| `_build_decimal_closure` / `_build_array_cast` / `_build_vector_cast` / `_build_residual_cast` | opteryx/expression/casts.pyx | Python closure | 1 | Specialized cast closures. **FLAG: Python row-loops.** |
| `if_null` / `if_not_null` | implementations/logical.py | cpdef | 2 | Null check variants. **FLAG: Python row-loop.** |
| `_greatest_kernel` / `_least_kernel` | registrar/utility.pyx | Python closure | N | Greatest/least. **FLAG: Closure.** |
| `jsonb_object_keys` | implementations/utility.pyx | cpdef | 1 | JSON object keys. **FLAG: Python row-loop.** |
| `humanize` | implementations/utility.pyx | cpdef | 1 | Humanize value. **FLAG: Python row-loop.** |
| `vector_random_strings` | nanobind/vector_misc.cpp | nanobind | 2-3 | Random string generation. |

---

---

## Risk Flags

### Kernels with C++ exception throws (Risk 1)
These require error handling (try/catch or out-param) to prevent exception propagation across the extern "C" boundary.

| Kernel Name | Category | Risk |
|---|---|---|
| `vector_json_extract` | BC_EXTRACTION | Likely throws on invalid JSON paths. |
| `vector_cast_*` (string → numeric) | BC_CAST | Likely throws on parse failures. |
| `vector_map_access_string` | BC_EXTRACTION | Likely throws on invalid keys. |
| `vector_ip_in_cidr` | BC_BINARY_OP | Likely throws on malformed CIDR. |
| `vector_concat` | BC_BINARY_OP | TBD — check nanobind implementation. |
| Math functions (sqrt, log, etc.) | BC_FUNCTION | May throw on domain errors. |

### Kernels that are Python closures (non-native)
These will require either:
- Writing a new C++ implementation, or
- Wrapping Python execution (non-ideal; violates CLAUDE.md §2/§3).

| Kernel Name | Category | Approach |
|---|---|---|
| `cast_to_double` | BC_CAST | Dispatch helper. Has native paths; fallback is Python row-loop. → **Write C wrapper dispatcher.** |
| `cast_to_int` | BC_CAST | Dispatch helper. Has native paths; fallback is Python row-loop. → **Write C wrapper dispatcher.** |
| `cast_to_varchar` | BC_CAST | Dispatch helper. Has native paths; fallback is Python row-loop. → **Write C wrapper dispatcher.** |
| `cast_to_boolean` | BC_CAST | Dispatch helper. Has native paths; fallback is Python row-loop. → **Write C wrapper dispatcher.** |
| `cast_to_date` | BC_CAST | Python row-loop. → **Write C++ implementation OR evaluate necessity.** |
| `_decimal_cast` | BC_CAST | Python row-loop via OrsoTypes.DECIMAL.parse. → **Likely needs C++ impl.** |
| `_array_cast` | BC_CAST | Python row-loop. → **Check if in scope for Phase 9.** |
| `_vector_cast` | BC_CAST | Python row-loop via OrsoTypes.VECTOR.parse. → **Check if in scope.** |
| `_varchar_cast_with_length` | BC_CAST | Python row-loop. → **Check if in scope.** |
| `_date_interval_op_draken` | BC_BINARY_OP | Draken-native Python wrapper. → **Likely has C++ backing; check.** |
| `_date_minus_date_draken` | BC_BINARY_OP | Draken-native Python wrapper. → **Likely has C++ backing; check.** |
| INTERVAL ± INTERVAL kernels | BC_BINARY_OP | Python closures. → **Check scope for Phase 9.** |
| String/Math/Date functions | BC_FUNCTION | TBD — depends on implementation. |

---

## Next Steps for Architect Review

1. **Completeness**: Is this list exhaustive? Any kernels missing from BC_EXTRACTION, BC_CAST, BC_BINARY_OP?
2. **BC_FUNCTION scope**: Should 9a enumerate and implement ALL SQL functions, or only a priority subset for initial unblock?
3. **Python closure strategy**: For kernels that are Python row-loops (e.g., `cast_to_double`, `_decimal_cast`), should 9a write C implementations or defer to 9f cleanup?
4. **Error handling**: Approve the proposed thread-local error slot + sentinel `VecResult` pattern for exception safety?
5. **Arithmetic wrapper**: Is the unified `draken_binary_arith(ctx, left, right)` wrapper approach acceptable for Draken Vector methods?

---

## Definition of Done (Inventory Phase)

**Completed**:
- [x] Enumerate BC_EXTRACTION kernels (4 sub-ops).
- [x] Enumerate BC_CAST kernels by source/target pair, flag Python closures.
- [x] Enumerate BC_BINARY_OP kernels by operator category.
- [x] Enumerate BC_FUNCTION kernels (~90+ total), categorized by function type.
- [x] Flag kernels with C++ exception risk.
- [x] Flag kernels that are Python closures (non-native) and Python row-loops.
- [x] Confirm **aggregates are out of 9a scope** (handled separately by aggregate operators).

**Architecture Decisions Made**:
- ✅ **Full enumeration** — all ~90+ BC_FUNCTION kernels listed, not a subset.
- ✅ **Aggregates out of scope** — 9a does not handle SUM, MIN, MAX, COUNT, etc.
- ✅ **C-native dispatch helpers** — `cast_to_double`, `cast_to_int`, etc. need C implementations, not Python wrappers.
- ✅ **Decomposed arithmetic** — separate `draken_add`, `draken_sub`, `draken_mul`, `draken_div`, `draken_mod` C functions, not unified dispatcher.
- ✅ **Reuse error handling** — arena/sentinel pattern from DV fast paths.

**Risk Flags**:
- 🚩 **Python row-loops flagged** in BC_FUNCTION table: `to_ascii`, `to_char`, `left_pad`, `right_pad`, `regex_replace`, `match_against`, `cosine_similarity` (text variant), `embed`, `split` (cpdef), `if_null`, `array_contains*`, `jsonb_object_keys`, `humanize`, and all `_build_*` cast closures.
- 🚩 **Python closures flagged**: `_iif_kernel`, `_coalesce_kernel`, `_concat_ws_kernel`, `_sort_kernel`, `_greatest_kernel`, `_least_kernel`.
- 🚩 **Dispatch helpers needing C implementation**: `cast_to_double`, `cast_to_int`, `cast_to_varchar`, `cast_to_boolean`, `cast_to_date`.

**Inventory ready for Phase 9a implementation** — proceed with C ABI header design and context struct definitions.
