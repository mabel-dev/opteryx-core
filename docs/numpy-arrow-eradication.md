# NumPy & PyArrow Eradication Audit

This document is a live audit of remaining NumPy and PyArrow usage in the `opteryx-core` tree.

## Scope

This file tracks:
- **hot-path elimination candidates** — code that still touches NumPy/PyArrow in expression, execution, or utility code
- **boundary-only uses** — code that intentionally bridges to Arrow or NumPy at the edges
- **vendored / third-party code** — usage outside the core engine, which may be left alone unless explicitly targeted

The goal is not “remove every import everywhere”; the goal is:

> **Keep NumPy and PyArrow out of the execution engine unless a file is an explicit interop boundary.**

---

## Audit rules

A usage is classified as one of:

- **Hot path**  
  Still affects expression evaluation, filters, type coercion, function execution, or scan execution.

- **Boundary / interop**  
  Acceptable if it exists solely to convert to or from Arrow/NumPy at the edge of the engine.

- **Legacy / dead**  
  No longer needed, or reachable only by old paths that have been removed from planning/execution.

- **Needs decision**  
  The file still uses NumPy/PyArrow, but whether it should remain is an architectural choice.

---

## Current live audit list

### 1) `opteryx/expression/operations/type_coercion.py`
- **Imports:** `numpy`, `pyarrow`
- **Refs:** `numpy[2]`, `pyarrow[12]`
- **Status:** hot path
- **Why it matters:** used by filter coercion in `filter_operations()`
- **Audit note:** this is still a real execution-path dependency, not just a convenience wrapper
- **Decision:** likely needs replacement or specialization

### 2) `opteryx/expression/operations/fastpath_dictionary.py`
- **Imports:** `pyarrow`
- **Status:** hot path / boundary hybrid
- **Why it matters:** dictionary-array fast path for filter operations
- **Audit note:** may be acceptable as a narrow boundary helper, but still part of active filtering logic
- **Decision:** review for replacement with Draken-native handling where possible

### 3) `opteryx/expression/operations/special_ops.py`
- **Imports:** `pyarrow`
- **Status:** hot path
- **Why it matters:** JSON-path helper currently returns Arrow arrays
- **Audit note:** active if JSON operators remain supported
- **Decision:** keep only if there is no Draken-native equivalent yet

### 4) `opteryx/expression/functions/implementations/text.py`
- **Imports:** `numpy`, `pyarrow`, `pyarrow as pa`, `pyarrow.compute`
- **Refs:** `numpy[41]`, `pyarrow[32]`
- **Status:** mixed
- **Why it matters:** string and embedding-related function kernels still use Arrow/NumPy in several places
- **Audit note:** some functions are legitimate boundary adapters, but others are still active mixed-path kernels
- **Decision:** split into boundary-only helpers vs kernels that can be made Draken-native

### 5) `opteryx/expression/functions/implementations/temporal.py`
- **Imports:** `numpy`, `pyarrow`, `pyarrow.compute`
- **Refs:** `numpy[4]`, `pyarrow[18]`
- **Status:** hot path
- **Why it matters:** temporal function kernels still depend on Arrow/NumPy for conversion and dispatch
- **Audit note:** active expression-layer code, not dead support code
- **Decision:** strong candidate for continued elimination

### 6) `opteryx/expression/functions/implementations/utility.py`
- **Imports:** `numpy`, `pyarrow`
- **Refs:** `numpy[17]`, `pyarrow[6]`
- **Status:** hot path / mixed
- **Why it matters:** utility kernels include array, JSON, and vector scoring helpers
- **Audit note:** this file historically mixes genuine helpers with Arrow/NumPy conversion glue
- **Decision:** must be audited function-by-function

### 7) `opteryx/expression/functions/registrar/__init__.py`
- **Imports:** `pyarrow`
- **Refs:** `pyarrow[1]`
- **Status:** boundary / registry helper
- **Why it matters:** function-kernel wrapper helpers may still construct Arrow arrays
- **Audit note:** likely acceptable only if it is truly just registration-layer glue
- **Decision:** keep only if it is not in the runtime hot path

### 8) `opteryx/expression/functions/registrar/arithmetic.py`
- **Imports:** `pyarrow.compute`
- **Refs:** `pyarrow[2]`
- **Status:** boundary / registry metadata
- **Why it matters:** registers arithmetic kernels, some still Arrow-backed
- **Audit note:** likely not hot path itself, but indicates residual Arrow-backed function definitions
- **Decision:** review whether each registered kernel is still intended

### 9) `opteryx/expression/functions/registrar/arithmetic_extended.py`
- **Imports:** `pyarrow.compute`
- **Refs:** `pyarrow[2]`
- **Status:** boundary / registry metadata
- **Why it matters:** same pattern as above
- **Decision:** same as arithmetic registrar

### 10) `opteryx/expression/functions/registrar/constant.py`
- **Imports:** `numpy`
- **Refs:** `numpy[5]`
- **Status:** plan-time boundary
- **Why it matters:** uses `numpy.datetime64` for compile-time constant folding
- **Audit note:** this is not an execution hot path, but it is still a real NumPy dependency
- **Decision:** either keep as plan-time interop or replace with native `datetime`

### 11) `opteryx/expression/intervals.py`
- **Imports:** `pyarrow`
- **Refs:** `pyarrow[8]`
- **Status:** mixed
- **Why it matters:** temporal interval helpers often bridge Arrow types
- **Audit note:** needs file-level inspection before deciding whether it is boundary-only or still engine-adjacent
- **Decision:** likely boundary/helper, but not yet verified

### 12) `opteryx/models/dataframe.py`
- **Imports:** `pyarrow`
- **Refs:** `pyarrow[3]`
- **Status:** boundary
- **Why it matters:** dataframe/result abstraction
- **Audit note:** likely acceptable as a result interchange layer
- **Decision:** probably keep unless the result API is being redesigned

### 13) `opteryx/models/execution_context.py`
- **Imports:** `pyarrow`
- **Refs:** `pyarrow[1]`
- **Status:** boundary / metadata
- **Why it matters:** execution metadata and context handling
- **Audit note:** likely acceptable if only used for output/context bridging
- **Decision:** keep unless it leaks into hot execution

### 14) `opteryx/types/schema.py`
- **Imports:** `pyarrow`
- **Refs:** `pyarrow[1]`
- **Status:** boundary
- **Why it matters:** schema-to-Arrow conversion is explicit interop
- **Audit note:** acceptable if it stays isolated to schema conversion
- **Decision:** keep as a boundary module unless schema conversion is redesigned

### 15) `opteryx/utils/dates.py`
- **Status:** clean
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Why it matters:** no NumPy or PyArrow imports remain in the current file
- **Audit note:** this file is no longer an active eradication target
- **Decision:** remove from the active audit list

### 16) `opteryx/utils/arrow.py`
- **Status:** removed
- **Why it matters:** this file does not exist in the current tree
- **Audit note:** stale path removed from the audit
- **Decision:** do not track

### 17) `opteryx/utils/sql.py`
- **Status:** clean
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Why it matters:** no NumPy or PyArrow imports remain in the current file
- **Audit note:** this file is no longer an active eradication target
- **Decision:** remove from the active audit list

### 18) `opteryx/third_party/maki_nage/distogram.py`
- **Imports:** `numpy`
- **Refs:** `numpy[1]`
- **Status:** vendored / external
- **Why it matters:** third-party statistics helper
- **Audit note:** not core engine code, but still a live NumPy dependency
- **Decision:** leave unless vendored cleanup is explicitly in scope

---

## Already removed or no longer part of the active audit target

These were previously part of the audit but are not currently treated as primary active targets:

- `opteryx/utils/parquet_decoder.py` — removed
- `opteryx/connectors/catalogs/local_catalog.py` — removed
- `opteryx/connectors/catalogs/gcs_catalog.py` — appears unused / legacy shim
- legacy Arrow fallback in `opteryx/expression/__init__.py` — removed
- old Arrow-based expression append path — removed

---

## Recommended next priorities

### Priority 1: active expression-layer code
1. `opteryx/expression/functions/implementations/utility.py`
2. `opteryx/expression/functions/implementations/temporal.py`
3. `opteryx/expression/operations/type_coercion.py`
4. `opteryx/expression/functions/implementations/text.py`

### Reference counts
- `opteryx/expression/functions/implementations/utility.py` — `numpy[17]`, `pyarrow[6]`
- `opteryx/expression/functions/implementations/temporal.py` — `numpy[4]`, `pyarrow[18]`
- `opteryx/expression/operations/type_coercion.py` — `numpy[2]`, `pyarrow[12]`
- `opteryx/expression/functions/implementations/text.py` — `numpy[41]`, `pyarrow[32]`
- `opteryx/expression/intervals.py` — `numpy[0]`, `pyarrow[8]`
- `opteryx/types/schema.py` — `numpy[0]`, `pyarrow[1]`
- `opteryx/expression/functions/registrar/arithmetic.py` — `numpy[0]`, `pyarrow[2]`
- `opteryx/expression/functions/registrar/arithmetic_extended.py` — `numpy[0]`, `pyarrow[2]`
- `opteryx/expression/functions/registrar/constant.py` — `numpy[5]`, `pyarrow[0]`
- `opteryx/expression/operations/fastpath_dictionary.py` — `numpy[0]`, `pyarrow[11]`
- `opteryx/expression/operations/special_ops.py` — `numpy[0]`, `pyarrow[3]`

### Priority 2: boundary helpers that may still be too broad
5. `opteryx/expression/intervals.py`

### Priority 3: registry / metadata-only imports
6. `opteryx/expression/functions/registrar/__init__.py`
7. `opteryx/expression/functions/registrar/arithmetic.py`
8. `opteryx/expression/functions/registrar/arithmetic_extended.py`
9. `opteryx/expression/functions/registrar/constant.py`

### Priority 4: explicit interop modules
10. `opteryx/types/schema.py`
11. `opteryx/models/dataframe.py`
12. `opteryx/models/execution_context.py`

---

## Audit status summary

- **Hot-path NumPy/PyArrow still present:** yes
- **Expression core fully clean:** not yet
- **Arrow allowed only at boundaries:** mostly, but a few active utility/kernel files still violate that rule or are ambiguous
- **Biggest remaining work:** expression implementations, temporal coercion, utility helpers, and interval/date glue

---

## Notes for follow-up

When auditing a file, classify each NumPy/PyArrow usage as one of:

- **Must remove**
- **Boundary-only and acceptable**
- **Legacy / dead**
- **Needs architectural decision**

This is the useful distinction. A top-level import alone does not tell you enough; the call sites and data flow decide whether it belongs.
