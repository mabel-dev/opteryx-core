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
- **Refs:** `numpy[2]`, `pyarrow[28]`
- **Status:** hot path
- **Why it matters:** used by filter coercion in `filter_operations()`
- **Audit note:** this is still a real execution-path dependency, not just a convenience wrapper
- **Decision:** likely needs replacement or specialization

### 2) `opteryx/expression/operations/fastpath_dictionary.py`
- **Imports:** `pyarrow`
- **Refs:** `numpy[0]`, `pyarrow[18]`
- **Status:** hot path / boundary hybrid
- **Why it matters:** dictionary-array fast path for filter operations
- **Audit note:** may be acceptable as a narrow boundary helper, but still part of active filtering logic
- **Decision:** review for replacement with Draken-native handling where possible

### 3) `opteryx/expression/operations/special_ops.py`
- **Imports:** `pyarrow`
- **Refs:** `numpy[4]`, `pyarrow[5]`
- **Status:** hot path
- **Why it matters:** JSON-path helper currently returns Arrow arrays
- **Audit note:** active if JSON operators remain supported
- **Decision:** keep only if there is no Draken-native equivalent yet

### 4) `opteryx/expression/functions/implementations/utility.py`
- **Imports:** `numpy`, `pyarrow`
- **Refs:** `numpy[78]`, `pyarrow[12]`
- **Status:** hot path / mixed
- **Why it matters:** utility kernels include array, JSON, and vector scoring helpers
- **Audit note:** this file historically mixes genuine helpers with Arrow/NumPy conversion glue
- **Decision:** must be audited function-by-function

### 5) `opteryx/expression/functions/implementations/temporal.py`
- **Imports:** `numpy`, `pyarrow`, `pyarrow.compute`
- **Refs:** `numpy[23]`, `pyarrow[22]`
- **Status:** hot path
- **Why it matters:** temporal function kernels still depend on Arrow/NumPy for conversion and dispatch
- **Audit note:** active expression-layer code, not dead support code
- **Decision:** strong candidate for continued elimination

### 6) `opteryx/expression/functions/implementations/text.py`
- **Status:** clean
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Why it matters:** no NumPy or PyArrow imports remain in the current file
- **Audit note:** this file is no longer an active eradication target
- **Decision:** remove from the active audit list

### 7) `opteryx/expression/functions/registrar/__init__.py`
- **Status:** clean
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Why it matters:** no NumPy or PyArrow imports remain in the current file
- **Audit note:** this file is no longer an active eradication target
- **Decision:** remove from the active audit list

### 8) `opteryx/expression/functions/registrar/arithmetic.py`
- **Imports (metadata):** (no current heavy Arrow/NumPy imports in the registrar module itself)
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Status:** boundary / registry metadata
- **Why it matters:** registers arithmetic kernels, some still Arrow-backed
- **Audit note:** likely not hot path itself, but indicates residual Arrow-backed function definitions
- **Decision:** review whether each registered kernel is still intended

### 9) `opteryx/expression/functions/registrar/constant.py`
- **Imports:** `numpy`
- **Refs:** `numpy[6]`, `pyarrow[0]`
- **Status:** plan-time boundary
- **Why it matters:** uses `numpy.datetime64` for compile-time constant folding
- **Audit note:** this is not an execution hot path, but it is still a real NumPy dependency
- **Decision:** either keep as plan-time interop or replace with native `datetime`

### 10) `opteryx/expression/intervals.py`
- **Imports:** `pyarrow`
- **Refs:** `numpy[2]`, `pyarrow[13]`
- **Status:** mixed
- **Why it matters:** temporal interval helpers often bridge Arrow types
- **Audit note:** needs file-level inspection before deciding whether it is boundary-only or still engine-adjacent
- **Decision:** likely boundary/helper, but not yet verified

### 11) `opteryx/models/dataframe.py`
- **Status:** clean
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Why it matters:** no NumPy or PyArrow imports remain in the current file
- **Audit note:** this file is no longer an active eradication target
- **Decision:** remove from the active audit list

### 12) `opteryx/models/execution_context.py`
- **Status:** clean
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Why it matters:** no NumPy or PyArrow imports remain in the current file
- **Audit note:** this file is no longer an active eradication target
- **Decision:** remove from the active audit list

### 13) `opteryx/types/schema.py`
- **Imports:** `pyarrow`
- **Refs:** `numpy[0]`, `pyarrow[24]`
- **Status:** boundary
- **Why it matters:** schema-to-Arrow conversion is explicit interop
- **Audit note:** acceptable if it stays isolated to schema conversion
- **Decision:** keep as a boundary module unless schema conversion is redesigned

### 14) `opteryx/utils/dates.py`
- **Status:** clean
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Why it matters:** no NumPy or PyArrow imports remain in the current file
- **Audit note:** this file is no longer an active eradication target
- **Decision:** remove from the active audit list

### 15) `opteryx/utils/sql.py`
- **Status:** clean
- **Refs:** `numpy[0]`, `pyarrow[0]`
- **Why it matters:** no NumPy or PyArrow imports remain in the current file
- **Audit note:** this file is no longer an active eradication target
- **Decision:** remove from the active audit list

### 16) `opteryx/third_party/maki_nage/distogram.pyx`
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

### Reference counts
- `opteryx/expression/functions/implementations/utility.py` — `numpy[78]`, `pyarrow[12]`
- `opteryx/expression/functions/implementations/temporal.py` — `numpy[23]`, `pyarrow[22]`
- `opteryx/expression/operations/type_coercion.py` — `numpy[2]`, `pyarrow[28]`
- `opteryx/expression/operations/fastpath_dictionary.py` — `numpy[0]`, `pyarrow[18]`
- `opteryx/expression/operations/special_ops.py` — `numpy[4]`, `pyarrow[5]`
- `opteryx/expression/intervals.py` — `numpy[2]`, `pyarrow[13]`
- `opteryx/types/schema.py` — `numpy[0]`, `pyarrow[24]`
- `opteryx/expression/functions/registrar/constant.py` — `numpy[6]`, `pyarrow[0]`

### Priority 2: boundary helpers that may still be too broad
4. `opteryx/expression/intervals.py`

### Priority 3: registry / metadata-only imports
5. `opteryx/expression/functions/registrar/arithmetic.py`
6. `opteryx/expression/functions/registrar/constant.py`

---

Notes on this update
- Removed the stale `arithmetic_extended` registrar reference from the "Reference counts" and "Priority 3" lists because the file is no longer present as a separate registrar module in the current tree.
- Updated reference counts for the tracked files to reflect current occurrences found in the codebase. These counts represent simple token occurrences of `numpy` / `pyarrow` in the file and are intended as a lightweight indicator (not a semantic import dependency graph).
- If you want, I can:
  - Re-run a fresh, precise scan to produce import-level counts (imports and attribute usages) rather than token counts.
  - Produce a small checklist and per-file plan (minimal patches) to remove NumPy/PyArrow from the hot-path files listed under Priority 1.

Meta: if you want me to change counting methodology (for example, count explicit `import` / `from` statements only, or produce a per-symbol map), tell me which method you prefer and I'll re-run the audit and update the document accordingly.
