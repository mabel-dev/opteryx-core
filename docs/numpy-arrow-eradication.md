# NumPy & PyArrow Eradication Audit

This document is the authoritative, repo-wide audit of remaining NumPy and PyArrow usage in the `opteryx-core` tree. It replaces the earlier prioritized subset and records the full set of Python/Cython sources that contain explicit imports or token references to `numpy` and `pyarrow`, along with a classification and recommended next action for each.

Scope
- This audit covers:
  - All `.py` and `.pyx` files under `opteryx/` that contain explicit `import` / `from` lines for `numpy` or `pyarrow` or otherwise mention those tokens in active source code (not only the previously tracked hot-path subset).
  - Test, dev, and third-party vendored files in the repository where they are relevant to understanding the eradication surface.
- The goal remains:
  > Keep NumPy and PyArrow out of the execution engine except where a file is an explicit, isolated interop boundary.

Methodology (repo-wide)
- This audit uses a straightforward, conservative approach:
  - Files were identified by searching for occurrences of the tokens `numpy` and `pyarrow` in `.py` and `.pyx` sources.
  - For each file we recorded whether it contains explicit import statements for those libraries (local or top-level), whether the mention is in code vs comment/docstring, and an architectural classification (Hot-path, Boundary/Interop, Test/Dev, Doc/comment, or Cython/Generated).
  - Counts in the "Reference counts" section are token occurrences in the file (a lightweight indicator). They are not a semantic import graph — they indicate where code mentions the tokens and can be used to prioritize inspection.

Repo-wide findings (files with `numpy` / `pyarrow` presence)
- The following files in the `opteryx/` package contain explicit imports or token occurrences for NumPy/PyArrow (grouped and annotated). This is the superset of locations discovered by the repo scan.

Hot-path / execution-adjacent (action recommended)
- `opteryx/expression/operations/type_coercion.py` — imports: `numpy`, `pyarrow`  
  Classification: Hot-path (used by filter coercion and runtime coercions). Action: prioritize removal/specialization.
- `opteryx/expression/operations/fastpath_dictionary.py` — imports: `pyarrow`  
  Classification: Hot-path / boundary-hybrid (dictionary-array fast path for filtering). Action: evaluate Draken-native fast path replacement.
- `opteryx/expression/intervals.py` — imports: `pyarrow` (interval helpers)  
  Classification: Mixed (temporal helpers that bridge Arrow types). Action: inspect and isolate boundary conversions.
- `opteryx/expression/evaluator/temporal_ops.py` — local `pyarrow` imports in specific ops  
  Classification: Hot-path (temporal comparisons) — Action: replace Arrow conversions with Draken-native types where possible.
- `opteryx/expression/functions/implementations/utility.py` — imports: `numpy`, `pyarrow`  
  Classification: Hot-path / mixed (array helpers, vector scoring). Action: function-by-function audit and refactor to minimize NumPy/pyarrow use in execution.

Boundary / explicit interop (acceptable if kept isolated)
- `opteryx/models/dataframe.py` — `import pyarrow` inside `arrow()` method  
  Classification: Boundary (DataFrame ⇄ PyArrow conversion). Action: keep isolated; document as explicit interop module.
- `opteryx/planner/__init__.py` — `import pyarrow` when producing/combining tables  
  Classification: Boundary (planner returns PyArrow tables for tabular results). Action: verify isolation and document boundary responsibilities.
- `opteryx/types/schema.py` — `import pyarrow` (schema ↔ Arrow mapping)  
  Classification: Boundary (schema conversions). Action: keep as isolated conversion boundary or prepare alternative API if schema conversion is redesigned.
- `opteryx/expression/evaluator/evaluation.py` — local `pyarrow` import when converting Arrow arrays to Draken vectors  
  Classification: Boundary (conversion path). Action: keep conversion localized; consider moving to a small interop module.

Local / conditional imports in runtime paths (usually small & localized)
- `opteryx/expression/binary_operators.py` — local `pyarrow` imports in JSON/Arrow helpers  
- `opteryx/expression/evaluator/comparisons.py` — local `pyarrow` imports in a specific operator (e.g., JSON path / AtQuestion) and doctest examples  
- `opteryx/expression/evaluator/function_execution.py` — local `pyarrow` import when engine == "arrow"  
  Classification: Localized runtime imports for Arrow-backed engines. Action: document and, where feasible, gate behind explicit interop adapters.
- `opteryx/__main__.py` — local `from pyarrow import parquet/csv` for CLI output writing  
  Classification: CLI boundary (acceptable).

Cython / generated sources and notes
- Some `.pyx` and generated C/C++ sources mention pyarrow/numpy in comments or contain import lines (these are usually part of the compiled extension surface and must be treated carefully):
  - `opteryx/compiled/vector_ops/vector_math.pyx` — token mention in docstring/comments (note: compiled module)
  - `opteryx/operators/distinct_node.pyx` — contains `import pyarrow` in generated Cython code/comments
  - `opteryx/operators/read_node.pyx` — `import pyarrow`
  Classification: Cython / compiled components. Action: treat as a separate track; do not generate Python fallback implementations for Cython logic (per project rules). If these imports are interface-only for conversions, consider minimal interop shims rather than broad dependency reintroductions.

Tests / dev scripts / docs (do not block eradication)
- `opteryx/third_party/maki_nage/tests/*` — `import numpy` in tests (vendored third-party tests)
- Dev scripts and test utilities (not an exhaustive list from other directories) also reference `numpy`/`pyarrow` and are acceptable to keep as-is unless you want to remove test/dev dependencies. Action: leave unless you explicitly want to remove dev/test deps.

Other small/local mentions
- `opteryx/utils/firestore_utils.py` — local/reference `import numpy as _np` in helper/example
- `opteryx/utils/vector_types.py` — docstring examples referencing `pyarrow`

Reference counts (token occurrences per tracked file)
- These counts are token occurrences of the words `numpy` and `pyarrow` inside the files — a lightweight indicator to prioritize inspection. They are not semantic import graphs.
- Prioritized files (current token counts found in repo scan):
  - `opteryx/expression/functions/implementations/utility.py` — `numpy[78]`, `pyarrow[12]`
  - `opteryx/expression/functions/implementations/temporal.py` — `numpy[23]`, `pyarrow[22]`
  - `opteryx/expression/operations/type_coercion.py` — `numpy[2]`, `pyarrow[28]`
  - `opteryx/expression/operations/fastpath_dictionary.py` — `numpy[0]`, `pyarrow[18]`
  - `opteryx/expression/operations/special_ops.py` — `numpy[4]`, `pyarrow[5]`
  - `opteryx/expression/intervals.py` — `numpy[2]`, `pyarrow[13]`
  - `opteryx/types/schema.py` — `numpy[0]`, `pyarrow[24]`
  - `opteryx/expression/functions/registrar/constant.py` — `numpy[6]`, `pyarrow[0]`

Notes on discrepancies vs earlier document
- The earlier, shorter list in the document was an intentionally focused, prioritized subset of hot-path files. This full audit enumerates all Python/Cython sources under `opteryx/` that mention or import `numpy`/`pyarrow` so you can see the true eradication surface.
- Cython/compiled sources and developer/test scripts will expand the list beyond the previous prioritized set. This is expected and intentional.

Classification summary (recommended triage)
- Priority 1 (Immediate): Hot-path execution files where NumPy/PyArrow are actively used in evaluation or filtering logic
  - `opteryx/expression/functions/implementations/utility.py`
  - `opteryx/expression/functions/implementations/temporal.py`
  - `opteryx/expression/operations/type_coercion.py`
  - `opteryx/expression/operations/fastpath_dictionary.py`
  - `opteryx/expression/intervals.py`
  - `opteryx/expression/evaluator/temporal_ops.py`
- Priority 2 (Boundary hardening): Modules that are acceptable interop boundaries but must remain small, well documented and isolated
  - `opteryx/models/dataframe.py`
  - `opteryx/planner/__init__.py`
  - `opteryx/types/schema.py`
  - `opteryx/expression/evaluator/evaluation.py`
- Priority 3 (Low / Tests / Docs): Leave for now unless you want to remove dev/test dependencies
  - `opteryx/third_party/maki_nage/tests/*`, `dev/*` scripts, and doc/example mentions

Recommended immediate actions (practical checklist)
1. Import-level audit pass (precision): produce a per-file list of explicit `import` / `from` lines with line numbers (this enables exact remediation patches). This document already records the files; do that next.
2. Hot-path surgical refactors:
   - For `type_coercion.py`: replace use-cases that create Arrow buffers with explicit small conversion helpers in a dedicated `interop` module; aim to remove NumPy from hot path.
   - For `utility.py` and `temporal.py`: audit function-by-function. Factor out non-execution helpers into a conversion-only module; keep hot-path kernels free of heavy imports.
   - For `fastpath_dictionary.py`: implement Draken-native dictionary handling or move Arrow-only code behind a narrow boundary function.
3. Boundary hardening:
   - Ensure `models/dataframe.py`, `planner/__init__.py`, and `types/schema.py` are the only places allowed to import `pyarrow` at the top level. Document their exact responsibility in `docs/` and add module-level comments explaining the boundary contract.
4. Cython/compiled component review:
   - For any `.pyx` or generated C/C++ files that reference Arrow/NumPy, ensure they are either part of the compiled engine (allowed) or refactored into small interop adapters. Per project policy, do not generate Python fallbacks for Cython code unless explicitly requested.
5. Tests and dev scripts:
   - Decide if you want tests/dev scripts to keep NumPy/PyArrow. My recommendation: keep them unless you want to reduce dev dependencies; they do not affect runtime hot-path performance.

Appendix — discovered files (explicit import or mention in `opteryx/` sources)
- `opteryx/expression/__init__.py` (local `pyarrow` import path detected in evaluation logic)
- `opteryx/expression/binary_operators.py` (local `pyarrow` imports for JSON/Arrow helpers)
- `opteryx/expression/evaluator/evaluation.py` (local `pyarrow` import for conversion)
- `opteryx/expression/evaluator/function_execution.py` (local `pyarrow` import for Arrow engine)
- `opteryx/expression/evaluator/temporal_ops.py` (local `pyarrow` imports)
- `opteryx/expression/intervals.py` (top-level `pyarrow`)
- `opteryx/expression/functions/implementations/utility.py` (`numpy` + `pyarrow`)
- `opteryx/expression/functions/implementations/temporal.py` (`numpy` + `pyarrow`)
- `opteryx/expression/operations/fastpath_dictionary.py` (top-level `pyarrow`)
- `opteryx/expression/operations/type_coercion.py` (top-level `numpy` + `pyarrow`)
- `opteryx/operators/read_node.pyx` (import `pyarrow`)
- `opteryx/planner/__init__.py` (import `pyarrow` when composing tables)
- `opteryx/third_party/maki_nage/tests/*` (numpy in tests)
- `opteryx/types/schema.py` (pyarrow top-level)

# KEEP
- `opteryx/models/dataframe.py` (pyarrow in `arrow()` method)  # keep
- `opteryx/__main__.py` (pyarrow imports for CLI output)  # keep

Notes about the KEEP section
- Per your recent edits, the two files listed above are the only confirmed, intentional PyArrow imports that should be retained as explicit interop boundaries in the `opteryx/` package.
- You noted you haven't checked every file yet. This document therefore:
  - Marks `opteryx/models/dataframe.py` and `opteryx/__main__.py` as the canonical, intentionally-allowed Arrow boundaries for now.
  - Leaves the rest of the discovered files (listed above) as candidates for review/remediation — some are hot-paths that need action, others are acceptable boundaries, and some are tests/docs.

Suggested immediate next steps
- If you'd like, I will now:
  1) Run an import-line audit for all `.py` and `.pyx` files under `opteryx/` and produce a precise list of `import` / `from` lines (file + line number + import text). This will confirm whether any other files truly import Arrow at runtime versus only mentioning it.
  2) Produce a short remediation plan for the Priority-1 hot-path files (e.g., `type_coercion.py`, `fastpath_dictionary.py`, `functions/implementations/*`) showing minimal, safe refactors to isolate or remove NumPy/PyArrow from hot execution paths.

Which of the two outputs do you want next? If you want the import-line audit, I will produce it and then update this document with exact import lines and an updated KEEP/TO-REVIEW list.
