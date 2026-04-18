# NumPy & PyArrow Eradication — Refreshed Import Audit

Generated: 2026-04-18T10:41:21.729Z (repository import-line scan limited to opteryx/)

Purpose
- Exact, actionable list of explicit `import`/`from` lines for `numpy` and `pyarrow` within the `opteryx/` package. Use this to drive surgical edits and update the eradication tracking matrix.

Findings (opteryx/ only)

Files that explicitly import numpy
- opteryx/expression/operations/type_coercion.py:3: import numpy

Files that explicitly import pyarrow (file:line:import)
- opteryx/__main__.py:188: from pyarrow import parquet
- opteryx/__main__.py:192: from pyarrow import csv
- opteryx/expression/__init__.py:530: import pyarrow as _pyarrow
- opteryx/expression/evaluator/function_execution.py:140: import pyarrow as _pa_abf
- opteryx/models/dataframe.py:65: import pyarrow
- opteryx/expression/operations/fastpath_dictionary.py:3: import pyarrow
- opteryx/expression/evaluator/evaluation.py:334: import pyarrow as _pa_local
- opteryx/expression/operations/type_coercion.py:4: import pyarrow
- opteryx/expression/evaluator/temporal_ops.py:116: import pyarrow as _pa_local
- opteryx/expression/evaluator/temporal_ops.py:163: import pyarrow as _pa_local
- opteryx/expression/evaluator/temporal_ops.py:208: import pyarrow as _pa_local

Notes
- Many references to numpy/pyarrow appear in tests, docs, dev scripts, and third_party; those are intentionally excluded from this list (this audit is opteryx/ only).
- Several modules the tracking matrix marked as "boundary" or "cold" do not currently contain direct `pyarrow` imports (e.g., opteryx/types/schema.py). Conversely, some hot-path modules do contain in-place imports that must be handled (listed above).
- Some imports are local/guarded (imported inside functions or under engine-specific branches). The file:line entries above indicate exactly where imports occur and whether they are import-level or local.

Recommended next steps
- [1] Produce a full import-line CSV (opteryx/, tests/, docs/, third_party/) to feed into the tracking matrix so it becomes authoritative.
- [2] For each `opteryx/` file listed above, open a small PR that (a) documents the interop, then (b) wraps the import behind an adapter or moves it to a non-hot path.
- [3] After adapters are in place, remove runtime dependency pins from packaging (move numpy to build-system requires if needed).

Do you want:
- A: a full repo-wide import-line CSV (includes tests/docs/third_party)?
- B: small remediation patches for the opteryx/ files above (one PR per file)?
- C: update docs/eradication-tracking-matrix.md to reflect these exact import locations now?

Reply with A, B, C (or multiple).