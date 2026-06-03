"""
Shared helpers for the ClickBench result-checking battery.

The golden results are generated offline by `dev/clickbench/generate_golden.py`
(DuckDB as oracle over the same parquet the engine reads) and checked in as
JSON. The test battery loads them and compares — no DuckDB at test time.

Co-hosting DuckDB and Opteryx (mimalloc) in one process is unstable, so the two
sides run in separate processes: the generator (DuckDB) writes the golden, the
battery (Opteryx-only) reads it. This mirrors the TPC-H result battery.

Comparison is value-correctness oriented: order-insensitive (rows are sorted)
and decimal/float-tolerant (numbers rounded to 2dp). Row ORDER is intentionally
not asserted — ordering on grouped output is tracked separately. Both sides
normalise through `normalize_rows`, so a correct result compares byte-for-byte.

NOTE on `ORDER BY ... LIMIT N` queries: order-insensitive comparison of a
limited result set is only valid when both engines select the SAME rows. When
the ORDER BY key has ties straddling the LIMIT boundary, the two engines may
keep different tie rows — a false mismatch, not a correctness bug. Such queries
are triaged in the battery's EXCLUDED set with the reason.
"""

import datetime
import decimal
import json
import math
import os
import re

# Dataset registry: logical name -> (opteryx relation, duckdb parquet glob).
# Globs are relative to the repo root.
DATASETS = {
    "tiny": ("testdata.clickbench_tiny", "testdata/clickbench_tiny/hits_*.parquet"),
    "full": ("scratch.hits", "scratch/hits/hits_*.parquet"),
}

DEFAULT_DATASET = "tiny"

_HERE = os.path.dirname(os.path.abspath(__file__))


def golden_path(dataset: str) -> str:
    return os.path.join(_HERE, "test_data", "clickbench", f"golden_{dataset}.json")


def query_key(statement: str) -> str:
    """Extract the stable ``qNN`` key from a ``/* NN */`` statement prefix."""
    match = re.search(r"/\*\s*(\d+)\s*\*/", statement)
    if match is None:
        raise ValueError(f"no /* NN */ query marker in statement: {statement[:60]}")
    return f"q{int(match.group(1)):02d}"


def _cell(value):
    """Normalise a single cell to a JSON-safe, comparison-stable form.

    Numbers (int/float/Decimal) collapse to a float rounded to 10 SIGNIFICANT
    figures (relative tolerance), so the same logical value compares equal
    regardless of physical type or trailing precision. A relative tolerance is
    required for float aggregates: e.g. AVG over a large-magnitude column is
    computed in double on both engines but the summation order differs, so the
    results agree only to ~13 sig figs — an absolute 2dp rounding would reject a
    correct ~1e18 result over a ~1e5 difference.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return ["b", value]
    if isinstance(value, (int, float, decimal.Decimal)):
        f = float(value)
        if not math.isfinite(f):
            return ["n", str(f)]
        return ["n", float(f"{f:.10g}")]
    if isinstance(value, (datetime.datetime, datetime.date)):
        return ["t", value.isoformat()]
    if isinstance(value, (bytes, bytearray)):
        return ["s", bytes(value).decode("utf-8", "replace")]
    return ["s", str(value)]


def normalize_rows(rows):
    """Normalise and sort rows for order-insensitive, type-tolerant comparison."""
    norm = [[_cell(v) for v in row] for row in rows]
    norm.sort(key=repr)
    return norm


def opteryx_result_rows(session, sql):
    """Run a query through execute_to_morsels and return a list of row tuples."""
    rows = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        names = morsel.column_names
        cols = [morsel.column(name).to_pylist() for name in names]
        for i in range(morsel.num_rows):
            rows.append(tuple(col[i] for col in cols))
    return rows


def load_golden(dataset: str):
    """Load the checked-in golden results for a dataset, or {} if not generated."""
    path = golden_path(dataset)
    if not os.path.exists(path):
        return {}
    with open(path, "r") as handle:
        return json.load(handle)
