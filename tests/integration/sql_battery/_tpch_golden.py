"""
Shared helpers for the TPC-H result-checking SLT battery.

The golden results are generated offline by `dev/tpch/generate_slt_golden.py`
(which cross-checks Opteryx against DuckDB at the same scale factor) and checked
in as JSON. The test battery loads them and compares — no DuckDB at test time.

Comparison is value-correctness oriented: order-insensitive (rows are sorted)
and decimal/float-tolerant (numbers rounded to 2dp). Row ORDER is intentionally
not asserted here — ordering on grouped output is tracked as a separate issue.
Both the generator and the battery normalise through `normalize_rows` so the
two sides are guaranteed to agree byte-for-byte on a correct result.
"""

import datetime
import decimal
import json
import os

TPCH_SCALE = "001"  # 0.01 scale factor — small, hermetic, full schema.

_HERE = os.path.dirname(os.path.abspath(__file__))
GOLDEN_PATH = os.path.join(_HERE, "test_data", "tpch", f"golden_{TPCH_SCALE}.json")


def _cell(value):
    """Normalise a single cell to a JSON-safe, comparison-stable form.

    Numbers (int/float/Decimal) collapse to a rounded float so the same logical
    value compares equal regardless of physical type or trailing precision.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return ["b", value]
    if isinstance(value, (int, float, decimal.Decimal)):
        return ["n", round(float(value), 2)]
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


def load_golden():
    """Load the checked-in golden results, or {} if not generated yet."""
    if not os.path.exists(GOLDEN_PATH):
        return {}
    with open(GOLDEN_PATH, "r") as handle:
        return json.load(handle)
