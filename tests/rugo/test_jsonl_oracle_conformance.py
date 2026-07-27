# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rugo <-> stdlib-json oracle conformance suite for JSONL — mirrors
test_oracle_conformance.py's Parquet matrix and test_csv_oracle_conformance.py's
CSV matrix, scoped to JSONL.

PyArrow has no JSONL writer, so it can't play oracle here the way it does for
Parquet/CSV. Unlike Parquet's page/encoding format (many valid physical
encodings of the same logical value) JSON text has one meaning per RFC 8259 —
so Python's stdlib `json` module (the reference implementation for the wire
format, not a competing engine) is a legitimate independent oracle:

  READ  : stdlib json.dumps builds a typed line   -> rugo reads it     -> values match.
  WRITE : rugo writes that column                  -> stdlib json.loads -> values match.

Type scope matches what rugo's JSONL reader actually infers: bool, int64,
double, string, and (uniform-scalar-element) arrays — see rugo/jsonl/__init__.py's
read_jsonl docstring. Shapes: DENSE, SPARSE (one null), ALLNULL, EMPTY —
matching the Parquet/CSV oracles' convention. Array element nulls are out of
scope (undocumented support); only whole-value (outer) nulls are exercised.
"""

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import draken  # noqa: F401 — must precede rugo native imports
import draken.draken_native as dn
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.rugo_native import read_jsonl as _read_jsonl
from rugo.rugo_native import write_jsonl

# ─────────────────────────────────────────────────────────────────────────────
# Oracle plumbing
# ─────────────────────────────────────────────────────────────────────────────


def oracle_write_jsonl(values) -> bytes:
    """Hand-built ground truth: one JSON object per line via stdlib json."""
    return ("\n".join(json.dumps({"v": v}) for v in values) + "\n").encode() if values else b""


def rugo_read_jsonl(data: bytes) -> list:
    r = _read_jsonl(
        data,
        columns=None,
        predicates=None,
        explicit_schema=None,
        infer_schema=True,
        infer_sample_size=5,
        parse_arrays=True,
        parse_objects=True,
        fail_on_error=True,
        use_threads=True,
    )
    if not r["success"]:
        # success=False only ever means "zero rows" here (see rugo/jsonl/__init__.py).
        return []
    return r["columns"][0].to_pylist()


def rugo_write_jsonl(values, dtype_name: str, is_array: bool) -> bytes:
    vec = dn.vector_array_from_sequence(values) if is_array else vector_from_sequence(
        values, dtype=dtype_name
    )
    morsel = Morsel.from_vectors(["v"], [vec])
    return write_jsonl(morsel)


def oracle_read_jsonl(data: bytes) -> list:
    """Independent parse of rugo's own WRITE output, via stdlib json."""
    text = data.decode()
    if not text:
        return []
    return [json.loads(line)["v"] for line in text.splitlines()]


# ─────────────────────────────────────────────────────────────────────────────
# Type matrix
# ─────────────────────────────────────────────────────────────────────────────
# Each case: (id, dtype_name, is_array, dense_values).

SCALAR_CASES = [
    ("bool", "BOOLEAN", False, [True, False, True]),
    ("int64", "INT64", False, [-(2**63), 0, 2**63 - 1]),
    ("float64", "DOUBLE", False, [-1.5, 0.0, 2.5]),
    ("string", "VARCHAR", False, ["", "abc", "a longer utf-8 ☃ string"]),
]

ARRAY_CASES = [
    ("list_int64", None, True, [[1, 2], [], [3]]),
    ("list_string", None, True, [["a", "bb"], [], ["c"]]),
]

ALL_CASES = SCALAR_CASES + ARRAY_CASES
SHAPES = ["dense", "sparse", "allnull", "empty"]


def _shape_values(dense: list, shape: str) -> list:
    if shape == "dense":
        return list(dense)
    if shape == "sparse":
        return [dense[0], None, dense[2]]
    if shape == "allnull":
        return [None, None, None]
    if shape == "empty":
        return []
    raise AssertionError(shape)


# ─────────────────────────────────────────────────────────────────────────────
# READ oracle: stdlib json builds a line → rugo reads → values match
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
@pytest.mark.parametrize("shape", SHAPES)
def test_read_matches_json_oracle(case, shape):
    case_id, _dtype_name, _is_array, dense = case
    values = _shape_values(dense, shape)
    data = oracle_write_jsonl(values)

    got = rugo_read_jsonl(data)
    if shape == "empty":
        assert got == []
        return
    assert got == values, f"READ mismatch {case_id}/{shape}: rugo={got!r} expected={values!r}"


# ─────────────────────────────────────────────────────────────────────────────
# WRITE oracle: rugo writes → stdlib json parses → values match
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("case", ALL_CASES, ids=[c[0] for c in ALL_CASES])
@pytest.mark.parametrize("shape", SHAPES)
def test_write_roundtrips_through_json_oracle(case, shape):
    case_id, dtype_name, is_array, dense = case
    values = _shape_values(dense, shape)
    data = rugo_write_jsonl(values, dtype_name, is_array)

    got = oracle_read_jsonl(data)
    assert got == values, (
        f"WRITE round-trip mismatch {case_id}/{shape}: rugo->json={got!r} expected={values!r}"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
