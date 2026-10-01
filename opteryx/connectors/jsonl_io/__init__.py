# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
JSONL IO — the planning-side pieces of READ_JSONL and JSONL dataset scans.

Execution is native (src/cpp/engine/native_jsonl_scan_source.hpp): its own decode
pool cuts each file into newline-aligned chunks of DEFAULT_CHUNK_SIZE and decodes
them through rugo's C++ JSONL path. Nothing here runs during execution.

What lives here:
- DEFAULT_CHUNK_SIZE, the chunk the native Source cuts at;
- iter_newline_chunks, used at BIND time to read a file's first chunk for schema
  inference — the same newline-aligned cut, so the bound schema comes from exactly
  the bytes the native Source's first chunk of that file holds;
- the predicate-pushdown capability (JsonlPredicatePushable, JSONL_OP_XLAT) and the
  reader's supported-type declaration (JSONL_SUPPORTED_TYPES).
"""

from typing import Iterator

from draken.draken_native import DrakenType

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.expression import NodeType
from opteryx.types.logical_type import LogicalCategory

# 128MB: interleaved A/B on JSONBench 10m (2026-09-30) put 128MB ahead of 64MB
# (total 0.973x/0.984x over two batches) and of 256MB (which regressed Q3/Q4).
DEFAULT_CHUNK_SIZE = 128 * 1024 * 1024

# Bound on how far past a chunk boundary we scan for the newline to extend to.
# Real JSONL records are far shorter than this; a miss here means the file has
# a single line longer than the probe window, and we fall back to scanning the
# rest of the buffer.
_NEWLINE_PROBE_WINDOW = 1024 * 1024

__all__ = [
    "iter_newline_chunks",
    "DEFAULT_CHUNK_SIZE",
    "JsonlPredicatePushable",
    "JSONL_OP_XLAT",
    "JSONL_SUPPORTED_TYPES",
]

# DrakenTypes the JSONL reader can currently produce/serve (Stage 1 limits).
# This is the READER's capability declaration — the binder's READ_JSONL branch
# and the filesystem connector's JSONL-dataset schema inference both gate on it.
# See the fuller rationale where the binder consumes it
# (opteryx/planner/binder/dataset.py, above the READ_JSONL branch).
JSONL_SUPPORTED_TYPES = {
    DrakenType.INT64,
    DrakenType.FLOAT64,
    DrakenType.BOOL,
    DrakenType.VARCHAR,
    DrakenType.NULL,
    DrakenType.ARRAY,
    DrakenType.VARIANT,
}

# Comparison ops rugo's (column, op, value) predicate tuples can express
# (rugo/src/jsonl/_jsonl_reader.pxi: op in ['==', '!=', '<', '<=', '>', '>=']).
# Maps Opteryx's COMPARISON_OPERATOR.value names to rugo's operator strings --
# note these are NOT the same strings as PredicatePushable.OPS_XLAT uses for
# other connectors (e.g. "=" there vs "==" here).
JSONL_OP_XLAT = {
    "Eq": "==",
    "NotEq": "!=",
    "Gt": ">",
    "GtEq": ">=",
    "Lt": "<",
    "LtEq": "<=",
}


class JsonlPredicatePushable(PredicatePushable):
    """Predicate-pushdown capability for READ_JSONL FunctionDataset nodes.

    Deliberately narrower than PredicatePushable's default ``can_push``: only
    a plain ``column OP literal`` comparison with an op in JSONL_OP_XLAT is
    representable as one of rugo's predicate tuples, so every other shape --
    BETWEEN, UNARY_OPERATOR (IsNull/IsEmpty/...), a boolean-valued FUNCTION --
    is rejected here rather than relying on PredicatePushable.can_push's
    generic "boolean function is its own predicate" bypass, which would mark
    something unpushable-to-rugo as pushable with no way to translate it at
    physical-plan time. Rejected predicates are left as ordinary Filter nodes
    above the scan by the optimizer -- a missed optimization, never a dropped
    predicate.
    """

    supports_predicate_pushdown = True

    PUSHABLE_OPS = {op: True for op in JSONL_OP_XLAT}

    # NVARCHAR is only ever a NESTED `->>` column on a JSONL scan (top-level strings
    # bind VARCHAR); rugo compares those as their rendered `->>` text, exactly as the
    # unpushed comparison would (rugo value_parser evaluate_nested_text).
    PUSHABLE_TYPES = {
        LogicalCategory.INTEGER,
        LogicalCategory.FLOAT,
        LogicalCategory.BOOLEAN,
        LogicalCategory.VARCHAR,
        LogicalCategory.NVARCHAR,
    }

    def can_push(self, operator, types=None) -> bool:
        condition = operator.condition
        if condition.node_type != NodeType.COMPARISON_OPERATOR:
            return False
        if condition.value not in JSONL_OP_XLAT:
            return False
        left, right = condition.left, condition.right
        if left is None or right is None:
            return False
        if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
            ident = left
        elif right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
            ident = right
        else:
            return False
        category = getattr(getattr(ident, "schema_column", None), "category", None)
        return category is None or category in self.PUSHABLE_TYPES


def iter_newline_chunks(data, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Iterator[memoryview]:
    """Split a buffer into newline-aligned chunks of ``chunk_size`` bytes.

    Every chunk boundary is pushed forward to the next newline so a JSONL
    record is never split across two chunks. Zero-copy -- yields memoryview
    slices of ``data``.
    """
    view = memoryview(data)
    length = len(view)
    start = 0
    while start < length:
        end = min(start + chunk_size, length)
        if end < length:
            window = bytes(view[end : end + _NEWLINE_PROBE_WINDOW])
            newline_pos = window.find(b"\n")
            if newline_pos == -1:
                newline_pos = bytes(view[end:]).find(b"\n")
                end = length if newline_pos == -1 else end + newline_pos + 1
            else:
                end = end + newline_pos + 1
        yield view[start:end]
        start = end
