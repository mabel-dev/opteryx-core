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
- first_schema_chunk, that first chunk for a plain OR compressed (gzip / zstd /
  lz4) file — for a compressed one, the first chunk of the decompressed stream,
  cut by the same rule the native Source's streaming decompressor uses;
- the predicate-pushdown capability (JsonlPredicatePushable, JSONL_OP_XLAT) and the
  reader's supported-type declaration (JSONL_SUPPORTED_TYPES).
"""

from typing import Iterator
from typing import Optional

from draken.draken_native import DrakenType

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.expression import NodeType
from opteryx.types.logical_type import LogicalCategory
from rugo.rugo_native import decompressed_first_chunk
from rugo.rugo_native import detect_compression

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
    "first_schema_chunk",
    "DEFAULT_CHUNK_SIZE",
    "JsonlPredicatePushable",
    "JSONL_EMPTINESS_XLAT",
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
# (rugo/src/jsonl/_jsonl_reader.pxi _JSONL_OPS). Maps Opteryx's
# COMPARISON_OPERATOR.value names to rugo's operator strings -- note these are
# NOT the same strings as PredicatePushable.OPS_XLAT uses for other connectors
# (e.g. "=" there vs "==" here). InList/NotInList carry the literal's tuple of
# members as the value; rugo evaluates them as any-EQ / all-NE, NULL cell => no row.
JSONL_OP_XLAT = {
    "Eq": "==",
    "NotEq": "!=",
    "Gt": ">",
    "GtEq": ">=",
    "Lt": "<",
    "LtEq": "<=",
    "InList": "in",
    "NotInList": "not in",
}

# Emptiness tests, pushed as the rugo string comparison they were rewritten from
# (predicate_rewriter turns `col = ''` / `col <> ''` into IsEmpty / IsNotEmpty).
# Against an empty literal rugo's `==` / `!=` is a length check on the value span the
# structural scan already found (value_parser.cpp apply_op_bytes) — the adjacent-quotes
# test, no compare, no decode. NULL and absent keys fail both, as SQL `col = ''` /
# `col <> ''` do.
JSONL_EMPTINESS_XLAT = {
    "IsEmpty": "==",
    "IsNotEmpty": "!=",
}

# IN-list ops: the list literal is always the RIGHT operand (`column IN (...)`),
# so these have no `literal OP column` mirror form.
_JSONL_LIST_OPS = {"InList", "NotInList"}


class JsonlPredicatePushable(PredicatePushable):
    """Predicate-pushdown capability for READ_JSONL FunctionDataset nodes.

    Deliberately narrower than PredicatePushable's default ``can_push``: only
    a plain ``column OP literal`` comparison with an op in JSONL_OP_XLAT (for
    IN / NOT IN, ``column IN (literal list)`` only), or IsEmpty / IsNotEmpty on a
    plain string column (JSONL_EMPTINESS_XLAT), is representable as one of rugo's
    predicate tuples, so every other shape -- BETWEEN, any other UNARY_OPERATOR
    (IsNull/IsTrue/...), a boolean-valued FUNCTION --
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
        if condition.node_type == NodeType.UNARY_OPERATOR:
            if condition.value not in JSONL_EMPTINESS_XLAT:
                return False
            ident = condition.centre
            if ident is None or ident.node_type != NodeType.IDENTIFIER or ident.schema_column is None:
                return False
            return ident.schema_column.category in (LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR)
        if condition.node_type != NodeType.COMPARISON_OPERATOR:
            return False
        if condition.value not in JSONL_OP_XLAT:
            return False
        left, right = condition.left, condition.right
        if left is None or right is None:
            return False
        if left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
            ident = left
        elif condition.value in _JSONL_LIST_OPS:
            return False
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


def first_schema_chunk(
    data, path: str, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> Optional[memoryview]:
    """The first chunk the native JSONL Source decodes from this file — the bytes
    bind-time schema inference must read. None for a file with no bytes.

    A compressed file (gzip / zstd / lz4, by magic bytes) yields the first
    newline-aligned chunk of its DECOMPRESSED stream, decompressing only that far.
    An unsupported codec, or an extension the bytes contradict, raises RuntimeError
    naming the file — compressed bytes are never handed to the parser as text.
    """
    if detect_compression(data, path) is None:
        return next(iter_newline_chunks(data, chunk_size), None)
    chunk = decompressed_first_chunk(data, path, chunk_size)
    return None if chunk is None else memoryview(chunk)
