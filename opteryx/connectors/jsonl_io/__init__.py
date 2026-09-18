# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
JSONL IO — thin glue between JsonlReadNode and rugo's JSONL reader.

rugo.jsonl.read_jsonl always decodes whatever buffer it is given into exactly
one Morsel (there is no lower-level streaming/chunked entry point exposed
today -- the reader's own chunked mode was removed as dead code). To let
JsonlReadNode stream morsels instead of buffering an entire file into one, the
file's bytes are split here into newline-aligned chunks and each chunk is
decoded through rugo independently, with the pushed-down projection/predicates
(Stage 2) passed to every chunk's decode.

The schema is resolved ONCE, at bind time, from the first record-bearing
chunk, and PINNED onto every chunk's decode as rugo's `explicit_schema`
(2026-09-17): each projected column is parsed strictly as its bound type, a
column this chunk lacks comes back typed and all-null, and a value that does
not fit the bound type fails loud naming the column, row and value. Before
this, rugo re-inferred every chunk from its own 5-row sample, and a column
that happened to be null for the first rows of a later chunk drifted to
VARCHAR and failed the whole query.
"""

from typing import Iterator, Optional, Sequence

from draken.draken_native import DrakenType
from draken.morsels.morsel import Morsel

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.expression import NodeType
from opteryx.types.logical_type import LogicalCategory
from rugo.rugo_native import read_jsonl as _rugo_read_jsonl

# Mirrors the chunk size used by the (now-removed) sequential chunked JSONL
# reader that used to live in rugo/src/jsonl/_jsonl_reader.pxi.
DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024

# Bound on how far past a chunk boundary we scan for the newline to extend to.
# Real JSONL records are far shorter than this; a miss here means the file has
# a single line longer than the probe window, and we fall back to scanning the
# rest of the buffer.
_NEWLINE_PROBE_WINDOW = 1024 * 1024

__all__ = [
    "iter_newline_chunks",
    "decode_chunk",
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

    PUSHABLE_TYPES = {
        LogicalCategory.INTEGER,
        LogicalCategory.FLOAT,
        LogicalCategory.BOOLEAN,
        LogicalCategory.VARCHAR,
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


def decode_chunk(
    chunk,
    columns: Optional[Sequence[str]] = None,
    predicates: Optional[Sequence[tuple]] = None,
    fail_on_error: bool = True,
    infer_schema: bool = True,
    infer_sample_size: int = 5,
    explicit_schema: Optional[dict] = None,
):
    """Decode one newline-aligned chunk via rugo.

    Returns ``(morsel, absent_columns)``. ``morsel`` is ``None`` if every row
    in this chunk was filtered out by ``predicates`` -- a benign zero-row
    result, not a decode failure, so the caller treats it as "this chunk
    contributed no rows". ``absent_columns`` lists the declared columns whose
    key appeared in NO record of the chunk (each is still in the morsel, typed
    and all-null); it is how the scan node tells a file that lacks the bound
    columns entirely from one where they are merely sparse.

    ``columns``/``predicates`` are the pushed-down projection (physical,
    pre-alias names) and predicate tuples for this scan. ``explicit_schema``
    is the bind-time schema pinned onto this chunk, ``{physical_name:
    str(ColumnType)}`` -- the platform's own type spelling, which rugo's
    declared-type vocabulary accepts verbatim. A value that does not fit its
    declared type raises ``ValueError`` from rugo naming the column, row and
    value. ``fail_on_error``/``infer_schema``/``infer_sample_size`` are
    READ_JSONL's resolved options (Stage 3; see opteryx.planner.binder.dataset),
    forwarded unchanged.

    Calls rugo's native entry point rather than the ``rugo.jsonl`` facade
    because the facade yields only the Morsel and drops ``absent_columns``.
    """
    result = _rugo_read_jsonl(
        chunk,
        columns=columns,
        predicates=predicates,
        explicit_schema=explicit_schema,
        fail_on_error=fail_on_error,
        infer_schema=infer_schema,
        infer_sample_size=infer_sample_size,
    )
    if not result["success"]:
        # Only ever means zero rows survived (see rugo/jsonl/__init__.py's
        # _JsonlReader.__iter__); genuine failures raise from rugo directly.
        return None, []
    morsel = Morsel.from_vectors(result["column_names"], result["columns"])
    return morsel, result["absent_columns"]
