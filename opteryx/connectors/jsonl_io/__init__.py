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

Because rugo infers each chunk's schema independently (there is no working
explicit_schema override to pin every chunk to one schema), JsonlReadNode is
responsible for validating that every chunk's decoded columns/types agree
with the schema resolved at bind time, and failing loudly if they don't.
"""

from typing import Iterator, Optional, Sequence

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.expression import NodeType
from opteryx.types.logical_type import LogicalCategory
from rugo.jsonl import read_jsonl as _rugo_read_jsonl

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
]

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
):
    """Decode one newline-aligned chunk into a single Draken Morsel via rugo.

    ``columns``/``predicates`` are the pushed-down projection (physical,
    pre-alias names) and predicate tuples for this scan; rugo applies both
    while decoding this chunk. ``fail_on_error``/``infer_schema``/
    ``infer_sample_size`` are READ_JSONL's resolved options (Stage 3; see
    opteryx.planner.binder.dataset), forwarded unchanged to rugo. Returns
    ``None`` if every row in this chunk was filtered out by ``predicates`` --
    a benign zero-row result, not a decode failure (see the fixed bug note in
    rugo/jsonl/__init__.py's _JsonlReader.__iter__), so the caller should
    simply treat it as "this chunk contributed no rows" rather than raising.
    """
    with _rugo_read_jsonl(
        chunk,
        columns=columns,
        predicates=predicates,
        fail_on_error=fail_on_error,
        infer_schema=infer_schema,
        infer_sample_size=infer_sample_size,
    ) as reader:
        return next(iter(reader), None)
