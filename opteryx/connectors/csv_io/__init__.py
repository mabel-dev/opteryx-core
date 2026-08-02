# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
CSV IO — thin glue between CsvReadNode and rugo's CSV reader.

Unlike rugo.jsonl.read_jsonl (which JsonlReadNode splits into newline-aligned
chunks to stream morsels out of a single-shot decoder -- see
opteryx.connectors.jsonl_io's module docstring), rugo.csv.read_csv has no
chunked entry point to work around: it already reads projection/predicates in
one native pass over the whole buffer and returns exactly one Morsel. There is
nothing to chunk, so CsvReadNode reads one Morsel per file, not one per chunk.
"""

from typing import Optional, Sequence

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.expression import NodeType
from opteryx.types.logical_type import LogicalCategory
from rugo.csv import read_csv as _rugo_read_csv

__all__ = [
    "read_csv_file",
    "CsvPredicatePushable",
    "CSV_OP_XLAT",
]

# Comparison ops rugo's (column, op, value) predicate tuples can express
# (rugo/src/csv/_csv_reader.pxi: op in ['==', '!=', '<', '<=', '>', '>=']).
# Same operator strings as JSONL_OP_XLAT's values -- not the same dict,
# because this connector's pushable shape is independent of JSONL's.
CSV_OP_XLAT = {
    "Eq": "==",
    "NotEq": "!=",
    "Gt": ">",
    "GtEq": ">=",
    "Lt": "<",
    "LtEq": "<=",
}


class CsvPredicatePushable(PredicatePushable):
    """Predicate-pushdown capability for READ_CSV FunctionDataset nodes.

    Deliberately narrower than PredicatePushable's default ``can_push`` --
    see JsonlPredicatePushable's identical reasoning in
    opteryx.connectors.jsonl_io. Only a plain ``column OP literal``
    comparison with an op in CSV_OP_XLAT is representable as one of rugo's
    predicate tuples; every other shape is left as an ordinary Filter node
    above the scan by the optimizer -- a missed optimization, never a
    dropped predicate.
    """

    supports_predicate_pushdown = True

    PUSHABLE_OPS = {op: True for op in CSV_OP_XLAT}

    # CSV columns are only ever typed INT64/FLOAT64/VARCHAR (rugo's
    # sniff_csv_column_types has no BOOL widening) -- no BOOLEAN entry here,
    # unlike JsonlPredicatePushable, since a CSV column can never be that type.
    PUSHABLE_TYPES = {
        LogicalCategory.INTEGER,
        LogicalCategory.FLOAT,
        LogicalCategory.VARCHAR,
    }

    def can_push(self, operator, types=None) -> bool:
        condition = operator.condition
        if condition.node_type != NodeType.COMPARISON_OPERATOR:
            return False
        if condition.value not in CSV_OP_XLAT:
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


def read_csv_file(
    data,
    columns: Optional[Sequence[str]] = None,
    predicates: Optional[Sequence[tuple]] = None,
    delimiter: str = ",",
    has_header: bool = True,
    fail_on_error: bool = True,
    infer_sample_size: int = 5,
):
    """Decode one CSV file (or buffer) into a single Draken Morsel via rugo.

    ``columns``/``predicates`` are the pushed-down projection (physical,
    pre-alias names) and predicate tuples for this scan. ``delimiter``/
    ``has_header``/``fail_on_error``/``infer_sample_size`` are READ_CSV's
    resolved options (see opteryx.planner.binder.dataset), forwarded
    unchanged to rugo. Always returns a Morsel (possibly zero rows) --
    unlike JSONL's decode_chunk, there is no "every row filtered out of this
    chunk" ambiguity to signal with None, because CSV reads the whole file
    in one pass rather than chunk by chunk.
    """
    with _rugo_read_csv(
        data,
        columns=columns,
        predicates=predicates,
        delimiter=delimiter,
        has_header=has_header,
        fail_on_error=fail_on_error,
        infer_sample_size=infer_sample_size,
    ) as reader:
        return next(iter(reader))
