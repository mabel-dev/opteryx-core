# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Output shape for `SHOW SOURCES FOR <table>` - the standing list.

One row per name in the dataset's `sources` (opteryx-catalog
PROVENANCE_DESIGN.md S2.2): the distinct, fully-qualified datasets whose data
is in the relation's CURRENT content, most recent first, at most 64. It is the
receipts along the current chain, materialised onto the dataset document so
that reading it costs one document and not a history walk - which is why this
statement, unlike SHOW SNAPSHOTS and SHOW LINEAGE, loads no history.

`complete` is the dataset's `sources-complete`, repeated on every row rather
than reported once, because a SHOW has one shape and a flag that appeared on
row one only would be a second shape hiding in the first. It is false when the
list may be missing a name: the 64 cap dropped one, or a commit in the chain
carried no receipt. An EMPTY list emits one row with `source_dataset` and
`position` null so that `complete` is still readable - a table with no
sources and an incomplete flag is "built from nothing that was recorded",
which is not the same as "built from nothing".

As with `snapshot_history`, the connector produces these dicts (see
OpteryxConnector.get_sources), so this module never imports opteryx_catalog.
Every source is named, whatever the caller may read - see `lineage_history`
for why that is a citation rather than a leak.
"""

from typing import Dict, List, Optional

# Column order IS the output column order. Values are the dtype tag handed to
# `vector_from_sequence`.
_SOURCE_LIST_COLUMNS = {
    # Null only on the single row of an empty list.
    "source_dataset": "VARCHAR",
    # 0 = most recent.
    "position": "INTEGER",
    "complete": "BOOLEAN",
}


def _source_list_column_types():
    from opteryx.types import logical_type as _lt

    return {
        "source_dataset": _lt.VARCHAR,
        "position": _lt.INT64,
        "complete": _lt.BOOLEAN,
    }


def sources_output_schema(relation_name: str = "$sources"):
    """The fixed RelationSchema `SHOW SOURCES FOR <table>` always returns.

    Every _SOURCE_LIST_COLUMNS column, never trimmed or projected. The
    row-count estimate is left to the caller (visit_show_sources), which holds
    the rows.
    """
    from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

    column_types = _source_list_column_types()
    return RelationSchema(
        name=relation_name,
        columns=[
            SchemaColumn(
                name=name,
                column_type=column_types[name],
                identity=mint_column_identity(relation_name, name),
            )
            for name in _SOURCE_LIST_COLUMNS
        ],
    )


def normalize_sources(
    names: Optional[List[str]], complete: Optional[bool]
) -> List[Dict[str, object]]:
    """The dataset's standing list as _SOURCE_LIST_COLUMNS rows.

    `names` is taken in the order the catalog keeps it - most recent first -
    and `position` is that order made explicit, so the number survives a
    consumer that re-sorts by name. `complete` is passed through untouched:
    None means the catalog did not say (a dataset object from before the
    field existed), and that is a different answer from either boolean.
    """
    if not names:
        return [{"source_dataset": None, "position": None, "complete": complete}]
    return [
        {"source_dataset": name, "position": position, "complete": complete}
        for position, name in enumerate(names)
    ]


def sources_to_morsel(rows: List[Dict[str, object]]):
    """Build the single `SHOW SOURCES FOR` Morsel from normalized rows, in
    the order they arrive - `position` already says what that order is."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    morsel = Morsel()
    for name, dtype in _SOURCE_LIST_COLUMNS.items():
        morsel.append_vector(
            name, vector_from_sequence([row.get(name) for row in rows], dtype=dtype)
        )
    return morsel
