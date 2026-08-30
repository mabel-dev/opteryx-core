# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Read a stored `dataset.metadata.sort_orders` value.

`sort_orders` has been written in three incompatible shapes over time - a
positional int index into the schema's columns, a bare column-name string, or
an Iceberg-style {"fields": [{"name", "direction"}]} dict - so anything reading
one has to know all three. This is the single place that does.

This mirrors opteryx_catalog.catalog.compaction.normalize_sort_order (the write
side owns the authoritative logic) rather than importing it: the
currently-installed opteryx_catalog wheel predates that helper.

Resolution precedence for naming a column is field_id -> name -> index.
"""

from typing import List, Optional


def _normalize_entry(entry) -> Optional[dict]:
    """One stored entry, in canonical form: {"name", "field_id", "index", "ascending"}."""
    if isinstance(entry, bool):
        return None  # bool is an int subclass; never a valid column index
    if isinstance(entry, int):
        return {"name": None, "field_id": None, "index": entry, "ascending": True}
    if isinstance(entry, str):
        return {"name": entry, "field_id": None, "index": None, "ascending": True}

    if isinstance(entry, dict):
        field = entry
        fields = entry.get("fields")
        if isinstance(fields, (list, tuple)) and fields:
            field = fields[0]
        if not isinstance(field, dict):
            return None

        name = field.get("name")
        field_id = field.get("source-id")
        if field_id is None:
            field_id = field.get("field-id")
        ascending = str(field.get("direction", "asc")).lower() != "desc"

        if name is None and field_id is None:
            return None
        return {"name": name, "field_id": field_id, "index": None, "ascending": ascending}

    return None


def normalize_sort_order(sort_orders) -> Optional[dict]:
    """Reduce a stored sort order to its PRIMARY sort key, in canonical form."""
    if not sort_orders:
        return None
    return _normalize_entry(sort_orders[0])


def _resolve_name(normalized: dict, columns) -> Optional[str]:
    name = normalized["name"]
    if name is None and normalized["field_id"] is not None:
        for column in columns:
            if getattr(column, "id", None) == normalized["field_id"]:
                name = column.name
                break
    if name is None and normalized["index"] is not None:
        index = normalized["index"]
        if 0 <= index < len(columns):
            name = columns[index].name
    return name


def sort_order_column_names(sort_orders, relation_schema) -> List[str]:
    """EVERY sort column, in priority order, resolved to column names.

    `normalize_sort_order` answers about the primary key alone, which is what a
    one-line display of a sort order wants. CLUSTER BY is a list, so recreating
    a table's layout needs all of them: rendering only the first would produce a
    statement that reads as complete and clusters by less than the table does.

    An entry that cannot be resolved to a column name is DROPPED rather than
    guessed at - but so is everything after it, because a clustering is an
    ordered key and a list with a hole in it is not the same layout with one
    column missing, it is a different layout.

    The Iceberg-style dict shape carries every field in one entry, so a stored
    value of that shape is one entry describing N columns; the older shapes
    carry one column each.
    """
    if not sort_orders:
        return []

    columns = relation_schema.columns if relation_schema is not None else []

    entries = []
    for entry in sort_orders:
        fields = entry.get("fields") if isinstance(entry, dict) else None
        if isinstance(fields, (list, tuple)) and fields:
            entries.extend(fields)
        else:
            entries.append(entry)

    names = []
    for entry in entries:
        normalized = _normalize_entry(entry)
        if normalized is None:
            break
        name = _resolve_name(normalized, columns)
        if name is None:
            break
        names.append(name)
    return names
