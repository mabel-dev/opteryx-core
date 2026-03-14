"""Helpers for exporting a generated type catalog from runtime metadata."""

from __future__ import annotations

import json
from collections import OrderedDict
from collections import defaultdict
from pathlib import Path
from typing import Any

from orso.types import OrsoTypes

from opteryx.rugo.converters.orso import JSONL_ARRAY_INNER_TYPE_ALIASES
from opteryx.rugo.converters.orso import JSONL_TYPE_MAP
from opteryx.rugo.converters.orso import ORSO_TYPE_ALIASES
from opteryx.rugo.converters.orso import PARQUET_LOGICAL_COMPLEX_PREFIXES
from opteryx.rugo.converters.orso import PARQUET_LOGICAL_TYPE_MAP
from opteryx.rugo.converters.orso import PARQUET_PHYSICAL_TYPE_MAP

_TYPE_PARAMETER_PROBES = OrderedDict(
    [
        (OrsoTypes.ARRAY, ["ARRAY<INTEGER>"]),
        (OrsoTypes.DECIMAL, ["DECIMAL(10,2)"]),
    ]
)


def _type_id(type_name: OrsoTypes | None) -> str | None:
    if type_name is None or type_name == OrsoTypes._MISSING_TYPE:
        return None
    return type_name.name.lower()


def _type_family(type_name: OrsoTypes) -> str:
    if type_name in {OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL}:
        return "numeric"
    if type_name in {OrsoTypes.DATE, OrsoTypes.TIME, OrsoTypes.TIMESTAMP}:
        return "temporal"
    if type_name in {OrsoTypes.ARRAY, OrsoTypes.STRUCT, OrsoTypes.JSONB}:
        return "nested"
    if type_name == OrsoTypes.BOOLEAN:
        return "boolean"
    if type_name == OrsoTypes.BLOB:
        return "binary"
    if type_name == OrsoTypes.VARCHAR:
        return "text"
    if type_name == OrsoTypes.INTERVAL:
        return "interval"
    if type_name == OrsoTypes.NULL:
        return "null"
    return "other"


def _type_flags(type_name: OrsoTypes) -> dict[str, bool]:
    parameterized_forms = _parameterized_forms(type_name)
    return {
        "numeric": bool(type_name.is_numeric()) and type_name != OrsoTypes.BOOLEAN,
        "temporal": bool(type_name.is_temporal()),
        "collection": type_name in {OrsoTypes.ARRAY, OrsoTypes.STRUCT, OrsoTypes.JSONB},
        "parameterized": bool(parameterized_forms),
    }


def _parameterized_forms(type_name: OrsoTypes) -> list[str]:
    supported_forms: list[str] = []
    for sample in _TYPE_PARAMETER_PROBES.get(type_name, []):
        parsed_type, *_ = OrsoTypes.from_name(sample)
        if parsed_type == type_name:
            supported_forms.append(sample)
    return supported_forms


def _alias_groups() -> dict[str, list[str]]:
    grouped: defaultdict[str, set[str]] = defaultdict(set)
    for alias, target in ORSO_TYPE_ALIASES.items():
        grouped[target].add(alias)
    return {key: sorted(values) for key, values in grouped.items()}


def _storage_mapping_groups(
    mapping: dict[str, OrsoTypes],
) -> dict[str, list[str]]:
    grouped: defaultdict[str, set[str]] = defaultdict(set)
    for spelling, type_name in mapping.items():
        type_id = _type_id(type_name)
        if type_id:
            grouped[type_id].add(spelling)
    return {key: sorted(values) for key, values in grouped.items()}


def export_type_catalog() -> OrderedDict[str, dict[str, Any]]:
    aliases_by_type = _alias_groups()
    parquet_physical = _storage_mapping_groups(PARQUET_PHYSICAL_TYPE_MAP)
    parquet_logical = _storage_mapping_groups(PARQUET_LOGICAL_TYPE_MAP)
    jsonl = _storage_mapping_groups(JSONL_TYPE_MAP)

    exported: dict[str, dict[str, Any]] = {}
    for type_name in sorted(OrsoTypes, key=lambda item: item.name):
        if type_name == OrsoTypes._MISSING_TYPE:
            continue

        type_id = _type_id(type_name)
        if type_id is None:
            continue

        aliases = aliases_by_type.get(type_id, [])
        entry: dict[str, Any] = {
            "canonical_name": type_name.name,
            "aliases": aliases,
            "accepted_spellings": sorted({type_id, *aliases}),
            "family": _type_family(type_name),
            "flags": _type_flags(type_name),
            "parameterized_forms": _parameterized_forms(type_name),
            "ingestion_mappings": {
                "parquet_physical": parquet_physical.get(type_id, []),
                "parquet_logical": parquet_logical.get(type_id, []),
                "jsonl": jsonl.get(type_id, []),
            },
        }

        if type_name == OrsoTypes.ARRAY:
            entry["ingestion_mappings"]["parquet_logical_patterns"] = ["array<...>"]
            entry["ingestion_mappings"]["jsonl_patterns"] = ["array<...>"]
            entry["element_type_aliases"] = sorted(
                set(JSONL_ARRAY_INNER_TYPE_ALIASES.values()) | {"integer", "double", "blob", "boolean", "jsonb"}
            )
        elif type_name == OrsoTypes.DECIMAL:
            entry["ingestion_mappings"]["parquet_logical_patterns"] = ["decimal(...)"]
        elif type_name == OrsoTypes.TIME:
            entry["ingestion_mappings"]["parquet_logical_patterns"] = [
                pattern + "..."
                for pattern, mapped_type in PARQUET_LOGICAL_COMPLEX_PREFIXES.items()
                if mapped_type == OrsoTypes.TIME
            ]
        elif type_name == OrsoTypes.TIMESTAMP:
            entry["ingestion_mappings"]["parquet_logical_patterns"] = [
                pattern + "..."
                for pattern, mapped_type in PARQUET_LOGICAL_COMPLEX_PREFIXES.items()
                if mapped_type == OrsoTypes.TIMESTAMP
            ]

        exported[type_id] = entry

    ordered = OrderedDict()
    for name in sorted(exported):
        ordered[name] = exported[name]
    return ordered


def write_type_catalog(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_type_catalog(), indent=4) + "\n",
        encoding="utf8",
    )
