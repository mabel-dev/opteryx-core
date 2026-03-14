import json
import os
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.reference import export_operator_catalog
from opteryx.reference import export_type_catalog


def test_type_catalog_json_matches_export():
    catalog_path = Path(__file__).resolve().parents[3] / "opteryx/reference/types.json"

    expected = export_type_catalog()
    actual = json.loads(catalog_path.read_text(encoding="utf8"))

    assert actual == expected


def test_operator_catalog_json_matches_export():
    catalog_path = Path(__file__).resolve().parents[3] / "opteryx/reference/operators.json"

    expected = export_operator_catalog()
    actual = json.loads(catalog_path.read_text(encoding="utf8"))

    assert actual == expected


def test_type_catalog_includes_runtime_metadata():
    catalog = export_type_catalog()

    integer_type = catalog["integer"]
    assert integer_type["canonical_name"] == "INTEGER"
    assert integer_type["family"] == "numeric"
    assert integer_type["flags"]["numeric"] is True
    assert integer_type["flags"]["temporal"] is False
    assert "int64" in integer_type["aliases"]
    assert "int32" in integer_type["ingestion_mappings"]["parquet_physical"]
    assert "int64" in integer_type["ingestion_mappings"]["jsonl"]

    decimal_type = catalog["decimal"]
    assert decimal_type["parameterized_forms"] == ["DECIMAL(10,2)"]
    assert "decimal(...)" in decimal_type["ingestion_mappings"]["parquet_logical_patterns"]

    array_type = catalog["array"]
    assert array_type["family"] == "nested"
    assert array_type["flags"]["collection"] is True
    assert array_type["parameterized_forms"] == ["ARRAY<INTEGER>"]
    assert "array<...>" in array_type["ingestion_mappings"]["jsonl_patterns"]
    assert "integer" in array_type["element_type_aliases"]


def test_operator_catalog_includes_binder_matrix_metadata():
    catalog = export_operator_catalog()

    eq_operator = catalog["Eq"]
    assert eq_operator["display_name"] == "="
    assert eq_operator["token"] == "="
    assert eq_operator["category"] == "comparison"
    assert eq_operator["summary"] == "Equality comparison."
    assert eq_operator["documentation"] == "Returns true when both operands compare equal."
    assert eq_operator["signature_count"] == 23
    assert eq_operator["result_types"] == ["boolean"]
    assert {
        "left_type": "integer",
        "right_type": "integer",
        "result_type": "boolean",
        "result_type_is_dynamic": False,
        "cost_estimate": 100.0,
    } in eq_operator["signatures"]

    concat_operator = catalog["StringConcat"]
    assert concat_operator["display_name"] == "||"
    assert concat_operator["category"] == "binary"
    assert {
        "left_type": "varchar",
        "right_type": "varchar",
        "result_type": "varchar",
        "result_type_is_dynamic": False,
        "cost_estimate": 100.0,
    } in concat_operator["signatures"]

    map_access = catalog["MapAccess"]
    assert map_access["display_name"] == "[]"
    assert map_access["category"] == "extraction"
    assert map_access["has_dynamic_result"] is True
    assert "dynamic" in map_access["notes"]
    assert {
        "left_type": "array",
        "right_type": "integer",
        "result_type": None,
        "result_type_is_dynamic": True,
        "cost_estimate": 100.0,
    } in map_access["signatures"]

    xor_operator = catalog["Xor"]
    assert xor_operator["display_name"] == "XOR"
    assert xor_operator["token"] == "XOR"
    assert xor_operator["category"] == "logical"
    assert xor_operator["summary"] == "Logical exclusive OR."
    assert xor_operator["signature_count"] == 1
    assert xor_operator["result_types"] == ["boolean"]
    assert {
        "left_type": "boolean",
        "right_type": "boolean",
        "result_type": "boolean",
        "result_type_is_dynamic": False,
        "cost_estimate": 100.0,
    } in xor_operator["signatures"]
