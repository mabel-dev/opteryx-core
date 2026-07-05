import json
import os
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from reference import export_aggregate_catalog
from reference import export_clauses_catalog
from reference import export_joins_catalog
from reference import export_operator_catalog
from reference import export_type_catalog
from reference import export_unary_ops_catalog


def test_aggregate_catalog_json_matches_export():
    catalog_path = Path(__file__).resolve().parents[3] / "opteryx/reference/aggregates.json"

    expected = export_aggregate_catalog()
    actual = json.loads(catalog_path.read_text(encoding="utf8"))

    assert actual == expected


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


def test_unary_ops_catalog_json_matches_export():
    catalog_path = Path(__file__).resolve().parents[3] / "opteryx/reference/unary_ops.json"

    expected = export_unary_ops_catalog()
    actual = json.loads(catalog_path.read_text(encoding="utf8"))

    assert actual == expected


def test_joins_catalog_json_matches_export():
    catalog_path = Path(__file__).resolve().parents[3] / "opteryx/reference/joins.json"

    expected = export_joins_catalog()
    actual = json.loads(catalog_path.read_text(encoding="utf8"))

    assert actual == expected


def test_clauses_catalog_json_matches_export():
    catalog_path = Path(__file__).resolve().parents[3] / "opteryx/reference/clauses.json"

    expected = export_clauses_catalog()
    actual = json.loads(catalog_path.read_text(encoding="utf8"))

    assert actual == expected


def test_type_catalog_includes_runtime_metadata():
    catalog = export_type_catalog()
    assert "struct" not in catalog

    integer_type = catalog["integer"]
    assert integer_type["canonical_name"] == "INTEGER"
    assert integer_type["family"] == "numeric"
    assert integer_type["flags"]["numeric"] is True
    assert integer_type["flags"]["temporal"] is False
    assert integer_type["metadata"]["description"] == "Signed 64-bit integer."
    assert integer_type["metadata"]["example"] == "42"
    assert integer_type["metadata"]["min"] == -9223372036854775808
    assert integer_type["metadata"]["max"] == 9223372036854775807
    assert "int64" in integer_type["aliases"]
    assert "int32" in integer_type["ingestion_mappings"]["parquet_physical"]
    assert "int64" in integer_type["ingestion_mappings"]["jsonl"]

    decimal_type = catalog["decimal"]
    assert decimal_type["metadata"]["description"].startswith("Fixed-point decimal number")
    assert decimal_type["metadata"]["example"] == "123.45"
    assert decimal_type["parameterized_forms"] == ["DECIMAL(10,2)"]
    assert "decimal(...)" in decimal_type["ingestion_mappings"]["parquet_logical_patterns"]

    array_type = catalog["array"]
    assert array_type["family"] == "nested"
    assert array_type["flags"]["collection"] is True
    assert array_type["metadata"]["description"] == "Array of values of a single type."
    assert array_type["parameterized_forms"] == ["ARRAY<INTEGER>"]
    assert "array<...>" in array_type["ingestion_mappings"]["jsonl_patterns"]
    assert "integer" in array_type["element_type_aliases"]

    vector_type = catalog["vector"]
    assert vector_type["family"] == "vector"
    assert vector_type["metadata"]["description"] == "Fixed-length numeric vector."
    assert vector_type["metadata"]["example"] == "[0.1, 0.2, 0.3]"


def test_operator_catalog_includes_binder_matrix_metadata():
    catalog = export_operator_catalog()

    eq_operator = catalog["Eq"]
    assert eq_operator["ast_symbol"] == "Eq"
    assert eq_operator["friendly_name"] == "Equals"
    assert eq_operator["sql_symbol"] == "="
    assert eq_operator["node_kind"] == "comparison"
    assert eq_operator["category"] == "comparison"
    assert eq_operator["description"] == "Equality comparison."
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
    assert concat_operator["ast_symbol"] == "StringConcat"
    assert concat_operator["friendly_name"] == "Concatenation"
    assert concat_operator["sql_symbol"] == "||"
    assert concat_operator["node_kind"] == "binary"
    assert concat_operator["category"] == "binary"
    assert {
        "left_type": "varchar",
        "right_type": "varchar",
        "result_type": "varchar",
        "result_type_is_dynamic": False,
        "cost_estimate": 100.0,
    } in concat_operator["signatures"]

    map_access = catalog["MapAccess"]
    assert map_access["friendly_name"] == "Subscript access"
    assert map_access["sql_symbol"] == "[]"
    assert map_access["node_kind"] == "extraction"
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
    assert xor_operator["ast_symbol"] == "Xor"
    assert xor_operator["friendly_name"] == "Logical XOR"
    assert xor_operator["sql_symbol"] == "XOR"
    assert xor_operator["node_kind"] == "logical"
    assert xor_operator["category"] == "logical"
    assert xor_operator["description"] == "Logical exclusive OR."
    assert xor_operator["signature_count"] == 1
    assert xor_operator["result_types"] == ["boolean"]
    assert {
        "left_type": "boolean",
        "right_type": "boolean",
        "result_type": "boolean",
        "result_type_is_dynamic": False,
        "cost_estimate": 100.0,
    } in xor_operator["signatures"]


def test_aggregate_catalog_includes_execution_support():
    catalog = export_aggregate_catalog()

    count = catalog["COUNT"]
    assert count["ast_symbol"] == "COUNT"
    assert count["friendly_name"] == "Count"
    assert count["kernel_name"] == "count"
    assert count["category"] == "counting"
    assert count["status"] == "active"
    assert count["support"]["global"] is True
    assert count["support"]["grouped"] is True
    assert count["sql_forms"] == ["COUNT(*)", "COUNT(expr)", "COUNT(DISTINCT expr)"]

    any_value = catalog["ANY_VALUE"]
    assert any_value["kernel_name"] == "any_value"
    assert any_value["support"]["global"] is False
    assert any_value["support"]["grouped"] is True
    assert any_value["status"] == "active"
    assert set(catalog) == {
        "ANY_VALUE",
        "APPROX_COUNT_DISTINCT",
        "APPROX_PERCENTILE",
        "ARRAY_AGG",
        "AVG",
        "COUNT",
        "COUNT_DISTINCT",
        "MAX",
        "MIN",
        "SUM",
    }


def test_unary_ops_catalog_includes_supported_predicates():
    catalog = export_unary_ops_catalog()

    assert set(catalog) == {
        "IsFalse",
        "IsNotFalse",
        "IsNotNull",
        "IsNotTrue",
        "IsNull",
        "IsTrue",
        "Not",
    }

    is_null = catalog["IsNull"]
    assert is_null["syntax_forms"] == ["expr IS NULL"]
    assert is_null["node_type"] == "UNARY_OPERATOR"
    assert is_null["status"] == "active"

    not_expr = catalog["Not"]
    assert not_expr["node_type"] == "NOT"
    assert not_expr["syntax_forms"] == ["NOT expr"]


def test_joins_catalog_includes_supported_join_types():
    catalog = export_joins_catalog()

    assert set(catalog) == {
        "cross_join",
        "full_outer",
        "inner",
        "left_anti",
        "left_outer",
        "left_semi",
        "natural",
        "non_equi",
        "right_anti",
        "right_outer",
        "right_semi",
    }

    inner = catalog["inner"]
    assert inner["normalized_to"] == "inner"
    assert inner["physical_node"] == "DrakenInnerJoinNode"
    assert inner["status"] == "supported"

    right_semi = catalog["right_semi"]
    assert right_semi["status"] == "unsupported"
    assert right_semi["physical_node"] is None


def test_clauses_catalog_includes_query_and_statement_capabilities():
    catalog = export_clauses_catalog()

    assert "top" in catalog
    assert catalog["top"]["status"] == "unsupported"
    assert catalog["top"]["syntax_forms"] == ["SELECT TOP n ..."]

    assert catalog["where"]["scope"] == "query_clause"
    assert catalog["where"]["planner_entry"] == "plan_query"
    assert catalog["select"]["scope"] == "statement"
    assert catalog["with"]["planner_entry"] == "extract_ctes"
