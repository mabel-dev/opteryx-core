import json
from pathlib import Path

from opteryx.expression.functions.signatures import export_function_signatures

from opteryx.expression.functions import catalog as function_catalog_module


def test_function_signatures_json_matches_catalog_export():
    # Ensure the catalog is in its default state; previous tests may have
    # registered additional functions.
    function_catalog_module._CATALOG = None

    signatures_path = (
        Path(__file__).resolve().parents[3]
        / "opteryx/expression/functions/function_signatures.json"
    )

    expected = export_function_signatures()
    actual = json.loads(signatures_path.read_text(encoding="utf8"))

    assert actual == expected


def test_function_signatures_export_includes_enriched_metadata():
    # Ensure the catalog is in its default state; previous tests may have registered additional functions.
    function_catalog_module._CATALOG = None

    signatures = export_function_signatures()

    round_function = signatures["ROUND"]
    assert round_function["catalog_name"] == "ROUND"
    assert round_function["summary"] == "Round to nearest integer."
    assert round_function["volatility"] == "immutable"
    assert round_function["deterministic"] is True
    assert round_function["foldable"] is False
    assert round_function["pushdown_safe"] is False
    assert round_function["lifecycle"]["status"] == "active"
    assert round_function["lifecycle"]["replacement"] is None

    round_signature = signatures["ROUND"]["overloads"][0]
    assert round_signature["id"] == "ROUND_1"
    assert round_signature["category"] == "Numeric Functions"
    assert round_signature["return_type"] == "double"
    assert round_signature["returns"]["type"] == "double"
    assert round_signature["returns"]["documentation"] != round_signature["documentation"]
    assert "half-to-even" in round_signature["notes"]
    assert round_signature["arity"] == {"minimum": 1, "maximum": None, "variadic": True}
    assert round_signature["execution"]["kernel_id"] == "default"
    assert round_signature["execution"]["null_policy"] == "compress"
    assert round_signature["execution"]["cost_us_per_million"] == 2.0
    assert "It is listed under" not in round_signature["documentation"]
    assert "For the same inputs" not in round_signature["documentation"]
    assert "Numeric value to round." in round_signature["parameters"][0]["documentation"]
    assert round_signature["parameters"][0]["optional"] is False
    assert round_signature["parameters"][0]["variadic"] is False
    assert round_signature["parameters"][0]["constant_only"] is False
    assert round_signature["parameters"][0]["null_handling"] == "strict"
    assert "CEILING" in round_signature["related_functions"]
    assert round_signature["parameters"][1]["optional"] is True
    assert round_signature["parameters"][1]["variadic"] is True

    datepart_signature = signatures["EXTRACT"]["overloads"][0]
    assert datepart_signature["label"] == "EXTRACT(part FROM date)"
    assert datepart_signature["return_type"] == "integer | double | date"
    assert datepart_signature["category"] == "Date & Time Functions"
    assert "double" in datepart_signature["returns"]["documentation"]
    assert "constant expression" in datepart_signature["parameters"][0]["documentation"]
    assert "Optional. Can be repeated." in round_signature["parameters"][1]["documentation"]

    substring_overloads = signatures["SUBSTRING"]["overloads"]
    assert substring_overloads[0]["label"] == "SUBSTRING(str FROM start)"
    assert substring_overloads[1]["label"] == "SUBSTRING(str FROM start FOR length)"
    assert "Canonical SQL-92 form" in substring_overloads[0]["notes"]
    assert "SUBSTRING(str[, start[, length]])" in substring_overloads[0]["notes"]

    trunc_overloads = signatures["TRUNC"]["overloads"]
    assert trunc_overloads[0]["label"] == "TRUNC(num, [scale...])"
    assert trunc_overloads[0]["category"] == "Numeric Functions"
    assert "toward zero" in trunc_overloads[0]["notes"]
    assert trunc_overloads[1]["label"] == "TRUNC(value, unit)"
    assert trunc_overloads[1]["category"] == "Date & Time Functions"
    assert "start of the specified unit" in trunc_overloads[1]["documentation"]
    assert "constant expression" in trunc_overloads[1]["notes"]

    trim_signature = signatures["TRIM"]["overloads"][0]
    assert trim_signature["label"] == "TRIM([BOTH|LEADING|TRAILING] [chars] FROM str)"
    assert "TRIM(str[, chars])" in trim_signature["notes"]

    position_signature = signatures["POSITION"]["overloads"][0]
    assert position_signature["label"] == "POSITION(needle IN haystack)"
    assert "POSITION(needle, haystack)" in position_signature["notes"]

    assert signatures["CURRENT_DATE"]["overloads"][0]["label"] == "CURRENT_DATE"
    assert "CURRENT_DATE()" in signatures["CURRENT_DATE"]["overloads"][0]["notes"]
    assert signatures["CURRENT_TIME"]["overloads"][0]["label"] == "CURRENT_TIME"
    assert "CURRENT_TIME()" in signatures["CURRENT_TIME"]["overloads"][0]["notes"]
    assert signatures["CURRENT_TIMESTAMP"]["overloads"][0]["label"] == "CURRENT_TIMESTAMP"
    assert "CURRENT_TIMESTAMP()" in signatures["CURRENT_TIMESTAMP"]["overloads"][0]["notes"]

    assert "MATCH" in signatures
    assert signatures["MATCH"]["catalog_name"] == "_MATCH_AGAINST"
    match_signature = signatures["MATCH"]["overloads"][0]
    assert match_signature["label"] == "MATCH(str) AGAINST(pattern)"
    assert "Canonical form is `MATCH(str) AGAINST(pattern)`" in match_signature["notes"]

    concat_signature = signatures["CONCAT"]["overloads"][0]
    assert concat_signature["execution"]["null_policy"] == "passthru"

    assert signatures["INITCAP"]["aliases"] == ["TITLE", "TITLECASE"]
    assert "TITLE" not in signatures
    assert "TITLECASE" not in signatures
    assert "TITLECASE" not in signatures["INITCAP"]["overloads"][0]["related_functions"]

    assert signatures["CURRENT_TIMESTAMP"]["aliases"] == ["NOW"]
    assert "NOW" not in signatures

    assert signatures["EXTRACT"]["aliases"] == []
    assert "DATEPART" not in signatures
    assert "DATE_PART" not in signatures
    assert signatures["TRUNC"]["aliases"] == ["TRUNCATE"]
    assert "DATE_TRUNC" not in signatures
    assert "DATETRUNC" not in signatures
    assert "TRUNCATE" not in signatures

    assert signatures["CEILING"]["aliases"] == ["CEIL"]
    assert "CEIL" not in signatures

    for hidden_name in ("ARRAY", "CASE", "GET_STRING", "PASSTHRU", "TRY_ARRAY"):
        assert hidden_name not in signatures
