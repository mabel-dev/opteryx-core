
from reference.reexport_catalogs import reexport_reference_catalogs


def test_reexport_reference_catalogs_writes_expected_targets(tmp_path):
    output_paths = reexport_reference_catalogs(tmp_path)

    assert output_paths == {
        "aggregates": tmp_path / "reference/aggregates.json",
        "clauses": tmp_path / "reference/clauses.json",
        "expressions": tmp_path / "reference/expressions.json",
        "joins": tmp_path / "reference/joins.json",
        "operators": tmp_path / "reference/operators.json",
        "unary_ops": tmp_path / "reference/unary_ops.json",
        "variables": tmp_path / "reference/variables.json",
        "types": tmp_path / "reference/types.json",
        "functions": tmp_path / "reference/function_signatures.json",
        "windows": tmp_path / "reference/windows.json",
    }

    for output_path in output_paths.values():
        assert output_path.exists()
        assert output_path.read_text(encoding="utf8").endswith("\n")
