
from opteryx.reference.reexport_catalogs import reexport_reference_catalogs


def test_reexport_reference_catalogs_writes_expected_targets(tmp_path):
    output_paths = reexport_reference_catalogs(tmp_path)

    assert output_paths == {
        "aggregates": tmp_path / "opteryx/reference/aggregates.json",
        "clauses": tmp_path / "opteryx/reference/clauses.json",
        "joins": tmp_path / "opteryx/reference/joins.json",
        "operators": tmp_path / "opteryx/reference/operators.json",
        "unary_ops": tmp_path / "opteryx/reference/unary_ops.json",
        "types": tmp_path / "opteryx/reference/types.json",
        "functions": tmp_path / "opteryx/functions/function_signatures.json",
    }

    for output_path in output_paths.values():
        assert output_path.exists()
        assert output_path.read_text(encoding="utf8").endswith("\n")
