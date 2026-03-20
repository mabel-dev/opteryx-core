from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf8")


def _slice_between(text: str, start_marker: str, end_marker: str) -> str:
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    return text[start:end]


def test_expression_dictionary_fastpath_section_has_no_arrow_numpy_materialization():
    text = _read("opteryx/expression/ops.py")
    section = _slice_between(
        text,
        "def _dictionary_fastpath(arr, operator, value):",
        "def _dictionary_supports_numeric_fastpath(arr):",
    )

    forbidden = (
        "compute.",
        "numpy.",
        "to_numpy(",
        "to_pylist(",
        "dictionary_decode(",
    )

    for token in forbidden:
        assert token not in section, f"unexpected token in dictionary fastpath section: {token}"


def test_expression_dictionary_fastpath_is_not_runtime_feature_gated_anymore():
    text = _read("opteryx/expression/ops.py")
    assert "features.draken_dict_expr_fastpath" not in text
    assert "dict_candidate = _has_dictionary_candidate(raw_arr)" in text


def test_groupby_dictionary_fastpath_is_not_runtime_feature_gated_anymore():
    text = _read("opteryx/operators/draken_aggregate_and_group_node.py")
    assert "features.draken_dict_groupby_fastpath" not in text
    assert "enable_dict_fastpath" not in text


def test_parquet_dictionary_decode_is_not_runtime_feature_gated_anymore():
    text = _read("third_party/mabel/rugo/parquet/parquet_reader.pyx")
    section = _slice_between(
        text,
        "cdef inline bint _should_emit_dictionary_vector(",
        "cdef inline bint _should_emit_constant_vector(",
    )
    assert "features.parquet_native_dictionary" not in section


def test_groupby_dictionary_motor_files_have_no_arrow_numpy_or_pylist():
    paths = [
        "opteryx/compiled/aggregations/carchar_group_state_engine.pyx",
        "opteryx/compiled/aggregations/group_by_draken.pyx",
        "opteryx/compiled/aggregations/group_by_draken_kernels/00_common.pyx",
        "opteryx/compiled/aggregations/group_by_draken_kernels/10_count_star_int64.pyx",
        "opteryx/compiled/aggregations/group_by_draken_kernels/20_count_distinct_int64.pyx",
        "opteryx/compiled/aggregations/group_by_draken_kernels/30_avg_int64_float64.pyx",
        "opteryx/compiled/aggregations/group_by_draken_kernels/90_factory.pyx",
    ]

    forbidden = ("pyarrow", "numpy", "to_pylist")

    for relpath in paths:
        text = _read(relpath)
        for token in forbidden:
            assert token not in text, f"unexpected token in {relpath}: {token}"


def test_legacy_dictionary_vector_source_is_removed():
    assert not (ROOT / "third_party/mabel/draken/vectors/dictionary_vector.pyx").exists()


def test_expression_constant_fastpath_section_has_no_arrow_numpy_materialization():
    text = _read("opteryx/expression/ops.py")
    section = _slice_between(
        text,
        "def _constant_fastpath(arr, operator, value):",
        "def _dictionary_vector(arr):",
    )

    forbidden = (
        "compute.",
        "numpy.",
        "to_numpy(",
        "to_pylist(",
    )

    for token in forbidden:
        assert token not in section, f"unexpected token in constant fastpath section: {token}"
