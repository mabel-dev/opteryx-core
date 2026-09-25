from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf8")


def _slice_between(text: str, start_marker: str, end_marker: str) -> str:
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    return text[start:end]


def test_parquet_dictionary_decode_is_not_runtime_feature_gated_anymore():
    text = _read("rugo/src/parquet/parquet_reader.pxi")
    section = _slice_between(
        text,
        "cdef inline bint _should_emit_dictionary_vector(",
        "cdef inline bint _should_emit_constant_vector(",
    )
    assert "features.parquet_native_dictionary" not in section


def test_legacy_dictionary_vector_source_is_removed():
    assert not (ROOT / "third_party/mabel/draken/vectors/dictionary_vector.pyx").exists()
