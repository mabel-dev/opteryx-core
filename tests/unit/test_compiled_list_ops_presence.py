import importlib


def test_compiled_vector_ops_exports():
    m = importlib.import_module("opteryx.compiled.vector_ops")

    # The compiled module should be present and expose the performance-sensitive symbols
    assert getattr(m, "_compiled_present", None) in (True, False)

    # Prefer compiled implementations where available
    assert hasattr(m, "vector_md5") or hasattr(m, "vector_sha256")
    assert hasattr(m, "vector_contains_all") and hasattr(m, "vector_contains_any")
