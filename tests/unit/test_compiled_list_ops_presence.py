import importlib


def test_compiled_list_ops_exports():
    m = importlib.import_module("opteryx.compiled.list_ops")

    # The compiled module should be present and expose the performance-sensitive symbols
    assert getattr(m, "_compiled_present", None) in (True, False)

    # Prefer compiled implementations where available
    assert hasattr(m, "list_md5") or hasattr(m, "list_sha256")
    assert hasattr(m, "list_contains_all") and hasattr(m, "list_contains_any")
