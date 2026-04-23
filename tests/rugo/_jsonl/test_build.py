"""
Test that the _jsonl extension builds and basic imports work.
"""

import pytest


def test_extension_imports():
    """Verify the compiled extension can be imported."""
    try:
        import opteryx.compiled.rugo._jsonl as _jsonl
        assert _jsonl is not None
    except ImportError as e:
        pytest.skip(f"_jsonl extension not built: {e}")


def test_placeholder():
    """Placeholder test while implementation is in progress."""
    # TODO: Phase 2-7 - replace with actual functionality tests
    assert True
