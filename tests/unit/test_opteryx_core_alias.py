def test_opteryx_core_alias():
    import opteryx as o
    import opteryx_core as oc

    # Basic sanity checks that the shim exposes the same API and version info
    assert oc.__version__ == o.__version__
    assert oc.query is o.query
    assert oc.__all__ == o.__all__
