import importlib
import warnings


def test_retired_feature_draken_dict_expr_strict_warns(monkeypatch):
    monkeypatch.setenv("FEATURE_DRAKEN_DICT_EXPR_STRICT", "1")

    import opteryx.config as config

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        importlib.reload(config)

    assert any(
        "FEATURE_DRAKEN_DICT_EXPR_STRICT is retired and ignored" in str(item.message)
        for item in caught
    )


def test_retired_feature_draken_dict_expr_strict_absent_has_no_warning(monkeypatch):
    monkeypatch.delenv("FEATURE_DRAKEN_DICT_EXPR_STRICT", raising=False)

    import opteryx.config as config

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        importlib.reload(config)

    assert not any(
        "FEATURE_DRAKEN_DICT_EXPR_STRICT is retired and ignored" in str(item.message)
        for item in caught
    )


def test_retired_feature_draken_dict_expr_fastpath_warns(monkeypatch):
    monkeypatch.setenv("FEATURE_DRAKEN_DICT_EXPR_FASTPATH", "0")

    import opteryx.config as config

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        importlib.reload(config)

    assert any(
        "FEATURE_DRAKEN_DICT_EXPR_FASTPATH is retired and ignored" in str(item.message)
        for item in caught
    )


def test_retired_feature_draken_dict_expr_fastpath_absent_has_no_warning(monkeypatch):
    monkeypatch.delenv("FEATURE_DRAKEN_DICT_EXPR_FASTPATH", raising=False)

    import opteryx.config as config

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        importlib.reload(config)

    assert not any(
        "FEATURE_DRAKEN_DICT_EXPR_FASTPATH is retired and ignored" in str(item.message)
        for item in caught
    )


def test_retired_groupby_feature_attribute_removed(monkeypatch):
    monkeypatch.delenv("FEATURE_DRAKEN_DICT_GROUPBY_FASTPATH", raising=False)

    import opteryx.config as config

    importlib.reload(config)

    assert not hasattr(config.features, "draken_dict_groupby_fastpath")


def test_retired_parquet_dictionary_feature_attribute_removed(monkeypatch):
    monkeypatch.delenv("FEATURE_PARQUET_NATIVE_DICTIONARY", raising=False)

    import opteryx.config as config

    importlib.reload(config)

    assert not hasattr(config.features, "parquet_native_dictionary")


def test_serial_reader_selector_parses_connector_list(monkeypatch):
    monkeypatch.setenv("FEATURE_USE_SERIAL_READER", "LOCAL,S3")
    monkeypatch.delenv("FEATURE_PARQUET_LOCAL_SERIAL_FASTPATH", raising=False)

    import opteryx.config as config

    importlib.reload(config)

    assert config.features.use_serial_reader == frozenset({"LOCAL", "S3"})


def test_serial_reader_selector_none_disables_serial_reader(monkeypatch):
    monkeypatch.setenv("FEATURE_USE_SERIAL_READER", "NONE")
    monkeypatch.delenv("FEATURE_PARQUET_LOCAL_SERIAL_FASTPATH", raising=False)

    import opteryx.config as config

    importlib.reload(config)

    assert config.features.use_serial_reader == frozenset()
