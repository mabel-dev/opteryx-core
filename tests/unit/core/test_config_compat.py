import importlib
import warnings

import pytest


def _reload_config():
    import opteryx.config as config

    return importlib.reload(config)


def test_get_bool_unset_uses_default(monkeypatch):
    monkeypatch.delenv("DISABLE_OPTIMIZER", raising=False)
    config = _reload_config()
    assert config.DISABLE_OPTIMIZER is False


def test_get_bool_true(monkeypatch):
    monkeypatch.setenv("DISABLE_OPTIMIZER", "true")
    config = _reload_config()
    assert config.DISABLE_OPTIMIZER is True


def test_get_bool_false(monkeypatch):
    monkeypatch.setenv("DISABLE_OPTIMIZER", "false")
    config = _reload_config()
    assert config.DISABLE_OPTIMIZER is False


def test_get_bool_zero(monkeypatch):
    monkeypatch.setenv("DISABLE_OPTIMIZER", "0")
    config = _reload_config()
    assert config.DISABLE_OPTIMIZER is False


def test_get_bool_one(monkeypatch):
    monkeypatch.setenv("DISABLE_OPTIMIZER", "1")
    config = _reload_config()
    assert config.DISABLE_OPTIMIZER is True


def test_get_bool_mixed_case_and_whitespace(monkeypatch):
    monkeypatch.setenv("DISABLE_OPTIMIZER", "  YeS ")
    config = _reload_config()
    assert config.DISABLE_OPTIMIZER is True


def test_get_bool_default_true_flag_can_be_disabled(monkeypatch):
    # ENABLE_ZERO_COPY defaults True: the old bool(str) idiom could never turn it off.
    monkeypatch.setenv("ENABLE_ZERO_COPY", "false")
    config = _reload_config()
    assert config.ENABLE_ZERO_COPY is False


def test_get_bool_default_true_flag_unset(monkeypatch):
    monkeypatch.delenv("ENABLE_ZERO_COPY", raising=False)
    config = _reload_config()
    assert config.ENABLE_ZERO_COPY is True


def test_get_bool_unrecognised_value_raises(monkeypatch):
    monkeypatch.setenv("DISABLE_OPTIMIZER", "maybe")
    with pytest.raises(ValueError):
        _reload_config()


def test_get_bool_feature_flag_false(monkeypatch):
    monkeypatch.setenv("FEATURE_ENABLE_DPCCP_JOIN_PLANNING", "0")
    config = _reload_config()
    assert config.features.enable_dpccp_join_planning is False


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
