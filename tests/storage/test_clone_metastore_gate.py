# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Only an Opteryx-backed relation can be the source of a CLONE.

A fork borrows the upstream's manifest entries and pins the snapshot they came
from against expiration. Both are mechanics of our own snapshot store, so a
relation held in an external metastore - Iceberg, Postgres - has nothing for a
fork to borrow and nothing keeping its files in place.

The question is asked PER RELATION, because one OpteryxConnector fronts the
native catalog for one workspace and an external metastore for the next; a
class-level flag on the connector cannot tell them apart. What is tested here
is the engine's half: that the connector puts the question to the metastore
holding the relation, that an undeclared answer is an error rather than a
guess, and that the sibling catalog declares what this depends on.
"""

import importlib.util
import os
import sys

import pytest

from opteryx.connectors import connector_factory
from opteryx.connectors import register_workspace
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import UnsupportedSyntaxError


class _NativeCatalog:
    """A metastore providing the snapshot and fork-registry mechanics."""

    supports_forking = True

    def __init__(self, workspace=None, **kwargs):
        pass


class _ExternalCatalog:
    """An external metastore - what IcebergMetastore inherits from Metastore."""

    supports_forking = False

    def __init__(self, workspace=None, **kwargs):
        pass


class _UndeclaredCatalog:
    """A metastore that has not said. Neither answer may be guessed for it."""

    def __init__(self, workspace=None, **kwargs):
        pass


@pytest.fixture
def workspaces():
    register_workspace("native", OpteryxConnector, catalog=_NativeCatalog)
    register_workspace("external", OpteryxConnector, catalog=_ExternalCatalog)
    register_workspace("undeclared", OpteryxConnector, catalog=_UndeclaredCatalog)


def test_a_native_metastore_can_be_forked(workspaces):
    connector = connector_factory("native.public.events", telemetry=None)

    assert connector.supports_forking("native.public.events") is True


def test_an_external_metastore_cannot_be_forked(workspaces):
    connector = connector_factory("external.public_data.nyc_taxicab_2021", telemetry=None)

    assert connector.supports_forking("external.public_data.nyc_taxicab_2021") is False


def test_the_same_connector_class_answers_differently_per_workspace(workspaces):
    """The reason this is a method and not a class attribute."""
    native = connector_factory("native.public.events", telemetry=None)
    external = connector_factory("external.public.events", telemetry=None)

    assert type(native) is type(external) is OpteryxConnector
    assert native.supports_forking("native.public.events")
    assert not external.supports_forking("external.public.events")


def test_a_metastore_that_has_not_declared_is_an_error_not_a_guess(workspaces):
    """Version skew must not be resolved by guessing. Refusing a valid clone is
    recoverable; a fork resting on files nothing has promised to keep is not,
    and silently allowing it is what a permissive default would do."""
    connector = connector_factory("undeclared.public.events", telemetry=None)

    with pytest.raises(UnsupportedSyntaxError, match="does not declare `supports_forking`"):
        connector.supports_forking("undeclared.public.events")


def test_a_connector_with_no_metastore_answers_no():
    """A Postgres or filesystem relation has no manifest of ours at all, so the
    base connector answers rather than raising - the clone is refused with the
    same message every other non-forkable source gets."""

    class _PlainConnector(BaseConnector):
        pass

    assert _PlainConnector().supports_forking("somewhere.a.b") is False


# --- the declaration this depends on ------------------------------------


def _sibling_catalog_module(module_name: str):
    """A module from the sibling ../opteryx-catalog checkout, by path.

    Loaded rather than imported for the reason `test_egress_protection`'s
    equivalent gives: whatever `opteryx_catalog` is installed may be a stale
    wheel, and what matters is the catalog this engine will ship beside
    (release order is catalog, then core).
    """
    repo = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "opteryx-catalog")
    )
    package_dir = os.path.join(repo, "opteryx_catalog")
    if not os.path.isdir(package_dir):
        raise AssertionError(
            f"sibling opteryx-catalog checkout not found at {repo}; this test reads the "
            "catalog source, not an installed wheel"
        )
    spec = importlib.util.spec_from_file_location(
        "_sibling_opteryx_catalog",
        os.path.join(package_dir, "__init__.py"),
        submodule_search_locations=[package_dir],
    )
    package = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = package
    spec.loader.exec_module(package)
    return importlib.import_module(f"{spec.name}.{module_name}")


def test_the_sibling_catalog_declares_forking_only_for_the_native_store():
    """The flag the connector reads is the catalog's to declare. An external
    metastore inherits the ABC's refusal by doing nothing, which is the point
    of the default - opting in has to be a positive act."""
    metastore = _sibling_catalog_module("catalog.metastore")
    catalog = _sibling_catalog_module("opteryx_catalog")

    assert metastore.Metastore.supports_forking is False
    assert catalog.OpteryxCatalog.supports_forking is True
