# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`information_schema.grants` -- the workspace's access, as a relation.

`SHOW GRANTS ON` and `SHOW EFFECTIVE GRANTS ON` answer one object at a time
and run as statements; an interface that wants to show access live has to
queue one per object. This table is both answers for every object in the
workspace, with `origin` saying which: `explicit` for a policy stored AT the
object, `inherited` for one covering it from above.

The engine's part is the seam these tests pin: which objects it asks about
(and in what shape), how it maps the capability's rows back to columns, the
pushdown that lets a dataset page read one object with no catalog walk, and
that the capability's extras -- policies on things the catalog no longer
holds -- are listed rather than lost. Coverage, the gate and the explicit
test belong to the capability and are tested where they live
(opteryx-access).
"""

import fnmatch

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


def _morsels_to_rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        count = len(next(iter(pydict.values()))) if pydict else 0
        for index in range(count):
            row = {}
            for key, values in pydict.items():
                value = values[index]
                if isinstance(value, bytes):
                    value = value.decode()
                row[key] = value
            rows.append(row)
    return rows


class _FakeCatalog:
    """Two collections; `ops` holds a table and a view, `sales` a table."""

    calls = []

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        _FakeCatalog.calls.append(("list_collections",))
        return ["ops", "sales"]

    def list_datasets(self, collection):
        _FakeCatalog.calls.append(("list_datasets", collection))
        return {"ops": ["audit_log"], "sales": ["orders"]}[collection]

    def list_views(self, collection):
        _FakeCatalog.calls.append(("list_views", collection))
        return {"ops": ["audit_summary"], "sales": []}[collection]

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)


# (principal, role, pattern) -- the stored policies of workspace `cat`. The
# last one names a dataset the catalog does not hold: a grant on something
# dropped, which the listing must still show.
_POLICIES = [
    ("alice", "owner", "cat.*"),
    ("bob", "writer", "cat.ops.*"),
    ("ginny", "reader", "cat.ops.audit_log"),
    ("hal", "reader", "cat.sales.retired"),
]


class _ScriptedCapability:
    """Answers `effective_grants_in` the way opteryx-access does -- one row per
    covering policy, every stored pattern also reported at itself -- with a
    plain fnmatch standing in for the matcher. Records what it was asked."""

    name = "scripted"

    def __init__(self, policies=_POLICIES, administers=None):
        self.policies = list(policies)
        self.administers = administers  # None: everything
        self.asked = []

    def can_perform_action(self, execution_context, resource, action):
        return True

    def can_perform_workspace_action(self, execution_context, workspace, action):
        return True

    def can_principal_perform_action(self, principal, resource, action):
        return True

    def can_principal_own_materialized_view(self, principal):
        return True

    def grants(self, identity, policies):
        return []

    def apply_grant(self, execution_context, pattern, role, principal):
        raise AssertionError("not reached")

    def apply_revoke(self, execution_context, pattern, role, principal):
        raise AssertionError("not reached")

    def grants_on(self, execution_context, pattern):
        raise AssertionError("not reached")

    def effective_grants_on(self, execution_context, pattern):
        raise AssertionError("not reached")

    def effective_grants_in(self, execution_context, workspace, objects):
        self.asked.append((execution_context.user, workspace, list(objects)))
        asked = list(objects)
        for _, _, pattern in self.policies:
            if pattern not in asked:
                asked.append(pattern)
        rows = []
        for object_pattern in asked:
            if self.administers is not None and object_pattern not in self.administers:
                continue
            for principal, role, pattern in self.policies:
                if not fnmatch.fnmatchcase(object_pattern, pattern):
                    continue
                rows.append(
                    {
                        "object": object_pattern,
                        "user": principal,
                        "pattern": pattern,
                        "level": {2: "workspace", 3: "collection"}.get(
                            len(pattern.split(".")), "dataset"
                        )
                        if pattern.endswith(".*")
                        else "dataset",
                        "role": role,
                        "explicit": pattern == object_pattern,
                    }
                )
        return rows


@pytest.fixture
def permissions_state():
    from opteryx import managers

    module = managers.permissions
    saved_active, saved_consulted = module._active, module._consulted
    yield module
    module._active, module._consulted = saved_active, saved_consulted


@pytest.fixture
def capability(permissions_state):
    from opteryx.managers.permissions import register_permissions_capability

    installed = _ScriptedCapability()
    permissions_state._active, permissions_state._consulted = permissions_state._CORE, False
    register_permissions_capability(installed)
    return installed


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _read(where="", user="alice"):
    session = opteryx.session(user=user, access_policies=_OWNER_POLICY)
    return _morsels_to_rows(
        session.execute_to_morsels(f"SELECT * FROM cat.information_schema.grants{where}")
    )


def _shape(rows):
    return [
        (r["object_kind"], r["object_name"], r["grantee"], r["pattern"], r["origin"]) for r in rows
    ]


def test_every_object_the_statements_can_name_is_asked_about(catalog_workspace, capability):
    """The workspace, each collection, each table AND view -- as the patterns
    the grant statements issue for them, in that order."""
    _read()

    [(user, workspace, objects)] = capability.asked
    assert user == "alice"
    assert workspace == "cat"
    assert objects == [
        "cat.*",
        "cat.ops.*",
        "cat.ops.audit_log",
        "cat.ops.audit_summary",
        "cat.sales.*",
        "cat.sales.orders",
    ]


def test_row_shape(catalog_workspace, capability):
    rows = [r for r in _read() if r["object_name"] == "cat.ops.audit_log"]

    assert rows == [
        {
            "grant_catalog": "cat",
            "grant_collection": "ops",
            "object_kind": "dataset",
            "object_name": "cat.ops.audit_log",
            "grantee": "alice",
            "role": "owner",
            "pattern": "cat.*",
            "level": "workspace",
            "origin": "inherited",
        },
        {
            "grant_catalog": "cat",
            "grant_collection": "ops",
            "object_kind": "dataset",
            "object_name": "cat.ops.audit_log",
            "grantee": "bob",
            "role": "writer",
            "pattern": "cat.ops.*",
            "level": "collection",
            "origin": "inherited",
        },
        {
            "grant_catalog": "cat",
            "grant_collection": "ops",
            "object_kind": "dataset",
            "object_name": "cat.ops.audit_log",
            "grantee": "ginny",
            "role": "reader",
            "pattern": "cat.ops.audit_log",
            "level": "dataset",
            "origin": "explicit",
        },
    ]


def test_origin_is_explicit_at_the_pattern_and_inherited_below_it(catalog_workspace, capability):
    rows = _read()

    assert ("collection", "cat.ops", "bob", "cat.ops.*", "explicit") in _shape(rows)
    assert ("dataset", "cat.ops.audit_summary", "bob", "cat.ops.*", "inherited") in _shape(rows)
    assert ("workspace", "cat", "alice", "cat.*", "explicit") in _shape(rows)
    assert ("dataset", "cat.sales.orders", "alice", "cat.*", "inherited") in _shape(rows)


def test_the_workspace_row_has_no_collection(catalog_workspace, capability):
    [row] = [r for r in _read() if r["object_kind"] == "workspace"]

    assert row["object_name"] == "cat"
    assert row["grant_collection"] is None


def test_a_grant_on_something_the_catalog_no_longer_holds_is_still_listed(
    catalog_workspace, capability
):
    """hal's policy names `cat.sales.retired`, which no listing returns. The
    engine never asked about it; the capability reports it at itself, and the
    engine reads that pattern the way the statements read an object name."""
    rows = _read()

    assert ("dataset", "cat.sales.retired", "hal", "cat.sales.retired", "explicit") in _shape(rows)
    [(_, _, objects)] = capability.asked
    assert "cat.sales.retired" not in objects


def test_explicit_rows_are_the_stored_policies_once_each(catalog_workspace, capability):
    """`WHERE origin = 'explicit'` is `SHOW GRANTS ON WORKSPACE`: every stored
    policy, exactly once, at its own object."""
    rows = _read(" WHERE origin = 'explicit'")

    assert sorted((r["grantee"], r["pattern"]) for r in rows) == sorted(
        (principal, pattern) for principal, _, pattern in _POLICIES
    )


def test_a_pinned_object_name_skips_the_catalog_entirely(catalog_workspace, capability):
    """The dataset page's read: one object, no catalog walk, and the
    statements' exact-name semantics -- the answer is about the name."""
    rows = _read(" WHERE object_name = 'cat.ops.audit_log'")

    assert _FakeCatalog.calls == []
    [(_, _, objects)] = capability.asked
    assert objects == ["cat.ops.audit_log"]
    assert {r["object_name"] for r in rows} == {"cat.ops.audit_log"}
    assert [r["grantee"] for r in rows] == ["alice", "bob", "ginny"]


def test_a_pinned_workspace_name_asks_about_the_workspace(catalog_workspace, capability):
    rows = _read(" WHERE object_name = 'cat'")

    [(_, _, objects)] = capability.asked
    assert objects == ["cat.*"]
    assert _shape(rows) == [("workspace", "cat", "alice", "cat.*", "explicit")]


def test_a_collection_predicate_skips_the_other_listings(catalog_workspace, capability):
    rows = _read(" WHERE grant_collection = 'sales'")

    listed = [call[1] for call in _FakeCatalog.calls if call[0] == "list_datasets"]
    assert listed == ["sales"]
    assert {r["object_name"] for r in rows} == {
        "cat.sales",
        "cat.sales.orders",
        "cat.sales.retired",
    }
    # The workspace row has no collection, so a collection predicate excludes it.
    assert not any(r["object_kind"] == "workspace" for r in rows)


def test_an_object_kind_predicate_skips_the_dataset_listings(catalog_workspace, capability):
    rows = _read(" WHERE object_kind = 'collection'")

    assert not any(call[0] in ("list_datasets", "list_views") for call in _FakeCatalog.calls)
    assert {r["object_name"] for r in rows} == {"cat.ops", "cat.sales"}


def test_a_catalog_predicate_skips_everything(catalog_workspace, capability):
    rows = _read(" WHERE grant_catalog = 'other'")

    assert rows == []
    assert _FakeCatalog.calls == []
    assert capability.asked == []


def test_an_object_the_caller_may_not_administer_has_no_rows(catalog_workspace, permissions_state):
    """The capability decides, per object, and skips rather than refuses; the
    engine shows what came back and nothing else. A collection owner sees
    their collection and what is under it."""
    from opteryx.managers.permissions import register_permissions_capability

    installed = _ScriptedCapability(
        administers={"cat.ops.*", "cat.ops.audit_log", "cat.ops.audit_summary"}
    )
    permissions_state._active, permissions_state._consulted = permissions_state._CORE, False
    register_permissions_capability(installed)

    rows = _read(user="bob")

    assert {r["object_name"] for r in rows} == {
        "cat.ops",
        "cat.ops.audit_log",
        "cat.ops.audit_summary",
    }


def test_a_pattern_naming_no_single_object_is_not_a_row(catalog_workspace, permissions_state):
    """A stored `cat.*.audit_log` (issuable only by hand, never by the SQL
    surface) covers datasets and shows as inherited on them, but has no
    object of its own to be explicit at."""
    from opteryx.managers.permissions import register_permissions_capability

    installed = _ScriptedCapability(
        policies=[("alice", "owner", "cat.*"), ("ivy", "reader", "cat.*.audit_log")]
    )
    permissions_state._active, permissions_state._consulted = permissions_state._CORE, False
    register_permissions_capability(installed)

    rows = _read()

    assert ("dataset", "cat.ops.audit_log", "ivy", "cat.*.audit_log", "inherited") in _shape(rows)
    assert not any(r["object_name"] == "cat.*.audit_log" for r in rows)


def test_the_intrinsic_capability_refuses_rather_than_reporting_nothing(catalog_workspace):
    """Under PermitAll there is no policy store: an empty table would say
    nobody holds anything, which is the one thing it must not say."""
    from opteryx.exceptions import InvalidConfigurationError

    with pytest.raises(InvalidConfigurationError):
        _read()
