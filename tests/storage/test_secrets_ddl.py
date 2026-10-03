# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""CREATE / DROP / SHOW SECRET in the engine (jobs.opteryx docs/design/secrets.md
§2.4, SEC-5 and SEC-8).

The engine RECOGNISES all three, PLANS two, and never executes CREATE - that is
jobs.opteryx's, at submission. What these pin is the boundary: a literal value
never appears in anything the parse hands to Python, the lift jobs calls puts
every literal in exactly one place, and DROP and SHOW are gated on the
workspace-level right to manage secrets.
"""

import os
import sys

_CATALOG_REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "opteryx-catalog")
)
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.third_party import sqloxide

CANARY = "CANARY-9b0e4f"
_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


# -- the parse and the lift ------------------------------------------------


def test_a_literal_never_appears_in_the_parse():
    sql = f"CREATE SECRET alerts IN analytics (TYPE 'http_endpoint', URL 'https://x/{CANARY}')"
    parsed = sqloxide.parse_sql(sql, _dialect="opteryx")
    assert CANARY not in repr(parsed)
    create = parsed[0]["CreateSecret"]
    assert create["secret_type"] == "http_endpoint"
    assert create["options"] == [{"key": "URL", "value": None}]
    assert create["workspace"]["value"] == "analytics"


def test_a_placeholder_is_reported_as_a_parameter():
    info = opteryx.analyze_query("CREATE SECRET a IN analytics (TYPE 'http_endpoint', URL :url)")
    assert info["query_type"] == "CreateSecret"
    assert info["parameters"] == ["url"]
    assert info["tables"] == []
    assert info["permission_required"] == "owner"


def test_the_lift_moves_every_literal_into_the_values():
    sql = (
        "CREATE OR REPLACE SECRET a IN analytics (TYPE 'aws_access_key', "
        f"ACCESS_KEY_ID :kid, SECRET_ACCESS_KEY '{CANARY}', SCOPE 's3://lake/x/')"
    )
    redacted, values = sqloxide.redact_secret_statement(sql)
    assert CANARY not in redacted
    assert redacted == (
        "CREATE OR REPLACE SECRET a IN analytics (TYPE 'aws_access_key', "
        "ACCESS_KEY_ID :kid, SECRET_ACCESS_KEY :redacted_secret_access_key, "
        "SCOPE :redacted_scope)"
    )
    assert values == {"redacted_secret_access_key": CANARY, "redacted_scope": "s3://lake/x/"}
    # The redacted statement classifies exactly as the original did.
    info = opteryx.analyze_query(redacted)
    assert info["query_type"] == "CreateSecret"
    assert info["parameters"] == ["kid", "redacted_scope", "redacted_secret_access_key"]


def test_the_lift_unescapes_doubled_quotes_and_spans_lines():
    sql = "CREATE SECRET a IN w (\n  TYPE 'http_endpoint',\n  HEADER_AUTHORIZATION 'it''s',\n  URL 'https://é/x')"
    redacted, values = sqloxide.redact_secret_statement(sql)
    assert values == {"redacted_header_authorization": "it's", "redacted_url": "https://é/x"}
    assert "it''s" not in redacted and "https" not in redacted


def test_the_lift_ignores_other_statements():
    assert sqloxide.redact_secret_statement("SELECT 'CREATE SECRET x'") is None
    assert sqloxide.redact_secret_statement("DROP SECRET a IN w") is None


@pytest.mark.parametrize(
    "sql",
    [
        f"CREATE SECRET a.b IN w (TYPE 'http_endpoint', URL '{CANARY}')",
        "CREATE SECRET a IN w (TYPE :t)",
        f"CREATE SECRET a IN w (TYPE 'x', URL ?, K '{CANARY}')",
        f"CREATE SECRET a IN w (TYPE 'x', URL '{CANARY}', url 'b')",
        f"CREATE OR REPLACE SECRET IF NOT EXISTS a IN w (TYPE 'x', URL '{CANARY}')",
        f"CREATE SECRET a IN w (TYPE 'x', URL '{CANARY}'",
        f"CREATE SECRET a (TYPE 'x', URL '{CANARY}')",
        f"CREATE SECRET a IN w (TYPE 'x', URL '{CANARY}') EXTRA",
    ],
)
def test_malformed_statements_are_refused_without_quoting_them(sql):
    for call in (
        lambda: sqloxide.redact_secret_statement(sql),
        lambda: sqloxide.parse_sql(sql, _dialect="opteryx"),
    ):
        with pytest.raises(ValueError) as raised:
            call()
        assert CANARY not in str(raised.value)


def test_a_parameter_may_not_squat_on_a_redacted_name():
    """Parses - it is a well-formed statement - but the lift refuses it, since
    `:redacted_url` must mean the value the lift put there and nothing else."""
    sql = f"CREATE SECRET a IN w (TYPE 'x', URL '{CANARY}', K :redacted_url)"
    sqloxide.parse_sql(sql, _dialect="opteryx")
    with pytest.raises(ValueError, match="reserved") as raised:
        sqloxide.redact_secret_statement(sql)
    assert CANARY not in str(raised.value)


def test_drop_and_show_accept_either_specifier():
    parsed = sqloxide.parse_sql(
        "DROP SECRET IF EXISTS a FROM w; DROP SECRET b IN w; SHOW SECRETS IN w; SHOW SECRETS FROM w",
        _dialect="opteryx",
    )
    assert [next(iter(p)) for p in parsed] == ["DropSecret", "DropSecret", "ShowSecrets", "ShowSecrets"]
    assert parsed[0]["DropSecret"]["if_exists"] is True


def test_secret_is_not_reserved():
    """Matched on word text, like every aside keyword: a column called `secret` still works."""
    parsed = sqloxide.parse_sql("SELECT secret FROM secrets", _dialect="opteryx")
    assert next(iter(parsed[0])) == "Query"


# -- execution ---------------------------------------------------------------


class _FakeCatalog:
    calls = []
    secrets = {}

    def __init__(self, workspace=None, **kwargs):
        self.workspace = workspace

    def drop_secret(self, name, author=None, if_exists=False):
        from opteryx_catalog.secrets import SecretNotFound

        _FakeCatalog.calls.append(("drop_secret", name, author, if_exists))
        if name not in _FakeCatalog.secrets:
            if if_exists:
                return False
            raise SecretNotFound(f"secret {name} does not exist")
        del _FakeCatalog.secrets[name]
        return True

    def list_secrets(self):
        return [dict(record, name=name) for name, record in sorted(_FakeCatalog.secrets.items())]


class _Capability:
    name = "scripted"

    def __init__(self, workspace_owner=True):
        self.workspace_owner = workspace_owner
        self.asked = []

    def can_perform_action(self, execution_context, resource, action):
        return True

    def can_perform_workspace_action(self, execution_context, workspace, action):
        self.asked.append((workspace, action))
        return self.workspace_owner

    def can_principal_perform_action(self, principal, resource, action):
        return True

    def can_principal_own_materialized_view(self, principal):
        return True

    def grants(self, identity, policies):
        return []

    def apply_grant(self, *a):
        raise AssertionError

    def apply_revoke(self, *a):
        raise AssertionError

    def grants_on(self, *a):
        raise AssertionError

    def effective_grants_on(self, *a):
        raise AssertionError

    def effective_grants_in(self, *a):
        raise AssertionError

    def set_workspace_maintenance(self, *a):
        raise AssertionError

    def workspace_maintenance(self, *a):
        return False


@pytest.fixture
def permissions_state():
    from opteryx import managers

    module = managers.permissions
    saved = module._active, module._consulted
    yield module
    module._active, module._consulted = saved


def _install(permissions_state, **kwargs):
    from opteryx.managers.permissions import register_permissions_capability

    capability = _Capability(**kwargs)
    permissions_state._active, permissions_state._consulted = permissions_state._CORE, False
    register_permissions_capability(capability)
    return capability


@pytest.fixture
def catalog_workspace():
    _FakeCatalog.calls = []
    _FakeCatalog.secrets = {
        "billing_reader": {
            "type": "gcs_service_account",
            "scope": "gs://mabel_logs/gcp_billing/",
            "created-by": "alice",
            "created-at-ms": 1_759_000_000_000,
            "updated-by": "alice",
            "updated-at-ms": 1_759_000_000_000,
            "last-used-at-ms": None,
            "use-count": 3,
        }
    }
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        data = morsel.to_arrow().to_pydict()
        for index in range(len(next(iter(data.values()))) if data else 0):
            rows.append({k: (v[index].decode() if isinstance(v[index], bytes) else v[index]) for k, v in data.items()})
    return rows


def test_the_engine_refuses_create_secret_without_quoting_it():
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    with pytest.raises(UnsupportedSyntaxError) as raised:
        list(
            session.execute_to_morsels(
                f"CREATE SECRET a IN cat (TYPE 'http_endpoint', URL 'https://x/{CANARY}')"
            )
        )
    assert "run by the platform" in str(raised.value)
    assert CANARY not in str(raised.value)


def test_a_task_cannot_be_a_create_secret(catalog_workspace):
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    with pytest.raises(Exception) as raised:
        list(
            session.execute_to_morsels(
                f"CREATE TASK cat.c.t AS CREATE SECRET a IN cat (TYPE 'http_endpoint', URL '{CANARY}')"
            )
        )
    assert CANARY not in str(raised.value)


def test_drop_secret_reaches_the_catalog_with_the_author(catalog_workspace, permissions_state):
    capability = _install(permissions_state)
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("DROP SECRET billing_reader IN cat"))
    assert catalog_workspace.calls == [("drop_secret", "billing_reader", "alice", False)]
    assert ("cat", "ALTER") in capability.asked


def test_drop_secret_that_does_not_exist(catalog_workspace, permissions_state):
    _install(permissions_state)
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    with pytest.raises(Exception, match="DROP SECRET IF EXISTS"):
        list(session.execute_to_morsels("DROP SECRET nope IN cat"))
    list(session.execute_to_morsels("DROP SECRET IF EXISTS nope IN cat"))


def test_drop_secret_needs_the_workspace_right(catalog_workspace, permissions_state):
    _install(permissions_state, workspace_owner=False)
    session = opteryx.session(user="bob", access_policies=_OWNER_POLICY)
    with pytest.raises(PermissionError, match="manage secrets"):
        list(session.execute_to_morsels("DROP SECRET billing_reader IN cat"))
    assert catalog_workspace.calls == []


def test_show_secrets_lists_everything_but_key_material(catalog_workspace, permissions_state):
    _install(permissions_state)
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    rows = _rows(session.execute_to_morsels("SHOW SECRETS IN cat"))
    assert len(rows) == 1
    row = rows[0]
    assert row["secret_name"] == "billing_reader"
    assert row["secret_type"] == "gcs_service_account"
    assert row["scope"] == "gs://mabel_logs/gcp_billing/"
    assert row["use_count"] == 3
    assert row["last_used_at"] is None
    assert not {"ciphertext", "nonce", "wrapped_dek", "wrapped-dek"} & set(row)


def test_show_secrets_needs_the_workspace_right(catalog_workspace, permissions_state):
    _install(permissions_state, workspace_owner=False)
    session = opteryx.session(user="bob", access_policies=_OWNER_POLICY)
    with pytest.raises(PermissionError, match="manage secrets"):
        _rows(session.execute_to_morsels("SHOW SECRETS IN cat"))
    with pytest.raises(PermissionError, match="manage secrets"):
        _rows(session.execute_to_morsels("SELECT * FROM cat.information_schema.secrets"))
