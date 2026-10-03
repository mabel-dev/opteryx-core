"""READ_PARQUET / READ_JSONL / READ_CSV with `credentials => '<workspace>.<name>'`.

jobs.opteryx docs/design/secrets.md §8.2 (SEC-14). The engine resolves a NAMED
stored secret through the deployment's registered resolver and reads with that
credential and nothing else:

- no resolver registered -> refused, never anonymous, never ambient;
- the value is a qualified string literal naming a secret, never the secret;
- the secret's type must match the path's scheme;
- the literal path and every glob-expanded file must be inside the secret's SCOPE;
- globs work on this path (listing is the customer credential's to allow);
- nothing credential-bearing reaches EXPLAIN, plan text or an error message.

No network: the resolver is a fake, the object store behind the binder's
filesystem factory is a dict, and the real S3 filesystem is used only for
presigning, which is local computation.
"""

import os
import pickle
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.managers import secrets as secrets_module
from opteryx.managers.secrets import ObjectStoreCredential
from opteryx.managers.secrets import register_secret_resolver

CANARY = "CANARY-77c1e0"
SCOPE = "gs://acme_bkt/exports/"

CSV_A = b"id,name\n1,alpha\n2,beta\n"
CSV_B = b"id,name\n3,gamma\n"


def _admits(path):
    """A stand-in for the catalog's scope_admits, bucket boundary included."""
    return path.startswith(SCOPE) and "/../" not in path


def _credential(reference="analytics.reader", kind="gcs_service_account"):
    secret = (
        {"key": {"type": "service_account", "private_key": CANARY}}
        if kind == "gcs_service_account"
        else {"access_key_id": "AKIAABCDEFGHIJKLMNOP", "secret_access_key": CANARY}
    )
    return ObjectStoreCredential(kind=kind, reference=reference, secret=secret, admits=_admits)


class _FakeStore:
    """The object store a credentialed filesystem would reach, as a dict."""

    def __init__(self, credential, objects):
        self._credential = credential
        self.objects = objects
        self.listed = []

    class _File:
        def __init__(self, data):
            self.memoryview = memoryview(data)

        def close(self):
            pass

    def _guard(self, path):
        assert self._credential.admits(path), f"store reached outside SCOPE: {path}"

    def list_files(self, base_dir, recursive=True):
        self.listed.append(base_dir)
        return sorted(p for p in self.objects if p.startswith(base_dir))

    def open_input_file(self, path, columns=None, filters=None):
        self._guard(path)
        return self._File(self.objects[path])

    open_input_stream = open_input_file

    def native_auth_header(self):
        return f"Bearer {CANARY}"

    def __repr__(self):
        return "<FakeStore>"


@pytest.fixture
def resolver(monkeypatch):
    calls = []
    stores = []
    objects = {
        SCOPE + "2026/a.csv": CSV_A,
        SCOPE + "2026/b.csv": CSV_B,
        "gs://acme_bkt/private/c.csv": CSV_B,
    }

    def _resolve(execution_context, reference, path):
        calls.append((reference, path))
        return _credential(reference)

    def _filesystem(credential, protocol):
        store = _FakeStore(credential, objects)
        stores.append(store)
        return store

    monkeypatch.setattr(
        "opteryx.planner.binder.dataset._credentialed_filesystem", _filesystem
    )
    # Ambient credentials must never be reached on this path.
    import opteryx.connectors.io_systems.gcs_filesystem as gcs_filesystem

    def _ambient(*a, **k):
        raise AssertionError("ambient GCS credentials were requested")

    monkeypatch.setattr(gcs_filesystem, "get_storage_credentials", _ambient)
    register_secret_resolver(_resolve)
    yield {"calls": calls, "stores": stores, "objects": objects}
    register_secret_resolver(None)


def _rows(sql):
    session = opteryx.session(user="alice")
    rows = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        data = morsel.to_arrow().to_pydict()
        for i in range(len(next(iter(data.values()))) if data else 0):
            rows.append({k: (v[i].decode() if isinstance(v[i], bytes) else v[i]) for k, v in data.items()})
    return rows


# -- refusals before any resolution ------------------------------------------


def test_without_a_resolver_credentials_is_refused():
    register_secret_resolver(None)
    with pytest.raises(PermissionError, match="no secret resolver"):
        _rows(f"SELECT * FROM READ_CSV('{SCOPE}2026/a.csv', credentials => 'analytics.reader')")


@pytest.mark.parametrize("reference", ["reader", "a.b.c", "analytics.", "x y.z"])
def test_the_reference_must_be_workspace_dot_name(resolver, reference):
    with pytest.raises(InvalidFunctionParameterError, match="<workspace>.<name>"):
        _rows(f"SELECT * FROM READ_CSV('{SCOPE}2026/a.csv', credentials => '{reference}')")
    assert resolver["calls"] == []


def test_the_reference_must_be_a_literal(resolver):
    with pytest.raises(InvalidFunctionParameterError, match="string literal"):
        _rows(f"SELECT * FROM READ_CSV('{SCOPE}2026/a.csv', credentials => 1)")


def test_credentials_only_apply_to_object_stores(resolver):
    with pytest.raises(InvalidFunctionParameterError, match="gs:// and s3://"):
        _rows("SELECT * FROM READ_CSV('testdata/x.csv', credentials => 'analytics.reader')")
    with pytest.raises(InvalidFunctionParameterError, match="gs:// and s3://"):
        _rows("SELECT * FROM READ_PARQUET('https://example.com/x.parquet', credentials => 'analytics.reader')")


def test_every_reader_accepts_the_option_and_nothing_new():
    for function in ("READ_PARQUET", "READ_JSONL", "READ_CSV"):
        with pytest.raises(InvalidFunctionParameterError):
            _rows(f"SELECT * FROM {function}('{SCOPE}a', credential => 'analytics.reader')")


# -- resolution, scope and globs ---------------------------------------------


def test_a_credentialed_read_uses_the_secret(resolver):
    rows = _rows(f"SELECT * FROM READ_CSV('{SCOPE}2026/a.csv', credentials => 'analytics.reader')")
    assert [r["name"] for r in rows] == ["alpha", "beta"]
    assert resolver["calls"] == [("analytics.reader", f"{SCOPE}2026/a.csv")]


def test_globs_work_and_every_file_is_scope_checked(resolver):
    rows = _rows(
        f"SELECT name FROM READ_CSV('{SCOPE}2026/*.csv', credentials => 'analytics.reader') ORDER BY name"
    )
    assert [r["name"] for r in rows] == ["alpha", "beta", "gamma"]
    assert resolver["stores"][0].listed == [f"{SCOPE}2026"]


def test_a_literal_path_outside_scope_is_refused(resolver):
    with pytest.raises(PermissionError, match="outside the SCOPE of secret analytics.reader"):
        _rows("SELECT * FROM READ_CSV('gs://acme_bkt/private/c.csv', credentials => 'analytics.reader')")


def test_a_glob_expanding_outside_scope_is_refused(resolver):
    # The literal pattern is inside the scope; one file it expands to is not.
    resolver["objects"][SCOPE + "2026/../../private/x.csv"] = CSV_B
    with pytest.raises(PermissionError, match="outside the SCOPE"):
        _rows(f"SELECT * FROM READ_CSV('{SCOPE}2026/*', credentials => 'analytics.reader')")


def test_the_secret_type_must_match_the_scheme(resolver, monkeypatch):
    def _resolve(execution_context, reference, path):
        return _credential(reference, kind="aws_access_key")

    register_secret_resolver(_resolve)
    with pytest.raises(PermissionError, match="cannot read gs://"):
        _rows(f"SELECT * FROM READ_CSV('{SCOPE}2026/a.csv', credentials => 'analytics.reader')")


def test_a_resolver_refusal_reaches_the_caller(resolver):
    def _resolve(execution_context, reference, path):
        raise PermissionError(f"you may not use secret {reference}")

    register_secret_resolver(_resolve)
    with pytest.raises(PermissionError, match="may not use secret analytics.reader"):
        _rows(f"SELECT * FROM READ_PARQUET('{SCOPE}a.parquet', credentials => 'analytics.reader')")


def test_no_secret_is_inferred_from_the_path(resolver):
    """Without the option the read is anonymous even though a secret would cover it."""
    with pytest.raises(Exception) as raised:
        _rows(f"SELECT * FROM READ_CSV('{SCOPE}2026/*.csv')")
    assert "glob patterns are not supported" in str(raised.value)
    assert resolver["calls"] == []


# -- nothing credential-bearing leaks -----------------------------------------


def test_explain_carries_the_reference_not_the_secret(resolver):
    rows = _rows(
        f"EXPLAIN SELECT * FROM READ_CSV('{SCOPE}2026/a.csv', credentials => 'analytics.reader')"
    )
    text = repr(rows)
    assert CANARY not in text


def test_credential_objects_refuse_to_print_or_pickle():
    credential = _credential()
    assert CANARY not in repr(credential) and CANARY not in str(credential)
    with pytest.raises(TypeError):
        pickle.dumps(credential)
    import copy

    assert copy.deepcopy(credential) is credential


def test_a_credentialed_s3_filesystem_never_uses_the_chain(monkeypatch):
    from opteryx.connectors.io_systems import s3_filesystem
    from opteryx.connectors.io_systems.credentialed_filesystem import ScopeRefused
    from opteryx.connectors.io_systems.credentialed_filesystem import credentialed_filesystem

    def _chain(*a, **k):
        raise AssertionError("the AWS credential chain was consulted")

    monkeypatch.setattr(s3_filesystem._CHAIN, "frozen", _chain)
    credential = ObjectStoreCredential(
        kind="aws_access_key",
        reference="analytics.lake",
        secret={
            "access_key_id": "AKIAABCDEFGHIJKLMNOP",
            "secret_access_key": "abcdEFGHijklMNOPqrstUVWXyz0123456789+/AB",
            "region": "eu-west-2",
        },
        admits=lambda p: p.startswith("s3://acme-lake/exports/"),
    )
    fs = credentialed_filesystem(credential, "s3")
    url = fs.rewrite_to_signed_url("s3://acme-lake/exports/a.parquet")
    assert "X-Amz-Signature=" in url and "AKIAABCDEFGHIJKLMNOP" in url
    with pytest.raises(ScopeRefused):
        fs.rewrite_to_signed_url("s3://acme-lake/private/a.parquet")
    with pytest.raises(ScopeRefused):
        fs.list_files("s3://acme-lake/")
    with pytest.raises(PermissionError, match="cannot read gs://"):
        credentialed_filesystem(credential, "gs")
    assert "abcdEFGH" not in repr(fs)


def test_a_service_account_key_with_a_foreign_token_endpoint_is_refused():
    from opteryx.connectors.io_systems.credentialed_filesystem import credentialed_filesystem

    credential = ObjectStoreCredential(
        kind="gcs_service_account",
        reference="analytics.reader",
        secret={"key": {"type": "service_account", "token_uri": "https://attacker.example/token"}},
        admits=_admits,
    )
    with pytest.raises(PermissionError, match="token endpoint"):
        credentialed_filesystem(credential, "gs")


def test_http_errors_never_quote_a_query_string():
    """A presigned URL's credential lives in its query string; the native client's
    error text keeps the path and drops the rest."""
    from opteryx.compiled.http_client import HttpClient

    client = HttpClient(max_connections=1, timeout_ms=2000)
    with pytest.raises(RuntimeError) as raised:
        client.get(f"http://127.0.0.1:9/bucket/key.parquet?X-Amz-Signature={CANARY}")
    message = str(raised.value)
    assert CANARY not in message
    assert "/bucket/key.parquet?<redacted>" in message


def test_s3_file_errors_never_quote_a_query_string():
    from opteryx.connectors.io_systems.s3_filesystem import S3File
    from opteryx.exceptions import DatasetReadError

    class _Client:
        def get(self, url, headers=None):
            raise RuntimeError("HTTP 403")

    with pytest.raises(DatasetReadError) as raised:
        S3File(f"https://acme.s3.amazonaws.com/k?X-Amz-Security-Token={CANARY}", _Client())
    assert CANARY not in str(raised.value)


# -- SEC-16: the canary, for credentials => reads --------------------------------


@pytest.fixture
def s3_canary(monkeypatch):
    """A REAL credentialed S3 filesystem whose session token is the canary,
    pointed at a closed local port: every fetch fails, with a real presigned URL
    (signature, key id, X-Amz-Security-Token) in hand when it does."""
    from opteryx.connectors.io_systems import s3_filesystem

    monkeypatch.setenv("AWS_S3_ENDPOINT", "http://127.0.0.1:9")
    monkeypatch.setattr(
        s3_filesystem._CHAIN, "frozen", lambda: (_ for _ in ()).throw(AssertionError("chain"))
    )

    def _resolve(execution_context, reference, path):
        return ObjectStoreCredential(
            kind="aws_access_key",
            reference=reference,
            secret={
                "access_key_id": "AKIAABCDEFGHIJKLMNOP",
                "secret_access_key": "abcdEFGHijklMNOPqrstUVWXyz0123456789+/AB",
                "session_token": CANARY,
                "region": "eu-west-2",
            },
            admits=lambda p: p.startswith("s3://acme-lake/exports/"),
        )

    register_secret_resolver(_resolve)
    yield
    register_secret_resolver(None)


@pytest.mark.parametrize("function", ["READ_PARQUET", "READ_CSV", "READ_JSONL"])
def test_the_canary_key_appears_in_no_sink(s3_canary, caplog, capfd, function):
    import logging

    caplog.set_level(logging.DEBUG)
    sql = (
        f"SELECT * FROM {function}('s3://acme-lake/exports/2026/a.dat', "
        "credentials => 'analytics.lake')"
    )
    sinks = {}
    for label, statement in (("query", sql), ("explain", f"EXPLAIN {sql}")):
        try:
            sinks[label] = repr(_rows(statement))
        except Exception as err:  # the store is unreachable: this is the error path
            sinks[f"{label} error"] = f"{type(err).__name__}: {err}"
            sinks[f"{label} error cause"] = repr(err.__cause__) + repr(err.__context__)
    out, err = capfd.readouterr()
    sinks.update({"logs": caplog.text, "stdout": out, "stderr": err})
    assert any("error" in name for name in sinks), "expected the read to fail against a closed port"
    leaked = [name for name, text in sinks.items() if CANARY in text]
    assert leaked == [], f"canary leaked into: {leaked}"
