from unittest.mock import Mock
from unittest.mock import patch

import pytest

from opteryx.connectors.io_systems.gcs_filesystem import _GCP_AUTH_SCOPES
from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem
from opteryx.connectors.io_systems.gcs_filesystem import get_storage_credentials


class FakeBlob:
    def __init__(self, return_value="https://signed.example/url"):
        self.return_value = return_value
        self.calls = []

    def generate_signed_url(self, **kwargs):
        self.calls.append(kwargs)
        return self.return_value


class FakeBucket:
    def __init__(self, blob):
        self._blob = blob
        self.requested_blob_name = None

    def blob(self, blob_name):
        self.requested_blob_name = blob_name
        return self._blob


class FakeStorageClient:
    def __init__(self, blob):
        self._blob = blob
        self.credentials = None
        self.requested_bucket_name = None

    def bucket(self, bucket_name):
        self.requested_bucket_name = bucket_name
        return FakeBucket(self._blob)


class FakeWorkloadIdentityCredentials:
    def __init__(self, token="token", service_account_email="default", principal=None):
        self.token = token
        self.valid = True
        self.service_account_email = service_account_email
        self._principal = principal

    def get_cred_info(self):
        if self._principal is None:
            return {}
        return {"principal": self._principal}


def _make_filesystem(credentials):
    filesystem = OpteryxGcsFileSystem.__new__(OpteryxGcsFileSystem)
    filesystem.client_credentials = credentials
    filesystem._token_lock = None
    filesystem._Request = None
    return filesystem


def test_get_storage_credentials_requests_cloud_platform_scope(monkeypatch):
    fake_credentials = object()

    monkeypatch.delenv("STORAGE_EMULATOR_HOST", raising=False)
    with patch("google.auth.default", return_value=(fake_credentials, "project-id")) as auth_default:
        credentials = get_storage_credentials()

    assert credentials is fake_credentials
    auth_default.assert_called_once_with(scopes=_GCP_AUTH_SCOPES)


def test_rewrite_to_signed_url_uses_resolved_workload_identity_principal():
    credentials = FakeWorkloadIdentityCredentials(
        token="signed-token",
        service_account_email="default",
        principal="cloud-run@mabeldev.iam.gserviceaccount.com",
    )
    filesystem = _make_filesystem(credentials)
    fake_blob = FakeBlob()
    fake_client = FakeStorageClient(fake_blob)

    with patch("google.cloud.storage.Client", return_value=fake_client) as storage_client:
        url = filesystem.rewrite_to_signed_url("gs://bucket/path/to/file.parquet", expiry_seconds=900)

    assert url == "https://signed.example/url"
    storage_client.assert_called_once_with(credentials=credentials)
    assert fake_client.requested_bucket_name == "bucket"
    assert fake_blob.calls[0]["service_account_email"] == "cloud-run@mabeldev.iam.gserviceaccount.com"
    assert fake_blob.calls[0]["access_token"] == "signed-token"
    assert fake_blob.calls[0]["method"] == "GET"
    assert fake_blob.calls[0]["version"] == "v4"


def test_rewrite_to_signed_url_fails_when_signer_identity_is_unresolved():
    credentials = FakeWorkloadIdentityCredentials(service_account_email="default", principal=None)
    filesystem = _make_filesystem(credentials)
    fake_blob = FakeBlob()
    fake_client = FakeStorageClient(fake_blob)

    with patch("google.cloud.storage.Client", return_value=fake_client):
        with pytest.raises(RuntimeError, match="Unable to determine the service account email"):
            filesystem.rewrite_to_signed_url("gs://bucket/path/to/file.parquet")


def test_rewrite_to_signed_url_keeps_service_account_key_path(monkeypatch):
    import google.oauth2.service_account as service_account_module

    class FakeServiceAccountCredentials:
        def __init__(self):
            self.token = "unused-token"
            self.valid = True

    credentials = FakeServiceAccountCredentials()
    filesystem = _make_filesystem(credentials)
    fake_blob = FakeBlob()
    fake_client = FakeStorageClient(fake_blob)

    monkeypatch.setattr(service_account_module, "Credentials", FakeServiceAccountCredentials)
    with patch("google.cloud.storage.Client", return_value=fake_client):
        filesystem.rewrite_to_signed_url("gs://bucket/path/to/file.parquet")

    assert "service_account_email" not in fake_blob.calls[0]
    assert "access_token" not in fake_blob.calls[0]
