"""READ_* must not reach this server's own files or network unless the deployment
opts in with ALLOW_LOCAL_READS (opteryx/config.py). Off: a local path is refused at
bind time, and an http(s) peer that is not a public address is refused natively at
connect time (so redirects and DNS tricks cannot reach it either).
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx import config
from opteryx.compiled.http_client import HttpClient


def test_local_read_refused_when_not_allowed(monkeypatch):
    monkeypatch.setattr(config, "ALLOW_LOCAL_READS", False)
    with pytest.raises(PermissionError, match="ALLOW_LOCAL_READS"):
        list(opteryx.session().execute_to_morsels(
            "SELECT * FROM READ_PARQUET('testdata/missions/space_missions.parquet')"
        ))


def test_local_read_allowed_when_opted_in(monkeypatch):
    monkeypatch.setattr(config, "ALLOW_LOCAL_READS", True)
    morsels = list(opteryx.session().execute_to_morsels(
        "SELECT COUNT(*) FROM READ_PARQUET('testdata/missions/space_missions.parquet')"
    ))
    assert morsels


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:9/x",        # loopback
        "http://169.254.169.254/x",    # cloud metadata (link-local)
        "http://10.0.0.1:9/x",         # RFC 1918
        "http://[::1]:9/x",            # IPv6 loopback
    ],
)
def test_http_client_refuses_non_public_peer(url):
    client = HttpClient(max_connections=1, timeout_ms=2000, deny_private_addresses=True)
    with pytest.raises(Exception, match="non-public address"):
        client.head(url)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
