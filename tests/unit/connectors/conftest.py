import pytest


@pytest.fixture(scope="module")
def allow_local_reads():
    """Tests that serve files from a loopback HTTP server opt in with
    `pytestmark = pytest.mark.usefixtures("allow_local_reads")`: the private-address
    guard refuses 127.0.0.1 unless ALLOW_LOCAL_READS is set. Restored after the module."""
    # Imported here, not at module top: test modules put the repo on sys.path first.
    from opteryx import config

    previous = config.ALLOW_LOCAL_READS
    config.ALLOW_LOCAL_READS = True
    yield
    config.ALLOW_LOCAL_READS = previous
