"""A pooled PostgreSQL connection that died while idle must not fail the next query.

The bug this pins: `PgPool::acquire` handed back an idle connection without
checking it, and nothing retried. A connection reaped by the server, a pooler or
a NAT while it sat in the pool therefore failed on the first write of the next
statement -- `postgres TLS write failed: ... Connection reset by peer` -- which
reached the caller as a 500 on a query that was perfectly valid. It presented as
an intermittent failure because it depended entirely on whether the connection
you happened to draw had been reaped since its last use.

Reproducing that needs a connection killed from OUTSIDE this process, so the
test puts a transparent TCP proxy between the client and the real server and cuts
the sockets while the connection is parked in the pool. The proxy forwards bytes
without interpreting them, so TLS is unaffected: the handshake is end to end
between the client and the real server.

`sslmode` is forced to `require` because the client now connects to 127.0.0.1,
and `verify-full` would fail the hostname check against the server's certificate
for reasons that have nothing to do with what is being tested.

Needs a reachable server; see test_postgres_connector.py for the configuration.
That module's `_connection_url` is deliberately not imported -- importing it
registers a workspace against the real host at import time, which is the one
thing this test must not do.
"""

import os
import socket
import sys
import threading
import urllib.parse

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.connectors import PostgresConnector

WORKSPACE = "pgstale"
RELATION = f"{WORKSPACE}.information_schema.schemata"


def _connection_url() -> str:
    for name in ("POSTGRES_TEST_CONNECTION", "DATA_CATALOG_CONNECTION"):
        value = os.environ.get(name)
        if value:
            return value
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    if os.path.exists(env_path):
        with open(env_path) as env_file:
            for line in env_file:
                if line.startswith("DATA_CATALOG_CONNECTION="):
                    value = line.split("=", 1)[1].strip()
                    if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
                        value = value[1:-1]
                    return value
    raise RuntimeError(
        "No PostgreSQL server configured: set POSTGRES_TEST_CONNECTION (or "
        "DATA_CATALOG_CONNECTION) to a postgresql:// URL"
    )


class KillableProxy:
    """A TCP forwarder to (host, port) whose established sockets can be cut."""

    def __init__(self, host: str, port: int):
        self.upstream = (host, port)
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.port = self.listener.getsockname()[1]
        self._sockets: list[socket.socket] = []
        self._lock = threading.Lock()
        self._stop = False
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while not self._stop:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            try:
                server = socket.create_connection(self.upstream, timeout=30)
            except OSError:
                client.close()
                continue
            with self._lock:
                self._sockets.extend((client, server))
            for a, b in ((client, server), (server, client)):
                threading.Thread(target=self._pump, args=(a, b), daemon=True).start()

    @staticmethod
    def _pump(src: socket.socket, dst: socket.socket):
        try:
            while True:
                chunk = src.recv(65536)
                if not chunk:
                    break
                dst.sendall(chunk)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.close()
                except OSError:
                    pass

    def kill_connections(self) -> int:
        """Cut every established connection, as a server-side reap would.

        RST rather than FIN (SO_LINGER 0): a reaped connection is likelier to be
        torn down than closed politely, and the abortive case is the one that
        produced the reported failure.
        """
        with self._lock:
            sockets, self._sockets = self._sockets, []
        for s in sockets:
            try:
                s.setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00"
                )
                s.close()
            except OSError:
                pass
        return len(sockets)

    def close(self):
        self._stop = True
        self.kill_connections()
        try:
            self.listener.close()
        except OSError:
            pass


@pytest.fixture(scope="module")
def proxied_workspace():
    url = urllib.parse.urlsplit(_connection_url())
    proxy = KillableProxy(url.hostname, url.port or 5432)
    opteryx.register_workspace(
        WORKSPACE,
        PostgresConnector,
        host="127.0.0.1",
        port=proxy.port,
        dbname=url.path.lstrip("/"),
        user=urllib.parse.unquote(url.username or ""),
        password=urllib.parse.unquote(url.password or ""),
        sslmode="require",
    )
    try:
        yield proxy
    finally:
        proxy.close()


def _schema_names():
    names = []
    for morsel in opteryx.session().execute_to_morsels(f"SELECT schema_name FROM {RELATION}"):
        names.extend(morsel.column("schema_name").to_pylist())
    return [n.decode("utf-8") if isinstance(n, bytes) else n for n in names]


def test_query_survives_a_pooled_connection_killed_while_idle(proxied_workspace):
    # First query: opens a connection through the proxy and, on completion,
    # parks it in the pool.
    assert "pg_catalog" in _schema_names()

    # The connection is now idle in the pool. Cut it the way a server-side idle
    # reap would -- this process is given no say and no notification.
    assert proxied_workspace.kill_connections() > 0, "no connection was established to kill"

    # The second query draws that dead connection. Before the fix this raised
    # `postgres TLS write failed: ... Connection reset by peer`; the liveness
    # check now drops it at acquire, and the retry covers the case where it dies
    # in the gap between that check and the write.
    assert "pg_catalog" in _schema_names()


def test_the_pool_keeps_working_after_repeated_kills(proxied_workspace):
    """Not a one-shot recovery: the pool stays usable across repeated reaps.

    A fix that recovered once but poisoned the bucket -- leaving the dead
    connection in it, or failing to re-stamp the one it replaced with -- would
    pass the test above and fail here on the second or third round.
    """
    for _ in range(3):
        assert "pg_catalog" in _schema_names()
        proxied_workspace.kill_connections()
    assert "pg_catalog" in _schema_names()
