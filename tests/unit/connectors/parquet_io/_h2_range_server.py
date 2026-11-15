"""A TLS + HTTP/2 server for byte-range GETs, so the native HttpClient's retries
can be tested over HTTP/2 multiplexing, the way it reads GCS.

Over HTTP/1.1 an aborted transfer always costs curl its connection, so a retry
dials afresh whatever the client does. Over HTTP/2 an abort resets only the
stream and the connection stays in curl's cache: whether a retry escapes a dead
connection is decided by the client, and only an HTTP/2 server can show it.

Any path holds `byte i = i % 251`. A GET answers its `Range: bytes=a-b` with 206.

    stall_connections  on the first N connections, every stream gets its headers
                       and then nothing more (a dead connection that stays open)

`server.connections` counts connections accepted. The certificate is self-signed
for `localhost`; point the client at `server.ca_file` (SSL_CERT_FILE).
"""

import os
import socket
import ssl
import sys
import tempfile
import threading
import time

import h2.config
import h2.connection
import h2.events

sys.path.insert(1, os.path.join(os.path.dirname(__file__), ".."))
from _grpc_read_object_server import _self_signed  # noqa: E402


def object_bytes(offset, length):
    return bytes((offset + i) % 251 for i in range(length))


def _range_of(headers):
    value = dict(headers).get("range", "")
    start, _, end = value.removeprefix("bytes=").partition("-")
    return int(start), int(end) - int(start) + 1


class H2RangeServer:
    def __init__(self, stall_connections=0):
        self.stall_connections = stall_connections
        self.connections = 0
        self._stop = threading.Event()

    def __enter__(self):
        self._dir = tempfile.TemporaryDirectory()
        self.ca_file, key_file = _self_signed(self._dir.name)
        self._tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._tls.load_cert_chain(self.ca_file, key_file)
        self._tls.set_alpn_protocols(["h2"])
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._listener.close()
        self._dir.cleanup()

    def _accept(self):
        while not self._stop.is_set():
            try:
                raw, _ = self._listener.accept()
            except OSError:
                return
            self.connections += 1
            stalled = self.connections <= self.stall_connections
            threading.Thread(target=self._serve, args=(raw, stalled), daemon=True).start()

    def _serve(self, raw, stalled):
        try:
            sock = self._tls.wrap_socket(raw, server_side=True)
        except (OSError, ssl.SSLError):
            return
        conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
        lock = threading.Lock()
        resets = set()
        with lock:
            conn.initiate_connection()
            sock.sendall(conn.data_to_send())
        while not self._stop.is_set():
            try:
                data = sock.recv(65536)
            except OSError:
                return
            if not data:
                return
            with lock:
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.RequestReceived):
                        headers = [(k.decode() if isinstance(k, bytes) else k,
                                    v.decode() if isinstance(v, bytes) else v) for k, v in event.headers]
                        threading.Thread(target=self._answer, daemon=True,
                                         args=(conn, sock, lock, resets, event.stream_id,
                                               headers, stalled)).start()
                    elif isinstance(event, h2.events.StreamReset):
                        resets.add(event.stream_id)
                pending = conn.data_to_send()
                if pending:
                    sock.sendall(pending)

    def _answer(self, conn, sock, lock, resets, sid, headers, stalled):
        offset, length = _range_of(headers)
        with lock:
            conn.send_headers(sid, [(":status", "206"), ("content-length", str(length))])
            sock.sendall(conn.data_to_send())
        if stalled:
            return  # headers, then silence on a connection that stays open
        payload = object_bytes(offset, length)
        while payload:
            with lock:
                if sid in resets or self._stop.is_set():
                    return
                n = min(len(payload), conn.local_flow_control_window(sid), conn.max_outbound_frame_size)
                if n > 0:
                    conn.send_data(sid, payload[:n], end_stream=(n == len(payload)))
                    sock.sendall(conn.data_to_send())
                    payload = payload[n:]
            if n <= 0:
                time.sleep(0.005)  # wait for the client's WINDOW_UPDATE
