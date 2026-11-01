"""A ReadObject-only gRPC server over TLS + HTTP/2, so the native DirectPath reader's
deadlines (docs/REMOTE_READ_FLOW_CONTROL_DESIGN.md §4.1) can be tested offline.

Any object holds `byte i = i % 251`. Each ReadObject stream answers its
(read_offset, read_limit) in 1 KiB ReadObjectResponse messages, then grpc-status 0.

    stall_offset  the stream whose read_offset equals this sends half its range,
                  then nothing more on an open stream (a dead backend)
    drip_s        seconds between messages on every stream (a crawling backend)

The certificate is self-signed for `localhost`; pass `server.ca_file` to the client.
"""

import datetime
import os
import socket
import ssl
import tempfile
import threading
import time

import h2.config
import h2.connection
import h2.events
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

MESSAGE_BYTES = 1024


def object_bytes(offset, length):
    return bytes((offset + i) % 251 for i in range(length))


def _varint(v):
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _read_varint(buf, at):
    v = shift = 0
    while True:
        b = buf[at]
        at += 1
        v |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return v, at


def _range_of(request):
    """(read_offset, read_limit) from one framed ReadObjectRequest (fields 4 and 5)."""
    msg = request[5:]
    at = 0
    offset = limit = 0
    while at < len(msg):
        key, at = _read_varint(msg, at)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, at = _read_varint(msg, at)
            if field == 4:
                offset = value
            elif field == 5:
                limit = value
        elif wire == 2:
            n, at = _read_varint(msg, at)
            at += n
        else:
            raise ValueError(f"unexpected wire type {wire} in ReadObjectRequest")
    return offset, limit


def _response(content):
    data = b"\x0a" + _varint(len(content)) + content  # ChecksummedData.content = 1
    msg = b"\x0a" + _varint(len(data)) + data  # ReadObjectResponse.checksummed_data = 1
    return b"\x00" + len(msg).to_bytes(4, "big") + msg


def _self_signed(directory):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_file = os.path.join(directory, "cert.pem")
    key_file = os.path.join(directory, "key.pem")
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(key_file, "wb") as f:
        f.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                  serialization.NoEncryption()))
    return cert_file, key_file


class ReadObjectServer:
    def __init__(self, stall_offset=None, drip_s=0.0):
        self.stall_offset = stall_offset
        self.drip_s = drip_s
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
            threading.Thread(target=self._serve, args=(raw,), daemon=True).start()

    def _serve(self, raw):
        sock = self._tls.wrap_socket(raw, server_side=True)
        conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
        lock = threading.Lock()
        bodies, resets = {}, set()

        def flush():
            data = conn.data_to_send()
            if data:
                sock.sendall(data)

        with lock:
            conn.initiate_connection()
            flush()
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
                        bodies[event.stream_id] = b""
                    elif isinstance(event, h2.events.DataReceived):
                        bodies[event.stream_id] += event.data
                        conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                    elif isinstance(event, h2.events.StreamEnded):
                        offset, limit = _range_of(bodies.pop(event.stream_id))
                        threading.Thread(target=self._answer, daemon=True,
                                         args=(conn, sock, lock, resets, event.stream_id, offset, limit)).start()
                    elif isinstance(event, h2.events.StreamReset):
                        resets.add(event.stream_id)
                flush()

    def _answer(self, conn, sock, lock, resets, sid, offset, limit):
        def send(payload):
            while len(payload):
                with lock:
                    if sid in resets or self._stop.is_set():
                        return False
                    n = min(len(payload), conn.local_flow_control_window(sid), conn.max_outbound_frame_size)
                    if n > 0:
                        conn.send_data(sid, payload[:n])
                        sock.sendall(conn.data_to_send())
                        payload = payload[n:]
                if n <= 0:
                    time.sleep(0.005)  # wait for the client's WINDOW_UPDATE
            return True

        with lock:
            conn.send_headers(sid, [(":status", "200"), ("content-type", "application/grpc")])
            sock.sendall(conn.data_to_send())
        end = offset + (limit // 2 if offset == self.stall_offset else limit)
        for at in range(offset, end, MESSAGE_BYTES):
            if not send(_response(object_bytes(at, min(MESSAGE_BYTES, end - at)))):
                return
            if self.drip_s:
                time.sleep(self.drip_s)
        if end < offset + limit:
            self._stop.wait()  # stalled: the stream stays open with nothing more to say
            return
        with lock:
            if sid in resets:
                return
            conn.send_headers(sid, [("grpc-status", "0")], end_stream=True)
            sock.sendall(conn.data_to_send())
