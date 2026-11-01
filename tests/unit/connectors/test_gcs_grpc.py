"""
Offline tests for the native GCS gRPC reader (src/cpp/gcs_grpc): CRC32C, the
streaming ReadObject response parser, the ALTS record layer, and the DirectPath
discovery parsers (xDS listener / cluster / endpoints, RLS). No network.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.compiled.gcs_grpc import alts_client_start_request_for_test
from opteryx.compiled.gcs_grpc import alts_gcm_rekey_encrypt_for_test_
from opteryx.compiled.gcs_grpc import alts_roundtrip_for_test
from opteryx.compiled.gcs_grpc import crc32c_of
from opteryx.compiled.gcs_grpc import parse_cla_for_test
from opteryx.compiled.gcs_grpc import parse_cluster_for_test
from opteryx.compiled.gcs_grpc import parse_listener_route_for_test
from opteryx.compiled.gcs_grpc import parse_read_object_body_for_test
from opteryx.compiled.gcs_grpc import parse_rls_response_for_test


def _crc32c_ref(data: bytes) -> int:
    c = 0xFFFFFFFF
    for b in data:
        c ^= b
        for _ in range(8):
            c = (c >> 1) ^ (0x82F63B78 if c & 1 else 0)
    return c ^ 0xFFFFFFFF


def _varint(v: int) -> bytes:
    out = bytearray()
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)
    return bytes(out)


def _len_field(number: int, payload: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(payload)) + payload


def _fixed32_field(number: int, value: int) -> bytes:
    return _varint((number << 3) | 5) + value.to_bytes(4, "little")


def _varint_field(number: int, value: int) -> bytes:
    return _varint(number << 3) + _varint(value)


def _str_field(number: int, value: str) -> bytes:
    return _len_field(number, value.encode())


def _any(type_url: str, value: bytes) -> bytes:
    return _str_field(1, type_url) + _len_field(2, value)


def _frame(message: bytes, compressed: int = 0) -> bytes:
    return bytes([compressed]) + len(message).to_bytes(4, "big") + message


def _response(content: bytes, *, crc=None, generation=None, extra=b"") -> bytes:
    """A ReadObjectResponse: checksummed_data(1){content(1), crc32c(2)}, metadata(4){generation(3)}."""
    data = _len_field(1, content) + _fixed32_field(2, _crc32c_ref(content) if crc is None else crc)
    msg = _len_field(1, data) + extra
    if generation is not None:
        msg += _len_field(4, _len_field(1, b"name") + _varint_field(3, generation))
    return _frame(msg)


def test_crc32c_check_value():
    assert crc32c_of(b"123456789") == 0xE3069283
    assert crc32c_of(b"") == 0


def test_crc32c_matches_reference_every_tail_length():
    rng = random.Random(7)
    for n in range(0, 70):
        data = bytes(rng.getrandbits(8) for _ in range(n))
        assert crc32c_of(data) == _crc32c_ref(data), n


@pytest.mark.parametrize("chunk", [1, 2, 3, 5, 7, 64, 4096, 1 << 20])
def test_parser_reassembles_at_every_split(chunk):
    rng = random.Random(chunk)
    parts = [bytes(rng.getrandbits(8) for _ in range(n)) for n in (1000, 1, 3000, 17)]
    # Unknown fields (varint 3, fixed64 5, len 9) must be skipped wherever they sit.
    unknown = _varint_field(30, 12345) + _varint((5 << 3) | 1) + b"\x00" * 8 + _len_field(9, b"skip me")
    body = _response(parts[0], generation=42, extra=unknown)
    body += b"".join(_response(p) for p in parts[1:])
    err, data, generation = parse_read_object_body_for_test(body, sum(map(len, parts)), chunk)
    assert err == ""
    assert data == b"".join(parts)
    assert generation == 42


def test_parser_rejects_crc_mismatch():
    body = _response(b"abcdef", crc=_crc32c_ref(b"abcdeg"))
    err, _, _ = parse_read_object_body_for_test(body, 6, 3)
    assert "CRC32C mismatch" in err


def test_parser_rejects_more_data_than_requested():
    err, _, _ = parse_read_object_body_for_test(_response(b"x" * 10), 9, 4)
    assert "more data than the range requested" in err


def test_parser_reports_short_stream():
    err, _, _ = parse_read_object_body_for_test(_response(b"x" * 10), 20, 4)
    assert err.startswith("short")


def test_parser_reports_truncated_message():
    body = _response(b"x" * 10)
    err, _, _ = parse_read_object_body_for_test(body[:-3], 10, 4)
    assert err == "stream ended inside a message"


def test_parser_rejects_compressed_message():
    body = _frame(_len_field(1, _len_field(1, b"abc")), compressed=1)
    err, _, _ = parse_read_object_body_for_test(body, 3, 4)
    assert "compressed" in err


def test_parser_rejects_field_overrunning_message():
    # checksummed_data claims 50 bytes inside a 10-byte message.
    msg = _varint((1 << 3) | 2) + _varint(50) + b"\x00" * 8
    err, _, _ = parse_read_object_body_for_test(_frame(msg), 5, 4)
    assert "overruns" in err


# ── ALTS record layer ──────────────────────────────────────────────────────

# gRPC's own known-answer vectors for the rekeying AES-128-GCM crypter
# (test/core/tsi/alts/crypt/aes_gcm_test.cc, NIST vectors 3 and 4 with the key
# expanded to 44 bytes, and vector 4 with nonce bit 15 flipped to cross a KDF
# counter boundary): (key, nonce, aad, plaintext, ciphertext+tag), hex.
_KEY = "feffe9928665731c6d6a8f9467308308fffee8938764721d6c6b8e9566318209fcfdeb908467711e6f688d96"
_PT = "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39"
_AAD = "feedfacedeadbeeffeedfacedeadbeefabaddad2"
REKEY_VECTORS = [
    (_KEY, "cafebabefacedbaddecaf888", "", _PT + "1aafd255",
     "1018ed5a1402a86516d6576d70b2ffccca261b94df88b58f53b64dfba435d18b2f6e3b7869f9353d4ac8cf09afb1663daa7b4017e6fc2c177c0c087c0df1162129952213cee1bc6e9c8495dd705e1f3d"),
    (_KEY, "cafebabefacedbaddecaf888", _AAD, _PT,
     "1018ed5a1402a86516d6576d70b2ffccca261b94df88b58f53b64dfba435d18b2f6e3b7869f9353d4ac8cf09afb1663daa7b4017e6fc2c177c0c087c4764565d077e9124001ddb27fc0848c5"),
    (_KEY, "ca7ebabefacedbaddecaf888", _AAD, _PT,
     "e650d3c0fb879327f2d03287fa93cd07342b136215adbca00c3bd5099ec41832b1d18e0423ed26bb12c6cd09debb29230a94c0cee15903656f85edb6fc509b1b28216382172ecbcc31e1e9b1"),
]


@pytest.mark.parametrize("key,nonce,aad,plaintext,expected", REKEY_VECTORS)
def test_alts_rekey_crypter_known_answers(key, nonce, aad, plaintext, expected):
    out = alts_gcm_rekey_encrypt_for_test_(bytes.fromhex(key), bytes.fromhex(nonce), bytes.fromhex(aad), bytes.fromhex(plaintext))
    assert out.hex() == expected


@pytest.mark.parametrize("size,frame", [(1, 16384), (16384, 16384), (3 * 1024 * 1024 + 17, 16384), (3 * 1024 * 1024 + 17, 1 << 20)])
def test_alts_transport_roundtrip_both_directions(size, frame):
    rng = random.Random(size + frame)
    key = bytes(rng.getrandbits(8) for _ in range(44))
    payload = rng.randbytes(size)
    assert alts_roundtrip_for_test(key, payload, frame) == ""


def test_alts_client_start_request_fields():
    req = alts_client_start_request_for_test("storage.googleapis.com")
    # HandshakerReq.client_start (1) is the only field.
    assert req[0] == (1 << 3) | 2
    start = req[2:] if req[1] < 0x80 else req[3:]
    assert b"\x08\x02" in start  # handshake_security_protocol = ALTS
    assert _str_field(2, "grpc") in start
    assert _str_field(3, "ALTSRP_GCM_AES128_REKEY") in start
    assert _str_field(8, "storage.googleapis.com") in start
    version = _varint_field(1, 2) + _varint_field(2, 1)
    assert _len_field(9, _len_field(1, version) + _len_field(2, version)) in start


# ── DirectPath discovery ───────────────────────────────────────────────────

HCM = "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager"
RLS = "type.googleapis.com/grpc.lookup.v1.RouteLookupClusterSpecifier"
EDS_CLUSTER = "xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.cluster.v3.Cluster/storage.googleapis.com/eds_cluster"
CFE = "xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.cluster.v3.Cluster/google_cfe_storage.googleapis.com"
PATH = "/google.storage.v2.Storage/ReadObject"


def _rls_config() -> bytes:
    names = b"".join(_len_field(1, _str_field(1, s)) for s in (
        "google.storage.v2.Storage", "google.storage.control.v2.StorageControl", "google.longrunning.Operations"))
    extra = _len_field(3, _str_field(1, "server") + _str_field(2, "service") + _str_field(3, "method"))
    headers = _len_field(2, _str_field(1, "x-goog-request-params") + _str_field(2, "x-goog-request-params"))
    builder = _len_field(2, names + extra + headers)
    return (builder + _str_field(3, "dns:///storage.googleapis.com") + _len_field(5, _varint_field(1, 300))
            + _len_field(6, _varint_field(1, 290)) + _str_field(9, CFE))


def _listener(*, rds=False, match=None, plugin_type=RLS) -> bytes:
    """The listener shape directpath-pa served on 2026-11-01 (design §10)."""
    match = _str_field(1, "") if match is None else match
    routes = (_len_field(3, _len_field(1, match) + _len_field(2, _str_field(37, "rls_config")))
              + _len_field(3, _len_field(1, _str_field(1, "")) + _len_field(2, _str_field(1, EDS_CLUSTER))))
    vhost = _str_field(1, "host_1") + _str_field(2, "*") + routes
    plugin = _len_field(12, _len_field(1, _str_field(1, "rls_config") + _len_field(2, _any(plugin_type, _len_field(1, _rls_config())))))
    route_config = _str_field(1, "route") + _len_field(2, vhost) + plugin
    hcm = _str_field(2, "trafficdirector") + (_len_field(3, b"") if rds else _len_field(4, route_config))
    return _str_field(1, "listener") + _len_field(19, _len_field(1, _any(HCM, hcm)))


def test_listener_route_goes_through_rls_with_the_bucket_key():
    d = parse_listener_route_for_test(_listener(), "storage.googleapis.com", PATH, "opteryx_data")
    assert d["use_rls"] is True
    assert d["lookup_service"] == "dns:///storage.googleapis.com"
    assert d["max_age_s"] == 300
    assert d["default_target"] == CFE
    assert d["key_map"] == {
        "server": "storage.googleapis.com",
        "service": "google.storage.v2.Storage",
        "method": "ReadObject",
        "x-goog-request-params": "bucket=projects%2F_%2Fbuckets%2Fopteryx_data",
    }
    # RouteLookupRequest: target_type (3) "grpc", reason (5) = REASON_MISS.
    assert d["rls_request"].startswith(_str_field(3, "grpc"))
    assert d["rls_request"].endswith(_varint_field(5, 1))


def test_listener_route_refuses_what_it_does_not_evaluate():
    with pytest.raises(RuntimeError, match="RDS"):
        parse_listener_route_for_test(_listener(rds=True), "storage.googleapis.com", PATH, "b")
    with pytest.raises(RuntimeError, match="route match field 6"):
        parse_listener_route_for_test(_listener(match=_str_field(1, "") + _len_field(6, b"")), "storage.googleapis.com", PATH, "b")
    with pytest.raises(RuntimeError, match="plugin type"):
        parse_listener_route_for_test(_listener(plugin_type="type.googleapis.com/other"), "storage.googleapis.com", PATH, "b")


def test_listener_route_falls_to_a_fixed_cluster_when_the_first_route_does_not_match():
    d = parse_listener_route_for_test(_listener(match=_str_field(1, "/other.Service/")), "storage.googleapis.com", PATH, "b")
    assert d == {"cluster": EDS_CLUSTER, "use_rls": False}


def test_cluster_must_be_eds():
    eds = _str_field(1, "c1") + _varint_field(2, 3) + _len_field(3, _str_field(2, "svc1"))
    assert parse_cluster_for_test(eds) == "svc1"
    assert parse_cluster_for_test(_str_field(1, "c1") + _varint_field(2, 3)) == "c1"
    with pytest.raises(RuntimeError, match="not an EDS cluster"):
        parse_cluster_for_test(_str_field(1, "c1"))  # STATIC (type absent)
    with pytest.raises(RuntimeError, match="not an EDS cluster"):
        parse_cluster_for_test(_str_field(1, "c1") + _len_field(38, b""))  # custom / aggregate


def _lb_endpoint(ip: str, port: int, health: int = 0) -> bytes:
    socket_address = _str_field(2, ip) + _varint_field(3, port)
    endpoint = _len_field(1, _len_field(1, socket_address))  # Endpoint.address.socket_address
    return _len_field(2, _len_field(1, endpoint) + (_varint_field(2, health) if health else b""))


def test_cluster_load_assignment_keeps_healthy_endpoints_of_the_best_priority():
    p0 = _len_field(2, _lb_endpoint("34.126.8.41", 22171) + _lb_endpoint("34.126.8.41", 22253, health=1)
                    + _lb_endpoint("34.126.8.42", 1, health=2) + _lb_endpoint("34.126.8.43", 2, health=5))
    p1 = _len_field(2, _lb_endpoint("10.0.0.1", 9) + _varint_field(5, 1))
    cla = _str_field(1, "svc1") + p1 + p0
    assert parse_cla_for_test(cla) == [("34.126.8.41", 22171), ("34.126.8.41", 22253), ("34.126.8.43", 2)]


def test_rls_response_targets():
    target = "xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.cluster.v3.Cluster/gcs-grpc-prod-us-east1-default-default.googleapis.com/eds_cluster"
    assert parse_rls_response_for_test(_str_field(3, target) + _str_field(2, "hd")) == [target]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
