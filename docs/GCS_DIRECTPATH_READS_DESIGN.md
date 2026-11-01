# GCS reads over gRPC Direct Connectivity (DirectPath)

**Status (2026-11-01):** rulings R1–R7 made (§9). Phase 0 passed. Phase 1 is built. Phase 2 is built and wired behind `GCS_TRANSPORT=grpc`, and its 16/64 gate PASSED: cold 0.884x, hot 0.948x (§7). Phase 3 is the architect's call.

**Problem:** cold reads are still slow. On the fleet size ladder (2026-10-30, bastian's 31 statements, chunk cache flushed between queries):

| node | cold total | hot total | cold − hot | cold IO-bound read rate (SF100 Q8, 8.3 GB) |
|---|---|---|---|---|
| 4 vCPU / 16 GB | 188.3 s | 136.0 s | 52.3 s | 415 MB/s |
| 8 vCPU / 32 GB | 150.6 s | 103.8 s | 46.8 s | 675 MB/s |
| 16 vCPU / 64 GB | 144.5 s | 86.9 s | 57.6 s | 690 MB/s |
| 32 vCPU / 128 GB | 128.1 s | 74.2 s | 53.9 s | 855 MB/s |

The chunk cache holds fetched bytes, so hot runs still decode. That makes **cold − hot ≈ time spent waiting on GCS**. That gap is the most any transport change can recover: about 40% of the cold total, and 0% of the hot total. The read rate stops improving from 8 vCPU even though the NIC keeps getting faster, so something other than the NIC limits cold reads. This design asks whether that limit is Google's front end (GFE) and, if so, removes it.

Separately, a single range read takes ~30 ms to first byte over h2, and streams at ~220 MB/s per connection (curl CLI from an n4a, 2026-10-30). Jobs made of many small ranges are bound by that latency.

---

## 1. What DirectPath is (verified facts)

Sources, read 2026-11-01:
- https://docs.cloud.google.com/storage/docs/direct-connectivity
- https://docs.cloud.google.com/storage/docs/enable-grpc-api
- `grpc/grpc` master: `src/core/resolver/google_c2p/google_c2p_resolver.cc`, `src/core/tsi/alts/{handshaker,frame_protector,crypt}/`
- `googleapis/googleapis` master: `google/storage/v2/storage.proto`

Items marked **NV** are not yet verified. They are listed in §10.

- **Where it works:** "Direct connectivity is only available for requests made from Compute Engine virtual machines." Cloud Run is not supported. The VM must have an attached service account, which is the VM's identity in the ALTS handshake. The VM must be in the bucket's region (or in a region that belongs to the bucket's multi-region). No bucket or project enablement step is documented.
- **Network:** egress must reach `34.126.0.0/18` (IPv4) and `2001:4860:8040::/42` (IPv6) on all ports, plus `storage.googleapis.com:443` and `directpath-pa.googleapis.com:443`. It is not supported with Private Service Connect or regional endpoints. Google recommends an MTU of at least 4096. A built-in check exists: `gcloud storage diagnose --test-type=DIRECT_CONNECTIVITY gs://BUCKET`.
- **What it removes:** "bypasses Google Front Ends (GFEs) to reduce latency." Google publishes no figures. Google also warns that gRPC holds more connections than REST, so high parallelism can hit "too many open files". That is the FD1024 class of failure we already know.
- **Discovery needs xDS.** In gRPC core, the `google-c2p:///storage.googleapis.com` resolver reaches DirectPath only through xDS: an ADS stream to `directpath-pa.googleapis.com` with `google_default` credentials. It asks the metadata server for the VM's zone, sends `TRAFFICDIRECTOR_DIRECTPATH_C2P_IPV6_CAPABLE: true` in the xDS node metadata, and sets `node.id = "C2P-<random>"`. The other branch is plain DNS to the GFE, which is not DirectPath.
- **ALTS handshake:** one multi-message gRPC call over a **plaintext** channel to a handshaker service. The address and method name are **NV** (believed to be the metadata server, port 8080, `/grpc.gcp.HandshakerService/DoHandshake`).
  - The client sends `StartClientHandshakeReq`: protocol ALTS, application protocols, record protocols, target identities, max frame size.
  - The result returns `key_data` (at least 44 bytes), the peer identity, the agreed record protocol and the max frame size.
- **ALTS record protocol:**
  - AES-128-GCM through OpenSSL EVP, with a 12-byte nonce and a 16-byte tag.
  - The 44-byte key splits into a 32-byte KDF key and a 12-byte nonce mask.
  - The record key is HMAC-SHA256(kdf_key, nonce[2..7] ‖ 0x01)[0..16]. It is re-derived whenever those nonce bytes change, so each key protects at most 2^16 frames.
  - The nonce is a little-endian counter XOR the mask. The counter's top byte is 0x80 on the client side.
  - Frames are 1 KiB to 1 MiB, 16 KiB by default, and the size is negotiated.
  - The frame header layout is **NV**.
  - Source size: about 1.3k lines across aes_gcm.cc (736), alts_frame_protector.cc (411) and alts_counter.cc (116). Its dependencies are OpenSSL EVP/HMAC plus thin `gpr_malloc`/`absl::Span` helpers.
- **Storage v2 read RPCs:**
  - `ReadObject` is server streaming, with `read_offset`/`read_limit`.
  - `BidiReadObject` is a two-way stream that carries **up to 100 ranges per request message**, each tagged with a caller-unique `read_id`. Data returns as `ObjectRangeData { ChecksummedData{content, crc32c}, read_range, range_end }`.
  - A redirect returns `read_handle` and `routing_token`. The token goes into `x-goog-request-params` on retry.
  - Requests route on `bucket` (`read_object_spec.bucket` for bidi).

## 2. Where it does and does not apply

| path | uses DirectPath? | why |
|---|---|---|
| Fleet workers (GCE, n4a, us-east1) reading same-region buckets | **yes** | the only environment Google supports |
| odata on Cloud Run (x86) | no | Cloud Run is unsupported; stays on REST |
| `READ_*` table functions (anonymous reads) | no | ALTS presents the VM's **system** identity. READANON rules system credentials never go on `READ_*`. These stay on REST. **Ruling R6.** |
| Buckets in another region | no | Google: "improves ... only when the Compute Engine instance and the Cloud Storage bucket are in the same region" |
| Hot reads (chunk cache hits) | n/a | no network |
| Planning | ~0 | manifests come from the catalog; parquet footers will come from the disk metadata cache (DISKMETA) |

When the fleet's GCS caching layer is switched on, more reads become hot and this design's payoff shrinks. It is worth building only for the cold reads that remain.

## 3. Shape of the change

There is one integration point, below the existing call sites. Today every `gs://` byte goes through `HttpClient` (`src/cpp/http_client.{hpp,cpp}`, libcurl, a thread-local `CURLM` per BS worker):

- `rugo/src/parquet/io_pipeline.hpp` `fetch_remote_groups` (data, one `get_many` per fetch block)
- `read_range` (single range)
- `rugo/src/parquet/filesystem.hpp` `FetchParquetFootersMany` / `file_size_of` (footers, HEAD)

The design adds a second transport for `gs://` only, **`GcsGrpcClient`**, in `src/cpp/` beside `HttpClient`. It belongs to the opteryx build only, like `HttpClient`. rugo stays curl-free and gRPC-free under its existing `RUGO_ENABLE_HTTP` gate.

The pipeline chooses the transport **once per scan**, at construction, from config. It never chooses per request and never falls back (§6).

```
io_pipeline (per scan: transport chosen once)
  ├── REST:       HttpClient::get_many(url, ranges)             [unchanged]
  └── DirectPath: GcsGrpcClient::read_many(bucket, object, generation?, ranges)
                     │  same contract: buffers in range order, HttpError-style
                     │  retryable/non-retryable errors, ByteGate admission
                     ├── C5 storage reader   (ReadObject stream per range, CRC32C)
                     ├── C1 gRPC-over-h2     (nghttp2 session, 5-byte msg framing, trailers)
                     ├── C3 ALTS             (handshaker RPC + AES-GCM-rekey record layer)
                     ├── C4 discovery        (minimal xDS ADS → backend IPv6 endpoints)
                     └── C2 protobuf wire    (hand-rolled, only the messages used)
```

The fetch threads, fetch-ahead, the C7 chunk cache, admission (`ByteGate`), memory charging (`FetchBlock::buffers_charge`) and decode stay **unchanged**. Only the source of the bytes changes. ByteGate admission applies to the DirectPath client too, so the process-wide bytes-in-flight contract still holds.

## 4. Components

### C1 — Minimal gRPC over HTTP/2
- Built on **nghttp2's session API**. nghttp2 is already a link dependency through curl (`setup.py --with-nghttp2`; CI installs `libnghttp2-dev`). nghttp2 does no I/O itself, because we feed it bytes, so the same session code runs over plaintext TCP (handshaker), TLS (xDS) and ALTS (storage).
- Needs only these: the 5-byte message prefix, `content-type: application/grpc`, `te: trailers`, `grpc-status`/`grpc-message` trailers, and server-streaming and bidirectional calls.
- No compression, no retries in the transport (the caller's retry budget applies, as today), and no load-balancing policies.
- **Not** vendored gRPC.

### C2 — Protobuf wire format
- Hand-rolled varint and length-delimited encode/decode for about 15 messages: handshaker request/response, xDS DiscoveryRequest/Response plus the Listener → RouteConfiguration → Cluster → ClusterLoadAssignment fields we read, and the ReadObject request/response.
- Unknown fields are skipped. A malformed required field raises an error.
- **RULED 2026-11-01 (superseded the same day, see C5):** BidiReadObject + one copy. Superseded because GCS refuses BidiReadObject on regional buckets; with `ReadObject`, `ChecksummedData.content` is parsed as it arrives and written **straight into the range's destination buffer**, with no intermediate copy.
- Alternative: vendor upb, which is what gRPC uses. That is far more code than the subset we need. **Ruling R2.**

### C3 — ALTS
- **Handshaker client:** C1 over plaintext h2c to the handshaker service. Send `StartClientHandshakeReq`, then shuttle `out_frames` and `bytes_consumed` between the backend socket and the handshaker until `result` arrives. Validate the result as gRPC does (non-empty peer service account, key length ≥ 44, matching record protocol). Any failure raises an error.
- **Record layer:** **vendor** `crypt/aes_gcm.cc`, `crypt/gsec.h`, `frame_protector/alts_counter.cc` and the frame protector from `grpc/grpc` (Apache-2.0) into `third_party/grpc_alts/`.
  - Strip them to OpenSSL EVP only: replace `gpr_malloc` with `malloc` and `absl::Span` with a pointer and length.
  - This is about 1.3k lines of crypto that is exact and easy to get wrong, so vendoring it beats rewriting it. **Ruling R1.**
  - It needs OpenSSL ≥ 1.1. `setup.py` already refuses anything older (`assert_openssl_thread_safe`).
- The frame size is negotiated up to 1 MiB. Large frames matter for throughput because each frame costs one GCM seal/open.

### C4 — Discovery (minimal xDS)
- One ADS stream over TLS (C1 on OpenSSL) to `directpath-pa.googleapis.com:443`, authorized with the VM's metadata-server access token (`google_default`).
- Node metadata as the c2p resolver sends it: zone from the metadata server, IPv6-capable flag, `C2P-<random>` id.
- Walk LDS → RDS → CDS → EDS for `storage.googleapis.com` and keep only the endpoint addresses. Locality weighting is ignored; connections are spread round-robin.
- The stream stays open to receive endpoint updates. An update affects new connections only.
- This is the largest unknown (§10). If discovery turns out to need more of xDS than this subset, stop and come back to the architect. Do not vendor the gRPC xDS client.

### C5 — Storage reader
- **RULED 2026-11-01: `ReadObject`, one server-streaming call per range**, all multiplexed on the pooled connections. `BidiReadObject` is unavailable: GCS answered PERMISSION_DENIED, "The BidiReadObject RPC is not yet available for general use, or for project (762690895289) or bucket (opteryx_data)". It belongs to the zonal "Rapid" bucket private preview, which needs a Google account team; we have none.
- `read_many(bucket, object, ranges)` starts one `ReadObject` (`read_offset`, `read_limit`) per range. A streaming parser follows each response's bytes and writes `content` straight into the range's buffer.
- A range is complete when the stream ends with grpc-status OK, on a message boundary, with exactly `length` bytes.
- **CRC32C is verified for every chunk.** A mismatch is a retryable error. `src/cpp/gcs_grpc/crc32c.hpp` selects the path at compile time: SSE4.2 on x86-64, ARMv8 CRC32 where the ISA floor enables it (Apple), and slice-by-8 tables otherwise. **RULED 2026-11-01:** the Linux AArch64 floor is now `armv8-a+crc+aes` (`build_common.py`), so the fleet gets hardware CRC32C too.
- Generation: each stream's first message carries the object's metadata. Every range of one `read_many` must report the same generation (and the pinned one, if given), else the read fails as FAILED_PRECONDITION.
- Auth: the scan's bearer token, the same one `http_headers_` sends today. ALTS authenticates the VM. The request is still authorized by the token.

### Connections and threads
Today each BS worker thread owns one `HttpClient` and drives its `CURLM` on its own thread. There is no event loop (UMBRAPAPERS: the event-loop fetcher was shelved).

**RULED (R3): process-wide pool.** One `GcsGrpcClient` per process owns N ALTS connections to the discovered backends, each one nghttp2 session carrying many h2 streams.
- Each connection has **one native I/O thread** that owns the socket, the ALTS record layer and the nghttp2 session. Nothing else touches them, so they need no locks.
- Fetch threads call `read_many`, which hands the block's ranges to a connection (round-robin, or the one with the fewest open streams) through a queue, then waits on a per-call completion. The I/O thread writes each range's bytes straight into the caller's preallocated buffers.
- This is a native transport, not the fetcher event loop that UMBRAPAPERS shelved: fetch threads, fetch-ahead and admission stay as they are.
- Handshakes: N per process, not one per fetch thread. File descriptors: N plus the xDS stream. That keeps us well clear of the FD1024 class, which Google itself warns about for gRPC.
- N defaults to 8. Phase 2 measures 4, 8 and 16 on the ladder and sets the default from the data (PERFNOTPOLICY).
- If a connection drops, its in-flight calls fail retryable, and the connection is redialled (a fresh handshake) before taking new work.

## 5. Config

- `GCS_TRANSPORT` = `rest` (default) | `grpc` (DirectPath). It follows the existing no-prefix env naming (envnames). It is per process, read at pipeline construction, and not SET-able per query unless asked for. **Ruling R4.**
- No auto mode. With `grpc`, a scan that cannot discover endpoints, handshake or connect **raises** a clear error (e.g. "DirectPath: handshaker unreachable at …; set GCS_TRANSPORT=rest or fix VPC routes to 2001:4860:8040::/42"). It never silently degrades to REST (CLAUDE.md §1).
- The fleet template sets `GCS_TRANSPORT=grpc`. Cloud Run never sets it.

## 6. Failure semantics
- Transient errors use the same contract as `HttpError.retryable`: `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `DEADLINE_EXCEEDED`, a broken connection, a CRC mismatch, a redirect. They go through the caller's existing `max_retries` budget. `NOT_FOUND`, `PERMISSION_DENIED`, `FAILED_PRECONDITION` (generation changed) and a handshake rejection are hard failures.
- Deadlines: the same rule as REST (bytes ÷ min bandwidth, with a floor), set as `grpc-timeout` and enforced locally.
- No silent fallback to REST, per request or per scan.

## 7. Phases and gates

**Phase 0: prove the plateau is the GFE (measurement only, nothing vendored, nothing in `opteryx/`).**
Run on a 16 vCPU / 64 GB n4a bench VM from template v4 (the same shape as the ladder), us-east1, against the SF100 Q8 object set. Throughput is the aggregate over many objects at matched concurrency.
1. **Our REST path, uncapped:** the engine cold scan with `max_host_connections`, `max_bytes_in_flight` (ByteGate) and fetch-ahead each raised well past the defaults. If the plateau moves, it is our own limit. In that case, fix that and stop this design.
2. **curl CLI, h2, many objects in parallel:** the transport ceiling without the engine.
3. **The stock `google-cloud-cpp` client with DirectPath on:** built on the VM in `scratch/`. Confirm with `gcloud storage diagnose --test-type=DIRECT_CONNECTIVITY` that DirectPath is actually used. Same ranges, same concurrency.

**Gate:** go ahead only if (3) beats (2) by **≥ 1.3x on aggregate cold throughput** or cuts **time to first byte by ≥ 30%**, and (1) has shown the plateau is not ours. Otherwise this design is dropped (KNOB, PROVEIT).
- Infra prerequisite: firewall and routes on the fleet VPC for the ranges in §1, plus IPv6 on the subnet. This is an IAM/VPC change that needs the architect's OK.

**Phase 0 result (2026-11-01, n4a-standard-4 spot bench VM from worker-fleet-v5, us-east1-b, no VPC changes):**
- DirectPath works over **IPv4 alone**. The Go client resolved `google-c2p:///storage.googleapis.com` through xDS and connected to `34.126.8.41`. IPv6 and the MTU change are not needed to start. The metadata server answered 404 to the IPv6 capability query, the MTU stayed at 1460, and the VPC stayed in auto mode.
- Probe: the Go storage client v1.70.0 (scratch only), ClickBench `hits` objects (~100 files), 8 MiB random ranges, 4 GiB per run, three arms interleaved with arm order alternating each rep, 3 reps. Medians, MB/s:

| concurrency | REST (JSON/XML via GFE) | gRPC via GFE | gRPC DirectPath | DirectPath ÷ REST |
|---|---|---|---|---|
| 4 | 321 | 312 | 506 | 1.6x |
| 16 | 269 | 267 | 1856 | 6.9x |
| 64 | 286 | 252 | 4014 | 14x |

- Time to first byte (64 KiB range, reused connection, p50): REST 29–31 ms, gRPC via the GFE 36 ms, DirectPath 33–36 ms. **There is no latency win.** The ~30 ms is GCS's own backend, so small-range jobs gain nothing.
- Checks: 40 random 8 MiB ranges were identical (sha256) on REST and DirectPath, and the NIC receive counter confirmed ~3.8 GB/s on the wire. Neither the REST arm nor the gRPC-via-GFE arm got past ~350 MB/s at any concurrency, so **the REST plateau is the GFE path, not the protocol**.
- **The gate passes on throughput** (≥ 1.3x) and fails on time to first byte.
- Arm (1) was run on 2026-11-01; results below.

**Arm (1) result (2026-11-01).**
- **Setup:**
  - Two VMs, n4a-standard-4 and n4a-standard-16, both from worker-fleet-v5, deleted after the run.
  - The released wheel 0.9.181 reads `gs://opteryx_data/benchmarks/clickbench/hits/data/` (14.7 GB) over REST through a `FileSystemConnector` workspace.
  - Every run is a fresh process, so every run is cold.
  - Arm order rotates each rep; 3 reps. `ulimit -n` is raised for every arm (see the fd note below).
  - MB/s is the NIC receive counter divided by wall time.
- **The three arms:**
  - **default:** the engine's own defaults.
  - **raised16:** host connections 16, bytes in flight uncapped, fetch-ahead 128, 32 IO workers.
  - **raised64:** host connections 64, bytes in flight uncapped, fetch-ahead 256, 64 IO workers.
- Median results:

| vCPU | query | default | raised16 | raised64 | CPU busy (default) |
|---|---|---|---|---|---|
| 4 | ClickBench Q21 (URL LIKE) | 3.75 s, 760 MB/s | 4.69 s | 6.26 s | 71% |
| 4 | Q22 | 4.42 s, 734 MB/s | 5.41 s | 7.37 s | 72% |
| 4 | Q23 (SELECT *) | 3.26 s | 5.41 s | 9.17 s | 61% |
| 16 | Q21 | 2.03 s, 1409 MB/s | 2.82 s | 4.20 s | 33% |
| 16 | Q22 | 2.53 s, 1287 MB/s | 3.33 s | 4.86 s | 32% |
| 16 | Q23 | 2.00 s | 3.28 s | 5.72 s | 25% |

- **Raising our caps never helps.** Every raise was slower, at every size; Q23 also fetched 1.6–2.7x more bytes, as prefetch the query threw away. **The plateau is not ours**, so the gate's condition (1) holds.
- **Our REST path already beats the Go REST probe by 2.4–4.7x** (760 MB/s on 4 vCPU and ~1.4 GB/s on 16, against the probe's ~300 MB/s). The "14x" in the Phase 0 table compares DirectPath with the Go client's REST, not with ours. Against the engine, DirectPath's transport headroom is **~0.7x at 4 parallel reads** (the Go probe's 506 MB/s is below our 760), **~1.3x at 16** (1856 against 1409), and **~2.8x at 64** (4014).
- **What decides the gain is CPU.**
  - At 4 vCPU the engine is ~70% CPU-busy at 760 MB/s, so a faster transport gains no more than ~1.3x before decode saturates.
  - At 16 vCPU it is only ~30% busy and IO-bound, so DirectPath could be worth up to ~2x on cold IO-bound scans there.
  - Phase 2's value is therefore on the 16/64 and larger fleet shapes, not on today's 4/16 prod node.
- **fd trap (not new):** with the default `ulimit -n 1024`, raised64 failed with `Couldn't resolve host name` / `Problem with the SSL CA cert`. That is the known fd ≥ 1024 failure.
- Harness: session scratchpad `engab/` (q.py, run.py, results-4/16.jsonl).

**Phase 1: C1 + C2 + C5 over TLS to `storage.googleapis.com` (gRPC through the GFE, no ALTS).**
This runs anywhere, Mac included, and exercises the whole read path, CRC32C and ReadObject. It is not expected to beat REST. It is a step toward Phase 2, not a product. Per R5 it is test-only: it is not a `GCS_TRANSPORT` value.
- Tests: CRC mismatch injection, a redirect, a generation change mid-scan, range-count edges at 100 and 101, GFE-path reads against the live test bucket only if authorised (LIVESUITE).

**Phase 2: C3 + C4 → DirectPath on the VM.**
Rerun the cold size ladder (4/16, 8/32, 16/64, 32/128) with REST and DirectPath alternating, interleaved per query (thermal, ordbias). Also run the pypi small-range job for time to first byte.
- **Success:** cold total improves by at least 10% on 16/64 and nothing regresses on hot. Anything slower is deleted (SLOWDITCH).
- **RULED 2026-11-01 (after arm 1):** Phase 2 goes ahead and is judged on 16/64, the likely paying-fleet shape. On today's 4/16 prod node the gain is expected to be small (≤ ~1.3x, CPU-capped), and that is accepted.

**Phase 2 progress (2026-11-01).** C3 and C4 are built and live-verified on an n4a-standard-16 VM, which was deleted afterwards. Nothing is wired into the engine yet; the reader is reached only through the test binding.
- **Code:**
  - `src/cpp/gcs_grpc/transport.{hpp,cpp}`: TLS and plaintext transports.
  - `alts.{hpp,cpp}`: the handshake through the metadata-server handshaker, and `AltsTransport` on top of the vendored record protocol.
  - `directpath.{hpp,cpp}`: the metadata server, a persistent ADS watch (ACKs, resubscribe on reconnect), and RLS cached per bucket for maxAge. A route to the GFE default target raises an error.
  - `GcsGrpcClient::Options::direct_path`, with one ALTS pool per endpoint set, spread round-robin.
  - `third_party/grpc_alts/`: R1, pinned at grpc `df3dd4dc`, edits listed in its README.
- **Offline tests:** 30 in `tests/unit/connectors/test_gcs_grpc.py`.
  - gRPC's known-answer vectors for the rekeying crypter, including the KDF-boundary case.
  - ALTS round trips in both directions at 16 KiB and 1 MiB frames.
  - The handshake request.
  - Listener, route, RLS key-map and refusal cases; cluster and endpoint parsing.
- **Live results** (Linux aarch64, GCC, `-march=armv8-a+crc+aes`):
  - **Speed:** the first read, including discovery and ALTS, took 0.14–0.16 s.
  - **Correctness:** 50/50 random ranges were byte-identical to REST (sha256). A missing object gives status 5, not retryable.
  - **Peers:** only `34.126.8.41:<many ports>`, plus one handshaker connection on `:8080`.
- **Throughput:** 8 MiB ranges, MB/s at each number of reads in flight.

| ALTS connections | 4 in flight | 16 in flight | 64 in flight |
|---|---|---|---|
| 4 | 1418 | 2181 | 2294 |
| 8 (default) | 1328–1722 | 4260–4912 | 5104–5495 |
| 16 | 1486 | 5327 | 7035 |

- Each connection tops out at about 550 MB/s, because its one I/O thread both decrypts and parses. The best default for N is still for the engine ladder to decide (PERFNOTPOLICY). In the probe, 16 beats 8 by about 1.3x at 64 reads in flight.
- **Engine wiring (2026-11-01):**
  - `GCS_TRANSPORT=rest|grpc` and `GCS_GRPC_CONNECTIONS` (default 8) live in `opteryx/config.py`; an unknown value raises at import. `pool_reader.pyx` arms each scan's pipeline with `set_gcs_transport_grpc` at plan time.
  - In `io_pipeline.hpp`, all three gs:// range sites (data blocks, single ranges, bloom ranges) go through one `remote_get_many`.
  - `gcs_grpc::directpath_get_many` (`src/cpp/gcs_grpc/directpath_fetch.cpp`) gives REST's contract: HttpError, the scan's retry budget and the byte gate. `http_link` exposes the gate, timeout, backoff and retry counter from `http_client.cpp`, and the gate's link estimate is fed from the gRPC bytes.
  - The gRPC sources compile into `pool_reader.so` only. `_operators.so` resolves `directpath_get_many` from it at load (it is undefined there, defined in `pool_reader`), so the process has one client (R3).
  - **Not covered:** footers and HEAD (`filesystem.hpp`) stay on REST, and so do the vector-index build and the non-parquet readers. READ_* is unaffected, because anonymous paths are rewritten to https.
  - **Shortcut:** a retry re-reads the whole batch, not only the failed ranges.
- **Tests:** `tests/unit/connectors/parquet_io/test_gcs_transport_grpc.py`. A gs:// row group on an armed pipeline reaches DirectPath, which refuses a scan with no credential. The config refuses unknown values.
- **Cold ladder, 16/64 (2026-11-01).**
  - Setup: an n4a-standard-16 Debian 13 VM, deleted afterwards. A HEAD tree built with GCC 14 at `-march=armv8-a+crc+aes`. ClickBench hits (14.7 GB) in `gs://opteryx_data`, six queries (Q03, Q20, Q21, Q22, Q23, Q31), a fresh process per run, cold then hot, arm order rotating.
  - **Pass 1** (3 reps, the process's first query):

| arm | cold total | hot total |
|---|---|---|
| REST | 8.31 s | 1.83 s |
| grpc, 4 connections | 10.27 s (1.24x, slower) | 0.97x |
| grpc, 8 connections | 7.47 s (0.899x) | 0.98x |
| grpc, 16 connections | 7.47 s (0.898x) | 1.07x |

  - **Pass 2** (REST vs grpc8, 5 reps), with an identical warm-up query on another dataset first, so a long-lived worker's one-off connection setup is excluded:
    - cold total: REST 6.83 s, grpc8 6.04 s, **0.884x (11.6% faster)**;
    - hot: 0.948x;
    - the warm-up itself, which includes discovery and the handshakes: 1.12x;
    - per query: Q23 0.79x, Q21 0.84x, Q22 0.89x, Q31 0.94x, Q20 0.96x, Q03 0.96x.
  - **Gate: PASSED.** Cold improves by 11.6% at 16/64 and hot does not regress. The IO-heavy queries gain 16–21%.
  - **N default = 8.** 16 gives the same cold time but a worse hot one; 4 is slower than REST.
- **Still owed:** how often endpoints change, and Phase 3 (the fleet template flip, the architect's call).

**Phase 3: flip the fleet template to `grpc`.** Architect's call, with the numbers in front of them.

## 8. Expected effect
- **Phase 0 passes on throughput:** cold IO-bound queries gain up to the cold − hot gap. Realistically 10–20% of the cold total on 16/64 and larger, if a third to a half of the IO wait goes away.
- **Phase 0 passes on time to first byte only:** small-range jobs gain. Most of the ~30 ms is likely GCS's own storage backend, not the GFE, so expect tens of percent on those jobs, not multiples.
- **Hot reads, planning, decode, Cloud Run:** zero.
- **CPU:** no saving. ALTS is still AES-GCM, so client decryption cost stays the same; only the GFE hop goes away.

## 9. Rulings (2026-11-01)
- **R1 RULED:** vendor the ALTS crypto and record layer from `grpc/grpc` (Apache-2.0, about 1.3k lines) into `third_party/grpc_alts/`.
- **R2 RULED:** hand-written protobuf subset (C2). No upb.
- **R3 RULED:** a process-wide connection pool (§4, "Connections and threads").
- **R4 RULED:** `GCS_TRANSPORT` = `rest` | `grpc`, per process, default `rest` (off).
- **R5 RULED:** gRPC through the GFE ships only if we would ever use it. Today we wouldn't, so Phase 1's GFE path is test-only: tests construct the client against `storage.googleapis.com` over TLS directly, and `GCS_TRANSPORT=grpc` means DirectPath only.
- **R6 RULED:** `READ_*` and anonymous reads stay on REST.
- **R7 RULED (approved 2026-11-01):** the fleet VPC changes (routes and firewall to `34.126.0.0/18` and `2001:4860:8040::/42`, IPv6 on the subnet, MTU ≥ 4096). Billing checked 2026-11-01 against Google's pricing pages:
  - VPC firewall rules: "free of charge" (cloud.google.com/firewall/pricing).
  - External IPv6 on VMs and subnets: "You are not charged for external IPv6 address ranges that are assigned to subnets or for external IPv6 addresses that are assigned to VM instances" (cloud.google.com/vpc/network-pricing). Internal or external, there is no charge, so it doesn't matter which one DirectPath needs.
  - Data transfer: GCS → same-location Google Cloud service is free ("Data moves within the same location ... US-EAST1 to US-EAST1", cloud.google.com/storage/pricing). The bucket must be regional us-east1: "A region is not considered the same location as a multi-region."
  - Operations: the read is `storage.*.get`, Class B ($0.0004 per 1,000, regional Standard), the same row for JSON and gRPC. "Except as noted in the footnotes, each request is considered one operation." No footnote covers `BidiReadObject`, so whether a 100-range stream is billed once or per range is undocumented. Either way it is no more than today's one GET per range.
  - Routes and MTU: no price listed. The MTU change has an operational cost instead: "before you change the VPC network MTU, you _must_ first stop each compute instance" (docs.cloud.google.com/vpc/docs/mtu), so every VM on the fleet network has to stop.

## 10. Open unknowns: resolved 2026-11-01
Sources:
- the grpc/grpc master source, `src/core/tsi/alts/` and `src/core/resolver/google_c2p/`;
- a live xDS capture from the Go client (v1.70, grpc-go 1.84) on a us-east1-b n4a VM, deleted afterwards.

**ALTS handshake (C3)**
- **Handshaker service:** `metadata.google.internal.:8080`, reached over **plaintext h2c** (gRPC creates it with `grpc_insecure_credentials_create`).
  - Method: `/grpc.gcp.HandshakerService/DoHandshake`, a bidi stream.
  - The proto lives at `src/core/tsi/alts/handshaker/proto/handshaker.proto`.
- **Message fields we need:**
  - `HandshakerReq`: client_start=1, next=3.
  - `StartClientHandshakeReq`: handshake_security_protocol=1 (ALTS=2), application_protocols=2, record_protocols=3, target_identities=4, rpc_versions=9, target_name=8, max_frame_size=10.
  - `NextHandshakeMessageReq`: in_bytes=1.
  - `HandshakerResp`: out_frames=1, bytes_consumed=2, result=3, status=4.
  - `HandshakerResult`: application_protocol=1, record_protocol=2, key_data=3, peer_identity=4, max_frame_size=8.
  - `HandshakerStatus`: code=1, details=2.
  - `RpcProtocolVersions`: max=1, min=2; each `{major=1, minor=2}`.
- **Values:**
  - application protocol `"grpc"`
  - record protocol `"ALTSRP_GCM_AES128_REKEY"`
  - RPC versions max 2.1 and min 2.1
  - key_data is 44 bytes
- **Handshake loop:**
  1. Send `client_start` to the handshaker.
  2. Write `out_frames` to the backend socket.
  3. Pass the backend's bytes back to the handshaker as `next.in_bytes`.
  4. Repeat until `result` arrives.
  5. Any bytes after `bytes_consumed` in the last input are the first record-layer bytes.

**ALTS record layer (C3)**
- **Frame layout:** `len (u32 LE) | type = 6 (u32 LE) | ciphertext | 16-byte tag`. `len` counts everything after itself.
- **Frame size:** 16 KiB is the minimum and 1 MiB the maximum. The value used is `min(peer's max_frame_size, ours)`, floored at 16 KiB.
- **Nonce:** a 12-byte counter.
  - It increments little-endian over the low 8 bytes, which is the overflow limit for the rekeying protocol.
  - Byte 11 is 0x80 for frames the server seals and 0x00 for frames the client seals.
- **Rekeying key:**
  - key_data[0:32] is the KDF key and key_data[32:44] is the nonce mask.
  - AEAD key = `HMAC-SHA256(kdf_key, nonce[2:8] ‖ 0x01)[0:16]`, re-derived whenever nonce bytes 2–7 change, so at most 2^16 frames use one key.
  - The AEAD nonce is the counter XOR the nonce mask. The cipher is AES-128-GCM with a 16-byte tag.
- **R1 vendor set:**
  - `crypt/aes_gcm.cc`, `crypt/gsec.{h,cc}`, `frame_protector/{alts_counter,frame_handler}.*` and `zero_copy_frame_protector/alts_iovec_record_protocol.*`, about 2.8k lines with comments.
  - Their non-OpenSSL dependencies are `absl::Span`, `absl/log`, `gpr_malloc`/`gpr_zalloc`, `grpc_core::Crash` and the `tsi` result types. R1 already strips all of these.

**What `grpc_alts_is_running_on_gcp()` checks:** the BIOS product name file. It is only used for the error message; the config (R4) is explicit.

**Discovery (C4): it needs one more RPC than the design assumed.**
- **The real chain:**
  1. The metadata server gives the zone.
  2. One ADS stream (TLS + bearer) to `directpath-pa.googleapis.com:443`.
     - Node: `id = C2P-<random u64>`, `locality.zone`, client features `envoy.lb.does_not_support_overprovisioning`, `xds.config.resource-in-sotw`.
  3. LDS: `xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.listener.v3.Listener/storage.googleapis.com`.
  4. That gives an `ApiListener` → `HttpConnectionManager` with an **inline** `RouteConfiguration`, so there is **no RDS**.
  5. Its first route uses a **`clusterSpecifierPlugin` of type `grpc.lookup.v1.RouteLookupClusterSpecifier` (RLS)**:
     - lookup service `dns:///storage.googleapis.com`;
     - key builders for `google.storage.v2.Storage` with keys {host→`server`, service, method, header `x-goog-request-params`};
     - `maxAge` 300 s, `staleAge` 290 s;
     - default target `.../Cluster/google_cfe_storage.googleapis.com`, which is the GFE.
  6. A second route falls back to `.../Cluster/storage.googleapis.com/eds_cluster`.
  7. The client calls `/grpc.lookup.v1.RouteLookupService/RouteLookup` over TLS to `storage.googleapis.com`, with the key map {method, server, service, `x-goog-request-params: bucket=projects%2F_%2Fbuckets%2Fopteryx_data`}. For our bucket the answer was cluster `.../Cluster/gcs-grpc-prod-us-east1-default-default.googleapis.com/eds_cluster`.
  8. CDS for that cluster, then EDS (`ClusterLoadAssignment`) gives the endpoints: a handful of `34.126.x.x` IPs, each on many ports (`:22032`–`:22253`).
- **Load balancing:** Google's own policy is `weighted_round_robin` with ORCA backend-load weights (`blackoutPeriod` 10 s), wrapped in outlier detection.
- **Still unknown:** how often the endpoints change. The capture covered only ~2 s.
