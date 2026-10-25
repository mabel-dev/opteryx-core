# Remote Avro and ORC data files — `gs://` and `s3://` for catalog tables

Status: PHASE 1 BUILT 2026-10-25 (§11). R1–R7 accepted as recommended. Written at the architect's
request ("take remote files first … signed URLs vs an auth header … design doc before any code").
Repos: `opteryx-core` (the Sources, planning), `opteryx-iceberg` (nothing to change — see §2.5)
Companions: `ICEBERG_AVRO_ORC_DATA_FILES_DESIGN.md` (§4.4, D3), `ORC_READER_DESIGN.md`.

---

## 0. Why

D3 (2026-10-25) ruled Avro and ORC data files **local only**: a remote data file raises at plan time.
That was the right first step, because it let the whole path be built and tested against a local
warehouse. It is also the limit that matters most in practice: Iceberg tables overwhelmingly live on
object storage, so today an Iceberg table with Avro or ORC data files can only be read from a
laptop. This doc is the design that lifts the limit for `gs://` and `s3://`.

## 1. Scope

**In (phase 1):** catalog tables (the Iceberg shape) whose data files are `gs://` or `s3://`, for both
formats, read by the existing native Sources (`NativeAvroScanSource`, `NativeOrcScanSource`).

**Phase 2 (own decision, §7):** a *folder* of `.avro` / `.orc` files on `gs://` / `s3://`. It needs one
thing phase 1 does not: reading a file's schema at bind time without downloading the file.

**Out:** `http(s)://` data files (not a catalog scheme); Azure; vended or per-table credentials
(refused today everywhere, §2.5, and unchanged); ranged / partial reads (§8); writing.

## 2. What exists

### 2.1 The native client can already send a header

`HttpClient::get(url, headers, tuning)` (`src/cpp/http_client.hpp:173`) takes a header map. Both
Sources already own an `HttpClient` for remote files — but `NativeAvroScanSource::load` calls
`g.http->get(spec_->urls[i])` with **no headers**, and `NativeOrcScanSource` has no remote path at all
(it maps local files only). Errors from the client **redact the query string**
(`redact_url`, `http_client.cpp:45`), so a presigned URL's credential does not reach an exception.

### 2.2 Each filesystem already declares how a native read authenticates

The Parquet scan path settled this and states the rule (`pool_reader.pyx:2594`): *exactly one of* a
signed URL or an `Authorization` header authenticates a read — never both, never neither.

| | `signs_urls` | `native_auth_header()` | mechanism |
|---|---|---|---|
| **GCS** (`gcs_filesystem.py:140,161`) | False by default (`GCS_SIGN_URLS` opts in) | `"Bearer <token>"`, refreshed under a lock | one credential for the caller, covers every object |
| **S3** (`s3_filesystem.py:748,757`) | always True | always None | SigV4 signs the *request*, so no static header can authenticate two objects; presigning is local HMAC (microseconds) |

GCS signing is the expensive opt-in because on Compute Engine / Cloud Run it is an IAM `signBlob` round
trip, measured ~63 ms per object — ~6.3 s of overhead on a 100-file scan, repaid every query
(`gcs_filesystem.py:143-153`).

### 2.3 The filesystem for a catalog scan

`resolve_scan_filesystem(connector, paths)` (`parquet_read.pyx:39`): the connector's own `filesystem`
if it has one, else `create_filesystem(<scheme of the first path>)` — the platform's identity
(application-default GCS credentials; the AWS credential chain). It is already imported by the compiler
(`compiler.py:4102`).

### 2.4 There is a precedent for whole-object reads

`GcsFile` / `S3File` (`gcs_filesystem.py:50`, `s3_filesystem.py:576`) read one object whole with
`Authorization` (GCS) or a presigned URL (S3) **and `Accept-Encoding: identity`**, so GCS does not
decompress-transcode an object stored with `Content-Encoding: gzip`. This is exactly the read the Avro
and ORC Sources need, done natively.

### 2.5 Credentials are already the platform's, and anything else is refused

`opteryx-iceberg` reads every remote metadata file through `create_filesystem` with opteryx-core's own
credentials; where the table's properties name a different endpoint or access key, or carry a vended
GCS token, the read is **refused rather than silently served under a different identity**
(`fileio.py` docstring). Data files are read the same way by the Parquet scan. This design adds no
identity, and no `opteryx-iceberg` change.

### 2.6 What is not there

- `OpteryxGcsFileSystem` has no public `object_url(path)`: the gs:// → `https://storage.googleapis.com/…`
  rewrite is a function local to `get_file_info` (`gcs_filesystem.py:300`); the native Parquet pipeline
  does its own rewrite in C++ (`io_pipeline.hpp:2476`). Duplicating the rewrite is not acceptable.
- Both Avro / ORC read nodes refuse every `://` path (`native_file_locations` / `require_local_files`).
- The two Sources' specs have no header field.

## 3. Signed URL or auth header — the answer is "whichever the filesystem declares"

It is not a free choice, and making it one costs something either way:

| Option | GCS | S3 | verdict |
|---|---|---|---|
| **Always sign** | works; ~63 ms IAM round trip per file on Cloud Run (the production target) | the only thing that works | pays the measured GCS cost on every query |
| **Always send a header** | works; cheap | **impossible** (SigV4 is per request) | cannot be uniform |
| **Python callback per fetch** | — | — | Python on the execution path: CLAUDE.md §2 forbids it |
| **Follow the filesystem's own declaration** | bearer header (signing opt-in via `GCS_SIGN_URLS`) | presigned URL | **recommended** |

**R1 (recommended): the Sources follow `signs_urls` / `native_auth_header()` exactly as the Parquet
pipeline does.** A new filesystem gets correct behaviour by declaring its own mechanism; the Sources
have no knowledge of GCS or S3.

## 4. Design

### 4.1 Resolution happens at compile time, in Python, once per scan

`_compile_avro_scan` / `_compile_orc_scan` (planning-to-engine handoff; Python at plan time is
permitted) call one shared helper:

```python
native_locations(filesystem, files) -> (urls: list[str], auth_header: str)
```

- a path with no scheme → `url = ""` (local, memory-mapped, as today);
- a `gs://` / `s3://` path → `url` is `filesystem.rewrite_to_signed_url(path)` when
  `filesystem.signs_urls`, else `filesystem.object_url(path)` (a **new public method on the GCS
  filesystem**, the existing local function promoted — R5) with the single `auth_header` from
  `filesystem.native_auth_header()`;
- any other scheme (`s3a://`, `gcs://`, `http(s)://`, …) → `NotSupportedError` naming the file;
- (`Accept-Encoding: identity` is not the helper's business: the Sources' one fetch function sends it
  on every remote GET, §4.2.)

Compile time, not bind time or physical-planning time, so the token or signature is minted as close to
the first byte as the design allows, and never sits inside a plan object that could be held or cached.

### 4.2 The Sources

Both specs gain `auth_header` (one string; empty = none — one dataset has one scheme, and a manifest
with mixed schemes already raises `Mixed protocols in manifest`). `NativeOrcScanSource` gains `urls`
and an `HttpClient`, and its `open_file` follows the Avro one: local → mmap; remote → one `get` with
`{Authorization: <header>, Accept-Encoding: identity}` (or just the latter for a presigned URL).
`NativeAvroScanSource::load` passes the same headers. `READ_AVRO`'s anonymous and `credentials =>`
paths are unchanged: they pass an empty header and URLs the anonymous / signing filesystem built.

### 4.3 What the node shows

The header and URLs live only in the C++ spec held by `AvroScanPlan` / `OrcScanPlan`. They are not
attributes of `AvroReadNode` / `OrcReadNode`, so `EXPLAIN`, `plan_config`, `sensors` and telemetry
cannot print them. Error messages name the original `gs://…` path (already the case), never the URL.

### 4.4 Lifetime

The credential is minted at compile and used when a worker opens each file, which can be late in a long
scan. GCS bearer tokens last about an hour; S3 presigned URLs here are minted for 3600 s
(`_DEFAULT_EXPIRY_SECONDS`) and a temporary AWS session token can expire sooner. A scan that outlives
its credential gets a 401 / 403 on the next file open and **fails the query loudly**; nothing retries
with a refreshed credential (that would need Python on the execution path). Queries over a few
hundred MB take seconds, so this is a stated limit, not a defect; R4 asks whether it is acceptable.

### 4.5 Memory and speed (stated, not optimised)

A remote file is fetched whole and held for the life of its stripes / batches, as a local file is
mapped. The claim logic opens one file at a time and only when no ready file has work left, so
resident remote bytes are about one file (Iceberg's default target is 512 MiB) plus the file being
finished. The fetch is a single connection and is **not overlapped with decode** (the opening worker
fetches while the others wait). Per ruling, Avro/ORC performance is not a concern now; this is
recorded so nobody mistakes the shape for a design goal. Ranged reads (§8) are the way out.

### 4.6 The one-line trust statement

Catalog-supplied data paths are read with the platform's own identity, and are not scoped to the table's
location. That is already how the Parquet scan and all metadata reads behave; Avro and ORC inherit it,
they do not widen it. A catalog (or a manifest written by another engine) that names
`gs://some-other-bucket/…` is read if the platform identity can read it. R3 asks whether to close that
for all formats at once.

## 5. Errors (fail fast, no fallbacks)

Each names the dataset and the `gs://` / `s3://` path, and never the URL or credential:

- a scheme outside `gs` / `s3` (`s3a://`, `gcs://`, `http(s)://`);
- a filesystem that declares neither a signing hook nor a header while the path is remote;
- the GET failing: 401 / 403 (credential rejected or expired — say which), 404, timeout, connection
  error — from the client's own redacted message, logged in full (code 3 is already scrubbed to a
  path-free user message);
- a fetched body that is not an Avro / ORC file: the existing format errors, unchanged.

## 6. Tests

No live buckets (a full-suite run must never reach live services). A local `http.server` plays object
storage, and the filesystem factory is the seam:

- a fake filesystem class declaring `signs_urls = False`, `native_auth_header() -> "Bearer test"`,
  `object_url(path) -> http://127.0.0.1:<port>/…`; the server **rejects a request without the header**
  and serves the file — proves the header is attached and the body read, for Avro and ORC;
- the same with `signs_urls = True` and a `rewrite_to_signed_url` that appends a query the server
  checks — and an assertion that the query string appears nowhere in the raised error;
- an Iceberg table end to end: manifest data paths `gs://bucket/…`, `create_filesystem("gs")` patched to
  return the fake, using the existing `opteryx-iceberg/tests/_avro_data_files.py` and
  `_orc_data_files.py` fixtures (schema evolution, projection, filter, COUNT through the remote path);
- refusals: `s3a://`, a 403 (expired), a 404, and a mixed-scheme manifest;
- `Accept-Encoding: identity` present on every request;
- real GCS / S3 smoke only when the user authorises a live run.

## 7. Phase 2 — a folder dataset on `gs://` / `s3://`

`FileSystemTable` already lists a remote folder and builds its manifest. What fails today is the
*schema*: `_read_avro_schema` / `_read_orc_schema` call `open_input_file(...).memoryview`, which on a
remote filesystem **downloads the entire first file to read its header**. (Today that happens, and the
scan then refuses the path at compile — a confusing failure that phase 1 will turn into a working query
only if the schema read is fixed.) The fix is ranged reads through the filesystem's existing
`read_ranges(path, ranges)`: the ORC footer is the last 16 KiB plus one read if larger (the Parquet
footer reader at `filesystem_connector.py:_read_parquet_footer_envelope` is the model); the Avro header
is a prefix read, doubled until the header parses, with a cap. R6 asks whether phase 2 is wanted now.

## 8. Not in this design: ranged data reads

The way to stop downloading whole files is to read ranges — for ORC the stripe footers and only the
projected columns' streams; for Avro, blocks. It is also the largest piece (a streaming source,
read-ahead, byte budgets — the Parquet pipeline's `io_pipeline.hpp`), and under the functional-only
ruling it is not warranted. Recorded as the follow-up if remote Avro / ORC scans prove too slow or too
large.

## 9. Decisions

| # | Question | Recommendation |
|---|---|---|
| R1 | Signed URL vs auth header | **Follow each filesystem's own declaration** (§3): GCS bearer header (signing via `GCS_SIGN_URLS`), S3 presigned. No new policy. |
| R2 | Build order | Phase 1 (catalog tables) first and alone; phase 2 (folders) separately. |
| R3 | Scope catalog-supplied paths to the table's location, for **all** formats | **Not in this step**: it changes Parquet behaviour, and the posture is pre-existing. Raise it as its own item. |
| R4 | A scan that outlives its credential fails (401 / 403) rather than refreshing | **Accept** for now; refresh would need Python in the execution path. |
| R5 | Promote GCS's local `_object_url` to a public `object_url(path)` method | **Yes** — the alternative is duplicating the rewrite. A refactor of one function in one file. |
| R6 | Phase 2 now (ranged header / footer reads for remote folders) | **After phase 1 is working**; it is a different change (bind-time IO). |
| R7 | Whole-object GET, no overlap with decode (§4.5) | **Accept**: functional-only; ranged reads (§8) are the follow-up if needed. |

## 10. Build order (phase 1) — done, see §11

1. `OpteryxGcsFileSystem.object_url` (R5) + a unit test.
2. `native_locations` helper + tests with fake filesystems.
3. `AvroScanSpec` / `OrcScanSpec` `auth_header`; `OrcScanSpec` `urls` + `HttpClient`; Source `load` / `open_file`.
4. `AvroScanPlan` / `OrcScanPlan` constructors; the two `_compile_*_scan` call sites; drop the
   `://` refusal in `native_file_locations` / `require_local_files` for `gs` / `s3` only.
5. The end-to-end Iceberg tests of §6; `make q`, `make slt`; the design docs' D3 / D3b entries updated.

---

## 11. What was built (2026-10-25)

Phase 1, in the §10 order. R1–R7 accepted as recommended.

- `OpteryxGcsFileSystem.object_url(path)` (static): the rewrite `get_file_info` used to hold as a
  private inner function, now the one definition `get_file_info` calls (R5). Test in
  `tests/unit/connectors/test_gcs_filesystem.py`. (`read_ranges` and `GcsFile` still carry their own
  copies of the same two lines; left alone, not part of R5.)
- `opteryx/connectors/io_systems/native_locations.py`: `native_locations(filesystem, files, dataset)
  -> (urls, auth_header)` and `remote_files(files)`. Exactly one mechanism or an error; any scheme
  outside local / `gs` / `s3` is refused naming the file; a local+remote mix is refused; a remote
  filesystem declaring neither is refused. 9 tests with fake filesystems.
- `AvroScanSpec` / `OrcScanSpec` gain `auth_header`; `OrcScanSpec` gains `urls`; the ORC Source gains
  its remote path and `HttpClient`. **One shared fetch function** (`avro_detail::fetch_whole`) sends
  `Accept-Encoding: identity` and the `Authorization` value on every remote GET for both Sources
  (so `READ_AVRO`'s anonymous reads now send `identity` too, which is harmless).
- `AvroReadNode.native_scan_locations()` / `OrcReadNode.native_scan_locations()` resolve at compile
  time (`_compile_avro_scan` / `_compile_orc_scan`) through the platform filesystem
  (`resolve_scan_filesystem`). The old "remote is refused" check is gone for catalog tables.
- **Folder datasets stay local only, with a new, specific refusal** (§7): the code path would have
  made a remote folder "work" by downloading the whole first file to read its schema, which is not a
  surprise worth shipping. The node refuses it by name until phase 2.
- Tests: `opteryx-iceberg/tests/test_remote_avro_orc_data_files.py` (14: a local `http.server` as the
  store, refusing any unauthenticated request): bearer header and signed URL, each for Avro and ORC;
  a rejected credential fails the query and its secret appears nowhere in the error; a missing object;
  a filesystem declaring no credential never causes an anonymous read; `s3a://` and `gcs://` refused
  by name. The two older "remote refused at plan time" tests were deleted: they now reach the real
  credential flow, which a hermetic suite must not.

**S3 is verified against hadro** (mabel-dev/hadro, the repo's S3 test server: a read-only S3 over a
local directory that checks every request's SigV4 signature; `../hadro/src`, see
`tests/integration/test_s3_hadro.py`). `opteryx-iceberg/tests/test_remote_avro_orc_hadro.py` (12) runs
an Iceberg table with `s3://` Avro and ORC data files through the REAL `OpteryxS3FileSystem` (credential
chain, presigned URL) and the native Sources: results equal the same files read from local disk (the
oracle) under four queries; a wrong secret is refused by hadro's signature check and neither the
secret nor `X-Amz-Signature` appears in the error; a missing object fails the query. This was the
intended tool from the start and the first build above did not use it; the fake-store tests remain
because they are the only ones that exercise the bearer-header (GCS) mechanism.

Not verified: GCS. hadro is S3 only, and a real GCS object has not been read. The bearer-header path
is proven against the fake store; the filesystem's own `native_auth_header()` is what the Parquet scan
already relies on.
