# Anonymous remote reads — listing for `READ_*`, and `READ_ORC`

Status: BUILT 2026-10-25 (§6); A1–A7 accepted as recommended, including `READ_ORC` (reverses ORC design D1).
Written after two architect rulings:

1. **Anonymous reads are allowed** — including the *listing* of a public bucket's prefix.
2. **The system's credentials are never used for a `READ_*` request.** Not as a fallback, not to
   "escalate" a listing the anonymous request was denied, not for any reason.

(The trigger: a public bucket `gs://opteryx/formats/{avro,csv,jsonl,orc,parquet}/` holding
`space_missions` in five formats. Four read anonymously today with `READ_*('gs://…/file')`; the
folder, and ORC at all, cannot.)

Repos: `opteryx-core`. Related: `REMOTE_AVRO_ORC_DATA_FILES_DESIGN.md` (catalog tables, platform
credentials, a different and operator-controlled route), `ORC_READER_DESIGN.md` (D1).

---

## 0. The invariant, stated so it can be tested

> A `READ_*` function call is served by a filesystem object that **has no code path to the platform's
> credentials**: it never constructs `OpteryxGcsFileSystem` / `OpteryxS3FileSystem`, never calls
> `get_storage_credentials`, `CredentialChain.resolve`, `_bearer` or `presign`/`rewrite_to_signed_url`
> of those classes. Its requests carry no `Authorization` header and no SigV4 query parameters, and a
> denial (401 / 403) ends the query with an error — it is never retried with a credential.

The only credentialed `READ_*` form is the existing, explicit `credentials => '<workspace>.<name>'`
option (a *customer's* stored secret, scope-checked), which this design leaves untouched.

## 1. What exists

- `AnonymousGcsFileSystem` / `AnonymousS3FileSystem` (`io_systems/anonymous_*_filesystem.py`) already
  carry the invariant for **single objects**: a path becomes its public `https://` object URL, fetched by
  `OpteryxHttpFileSystem` with no header; the store's own ACL decides. Their docstrings give the
  reason for the rule (an IDOR: SQL text naming an attacker-chosen bucket, read with the process's
  identity).
- **They deliberately have no `list_files`**, and every `READ_*` binder branch refuses a glob on
  `gs://` / `s3://` (`dataset.py`: ~8 sites, e.g. `:680`, `:950`, `:1090`, `:1318`). The stated reason is
  that *listing* is a separate IAM permission from object GET and "is not assumed granted anonymously",
  so the code refuses rather than escalate to an authenticated listing. That is rule 2, applied
  conservatively. Ruling 1 relaxes it: listing may be *tried anonymously*.
- A listing needs no credential on a public bucket. Probed 2026-10-25 against `gs://opteryx` with no
  credentials: the JSON API (`GET storage/v1/b/opteryx/o?prefix=formats/orc/`) and the XML API both
  answer 200 with the objects and sizes. The listing also contains a **zero-byte placeholder object
  named `formats/orc/`** (a console-created "folder"), which a listing must not return as a data file.
- `OpteryxGcsFileSystem.list_file_infos` already implements the listing (JSON API, `maxResults`
  paging, a trailing `/` on the prefix so `space_missions` does not also match `space_missions_backup`,
  fully-schemed `gs://` results, sizes carried). It adds a bearer header; nothing else is
  credential-specific.
- `READ_ORC` does not exist (ORC design D1: "no, in this step"). So an anonymous ORC object has no way
  into SQL at all today.
- The catalog / workspace route (`register_workspace(...)` + `FileSystemTable`) uses platform
  credentials by design — an *operator* chose the bucket — and is unchanged. `FROM 'gs://…'` stays
  invalid: `connectors/__init__.py` documents that a bucket URL is not a relation name, and an
  unregistered string falls to the local-disk slot (which is why that query said "not found").
- `tests/unit/connectors/test_read_jsonl_gcs_denied.py` and `tests/integration/test_s3_hadro.py`
  (`test_read_functions_never_sign`, `test_read_functions_refuse_globs`) pin today's behaviour, including
  the refusal that ruling 1 reverses.

## 2. Design

### 2.1 Anonymous listing (the change ruling 1 asks for)

Add `list_files(base_dir, recursive=True)` to `AnonymousGcsFileSystem` and `AnonymousS3FileSystem`:

- **GCS:** the JSON API listing `OpteryxGcsFileSystem.list_file_infos` uses, **without** the
  `Authorization` header. The listing loop moves to one shared function that takes the request headers
  as an argument; the credentialed class passes its bearer, the anonymous class passes `{}`. (One
  implementation of pagination and of the trailing-`/` rule; the *anonymous class still contains no
  reference to credential code* — the function takes headers, it does not fetch them.)
- **S3:** unsigned ListObjectsV2 (`list-type=2`, `prefix`, continuation tokens) against the same
  endpoint resolution the object URLs already use (`endpoint_parts`).
- Results are fully-schemed paths, as today; **placeholder objects (names ending `/`) are dropped**.
- **A denied listing** (401 / 403, or an S3 `AccessDenied`) raises `DatasetReadError` naming the
  bucket and saying the bucket does not allow anonymous listing, so the file must be named exactly.
  There is **no retry with credentials**. A 404 is "no such bucket".

The binder then drops its eight "glob patterns are not supported for gs:// / s3://" refusals for the
anonymous branches and calls the existing `_resolve_glob_files(path, anonymous_filesystem)`, which
already uses `fnmatch` over `list_files(base_dir, recursive=True)`. The misleading hint in those
messages ("or read the whole prefix without a wildcard") goes: no such form exists.

The `credentials =>` branches already list through their scoped filesystem and are untouched.

### 2.2 `READ_ORC`

The Avro, JSONL, Parquet and CSV readers each have a `READ_*` function; ORC is the odd one out, and
the public ORC folder cannot be reached without one. Add `READ_ORC('path-or-glob'[, credentials => …])`
shaped exactly like `READ_AVRO`:

- binder: scheme rules, credential/anonymous filesystem choice, glob → file list, schema from the
  **first file's footer** (`orc_io.header_schema`), columns matched by name (`match_by_id` false);
- `OrcReadNode` is already the planning node for a Scan; it gains the READ_AVRO-style construction
  (files, credentialed filesystem) so one node serves both, with `native_scan_locations` taking the
  anonymous branch: the anonymous filesystem's public URL, **empty `auth_header`**;
- the native `NativeOrcScanSource` already fetches a remote file (phase 1) with the headers it is given;
- wiring at the same ~10 touch points READ_AVRO has (function table, logical-plan renderer,
  projection pushdown, plan-step fields, physical planner).

Same option set as READ_AVRO (`credentials` only). Nested columns are refused at bind time by
`header_schema`, as for a folder.

**This reverses ORC design D1.** It is the architect's call (§4, A2).

### 2.3 Bind-time schema read downloads the first file

`READ_AVRO` and `READ_ORC` read the first file whole at bind to get the schema, and the scan then
fetches every file whole. For `READ_*` the cost is one extra copy of the first file. Under the
functional-only ruling that is accepted; ranged header / footer reads are the optimisation if it is
ever worth it (the footer is the last 16 KiB; the Avro header is a prefix).

### 2.4 What does not change

`FROM 'gs://…'` stays invalid; registered workspaces keep their credentials and listing; the catalog
route of phase 1 is unchanged. A public bucket as a *named workspace* (an operator-registered
anonymous connector) is not needed once `READ_*` can list, and is left out (A4).

## 3. Tests

The invariant is tested **structurally**, not by hoping a request looks anonymous:

1. **Tripwire.** For every `READ_*` (JSONL, PARQUET, CSV, AVRO, ORC) × `gs://` and `s3://` × {exact file,
   glob}: monkeypatch `OpteryxGcsFileSystem.__init__`, `OpteryxS3FileSystem.__init__`,
   `get_storage_credentials` and `CredentialChain.resolve` to raise `AssertionError`, put valid-looking
   platform credentials in the environment, and run the query. It must succeed or fail *without*
   tripping any of them.
2. **On the wire.** A recording local object store (the pattern in
   `opteryx-iceberg/tests/test_remote_avro_orc_data_files.py`) and hadro: every request — object GETs
   *and* listings — has no `Authorization` header and no `X-Amz-*` query. Against the
   signature-checking hadro with valid keys in the environment, a `READ_*` glob is **refused**
   (anonymous listing denied → the error of §2.1), not served.
3. **Behaviour.** Against the open hadro and the local store: a glob over `s3://` / `gs://` lists, skips
   a `folder/` placeholder, pages past 1,000 objects, matches by `fnmatch`, and reads all matched files
   (five formats). A denied listing says so and names the bucket. A prefix that is a sibling
   (`space_missions` vs `space_missions_backup`) is not matched.
4. **The existing tests that pin the old refusal are updated, not deleted:** `test_read_functions_refuse_globs`
   becomes the listing tests above; `test_read_functions_never_sign` and the GCS-denied tests stay and
   gain the tripwire.
5. **Live (only when authorised):** the five `gs://opteryx/formats/*` objects and the `formats/orc/`
   listing, anonymously.

## 4. Decisions

| # | Question | Recommendation |
|---|---|---|
| A1 | Anonymous listing for `READ_*` globs on `gs://` and `s3://` (ruling 1) | **Yes**, tried anonymously, denial is an error, never escalated (ruling 2). |
| A2 | Add `READ_ORC` (reverses ORC design D1) | **Yes** — without it an anonymous ORC object cannot be read from SQL at all. |
| A3 | Cap the number of objects one glob listing may return (SQL text can point it at any public bucket) | **Yes**: a hard cap (suggest 10,000, one config value), exceeded → error naming the cap. Without it a single query can page a multi-million-object bucket. |
| A4 | An anonymous operator-registered workspace | **Not now**; `READ_*` listing covers the need. |
| A5 | Schema read downloads the first file (§2.3) | **Accept**; ranged reads are the follow-up if needed. |
| A6 | One shared listing function taking headers (§2.1) | **Yes**: one pagination implementation, and the anonymous class still holds no credential code. |
| A7 | `READ_*` still has no listing for hosts other than `gs`/`s3` | Unchanged (`http(s)` has no directory to list). |

## 5. Build order (after the rulings)

1. Shared listing function; `list_files` on the two anonymous filesystems; unit tests incl. paging,
   placeholder, sibling prefix, denied listing, the cap.
2. Binder: drop the eight refusals for the anonymous branches; invariant tripwire tests for the existing
   four readers (they should already pass for exact files).
3. `READ_ORC`: binder branch, node construction, compiler lowering, the ~10 wiring points; tests
   mirroring `test_read_avro.py` plus the hadro / local-store cases.
4. Update the pinned tests (§3.4); `make q`, `make slt`.
5. With authorisation: the live read of the public bucket's five formats.

---

## 6. What was built (2026-10-25)

Steps 1–4 of §5; step 5 (the live read of the public bucket) is recorded at the end.

- `io_systems/object_listing.py` — **the one listing implementation, credential-free**:
  `list_gcs_objects(get, base_dir, recursive, headers, anonymous, limit, api)` and
  `list_s3_objects(get, page_url, ...)`, plus `anonymous_listing_limit()` (setting
  `READ_FUNCTION_LISTING_LIMIT`, default 10,000, a non-positive value raises) and `rfc3339_to_ns`
  (moved from `gcs_filesystem`). `OpteryxGcsFileSystem.list_file_infos` and
  `OpteryxS3FileSystem.list_file_infos` are now one call into it (the bearer / the presigner passed in).
- `AnonymousGcsFileSystem` / `AnonymousS3FileSystem` gain `list_file_infos` / `list_files`. **Each
  passes the shared function a `get` that calls the client with no headers and raises if given any**,
  so the "the fake client's `.get(url)` cannot carry a header" property the existing tests rely on is
  kept. A 401 / 403 raises `DatasetReadError` ("does not allow anonymous listing … Name the file
  exactly instead"); there is no retry. Placeholder objects are dropped; the prefix is a directory.
- `planner/binder/dataset.py`: the eight "glob patterns are not supported for gs:// / s3://" refusals
  are gone. The front half READ_AVRO and READ_ORC share (path argument, scheme rules, filesystem
  choice, glob → files, credential scope) is one helper, `_bare_reader_source`; READ_AVRO now calls it
  too (deliberate: copying 90 lines was the alternative). A new `READ_ORC` branch: schema from the first
  file's footer, columns by name, `credentials` the only option.
- `native_locations.function_scan_urls(function, path, files, credentialed_filesystem)` — **the rule in
  one function**: local → ""; http(s) → as given; gs / s3 → the anonymous filesystem's public URL, no
  header; only `credentials =>` signs, with the customer's secret. `AvroReadNode.native_file_locations`
  and the new `OrcReadNode` function-scan branch both call it. (This was needed: the first `OrcReadNode`
  reached `resolve_scan_filesystem`, which builds the *platform* filesystem for a connector-less node —
  exactly what must not happen to a `READ_*`. The tripwire test would have caught it.)
- `FunctionDatasetStep` gains `orc_credentialed_filesystem`, `orc_files`, `orc_physical_by_identity`,
  `orc_physical_columns`, `orc_reader_columns` (all seven places the `avro_*` fields appear), and
  `READ_ORC` is wired through the logical planner, renderer, projection pushdown, physical planner and
  `_compile_orc_scan`; `OrcReadNode` takes `orc_credentialed_filesystem` and `orc_function_scan`.
- Generator text only: `reference/clauses_catalog.py` names READ_ORC beside the other readers (the JSON
  is regenerated by the pre-commit hook).

### 6.1 Tests

- `tests/unit/connectors/test_read_functions_never_use_system_credentials.py` (24): the invariant. Every
  READ_* (PARQUET, AVRO, JSONL, CSV, ORC) × gs:// and s3:// × exact file and glob, with
  `OpteryxGcsFileSystem.__init__`, `get_storage_credentials`, `OpteryxS3FileSystem.__init__` and
  `CredentialChain.resolve` rigged to raise and platform-looking credentials in the environment; and the
  recording object store asserts no `Authorization`, no `X-Amz-*`, no SigV4 query on **any** request,
  listings included. A denied listing is one request, then an error. A sibling prefix and a folder
  placeholder are not read.
- `test_object_listing.py` (18): paging, placeholders, directory prefix, delimiter, headers exactly as
  supplied, denial (anonymous message / credentialed message), the cap, the setting, and both anonymous
  classes against the recording store.
- `test_read_orc.py` (14); `tests/integration/test_s3_hadro.py` gains globs listed anonymously against the
  open hadro (equal to local disk) and "a denied anonymous listing is never retried with the platform's
  credentials" against the signature-checking one.
- **Pinned tests updated, not deleted:** `test_read_functions_refuse_globs` (hadro) became the two tests
  above; `test_read_jsonl_gcs_glob_is_rejected_before_any_network_call` became "listed anonymously" and
  "denied listing fails loud with one request"; `test_no_secret_is_inferred_from_the_path` — which, with the
  refusal gone, **made a real anonymous request to storage.googleapis.com from the test suite** — now
  points the listing at a closed local port and asserts the secret resolver is never consulted; two
  `READ_AVRO` glob-refusal rows were removed from `test_argument_refusals`.

### 6.2 Found, not fixed

Three tests fail in the current tree through code this work does not touch (their assertions expect
message text the native error scrub, `build_terminal_exc` code 3 and the Parquet footer-error rewrite,
no longer produces): `test_read_avro.py::test_glob_file_that_cannot_resolve_fails_naming_it`,
`::test_corrupt_file_fails_naming_it`, and `test_s3_hadro.py::test_catalog_scan_signs_with_platform_credentials`
(`"403" in message`). Not checked against a clean checkout, which this session cannot make.

### 6.3 Live (2026-10-25, anonymous, read-only)

`gs://opteryx/formats/{parquet,avro,jsonl,csv,orc}/*.<ext>` read with `READ_PARQUET`, `READ_AVRO`,
`READ_JSONL`, `READ_CSV` and `READ_ORC`, with `OpteryxGcsFileSystem.__init__`, `get_storage_credentials`
and `OpteryxS3FileSystem.__init__` rigged to raise: all five returned exactly the local file's seven
aggregates (4,630 rows; 1,250 priced; `SUM(Price)` 137,224.45; 62 companies; 4,556 missions), in 1.0–2.1 s
each, through their native Sources (CSV has none). `READ_ORC('gs://opteryx/formats/*/*.orc')` also works.
The bucket allows anonymous listing (JSON and XML APIs); a bucket that does not fails the query saying so.
