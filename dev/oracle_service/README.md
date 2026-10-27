# Oracle service

A small stateless HTTP service for the rugo riscv64 release floor. POST the bytes of
a Parquet, ORC or Avro file; get back canonical JSON describing what an
**independent reader** sees. The riscv64 board has no PyArrow, so it writes files with
rugo, posts them here, and compares the answer with what rugo wrote.

Dev tooling: lives in `dev/`, is never packaged, and never imports `rugo`, `draken`
or `opteryx` (a test enforces it). This is the one place PyArrow is the intended
reader. No NumPy is imported.

| Format  | Independent readers                                                |
|---------|--------------------------------------------------------------------|
| Parquet | `pyarrow.parquet`, plus a minimal Thrift reader for page headers   |
| ORC     | `pyarrow.orc`                                                      |
| Avro    | `fastavro` **and** Apache's reference `avro`; they must agree      |

## Run

Needs `pyarrow`, `fastavro` and `avro` installed. Stdlib-only otherwise.

```bash
cd dev && python -m oracle_service --host 127.0.0.1 --port 8765
```

`--host` is the interface to bind (default `127.0.0.1`; the operator picks the LAN
address), `--max-body-mib` the request size limit (default 64). The package directory
is self-contained: copy `oracle_service/` to the host and run the same command next to it.

## API

| Request | Answer |
|---|---|
| `POST /v1/inspect?format=parquet\|orc\|avro`, body = raw file bytes, `Content-Length` required | 200 + the JSON below |
| `GET /healthz` | 200 `{"schema_version", "status": "ok", "max_body_bytes", "readers": {...versions}}` |

Every failure is a non-2xx with `{"schema_version", "error": {"kind", "message", "traceback"?}}`
and **no** partial description. The reader's own error text is in `message`.

| Status | `kind` | Cause |
|---|---|---|
| 400 | `bad_request` | missing / repeated / unknown `format`, empty or short body, bad `Content-Length` |
| 404 | `not_found` | any other path |
| 411 | `length_required` | no `Content-Length` (chunked uploads are not accepted) |
| 413 | `too_large` | body over the limit (rejected before it is read) |
| 422 | `reader_error` | a reader refused the file (corrupt, wrong `format`, codec a reader lacks…) |
| 422 | `unsupported` | a file feature the oracle will not describe rather than describe wrongly |
| 500 | `reader_disagreement` | fastavro and Apache avro saw different things |

## Client

`client.py` is stdlib-only (copy `oracle_service/__init__.py` + `client.py` to a host
without PyArrow):

```python
from oracle_service import client
doc = client.inspect_path("out.parquet", "parquet", "http://192.168.4.10:8765")
doc = client.inspect_bytes(data, "avro", base_url)
```

It raises `client.OracleClientError` (`.status` is the HTTP status, `None` if the
service could not be reached) for an unreachable service, a non-2xx answer, an
unknown `schema_version`, or a `sha256` that does not match the bytes sent. It never
skips and never falls back.

## Response

```
schema_version  int       currently 1; bumped on ANY change to the shape or the value encoding
format          "parquet" | "orc" | "avro"
input           {bytes, sha256}                       what the service received
reader          {library: version}                    the independent readers used
file            format-specific facts (below)
schema          {fields: [{name, nullable, type}], ...}
columns         [{name, values: [...]}]               ALL rows, column-wise, in file order
```

Column names may repeat in Parquet/ORC, so `columns` and `schema.fields` are ordered
lists, not objects.

* **Parquet** `file`: `num_rows, num_row_groups, num_columns, created_by, format_version,
  serialized_footer_size, key_value_metadata, parquet_schema` (the schema tree as text,
  with required/optional/repeated). `schema.fields` is the Arrow view;
  `schema.parquet_leaves` is, per leaf, `physical_type`, `logical_type` (Parquet's own
  JSON), `converted_type`, `type_length`, `max_definition_level`, `max_repetition_level`.
  `row_groups[]` → `{index, num_rows, total_byte_size, columns[]}` where each column
  chunk has `path, physical_type, compression, encodings, data_page_versions` (`[1]`,
  `[2]` or both), `pages[]` (type, offset, sizes, `num_values`, `encoding` — read from
  the page headers in the file bytes, since PyArrow does not expose them),
  `num_values, total_*_size, has_dictionary_page, has_column_index, has_offset_index,
  bloom_filter_*` and `statistics` = `{has_min_max, null_count, distinct_count,
  num_values, min_raw, max_raw, min, max}`. `min_raw`/`max_raw` are the stored physical
  value; `min`/`max` apply the logical type (decimal, unsigned, timestamp, time, date,
  string) and equal the raw form when the type has no logical reading.
  Note: if the file carries an `ARROW:schema` entry PyArrow restores the original
  Arrow types from it; that is visible in `key_value_metadata` and `schema.fields`.
* **ORC** `file`: `num_rows, num_stripes, compression, compression_block_size, writer,
  writer_version, file_version, software_version, row_index_stride, content_length,
  file_length, file_footer_length, file_postscript_length, stripe_statistics_*,
  user_metadata`; plus `stripes[] = {index, num_rows}`. PyArrow does not expose ORC
  column statistics or stripe offsets.
* **Avro** `file`: `num_rows, codec, schema_json` (verbatim from the header),
  `user_metadata, sync_marker`. `schema.fields[].type` is a tree of
  `{kind, nullable, logical_type?, precision?, scale?, unit?, timezone?, name?, size?,
  symbols?, element?, values?, fields?}`; a self-referencing record is
  `{"kind": "ref", "name": ...}`. Both readers' rows, codec and header metadata are
  compared; any difference is a 500 `reader_disagreement`. Avro logical types are
  applied by this service from the schema on **raw** values (the header is rewritten
  with `logicalType` removed, data blocks byte-for-byte, so neither reader turns
  decimals/timestamps into lossy Python objects); the original file is also read once
  by fastavro with normal logical decoding so invalid logical data is still rejected.
  Refused with 422 `unsupported`: a non-record top-level schema, and a union with more
  than one non-null branch (the readers do not say which branch a value took).
  A codec Apache avro cannot read (e.g. `xz`) is a 422 `reader_error`, not a fallback.

## Canonical value encoding (`schema_version` 1)

Values are encoded from the **schema**, never by guessing from the value. Nulls are
JSON `null` at every level. Nothing that JSON numbers could corrupt is a JSON number.

| Value | Encoding | Example |
|---|---|---|
| bool | JSON `true` / `false` | `true` |
| int8/16/32, uint8/16/32, Avro `int` | JSON number | `-128` |
| int64, Avro `long` | `i64:<decimal>` | `"i64:9007199254740993"` |
| uint64 | `u64:<decimal>` | `"u64:18446744073709551615"` |
| float16/32/64, Avro `float`/`double` | `f16\|f32\|f64:<hex bit pattern>:<readable>` | `"f64:7ff8000000000000:nan"`, `"f32:80000000:-0.0"`, `"f64:fff0000000000000:-inf"`, `"f32:3f8ccccd:1.1"` |
| DECIMAL (any width), Avro decimal | `dec:<exact decimal>` with exactly `scale` fractional digits | `"dec:-0.01"`, `"dec:1.50"` |
| TIMESTAMP | `ts[<unit>]:<int since epoch>`; unit `s\|ms\|us\|ns` | `"ts[ns]:4611686018427387904"` |
| DATE | `date32:<days>` / `date64:<ms>` | `"date32:18263"` |
| TIME | `time[<unit>]:<int since midnight>` | `"time[us]:86399999999"` |
| DURATION | `dur[<unit>]:<int>` | `"dur[us]:5"` |
| BINARY, fixed, FIXED_SIZE_BINARY | `b64:<base64>` | `"b64:AP8="`, `"b64:"` |
| string, Avro enum / uuid | plain JSON string | `"héllo"` |
| list, fixed_size_list, array | JSON array | `[1, null, 3]` |
| struct, Avro record | JSON object keyed by field name (duplicate field names are refused) | `{"x": "i64:1"}` |
| map | array of `[key, value]` pairs, in file order | `[["a", 1]]` |

* The **hex bit pattern is authoritative**; the readable text is for people. NaN,
  +inf, −inf and −0.0 are therefore distinct and explicit. NaN payload bits are
  reported as read; a signalling NaN may be quietened by a reader that widens a
  32-bit float to a Python float (Avro).
* The timezone / local-ness of a timestamp is in `schema` (`timezone`), not in the value.
* Dictionary-encoded columns are decoded to their values.
* An Avro logical type the service does not interpret is reported with its underlying
  physical encoding and its `logical_type` name in `schema`.
* Arrow types outside the table (unions, intervals, run-end/view lists) are refused
  with 422 `unsupported`.

## Response example (Avro, 2 rows)

```json
{"schema_version": 1, "format": "avro",
 "input": {"bytes": 326, "sha256": "d77463e7…"},
 "reader": {"fastavro": "1.13.1", "avro": "1.12.2"},
 "file": {"num_rows": 2, "codec": "deflate", "schema_json": "{…}", "user_metadata": {}, "sync_marker": "b64:K7j8gTf/a0HnKDHrtOfgcA=="},
 "schema": {"record": "Ex", "fields": [
   {"name": "id", "type": {"kind": "long", "nullable": true}},
   {"name": "x", "type": {"kind": "double", "nullable": false}},
   {"name": "price", "type": {"kind": "bytes", "nullable": true, "logical_type": "decimal", "precision": 10, "scale": 2}}]},
 "columns": [
   {"name": "id", "values": ["i64:9007199254740993", null]},
   {"name": "x", "values": ["f64:7ff8000000000000:nan", "f64:8000000000000000:-0.0"]},
   {"name": "price", "values": ["dec:-1.50", null]}]}
```

## Tests

```bash
python -m pytest tests/dev_oracle_service/test_oracle_service.py -q
```

Starts the service on an ephemeral localhost port. PyArrow and fastavro write the
files; expected values are literals or struct/integer arithmetic in the test, never
the service's own encoder.
