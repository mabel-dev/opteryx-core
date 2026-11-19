# @opteryx/rugo

> **Alpha.** Published under the `alpha` dist-tag. The API, the metadata shapes and the value mapping may change between releases.

[Rugo](https://rugo.dev)'s Parquet, ORC and Avro readers, compiled to WebAssembly from the same C++ that ships in
the `rugo` Python wheel. Works in the browser and in Node.

- Browser and Node (>= 24), ESM only, zero dependencies
- ~358 KB gzipped wasm, single-threaded (no COOP/COEP headers needed)
- **Parquet** reads only the byte ranges it needs: the footer (usually one read of the last 64 KB), then one read
  per requested column chunk. Nothing else of the file is fetched.
- **ORC and Avro** read the **whole file** into memory in one read: the ORC reader addresses the whole file, and
  Avro has no index. Files of 4 GB or more are refused; in practice the limit is about 2 GB.

## Install

```bash
npm install @opteryx/rugo@alpha
```

## Use

Every reader takes a source: `blobSource(file)` for a local `Blob`/`File` (via `Blob.slice()`), or
`await httpSource(url)` for a remote file (the server must honour `Range` requests). Any object with `size` and
`read(offset, length) -> Promise<Uint8Array>` works too.

```js
import { blobSource, httpSource, openAvro, openOrc, openParquet, readFooter } from "@opteryx/rugo";

// Parquet footer only
const footer = await readFooter(blobSource(file));
console.log(footer.num_rows, footer.schema.map((c) => `${c.name}: ${c.logical_type || c.physical_type}`));

// Parquet values, one row group at a time
const pq = await openParquet(await httpSource("https://example.com/data.parquet"));
try {
  const { num_rows, columns } = await pq.readRowGroup(0, ["id", "name"]);  // omit the list for all columns
  console.log(num_rows, columns.id[0], columns.name[0]);
} finally {
  pq.close();  // frees the file's state in the wasm heap
}

// ORC, one stripe at a time
const orc = await openOrc(blobSource(file));
try {
  console.log(orc.meta.stripes, orc.meta.columns.filter((c) => !c.readable));  // unreadable columns say why
  const { columns } = await orc.readStripe(0);
} finally {
  orc.close();
}

// Avro, one batch (whole blocks, up to 65,536 rows) at a time; dotted paths reach into records
const avro = await openAvro(blobSource(file));
try {
  const { columns } = await avro.readBatch(0, ["Rocket.Name"]);
} finally {
  avro.close();
}
```

## Values

Each column is an array of values, with `null` for nulls:

| Column type | JS value |
|---|---|
| 8/16/32-bit ints, floats | `number` |
| dates, 32-bit times | `number` (stored ints: days, time units) |
| int64, uint64, timestamps, 64-bit times | `bigint` (stored ints; the type gives the unit) |
| bool | `boolean` |
| varchar / string, enum | `string` |
| binary, varbinary | `Uint8Array` |
| decimal | `string`, scaled |
| uuid | hyphenated hex `string` |
| interval | `{ months: bigint, us: bigint }` |
| list / array | nested arrays, any depth: `null` for a null list, `[]` for an empty one, `null` elements allowed |
| ORC struct / map, Avro record / map | JSON text `string`, as the native readers render them |

Parquet list elements are narrower: ints, floats, bools and strings only. String elements are `string` for
`array<varchar>` and `Uint8Array` otherwise; decimal, uuid and interval elements are not decoded inside Parquet lists.

## Errors

Everything rejects with an `Error` rather than returning partial results:
- a malformed or wrong-format file
- a server that ignores `Range`
- an out-of-range row group, stripe or batch
- an unknown column
- a read after `close()`
- any native decode refusal (the message is passed through)

## Versioning

Versions match the `rugo` Python package: `@opteryx/rugo@0.5.12` is built from the same source as `rugo==0.5.12`.

## License

Apache-2.0
