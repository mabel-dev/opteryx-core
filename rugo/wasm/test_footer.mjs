// CI gate for the published bundle: Parquet footer read and column decode through both
// byte paths, ORC and Avro whole-file decode, on committed files, checked against native
// rugo's figures for them.
// Usage: node test_footer.mjs   (after ./build.sh)
import assert from "node:assert";
import { spawn } from "node:child_process";
import { openAsBlob } from "node:fs";
import { createInterface } from "node:readline";
import { blobSource, httpSource, openAvro, openOrc, openParquet, readFooter } from "./rugo_footer.mjs";

const FILE = new URL("../../testdata/parquet_tests/delta_length_byte_array.parquet", import.meta.url).pathname;
// native: rugo.parquet.read_metadata(FILE) -> 1000 rows, 1 row group, FRUIT byte_array/varchar
function check(fs, how) {
  assert.equal(fs.num_rows, 1000, how);
  assert.equal(fs.num_row_groups, 1, how);
  assert.deepEqual(fs.schema.map((c) => [c.name, c.physical_type, c.logical_type]), [["FRUIT", "byte_array", "varchar"]], how);
  assert.equal(fs.row_groups[0].columns[0].num_values, 1000, how);
  console.log(how, "ok");
}

// native: rugo.parquet.read_parquet(FILE) FRUIT -> 1000 distinct, no nulls
async function checkRows(src, how) {
  const f = await openParquet(src);
  const before = src.reads.length;
  const { num_rows, columns } = await f.readRowGroup(0);
  f.close();
  const v = columns.FRUIT;
  assert.equal(num_rows, 1000, how);
  assert.equal(v.length, 1000, how);
  assert.deepEqual(v.slice(0, 3), ["apple_banana_mango0", "apple_banana_mango1", "apple_banana_mango4"], how);
  assert.equal(v[999], "apple_banana_mango998001", how);
  assert.equal(new Set(v).size, 1000, how);
  assert.equal(src.reads.length - before, 1, `${how}: one read per column chunk`);
  console.log(how, "rows ok");
}

check(await readFooter(blobSource(await openAsBlob(FILE))), "blob-slice");
await checkRows(blobSource(await openAsBlob(FILE)), "blob-slice");

// zstd + ALP floats, 5 row groups. native: row group 4, float_plain[0] and double_plain[2]
{
  const ALP = new URL("../../testdata/parquet_tests/alp_extended.zstd.parquet", import.meta.url).pathname;
  const f = await openParquet(blobSource(await openAsBlob(ALP)));
  const { columns } = await f.readRowGroup(4, ["float_plain", "double_plain"]);
  assert.equal(columns.float_plain[0], -7.999999874453996e18);
  assert.equal(columns.double_plain[2], 3.217561424067181e18);
  await assert.rejects(f.readRowGroup(9), /out of range/);
  await assert.rejects(f.readRowGroup(0, ["nope"]), /no column 'nope'/);
  f.close();
  console.log("zstd/alp rows ok");
}

// LIST columns (dev/generate_wasm_list_fixture.py), row group 2. native: rugo.parquet.read_parquet
{
  const LISTS = new URL("../../testdata/parquet_tests/wasm_lists.parquet", import.meta.url).pathname;
  const f = await openParquet(blobSource(await openAsBlob(LISTS)));
  const { columns: c } = await f.readRowGroup(2);
  f.close();
  const head = (name) => c[name].slice(0, 5);
  assert.deepEqual(head("ints"), [[2000, null, -2000], null, [], [2003], [2004, null, -2004]]);
  assert.deepEqual(head("u64"), [null, [2n ** 64n - 1n, 2001n], [], null, [2n ** 64n - 1n, 2004n]]);
  assert.deepEqual(head("bools"), [[true, null, false], [], null, [false], [true, null, false]]);
  assert.deepEqual(head("tags_req"), [null, ["x", "y1"], [], null, ["x", "y4"]]);
  assert.deepEqual(head("nested"), [[[2000n, null], null, []], null, [], [[1n], [2n, 3n]], [[2004n, null], null, []]]);
  assert.deepEqual(head("deep"), [[[[]]], [[["p", null]], [null, []]], null, [[[]]], [[["p", null]], [null, []]]]);
  assert.deepEqual(head("blobs"), [[], [new Uint8Array([0, 255]), null], null, [], [new Uint8Array([0, 255]), null]]);
  assert.equal(c.tags.length, 1000);
  console.log("list rows ok");
}

// ORC (dev/generate_wasm_orc_fixture.py): row i is id i, name "name-<i%97>" (+ a tail when
// i%5 == 0), dec (37i - 90000)/100, kv {"a":i,"b":-i}; every column null when i%7 == 3.
{
  const orcFile = (c) => new URL(`../../testdata/orc_tests/wasm_types.${c}.orc`, import.meta.url).pathname;
  const f = await openOrc(blobSource(await openAsBlob(orcFile("uncompressed"))));
  assert.deepEqual(f.meta.stripes, [1000, 1000, 1000, 1000, 1000]);
  const { num_rows, columns: c } = await f.readStripe(4, ["id", "name", "dec", "kv", "pt", "tags"]);
  assert.equal(num_rows, 1000);
  assert.deepEqual(c.id.slice(998), [4998n, 4999n]);
  assert.deepEqual(c.name.slice(998), ["name-51", "name-52"]);
  assert.deepEqual(c.dec.slice(998), ["949.26", "949.63"]);
  assert.deepEqual(c.kv.slice(998), ['{"a":4998,"b":-4998}', '{"a":4999,"b":-4999}']);
  assert.deepEqual(c.pt.slice(0, 4), [null, '{"x":4001,"y":"y1"}', '{"x":4002,"y":"y2"}', '{"x":4003,"y":"y3"}']);
  assert.deepEqual(c.tags.slice(0, 2), [[[]], [["t2"], []]]);
  await assert.rejects(f.readStripe(5), /out of range/);
  f.close();
  for (const codec of ["zlib", "snappy", "lz4", "zstd"]) {
    const g = await openOrc(blobSource(await openAsBlob(orcFile(codec))));
    const { columns } = await g.readStripe(0, ["id", "name", "dec"]);
    g.close();
    assert.deepEqual([columns.id[4999], columns.name[4999], columns.dec[4999]], [4999n, "name-52", "949.63"], codec);
  }
  console.log("orc rows ok");
}

// Avro: deflate (space_missions), zstandard and bzip2. native: rugo.avro.read_avro
{
  const avro = async (rel) => openAvro(blobSource(await openAsBlob(new URL(`../../testdata/avro/${rel}`, import.meta.url).pathname)));
  let f = await avro("space_missions.avro");
  assert.equal(f.meta.codec, "deflate");
  assert.equal(f.meta.num_rows, 4630);
  let { columns: c } = await f.readBatch(0, ["Company", "Rocket.Name", "Price"]);
  assert.deepEqual(c.Company.slice(0, 3), ["RVSN USSR", "RVSN USSR", "US Navy"]);
  assert.deepEqual(c["Rocket.Name"].slice(0, 3), ["Sputnik 8K71PS", "Sputnik 8K71PS", "Vanguard"]);
  assert.deepEqual(c.Price.slice(0, 3), [null, null, null]);
  assert.equal(c.Price[4629], 29.75);
  await assert.rejects(f.readBatch(0, ["NoSuchField"]), /NoSuchField/);
  f.close();
  f = await avro("apache/data/weather-zstd.avro");
  ({ columns: c } = await f.readBatch(0));
  f.close();
  assert.deepEqual([c.station.slice(0, 2), c.time.slice(0, 2), c.temp.slice(0, 2)],
    [["011990-99999", "011990-99999"], [-619524000000n, -619506000000n], [0, 22]]);
  f = await avro("kafka_connect/confluent/codec_bzip2/events/partition=1/events+1+0000000000.avro");
  ({ columns: c } = await f.readBatch(0));
  f.close();
  assert.deepEqual([c.id.slice(0, 2), c.name.slice(0, 2), c.amount.slice(0, 2)],
    [[0n, 1n], ["event-0", "event-1"], ["-183064.27", "-7231359.84"]]);
  console.log("avro rows ok");
}

const ORC_Z = new URL("../../testdata/orc_tests/wasm_types.zstd.orc", import.meta.url).pathname;
const srv = spawn(process.execPath, [new URL("./serve.mjs", import.meta.url).pathname, "0", FILE, ORC_Z]);
const port = await new Promise((r) => createInterface({ input: srv.stdout }).once("line", r));
try {
  const url = `http://127.0.0.1:${port}/data/delta_length_byte_array.parquet`;
  check(await readFooter(await httpSource(url)), "http-range");
  await checkRows(await httpSource(url), "http-range");
  const f = await openOrc(await httpSource(`http://127.0.0.1:${port}/data/wasm_types.zstd.orc`));
  const { columns } = await f.readStripe(0, ["id", "name"]);
  f.close();
  assert.deepEqual([columns.id[4999], columns.name[4999]], [4999n, "name-52"]);
  console.log("http-range orc ok");
} finally {
  srv.kill();
}

const notParquet = new Blob([new TextEncoder().encode("this is not a parquet file")]);
await assert.rejects(readFooter(blobSource(notParquet)), /not a parquet file/);
console.log("non-parquet refused ok");
