// CI gate for the published bundle: footer read through both byte paths on a
// committed file, checked against native rugo's figures for it.
// Usage: node test_footer.mjs   (after ./build.sh)
import assert from "node:assert";
import { spawn } from "node:child_process";
import { openAsBlob } from "node:fs";
import { createInterface } from "node:readline";
import { blobSource, httpSource, readFooter } from "./rugo_footer.mjs";

const FILE = new URL("../../testdata/parquet_tests/delta_length_byte_array.parquet", import.meta.url).pathname;
// native: rugo.parquet.read_metadata(FILE) -> 1000 rows, 1 row group, FRUIT byte_array/varchar
function check(fs, how) {
  assert.equal(fs.num_rows, 1000, how);
  assert.equal(fs.num_row_groups, 1, how);
  assert.deepEqual(fs.schema.map((c) => [c.name, c.physical_type, c.logical_type]), [["FRUIT", "byte_array", "varchar"]], how);
  assert.equal(fs.row_groups[0].columns[0].num_values, 1000, how);
  console.log(how, "ok");
}

check(await readFooter(blobSource(await openAsBlob(FILE))), "blob-slice");

const srv = spawn(process.execPath, [new URL("./serve.mjs", import.meta.url).pathname, "0", FILE]);
const port = await new Promise((r) => createInterface({ input: srv.stdout }).once("line", r));
try {
  check(await readFooter(await httpSource(`http://127.0.0.1:${port}/data/delta_length_byte_array.parquet`)), "http-range");
} finally {
  srv.kill();
}

const notParquet = new Blob([new TextEncoder().encode("this is not a parquet file")]);
await assert.rejects(readFooter(blobSource(notParquet)), /not a parquet file/);
console.log("non-parquet refused ok");
