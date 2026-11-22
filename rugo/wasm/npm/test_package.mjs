// npm release floor for @opteryx/rugo: imports the PACKED package by name from a fresh
// project, as a user would, so a broken exports map, a file missing from `files` or a wasm
// that doesn't resolve beside its glue fails here instead of on npm. test_footer.mjs covers
// the decoding itself; this covers the package around it. Must run from the consumer
// project (bare-specifier resolution starts at the script's own folder):
//   cp test_package.mjs <consumer>/ && cd <consumer> && node test_package.mjs <repo-root>
import assert from "node:assert/strict";
import { openAsBlob } from "node:fs";
import { access, readFile } from "node:fs/promises";
import path from "node:path";
import * as rugo from "@opteryx/rugo";

const repo = process.argv[2];
assert.ok(repo, "usage: node test_package.mjs <repo-root>");
const fixture = (rel) => path.join(repo, "testdata", rel);

const EXPORTS = ["blobSource", "httpSource", "loadRugo", "openAvro", "openOrc", "openParquet", "readFooter"];
assert.deepEqual(Object.keys(rugo).sort(), EXPORTS);
for (const name of EXPORTS) assert.equal(typeof rugo[name], "function", name);

// The exports map doesn't expose package.json, so read the installed copy by path.
const pkgPath = path.join(process.cwd(), "node_modules/@opteryx/rugo/package.json");
const pkg = JSON.parse(await readFile(pkgPath, "utf8"));
const version = /__version__ = "([^"]+)"/.exec(await readFile(path.join(repo, "rugo/__version__.py"), "utf8"))[1];
assert.equal(pkg.version, version, "package version must be rugo/__version__.py");
await access(path.join(path.dirname(pkgPath), pkg.exports["."].types));
console.log("exports, version, types ok");

const blob = async (rel) => rugo.blobSource(await openAsBlob(fixture(rel)));

const pq = await rugo.openParquet(await blob("parquet_tests/delta_length_byte_array.parquet"));
assert.equal(pq.footer.num_rows, 1000);
const rg = await pq.readRowGroup(0, ["FRUIT"]);
assert.equal(rg.num_rows, 1000);
assert.equal(typeof rg.columns.FRUIT[0], "string");
pq.close();
console.log("parquet ok");

const orc = await rugo.openOrc(await blob("orc_tests/wasm_types.zstd.orc"));
assert.equal(orc.meta.stripes.reduce((a, b) => a + b, 0), orc.meta.num_rows);
assert.equal((await orc.readStripe(0)).num_rows, orc.meta.stripes[0]);
orc.close();
console.log("orc ok");

const avro = await rugo.openAvro(await blob("avro/space_missions.avro"));
assert.equal(avro.meta.num_rows, 4630);
assert.equal((await avro.readBatch(0)).num_rows, avro.meta.batches[0]);
avro.close();
console.log("avro ok");

await assert.rejects(rugo.openParquet(await blob("avro/space_missions.avro")), /PAR1/);
console.log("wrong format refused ok");
