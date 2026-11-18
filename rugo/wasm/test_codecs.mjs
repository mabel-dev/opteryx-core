// Round-trips Node-compressed bytes through rugo's wasm decompressors (gzip, zstd).
// Snappy / LZ4_RAW have no Node built-in compressor and are NOT exercised here.
import zlib from "node:zlib";
import assert from "node:assert";
import create from "./build/rugo_codecs.mjs";
const m = await create();
const plain = Buffer.from(Array.from({ length: 200000 }, (_, i) => `row ${i % 977};`).join(""));
for (const [name, codec, comp] of [["GZIP", 2, zlib.gzipSync(plain)], ["ZSTD", 6, zlib.zstdCompressSync(plain)]]) {
  const pi = m._malloc(comp.length), po = m._malloc(plain.length);
  m.HEAPU8.set(comp, pi);
  assert.equal(m._rugo_decompress(codec, pi, comp.length, po, plain.length), 0, name);
  assert.ok(Buffer.from(m.HEAPU8.subarray(po, po + plain.length)).equals(plain), name);
  // wrong declared size must fail, not truncate
  assert.equal(m._rugo_decompress(codec, pi, comp.length, po, plain.length - 1), 1, name + " size check");
  m._free(pi); m._free(po);
  console.log(name, "ok", comp.length, "->", plain.length);
}
assert.equal(m._rugo_decompress(4, 0, 0, 0, 0), 1, "BROTLI must refuse");
console.log("BROTLI refused (as native)");
