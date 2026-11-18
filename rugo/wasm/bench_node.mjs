// Usage: node bench_node.mjs <file.parquet> [--http]
// Measures time-to-schema via Blob.slice (fs.openAsBlob) or a local Range server.
import { openAsBlob, statSync } from "node:fs";
import { open } from "node:fs/promises";
import http from "node:http";
import { blobSource, httpSource, readFooter, loadRugo } from "./rugo_footer.mjs";

const [path, mode] = process.argv.slice(2);
const size = statSync(path).size;

let served = 0, server;
async function rangeServer() {
  const fh = await open(path);
  server = http.createServer(async (req, res) => {
    let [, a, b] = /bytes=(\d*)-(\d*)/.exec(req.headers.range ?? "") ?? [];
    if (a === undefined) { res.writeHead(200, { "Content-Length": size }); return res.end(); } // never stream it
    let start = a === "" ? size - Number(b) : Number(a);
    let end = a === "" || b === "" ? size - 1 : Number(b);
    start = Math.max(0, start);
    const buf = Buffer.alloc(end - start + 1);
    await fh.read(buf, 0, buf.length, start);
    served += buf.length;
    res.writeHead(206, { "Content-Range": `bytes ${start}-${end}/${size}`, "Content-Length": buf.length });
    res.end(buf);
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return `http://127.0.0.1:${server.address().port}/f.parquet`;
}

const t0 = performance.now();
const m = await loadRugo();
const tInit = performance.now() - t0;

const t1 = performance.now();
const src = mode === "--http" ? await httpSource(await rangeServer()) : blobSource(await openAsBlob(path));
const fs = await readFooter(src);
const tSchema = performance.now() - t1;

const bytesRead = src.reads.reduce((s, [, n]) => s + n, 0);
console.log(JSON.stringify({
  file: path, file_bytes: size, mode: mode === "--http" ? "http-range" : "blob-slice",
  num_rows: fs.num_rows, columns: fs.schema.length, row_groups: fs.num_row_groups,
  reads: src.reads.length, bytes_read: bytesRead, http_bytes_served: mode === "--http" ? served : undefined,
  wasm_init_ms: +tInit.toFixed(1), time_to_schema_ms: +tSchema.toFixed(1),
  wasm_heap_bytes: m.HEAPU8.length, rss_bytes: process.memoryUsage().rss,
}, null, 1));
console.log("first column:", JSON.stringify(fs.schema[0]), "\nrg0.col0:", JSON.stringify(fs.row_groups[0]?.columns[0]));
server?.close();
