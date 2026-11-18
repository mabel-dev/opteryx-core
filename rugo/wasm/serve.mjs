// Dev harness: static files from this dir + /data/<name> served with HTTP Range from argv paths.
// Usage: node serve.mjs <port> <file.parquet>...
import http from "node:http";
import { open, readFile, stat } from "node:fs/promises";
import { basename, extname, join } from "node:path";

const [port, ...files] = process.argv.slice(2);
const data = Object.fromEntries(files.map((f) => [basename(f), f]));
const types = { ".html": "text/html", ".mjs": "text/javascript", ".wasm": "application/wasm" };
let served = 0;

http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  if (url.pathname === "/served") { res.end(String(served)); return; }
  if (url.pathname.startsWith("/data/")) {
    const path = data[url.pathname.slice(6)];
    if (!path) { res.writeHead(404); return res.end(); }
    const size = (await stat(path)).size;
    const m = /bytes=(\d*)-(\d*)/.exec(req.headers.range ?? "");
    if (!m) { res.writeHead(416); return res.end(); }  // refuse whole-file reads outright
    const start = Math.max(0, m[1] === "" ? size - Number(m[2]) : Number(m[1]));
    const end = m[1] === "" || m[2] === "" ? size - 1 : Number(m[2]);
    const buf = Buffer.alloc(end - start + 1);
    const fh = await open(path);
    await fh.read(buf, 0, buf.length, start);
    await fh.close();
    served += buf.length;
    res.writeHead(206, { "Content-Range": `bytes ${start}-${end}/${size}`, "Content-Length": buf.length,
                         "Access-Control-Expose-Headers": "Content-Range" });
    return res.end(buf);
  }
  const p = join(import.meta.dirname, url.pathname === "/" ? "harness.html" : url.pathname);
  const body = await readFile(p).catch(() => null);
  if (!body) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "Content-Type": types[extname(p)] ?? "application/octet-stream" });
  res.end(body);
}).listen(Number(port), "127.0.0.1", function () { console.log(this.address().port); });
