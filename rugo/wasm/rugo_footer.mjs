// RUGO-1 spike: read a Parquet footer through rugo's WASM build without ever
// touching column data. A "source" is { size, read(offset, length) -> Uint8Array }.
import createRugo from "./dist/rugo.mjs";

// First read = last TAIL bytes: one round trip for nearly every real footer.
const TAIL = 64 * 1024;

// Local file: Blob/File.slice() reads only the requested byte range.
export function blobSource(blob) {
  return {
    size: blob.size,
    reads: [],
    async read(offset, length) {
      this.reads.push([offset, length]);
      return new Uint8Array(await blob.slice(offset, offset + length).arrayBuffer());
    },
  };
}

// Remote file: HTTP Range requests. Size comes from Content-Range of the first read.
export async function httpSource(url) {
  const src = {
    url, size: 0, reads: [],
    async read(offset, length) {
      this.reads.push([offset, length]);
      const r = await fetch(url, { headers: { Range: `bytes=${offset}-${offset + length - 1}` } });
      if (r.status !== 206) throw new Error(`${url}: server ignored Range (HTTP ${r.status})`);
      return new Uint8Array(await r.arrayBuffer());
    },
  };
  // Suffix range gets the size and the tail in one request.
  const r = await fetch(url, { headers: { Range: `bytes=-${TAIL}` } });
  if (r.status !== 206) throw new Error(`${url}: server ignored Range (HTTP ${r.status})`);
  const m = /\/(\d+)$/.exec(r.headers.get("Content-Range") ?? "");
  if (!m) throw new Error(`${url}: no Content-Range total`);
  src.size = Number(m[1]);
  const tail = new Uint8Array(await r.arrayBuffer());
  src.reads.push([src.size - tail.length, tail.length]);
  src.prefetched = tail;
  return src;
}

let modP;
export function loadRugo() { return (modP ??= createRugo()); }

export async function readFooter(src) {
  const m = await loadRugo();
  let tail = src.prefetched ?? (await src.read(Math.max(0, src.size - TAIL), Math.min(TAIL, src.size)));
  if (tail.length < 8) throw new Error("file too small to be parquet");

  const p8 = m._malloc(8);
  m.HEAPU8.set(tail.subarray(tail.length - 8), p8);
  const footerLen = Number(m._rugo_footer_length(p8));
  m._free(p8);
  if (footerLen < 0) throw new Error("not a parquet file (no PAR1 trailer)");
  if (footerLen + 8 > src.size) throw new Error("footer length invalid");

  const need = footerLen + 8;
  if (tail.length < need) {
    // Footer larger than the speculative tail: fetch only the missing prefix.
    const head = await src.read(src.size - need, need - tail.length);
    const all = new Uint8Array(need);
    all.set(head, 0);
    all.set(tail, need - tail.length);
    tail = all;
  } else {
    tail = tail.subarray(tail.length - need);
  }

  const p = m._malloc(need);
  m.HEAPU8.set(tail, p);
  const rc = m._rugo_parse_footer(p, need);
  m._free(p);
  const text = new TextDecoder().decode(m.HEAPU8.subarray(m._rugo_result(), m._rugo_result() + m._rugo_result_len()));
  if (rc !== 0) throw new Error(`rugo: ${text}`);
  return JSON.parse(text);
}
