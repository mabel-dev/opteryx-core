// rugo's Parquet, ORC and Avro readers in WASM. readFooter reads only a Parquet
// footer; openParquet also decodes column chunks, fetching exactly each requested
// chunk's byte range. openOrc / openAvro read the whole file (see each).
// A "source" is { size, read(offset, length) -> Uint8Array }.
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

// Footer bytes (+ 8-byte trailer): the speculative tail, plus the missing prefix if larger.
async function footerBytes(m, src) {
  let tail = src.prefetched ?? (await src.read(Math.max(0, src.size - TAIL), Math.min(TAIL, src.size)));
  if (tail.length < 8) throw new Error("file too small to be parquet");

  const p8 = m._malloc(8);
  m.HEAPU8.set(tail.subarray(tail.length - 8), p8);
  const footerLen = Number(m._rugo_footer_length(p8));
  m._free(p8);
  if (footerLen < 0) throw new Error("not a parquet file (no PAR1 trailer)");
  if (footerLen + 8 > src.size) throw new Error("footer length invalid");

  const need = footerLen + 8;
  if (tail.length >= need) return tail.subarray(tail.length - need);
  const head = await src.read(src.size - need, need - tail.length);
  const all = new Uint8Array(need);
  all.set(head, 0);
  all.set(tail, need - tail.length);
  return all;
}

function resultText(m) {
  const p = m._rugo_result();
  return new TextDecoder().decode(m.HEAPU8.subarray(p, p + m._rugo_result_len()));
}

// Copies `bytes` into the wasm heap, runs fn(ptr, length), frees.
function withHeap(m, bytes, fn) {
  const p = m._malloc(bytes.length);
  if (p === 0) throw new Error(`rugo: cannot allocate ${bytes.length} bytes`);
  try {
    m.HEAPU8.set(bytes, p);
    return fn(p, bytes.length);
  } finally {
    m._free(p);
  }
}

// DrakenType tags (draken/core/buffers.h).
const T = {
  INT8: 1, INT16: 2, INT32: 3, INT64: 4, DECIMAL: 5, FLOAT32: 20, FLOAT64: 21, DATE32: 30,
  TIMESTAMP64: 40, TIME32: 41, TIME64: 42, INTERVAL: 43, BOOL: 50, VARCHAR: 60, NVARCHAR: 63,
  VARBINARY: 64, ARRAY: 80, DECIMAL128: 103, UINT8: 104, UINT16: 105, UINT32: 106, UINT64: 107, UUID: 108,
};

function scaled(v, scale) {
  if (scale === 0) return v.toString();
  const neg = v < 0n;
  const s = (neg ? -v : v).toString().padStart(scale + 1, "0");
  return `${neg ? "-" : ""}${s.slice(0, -scale)}.${s.slice(-scale)}`;
}

const isNull = (heap, bitmap, i) => bitmap !== 0 && ((heap[bitmap + (i >> 3)] >> (i & 7)) & 1) === 0;

// Value i of a Draken buffer of `type` at `data` (strings: DrakenStringSlot[] at `data`
// with long values in `arena`). Dates, times and timestamps are the stored integers.
function getter(m, type, data, arena, scale) {
  const dv = new DataView(m.HEAPU8.buffer);
  const heap = m.HEAPU8;
  switch (type) {
    case T.INT8: return (i) => dv.getInt8(data + i);
    case T.INT16: return (i) => dv.getInt16(data + 2 * i, true);
    case T.INT32: case T.DATE32: case T.TIME32: return (i) => dv.getInt32(data + 4 * i, true);
    case T.INT64: case T.TIMESTAMP64: case T.TIME64: return (i) => dv.getBigInt64(data + 8 * i, true);
    case T.UINT8: return (i) => dv.getUint8(data + i);
    case T.UINT16: return (i) => dv.getUint16(data + 2 * i, true);
    case T.UINT32: return (i) => dv.getUint32(data + 4 * i, true);
    case T.UINT64: return (i) => dv.getBigUint64(data + 8 * i, true);
    case T.FLOAT32: return (i) => dv.getFloat32(data + 4 * i, true);
    case T.FLOAT64: return (i) => dv.getFloat64(data + 8 * i, true);
    case T.BOOL: return (i) => ((heap[data + (i >> 3)] >> (i & 7)) & 1) === 1;
    case T.DECIMAL: return (i) => scaled(dv.getBigInt64(data + 8 * i, true), scale);
    case T.DECIMAL128:
      return (i) => scaled((dv.getBigInt64(data + 16 * i + 8, true) << 64n) | dv.getBigUint64(data + 16 * i, true), scale);
    case T.UUID:
      return (i) => {
        const h = Array.from(heap.subarray(data + 16 * i, data + 16 * i + 16), (b) => b.toString(16).padStart(2, "0")).join("");
        return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
      };
    case T.INTERVAL:
      return (i) => ({ months: dv.getBigInt64(data + 16 * i, true), us: dv.getBigInt64(data + 16 * i + 8, true) });
    case T.VARCHAR:
    case T.NVARCHAR:
    case T.VARBINARY: {
      // DrakenStringSlot (draken/core/string_slot.h): u32 length, then <= 12 bytes inline,
      // else u32 prefix, u32 hash, u32 arena offset.
      const td = new TextDecoder();
      const bytes = (i) => {
        const s = data + 16 * i;
        const len = dv.getUint32(s, true);
        const at = len <= 12 ? s + 4 : arena + dv.getUint32(s + 12, true);
        return heap.subarray(at, at + len);
      };
      return type === T.VARBINARY ? (i) => bytes(i).slice() : (i) => td.decode(bytes(i));
    }
    default:
      throw new Error(`rugo: no JS rendering for DrakenType ${type}`);
  }
}

// The materialized column rugo_decode_column left behind, as a JS array (null = null).
function readColumn(m) {
  const [type, n, data, validity, arena, , , scale] = new Uint32Array(m.HEAPU8.buffer, m._rugo_column_info(), 8);
  const get = getter(m, type, data, arena, scale);
  const out = new Array(n);
  for (let i = 0; i < n; i++) out[i] = isNull(m.HEAPU8, validity, i) ? null : get(i);
  return out;
}

// OutKind (rugo/src/avro/avro_reader.hpp)
const K = { Raw: 0, String: 1, Dict: 2, Decimal64: 3, Decimal128: 4, Timestamp: 5, Time64: 6, Ipv4: 7,
  Array: 8, ConstRaw: 9, ConstString: 10 };

// An AvroColumn (the carrier the ORC and Avro readers both produce) as a JS array.
function readAvColumn(m, ptr) {
  const [kind, type, n, data, validity, slots, arena, , codes, , , scale, offsets, childValidity, childType,
    childLength, childCol] = Array.from(new Uint32Array(m.HEAPU8.buffer, m._rugo_avcol_info(ptr), 17));
  const heap = m.HEAPU8;
  const dv = new DataView(heap.buffer);
  const code = (i) => dv.getUint32(codes + 4 * i, true);
  let get;
  switch (kind) {
    case K.Raw: case K.Decimal64: case K.Decimal128: case K.Timestamp: case K.Time64: case K.Ipv4:
      get = getter(m, type, data, arena, scale); break;
    case K.String: get = getter(m, type, slots, arena, scale); break;
    case K.Dict: { const g = getter(m, type, slots, arena, scale); get = (i) => g(code(i)); break; }
    case K.ConstRaw: { const g = getter(m, type, data, arena, scale); get = (i) => g(code(i)); break; }
    case K.ConstString: { const g = getter(m, type, slots, arena, scale); get = (i) => g(code(i)); break; }
    case K.Array: {
      let child;
      if (childType === T.ARRAY) {
        child = readAvColumn(m, childCol);
      } else {
        const g = getter(m, childType, childType === T.VARCHAR || childType === T.NVARCHAR || childType === T.VARBINARY ? slots : data, arena, scale);
        child = new Array(childLength);
        for (let j = 0; j < childLength; j++) child[j] = isNull(heap, childValidity, j) ? null : g(j);
      }
      get = (i) => child.slice(dv.getInt32(offsets + 4 * i, true), dv.getInt32(offsets + 4 * i + 4, true));
      break;
    }
    default:
      throw new Error(`rugo: unknown column kind ${kind}`);
  }
  const out = new Array(n);
  for (let i = 0; i < n; i++) out[i] = isNull(heap, validity, i) ? null : get(i);
  return out;
}

// The batch an ORC / Avro decode left behind: { num_rows, columns: { name: values[] } }.
function readBatch(m, names) {
  const width = m._rugo_batch_width();
  if (width !== names.length) throw new Error(`rugo: decoded ${width} columns, expected ${names.length}`);
  const columns = {};
  for (let c = 0; c < width; c++) columns[names[c]] = readAvColumn(m, m._rugo_batch_column(c));
  return { num_rows: m._rugo_batch_rows(), columns };
}

// NUL-separated UTF-8 names in the heap; fn(ptr, length); 0-length for "every column".
function withNames(m, names, fn) {
  if (names === undefined) return fn(0, 0);
  const enc = new TextEncoder();
  const parts = names.map((n) => enc.encode(`${n}\0`));
  const all = new Uint8Array(parts.reduce((a, p) => a + p.length, 0));
  let at = 0;
  for (const p of parts) { all.set(p, at); at += p.length; }
  return withHeap(m, all, fn);
}

// The whole file into the wasm heap; ownership passes to `open` (which frees it on failure).
async function wholeFile(m, src, open, what) {
  if (src.size >= 2 ** 32) throw new Error(`rugo: ${what} file of ${src.size} bytes exceeds the wasm32 heap`);
  const bytes = await src.read(0, src.size);
  const p = m._malloc(bytes.length);
  if (p === 0) throw new Error(`rugo: cannot allocate ${bytes.length} bytes for the ${what} file`);
  m.HEAPU8.set(bytes, p);
  const h = open(p, bytes.length);
  const text = resultText(m);
  if (h === 0) throw new Error(`rugo: ${text}`);
  return [h, JSON.parse(text)];
}

// ORC: the native reader addresses the whole file, so the whole file is read into the
// wasm heap (no Range reads). readStripe decodes one stripe's columns (default all).
export async function openOrc(src) {
  const m = await loadRugo();
  const [h, meta] = await wholeFile(m, src, (p, n) => m._rugo_orc_open(p, n), "ORC");
  let open = true;
  return {
    meta,
    async readStripe(stripe, columns) {
      if (!open) throw new Error("rugo: file is closed");
      if (!Number.isInteger(stripe) || stripe < 0 || stripe >= meta.stripes.length) {
        throw new Error(`rugo: stripe ${stripe} out of range (file has ${meta.stripes.length})`);
      }
      const rc = withNames(m, columns, (p, n) => m._rugo_orc_decode(h, stripe, p, n));
      if (rc !== 0) throw new Error(`rugo: stripe ${stripe}: ${resultText(m)}`);
      try {
        return readBatch(m, columns ?? meta.columns.map((c) => c.name));
      } finally {
        m._rugo_batch_release();
      }
    },
    close() {
      if (open) m._rugo_orc_close(h);
      open = false;
    },
  };
}

// Avro: a container file has no index, so the whole file is read into the wasm heap.
// readBatch decodes batch `i` (whole blocks, up to 65,536 rows); columns may be dotted.
export async function openAvro(src) {
  const m = await loadRugo();
  const [h, meta] = await wholeFile(m, src, (p, n) => m._rugo_avro_open(p, n), "Avro");
  let open = true;
  return {
    meta,
    async readBatch(batch, columns) {
      if (!open) throw new Error("rugo: file is closed");
      if (!Number.isInteger(batch) || batch < 0 || batch >= meta.batches.length) {
        throw new Error(`rugo: batch ${batch} out of range (file has ${meta.batches.length})`);
      }
      const rc = withNames(m, columns, (p, n) => m._rugo_avro_decode(h, batch, p, n));
      if (rc !== 0) throw new Error(`rugo: batch ${batch}: ${resultText(m)}`);
      try {
        return readBatch(m, columns ?? meta.columns);
      } finally {
        m._rugo_batch_release();
      }
    },
    close() {
      if (open) m._rugo_avro_close(h);
      open = false;
    },
  };
}

// The LIST column rugo_decode_column left behind, in the engine's TAG_ARRAY wire
// format (rugo/src/parquet/ipc_serialize.hpp, serialize_list_column): one block per
// nesting level, each with its entries' validity and Arrow-style child offsets;
// the innermost block carries the leaf values. Returns nested JS arrays (null = null).
function readList(m) {
  const [ptr, len, leafText] = new Uint32Array(m.HEAPU8.buffer, m._rugo_list_info(), 3);
  const b = m.HEAPU8.subarray(ptr, ptr + len);
  const dv = new DataView(b.buffer, b.byteOffset, len);
  const td = new TextDecoder();
  let o = 0;
  const u8 = () => b[o++];
  const u32 = () => { const v = dv.getUint32(o, true); o += 4; return v; };
  const bitmap = () => { const n = u32(); const at = o; o += n; return n === 0 ? null : b.subarray(at, at + n); };
  const bit = (bm, i) => bm === null || ((bm[i >> 3] >> (i & 7)) & 1) === 1;
  const ints = (n) => { const a = new Int32Array(n); for (let i = 0; i < n; i++) a[i] = dv.getInt32(o + 4 * i, true); o += 4 * n; return a; };

  if (u8() !== 11) throw new Error("rugo: LIST column is not a TAG_ARRAY block");
  const numRows = u32();
  const levels = [];          // levels[k] = { valid, offsets } for nesting depth k+1
  let valid = bitmap();       // validity of this level's entries (null only when there are none)
  let entries = numRows;
  let leaf;
  for (;;) {
    const tag = u8();
    const count = u32();
    levels.push({ valid, offsets: ints(entries + 1) });
    if (tag === 8) {          // CHILD_ARRAY: the children are the next level's entries
      valid = bitmap();
      entries = count;
      continue;
    }
    const lv = bitmap();      // null = no null elements
    const vals = new Array(count);
    const fixed = { 1: [8, (p) => dv.getBigInt64(p, true)], 2: [4, (p) => dv.getInt32(p, true)],
      3: [4, (p) => dv.getFloat32(p, true)], 4: [8, (p) => dv.getFloat64(p, true)],
      7: [8, (p) => dv.getBigUint64(p, true)], 9: [1, (p) => dv.getInt8(p)], 10: [2, (p) => dv.getInt16(p, true)],
      11: [1, (p) => dv.getUint8(p)], 12: [2, (p) => dv.getUint16(p, true)], 13: [4, (p) => dv.getUint32(p, true)] }[tag];
    if (tag === 6) {          // CHILD_STRING: u32 length + bytes per slot
      for (let i = 0; i < count; i++) {
        const n = u32();
        vals[i] = leafText ? td.decode(b.subarray(o, o + n)) : b.slice(o, o + n);
        o += n;
      }
    } else if (tag === 5) {   // CHILD_BOOL: bit-packed, LSB-first
      for (let i = 0; i < count; i++) vals[i] = ((b[o + (i >> 3)] >> (i & 7)) & 1) === 1;
      o += (count + 7) >> 3;
    } else if (fixed) {
      const [w, get] = fixed;
      for (let i = 0; i < count; i++) vals[i] = get(o + w * i);
      o += w * count;
    } else {
      throw new Error(`rugo: unknown LIST leaf tag ${tag}`);
    }
    leaf = (i) => (bit(lv, i) ? vals[i] : null);
    break;
  }
  if (o !== len) throw new Error(`rugo: LIST wire block has ${len - o} trailing bytes`);

  const D = levels.length;
  const build = (k, i) => {
    const { valid: v, offsets } = levels[k];
    if (!bit(v, i)) return null;
    const out = [];
    for (let j = offsets[i]; j < offsets[i + 1]; j++) out.push(k + 1 < D ? build(k + 1, j) : leaf(j));
    return out;
  };
  const rows = new Array(numRows);
  for (let i = 0; i < numRows; i++) rows[i] = build(0, i);
  return rows;
}

// Parses the footer and keeps it in the module for column decode. close() frees it.
export async function openParquet(src) {
  const m = await loadRugo();
  const bytes = await footerBytes(m, src);
  const h = withHeap(m, bytes, (p, n) => m._rugo_open(p, n));
  const text = resultText(m);
  if (h === 0) throw new Error(`rugo: ${text}`);
  const footer = JSON.parse(text);
  let open = true;

  return {
    footer,
    // Decodes `columns` (default: all) of row group `rg`: one read per column chunk,
    // nothing else of the file. Returns { num_rows, columns: { name: values[] } }.
    async readRowGroup(rg, columns) {
      if (!open) throw new Error("rugo: file is closed");
      const group = footer.row_groups[rg];
      if (!Number.isInteger(rg) || group === undefined) {
        throw new Error(`rugo: row group ${rg} out of range (file has ${footer.num_row_groups})`);
      }
      const names = columns ?? group.columns.map((c) => c.name);
      const idx = names.map((name) => {
        const i = group.columns.findIndex((c) => c.name === name);
        if (i < 0) throw new Error(`rugo: no column '${name}' in row group ${rg}`);
        return i;
      });
      const chunks = await Promise.all(
        idx.map((i) => src.read(m._rugo_chunk_offset(h, rg, i), m._rugo_chunk_length(h, rg, i))),
      );
      const out = {};
      idx.forEach((i, k) => {
        const rc = withHeap(m, chunks[k], (p, n) => m._rugo_decode_column(h, rg, i, p, n));
        if (rc === 1) throw new Error(`rugo: row group ${rg}, ${resultText(m)}`);
        try {
          out[names[k]] = rc === 2 ? readList(m) : readColumn(m);
        } finally {
          m._rugo_column_release();
        }
        if (out[names[k]].length !== group.num_rows) {
          throw new Error(`rugo: row group ${rg}, column '${names[k]}': decoded ${out[names[k]].length} rows, row group has ${group.num_rows}`);
        }
      });
      return { num_rows: group.num_rows, columns: out };
    },
    close() {
      if (open) m._rugo_close(h);
      open = false;
    },
  };
}

export async function readFooter(src) {
  const f = await openParquet(src);
  f.close();
  return f.footer;
}
