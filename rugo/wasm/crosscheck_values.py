# Decoded values: wasm vs native rugo, every row group / stripe / batch and column, on
# each file given (.parquet: openParquet vs rugo.parquet.read_parquet; .orc: openOrc vs
# rugo.orc.read_orc; .avro: openAvro vs rugo.avro.read_avro). Dev tool (needs native
# rugo built + node).
# Usage: python rugo/wasm/crosscheck_values.py <file>...   (after build.sh)
import datetime
import decimal
import json
import os
import subprocess
import sys
import uuid

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))
import rugo.avro
import rugo.orc
import rugo.parquet

HERE = os.path.dirname(os.path.abspath(__file__))
EPOCH_D = datetime.date(1970, 1, 1)
EPOCH_TS = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
UNIT = {"ms": 1000, "us": 1, "ns": None}

# wasm values as JSON: BigInt -> "int:<n>", non-finite -> "float:<v>", bytes -> hex, interval -> [months, us]
DUMP = """
import { openAsBlob } from "node:fs";
import { blobSource, openAvro, openOrc, openParquet } from "%s/rugo_footer.mjs";
const path = process.argv[1];
const src = blobSource(await openAsBlob(path));
const out = { schema: [], groups: [], error: null };
if (path.endsWith(".parquet")) {
  const f = await openParquet(src);
  out.schema = f.footer.row_groups[0]?.columns.map((c) => [c.name, c.logical_type]) ?? [];
  for (let rg = 0; rg < f.footer.num_row_groups; rg++) {
    const g = { columns: {}, errors: {} };
    for (const c of f.footer.row_groups[rg].columns) {
      try { g.columns[c.name] = (await f.readRowGroup(rg, [c.name])).columns[c.name]; }
      catch (e) { g.errors[c.name] = String(e.message); }
    }
    out.groups.push(g);
  }
  f.close();
} else {
  // ORC stripes / Avro batches, every column at once (as the native readers do)
  try {
    const orc = path.endsWith(".orc");
    const f = orc ? await openOrc(src) : await openAvro(src);
    const parts = orc ? f.meta.stripes.length : f.meta.batches.length;
    for (let i = 0; i < parts; i++) {
      try { out.groups.push({ columns: (await (orc ? f.readStripe(i) : f.readBatch(i))).columns, errors: {} }); }
      catch (e) { out.groups.push({ columns: {}, errors: { "*": String(e.message) } }); }
    }
    f.close();
  } catch (e) { out.error = String(e.message); }
}
process.stdout.write(JSON.stringify(out, (k, v) =>
  typeof v === "bigint" ? `int:${v}` : typeof v === "number" && !Number.isFinite(v) ? `float:${v}` : v instanceof Uint8Array ? Buffer.from(v).toString("hex") :
  v !== null && typeof v === "object" && "months" in v ? [v.months, v.us] : v));
""" % HERE


def native_norm(v, logical):
    # native renders logical types as Python objects; wasm hands back the stored value
    if v is None:
        return None
    if isinstance(v, list):
        return [native_norm(x, logical) for x in v]
    if isinstance(v, datetime.datetime):
        us = (v - EPOCH_TS) // datetime.timedelta(microseconds=1)
        unit = next((u for u in UNIT if f"[{u}" in logical or f"({u}" in logical or logical.endswith(u)), "us")
        return us // 1000 if unit == "ms" else us * 1000 if unit == "ns" else us
    if isinstance(v, datetime.date):
        return (v - EPOCH_D).days
    if isinstance(v, datetime.time):
        return ((v.hour * 60 + v.minute) * 60 + v.second) * 1_000_000 + v.microsecond
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dict):
        return {k: native_norm(x, "") for k, x in v.items()}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    return v


def wasm_norm(v):
    if isinstance(v, list):
        return [wasm_norm(x) for x in v]
    if isinstance(v, str) and v.startswith("float:"):
        return float({"float:Infinity": "inf", "float:-Infinity": "-inf", "float:NaN": "nan"}[v])
    if isinstance(v, str) and v.startswith("int:"):
        return int(v[4:])
    return v


def same(a, b):
    if isinstance(a, list) or isinstance(b, list):
        return isinstance(a, list) and isinstance(b, list) and len(a) == len(b) and all(map(same, a, b))
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is b
        return float(a) == float(b) or (float(a) != float(a) and float(b) != float(b))
    return a == b


def check(path):
    out = subprocess.run(["node", "--input-type=module", "-e", DUMP, path], capture_output=True, text=True)
    if out.returncode != 0:
        print(f"FAIL {path}: wasm dump failed\n{out.stderr}")
        return False
    w = json.loads(out.stdout)
    logical = dict(w["schema"])
    ok = True
    try:
        if path.endswith(".parquet"):
            native = list(rugo.parquet.read_parquet(path, threads=1))
        elif path.endswith(".orc"):
            native = list(rugo.orc.read_orc(path))
        else:
            with rugo.avro.read_avro(path, max_threads=1) as r:
                native = list(r)
    except (RuntimeError, ValueError, NotImplementedError) as e:
        refused = [n for g in w["groups"] for n in g["errors"]] + ([w["error"]] if w["error"] else [])
        print(("ok  " if refused else "FAIL"), path, f"(native refuses: {str(e)[:120]}; wasm refuses {sorted(set(refused))[:3]})")
        return bool(refused)
    if w["error"]:
        print(f"FAIL {path}: wasm refuses ({w['error']}), native reads it")
        return False
    if len(native) != len(w["groups"]):
        print(f"FAIL {path}: native {len(native)} parts, wasm {len(w['groups'])}")
        return False
    if True:
        for rg, m in enumerate(native):
            g = w["groups"][rg]
            for name in m.column_names:
                n = name.decode()
                if n in g["errors"] or "*" in g["errors"]:
                    print(f"  rg{rg} {n}: wasm refused: {g['errors'].get(n, g['errors'].get('*'))}")
                    ok = False
                    continue
                nat = [native_norm(v, logical.get(n, "")) for v in m.column(name).to_pylist()]
                was = [wasm_norm(v) for v in g["columns"][n]]
                bad = next((i for i, (a, b) in enumerate(zip(nat, was)) if not same(a, b)), None)
                if len(nat) != len(was) or bad is not None:
                    i = bad if bad is not None else min(len(nat), len(was))
                    print(f"  rg{rg} {n}: MISMATCH at row {i}: native={nat[i:i+1]} wasm={was[i:i+1]} (len {len(nat)} vs {len(was)})")
                    ok = False
    print(("ok  " if ok else "FAIL"), path)
    return ok


if __name__ == "__main__":
    results = [check(p) for p in sys.argv[1:]]
    sys.exit(0 if all(results) else 1)
