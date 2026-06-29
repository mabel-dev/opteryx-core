"""
bench_vs_arrow_duckdb.py — Benchmark: rugo vs PyArrow vs DuckDB.

Measures read throughput, peak RSS during read, package install size,
and cold import time.

Downloads astronauts.parquet on first run (small file for quick test).
For a more representative benchmark, place a larger parquet file named
hits.parquet in the current directory — it will be preferred automatically.

Run from any directory:
    python bench_vs_arrow_duckdb.py

Dependencies:
    pip install rugo pyarrow duckdb
"""
import os, sys, time, urllib.request, gc, resource, subprocess, importlib.util, glob

# If running from the opteryx-core source tree, prefer the local build.
_here = os.path.dirname(os.path.abspath(__file__))
_repo = os.path.join(_here, "..", "..")
if glob.glob(os.path.join(_repo, "rugo", "parquet_reader*.so")):
    sys.path.insert(0, os.path.abspath(_repo))

# ── fetch test file ───────────────────────────────────────────────────────────
_URL  = "https://raw.githubusercontent.com/mabel-dev/opteryx-core/main/testdata/astronauts/astronauts.parquet"
_FILE = "astronauts.parquet"

if os.path.exists("hits.parquet"):
    _FILE = "hits.parquet"
elif not os.path.exists(_FILE):
    print(f"downloading {_URL} ...")
    urllib.request.urlretrieve(_URL, _FILE)

COLS = ["WatchID", "CounterID", "EventDate", "UserID", "URL", "Title"]
REPS = 5

# ── helpers ───────────────────────────────────────────────────────────────────
def _median(xs):
    s = sorted(xs); n = len(s)
    return s[n//2] if n % 2 else (s[n//2-1] + s[n//2]) / 2

def _rss_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e6  # macOS returns bytes; Linux returns kilobytes

def _measure(fn):
    """(median_s, peak_rss_delta_mb, rows)"""
    times, peaks = [], []
    rows = 0
    for _ in range(REPS):
        gc.collect()
        before = _rss_mb()
        t0 = time.perf_counter()
        rows = fn()
        elapsed = time.perf_counter() - t0
        after = _rss_mb()
        times.append(elapsed)
        peaks.append(max(0.0, after - before))
    return _median(times), _median(peaks), rows

def _pkg_size_mb(name):
    spec = importlib.util.find_spec(name)
    if not spec: return None
    root = (list(spec.submodule_search_locations) or [spec.origin])[0]
    return sum(
        os.path.getsize(os.path.join(dp, f))
        for dp, _, files in os.walk(root) for f in files
    ) / 1e6

def _cold_import_ms(pkg):
    """Measure cold import in a fresh interpreter subprocess."""
    r = subprocess.run(
        [sys.executable, "-c",
         f"import time; t=time.perf_counter(); import {pkg}; print(f'{{(time.perf_counter()-t)*1000:.0f}}')"],
        capture_output=True, text=True, timeout=30
    )
    try:
        return float(r.stdout.strip())
    except Exception:
        return None

# ── benchmarks ────────────────────────────────────────────────────────────────
results = {}

try:
    from rugo.parquet_reader import read_parquet_from_path
    def _rugo_mmap():
        return sum(m.num_rows for m in read_parquet_from_path(_FILE, column_names=COLS))
    results["rugo (mmap)"] = _measure(_rugo_mmap) + (_pkg_size_mb("rugo"),)
except Exception as e:
    results["rugo (mmap)"] = (None, None, 0, _pkg_size_mb("rugo"), str(e))

try:
    from rugo.parquet_reader import read_parquet
    with open(_FILE, "rb") as f: _raw = f.read()
    def _rugo_bytes():
        return sum(m.num_rows for m in read_parquet(_raw, column_names=COLS))
    results["rugo (decode)"] = _measure(_rugo_bytes) + (_pkg_size_mb("rugo"),)
except Exception as e:
    results["rugo (decode)"] = (None, None, 0, _pkg_size_mb("rugo"), str(e))

try:
    import pyarrow.parquet as pq
    pq.read_table(_FILE, columns=COLS)  # warm OS cache
    def _pyarrow():
        return len(pq.read_table(_FILE, columns=COLS))
    results["pyarrow"] = _measure(_pyarrow) + (_pkg_size_mb("pyarrow"),)
except ImportError:
    results["pyarrow"] = (None, None, 0, None, "not installed")
except Exception as e:
    results["pyarrow"] = (None, None, 0, _pkg_size_mb("pyarrow"), str(e))

try:
    import duckdb
    col_list = ", ".join(COLS)
    duckdb.query(f"SELECT {col_list} FROM read_parquet('{_FILE}')").fetchall()
    def _duckdb():
        return len(duckdb.query(f"SELECT {col_list} FROM read_parquet('{_FILE}')").fetchall())
    results["duckdb"] = _measure(_duckdb) + (_pkg_size_mb("duckdb"),)
except ImportError:
    results["duckdb"] = (None, None, 0, None, "not installed")
except Exception as e:
    results["duckdb"] = (None, None, 0, _pkg_size_mb("duckdb"), str(e))

# ── cold import times (subprocess) ───────────────────────────────────────────
print("measuring cold import times (subprocess)...")
import_ms = {
    "rugo":    _cold_import_ms("rugo"),
    "pyarrow": _cold_import_ms("pyarrow"),
    "duckdb":  _cold_import_ms("duckdb"),
}

# ── report ────────────────────────────────────────────────────────────────────
file_mb = os.path.getsize(_FILE) / 1e6
print(f"\nParquet read benchmark  ({file_mb:.1f} MB file, {REPS} reps, median)")
print(f"file:    {os.path.abspath(_FILE)}")
print(f"columns: {COLS}")

print(f"\n{'library':<22}  {'time (s)':>9}  {'peak RSS Δ':>11}  {'rows':>10}")
print("-" * 60)
for lib, v in results.items():
    t, mem, rows = v[0], v[1], v[2]
    err = v[4] if len(v) > 4 else None
    if t is None:
        print(f"  {lib:<20}  {'n/a':>9}  {'n/a':>11}  {err}")
    else:
        print(f"  {lib:<20}  {t:>9.3f}  {mem:>10.1f}M  {rows:>10,}")

print(f"\n{'library':<22}  {'install size':>13}  {'cold import':>12}")
print("-" * 52)
shown = set()
for lib, v in results.items():
    pkg = lib.split()[0]
    if pkg in shown: continue
    shown.add(pkg)
    pkg_mb = v[3]
    imp = import_ms.get(pkg)
    size_s = f"{pkg_mb:.1f} MB" if pkg_mb else "n/a"
    imp_s  = f"{imp:.0f} ms"   if imp     else "n/a"
    print(f"  {lib:<20}  {size_s:>13}  {imp_s:>12}")

print()
pa = results.get("pyarrow", (None,))
rm = results.get("rugo (mmap)", (None,))
if pa[0] and rm[0]:
    faster = rm[0] < pa[0]
    ratio  = pa[0]/rm[0] if faster else rm[0]/pa[0]
    print(f"speed:   rugo mmap is {ratio:.1f}x {'faster' if faster else 'slower'} than pyarrow")
if pa[1] is not None and rm[1] is not None and pa[1] > 0 and rm[1] > 0:
    r = pa[1]/rm[1]
    print(f"memory:  rugo uses {r:.1f}x {'less' if r > 1 else 'more'} peak RSS than pyarrow")
elif rm[1] is not None and pa[1] is not None:
    print(f"memory:  rugo Δ={rm[1]:.1f}M  pyarrow Δ={pa[1]:.1f}M")
if pa[3] and rm[3]:
    print(f"size:    rugo is {pa[3]/rm[3]:.1f}x smaller install than pyarrow")
imp_r = import_ms.get("rugo"); imp_p = import_ms.get("pyarrow")
if imp_r and imp_p:
    print(f"import:  rugo loads {imp_p/imp_r:.1f}x faster than pyarrow ({imp_r:.0f} ms vs {imp_p:.0f} ms)")
