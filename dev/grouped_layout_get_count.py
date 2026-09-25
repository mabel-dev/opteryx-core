"""Count range GETs per ClickBench query, per parquet layout, on the NATIVE scan path.

Serves each dataset directory over dev/throttle_server.py (no latency, no
bandwidth cap — this measures REQUEST COUNTS and bytes, not time) and registers
it as a workspace whose files are the served URLs, so the same SQL the
benchmark runs takes the production scan path (compiler -> open_native_scan_plan
-> ParquetIOPipeline over HTTP) and the pipeline's own telemetry reports the
ranges it issued: `io_http_request_count` (range GETs), `io_bytes_fetched`.

    python dev/grouped_layout_get_count.py \\
        --arm today256k=scratch/hits_rugo_262k \\
        --arm grouped64k=scratch/hits_grouped_64k \\
        --arm rowmajor64k=scratch/hits_rowmajor_64k \\
        [--queries 2,3,7,10,24] [--out results.json]

Dev tooling only — never imported by production code. The workspace shim lists
a LOCAL mirror directory and hands the scan `http://` URLs; every byte the scan
reads then goes over HTTP through the pipeline's fetch path.
"""

import argparse
import json
import os
import subprocess
import sys
import time

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests/performance/clickbench/opteryx"))
os.chdir(REPO)

SERVER = os.path.join(REPO, "dev/throttle_server.py")


def start_server(root):
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--root", root, "--port", "0", "--rtt-ms", "0",
         "--bandwidth-mbps", "0", "--error-rate", "0", "--seed", "1"],
        stdout=subprocess.PIPE, text=True,
    )
    ready = proc.stdout.readline()
    if not ready.startswith("READY"):
        proc.kill()
        raise RuntimeError(f"throttle server failed to start: {ready!r}")
    return proc, int(ready.strip().split("port=")[1])


def make_http_workspace(name, local_dir, port):
    """Register `name` as a workspace whose only dataset is `local_dir` served over HTTP."""
    from opteryx.connectors import FileSystemConnector, register_workspace
    from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem

    files = sorted(f for f in os.listdir(local_dir) if f.endswith(".parquet") and "manifest" not in f)
    urls = [f"http://127.0.0.1:{port}/{f}" for f in files]
    sizes = {u: os.path.getsize(os.path.join(local_dir, f)) for u, f in zip(urls, files)}

    class ServedDirFileSystem(OpteryxHttpFileSystem):
        # The native scan gate admits a remote path only when the filesystem can
        # sign it (an unsigned fetch would 401 against real object storage). The
        # served URLs need no credential, so "signing" is the identity — which is
        # exactly what routes the query onto the production native path instead
        # of the trampoline.
        signs_urls = True

        def rewrite_to_signed_url(self, path, expiry_seconds=3600):
            return path

        def list_files(self, base_dir, recursive=True):
            return list(urls)

        def get_file_size(self, path):
            return sizes[path]

        def get_file_info(self, paths):
            # The connector stats the data files and the (absent) dataset
            # manifest together; answer from the listing instead of HEADing a
            # relative manifest path the HTTP filesystem cannot resolve.
            from opteryx.connectors.io_systems.http_filesystem import FileInfo, FileType

            return [FileInfo(path=p, type=FileType.File, size=sizes[p]) if p in sizes
                    else FileInfo(path=p, type=FileType.NotFound) for p in paths]

    register_workspace(name, FileSystemConnector, filesystem=ServedDirFileSystem(),
                       storage_type="HTTP")
    return urls


def run_query(sql):
    import opteryx

    session = opteryx.session()
    rows = 0
    t0 = time.monotonic_ns()
    for m in session.execute_to_morsels(sql):
        rows += m.num_rows
    ms = (time.monotonic_ns() - t0) / 1e6
    tele = session.telemetry
    diags = tele.get("io_scan_diagnostics") or []
    out = {
        "rows": rows,
        "ms": round(ms, 1),
        "gets": sum(int(d.get("http_request_count", 0)) for d in diags),
        "fetch_ops": sum(int(d.get("http_fetch_ops", 0)) for d in diags),
        "bytes": sum(int(d.get("bytes_fetched", 0)) for d in diags),
        "retries": sum(int(d.get("http_retries", 0)) for d in diags),
    }
    session.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", action="append", required=True, help="name=dir")
    ap.add_argument("--queries", default="", help="comma-separated 1-based query numbers")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    import runner  # tests/performance/clickbench/opteryx/runner.py: STATEMENTS

    stmts = [(i + 1, s) for i, (s, _) in enumerate(runner.STATEMENTS)]
    if args.queries:
        want = {int(q) for q in args.queries.split(",")}
        stmts = [(i, s) for i, s in stmts if i in want]

    arms = []
    for spec in args.arm:
        name, d = spec.split("=", 1)
        arms.append((name, os.path.abspath(d)))

    results = {name: {} for name, _ in arms}
    servers = []
    try:
        for name, d in arms:
            proc, port = start_server(d)
            servers.append(proc)
            make_http_workspace(name, d, port)
            print(f"{name}: {d} on port {port}", flush=True)
        for qno, stmt in stmts:
            for name, _ in arms:
                sql = stmt.replace("{DATASET}", f"{name}.hits")
                try:
                    r = run_query(sql)
                except Exception as e:  # report, never hide
                    r = {"error": f"{type(e).__name__}: {e}"}
                results[name][qno] = r
                print(f"Q{qno:02d} {name:>14}: {r}", flush=True)
            if args.out:
                json.dump(results, open(args.out, "w"), indent=1)
    finally:
        for p in servers:
            p.kill()
            p.wait()

    print("\nTOTALS")
    for name, _ in arms:
        ok = [r for r in results[name].values() if "error" not in r]
        print(f"{name:>14}: GETs {sum(r['gets'] for r in ok):>8}  fetch_ops {sum(r['fetch_ops'] for r in ok):>7}  "
              f"GiB {sum(r['bytes'] for r in ok) / 2**30:8.2f}  errors {len(results[name]) - len(ok)}")


if __name__ == "__main__":
    main()
