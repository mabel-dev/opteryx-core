"""Recall@10 of the IVF index vs exact search on the NVD fixture, real MiniLM (dev tool).

Usage: python dev/vector_index_recall.py <dir holding nvd_index/v.skene and c.skene>
"""

import os, sys, time, statistics
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, REPO)
os.environ.setdefault("OPTERYX_MINILM_MODEL_DIR", os.path.expanduser("~/.cache/opteryx/all-MiniLM-L6-v2"))
from opteryx.types.vectors.embedding_capability import active_embedding_capability, install_minilm_capability
install_minilm_capability()
from draken.ops.kernels._kernel_registry import lookup_kernel
from rugo.parquet import read_metadata
from opteryx.operators._operators import search_vector_index_file

# Index files from a local build of the NVD fixture (dev/vector_fixture.py / build_vector_index_local).
S = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))
data = f"{REPO}/testdata/nvd/public_security_nvd_vulnerabilities_data_189846a19c45f3ef-2b98116318cb-2.parquet"
rows = read_metadata(data).num_rows
v, c = f"{S}/nvd_index/v.skene", f"{S}/nvd_index/c.skene"
vb, cb = os.path.getsize(v), os.path.getsize(c)
fn, _ = lookup_kernel("draken_embed"); dims = active_embedding_capability().dimensions
QUERIES = [
    "buffer overflow in a network service allows remote code execution",
    "SQL injection in a login form", "cross-site scripting in a web admin panel",
    "denial of service via crafted packet", "privilege escalation in the Linux kernel",
    "use after free in a web browser", "path traversal allows reading arbitrary files",
    "hardcoded credentials in router firmware", "integer overflow in an image parser",
    "authentication bypass in a VPN appliance", "cross-site request forgery in a CMS plugin",
    "XML external entity injection", "insecure deserialization in a Java application",
    "information disclosure through verbose error messages", "race condition in file handling",
    "memory leak leading to denial of service", "command injection in a router web interface",
    "weak cryptography in TLS implementation", "open redirect vulnerability",
    "server-side request forgery in a cloud service", "null pointer dereference crash",
    "heap overflow in a PDF reader", "WordPress plugin stored XSS",
    "unauthenticated remote code execution in Apache", "Microsoft Windows elevation of privilege",
    "Cisco IOS denial of service", "Adobe Flash Player memory corruption",
    "PHP application arbitrary file upload", "Oracle database unspecified vulnerability",
    "Android app leaks sensitive data", "format string vulnerability in a logging function",
    "session fixation in a web framework", "default password on IoT camera",
    "out-of-bounds read in a font library", "improper certificate validation",
    "LDAP injection", "clickjacking", "directory listing exposes backups",
    "timing attack on password comparison", "stack exhaustion through deep recursion",
]
def search(q, k, nprobe):
    t0 = time.perf_counter()
    hits, stats = search_vector_index_file(v, vb, c, cb, q, fn, dims, k, nprobe, rows)
    return [o for o, _ in hits], stats, time.perf_counter() - t0

K = 10
truth = {}
_, s0, _ = search(QUERIES[0], K, 1)
clusters = s0["clusters"]
t_exact = []
for q in QUERIES:
    ids, stats, dt = search(q, K, clusters)
    truth[q] = set(ids); t_exact.append(dt)
print(f"rows={rows} clusters={clusters} queries={len(QUERIES)} k={K}")
print(f"exact (nprobe={clusters}): {statistics.median(t_exact)*1000:.1f} ms/query median, rows scored {stats['rows_scored']}")
print("nprobe  recall@10  min_recall  ms/query  row_groups  rows_scored")
for nprobe in (1, 2, 4, 8, 16, 32, 64, 128):
    rec, times, rgs, scored = [], [], [], []
    for q in QUERIES:
        ids, stats, dt = search(q, K, nprobe)
        rec.append(len(truth[q] & set(ids)) / K); times.append(dt)
        rgs.append(stats["row_groups_read"]); scored.append(stats["rows_scored"])
    print(f"{nprobe:6d}  {statistics.mean(rec):9.3f}  {min(rec):10.2f}  {statistics.median(times)*1000:8.1f}  {statistics.mean(rgs):10.1f}  {statistics.mean(scored):11.0f}")
