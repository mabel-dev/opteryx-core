"""Build a real-embedding fixture for the vector-index measurements (VECTOR_INDEX_DESIGN §12).

Embeds every NVD vulnerability description (testdata.nvd, ~335k real English texts) with the
engine's own MiniLM capability — the same tokenizer, model and pooling the kernel uses — and
writes raw row-major fp16 rows. Also embeds a fixed list of natural-language queries.

Needs `pip install --no-deps "onnxruntime>=1.30,<2"` and OPTERYX_MINILM_MODEL_DIR. Output goes
outside the repo (the vectors are ~250 MB):

    python dev/vector_fixture.py [out_dir]     # default ~/.cache/opteryx/fixtures

Writes nvd_minilm.f16, nvd_minilm_queries.f16 and nvd_minilm.json (shape + throughput).
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(1, os.path.join(os.path.dirname(__file__), ".."))

import opteryx
from opteryx.compiled.nanobind import minilm_native
from opteryx.types.vectors.embedding_capability import install_minilm_capability

QUERIES = [
    "buffer overflow in an image parser",
    "SQL injection in a login form",
    "cross-site scripting in a web admin panel",
    "denial of service by sending crafted network packets",
    "privilege escalation through a kernel driver",
    "remote code execution via deserialization",
    "hard-coded credentials in router firmware",
    "path traversal allows reading arbitrary files",
    "use after free in a web browser",
    "authentication bypass in a VPN appliance",
    "integer overflow when decoding video",
    "information disclosure through verbose error messages",
    "cross-site request forgery changes account settings",
    "weak encryption of stored passwords",
    "XML external entity injection",
    "race condition in file permissions",
    "memory leak exhausts server memory",
    "command injection through a ping utility",
    "open redirect on a login page",
    "certificate validation is not performed",
]
BATCH = 64
THREADS = max(1, (os.cpu_count() or 2) - 2)


def main(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    capability = install_minilm_capability()
    dims = capability.dimensions

    morsels = list(opteryx.session().execute_to_morsels("SELECT description FROM testdata.nvd"))
    texts = [t for m in morsels for t in m.column("description").to_pylist()]
    if any(t is None for t in texts):
        raise ValueError("testdata.nvd has null descriptions; the fixture expects none")

    # Batch similar lengths together (padding is to the longest in a batch), then scatter
    # back so row i of the output is text i.
    order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
    batches = [order[i : i + BATCH] for i in range(0, len(order), BATCH)]
    row_bytes = dims * 2
    out = bytearray(len(texts) * row_bytes)

    def run(batch):
        block = minilm_native.embed_to_fp16_bytes([texts[i] for i in batch])
        for j, i in enumerate(batch):
            out[i * row_bytes : (i + 1) * row_bytes] = block[j * row_bytes : (j + 1) * row_bytes]

    start = time.perf_counter()
    with ThreadPoolExecutor(THREADS) as pool:
        list(pool.map(run, batches))
    elapsed = time.perf_counter() - start

    with open(os.path.join(out_dir, "nvd_minilm.f16"), "wb") as f:
        f.write(out)
    with open(os.path.join(out_dir, "nvd_minilm_queries.f16"), "wb") as f:
        f.write(minilm_native.embed_to_fp16_bytes(QUERIES))
    meta = {
        "rows": len(texts),
        "dims": dims,
        "queries": len(QUERIES),
        "identity": capability.identity,
        "threads": THREADS,
        "seconds": round(elapsed, 2),
        "rows_per_second": round(len(texts) / elapsed, 1),
    }
    with open(os.path.join(out_dir, "nvd_minilm.json"), "w") as f:
        json.dump(meta, f, indent=2)
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/.cache/opteryx/fixtures"))
