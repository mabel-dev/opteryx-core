import os
import subprocess
import sys

sys.path.insert(0, os.getcwd())
from tests.performance.clickbench.opteryx import runner

PY = "/Users/justin/.pyenv/versions/3.13.12/bin/python3"
DYLD = "/Library/Developer/CommandLineTools/usr/lib/clang/21/lib/darwin/libclang_rt.asan_osx_dynamic.dylib"
ASAN_ENV = dict(os.environ)
ASAN_ENV["DYLD_INSERT_LIBRARIES"] = DYLD
ASAN_ENV["ASAN_OPTIONS"] = "detect_leaks=0"
ASAN_ENV["OPTERYX_DEBUG"] = "1"

n = len(runner.STATEMENTS)
for i in range(n):
    print(f"Running query {i:02d}")
    cmd = [PY, "dev/click_range.py", str(i), str(i)]
    p = subprocess.run(cmd, env=ASAN_ENV, capture_output=True, text=True)
    out = p.stdout
    err = p.stderr
    rc = p.returncode
    print(out)
    if rc == 0:
        print(f"OK {i:02d}\n")
        continue
    if rc == 2:
        print(f"INCONCLUSIVE (Parquet decode) {i:02d}\n")
        continue
    # rc == 1 or crash
    # Check stderr for ASAN heap-use-after-free
    if "heap-use-after-free" in err or "AddressSanitizer" in err:
        print("ASAN report for index", i)
        print(err)
        sys.exit(0)
    # Print both for inspection and continue searching
    print("Non-ASAN failure at index", i, "rc=", rc)
    print("STDERR:\n", err)
    print("STDOUT:\n", out)
    # continue scanning
print("Scan complete; no ASAN heap-use-after-free found in individual queries")
