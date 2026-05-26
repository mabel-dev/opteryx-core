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

start = 0
end = len(runner.STATEMENTS) - 1

print("Starting bisect over %d statements" % (end + 1))

while start < end:
    mid = (start + end) // 2
    print(f"Testing range [{start}, {mid}]")
    cmd = [PY, "dev/click_range.py", str(start), str(mid)]
    p = subprocess.Popen(cmd, env=ASAN_ENV)
    rc = p.wait()
    if rc == 0:
        # left half OK -> failing query in right half
        start = mid + 1
    else:
        # failure in left half
        end = mid

print("Bisect result index:", start)
# run final single test to get ASAN output
cmd = [PY, "dev/click_range.py", str(start), str(start)]
print("Running final single test with ASAN to capture report: ", cmd)
subprocess.call(cmd, env=ASAN_ENV)
