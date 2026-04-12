"""Check presence of important vendored headers used for native bindings.

This script is intended to be run in CI before the build step to ensure required
headers (nanobind) are present in the repository.

Exit code 0 on success; non-zero on failure.
"""

import os
import sys

required_candidates = {
    "nanobind": [
        "third_party/nanobind/nanobind.h",
        "third_party/nanobind/nanobind/nanobind.h",
    ],
}

failed = False
for name, candidates in required_candidates.items():
    if not any(os.path.exists(p) for p in candidates):
        print(f"Missing vendored dependency: {name} -> expected one of: {candidates}")
        failed = True

if failed:
    sys.exit(1)

print("All vendored dependencies present.")
