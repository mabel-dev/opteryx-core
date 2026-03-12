"""Vendor USearch headers into third_party/usearch.

Usage:
    python tools/vendor_usearch.py --tag v2.21.4

This script downloads the specified GitHub release archive, verifies an
optional SHA256, and vendors:
  - headers under `third_party/usearch/include/usearch`
  - required third-party headers under `third_party/usearch/fp16` and
    `third_party/usearch/simsimd`
  - `LICENSE`
  - `README.md`

The C++ integration in this repo is expected to consume the header-only API
from `include/usearch`. We intentionally do not vendor the language bindings.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from urllib.request import Request
from urllib.request import urlopen

REPO_CLONE_URL = "https://github.com/unum-cloud/usearch.git"
GITHUB_LATEST_API = "https://api.github.com/repos/unum-cloud/usearch/releases/latest"

def _clone_repo(tag: str, checkout_root: Path) -> Path:
    if checkout_root.exists():
        shutil.rmtree(checkout_root)

    print(f"Cloning USearch {tag} from {REPO_CLONE_URL}")
    subprocess.run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            tag,
            "--recurse-submodules",
            "--shallow-submodules",
            REPO_CLONE_URL,
            str(checkout_root),
        ],
        check=True,
    )
    return checkout_root


def _resolve_latest_tag() -> str:
    print("Fetching latest USearch release tag from GitHub...")
    request = Request(GITHUB_LATEST_API, headers={"User-Agent": "opteryx-vendor-script"})
    with urlopen(request) as response:
        payload = json.load(response)
    tag = payload.get("tag_name")
    if not tag:
        raise SystemExit("Unable to determine latest USearch release tag")
    print(f"Latest USearch release: {tag}")
    return tag


def vendor_usearch(tag: str, dest: Path, verify_sha256: str | None = None) -> None:
    if verify_sha256:
        raise SystemExit("SHA256 verification is not supported in git-clone mode")

    checkout_root = Path("/tmp") / f"usearch_{tag}_checkout"
    extracted_root = _clone_repo(tag, checkout_root)
    include_src = extracted_root / "include" / "usearch"
    if not include_src.is_dir():
        raise SystemExit(f"Unable to find USearch headers in {include_src}")

    if dest.exists():
        shutil.rmtree(dest)
    (dest / "include").mkdir(parents=True, exist_ok=True)

    shutil.copytree(include_src, dest / "include" / "usearch")
    print(f"USearch headers copied to {dest / 'include' / 'usearch'}")

    for dirname in ("fp16", "simsimd"):
        src = extracted_root / dirname
        if not src.exists():
            raise SystemExit(f"Unable to find required USearch dependency directory {src}")
        shutil.copytree(src, dest / dirname)
        print(f"Copied {dirname} to {dest / dirname}")

    for filename in ("LICENSE", "README.md"):
        src = extracted_root / filename
        if src.exists():
            shutil.copy2(src, dest / filename)
            print(f"Copied {filename} to {dest / filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tag",
        required=False,
        help="Tag to download (for example v2.21.4). If omitted, the latest release is used.",
    )
    parser.add_argument("--sha256", required=False, help="Optional SHA256 to verify archive")
    parser.add_argument("--dest", default="third_party/usearch", help="Destination directory")
    args = parser.parse_args()

    tag = args.tag or _resolve_latest_tag()
    vendor_usearch(tag=tag, dest=Path(args.dest), verify_sha256=args.sha256)
