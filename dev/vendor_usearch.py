"""Vendor the fp16 conversion headers that ship with USearch into third_party/usearch.

Usage:
    python dev/vendor_usearch.py --tag v2.21.4

Only `fp16/` (plus USearch's LICENSE and README) is vendored: draken's fp16 <-> fp32
conversions (draken/core/fp16.h) use it. USearch's own index headers and SimSIMD are NOT
vendored — the vector index is IVF-flat in draken/ops/ann (D-5, 2026-10-02), which needs
neither.
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
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    for dirname in ("fp16",):
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
