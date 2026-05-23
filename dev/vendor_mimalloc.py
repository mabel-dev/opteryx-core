"""Vendor mimalloc headers and sources into third_party/mimalloc.

Usage:
    python dev/vendor_mimalloc.py --tag v3.3.2

This script will download the specified release from GitHub, verify an optional
SHA256, and vendor:
  - headers under `third_party/mimalloc/include`
  - sources under `third_party/mimalloc/src` (single-TU build via `src/static.c`)
  - the upstream LICENSE under `third_party/mimalloc/LICENSE`

The build integrates mimalloc through `src/static.c` (a single translation unit
that #includes every other `src/*.c` it needs); no upstream CMake is used. See
setup.py for the wiring — mimalloc is compiled into the new `draken` extensions
ONLY, with mimalloc's global malloc override left OFF (we call `mi_*` explicitly).

Note: Running this script is not automatic in CI; prefer checking vendored files
into the repo for reproducible builds. This script exists to make it easier to
pull upstream updates.
"""

import argparse
import hashlib
import os
import shutil
import tarfile
from urllib.request import urlopen

REPO_ARCHIVE_URL = "https://github.com/microsoft/mimalloc/archive/refs/tags/{tag}.tar.gz"
GITHUB_LATEST_API = "https://api.github.com/repos/microsoft/mimalloc/releases/latest"


def download_and_extract(tag: str, dest: str, verify_sha256: str | None = None):
    url = REPO_ARCHIVE_URL.format(tag=tag)
    print(f"Downloading mimalloc {tag} from {url}")

    tmp_tar = f"/tmp/mimalloc_{tag}.tar.gz"
    with urlopen(url) as r, open(tmp_tar, "wb") as f:
        shutil.copyfileobj(r, f)

    if verify_sha256:
        print("Verifying SHA256...")
        h = hashlib.sha256()
        with open(tmp_tar, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                h.update(chunk)
        digest = h.hexdigest()
        if digest != verify_sha256:
            raise SystemExit(f"SHA256 mismatch: got {digest}, expected {verify_sha256}")

    print("Extracting mimalloc archive...")
    with tarfile.open(tmp_tar, "r:gz") as tar:
        tar.extractall(path="/tmp")

    # Find extracted basedir
    base = next((d for d in os.listdir("/tmp") if d.startswith("mimalloc-")), None)
    if base is None:
        raise SystemExit("Unable to find extracted mimalloc files in /tmp")
    base_path = os.path.join("/tmp", base)

    if os.path.exists(dest):
        shutil.rmtree(dest)
    os.makedirs(dest, exist_ok=True)

    include_dir = os.path.join(base_path, "include")
    if not os.path.isdir(os.path.join(include_dir, "mimalloc")):
        raise SystemExit(f"Unable to find mimalloc headers in {include_dir}")
    shutil.copytree(include_dir, os.path.join(dest, "include"))
    print(f"mimalloc headers copied to {os.path.join(dest, 'include')}")

    src_dir = os.path.join(base_path, "src")
    if not os.path.isfile(os.path.join(src_dir, "static.c")):
        raise SystemExit(f"Unable to find mimalloc src/static.c in {src_dir}")
    shutil.copytree(src_dir, os.path.join(dest, "src"))
    print(f"mimalloc sources copied to {os.path.join(dest, 'src')}")

    license_src = os.path.join(base_path, "LICENSE")
    if not os.path.isfile(license_src):
        raise SystemExit(f"Unable to find mimalloc LICENSE in {base_path}")
    shutil.copy2(license_src, os.path.join(dest, "LICENSE"))
    print(f"mimalloc LICENSE copied to {os.path.join(dest, 'LICENSE')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tag",
        required=False,
        help="Tag to download (e.g. v3.3.2). If omitted, the latest release will be used.",
    )
    parser.add_argument("--sha256", required=False, help="Optional SHA256 to verify archive")
    parser.add_argument("--dest", default="third_party/mimalloc", help="Destination directory")

    args = parser.parse_args()

    tag = args.tag
    if not tag:
        print("Fetching latest release tag from GitHub...")
        import json
        from urllib.request import Request

        req = Request(GITHUB_LATEST_API, headers={"User-Agent": "opteryx-vendor-script"})
        with urlopen(req) as r:
            data = json.load(r)
            tag = data.get("tag_name")
            if not tag:
                raise SystemExit("Unable to determine latest mimalloc tag from GitHub API")
        print(f"Latest mimalloc release: {tag}")

    download_and_extract(tag, args.dest, args.sha256)
