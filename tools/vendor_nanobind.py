"""Vendor nanobind headers and runtime sources into third_party/nanobind.

Usage:
    python tools/vendor_nanobind.py --tag v0.8.3

This script will download the specified release from GitHub, verify an optional
SHA256, and vendor:
  - headers under `third_party/nanobind/nanobind`
  - runtime sources under `third_party/nanobind/src`
  - robin_map headers under `third_party/nanobind/ext/robin_map/include`

Note: Running this script is not automatic in CI; prefer checking vendored
files into the repo for reproducible builds. This script exists to make it
easier to pull upstream updates.
"""

import argparse
import hashlib
import os
import shutil
import subprocess
import tarfile
from urllib.request import urlopen

REPO_ARCHIVE_URL = "https://github.com/wjakob/nanobind/archive/refs/tags/{tag}.tar.gz"
GITHUB_LATEST_API = "https://api.github.com/repos/nanobind/nanobind/releases/latest"


def download_and_extract(tag: str, dest: str, verify_sha256: str | None = None):
    url = REPO_ARCHIVE_URL.format(tag=tag)
    print(f"Downloading nanobind {tag} from {url}")

    tmp_tar = f"/tmp/nanobind_{tag}.tar.gz"
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

    print("Extracting nanobind archive...")
    with tarfile.open(tmp_tar, "r:gz") as tar:
        tar.extractall(path="/tmp")

    # Find extracted basedir
    base = next((d for d in os.listdir('/tmp') if d.startswith('nanobind-') or d.startswith('nanobind_')), None)
    if base is None:
        raise SystemExit("Unable to find extracted nanobind files in /tmp")

    if os.path.exists(dest):
        shutil.rmtree(dest)
    os.makedirs(dest, exist_ok=True)

    include_dir = os.path.join('/tmp', base, 'include', 'nanobind')
    if not os.path.isdir(include_dir):
        raise SystemExit(f"Unable to find nanobind headers in {include_dir}")
    shutil.copytree(include_dir, os.path.join(dest, 'nanobind'))
    print(f"Nanobind headers copied to {os.path.join(dest, 'nanobind')}")

    src_dir = os.path.join('/tmp', base, 'src')
    if not os.path.isdir(src_dir):
        raise SystemExit(f"Unable to find nanobind runtime sources in {src_dir}")
    shutil.copytree(src_dir, os.path.join(dest, 'src'))
    print(f"Nanobind runtime sources copied to {os.path.join(dest, 'src')}")

    print("Fetching robin_map submodule required by nanobind runtime...")
    subprocess.run(
        ["git", "-C", os.path.join('/tmp', base), "submodule", "update", "--init", "--depth", "1"],
        check=True,
    )

    robin_include = os.path.join('/tmp', base, 'ext', 'robin_map', 'include')
    if not os.path.isdir(robin_include):
        raise SystemExit(f"Unable to find robin_map includes in {robin_include}")
    robin_dest = os.path.join(dest, 'ext', 'robin_map', 'include')
    os.makedirs(os.path.dirname(robin_dest), exist_ok=True)
    shutil.copytree(robin_include, robin_dest)
    print(f"robin_map headers copied to {robin_dest}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tag', required=False, help='Tag to download (e.g. v0.8.3). If omitted, the latest release will be used.')
    parser.add_argument('--sha256', required=False, help='Optional SHA256 to verify archive')
    parser.add_argument('--dest', default='third_party/nanobind', help='Destination directory')

    args = parser.parse_args()

    tag = args.tag
    if not tag:
        print("Fetching latest release tag from GitHub...")
        # Use GitHub API to find latest release tag
        import json
        from urllib.request import Request
        from urllib.request import urlopen

        req = Request(GITHUB_LATEST_API, headers={"User-Agent": "opteryx-vendor-script"})
        with urlopen(req) as r:
            data = json.load(r)
            tag = data.get('tag_name')
            if not tag:
                raise SystemExit("Unable to determine latest nanobind tag from GitHub API")
        print(f"Latest nanobind release: {tag}")

    download_and_extract(tag, args.dest, args.sha256)
