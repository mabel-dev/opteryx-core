#!/bin/bash
#
# arch_tree.sh - copy this working tree's SOURCES into an isolated benchmark tree,
# locally or on a remote box, link the benchmark datasets into it, and build it.
#
# Benchmarks that swap extension modules (dev/ab_bench.py --arms ... "so") must
# not run in the shared working tree: other sessions build and import there.
#
# Usage: dev/arch_tree.sh <dest dir> [user@host]
#   DATA_ROOT    dir holding scratch/ and testdata/ dataset dirs on the target
#                (default: this repo, i.e. the local datasets)
#   DATASETS     space-separated dataset paths relative to DATA_ROOT to link
#   PYTHON       interpreter on the target (default: python3)
#   JOBS         build parallelism on the target (default: nproc)
#   SSH_KEY      ssh identity for the remote host (default: ~/.ssh/id_x86box)
#   NO_BUILD=1   sync only
#
# The target tree is wiped and recreated each run. Everything is source-only:
# .git, build outputs, .so files and the datasets themselves are excluded.

set -euo pipefail

dest="${1:?usage: arch_tree.sh <dest dir> [user@host]}"
host="${2:-}"
repo="$(cd "$(dirname "$0")/.." && pwd)"
: "${DATA_ROOT:=$repo}"
: "${DATASETS:=scratch/hits_rugo_262k testdata/tpch_1_skene testdata/tpch_1_skene.skene-v3 testdata/tpch_10_skene testdata/tpch_10_skene.skene-v3 testdata/job_skene testdata/job_skene.skene-v3}"
: "${PYTHON:=python3}"
: "${SSH_KEY:=$HOME/.ssh/id_x86box}"

run() {
    if [ -n "$host" ]; then
        ssh -i "$SSH_KEY" -o BatchMode=yes "$host" "$@"
    else
        bash -c "$*"
    fi
}

case "$dest" in
    /*|~*) ;;
    *) echo "dest must be an absolute path" >&2; exit 1 ;;
esac
if [ -z "$host" ] && [ "$(cd "$dest" 2>/dev/null && pwd)" = "$repo" ]; then
    echo "refusing to use the working tree itself as the benchmark tree" >&2
    exit 1
fi

run "rm -rf '$dest' && mkdir -p '$dest'"

# COPYFILE_DISABLE: macOS tar otherwise writes AppleDouble ._* sidecars, which
# the scan then reads as corrupt parquet on Linux.
COPYFILE_DISABLE=1 tar -C "$repo" -cf - \
    --exclude=./.git --exclude=./scratch --exclude=./testdata --exclude=./build \
    --exclude=./target --exclude=./ClickBench --exclude=./.claude \
    --exclude=./.hypothesis --exclude=./.pytest_cache --exclude=./.ruff_cache \
    --exclude=./dev/bench_results --exclude='*.so' --exclude='*.dylib' --exclude='*.o' \
    --exclude='__pycache__' . |
    run "tar -xf - -C '$dest' 2>/dev/null; find '$dest' -name '._*' -delete"

for ds in $DATASETS; do
    run "test -e '$DATA_ROOT/$ds' || { echo 'missing dataset $DATA_ROOT/$ds' >&2; exit 1; }
         mkdir -p '$dest/$(dirname "$ds")' && ln -s '$DATA_ROOT/$ds' '$dest/$ds'"
done

if [ "${NO_BUILD:-0}" != 1 ]; then
    jobs="${JOBS:-$(run 'nproc 2>/dev/null || sysctl -n hw.ncpu')}"
    run "cd '$dest' && export PATH=\$HOME/.cargo/bin:\$PATH && '$PYTHON' setup.py build_ext --inplace -j $jobs > build.log 2>&1 || { tail -30 build.log; exit 1; }"
    run "cd '$dest' && PYTHONPATH='$dest' '$PYTHON' -c 'import opteryx, sys; assert opteryx.__file__.startswith(\"$dest\"), opteryx.__file__; print(\"built:\", opteryx.__file__, sys.version.split()[0])'"
fi
