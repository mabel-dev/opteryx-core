#!/bin/bash
#
# build_so_variant.sh - build ONE extension module of a benchmark tree with extra
# -D defines, save the result as a standalone .so, and put the tree's original
# module back. The saved .so is then an arm for dev/ab_bench.py --arms ("so").
#
# For A/B of compile-time constants without a second tree. Run it against a
# benchmark tree made by dev/arch_tree.sh, never the shared working tree.
#
# Usage: dev/build_so_variant.sh <tree> <out .so> <-DNAME=VALUE ...>
#   HOST     run on this ssh host instead of locally (tree/out are remote paths)
#   EXT      extension module path without suffix (default opteryx/operators/_operators)
#   TOUCH    source file to touch so setuptools rebuilds EXT (default $EXT.pyx; for a
#            C++-only extension such as draken/draken_native, a source it compiles)
#   PYTHON   interpreter on the target (default python3)
#   JOBS     build parallelism (default nproc)
#   SSH_KEY  default ~/.ssh/id_x86box
#
# Fails unless every define appears on a compile command in the build log
# (CFLAGS does not reach clang++; the defines go in via CPPFLAGS) and the
# rebuilt module differs from the original.

set -euo pipefail

tree="${1:?usage: build_so_variant.sh <tree> <out .so> -DNAME=VALUE ...}"
out="${2:?missing <out .so>}"
shift 2
[ $# -gt 0 ] || { echo "no -D defines given" >&2; exit 1; }
for d in "$@"; do
    case "$d" in -D*) ;; *) echo "not a -D define: $d" >&2; exit 1 ;; esac
done
: "${EXT:=opteryx/operators/_operators}"
: "${TOUCH:=$EXT.pyx}"
: "${PYTHON:=python3}"
: "${SSH_KEY:=$HOME/.ssh/id_x86box}"
defines="$*"

script=$(cat <<EOF
set -euo pipefail
cd '$tree'
export PATH=\$HOME/.cargo/bin:\$PATH
so=\$(ls $EXT.cpython-*.so)
# Snapshot EVERY in-place extension module: touching a shared source (e.g. a
# draken/simd TU) rebuilds every extension that compiles it, not just EXT, and
# all of them must go back to the original build afterwards.
stash=\$(mktemp -d)
find . \( -path ./build -o -path ./target \) -prune -o -name '*.so' -print > "\$stash/list"
while read -r f; do mkdir -p "\$stash/\$(dirname "\$f")"; cp -p "\$f" "\$stash/\$f"; done < "\$stash/list"
restore_all() {
    while read -r f; do
        if ! cmp -s "\$f" "\$stash/\$f"; then rm -f "\$f"; cp -p "\$stash/\$f" "\$f"; fi   # unlink-then-copy
        cmp -s "\$f" "\$stash/\$f" || { echo "restore of \$f failed — tree is on the WRONG binary" >&2; exit 1; }
    done < "\$stash/list"
}
log="variant-\$(basename '$out' .so).log"
touch '$TOUCH'
jobs=\${JOBS:-\$(nproc 2>/dev/null || sysctl -n hw.ncpu)}
if ! CPPFLAGS='$defines' '$PYTHON' setup.py build_ext --inplace -j \$jobs > "\$log" 2>&1; then
    tail -30 "\$log"; restore_all; exit 1
fi
for d in $defines; do
    grep -q -- "\$d" "\$log" || { echo "define \$d never reached a compile command (see \$log)" >&2; restore_all; exit 1; }
done
if cmp -s "\$so" "\$stash/./\$so"; then
    echo "rebuilt module is identical to the original — the defines changed nothing" >&2
    restore_all; exit 1
fi
changed=\$(while read -r f; do cmp -s "\$f" "\$stash/\$f" || echo "\$f"; done < "\$stash/list" | tr '\n' ' ')
mkdir -p "\$(dirname '$out')"
rm -f '$out'
cp "\$so" '$out'
restore_all
rm -rf "\$stash"
echo "variant: $out  ($defines)"
echo "  rebuilt (all restored): \$changed"
EOF
)

if [ -n "${HOST:-}" ]; then
    ssh -i "$SSH_KEY" -o BatchMode=yes "$HOST" "JOBS='${JOBS:-}' bash -s" <<<"$script"
else
    JOBS="${JOBS:-}" bash -c "$script"
fi
