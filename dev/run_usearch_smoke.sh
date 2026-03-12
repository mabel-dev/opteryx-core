#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_BIN="/tmp/usearch_smoke"

clang++ \
  -O2 \
  -std=c++20 \
  -DUSEARCH_USE_SIMSIMD=1 \
  -I"$ROOT_DIR/third_party/usearch/include" \
  -I"$ROOT_DIR/third_party/usearch/fp16/include" \
  -I"$ROOT_DIR/third_party/usearch/simsimd/include" \
  "$ROOT_DIR/dev/usearch_smoke.cpp" \
  -o "$OUT_BIN"

"$OUT_BIN"
