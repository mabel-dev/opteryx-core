#!/usr/bin/env bash
# RUGO-1 spike build. Needs emscripten (emcc/em++) on PATH. Run from anywhere.
#   dist/rugo.{mjs,wasm}          footer reader (the PoC)
#   build/rugo_codecs.{mjs,wasm}  size probe: footer reader + rugo's page decompressors
set -euo pipefail
cd "$(dirname "$0")"
R=../..
mkdir -p dist build

LINK=(-sMODULARIZE=1 -sEXPORT_ES6=1 -sENVIRONMENT=web,node -sALLOW_MEMORY_GROWTH=1
      -sMAXIMUM_MEMORY=4GB -sFILESYSTEM=0 -sEXPORTED_RUNTIME_METHODS=HEAPU8)
EXP=_rugo_footer_length,_rugo_parse_footer,_rugo_result,_rugo_result_len,_malloc,_free
CXX=(-O3 -std=c++20 -fwasm-exceptions -I$R/rugo/src/parquet -I$R/rugo/src -I$R/third_party/ankerl)

em++ "${CXX[@]}" rugo_wasm.cpp $R/rugo/src/parquet/metadata.cpp \
  -o dist/rugo.mjs "${LINK[@]}" -sEXPORTED_FUNCTIONS=$EXP

# --- codec size probe (same vendored sources build_common.py compiles) ---
C=(-O3 -I$R/third_party/libdeflate -I$R/third_party/lz4)
for c in $R/third_party/libdeflate/lib/{deflate_decompress,gzip_decompress,crc32,utils}.c $R/third_party/lz4/lz4.c; do
  emcc "${C[@]}" -c "$c" -o build/$(basename "$c" .c).o
done
ZSTD=$(cd $R && python3 -c "import build_common as b; print(' '.join(s for s in b.get_zstd_vendor_sources() if s.endswith('.cpp')))")
em++ "${CXX[@]}" -I$R/draken -I$R/third_party/zstd -I$R/third_party/zstd/common -I$R/third_party/snappy \
  -I$R/third_party/lz4 -I$R/third_party/libdeflate \
  rugo_wasm.cpp rugo_codecs_probe.cpp $R/rugo/src/parquet/metadata.cpp $R/rugo/src/parquet/compression.cpp \
  $R/third_party/snappy/{snappy,snappy-sinksource,snappy-stubs-internal}.cc \
  $(for s in $ZSTD; do echo $R/$s; done) build/*.o \
  -o build/rugo_codecs.mjs "${LINK[@]}" -sEXPORTED_FUNCTIONS=$EXP,_rugo_decompress

for f in dist/rugo.wasm build/rugo_codecs.wasm; do
  echo "$f raw=$(wc -c <$f) gzip9=$(gzip -9 -c $f | wc -c) brotli11=$(brotli -c -q 11 $f | wc -c)"
done
