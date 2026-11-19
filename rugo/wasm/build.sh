#!/usr/bin/env bash
# rugo wasm build. Needs emscripten (emcc/em++) on PATH. Run from anywhere.
#   dist/rugo.{mjs,wasm}          footer reader + column-chunk decode (the published bundle)
#   build/rugo_codecs.{mjs,wasm}  codec probe: rugo's page decompressors alone
set -euo pipefail
cd "$(dirname "$0")"
R=../..
mkdir -p dist build

LINK=(-sMODULARIZE=1 -sEXPORT_ES6=1 -sENVIRONMENT=web,node -sALLOW_MEMORY_GROWTH=1
      -sMAXIMUM_MEMORY=4GB -sFILESYSTEM=0 -sEXPORTED_RUNTIME_METHODS=HEAPU8)
EXP=_rugo_footer_length,_rugo_open,_rugo_close,_rugo_result,_rugo_result_len,_malloc,_free
EXP=$EXP,_rugo_chunk_offset,_rugo_chunk_length,_rugo_decode_column,_rugo_column_info,_rugo_column_release,_rugo_list_info
EXP=$EXP,_rugo_batch_rows,_rugo_batch_width,_rugo_batch_column,_rugo_batch_release,_rugo_avcol_info
EXP=$EXP,_rugo_orc_open,_rugo_orc_close,_rugo_orc_decode,_rugo_avro_open,_rugo_avro_close,_rugo_avro_decode
INC=(-I$R/rugo/src/parquet -I$R/rugo/src -I$R/third_party/ankerl -I$R -I$R/draken -I$R/draken/core
     -I$R/draken/simd -I$R/src/cpp -I$R/third_party/zstd -I$R/third_party/zstd/common
     -I$R/third_party/snappy -I$R/third_party/lz4 -I$R/third_party/libdeflate -I$R/third_party/cyan4973 -I$R/third_party/mabel/carchar -I$R/third_party/mabel -I$R/third_party/yyjson/src -I$R/third_party/bzip2 -I$R/third_party/fastfloat -I$R/third_party/jkjeon/include -I$R/third_party/miniz)
CXX=(-O3 -std=c++20 -fwasm-exceptions "${INC[@]}")

# vendored codecs + the ORC/Avro readers' C libraries (the same sources build_common.py
# compiles for rugo_native)
C=(-O3 -I$R/third_party/libdeflate -I$R/third_party/lz4 -I$R/third_party/bzip2 -I$R/third_party/mabel/base64)
for c in $R/third_party/libdeflate/lib/{deflate_decompress,gzip_decompress,crc32,utils}.c $R/third_party/lz4/lz4.c \
         $R/rugo/src/avro/_bzip2_lib.c $R/third_party/yyjson/src/yyjson.c \
         $R/third_party/mabel/base64/{_base64,_base64_dispatch,_base64_neon,_base64_avx2}.c; do
  emcc "${C[@]}" -c "$c" -o build/$(basename "$c" .c).o
done
ZSTD=$(cd $R && python3 -c "import build_common as b; print(' '.join(s for s in b.get_zstd_vendor_sources() if s.endswith('.cpp')))")
CODECS=($R/rugo/src/parquet/compression.cpp $R/third_party/snappy/{snappy,snappy-sinksource,snappy-stubs-internal}.cc
        $(for s in $ZSTD; do echo $R/$s; done) build/*.o)

# footer + column-chunk decode: rugo's own sources, unchanged
P=$R/rugo/src/parquet
em++ "${CXX[@]}" rugo_wasm.cpp $P/metadata.cpp $P/decode_column.cpp $P/decode_page.cpp $P/decode_encodings.cpp \
  $P/column_materialize.cpp $R/draken/core/chunk_cache.cpp $R/draken/core/mem_account.cpp "${CODECS[@]}" \
  $R/rugo/src/orc/orc_reader.cpp $R/rugo/src/avro/{avro_reader,avro_schema,avro_container}.cpp \
  -o dist/rugo.mjs "${LINK[@]}" -sEXPORTED_FUNCTIONS=$EXP

# codec probe: decompressors alone, driven directly by test_codecs.mjs
em++ "${CXX[@]}" rugo_codecs_probe.cpp "${CODECS[@]}" \
  -o build/rugo_codecs.mjs "${LINK[@]}" -sEXPORTED_FUNCTIONS=_rugo_decompress,_malloc,_free

for f in dist/rugo.wasm build/rugo_codecs.wasm; do
  echo "$f raw=$(wc -c <$f) gzip9=$(gzip -9 -c $f | wc -c) brotli11=$(brotli -c -q 11 $f | wc -c)"
done
