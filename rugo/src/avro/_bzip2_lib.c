/* rugo/src/avro/_bzip2_lib.c — libbzip2 1.0.8 (third_party/bzip2, unmodified) as one
 * translation unit, so every build that carries the Avro reader compiles it the same
 * way with BZ_NO_STDIO set. Without BZ_NO_STDIO an internal consistency failure
 * prints and calls exit(3); with it libbzip2 calls bz_internal_error(), defined in
 * avro_container.cpp, and the reader raises. See third_party/bzip2/BZIP2_VERSION.txt. */
#define BZ_NO_STDIO 1

#include "blocksort.c"
#include "huffman.c"
#include "crctable.c"
#include "randtable.c"
#include "compress.c"
#include "decompress.c"
#include "bzlib.c"
