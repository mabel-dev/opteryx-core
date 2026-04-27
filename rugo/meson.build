# Rugo package build configuration
# Defines 3 compiled extensions: parquet reader, JSONL reader, _JSONL reader

# Get Python module (re-import for local scope in subdir context)
py = import('python')

# Architecture detection
cpu_family = host_machine.cpu_family()
os = host_machine.system()

# Compiler argument variables (used per-extension to avoid global conflicts)
cpp_args = ['-O3', '-std=c++20']
c_args = ['-O3', '-std=c11']

# SIMD flags based on architecture
simd_args = []
if cpu_family == 'x86_64'
  simd_args += ['-msse4.2', '-mavx2', '-march=haswell']
elif cpu_family == 'arm'
  simd_args += ['-mfpu=neon']
endif
cpp_args += simd_args
c_args += simd_args

# Warning suppressions
warning_args = [
  '-Wno-unused-function',
  '-Wno-unreachable-code-fallthrough',
  '-Wno-sign-compare',
  '-Wno-unused-command-line-argument'
]
cpp_args += warning_args
c_args += warning_args

# Platform-specific flags
link_args = []
if os == 'linux'
  cpp_args += ['-fvisibility=default']
  link_args += ['-static-libstdc++', '-static-libgcc']
  link_args += ['-Wl,--no-as-needed', '-lcrypto', '-Wl,--as-needed']
elif os == 'darwin'
  link_args += ['-static-libstdc++']
endif

# Optional optimizations (handled by root project)

# Include directories
inc = include_directories(
  'src/h',
  'src/cpp',
  '../src/cpp',
  'src/parquet',
  '../draken/src',
  '../draken/src/core',
  '../third_party/bshoshany',
  '../third_party/moodycamel',
  '../third_party/fastfloat',
  '../third_party/fastfloat/fast_float',
  '../third_party/yyjson/src',
)

# ============================================================================
# VENDORED COMPRESSION LIBRARIES (Static linking)
# ============================================================================

# Zstd sources (vendored in src/parquet/vendor/zstd/)
zstd_sources = [
  'src/parquet/vendor/zstd/common/debug.cpp',
  'src/parquet/vendor/zstd/common/entropy_common.cpp',
  'src/parquet/vendor/zstd/common/error_private.cpp',
  'src/parquet/vendor/zstd/common/fse_decompress.cpp',
  'src/parquet/vendor/zstd/common/pool.cpp',
  'src/parquet/vendor/zstd/common/threading.cpp',
  'src/parquet/vendor/zstd/common/xxhash.cpp',
  'src/parquet/vendor/zstd/common/zstd_common.cpp',
  'src/parquet/vendor/zstd/decompress/huf_decompress.cpp',
  'src/parquet/vendor/zstd/decompress/zstd_ddict.cpp',
  'src/parquet/vendor/zstd/decompress/zstd_decompress.cpp',
  'src/parquet/vendor/zstd/decompress/zstd_decompress_block.cpp',
]

# Snappy sources (3 files)
snappy_sources = [
  '../third_party/snappy/snappy.cc',
  '../third_party/snappy/snappy-sinksource.cc',
  '../third_party/snappy/snappy-stubs-internal.cc',
]

# LZ4 sources (1 file)
lz4_sources = [
  '../third_party/lz4/lz4.c',
]

# ============================================================================
# A. PARQUET READER EXTENSION (1)
# ============================================================================

python.extension_module('opteryx_compiled_rugo_parquet',
  'src/parquet/parquet_reader.cpp',
  sources: [
    'src/parquet/decode.cpp',
    'src/parquet/decode_page.cpp',
    'src/parquet/decode_column.cpp',
    'src/parquet/decode_encodings.cpp',
    'src/parquet/page_value_decoder.cpp',
    'src/parquet/metadata.cpp',
    'src/parquet/compression.cpp',
    'src/parquet/bloom_filter.cpp',
    '../src/cpp/cpu_features.cpp',
  ] + zstd_sources + snappy_sources + lz4_sources,
  include_directories: inc,
  cpp_args: ['-DHAVE_SNAPPY=1', '-DHAVE_LZ4=1', '-DHAVE_ZSTD=1', '-DZSTD_STATIC_LINKING_ONLY=1'],
)

# ============================================================================
# B. JSONL READER EXTENSIONS (2)
# ============================================================================

# High-level JSONL reader (pre-generated C++ output)
python.extension_module('opteryx_compiled_rugo_jsonl',
  'src/jsonl/jsonl_reader.cpp',
  sources: [
    'src/jsonl/decode.cpp',
    '../third_party/yyjson/src/yyjson.c',
  ],
  include_directories: inc,
)

# Pure C++ JSONL reader (pre-generated C++ output)
python.extension_module('opteryx_compiled_rugo__jsonl',
  'src/_jsonl/_jsonl_reader.cpp',
  sources: [
    'src/_jsonl/core/column_builder.cpp',
    'src/_jsonl/core/field_span.cpp',
    'src/_jsonl/core/interpreter.cpp',
    'src/_jsonl/core/jsonl_reader.cpp',
    'src/_jsonl/core/structural_scan.cpp',
    'src/_jsonl/core/value_parser.cpp',
    '../src/cpp/cpu_features.cpp',
  ],
  include_directories: inc,
)

# ============================================================================
# Python package installation
# ============================================================================
# Note: Python source file installation is handled by mesonpy build backend
