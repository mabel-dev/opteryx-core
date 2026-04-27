# Draken package build configuration
# Defines 22 compiled extensions: 18 core + 4 additional
#
# Strategy: build from pre-generated Cython C++ output files (.cpp) directly.
# Meson's Cython→C++ pipeline incorrectly treats --cplus output as C.
# The pre-generated .cpp files in src/ are correct, tested, ready to compile.

# Architecture detection
cpu_family = host_machine.cpu_family()
os = host_machine.system()

# Include directories
inc = include_directories(
  'src',
  'src/h',
  'src/cpp',
  'src/interop',
  'src/core',
  '../src/cpp',
  '..',                           # resolves opteryx/compiled/draken/core/*.pxd references
  '../third_party/mabel/carchar',
  '../third_party/mabel/parvi',
  '../third_party/cyan4973',
  '../third_party/bshoshany',
  '../third_party/moodycamel',
)

# Shared SIMD sources included in most Draken extensions
simd_sources = [
  '../src/cpp/simd_hash.cpp',
  '../src/cpp/simd_bitops.cpp',
  '../src/cpp/simd_env.cpp',
  '../src/cpp/simd_search.cpp',
  '../src/cpp/cpu_features.cpp',
]

# ============================================================================
# A. CORE DRAKEN EXTENSIONS (18) - Vector types, Morsels, Interop
# ============================================================================

# Interop extensions (2)
python.extension_module('opteryx_compiled_draken_interop_arrow',
  'src/interop/arrow.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_interop_vector_sequence',
  'src/interop/vector_sequence.cpp',
  sources: simd_sources,
  include_directories: inc,
)

# Vector extensions (13)
python.extension_module('opteryx_compiled_draken_vectors_vector',
  'src/vectors/vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_bool_vector',
  'src/vectors/bool_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_float64_vector',
  'src/vectors/float64_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_float32_vector',
  'src/vectors/float32_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_int64_vector',
  'src/vectors/int64_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_integer_vector',
  'src/vectors/integer_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_date32_vector',
  'src/vectors/date32_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_timestamp_vector',
  'src/vectors/timestamp_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_time_vector',
  'src/vectors/time_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_interval_vector',
  'src/vectors/interval_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_array_vector',
  'src/vectors/array_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_vector_vector',
  'src/vectors/vector_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors_scalar_constructors',
  'src/vectors/scalar_constructors.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_vectors__decimal_vector',
  'src/vectors/_decimal_vector.cpp',
  sources: simd_sources,
  include_directories: inc,
)

# Data structure extensions (2)
python.extension_module('opteryx_compiled_draken_morsels_morsel',
  'src/morsels/morsel.cpp',
  sources: simd_sources,
  include_directories: inc,
)

python.extension_module('opteryx_compiled_draken_storage_morsel_io',
  'src/storage/morsel_io.cpp',
  sources: simd_sources,
  include_directories: inc,
)

# ============================================================================
# B. ADDITIONAL DRAKEN EXTENSIONS (4)
# ============================================================================

# String vector with extended SIMD helpers
# XXH_INLINE_ALL: inline xxhash functions rather than linking a separate object
# Also needs simd_string_ops.cpp for simd_to_lower etc.
python.extension_module('opteryx_compiled_draken_vectors_string_vector',
  'src/vectors/string_vector.cpp',
  sources: simd_sources + ['../src/cpp/simd_string_ops.cpp'],
  include_directories: inc,
  cpp_args: ['-DXXH_INLINE_ALL=1'],
)

# Arithmetic kernels (pre-generated Cython C output)
python.extension_module('opteryx_compiled_draken_vectors_arithmetic_kernels',
  'src/vectors/arithmetic_kernels.c',
  include_directories: inc,
)

# Align helper (pre-generated Cython C output)
python.extension_module('opteryx_compiled_draken_morsels_align',
  'src/morsels/align.c',
  include_directories: inc,
)

# Hash API shim
python.extension_module('opteryx_compiled_draken_vectors__hash_api',
  'src/vectors/_hash_api.cpp',
  sources: [
    '../src/cpp/simd_hash.cpp',
    '../src/cpp/simd_bitops.cpp',
    '../src/cpp/simd_env.cpp',
    '../src/cpp/cpu_features.cpp',
  ],
  include_directories: inc,
)

# ============================================================================
# Python package installation
# ============================================================================
# Note: Python source file installation is handled by mesonpy build backend
