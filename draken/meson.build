# Draken package build configuration
# Defines 22 compiled extensions: 18 core + 4 additional
#
# All extensions are compiled directly from .pyx source files.

# Architecture detection
cpu_family = host_machine.cpu_family()
os = host_machine.system()

# Include directories
inc = include_directories(
  'src',
  'src/interop',
  'src/core',
  '../src/cpp',
  '..',                           # resolves draken/core/*.pxd, draken/vectors/*.pxd etc.
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
# A. CORE DRAKEN EXTENSIONS (22) - Vector types, Morsels, Interop
# ============================================================================

# Interop extensions (2)
python.extension_module('opteryx_compiled_draken_interop_arrow',
  'interop/arrow.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_interop_vector_sequence',
  'interop/vector_sequence.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

# Vector extensions (17)
python.extension_module('opteryx_compiled_draken_vectors_vector',
  'vectors/vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_bool_vector',
  'vectors/bool_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_float64_vector',
  'vectors/float64_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_float32_vector',
  'vectors/float32_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_int64_vector',
  'vectors/int64_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_integer_vector',
  'vectors/integer_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_date32_vector',
  'vectors/date32_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_timestamp_vector',
  'vectors/timestamp_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_time_vector',
  'vectors/time_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_interval_vector',
  'vectors/interval_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_array_vector',
  'vectors/array_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_vector_vector',
  'vectors/vector_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_scalar_constructors',
  'vectors/scalar_constructors.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors__decimal_vector',
  'vectors/_decimal_vector.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors_string_vector',
  'vectors/string_vector.pyx',
  sources: simd_sources + ['../src/cpp/simd_string_ops.cpp'],
  include_directories: inc,
  override_options: ['cython_language=cpp'],
  cpp_args: ['-DXXH_INLINE_ALL=1'],
)

python.extension_module('opteryx_compiled_draken_vectors_arithmetic_kernels',
  'vectors/arithmetic_kernels.pyx',
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_vectors__hash_api',
  'vectors/_hash_api.pyx',
  sources: [
    '../src/cpp/simd_hash.cpp',
    '../src/cpp/simd_bitops.cpp',
    '../src/cpp/simd_env.cpp',
    '../src/cpp/cpu_features.cpp',
  ],
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

# Data structure extensions (3)
python.extension_module('opteryx_compiled_draken_morsels_morsel',
  'morsels/morsel.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_morsels_align',
  'morsels/align.pyx',
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

python.extension_module('opteryx_compiled_draken_storage_morsel_io',
  'storage/morsel_io.pyx',
  sources: simd_sources,
  include_directories: inc,
  override_options: ['cython_language=cpp'],
)

# ============================================================================
# Python package installation
# ============================================================================
# Note: Python source file installation is handled by mesonpy build backend
