# Opteryx Monorepo - Root Meson Build Configuration
#
# This file orchestrates the build of three packages:
# 1. draken     - Columnar vector library (no dependencies)
# 2. rugo       - Parquet/JSONL reader (depends on draken)
# 3. opteryx    - SQL query engine (depends on draken, rugo)

project('opteryx-monorepo', 'cpp', 'c', 'cython',
        version: '0.7.0',
        default_options: ['cpp_std=c++20', 'c_std=c11'],
        license: 'Apache-2.0')

# Configuration options
enable_lto = get_option('enable_lto')
enable_pgo = get_option('enable_pgo')
pgo_phase = get_option('pgo_phase')

# Print configuration
message('=== Opteryx Monorepo Build Configuration ===')
message('Version: @0@'.format(meson.project_version()))
message('Architecture: @0@'.format(host_machine.cpu_family()))
message('OS: @0@'.format(host_machine.system()))
message('Compiler: @0@'.format(meson.get_compiler('cpp').get_id()))
message('LTO enabled: @0@'.format(enable_lto))
pgo_info = enable_pgo ? pgo_phase : 'disabled'
message('PGO enabled: @0@ (@1@)'.format(enable_pgo, pgo_info))
message('========================================')

# Get Python module (needed for all packages)
py = import('python')
python = py.find_installation()

# Architecture detection (needed for all packages)
cpu_family = host_machine.cpu_family()
os = host_machine.system()

# Global compiler arguments
add_global_arguments('-O3', language: ['cpp', 'c'])
add_global_arguments('-std=c++20', language: 'cpp')
add_global_arguments('-std=c11', language: 'c')

# Cython include path: repo root so pxd files resolve at opteryx/compiled/draken/core/*.pxd
add_project_arguments('-I' + meson.source_root(), language: 'cython')

# SIMD flags based on architecture
simd_args = []
if cpu_family == 'x86_64'
  simd_args += ['-msse4.2', '-mavx2', '-march=haswell']
elif cpu_family == 'arm'
  simd_args += ['-mfpu=neon']
endif
add_global_arguments(simd_args, language: ['cpp', 'c'])

# Warning suppressions
warning_args = [
  '-Wno-unused-function',
  '-Wno-unreachable-code-fallthrough',
  '-Wno-sign-compare',
  '-Wno-unused-command-line-argument'
]
add_global_arguments(warning_args, language: ['cpp', 'c'])

# Platform-specific flags
if os == 'linux'
  add_global_arguments('-fvisibility=default', language: 'cpp')
  add_global_link_arguments('-static-libstdc++', '-static-libgcc', language: 'cpp')
elif os == 'darwin'
  add_global_link_arguments('-static-libstdc++', language: 'cpp')
endif

# Optional optimizations
if enable_lto
  add_global_arguments('-flto', language: ['cpp', 'c'])
  add_global_link_arguments('-flto', language: ['cpp', 'c'])
endif

# Build subdirectories (no longer subprojects - flat structure)
message('Building Draken (columnar vector library)...')
subdir('draken')

message('Building Rugo (parquet/JSONL reader)...')
subdir('rugo')

message('Building Opteryx (SQL query engine)...')
subdir('opteryx')

message('========================================')
message('Build complete!')
message('  - Draken built')
message('  - Rugo built')
message('  - Opteryx built')
message('')
message('To run tests: meson test -C build')
message('To install: meson install -C build')
