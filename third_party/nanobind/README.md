This folder contains the vendored nanobind headers (required for builds).

To vendor the official nanobind headers, run the helper script:

    python tools/vendor_nanobind.py --tag <tag>

(Example: `python tools/vendor_nanobind.py --tag v2.11.0`)

Files will be placed under `third_party/nanobind/nanobind/*.h` and `nanobind/stl/*`.

Note: The repository **requires** these vendored headers for reproducible builds. If
`third_party/nanobind/nanobind.h` is not present the build will fail with a clear
message directing you to vendor the headers.
