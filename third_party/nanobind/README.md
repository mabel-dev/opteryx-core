This folder contains vendored nanobind files required for builds:

- headers under `nanobind/`
- runtime sources under `src/`
- robin_map headers under `ext/robin_map/include/`

To vendor official nanobind files, run the helper script:

    python tools/vendor_nanobind.py --tag <tag>

(Example: `python tools/vendor_nanobind.py --tag v2.11.0`)

Files will be placed under:
- `third_party/nanobind/nanobind/*`
- `third_party/nanobind/src/*`
- `third_party/nanobind/ext/robin_map/include/*`

Note: The repository **requires** these vendored files for reproducible builds.
If the expected nanobind paths are not present, the build will fail with a clear
message directing you to vendor them.
