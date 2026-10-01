# ONNX Runtime C API headers (vendored)

Source: https://github.com/microsoft/onnxruntime, tag `v1.30.0`,
`include/onnxruntime/core/session/`. Licence: MIT (`LICENSE`, copied from the same tag).

Headers only. Nothing here is compiled or linked: `src/cpp/minilm_native.cpp` loads the
ONNX Runtime shared library **at runtime** (`dlopen`) from the `onnxruntime` pip package,
installed with `pip install --no-deps "onnxruntime>=1.30,<2"` (only its native library is
used; its Python dependencies are never installed or imported), and calls it through the
stable C API (`OrtGetApiBase`). `ORT_API_VERSION` here is 30, so the installed library must
be 1.30 or newer.

SHA-256:
    e035e30c27e74c8c00e0f483e576e12b4067d17f12e9237fd6eff8b346c9b381  onnxruntime_c_api.h
    e6c986c9e98583f8113b2c6bc3864814883b806d501cf24da4d239c45753e235  onnxruntime_ep_c_api.h
    5ce3b054e798eced8d14f5b86e98692fd33470463f96194ce0700a2d53dd8721  onnxruntime_error_code.h
