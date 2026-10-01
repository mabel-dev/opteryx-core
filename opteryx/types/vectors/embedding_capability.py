# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The active embedding capability — the ONE place the embedding width is decided.

VECTOR is not a SQL type (architect ruling 2026-10-01): vectors exist only inside vector
indexes. Text reaches an embedding through COSINE_SIMILARITY / COSINE_DISTANCE / MATCH over
text (and, later, the vector index builder), all of which call the registered
`draken_embed` kernel.

The core kernel (draken/ops/kernels/function_vector_distance.cpp) is the static hashed
projection: part of the zero-dependency core, a total function of its input, therefore
ALWAYS present. It is deliberately lexical, not semantic.

A semantic embedder is an *installable capability* that registers its own `draken_embed`
over the core one. Capability registration is the ONLY sanctioned way to change what an
embedding means. There is no provider sniffing on the execution path and no fallback:
whatever is registered when a query BINDS is what runs.

Contract for a capability:
  - Register during startup, BEFORE any query that embeds is planned. Re-registering with
    a different width once the width has been planned raises.
  - `dimensions` is what the kernel WILL produce, every time, for every input.
  - `identity` names the exact model (for MiniLM, the SHA-256 of its weights), so that a
    persisted index can refuse to be queried with a different model.
"""

import hashlib
import importlib.util
import os
from dataclasses import dataclass
from pathlib import Path

from opteryx.exceptions import InvalidConfigurationError

# The core kernel's registry name. A capability replaces this entry.
_EMBED_KERNEL_NAME = "draken_embed"

# Width of the core static-hash embedding.
_CORE_DIMENSIONS = 256


@dataclass(frozen=True)
class EmbeddingCapability:
    """What an embedding currently means.

    `name` is for diagnostics. `identity` is the exact embedder: two capabilities with the
    same identity produce the same vector for the same text.
    """

    name: str
    dimensions: int
    identity: str


_CORE = EmbeddingCapability(
    name="static-hash", dimensions=_CORE_DIMENSIONS, identity=f"static-hash:{_CORE_DIMENSIONS}"
)

_active: EmbeddingCapability = _CORE
# Set once the width has been baked into a plan. After that it is load-bearing for
# compiled bytecode and cannot change under it.
_width_observed: bool = False


def active_embedding_capability() -> EmbeddingCapability:
    """The capability embeddings currently resolve to. Never None — the core is always there."""
    return _active


def embedding_dimensions() -> int:
    """Width the active capability produces. Called at plan time; marks the width as committed."""
    global _width_observed
    _width_observed = True
    return _active.dimensions


def register_embedding_capability(
    name: str, dimensions: int, kernel_ptr: int, identity: str
) -> None:
    """Install `kernel_ptr` as the embedding kernel, replacing the core static-hash one.

    Args:
        name: capability name, for diagnostics (e.g. "minilm-l6-v2").
        dimensions: the width this kernel produces — for every input, always.
        kernel_ptr: address of a `VecResult (*)(void*, const DrakenVector* const*, uint32_t)`
            C-ABI kernel. It must live for the process lifetime, and must honour the width
            handed to it in `vector_dim_ctx` or return an error sentinel.
        identity: the exact embedder (see EmbeddingCapability).

    Raises:
        InvalidConfigurationError: on a bad width, or on a width change after the width has
            already been planned.
    """
    global _active

    if not isinstance(dimensions, int) or isinstance(dimensions, bool) or not (
        1 <= dimensions <= 65535
    ):
        raise InvalidConfigurationError(
            config_item="embedding_capability.dimensions",
            provided_value=repr(dimensions),
            valid_value_description="an integer width between 1 and 65535.",
        )
    if kernel_ptr == 0:
        raise InvalidConfigurationError(
            config_item="embedding_capability.kernel_ptr",
            provided_value="0",
            valid_value_description="a non-null C-ABI kernel address.",
        )
    if _width_observed and dimensions != _active.dimensions:
        raise InvalidConfigurationError(
            config_item="embedding_capability",
            provided_value=f"{name} (width {dimensions})",
            valid_value_description=(
                f"a capability of width {_active.dimensions} — embeddings have already been "
                "planned at that width in this process. Register the capability during "
                "startup, before any query that embeds is planned."
            ),
        )

    from draken.ops.kernels._kernel_registry import register_kernel

    register_kernel(_EMBED_KERNEL_NAME, kernel_ptr)
    _active = EmbeddingCapability(name=name, dimensions=dimensions, identity=identity)


def _onnxruntime_library() -> Path:
    """The ONNX Runtime shared library inside the installed `onnxruntime` package.

    Located WITHOUT importing the package — only its native library is used. Install it with
    `pip install --no-deps "onnxruntime>=1.30,<2"`: its Python dependencies are never
    needed. The wheel ships exactly one runtime library in `capi/`
    (`libonnxruntime.so.<version>` on Linux, `libonnxruntime.<version>.dylib` on macOS).
    """
    from opteryx.exceptions import MissingDependencyError

    spec = importlib.util.find_spec("onnxruntime")
    if spec is None or not spec.submodule_search_locations:
        raise MissingDependencyError(
            "onnxruntime",
            hint="onnxruntime is not installed — install its native library with "
            '`pip install --no-deps "onnxruntime>=1.30,<2"` to use the MiniLM embedding '
            "capability.",
        )
    capi = Path(next(iter(spec.submodule_search_locations))) / "capi"
    candidates = sorted(capi.glob("libonnxruntime.so.*")) + sorted(
        capi.glob("libonnxruntime.*.dylib")
    )
    if len(candidates) != 1:
        raise MissingDependencyError(
            "onnxruntime",
            hint=f"expected exactly one onnxruntime runtime library in {capi}, found "
            f"{[c.name for c in candidates]} — reinstall onnxruntime.",
        )
    return candidates[0]


def install_minilm_capability(max_length: int = 256) -> EmbeddingCapability:
    """Make embeddings mean MiniLM (all-MiniLM-L6-v2) for the rest of this process.

    Needs onnxruntime's native library (`pip install --no-deps "onnxruntime>=1.30,<2"`)
    and the model, which is NOT shipped:
    download `model.onnx` + `vocab.txt` yourself (in a deployed image, at container build)
    and point `OPTERYX_MINILM_MODEL_DIR` at the directory. Call during startup, before any
    query that embeds is planned. Explicit by design: nothing installs it implicitly.

    Raises:
        MissingDependencyError: onnxruntime is not installed, or the model is absent.
        InvalidConfigurationError: embeddings were already planned at another width.
    """
    from opteryx.compiled.nanobind import minilm_native
    from opteryx.exceptions import MissingDependencyError

    configured = os.environ.get("OPTERYX_MINILM_MODEL_DIR", "").strip()
    if not configured:
        raise MissingDependencyError(
            "all-MiniLM-L6-v2",
            hint="the MiniLM model is not configured — download all-MiniLM-L6-v2 (model.onnx "
            "+ vocab.txt) and set OPTERYX_MINILM_MODEL_DIR to that directory.",
        )
    model_dir = Path(configured).expanduser()
    model_path = model_dir / "model.onnx"
    vocab_path = model_dir / "vocab.txt"
    if not model_path.is_file() or not vocab_path.is_file():
        raise MissingDependencyError(
            "all-MiniLM-L6-v2",
            hint=f"the MiniLM model is not present at {model_dir} (expected model.onnx and "
            "vocab.txt) — check OPTERYX_MINILM_MODEL_DIR.",
        )
    library = _onnxruntime_library()

    digest = hashlib.sha256()
    for path in (model_path, vocab_path):
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)

    kernel_ptr, dimensions = minilm_native.install_embed_capability(
        str(library), str(model_path), str(vocab_path), max_length
    )
    register_embedding_capability(
        "minilm-l6-v2",
        int(dimensions),
        int(kernel_ptr),
        identity=f"minilm-l6-v2:{max_length}:sha256:{digest.hexdigest()}",
    )
    return active_embedding_capability()
