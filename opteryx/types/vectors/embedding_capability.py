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

"""The active EMBED capability — the ONE place EMBED's width is decided.

EMBED's kernel is `draken_embed` (draken/ops/kernels/function_vector_distance.cpp): the
static hashed projection. It is part of the zero-dependency core, is a total function of
its input, and is therefore ALWAYS present — the engine can never fail to plan an EMBED.
It is deliberately lexical, not semantic.

A richer embedder (MiniLM et al.) is an *installable capability*: an out-of-tree module
that registers its own `draken_embed` kernel over the core one. Capability registration
is the ONLY sanctioned way to change what EMBED means. There is no provider sniffing on
the execution path and no fallback: whatever is registered when a query BINDS is what
runs.

Why a registry rather than reading a provider at execution time: EMBED's return type is
`VECTOR(n)`, fixed into the plan at bind time, and the projection boundary copies rows at
exactly that stride. If the kernel produced a different width than the plan declared, the
copy would read the wrong stride — so the width cannot be discovered late. The binder
asks THIS module for the width, and hands the same number down to the kernel in a
`vector_dim_ctx`. One number, one source, no way for the two to disagree.

Contract for a capability:
  - Register during import/startup, BEFORE any query that uses EMBED is planned.
    Re-registering with a different width once EMBED has been bound raises: plans
    already compiled carry the old width.
  - `dimensions` is what the kernel WILL produce, every time, for every input.
  - The kernel must honour the width it is handed in `vector_dim_ctx`, or fail loud.
    A hashed projection is width-agnostic (slot = hash % dims) so it honours anything;
    a model-backed embedder whose width is fixed must reject a width it cannot produce
    rather than silently return a differently-shaped vector.
"""

from dataclasses import dataclass

from opteryx.exceptions import InvalidConfigurationError

# The core kernel's registry name. A capability replaces this entry.
_EMBED_KERNEL_NAME = "draken_embed"

# Width of the core static-hash EMBED. Matches _StaticHashEmbeddingProvider's default so
# the Python provider stays a usable oracle for the kernel (they are bit-exact).
_CORE_DIMENSIONS = 256


@dataclass(frozen=True)
class EmbeddingCapability:
    """What EMBED currently means. `name` is for diagnostics only."""

    name: str
    dimensions: int


_CORE = EmbeddingCapability(name="static-hash", dimensions=_CORE_DIMENSIONS)

_active: EmbeddingCapability = _CORE
# Set once EMBED's width has been baked into a plan. After that the width is load-bearing
# for compiled bytecode and cannot change under it.
_width_observed: bool = False


def active_embedding_capability() -> EmbeddingCapability:
    """The capability EMBED currently resolves to. Never None — the core is always there."""
    return _active


def embedding_dimensions() -> int:
    """Width EMBED will produce. Called by the binder; marks the width as committed."""
    global _width_observed
    _width_observed = True
    return _active.dimensions


def register_embedding_capability(name: str, dimensions: int, kernel_ptr: int) -> None:
    """Install `kernel_ptr` as EMBED's kernel, replacing the core static-hash one.

    Args:
        name: capability name, for diagnostics (e.g. "minilm-l6-v2").
        dimensions: the width this kernel produces — for every input, always.
        kernel_ptr: address of a `VecResult (*)(void*, const DrakenVector* const*, uint32_t)`
            C-ABI kernel. It must live for the process lifetime (i.e. be a symbol in a
            loaded extension module), and must honour the width handed to it in
            `vector_dim_ctx` or return an error sentinel.

    Raises:
        InvalidConfigurationError: on a bad width, or on a width change after EMBED has
            already been planned at the old width.
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
        # Bytecode already compiled against the old width would keep copying rows at the
        # old stride while the new kernel emitted a different one. Refuse rather than
        # corrupt already-planned queries.
        raise InvalidConfigurationError(
            config_item="embedding_capability",
            provided_value=f"{name} (width {dimensions})",
            valid_value_description=(
                f"a capability of width {_active.dimensions} — EMBED has already been "
                "planned at that width in this process. Register the capability during "
                "startup, before any query using EMBED is planned."
            ),
        )

    from draken.ops.kernels._kernel_registry import register_kernel

    register_kernel(_EMBED_KERNEL_NAME, kernel_ptr)
    _active = EmbeddingCapability(name=name, dimensions=dimensions)


def install_minilm_capability(max_length: int = 256) -> EmbeddingCapability:
    """Make EMBED mean MiniLM (all-MiniLM-L6-v2) for the rest of this process.

    Call during startup, before any query using EMBED is planned. Explicit by design:
    building the extension (OPTERYX_BUILD_EMBEDDINGS=1) must not silently change what
    EMBED means — a query's answers would depend on how the wheel was compiled.

    Raises:
        MissingDependencyError: the extension was not built, or the model is absent.
        InvalidConfigurationError: EMBED has already been planned at another width.
    """
    from opteryx.exceptions import MissingDependencyError
    from opteryx.types.vectors.embeddings import _minilm_model_dir

    model_dir = _minilm_model_dir()
    model_path = model_dir / "model.onnx"
    vocab_path = model_dir / "vocab.txt"
    if not model_path.exists() or not vocab_path.exists():
        raise MissingDependencyError(
            f"the MiniLM model is not present at {model_dir}"
        )
    try:
        from opteryx.compiled.nanobind import minilm_native
    except ImportError as err:
        # Not flow control: this is the one place the optional extension's absence is a
        # real, reportable configuration fact rather than something to route around.
        raise MissingDependencyError(
            "opteryx.compiled.nanobind.minilm_native — rebuild with "
            "OPTERYX_BUILD_EMBEDDINGS=1 to install the MiniLM EMBED capability"
        ) from err

    kernel_ptr, dimensions = minilm_native.install_embed_capability(
        str(model_path), str(vocab_path), max_length
    )
    register_embedding_capability("minilm-l6-v2", int(dimensions), int(kernel_ptr))
    return active_embedding_capability()


def _reset_embedding_capability_for_tests() -> None:
    """Restore the core capability. Test-only — does NOT unregister the native kernel,
    which is process-lifetime by construction."""
    global _active, _width_observed
    _active = _CORE
    _width_observed = False
