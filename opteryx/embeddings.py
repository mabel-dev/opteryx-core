from __future__ import annotations

from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path

import numpy

from opteryx.exceptions import InvalidConfigurationError

_embedding_provider = None
_default_embedding_provider = None
_embedding_cache = OrderedDict()
_EMBEDDING_CACHE_MAX_ENTRIES = 4096


class _MiniLMNativeEmbeddingProvider:
    def __init__(self):
        from opteryx.nanobind import minilm_native

        model_dir = Path(__file__).resolve().parent.parent / "third_party" / "models" / "all-MiniLM-L6-v2"
        model_path = model_dir / "model.onnx"
        vocab_path = model_dir / "vocab.txt"
        self._embedder = minilm_native.MiniLMEmbedder(str(model_path), str(vocab_path), 256)

    def embed_text(self, text: str) -> list[float]:
        return self._embedder.embed_text(text)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return self._embedder.embed_texts(texts)


def _load_default_embedding_provider():
    global _default_embedding_provider

    if _default_embedding_provider is not None:
        return _default_embedding_provider

    model_dir = Path(__file__).resolve().parent.parent / "third_party" / "models" / "all-MiniLM-L6-v2"
    if not (model_dir / "model.onnx").exists() or not (model_dir / "vocab.txt").exists():
        return None

    try:
        _default_embedding_provider = _MiniLMNativeEmbeddingProvider()
    except ImportError:
        return None

    return _default_embedding_provider


def register_embedding_provider(provider) -> None:
    """Register the process-wide embedding provider used by EMBED(...)."""
    global _embedding_provider
    _embedding_provider = provider
    _clear_embedding_cache()


def clear_embedding_provider() -> None:
    """Clear the process-wide embedding provider."""
    global _embedding_provider
    _embedding_provider = None
    _clear_embedding_cache()


def get_embedding_provider():
    """Return the configured embedding provider, if any."""
    return _embedding_provider or _load_default_embedding_provider()


def _raise_invalid_provider(provider, detail: str) -> None:
    raise InvalidConfigurationError(
        config_item="embedding_provider",
        provided_value=type(provider).__name__,
        valid_value_description=detail,
    )


def _coerce_embedding_vector(vector) -> list[float]:
    if isinstance(vector, numpy.ndarray):
        if vector.ndim != 1:
            _raise_invalid_provider(
                _embedding_provider,
                "a provider returning one 1-dimensional numeric vector per input value.",
            )
        vector = vector.tolist()
    elif not isinstance(vector, (list, tuple)):
        _raise_invalid_provider(
            _embedding_provider,
            "a provider returning one numeric vector per input value.",
        )

    try:
        return [float(value) for value in vector]
    except (TypeError, ValueError) as err:
        raise InvalidConfigurationError(
            config_item="embedding_provider",
            provided_value=type(vector).__name__,
            valid_value_description="a numeric vector result.",
        ) from err


def _coerce_embedding_batch(value, expected_count: int) -> list[list[float]] | None:
    if isinstance(value, numpy.ndarray):
        if value.ndim == 2 and value.shape[0] == expected_count:
            return [_coerce_embedding_vector(row) for row in value]
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return None
    if len(value) != expected_count:
        return None
    try:
        return [_coerce_embedding_vector(row) for row in value]
    except InvalidConfigurationError:
        return None


def _provider_batch(provider, texts: list[str]) -> list[list[float]] | None:
    if hasattr(provider, "embed_texts"):
        try:
            return _coerce_embedding_batch(provider.embed_texts(texts), len(texts))
        except TypeError:
            return None
    if hasattr(provider, "embed_many"):
        try:
            return _coerce_embedding_batch(provider.embed_many(texts), len(texts))
        except TypeError:
            return None
    if hasattr(provider, "embed"):
        try:
            return _coerce_embedding_batch(provider.embed(texts), len(texts))
        except TypeError:
            return None
    if callable(provider):
        try:
            return _coerce_embedding_batch(provider(texts), len(texts))
        except TypeError:
            return None
    return None


def _provider_single(provider, text: str) -> list[float]:
    if hasattr(provider, "embed_text"):
        return _coerce_embedding_vector(provider.embed_text(text))
    if hasattr(provider, "embed"):
        return _coerce_embedding_vector(provider.embed(text))
    if callable(provider):
        return _coerce_embedding_vector(provider(text))
    _raise_invalid_provider(
        provider,
        "configured via opteryx.register_embedding_provider(...) with a callable or embed_text(s) method.",
    )


def _clear_embedding_cache() -> None:
    _embedding_cache.clear()


def _embedding_cache_get(text: str) -> list[float] | None:
    vector = _embedding_cache.get(text)
    if vector is None:
        return None
    _embedding_cache.move_to_end(text)
    return list(vector)


def _embedding_cache_put(text: str, vector: list[float]) -> None:
    _embedding_cache[text] = tuple(vector)
    _embedding_cache.move_to_end(text)
    if len(_embedding_cache) > _EMBEDDING_CACHE_MAX_ENTRIES:
        _embedding_cache.popitem(last=False)


def embed_text_values(texts: list[str]) -> list[list[float]]:
    """Embed a batch of text values using the configured provider."""
    provider = get_embedding_provider()
    if provider is None:
        raise InvalidConfigurationError(
            config_item="embedding_provider",
            provided_value="unset",
            valid_value_description="configured via opteryx.register_embedding_provider(...).",
        )

    results = [None] * len(texts)
    missing_texts = []
    missing_positions = []
    missing_unique = []
    seen_missing = set()

    for index, text in enumerate(texts):
        cached = _embedding_cache_get(text)
        if cached is not None:
            results[index] = cached
            continue

        missing_texts.append(text)
        missing_positions.append(index)
        if text not in seen_missing:
            missing_unique.append(text)
            seen_missing.add(text)

    if missing_unique:
        batch = _provider_batch(provider, missing_unique)
        if batch is None:
            batch = [_provider_single(provider, text) for text in missing_unique]

        unique_vectors = {}
        for text, vector in zip(missing_unique, batch, strict=True):
            unique_vectors[text] = vector
            _embedding_cache_put(text, vector)

        for index, text in zip(missing_positions, missing_texts, strict=True):
            results[index] = list(unique_vectors[text])

    return results
