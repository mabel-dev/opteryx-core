from __future__ import annotations

import os
import re
from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path

import numpy
from opteryx.exceptions import InvalidConfigurationError
from opteryx.third_party.cyan4973.xxhash import hash_bytes

_embedding_provider = None
_default_embedding_provider = None
_embedding_cache = OrderedDict()
_EMBEDDING_CACHE_MAX_ENTRIES = 4096
_STATIC_FEATURE_CACHE_MAX_ENTRIES = 65536
_STATIC_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+(?:['_-][A-Za-z0-9]+)*|[^\w\s]", re.UNICODE)
_STATIC_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "but",
        "by",
        "for",
        "from",
        "has",
        "have",
        "i",
        "if",
        "in",
        "is",
        "it",
        "its",
        "me",
        "my",
        "of",
        "on",
        "or",
        "our",
        "so",
        "that",
        "the",
        "their",
        "them",
        "there",
        "they",
        "this",
        "to",
        "was",
        "we",
        "were",
        "with",
        "would",
        "you",
        "your",
    }
)


class _StaticHashEmbeddingProvider:
    """
    Fast static embedding provider.

    This is a model2vec-style compromise: tokenize, map features into a fixed embedding
    space with deterministic hashing, then pool and normalize. It is dramatically cheaper
    than transformer inference, but quality is lower and more lexical.
    """

    def __init__(
        self,
        *,
        dimensions: int = 256,
        include_bigrams: bool = True,
        char_ngram_min: int = 3,
        char_ngram_max: int = 4,
    ):
        if dimensions <= 0:
            raise ValueError("dimensions must be positive")
        self._dimensions = dimensions
        self._include_bigrams = include_bigrams
        self._char_ngram_min = char_ngram_min
        self._char_ngram_max = max(char_ngram_min, char_ngram_max)
        self._feature_cache = OrderedDict()
        self._projection_scale = numpy.float32(2**-0.5)

    @property
    def dimensions(self) -> int:
        return self._dimensions

    def _normalize(self, text: str) -> str:
        return " ".join(text.lower().split())

    def _tokenize(self, text: str) -> list[str]:
        tokens = []
        for token in _STATIC_TOKEN_PATTERN.findall(self._normalize(text)):
            if not any(ch.isalnum() for ch in token):
                continue
            if token in _STATIC_STOPWORDS:
                continue
            if len(token) <= 1:
                continue
            tokens.append(token)
        return tokens

    def _feature_projections(self, feature: bytes):
        cached = self._feature_cache.get(feature)
        if cached is not None:
            self._feature_cache.move_to_end(feature)
            return cached

        first = hash_bytes(feature)
        second = hash_bytes(b"\x01" + feature)
        projections = (
            (
                first % self._dimensions,
                self._projection_scale if ((first >> 63) & 1) == 0 else -self._projection_scale,
            ),
            (
                second % self._dimensions,
                self._projection_scale if ((second >> 63) & 1) == 0 else -self._projection_scale,
            ),
        )
        self._feature_cache[feature] = projections
        self._feature_cache.move_to_end(feature)
        if len(self._feature_cache) > _STATIC_FEATURE_CACHE_MAX_ENTRIES:
            self._feature_cache.popitem(last=False)
        return projections

    def _add_feature(self, vector: numpy.ndarray, feature: bytes, weight: float) -> None:
        for index, sign in self._feature_projections(feature):
            vector[index] += sign * weight

    def embed_text(self, text: str) -> numpy.ndarray:
        vector = numpy.zeros(self._dimensions, dtype=numpy.float32)
        tokens = self._tokenize(text)
        if not tokens:
            return vector

        for position, token in enumerate(tokens):
            encoded = token.encode("utf8", errors="ignore")
            if not encoded:
                continue
            self._add_feature(vector, b"u:" + encoded, 1.0)

            if self._include_bigrams and position + 1 < len(tokens):
                next_token = tokens[position + 1].encode("utf8", errors="ignore")
                if next_token:
                    self._add_feature(vector, b"b:" + encoded + b" " + next_token, 0.5)

            wrapped = f"<{token}>"
            max_ngram = min(self._char_ngram_max, len(wrapped))
            for ngram_size in range(self._char_ngram_min, max_ngram + 1):
                for start in range(len(wrapped) - ngram_size + 1):
                    self._add_feature(
                        vector,
                        b"g:" + wrapped[start : start + ngram_size].encode("utf8", errors="ignore"),
                        0.25,
                    )

        norm = numpy.linalg.norm(vector)
        if norm != 0.0:
            vector /= norm
        return vector

    def embed_texts(self, texts: list[str]) -> numpy.ndarray:
        if not texts:
            return numpy.empty((0, self._dimensions), dtype=numpy.float32)
        return numpy.vstack([self.embed_text(text) for text in texts]).astype(
            numpy.float32, copy=False
        )

    def _extract_active_texts(self, values):
        if hasattr(values, "to_arrow"):
            values = values.to_arrow().to_pylist()
        positions = []
        texts = []
        for index, value in enumerate(values):
            if value is None:
                continue
            if isinstance(value, bytes):
                value = value.decode("utf8", errors="ignore")
            else:
                value = str(value)
            value = value.strip()
            if not value:
                continue
            positions.append(index)
            texts.append(value)
        return positions, texts

    def score_texts(self, query_text: str, texts: list[str]) -> numpy.ndarray:
        if not texts:
            return numpy.empty(0, dtype=numpy.float32)
        embedded = self.embed_texts([query_text, *texts])
        query_vector = embedded[0]
        row_vectors = embedded[1:]
        return numpy.asarray(row_vectors @ query_vector, dtype=numpy.float32)

    def score_string_vector(self, query_text: str, values):
        positions, texts = self._extract_active_texts(values)
        return (
            numpy.asarray(positions, dtype=numpy.int64),
            self.score_texts(query_text, texts),
        )


class _HybridEmbeddingProvider:
    prefer_score_string_vector = True

    def __init__(
        self,
        *,
        static_dimensions: int = 256,
        rerank_k: int = 96,
        include_bigrams: bool = True,
        char_ngram_min: int = 3,
        char_ngram_max: int = 4,
    ):
        self._static = _StaticHashEmbeddingProvider(
            dimensions=static_dimensions,
            include_bigrams=include_bigrams,
            char_ngram_min=char_ngram_min,
            char_ngram_max=char_ngram_max,
        )
        self._reranker = _MiniLMNativeEmbeddingProvider()
        self._rerank_k = max(1, rerank_k)

    def embed_text(self, text: str) -> list[float]:
        return self._reranker.embed_text(text)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return self._reranker.embed_texts(texts)

    def _tokenize(self, text: str) -> list[str]:
        return self._static._tokenize(text)

    def _lexical_scores(self, query_text: str, texts: list[str]) -> numpy.ndarray:
        query_tokens = self._tokenize(query_text)
        if not query_tokens or not texts:
            return numpy.zeros(len(texts), dtype=numpy.float32)

        query_term_counts = {}
        for token in query_tokens:
            query_term_counts[token] = query_term_counts.get(token, 0) + 1
        query_term_set = set(query_term_counts)
        query_bigrams = {
            f"{query_tokens[i]} {query_tokens[i + 1]}" for i in range(len(query_tokens) - 1)
        }
        docs = []
        document_frequency = {term: 0 for term in query_term_set}
        bigram_frequency = {bigram: 0 for bigram in query_bigrams}
        total_doc_length = 0

        for text in texts:
            tokens = self._tokenize(text)
            total_doc_length += len(tokens)
            token_counts = {}
            token_positions = {}
            for position, token in enumerate(tokens):
                token_counts[token] = token_counts.get(token, 0) + 1
                token_positions.setdefault(token, []).append(position)

            doc_bigrams = {f"{tokens[i]} {tokens[i + 1]}" for i in range(len(tokens) - 1)}
            for term in query_term_set:
                if term in token_counts:
                    document_frequency[term] += 1
            for bigram in query_bigrams:
                if bigram in doc_bigrams:
                    bigram_frequency[bigram] += 1

            docs.append((tokens, token_counts, token_positions, doc_bigrams))

        doc_count = max(1, len(texts))
        average_doc_length = max(1.0, total_doc_length / doc_count)
        k1 = numpy.float32(1.5)
        b = numpy.float32(0.75)
        term_idf = {
            term: numpy.float32(max(0.05, numpy.log1p((doc_count - df + 0.5) / (df + 0.5))))
            for term, df in document_frequency.items()
        }
        bigram_idf = {
            bigram: numpy.float32(max(0.05, numpy.log1p((doc_count - df + 0.5) / (df + 0.5))))
            for bigram, df in bigram_frequency.items()
        }

        scores = numpy.zeros(len(texts), dtype=numpy.float32)
        query_len = len(query_tokens)

        for index, (tokens, token_counts, token_positions, doc_bigrams) in enumerate(docs):
            if not tokens:
                continue

            score = numpy.float32(0.0)
            matched_terms = 0
            doc_length = len(tokens)
            length_norm = k1 * (1.0 - b + b * (doc_length / average_doc_length))
            for term in query_term_set:
                tf = token_counts.get(term, 0)
                if tf == 0:
                    continue
                matched_terms += 1
                query_weight = numpy.float32(1.0 + 0.25 * min(query_term_counts[term] - 1, 2))
                tf_component = ((k1 + 1.0) * tf) / (length_norm + tf)
                score += term_idf[term] * query_weight * numpy.float32(tf_component)

            if query_bigrams:
                for bigram in query_bigrams:
                    if bigram in doc_bigrams:
                        score += numpy.float32(2.5) * bigram_idf[bigram]

            if query_len > 1 and len(tokens) >= query_len:
                contiguous = False
                for start in range(len(tokens) - query_len + 1):
                    if tokens[start : start + query_len] == query_tokens:
                        contiguous = True
                        break
                if contiguous:
                    score += numpy.float32(3.0)

            if matched_terms >= 2:
                covered_positions = []
                for term in query_tokens:
                    positions = token_positions.get(term)
                    if positions:
                        covered_positions.append(positions[0])
                if len(covered_positions) >= 2:
                    span_width = max(1, max(covered_positions) - min(covered_positions) + 1)
                    score += numpy.float32((matched_terms * matched_terms) / span_width)

            coverage = matched_terms / max(1, len(query_term_set))
            score *= numpy.float32(0.25 + 0.75 * coverage)
            scores[index] = score

        return scores

    def score_string_vector(self, query_text: str, values):
        positions, texts = self._static._extract_active_texts(values)
        if not texts:
            return (
                numpy.empty(0, dtype=numpy.int64),
                numpy.empty(0, dtype=numpy.float32),
            )

        lexical_scores = self._lexical_scores(query_text, texts)
        shortlist = min(
            len(texts),
            max(self._rerank_k, min(len(texts), 8 * int(len(texts) ** 0.5))),
        )
        if shortlist >= len(texts):
            candidate_indices = numpy.arange(len(texts), dtype=numpy.int64)
        else:
            candidate_indices = numpy.argpartition(lexical_scores, -shortlist)[-shortlist:]
            candidate_indices = candidate_indices[
                numpy.argsort(lexical_scores[candidate_indices])[::-1]
            ]

        candidate_texts = [texts[index] for index in candidate_indices.tolist()]
        rerank_embeddings = numpy.asarray(
            self._reranker.embed_texts([query_text, *candidate_texts]),
            dtype=numpy.float32,
        )
        query_vector = rerank_embeddings[0]
        row_vectors = rerank_embeddings[1:]

        try:
            from opteryx.nanobind import vector_search

            rerank_scores = numpy.asarray(
                vector_search.score_cosine(query_vector, row_vectors),
                dtype=numpy.float32,
            )
        except (ImportError, ValueError):
            rerank_scores = numpy.zeros(len(candidate_texts), dtype=numpy.float32)
            query_norm = numpy.linalg.norm(query_vector)
            if query_norm != 0.0:
                row_norms = numpy.linalg.norm(row_vectors, axis=1)
                valid_mask = row_norms != 0.0
                if numpy.any(valid_mask):
                    rerank_scores[valid_mask] = (row_vectors[valid_mask] @ query_vector) / (
                        row_norms[valid_mask] * query_norm
                    )

        final_scores = lexical_scores * numpy.float32(0.15)
        final_scores[candidate_indices] = rerank_scores
        return (
            numpy.asarray(positions, dtype=numpy.int64),
            numpy.asarray(final_scores, dtype=numpy.float32),
        )


class _MiniLMNativeEmbeddingProvider:
    def __init__(self):
        from opteryx.nanobind import minilm_native

        model_dir = (
            Path(__file__).resolve().parent.parent / "third_party" / "models" / "all-MiniLM-L6-v2"
        )
        model_path = model_dir / "model.onnx"
        vocab_path = model_dir / "vocab.txt"
        self._embedder = minilm_native.MiniLMEmbedder(str(model_path), str(vocab_path), 256)

    def embed_text(self, text: str) -> list[float]:
        return self._embedder.embed_text(text)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return self._embedder.embed_texts(texts)

    def score_string_vector(self, query_text: str, values):
        scorer = getattr(self._embedder, "score_string_vector", None)
        if scorer is None:
            raise AttributeError("score_string_vector")

        data_buffer, offsets_buffer, null_buffer = values.buffers()
        if null_buffer is None:
            null_buffer = memoryview(b"\xff" * ((len(values) + 7) >> 3))
        positions, scores = scorer(
            query_text,
            data_buffer,
            offsets_buffer,
            null_buffer,
            len(values),
        )
        return (
            numpy.asarray(positions, dtype=numpy.int64),
            numpy.asarray(scores, dtype=numpy.float32),
        )


def _load_default_embedding_provider():
    global _default_embedding_provider

    if _default_embedding_provider is not None:
        return _default_embedding_provider

    selected_provider = os.environ.get("OPTERYX_EMBEDDING_PROVIDER", "").strip().lower()
    if selected_provider in {"static", "static-hash", "fast"}:
        _default_embedding_provider = _StaticHashEmbeddingProvider()
        return _default_embedding_provider
    if selected_provider in {"hybrid", "hybrid-rerank"}:
        _default_embedding_provider = _HybridEmbeddingProvider()
        return _default_embedding_provider

    model_dir = (
        Path(__file__).resolve().parent.parent / "third_party" / "models" / "all-MiniLM-L6-v2"
    )
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
    global _default_embedding_provider
    _default_embedding_provider = None


def get_embedding_provider():
    """Return the configured embedding provider, if any."""
    return _embedding_provider or _load_default_embedding_provider()


def create_static_embedding_provider(
    *,
    dimensions: int = 256,
    include_bigrams: bool = True,
    char_ngram_min: int = 3,
    char_ngram_max: int = 4,
):
    """Create a fast static embedding provider for search/ranking workloads."""
    return _StaticHashEmbeddingProvider(
        dimensions=dimensions,
        include_bigrams=include_bigrams,
        char_ngram_min=char_ngram_min,
        char_ngram_max=char_ngram_max,
    )


def create_hybrid_embedding_provider(
    *,
    static_dimensions: int = 256,
    rerank_k: int = 96,
    include_bigrams: bool = True,
    char_ngram_min: int = 3,
    char_ngram_max: int = 4,
):
    """Create a fast-recall, MiniLM-reranked embedding provider."""
    return _HybridEmbeddingProvider(
        static_dimensions=static_dimensions,
        rerank_k=rerank_k,
        include_bigrams=include_bigrams,
        char_ngram_min=char_ngram_min,
        char_ngram_max=char_ngram_max,
    )


def use_static_embedding_provider(
    *,
    dimensions: int = 256,
    include_bigrams: bool = True,
    char_ngram_min: int = 3,
    char_ngram_max: int = 4,
) -> None:
    """Register the built-in static embedding provider."""
    register_embedding_provider(
        create_static_embedding_provider(
            dimensions=dimensions,
            include_bigrams=include_bigrams,
            char_ngram_min=char_ngram_min,
            char_ngram_max=char_ngram_max,
        )
    )


def use_hybrid_embedding_provider(
    *,
    static_dimensions: int = 256,
    rerank_k: int = 96,
    include_bigrams: bool = True,
    char_ngram_min: int = 3,
    char_ngram_max: int = 4,
) -> None:
    """Register the built-in hybrid embedding provider."""
    register_embedding_provider(
        create_hybrid_embedding_provider(
            static_dimensions=static_dimensions,
            rerank_k=rerank_k,
            include_bigrams=include_bigrams,
            char_ngram_min=char_ngram_min,
            char_ngram_max=char_ngram_max,
        )
    )


def _raise_invalid_provider(provider, detail: str) -> None:
    raise InvalidConfigurationError(
        config_item="embedding_provider",
        provided_value=type(provider).__name__,
        valid_value_description=detail,
    )


def _coerce_embedding_vector_array(vector) -> numpy.ndarray:
    if isinstance(vector, numpy.ndarray):
        if vector.ndim != 1:
            _raise_invalid_provider(
                _embedding_provider,
                "a provider returning one 1-dimensional numeric vector per input value.",
            )
        coerced = numpy.asarray(vector, dtype=numpy.float32)
        if coerced.ndim != 1:
            _raise_invalid_provider(
                _embedding_provider,
                "a provider returning one 1-dimensional numeric vector per input value.",
            )
        return coerced
    elif not isinstance(vector, (list, tuple)):
        _raise_invalid_provider(
            _embedding_provider,
            "a provider returning one numeric vector per input value.",
        )

    try:
        return numpy.asarray(vector, dtype=numpy.float32)
    except (TypeError, ValueError) as err:
        raise InvalidConfigurationError(
            config_item="embedding_provider",
            provided_value=type(vector).__name__,
            valid_value_description="a numeric vector result.",
        ) from err


def _stack_embedding_rows(rows, expected_count: int) -> numpy.ndarray | None:
    if len(rows) != expected_count:
        return None
    if expected_count == 0:
        return numpy.empty((0, 0), dtype=numpy.float32)

    width = rows[0].shape[0]
    if width == 0:
        return numpy.empty((expected_count, 0), dtype=numpy.float32)
    if any(row.ndim != 1 or row.shape[0] != width for row in rows):
        _raise_invalid_provider(
            _embedding_provider,
            "a provider returning one fixed-width numeric vector per input value.",
        )
    return numpy.vstack(rows).astype(numpy.float32, copy=False)


def _coerce_embedding_batch_array(value, expected_count: int) -> numpy.ndarray | None:
    if isinstance(value, numpy.ndarray):
        if value.ndim == 2 and value.shape[0] == expected_count:
            return numpy.asarray(value, dtype=numpy.float32)
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return None
    try:
        rows = [_coerce_embedding_vector_array(row) for row in value]
    except InvalidConfigurationError:
        return None
    return _stack_embedding_rows(rows, expected_count)


def _provider_batch(provider, texts: list[str]) -> numpy.ndarray | None:
    if hasattr(provider, "embed_texts"):
        try:
            return _coerce_embedding_batch_array(provider.embed_texts(texts), len(texts))
        except TypeError:
            return None
    if hasattr(provider, "embed_many"):
        try:
            return _coerce_embedding_batch_array(provider.embed_many(texts), len(texts))
        except TypeError:
            return None
    if hasattr(provider, "embed"):
        try:
            return _coerce_embedding_batch_array(provider.embed(texts), len(texts))
        except TypeError:
            return None
    if callable(provider):
        try:
            return _coerce_embedding_batch_array(provider(texts), len(texts))
        except TypeError:
            return None
    return None


def _provider_single(provider, text: str) -> numpy.ndarray:
    if hasattr(provider, "embed_text"):
        return _coerce_embedding_vector_array(provider.embed_text(text))
    if hasattr(provider, "embed"):
        return _coerce_embedding_vector_array(provider.embed(text))
    if callable(provider):
        return _coerce_embedding_vector_array(provider(text))
    _raise_invalid_provider(
        provider,
        "configured via opteryx.register_embedding_provider(...) with a callable or embed_text(s) method.",
    )


def _clear_embedding_cache() -> None:
    _embedding_cache.clear()


def _embedding_cache_get(text: str) -> numpy.ndarray | None:
    vector = _embedding_cache.get(text)
    if vector is None:
        return None
    _embedding_cache.move_to_end(text)
    return vector


def _embedding_cache_put(text: str, vector) -> None:
    cached = numpy.asarray(vector, dtype=numpy.float32)
    cached.setflags(write=False)
    _embedding_cache[text] = cached
    _embedding_cache.move_to_end(text)
    if len(_embedding_cache) > _EMBEDDING_CACHE_MAX_ENTRIES:
        _embedding_cache.popitem(last=False)


def embed_text_matrix(texts: list[str]) -> numpy.ndarray:
    """Embed a batch of text values into a contiguous float32 matrix."""
    provider = get_embedding_provider()
    if provider is None:
        raise InvalidConfigurationError(
            config_item="embedding_provider",
            provided_value="unset",
            valid_value_description="configured via opteryx.register_embedding_provider(...).",
        )

    results = [None] * len(texts)
    missing_positions = []
    missing_unique = []
    seen_missing = set()

    for index, text in enumerate(texts):
        cached = _embedding_cache_get(text)
        if cached is not None:
            results[index] = cached
            continue

        missing_positions.append(index)
        if text not in seen_missing:
            missing_unique.append(text)
            seen_missing.add(text)

    if missing_unique:
        batch = _provider_batch(provider, missing_unique)
        if batch is None:
            batch = _stack_embedding_rows(
                [_provider_single(provider, text) for text in missing_unique],
                len(missing_unique),
            )

        unique_vectors = {}
        for text, vector in zip(missing_unique, batch, strict=True):
            _embedding_cache_put(text, vector)
            unique_vectors[text] = _embedding_cache_get(text)

        for index in missing_positions:
            results[index] = unique_vectors[texts[index]]

    if not results:
        return numpy.empty((0, 0), dtype=numpy.float32)

    return numpy.vstack(results).astype(numpy.float32, copy=False)


def embed_text_values(texts: list[str]) -> list[list[float]]:
    """Embed a batch of text values using the configured provider."""
    matrix = embed_text_matrix(texts)
    return [row.tolist() for row in matrix]
