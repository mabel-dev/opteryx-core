from __future__ import annotations

import math
import os
import re
from array import array
from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path

from draken.vectors.vector import Vector as VectorVector

from opteryx.exceptions import InvalidConfigurationError, MissingDependencyError
from opteryx.third_party.cyan4973.xxhash import hash_bytes
from opteryx.types.vectors import vector_math

_embedding_provider = None
_default_embedding_provider = None
# Cache stores raw fp16 row bytes (dimensions * 2 bytes) keyed by source text.
_embedding_cache: "OrderedDict[str, tuple[int, bytes]]" = OrderedDict()
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
    """Fast static embedding provider (model2vec-style hashed projection).

    Tokenize, map features into a fixed embedding space with deterministic
    hashing, then pool, L2-normalize, and pack to fp16 inside a single Cython
    kernel call per text.
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
        self._projection_scale = float(2**-0.5)

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
        scale = self._projection_scale
        projections = (
            (first % self._dimensions, scale if ((first >> 63) & 1) == 0 else -scale),
            (second % self._dimensions, scale if ((second >> 63) & 1) == 0 else -scale),
        )
        self._feature_cache[feature] = projections
        self._feature_cache.move_to_end(feature)
        if len(self._feature_cache) > _STATIC_FEATURE_CACHE_MAX_ENTRIES:
            self._feature_cache.popitem(last=False)
        return projections

    def _gather_contributions(self, text: str) -> tuple[array, array]:
        """Return (indices, contributions) typed arrays for a single text."""
        indices = array("i")
        contributions = array("f")
        tokens = self._tokenize(text)
        if not tokens:
            return indices, contributions

        for position, token in enumerate(tokens):
            encoded = token.encode("utf8", errors="ignore")
            if not encoded:
                continue
            for idx, sign in self._feature_projections(b"u:" + encoded):
                indices.append(idx)
                contributions.append(sign)

            if self._include_bigrams and position + 1 < len(tokens):
                next_token = tokens[position + 1].encode("utf8", errors="ignore")
                if next_token:
                    for idx, sign in self._feature_projections(b"b:" + encoded + b" " + next_token):
                        indices.append(idx)
                        contributions.append(sign * 0.5)

            wrapped = f"<{token}>"
            max_ngram = min(self._char_ngram_max, len(wrapped))
            for ngram_size in range(self._char_ngram_min, max_ngram + 1):
                for start in range(len(wrapped) - ngram_size + 1):
                    feature = b"g:" + wrapped[start : start + ngram_size].encode(
                        "utf8", errors="ignore"
                    )
                    for idx, sign in self._feature_projections(feature):
                        indices.append(idx)
                        contributions.append(sign * 0.25)
        return indices, contributions

    def embed_text(self, text: str) -> VectorVector:
        vv = vector_math.new_matrix(1, self._dimensions)
        indices, contributions = self._gather_contributions(text)
        vector_math.pack_static_hash_row(vv, 0, indices, contributions)
        return vv

    def embed_texts(self, texts: list[str]) -> VectorVector:
        n = len(texts)
        vv = vector_math.new_matrix(n, self._dimensions)
        for i, text in enumerate(texts):
            indices, contributions = self._gather_contributions(text)
            vector_math.pack_static_hash_row(vv, i, indices, contributions)
        return vv

    def _extract_active_texts(self, values):
        if getattr(values, "to_arrow", None) is not None:
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

    def score_texts(self, query_text: str, texts: list[str]) -> list[float]:
        if not texts:
            return []
        embedded = self.embed_texts([query_text, *texts])
        # Dot product of fp16 query against each fp16 row, accumulated in fp32.
        return [vector_math.dot_fp16(embedded, 0, i + 1) for i in range(len(texts))]

    def score_string_vector(self, query_text: str, values):
        positions, texts = self._extract_active_texts(values)
        return (positions, self.score_texts(query_text, texts))


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

    def embed_text(self, text: str) -> VectorVector:
        return self._reranker.embed_text(text)

    def embed_texts(self, texts: list[str]) -> VectorVector:
        return self._reranker.embed_texts(texts)

    def _tokenize(self, text: str) -> list[str]:
        return self._static._tokenize(text)

    def _lexical_scores(self, query_text: str, texts: list[str]) -> list[float]:
        query_tokens = self._tokenize(query_text)
        if not query_tokens or not texts:
            return [0.0] * len(texts)

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
        k1 = 1.5
        b = 0.75
        term_idf = {
            term: max(0.05, math.log1p((doc_count - df + 0.5) / (df + 0.5)))
            for term, df in document_frequency.items()
        }
        bigram_idf = {
            bigram: max(0.05, math.log1p((doc_count - df + 0.5) / (df + 0.5)))
            for bigram, df in bigram_frequency.items()
        }

        scores = [0.0] * len(texts)
        query_len = len(query_tokens)

        for index, (tokens, token_counts, token_positions, doc_bigrams) in enumerate(docs):
            if not tokens:
                continue

            score = 0.0
            matched_terms = 0
            doc_length = len(tokens)
            length_norm = k1 * (1.0 - b + b * (doc_length / average_doc_length))
            for term in query_term_set:
                tf = token_counts.get(term, 0)
                if tf == 0:
                    continue
                matched_terms += 1
                query_weight = 1.0 + 0.25 * min(query_term_counts[term] - 1, 2)
                tf_component = ((k1 + 1.0) * tf) / (length_norm + tf)
                score += term_idf[term] * query_weight * tf_component

            if query_bigrams:
                for bigram in query_bigrams:
                    if bigram in doc_bigrams:
                        score += 2.5 * bigram_idf[bigram]

            if query_len > 1 and len(tokens) >= query_len:
                contiguous = False
                for start in range(len(tokens) - query_len + 1):
                    if tokens[start : start + query_len] == query_tokens:
                        contiguous = True
                        break
                if contiguous:
                    score += 3.0

            if matched_terms >= 2:
                covered_positions = []
                for term in query_tokens:
                    positions = token_positions.get(term)
                    if positions:
                        covered_positions.append(positions[0])
                if len(covered_positions) >= 2:
                    span_width = max(1, max(covered_positions) - min(covered_positions) + 1)
                    score += (matched_terms * matched_terms) / span_width

            coverage = matched_terms / max(1, len(query_term_set))
            score *= 0.25 + 0.75 * coverage
            scores[index] = score

        return scores

    def score_string_vector(self, query_text: str, values):
        positions, texts = self._static._extract_active_texts(values)
        if not texts:
            return ([], [])

        lexical_scores = self._lexical_scores(query_text, texts)
        shortlist = min(
            len(texts),
            max(self._rerank_k, min(len(texts), 8 * int(len(texts) ** 0.5))),
        )
        if shortlist >= len(texts):
            candidate_indices = list(range(len(texts)))
        else:
            candidate_indices = vector_math.argsort(lexical_scores, reverse=True)[:shortlist]

        candidate_texts = [texts[index] for index in candidate_indices]
        rerank_vv = self._reranker.embed_texts([query_text, *candidate_texts])
        # Cosine of fp16 query against fp16 rerank rows, accumulated in fp32.
        query_norm_sq = vector_math.dot_fp16(rerank_vv, 0, 0)
        rerank_scores = [0.0] * len(candidate_texts)
        if query_norm_sq > 0.0:
            query_norm = math.sqrt(query_norm_sq)
            for index in range(len(candidate_texts)):
                row_idx = index + 1
                row_norm_sq = vector_math.dot_fp16(rerank_vv, row_idx, row_idx)
                if row_norm_sq > 0.0:
                    rerank_scores[index] = vector_math.dot_fp16(rerank_vv, 0, row_idx) / (
                        math.sqrt(row_norm_sq) * query_norm
                    )

        final_scores = [score * 0.15 for score in lexical_scores]
        for candidate_index, rerank_score in zip(candidate_indices, rerank_scores):
            final_scores[candidate_index] = rerank_score
        return (positions, final_scores)


class _MiniLMNativeEmbeddingProvider:
    def __init__(self):
        from opteryx.compiled.nanobind import minilm_native

        model_dir = (
            Path(__file__).resolve().parent.parent.parent
            / "third_party"
            / "models"
            / "all-MiniLM-L6-v2"
        )
        model_path = model_dir / "model.onnx"
        vocab_path = model_dir / "vocab.txt"
        self._embedder = minilm_native.MiniLMEmbedder(str(model_path), str(vocab_path), 256)
        self._dimensions = int(self._embedder.dimensions)

    @property
    def dimensions(self) -> int:
        return self._dimensions

    def embed_text(self, text: str) -> VectorVector:
        fp32_row = array("f", self._embedder.embed_text(text))
        if len(fp32_row) != self._dimensions:
            raise InvalidConfigurationError(
                config_item="embedding_provider",
                provided_value=f"width {len(fp32_row)}",
                valid_value_description=f"a vector of width {self._dimensions}.",
            )
        vv = vector_math.new_matrix(1, self._dimensions)
        vector_math.pack_fp32_row(vv, 0, fp32_row)
        return vv

    def embed_texts(self, texts: list[str]) -> VectorVector:
        rows = self._embedder.embed_texts(texts)
        n = len(rows)
        vv = vector_math.new_matrix(n, self._dimensions)
        for i, row in enumerate(rows):
            fp32_row = array("f", row)
            vector_math.pack_fp32_row(vv, i, fp32_row)
        return vv

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
        return (list(positions), list(scores))


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
        Path(__file__).resolve().parent.parent.parent
        / "third_party"
        / "models"
        / "all-MiniLM-L6-v2"
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


def _coerce_to_fp32_array(value, expected_width: int) -> array:
    """Coerce a provider's per-text result into a typed fp32 array of `expected_width`."""
    if isinstance(value, (str, bytes, bytearray)):
        _raise_invalid_provider(
            _embedding_provider,
            "a provider returning one numeric vector per input value.",
        )
    try:
        out = array("f", value)
    except (TypeError, ValueError) as err:
        raise InvalidConfigurationError(
            config_item="embedding_provider",
            provided_value=type(value).__name__,
            valid_value_description="a numeric vector result.",
        ) from err
    if len(out) != expected_width:
        _raise_invalid_provider(
            _embedding_provider,
            f"a provider returning fixed-width numeric vectors of width {expected_width}.",
        )
    return out


def _provider_dimensions(provider) -> int | None:
    dims = getattr(provider, "dimensions", None)
    if dims is None:
        return None
    try:
        return int(dims)
    except (TypeError, ValueError):
        return None


def _provider_batch_rows(provider, texts: list[str]):
    """Return whatever the provider gives us for a batch (or None if it can't)."""
    for attr in ("embed_texts", "embed_many", "embed"):
        method = getattr(provider, attr, None)
        if method is None:
            continue
        try:
            return method(texts)
        except TypeError:
            continue
    if callable(provider):
        try:
            return provider(texts)
        except TypeError:
            return None
    return None


def _provider_single_row(provider, text: str):
    for attr in ("embed_text", "embed"):
        method = getattr(provider, attr, None)
        if method is not None:
            return method(text)
    if callable(provider):
        return provider(text)
    _raise_invalid_provider(
        provider,
        "configured via opteryx.register_embedding_provider(...) with a callable or embed_text(s) method.",
    )


def _embed_via_provider(provider, texts: list[str]) -> VectorVector:
    """Run `texts` through the provider and return the result as a fp16 VectorVector.

    Built-in providers may return a VectorVector directly; user-defined providers
    typically return a sequence of numeric rows that we coerce via fp32 → fp16.
    """
    rows = _provider_batch_rows(provider, texts)

    if isinstance(rows, VectorVector):
        if len(rows) != len(texts):
            _raise_invalid_provider(
                provider,
                "a provider returning one numeric vector per input value.",
            )
        return rows

    if rows is None:
        # Per-text fallback.
        first = _provider_single_row(provider, texts[0])
        if isinstance(first, VectorVector):
            single_rows = [first] + [_provider_single_row(provider, t) for t in texts[1:]]
            for r in single_rows:
                if not isinstance(r, VectorVector) or len(r) != 1:
                    _raise_invalid_provider(provider, "a consistent provider return type.")
            dims = single_rows[0]._nb.logical_type_dimension
            vv = vector_math.new_matrix(len(texts), dims)
            for i, single in enumerate(single_rows):
                vector_math.write_row_bytes(vv, i, vector_math.row_bytes(single, 0))
            return vv
        rows = [first] + [_provider_single_row(provider, t) for t in texts[1:]]

    if isinstance(rows, VectorVector):
        return rows

    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        _raise_invalid_provider(
            provider,
            "a provider returning a sequence of numeric vectors for a batch.",
        )
    if len(rows) != len(texts):
        _raise_invalid_provider(
            provider,
            "a provider returning one numeric vector per input value.",
        )

    dims = _provider_dimensions(provider)
    if dims is None:
        first = _coerce_to_fp32_array_unbounded(rows[0])
        dims = len(first)
        vv = vector_math.new_matrix(len(rows), dims)
        vector_math.pack_fp32_row(vv, 0, first)
        for i in range(1, len(rows)):
            fp32 = _coerce_to_fp32_array(rows[i], dims)
            vector_math.pack_fp32_row(vv, i, fp32)
        return vv

    vv = vector_math.new_matrix(len(rows), dims)
    for i, row in enumerate(rows):
        fp32 = _coerce_to_fp32_array(row, dims)
        vector_math.pack_fp32_row(vv, i, fp32)
    return vv


def _coerce_to_fp32_array_unbounded(value) -> array:
    if isinstance(value, (str, bytes, bytearray)):
        _raise_invalid_provider(
            _embedding_provider,
            "a provider returning one numeric vector per input value.",
        )
    try:
        return array("f", value)
    except (TypeError, ValueError) as err:
        raise InvalidConfigurationError(
            config_item="embedding_provider",
            provided_value=type(value).__name__,
            valid_value_description="a numeric vector result.",
        ) from err


def _clear_embedding_cache() -> None:
    _embedding_cache.clear()


def _embedding_cache_get(text: str) -> tuple[int, bytes] | None:
    entry = _embedding_cache.get(text)
    if entry is None:
        return None
    _embedding_cache.move_to_end(text)
    return entry


def _embedding_cache_put(text: str, dimensions: int, row_data: bytes) -> None:
    _embedding_cache[text] = (dimensions, row_data)
    _embedding_cache.move_to_end(text)
    if len(_embedding_cache) > _EMBEDDING_CACHE_MAX_ENTRIES:
        _embedding_cache.popitem(last=False)


def _raise_embeddings_unavailable() -> None:
    """Fail loud when the optional embeddings capability is absent.

    EMBED and the text overloads of COSINE_SIMILARITY/COSINE_DISTANCE depend on an
    embedding provider, shipped as the optional `opteryx-embeddings` package (kept out of
    the zero-dependency core). There is no silent fallback — instruct the user how to
    enable it.
    """
    raise MissingDependencyError(
        "opteryx-embeddings",
        hint=(
            "EMBED and text-based COSINE_SIMILARITY/COSINE_DISTANCE require the optional "
            "embeddings capability, which is not installed.\n"
            "Install it with:  pip install opteryx-embeddings\n"
            "or register your own provider via opteryx.register_embedding_provider(...)."
        ),
    )


def embed_text_matrix(texts: list[str]) -> VectorVector:
    """Embed `texts` into a fp16 VectorVector of length len(texts) × dimensions."""
    provider = get_embedding_provider()
    if provider is None:
        _raise_embeddings_unavailable()

    if not texts:
        dims = _provider_dimensions(provider) or 0
        return vector_math.new_matrix(0, dims)

    cached_rows: dict[int, tuple[int, bytes]] = {}
    missing_positions: list[int] = []
    missing_unique: list[str] = []
    seen_missing: set[str] = set()

    for index, text in enumerate(texts):
        entry = _embedding_cache_get(text)
        if entry is not None:
            cached_rows[index] = entry
            continue
        missing_positions.append(index)
        if text not in seen_missing:
            missing_unique.append(text)
            seen_missing.add(text)

    new_rows: VectorVector | None = None
    if missing_unique:
        new_rows = _embed_via_provider(provider, missing_unique)

    # Determine dimensions from whichever source actually produced rows.
    if new_rows is not None:
        dimensions = new_rows._nb.logical_type_dimension
    else:
        dimensions = next(iter(cached_rows.values()))[0]

    # Validate cached rows are dimensionally compatible (provider may have changed).
    for index, (cached_dims, _) in list(cached_rows.items()):
        if cached_dims != dimensions:
            del cached_rows[index]
            missing_positions.append(index)
            text = texts[index]
            if text not in seen_missing:
                missing_unique.append(text)
                seen_missing.add(text)

    if missing_positions and (
        new_rows is None or new_rows._nb.logical_type_dimension != dimensions
    ):
        # Re-embed any rows we just invalidated.
        new_rows = _embed_via_provider(provider, missing_unique)
        dimensions = new_rows._nb.logical_type_dimension

    out = vector_math.new_matrix(len(texts), dimensions)

    # Copy cached rows into place.
    for index, (_, row_data) in cached_rows.items():
        vector_math.write_row_bytes(out, index, row_data)

    # Map missing texts to their row index in `new_rows` and copy across.
    if new_rows is not None:
        unique_to_new_idx = {text: i for i, text in enumerate(missing_unique)}
        for output_index in missing_positions:
            text = texts[output_index]
            new_idx = unique_to_new_idx[text]
            row_data = vector_math.row_bytes(new_rows, new_idx)
            vector_math.write_row_bytes(out, output_index, row_data)
            _embedding_cache_put(text, dimensions, row_data)

    return out


def embed_text_values(texts: list[str]) -> list[list[float]]:
    """Embed `texts` and return them as fp32 Python lists (lossy widen for compatibility)."""
    matrix = embed_text_matrix(texts)
    return matrix.to_pylist()
