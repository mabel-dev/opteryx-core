"""Vector similarity scoring and ranking operations.

This module provides vector ranking functionality for top-k similarity search,
replacing NumPy-based implementations in heap_sort_node.

Key operations:
- Score sanitization (NaN, infinity handling)
- Top-k candidate selection
- Ranking with row ID tiebreaker
"""

import heapq
from typing import List, Tuple


def sanitize_scores(scores: List[float], metric: str = "COSINE_SIMILARITY") -> List[float]:
    """
    Sanitize vector scores by handling NaN and infinity values.

    Replaces: numpy.nan_to_num(scores, nan=0.0, posinf=0.0, neginf=0.0)
              + numpy.clip(scores, -1.0, 1.0) for COSINE_SIMILARITY

    Args:
        scores: List of float scores from similarity computation
        metric: "COSINE_SIMILARITY" or "COSINE_DISTANCE"

    Returns:
        List of sanitized floats (NaN→0, ±inf→0, cosine_sim clipped to [-1, 1])
    """
    result = []
    for score in scores:
        # NaN check: NaN != NaN is True in Python
        if score != score:
            result.append(0.0)
        # Infinity handling
        elif score == float("inf") or score == float("-inf"):
            result.append(0.0)
        else:
            # Clamp cosine similarity to valid range
            if metric == "COSINE_SIMILARITY":
                result.append(max(-1.0, min(1.0, score)))
            else:
                result.append(score)

    return result


def select_top_k_indices(scores: List[float], k: int, descending: bool = True) -> List[int]:
    """
    Select k indices with highest (or lowest) scores.

    Replaces: numpy.argpartition(±scores, k-1)[:k]
    Uses heapq for O(n log k) performance.

    Args:
        scores: List of numeric scores
        k: Number of top scores to select
        descending: True→largest k scores, False→smallest k scores

    Returns:
        List of k indices into scores array
    """
    if k >= len(scores):
        return list(range(len(scores)))
    if k <= 0:
        return []

    # Create (score, index) pairs for selection
    indexed_scores = [(scores[i], i) for i in range(len(scores))]

    if descending:
        top_k = heapq.nlargest(k, indexed_scores)
    else:
        top_k = heapq.nsmallest(k, indexed_scores)

    return [idx for _, idx in top_k]


def rank_with_tiebreaker(
    all_scores: List[float],
    candidate_indices: List[int],
    row_ids: List[int],
    descending: bool = True,
) -> List[int]:
    """
    Sort candidates by score with row_id as tiebreaker.

    Replaces: numpy.lexsort((row_ids[candidates], ±scores[candidates]))

    Args:
        all_scores: Original scores array
        candidate_indices: Indices to rank (into scores and row_ids)
        row_ids: Row IDs for stable tiebreaker
        descending: True→descending scores, False→ascending

    Returns:
        Sorted candidate_indices ordered by (score, row_id)
    """
    # Build tuples for sorting: (score_sort_key, row_id, original_position)
    sort_tuples = []
    for rank, idx in enumerate(candidate_indices):
        score = all_scores[idx]
        row_id = row_ids[idx]

        if descending:
            # Negate for descending (Python sorts ascending by default)
            sort_tuples.append((-score, row_id, rank))
        else:
            sort_tuples.append((score, row_id, rank))

    # Sort by (score_sort_key, row_id)
    sort_tuples.sort()

    # Return candidate indices in ranked order
    return [candidate_indices[rank] for _, _, rank in sort_tuples]


def compute_distance_scores(
    similarity_scores: List[float],
    metric: str,
) -> List[float]:
    """
    Convert similarity scores to distance scores if needed.

    For COSINE_DISTANCE, converts from cosine similarity to distance.

    Args:
        similarity_scores: Raw cosine similarity scores
        metric: "COSINE_SIMILARITY" or "COSINE_DISTANCE"

    Returns:
        Scores in the metric's native space
    """
    if metric == "COSINE_DISTANCE":
        return [1.0 - sim for sim in similarity_scores]
    return similarity_scores


def vector_exact_search_top_k(
    similarity_scores: List[float],
    source_row_indices: List[int],
    k: int,
    metric: str = "COSINE_SIMILARITY",
) -> List[int]:
    """
    Find the k most similar vectors using exact search with ranking.

    Main entry point for vector top-k operations in heap_sort_node.
    Coordinates score sanitization, top-k selection, and final ranking.

    Args:
        similarity_scores: Pre-computed cosine similarity for each candidate
        source_row_indices: Row ID for each candidate (for tiebreaking/result)
        k: Number of results to return
        metric: "COSINE_SIMILARITY" or "COSINE_DISTANCE"

    Returns:
        List of k row indices from source_row_indices in ranked order
    """
    if not similarity_scores or k <= 0:
        return []

    # Step 1: Sanitize scores (handle NaN, inf, clamp ranges)
    scores = sanitize_scores(similarity_scores, metric)

    # Step 2: Convert similarity to distance if needed
    if metric == "COSINE_DISTANCE":
        scores = [1.0 - s for s in scores]
        descending = False  # For distance, smaller is better
    else:
        descending = True  # For similarity, larger is better

    # Step 3: Select top-k candidates
    candidate_indices = select_top_k_indices(scores, min(k, len(scores)), descending)

    if not candidate_indices:
        return []

    # Step 4: Rank candidates with tiebreaker (row_id)
    ranked_indices = rank_with_tiebreaker(scores, candidate_indices, source_row_indices, descending)

    # Step 5: Map back to original row indices and return top-k
    result = [source_row_indices[idx] for idx in ranked_indices[:k]]

    return result
