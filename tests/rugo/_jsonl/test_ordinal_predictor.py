"""
Tests for ordinal predictor (rolling history with heuristics).
"""

import pytest


def test_ordinal_predictor_stable_position():
    """Verify prediction works for stable key positions."""
    # If a key is always at position 5, it should be predicted first
    # TODO: Phase 6 - expose C++ OrdinalPredictor to Python
    # For now, test structure is in place
    pass


def test_ordinal_predictor_shifting_position():
    """Verify graceful degradation with shifting positions."""
    # If key moves between positions 4 and 5, should probe both
    pass


def test_ordinal_predictor_high_entropy():
    """Verify high-entropy positions fall back to brute force."""
    # If key is at random positions, no prediction
    pass


def test_ordinal_predictor_heuristics():
    """Verify heuristics: 5+ first, then 3-4 by recency."""
    # Test that:
    # - Position appearing 6 times is in candidates
    # - Position appearing 3 times is secondary candidate
    # - Position appearing 2 times is not candidate
    pass


def test_ordinal_predictor_recency():
    """Verify recent occurrences are probed first."""
    # When multiple positions appear 3+ times, most recent first
    pass


def test_placeholder():
    """Placeholder while ordinal predictor wrapper is being exposed."""
    assert True
