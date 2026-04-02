"""
Tests for cost-aware dispatch ordering (Patch 3).

Validates that codec metrics tracking and cost-aware dispatch:
- Records decode times per compression codec
- Estimates decode costs using historical data or defaults
- Prioritizes by estimated decode cost to reduce queue variance
- Respects warm-start behavior for first row group
- Handles mixed-codec workloads efficiently
"""

from unittest.mock import MagicMock, patch

import pytest

from opteryx.connectors.parquet_io.io_process_ring import (
    _CodecMetrics,
    _estimate_decode_cost,
    _IOColumnWork,
    _record_decode_cost,
)


class TestCodecMetricsTracking:
    """Test codec metrics recording and statistics."""

    def test_codec_metrics_creation(self):
        """Verify CodecMetrics dataclass initialization."""
        metrics = _CodecMetrics(codec_name="SNAPPY")
        assert metrics.codec_name == "SNAPPY"
        assert len(metrics.samples) == 0
        assert metrics.avg_ns_per_byte == 0.0

    def test_codec_metrics_samples_fixed_length(self):
        """Verify samples deque has max length of 100."""
        metrics = _CodecMetrics(codec_name="GZIP")
        # Add 150 samples
        for i in range(150):
            metrics.samples.append(float(i))
        # Should only keep last 100
        assert len(metrics.samples) == 100
        assert metrics.samples[0] == 50.0
        assert metrics.samples[99] == 149.0

    def test_record_decode_cost_creates_metric(self):
        """Verify record_decode_cost creates new codec metrics."""
        codec_metrics = {}
        _record_decode_cost(codec_metrics, "SNAPPY", 1000, 50000)

        assert "SNAPPY" in codec_metrics
        assert codec_metrics["SNAPPY"].codec_name == "SNAPPY"
        assert len(codec_metrics["SNAPPY"].samples) == 1
        assert codec_metrics["SNAPPY"].samples[0] == pytest.approx(50.0)

    def test_record_decode_cost_updates_average(self):
        """Verify average is computed after 10 samples."""
        codec_metrics = {}

        # Add 9 samples - no average yet
        for i in range(9):
            _record_decode_cost(codec_metrics, "SNAPPY", 1000, 10000 + i * 1000)

        assert codec_metrics["SNAPPY"].avg_ns_per_byte == 0.0

        # Add 10th sample - average should be computed
        _record_decode_cost(codec_metrics, "SNAPPY", 1000, 19000)

        assert codec_metrics["SNAPPY"].avg_ns_per_byte > 0.0
        expected_avg = sum(codec_metrics["SNAPPY"].samples) / len(codec_metrics["SNAPPY"].samples)
        assert codec_metrics["SNAPPY"].avg_ns_per_byte == pytest.approx(expected_avg)

    def test_record_decode_cost_handles_zero_bytes(self):
        """Verify record_decode_cost handles zero-length columns gracefully."""
        codec_metrics = {}
        _record_decode_cost(codec_metrics, "PLAIN", 0, 1000)

        # Should not create metrics entry for zero-byte columns
        assert "PLAIN" not in codec_metrics or len(codec_metrics["PLAIN"].samples) == 0

    def test_record_decode_cost_handles_empty_codec(self):
        """Verify record_decode_cost handles empty codec string."""
        codec_metrics = {}
        _record_decode_cost(codec_metrics, "", 1000, 50000)

        # Should map to "UNKNOWN"
        assert "UNKNOWN" in codec_metrics
        assert codec_metrics["UNKNOWN"].codec_name == "UNKNOWN"

    def test_record_decode_cost_multiple_codecs(self):
        """Verify tracking multiple codecs independently."""
        codec_metrics = {}

        _record_decode_cost(codec_metrics, "SNAPPY", 1000, 50000)
        _record_decode_cost(codec_metrics, "GZIP", 1000, 200000)
        _record_decode_cost(codec_metrics, "LZ4", 1000, 20000)

        assert len(codec_metrics) == 3
        assert codec_metrics["SNAPPY"].samples[0] == pytest.approx(50.0)
        assert codec_metrics["GZIP"].samples[0] == pytest.approx(200.0)
        assert codec_metrics["LZ4"].samples[0] == pytest.approx(20.0)

    def test_record_decode_cost_accumulates(self):
        """Verify costs accumulate for same codec."""
        codec_metrics = {}

        for i in range(20):
            _record_decode_cost(codec_metrics, "SNAPPY", 1000, 10000 + i * 1000)

        # Should have last 100 samples (or 20 in this case)
        assert len(codec_metrics["SNAPPY"].samples) == 20
        # Average should be computed
        assert codec_metrics["SNAPPY"].avg_ns_per_byte > 0.0


class TestEstimateCost:
    """Test decode cost estimation."""

    def test_estimate_with_default_codec_rate(self):
        """Verify estimation uses default rates for unknown codecs."""
        codec_metrics = {}

        # SNAPPY default is 100 ns/byte
        estimated = _estimate_decode_cost(codec_metrics, "SNAPPY", 1000)
        assert estimated == 100000  # 1000 bytes * 100 ns/byte

        # GZIP default is 1000 ns/byte
        estimated = _estimate_decode_cost(codec_metrics, "GZIP", 500)
        assert estimated == 500000  # 500 bytes * 1000 ns/byte

    def test_estimate_with_all_default_codecs(self):
        """Verify all codec defaults are reasonable."""
        codec_metrics = {}
        defaults = {
            "SNAPPY": 100,
            "GZIP": 1000,
            "LZ4": 50,
            "ZSTD": 200,
            "PLAIN": 10,
            "RLE": 20,
            "DELTA": 30,
        }

        for codec, rate in defaults.items():
            estimated = _estimate_decode_cost(codec_metrics, codec, 1000)
            assert estimated == rate * 1000

    def test_estimate_with_unknown_codec_default(self):
        """Verify unknown codec uses generic default of 100."""
        codec_metrics = {}
        estimated = _estimate_decode_cost(codec_metrics, "UNKNOWN_CODEC", 1000)
        # Should use generic default of 100 ns/byte
        assert estimated == 100000

    def test_estimate_uses_historical_data(self):
        """Verify estimation uses historical average when available."""
        codec_metrics = {}

        # Record some history
        for i in range(15):
            _record_decode_cost(codec_metrics, "SNAPPY", 1000, 25000 + i * 1000)

        # Now estimate - should use historical average, not default
        estimated = _estimate_decode_cost(codec_metrics, "SNAPPY", 1000)

        # Average should be around 25 + 7 = 32 ns/byte (middle of 25-39)
        assert 25000 <= estimated <= 40000
        # Should NOT be the default of 100,000
        assert estimated != 100000

    def test_estimate_prefers_historical_over_default(self):
        """Verify historical data takes precedence over defaults."""
        codec_metrics = {}

        # Record history showing SNAPPY is slow (opposite of default)
        for i in range(15):
            _record_decode_cost(codec_metrics, "SNAPPY", 100, 50000)

        estimated = _estimate_decode_cost(codec_metrics, "SNAPPY", 100)

        # Should use historical (~500 ns/byte) not default (100 ns/byte)
        assert estimated > 40000

    def test_estimate_with_mixed_codecsand_sizes(self):
        """Verify estimates scale correctly with column size."""
        codec_metrics = {}

        # Record history for LZ4
        for i in range(15):
            _record_decode_cost(codec_metrics, "LZ4", 1000, 25000)

        # Estimate for different sizes
        est_1k = _estimate_decode_cost(codec_metrics, "LZ4", 1000)
        est_2k = _estimate_decode_cost(codec_metrics, "LZ4", 2000)
        est_half = _estimate_decode_cost(codec_metrics, "LZ4", 500)

        # Costs should scale linearly with size
        assert est_2k == pytest.approx(est_1k * 2)
        assert est_half == pytest.approx(est_1k * 0.5)


class TestPickDispatchStateOrdering:
    """Test cost-aware dispatch state selection."""

    def _make_column_work(self, name: str, codec: str, size: int) -> _IOColumnWork:
        """Helper to create column work items."""
        return _IOColumnWork(
            name=name,
            stats={"compression_codec": codec},
            offset=0,
            length=size,
        )

    def test_warm_start_prioritized(self):
        """Verify warm-start takes priority over cost ordering."""
        from opteryx.connectors.parquet_io.io_process_ring import _IORowGroupState

        codec_metrics = {}
        active_states = {}
        first_rowgroup_key = None
        warm_start_remaining = 10
        per_rowgroup_cap = 4

        # Create first row group (for warm-start)
        col1 = self._make_column_work("col1", "GZIP", 10000)  # Expensive
        state1 = _IORowGroupState(
            file_seq=0,
            path="file1.parquet",
            rg_idx=0,
            admitted_ns=1000,
            column_order=["col1"],
            pending_columns=[col1],
        )
        key1 = (0, 0)
        active_states[key1] = state1
        first_rowgroup_key = key1

        # Create second row group (cheaper)
        col2 = self._make_column_work("col2", "SNAPPY", 100)  # Cheap
        state2 = _IORowGroupState(
            file_seq=0,
            path="file1.parquet",
            rg_idx=1,
            admitted_ns=2000,
            column_order=["col2"],
            pending_columns=[col2],
        )
        key2 = (0, 1)
        active_states[key2] = state2

        # Pick dispatch - should choose first RG due to warm-start
        def _pick_dispatch_state():
            nonlocal warm_start_remaining
            if warm_start_remaining > 0 and first_rowgroup_key in active_states:
                first_state = active_states[first_rowgroup_key]
                if first_state.pending_columns and first_state.in_flight < per_rowgroup_cap:
                    warm_start_remaining -= 1
                    return first_rowgroup_key, first_state

            candidates = []
            for key, state in active_states.items():
                if not state.pending_columns or state.in_flight >= per_rowgroup_cap:
                    continue
                col = state.pending_columns[0]
                codec = col.stats.get("compression_codec", "PLAIN")
                cost = _estimate_decode_cost(codec_metrics, codec, col.length)
                candidates.append((cost, col.length, -state.admitted_ns, key, state))

            if not candidates:
                return None
            candidates.sort(reverse=True, key=lambda x: (x[0], x[1], x[2]))
            _, _, _, key, state = candidates[0]
            return key, state

        # First pick should be key1 (warm-start)
        result = _pick_dispatch_state()
        assert result[0] == key1

    def test_cost_aware_ordering_by_codec(self):
        """Verify dispatch prioritizes by estimated decode cost."""
        from opteryx.connectors.parquet_io.io_process_ring import _IORowGroupState

        codec_metrics = {}

        # Establish history: SNAPPY is slow, LZ4 is fast
        for i in range(15):
            _record_decode_cost(codec_metrics, "SNAPPY", 1000, 100000)  # 100 ns/byte
            _record_decode_cost(codec_metrics, "LZ4", 1000, 20000)  # 20 ns/byte

        active_states = {}
        per_rowgroup_cap = 4

        # Create row groups with different codecs
        snappy_col = self._make_column_work("col_snappy", "SNAPPY", 1000)
        snappy_state = _IORowGroupState(
            file_seq=0,
            path="file1.parquet",
            rg_idx=0,
            admitted_ns=1000,
            column_order=["col_snappy"],
            pending_columns=[snappy_col],
        )
        key_snappy = (0, 0)
        active_states[key_snappy] = snappy_state

        lz4_col = self._make_column_work("col_lz4", "LZ4", 1000)
        lz4_state = _IORowGroupState(
            file_seq=0,
            path="file1.parquet",
            rg_idx=1,
            admitted_ns=2000,
            column_order=["col_lz4"],
            pending_columns=[lz4_col],
        )
        key_lz4 = (0, 1)
        active_states[key_lz4] = lz4_state

        # Calculate costs
        snappy_cost = _estimate_decode_cost(codec_metrics, "SNAPPY", 1000)
        lz4_cost = _estimate_decode_cost(codec_metrics, "LZ4", 1000)

        # SNAPPY should have higher cost
        assert snappy_cost > lz4_cost

        # Despite SNAPPY being admitted first, it should be picked last
        # (cost-aware dispatch picks high-cost items first to reduce queue variance)
        candidates = []
        for key, state in active_states.items():
            if not state.pending_columns or state.in_flight >= per_rowgroup_cap:
                continue
            col = state.pending_columns[0]
            codec = col.stats.get("compression_codec", "PLAIN")
            cost = _estimate_decode_cost(codec_metrics, codec, col.length)
            candidates.append((cost, col.length, -state.admitted_ns, key, state))

        candidates.sort(reverse=True, key=lambda x: (x[0], x[1], x[2]))

        # First pick should be SNAPPY (highest cost first)
        assert candidates[0][3] == key_snappy

    def test_cost_ordering_with_multiple_candidates(self):
        """Verify cost ordering with multiple row groups."""
        from opteryx.connectors.parquet_io.io_process_ring import _IORowGroupState

        codec_metrics = {}

        # Setup: 3 different codecs
        for i in range(15):
            _record_decode_cost(codec_metrics, "GZIP", 1000, 200000)  # 200 ns/byte - most expensive
            _record_decode_cost(codec_metrics, "SNAPPY", 1000, 100000)  # 100 ns/byte - medium
            _record_decode_cost(codec_metrics, "LZ4", 1000, 20000)  # 20 ns/byte - cheapest

        active_states = {}
        per_rowgroup_cap = 4

        codecs = ["GZIP", "SNAPPY", "LZ4"]
        for i, codec in enumerate(codecs):
            col = self._make_column_work(f"col_{codec}", codec, 1000)
            state = _IORowGroupState(
                file_seq=0,
                path="file1.parquet",
                rg_idx=i,
                admitted_ns=1000 + i,
                column_order=[f"col_{codec}"],
                pending_columns=[col],
            )
            key = (0, i)
            active_states[key] = state

        # Collect costs
        costs = []
        for key, state in active_states.items():
            col = state.pending_columns[0]
            codec = col.stats.get("compression_codec", "PLAIN")
            cost = _estimate_decode_cost(codec_metrics, codec, col.length)
            costs.append((cost, key, codec))

        # Should be ordered: GZIP > SNAPPY > LZ4
        costs.sort(reverse=True, key=lambda x: x[0])
        assert costs[0][2] == "GZIP"
        assert costs[1][2] == "SNAPPY"
        assert costs[2][2] == "LZ4"

    def test_size_tiebreaker_for_equal_costs(self):
        """Verify size is used as tiebreaker when costs are equal."""
        from opteryx.connectors.parquet_io.io_process_ring import _IORowGroupState

        codec_metrics = {}
        active_states = {}
        per_rowgroup_cap = 4

        # Both use PLAIN codec (default)
        col1 = self._make_column_work("col1", "PLAIN", 5000)
        state1 = _IORowGroupState(
            file_seq=0,
            path="file1.parquet",
            rg_idx=0,
            admitted_ns=1000,
            column_order=["col1"],
            pending_columns=[col1],
        )
        key1 = (0, 0)
        active_states[key1] = state1

        col2 = self._make_column_work("col2", "PLAIN", 2000)
        state2 = _IORowGroupState(
            file_seq=0,
            path="file1.parquet",
            rg_idx=1,
            admitted_ns=1001,
            column_order=["col2"],
            pending_columns=[col2],
        )
        key2 = (0, 1)
        active_states[key2] = state2

        # Build candidates
        candidates = []
        for key, state in active_states.items():
            col = state.pending_columns[0]
            codec = col.stats.get("compression_codec", "PLAIN")
            cost = _estimate_decode_cost(codec_metrics, codec, col.length)
            candidates.append((cost, col.length, -state.admitted_ns, key, state))

        candidates.sort(reverse=True, key=lambda x: (x[0], x[1], x[2]))

        # When costs are equal, larger column should be picked first
        assert candidates[0][3] == key1  # 5000 bytes > 2000 bytes


class TestConfigurationFlag:
    """Test OPTERYX_TRACK_CODEC_METRICS configuration."""

    def test_config_flag_exists(self):
        """Verify OPTERYX_TRACK_CODEC_METRICS config is available."""
        from opteryx import config as _cfg

        assert hasattr(_cfg, "OPTERYX_TRACK_CODEC_METRICS")

    def test_tracking_enabled_by_default(self):
        """Verify codec metrics tracking is enabled by default."""
        from opteryx import config as _cfg

        # Default should be enabled
        assert _cfg.OPTERYX_TRACK_CODEC_METRICS is True

    def test_tracking_can_be_disabled(self, monkeypatch):
        """Verify codec metrics tracking can be disabled via environment."""
        monkeypatch.setenv("OPTERYX_TRACK_CODEC_METRICS", "0")

        # Force reload to pick up the env var
        import importlib

        import opteryx.config as cfg_module

        importlib.reload(cfg_module)

        # Verify it's disabled
        assert cfg_module.OPTERYX_TRACK_CODEC_METRICS is False

        # Restore default
        monkeypatch.delenv("OPTERYX_TRACK_CODEC_METRICS", raising=False)
        importlib.reload(cfg_module)


class TestMixedCodecWorkloads:
    """Test mixed-codec scenarios that benefit from cost-aware ordering."""

    def test_mixed_codec_workload_prioritization(self):
        """Verify mixed-codec workloads are ordered optimally."""
        from opteryx.connectors.parquet_io.io_process_ring import _IORowGroupState

        codec_metrics = {}

        # Simulate workload: mixed codecs with varied sizes
        workloads = [
            ("GZIP", 5000),  # Large, expensive
            ("SNAPPY", 8000),  # Large, medium
            ("LZ4", 2000),  # Small, cheap
            ("PLAIN", 1000),  # Small, very cheap
        ]

        # Build history
        for codec, _ in workloads:
            # GZIP: 200 ns/byte
            if codec == "GZIP":
                for i in range(15):
                    _record_decode_cost(codec_metrics, codec, 1000, 200000)
            # SNAPPY: 100 ns/byte
            elif codec == "SNAPPY":
                for i in range(15):
                    _record_decode_cost(codec_metrics, codec, 1000, 100000)
            # LZ4: 20 ns/byte
            elif codec == "LZ4":
                for i in range(15):
                    _record_decode_cost(codec_metrics, codec, 1000, 20000)
            # PLAIN: 5 ns/byte
            else:
                for i in range(15):
                    _record_decode_cost(codec_metrics, codec, 1000, 5000)

        # Calculate total costs
        total_costs = []
        for codec, size in workloads:
            cost = _estimate_decode_cost(codec_metrics, codec, size)
            total_costs.append((cost, codec, size))

        # Sort by cost (cost-aware ordering)
        total_costs.sort(reverse=True, key=lambda x: x[0])

        # Verify ordering: more expensive ones first
        # GZIP (5000 * 200) = 1,000,000 ns
        # SNAPPY (8000 * 100) = 800,000 ns
        # LZ4 (2000 * 20) = 40,000 ns
        # PLAIN (1000 * 5) = 5,000 ns
        assert total_costs[0][1] == "GZIP"
        assert total_costs[1][1] == "SNAPPY"
        assert total_costs[2][1] == "LZ4"
        assert total_costs[3][1] == "PLAIN"

    def test_handles_unknown_codec_gracefully(self):
        """Verify unknown codecs fall back to default without error."""
        codec_metrics = {}

        # Should not raise
        estimated = _estimate_decode_cost(codec_metrics, "UNKNOWN", 1000)
        assert estimated == 100000  # Generic default

    def test_zero_size_column_estimation(self):
        """Verify zero-size columns are handled."""
        codec_metrics = {}

        estimated = _estimate_decode_cost(codec_metrics, "SNAPPY", 0)
        assert estimated == 0
