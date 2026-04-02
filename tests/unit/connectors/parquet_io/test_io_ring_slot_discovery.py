"""
Tests for _SharedMemoryRing O(1) slot discovery with cursor.

Validates that the cursor-based round-robin free slot discovery:
- Finds free slots efficiently (O(1) typical case)
- Advances cursor correctly after each find
- Wraps around the ring properly
- Handles partial ring fill scenarios
"""

from multiprocessing.shared_memory import SharedMemory

import pytest

from opteryx.connectors.parquet_io.io_process_ring import FREE, WRITING, _SharedMemoryRing


@pytest.fixture
def ring():
    """Create a test ring with 16 slots of 4KB each."""
    test_ring = _SharedMemoryRing(
        slot_bytes=4096,
        slot_count=16,
        name=None,
        create=True,
    )
    test_ring.initialize_free()
    yield test_ring
    test_ring.unlink()


class TestSlotDiscoveryCursor:
    """Test cursor-based round-robin slot discovery."""

    def test_cursor_initialized_to_zero(self, ring):
        """Verify cursor starts at 0."""
        assert ring.free_slot_cursor == 0

    def test_cursor_advances_after_find(self, ring):
        """Verify cursor advances after finding a slot."""
        slot_id = ring._find_free_slot_from_bitmap()
        assert slot_id == 0
        # Cursor should advance to next slot after this one
        assert ring.free_slot_cursor == 1

    def test_cursor_wraps_around(self, ring):
        """Verify cursor wraps to 0 after reaching end."""
        ring.free_slot_cursor = ring.slot_count - 1
        slot_id = ring._find_free_slot_from_bitmap()
        # Should find slot at position (slot_count - 1)
        assert slot_id == ring.slot_count - 1
        # Cursor should wrap to 0
        assert ring.free_slot_cursor == 0

    def test_sequential_discovery_advances_cursor(self, ring):
        """Verify sequential slot discovery advances cursor each time."""
        discovered = []
        for i in range(8):
            slot_id = ring._find_free_slot_from_bitmap()
            discovered.append(slot_id)
            # Mark as used so next search continues
            ring.write_state(slot_id, WRITING)
            ring.free_slot_bitmap[slot_id] = 1

        # First 8 slots should be found sequentially
        assert discovered == list(range(8))

    def test_round_robin_discovery(self, ring):
        """Verify round-robin behavior after wrapping."""
        # Manually set cursor near end
        ring.free_slot_cursor = 14

        # Find slot near end
        slot_id_1 = ring._find_free_slot_from_bitmap()
        assert slot_id_1 == 14
        assert ring.free_slot_cursor == 15

        # Mark as used
        ring.write_state(slot_id_1, WRITING)
        ring.free_slot_bitmap[slot_id_1] = 1

        # Next find should wrap and get slot 15
        slot_id_2 = ring._find_free_slot_from_bitmap()
        assert slot_id_2 == 15
        assert ring.free_slot_cursor == 0

        # Mark as used
        ring.write_state(slot_id_2, WRITING)
        ring.free_slot_bitmap[slot_id_2] = 1

        # Next find should wrap to 0
        slot_id_3 = ring._find_free_slot_from_bitmap()
        assert slot_id_3 == 0
        assert ring.free_slot_cursor == 1

    def test_claim_free_slot_updates_cursor(self, ring):
        """Verify claim_free_slot updates cursor correctly."""
        from multiprocessing import Event

        cancel_event = Event()

        # First claim
        slot_id_1, _, _ = ring.claim_free_slot(cancel_event)
        assert slot_id_1 == 0
        assert ring.free_slot_cursor == 1

        # Free it by marking FREE and resetting bitmap
        ring.write_state(slot_id_1, FREE)
        ring.free_slot_bitmap[slot_id_1] = 0

        # Second claim should get slot 1 (cursor is at 1)
        slot_id_2, _, _ = ring.claim_free_slot(cancel_event)
        assert slot_id_2 == 1
        assert ring.free_slot_cursor == 2

    def test_return_none_when_all_slots_full(self, ring):
        """Verify returns None when all slots are in use."""
        # Mark all slots as in use
        for i in range(ring.slot_count):
            ring.write_state(i, WRITING)
            ring.free_slot_bitmap[i] = 1

        # Should return None
        result = ring._find_free_slot_from_bitmap()
        assert result is None

    def test_find_slot_after_partial_fill(self, ring):
        """Verify efficient discovery with partial ring fill."""
        # Mark first 8 slots as used
        for i in range(8):
            ring.write_state(i, WRITING)
            ring.free_slot_bitmap[i] = 1

        # Set cursor past the used slots
        ring.free_slot_cursor = 7

        # Next find should quickly get slot 8 (one iteration)
        slot_id = ring._find_free_slot_from_bitmap()
        assert slot_id == 8
        assert ring.free_slot_cursor == 9

    def test_bitmap_staleness_recovery(self, ring):
        """Verify recovery from stale bitmap."""
        # Set bitmap to indicate slot 0 is free
        ring.free_slot_bitmap[0] = 0
        # But actually mark it as in use in the state
        ring.write_state(0, WRITING)

        # Discovery should detect staleness and continue
        slot_id = ring._find_free_slot_from_bitmap()
        # Should skip slot 0 (bitmap updated) and find slot 1
        assert slot_id == 1
        # Bitmap should be corrected
        assert ring.free_slot_bitmap[0] == 1

    def test_initialize_free_resets_cursor(self, ring):
        """Verify initialize_free resets cursor."""
        ring.free_slot_cursor = 5
        ring.initialize_free()
        assert ring.free_slot_cursor == 0

    def test_cursor_position_after_multiple_cycles(self, ring):
        """Verify cursor correctness after multiple full ring cycles."""
        slot_ids = []
        for cycle in range(2):
            for _ in range(ring.slot_count):
                slot_id = ring._find_free_slot_from_bitmap()
                if slot_id is not None:
                    slot_ids.append(slot_id)
                    ring.write_state(slot_id, WRITING)
                    ring.free_slot_bitmap[slot_id] = 1

            # Free all slots after first cycle to enable second cycle
            if cycle == 0:
                for i in range(ring.slot_count):
                    ring.write_state(i, FREE)
                    ring.free_slot_bitmap[i] = 0

        # Verify we got 2 * slot_count discoveries
        assert len(slot_ids) == 2 * ring.slot_count

        # Reset all to free
        ring.initialize_free()

        # Next cycle should start fresh from 0
        slot_id = ring._find_free_slot_from_bitmap()
        assert slot_id == 0
        assert ring.free_slot_cursor == 1

    def test_performance_o1_discovery(self, ring):
        """Verify O(1) discovery in typical case (recently freed slot)."""
        import time

        # Fill all but last slot
        for i in range(ring.slot_count - 1):
            ring.write_state(i, WRITING)
            ring.free_slot_bitmap[i] = 1

        ring.free_slot_cursor = ring.slot_count - 1

        # Discover the last free slot (should be immediate)
        start = time.perf_counter_ns()
        slot_id = ring._find_free_slot_from_bitmap()
        elapsed_ns = time.perf_counter_ns() - start

        assert slot_id == ring.slot_count - 1
        # O(1) discovery should be very fast (< 1µs typically)
        # We're lenient here since timing is machine-dependent
        assert elapsed_ns < 10000  # 10µs threshold

    def test_discovery_with_sparse_free_slots(self, ring):
        """Verify discovery efficiency with sparse free slots."""
        # Mark slots 0, 2, 4, 6, ... as used (even slots)
        for i in range(0, ring.slot_count, 2):
            ring.write_state(i, WRITING)
            ring.free_slot_bitmap[i] = 1

        # Set cursor to odd position
        ring.free_slot_cursor = 1

        # Should find odd slots efficiently
        discovered = []
        for _ in range(ring.slot_count // 2):
            slot_id = ring._find_free_slot_from_bitmap()
            if slot_id is not None:
                discovered.append(slot_id)
                ring.write_state(slot_id, WRITING)
                ring.free_slot_bitmap[slot_id] = 1

        # Should find all odd-numbered slots
        expected = [i for i in range(1, ring.slot_count, 2)]
        assert discovered == expected

    def test_discovery_consistency_across_states(self, ring):
        """Verify discovery is consistent regardless of slot state transitions."""
        # Pattern: find, mark used, free, find again
        slot_id_1 = ring._find_free_slot_from_bitmap()
        assert slot_id_1 == 0

        ring.write_state(slot_id_1, WRITING)
        ring.free_slot_bitmap[slot_id_1] = 1

        # Find another
        slot_id_2 = ring._find_free_slot_from_bitmap()
        assert slot_id_2 == 1

        # Free the first one
        ring.write_state(slot_id_1, FREE)
        ring.free_slot_bitmap[slot_id_1] = 0

        # Cursor should be at 2 now
        # Next find should wrap and eventually find slot_id_1
        found = False
        for _ in range(ring.slot_count):
            sid = ring._find_free_slot_from_bitmap()
            if sid == slot_id_1:
                found = True
                break
            ring.write_state(sid, WRITING)
            ring.free_slot_bitmap[sid] = 1

        assert found


class TestSlotDiscoveryIntegration:
    """Integration tests with actual slot operations."""

    def test_full_slot_lifecycle_with_cursor(self, ring):
        """Test complete slot lifecycle: claim -> use -> free -> reclaim."""
        from multiprocessing import Event

        cancel_event = Event()

        # Claim slot
        slot_id_1, _, _ = ring.claim_free_slot(cancel_event)
        assert slot_id_1 == 0

        # Use it (simulated by write_frame would happen here)
        ring.write_state(slot_id_1, 2)  # READY state

        # Read it (simulated)
        ring.write_state(slot_id_1, 3)  # READING state

        # Free it
        ring.write_state(slot_id_1, FREE)
        ring.free_slot_bitmap[slot_id_1] = 0

        # Claim again - should eventually reclaim it
        for _ in range(20):  # Try a few times to wrap around
            slot_id_2, _, _ = ring.claim_free_slot(cancel_event)
            if slot_id_2 == slot_id_1:
                break

        assert slot_id_2 == slot_id_1

    def test_claim_multiple_slots_sequential(self, ring):
        """Test claiming multiple slots in sequence."""
        from multiprocessing import Event

        cancel_event = Event()

        claimed = []
        for i in range(8):
            slot_id, _, _ = ring.claim_free_slot(cancel_event)
            claimed.append(slot_id)
            # Don't free them yet

        # First 8 claims should be sequential
        assert claimed == list(range(8))

    def test_alternating_claim_and_free(self, ring):
        """Test pattern of claiming and freeing slots."""
        from multiprocessing import Event

        cancel_event = Event()

        # Claim and free repeatedly
        claimed_ids = []
        for i in range(ring.slot_count * 2):
            slot_id, _, _ = ring.claim_free_slot(cancel_event)
            claimed_ids.append(slot_id)

            # Free immediately
            ring.write_state(slot_id, FREE)
            ring.free_slot_bitmap[slot_id] = 0

        # All slots should have been used
        unique_slots = set(claimed_ids)
        assert len(unique_slots) == ring.slot_count
