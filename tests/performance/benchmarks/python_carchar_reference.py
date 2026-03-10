from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Callable
from typing import Iterator


EMPTY = 0x80
GROUP_SIZE = 16
MASK_64 = (1 << 64) - 1
MIN_CAPACITY = GROUP_SIZE


def _next_power_of_two(value: int) -> int:
    if value <= 1:
        return 1
    return 1 << (value - 1).bit_length()


def _normalize_key(key: int) -> int:
    if not isinstance(key, int):
        raise TypeError("Carchar keys must be integers")
    return key & MASK_64


def _tag(key: int) -> int:
    tag = (key >> 57) & 0x7F
    if tag in (0x00, 0x7E):
        return 0x01
    return tag


@dataclass(frozen=True, slots=True)
class CarcharStats:
    capacity: int
    size: int
    resize_count: int
    lookup_count: int
    insert_count: int
    total_probes: int
    max_probe_length: int
    bytes_estimate: int

    @property
    def load_factor(self) -> float:
        if self.capacity == 0:
            return 0.0
        return self.size / self.capacity

    @property
    def average_probe_length(self) -> float:
        operations = self.lookup_count + self.insert_count
        if operations == 0:
            return 0.0
        return self.total_probes / operations


class CarcharIndex:
    __slots__ = (
        "_capacity",
        "_control",
        "_hashes",
        "_payload_refs",
        "_size",
        "_load_factor",
        "_resize_count",
        "_lookup_count",
        "_insert_count",
        "_total_probes",
        "_max_probe_length",
    )

    def __init__(self, initial_capacity: int = MIN_CAPACITY, load_factor: float = 0.80) -> None:
        if not 0.0 < load_factor < 1.0:
            raise ValueError("load_factor must be between 0 and 1")

        capacity = max(MIN_CAPACITY, _next_power_of_two(initial_capacity))
        self._capacity = capacity
        self._control = [EMPTY] * capacity
        self._hashes = [0] * capacity
        self._payload_refs = [-1] * capacity
        self._size = 0
        self._load_factor = load_factor
        self._resize_count = 0
        self._lookup_count = 0
        self._insert_count = 0
        self._total_probes = 0
        self._max_probe_length = 0

    def __len__(self) -> int:
        return self._size

    @property
    def capacity(self) -> int:
        return self._capacity

    def reserve(self, expected_entries: int) -> None:
        if expected_entries <= 0:
            return
        target = _next_power_of_two(
            max(MIN_CAPACITY, ceil(expected_entries / self._load_factor))
        )
        if target > self._capacity:
            self._resize(target)

    def lookup(self, key: int) -> int | None:
        key = _normalize_key(key)
        slot, found, probes = self._find_slot(key)
        self._lookup_count += 1
        self._record_probe_length(probes)
        if not found:
            return None
        return self._payload_refs[slot]

    def insert_new(self, key: int, payload_ref: int) -> int:
        self._ensure_insert_capacity()
        key = _normalize_key(key)
        slot, found, probes = self._find_slot(key)
        self._insert_count += 1
        self._record_probe_length(probes)
        if found:
            raise KeyError(f"key {key} already exists")
        self._insert_at(slot, key, payload_ref)
        return slot

    def find_or_insert(
        self, key: int, payload_factory: Callable[[], int]
    ) -> tuple[int, bool]:
        self._ensure_insert_capacity()
        key = _normalize_key(key)
        slot, found, probes = self._find_slot(key)
        self._insert_count += 1
        self._record_probe_length(probes)
        if found:
            return self._payload_refs[slot], False
        payload_ref = payload_factory()
        self._insert_at(slot, key, payload_ref)
        return payload_ref, True

    def items(self) -> Iterator[tuple[int, int]]:
        for slot in range(self._capacity):
            if self._control[slot] != EMPTY:
                yield self._hashes[slot], self._payload_refs[slot]

    def stats(self) -> CarcharStats:
        return CarcharStats(
            capacity=self._capacity,
            size=self._size,
            resize_count=self._resize_count,
            lookup_count=self._lookup_count,
            insert_count=self._insert_count,
            total_probes=self._total_probes,
            max_probe_length=self._max_probe_length,
            bytes_estimate=self._estimated_bytes(),
        )

    def _estimated_bytes(self) -> int:
        return self._capacity * (1 + 8 + 8)

    def _ensure_insert_capacity(self) -> None:
        if self._size + 1 > int(self._capacity * self._load_factor):
            self._resize(self._capacity * 2)

    def _record_probe_length(self, probes: int) -> None:
        self._total_probes += probes
        if probes > self._max_probe_length:
            self._max_probe_length = probes

    def _insert_at(self, slot: int, key: int, payload_ref: int) -> None:
        self._control[slot] = _tag(key)
        self._hashes[slot] = key
        self._payload_refs[slot] = payload_ref
        self._size += 1

    def _find_slot(self, key: int) -> tuple[int, bool, int]:
        mask = self._capacity - 1
        tag = _tag(key)
        slot = key & mask
        probes = 0

        while probes < self._capacity:
            probes += 1
            control = self._control[slot]
            if control == EMPTY:
                return slot, False, probes
            if control == tag and self._hashes[slot] == key:
                return slot, True, probes
            slot = (slot + 1) & mask

        raise RuntimeError("Carchar probe exhausted table capacity")

    def _resize(self, new_capacity: int) -> None:
        new_capacity = max(MIN_CAPACITY, _next_power_of_two(new_capacity))
        old_hashes = self._hashes
        old_payloads = self._payload_refs
        old_control = self._control

        self._capacity = new_capacity
        self._control = [EMPTY] * new_capacity
        self._hashes = [0] * new_capacity
        self._payload_refs = [-1] * new_capacity
        self._size = 0
        self._resize_count += 1

        for slot, control in enumerate(old_control):
            if control == EMPTY:
                continue
            new_slot, found, _ = self._find_slot(old_hashes[slot])
            if found:
                raise RuntimeError("duplicate key encountered while resizing Carchar")
            self._insert_at(new_slot, old_hashes[slot], old_payloads[slot])


class CarcharJoinIndex(CarcharIndex):
    __slots__ = (
        "_row_counts",
        "_row_inline0",
        "_row_inline1",
        "_row_overflow_head",
        "_row_overflow_tail",
        "_overflow_values",
        "_overflow_next",
    )

    def __init__(self, initial_capacity: int = MIN_CAPACITY, load_factor: float = 0.80) -> None:
        super().__init__(initial_capacity=initial_capacity, load_factor=load_factor)
        self._row_counts: list[int] = []
        self._row_inline0: list[int] = []
        self._row_inline1: list[int] = []
        self._row_overflow_head: list[int] = []
        self._row_overflow_tail: list[int] = []
        self._overflow_values: list[int] = []
        self._overflow_next: list[int] = []

    def insert_row(self, key: int, row_id: int) -> tuple[int, bool]:
        payload_ref, created = self.find_or_insert(key, lambda: self._allocate_row_list(row_id))
        if not created:
            self.append_join_row(payload_ref, row_id)
        return payload_ref, created

    def append_join_row(self, payload_ref: int, row_id: int) -> None:
        count = self._row_counts[payload_ref]
        if count == 1:
            self._row_inline1[payload_ref] = row_id
        elif count >= 2:
            self._append_overflow(payload_ref, row_id)
        else:
            raise RuntimeError("invalid row-list count")
        self._row_counts[payload_ref] = count + 1

    def rows_for(self, key: int) -> list[int]:
        payload_ref = self.lookup(key)
        if payload_ref is None:
            return []
        return self.rows_from_payload(payload_ref)

    def row_count_for(self, key: int) -> int:
        payload_ref = self.lookup(key)
        if payload_ref is None:
            return 0
        return self._row_counts[payload_ref]

    def probe_row_count_sum(self, keys) -> int:
        total = 0
        for key in keys:
            total += self.row_count_for(int(key))
        return total

    def rows_from_payload(self, payload_ref: int) -> list[int]:
        count = self._row_counts[payload_ref]
        rows: list[int] = []
        if count >= 1:
            rows.append(self._row_inline0[payload_ref])
        if count >= 2:
            rows.append(self._row_inline1[payload_ref])

        overflow_ref = self._row_overflow_head[payload_ref]
        while overflow_ref != -1:
            rows.append(self._overflow_values[overflow_ref])
            overflow_ref = self._overflow_next[overflow_ref]
        return rows

    def _allocate_row_list(self, row_id: int) -> int:
        payload_ref = len(self._row_counts)
        self._row_counts.append(1)
        self._row_inline0.append(row_id)
        self._row_inline1.append(-1)
        self._row_overflow_head.append(-1)
        self._row_overflow_tail.append(-1)
        return payload_ref

    def _append_overflow(self, payload_ref: int, row_id: int) -> None:
        overflow_ref = len(self._overflow_values)
        self._overflow_values.append(row_id)
        self._overflow_next.append(-1)

        tail = self._row_overflow_tail[payload_ref]
        if tail == -1:
            self._row_overflow_head[payload_ref] = overflow_ref
        else:
            self._overflow_next[tail] = overflow_ref
        self._row_overflow_tail[payload_ref] = overflow_ref

    def _estimated_bytes(self) -> int:
        base = super()._estimated_bytes()
        row_entries = len(self._row_counts) * (4 + 4 + 4 + 4 + 4)
        overflow_entries = len(self._overflow_values) * (4 + 4)
        return base + row_entries + overflow_entries
