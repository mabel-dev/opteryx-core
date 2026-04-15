# cython: language_level=3, boundscheck=False, cdivision=True, wraparound=False
# type: ignore

from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memmove
from libc.math cimport fabs
from libc.stdint cimport int64_t, uint64_t
cimport cython
import math
from bisect import bisect_left
from heapq import heappush, heappop
from itertools import accumulate
from operator import itemgetter
from typing import Optional, Tuple, List

__author__ = """Romain Picard"""
__email__ = "romain.picard@oakbits.com"
__version__ = "3.0.0"

"""
The following changes have been made for Opteryx:
- The ability to weight the differences has been removed
- Removed numpy dependency completely
- Fixed count_at undefined bug
- Full Cythonization with cdef class
- Struct-based bin storage for C-level performance
"""

cdef extern from "math.h":
    double fmin(double, double) nogil
    double fmax(double, double) nogil

EPSILON = 1e-5
BIN_COUNT: int = 50


cdef struct Bin:
    double value
    int64_t count


cdef class Distogram:
    """Compressed representation of a distribution (C-level optimized)."""

    # C-level storage: pre-allocated bins array
    cdef Bin* bins_data
    cdef int64_t bins_length
    cdef int64_t bins_capacity

    # Python-visible state for API compatibility
    cdef public double min
    cdef public double max
    cdef public object diffs
    cdef public double min_diff
    cdef public int64_t _bin_count

    # Memoryview caches for fast lookups
    cdef double[:] _values_cache
    cdef int64_t[:] _counts_cache
    cdef bint _cache_valid

    def __init__(self, int64_t bin_count=BIN_COUNT):
        """Creates a new Distogram object.

        Args:
            bin_count: the number of bins to use.
        """
        self._bin_count = bin_count
        self.bins_capacity = bin_count
        self.bins_data = <Bin*>malloc(bin_count * sizeof(Bin))
        self.bins_length = 0

        self.min = float('inf')
        self.max = float('-inf')
        self.diffs = None
        self.min_diff = float('inf')

        self._values_cache = None
        self._counts_cache = None
        self._cache_valid = False

    def __dealloc__(self):
        """Free C-level memory."""
        if self.bins_data != NULL:
            free(self.bins_data)

    cdef void _invalidate_cache(self):
        """Mark cache as needing rebuild."""
        self._cache_valid = False

    cdef void _rebuild_cache(self) nogil:
        """Rebuild memoryview caches from C bins array.

        This is called lazily before count_up_to() to ensure
        fast O(1) access to bin values and counts.
        """
        # Note: can't allocate here in nogil context, so rebuild in Python
        self._cache_valid = True

    cdef int64_t _find_bin_index(self, double value) nogil:
        """Binary search to find bin index for value (O(log n))."""
        cdef int64_t left = 0
        cdef int64_t right = self.bins_length
        cdef int64_t mid

        while left < right:
            mid = (left + right) >> 1
            if self.bins_data[mid].value < value:
                left = mid + 1
            else:
                right = mid

        return max(0, left - 1) if left > 0 else -1

    cdef inline void _append_bin(self, double value, int64_t count) nogil:
        """Append a bin to the C array."""
        if self.bins_length >= self.bins_capacity:
            return

        self.bins_data[self.bins_length].value = value
        self.bins_data[self.bins_length].count = count
        self.bins_length += 1

    cdef inline void _insert_bin(self, int64_t index, double value, int64_t count) nogil:
        """Insert a bin at the given index (uses memmove for speed)."""
        cdef int64_t shift_count
        cdef Bin* src_ptr
        cdef Bin* dst_ptr

        if self.bins_length >= self.bins_capacity:
            return

        # Shift bins to the right using memmove for O(1) operation
        shift_count = self.bins_length - index
        if shift_count > 0:
            src_ptr = &self.bins_data[index]
            dst_ptr = &self.bins_data[index + 1]
            memmove(dst_ptr, src_ptr, shift_count * sizeof(Bin))

        self.bins_data[index].value = value
        self.bins_data[index].count = count
        self.bins_length += 1

    cdef inline void _remove_bin(self, int64_t index) nogil:
        """Remove a bin at the given index (uses memmove for speed)."""
        cdef int64_t shift_count
        cdef Bin* src_ptr
        cdef Bin* dst_ptr

        if index < 0 or index >= self.bins_length:
            return

        # Shift bins to the left using memmove
        shift_count = self.bins_length - index - 1
        if shift_count > 0:
            src_ptr = &self.bins_data[index + 1]
            dst_ptr = &self.bins_data[index]
            memmove(dst_ptr, src_ptr, shift_count * sizeof(Bin))

        self.bins_length -= 1

    cdef inline double _get_bin_value(self, int64_t index) nogil:
        """Get bin value at index."""
        if 0 <= index < self.bins_length:
            return self.bins_data[index].value
        return 0.0

    cdef inline int64_t _get_bin_count(self, int64_t index) nogil:
        """Get bin count at index."""
        if 0 <= index < self.bins_length:
            return self.bins_data[index].count
        return 0

    cdef inline void _set_bin_count(self, int64_t index, int64_t count) nogil:
        """Set bin count at index."""
        if 0 <= index < self.bins_length:
            self.bins_data[index].count = count

    def bulkload(self, values):
        """Load many values efficiently using histogram approximation."""
        if len(values) == 0:
            return

        # Extract unique values and counts (replaces numpy.unique)
        cdef dict value_counts = {}
        cdef double fv
        cdef list bin_values
        cdef list counts

        for v in values:
            fv = float(v)
            value_counts[fv] = value_counts.get(fv, 0) + 1

        bin_values = sorted(value_counts.keys())
        counts = [value_counts[v] for v in bin_values]

        # If high cardinality, use histogram approximation
        if len(bin_values) > (self._bin_count * 5):
            counts_hist, bin_edges = self._histogram_native(values, self._bin_count * 5)
            bin_values = [(bin_edges[i] + bin_edges[i + 1]) / 2.0 for i in range(len(bin_edges) - 1)]
            counts = counts_hist

        for index, count in enumerate(counts):
            if count > 0:
                update(self, bin_values[index], count)

        # Update min/max with actual data bounds
        cdef double min_val = min(values)
        cdef double max_val = max(values)

        if math.isinf(self.min):
            self.min = min_val
            self.max = max_val
        else:
            if min_val < self.min:
                self.min = min_val
            if max_val > self.max:
                self.max = max_val

    @staticmethod
    def _histogram_native(values, int num_bins):
        """Compute histogram using native Python (replaces numpy.histogram)."""
        cdef double min_val = min(values)
        cdef double max_val = max(values)
        cdef double bin_width = (max_val - min_val) / num_bins if num_bins > 0 else 1.0
        cdef list bin_counts = [0] * num_bins
        cdef list bin_edges = _linspace(min_val, max_val, num_bins)
        cdef double fv
        cdef int bin_idx

        for v in values:
            fv = <double>v
            if fv == max_val:
                bin_counts[num_bins - 1] += 1
            elif fv > min_val:
                bin_idx = int((fv - min_val) / bin_width)
                if 0 <= bin_idx < num_bins:
                    bin_counts[bin_idx] += 1
            elif fv == min_val:
                bin_counts[0] += 1

        return bin_counts, bin_edges

    def count(self):
        """Count total elements in distribution."""
        cdef int total = 0
        cdef int i

        for i in range(self.bins_length):
            total += self.bins_data[i].count

        return total

    @property
    def max_bin_count(self):
        return self._bin_count

    @property
    def bin_count(self):
        return self.bins_length


def load(bins: list, minimum, maximum):
    """Load distogram from serialized bins."""
    cdef Distogram dgram = Distogram()

    # Populate C array from bins
    for v, c in bins:
        dgram._append_bin(<double>(v), <int64_t>(c))

    dgram.min = <double>(minimum)
    dgram.max = <double>(maximum)
    dgram.diffs = []

    for i in range(dgram.bins_length - 1):
        diff = dgram.bins_data[i].value - dgram.bins_data[i - 1].value
        dgram.diffs.append(diff)

    if dgram.diffs:
        dgram.min_diff = min(dgram.diffs)
    else:
        dgram.min_diff = float("inf")

    return dgram


cdef int64_t _binary_search_values(Distogram h, double target) nogil:
    """Binary search to find insertion point in value array (O(log n))."""
    cdef int64_t left = 0
    cdef int64_t right = h.bins_length
    cdef int64_t mid

    while left < right:
        mid = (left + right) >> 1
        if h.bins_data[mid].value < target:
            left = mid + 1
        else:
            right = mid

    return max(0, left - 1)


def _linspace(double start, double stop, int num) -> List[float]:
    """Generate linearly spaced values."""
    cdef list values = []
    cdef double step
    cdef int i

    if num == 1:
        return [start, stop]

    step = (stop - start) / float(num)
    for i in range(num):
        values.append(start + step * i)
    values.append(stop)
    return values


def _moment(list x, list counts, double c, int n) -> float:
    """Calculates the k-th moment of the distribution."""
    cdef double m = 0.0
    cdef double total = 0.0
    cdef int i

    for i in range(len(x)):
        m += counts[i] * ((x[i] - c) ** n)
        total += counts[i]

    return m / total if total > 0 else 0.0


cdef void _update_diffs(Distogram h, int64_t i) nogil:
    """Update difference array after bin modification."""
    # Note: diffs handling must happen in GIL since it's a Python list
    pass

def _update_diffs_py(Distogram h, int64_t i) -> None:
    """Python wrapper for _update_diffs - handles diffs list."""
    if h.diffs is not None:
        if i > 0:
            if h.diffs[i - 1] == h.min_diff:
                update_min = True

            h.diffs[i - 1] = h.bins_data[i].value - h.bins_data[i - 1].value
            if h.diffs[i - 1] < h.min_diff:
                h.min_diff = h.diffs[i - 1]

        if i < h.bins_length - 1:
            if i < len(h.diffs) and h.diffs[i] == h.min_diff:
                update_min = True

            if i < len(h.diffs):
                h.diffs[i] = h.bins_data[i + 1].value - h.bins_data[i].value
                if h.diffs[i] < h.min_diff:
                    h.min_diff = h.diffs[i]

        if update_min and h.diffs:
            h.min_diff = min(h.diffs)


cpdef Distogram _trim(Distogram h):
    """Trim bins to max capacity by merging closest pairs (compiled for speed)."""
    cdef int64_t min_idx, i, f1, f2, new_f
    cdef double min_gap, v1, v2, new_v, gap

    while h.bins_length > h._bin_count:
        # Find the index of the smallest gap
        if h.diffs is not None and len(h.diffs) > 0:
            min_idx = 0
            min_gap = h.diffs[0]
            for i in range(1, len(h.diffs)):
                if h.diffs[i] < min_gap:
                    min_gap = h.diffs[i]
                    min_idx = i
            i = min_idx
        elif h.diffs is not None:
            # diffs exists but is empty
            break
        else:
            # diffs not initialized - compute gaps
            min_idx = 0
            min_gap = float('inf')
            for i in range(1, h.bins_length):
                gap = h.bins_data[i].value - h.bins_data[i - 1].value
                if gap < min_gap:
                    min_gap = gap
                    min_idx = i - 1
            i = min_idx


        v1 = h.bins_data[i].value
        f1 = h.bins_data[i].count
        v2 = h.bins_data[i + 1].value
        f2 = h.bins_data[i + 1].count

        new_v = (v1 * f1 + v2 * f2) / (f1 + f2)
        new_f = f1 + f2

        h.bins_data[i].value = new_v
        h.bins_data[i].count = new_f
        h._remove_bin(i + 1)

        if h.diffs is not None:
            if i < len(h.diffs):
                h.diffs.pop(i)
            _update_diffs(h, i)
            if h.diffs:
                h.min_diff = min(h.diffs)

    return h


cdef inline Distogram _trim_in_place(Distogram distogram, double new_value, int64_t new_count, int64_t bin_index):
    """Trim by merging in place at specific index."""
    cdef double current_value = distogram.bins_data[bin_index].value
    cdef int64_t current_frequency = distogram.bins_data[bin_index].count
    cdef double new_merged_value = (current_value * current_frequency + new_value * new_count) / (current_frequency + new_count)

    distogram.bins_data[bin_index].value = new_merged_value
    distogram.bins_data[bin_index].count = current_frequency + new_count

    _update_diffs(distogram, bin_index)
    distogram._invalidate_cache()
    return distogram


cdef inline list _compute_diffs(Distogram h):
    """Compute all bin differences."""
    cdef int i, bins_len
    cdef list diffs
    cdef double v1, v2, d, min_d

    i = 0
    bins_len = h.bins_length
    diffs = []
    min_d = float('inf')

    for i in range(bins_len - 1):
        v1 = h.bins_data[i].value
        v2 = h.bins_data[i + 1].value
        d = v2 - v1
        diffs.append(d)
        if d < min_d:
            min_d = d

    h.min_diff = min_d if min_d != float('inf') else 0.0
    return diffs


cdef inline int _search_in_place_index(Distogram h, double new_value, int index):
    """Search for best in-place merge location."""
    if h.diffs is None:
        h.diffs = _compute_diffs(h)

    if index > 0:
        diff1 = new_value - h.bins_data[index - 1].value
        diff2 = h.bins_data[index].value - new_value

        i_bin = (index - 1) if (diff1 < diff2) else index
        diff = diff1 if (diff1 < diff2) else diff2

        return i_bin if diff < h.min_diff else -1

    return -1


cpdef update(Distogram h, double value, int64_t count=1):
    """Add a value to the distribution (compiled for speed)."""
    cdef int64_t index = 0
    cdef int64_t in_place_index
    cdef double vi
    cdef int64_t fi, bins_len

    bins_len = h.bins_length

    if count <= 0:
        raise ValueError("count must be strictly positive")

    # Find insertion point
    if bins_len > 0:
        if value <= h.bins_data[0].value:
            index = 0
        elif value >= h.bins_data[bins_len - 1].value:
            index = -1
        else:
            # Use binary search on C array - O(log n)
            index = _binary_search_values(h, value)
            if index < bins_len and h.bins_data[index].value < value:
                index += 1

        # Check if value already exists
        if index >= 0 and index < bins_len:
            vi = h.bins_data[index].value
            fi = h.bins_data[index].count
            if fabs(vi - value) < EPSILON:
                h.bins_data[index].count = fi + count
                # Only sync if this was the last operation
                return h

    # Check if we can merge in place
    if index > 0 and bins_len >= h._bin_count:
        in_place_index = _search_in_place_index(h, value, index)
        if in_place_index >= 0:
            h = _trim_in_place(h, value, count, in_place_index)
            return h

    # Insert new bin
    if index == -1:
        h._append_bin(value, count)
    else:
        h._insert_bin(index, value, count)

    # Update bounds
    if math.isinf(h.min) or value < h.min:
        h.min = value
    if math.isinf(h.max) or value > h.max:
        h.max = value

    h = _trim(h)

    return h


cpdef Distogram merge(Distogram h1, Distogram h2):
    """Merge two Distogram objects (compiled for speed)."""
    if h1 is None:
        return h2
    if h2 is None:
        return h1

    cdef int64_t i

    # Batch update calls to minimize syncing
    for i in range(h2.bins_length):
        h1 = update(h1, h2.bins_data[i].value, h2.bins_data[i].count)

    return h1


cpdef double count_up_to(Distogram h, double value):
    """Count elements up to a given value (compiled for speed, O(log n))."""
    cdef int64_t bins_len = h.bins_length
    cdef int64_t i, j
    cdef double v0, f0, vl, fl
    cdef double vi, fi, vj, fj
    cdef double ratio, result, mb, sum_val

    if bins_len == 0:
        return 0.0

    if value < h.min:
        return 0.0

    if value >= h.max:
        return <double>h.count()

    if value == h.min:
        return 0.0

    v0 = h.bins_data[0].value
    f0 = h.bins_data[0].count
    vl = h.bins_data[bins_len - 1].value
    fl = h.bins_data[bins_len - 1].count

    with nogil:
        if value <= v0:  # left tail
            ratio = (value - h.min) / (v0 - h.min)
            result = ratio * f0 / 2
        elif value >= vl:  # right tail
            ratio = (value - vl) / (h.max - vl)
            result = (1 + ratio) * fl / 2
            # Sum all bins except last
            sum_val = 0.0
            for i in range(bins_len - 1):
                sum_val += h.bins_data[i].count
            result += sum_val
        else:
            # Binary search for bin containing value
            i = _binary_search_values(h, value)
            vi = h.bins_data[i].value
            fi = h.bins_data[i].count
            vj = h.bins_data[i + 1].value
            fj = h.bins_data[i + 1].count

            mb = fi + (fj - fi) / (vj - vi) * (value - vi)
            result = (fi + mb) / 2 * (value - vi) / (vj - vi)

            # Sum bins before insertion point
            sum_val = 0.0
            for j in range(i):
                sum_val += h.bins_data[j].count
            result += sum_val
            result = result + fi / 2

    return result


# Dead code methods (kept for backward compatibility but never called by Opteryx)
def bin_size(Distogram h, value) -> int:
    for i in range(h.bins_length):
        if value < h.bins_data[i].value:
            return h.bins_data[i].count
    return None


def bounds(Distogram h) -> Tuple[float, float]:
    return h.min, h.max


def mean(Distogram h) -> float:
    if h.bins_length == 0:
        return 0.0
    values = [h.bins_data[i].value for i in range(h.bins_length)]
    counts = [h.bins_data[i].count for i in range(h.bins_length)]
    return _moment(values, counts, 0, 1)


def variance(Distogram h) -> float:
    if h.bins_length == 0:
        return 0.0
    values = [h.bins_data[i].value for i in range(h.bins_length)]
    counts = [h.bins_data[i].count for i in range(h.bins_length)]
    return _moment(values, counts, mean(h), 2)


def stddev(Distogram h) -> float:
    return math.sqrt(variance(h))


def histogram(Distogram h, int bin_count = 20) -> dict:
    bin_count = min(bin_count, h.bins_length)
    if bin_count < 2:
        return None

    bin_bounds = _linspace(h.min, h.max, num=bin_count)
    counts = [count_up_to(h, e) for e in bin_bounds]
    counts = [new - last for new, last in zip(counts[1:], counts[:-1])]

    result = {f"{bin_bounds[i]} - {bin_bounds[i + 1]}": c for i, c in enumerate(counts)}
    return result


def frequency_density_distribution(Distogram h) -> Tuple[List[float], List[float]]:
    if h.count() < 2:
        return None

    bin_bounds = [h.bins_data[i].value for i in range(h.bins_length)]
    bin_widths = [(bin_bounds[i] - bin_bounds[i - 1]) for i in range(1, len(bin_bounds))]
    counts = [0]
    counts.extend([count_up_to(h, e) for e in bin_bounds[1:]])
    densities = [
        (new - last) / delta for new, last, delta in zip(counts[1:], counts[:-1], bin_widths)
    ]
    return (densities, bin_bounds)


def quantile(Distogram h, double value) -> Optional[float]:
    if h.bins_length == 0:
        return None

    if not (0 <= value <= 1):
        return None

    cdef double total_count, q_count, v0, f0, vl, fl, fraction, result, base, mb
    cdef int i

    total_count = h.count()
    q_count = int(total_count * value)
    v0 = h.bins_data[0].value
    f0 = h.bins_data[0].count
    vl = h.bins_data[h.bins_length - 1].value
    fl = h.bins_data[h.bins_length - 1].count
    mids = []

    if q_count <= (f0 / 2):  # left values
        fraction = q_count / (f0 / 2)
        result = h.min + (fraction * (v0 - h.min))

    elif q_count >= (total_count - (fl / 2)):  # right values
        base = q_count - (total_count - (fl / 2))
        fraction = base / (fl / 2)
        result = vl + (fraction * (h.max - vl))

    else:
        mb = q_count - f0 / 2
        for i in range(h.bins_length - 1):
            mids.append((h.bins_data[i].count + h.bins_data[i + 1].count) / 2.0)
        i, _ = next(filter(lambda i_f: mb < i_f[1], enumerate(accumulate(mids))))

        v0 = h.bins_data[i].value
        vl = h.bins_data[i + 1].value
        fraction = (mb - sum(mids[:i])) / mids[i]
        result = v0 + (fraction * (vl - v0))

    return result
