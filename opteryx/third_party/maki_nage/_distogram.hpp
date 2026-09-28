// Distogram - a compressed, streaming histogram (Ben-Haim & Tom-Tov).
// Originally distogram 3.0.0 by Romain Picard (MIT, see LICENSE.txt); this is
// Opteryx's native port. It carries no Python: the planner's statistics hold
// it by shared_ptr and read it from C++, and distogram.pyx wraps it for Python.
//
// Behaviour is the Cython implementation's, to the bit (the estimates the
// planner makes from it must not move), including two of its quirks:
//   - the in-place merge threshold `min_diff_` is recomputed only after a trim
//     has invalidated it, so an in-place merge or an insert without a trim
//     leaves it describing older bins;
//   - bounds (`min_`/`max_`) move only when a value opens a new bin.
// Bins are sorted by value; `prefix_` (running counts before each bin) is kept
// current by every mutation, so reads are const and lock-free.

#pragma once

#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

#include "opteryx/third_party/maki_nage/_distogram_core.h"

namespace maki_nage {

inline constexpr int64_t kDefaultBinCount = 50;
inline constexpr double kEpsilon = 1e-5;

struct Bin {
    double value;
    int64_t count;
};

class Distogram {
public:
    explicit Distogram(int64_t bin_count = kDefaultBinCount) : bin_count_(bin_count) {
        if (bin_count < 1) throw std::invalid_argument("a distogram needs at least one bin");
        bins_.reserve(static_cast<size_t>(bin_count) + 1);
    }

    // An equi-width histogram from per-bucket counts over [minimum, maximum], as
    // draken's `Vector.histogram_bucket` produces them: `bin = int(frac * (n - 1))`,
    // so buckets 0..n-2 split the range into n-1 equal slices and bucket n-1 is
    // a singleton holding exactly `maximum`. Centres use that slice width (a
    // `span / n` spacing put every centre half a slice low: geomean q-error
    // 11.6 -> 5.6 over 6 column shapes x 276 predicates); the top bucket's
    // centre, which would land past `maximum`, is clamped to it.
    static Distogram from_counts(const int64_t* counts, int64_t num_bins, double minimum, double maximum) {
        Distogram d(num_bins > kDefaultBinCount ? num_bins : kDefaultBinCount);
        d.min_ = minimum;
        d.max_ = maximum;
        if (num_bins == 0) return d;
        const int64_t total = distogram_sum_i64(counts, num_bins);
        if (minimum == maximum) {
            if (total != 0) {
                d.bins_.push_back(Bin{minimum, total});
                d.total_ = total;
            }
            d.rebuild_prefix();
            return d;
        }
        const double span = maximum - minimum;
        const int64_t slices = num_bins > 1 ? num_bins - 1 : 1;
        d.total_ = total;
        for (int64_t k = 0; k < num_bins; ++k) {
            const int64_t count = counts[k];
            if (count == 0) continue;
            double center = minimum + (k + 0.5) * span / slices;
            if (center > maximum) center = maximum;
            d.bins_.push_back(Bin{center, count});
        }
        d.rebuild_prefix();
        return d;
    }

    // Add `count` occurrences of `value`.
    void update(double value, int64_t count) {
        add(value, count);
        rebuild_prefix();
    }

    // Fold `other`'s bins into this one, bin by bin.
    void merge(const Distogram& other) {
        for (const Bin& b : other.bins_) add(b.value, b.count);
        rebuild_prefix();
    }

    // The estimated number of values <= `value`.
    double count_up_to(double value) const {
        const int64_t n = static_cast<int64_t>(bins_.size());
        if (n == 0) return 0.0;
        if (value < min_) return 0.0;
        if (value >= max_) return static_cast<double>(total_);
        if (value == min_) return 0.0;

        const double v0 = bins_[0].value;
        const double f0 = static_cast<double>(bins_[0].count);
        const double vl = bins_[n - 1].value;
        const double fl = static_cast<double>(bins_[n - 1].count);
        double result;
        if (value <= v0) {
            const double ratio = (value - min_) / (v0 - min_);
            result = ratio * f0 / 2;
        } else if (value >= vl) {
            const double ratio = (value - vl) / (max_ - vl);
            result = (1 + ratio) * fl / 2;
            result += prefix_[n - 1];
        } else {
            const int64_t i = floor_index(value);
            const double vi = bins_[i].value;
            const double fi = static_cast<double>(bins_[i].count);
            const double vj = bins_[i + 1].value;
            const double fj = static_cast<double>(bins_[i + 1].count);
            const double mb = fi + (fj - fi) / (vj - vi) * (value - vi);
            result = (fi + mb) / 2 * (value - vi) / (vj - vi);
            result += prefix_[i];
            result = result + fi / 2;
        }
        return result;
    }

    int64_t count() const { return total_; }
    int64_t bin_count() const { return static_cast<int64_t>(bins_.size()); }
    int64_t max_bin_count() const { return bin_count_; }
    double min() const { return min_; }
    double max() const { return max_; }
    const std::vector<Bin>& bins() const { return bins_; }

private:
    // The last bin whose value is below `target` (0 when none is).
    int64_t floor_index(double target) const {
        int64_t left = 0;
        int64_t right = static_cast<int64_t>(bins_.size());
        while (left < right) {
            const int64_t mid = (left + right) >> 1;
            if (bins_[mid].value < target) left = mid + 1;
            else right = mid;
        }
        return left > 0 ? left - 1 : 0;
    }

    void rebuild_prefix() {
        prefix_.resize(bins_.size());
        int64_t running = 0;
        for (size_t i = 0; i < bins_.size(); ++i) {
            prefix_[i] = running;
            running += bins_[i].count;
        }
    }

    // The bin to merge `value` into rather than opening a new one next to it,
    // or -1: its nearer neighbour, when that is closer than any two bins are.
    int64_t in_place_index(double value, int64_t index) {
        if (!min_diff_valid_) {
            double smallest = std::numeric_limits<double>::infinity();
            for (size_t i = 0; i + 1 < bins_.size(); ++i) {
                const double d = bins_[i + 1].value - bins_[i].value;
                if (d < smallest) smallest = d;
            }
            min_diff_ = std::isinf(smallest) ? 0.0 : smallest;
            min_diff_valid_ = true;
        }
        const double below = value - bins_[index - 1].value;
        const double above = bins_[index].value - value;
        const int64_t bin = below < above ? index - 1 : index;
        const double diff = below < above ? below : above;
        return diff < min_diff_ ? bin : -1;
    }

    // Merge the closest pair of bins until at most `bin_count_` remain.
    void trim() {
        while (static_cast<int64_t>(bins_.size()) > bin_count_) {
            size_t at = 0;
            double gap_min = std::numeric_limits<double>::infinity();
            for (size_t i = 1; i < bins_.size(); ++i) {
                const double gap = bins_[i].value - bins_[i - 1].value;
                if (gap < gap_min) {
                    gap_min = gap;
                    at = i - 1;
                }
            }
            const double v1 = bins_[at].value;
            const int64_t f1 = bins_[at].count;
            const double v2 = bins_[at + 1].value;
            const int64_t f2 = bins_[at + 1].count;
            bins_[at].value = (v1 * f1 + v2 * f2) / (f1 + f2);
            bins_[at].count = f1 + f2;
            bins_.erase(bins_.begin() + static_cast<std::ptrdiff_t>(at) + 1);
            min_diff_valid_ = false;
        }
    }

    // update() without the prefix rebuild (merge rebuilds once at its end).
    void add(double value, int64_t count) {
        if (count <= 0) throw std::invalid_argument("count must be strictly positive");
        const int64_t n = static_cast<int64_t>(bins_.size());
        int64_t index = 0;
        if (n > 0) {
            if (value <= bins_[0].value) {
                index = 0;
            } else if (value >= bins_[n - 1].value) {
                index = -1;
            } else {
                index = floor_index(value);
                if (index < n && bins_[index].value < value) ++index;
            }
            if (index >= 0 && index < n && std::fabs(bins_[index].value - value) < kEpsilon) {
                bins_[index].count += count;
                total_ += count;
                return;
            }
        }
        if (index > 0 && n >= bin_count_) {
            const int64_t k = in_place_index(value, index);
            if (k >= 0) {
                const double v = bins_[k].value;
                const int64_t f = bins_[k].count;
                bins_[k].value = (v * f + value * count) / (f + count);
                bins_[k].count = f + count;
                total_ += count;
                return;
            }
        }
        if (index == -1) bins_.push_back(Bin{value, count});
        else bins_.insert(bins_.begin() + index, Bin{value, count});
        total_ += count;
        if (std::isinf(min_) || value < min_) min_ = value;
        if (std::isinf(max_) || value > max_) max_ = value;
        trim();
    }

    int64_t bin_count_;
    std::vector<Bin> bins_;
    std::vector<int64_t> prefix_;
    int64_t total_ = 0;
    double min_ = std::numeric_limits<double>::infinity();
    double max_ = -std::numeric_limits<double>::infinity();
    double min_diff_ = std::numeric_limits<double>::infinity();
    bool min_diff_valid_ = false;
};

}  // namespace maki_nage
