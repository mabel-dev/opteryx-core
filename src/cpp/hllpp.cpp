#include "hllpp.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <stdexcept>

namespace {

constexpr long double kTwoTo64 = 18446744073709551616.0L;

}  // namespace

HllppSketch::HllppSketch(
    int precision,
    std::size_t explicit_threshold,
    std::size_t sparse_threshold
)
    : precision_(precision),
      register_count_(0),
      explicit_threshold_(0),
      sparse_threshold_(0),
      mode_(Mode::EXPLICIT) {
    if (precision < kMinPrecision || precision > kMaxPrecision) {
        throw std::invalid_argument("HLL++ precision must be between 4 and 18");
    }

    register_count_ = static_cast<std::size_t>(1) << precision_;
    explicit_threshold_ = explicit_threshold > 0
        ? explicit_threshold
        : default_explicit_threshold(register_count_);
    sparse_threshold_ = sparse_threshold > 0
        ? sparse_threshold
        : default_sparse_threshold(register_count_);
    sparse_threshold_ = std::min(sparse_threshold_, register_count_);

    explicit_hashes_.reserve(explicit_threshold_);
}

int HllppSketch::precision() const noexcept {
    return precision_;
}

std::size_t HllppSketch::register_count() const noexcept {
    return register_count_;
}

HllppSketch::Mode HllppSketch::mode() const noexcept {
    return mode_;
}

std::string_view HllppSketch::mode_name() const noexcept {
    switch (mode_) {
        case Mode::EXPLICIT:
            return "explicit";
        case Mode::SPARSE:
            return "sparse";
        case Mode::DENSE:
            return "dense";
    }
    return "unknown";
}

void HllppSketch::reset() {
    mode_ = Mode::EXPLICIT;
    explicit_hashes_.clear();
    sparse_registers_.clear();
    dense_registers_.clear();
    explicit_hashes_.reserve(explicit_threshold_);
}

void HllppSketch::add_hash(std::uint64_t hash) {
    if (mode_ == Mode::EXPLICIT) {
        if (contains_explicit_hash(hash)) {
            return;
        }

        if (explicit_hashes_.size() < explicit_threshold_) {
            explicit_hashes_.push_back(hash);
            return;
        }

        promote_explicit_to_sparse();
    }

    add_register(register_index(hash, precision_), rho(hash, precision_));
}

void HllppSketch::add_hashes(const std::uint64_t* hashes, std::size_t count) {
    if (hashes == nullptr || count == 0) {
        return;
    }
    for (std::size_t i = 0; i < count; ++i) {
        add_hash(hashes[i]);
    }
}

bool HllppSketch::merge(const HllppSketch& other) {
    if (precision_ != other.precision_) {
        return false;
    }

    if (this == &other) {
        return true;
    }

    if (other.mode_ == Mode::EXPLICIT) {
        for (std::uint64_t hash : other.explicit_hashes_) {
            add_hash(hash);
        }
        return true;
    }

    if (mode_ == Mode::EXPLICIT) {
        promote_explicit_to_sparse();
    }

    if (other.mode_ == Mode::DENSE) {
        promote_to_dense();
        for (std::size_t i = 0; i < register_count_; ++i) {
            dense_registers_[i] = std::max(dense_registers_[i], other.dense_registers_[i]);
        }
        return true;
    }

    if (mode_ == Mode::DENSE) {
        for (const auto& entry : other.sparse_registers_) {
            update_dense(entry.first, entry.second);
        }
        return true;
    }

    for (const auto& entry : other.sparse_registers_) {
        update_sparse(entry.first, entry.second);
    }
    if (sparse_registers_.size() > sparse_threshold_) {
        promote_sparse_to_dense();
    }
    return true;
}

std::uint64_t HllppSketch::estimate() const {
    if (mode_ == Mode::EXPLICIT) {
        return static_cast<std::uint64_t>(explicit_hashes_.size());
    }

    const double estimate_value = raw_estimate();
    const std::size_t zeros = zero_registers();
    if (zeros > 0) {
        const double linear = static_cast<double>(register_count_) *
            std::log(static_cast<double>(register_count_) / static_cast<double>(zeros));
        if (linear <= (2.5 * static_cast<double>(register_count_))) {
            return static_cast<std::uint64_t>(std::llround(linear));
        }
    }

    long double corrected = static_cast<long double>(estimate_value);
    if (corrected > (kTwoTo64 / 30.0L)) {
        corrected = -kTwoTo64 * std::log1pl(-corrected / kTwoTo64);
    }

    if (corrected < 0.0L) {
        return 0;
    }
    if (corrected >= static_cast<long double>(std::numeric_limits<std::uint64_t>::max())) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return static_cast<std::uint64_t>(std::llround(static_cast<double>(corrected)));
}

std::size_t HllppSketch::explicit_size() const noexcept {
    return explicit_hashes_.size();
}

std::size_t HllppSketch::sparse_size() const noexcept {
    return sparse_registers_.size();
}

std::size_t HllppSketch::default_explicit_threshold(std::size_t register_count) noexcept {
    return std::min<std::size_t>(256, std::max<std::size_t>(64, register_count / 16));
}

std::size_t HllppSketch::default_sparse_threshold(std::size_t register_count) noexcept {
    return std::max<std::size_t>(32, register_count / 4);
}

double HllppSketch::alpha_for(std::size_t register_count) noexcept {
    if (register_count == 16) {
        return 0.673;
    }
    if (register_count == 32) {
        return 0.697;
    }
    if (register_count == 64) {
        return 0.709;
    }
    return 0.7213 / (1.0 + 1.079 / static_cast<double>(register_count));
}

std::uint8_t HllppSketch::rho(std::uint64_t hash, int precision) noexcept {
    const std::uint64_t shifted = hash << precision;
    if (shifted == 0) {
        return static_cast<std::uint8_t>(65 - precision);
    }
    return static_cast<std::uint8_t>(std::countl_zero(shifted) + 1);
}

std::uint32_t HllppSketch::register_index(std::uint64_t hash, int precision) noexcept {
    return static_cast<std::uint32_t>(hash >> (64 - precision));
}

bool HllppSketch::contains_explicit_hash(std::uint64_t hash) const noexcept {
    return std::find(explicit_hashes_.begin(), explicit_hashes_.end(), hash) != explicit_hashes_.end();
}

void HllppSketch::add_register(std::uint32_t index, std::uint8_t value) {
    if (mode_ == Mode::DENSE) {
        update_dense(index, value);
        return;
    }

    update_sparse(index, value);
    if (sparse_registers_.size() > sparse_threshold_) {
        promote_sparse_to_dense();
    }
}

void HllppSketch::update_sparse(std::uint32_t index, std::uint8_t value) {
    mode_ = Mode::SPARSE;
    auto [it, inserted] = sparse_registers_.try_emplace(index, value);
    if (!inserted && value > it->second) {
        it->second = value;
    }
}

void HllppSketch::update_dense(std::uint32_t index, std::uint8_t value) {
    if (dense_registers_.empty()) {
        dense_registers_.assign(register_count_, 0);
    }
    if (value > dense_registers_[index]) {
        dense_registers_[index] = value;
    }
}

void HllppSketch::promote_explicit_to_sparse() {
    if (mode_ != Mode::EXPLICIT) {
        return;
    }

    mode_ = Mode::SPARSE;
    sparse_registers_.reserve(std::max<std::size_t>(explicit_hashes_.size(), 32));
    for (std::uint64_t hash : explicit_hashes_) {
        update_sparse(register_index(hash, precision_), rho(hash, precision_));
    }
    explicit_hashes_.clear();
}

void HllppSketch::promote_sparse_to_dense() {
    if (mode_ == Mode::DENSE) {
        return;
    }

    dense_registers_.assign(register_count_, 0);
    for (const auto& entry : sparse_registers_) {
        dense_registers_[entry.first] = std::max(dense_registers_[entry.first], entry.second);
    }
    sparse_registers_.clear();
    sparse_registers_.rehash(0);
    mode_ = Mode::DENSE;
}

void HllppSketch::promote_to_dense() {
    if (mode_ == Mode::EXPLICIT) {
        promote_explicit_to_sparse();
    }
    if (mode_ == Mode::SPARSE) {
        promote_sparse_to_dense();
    }
}

double HllppSketch::raw_estimate() const {
    double denominator = 0.0;

    if (mode_ == Mode::DENSE) {
        for (std::uint8_t value : dense_registers_) {
            denominator += std::ldexp(1.0, -static_cast<int>(value));
        }
    } else {
        const std::size_t zeros = register_count_ - sparse_registers_.size();
        denominator += static_cast<double>(zeros);
        for (const auto& entry : sparse_registers_) {
            denominator += std::ldexp(1.0, -static_cast<int>(entry.second));
        }
    }

    const double m = static_cast<double>(register_count_);
    return alpha_for(register_count_) * m * m / denominator;
}

std::size_t HllppSketch::zero_registers() const {
    if (mode_ == Mode::DENSE) {
        return static_cast<std::size_t>(std::count(dense_registers_.begin(), dense_registers_.end(), 0));
    }
    if (mode_ == Mode::SPARSE) {
        return register_count_ - sparse_registers_.size();
    }
    return register_count_;
}
