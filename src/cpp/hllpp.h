#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <vector>

class HllppSketch {
public:
    enum class Mode : std::uint8_t {
        EXPLICIT = 0,
        SPARSE = 1,
        DENSE = 2,
    };

    static constexpr int kMinPrecision = 4;
    static constexpr int kMaxPrecision = 18;

    explicit HllppSketch(
        int precision = 14,
        std::size_t explicit_threshold = 0,
        std::size_t sparse_threshold = 0
    );

    int precision() const noexcept;
    std::size_t register_count() const noexcept;
    Mode mode() const noexcept;
    std::string_view mode_name() const noexcept;

    void reset();
    void add_hash(std::uint64_t hash);
    void add_hashes(const std::uint64_t* hashes, std::size_t count);
    bool merge(const HllppSketch& other);

    std::uint64_t estimate() const;
    std::size_t explicit_size() const noexcept;
    std::size_t sparse_size() const noexcept;

private:
    int precision_;
    std::size_t register_count_;
    std::size_t explicit_threshold_;
    std::size_t sparse_threshold_;
    Mode mode_;
    std::vector<std::uint64_t> explicit_hashes_;
    std::unordered_map<std::uint32_t, std::uint8_t> sparse_registers_;
    std::vector<std::uint8_t> dense_registers_;

    static std::size_t default_explicit_threshold(std::size_t register_count) noexcept;
    static std::size_t default_sparse_threshold(std::size_t register_count) noexcept;
    static double alpha_for(std::size_t register_count) noexcept;
    static std::uint8_t rho(std::uint64_t hash, int precision) noexcept;
    static std::uint32_t register_index(std::uint64_t hash, int precision) noexcept;

    bool contains_explicit_hash(std::uint64_t hash) const noexcept;
    void add_register(std::uint32_t index, std::uint8_t value);
    void update_sparse(std::uint32_t index, std::uint8_t value);
    void update_dense(std::uint32_t index, std::uint8_t value);
    void promote_explicit_to_sparse();
    void promote_sparse_to_dense();
    void promote_to_dense();
    double raw_estimate() const;
    std::size_t zero_registers() const;
};
