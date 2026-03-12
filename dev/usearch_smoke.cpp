#include <cstdlib>
#include <iostream>
#include <vector>

#include <usearch/index_dense.hpp>

namespace {

using unum::usearch::index_dense_t;
using unum::usearch::index_limits_t;
using unum::usearch::metric_kind_t;
using unum::usearch::metric_punned_t;
using unum::usearch::scalar_kind_t;

int fail(char const* message) {
    std::cerr << "usearch_smoke: " << message << '\n';
    return 1;
}

} // namespace

int main() {
    constexpr std::size_t dimensions = 3;
    constexpr std::size_t wanted = 3;

    metric_punned_t metric(dimensions, metric_kind_t::cos_k, scalar_kind_t::f32_k);
    auto index_result = index_dense_t::make(metric);
    if (!index_result) {
        return fail(index_result.error.release());
    }

    index_dense_t index = std::move(index_result.index);
    index.reserve(index_limits_t {5, 1});

    std::vector<std::pair<std::uint64_t, std::vector<float>>> rows = {
        {10, {1.0f, 0.0f, 0.0f}},
        {20, {0.9f, 0.1f, 0.0f}},
        {30, {0.0f, 1.0f, 0.0f}},
        {40, {0.7f, 0.3f, 0.0f}},
        {50, {0.0f, 0.0f, 1.0f}},
    };

    for (auto const& row : rows) {
        auto add_result = index.add(row.first, row.second.data());
        if (!add_result) {
            return fail(add_result.error.release());
        }
    }

    std::vector<float> query = {1.0f, 0.0f, 0.0f};
    auto search_result = index.search(query.data(), wanted, index_dense_t::any_thread(), false);
    if (!search_result) {
        return fail(search_result.error.release());
    }

    std::vector<index_dense_t::vector_key_t> keys(wanted);
    std::vector<index_dense_t::distance_t> distances(wanted);
    std::size_t found = search_result.dump_to(keys.data(), distances.data(), wanted);

    std::cout << "index_size=" << index.size() << '\n';
    std::cout << "query=[1,0,0] wanted=" << wanted << " found=" << found << '\n';
    for (std::size_t i = 0; i != found; ++i) {
        std::cout << "match[" << i << "] key=" << keys[i] << " distance=" << distances[i] << '\n';
    }

    if (found == 0 || keys[0] != 10) {
        return fail("unexpected nearest neighbor");
    }

    return 0;
}
