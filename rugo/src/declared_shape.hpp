#pragma once
// rugo/src/declared_shape.hpp - the shape a TABLE declares for one nested column, which each data file
// is verified against before its names are rendered as JSON (docs/UUID_AND_NESTED_COLUMNS_DESIGN.md
// §3.3, option (b)). Shared by the Parquet and ORC readers so they cannot disagree about it.

#include <cstdint>
#include <string>

namespace rugo {

// One node of a declared shape, pre-order (the order NestedField.flatten() produces).
// kind: 0 scalar, 1 struct, 2 list, 3 map. `name` is compared for a struct member only.
struct DeclaredNode {
    int kind = 0;
    int64_t field_id = 0;
    std::string name;
    int n_children = 0;
};

}  // namespace rugo
