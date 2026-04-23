#ifndef _JSONL_PARSE_CONTEXT_HPP_
#define _JSONL_PARSE_CONTEXT_HPP_

#include <string>
#include <vector>
#include <map>
#include <cstdint>

namespace rugo::_jsonl {

// Predicate for filtering records during parsing
struct Predicate {
    // Op codes: 0=EQ, 1=NE, 2=LT, 3=LE, 4=GT, 5=GE
    std::string column;
    uint8_t op;  // Op enum value (0-5)
    std::string value;  // Raw JSON value as string
};

// Parse context: projection, predicates, schema (immutable per Reader session)
struct ParseContext {
    // Projection: which columns to extract (empty = all columns)
    std::vector<std::string> projected_columns;

    // Predicates: filter records during parsing
    std::vector<Predicate> predicates;

    // Schema: expected column types (column_name → type string)
    // Type strings: "int64", "float64", "string", "bool", "null", etc.
    std::map<std::string, std::string> explicit_schema;

    // Schema inference settings
    bool infer_schema = true;
    size_t infer_sample_size = 5;

    // Parsing options
    bool parse_arrays = true;  // Parse arrays into Python lists or leave as spans
    bool parse_objects = true;  // Parse objects into Python dicts or leave as bytes
    bool fail_on_error = true;  // Raise on malformed JSON or warn and continue
};

}  // namespace rugo::_jsonl

#endif  // _JSONL_PARSE_CONTEXT_HPP_
