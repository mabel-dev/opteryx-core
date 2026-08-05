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

    // Cached numeric/boolean parse of `value`, populated once by
    // value_parser::prepare_predicate before the per-record evaluation loop starts —
    // evaluate_predicate() must not re-parse `value` itself, or a selective predicate
    // re-parses the same constant on every row it's checked against.
    int64_t pred_int = 0;
    double  pred_float = 0.0;
    bool    pred_parsed_int = false;
    bool    pred_parsed_float = false;
    bool    pred_bool = false;
    bool    pred_parsed_bool = false;
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
    // parse_arrays: materialize a uniform-scalar-element JSON array column as a
    // DRAKEN_ARRAY vector (column_builder.cpp's parse_array_column); nested containers or
    // a heterogeneous mix of element kinds fall back to raw JSON text (DRAKEN_VARCHAR),
    // same as parse_arrays=false. When false, arrays are always raw JSON text.
    bool parse_arrays = true;
    // parse_objects: tag a JSON-object column as DRAKEN_VARIANT instead of DRAKEN_VARCHAR
    // (identical raw-JSON-text storage either way — only the type tag differs).
    bool parse_objects = true;
    bool fail_on_error = true;  // Raise on malformed JSON or warn and continue
};

}  // namespace rugo::_jsonl

#endif  // _JSONL_PARSE_CONTEXT_HPP_
