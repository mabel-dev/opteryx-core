// Fuzz rugo's JSONL reader: arbitrary bytes in, no crash out.
//
// The JSONL path is a hand-written SIMD structural index (in-string structure
// masked out) feeding a document mapper that bounds container values by bracket
// depth over that index. Both work in offsets into the caller's buffer rather than on a parsed
// tree, so an unterminated string, an unbalanced bracket, or a record that ends
// mid-escape is a bounds question rather than a parse question — which is
// exactly the class of bug a sanitizer catches and a unit test does not.
//
// The chain here is the real one: windowed structural index -> document map ->
// key discovery, unprojected and through a nested projection. It stops short of the column builders, which return PyObject* and
// so cannot run outside an interpreter.
//
// The oracle is the sanitizer, not the return value. Malformed JSONL producing
// no records, or a record flagged malformed, is the reader working.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <vector>

#include "jsonl/core/field_span.hpp"
#include "jsonl/core/interpreter.hpp"
#include "jsonl/core/jsonl_reader.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    using namespace rugo::_jsonl;

    try {
        RecordSet records = build_map(data, size);
        sample_record_keys(records, data, 5);
    } catch (const std::exception&) {
    } catch (...) {
    }

    // The projected walk: a top-level key plus a nested sub-key, so the depth-only
    // container bound and the nested-key walk inside it both run.
    try {
        ParseContext ctx;
        ctx.fail_on_error = false;
        ctx.projected_columns = {"a", "a->>'b'"};
        interpret_jsonl(data, size, ctx, ctx.projected_columns);
    } catch (const std::exception&) {
    } catch (...) {
    }

    // The Sparser-style prefilter, which walks records and runs a Volnitsky
    // substring search over them. Needle taken from the input so the fuzzer can
    // steer it; an empty needle is itself a case worth reaching.
    try {
        const size_t needle_len = size >= 4 ? (data[0] % 8) : 0;
        if (needle_len <= size) {
            volnitsky_prefilter(data, size, data, needle_len);
        }
    } catch (const std::exception&) {
    } catch (...) {
    }

    return 0;
}
