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

    // The Sparser-style prefilter's segment scan, driven by both drivers: Volnitsky (no
    // sieve) and the SIMD sieve (positions from the input). Needles, the confirm clause and
    // the \u keep are taken from the input so the fuzzer can steer them. A needle under 2
    // bytes is never built (the gate's floor is 4).
    try {
        if (size >= 8) {
            const size_t n1 = 2 + data[0] % 14, n2 = 2 + data[1] % 20;
            if (n1 + n2 + 4 <= size) {
                PrefilterPlan plan;
                plan.clauses.push_back({{std::string(reinterpret_cast<const char*>(data + 4), n1),
                                         std::string(reinterpret_cast<const char*>(data + 4 + n1), n2)},
                                        (data[2] & 1) != 0});
                if (data[2] & 2)
                    plan.clauses.push_back({{std::string(reinterpret_cast<const char*>(data + 4), n2)},
                                            (data[2] & 4) != 0});
                prefilter_plan_segments(data, 0, size, plan);
                plan.sieve = {{data[3] % n1, (data[3] >> 4) % n1}, {0, static_cast<uint32_t>(n2 - 1)}};
                prefilter_plan_segments(data, 0, size, plan);
            }
        }
    } catch (const std::exception&) {
    } catch (...) {
    }

    return 0;
}
