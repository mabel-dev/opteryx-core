// Self-test of variant_encode.hpp: JSON text -> Variant binary -> (variant_binary.hpp) text.
//
// Three checks, none of which trusts the encoder to judge itself:
//   1. a table of documents against the exact text the DECODER renders from the encoding;
//   2. every document, shredded by a schema learnt from the set, put back together by a
//      reference reassembly written here straight from VariantShredding.md, and compared
//      with the unshredded value (numbers as their source bytes, object order ignored);
//   3. the schema that was learnt, and where chosen rows landed.
//
// make variant-encode-test (ASan/UBSan).
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "variant_binary.hpp"
#include "variant_encode.hpp"

using namespace rugo::variant;

namespace {

int failures = 0;

void check(bool ok, const std::string& what) {
    if (ok) return;
    ++failures;
    std::fprintf(stderr, "FAIL: %s\n", what.c_str());
}

struct Doc {
    const uint8_t* ptr;
    uint32_t len;
};

struct Rows {
    std::vector<std::string> text;   // one JSON document per row; "\0" (one NUL) marks SQL NULL
    std::vector<Doc> docs;
    std::vector<uint8_t> validity;
    void seal() {
        validity.assign((text.size() + 7) / 8, 0);
        for (size_t r = 0; r < text.size(); ++r) {
            docs.push_back({reinterpret_cast<const uint8_t*>(text[r].data()), static_cast<uint32_t>(text[r].size())});
            if (!(text[r].size() == 1 && text[r][0] == '\0')) validity[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
        }
    }
};

bool present(const ShredLeaf& l, size_t r) { return l.def[r] == l.max_def; }

std::string bytes_of(const ShredLeaf& l, size_t r) {
    return l.bytes.substr(l.offset[r], l.offset[r + 1] - l.offset[r]);
}

// The value of row `r`, unshredded: rendered by the decoder from `metadata` + `value`.
bool unshredded(const Shredder& s, size_t r, std::string& out) {
    const std::vector<ShredLeaf>& l = s.leaves();
    if (l[0].def[r] == 0) return false;
    const std::string md = bytes_of(l[0], r), v = bytes_of(l[1], r);
    render(out, parse_metadata(reinterpret_cast<const uint8_t*>(md.data()), md.size()),
           reinterpret_cast<const uint8_t*>(v.data()), v.size());
    return true;
}

// Reference reassembly of one {value, typed_value} pair (VariantShredding.md, "Reconstructing
// a shredded variant"). `leaf` walks the leaves in schema order; false = the value is missing.
bool reassemble(const std::vector<ShredLeaf>& leaves, const ShredType& type, size_t& leaf, size_t r,
                const Metadata& md, std::string& out) {
    const ShredLeaf& value = leaves[leaf++];
    const std::string v = present(value, r) ? bytes_of(value, r) : std::string();
    const uint8_t* vp = reinterpret_cast<const uint8_t*>(v.data());
    if (type.kind == ShredType::None) {
        if (!present(value, r)) return false;
        render(out, md, vp, v.size());
        return true;
    }
    if (type.kind != ShredType::Object) {
        const ShredLeaf& typed = leaves[leaf++];
        if (present(typed, r)) {
            check(!present(value, r), "a row holds both value and a scalar typed_value");
            switch (type.kind) {
            case ShredType::String: {
                const std::string s = bytes_of(typed, r);
                rugo_text::json_string(out, s.data(), s.size());
                break;
            }
            case ShredType::Bool: out += typed.boolean[r] ? "true" : "false"; break;
            case ShredType::Int64: rugo_text::fmt_int64(out, typed.i64[r]); break;
            default:
                rugo_text::fmt_decimal(out, type.precision <= 18 ? static_cast<__int128>(typed.i64[r]) : typed.i128[r], type.scale);
            }
            return true;
        }
        if (!present(value, r)) return false;
        render(out, md, vp, v.size());
        return true;
    }
    // An object: the typed_value group is present when its leaves' levels reach it.
    const bool typed_present = leaves[leaf].def[r] >= value.max_def;
    std::string object = "{";
    bool first = true;
    for (const ShredField& f : type.fields) {
        std::string member;
        if (reassemble(leaves, f.type, leaf, r, md, member)) {
            check(typed_present, "a field has a value under a null typed_value");
            if (!first) object.push_back(',');
            first = false;
            rugo_text::json_string(object, f.name.data(), f.name.size());
            object.push_back(':');
            object += member;
        }
    }
    if (!typed_present) {
        if (!present(value, r)) return false;
        render(out, md, vp, v.size());
        return true;
    }
    if (present(value, r)) object_members(object, md, vp, v.size(), first);
    object.push_back('}');
    out += object;
    return true;
}

bool same_json(const std::string& a, const std::string& b) {
    yyjson_doc* da = yyjson_read(a.data(), a.size(), YYJSON_READ_NUMBER_AS_RAW);
    yyjson_doc* db = yyjson_read(b.data(), b.size(), YYJSON_READ_NUMBER_AS_RAW);
    const bool same = da != nullptr && db != nullptr && yyjson_equals(yyjson_doc_get_root(da), yyjson_doc_get_root(db));
    yyjson_doc_free(da);
    yyjson_doc_free(db);
    return same;
}

std::string describe(const ShredType& t) {
    switch (t.kind) {
    case ShredType::None: return "-";
    case ShredType::String: return "string";
    case ShredType::Bool: return "bool";
    case ShredType::Int64: return "int64";
    case ShredType::Decimal: return "decimal(" + std::to_string(t.precision) + "," + std::to_string(t.scale) + ")";
    case ShredType::Object: {
        std::string s = "{";
        for (size_t i = 0; i < t.fields.size(); ++i)
            s += (i ? "," : "") + t.fields[i].name + ":" + describe(t.fields[i].type);
        return s + "}";
    }
    }
    return "?";
}

// ── 1. the exact text the decoder renders ───────────────────────────────────

void test_rendering() {
    const std::string long_string(100, 'x');
    std::string wide = "{";
    std::string wide_sorted = "{";
    std::vector<std::string> names;
    for (int i = 0; i < 300; ++i) names.push_back("k" + std::to_string(i));
    for (int i = 299; i >= 0; --i) wide += "\"" + names[static_cast<size_t>(i)] + "\":" + std::to_string(i) + (i ? "," : "}");
    std::sort(names.begin(), names.end());
    for (size_t i = 0; i < names.size(); ++i)
        wide_sorted += "\"" + names[i] + "\":" + names[i].substr(1) + (i + 1 < names.size() ? "," : "}");

    const std::vector<std::pair<std::string, std::string>> cases = {
        {"null", "null"},
        {"true", "true"},
        {"false", "false"},
        {"0", "0"},
        {"-1", "-1"},
        {"127", "127"},
        {"128", "128"},
        {"-32769", "-32769"},
        {"2147483648", "2147483648"},
        {"9223372036854775807", "9223372036854775807"},
        {"-9223372036854775808", "-9223372036854775808"},
        {"9223372036854775808", "9223372036854775808"},                                    // past int64: decimal, scale 0
        {"12345678901234567890123456789012345678", "12345678901234567890123456789012345678"},
        {"1.10", "1.10"},
        {"0.050", "0.050"},
        {"-0.5", "-0.5"},
        {"0.0", "0.0"},
        {"123456789.123456789", "123456789.123456789"},
        {"1234567890123456789.5", "1234567890123456789.5"},
        {"1e2", "100.0"},                                                                  // D3: a double, canonical text
        {"-0", "0.0"},
        {"-0.0", "0.0"},
        {"1.5E-3", "0.0015"},
        {"123456789012345678901234567890123456789", "1.2345678901234568e+38"},             // 39 digits: a double
        {"\"\"", "\"\""},
        {"\"short\"", "\"short\""},
        {"\"" + long_string + "\"", "\"" + long_string + "\""},
        {"\"esc \\\" \\\\ \\n \\u00e9 \\ud83d\\ude00\"", "\"esc \\\" \\\\ \\n \xc3\xa9 \xf0\x9f\x98\x80\""},
        {"[]", "[]"},
        {"{}", "{}"},
        {"[1,\"two\",[3,[4]],{\"b\":1,\"a\":null}]", "[1,\"two\",[3,[4]],{\"a\":null,\"b\":1}]"},
        {" { \"b\" : 1 , \"a\" : { \"z\" : [ ] , \"y\" : true } } ", "{\"a\":{\"y\":true,\"z\":[]},\"b\":1}"},
        {"{\"a\":1,\"b\":2,\"a\":3}", "{\"a\":1,\"b\":2}"},                               // W6: the first of a repeated name
        {"{\"\":0,\"a\":{\"\":1}}", "{\"\":0,\"a\":{\"\":1}}"},
        {wide, wide_sorted},                                                               // a large (> 255 member) object
    };
    Rows rows;
    for (const auto& c : cases) rows.text.push_back(c.first);
    rows.seal();
    Shredder s("data", ShredType{});
    s.encode(rows.docs.data(), nullptr, rows.docs.size());
    check(s.leaves().size() == 2, "an unshredded column is metadata + value");
    for (size_t r = 0; r < cases.size(); ++r) {
        std::string got;
        check(unshredded(s, r, got), "row " + std::to_string(r) + " is null");
        check(got == cases[r].second, cases[r].first.substr(0, 60) + " rendered " + got.substr(0, 80) +
                                          ", expected " + cases[r].second.substr(0, 80));
    }

    // A long array: 4-byte count, 2-byte offsets.
    std::string arr = "[";
    for (int i = 0; i < 1000; ++i) arr += std::to_string(i * 7) + (i < 999 ? "," : "]");
    Rows one;
    one.text = {arr};
    one.seal();
    s.encode(one.docs.data(), nullptr, 1);
    std::string got;
    unshredded(s, 0, got);
    check(got == arr, "a 1000-element array round-trips");
}

void test_refusals() {
    for (const char* bad : {"1e999", "-1e999", "{not json", ""}) {
        Rows rows;
        rows.text = {bad};
        rows.seal();
        Shredder s("data", ShredType{});
        bool threw = false;
        try { s.encode(rows.docs.data(), nullptr, 1); } catch (const std::runtime_error&) { threw = true; }
        check(threw, std::string("'") + bad + "' was not refused");
    }
}

// The JSONL reader refuses a record read as a VARIANT exactly when the encoder would:
// rugo::variant_too_deep (variant_limit.hpp) against encode(), around kVariantMaxDepth,
// nesting arrays and objects around an empty container, a scalar and an object. A string of
// brackets with an escaped quote sits in every record: brackets in strings nest nothing.
void test_depth_limit() {
    const std::string pad = "{\"s\":\"\\\"" + [] { std::string p; for (int i = 0; i < 1500; ++i) p += "[{"; return p; }() + "\",\"a\":";
    int refused = 0, held = 0;
    for (int n = rugo::kVariantMaxDepth - 2; n <= rugo::kVariantMaxDepth + 1; ++n) {
        for (const char* inner : {"", "0", "{}", "{\"k\":[1]}"}) {
            for (bool objects : {false, true}) {
                std::string doc = pad;
                for (int i = 0; i < n; ++i) doc += objects ? "{\"o\":" : "[";
                doc += objects && inner[0] == '\0' ? "null" : inner;
                for (int i = 0; i < n; ++i) doc += objects ? "}" : "]";
                doc += "}";
                Rows rows;
                rows.text = {doc};
                rows.seal();
                Shredder s("data", ShredType{});
                bool threw = false;
                try { s.encode(rows.docs.data(), nullptr, 1); } catch (const std::runtime_error&) { threw = true; }
                const bool deep = rugo::variant_too_deep(reinterpret_cast<const uint8_t*>(doc.data()), doc.size());
                check(deep == threw, std::string(objects ? "objects" : "arrays") + " x" + std::to_string(n) +
                                         " around '" + inner + "': the reader says " + (deep ? "too deep" : "fits") +
                                         ", the encoder " + (threw ? "refused it" : "wrote it"));
                if (threw) ++refused;
                else {
                    ++held;
                    std::string got;
                    unshredded(s, 0, got);   // the renderer holds the same limit
                }
            }
        }
    }
    check(refused > 0 && held > 0, "the cases straddle the limit");
}

// ── 2 + 3. shredding ────────────────────────────────────────────────────────

std::string document(int i) {
    const std::string n = std::to_string(i);
    if (i % 128 == 21) return "{\"kind\":\"commit\",\"seldom\":" + n + "}";   // 1 row in 128
    switch (i % 16) {
    case 0: return "{\"kind\":\"commit\",\"n\":" + n + ",\"price\":" + n + ".50,\"ok\":true,\"commit\":{\"collection\":\"c" +
                   std::to_string(i % 7) + "\",\"rev\":" + n + ",\"record\":{\"text\":\"t" + n + "\",\"langs\":[\"en\",null]}}}";
    case 1: return "{\"kind\":\"commit\",\"n\":1.5,\"price\":2.25,\"ok\":false,\"commit\":{\"collection\":\"x\",\"rev\":-1},\"text\":\"esc \\\" \\\\ \\n\"}";
    case 2: return "{\"kind\":7,\"commit\":\"not an object\",\"text\":\"null\",\"ok\":\"yes\",\"price\":3}";
    case 3: return std::string(1, '\0');   // SQL NULL
    case 4: return "null";
    case 5: return "\"a string\"";
    case 6: return "{}";
    case 7: return "[1,{\"kind\":\"in an array\"}]";
    case 8: return "{\"kind\":null,\"commit\":{\"collection\":null,\"rev\":\"not a number\"}}";
    case 9: return "{\"kind\":\"identity\",\"commit\":{\"collection\":{\"nested\":\"object\"},\"rev\":2.5,\"rare\":1}}";
    case 10: return "{\"kind\":[\"a\",\"list\"],\"commit\":{\"collection\":[\"a\",\"b\"]},\"n\":1e2}";
    case 11: return "{\"kind\":\"keys\",\"commit\":{\"a'b\":1,\"\":2,\"collection\":\"c\"},\"n\":-0}";
    case 12: return "{\"kind\":\"numbers\",\"n\":9007199254740993,\"price\":1.5,\"commit\":{}}";
    case 13: return "{\"commit\":{\"rev\":0,\"collection\":\"\"},\"kind\":\"\",\"kind\":\"second\"}";
    case 14: return "{\"kind\":\"commit\",\"commit\":{\"collection\":\"" + std::string(300, 'x') + "\",\"rev\":" + n + "},\"n\":" + n + "}";
    default: return "{\"kind\":\"account\",\"extra_" + std::to_string(i % 3) + "\":" + n + ",\"commit\":[1,2],\"n\":" + n + ",\"ok\":true}";
    }
}

void test_shredding() {
    Rows rows;
    for (int i = 0; i < 4000; ++i) rows.text.push_back(document(i));
    rows.seal();

    const ShredType learnt = Shredder::infer(rows.docs.data(), rows.validity.data(), rows.docs.size());
    // A field is shredded when at least 1 in 64 of its parent objects has it: `rare` (1 `commit`
    // in 8) and each `extra_*` (1 row in 48) are, `seldom` (1 row in 128) is not. `price` is at
    // scale 2 in most of the rows that hold a decimal; `commit.record.langs` is an array.
    check(describe(learnt) ==
              "{commit:{:int64,a'b:int64,collection:string,rare:int64,record:{text:string},rev:int64},"
              "extra_0:int64,extra_1:int64,extra_2:int64,kind:string,n:int64,ok:bool,price:decimal(18,2),text:string}",
          "learnt " + describe(learnt));

    Shredder shredded("data", learnt), plain("data", ShredType{});
    shredded.encode(rows.docs.data(), rows.validity.data(), rows.docs.size());
    plain.encode(rows.docs.data(), rows.validity.data(), rows.docs.size());
    const std::vector<ShredLeaf>& leaves = shredded.leaves();
    for (const ShredLeaf& l : leaves)
        check(l.def.size() == rows.docs.size(), "leaf " + l.path.back() + " has a row count of its own");

    for (size_t r = 0; r < rows.docs.size(); ++r) {
        std::string want, got;
        const bool held = unshredded(plain, r, want);
        check(held == (leaves[0].def[r] == 1), "row " + std::to_string(r) + ": nullness differs");
        if (!held) {
            for (const ShredLeaf& l : leaves) check(l.def[r] == 0, "a leaf of a NULL row is not at level 0");
            continue;
        }
        const std::string md = bytes_of(leaves[0], r);
        size_t leaf = 1;
        const bool there = reassemble(leaves, learnt, leaf, r,
                                      parse_metadata(reinterpret_cast<const uint8_t*>(md.data()), md.size()), got);
        check(there && leaf == leaves.size(), "row " + std::to_string(r) + " did not reassemble");
        check(same_json(want, got), "row " + std::to_string(r) + ": " + got.substr(0, 120) + " is not " + want.substr(0, 120));
    }

    auto leaf_at = [&](const std::vector<std::string>& path) -> const ShredLeaf& {
        for (const ShredLeaf& l : leaves) if (l.path == path) return l;
        check(false, "no leaf " + path.back());
        return leaves[0];
    };
    const ShredLeaf& kind = leaf_at({"data", "typed_value", "kind", "typed_value"});
    const ShredLeaf& kind_value = leaf_at({"data", "typed_value", "kind", "value"});
    const ShredLeaf& collection = leaf_at({"data", "typed_value", "commit", "typed_value", "collection", "typed_value"});
    const ShredLeaf& residual = leaf_at({"data", "value"});
    const ShredLeaf& price = leaf_at({"data", "typed_value", "price", "typed_value"});
    check(kind.max_def == 3 && collection.max_def == 4 && residual.max_def == 2, "definition levels follow the nesting");
    // Row 0: everything shredded but `commit.record.langs`, the residual of `record` - not of
    // the row, whose own `value` is null; its name is the row's whole dictionary.
    check(present(kind, 0) && bytes_of(kind, 0) == "commit" && !present(kind_value, 0), "row 0 kind is typed");
    check(present(collection, 0) && bytes_of(collection, 0) == "c0", "row 0 commit.collection is typed");
    check(!present(residual, 0), "row 0 has no top-level residual");
    check(bytes_of(leaves[0], 0) == std::string("\x11\x01\x00\x05langs", 9), "row 0's dictionary is `langs`");
    check(present(leaf_at({"data", "typed_value", "commit", "typed_value", "record", "value"}), 0),
          "row 0 commit.record has a residual");
    check(present(price, 0) && price.i64[0] == 50, "row 0 price is the decimal 0.50");
    // Row 2: `kind` is a number, `commit` a string: both fall to their `value`.
    check(!present(kind, 2) && present(kind_value, 2), "row 2 kind is binary");
    check(collection.def[2] == 2, "row 2 commit.typed_value is null");
    // Row 6 `{}`: typed_value present, every field missing, no residual.
    check(kind.def[6] == 2 && kind_value.def[6] == 2 && !present(residual, 6), "row 6 is an object with no fields");
    check(bytes_of(leaves[0], 6) == std::string("\x11\x00\x00", 3), "an empty dictionary is three bytes");
    // Row 7: an array - the whole row is the top-level value.
    check(present(residual, 7) && kind.def[7] == 1, "row 7 is binary at the top");
    // Row 8: a JSON null field is a Variant null in `value`, not a missing field.
    check(present(kind_value, 8) && bytes_of(kind_value, 8) == std::string(1, '\0'), "row 8 kind is the Variant null");
    // Row 13: the first `kind` wins.
    check(present(kind, 13) && bytes_of(kind, 13).empty(), "row 13 keeps the first of a repeated name");
    // Row 12: 1.5 is not at the column's scale, so it stays binary.
    check(!present(price, 12), "row 12 price is at another scale");
}

void test_inference_limits() {
    // A scalar column, a column with no dominant shape, and an object too wide to shred whole.
    Rows strings, mixed, wide;
    for (int i = 0; i < 100; ++i) {
        strings.text.push_back("\"s" + std::to_string(i) + "\"");
        mixed.text.push_back(i % 3 == 0 ? "1" : i % 3 == 1 ? "\"a\"" : "[1]");
        std::string doc = "{";
        for (int f = 0; f < 200; ++f) doc += "\"f" + std::to_string(f) + "\":" + std::to_string(f) + (f < 199 ? "," : "}");
        wide.text.push_back(doc);
    }
    strings.seal(); mixed.seal(); wide.seal();
    check(describe(Shredder::infer(strings.docs.data(), nullptr, 100)) == "string", "a column of strings is a typed string");
    check(describe(Shredder::infer(mixed.docs.data(), nullptr, 100)) == "-", "no dominant type: unshredded");
    const ShredType capped = Shredder::infer(wide.docs.data(), nullptr, 100);
    check(capped.fields.size() == static_cast<size_t>(kShredFields), "the field cap holds");
    Shredder s("data", capped), plain("data", ShredType{});
    s.encode(wide.docs.data(), nullptr, 100);
    plain.encode(wide.docs.data(), nullptr, 100);
    std::string want, got;
    unshredded(plain, 0, want);
    const std::string md = bytes_of(s.leaves()[0], 0);
    size_t leaf = 1;
    reassemble(s.leaves(), capped, leaf, 0, parse_metadata(reinterpret_cast<const uint8_t*>(md.data()), md.size()), got);
    check(same_json(want, got), "the fields past the cap are in the residual");
}

// ── 4. row ranges ───────────────────────────────────────────────────────────

// Encoding rows in ranges gives, range by range, exactly the entries of encoding them all: what
// the writer's parallel encode joins back together (docs/parquet_variant_parallel_encode_design.md).
void test_ranges() {
    Rows rows;
    for (int i = 0; i < 4001; ++i) rows.text.push_back(document(i));
    rows.seal();
    const ShredType learnt = Shredder::infer(rows.docs.data(), rows.validity.data(), rows.docs.size());
    Shredder whole("data", learnt);
    whole.encode(rows.docs.data(), rows.validity.data(), rows.docs.size());
    // Bounds off a multiple of 8 (the validity bitmap's bytes) and a range of one row.
    const std::vector<size_t> bounds = {0, 3, 1001, 1002, 2731, 4001};
    std::vector<Shredder> parts;
    for (size_t b = 0; b + 1 < bounds.size(); ++b) {
        parts.emplace_back("data", learnt);
        parts.back().encode(rows.docs.data(), rows.validity.data(), bounds[b], bounds[b + 1]);
    }
    const std::vector<ShredLeaf>& all = whole.leaves();
    for (size_t k = 0; k < all.size(); ++k) {
        const std::string name = "leaf " + std::to_string(k) + " (" + all[k].path.back() + ")";
        std::vector<uint8_t> def, boolean;
        std::vector<int64_t> i64;
        std::vector<__int128> i128;
        std::vector<std::string> payload;
        for (const Shredder& p : parts) {
            const ShredLeaf& l = p.leaves()[k];
            def.insert(def.end(), l.def.begin(), l.def.end());
            boolean.insert(boolean.end(), l.boolean.begin(), l.boolean.end());
            i64.insert(i64.end(), l.i64.begin(), l.i64.end());
            i128.insert(i128.end(), l.i128.begin(), l.i128.end());
            if (!l.offset.empty())
                for (size_t r = 0; r + 1 < l.offset.size(); ++r) payload.push_back(bytes_of(l, r));
        }
        check(def == all[k].def, name + ": definition levels differ");
        check(boolean == all[k].boolean, name + ": booleans differ");
        check(i64 == all[k].i64, name + ": int64s differ");
        check(i128 == all[k].i128, name + ": int128s differ");
        std::vector<std::string> want;
        if (!all[k].offset.empty())
            for (size_t r = 0; r + 1 < all[k].offset.size(); ++r) want.push_back(bytes_of(all[k], r));
        check(payload == want, name + ": bytes differ");
    }

    // An invalid document is reported by its row among all the rows, not within its range.
    Rows bad;
    for (int i = 0; i < 20; ++i) bad.text.push_back(i == 13 ? "{not json" : "{\"a\":1}");
    bad.seal();
    Shredder tail("data", ShredType{});
    std::string message;
    try { tail.encode(bad.docs.data(), bad.validity.data(), 10, 20); } catch (const std::runtime_error& e) { message = e.what(); }
    check(message.find("at row 13:") != std::string::npos, "invalid JSON reported as: " + message);
}

}  // namespace

int main() {
    test_rendering();
    test_refusals();
    test_depth_limit();
    test_shredding();
    test_inference_limits();
    test_ranges();
    if (failures != 0) {
        std::fprintf(stderr, "variant encode: %d check(s) failed\n", failures);
        return 1;
    }
    std::puts("variant encode: all checks passed");
    return 0;
}
