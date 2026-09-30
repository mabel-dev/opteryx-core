// See nested_json.hpp for the contract and the rendering rulings.
#include "nested_json.hpp"

#include <cstring>
#include <stdexcept>

#include "interop/value_format.hpp"   // fmt_*, json_string, LogicalDesc units
#include "ops/float_ops.h"            // fp_canon: the same -0.0 / NaN canon as the scan
#include "base64/_base64.h"           // mabel bintob64 / b64_encoded_size

namespace rugo {
namespace nested {

using rugo_text::U_MS;
using rugo_text::U_NS;
using rugo_text::U_S;
using rugo_text::U_US;

namespace {

enum class NK : uint8_t { Scalar, Struct, List, Map, Repeated };
enum class SK : uint8_t { Bool, Int, UInt, F32, F64, String, Binary, Date, Timestamp, Time, Decimal };

struct Node {
    NK kind = NK::Scalar;
    std::string key;           // pre-rendered `"name":` for a struct member
    int32_t def = 0;           // the node is present iff the entry's def level >= def
    int32_t rep_def = 0;       // List/Map/Repeated: entries exist iff def >= rep_def
    int32_t rep_level = 0;     // List/Map/Repeated: repetition level of the repeated node
    int32_t first_leaf = 0;    // first leaf under this node (schema DFS order)
    int32_t n_leaves = 0;
    std::vector<Node> children;
    // Scalar
    SK sk = SK::Int;
    int unit = U_US;
    int scale = 0;
    bool is_unsigned = false;
};

[[noreturn]] void fail(const std::string& column, const std::string& what) {
    throw std::runtime_error("STRUCT/MAP column '" + column + "': " + what);
}

std::string member_key(const std::string& name) {
    std::string k;
    rugo_text::json_string(k, name.data(), name.size());
    k.push_back(':');
    return k;
}

int parse_unit(const std::string& lt, size_t open) {
    // "timestamp[ms]" / "time[us,UTC]"
    const size_t close = lt.find_first_of(",]", open + 1);
    const std::string u = lt.substr(open + 1, close - open - 1);
    if (u == "s") return U_S;
    if (u == "ms") return U_MS;
    if (u == "us") return U_US;
    if (u == "ns") return U_NS;
    return -1;
}

}  // namespace

struct Plan {
    std::string name;
    Node root;
    int32_t n_leaves = 0;
};

namespace {

struct Builder {
    const std::string& column;
    int32_t leaf_ctr = 0;

    void scalar(const SchemaElement& e, Node& n) {
        const std::string& lt = e.logical_type;
        const std::string& pt = e.physical_type;
        n.kind = NK::Scalar;
        if (lt.rfind("decimal", 0) == 0) {
            n.sk = SK::Decimal;
            const size_t c = lt.find(',');
            const size_t r = lt.find(')');
            if (c == std::string::npos || r == std::string::npos || r < c)
                fail(column, "leaf '" + e.full_name + "' has a malformed decimal annotation '" + lt + "'");
            n.scale = std::atoi(lt.c_str() + c + 1);
            if (pt != "int32" && pt != "int64" && pt != "fixed_len_byte_array" && pt != "byte_array")
                fail(column, "decimal leaf '" + e.full_name + "' has unsupported physical type '" + pt + "'");
            return;
        }
        if (lt == "date32[day]") { n.sk = SK::Date; return; }
        if (lt.rfind("timestamp[", 0) == 0) {
            n.sk = SK::Timestamp;
            n.unit = parse_unit(lt, lt.find('['));
            if (n.unit < 0) fail(column, "leaf '" + e.full_name + "' has unsupported timestamp unit in '" + lt + "'");
            return;
        }
        if (pt == "int96") { n.sk = SK::Timestamp; n.unit = U_NS; return; }
        if (lt.rfind("time[", 0) == 0) {
            n.sk = SK::Time;
            n.unit = parse_unit(lt, lt.find('['));
            if (n.unit < 0) fail(column, "leaf '" + e.full_name + "' has unsupported time unit in '" + lt + "'");
            return;
        }
        if (pt == "boolean") { n.sk = SK::Bool; return; }
        if (pt == "float32") { n.sk = SK::F32; return; }
        if (pt == "float64") { n.sk = SK::F64; return; }
        if (pt == "byte_array" || pt == "fixed_len_byte_array") {
            if (lt == "varchar" || lt == "enum" || lt == "json") { n.sk = SK::String; return; }
            if (lt.empty() || lt == "binary" || lt == "bson" ||
                lt.rfind("fixed_len_byte_array", 0) == 0) {
                n.sk = SK::Binary;
                return;
            }
            fail(column, "leaf '" + e.full_name + "' has unsupported byte-array annotation '" + lt + "'");
        }
        if (pt == "int32" || pt == "int64") {
            if (lt.rfind("uint", 0) == 0) { n.sk = SK::UInt; n.is_unsigned = true; return; }
            n.sk = SK::Int;
            return;
        }
        fail(column, "leaf '" + e.full_name + "' has unsupported type '" + pt +
                     (lt.empty() ? "" : "' / '" + lt) + "'");
    }

    // `acc_def` / `acc_rep`: levels contributed by everything ABOVE `e`.
    Node build(const SchemaElement& e, int32_t acc_def, int32_t acc_rep) {
        if (e.repetition_type == 2) {
            // A bare REPEATED field (legacy list). No null state of its own: absent
            // means an empty list.
            Node r;
            r.kind = NK::Repeated;
            r.key = member_key(e.name);
            r.def = acc_def;
            r.rep_def = acc_def + 1;
            r.rep_level = acc_rep + 1;
            r.first_leaf = leaf_ctr;
            r.children.push_back(body(e, acc_def + 1, acc_rep + 1, true));
            r.n_leaves = leaf_ctr - r.first_leaf;
            return r;
        }
        return body(e, acc_def, acc_rep, false);
    }

    // `counted`: the level of `e` itself is already in acc_def (a repeated node's
    // element, or a container's inner element whose repeated parent was counted).
    Node body(const SchemaElement& e, int32_t acc_def, int32_t acc_rep, bool counted) {
        Node n;
        n.key = member_key(e.name);
        n.def = counted ? acc_def : acc_def + (e.repetition_type == 1 ? 1 : 0);
        n.first_leaf = leaf_ctr;

        if (e.children.empty()) {
            scalar(e, n);
            ++leaf_ctr;
            n.n_leaves = 1;
            return n;
        }

        if (e.logical_type == "array") {
            n.kind = NK::List;
            if (e.children.size() != 1 || e.children[0].repetition_type != 2)
                fail(column, "LIST '" + e.full_name + "' is not the standard "
                             "group { repeated <element> } shape");
            const SchemaElement& rp = e.children[0];
            n.rep_def = n.def + 1;
            n.rep_level = acc_rep + 1;
            if (rp.children.empty()) {
                Node el;                       // legacy 2-level list: the repeated leaf IS the element
                el.key = member_key(rp.name);
                el.def = n.rep_def;
                el.first_leaf = leaf_ctr;
                scalar(rp, el);
                ++leaf_ctr;
                el.n_leaves = 1;
                n.children.push_back(std::move(el));
            } else if (rp.children.size() == 1) {
                n.children.push_back(build(rp.children[0], n.rep_def, n.rep_level));
            } else {                           // legacy: the repeated group IS a struct element
                Node el;
                el.kind = NK::Struct;
                el.key = member_key(rp.name);
                el.def = n.rep_def;
                el.first_leaf = leaf_ctr;
                for (const SchemaElement& c : rp.children)
                    el.children.push_back(build(c, n.rep_def, n.rep_level));
                el.n_leaves = leaf_ctr - el.first_leaf;
                n.children.push_back(std::move(el));
            }
        } else if (e.logical_type == "map") {
            n.kind = NK::Map;
            if (e.children.size() != 1 || e.children[0].repetition_type != 2 ||
                e.children[0].children.empty() || e.children[0].children.size() > 2)
                fail(column, "MAP '" + e.full_name + "' is not the standard "
                             "group { repeated group key_value { key; value } } shape");
            const SchemaElement& kv = e.children[0];
            n.rep_def = n.def + 1;
            n.rep_level = acc_rep + 1;
            Node key = build(kv.children[0], n.rep_def, n.rep_level);
            if (key.kind != NK::Scalar || key.sk != SK::String)
                fail(column, "MAP '" + e.full_name + "' has a non-string key ('" +
                             kv.children[0].logical_type + "' / '" + kv.children[0].physical_type +
                             "'); JSON object keys must be strings");
            n.children.push_back(std::move(key));
            if (kv.children.size() == 2)
                n.children.push_back(build(kv.children[1], n.rep_def, n.rep_level));
        } else {
            n.kind = NK::Struct;
            for (const SchemaElement& c : e.children)
                n.children.push_back(build(c, n.def, acc_rep));
        }
        n.n_leaves = leaf_ctr - n.first_leaf;
        return n;
    }
};

}  // namespace

std::shared_ptr<const Plan> build_plan(const FileStats& fs, const std::string& name,
                                       std::string& err) {
    const SchemaElement* top = nullptr;
    for (const SchemaElement& e : fs.schema)
        if (e.name == name) { top = &e; break; }
    if (top == nullptr) { err = "no top-level field named '" + name + "' in the footer schema"; return nullptr; }
    if (top->children.empty()) { err = "'" + name + "' is a plain column, not a STRUCT/MAP"; return nullptr; }
    if (top->repetition_type == 2) {
        err = "'" + name + "' is a repeated group (a LIST of STRUCT/MAP), which this reader does not render";
        return nullptr;
    }
    if (top->logical_type == "array") {
        err = "'" + name + "' is a LIST; a LIST of STRUCT/MAP is not rendered as JSON text";
        return nullptr;
    }
    try {
        Builder b{name};
        auto plan = std::make_shared<Plan>();
        plan->name = name;
        plan->root = b.body(*top, 0, 0, false);
        plan->n_leaves = b.leaf_ctr;
        return plan;
    } catch (const std::runtime_error& ex) {
        err = ex.what();
        return nullptr;
    }
}

size_t leaf_count(const Plan& plan) { return static_cast<size_t>(plan.n_leaves); }

// ── assembly ────────────────────────────────────────────────────────────────

namespace {

struct Leaf {
    const DecodedColumn* d = nullptr;
    size_t pos = 0;          // entry index (one per level triple, present or not)
    size_t n = 0;            // entries in this leaf
    size_t vi = 0;           // next value in the plain / dict_indices streams
    size_t run = 0;          // RLE cursor (run index)
    int64_t run_left = 0;    // RLE cursor (values left in the current run)
};

inline int32_t def_at(const Leaf& l) {
    return l.d->def_levels.empty() ? l.d->max_def_level : l.d->def_levels[l.pos];
}
inline int32_t rep_at(const Leaf& l) {
    return l.d->rep_levels.empty() ? 0 : l.d->rep_levels[l.pos];
}

inline size_t rle_next(const std::vector<int32_t>& runs, size_t& run, int64_t& left) {
    while (left == 0) {
        if (run >= runs.size()) throw std::runtime_error("RLE run table exhausted");
        left = runs[run];
        if (left == 0) ++run;
    }
    const size_t idx = run;
    if (--left == 0) ++run;
    return idx;
}

inline int32_t packed_code(const uint8_t* arr, size_t i, uint8_t width) {
    if (width == 1) return static_cast<int32_t>(arr[i]);
    if (width == 2) return static_cast<int32_t>(arr[2 * i] | (static_cast<uint32_t>(arr[2 * i + 1]) << 8));
    return static_cast<int32_t>(arr[4 * i] | (static_cast<uint32_t>(arr[4 * i + 1]) << 8) |
                                (static_cast<uint32_t>(arr[4 * i + 2]) << 16) |
                                (static_cast<uint32_t>(arr[4 * i + 3]) << 24));
}

struct Assembler {
    const Plan& plan;
    std::vector<Leaf> L;
    std::string o;    // the growing blob of JSON documents

    [[noreturn]] void bad(const std::string& what) const { fail(plan.name, what); }

    void skip1(const Node& n) {
        for (int32_t k = n.first_leaf; k < n.first_leaf + n.n_leaves; ++k) ++L[k].pos;
    }

    // ── leaf value readers: ONE value from the leaf's present-value stream ──────

    void str_value(Leaf& l, const uint8_t*& p, uint32_t& len) {
        const DecodedColumn& d = *l.d;
        if (!d.rle_str_lens.empty() && !d.rle_run_lengths.empty()) {
            const size_t r = rle_next(d.rle_run_lengths, l.run, l.run_left);
            p = d.rle_str_arena.data() + d.rle_str_offsets[r];
            len = static_cast<uint32_t>(d.rle_str_lens[r]);
        } else if (!d.dict_codes_array.empty()) {
            const int32_t c = packed_code(d.dict_codes_array.data(), l.pos, d.code_width);
            p = d.string_dict_arena.data() + d.string_dict_offsets[c];
            len = static_cast<uint32_t>(d.string_dict_lens[c]);
        } else if (!d.dict_indices.empty()) {
            const int32_t c = d.dict_indices[l.vi++];
            if (!d.string_dict_arena.empty()) {
                p = d.string_dict_arena.data() + d.string_dict_offsets[c];
                len = static_cast<uint32_t>(d.string_dict_lens[c]);
            } else {
                p = d.string_arena.data() + d.string_offsets[c];
                len = static_cast<uint32_t>(d.string_lens[c]);
            }
        } else {
            if (l.vi >= d.string_lens.size()) bad("string value stream exhausted");
            p = d.string_arena.data() + d.string_offsets[l.vi];
            len = static_cast<uint32_t>(d.string_lens[l.vi]);
            ++l.vi;
        }
    }

    // Integer-family value as the raw 64-bit pattern (sign-extended from int32
    // physical, exactly as stored from int64).
    int64_t int_value(Leaf& l) {
        const DecodedColumn& d = *l.d;
        if (d.type == "int64") {
            if (!d.int64_values.empty()) return d.int64_values[l.vi++];
            if (!d.rle_int64_values.empty() && !d.rle_run_lengths.empty())
                return d.rle_int64_values[rle_next(d.rle_run_lengths, l.run, l.run_left)];
            if (!d.dict_codes_array.empty())
                return d.dict_int64_values[packed_code(d.dict_codes_array.data(), l.pos, d.code_width)];
            if (!d.dict_indices.empty()) return d.dict_int64_values[d.dict_indices[l.vi++]];
        } else if (d.type == "int32") {
            if (!d.int32_values.empty()) return static_cast<int64_t>(d.int32_values[l.vi++]);
            if (!d.rle_int64_values.empty() && !d.rle_run_lengths.empty())
                return d.rle_int64_values[rle_next(d.rle_run_lengths, l.run, l.run_left)];
            if (!d.dict_codes_array.empty())
                return static_cast<int64_t>(
                    d.dict_int32_values[packed_code(d.dict_codes_array.data(), l.pos, d.code_width)]);
            if (!d.dict_indices.empty())
                return static_cast<int64_t>(d.dict_int32_values[d.dict_indices[l.vi++]]);
        }
        bad("integer value stream exhausted or of unexpected physical type '" + d.type + "'");
    }

    __int128 decimal_value(Leaf& l) {
        const DecodedColumn& d = *l.d;
        if (d.type == "int128") {
            if (!d.int128_values.empty()) return d.int128_values[l.vi++];
            if (!d.dict_codes_array.empty())
                return d.dict_int128_values[packed_code(d.dict_codes_array.data(), l.pos, d.code_width)];
            if (!d.dict_indices.empty()) return d.dict_int128_values[d.dict_indices[l.vi++]];
            bad("decimal value stream exhausted");
        }
        return static_cast<__int128>(int_value(l));
    }

    double float_value(Leaf& l, bool f32) {
        const DecodedColumn& d = *l.d;
        if (f32) {
            if (!d.float32_values.empty()) return d.float32_values[l.vi++];
            if (!d.rle_float64_values.empty() && !d.rle_run_lengths.empty())
                return d.rle_float64_values[rle_next(d.rle_run_lengths, l.run, l.run_left)];
            if (!d.dict_codes_array.empty())
                return d.dict_float32_values[packed_code(d.dict_codes_array.data(), l.pos, d.code_width)];
            if (!d.dict_indices.empty()) return d.dict_float32_values[d.dict_indices[l.vi++]];
        } else {
            if (!d.float64_values.empty()) return d.float64_values[l.vi++];
            if (!d.rle_float64_values.empty() && !d.rle_run_lengths.empty())
                return d.rle_float64_values[rle_next(d.rle_run_lengths, l.run, l.run_left)];
            if (!d.dict_codes_array.empty())
                return d.dict_float64_values[packed_code(d.dict_codes_array.data(), l.pos, d.code_width)];
            if (!d.dict_indices.empty()) return d.dict_float64_values[d.dict_indices[l.vi++]];
        }
        bad("float value stream exhausted");
    }

    // Render the scalar at the leaf's current entry (known present) and consume its value.
    void emit_scalar(const Node& n, Leaf& l) {
        switch (n.sk) {
        case SK::Int:
            rugo_text::fmt_int64(o, int_value(l));
            return;
        case SK::UInt: {
            const int64_t v = int_value(l);
            // physical int32 holds a 32-bit pattern; int64 holds the 64-bit one
            rugo_text::fmt_uint64(o, l.d->type == "int32"
                                         ? static_cast<uint64_t>(static_cast<uint32_t>(v))
                                         : static_cast<uint64_t>(v));
            return;
        }
        case SK::F32: {
            const float v = draken::ops::fp_canon(static_cast<float>(float_value(l, true)));
            if (rugo_text::double_is_nan_or_inf(static_cast<double>(v))) o.append("null");
            else rugo_text::fmt_float(o, v);
            return;
        }
        case SK::F64: {
            const double v = draken::ops::fp_canon(float_value(l, false));
            if (rugo_text::double_is_nan_or_inf(v)) o.append("null");
            else rugo_text::fmt_double(o, v);
            return;
        }
        case SK::Bool: {
            if (l.vi >= l.d->boolean_values.size()) bad("boolean value stream exhausted");
            o.append(l.d->boolean_values[l.vi++] & 1 ? "true" : "false");
            return;
        }
        case SK::String: {
            const uint8_t* p; uint32_t len;
            str_value(l, p, len);
            rugo_text::json_string(o, reinterpret_cast<const char*>(p), len);
            return;
        }
        case SK::Binary: {
            const uint8_t* p; uint32_t len;
            str_value(l, p, len);
            o.push_back('"');
            const size_t at = o.size();
            o.resize(at + b64_encoded_size(len));            // includes bintob64's trailing NUL
            bintob64(&o[at], p, len);
            o.resize(at + b64_encoded_size(len) - 1);
            o.push_back('"');
            return;
        }
        case SK::Date:
            rugo_text::fmt_date_quoted(o, static_cast<int32_t>(int_value(l)));
            return;
        case SK::Timestamp:
            rugo_text::fmt_timestamp_quoted(o, int_value(l), n.unit);
            return;
        case SK::Time:
            rugo_text::fmt_time_quoted(o, int_value(l), n.unit);
            return;
        case SK::Decimal:
            rugo_text::fmt_decimal(o, decimal_value(l), n.scale);
            return;
        }
    }

    bool more(const Node& n) const {
        const Leaf& f = L[n.first_leaf];
        return f.pos < f.n && rep_at(f) == n.rep_level;
    }

    void render(const Node& n) {
        switch (n.kind) {
        case NK::Scalar: {
            Leaf& l = L[n.first_leaf];
            if (l.pos >= l.n) bad("levels exhausted before the row ended");
            if (def_at(l) < n.def) { o.append("null"); ++l.pos; return; }
            emit_scalar(n, l);
            ++l.pos;
            return;
        }
        case NK::Struct: {
            const Leaf& f = L[n.first_leaf];
            if (f.pos >= f.n) bad("levels exhausted before the row ended");
            if (def_at(f) < n.def) { o.append("null"); skip1(n); return; }
            o.push_back('{');
            for (size_t i = 0; i < n.children.size(); ++i) {
                if (i) o.push_back(',');
                o.append(n.children[i].key);
                render(n.children[i]);
            }
            o.push_back('}');
            return;
        }
        case NK::List: {
            const Leaf& f = L[n.first_leaf];
            if (f.pos >= f.n) bad("levels exhausted before the row ended");
            const int32_t d = def_at(f);
            if (d < n.def) { o.append("null"); skip1(n); return; }
            if (d < n.rep_def) { o.append("[]"); skip1(n); return; }
            o.push_back('[');
            for (;;) {
                render(n.children[0]);
                if (!more(n)) break;
                o.push_back(',');
            }
            o.push_back(']');
            return;
        }
        case NK::Repeated: {
            const Leaf& f = L[n.first_leaf];
            if (f.pos >= f.n) bad("levels exhausted before the row ended");
            if (def_at(f) < n.rep_def) { o.append("[]"); skip1(n); return; }
            o.push_back('[');
            for (;;) {
                render(n.children[0]);
                if (!more(n)) break;
                o.push_back(',');
            }
            o.push_back(']');
            return;
        }
        case NK::Map: {
            const Leaf& f = L[n.first_leaf];
            if (f.pos >= f.n) bad("levels exhausted before the row ended");
            const int32_t d = def_at(f);
            if (d < n.def) { o.append("null"); skip1(n); return; }
            if (d < n.rep_def) { o.append("{}"); skip1(n); return; }
            o.push_back('{');
            const Node& key = n.children[0];
            for (;;) {
                Leaf& kl = L[key.first_leaf];
                if (def_at(kl) < key.def) bad("a MAP entry has a NULL key");
                emit_scalar(key, kl);       // a string, rendered as a JSON string
                ++kl.pos;
                o.push_back(':');
                if (n.children.size() == 2) render(n.children[1]);
                else o.append("null");
                if (!more(n)) break;
                o.push_back(',');
            }
            o.push_back('}');
            return;
        }
        }
    }
};

}  // namespace

void assemble(const Plan& plan, const std::vector<DecodedColumn>& leaves,
              const uint8_t* row_mask, DecodedColumn& out) {
    if (leaves.size() != static_cast<size_t>(plan.n_leaves))
        fail(plan.name, "expected " + std::to_string(plan.n_leaves) + " leaf chunks, got " +
                        std::to_string(leaves.size()));

    Assembler a{plan, {}, {}};
    a.L.resize(leaves.size());
    size_t rows = 0;
    for (size_t k = 0; k < leaves.size(); ++k) {
        const DecodedColumn& d = leaves[k];
        Leaf& l = a.L[k];
        l.d = &d;
        l.n = !d.def_levels.empty() ? d.def_levels.size()
                                    : static_cast<size_t>(d.num_rows);
        if (!d.def_levels.empty() && !d.rep_levels.empty() && d.def_levels.size() != d.rep_levels.size())
            fail(plan.name, "leaf " + std::to_string(k) + " has mismatched level counts");
        if (k == 0) {
            if (d.rep_levels.empty()) rows = l.n;
            else for (int32_t r : d.rep_levels) if (r == 0) ++rows;
        }
    }

    std::vector<uint32_t> offs, lens;
    std::vector<uint8_t> valid((rows + 7) / 8, 0);
    bool any_null = false;
    size_t n_out = 0;
    a.o.reserve(rows * 32);

    const Node& root = plan.root;
    for (size_t r = 0; r < rows; ++r) {
        const bool selected = row_mask == nullptr || row_mask[r] != 0;
        const Leaf& f = a.L[root.first_leaf];
        if (f.pos >= f.n) fail(plan.name, "levels exhausted before row " + std::to_string(r));
        if (def_at(f) < root.def) {                       // the group itself is NULL
            a.skip1(root);
            if (selected) {
                offs.push_back(0);
                lens.push_back(0);
                ++n_out;
                any_null = true;
            }
            continue;
        }
        const size_t mark = a.o.size();
        a.render(root);
        if (!selected) { a.o.resize(mark); continue; }
        if (a.o.size() > UINT32_MAX) fail(plan.name, "rendered JSON exceeds 4 GiB for one row group");
        offs.push_back(static_cast<uint32_t>(mark));
        lens.push_back(static_cast<uint32_t>(a.o.size() - mark));
        valid[n_out >> 3] |= static_cast<uint8_t>(1u << (n_out & 7));
        ++n_out;
    }
    for (size_t k = 0; k < a.L.size(); ++k)
        if (a.L[k].pos != a.L[k].n)
            fail(plan.name, "leaf " + std::to_string(k) + " still has " +
                            std::to_string(a.L[k].n - a.L[k].pos) + " unread entries after the last row");

    out.reset();
    out.type = "byte_array";
    out.logical_type = "varchar";
    out.num_rows = static_cast<int32_t>(n_out);
    out.success = true;
    out.string_arena.assign(a.o.begin(), a.o.end());
    out.string_offsets.assign(offs.begin(), offs.end());
    out.string_lens.assign(lens.begin(), lens.end());
    if (any_null) {
        valid.resize((n_out + 7) / 8);
        out.valid_bits = std::move(valid);
    }
}

}  // namespace nested

// ── projection resolution ───────────────────────────────────────────────────

namespace {

bool has_prefix(const std::string& s, const std::string& name) {
    return s.size() > name.size() && s.compare(0, name.size(), name) == 0 && s[name.size()] == '.';
}

}  // namespace

bool group_renderable(const FileStats& fs, const std::string& name, std::string& err) {
    return nested::build_plan(fs, name, err) != nullptr;
}

bool resolve_projection(const FileStats& fs, const std::vector<int>& rg_idxs,
                        const std::vector<std::string>& projected,
                        std::vector<std::string>& names,
                        std::vector<std::vector<ColumnStats>>& stats,
                        std::shared_ptr<const NestedSpec>& nested_out, std::string& err) {
    names.clear();
    stats.assign(rg_idxs.size(), {});
    nested_out.reset();
    if (rg_idxs.empty()) { names = projected; return true; }

    // Decide plain vs group from the first row group: the schema is per FILE, so
    // every row group of the file agrees.
    const RowGroupStats& rg0 = fs.row_groups[static_cast<size_t>(rg_idxs[0])];
    std::vector<std::shared_ptr<const nested::Plan>> plans(projected.size());
    bool any_group = false;
    for (size_t k = 0; k < projected.size(); ++k) {
        bool exact = false, prefixed = false;
        for (const ColumnStats& cs : rg0.columns) {
            if (cs.name == projected[k]) { exact = true; break; }
            if (has_prefix(cs.name, projected[k])) prefixed = true;
        }
        if (exact || !prefixed) continue;
        plans[k] = nested::build_plan(fs, projected[k], err);
        if (!plans[k]) return false;
        any_group = true;
    }

    auto spec = std::make_shared<NestedSpec>();
    for (size_t m = 0; m < rg_idxs.size(); ++m) {
        const RowGroupStats& rg = fs.row_groups[static_cast<size_t>(rg_idxs[m])];
        std::vector<ColumnStats>& out = stats[m];
        std::vector<std::string> row_names;
        std::vector<uint32_t> orig;
        std::vector<NestedGroup> groups;
        for (size_t k = 0; k < projected.size(); ++k) {
            if (!plans[k]) {
                bool found = false;
                for (const ColumnStats& cs : rg.columns) {
                    if (cs.name == projected[k]) {
                        row_names.push_back(cs.name);
                        orig.push_back(static_cast<uint32_t>(k));
                        out.push_back(cs);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    err = "row group is missing projected column '" + projected[k] +
                          "' (schema evolution is not supported on this path)";
                    return false;
                }
                continue;
            }
            NestedGroup g;
            g.name = projected[k];
            g.first = row_names.size();
            g.plan = plans[k];
            long prev = -1;
            for (size_t ci = 0; ci < rg.columns.size(); ++ci) {
                const ColumnStats& cs = rg.columns[ci];
                if (cs.name != projected[k] && !has_prefix(cs.name, projected[k])) continue;
                if (prev >= 0 && static_cast<long>(ci) != prev + 1) {
                    err = "column '" + projected[k] + "': its leaf chunks are not contiguous in the row group";
                    return false;
                }
                prev = static_cast<long>(ci);
                row_names.push_back(cs.name);
                orig.push_back(static_cast<uint32_t>(k));
                out.push_back(cs);
            }
            g.count = row_names.size() - g.first;
            if (g.count != nested::leaf_count(*plans[k])) {
                err = "column '" + projected[k] + "': the row group has " + std::to_string(g.count) +
                      " leaf chunks but the schema declares " +
                      std::to_string(nested::leaf_count(*plans[k]));
                return false;
            }
            groups.push_back(std::move(g));
        }
        if (m == 0) {
            names = row_names;
            spec->orig = orig;
            spec->groups = groups;
        } else if (row_names != names) {
            err = "the row groups of one file disagree on the projected leaf chunks";
            return false;
        }
    }
    if (any_group) nested_out = spec;
    return true;
}

}  // namespace rugo
