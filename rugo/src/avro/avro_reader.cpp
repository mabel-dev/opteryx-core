#include "avro_reader.hpp"

#include <cmath>
#include <cstring>
#include <map>
#include <memory>
#include <stdexcept>

#include "avro_container.hpp"
#include "avro_schema.hpp"
#include "avro_varint.hpp"
#include "base64/_base64.h"         // mabel bintob64 (exported by draken_native)
#include "core/alloc.h"
#include "core/append_buffer.h"
#include "interop/value_format.hpp" // the NESTEDJSON value rendering, shared with parquet
#include "ops/float_ops.h"          // fp_canon: -0.0 / NaN canon, as parquet's renderer
#include "yyjson.h"                 // field defaults (compile time only)

namespace rugo::avro {

// ── AvroColumn ownership ────────────────────────────────────────────────────

void AvroColumn::steal(AvroColumn& o) noexcept {
    kind = o.kind; type = o.type; length = o.length;
    data = o.data; validity = o.validity; slots = o.slots; arena = o.arena;
    arena_len = o.arena_len; codes = o.codes; dict_len = o.dict_len;
    precision = o.precision; scale = o.scale;
    offsets = o.offsets; child_validity = o.child_validity;
    child_type = o.child_type; child_length = o.child_length;
    o.disown();
}

void AvroColumn::release_all() noexcept {
    draken_free(data); draken_free(validity); draken_free(slots); draken_free(arena); draken_free(codes);
    draken_free(offsets); draken_free(child_validity);
    disown();
}

namespace {

[[noreturn]] void fail(const std::string& msg) { throw std::runtime_error("Avro: " + msg); }

// ── the program (docs §5.1) ─────────────────────────────────────────────────

enum Code : uint8_t {
    // reads: write row r of column `col`
    R_BOOL, R_INT32, R_INT64, R_FLOAT, R_DOUBLE, R_BYTES, R_FIXED, R_ENUM,
    R_TIME_MS,       // int millis -> int64 micros
    R_TS_MS,         // long millis -> int64 micros (checked)
    R_DEC_BYTES64, R_DEC_BYTES128, R_DEC_FIXED64, R_DEC_FIXED128,
    // reader-schema promotions (docs §19.3)
    R_INT_AS_I64, R_INT_AS_F32, R_INT_AS_F64, R_LONG_AS_F32, R_LONG_AS_F64, R_FLOAT_AS_F64,
    R_ENUM_MAP,      // writer enum index -> reader index through enum_maps[sub ..]
    R_ARRAY,         // ARRAY column `col`; its items are read into child column `arg`
    R_JSON,          // render the `sub` J_* ops that follow as JSON text into `col`
    // skips
    S_VARINT, S_BOOL, S_FIXED, S_BYTES, S_ARRAY, S_MAP,
    // ["null", T]: on the null branch, NULL the listed columns and jump past T's ops
    OPT,
    // JSON rendering (inside R_JSON), the parquet NESTEDJSON rules
    J_LIT,           // append literal `arg`
    J_BOOL, J_INT, J_LONG, J_FLOAT, J_DOUBLE, J_STR, J_B64, J_FIXED_B64, J_ENUM,
    J_DATE, J_TIME_MS, J_TIME_US, J_TS_MS, J_TS_US, J_DEC_BYTES, J_DEC_FIXED,
    J_ARRAY, J_MAP,  // items/values are the `sub` ops that follow
    J_OPT,           // null branch -> `null`, else the `sub` ops that follow
    // a record whose fields the reader schema orders differently from the file:
    J_REC_BEGIN,     // open a frame of `arg` slots
    J_SLOT,          // render the `sub` ops that follow into slot `arg`
    J_REC_END,       // write the record: rec_table[arg .. arg + 2*col) = (key lit, slot | ~default lit)
};

struct Op {
    Code     code;
    uint8_t  null_branch = 0;  // OPT / J_OPT
    uint32_t col = 0;          // R_*: column. OPT: first entry in null_cols.
                               // J_ENUM: symbol count. J_DEC_*: precision. J_REC_END: fields
    uint32_t arg = 0;          // R_FIXED/S_FIXED/R_DEC_FIXED*/J_FIXED_B64/J_DEC_FIXED: byte size.
                               // R_ENUM/R_ENUM_MAP: writer symbol count. R_ARRAY: child column.
                               // OPT: null_cols count. J_LIT: literal. J_ENUM: first literal.
                               // J_REC_BEGIN: slots. J_SLOT: slot. J_REC_END: rec_table offset
    uint32_t sub = 0;          // OPT/S_ARRAY/S_MAP/R_ARRAY/R_JSON/J_ARRAY/J_MAP/J_OPT/J_SLOT: length
                               // of the sub-program that follows. R_DEC_*: precision.
                               // J_DEC_*: scale. R_ENUM_MAP: enum_maps offset
};

struct ColSpec {
    std::string name;
    OutKind     kind = OutKind::Raw;
    DrakenType  type = DRAKEN_INT64;
    uint32_t    width = 0;          // bytes per row for fixed-width data
    bool        nullable = false;
    bool        hidden = false;     // an ARRAY's child: grows per element, not per row
    uint32_t    child = 0;          // Array: the child column
    uint8_t     precision = 0, scale = 0;
    const std::vector<std::string>* symbols = nullptr;  // Dict
    // ConstRaw / ConstString: the one value (fixed-width bytes, or string bytes),
    // or NULL for every row.
    std::string const_value;
    bool        const_null = false;
};

struct Program {
    std::vector<Op>          ops;
    std::vector<uint32_t>    null_cols;
    std::vector<ColSpec>     cols;      // [0, n_out) are output columns, then hidden children
    std::vector<std::string> lits;      // J_LIT text
    std::vector<uint32_t>    enum_maps; // R_ENUM_MAP tables
    std::vector<int64_t>     rec_table; // J_REC_END: (key literal, slot or ~default literal) pairs
    size_t                   n_out = 0;
};

// A requested path, as a trie over field names.
struct Req {
    std::map<std::string, Req> kids;
    int col = -1;
    std::string path;
};

std::string quoted(const std::string& s) {
    std::string o;
    rugo_text::json_string(o, s.data(), s.size());
    return o;
}

const Node* unwrap(const Node* t) { return t->kind == Kind::Union ? t->value_branch() : t; }

// Big-endian two's complement -> __int128. Longer than 16 bytes only with redundant
// sign-extension bytes.
__int128 be_twos(const uint8_t* b, uint64_t n) {
    if (n == 0) return 0;
    const uint8_t ext = (b[0] & 0x80) ? 0xFF : 0x00;
    while (n > 16) {
        if (b[0] != ext || ((b[1] & 0x80) != (ext & 0x80))) fail("a decimal value is wider than 128 bits");
        ++b; --n;
    }
    unsigned __int128 u = ext ? ~static_cast<unsigned __int128>(0) : 0;
    for (uint64_t i = 0; i < n; ++i) u = (u << 8) | b[i];
    return static_cast<__int128>(u);
}

// pow10[i] as __int128, i = 0..38: the exclusive bound on |unscaled| for precision i.
struct Pow10 {
    __int128 v[39];
    Pow10() { v[0] = 1; for (int i = 1; i < 39; ++i) v[i] = v[i - 1] * 10; }
};
const Pow10& pow10() { static const Pow10 t; return t; }

__int128 checked_decimal(const uint8_t* b, uint64_t n, uint32_t precision) {
    const __int128 v = be_twos(b, n);
    const __int128 lim = pow10().v[precision];
    if (v >= lim || v <= -lim) fail("a decimal value exceeds its declared precision " + std::to_string(precision));
    return v;
}

// ── field defaults (docs §16) ───────────────────────────────────────────────

// An Avro default as a value. JSON text is parsed at compile time only.
class DefaultValue {
public:
    DefaultValue(const std::string& json, const std::string& col)
        : doc_(yyjson_read(json.data(), json.size(), 0), [](yyjson_doc* d) { yyjson_doc_free(d); }), col_(col) {
        if (!doc_) fail("column '" + col + "': its default is not valid JSON");
        v_ = yyjson_doc_get_root(doc_.get());
    }
    bool is_null() const { return yyjson_is_null(v_); }
    bool boolean() const { if (!yyjson_is_bool(v_)) bad("a boolean"); return yyjson_get_bool(v_); }
    int64_t integer(int64_t lo, int64_t hi) const {
        if (!yyjson_is_int(v_)) bad("an integer");
        const int64_t i = yyjson_get_sint(v_);
        if (yyjson_is_uint(v_) && yyjson_get_uint(v_) > static_cast<uint64_t>(INT64_MAX)) bad("an integer in range");
        if (i < lo || i > hi) bad("an integer in range");
        return i;
    }
    double real() const {
        if (yyjson_is_num(v_)) return yyjson_get_num(v_);
        if (yyjson_is_str(v_)) {
            const std::string s(yyjson_get_str(v_), yyjson_get_len(v_));
            if (s == "NaN") return std::nan("");
            if (s == "Infinity") return HUGE_VAL;
            if (s == "-Infinity") return -HUGE_VAL;
        }
        bad("a number");
    }
    std::string text() const {
        if (!yyjson_is_str(v_)) bad("a string");
        return std::string(yyjson_get_str(v_), yyjson_get_len(v_));
    }
    // bytes / fixed defaults: a JSON string whose code points 0-255 are the bytes.
    std::string bytes() const {
        const std::string s = text();
        std::string out;
        for (size_t i = 0; i < s.size();) {
            const uint8_t b = static_cast<uint8_t>(s[i]);
            uint32_t cp;
            if (b < 0x80) { cp = b; i += 1; }
            else if ((b & 0xE0) == 0xC0 && i + 1 < s.size()) {
                cp = ((b & 0x1Fu) << 6) | (static_cast<uint8_t>(s[i + 1]) & 0x3Fu);
                i += 2;
            } else bad("a byte string (code points 0-255)");
            if (cp > 255) bad("a byte string (code points 0-255)");
            out.push_back(static_cast<char>(cp));
        }
        return out;
    }

private:
    [[noreturn]] void bad(const char* what) const { fail("column '" + col_ + "': its default is not " + what); }
    std::unique_ptr<yyjson_doc, void (*)(yyjson_doc*)> doc_;
    yyjson_val* v_ = nullptr;
    std::string col_;
};

// ── the compiler: writer schema (the file's) resolved against the reader's ─────

class Compiler {
public:
    // `schema_name` names the schema columns resolve against, for error messages.
    Compiler(Program& p, const char* schema_name) : p_(p), schema_name_(schema_name) {}

    void compile_root(const Node* w, const Node* r, Req& req) {
        if (w->kind != Kind::Record) fail("the file's schema is a " + std::string(kind_name(w->kind)) +
                                          ", not a record; only record schemas are supported");
        if (r->kind != Kind::Record) fail("the reader schema is not a record");
        compile_record(w, r, req);
    }

private:
    Program& p_;
    const char* schema_name_;
    size_t fuse_floor_ = 0;  // ops at or after this index may be fused with the next op

    void push(const Op& op) { p_.ops.push_back(op); }
    bool can_fuse(Code c) const { return p_.ops.size() > fuse_floor_ && p_.ops.back().code == c; }

    // Fixed-size skips fuse: consecutive skipped float/double/fixed fields cost one op.
    void skip_fixed(uint32_t n) {
        if (n == 0) return;
        if (can_fuse(S_FIXED)) { p_.ops.back().arg += n; return; }
        push(Op{S_FIXED, 0, 0, n, 0});
    }

    // Adjacent JSON literals fuse: `{"a":` is one op.
    void lit(const std::string& s) {
        if (can_fuse(J_LIT)) { p_.lits[p_.ops.back().arg] += s; return; }
        push(Op{J_LIT, 0, 0, add_lit(s), 0});
    }
    uint32_t add_lit(const std::string& s) {
        p_.lits.push_back(s);
        return static_cast<uint32_t>(p_.lits.size() - 1);
    }

    // Open a sub-program after op `at`; close() records its length and fences fusion.
    size_t open(const Op& op) { push(op); fuse_floor_ = p_.ops.size(); return p_.ops.size() - 1; }
    void close(size_t at) {
        p_.ops[at].sub = static_cast<uint32_t>(p_.ops.size() - at - 1);
        fuse_floor_ = p_.ops.size();
    }

    void collect_cols(const Req& r, std::vector<uint32_t>& out) {
        if (r.col >= 0) out.push_back(static_cast<uint32_t>(r.col));
        for (auto& kv : r.kids) collect_cols(kv.second, out);
    }

    void mark_nullable(const Req& r) {
        if (r.col >= 0) p_.cols[static_cast<size_t>(r.col)].nullable = true;
        for (auto& kv : r.kids) mark_nullable(kv.second);
    }

    [[noreturn]] static void nullable_into_required(const std::string& col) {
        fail("column '" + col + "' is nullable in the file but required in the reader schema");
    }

    // The reader field a writer field resolves to: by field-id when the writer field
    // carries one and the reader record uses ids, else by name. -1 = none.
    static int match(const Field& wf, const Node* r) {
        bool r_ids = false;
        for (const Field& rf : r->fields) r_ids |= rf.field_id >= 0;
        for (size_t i = 0; i < r->fields.size(); ++i) {
            const Field& rf = r->fields[i];
            if (wf.field_id >= 0 && r_ids ? rf.field_id == wf.field_id : rf.name == wf.name)
                return static_cast<int>(i);
        }
        return -1;
    }

    // Writer field i -> reader field index (or -1); a reader field matched twice is refused.
    static std::vector<int> match_all(const Node* w, const Node* r, const std::string& where) {
        std::vector<int> m(w->fields.size());
        std::vector<char> taken(r->fields.size(), 0);
        for (size_t i = 0; i < w->fields.size(); ++i) {
            m[i] = match(w->fields[i], r);
            if (m[i] >= 0) {
                if (taken[static_cast<size_t>(m[i])])
                    fail("two of the file's fields resolve to reader field '" + r->fields[static_cast<size_t>(m[i])].name +
                         "'" + where);
                taken[static_cast<size_t>(m[i])] = 1;
            }
        }
        return m;
    }

    void compile_record(const Node* w, const Node* r, Req& req) {
        const std::vector<int> m = match_all(w, r, "");
        std::vector<char> matched(r->fields.size(), 0);
        for (size_t i = 0; i < w->fields.size(); ++i) {
            const Field& wf = w->fields[i];
            if (m[i] < 0) { emit_skip(wf.type); continue; }
            const Field& rf = r->fields[static_cast<size_t>(m[i])];
            matched[static_cast<size_t>(m[i])] = 1;
            auto it = req.kids.find(rf.name);
            if (it == req.kids.end()) { emit_skip(wf.type); continue; }
            compile_field(wf.type, rf.type, it->second);
        }
        for (auto& kv : req.kids) {
            int ri = -1;
            for (size_t i = 0; i < r->fields.size(); ++i)
                if (r->fields[i].name == kv.first) ri = static_cast<int>(i);
            if (ri < 0) fail("column '" + kv.second.path + "' is not in " + std::string(schema_name_));
            if (!matched[static_cast<size_t>(ri)]) absent(r->fields[static_cast<size_t>(ri)], kv.second);
        }
    }

    void compile_field(const Node* wt, const Node* rt, Req& req) {
        if (req.col >= 0 && !req.kids.empty())
            fail("column '" + req.path + "' is selected both whole and by a field inside it");
        if (req.col >= 0) { emit_read(wt, rt, static_cast<uint32_t>(req.col)); return; }
        // An intermediate: must be a record, or a nullable record, on both sides.
        const Node* wrec = unwrap(wt);
        const Node* rrec = unwrap(rt);
        if (rrec->kind != Kind::Record)
            fail("column '" + req.path + "' is a " + kind_name(rrec->kind) + ", so fields inside it cannot be selected");
        if (wrec->kind != Kind::Record)
            fail("column '" + req.path + "' is a " + kind_name(wrec->kind) + " in the file, not a record");
        if (wt->kind == Kind::Union) {
            if (rt->kind != Kind::Union) nullable_into_required(req.path);
            std::vector<uint32_t> nc;
            collect_cols(req, nc);
            mark_nullable(req);
            const size_t at = open(Op{OPT, wt->null_branch, static_cast<uint32_t>(p_.null_cols.size()),
                                      static_cast<uint32_t>(nc.size()), 0});
            p_.null_cols.insert(p_.null_cols.end(), nc.begin(), nc.end());
            compile_record(wrec, rrec, req);
            close(at);
        } else {
            compile_record(wrec, rrec, req);
        }
    }

    void emit_read(const Node* wt, const Node* rt, uint32_t col) {
        if (wt->kind == Kind::Union) {
            if (rt->kind != Kind::Union) nullable_into_required(p_.cols[col].name);
            p_.cols[col].nullable = true;
            const size_t at = open(Op{OPT, wt->null_branch, static_cast<uint32_t>(p_.null_cols.size()), 1, 0});
            p_.null_cols.push_back(col);
            emit_read_value(wt->value_branch(), rt->value_branch(), col);
            close(at);
            return;
        }
        emit_read_value(wt, unwrap(rt), col);
    }

    // An ARRAY child must be a plain scalar Draken ARRAY can hold.
    static bool array_scalar(const Node* t) {
        switch (t->kind) {
            case Kind::Boolean: case Kind::Float: case Kind::Double: return true;
            case Kind::Int: case Kind::Long: case Kind::String: case Kind::Bytes: case Kind::Fixed:
                return t->logical == Logical::None;
            default: return false;
        }
    }

    // Nested shapes rendered as JSON text: records, maps, and arrays of nested shapes.
    static bool is_json(const Node* t) {
        if (t->kind == Kind::Record || t->kind == Kind::Map) return true;
        if (t->kind != Kind::Array) return false;
        const Kind k = unwrap(t->items)->kind;
        return k == Kind::Record || k == Kind::Array || k == Kind::Map;
    }

    // May a value written as `w` be read as `r`? (the spec's promotions)
    static bool promotes(Kind w, Kind r) {
        if (w == r) return true;
        switch (w) {
            case Kind::Int:    return r == Kind::Long || r == Kind::Float || r == Kind::Double;
            case Kind::Long:   return r == Kind::Float || r == Kind::Double;
            case Kind::Float:  return r == Kind::Double;
            case Kind::String: return r == Kind::Bytes;
            case Kind::Bytes:  return r == Kind::String;
            default:           return false;
        }
    }

    void check_compatible(const Node* w, const Node* r, const std::string& col) {
        if (!promotes(w->kind, r->kind))
            fail("column '" + col + "': the file's " + kind_name(w->kind) + " cannot be read as " + kind_name(r->kind));
        if (w->kind == Kind::Fixed && w->fixed_size != r->fixed_size)
            fail("column '" + col + "': the file's fixed(" + std::to_string(w->fixed_size) + ") cannot be read as fixed(" +
                 std::to_string(r->fixed_size) + ")");
        if (w->kind == Kind::Enum)
            for (const std::string& s : w->symbols) {
                bool found = false;
                for (const std::string& t : r->symbols) found |= (s == t);
                if (!found) fail("column '" + col + "': the file's enum symbol '" + s + "' is not in the reader's enum");
            }
    }

    void set_spec(uint32_t col, OutKind k, DrakenType dt, uint32_t w) {
        ColSpec& cs = p_.cols[col];
        cs.kind = k; cs.type = dt; cs.width = w;
    }

    void emit_read_value(const Node* w, const Node* r, uint32_t col) {
        // NB: p_.cols may grow below (an ARRAY child) — index, never hold a reference.
        const std::string name = p_.cols[col].name;
        if (w->kind == Kind::Null) fail("column '" + name + "' has type null, which is not supported");
        if (is_json(w)) {
            if (r->kind != w->kind)
                fail("column '" + name + "': the file's " + kind_name(w->kind) + " cannot be read as " + kind_name(r->kind));
            set_spec(col, OutKind::String, DRAKEN_NVARCHAR, 0);
            const size_t at = open(Op{R_JSON, 0, col, 0, 0});
            emit_json(w, r, name);
            close(at);
            return;
        }
        if (w->kind == Kind::Array) {
            if (r->kind != Kind::Array)
                fail("column '" + name + "': the file's array cannot be read as " + kind_name(r->kind));
            if (!array_scalar(unwrap(w->items)))
                fail("column '" + name + "' is an array of " +
                     (unwrap(w->items)->kind == Kind::Enum ? std::string("enum") : std::string("a logical type")) +
                     ", which ARRAY cannot hold; not supported");
            ColSpec child;
            child.name = name + "[]";
            child.hidden = true;
            p_.cols.push_back(std::move(child));
            const uint32_t ci = static_cast<uint32_t>(p_.cols.size() - 1);
            set_spec(col, OutKind::Array, DRAKEN_ARRAY, 0);
            p_.cols[col].child = ci;
            const size_t at = open(Op{R_ARRAY, 0, col, ci, 0});
            emit_read(w->items, r->items, ci);
            close(at);
            return;
        }
        if (unwrap(r)->kind == Kind::Array && w->kind != Kind::Array)
            fail("column '" + name + "': the file's " + kind_name(w->kind) + " cannot be read as array");
        check_compatible(w, r, name);
        // Logical types follow the FILE (fastavro / Apache): the writer's logical type
        // decides the column; the reader's is not consulted.
        if (w->logical != Logical::None) { emit_plain(w, col); return; }
        if (w->kind == Kind::Enum) {
            if (w->symbols == r->symbols) { emit_plain(w, col); return; }
            set_spec(col, OutKind::Dict, DRAKEN_VARCHAR, 0);
            p_.cols[col].symbols = &r->symbols;
            const uint32_t off = static_cast<uint32_t>(p_.enum_maps.size());
            for (const std::string& s : w->symbols)
                for (size_t j = 0; j < r->symbols.size(); ++j)
                    if (r->symbols[j] == s) p_.enum_maps.push_back(static_cast<uint32_t>(j));
            push(Op{R_ENUM_MAP, 0, col, static_cast<uint32_t>(w->symbols.size()), off});
            return;
        }
        auto raw = [&](Code c, DrakenType dt, uint32_t width) {
            set_spec(col, OutKind::Raw, dt, width);
            push(Op{c, 0, col, 0, 0});
        };
        switch (w->kind) {
            case Kind::Int:
                if (r->kind == Kind::Long) { raw(R_INT_AS_I64, DRAKEN_INT64, 8); return; }
                if (r->kind == Kind::Float) { raw(R_INT_AS_F32, DRAKEN_FLOAT32, 4); return; }
                if (r->kind == Kind::Double) { raw(R_INT_AS_F64, DRAKEN_FLOAT64, 8); return; }
                break;
            case Kind::Long:
                if (r->kind == Kind::Float) { raw(R_LONG_AS_F32, DRAKEN_FLOAT32, 4); return; }
                if (r->kind == Kind::Double) { raw(R_LONG_AS_F64, DRAKEN_FLOAT64, 8); return; }
                break;
            case Kind::Float:
                if (r->kind == Kind::Double) { raw(R_FLOAT_AS_F64, DRAKEN_FLOAT64, 8); return; }
                break;
            case Kind::String:
            case Kind::Bytes:
                // string <-> bytes: the same bytes, the reader's tag
                set_spec(col, OutKind::String, r->kind == Kind::String ? DRAKEN_VARCHAR : DRAKEN_VARBINARY, 0);
                push(Op{R_BYTES, 0, col, 0, 0});
                return;
            default:
                break;
        }
        emit_plain(w, col);
    }

    // A scalar read with the file's own type (and logical type).
    void emit_plain(const Node* t, uint32_t col) {
        const std::string name = p_.cols[col].name;
        auto raw = [&](Code c, DrakenType dt, uint32_t w) { set_spec(col, OutKind::Raw, dt, w); push(Op{c, 0, col, 0, 0}); };
        switch (t->kind) {
            case Kind::Boolean: raw(R_BOOL, DRAKEN_BOOL, 0); return;
            case Kind::Int:
                if (t->logical == Logical::Date) { raw(R_INT32, DRAKEN_DATE32, 4); return; }
                if (t->logical == Logical::TimeMillis) {
                    set_spec(col, OutKind::Time64, DRAKEN_TIME64, 8);
                    push(Op{R_TIME_MS, 0, col, 0, 0});
                    return;
                }
                raw(R_INT32, DRAKEN_INT32, 4);
                return;
            case Kind::Long:
                if (t->logical == Logical::TimeMicros) {
                    set_spec(col, OutKind::Time64, DRAKEN_TIME64, 8);
                    push(Op{R_INT64, 0, col, 0, 0});
                    return;
                }
                if (t->logical == Logical::TimestampMillis || t->logical == Logical::TimestampMicros) {
                    set_spec(col, OutKind::Timestamp, DRAKEN_TIMESTAMP64, 8);
                    push(Op{t->logical == Logical::TimestampMillis ? R_TS_MS : R_INT64, 0, col, 0, 0});
                    return;
                }
                raw(R_INT64, DRAKEN_INT64, 8);
                return;
            case Kind::Float:  raw(R_FLOAT, DRAKEN_FLOAT32, 4); return;
            case Kind::Double: raw(R_DOUBLE, DRAKEN_FLOAT64, 8); return;
            case Kind::Bytes:
            case Kind::Fixed:
            case Kind::String: {
                if (t->logical == Logical::Uuid)
                    fail("column '" + name + "' is a uuid, which is not supported (as for parquet)");
                if (t->logical == Logical::Decimal) {
                    const bool wide = t->precision > 18;
                    set_spec(col, wide ? OutKind::Decimal128 : OutKind::Decimal64,
                             wide ? DRAKEN_DECIMAL128 : DRAKEN_DECIMAL, wide ? 16 : 8);
                    p_.cols[col].precision = t->precision;
                    p_.cols[col].scale = t->scale;
                    const Code c = t->kind == Kind::Fixed ? (wide ? R_DEC_FIXED128 : R_DEC_FIXED64)
                                                          : (wide ? R_DEC_BYTES128 : R_DEC_BYTES64);
                    push(Op{c, 0, col, t->fixed_size, static_cast<uint32_t>(t->precision)});
                    return;
                }
                set_spec(col, OutKind::String, t->kind == Kind::String ? DRAKEN_VARCHAR : DRAKEN_VARBINARY, 0);
                if (t->kind == Kind::Fixed) push(Op{R_FIXED, 0, col, t->fixed_size, 0});
                else push(Op{R_BYTES, 0, col, 0, 0});
                return;
            }
            case Kind::Enum:
                set_spec(col, OutKind::Dict, DRAKEN_VARCHAR, 0);
                p_.cols[col].symbols = &t->symbols;
                push(Op{R_ENUM, 0, col, static_cast<uint32_t>(t->symbols.size()), 0});
                return;
            default:
                fail("column '" + name + "': unexpected " + kind_name(t->kind));
        }
    }

    // ── JSON text (parquet's NESTEDJSON rules), resolved against the reader ──

    void emit_json(const Node* w, const Node* r, const std::string& col) {
        if (w->kind == Kind::Union) {
            if (r->kind != Kind::Union) nullable_into_required(col);
            const size_t at = open(Op{J_OPT, w->null_branch});
            emit_json(w->value_branch(), r->value_branch(), col);
            close(at);
            return;
        }
        r = unwrap(r);
        if (w->kind == Kind::Null) { lit("null"); return; }
        if (w->kind == Kind::Record) {
            if (r->kind != Kind::Record) fail("column '" + col + "': the file's record cannot be read as " + kind_name(r->kind));
            emit_json_record(w, r, col);
            return;
        }
        if (w->kind == Kind::Array || w->kind == Kind::Map) {
            if (r->kind != w->kind)
                fail("column '" + col + "': the file's " + kind_name(w->kind) + " cannot be read as " + kind_name(r->kind));
            const size_t at = open(Op{w->kind == Kind::Array ? J_ARRAY : J_MAP});
            emit_json(w->kind == Kind::Array ? w->items : w->values, w->kind == Kind::Array ? r->items : r->values, col);
            close(at);
            return;
        }
        check_compatible(w, r, col);
        if (w->logical == Logical::None && (w->kind == Kind::String || w->kind == Kind::Bytes)) {
            push(Op{r->kind == Kind::String ? J_STR : J_B64});  // string <-> bytes: the reader's rendering
            return;
        }
        emit_json_scalar(w, col);
    }

    void emit_json_scalar(const Node* t, const std::string& col) {
        switch (t->kind) {
            case Kind::Boolean: push(Op{J_BOOL}); return;
            case Kind::Int:
                push(Op{t->logical == Logical::Date ? J_DATE : t->logical == Logical::TimeMillis ? J_TIME_MS : J_INT});
                return;
            case Kind::Long:
                push(Op{t->logical == Logical::TimeMicros        ? J_TIME_US
                        : t->logical == Logical::TimestampMillis ? J_TS_MS
                        : t->logical == Logical::TimestampMicros ? J_TS_US
                                                                 : J_LONG});
                return;
            case Kind::Float: push(Op{J_FLOAT}); return;
            case Kind::Double: push(Op{J_DOUBLE}); return;
            case Kind::String:
            case Kind::Bytes:
            case Kind::Fixed:
                if (t->logical == Logical::Uuid)
                    fail("column '" + col + "' holds a uuid, which is not supported (as for parquet)");
                if (t->logical == Logical::Decimal) {
                    push(Op{t->kind == Kind::Fixed ? J_DEC_FIXED : J_DEC_BYTES, 0, t->precision, t->fixed_size, t->scale});
                    return;
                }
                if (t->kind == Kind::String) push(Op{J_STR});
                else if (t->kind == Kind::Bytes) push(Op{J_B64});
                else push(Op{J_FIXED_B64, 0, 0, t->fixed_size, 0});
                return;
            case Kind::Enum: {
                const uint32_t first = static_cast<uint32_t>(p_.lits.size());
                for (const std::string& s : t->symbols) p_.lits.push_back(quoted(s));
                push(Op{J_ENUM, 0, static_cast<uint32_t>(t->symbols.size()), first, 0});
                return;
            }
            default:
                fail("column '" + col + "': unexpected " + kind_name(t->kind));
        }
    }

    static std::string member_key(const Node* r, size_t i) {
        return (i == 0 ? "{" : ",") + quoted(r->fields[i].name) + ":";
    }

    void emit_json_record(const Node* w, const Node* r, const std::string& col) {
        const std::vector<int> m = match_all(w, r, " in column '" + col + "'");
        std::vector<char> matched(r->fields.size(), 0);
        bool in_order = true;
        int last = -1;
        for (int ri : m) {
            if (ri < 0) continue;
            matched[static_cast<size_t>(ri)] = 1;
            in_order &= ri > last;
            last = ri;
        }
        if (r->fields.empty()) {
            for (const Field& wf : w->fields) emit_skip(wf.type);
            lit("{}");
            return;
        }
        if (in_order) {
            // Direct: the file's fields arrive in the reader's order; a reader-only field
            // is its default literal, written where it belongs.
            size_t next = 0;
            auto defaults_until = [&](size_t limit) {
                for (; next < limit; ++next) {
                    lit(member_key(r, next));
                    lit(default_json(r->fields[next], col));
                }
            };
            for (size_t i = 0; i < w->fields.size(); ++i) {
                if (m[i] < 0) { emit_skip(w->fields[i].type); continue; }
                const size_t ri = static_cast<size_t>(m[i]);
                defaults_until(ri);
                lit(member_key(r, ri));
                emit_json(w->fields[i].type, r->fields[ri].type, col);
                next = ri + 1;
            }
            defaults_until(r->fields.size());
            lit("}");
            return;
        }
        // Reordered: render each field into its slot, then write them in the reader's order.
        push(Op{J_REC_BEGIN, 0, 0, static_cast<uint32_t>(r->fields.size()), 0});
        for (size_t i = 0; i < w->fields.size(); ++i) {
            if (m[i] < 0) { emit_skip(w->fields[i].type); continue; }
            const size_t at = open(Op{J_SLOT, 0, 0, static_cast<uint32_t>(m[i]), 0});
            emit_json(w->fields[i].type, r->fields[static_cast<size_t>(m[i])].type, col);
            close(at);
        }
        const uint32_t off = static_cast<uint32_t>(p_.rec_table.size());
        for (size_t i = 0; i < r->fields.size(); ++i) {
            p_.rec_table.push_back(add_lit(member_key(r, i)));
            p_.rec_table.push_back(matched[i] ? static_cast<int64_t>(i) : ~static_cast<int64_t>(add_lit(default_json(r->fields[i], col))));
        }
        push(Op{J_REC_END, 0, static_cast<uint32_t>(r->fields.size()), off, 0});
        fuse_floor_ = p_.ops.size();
    }

    // A reader-only field inside a JSON column: its default as JSON text (NULL when it
    // has none, as a top-level reader-only column). Record / map / array defaults are
    // refused (docs §16).
    std::string default_json(const Field& f, const std::string& col) {
        if (!f.has_default) return "null";
        const DefaultValue d(f.default_json, col);
        // A union's default is for its FIRST branch.
        const Node* t = f.type->kind == Kind::Union ? f.type->branches[0] : f.type;
        std::string o;
        switch (t->kind) {
            case Kind::Null:
                if (!d.is_null()) fail("column '" + col + "': the default of '" + f.name + "' is not null");
                return "null";
            case Kind::Boolean: return d.boolean() ? "true" : "false";
            case Kind::Int:
            case Kind::Long: {
                const int64_t v = t->kind == Kind::Int ? d.integer(INT32_MIN, INT32_MAX) : d.integer(INT64_MIN, INT64_MAX);
                if (t->logical == Logical::Date) rugo_text::fmt_date_quoted(o, static_cast<int32_t>(v));
                else if (t->logical == Logical::TimeMillis) rugo_text::fmt_time_quoted(o, v, rugo_text::U_MS);
                else if (t->logical == Logical::TimeMicros) rugo_text::fmt_time_quoted(o, v, rugo_text::U_US);
                else if (t->logical == Logical::TimestampMillis) rugo_text::fmt_timestamp_quoted(o, v, rugo_text::U_MS);
                else if (t->logical == Logical::TimestampMicros) rugo_text::fmt_timestamp_quoted(o, v, rugo_text::U_US);
                else rugo_text::fmt_int64(o, v);
                return o;
            }
            case Kind::Float:
            case Kind::Double: {
                const double v = d.real();
                if (std::isnan(v) || std::isinf(v)) return "null";
                if (t->kind == Kind::Float) rugo_text::fmt_float(o, static_cast<float>(v));
                else rugo_text::fmt_double(o, v);
                return o;
            }
            case Kind::String:
                if (t->logical == Logical::Uuid) fail("column '" + col + "' holds a uuid, which is not supported");
                return quoted(d.text());
            case Kind::Bytes:
            case Kind::Fixed: {
                const std::string b = d.bytes();
                if (t->kind == Kind::Fixed && b.size() != t->fixed_size)
                    fail("column '" + col + "': the default of '" + f.name + "' is not " + std::to_string(t->fixed_size) + " bytes");
                if (t->logical == Logical::Decimal) {
                    rugo_text::fmt_decimal(o, checked_decimal(reinterpret_cast<const uint8_t*>(b.data()), b.size(), t->precision), t->scale);
                    return o;
                }
                if (t->logical == Logical::Uuid) fail("column '" + col + "' holds a uuid, which is not supported");
                o.push_back('"');
                const size_t at = o.size();
                o.resize(at + b64_encoded_size(b.size()));
                bintob64(&o[at], b.data(), b.size());
                o.resize(at + b64_encoded_size(b.size()) - 1);
                o.push_back('"');
                return o;
            }
            case Kind::Enum: {
                const std::string s = d.text();
                bool found = false;
                for (const std::string& x : t->symbols) found |= (x == s);
                if (!found) fail("column '" + col + "': the default of '" + f.name + "' is not one of its symbols");
                return quoted(s);
            }
            default:
                fail("column '" + col + "': a " + std::string(kind_name(t->kind)) + " default for '" + f.name +
                     "' is not supported");
        }
    }

    // ── reader-only columns: constants (docs §16, §19.3) ──

    // A reader field the file does not hold. A leaf is its default as a constant (NULL
    // when it has no default); a record selected through is NULL in every leaf below.
    void absent(const Field& f, Req& req) {
        if (req.col >= 0) { constant(f, static_cast<uint32_t>(req.col)); return; }
        const Node* rec = unwrap(f.type);
        if (rec->kind != Kind::Record)
            fail("column '" + req.path + "' is a " + kind_name(rec->kind) + ", so fields inside it cannot be selected");
        if (f.has_default && !DefaultValue(f.default_json, req.path).is_null())
            fail("column '" + req.path + "': a record default is not supported");
        null_leaves(rec, req);
    }

    void null_leaves(const Node* rec, Req& req) {
        for (auto& kv : req.kids) {
            const Field* f = nullptr;
            for (const Field& x : rec->fields)
                if (x.name == kv.first) f = &x;
            if (f == nullptr) fail("column '" + kv.second.path + "' is not in " + std::string(schema_name_));
            if (kv.second.col >= 0) {
                Field no_default = *f;
                no_default.has_default = false;
                constant(no_default, static_cast<uint32_t>(kv.second.col));
            } else {
                const Node* inner = unwrap(f->type);
                if (inner->kind != Kind::Record)
                    fail("column '" + kv.second.path + "' is a " + kind_name(inner->kind) + ", so fields inside it cannot be selected");
                null_leaves(inner, kv.second);
            }
        }
    }

    void constant(const Field& f, uint32_t col) {
        ColSpec& cs = p_.cols[col];
        const std::string& name = cs.name;
        const bool null = !f.has_default || DefaultValue(f.default_json, name).is_null();
        // A union's default is for its first branch; a NULL constant takes the value type.
        const Node* t = f.type->kind == Kind::Union ? (null ? f.type->value_branch() : f.type->branches[0]) : f.type;
        if (null && t->kind == Kind::Null) t = f.type->value_branch();
        if (!null && t->kind == Kind::Null) fail("column '" + name + "': its default is not null");
        cs.const_null = null;
        cs.nullable = cs.nullable || null;
        auto fixed = [&](DrakenType dt, const void* v, uint32_t w) {
            cs.kind = OutKind::ConstRaw; cs.type = dt; cs.width = w;
            cs.const_value.assign(static_cast<const char*>(v), w);
        };
        auto text = [&](DrakenType dt, const std::string& v) {
            cs.kind = OutKind::ConstString; cs.type = dt; cs.const_value = v;
        };
        if (t->logical == Logical::Uuid) fail("column '" + name + "' is a uuid, which is not supported (as for parquet)");
        // TIME / TIMESTAMP (microseconds) and DECIMAL constants carry their logical type
        // on the column. The engine attaches it natively; the Python edge has no
        // producer that does, and refuses them (docs §19.4).
        if (t->logical == Logical::TimeMillis || t->logical == Logical::TimeMicros ||
            t->logical == Logical::TimestampMillis || t->logical == Logical::TimestampMicros) {
            const bool ts = t->logical == Logical::TimestampMillis || t->logical == Logical::TimestampMicros;
            int64_t v = 0;
            if (!null) {
                const DefaultValue d(f.default_json, name);
                v = t->kind == Kind::Int ? d.integer(INT32_MIN, INT32_MAX) : d.integer(INT64_MIN, INT64_MAX);
                if ((t->logical == Logical::TimestampMillis || t->logical == Logical::TimeMillis) &&
                    __builtin_mul_overflow(v, int64_t(1000), &v))
                    fail("column '" + name + "': its default overflows microseconds");
            }
            fixed(ts ? DRAKEN_TIMESTAMP64 : DRAKEN_TIME64, &v, 8);
            return;
        }
        if (t->logical == Logical::Decimal) {
            const bool wide = t->precision > 18;
            __int128 v = 0;
            if (!null) {
                const std::string b = DefaultValue(f.default_json, name).bytes();
                if (t->kind == Kind::Fixed && b.size() != t->fixed_size)
                    fail("column '" + name + "': its default is not " + std::to_string(t->fixed_size) + " bytes");
                v = checked_decimal(reinterpret_cast<const uint8_t*>(b.data()), b.size(), t->precision);
            }
            cs.precision = t->precision;
            cs.scale = t->scale;
            if (wide) { fixed(DRAKEN_DECIMAL128, &v, 16); return; }
            const int64_t v64 = static_cast<int64_t>(v);
            fixed(DRAKEN_DECIMAL, &v64, 8);
            return;
        }
        if (null) {
            switch (t->kind) {
                case Kind::Boolean: { const uint8_t z = 0; fixed(DRAKEN_BOOL, &z, 1); return; }
                case Kind::Int: { const int32_t z = 0; fixed(t->logical == Logical::Date ? DRAKEN_DATE32 : DRAKEN_INT32, &z, 4); return; }
                case Kind::Long: { const int64_t z = 0; fixed(DRAKEN_INT64, &z, 8); return; }
                case Kind::Float: { const float z = 0; fixed(DRAKEN_FLOAT32, &z, 4); return; }
                case Kind::Double: { const double z = 0; fixed(DRAKEN_FLOAT64, &z, 8); return; }
                case Kind::String: case Kind::Enum: text(DRAKEN_VARCHAR, ""); return;
                case Kind::Bytes: case Kind::Fixed: text(DRAKEN_VARBINARY, ""); return;
                case Kind::Record: case Kind::Map: text(DRAKEN_NVARCHAR, ""); return;
                case Kind::Array:
                    if (is_json(t)) { text(DRAKEN_NVARCHAR, ""); return; }
                    fail("column '" + name + "' is an ARRAY not in the file; an absent ARRAY is refused (as for parquet)");
                default: fail("column '" + name + "': unexpected " + kind_name(t->kind));
            }
        }
        const DefaultValue d(f.default_json, name);
        switch (t->kind) {
            case Kind::Boolean: { const uint8_t b = d.boolean() ? 1 : 0; fixed(DRAKEN_BOOL, &b, 1); return; }
            case Kind::Int: {
                const int32_t v = static_cast<int32_t>(d.integer(INT32_MIN, INT32_MAX));
                fixed(t->logical == Logical::Date ? DRAKEN_DATE32 : DRAKEN_INT32, &v, 4);
                return;
            }
            case Kind::Long: { const int64_t v = d.integer(INT64_MIN, INT64_MAX); fixed(DRAKEN_INT64, &v, 8); return; }
            case Kind::Float: { const float v = static_cast<float>(d.real()); fixed(DRAKEN_FLOAT32, &v, 4); return; }
            case Kind::Double: { const double v = d.real(); fixed(DRAKEN_FLOAT64, &v, 8); return; }
            case Kind::String: text(DRAKEN_VARCHAR, d.text()); return;
            case Kind::Bytes: text(DRAKEN_VARBINARY, d.bytes()); return;
            case Kind::Fixed: {
                const std::string b = d.bytes();
                if (b.size() != t->fixed_size) fail("column '" + name + "': its default is not " + std::to_string(t->fixed_size) + " bytes");
                text(DRAKEN_VARBINARY, b);
                return;
            }
            case Kind::Enum: {
                const std::string s = d.text();
                bool found = false;
                for (const std::string& x : t->symbols) found |= (x == s);
                if (!found) fail("column '" + name + "': its default is not one of its symbols");
                text(DRAKEN_VARCHAR, s);
                return;
            }
            default:
                fail("column '" + name + "': a " + std::string(kind_name(t->kind)) + " default is not supported");
        }
    }

    void emit_skip(const Node* t) {
        switch (t->kind) {
            case Kind::Null: return;
            case Kind::Boolean: push(Op{S_BOOL, 0, 0, 0, 0}); return;
            case Kind::Int:
            case Kind::Long:
            case Kind::Enum: push(Op{S_VARINT, 0, 0, 0, 0}); return;
            case Kind::Float: skip_fixed(4); return;
            case Kind::Double: skip_fixed(8); return;
            case Kind::Fixed: skip_fixed(t->fixed_size); return;
            case Kind::Bytes:
            case Kind::String: push(Op{S_BYTES, 0, 0, 0, 0}); return;
            case Kind::Record:
                for (const Field& f : t->fields) emit_skip(f.type);
                return;
            case Kind::Array: {
                const size_t at = open(Op{S_ARRAY, 0, 0, 0, 0});
                emit_skip(t->items);
                close(at);
                return;
            }
            case Kind::Map: {
                const size_t at = open(Op{S_MAP, 0, 0, 0, 0});
                emit_skip(t->values);
                close(at);
                return;
            }
            case Kind::Union: {
                const size_t at = open(Op{OPT, t->null_branch, 0, 0, 0});
                emit_skip(t->value_branch());
                close(at);
                return;
            }
        }
    }
};

// ── per-batch sinks ─────────────────────────────────────────────────────────

constexpr size_t pad8(size_t bytes) { return (bytes + 7) & ~size_t(7); }

struct Sink {
    const ColSpec*                     spec;
    draken::AppendBuffer<uint8_t>      data;      // fixed-width rows, or BOOL bits
    draken::AppendBuffer<DrakenStringSlot> slots; // String
    draken::AppendBuffer<uint8_t>      arena;     // String long-form payloads
    draken::AppendBuffer<uint32_t>     codes;     // Dict
    draken::AppendBuffer<int32_t>      offsets;   // Array
    draken::AppendBuffer<uint8_t>      validity;  // nullable only; starts all-valid
    uint32_t                           nulls = 0;
    uint32_t                           n = 0;     // hidden (ARRAY child): elements so far
};

class Decoder {
public:
    Decoder(const Program& p) : p_(p) {}

    void begin_batch() {
        sinks_.clear();
        sinks_.resize(p_.cols.size());
        for (size_t i = 0; i < sinks_.size(); ++i) {
            sinks_[i].spec = &p_.cols[i];
            if (p_.cols[i].kind == OutKind::Array) sinks_[i].offsets.push_back(0);
        }
        rows_ = 0;
    }
    uint32_t rows() const { return rows_; }

    // Decode one block's `count` records from [p, end). The block must be consumed exactly.
    void block(const uint8_t* p, size_t n, uint32_t count) {
        const uint32_t base = rows_;
        const uint32_t total = base + count;
        for (Sink& s : sinks_)
            if (!s.spec->hidden) grow(s, total);
        Cursor c{p, p + n};
        const Op* ops = p_.ops.data();
        const Op* end = ops + p_.ops.size();
        for (uint32_t r = base; r < total; ++r) run(ops, end, c, r);
        if (c.p != c.end) fail("a block has trailing bytes after its records (the file is corrupt)");
        rows_ = total;
    }

    AvroBatch finish() {
        AvroBatch b;
        b.rows = rows_;
        b.columns.resize(p_.n_out);
        for (size_t i = 0; i < p_.n_out; ++i) finish_col(sinks_[i], rows_, b.columns[i]);
        sinks_.clear();
        return b;
    }

private:
    const Program& p_;
    std::vector<Sink> sinks_;
    uint32_t rows_ = 0;
    std::string json_;               // the JSON column value being rendered (one at a time)
    std::string* out_ = &json_;      // where J_* ops write: json_, or a J_SLOT's slot
    std::vector<std::vector<std::string>> frames_;  // J_REC_BEGIN .. J_REC_END, nested
    size_t depth_ = 0;

    static void grow(Sink& s, uint32_t total) {
        const ColSpec& cs = *s.spec;
        switch (cs.kind) {
            case OutKind::String: s.slots.resize_uninit(total); break;
            case OutKind::Dict:   s.codes.resize_uninit(total); break;
            case OutKind::Array:  s.offsets.resize_uninit(static_cast<size_t>(total) + 1); break;
            case OutKind::ConstRaw:
            case OutKind::ConstString: break;  // one value; only validity is per row
            default:
                if (cs.type == DRAKEN_BOOL) s.data.resize_fill(pad8((total + 7) / 8), 0);
                else s.data.resize_uninit(static_cast<size_t>(total) * cs.width);
        }
        if (cs.nullable) s.validity.resize_fill(pad8((total + 7) / 8), cs.const_null ? 0x00 : 0xFF);
    }

    void set_null(uint32_t col, uint32_t r) {
        Sink& s = sinks_[col];
        s.validity[r >> 3] &= static_cast<uint8_t>(~(1u << (r & 7)));
        ++s.nulls;
        const ColSpec& cs = *s.spec;
        switch (cs.kind) {
            case OutKind::String: str_init_null(&s.slots[r]); break;
            case OutKind::Dict:   s.codes[r] = 0; break;
            case OutKind::Array:  s.offsets[r + 1] = s.offsets[r]; break;
            case OutKind::ConstRaw:
            case OutKind::ConstString: break;
            default:
                if (cs.type != DRAKEN_BOOL) std::memset(s.data.data() + static_cast<size_t>(r) * cs.width, 0, cs.width);
        }
    }

    template <typename T>
    T* at(uint32_t col, uint32_t r) { return reinterpret_cast<T*>(sinks_[col].data.data()) + r; }

    static void put_bytes(Sink& s, uint32_t r, const uint8_t* b, uint64_t n) {
        if (n <= STR_INLINE_MAX) {
            str_init_inline(&s.slots[r], b, static_cast<uint32_t>(n));
            return;
        }
        const size_t off = s.arena.size();
        if (off + n > UINT32_MAX) fail("a batch's string bytes exceed 4 GiB");
        s.arena.append(b, n);
        draken_build_string_slot(&s.slots[r], b, static_cast<uint32_t>(n), static_cast<uint32_t>(off));
    }

    void b64(const uint8_t* p, uint64_t n) {
        std::string& o = *out_;
        o.push_back('"');
        const size_t at = o.size();
        o.resize(at + b64_encoded_size(n));  // includes bintob64's trailing NUL
        bintob64(&o[at], p, n);
        o.resize(at + b64_encoded_size(n) - 1);
        o.push_back('"');
    }

    void run(const Op* op, const Op* end, Cursor& c, uint32_t r) {
        while (op < end) {
            switch (op->code) {
                case R_BOOL:
                    if (read_bool(c)) sinks_[op->col].data[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
                    break;
                case R_INT32:  *at<int32_t>(op->col, r) = read_int(c); break;
                case R_INT64:  *at<int64_t>(op->col, r) = read_long(c); break;
                case R_FLOAT:  *at<float>(op->col, r) = read_float(c); break;
                case R_DOUBLE: *at<double>(op->col, r) = read_double(c); break;
                case R_INT_AS_I64: *at<int64_t>(op->col, r) = read_int(c); break;
                case R_INT_AS_F32: *at<float>(op->col, r) = static_cast<float>(read_int(c)); break;
                case R_INT_AS_F64: *at<double>(op->col, r) = static_cast<double>(read_int(c)); break;
                case R_LONG_AS_F32: *at<float>(op->col, r) = static_cast<float>(read_long(c)); break;
                case R_LONG_AS_F64: *at<double>(op->col, r) = static_cast<double>(read_long(c)); break;
                case R_FLOAT_AS_F64: *at<double>(op->col, r) = static_cast<double>(read_float(c)); break;
                case R_BYTES: {
                    const uint64_t n = read_length(c);
                    put_bytes(sinks_[op->col], r, c.p, n);
                    c.p += n;
                    break;
                }
                case R_FIXED:
                    need(c, op->arg);
                    put_bytes(sinks_[op->col], r, c.p, op->arg);
                    c.p += op->arg;
                    break;
                case R_ENUM: {
                    const int32_t i = read_int(c);
                    if (i < 0 || static_cast<uint32_t>(i) >= op->arg) fail("an enum index is out of range");
                    sinks_[op->col].codes[r] = static_cast<uint32_t>(i);
                    break;
                }
                case R_ENUM_MAP: {
                    const int32_t i = read_int(c);
                    if (i < 0 || static_cast<uint32_t>(i) >= op->arg) fail("an enum index is out of range");
                    sinks_[op->col].codes[r] = p_.enum_maps[op->sub + static_cast<uint32_t>(i)];
                    break;
                }
                case R_TIME_MS: *at<int64_t>(op->col, r) = static_cast<int64_t>(read_int(c)) * 1000; break;
                case R_TS_MS: {
                    int64_t us;
                    if (__builtin_mul_overflow(read_long(c), int64_t(1000), &us))
                        fail("a timestamp-millis value overflows microseconds");
                    *at<int64_t>(op->col, r) = us;
                    break;
                }
                case R_DEC_BYTES64:
                case R_DEC_BYTES128: {
                    const uint64_t n = read_length(c);
                    const __int128 v = checked_decimal(c.p, n, op->sub);
                    if (op->code == R_DEC_BYTES128) *at<__int128>(op->col, r) = v;
                    else *at<int64_t>(op->col, r) = static_cast<int64_t>(v);
                    c.p += n;
                    break;
                }
                case R_DEC_FIXED64:
                case R_DEC_FIXED128: {
                    need(c, op->arg);
                    const __int128 v = checked_decimal(c.p, op->arg, op->sub);
                    if (op->code == R_DEC_FIXED128) *at<__int128>(op->col, r) = v;
                    else *at<int64_t>(op->col, r) = static_cast<int64_t>(v);
                    c.p += op->arg;
                    break;
                }
                case R_ARRAY: {
                    Sink& child = sinks_[op->arg];
                    const Op* item = op + 1;
                    const Op* item_end = item + op->sub;
                    for (;;) {
                        int64_t n = read_long(c);
                        if (n == 0) break;
                        if (n < 0) { n = -n; read_long(c); }  // the byte size; items are read anyway
                        if (static_cast<uint64_t>(child.n) + static_cast<uint64_t>(n) > INT32_MAX)
                            fail("column '" + p_.cols[op->col].name + "': a batch holds more than 2^31 array elements");
                        const uint32_t to = child.n + static_cast<uint32_t>(n);
                        grow(child, to);
                        for (uint32_t e = child.n; e < to; ++e) run(item, item_end, c, e);
                        child.n = to;
                    }
                    sinks_[op->col].offsets[r + 1] = static_cast<int32_t>(child.n);
                    op += op->sub;
                    break;
                }
                case R_JSON: {
                    json_.clear();
                    out_ = &json_;
                    run(op + 1, op + 1 + op->sub, c, r);
                    put_bytes(sinks_[op->col], r, reinterpret_cast<const uint8_t*>(json_.data()), json_.size());
                    op += op->sub;
                    break;
                }
                case S_VARINT: skip_varint(c); break;
                case S_BOOL: need(c, 1); ++c.p; break;
                case S_FIXED: need(c, op->arg); c.p += op->arg; break;
                case S_BYTES: c.p += read_length(c); break;
                case S_ARRAY:
                case S_MAP: {
                    const Op* item = op + 1;
                    const Op* item_end = item + op->sub;
                    for (;;) {
                        int64_t n = read_long(c);
                        if (n == 0) break;
                        if (n < 0) {  // |n| items, then their byte size: skip them whole
                            c.p += read_length(c);
                            continue;
                        }
                        for (int64_t i = 0; i < n; ++i) {
                            if (op->code == S_MAP) c.p += read_length(c);  // the key
                            run(item, item_end, c, r);
                        }
                    }
                    op += op->sub;  // past the item program
                    break;
                }
                case OPT: {
                    const int64_t b = read_long(c);
                    if (b == op->null_branch) {
                        const uint32_t* nc = p_.null_cols.data() + op->col;
                        for (uint32_t k = 0; k < op->arg; ++k) set_null(nc[k], r);
                        op += op->sub;  // past the value program
                    } else if (b != 1 - op->null_branch) {
                        fail("a union branch index is out of range");
                    }
                    break;
                }
                // ── JSON ──
                case J_LIT: *out_ += p_.lits[op->arg]; break;
                case J_BOOL: *out_ += read_bool(c) ? "true" : "false"; break;
                case J_INT: rugo_text::fmt_int64(*out_, read_int(c)); break;
                case J_LONG: rugo_text::fmt_int64(*out_, read_long(c)); break;
                case J_FLOAT: {
                    const float v = draken::ops::fp_canon(read_float(c));
                    if (rugo_text::double_is_nan_or_inf(static_cast<double>(v))) *out_ += "null";
                    else rugo_text::fmt_float(*out_, v);
                    break;
                }
                case J_DOUBLE: {
                    const double v = draken::ops::fp_canon(read_double(c));
                    if (rugo_text::double_is_nan_or_inf(v)) *out_ += "null";
                    else rugo_text::fmt_double(*out_, v);
                    break;
                }
                case J_STR: {
                    const uint64_t n = read_length(c);
                    rugo_text::json_string(*out_, reinterpret_cast<const char*>(c.p), n);
                    c.p += n;
                    break;
                }
                case J_B64: {
                    const uint64_t n = read_length(c);
                    b64(c.p, n);
                    c.p += n;
                    break;
                }
                case J_FIXED_B64: need(c, op->arg); b64(c.p, op->arg); c.p += op->arg; break;
                case J_ENUM: {
                    const int32_t i = read_int(c);
                    if (i < 0 || static_cast<uint32_t>(i) >= op->col) fail("an enum index is out of range");
                    *out_ += p_.lits[op->arg + static_cast<uint32_t>(i)];
                    break;
                }
                case J_DATE: rugo_text::fmt_date_quoted(*out_, read_int(c)); break;
                case J_TIME_MS: rugo_text::fmt_time_quoted(*out_, read_int(c), rugo_text::U_MS); break;
                case J_TIME_US: rugo_text::fmt_time_quoted(*out_, read_long(c), rugo_text::U_US); break;
                case J_TS_MS: rugo_text::fmt_timestamp_quoted(*out_, read_long(c), rugo_text::U_MS); break;
                case J_TS_US: rugo_text::fmt_timestamp_quoted(*out_, read_long(c), rugo_text::U_US); break;
                case J_DEC_BYTES: {
                    const uint64_t n = read_length(c);
                    rugo_text::fmt_decimal(*out_, checked_decimal(c.p, n, op->col), static_cast<int>(op->sub));
                    c.p += n;
                    break;
                }
                case J_DEC_FIXED:
                    need(c, op->arg);
                    rugo_text::fmt_decimal(*out_, checked_decimal(c.p, op->arg, op->col), static_cast<int>(op->sub));
                    c.p += op->arg;
                    break;
                case J_ARRAY:
                case J_MAP: {
                    const bool is_map = op->code == J_MAP;
                    const Op* item = op + 1;
                    const Op* item_end = item + op->sub;
                    out_->push_back(is_map ? '{' : '[');
                    bool first = true;
                    for (;;) {
                        int64_t n = read_long(c);
                        if (n == 0) break;
                        if (n < 0) { n = -n; read_long(c); }  // the byte size; items are rendered anyway
                        for (int64_t i = 0; i < n; ++i) {
                            if (!first) out_->push_back(',');
                            first = false;
                            if (is_map) {
                                const uint64_t kn = read_length(c);
                                rugo_text::json_string(*out_, reinterpret_cast<const char*>(c.p), kn);
                                c.p += kn;
                                out_->push_back(':');
                            }
                            run(item, item_end, c, r);
                        }
                    }
                    out_->push_back(is_map ? '}' : ']');
                    op += op->sub;
                    break;
                }
                case J_OPT: {
                    const int64_t b = read_long(c);
                    if (b == op->null_branch) {
                        *out_ += "null";
                        op += op->sub;
                    } else if (b != 1 - op->null_branch) {
                        fail("a union branch index is out of range");
                    }
                    break;
                }
                case J_REC_BEGIN: {
                    if (frames_.size() <= depth_) frames_.emplace_back();
                    std::vector<std::string>& f = frames_[depth_++];
                    f.resize(op->arg);
                    for (std::string& s : f) s.clear();
                    break;
                }
                case J_SLOT: {
                    std::string* saved = out_;
                    out_ = &frames_[depth_ - 1][op->arg];
                    run(op + 1, op + 1 + op->sub, c, r);
                    out_ = saved;
                    op += op->sub;
                    break;
                }
                case J_REC_END: {
                    const std::vector<std::string>& f = frames_[depth_ - 1];
                    const int64_t* t = p_.rec_table.data() + op->arg;
                    for (uint32_t i = 0; i < op->col; ++i) {
                        *out_ += p_.lits[static_cast<size_t>(t[2 * i])];
                        const int64_t src = t[2 * i + 1];
                        *out_ += src >= 0 ? f[static_cast<size_t>(src)] : p_.lits[static_cast<size_t>(~src)];
                    }
                    *out_ += '}';
                    --depth_;
                    break;
                }
            }
            ++op;
        }
    }

    // Validity: dropped when nothing was null, else the padding bits past `n` cleared.
    static uint8_t* take_validity(Sink& s, uint32_t n) {
        if (!s.spec->nullable || (s.nulls == 0 && !s.spec->const_null)) return nullptr;
        if (n & 7) s.validity[n >> 3] &= static_cast<uint8_t>((1u << (n & 7)) - 1);
        return s.validity.release();
    }

    static uint32_t* zero_codes(uint32_t n) {
        auto* z = static_cast<uint32_t*>(draken_calloc(n ? n : 1, sizeof(uint32_t)));
        if (z == nullptr) throw std::bad_alloc();
        return z;
    }

    void finish_col(Sink& s, uint32_t n, AvroColumn& out) {
        const ColSpec& cs = *s.spec;
        out.kind = cs.kind;
        out.type = cs.type;
        out.length = n;
        out.precision = cs.precision;
        out.scale = cs.scale;
        out.validity = take_validity(s, n);
        switch (cs.kind) {
            case OutKind::String:
                out.slots = s.slots.release();
                out.arena_len = s.arena.size();
                out.arena = s.arena.release();
                break;
            case OutKind::Dict: {
                const auto& sym = *cs.symbols;
                draken::AppendBuffer<DrakenStringSlot> dslots;
                draken::AppendBuffer<uint8_t> darena;
                dslots.resize_uninit(sym.size());
                for (size_t i = 0; i < sym.size(); ++i) {
                    const uint8_t* b = reinterpret_cast<const uint8_t*>(sym[i].data());
                    const uint32_t len = static_cast<uint32_t>(sym[i].size());
                    if (len <= STR_INLINE_MAX) { str_init_inline(&dslots[i], b, len); continue; }
                    const uint32_t off = static_cast<uint32_t>(darena.size());
                    darena.append(b, len);
                    draken_build_string_slot(&dslots[i], b, len, off);
                }
                out.dict_len = static_cast<uint32_t>(sym.size());
                out.slots = dslots.release();
                out.arena_len = darena.size();
                out.arena = darena.release();
                out.codes = s.codes.release();
                break;
            }
            case OutKind::Array: {
                Sink& ch = sinks_[cs.child];
                out.offsets = s.offsets.release();
                out.child_type = ch.spec->type;
                out.child_length = ch.n;
                out.child_validity = take_validity(ch, ch.n);
                if (ch.spec->kind == OutKind::String) {
                    out.slots = ch.slots.release();
                    out.arena_len = ch.arena.size();
                    out.arena = ch.arena.release();
                } else {
                    out.data = ch.data.release();
                }
                break;
            }
            case OutKind::ConstRaw: {
                // One value, positions all 0 (CLAUDE.md §11).
                auto* v = static_cast<uint8_t*>(draken_calloc(16, 1));
                if (v == nullptr) throw std::bad_alloc();
                std::memcpy(v, cs.const_value.data(), cs.const_value.size());
                if (cs.type == DRAKEN_BOOL) v[0] = v[0] ? 1 : 0;  // bit 0 of the packed value
                out.data = v;
                out.codes = zero_codes(n);
                break;
            }
            case OutKind::ConstString: {
                draken::AppendBuffer<DrakenStringSlot> slot;
                draken::AppendBuffer<uint8_t> arena;
                slot.resize_uninit(1);
                const auto* b = reinterpret_cast<const uint8_t*>(cs.const_value.data());
                const uint32_t len = static_cast<uint32_t>(cs.const_value.size());
                if (len <= STR_INLINE_MAX) str_init_inline(&slot[0], b, len);
                else { arena.append(b, len); draken_build_string_slot(&slot[0], b, len, 0); }
                out.slots = slot.release();
                out.arena_len = arena.size();
                out.arena = arena.release();
                out.codes = zero_codes(n);
                break;
            }
            default:
                out.data = s.data.release();
        }
    }
};

void parse_columns(const std::vector<std::string>& columns, Req& root, Program& p) {
    for (size_t i = 0; i < columns.size(); ++i) {
        const std::string& path = columns[i];
        if (path.empty()) fail("an empty column name");
        Req* node = &root;
        size_t start = 0;
        for (;;) {
            const size_t dot = path.find('.', start);
            const std::string part = path.substr(start, dot == std::string::npos ? std::string::npos : dot - start);
            if (part.empty()) fail("column '" + path + "' has an empty path segment");
            node = &node->kids[part];
            node->path = path.substr(0, dot);
            if (dot == std::string::npos) break;
            start = dot + 1;
        }
        if (node->col >= 0) fail("column '" + path + "' is selected twice");
        node->col = static_cast<int>(i);
        ColSpec cs;
        cs.name = path;
        p.cols.push_back(std::move(cs));
    }
    p.n_out = columns.size();
}

}  // namespace

void read_avro_header(const uint8_t* data, size_t size, AvroRead& out) {
    Header h = read_header(data, size);
    out.schema_json = std::move(h.schema_json);
    out.metadata = std::move(h.metadata);
}

// ── AvroStream ──────────────────────────────────────────────────────────────

struct AvroStream::Impl {
    const uint8_t* data;
    size_t size;
    Header header;
    Schema writer;
    Schema reader;
    std::vector<std::string> names;
    Program prog;
    std::unique_ptr<Decoder> dec;
    std::unique_ptr<BlockReader> blocks;
    draken::AppendBuffer<uint8_t> scratch;
    bool count_only = false;   // no columns: rows come from the block headers alone
    uint32_t counted = 0;      // count_only: rows in the batch being built
    Block pending;             // a block that did not fit the previous batch
    bool has_pending = false;

    uint32_t rows() const { return count_only ? counted : dec->rows(); }

    AvroBatch finish() {
        if (!count_only) {
            AvroBatch b = dec->finish();
            dec->begin_batch();
            return b;
        }
        AvroBatch b;
        b.rows = counted;
        counted = 0;
        return b;
    }
};

AvroStream::AvroStream(const uint8_t* data, size_t size, const std::vector<std::string>& columns,
                       bool all_columns, const std::string& reader_schema_json)
    : impl_(std::make_unique<Impl>()) {
    Impl& m = *impl_;
    m.data = data;
    m.size = size;
    m.header = read_header(data, size);
    m.writer = Schema::parse(m.header.schema_json.data(), m.header.schema_json.size());
    if (!reader_schema_json.empty()) m.reader = Schema::parse(reader_schema_json.data(), reader_schema_json.size());
    const Node* r = reader_schema_json.empty() ? m.writer.root() : m.reader.root();

    m.names = columns;
    if (all_columns) {
        if (!columns.empty()) fail("pass either columns or all_columns, not both");
        if (r->kind != Kind::Record) fail("the schema is not a record");
        for (const Field& f : r->fields) m.names.push_back(f.name);
    }
    m.count_only = m.names.empty();

    Req req;
    parse_columns(m.names, req, m.prog);
    Compiler(m.prog, reader_schema_json.empty() ? "the file's schema" : "the reader schema")
        .compile_root(m.writer.root(), r, req);
    m.dec = std::make_unique<Decoder>(m.prog);
    m.dec->begin_batch();
    m.blocks = std::make_unique<BlockReader>(data, size, m.header);
}

AvroStream::~AvroStream() = default;

const std::string& AvroStream::schema_json() const { return impl_->header.schema_json; }
const std::vector<std::pair<std::string, std::string>>& AvroStream::metadata() const { return impl_->header.metadata; }
const std::vector<std::string>& AvroStream::column_names() const { return impl_->names; }

std::vector<AvroColumnType> AvroStream::column_types() const {
    const Program& p = impl_->prog;
    std::vector<AvroColumnType> out(p.n_out);
    for (size_t i = 0; i < p.n_out; ++i) {
        const ColSpec& cs = p.cols[i];
        AvroColumnType& t = out[i];
        t.type = cs.type;
        if (cs.type == DRAKEN_TIMESTAMP64) t.logical_kind = 1;
        else if (cs.type == DRAKEN_TIME64) t.logical_kind = 2;
        else if (cs.type == DRAKEN_DECIMAL || cs.type == DRAKEN_DECIMAL128) {
            t.logical_kind = 3;
            t.precision = cs.precision;
            t.scale = cs.scale;
        }
        if (cs.kind == OutKind::Array) t.child_type = p.cols[cs.child].type;
    }
    return out;
}

bool AvroStream::next(AvroBatch& out) {
    Impl& m = *impl_;
    for (;;) {
        Block b;
        if (m.has_pending) {
            b = m.pending;
            m.has_pending = false;
        } else if (!m.blocks->next(b)) {
            if (m.rows() == 0) return false;
            out = m.finish();
            return true;
        }
        if (b.count > static_cast<int64_t>(UINT32_MAX)) fail("a block holds more than 2^32 records");
        const uint32_t count = static_cast<uint32_t>(b.count);
        if (count == 0) continue;
        if (m.rows() > 0 && static_cast<uint64_t>(m.rows()) + count > kBatchRows) {
            // This block starts the next batch; the one built so far is done.
            m.pending = b;
            m.has_pending = true;
            out = m.finish();
            return true;
        }
        if (static_cast<uint64_t>(m.rows()) + count > UINT32_MAX) fail("a batch exceeds 2^32 rows");
        if (m.count_only) {
            m.counted += count;
            continue;
        }
        const auto payload = block_payload(b, m.header.codec, m.scratch);
        m.dec->block(payload.first, payload.second, count);
    }
}

void read_avro_buffer(const uint8_t* data, size_t size, const std::vector<std::string>& columns,
                      bool all_columns, const std::string& reader_schema_json, AvroRead& out) {
    AvroStream stream(data, size, columns, all_columns, reader_schema_json);
    out.schema_json = stream.schema_json();
    out.metadata = stream.metadata();
    out.column_names = stream.column_names();
    out.batches.clear();
    AvroBatch batch;
    while (stream.next(batch)) out.batches.push_back(std::move(batch));
}

}  // namespace rugo::avro
