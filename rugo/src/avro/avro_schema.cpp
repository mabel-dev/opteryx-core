#include "avro_schema.hpp"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_set>

#include "yyjson.h"

namespace rugo::avro {

const char* kind_name(Kind k) {
    switch (k) {
        case Kind::Null:    return "null";
        case Kind::Boolean: return "boolean";
        case Kind::Int:     return "int";
        case Kind::Long:    return "long";
        case Kind::Float:   return "float";
        case Kind::Double:  return "double";
        case Kind::Bytes:   return "bytes";
        case Kind::String:  return "string";
        case Kind::Record:  return "record";
        case Kind::Enum:    return "enum";
        case Kind::Array:   return "array";
        case Kind::Map:     return "map";
        case Kind::Union:   return "union";
        case Kind::Fixed:   return "fixed";
    }
    return "?";
}

namespace {

// Largest decimal precision a two's-complement fixed(n) can hold, n = 1..16.
constexpr uint8_t kFixedMaxPrecision[17] = {0, 2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38};

[[noreturn]] void fail(const std::string& path, const std::string& msg) {
    throw std::runtime_error("Avro schema" + (path.empty() ? std::string() : " at '" + path + "'") + ": " + msg);
}

std::string str_of(yyjson_val* v) { return std::string(yyjson_get_str(v), yyjson_get_len(v)); }

bool primitive_kind(const std::string& s, Kind& out) {
    if (s == "null")    { out = Kind::Null;    return true; }
    if (s == "boolean") { out = Kind::Boolean; return true; }
    if (s == "int")     { out = Kind::Int;     return true; }
    if (s == "long")    { out = Kind::Long;    return true; }
    if (s == "float")   { out = Kind::Float;   return true; }
    if (s == "double")  { out = Kind::Double;  return true; }
    if (s == "bytes")   { out = Kind::Bytes;   return true; }
    if (s == "string")  { out = Kind::String;  return true; }
    return false;
}

}  // namespace

struct SchemaParser {
    Schema& s;
    std::unordered_set<std::string> in_progress;  // named types whose definition is open

    Node& make() { s.nodes_.emplace_back(); return s.nodes_.back(); }

    std::string full_name(yyjson_val* obj, const std::string& enclosing_ns, const std::string& path,
                          std::string& ns_out) {
        yyjson_val* name = yyjson_obj_get(obj, "name");
        if (!yyjson_is_str(name)) fail(path, "a named type has no string 'name'");
        std::string n = str_of(name);
        if (n.find('.') != std::string::npos) {
            ns_out = n.substr(0, n.rfind('.'));
            return n;
        }
        yyjson_val* ns = yyjson_obj_get(obj, "namespace");
        if (ns != nullptr && !yyjson_is_null(ns)) {
            if (!yyjson_is_str(ns)) fail(path, "'namespace' is not a string");
            ns_out = str_of(ns);
        } else {
            ns_out = enclosing_ns;
        }
        return ns_out.empty() ? n : ns_out + "." + n;
    }

    void register_named(const std::string& fname, const Node* node, const std::string& path) {
        if (!s.named_.emplace(fname, node).second) fail(path, "named type '" + fname + "' is defined twice");
    }

    const Node* reference(const std::string& name, const std::string& ns, const std::string& path) {
        std::string candidates[2];
        size_t nc = 0;
        if (name.find('.') == std::string::npos && !ns.empty()) candidates[nc++] = ns + "." + name;
        candidates[nc++] = name;
        for (size_t i = 0; i < nc; ++i) {
            if (in_progress.count(candidates[i]))
                fail(path, "recursive type '" + candidates[i] + "' is not supported");
            auto it = s.named_.find(candidates[i]);
            if (it != s.named_.end()) return it->second;
        }
        fail(path, "unknown type '" + name + "'");
    }

    void apply_logical(yyjson_val* obj, Node& n, const std::string& path) {
        yyjson_val* lt = yyjson_obj_get(obj, "logicalType");
        if (lt == nullptr) return;
        if (!yyjson_is_str(lt)) fail(path, "'logicalType' is not a string");
        const std::string l = str_of(lt);
        auto need = [&](bool ok) {
            if (!ok) fail(path, "logicalType '" + l + "' is not valid on " + kind_name(n.kind));
        };
        if (l == "date")                  { need(n.kind == Kind::Int);  n.logical = Logical::Date; }
        else if (l == "time-millis")      { need(n.kind == Kind::Int);  n.logical = Logical::TimeMillis; }
        else if (l == "time-micros")      { need(n.kind == Kind::Long); n.logical = Logical::TimeMicros; }
        else if (l == "timestamp-millis") { need(n.kind == Kind::Long); n.logical = Logical::TimestampMillis; }
        else if (l == "timestamp-micros") { need(n.kind == Kind::Long); n.logical = Logical::TimestampMicros; }
        else if (l == "uuid") {
            need(n.kind == Kind::String || (n.kind == Kind::Fixed && n.fixed_size == 16));
            n.logical = Logical::Uuid;
        } else if (l == "decimal") {
            need(n.kind == Kind::Bytes || n.kind == Kind::Fixed);
            yyjson_val* p = yyjson_obj_get(obj, "precision");
            yyjson_val* sc = yyjson_obj_get(obj, "scale");
            if (!yyjson_is_int(p)) fail(path, "decimal has no integer 'precision'");
            const int64_t prec = yyjson_get_sint(p);
            int64_t scale = 0;
            if (sc != nullptr) {
                if (!yyjson_is_int(sc)) fail(path, "decimal 'scale' is not an integer");
                scale = yyjson_get_sint(sc);
            }
            if (prec < 1) fail(path, "decimal precision must be at least 1");
            if (prec > 38) fail(path, "decimal precision " + std::to_string(prec) + " is wider than 38, which is not supported");
            if (scale < 0 || scale > prec) fail(path, "decimal scale " + std::to_string(scale) + " is outside 0..precision");
            if (n.kind == Kind::Fixed && n.fixed_size <= 16 && prec > kFixedMaxPrecision[n.fixed_size])
                fail(path, "decimal precision " + std::to_string(prec) + " does not fit fixed(" +
                               std::to_string(n.fixed_size) + ")");
            n.logical = Logical::Decimal;
            n.precision = static_cast<uint8_t>(prec);
            n.scale = static_cast<uint8_t>(scale);
        } else if (l == "timestamp-nanos" || l == "local-timestamp-millis" || l == "local-timestamp-micros" ||
                   l == "local-timestamp-nanos" || l == "duration" || l == "big-decimal") {
            fail(path, "logicalType '" + l + "' is not supported");
        }
        // Any other logicalType: the spec says use the underlying type.
    }

    const Node* parse(yyjson_val* v, const std::string& ns, const std::string& path) {
        if (yyjson_is_str(v)) {
            const std::string name = str_of(v);
            Kind k;
            if (primitive_kind(name, k)) {
                Node& n = make();
                n.kind = k;
                return &n;
            }
            return reference(name, ns, path);
        }
        if (yyjson_is_arr(v)) return parse_union(v, ns, path);
        if (!yyjson_is_obj(v)) fail(path, "a type must be a string, an array or an object");

        yyjson_val* t = yyjson_obj_get(v, "type");
        if (t == nullptr) fail(path, "a type object has no 'type'");
        if (!yyjson_is_str(t)) {
            // {"type": {...}} / {"type": [...]}: a wrapped type.
            return parse(t, ns, path);
        }
        const std::string type = str_of(t);
        Kind k;
        if (primitive_kind(type, k)) {
            Node& n = make();
            n.kind = k;
            apply_logical(v, n, path);
            return &n;
        }
        if (type == "record" || type == "error") return parse_record(v, ns, path);
        if (type == "enum") {
            std::string inner_ns;
            const std::string fname = full_name(v, ns, path, inner_ns);
            Node& n = make();
            n.kind = Kind::Enum;
            n.full_name = fname;
            yyjson_val* syms = yyjson_obj_get(v, "symbols");
            if (!yyjson_is_arr(syms) || yyjson_arr_size(syms) == 0)
                fail(path, "enum '" + fname + "' has no 'symbols'");
            size_t i, max;
            yyjson_val* sym;
            std::unordered_set<std::string> seen;
            yyjson_arr_foreach(syms, i, max, sym) {
                if (!yyjson_is_str(sym)) fail(path, "enum symbol is not a string");
                std::string s_ = str_of(sym);
                if (!seen.insert(s_).second) fail(path, "enum symbol '" + s_ + "' is repeated");
                n.symbols.push_back(std::move(s_));
            }
            register_named(fname, &n, path);
            return &n;
        }
        if (type == "fixed") {
            std::string inner_ns;
            const std::string fname = full_name(v, ns, path, inner_ns);
            Node& n = make();
            n.kind = Kind::Fixed;
            n.full_name = fname;
            yyjson_val* size = yyjson_obj_get(v, "size");
            if (!yyjson_is_int(size) || yyjson_get_sint(size) < 0 || yyjson_get_sint(size) > 0x7FFFFFFF)
                fail(path, "fixed '" + fname + "' has no valid 'size'");
            n.fixed_size = static_cast<uint32_t>(yyjson_get_sint(size));
            apply_logical(v, n, path);
            register_named(fname, &n, path);
            return &n;
        }
        if (type == "array") {
            yyjson_val* items = yyjson_obj_get(v, "items");
            if (items == nullptr) fail(path, "array has no 'items'");
            Node& n = make();
            n.kind = Kind::Array;
            n.items = parse(items, ns, path + "[]");
            return &n;
        }
        if (type == "map") {
            yyjson_val* values = yyjson_obj_get(v, "values");
            if (values == nullptr) fail(path, "map has no 'values'");
            Node& n = make();
            n.kind = Kind::Map;
            n.values = parse(values, ns, path + "{}");
            return &n;
        }
        // A named-type reference written as {"type": "Name"}.
        return reference(type, ns, path);
    }

    const Node* parse_union(yyjson_val* arr, const std::string& ns, const std::string& path) {
        if (yyjson_arr_size(arr) != 2)
            fail(path, "a union of " + std::to_string(yyjson_arr_size(arr)) +
                           " branches is not supported (only [\"null\", T])");
        Node& n = make();
        n.kind = Kind::Union;
        size_t i, max;
        yyjson_val* b;
        yyjson_arr_foreach(arr, i, max, b) { n.branches.push_back(parse(b, ns, path)); }
        const bool null0 = n.branches[0]->kind == Kind::Null;
        const bool null1 = n.branches[1]->kind == Kind::Null;
        if (null0 == null1)
            fail(path, null0 ? "a union of two nulls is not valid"
                             : "a union without a null branch is not supported (only [\"null\", T])");
        n.null_branch = null0 ? 0 : 1;
        if (n.value_branch()->kind == Kind::Union) fail(path, "a union directly inside a union is not valid");
        return &n;
    }

    const Node* parse_record(yyjson_val* v, const std::string& ns, const std::string& path) {
        std::string inner_ns;
        const std::string fname = full_name(v, ns, path, inner_ns);
        if (s.named_.count(fname)) fail(path, "named type '" + fname + "' is defined twice");
        Node& n = make();
        n.kind = Kind::Record;
        n.full_name = fname;
        yyjson_val* fields = yyjson_obj_get(v, "fields");
        if (!yyjson_is_arr(fields)) fail(path, "record '" + fname + "' has no 'fields' array");
        in_progress.insert(fname);
        std::unordered_set<std::string> seen;
        size_t i, max;
        yyjson_val* f;
        yyjson_arr_foreach(fields, i, max, f) {
            if (!yyjson_is_obj(f)) fail(path, "a record field is not an object");
            yyjson_val* fn = yyjson_obj_get(f, "name");
            if (!yyjson_is_str(fn)) fail(path, "a record field has no string 'name'");
            Field field;
            field.name = str_of(fn);
            if (!seen.insert(field.name).second) fail(path, "field '" + field.name + "' is repeated");
            const std::string fpath = path.empty() ? field.name : path + "." + field.name;
            yyjson_val* ft = yyjson_obj_get(f, "type");
            if (ft == nullptr) fail(fpath, "field has no 'type'");
            yyjson_val* fid = yyjson_obj_get(f, "field-id");
            if (fid != nullptr) {
                if (!yyjson_is_int(fid)) fail(fpath, "'field-id' is not an integer");
                field.field_id = yyjson_get_sint(fid);
            }
            yyjson_val* def = yyjson_obj_get(f, "default");
            if (def != nullptr) {
                size_t dlen = 0;
                char* text = yyjson_val_write(def, 0, &dlen);
                if (text == nullptr) fail(fpath, "the 'default' value cannot be serialised");
                field.has_default = true;
                field.default_json.assign(text, dlen);
                free(text);
            }
            field.type = parse(ft, inner_ns, fpath);
            n.fields.push_back(std::move(field));
        }
        in_progress.erase(fname);
        register_named(fname, &n, path);
        return &n;
    }
};

Schema Schema::parse(const char* json, size_t len) {
    std::unique_ptr<yyjson_doc, void (*)(yyjson_doc*)> doc(
        yyjson_read(json, len, 0), [](yyjson_doc* d) { yyjson_doc_free(d); });
    if (!doc) throw std::runtime_error("Avro schema: the schema is not valid JSON");
    Schema s;
    SchemaParser p{s, {}};
    s.root_ = p.parse(yyjson_doc_get_root(doc.get()), "", "");
    return s;
}

}  // namespace rugo::avro
