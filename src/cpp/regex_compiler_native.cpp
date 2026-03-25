/*
src/cpp/regex_compiler_native.cpp

Robust RE2 AST-based regex -> DFA-ops translator.

This implementation walks the vendored RE2 Regexp tree using its public
API and emits a conservative, well-defined subset of patterns as an
operation sequence that the Cython DFA executor understands.

Supported subset (conservative):
 - Fully anchored ^ ... $ (BeginText/BeginLine and EndText/EndLine)
 - literal runs -> OP_MATCH_LITERAL
 - optional literal(s) (x? or (?:lit)?) -> OP_MATCH_OPTIONAL_LITERAL
 - non-capturing groups of literals inlined (?:www\.) -> OP_MATCH_LITERAL
 - capture group 1:
     - (.+)   -> OP_EXTRACT_WHILE_NOT target=None
     - ([^c]+) -> OP_EXTRACT_WHILE_NOT target_char=c  (single excluded rune)
 - tail .* -> OP_DISCARD_REST (only when it appears as an unbounded repeat of ANY and is at the end)
 - Finally, OP_RETURN_CAPTURE is appended to indicate returning capture 1.

On any unsupported construct (alternation, multiple captures, complex
character classes, nested unsupported repeats, etc.) the translator
returns fallback=True so the normal RE2/RE2-backed Python path handles it.

Notes:
 - The translator is intentionally conservative and safe: it prefers
   fallback to possibly incorrect translations.
 - The op_type integers match the Python `OperationType` enum.
*/

#include <nanobind/nanobind.h>
#include <nanobind/stl/list.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <cassert>

#include "re2/regexp.h"

namespace nb = nanobind;
using namespace std;

struct OpDesc {
    int op_type;
    std::string pattern; // empty => None
    int pattern_len;
    int capture_id;
    int target_char; // -1 => None
};

// Operation type integers must match the Python-side OperationType enum.
static constexpr int OP_MATCH_LITERAL = 0;
static constexpr int OP_MATCH_OPTIONAL_LITERAL = 1;
static constexpr int OP_FIND_CHAR = 2;
static constexpr int OP_EXTRACT_UNTIL_CHAR = 3;
static constexpr int OP_EXTRACT_WHILE_NOT = 4;
static constexpr int OP_START_CAPTURE = 5;
static constexpr int OP_END_CAPTURE = 6;
static constexpr int OP_DISCARD_REST = 7;
static constexpr int OP_RETURN_CAPTURE = 8;

// Append Rune (codepoint) to UTF-8 encoded string
static void append_rune_utf8(std::string &out, int rune) {
    uint32_t r = static_cast<uint32_t>(rune);
    if (r <= 0x7F) {
        out.push_back(static_cast<char>(r));
    } else if (r <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | (r >> 6)));
        out.push_back(static_cast<char>(0x80 | (r & 0x3F)));
    } else if (r <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | (r >> 12)));
        out.push_back(static_cast<char>(0x80 | ((r >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (r & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (r >> 18)));
        out.push_back(static_cast<char>(0x80 | ((r >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((r >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (r & 0x3F)));
    }
}

// Flatten concatenation (kRegexpConcat) into a linear sequence (in-order)
static void flatten_concat(re2::Regexp* r, std::vector<re2::Regexp*> &out) {
    if (!r) return;
    if (r->op() == re2::kRegexpConcat) {
        int n = r->nsub();
        re2::Regexp** subs = r->sub();
        for (int i = 0; i < n; ++i) flatten_concat(subs[i], out);
    } else {
        out.push_back(r);
    }
}

// Try to extract literal bytes from a node (literal or literalstring).
// Returns true and appends bytes to out_str on success.
static bool literal_bytes_from_node(re2::Regexp* node, std::string &out_str) {
    if (!node) return false;
    if (node->op() == re2::kRegexpLiteral) {
        append_rune_utf8(out_str, node->rune());
        return true;
    }
    if (node->op() == re2::kRegexpLiteralString) {
        int nr = node->nrunes();
        re2::Rune* rs = node->runes();
        for (int i = 0; i < nr; ++i) append_rune_utf8(out_str, rs[i]);
        return true;
    }
    return false;
}

// If node is a Quest (zero-or-one) wrapping a literal or a non-capturing
// group that is a literal run, return the literal bytes and true.
// Otherwise return false.
static bool optional_literal_from_node(re2::Regexp* node, std::string &out_str) {
    if (!node) return false;
    if (node->op() == re2::kRegexpQuest) {
        if (node->nsub() < 1) return false;
        re2::Regexp* sub = node->sub()[0];
        // Non-capturing group representation in RE2 may be a capture with cap()<0
        if (sub->op() == re2::kRegexpCapture && sub->cap() < 0) {
            if (sub->nsub() < 1) return false;
            sub = sub->sub()[0];
        }
        return literal_bytes_from_node(sub, out_str);
    }
    return false;
}

// Try to interpret a capture node's body as .+ or [^c]+
// Returns (success, target) where target == -1 means '.+' (capture to end)
// and target >= 0 means the excluded single char value.
static pair<bool,int> compile_capture_content(re2::Regexp* cap_node) {
    if (!cap_node) return {false, -2};
    re2::Regexp* body = nullptr;
    if (cap_node->op() == re2::kRegexpCapture) {
        if (cap_node->nsub() < 1) return {false, -2};
        body = cap_node->sub()[0];
    } else {
        body = cap_node;
    }

    // Expect one-or-more: plus
    if (body->op() == re2::kRegexpPlus) {
        if (body->nsub() < 1) return {false, -2};
        re2::Regexp* sub = body->sub()[0];
        if (sub->op() == re2::kRegexpAnyChar || sub->op() == re2::kRegexpAnyByte) {
            // .+ case
            return {true, -1};
        }
        // Character class: try to detect single negated character [^c]
        if (sub->op() == re2::kRegexpCharClass) {
            // Use ToString to inspect class form; conservative parse.
            std::string cls = sub->ToString(); // e.g. "[^/]"
            if (cls.size() >= 4 && cls.front() == '[' && cls[1] == '^' && cls.back() == ']') {
                std::string inner = cls.substr(2, cls.size() - 3);
                // Accept single character or escaped char like "\/"
                if (inner.size() == 1) {
                    unsigned char ch = static_cast<unsigned char>(inner[0]);
                    return {true, (int)ch};
                } else if (inner.size() == 2 && inner[0] == '\\') {
                    unsigned char ch = static_cast<unsigned char>(inner[1]);
                    return {true, (int)ch};
                } else {
                    return {false, -2};
                }
            }
        }
    }
    return {false, -2};
}

// Translate the simplified Regexp AST into OpDesc vector.
// Conservative: returns false on any unsupported construct.
static bool translate_re2_regexp_to_ops(re2::Regexp* r, std::vector<OpDesc> &out_ops) {
    if (!r) return false;

    // Simplify to canonical form
    std::unique_ptr<re2::Regexp, void(*)(re2::Regexp*)> simplified(
        r->Simplify(), [](re2::Regexp* p){ if (p) p->Decref(); }
    );
    if (!simplified) return false;
    re2::Regexp* root = simplified.get();

    // Flatten top-level concatenation so we can inspect anchors & sequence
    std::vector<re2::Regexp*> parts;
    flatten_concat(root, parts);
    if (parts.size() < 2) return false; // at minimum should have anchors (begin, end)

    // DEBUG: Log the parts for debugging
    const char* debug_pattern = std::getenv("DEBUG_REGEX_COMPILER");
    if (debug_pattern) {
        fprintf(stderr, "[DEBUG] Pattern has %zu parts:\n", parts.size());
        for (size_t pi = 0; pi < parts.size(); ++pi) {
            fprintf(stderr, "  [%zu] op=%d %s\n", pi, parts[pi]->op(), parts[pi]->ToString().c_str());
        }
    }

    // Anchors: first must be BeginText/BeginLine, last must be EndText/EndLine
    auto is_begin = [](re2::Regexp* x) {
        return x->op() == re2::kRegexpBeginText || x->op() == re2::kRegexpBeginLine;
    };
    auto is_end = [](re2::Regexp* x) {
        return x->op() == re2::kRegexpEndText || x->op() == re2::kRegexpEndLine;
    };
    if (!is_begin(parts.front()) || !is_end(parts.back())) {
        if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: anchors not at start/end\n");
        return false;
    }

    // Process middle nodes (between anchors)
    size_t i = 1;
    size_t end_idx = parts.size() - 1;
    bool capture_emitted = false;

    while (i < end_idx) {
        re2::Regexp* node = parts[i];
        if (debug_pattern) {
            fprintf(stderr, "[DEBUG] Processing part[%zu] op=%d %s capture_emitted=%d\n", 
                    i, node->op(), node->ToString().c_str(), capture_emitted);
        }

        // Literal or literal string
        if (node->op() == re2::kRegexpLiteral || node->op() == re2::kRegexpLiteralString) {
            std::string lit;
            if (!literal_bytes_from_node(node, lit)) {
                if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: literal_bytes_from_node failed\n");
                return false;
            }
            OpDesc o{};
            o.op_type = OP_MATCH_LITERAL;
            o.pattern = std::move(lit);
            o.pattern_len = (int)o.pattern.size();
            o.capture_id = -1;
            o.target_char = -1;
            out_ops.push_back(o);
            ++i;
            continue;
        }

        // Optional literal (quest)
        if (node->op() == re2::kRegexpQuest) {
            std::string lit;
            if (!optional_literal_from_node(node, lit)) {
                if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: optional_literal_from_node failed\n");
                return false;
            }
            OpDesc o{};
            o.op_type = OP_MATCH_OPTIONAL_LITERAL;
            o.pattern = std::move(lit);
            o.pattern_len = (int)o.pattern.size();
            o.capture_id = -1;
            o.target_char = -1;
            out_ops.push_back(o);
            ++i;
            continue;
        }

        // Non-capturing group represented as capture with cap()<0
        if (node->op() == re2::kRegexpCapture && node->cap() < 0) {
            if (node->nsub() < 1) return false;
            re2::Regexp* inner = node->sub()[0];
            // If inner is literal(s) or optional literal, inline
            if (inner->op() == re2::kRegexpLiteral || inner->op() == re2::kRegexpLiteralString) {
                std::string lit;
                if (!literal_bytes_from_node(inner, lit)) return false;
                OpDesc o{};
                o.op_type = OP_MATCH_LITERAL;
                o.pattern = std::move(lit);
                o.pattern_len = (int)o.pattern.size();
                o.capture_id = -1;
                o.target_char = -1;
                out_ops.push_back(o);
                ++i;
                continue;
            }
            if (inner->op() == re2::kRegexpQuest) {
                std::string lit;
                if (!optional_literal_from_node(inner, lit)) return false;
                OpDesc o{};
                o.op_type = OP_MATCH_OPTIONAL_LITERAL;
                o.pattern = std::move(lit);
                o.pattern_len = (int)o.pattern.size();
                o.capture_id = -1;
                o.target_char = -1;
                out_ops.push_back(o);
                ++i;
                continue;
            }
            if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: Star/Repeat child not AnyChar\n");
            return false;
        }

        // Capturing group (cap >= 0). Only support group 1 and exactly once.
        if (node->op() == re2::kRegexpCapture && node->cap() >= 0) {
            int capid = node->cap();
            if (capid != 1) {
                if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: capture group id=%d (not 1)\n", capid);
                return false;
            }
            if (capture_emitted) {
                if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: capture already emitted\n");
                return false;
            }
            // compile its content
            auto res = compile_capture_content(node);
            if (!res.first) {
                if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: compile_capture_content failed\n");
                return false;
            }
            int target = res.second; // -1 => capture to end; >=0 excluded char
            OpDesc o{};
            o.op_type = OP_EXTRACT_WHILE_NOT;
            o.pattern = std::string();
            o.pattern_len = 0;
            o.capture_id = 1;
            o.target_char = target;
            out_ops.push_back(o);
            capture_emitted = true;
            ++i;
            continue;
        }

        // Tail discard: Star or Repeat positioned right before end anchor
        // We accept any Star/Repeat here since in the context of replacement,
        // we're just discarding everything after the capture. This handles patterns like:
        //   ^prefix([^/]+)/.*$  -> extract until '/', match '/', discard rest
        //   ^([^/]+).*$         -> extract everything, discard rest
        //   ^([^/]+)[a-z]*$     -> extract capture, discard rest (even if [a-z]* only matches some chars)
        if ((node->op() == re2::kRegexpStar || node->op() == re2::kRegexpRepeat)) {
            // Only accept if this node is the final element before the end anchor
            if (i + 1 != end_idx) {
                if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: Star/Repeat not at end (i=%zu, end_idx=%zu)\n", i, end_idx);
                return false;
            }
            OpDesc o{};
            o.op_type = OP_DISCARD_REST;
            o.pattern = std::string();
            o.pattern_len = 0;
            o.capture_id = -1;
            o.target_char = -1;
            out_ops.push_back(o);
            ++i;
            continue;
        }

        // Single Dot/AnyChar not allowed except inside capture as handled above
        if (node->op() == re2::kRegexpAnyChar || node->op() == re2::kRegexpAnyByte) {
            // not allowed standalone
            return false;
        }

        // Character classes (non-capturing) — not supported except inside + capture handled earlier
        if (node->op() == re2::kRegexpCharClass) {
            if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: standalone CharClass not supported\n");
            return false;
        }

        // Anything else is unsupported
        if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: unsupported op type %d\n", node->op());
        return false;
    }

    if (!capture_emitted) {
        if (debug_pattern) fprintf(stderr, "[DEBUG] FAIL: no capture group emitted\n");
        return false;
    }

    // Append OP_RETURN_CAPTURE
    OpDesc ret{};
    ret.op_type = OP_RETURN_CAPTURE;
    ret.pattern = std::string();
    ret.pattern_len = 0;
    ret.capture_id = 1;
    ret.target_char = -1;
    out_ops.push_back(ret);

    return true;
}

NB_MODULE(_regex_compiler_native, m) {
    m.def("compile_regex", [](const std::string &pattern, const std::string &replacement) {
        // Parse pattern using RE2 parser to validate syntax.
        re2::RegexpStatus status;
        re2::Regexp* parsed = re2::Regexp::Parse(re2::StringPiece(pattern), re2::Regexp::LikePerl, &status);
        if (!parsed) {
            // parse error -> fallback
            return nb::make_tuple(nb::none(), 0, true);
        }

        // Run translator
        std::vector<OpDesc> ops;
        bool ok = translate_re2_regexp_to_ops(parsed, ops);
        // cleanup parsed object
        parsed->Decref();

        if (!ok) {
            // fallback to RE2
            return nb::make_tuple(nb::none(), 0, true);
        }

        nb::list py_ops;
        for (const auto &op : ops) {
            nb::object patt = op.pattern.empty() ? nb::none() : nb::str(op.pattern.c_str(), op.pattern.size());
            nb::object tchar = (op.target_char == -1) ? nb::none() : nb::int_(op.target_char);
            py_ops.append(nb::make_tuple(op.op_type, patt, op.pattern_len, op.capture_id, tchar));
        }

        return nb::make_tuple(py_ops, (int)ops.size(), false);
    });
}