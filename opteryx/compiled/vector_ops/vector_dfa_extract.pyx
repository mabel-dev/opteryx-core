# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Native DFA-style regex extraction for a narrow, explicitly supported subset.

REGEXP_REPLACE(s, pat, '\\1') is a capture-group extraction when `pat` matches
the entire input — the matched span is the whole string, so the result is just
the captured group (or the input untouched on no match). This kernel implements
that specialisation: the optimizer rewrites qualifying REGEXP_REPLACE calls to
_DFA_EXTRACT, and this module executes the compiled DFA procedure over a
Draken StringVector directly.

- optimizer compiles supported regex+'\\1' pairs into a compact blob
- execution decodes the constant program blob once
- execution interprets the decoded procedure over StringVector data
- preserve constant and dictionary encodings where possible
- no Python fallback in the hot path

The engine is intentionally generic at the execution layer. It does not contain
benchmark/domain-specific helpers such as "extract_url_host". Instead, supported
patterns are compiled into a sequence of generic operations like:

- consume literal
- consume optional literal
- capture until delimiter
- consume to end
- return capture

Supported subset (initial implementation)
-----------------------------------------
The current execution format supports the procedure shape used for:

    ^https?://(?:www\.)?([^/]+)/.*$   ->   \1

which corresponds to:

    consume("http")
    consume_optional("s")
    consume("://")
    consume_optional("www.")
    capture_until("/")
    consume("/
    consume_to_end()
    return_capture()

second example:

    ^CVE-([^-]+)-.*$

maps to:

    consume("CVE-")
    capture_until("-")
    consume("-")
    consume_to_end()
    return_capture()

Semantics follow RE2 (Perl leftmost-first), which is what REGEXP_REPLACE means:

- an optional literal is greedy — tried consumed first, and on a later failure
  the match resumes with it skipped (the optionals are the only choice points);
- consume_to_end is `.*` / `.+` over "any character": `.*` may consume nothing,
  and `.` without (?s) must not cross a newline. It is only ever the last step.

Patterns outside this — another class repeated (`[0-9]*`), a lazy optional,
`.*` before a later step, a newline-sensitive or lazy tail with no `$` — are
not compiled, so REGEXP_REPLACE refuses them instead of answering wrongly.

Program blobs are expected to be optimizer-produced constant literals.
Malformed or unsupported blobs raise ValueError.

This file is designed to be included from `vector_ops.pyx`.
"""

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, uint8_t, uint32_t, uint64_t
from libc.stdlib cimport free, malloc, getenv
from libc.string cimport memchr, memcmp, memcpy, memset

from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.object cimport PyObject
from libcpp.string cimport string

cdef extern from "dfa_simd_dispatch.h":
    # Compile-time NEON/AVX dispatch — no per-call function-pointer indirection.
    int dfa_find_char(const char* data, size_t length, char target) noexcept nogil

cdef extern from "re2/stringpiece.h" namespace "re2":
    cdef cppclass StringPiece:
        StringPiece() except +
        StringPiece(const char* data, size_t length) except +

cdef extern from "re2/regexp.h" namespace "re2":
    cdef enum RegexpOp:
        kRegexpNoMatch
        kRegexpEmptyMatch
        kRegexpLiteral
        kRegexpLiteralString
        kRegexpConcat
        kRegexpAlternate
        kRegexpStar
        kRegexpPlus
        kRegexpQuest
        kRegexpRepeat
        kRegexpCapture
        kRegexpAnyChar
        kRegexpAnyByte
        kRegexpBeginLine
        kRegexpEndLine
        kRegexpWordBoundary
        kRegexpNoWordBoundary
        kRegexpBeginText
        kRegexpEndText
        kRegexpCharClass
        kRegexpHaveMatch

    cdef cppclass RegexpStatus:
        RegexpStatus() except +
        bint ok() const
        string Text() const

    cdef struct RuneRange:
        int lo
        int hi

    cdef cppclass CharClass:
        int size()
        RuneRange* begin()
        RuneRange* end()

    cdef cppclass Regexp:
        RegexpOp op()
        int nsub()
        Regexp** sub()
        int min()
        int max()
        int cap()
        int nrunes()
        int match_id()
        int parse_flags()
        int rune()
        int Ref()
        int* runes()
        CharClass* cc()
        Regexp* Incref()
        void Decref()
        Regexp* Simplify()
        @staticmethod
        Regexp* Parse(const StringPiece& s, RegexpParseFlags flags, RegexpStatus* status)

# Nested enum re2::Regexp::ParseFlags — declared at namespace "re2::Regexp" so
# Cython emits the correct re2::Regexp::LikePerl etc. in generated C++.
cdef extern from "re2/regexp.h" namespace "re2::Regexp":
    cdef enum RegexpParseFlags "re2::Regexp::ParseFlags":
        NoParseFlags
        FoldCase
        Literal
        ClassNL
        DotNL
        MatchNL
        OneLine
        Latin1
        NonGreedy
        PerlClasses
        PerlB
        PerlX
        UnicodeGroups
        NeverNL
        NeverCapture
        LikePerl

cdef int DFA_MAX_LITERAL_BYTES = 64

from draken.core.buffers cimport (
    DrakenVector,
    DrakenStringArena,
    DrakenStringSlot,
    DrakenType,
    DRAKEN_VARCHAR,
    str_length,
    str_data,
    str_init_null,
    str_is_inline,
    str_clone_with_offset,
    STR_INLINE_MAX,
)
from draken.vectors.vector cimport Vector

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

cdef extern from "core/string_slot.h":
    void draken_build_string_slot(DrakenStringSlot* slot, const uint8_t* data, uint32_t length, uint32_t arena_offset) noexcept nogil

cdef extern from "vectors/_vector_bridge.h":
    object draken_vector_own_string(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint8_t* validity, uint32_t length, DrakenType type)
    object draken_vector_own_string_dict(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint32_t* codes, uint32_t data_length,
        uint8_t* validity, uint32_t length, DrakenType type)

cdef extern from "core/buffers.h" nogil:
    int draken_is_compressed(const DrakenVector* v)
    int draken_is_constant(const DrakenVector* v)

# c-native BC_FUNCTION kernel plumbing (Phase 9a-fn shape) — lets _DFA_EXTRACT
# dispatch straight from the nogil DV VM, same ABI as draken/ops/kernels/*.
cdef extern from "ops/vec_result.h":
    ctypedef struct VecResult:
        void*             data
        uint8_t*          validity
        const uint32_t*   selection
        bint              owns_selection
        uint32_t          data_length
        uint32_t          length
        DrakenType        type
        uint8_t           flags
        uint8_t           validity_embedded
        uint8_t*          arena
        uint8_t           arena_lender

cdef extern from "ops/kernels/error_handling.h" nogil:
    VecResult draken_error_sentinel(const char* error_message)

cdef extern from "ops/kernels/result_helpers.h" nogil:
    VecResult vecresult_from_string_buffers(
        DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
        uint8_t* validity, uint32_t length, DrakenType type)
    uint8_t* vecresult_string_block_alloc(
        uint32_t length, size_t arena_len, int want_validity,
        DrakenStringSlot** out_slots, uint8_t** out_arena, uint8_t** out_validity)
    VecResult vecresult_from_string_block(
        uint8_t* block, uint32_t length, size_t arena_len, int has_validity, DrakenType type)

cdef extern from "ops/kernels/result_helpers.h" nogil:
    void kernel_borrow_input_arena(VecResult& r, const DrakenVector* v, uint8_t arg)
    uint32_t kernel_lender_offset(const DrakenVector* v, const uint8_t* p)

cdef extern from "_dfa_extract_shape.h" nogil:
    int dfa_x_live_mask(const DrakenVector* v, uint8_t** mask)
    int dfa_x_preserve_shape(VecResult* r, const DrakenVector* v)


cdef enum DfaOpType:
    DFA_OP_CONSUME_LITERAL = 1
    DFA_OP_CONSUME_OPTIONAL_LITERAL = 2
    DFA_OP_CAPTURE_UNTIL_CHAR = 3
    DFA_OP_CONSUME_TO_END = 4
    DFA_OP_RETURN_CAPTURE = 5


# Program blob version. 2 added the DFA_OP_CONSUME_TO_END flags byte.
cdef int DFA_PROGRAM_VERSION = 2

# DFA_OP_CONSUME_TO_END flags (the op's one payload byte).
cdef enum DfaRestFlags:
    DFA_REST_EMPTY_OK = 1      # `.*` — may consume nothing (`.+` may not)
    DFA_REST_NO_NEWLINE = 2    # `.` without (?s) — the rest must hold no '\n'


cdef struct DfaOp:
    int op_type
    const char* literal
    Py_ssize_t literal_len
    char target_char
    uint8_t rest_flags
    # For literals <= 8 bytes: the literal packed into a uint64 (native-endian,
    # zero-padded) plus a byte mask, so the hot loop compares with a single
    # register op instead of an out-of-line memcmp call. packed/mask are only
    # valid when literal_len <= 8; longer literals use the literal pointer path.
    uint64_t packed
    uint64_t mask


cdef struct DfaProcedure:
    DfaOp ops[8]
    int op_count


cdef struct DfaProgramBuilder:
    uint8_t op_types[8]
    const char* literals[8]
    Py_ssize_t literal_lens[8]
    char target_chars[8]
    uint8_t rest_flags[8]
    # Compile-time only (never encoded): the consume_to_end came from a lazy
    # `.*?` / `.+?`, which differs from the greedy form when there is no `$`.
    bint rest_lazy[8]
    char literal_storage[8][64]
    int op_count


cdef inline uint8_t _read_u8(const char** p) noexcept nogil:
    cdef uint8_t value = (<const uint8_t*>p[0])[0]
    p[0] += 1
    return value


cdef inline uint32_t _read_u32(const char** p) noexcept nogil:
    cdef const uint8_t* src = <const uint8_t*>p[0]
    cdef uint32_t value = (
        <uint32_t>src[0]
        | (<uint32_t>src[1] << 8)
        | (<uint32_t>src[2] << 16)
        | (<uint32_t>src[3] << 24)
    )
    p[0] += 4
    return value


cdef inline bint _decode_procedure(
    const char* program_ptr,
    Py_ssize_t program_len,
    DfaProcedure* proc,
) noexcept nogil:
    """Parse a compiled DFA program blob. Returns False (never raises) on any
    malformed input — nogil so the c-native BC_FUNCTION kernel below can decode
    the (per-morsel constant) program without touching the GIL. The sole GIL-side
    caller (vector_dfa_extract) raises ValueError itself on a False return."""
    cdef const char* p = program_ptr
    cdef const char* end = program_ptr + program_len
    cdef uint8_t version
    cdef uint8_t op_count
    cdef uint8_t op_type
    cdef uint32_t literal_len
    cdef int i

    if program_ptr == NULL or program_len < 2:
        return False

    version = _read_u8(&p)
    if version != DFA_PROGRAM_VERSION:
        return False

    op_count = _read_u8(&p)
    if op_count == 0 or op_count > 8:
        return False

    for i in range(op_count):
        if p >= end:
            return False

        op_type = _read_u8(&p)
        proc.ops[i].op_type = op_type
        proc.ops[i].literal = NULL
        proc.ops[i].literal_len = 0
        proc.ops[i].target_char = <char>0
        proc.ops[i].rest_flags = 0
        proc.ops[i].packed = 0
        proc.ops[i].mask = 0

        if op_type == DFA_OP_CONSUME_LITERAL or op_type == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            if end - p < 4:
                return False
            literal_len = _read_u32(&p)
            if literal_len == 0:
                return False
            if end - p < literal_len:
                return False
            proc.ops[i].literal = p
            proc.ops[i].literal_len = <Py_ssize_t>literal_len
            # Precompute the packed/mask register form for short literals once,
            # here, rather than per row in the hot loop.
            if literal_len <= 8:
                memcpy(&proc.ops[i].packed, p, <size_t>literal_len)
                if literal_len == 8:
                    proc.ops[i].mask = <uint64_t>0xFFFFFFFFFFFFFFFF
                else:
                    proc.ops[i].mask = ((<uint64_t>1) << (8 * literal_len)) - <uint64_t>1
            p += literal_len

        elif op_type == DFA_OP_CAPTURE_UNTIL_CHAR:
            if p >= end:
                return False
            proc.ops[i].target_char = <char>_read_u8(&p)

        elif op_type == DFA_OP_CONSUME_TO_END:
            if p >= end:
                return False
            proc.ops[i].rest_flags = _read_u8(&p)
            if (proc.ops[i].rest_flags & ~(DFA_REST_EMPTY_OK | DFA_REST_NO_NEWLINE)) != 0:
                return False

        elif op_type == DFA_OP_RETURN_CAPTURE:
            pass

        else:
            return False

    if p != end:
        return False

    proc.op_count = op_count
    return True


cdef inline void _write_u8(char** p, uint8_t value) noexcept:
    (<uint8_t*>p[0])[0] = value
    p[0] += 1


cdef inline void _write_u32(char** p, uint32_t value) noexcept:
    cdef uint8_t* dst = <uint8_t*>p[0]
    dst[0] = <uint8_t>(value & 0xFF)
    dst[1] = <uint8_t>((value >> 8) & 0xFF)
    dst[2] = <uint8_t>((value >> 16) & 0xFF)
    dst[3] = <uint8_t>((value >> 24) & 0xFF)
    p[0] += 4


cdef inline void _builder_reset(DfaProgramBuilder* builder) noexcept:
    cdef int i
    builder.op_count = 0
    for i in range(8):
        builder.literals[i] = NULL
        builder.literal_lens[i] = 0
        builder.target_chars[i] = <char>0
        builder.rest_flags[i] = 0
        builder.rest_lazy[i] = False


cdef inline bint _builder_append_literal(
    DfaProgramBuilder* builder,
    uint8_t op_type,
    const char* literal,
    Py_ssize_t literal_len,
) noexcept:
    cdef int slot
    if builder.op_count >= 8:
        return False
    if literal == NULL or literal_len <= 0 or literal_len > DFA_MAX_LITERAL_BYTES:
        return False
    slot = builder.op_count
    memcpy(builder.literal_storage[slot], literal, <size_t>literal_len)
    builder.op_types[slot] = op_type
    builder.literals[slot] = <const char*>builder.literal_storage[slot]
    builder.literal_lens[slot] = literal_len
    builder.target_chars[slot] = <char>0
    builder.op_count += 1
    return True


cdef inline bint _builder_append_target(
    DfaProgramBuilder* builder,
    uint8_t op_type,
    char target_char,
) noexcept:
    if builder.op_count >= 8:
        return False
    builder.op_types[builder.op_count] = op_type
    builder.literals[builder.op_count] = NULL
    builder.literal_lens[builder.op_count] = 0
    builder.target_chars[builder.op_count] = target_char
    builder.op_count += 1
    return True


cdef inline bint _builder_append_simple(
    DfaProgramBuilder* builder,
    uint8_t op_type,
) noexcept:
    if builder.op_count >= 8:
        return False
    builder.op_types[builder.op_count] = op_type
    builder.literals[builder.op_count] = NULL
    builder.literal_lens[builder.op_count] = 0
    builder.target_chars[builder.op_count] = <char>0
    builder.op_count += 1
    return True


cdef inline bint _builder_append_rest(
    DfaProgramBuilder* builder,
    uint8_t rest_flags,
    bint rest_lazy,
) noexcept:
    if not _builder_append_simple(builder, DFA_OP_CONSUME_TO_END):
        return False
    builder.rest_flags[builder.op_count - 1] = rest_flags
    builder.rest_lazy[builder.op_count - 1] = rest_lazy
    return True


cdef inline bint _is_ascii_literal_regexp(Regexp* re) noexcept:
    cdef int i
    cdef int rune_value
    if re == NULL:
        return False
    if re.op() == kRegexpLiteral:
        rune_value = re.rune()
        return 0 <= rune_value <= 127
    if re.op() != kRegexpLiteralString:
        return False
    for i in range(re.nrunes()):
        rune_value = re.runes()[i]
        if rune_value < 0 or rune_value > 127:
            return False
    return True


cdef inline bint _extract_ascii_literal(
    Regexp* re,
    const char** literal_ptr,
    Py_ssize_t* literal_len,
    char* literal_buf,
) noexcept:
    cdef int rune_value
    cdef int i
    if re == NULL:
        return False
    if re.op() == kRegexpLiteral:
        rune_value = re.rune()
        if rune_value < 0 or rune_value > 127:
            return False
        literal_buf[0] = <char>rune_value
        literal_ptr[0] = <const char*>literal_buf
        literal_len[0] = 1
        return True
    if re.op() != kRegexpLiteralString:
        return False
    if not _is_ascii_literal_regexp(re):
        return False
    if re.nrunes() <= 0 or re.nrunes() > DFA_MAX_LITERAL_BYTES:
        return False
    for i in range(re.nrunes()):
        rune_value = re.runes()[i]
        if rune_value < 0 or rune_value > 127:
            return False
        literal_buf[i] = <char>rune_value
    literal_ptr[0] = <const char*>literal_buf
    literal_len[0] = re.nrunes()
    return True


cdef inline bint _is_optional_literal(Regexp* re) noexcept:
    if re == NULL:
        return False
    # A lazy `??` prefers SKIPPING the literal; the executor only knows the greedy
    # order (consume first), so a lazy optional is refused, not run greedily.
    if re.op() == kRegexpQuest and re.nsub() == 1 and (re.parse_flags() & NonGreedy) == 0:
        return _is_ascii_literal_regexp(re.sub()[0])
    return False


cdef inline bint _is_consume_rest(Regexp* re, uint8_t* rest_flags, bint* rest_lazy) noexcept:
    """`.*` / `.+` — a repeat of "any character", the only thing consume_to_end
    implements. Qualifying repeats: AnyChar / AnyByte, the full rune range, or `.`
    without (?s), which is [^\\n] and sets DFA_REST_NO_NEWLINE. Any other class
    (`[0-9]*`) is refused — consuming the rest would accept bytes it rejects."""
    cdef Regexp* sub
    cdef CharClass* char_class
    cdef RuneRange* it
    cdef RuneRange* end
    cdef int range_count = 0
    cdef int first_lo = -1
    cdef int first_hi = -1
    cdef int second_lo = -1
    cdef int second_hi = -1

    if re == NULL or re.nsub() != 1:
        return False
    if re.op() != kRegexpStar and re.op() != kRegexpPlus:
        return False
    sub = re.sub()[0]
    if sub == NULL:
        return False

    rest_flags[0] = DFA_REST_EMPTY_OK if re.op() == kRegexpStar else 0
    rest_lazy[0] = (re.parse_flags() & NonGreedy) != 0

    if sub.op() == kRegexpAnyChar or sub.op() == kRegexpAnyByte:
        return True
    if sub.op() != kRegexpCharClass:
        return False
    char_class = sub.cc()
    if char_class == NULL:
        return False

    it = char_class.begin()
    end = char_class.end()
    while it != end:
        if range_count == 0:
            first_lo = it.lo
            first_hi = it.hi
        elif range_count == 1:
            second_lo = it.lo
            second_hi = it.hi
        range_count += 1
        it += 1

    if range_count == 1 and first_lo == 0 and first_hi == 1114111:
        return True
    if (range_count == 2 and first_lo == 0 and first_hi == 9
            and second_lo == 11 and second_hi == 1114111):
        rest_flags[0] |= DFA_REST_NO_NEWLINE
        return True
    return False


cdef inline bint _is_capture_until_char(Regexp* re, char* target_char) noexcept:
    cdef Regexp* inner
    cdef Regexp* repeated
    cdef CharClass* char_class
    cdef RuneRange* it
    cdef RuneRange* end
    cdef int range_count = 0
    cdef int first_lo = -1
    cdef int first_hi = -1
    cdef int second_lo = -1
    cdef int second_hi = -1
    cdef int excluded

    if re == NULL or re.op() != kRegexpCapture or re.nsub() != 1 or re.cap() != 1:
        return False

    inner = re.sub()[0]
    if inner == NULL:
        return False

    if inner.op() == kRegexpPlus and inner.nsub() == 1:
        repeated = inner.sub()[0]
    elif inner.op() == kRegexpRepeat and inner.nsub() == 1 and inner.min() == 1 and inner.max() == -1:
        repeated = inner.sub()[0]
    else:
        # A bare [^X] is ONE character, not "everything up to X".
        return False

    if repeated == NULL or repeated.op() != kRegexpCharClass:
        return False

    char_class = repeated.cc()
    if char_class == NULL:
        return False

    # Accept any [^X]+ where X is a single ASCII byte, recognised as the two
    # ranges [0, X-1] and [X+1, 1114111]. Multi-byte exclusions and multi-char
    # exclusions are rejected because the byte-level scanner can only locate
    # a single concrete byte.
    it = char_class.begin()
    end = char_class.end()

    while it != end:
        if range_count == 0:
            first_lo = it.lo
            first_hi = it.hi
        elif range_count == 1:
            second_lo = it.lo
            second_hi = it.hi
        range_count += 1
        it += 1

    if range_count != 2:
        return False
    if first_lo != 0 or second_hi != 1114111:
        return False
    if first_hi + 2 != second_lo:
        return False

    excluded = first_hi + 1
    if excluded < 1 or excluded > 127:
        return False

    target_char[0] = <char>excluded
    return True


cdef inline bint _lower_regexp_to_builder(
    Regexp* re,
    DfaProgramBuilder* builder,
) noexcept:
    cdef int i
    cdef Regexp* child
    cdef const char* literal_ptr = NULL
    cdef Py_ssize_t literal_len = 0
    cdef char target_char = <char>0
    cdef char literal_buf[64]
    cdef uint8_t rest_flags = 0
    cdef bint rest_lazy = False

    if re == NULL:
        return False

    if re.op() == kRegexpBeginText:
        return True

    if re.op() == kRegexpEndText:
        return True

    if re.op() == kRegexpConcat:
        for i in range(re.nsub()):
            child = re.sub()[i]
            if child == NULL:
                return False
            if child.op() == kRegexpBeginText:
                continue
            if child.op() == kRegexpEndText:
                continue
            if _extract_ascii_literal(child, &literal_ptr, &literal_len, literal_buf):
                if not _builder_append_literal(
                    builder,
                    DFA_OP_CONSUME_LITERAL,
                    literal_ptr,
                    literal_len,
                ):
                    return False
                continue
            if _is_optional_literal(child):
                if not _extract_ascii_literal(child.sub()[0], &literal_ptr, &literal_len, literal_buf):
                    return False
                if not _builder_append_literal(
                    builder,
                    DFA_OP_CONSUME_OPTIONAL_LITERAL,
                    literal_ptr,
                    literal_len,
                ):
                    return False
                continue
            if _is_capture_until_char(child, &target_char):
                if not _builder_append_target(
                    builder,
                    DFA_OP_CAPTURE_UNTIL_CHAR,
                    target_char,
                ):
                    return False
                continue
            if _is_consume_rest(child, &rest_flags, &rest_lazy):
                if not _builder_append_rest(builder, rest_flags, rest_lazy):
                    return False
                continue
            return False
        return True

    if _extract_ascii_literal(re, &literal_ptr, &literal_len, literal_buf):
        return _builder_append_literal(
            builder,
            DFA_OP_CONSUME_LITERAL,
            literal_ptr,
            literal_len,
        )

    if _is_optional_literal(re):
        if not _extract_ascii_literal(re.sub()[0], &literal_ptr, &literal_len, literal_buf):
            return False
        return _builder_append_literal(
            builder,
            DFA_OP_CONSUME_OPTIONAL_LITERAL,
            literal_ptr,
            literal_len,
        )

    if _is_capture_until_char(re, &target_char):
        return _builder_append_target(
            builder,
            DFA_OP_CAPTURE_UNTIL_CHAR,
            target_char,
        )

    if _is_consume_rest(re, &rest_flags, &rest_lazy):
        return _builder_append_rest(builder, rest_flags, rest_lazy)

    return False


cdef inline bytes _encode_builder_program(DfaProgramBuilder* builder):
    cdef Py_ssize_t total_len = 2
    cdef Py_ssize_t i
    cdef bytes program
    cdef char* out_ptr

    if builder.op_count <= 0 or builder.op_count > 8:
        raise ValueError("vector_dfa_extract: compiled program op count is invalid")

    for i in range(builder.op_count):
        total_len += 1
        if builder.op_types[i] == DFA_OP_CONSUME_LITERAL or builder.op_types[i] == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            total_len += 4 + builder.literal_lens[i]
        elif builder.op_types[i] == DFA_OP_CAPTURE_UNTIL_CHAR or builder.op_types[i] == DFA_OP_CONSUME_TO_END:
            total_len += 1

    program = bytes(total_len)
    out_ptr = <char*>program

    _write_u8(&out_ptr, <uint8_t>DFA_PROGRAM_VERSION)
    _write_u8(&out_ptr, <uint8_t>builder.op_count)

    for i in range(builder.op_count):
        _write_u8(&out_ptr, builder.op_types[i])
        if builder.op_types[i] == DFA_OP_CONSUME_LITERAL or builder.op_types[i] == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            _write_u32(&out_ptr, <uint32_t>builder.literal_lens[i])
            memcpy(out_ptr, builder.literals[i], <size_t>builder.literal_lens[i])
            out_ptr += builder.literal_lens[i]
        elif builder.op_types[i] == DFA_OP_CAPTURE_UNTIL_CHAR:
            _write_u8(&out_ptr, <uint8_t>builder.target_chars[i])
        elif builder.op_types[i] == DFA_OP_CONSUME_TO_END:
            _write_u8(&out_ptr, builder.rest_flags[i])

    return program


cpdef object compile_dfa_program(bytes pattern, bytes replacement):
    cdef char* pattern_buf = NULL
    cdef char* replacement_buf = NULL
    cdef Py_ssize_t pattern_len = 0
    cdef Py_ssize_t replacement_len = 0
    cdef StringPiece pattern_piece
    cdef RegexpStatus status
    cdef Regexp* parsed = NULL
    cdef Regexp* simplified = NULL
    cdef DfaProgramBuilder builder
    cdef bint has_begin_anchor = False
    cdef bint has_end_anchor = False
    cdef Regexp* first_child = NULL
    cdef Regexp* last_child = NULL
    cdef int last_op_type
    cdef int op_idx
    cdef int next_op_type
    cdef char boundary_char

    PyBytes_AsStringAndSize(pattern, &pattern_buf, &pattern_len)
    PyBytes_AsStringAndSize(replacement, &replacement_buf, &replacement_len)

    if replacement_len != 2 or replacement_buf[0] != 92 or replacement_buf[1] != 49:
        return None

    pattern_piece = StringPiece(pattern_buf, <size_t>pattern_len)
    parsed = Regexp.Parse(pattern_piece, LikePerl, &status)
    if parsed == NULL:
        if status.ok():
            return None
        raise ValueError(status.Text().decode("utf8"))

    try:
        simplified = parsed.Simplify()
        if simplified == NULL:
            return None

        # The executor returns only the captured group on success. That is
        # equivalent to REGEXP_REPLACE(s, pattern, '\1') only when the program
        # consumes the entire input (i.e. anchored ^...$ or terminating in a
        # consume-to-end). Otherwise bytes outside the match are silently
        # dropped. Refuse to compile patterns that do not satisfy this so the
        # optimizer falls back to the full RE2 path.
        if simplified.op() == kRegexpConcat and simplified.nsub() >= 1:
            first_child = simplified.sub()[0]
            last_child = simplified.sub()[simplified.nsub() - 1]
            if first_child != NULL and first_child.op() == kRegexpBeginText:
                has_begin_anchor = True
            if last_child != NULL and last_child.op() == kRegexpEndText:
                has_end_anchor = True
        elif simplified.op() == kRegexpBeginText:
            has_begin_anchor = True
            has_end_anchor = True

        if not has_begin_anchor:
            return None

        _builder_reset(&builder)
        if not _lower_regexp_to_builder(simplified, &builder):
            return None

        if builder.op_count == 0:
            return None

        last_op_type = builder.op_types[builder.op_count - 1]
        if last_op_type != DFA_OP_CONSUME_TO_END and not has_end_anchor:
            return None

        # consume_to_end takes everything left, so a step after it could only ever
        # fail — `^M.*r([^y]+)y$` needs backtracking INTO the `.*`. Last step only.
        for op_idx in range(builder.op_count - 1):
            if builder.op_types[op_idx] == DFA_OP_CONSUME_TO_END:
                return None

        # With no `$` the match may stop short of the end, and REGEXP_REPLACE then
        # keeps the unmatched tail: `.` stops at a newline, a lazy tail matches as
        # little as it can. The executor returns the capture alone, so only a greedy
        # tail that crosses newlines may go unanchored.
        if last_op_type == DFA_OP_CONSUME_TO_END and not has_end_anchor:
            if builder.rest_lazy[builder.op_count - 1]:
                return None
            if builder.rest_flags[builder.op_count - 1] & DFA_REST_NO_NEWLINE:
                return None

        # Every CAPTURE_UNTIL_CHAR(X) must be immediately followed by a
        # CONSUME_LITERAL whose first byte is X. Without this, a regex like
        # ^M([^s]+).*$ would compile (no 's' required by the pattern) but the
        # DFA would fail to find 's' in "Mercury" and return the input unchanged
        # instead of the correct "ercury".
        for op_idx in range(builder.op_count - 1):
            if builder.op_types[op_idx] == DFA_OP_CAPTURE_UNTIL_CHAR:
                next_op_type = builder.op_types[op_idx + 1]
                if next_op_type != DFA_OP_CONSUME_LITERAL:
                    return None
                boundary_char = builder.target_chars[op_idx]
                if builder.literal_lens[op_idx + 1] < 1:
                    return None
                if builder.literals[op_idx + 1][0] != boundary_char:
                    return None

        if last_op_type != DFA_OP_RETURN_CAPTURE:
            if not _builder_append_simple(&builder, DFA_OP_RETURN_CAPTURE):
                return None

        return _encode_builder_program(&builder)
    finally:
        if simplified != NULL:
            simplified.Decref()
        parsed.Decref()


cdef inline bint _slice_equals(
    const char* value_ptr,
    Py_ssize_t value_len,
    const char* literal,
    Py_ssize_t literal_len,
) noexcept:
    if value_len != literal_len:
        return False
    return memcmp(value_ptr, literal, <size_t>literal_len) == 0


cdef inline void _extract_const_slice(
    Vector vec,
    const char** data_ptr,
    Py_ssize_t* data_len,
) except *:
    """Read the compiled DFA program from row 0 via the unified view.

    Callers needing literal semantics should pass a single-row or
    constant-layout vector.
    """
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* prog_arena
    cdef DrakenStringSlot* prog_slot
    if uv.length == 0:
        raise ValueError("vector_dfa_extract: empty program vector")
    if uv.validity != NULL and not ((uv.validity[0] >> 0) & 1):
        raise ValueError("vector_dfa_extract: compiled program must be non-null")
    prog_arena = <DrakenStringArena*>uv.data
    prog_slot = &prog_arena.slots[uv.selection[0]]
    data_ptr[0] = <const char*>str_data(prog_slot, prog_arena.arena)
    data_len[0] = <Py_ssize_t>str_length(prog_slot)





cdef inline bint _execute_procedure(
    const char* src,
    Py_ssize_t src_len,
    DfaProcedure* proc,
    const char** out_ptr,
    Py_ssize_t* out_len,
) noexcept nogil:
    """Run the procedure over one value, in RE2's (Perl leftmost-first) order.

    Optional literals are the only choice points — a literal, a [^X]+ capture
    and the trailing consume_to_end each have one way to match — so the search
    is: take an optional when it matches and remember where it was taken; on a
    later failure, resume after that optional with it skipped. At most one
    resume point per op, so the stack is the op limit (8)."""
    cdef const char* p = src
    cdef const char* end = src + src_len
    cdef const char* capture_ptr = NULL
    cdef Py_ssize_t capture_len = 0
    cdef int i = 0
    cdef int char_pos
    cdef DfaOp* op
    cdef Py_ssize_t remaining
    cdef uint64_t loaded
    cdef bint ok
    cdef bint took
    cdef int resume_op[8]
    cdef const char* resume_p[8]
    cdef int resume_count = 0

    out_ptr[0] = NULL
    out_len[0] = 0

    while i < proc.op_count:
        op = &proc.ops[i]
        ok = True

        if op.op_type == DFA_OP_CONSUME_LITERAL:
            remaining = <Py_ssize_t>(end - p)
            if remaining < op.literal_len:
                ok = False
            elif op.literal_len <= 8 and remaining >= 8:
                # Fast path: one unaligned 8-byte load + masked register compare.
                # Safe only when >= 8 bytes remain (tight arenas have no slack);
                # the rare <8-byte tail falls through to memcmp below.
                memcpy(&loaded, p, 8)
                ok = (loaded & op.mask) == op.packed
            else:
                ok = memcmp(p, op.literal, <size_t>op.literal_len) == 0
            if ok:
                p += op.literal_len

        elif op.op_type == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            remaining = <Py_ssize_t>(end - p)
            if remaining < op.literal_len:
                took = False
            elif op.literal_len <= 8 and remaining >= 8:
                memcpy(&loaded, p, 8)
                took = (loaded & op.mask) == op.packed
            else:
                took = memcmp(p, op.literal, <size_t>op.literal_len) == 0
            if took:
                resume_op[resume_count] = i + 1
                resume_p[resume_count] = p
                resume_count += 1
                p += op.literal_len

        elif op.op_type == DFA_OP_CAPTURE_UNTIL_CHAR:
            remaining = <Py_ssize_t>(end - p)
            # dfa_find_char returns the first match index in [0, remaining), or
            # -1. char_pos <= 0 covers both "not found" and "empty capture";
            # char_pos > 0 guarantees p < p+char_pos < end.
            char_pos = dfa_find_char(p, <size_t>remaining, op.target_char) if remaining > 0 else -1
            if char_pos <= 0:
                ok = False
            else:
                capture_ptr = p
                capture_len = <Py_ssize_t>char_pos
                p = p + char_pos

        elif op.op_type == DFA_OP_CONSUME_TO_END:
            if p == end:
                ok = (op.rest_flags & DFA_REST_EMPTY_OK) != 0
            elif (op.rest_flags & DFA_REST_NO_NEWLINE) and memchr(p, 10, <size_t>(end - p)) != NULL:
                ok = False
            else:
                p = end

        elif op.op_type == DFA_OP_RETURN_CAPTURE:
            if capture_ptr != NULL and p == end:
                out_ptr[0] = capture_ptr
                out_len[0] = capture_len
                return True
            ok = False

        else:
            return False

        if ok:
            i += 1
        elif resume_count == 0:
            return False
        else:
            resume_count -= 1
            i = resume_op[resume_count]
            p = resume_p[resume_count]

    return False


cdef object _vector_dfa_extract_compressed(
    DrakenVector* uv,
    DrakenStringArena* src_arena,
    DfaProcedure* proc,
):
    """Shape-preserving DFA execution for compressed (dict/constant) input.

    The DFA is a pure function of the input bytes, so a value that appears in
    `data_length` unique slots is executed once per UNIQUE value instead of
    once per logical row. The result preserves the input's encoding: the same
    `selection` codes are reused, so the output stays compressed and downstream
    operators see the cheaper encoding via the uniform data[selection[i]] path.

    Validity is per-logical-row and independent of value identity, so it is
    copied through unchanged; a value referenced only by null rows is still
    executed (harmless — its result is never selected).
    """
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t k = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t j
    cdef const char* out_ptr = NULL
    cdef Py_ssize_t out_len = 0
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t row_len = 0
    cdef const char* row_ptr = NULL
    cdef Py_ssize_t null_bytes = 0
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen

    # First pass: cache (ptr, len) per UNIQUE value (k entries, not n).
    # cap_lens[j] >= 0 → DFA matched; length of capture.
    # cap_lens[j] < 0  → no match; passthrough; actual length = -cap_lens[j] - 1.
    cdef const char** cap_ptrs = <const char**>malloc(<size_t>k * sizeof(const char*))
    cdef int32_t* cap_lens = <int32_t*>malloc(<size_t>k * sizeof(int32_t))

    if cap_ptrs == NULL or cap_lens == NULL:
        free(cap_ptrs)
        free(cap_lens)
        raise MemoryError("vector_dfa_extract: cache allocation failed")

    with nogil:
        for j in range(k):
            slot = &src_arena.slots[j]
            slen = str_length(slot)
            sdata = str_data(slot, src_arena.arena)
            row_ptr = <const char*>sdata
            row_len = <Py_ssize_t>slen

            if _execute_procedure(row_ptr, row_len, proc, &out_ptr, &out_len):
                cap_ptrs[j] = out_ptr
                cap_lens[j] = <int32_t>out_len
                # Only long (> 12 byte) strings occupy the arena.
                if out_len > STR_INLINE_MAX:
                    total_bytes += out_len
            else:
                cap_ptrs[j] = row_ptr
                cap_lens[j] = <int32_t>(-row_len - 1)
                if row_len > STR_INLINE_MAX:
                    total_bytes += row_len

    # Allocate value-array slots (k), arena, codes (n), and validity (n).
    cdef size_t slots_sz = <size_t>k * sizeof(DrakenStringSlot) if k > 0 else sizeof(DrakenStringSlot)
    cdef size_t arena_sz = <size_t>total_bytes if total_bytes > 0 else 1
    cdef DrakenStringSlot* out_slots = <DrakenStringSlot*>draken_malloc(slots_sz)
    cdef uint8_t* out_arena = <uint8_t*>draken_malloc(arena_sz)
    cdef uint32_t* out_codes = <uint32_t*>draken_malloc(<size_t>n * sizeof(uint32_t) if n > 0 else sizeof(uint32_t))
    cdef uint8_t* out_null = NULL

    if out_slots == NULL or out_arena == NULL or out_codes == NULL:
        free(cap_ptrs)
        free(cap_lens)
        draken_free(out_slots)
        draken_free(out_arena)
        draken_free(out_codes)
        raise MemoryError("vector_dfa_extract: output allocation failed")

    memcpy(out_codes, sel, <size_t>n * sizeof(uint32_t))

    if nulls != NULL:
        null_bytes = (n + 7) >> 3
        out_null = <uint8_t*>draken_malloc(<size_t>null_bytes if null_bytes > 0 else 1)
        if out_null == NULL:
            free(cap_ptrs)
            free(cap_lens)
            draken_free(out_slots)
            draken_free(out_arena)
            draken_free(out_codes)
            raise MemoryError("vector_dfa_extract: validity allocation failed")
        memcpy(out_null, nulls, <size_t>null_bytes)

    # Second pass: fill the value-array slots and arena (k entries).
    cdef const char* cached_ptr
    cdef int32_t cached_len
    cdef uint32_t copy_len
    cdef size_t arena_used = 0

    cdef DrakenStringSlot* psrc
    with nogil:
        for j in range(k):
            cached_ptr = cap_ptrs[j]
            cached_len = cap_lens[j]

            if cached_len >= 0:
                # Matched capture: fresh substring — build it (hash unknown).
                copy_len = <uint32_t>cached_len
                if copy_len > <uint32_t>STR_INLINE_MAX:
                    memcpy(out_arena + arena_used, cached_ptr, <size_t>copy_len)
                    draken_build_string_slot(&out_slots[j], <const uint8_t*>cached_ptr, copy_len, <uint32_t>arena_used)
                    arena_used += <size_t>copy_len
                else:
                    draken_build_string_slot(&out_slots[j], <const uint8_t*>cached_ptr, copy_len, 0)
            else:
                # Passthrough: clone the source slot verbatim (reuse prefix + hash32).
                psrc = &src_arena.slots[j]
                if str_is_inline(psrc):
                    str_clone_with_offset(&out_slots[j], psrc, 0)
                else:
                    copy_len = <uint32_t>(-cached_len - 1)
                    memcpy(out_arena + arena_used, cached_ptr, <size_t>copy_len)
                    str_clone_with_offset(&out_slots[j], psrc, <uint32_t>arena_used)
                    arena_used += <size_t>copy_len

    free(cap_ptrs)
    free(cap_lens)

    return draken_vector_own_string_dict(
        out_slots, out_arena, arena_used, out_codes, <uint32_t>k,
        out_null, <uint32_t>n, DRAKEN_VARCHAR,
    )


# ---------------------------------------------------------------------------
# c-native BC_FUNCTION kernel — draken__dfa_extract(strings, compiled_program)
# ---------------------------------------------------------------------------

# One output slot. Every output is a byte range of its input value — a capture, or
# the whole value when nothing matched — so a LONG output (> 12 bytes) can only come
# from a long input, whose bytes live in the input's arena: its slot points there
# (kernel_borrow_input_arena), and no output byte is copied.
cdef inline void _dfa_x_slot(
    DrakenStringSlot* out, const DrakenVector* v, const DrakenStringSlot* src,
    bint matched, const char* cap_ptr, Py_ssize_t cap_len,
) noexcept nogil:
    if not matched:
        memcpy(out, src, sizeof(DrakenStringSlot))     # the value itself, offset and all
    elif cap_len > STR_INLINE_MAX:
        draken_build_string_slot(out, <const uint8_t*>cap_ptr, <uint32_t>cap_len,
                                 kernel_lender_offset(v, <const uint8_t*>cap_ptr))
    else:
        draken_build_string_slot(out, <const uint8_t*>cap_ptr, <uint32_t>cap_len, 0)


# Compressed (dict/constant) input: the procedure is a pure function of the value
# bytes, so it runs once per live PHYSICAL value (kernel_live_slots) instead of
# once per logical row, and the result keeps the input's encoding
# (kernel_preserve_shape), so the GROUP BY key downstream stays compressed.
cdef VecResult _dfa_extract_kernel_compressed(
    const DrakenVector* v, DfaProcedure* proc,
) noexcept nogil:
    cdef const DrakenStringArena* src_arena = <const DrakenStringArena*>v.data
    cdef uint32_t k = v.data_length
    cdef uint8_t* live = NULL
    if not dfa_x_live_mask(v, &live):
        return draken_error_sentinel("draken__dfa_extract: allocation failed")

    cdef DrakenStringSlot* out_slots = NULL
    cdef uint8_t* out_unused_arena = NULL
    cdef uint8_t* out_unused_validity = NULL
    cdef uint8_t* block = vecresult_string_block_alloc(
        k, 0, 0, &out_slots, &out_unused_arena, &out_unused_validity)
    if block == NULL:
        draken_free(live)
        return draken_error_sentinel("draken__dfa_extract: output allocation failed")

    cdef uint32_t j
    cdef const DrakenStringSlot* slot
    cdef const char* out_ptr = NULL
    cdef Py_ssize_t out_len = 0
    cdef bint matched
    # Dead slots (no valid row reads them) are never selected: null them, skip the work.
    for j in range(k):
        if live != NULL and not live[j]:
            str_init_null(&out_slots[j])
            continue
        slot = &src_arena.slots[j]
        matched = _execute_procedure(<const char*>str_data(slot, src_arena.arena),
                                     <Py_ssize_t>str_length(slot), proc, &out_ptr, &out_len)
        _dfa_x_slot(&out_slots[j], v, slot, matched, out_ptr, out_len)
    draken_free(live)

    cdef VecResult r = vecresult_from_string_block(block, k, 0, 0, DRAKEN_VARCHAR)
    kernel_borrow_input_arena(r, v, 0)
    if not dfa_x_preserve_shape(&r, v):
        draken_free(block)
        return draken_error_sentinel("draken__dfa_extract: allocation failed")
    return r

# The generic NT_FUNCTION lowering (compiled_expression.pyx) resolves
# "draken_" + func_val.lower() for ANY function name with no special-casing
# needed; for _DFA_EXTRACT that's "draken__dfa_extract" (double underscore).
# Both call arguments are pushed as usual — args[1] is the optimizer's
# compiled-program VARBINARY CONSTANT literal (same shape every row), decoded
# ONCE per kernel call (not per row) via the nogil _decode_procedure above.
# A compressed string input (draken_is_compressed — dict or constant) runs once
# per physical value in _dfa_extract_kernel_compressed above (compression-aware
# kernels directive, 2026-10-18); every other shape takes the uniform
# data[selection[i]] row loop below. Both give the same value per row.
cdef VecResult _dfa_extract_kernel(
    void* ctx, const DrakenVector* const* args, uint32_t nargs,
) noexcept nogil:
    if nargs != 2:
        return draken_error_sentinel("draken__dfa_extract: expected 2 arguments")
    cdef const DrakenVector* v = args[0]
    cdef const DrakenVector* pv = args[1]
    if pv.length == 0:
        return draken_error_sentinel("draken__dfa_extract: missing compiled program")

    cdef const DrakenStringArena* p_arena = <const DrakenStringArena*>pv.data
    cdef const DrakenStringSlot* p_slot = &p_arena.slots[pv.selection[0]]
    cdef const char* program_ptr = <const char*>str_data(p_slot, p_arena.arena)
    cdef Py_ssize_t program_len = <Py_ssize_t>str_length(p_slot)

    cdef DfaProcedure proc
    if not _decode_procedure(program_ptr, program_len, &proc):
        return draken_error_sentinel("draken__dfa_extract: compiled program blob is invalid")

    if draken_is_compressed(v):
        return _dfa_extract_kernel_compressed(v, &proc)

    cdef const DrakenStringArena* src_arena = <const DrakenStringArena*>v.data
    cdef const uint32_t* sel = v.selection
    cdef const uint8_t* nulls = v.validity
    cdef Py_ssize_t n = <Py_ssize_t>v.length

    cdef Py_ssize_t i
    cdef const char* out_ptr = NULL
    cdef Py_ssize_t out_len = 0
    cdef const DrakenStringSlot* slot
    cdef bint matched

    cdef size_t slots_sz = <size_t>n * sizeof(DrakenStringSlot) if n > 0 else sizeof(DrakenStringSlot)
    cdef DrakenStringSlot* out_slots = <DrakenStringSlot*>draken_malloc(slots_sz)
    if out_slots == NULL:
        return draken_error_sentinel("draken__dfa_extract: output allocation failed")

    # Word-padded: vecresult_from_string_buffers copies the padded bitmap width.
    cdef size_t null_bytes = 0
    cdef uint8_t* out_null = NULL
    if nulls != NULL:
        null_bytes = ((<size_t>n + 7) >> 3)
        out_null = <uint8_t*>draken_malloc(((null_bytes + 7) & ~(<size_t>7)) if null_bytes > 0 else 8)
        if out_null == NULL:
            draken_free(out_slots)
            return draken_error_sentinel("draken__dfa_extract: validity allocation failed")
        memset(out_null, 0, ((null_bytes + 7) & ~(<size_t>7)) if null_bytes > 0 else 8)
        memcpy(out_null, nulls, null_bytes)

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            memset(&out_slots[i], 0, sizeof(DrakenStringSlot))
            continue
        slot = &src_arena.slots[sel[i]]
        matched = _execute_procedure(<const char*>str_data(slot, src_arena.arena),
                                     <Py_ssize_t>str_length(slot), &proc, &out_ptr, &out_len)
        _dfa_x_slot(&out_slots[i], v, slot, matched, out_ptr, out_len)

    cdef VecResult r = vecresult_from_string_buffers(
        out_slots, NULL, 0, out_null, <uint32_t>n, DRAKEN_VARCHAR)
    if r.data != NULL:
        kernel_borrow_input_arena(r, v, 0)
    return r


# Register with the C-ABI kernel registry at import time (this module is
# textually included into vector_ops.pyx, which the function registrar
# imports before any bind-time resolution can run — see text.pyx:363).
from draken.ops.kernels._kernel_registry import register_kernel as _register_dfa_kernel
_register_dfa_kernel("draken__dfa_extract", <unsigned long long><size_t>&_dfa_extract_kernel)


cpdef object vector_dfa_extract(
    Vector data,
    Vector compiled_program,
):
    """
    Execute an optimizer-compiled DFA replacement over a string Vector.

    The execution kernel consumes a constant-encoded compiled program blob,
    decodes it once, and interprets the decoded procedure over the input data.
    """
    cdef const char* program_ptr = NULL
    cdef Py_ssize_t program_len = 0
    cdef DfaProcedure proc

    _extract_const_slice(compiled_program, &program_ptr, &program_len)
    if not _decode_procedure(program_ptr, program_len, &proc):
        raise ValueError("vector_dfa_extract: compiled program blob is invalid")

    cdef DrakenVector* uv = data.unified()
    cdef DrakenStringArena* src_arena = <DrakenStringArena*>uv.data

    if draken_is_compressed(uv):
        return _vector_dfa_extract_compressed(uv, src_arena, &proc)

    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef const char* out_ptr = NULL
    cdef Py_ssize_t out_len = 0
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t row_len = 0
    cdef const char* row_ptr = NULL
    cdef Py_ssize_t null_bytes = 0
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen

    # First pass: cache (ptr, len) per row.
    # cap_ptrs[i] == NULL → null row.
    # cap_lens[i] >= 0   → DFA matched; length of capture.
    # cap_lens[i] < 0    → no match; passthrough; actual length = -cap_lens[i] - 1.
    cdef const char** cap_ptrs = <const char**>malloc(<size_t>n * sizeof(const char*))
    cdef int32_t* cap_lens = <int32_t*>malloc(<size_t>n * sizeof(int32_t))

    if cap_ptrs == NULL or cap_lens == NULL:
        free(cap_ptrs)
        free(cap_lens)
        raise MemoryError("vector_dfa_extract: cache allocation failed")

    with nogil:
        for i in range(n):
            if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                cap_ptrs[i] = NULL
                cap_lens[i] = 0
                continue

            slot = &src_arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = str_data(slot, src_arena.arena)
            row_ptr = <const char*>sdata
            row_len = <Py_ssize_t>slen

            if _execute_procedure(row_ptr, row_len, &proc, &out_ptr, &out_len):
                cap_ptrs[i] = out_ptr
                cap_lens[i] = <int32_t>out_len
                # Only long (> 12 byte) strings occupy the arena; inline ones
                # live in the slot. Size the arena to exactly the long bytes.
                if out_len > STR_INLINE_MAX:
                    total_bytes += out_len
            else:
                cap_ptrs[i] = row_ptr
                cap_lens[i] = <int32_t>(-row_len - 1)
                if row_len > STR_INLINE_MAX:
                    total_bytes += row_len

    # Allocate output slots and arena.
    cdef size_t slots_sz = <size_t>n * sizeof(DrakenStringSlot) if n > 0 else sizeof(DrakenStringSlot)
    cdef size_t arena_sz = <size_t>total_bytes if total_bytes > 0 else 1
    cdef DrakenStringSlot* out_slots = <DrakenStringSlot*>draken_malloc(slots_sz)
    cdef uint8_t* out_arena = <uint8_t*>draken_malloc(arena_sz)
    cdef uint8_t* out_null = NULL

    if out_slots == NULL or out_arena == NULL:
        free(cap_ptrs)
        free(cap_lens)
        draken_free(out_slots)
        draken_free(out_arena)
        raise MemoryError("vector_dfa_extract: output allocation failed")

    if nulls != NULL:
        null_bytes = (n + 7) >> 3
        out_null = <uint8_t*>draken_malloc(<size_t>null_bytes if null_bytes > 0 else 1)
        if out_null == NULL:
            free(cap_ptrs)
            free(cap_lens)
            draken_free(out_slots)
            draken_free(out_arena)
            raise MemoryError("vector_dfa_extract: validity allocation failed")
        memcpy(out_null, nulls, <size_t>null_bytes)

    # Second pass: fill slots and arena.
    cdef const char* cached_ptr
    cdef int32_t cached_len
    cdef uint32_t copy_len
    cdef size_t arena_used = 0

    cdef DrakenStringSlot* psrc
    with nogil:
        for i in range(n):
            cached_ptr = cap_ptrs[i]
            cached_len = cap_lens[i]

            if cached_ptr == NULL:
                str_init_null(&out_slots[i])
                continue

            if cached_len >= 0:
                # Matched capture: a fresh substring, hash not known — build it.
                copy_len = <uint32_t>cached_len
                if copy_len > <uint32_t>STR_INLINE_MAX:
                    memcpy(out_arena + arena_used, cached_ptr, <size_t>copy_len)
                    draken_build_string_slot(&out_slots[i], <const uint8_t*>cached_ptr, copy_len, <uint32_t>arena_used)
                    arena_used += <size_t>copy_len
                else:
                    draken_build_string_slot(&out_slots[i], <const uint8_t*>cached_ptr, copy_len, 0)
            else:
                # Passthrough: identical bytes to the source row. Clone the source
                # slot verbatim (precomputed prefix + hash32) instead of rehashing.
                psrc = &src_arena.slots[sel[i]]
                if str_is_inline(psrc):
                    str_clone_with_offset(&out_slots[i], psrc, 0)
                else:
                    copy_len = <uint32_t>(-cached_len - 1)
                    memcpy(out_arena + arena_used, cached_ptr, <size_t>copy_len)
                    str_clone_with_offset(&out_slots[i], psrc, <uint32_t>arena_used)
                    arena_used += <size_t>copy_len

    free(cap_ptrs)
    free(cap_lens)

    # NOTE: removed an OPTERYX_DEBUG_VALIDATE_SLOTS debug-validation block here
    # that referenced str_is_inline (no longer in draken's exposed surface).
    # If debug arena-bounds validation is wanted again, expose str_is_inline
    # via buffers.pxd or write the slot inspection in C++.

    return draken_vector_own_string(out_slots, out_arena, arena_used, out_null, <uint32_t>n, DRAKEN_VARCHAR)
