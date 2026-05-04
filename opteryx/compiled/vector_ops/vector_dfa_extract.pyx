# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

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

Program blobs are expected to be optimizer-produced constant literals.
Malformed or unsupported blobs raise ValueError.

This file is designed to be included from `vector_ops.pyx`.
"""

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, uint8_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcmp, memcpy, memset

from cpython.bytes cimport PyBytes_AsStringAndSize
from libcpp.string cimport string

cdef extern from "simd_search.h":
    int neon_search(const char* data, size_t length, char target)
    int avx_search(const char* data, size_t length, char target)

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

import platform

cdef int (*simd_find_char)(const char*, size_t, char)

_arch = platform.machine().lower()
if _arch in ("arm64", "aarch64"):
    simd_find_char = neon_search
else:
    simd_find_char = avx_search

from draken.core.buffers cimport (
    DrakenVarBuffer,
    DRAKEN_ENCODING_DICTIONARY,
)
from draken.vectors.string_vector cimport StringVector, from_packed_dict


cdef enum DfaOpType:
    DFA_OP_CONSUME_LITERAL = 1
    DFA_OP_CONSUME_OPTIONAL_LITERAL = 2
    DFA_OP_CAPTURE_UNTIL_CHAR = 3
    DFA_OP_CONSUME_TO_END = 4
    DFA_OP_RETURN_CAPTURE = 5


cdef struct DfaOp:
    int op_type
    const char* literal
    Py_ssize_t literal_len
    char target_char


cdef struct DfaProcedure:
    DfaOp ops[8]
    int op_count


cdef struct DfaProgramBuilder:
    uint8_t op_types[8]
    const char* literals[8]
    Py_ssize_t literal_lens[8]
    char target_chars[8]
    char literal_storage[8][64]
    int op_count


cdef inline uint8_t _read_u8(const char** p) noexcept:
    cdef uint8_t value = (<const uint8_t*>p[0])[0]
    p[0] += 1
    return value


cdef inline uint32_t _read_u32(const char** p) noexcept:
    cdef const uint8_t* src = <const uint8_t*>p[0]
    cdef uint32_t value = (
        <uint32_t>src[0]
        | (<uint32_t>src[1] << 8)
        | (<uint32_t>src[2] << 16)
        | (<uint32_t>src[3] << 24)
    )
    p[0] += 4
    return value


cdef inline void _decode_procedure(
    const char* program_ptr,
    Py_ssize_t program_len,
    DfaProcedure* proc,
) except *:
    cdef const char* p = program_ptr
    cdef const char* end = program_ptr + program_len
    cdef uint8_t version
    cdef uint8_t op_count
    cdef uint8_t op_type
    cdef uint32_t literal_len
    cdef int i

    if program_ptr == NULL or program_len < 2:
        raise ValueError("vector_dfa_extract: compiled program blob is invalid")

    version = _read_u8(&p)
    if version != 1:
        raise ValueError("vector_dfa_extract: unsupported compiled program version")

    op_count = _read_u8(&p)
    if op_count == 0 or op_count > 8:
        raise ValueError("vector_dfa_extract: compiled program op count is invalid")

    for i in range(op_count):
        if p >= end:
            raise ValueError("vector_dfa_extract: compiled program truncated")

        op_type = _read_u8(&p)
        proc.ops[i].op_type = op_type
        proc.ops[i].literal = NULL
        proc.ops[i].literal_len = 0
        proc.ops[i].target_char = <char>0

        if op_type == DFA_OP_CONSUME_LITERAL or op_type == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            if end - p < 4:
                raise ValueError("vector_dfa_extract: compiled literal header truncated")
            literal_len = _read_u32(&p)
            if literal_len == 0:
                raise ValueError("vector_dfa_extract: compiled literal length is invalid")
            if end - p < literal_len:
                raise ValueError("vector_dfa_extract: compiled literal payload truncated")
            proc.ops[i].literal = p
            proc.ops[i].literal_len = <Py_ssize_t>literal_len
            p += literal_len

        elif op_type == DFA_OP_CAPTURE_UNTIL_CHAR:
            if p >= end:
                raise ValueError("vector_dfa_extract: compiled capture target missing")
            proc.ops[i].target_char = <char>_read_u8(&p)

        elif op_type == DFA_OP_CONSUME_TO_END or op_type == DFA_OP_RETURN_CAPTURE:
            pass

        else:
            raise ValueError("vector_dfa_extract: compiled program contains unsupported opcode")

    if p != end:
        raise ValueError("vector_dfa_extract: compiled program has trailing bytes")

    proc.op_count = op_count


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
    if re.op() == kRegexpQuest and re.nsub() == 1:
        return _is_ascii_literal_regexp(re.sub()[0])
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
        repeated = inner

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
            if child.nsub() == 1 and child.sub()[0] != NULL:
                if child.op() == kRegexpStar or child.op() == kRegexpPlus:
                    if (
                        child.sub()[0].op() == kRegexpAnyChar
                        or child.sub()[0].op() == kRegexpAnyByte
                        or child.sub()[0].op() == kRegexpCharClass
                    ):
                        if not _builder_append_simple(builder, DFA_OP_CONSUME_TO_END):
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

    if re.nsub() == 1 and re.sub()[0] != NULL:
        if re.op() == kRegexpStar or re.op() == kRegexpPlus:
            if (
                re.sub()[0].op() == kRegexpAnyChar
                or re.sub()[0].op() == kRegexpAnyByte
                or re.sub()[0].op() == kRegexpCharClass
            ):
                return _builder_append_simple(builder, DFA_OP_CONSUME_TO_END)

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
        elif builder.op_types[i] == DFA_OP_CAPTURE_UNTIL_CHAR:
            total_len += 1

    program = bytes(total_len)
    out_ptr = <char*>program

    _write_u8(&out_ptr, 1)
    _write_u8(&out_ptr, <uint8_t>builder.op_count)

    for i in range(builder.op_count):
        _write_u8(&out_ptr, builder.op_types[i])
        if builder.op_types[i] == DFA_OP_CONSUME_LITERAL or builder.op_types[i] == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            _write_u32(&out_ptr, <uint32_t>builder.literal_lens[i])
            memcpy(out_ptr, builder.literals[i], <size_t>builder.literal_lens[i])
            out_ptr += builder.literal_lens[i]
        elif builder.op_types[i] == DFA_OP_CAPTURE_UNTIL_CHAR:
            _write_u8(&out_ptr, <uint8_t>builder.target_chars[i])

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
    StringVector vec,
    const char** data_ptr,
    Py_ssize_t* data_len,
) except *:
    if not vec._has_const:
        raise ValueError("vector_dfa_extract: compiled program must be constant encoded")
    if vec._const_is_null or vec._const_value == NULL:
        raise ValueError("vector_dfa_extract: compiled program must be non-null")

    data_ptr[0] = <const char*>vec._const_value.data
    data_len[0] = <Py_ssize_t>vec._const_value.length





cdef inline bint _execute_procedure(
    const char* src,
    Py_ssize_t src_len,
    DfaProcedure* proc,
    const char** out_ptr,
    Py_ssize_t* out_len,
) noexcept:
    cdef const char* p = src
    cdef const char* end = src + src_len
    cdef const char* capture_ptr = NULL
    cdef Py_ssize_t capture_len = 0
    cdef int i
    cdef int char_pos
    cdef DfaOp* op
    cdef Py_ssize_t remaining
    cdef const char* scan

    out_ptr[0] = NULL
    out_len[0] = 0

    for i in range(proc.op_count):
        op = &proc.ops[i]

        if op.op_type == DFA_OP_CONSUME_LITERAL:
            remaining = <Py_ssize_t>(end - p)
            if remaining < op.literal_len:
                return False
            if memcmp(p, op.literal, <size_t>op.literal_len) != 0:
                return False
            p += op.literal_len

        elif op.op_type == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            remaining = <Py_ssize_t>(end - p)
            if remaining >= op.literal_len and memcmp(p, op.literal, <size_t>op.literal_len) == 0:
                p += op.literal_len

        elif op.op_type == DFA_OP_CAPTURE_UNTIL_CHAR:
            if p >= end:
                return False
            remaining = <Py_ssize_t>(end - p)
            if remaining <= 0:
                return False
            char_pos = simd_find_char(p, <size_t>remaining, op.target_char)
            if char_pos < 0:
                return False
            if char_pos == 0:
                return False
            scan = p + char_pos
            if scan < p or scan >= end:
                return False
            capture_ptr = p
            capture_len = <Py_ssize_t>(scan - p)
            p = scan

        elif op.op_type == DFA_OP_CONSUME_TO_END:
            if p >= end:
                return False
            p = end

        elif op.op_type == DFA_OP_RETURN_CAPTURE:
            if capture_ptr == NULL:
                return False
            if p != end:
                return False
            out_ptr[0] = capture_ptr
            out_len[0] = capture_len
            return True

        else:
            return False

    return False


cpdef StringVector vector_dfa_extract(
    StringVector data,
    StringVector compiled_program,
):
    """
    Execute an optimizer-compiled DFA replacement over a StringVector.

    The execution kernel consumes a constant-encoded compiled program blob,
    decodes it once, and interprets the decoded procedure over the input data.

    Encoding behavior:
    - constant input    -> execute once, replicate result/null
    - dictionary input  -> transform dictionary values once, preserve row codes
    - dense input       -> row-by-row native execution
    """
    cdef const char* program_ptr = NULL
    cdef Py_ssize_t program_len = 0
    cdef DfaProcedure proc

    _extract_const_slice(compiled_program, &program_ptr, &program_len)
    _decode_procedure(program_ptr, program_len, &proc)

    cdef Py_ssize_t n = data.ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef const char* out_ptr = NULL
    cdef Py_ssize_t out_len = 0
    cdef DrakenVarBuffer* ptr
    cdef StringVector out_vec
    cdef DrakenVarBuffer* out_ptr_buf
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t row_len = 0
    cdef const char* row_ptr = NULL
    cdef Py_ssize_t write_offset = 0
    cdef Py_ssize_t null_bytes = 0
    cdef Py_ssize_t dict_size
    cdef object new_dict_sv

    # Constant encoding: execute once, replicate.
    if data._has_const:
        if data._const_is_null or data._const_value == NULL:
            out_vec = StringVector(n, 0)
            out_ptr_buf = out_vec.ptr
            null_bytes = (n + 7) >> 3
            if null_bytes > 0 and out_ptr_buf.null_bitmap != NULL:
                memset(out_ptr_buf.null_bitmap, 0, <size_t>null_bytes)
            for i in range(n + 1):
                out_ptr_buf.offsets[i] = 0
            return out_vec

        row_ptr = <const char*>data._const_value.data
        row_len = <Py_ssize_t>data._const_value.length
        if _execute_procedure(
            row_ptr,
            row_len,
            &proc,
            &out_ptr,
            &out_len,
        ):
            total_bytes = out_len * n
        else:
            out_ptr = row_ptr
            out_len = row_len
            total_bytes = row_len * n

        out_vec = StringVector(n, total_bytes)
        out_ptr_buf = out_vec.ptr
        if out_ptr_buf.null_bitmap != NULL:
            null_bytes = (n + 7) >> 3
            if null_bytes > 0:
                memset(out_ptr_buf.null_bitmap, 0xFF, <size_t>null_bytes)

        write_offset = 0
        out_ptr_buf.offsets[0] = 0
        for i in range(n):
            if out_len > 0:
                memcpy((<char*>out_ptr_buf.data) + write_offset, out_ptr, <size_t>out_len)
            write_offset += out_len
            out_ptr_buf.offsets[i + 1] = <int32_t>write_offset
        return out_vec

    # Dictionary encoding: apply DFA once per unique entry, repack with same codes.
    # DFA output is always <= input (capture is a substring, passthrough is the
    # original), so pre-allocate from the original dict byte total and write in
    # a single pass — no size-counting pass needed.
    if data._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_size = data._dict_values.length
        new_dict_sv = StringVector(dict_size, <Py_ssize_t>data._dict_values.offsets[dict_size])
        out_ptr_buf = (<StringVector>new_dict_sv).ptr
        write_offset = 0
        out_ptr_buf.offsets[0] = 0
        for i in range(dict_size):
            start = data._dict_values.offsets[i]
            end = data._dict_values.offsets[i + 1]
            row_ptr = (<const char*>data._dict_values.data) + start
            row_len = <Py_ssize_t>(end - start)
            if _execute_procedure(row_ptr, row_len, &proc, &out_ptr, &out_len):
                if out_len > 0:
                    memcpy((<char*>out_ptr_buf.data) + write_offset, out_ptr, <size_t>out_len)
                write_offset += out_len
            else:
                if row_len > 0:
                    memcpy((<char*>out_ptr_buf.data) + write_offset, row_ptr, <size_t>row_len)
                write_offset += row_len
            out_ptr_buf.offsets[i + 1] = <int32_t>write_offset
        return from_packed_dict(
            data._dict_codes, data._dict_code_width, n,
            out_ptr_buf.offsets, <const uint8_t*>out_ptr_buf.data, dict_size,
            data._dict_accessor.row_nulls,
        )

    ptr = data.ptr

    # Cache (capture_offset_from_data_start, capture_len) per row so the write
    # pass never calls _execute_procedure again — pure array reads + memcpy.
    # Negative capture_len signals "no match; emit original row".
    cdef int32_t* cap_offsets = <int32_t*>malloc(<size_t>n * sizeof(int32_t))
    cdef int32_t* cap_lens = <int32_t*>malloc(<size_t>n * sizeof(int32_t))

    if cap_offsets == NULL or cap_lens == NULL:
        free(cap_offsets)
        free(cap_lens)
        raise MemoryError("vector_dfa_extract: cache allocation failed")

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
            cap_offsets[i] = 0
            cap_lens[i] = 0
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        row_ptr = (<const char*>ptr.data) + start
        row_len = <Py_ssize_t>(end - start)

        if _execute_procedure(row_ptr, row_len, &proc, &out_ptr, &out_len):
            cap_offsets[i] = <int32_t>(out_ptr - (<const char*>ptr.data))
            cap_lens[i] = <int32_t>out_len
            total_bytes += out_len
        else:
            cap_offsets[i] = start
            cap_lens[i] = <int32_t>(-row_len - 1)  # sentinel: negative means passthrough
            total_bytes += row_len

    out_vec = StringVector(n, total_bytes)
    out_ptr_buf = out_vec.ptr

    if ptr.null_bitmap != NULL and out_ptr_buf.null_bitmap != NULL:
        null_bytes = (n + 7) >> 3
        if null_bytes > 0:
            memcpy(out_ptr_buf.null_bitmap, ptr.null_bitmap, <size_t>null_bytes)
    elif out_ptr_buf.null_bitmap != NULL:
        null_bytes = (n + 7) >> 3
        if null_bytes > 0:
            memset(out_ptr_buf.null_bitmap, 0xFF, <size_t>null_bytes)

    write_offset = 0
    out_ptr_buf.offsets[0] = 0

    cdef int32_t cached_off, cached_len, copy_len

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
            out_ptr_buf.offsets[i + 1] = <int32_t>write_offset
            continue

        cached_off = cap_offsets[i]
        cached_len = cap_lens[i]

        if cached_len >= 0:
            # DFA matched: emit the cached capture slice
            copy_len = cached_len
            if copy_len > 0:
                memcpy(
                    (<char*>out_ptr_buf.data) + write_offset,
                    (<const char*>ptr.data) + cached_off,
                    <size_t>copy_len,
                )
            write_offset += copy_len
        else:
            # Passthrough: negative sentinel encodes original row length as (-len - 1)
            copy_len = <int32_t>(-cached_len - 1)
            if copy_len > 0:
                memcpy(
                    (<char*>out_ptr_buf.data) + write_offset,
                    (<const char*>ptr.data) + cached_off,
                    <size_t>copy_len,
                )
            write_offset += copy_len

        out_ptr_buf.offsets[i + 1] = <int32_t>write_offset

    free(cap_offsets)
    free(cap_lens)

    return out_vec
