# Shared by the CSV and JSONL readers: a pushed predicate literal's KIND is its
# Python type, decided once here and carried to C++ beside its text. The C++ side
# (predicate_literal.hpp) checks that kind against the column's type and raises on
# a mismatch — the literal's text is never re-sniffed to decide what it is, which
# is what let the string '1' compare as a number.

from libc.stdint cimport uint8_t


cdef extern from "predicate_literal.hpp" namespace "rugo":
    cdef enum LiteralKind:
        LITERAL_STRING
        LITERAL_INT
        LITERAL_FLOAT
        LITERAL_BOOL


cdef tuple _predicate_literal(col, op, val):
    """Return (kind, text bytes) for one predicate literal, or raise ValueError.

    None is refused (a comparison with NULL matches no row — 'is null' / 'is not
    null' spell a null test), and so is any type outside str / bytes / int / float /
    bool: there is no rule for comparing it, and str() of it is not one.
    """
    if val is None:
        raise ValueError(
            f"predicate {op!r} on {col!r} cannot take None — a comparison with NULL "
            f"matches no row; use 'is null' / 'is not null'"
        )
    # bool before int: bool is an int subclass, and True must not compare as 1.
    if isinstance(val, bool):
        # JSON's spelling, not Python's str(True) == "True".
        return <uint8_t>LITERAL_BOOL, (b'true' if val else b'false')
    if isinstance(val, int):
        return <uint8_t>LITERAL_INT, str(val).encode('utf-8')
    if isinstance(val, float):
        return <uint8_t>LITERAL_FLOAT, repr(val).encode('utf-8')
    if isinstance(val, bytes):
        # Opteryx's bound VARCHAR literals arrive as bytes; str(b'x') would be the
        # repr "b'x'", not the string's own bytes.
        return <uint8_t>LITERAL_STRING, val
    if isinstance(val, str):
        return <uint8_t>LITERAL_STRING, val.encode('utf-8')
    raise ValueError(
        f"predicate {op!r} on {col!r}: unsupported literal type "
        f"{type(val).__name__} ({val!r}); a predicate literal must be str, bytes, "
        f"int, float or bool"
    )
