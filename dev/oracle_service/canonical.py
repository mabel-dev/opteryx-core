"""
Canonical value encoding shared by every format. See README.md for the contract.

Values are encoded from the SCHEMA, never by inspecting a Python value's type:
64-bit integers, decimals, temporals, binary and floats become tagged strings so
no JSON number can silently lose precision, a NaN payload, or the sign of zero.
"""

import base64
import struct

from .errors import Unsupported

# float kind -> (struct format, byte width, max significant digits, smallest magnitude that
# overflows to infinity when packed: struct raises instead of rounding, so candidates at or
# above it are skipped)
_FLOATS = {
    "f64": ("<d", 8, 17, float("inf")),
    "f32": ("<f", 4, 9, 3.4028235677973366e38),
    "f16": ("<e", 2, 5, 65520.0),
}


def encode_i64(value: int) -> str:
    return "i64:%d" % value


def encode_u64(value: int) -> str:
    return "u64:%d" % value


def encode_binary(value: bytes) -> str:
    return "b64:" + base64.b64encode(value).decode("ascii")


def encode_unit_int(tag: str, unit: str, value: int) -> str:
    return "%s[%s]:%d" % (tag, unit, value)


def decimal_text(unscaled: int, scale: int) -> str:
    """Exact decimal rendering of unscaled * 10**-scale; always `scale` fractional digits."""
    sign = "-" if unscaled < 0 else ""
    digits = str(abs(unscaled))
    if scale > 0:
        digits = digits.zfill(scale + 1)
        return "%s%s.%s" % (sign, digits[:-scale], digits[-scale:])
    return sign + digits + "0" * (-scale)


def encode_decimal(unscaled: int, scale: int) -> str:
    return "dec:" + decimal_text(unscaled, scale)


def encode_float_bits(kind: str, bits: int) -> str:
    """`<kind>:<hex bit pattern>:<readable>`; the hex is authoritative, the text is for people."""
    fmt, width, digits, overflow = _FLOATS[kind]
    raw = bits.to_bytes(width, "little")
    value = struct.unpack(fmt, raw)[0]
    if value != value:
        text = "nan"
    elif value == float("inf"):
        text = "inf"
    elif value == float("-inf"):
        text = "-inf"
    else:
        text = repr(value)
        for n in range(1, digits + 1):
            candidate = "%.*g" % (n, value)
            if abs(float(candidate)) < overflow and struct.pack(fmt, float(candidate)) == raw:
                text = candidate
                break
        if "e" not in text and "." not in text:
            text += ".0"
    return "%s:%0*x:%s" % (kind, width * 2, bits, text)


def encode_float_value(kind: str, value: float) -> str:
    """Encode an already-decoded Python float (Avro readers hand floats back, not bits)."""
    fmt, width, _digits, _overflow = _FLOATS[kind]
    bits = int.from_bytes(struct.pack(fmt, value), "little")
    return encode_float_bits(kind, bits)
