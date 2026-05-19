# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport uint8_t, uint32_t, int8_t, int32_t
from libc.string cimport memset

from draken.vectors.string_vector cimport StringVector
from draken.vectors.bool_vector cimport BoolVector
from draken.core.buffers cimport DrakenVarBuffer, DrakenVector, DrakenConstantStringPayload
from cpython.bytes cimport PyBytes_FromStringAndSize


cdef inline int parse_ip_to_int(const char* ip, size_t length, uint32_t* out) nogil:
    """
    Convert an IPv4 string (not NUL-terminated) to uint32.
    Returns 0 on success, -1 on parse error (invalid IP).
    """
    cdef uint32_t result = 0
    cdef uint32_t num
    cdef int8_t shift = 24
    cdef size_t i = 0
    cdef char c
    cdef int octet_count = 0
    cdef int digit_count

    while octet_count < 4:
        num = 0
        digit_count = 0
        while i < length:
            c = ip[i]
            if c < 48 or c > 57:  # not a digit
                break
            num = num * 10 + (c - 48)
            digit_count += 1
            i += 1
        if digit_count == 0:
            return -1  # empty octet
        if num > 255:
            return -1  # octet out of range
        result += num << shift
        shift -= 8
        octet_count += 1
        if octet_count < 4:
            if i >= length or ip[i] != 46:  # 46 = '.'
                return -1  # missing dot or extra chars
            i += 1  # skip '.'
        else:
            if i < length:
                return -1  # trailing garbage

    out[0] = result
    return 0


cpdef BoolVector vector_ip_in_cidr(StringVector vec, StringVector cidr):
    """
    Check if each IP address in vec falls within a CIDR block.

    Parameters:
        vec: StringVector of IP address strings.
        cidr: CIDR notation as a constant-encoded StringVector.

    Returns:
        BoolVector: True where the IP is inside the CIDR block.
    """
    from opteryx.exceptions import IncorrectTypeError

    cdef DrakenVector* _cidr_uv = cidr.unified()
    cdef DrakenConstantStringPayload* _cidr_csp
    cdef bytes cidr_bytes

    if cidr.ptr.offsets != NULL or cidr._german_dict_values != NULL:  # constant iff both are NULL
        raise IncorrectTypeError("CIDR argument must be constant encoded StringVector")
    if _cidr_uv.validity != NULL:
        raise ValueError("CIDR argument must not be NULL")
    _cidr_csp = <DrakenConstantStringPayload*>_cidr_uv.data
    cidr_bytes = PyBytes_FromStringAndSize(<const char*>_cidr_csp.data, _cidr_csp.length)

    cdef int slash_idx = cidr_bytes.find(b'/')
    if slash_idx == -1:
        raise ValueError("Invalid CIDR notation: missing /")
    cdef int mask_size = int(cidr_bytes[slash_idx + 1 :])
    if mask_size < 0 or mask_size > 32:
        raise ValueError("Invalid CIDR notation: mask out of range")

    cdef bytes base_ip_bytes = cidr_bytes[:slash_idx]
    cdef uint32_t netmask = (0xFFFFFFFF << (32 - mask_size)) & 0xFFFFFFFF
    cdef uint32_t base_ip = 0
    if parse_ip_to_int(base_ip_bytes, len(base_ip_bytes), &base_ip) != 0:
        raise ValueError(
            f"Invalid CIDR base address: {base_ip_bytes.decode('ascii', 'replace')}"
        )

    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3

    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    memset(dst, 0, nbytes)

    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint32_t ip_int

    for i in range(n):
        # Skip nulls
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        if end <= start:
            continue
        if parse_ip_to_int(
            <const char*>ptr.data + start, <size_t>(end - start), &ip_int
        ) != 0:
            raise ValueError(
                f"Invalid IP address: "
                f"{(<char*>ptr.data + start)[:end - start].decode('ascii', 'replace')}"
            )
        if (ip_int & netmask) == base_ip:
            dst[i >> 3] |= (<uint8_t>1 << (i & 7))

    return out
