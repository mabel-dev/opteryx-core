# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""
Key serialization for compact binary representation of group keys.
Converts Python key components (int, str, float, None, date, etc.) to bytes.
"""

import struct


cpdef bytes serialize_key_components(list components):
    """
    Serialize a list of key components to bytes.

    Each component can be: int, float, str, bytes, None, or date-like objects.
    Returns a bytes object suitable for use as a dict key.

    Format:
    - 4 bytes: number of components (uint32_t, little-endian)
    - For each component:
      - 1 byte: type tag
      - N bytes: data (variable length)
    """
    serialized = []

    for component in components:
        # Dispatch based on type
        if component is None:
            # Type tag for NULL
            serialized.append(bytes([0]))
        elif isinstance(component, bool):
            # Must check bool before int since bool is subclass of int
            serialized.append(bytes([3]))  # TYPE_BOOL
            serialized.append(bytes([1 if component else 0]))
        elif isinstance(component, int):
            serialized.append(bytes([1]))  # TYPE_INT64
            serialized.append(struct.pack('<q', component))
        elif isinstance(component, float):
            serialized.append(bytes([2]))  # TYPE_FLOAT64
            serialized.append(struct.pack('<d', component))
        elif isinstance(component, (str, bytes)):
            serialized.append(bytes([4]))  # TYPE_BYTES
            if isinstance(component, str):
                b = component.encode('utf-8')
            else:
                b = component
            # Store length as uint32_t little-endian, then data
            serialized.append(struct.pack('<I', len(b)))
            serialized.append(b)
        else:
            # Fallback: convert to string representation
            serialized.append(bytes([4]))  # TYPE_BYTES
            s = str(component)
            b = s.encode('utf-8')
            serialized.append(struct.pack('<I', len(b)))
            serialized.append(b)

    # Concatenate all components with count prefix
    result = struct.pack('<I', len(components))
    for component_bytes in serialized:
        result += component_bytes

    return result


cpdef list deserialize_key_components(bytes data):
    """
    Deserialize key components from bytes back to Python objects.

    Used during finalization to reconstruct the original group keys for output.

    Format:
    - 4 bytes: number of components (uint32_t, little-endian)
    - For each component:
      - 1 byte: type tag
      - N bytes: data (variable length)
    """
    pos = 0
    result = []

    # Read component count
    if len(data) < 4:
        raise ValueError("Invalid serialized key: too short")

    count = struct.unpack('<I', data[0:4])[0]
    pos = 4

    for _ in range(count):
        if pos >= len(data):
            raise ValueError("Invalid serialized key: truncated")

        type_tag = data[pos]
        pos += 1

        if type_tag == 0:  # TYPE_NULL
            result.append(None)
        elif type_tag == 1:  # TYPE_INT64
            if pos + 8 > len(data):
                raise ValueError("Invalid serialized key: truncated int64")
            value = struct.unpack('<q', data[pos:pos+8])[0]
            result.append(int(value))
            pos += 8
        elif type_tag == 2:  # TYPE_FLOAT64
            if pos + 8 > len(data):
                raise ValueError("Invalid serialized key: truncated float64")
            value = struct.unpack('<d', data[pos:pos+8])[0]
            result.append(float(value))
            pos += 8
        elif type_tag == 3:  # TYPE_BOOL
            if pos + 1 > len(data):
                raise ValueError("Invalid serialized key: truncated bool")
            result.append(bool(data[pos]))
            pos += 1
        elif type_tag == 4:  # TYPE_BYTES
            if pos + 4 > len(data):
                raise ValueError("Invalid serialized key: truncated length")
            length = struct.unpack('<I', data[pos:pos+4])[0]
            pos += 4
            if pos + length > len(data):
                raise ValueError("Invalid serialized key: truncated data")
            result.append(bytes(data[pos:pos+length]))
            pos += length
        else:
            raise ValueError(f"Unknown type tag in serialized key: {type_tag}")

    return result
