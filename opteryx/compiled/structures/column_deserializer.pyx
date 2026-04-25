# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""
Deserialize column descriptors from MemoryPool into Draken vectors.

Reverses the serialization from column_descriptor, reading raw bytes from
MemoryPool and constructing typed Draken vectors.
"""

from libc.stdint cimport int32_t, int64_t
import struct
import json
from typing import Dict, Any

from opteryx.compiled.structures.memory_pool cimport MemoryPool
from opteryx.compiled.structures.column_descriptor cimport ColumnDescriptor
from opteryx.compiled.draken.vectors.integer_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector


cpdef dict deserialize_column(ColumnDescriptor descriptor, MemoryPool pool):
    """Deserialize a column from MemoryPool using descriptor.

    Returns a dict with:
      - 'vector': The Draken vector (Int64Vector, StringVector, etc.)
      - 'null_bitmap': Arrow-style validity bitmap
      - 'null_count': Number of nulls
    """
    if descriptor.ref_id < 0:
        raise ValueError(f"Invalid ref_id: {descriptor.ref_id}")

    # Read raw bytes from MemoryPool
    raw_bytes = pool.read(descriptor.ref_id, False, False)
    if not raw_bytes:
        raise ValueError(f"Failed to read column {descriptor.column_name} from pool")

    col_type = descriptor.column_type

    # Deserialize based on type
    if col_type == "int64":
        return _deserialize_int64(raw_bytes, descriptor)
    elif col_type == "int32":
        return _deserialize_int32(raw_bytes, descriptor)
    elif col_type == "float64":
        return _deserialize_float64(raw_bytes, descriptor)
    elif col_type == "float32":
        return _deserialize_float32(raw_bytes, descriptor)
    elif col_type == "boolean":
        return _deserialize_boolean(raw_bytes, descriptor)
    elif col_type == "string":
        return _deserialize_string(raw_bytes, descriptor)
    else:
        raise ValueError(f"Unsupported column type: {col_type}")


cdef _deserialize_int64(bytes raw_bytes, ColumnDescriptor descriptor):
    """Deserialize int64 column."""
    cdef int offset = 0

    # Parse header
    type_len = struct.unpack('<I', raw_bytes[offset:offset+4])[0]
    offset += 4
    offset += type_len  # skip type string

    num_rows = struct.unpack('<q', raw_bytes[offset:offset+8])[0]
    offset += 8

    null_bitmap_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    null_bitmap = raw_bytes[offset:offset+null_bitmap_len]
    offset += null_bitmap_len

    data_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    data_bytes = raw_bytes[offset:offset+data_len]

    # Unpack int64 array
    cdef int num_values = data_len // 8
    cdef list values = []
    for i in range(num_values):
        val = struct.unpack('<q', data_bytes[i*8:(i+1)*8])[0]
        values.append(val)

    # Create Int64Vector
    vector = Int64Vector.from_list(values)

    return {
        'vector': vector,
        'null_bitmap': null_bitmap,
        'null_count': descriptor.null_count,
    }


cdef _deserialize_int32(bytes raw_bytes, ColumnDescriptor descriptor):
    """Deserialize int32 column."""
    cdef int offset = 0

    # Parse header
    type_len = struct.unpack('<I', raw_bytes[offset:offset+4])[0]
    offset += 4
    offset += type_len

    num_rows = struct.unpack('<q', raw_bytes[offset:offset+8])[0]
    offset += 8

    null_bitmap_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    null_bitmap = raw_bytes[offset:offset+null_bitmap_len]
    offset += null_bitmap_len

    data_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    data_bytes = raw_bytes[offset:offset+data_len]

    # Unpack int32 array
    cdef int num_values = data_len // 4
    cdef list values = []
    for i in range(num_values):
        val = struct.unpack('<i', data_bytes[i*4:(i+1)*4])[0]
        values.append(val)

    # Create Int64Vector (convert from int32)
    vector = Int64Vector.from_list([int(v) for v in values])

    return {
        'vector': vector,
        'null_bitmap': null_bitmap,
        'null_count': descriptor.null_count,
    }


cdef _deserialize_float64(bytes raw_bytes, ColumnDescriptor descriptor):
    """Deserialize float64 column - stored as Int64Vector with bitcast."""
    cdef int offset = 0

    # Parse header
    type_len = struct.unpack('<I', raw_bytes[offset:offset+4])[0]
    offset += 4
    offset += type_len

    num_rows = struct.unpack('<q', raw_bytes[offset:offset+8])[0]
    offset += 8

    null_bitmap_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    null_bitmap = raw_bytes[offset:offset+null_bitmap_len]
    offset += null_bitmap_len

    data_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    data_bytes = raw_bytes[offset:offset+data_len]

    # Unpack float64 array
    cdef int num_values = data_len // 8
    cdef list values = []
    for i in range(num_values):
        val = struct.unpack('<d', data_bytes[i*8:(i+1)*8])[0]
        values.append(val)

    # For now, store as list - real implementation would create Float64Vector
    vector = values

    return {
        'vector': vector,
        'null_bitmap': null_bitmap,
        'null_count': descriptor.null_count,
    }


cdef _deserialize_float32(bytes raw_bytes, ColumnDescriptor descriptor):
    """Deserialize float32 column."""
    cdef int offset = 0

    # Parse header
    type_len = struct.unpack('<I', raw_bytes[offset:offset+4])[0]
    offset += 4
    offset += type_len

    num_rows = struct.unpack('<q', raw_bytes[offset:offset+8])[0]
    offset += 8

    null_bitmap_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    null_bitmap = raw_bytes[offset:offset+null_bitmap_len]
    offset += null_bitmap_len

    data_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    data_bytes = raw_bytes[offset:offset+data_len]

    # Unpack float32 array
    cdef int num_values = data_len // 4
    cdef list values = []
    for i in range(num_values):
        val = struct.unpack('<f', data_bytes[i*4:(i+1)*4])[0]
        values.append(val)

    vector = values

    return {
        'vector': vector,
        'null_bitmap': null_bitmap,
        'null_count': descriptor.null_count,
    }


cdef _deserialize_boolean(bytes raw_bytes, ColumnDescriptor descriptor):
    """Deserialize boolean column."""
    cdef int offset = 0

    # Parse header
    type_len = struct.unpack('<I', raw_bytes[offset:offset+4])[0]
    offset += 4
    offset += type_len

    num_rows = struct.unpack('<q', raw_bytes[offset:offset+8])[0]
    offset += 8

    null_bitmap_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    null_bitmap = raw_bytes[offset:offset+null_bitmap_len]
    offset += null_bitmap_len

    data_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    data_bytes = raw_bytes[offset:offset+data_len]

    # Unpack boolean array
    cdef list values = [bool(b) for b in data_bytes]

    vector = values

    return {
        'vector': vector,
        'null_bitmap': null_bitmap,
        'null_count': descriptor.null_count,
    }


cdef _deserialize_string(bytes raw_bytes, ColumnDescriptor descriptor):
    """Deserialize string column."""
    cdef int offset = 0

    # Parse header
    type_len = struct.unpack('<I', raw_bytes[offset:offset+4])[0]
    offset += 4
    offset += type_len

    num_rows = struct.unpack('<q', raw_bytes[offset:offset+8])[0]
    offset += 8

    null_bitmap_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    null_bitmap = raw_bytes[offset:offset+null_bitmap_len]
    offset += null_bitmap_len

    data_len = struct.unpack('<Q', raw_bytes[offset:offset+8])[0]
    offset += 8
    data_bytes = raw_bytes[offset:offset+data_len]

    # Check if dictionary-encoded
    is_dict = struct.unpack('<I', data_bytes[0:4])[0]
    data_offset = 4

    if is_dict:
        # Dictionary-encoded: indices + dictionary
        indices_len = struct.unpack('<Q', data_bytes[data_offset:data_offset+8])[0]
        data_offset += 8
        indices_bytes = data_bytes[data_offset:data_offset+indices_len]
        data_offset += indices_len

        dict_len = struct.unpack('<Q', data_bytes[data_offset:data_offset+8])[0]
        data_offset += 8
        dict_bytes = data_bytes[data_offset:data_offset+dict_len]

        # Reconstruct dictionary
        dictionary = json.loads(dict_bytes.decode('utf-8'))

        # Reconstruct values using indices
        cdef int num_indices = indices_len // 4
        cdef list values = []
        for i in range(num_indices):
            idx = struct.unpack('<i', indices_bytes[i*4:(i+1)*4])[0]
            if 0 <= idx < len(dictionary):
                values.append(dictionary[idx])
            else:
                values.append(None)
    else:
        # Plain strings
        string_json = data_bytes[4:].decode('utf-8')
        values = json.loads(string_json)

    # Create StringVector
    vector = StringVector.from_list(values)

    return {
        'vector': vector,
        'null_bitmap': null_bitmap,
        'null_count': descriptor.null_count,
    }


cpdef dict deserialize_row_group(dict column_descriptors, MemoryPool pool):
    """Deserialize all columns in a row group.

    Args:
        column_descriptors: Dict of column_name → ColumnDescriptor
        pool: MemoryPool to read from

    Returns:
        Dict of column_name → {'vector': ..., 'null_bitmap': ..., 'null_count': ...}
    """
    row_group = {}
    for col_name, descriptor_dict in column_descriptors.items():
        descriptor = ColumnDescriptor.from_dict(descriptor_dict)
        row_group[col_name] = deserialize_column(descriptor, pool)
    return row_group
