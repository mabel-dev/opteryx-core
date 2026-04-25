# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""
Column descriptor serialization for zero-copy IPC via MemoryPool.

Stores column metadata (type, num_rows, encoding) separately from raw bytes.
Format:
  [4 bytes: name_len | name | type_len | type | num_rows (int64) | null_count (int64) | metadata_len | metadata]
"""

from libc.stdint cimport int64_t, uint32_t
from cpython.bytes cimport PyBytes_FromStringAndSize
import struct
from typing import Dict, Any

cdef class ColumnDescriptor:
    """Lightweight descriptor for a column in MemoryPool."""

    cdef public str column_name
    cdef public str column_type
    cdef public int64_t num_rows
    cdef public int64_t null_count
    cdef public int64_t ref_id
    cdef public int64_t data_offset
    cdef public int64_t data_length
    cdef public dict metadata

    def __cinit__(self, str column_name, str column_type, int64_t num_rows,
                  int64_t null_count=0, int64_t ref_id=-1,
                  int64_t data_offset=0, int64_t data_length=0, dict metadata=None):
        self.column_name = column_name
        self.column_type = column_type
        self.num_rows = num_rows
        self.null_count = null_count
        self.ref_id = ref_id
        self.data_offset = data_offset
        self.data_length = data_length
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert descriptor to dict for transport."""
        return {
            'column_name': self.column_name,
            'column_type': self.column_type,
            'num_rows': self.num_rows,
            'null_count': self.null_count,
            'ref_id': self.ref_id,
            'data_offset': self.data_offset,
            'data_length': self.data_length,
            'metadata': self.metadata,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> ColumnDescriptor:
        """Create descriptor from dict."""
        return ColumnDescriptor(
            column_name=d['column_name'],
            column_type=d['column_type'],
            num_rows=d['num_rows'],
            null_count=d.get('null_count', 0),
            ref_id=d.get('ref_id', -1),
            data_offset=d.get('data_offset', 0),
            data_length=d.get('data_length', 0),
            metadata=d.get('metadata', {}),
        )


cpdef bytes serialize_descriptor(ColumnDescriptor desc):
    """Serialize descriptor to bytes for storage in MemoryPool metadata."""
    import json

    # Encode strings to bytes
    name_bytes = desc.column_name.encode('utf-8')
    type_bytes = desc.column_type.encode('utf-8')
    metadata_bytes = json.dumps(desc.metadata).encode('utf-8')

    # Pack: name_len, name, type_len, type, num_rows, null_count, metadata_len, metadata
    parts = []
    parts.append(struct.pack('<I', len(name_bytes)))
    parts.append(name_bytes)
    parts.append(struct.pack('<I', len(type_bytes)))
    parts.append(type_bytes)
    parts.append(struct.pack('<q', desc.num_rows))
    parts.append(struct.pack('<q', desc.null_count))
    parts.append(struct.pack('<I', len(metadata_bytes)))
    parts.append(metadata_bytes)

    return b''.join(parts)


cpdef ColumnDescriptor deserialize_descriptor(bytes data):
    """Deserialize descriptor from bytes."""
    import json

    cdef int offset = 0

    # Read name
    name_len = struct.unpack('<I', data[offset:offset+4])[0]
    offset += 4
    name = data[offset:offset+name_len].decode('utf-8')
    offset += name_len

    # Read type
    type_len = struct.unpack('<I', data[offset:offset+4])[0]
    offset += 4
    col_type = data[offset:offset+type_len].decode('utf-8')
    offset += type_len

    # Read num_rows
    num_rows = struct.unpack('<q', data[offset:offset+8])[0]
    offset += 8

    # Read null_count
    null_count = struct.unpack('<q', data[offset:offset+8])[0]
    offset += 8

    # Read metadata
    metadata_len = struct.unpack('<I', data[offset:offset+4])[0]
    offset += 4
    metadata = json.loads(data[offset:offset+metadata_len].decode('utf-8'))

    return ColumnDescriptor(
        column_name=name,
        column_type=col_type,
        num_rows=num_rows,
        null_count=null_count,
        metadata=metadata,
    )


cpdef list serialize_row_group(dict row_group):
    """Serialize a row group's column descriptors.

    Returns list of (column_name, descriptor_dict) tuples for queue transport.
    """
    descriptors = []
    for col_name, descriptor in row_group.items():
        if isinstance(descriptor, ColumnDescriptor):
            descriptors.append((col_name, descriptor.to_dict()))
        elif isinstance(descriptor, dict):
            descriptors.append((col_name, descriptor))
    return descriptors


cpdef dict deserialize_row_group(list descriptors):
    """Reconstruct row group dict from serialized descriptors."""
    row_group = {}
    for col_name, desc_dict in descriptors:
        row_group[col_name] = ColumnDescriptor.from_dict(desc_dict)
    return row_group
