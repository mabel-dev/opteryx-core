"""
Oracle service: an independent-reader inspection service for the rugo release floor.

POST the bytes of a Parquet, ORC or Avro file; get back canonical JSON describing
what PyArrow / fastavro / Apache avro see. Dev tooling only — never imported by
production code, and it must never import rugo, draken or opteryx.

This module is deliberately import-light (no third-party imports) so the client can
be copied to a host that has no PyArrow.
"""

SCHEMA_VERSION = 1
