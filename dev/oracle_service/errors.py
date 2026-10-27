"""Errors the oracle raises on purpose, each carrying the HTTP status it maps to."""


class OracleError(Exception):
    status = 500
    kind = "oracle_internal"


class BadRequest(OracleError):
    status = 400
    kind = "bad_request"


class LengthRequired(OracleError):
    status = 411
    kind = "length_required"


class BodyTooLarge(OracleError):
    status = 413
    kind = "too_large"


class Unsupported(OracleError):
    """A file feature the oracle refuses to describe rather than describe wrongly."""

    status = 422
    kind = "unsupported"


class ReaderDisagreement(OracleError):
    """Two independent readers saw different things in the same file."""

    status = 500
    kind = "reader_disagreement"
