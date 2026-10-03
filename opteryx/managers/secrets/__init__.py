# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The secret-resolver capability - how `credentials =>` on a READ_* becomes a credential.

    SELECT * FROM READ_PARQUET('gs://bucket/prefix/*.parquet',
                               credentials => 'analytics.billing_reader')

The statement names a stored customer secret; it never carries one (jobs.opteryx
docs/design/secrets.md §8.2). Turning the name into a credential is a
DEPLOYMENT's business - where secrets are stored, how they are decrypted, who
may use which - so the engine holds none of it. A deployment registers a
resolver at start-up, the way it registers its permissions capability:

    opteryx.register_secret_resolver(resolver)

where `resolver(execution_context, reference, path) -> ObjectStoreCredential`
answers for the session doing the asking, or raises. The worker's resolver reads
the secret from the catalog, unwraps it under KMS, and checks (§9.1): the
reference names a secret that exists, the caller holds the right to use it, its
type matches the path's scheme, and the path is under its SCOPE.

**With no resolver registered, `credentials =>` is refused.** Not answered with
ambient credentials, not read anonymously: a deployment that has not said how
secrets resolve has no secrets, and a statement asking for one is an error. The
same rule as a missing permissions capability, made stricter because the
permissive answer here would be the dangerous one.

The returned credential is held by one query's filesystem and dropped with it.
It refuses to print or pickle, so it cannot reach plan text, EXPLAIN, an error
message or a cache by accident.
"""

import re
from typing import Callable
from typing import Optional

from opteryx.exceptions import InvalidConfigurationError

__all__ = (
    "ObjectStoreCredential",
    "register_secret_resolver",
    "resolve_secret",
    "secret_resolver_registered",
)

GCS_SERVICE_ACCOUNT = "gcs_service_account"
AWS_ACCESS_KEY = "aws_access_key"

# The scheme each credential kind may be pointed at.
SCHEMES = {GCS_SERVICE_ACCOUNT: "gs", AWS_ACCESS_KEY: "s3"}

# `workspace.name` - qualified, because a bare READ_* has no workspace of its own
# to resolve a name against.
_REFERENCE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")

_resolver: Optional[Callable] = None


class ObjectStoreCredential:
    """A resolved object-store credential, for one query.

    `secret` is the decrypted payload in the catalog's shape -
    `{"key": <service-account JSON>}` for `gcs_service_account`;
    `{"access_key_id", "secret_access_key", "session_token"?, "region"?}` for
    `aws_access_key`. `admits(path)` is the secret's SCOPE check, supplied by
    the resolver so there is one implementation of it (the catalog's); the
    engine calls it for the literal path and for every file a glob expands to.
    """

    __slots__ = ("kind", "reference", "_secret", "_admits")

    def __init__(self, *, kind: str, reference: str, secret: dict, admits: Callable[[str], bool]):
        if kind not in SCHEMES:
            raise ValueError(f"unknown credential kind {kind!r}")
        if not callable(admits):
            raise ValueError("a credential must carry its scope check")
        self.kind = kind
        self.reference = reference
        self._secret = secret
        self._admits = admits

    @property
    def scheme(self) -> str:
        return SCHEMES[self.kind]

    @property
    def secret(self) -> dict:
        return self._secret

    def admits(self, path: str) -> bool:
        try:
            return bool(self._admits(path))
        except Exception:  # a scope check that cannot answer refuses
            return False

    def __repr__(self) -> str:
        return f"<ObjectStoreCredential {self.reference} kind={self.kind}>"

    __str__ = __repr__

    def __reduce__(self):
        raise TypeError("a resolved credential cannot be serialised")

    def __deepcopy__(self, memo):
        # Plans are deep-copied by the optimizer; the credential is shared,
        # never duplicated, so there is exactly one object to drop.
        return self


def register_secret_resolver(resolver: Optional[Callable]) -> None:
    """Install `resolver(execution_context, reference, path) -> ObjectStoreCredential`.

    None uninstalls it, which makes `credentials =>` refused again.
    """
    global _resolver
    if resolver is not None and not callable(resolver):
        raise InvalidConfigurationError(
            config_item="secret_resolver",
            provided_value=type(resolver).__name__,
            valid_value_description="a callable (execution_context, reference, path) -> credential",
        )
    _resolver = resolver


def secret_resolver_registered() -> bool:
    return _resolver is not None


def validate_reference(reference) -> str:
    """The `credentials =>` value, checked for shape. Raises ValueError."""
    if not isinstance(reference, str) or not _REFERENCE.match(reference):
        raise ValueError(
            "credentials => names a secret as '<workspace>.<name>', for example "
            "credentials => 'analytics.billing_reader'"
        )
    return reference.lower()


def resolve_secret(execution_context, reference: str, path: str) -> ObjectStoreCredential:
    """Resolve `reference` for a read of `path`, through the registered resolver.

    Raises PermissionError when no resolver is registered, and lets the
    resolver's own refusals through: an access check that refused is the
    caller's to see, and the resolver's messages name nothing from a secret but
    its name.
    """
    reference = validate_reference(reference)
    if _resolver is None:
        raise PermissionError(
            "credentials => is not available: this deployment has no secret resolver, "
            "so no stored secret can be used"
        )
    credential = _resolver(execution_context, reference, path)
    if not isinstance(credential, ObjectStoreCredential):
        raise InvalidConfigurationError(
            config_item="secret_resolver",
            provided_value=type(credential).__name__,
            valid_value_description="an ObjectStoreCredential",
        )
    return credential
