"""Utility to call the authorize service `/v1/evaluate` endpoint.

This module is intentionally lightweight and designed to be copy-pastable into
other repositories. It provides:

- `Action` enum with valid permission actions (read, write, delete, create)
- `check_permission` function to call the evaluate endpoint and return a
  structured `EvaluateResponse` object
- `is_allowed` convenience to check a single resource

Usage example:

    from check_permission import check_permission, Action

    resp = check_permission(
        base_url="https://authorize.opteryx.app",
        bearer_token="my-token",
        principal="me",
        action=Action.READ,
        resources=["analytics.sales.orders"],
    )
    print(resp.decision, resp.results)

Environment variables honored:
- `AUTHORIZE_URL` - base URL for the authorize service (default: https://authorize.opteryx.app)
- `AUTHORIZE_BEARER_TOKEN` - bearer token to use if one is not passed explicitly
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import requests


class Action(str, Enum):
    """Valid permission actions."""

    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    CREATE = "create"


@dataclass
class ResourceResult:
    resource: str
    predicate: str
    policy: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass
class EvaluateResponse:
    decision: str
    results: List[ResourceResult]


class PermissionCheckError(Exception):
    """Raised when there is a problem checking permissions."""


def _get_base_url(base_url: Optional[str]) -> str:
    if base_url:
        return base_url.rstrip("/")
    return "https://authorize.opteryx.app"


def _get_token(bearer_token: Optional[str]) -> Optional[str]:
    if bearer_token:
        return bearer_token
    return os.environ.get("AUTHORIZE_BEARER_TOKEN")


def _parse_result(obj: Dict[str, Any]) -> ResourceResult:
    return ResourceResult(
        resource=obj.get("resource"),
        predicate=obj.get("predicate"),
        policy=obj.id,
        raw=obj,
    )


def check_permission(
    *,
    base_url: Optional[str] = None,
    bearer_token: Optional[str] = None,
    principal: str | Dict[str, Any],
    action: Action | str,
    resources: List[str],
    timeout: float = 5.0,
) -> EvaluateResponse:
    """Call the authorize service `/v1/evaluate` endpoint and return a parsed response.

    Args:
        base_url: Base URL for the authorize service (defaults to env `AUTHORIZE_URL`).
        bearer_token: Optional bearer token for Authorization header (defaults to env `AUTHORIZE_BEARER_TOKEN`).
        principal: Principal to evaluate (e.g. "me" or {"identity": "alice"}).
        action: Permission action (use `Action` enum or a string).
        resources: List of resource strings to evaluate.
        timeout: Request timeout in seconds.

    Returns:
        `EvaluateResponse` containing overall decision and per-resource results.

    Raises:
        PermissionCheckError on network or service errors.
    """

    burl = _get_base_url(base_url)
    token = _get_token(bearer_token)

    url = f"{burl}/v1/evaluate"

    # Normalize action
    action_val = action.value if isinstance(action, Action) else str(action)

    payload = {"principal": principal, "action": action_val, "resources": resources}

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
    except requests.RequestException as exc:  # network-level errors
        raise PermissionCheckError("network error while contacting authorize service") from exc

    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        raise PermissionCheckError(
            f"authorize service returned status {resp.status_code}: {resp.text}"
        ) from exc

    try:
        data = resp.json()
    except ValueError as exc:
        raise PermissionCheckError("invalid JSON response from authorize service") from exc

    decision = data.get("decision")
    if decision is None:
        raise PermissionCheckError("missing `decision` in response")

    raw_results = data.get("results", [])
    results = [_parse_result(r) for r in raw_results]

    return EvaluateResponse(decision=decision, results=results)


def is_allowed(
    base_url: Optional[str] = None,
    bearer_token: Optional[str] = None,
    principal: str | Dict[str, Any] = "me",
    action: Action | str = Action.READ,
    resource: str | None = None,
    resources: Optional[List[str]] = None,
    timeout: float = 5.0,
) -> bool:
    """Convenience helper to check permission for a single resource or a list.

    If `resource` is provided it checks only that resource. Otherwise `resources` must
    be provided and the overall decision is returned (True when decision == "allow").
    """

    if resource and resources:
        raise ValueError("provide either 'resource' or 'resources', not both")

    if resource:
        resources_list = [resource]
    elif resources:
        resources_list = resources
    else:
        raise ValueError("either 'resource' or 'resources' must be provided")

    resp = check_permission(
        base_url=base_url,
        bearer_token=bearer_token,
        principal=principal,
        action=action,
        resources=resources_list,
        timeout=timeout,
    )

    return resp.decision == "allow"


__all__ = [
    "Action",
    "check_permission",
    "is_allowed",
    "EvaluateResponse",
    "ResourceResult",
    "PermissionCheckError",
]
