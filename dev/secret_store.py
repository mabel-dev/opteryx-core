"""Secrets from the Vaultwarden at vault.internal, read through the Bitwarden CLI
(`brew install bitwarden-cli`).

The bootstrap credentials are SECRET_MANAGER_USERNAME / SECRET_MANAGER_PASSWORD in the
repo-root .env. Each secret is a Vaultwarden item named exactly like its environment
variable; its value is the item's login password, or the note of a secure note.

    from secret_store import get_secret                         # with dev/ on sys.path
    python dev/secret_store.py list                              # item names, never values
    python dev/secret_store.py run NAME [NAME ...] -- CMD ...   # CMD runs with NAMEs in its env

Local only: Vaultwarden is on the LAN, CI cannot reach it. Nothing here prints a value.
The bw state lives in ~/.config/opteryx-secrets/bw, apart from any personal bw login; the
server's self-signed certificate is pinned at ~/.config/opteryx-secrets/vault.internal.pem.
"""

from __future__ import annotations

import functools
import json
import os
import subprocess
import sys

import dotenv

SERVER = "https://vault.internal:8443"
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_DIR = os.path.expanduser("~/.config/opteryx-secrets")
CA_FILE = os.path.join(STATE_DIR, "vault.internal.pem")


def _bw(*args: str, session: str | None = None, password: str | None = None) -> str:
    env = {k: v for k, v in os.environ.items() if k not in ("BW_SESSION", "BW_PASSWORD")}
    env["BITWARDENCLI_APPDATA_DIR"] = os.path.join(STATE_DIR, "bw")
    env["NODE_EXTRA_CA_CERTS"] = CA_FILE
    if session is not None:
        env["BW_SESSION"] = session
    if password is not None:
        env["BW_PASSWORD"] = password
    result = subprocess.run(["bw", "--nointeraction", *args], env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"bw {args[0]} failed (rc {result.returncode}): {result.stderr.strip()}")
    return result.stdout


@functools.cache
def _items() -> dict[str, list[dict]]:
    """Unlock once per process and return every visible item, grouped by name."""
    if not os.path.exists(CA_FILE):
        raise FileNotFoundError(
            f"{CA_FILE} is missing - pin the server certificate with: openssl s_client -connect "
            f"vault.internal:8443 </dev/null 2>/dev/null | openssl x509 > {CA_FILE}"
        )
    bootstrap = dotenv.dotenv_values(os.path.join(REPO_ROOT, ".env"))
    user = bootstrap["SECRET_MANAGER_USERNAME"]
    password = bootstrap["SECRET_MANAGER_PASSWORD"]

    status = json.loads(_bw("status"))
    if status["status"] == "unauthenticated":
        if status["serverUrl"] != SERVER:
            _bw("config", "server", SERVER)
        session = _bw("login", user, "--passwordenv", "BW_PASSWORD", "--raw", password=password)
    else:
        if status["serverUrl"] != SERVER or status["userEmail"].lower() != user.lower():
            raise RuntimeError(
                f"bw state in {STATE_DIR}/bw is logged in to {status['serverUrl']} as another user; "
                f"run `BITWARDENCLI_APPDATA_DIR={STATE_DIR}/bw bw logout` and retry"
            )
        session = _bw("unlock", "--passwordenv", "BW_PASSWORD", "--raw", password=password)
        _bw("sync", session=session)

    grouped: dict[str, list[dict]] = {}
    for item in json.loads(_bw("list", "items", session=session)):
        grouped.setdefault(item["name"], []).append(item)
    return grouped


def get_secret(name: str) -> str:
    """The value of the Vaultwarden item called `name`; raises KeyError if there is none."""
    matches = _items().get(name)
    if matches is None:
        raise KeyError(f"no Vaultwarden item named {name!r} visible to SECRET_MANAGER_USERNAME")
    if len(matches) > 1:
        raise RuntimeError(f"{len(matches)} Vaultwarden items are named {name!r}; names must be unique")
    item = matches[0]
    if item["type"] == 1:
        value = item["login"]["password"]
    elif item["type"] == 2:
        value = item["notes"]
    else:
        raise RuntimeError(f"Vaultwarden item {name!r} is type {item['type']}; secrets are logins or secure notes")
    if not value:
        raise ValueError(f"Vaultwarden item {name!r} has no value")
    return value


def main(argv: list[str]) -> int:
    if argv == ["list"]:
        for name in sorted(_items()):
            print(name)
        return 0
    if argv[:1] == ["run"] and "--" in argv:
        split = argv.index("--")
        names, command = argv[1:split], argv[split + 1 :]
        if names and command:
            env = dict(os.environ)
            env.update({name: get_secret(name) for name in names})
            os.execvpe(command[0], command, env)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
