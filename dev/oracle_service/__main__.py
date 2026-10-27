"""Run the oracle:  cd dev && python -m oracle_service --host 192.168.4.10 --port 8765"""

import argparse

from .service import DEFAULT_MAX_BODY_BYTES, OracleServer, readers


def main() -> None:
    parser = argparse.ArgumentParser(prog="oracle_service", description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="interface to bind (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--max-body-mib",
        type=int,
        default=DEFAULT_MAX_BODY_BYTES // (1024 * 1024),
        help="largest accepted request body in MiB (default %(default)s)",
    )
    args = parser.parse_args()
    server = OracleServer((args.host, args.port), args.max_body_mib * 1024 * 1024)
    print("oracle listening on http://%s:%d  readers=%s" % (args.host, server.server_port, readers()), flush=True)
    server.serve_forever()


main()
