from __future__ import annotations

import argparse
import sys

from . import client as client_mod
from .exceptions import ConfigError, SocketTelematicsError
from .server import run_server


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="socket_telematics",
        description="Socket Telematics: TCP telemetry server + simulated clients",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    server_p = sub.add_parser("server", help="Start the telemetry TCP server")
    server_p.add_argument("--host", default="127.0.0.1")
    server_p.add_argument("--port", default=5000, type=int)
    server_p.add_argument("--db", default="socket_telematics.db", help="SQLite DB path")
    server_p.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    server_p.add_argument(
        "--status-refresh",
        default=1.0,
        type=float,
        help="Seconds between status-line updates (active clients always visible)",
    )

    client_p = sub.add_parser("client", help="Run a simulated client")
    client_p.add_argument("--host", default="127.0.0.1")
    client_p.add_argument("--port", default=5000, type=int)
    client_p.add_argument("--client-id", required=True)
    client_p.add_argument("--interval", default=1.0, type=float, help="Default send interval (seconds)")
    client_p.add_argument(
        "--interval-config",
        default=None,
        help="Path to per-client interval file (.txt/.xml). Overrides --interval when present for that client.",
    )
    client_p.add_argument(
        "--fault",
        action="append",
        default=[],
        choices=["overtemp", "highrpm", "lowfuel", "highspeed"],
        help="Trigger alerts by injecting a fault condition (repeatable)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "server":
            return run_server(
                host=args.host,
                port=args.port,
                db_path=args.db,
                log_level=args.log_level,
                status_refresh_s=args.status_refresh,
            )

        if args.command == "client":
            return client_mod.run_client(
                host=args.host,
                port=args.port,
                client_id=args.client_id,
                interval_s=args.interval,
                interval_config=args.interval_config,
                faults=set(args.fault),
            )

        parser.error(f"Unknown command: {args.command}")
        return 2

    except ConfigError as exc:
        print(f"config_error: {exc}", file=sys.stderr)
        return 2
    except SocketTelematicsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
