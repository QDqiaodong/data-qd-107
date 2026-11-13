"""Command line entry point for the export-task capture client.

Usage::

    python -m contract_warehouse.capture run \
        --endpoint http://127.0.0.1:8080 --request-id ID \
        --state FILE --output DIR
    python -m contract_warehouse.capture status --state FILE

``run`` is serially resumable: repeat the exact same command after a crash,
lost creation receipt or partial download.  ``status`` is strictly read-only:
it never opens a network connection and never creates a database.
"""

from __future__ import annotations

import argparse
import json
import sqlite3

from . import engine
from .errors import CaptureError


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="contract_warehouse.capture",
        description="Resumable loopback export-task capture client",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser(
        "run", help="create/resume one export task and deliver its directory"
    )
    run_parser.add_argument("--endpoint", required=True)
    run_parser.add_argument("--request-id", required=True)
    run_parser.add_argument("--state", required=True)
    run_parser.add_argument("--output", required=True)

    status_parser = sub.add_parser(
        "status", help="read-only local state projection (no network, no db create)"
    )
    status_parser.add_argument("--state", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            receipt = engine.run(
                endpoint=args.endpoint,
                request_id=args.request_id,
                state_path=args.state,
                output=args.output,
            )
            print(json.dumps(receipt, sort_keys=True, ensure_ascii=False))
            return
        snapshot = engine.status(args.state)
        print(json.dumps(snapshot, sort_keys=True, ensure_ascii=False))
    except (CaptureError, OSError, sqlite3.Error, OverflowError) as exc:
        parser.exit(2, f"{type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
