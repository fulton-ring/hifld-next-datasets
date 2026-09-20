"""CLI for an inspectable local two-bucket Portolan Dagster run."""

from __future__ import annotations

import argparse
from pathlib import Path

from .workflow import execute_portolan_manifest, rollback_portolan_release


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("promote", "rollback"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument(
        "--generation", help="Complete release UUID to select for rollback."
    )
    parser.add_argument("--catalog-only", action="store_true")
    parser.add_argument(
        "--dagster-home",
        type=Path,
        default=Path(".local/dagster"),
        help="Persistent local Dagster run storage.",
    )
    args = parser.parse_args()
    if args.command == "rollback":
        if args.generation is None:
            parser.error("rollback requires --generation")
        pointer = rollback_portolan_release(args.generation)
        print(f"generation={pointer.generation} rolled_back=true")
        return
    if args.manifest is None:
        parser.error("promote requires --manifest")
    result = execute_portolan_manifest(
        args.manifest, args.dagster_home, catalog_only=args.catalog_only
    )
    print(f"run_id={result.run_id} success={result.success}")
    if not result.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
