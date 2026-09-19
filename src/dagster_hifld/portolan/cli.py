"""CLI for an inspectable local two-bucket Portolan Dagster run."""

from __future__ import annotations

import argparse
from pathlib import Path

from .workflow import execute_portolan_manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("promote",))
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--catalog-only", action="store_true")
    parser.add_argument(
        "--dagster-home",
        type=Path,
        default=Path(".local/dagster"),
        help="Persistent local Dagster run storage.",
    )
    args = parser.parse_args()
    result = execute_portolan_manifest(
        args.manifest, args.dagster_home, catalog_only=args.catalog_only
    )
    print(f"run_id={result.run_id} success={result.success}")
    if not result.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
